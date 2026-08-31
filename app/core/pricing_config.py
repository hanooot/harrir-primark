"""
Dynamic pricing config fetched from the commission API.

Each cron run calls `fetch_pricing_config()` once at start and threads the
resulting `PricingConfig` through the price-update pipeline. This replaces the
previously-hardcoded category weights/CBM, commission tiers, and shipping
constants in `app.core.task_primark`.

Endpoints (base = AUTOMATION_BACKEND_URL):
  POST {host}/auth/login   → mints a fresh JWT each cron run
  GET  /commission/get_all_categories_weight?page=1&limit=1000
  GET  /commission/shipping-constants
  GET  /commission/formulas    (kept for parity but not parsed — current Python
                                in task_primark.build_price_payload already
                                implements the same formulas)

Auth: Bearer JWT minted via /auth/login at the start of each cron run, using
COMMISSION_API_EMAIL + COMMISSION_API_PASSWORD. Logging in per cron run avoids
the JWT-expiry rotation problem (tokens are short-lived).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from app.config import get_config

logger = logging.getLogger(__name__)

CATEGORY_LIMIT = 1000
HTTP_TIMEOUT = 15.0
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0


@dataclass(frozen=True)
class CommissionTier:
    min: float
    max: Optional[float]
    multiplier: float


@dataclass(frozen=True)
class CategoryEntry:
    parent_name: Optional[str]
    child_name: Optional[str]
    full_path: str
    slug: str
    is_subcategory: bool
    weight: float
    cbm: float
    tiers: Tuple[CommissionTier, ...]


@dataclass(frozen=True)
class ShippingConstants:
    sea_usd_per_cbm: float
    air_usd_per_kg: float
    min_air_usd: float


@dataclass(frozen=True)
class PricingConfig:
    """Snapshot of remote pricing config for a single cron run."""

    categories: Tuple[CategoryEntry, ...]
    shipping: ShippingConstants
    # Default tiers used when a category isn't found in the remote list.
    # Sourced from the first category's tiers (they're uniform in current API).
    default_tiers: Tuple[CommissionTier, ...] = field(default_factory=tuple)
    # Default weight/cbm fallback when nothing matches.
    default_weight: float = 0.5
    default_cbm: float = 0.005

    # Indexes built in __post_init__ for fast lookup.
    _by_full_path: Dict[str, CategoryEntry] = field(default_factory=dict, repr=False)
    _by_child_name: Dict[str, CategoryEntry] = field(default_factory=dict, repr=False)
    _by_parent_name: Dict[str, CategoryEntry] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        # frozen=True blocks setattr; use object.__setattr__ for the indexes.
        by_full_path: Dict[str, CategoryEntry] = {}
        by_child: Dict[str, CategoryEntry] = {}
        by_parent: Dict[str, CategoryEntry] = {}
        for entry in self.categories:
            by_full_path[entry.full_path.strip().lower()] = entry
            if entry.child_name:
                # Last-writer-wins is fine — duplicate child names across parents
                # will fall back to the full-path lookup when both parent + child
                # are provided in the input string.
                by_child[entry.child_name.strip().lower()] = entry
            if entry.parent_name and not entry.is_subcategory:
                by_parent[entry.parent_name.strip().lower()] = entry
        object.__setattr__(self, "_by_full_path", by_full_path)
        object.__setattr__(self, "_by_child_name", by_child)
        object.__setattr__(self, "_by_parent_name", by_parent)

    def lookup(self, category_name: str) -> Optional[CategoryEntry]:
        """Find the category entry matching `category_name`.

        Accepts either "Category" or "Category > Subcategory".
        """
        if not category_name:
            return None
        name = category_name.strip()
        if not name:
            return None

        if ">" in name:
            parts = [p.strip() for p in name.split(">") if p.strip()]
            full = " > ".join(parts)
            entry = self._by_full_path.get(full.lower())
            if entry:
                return entry
            # Fall back to last segment as a child-name lookup.
            if parts:
                entry = self._by_child_name.get(parts[-1].lower())
                if entry:
                    return entry
            # Fall back to first segment as parent.
            if parts:
                entry = self._by_parent_name.get(parts[0].lower())
                if entry:
                    return entry
            return None

        # No ">": try parent, then full_path, then child-name.
        lower = name.lower()
        return (
            self._by_parent_name.get(lower)
            or self._by_full_path.get(lower)
            or self._by_child_name.get(lower)
        )

    def get_weight_cbm_tiers(
        self, category_name: str
    ) -> Tuple[float, float, Tuple[CommissionTier, ...]]:
        entry = self.lookup(category_name)
        if entry and entry.weight > 0 and entry.cbm > 0:
            return entry.weight, entry.cbm, entry.tiers or self.default_tiers
        # Either no match, or matched a parent-only stub with zeros — fall back.
        return self.default_weight, self.default_cbm, self.default_tiers


def calculate_commission(
    price_usd: float, tiers: Tuple[CommissionTier, ...]
) -> float:
    """Apply tiered commission to a USD price using the first matching tier."""
    if price_usd <= 0 or not tiers:
        return 0.0
    for tier in tiers:
        upper = tier.max if tier.max is not None else float("inf")
        if tier.min <= price_usd <= upper:
            return price_usd * tier.multiplier
    # Past the last tier's upper bound — use the open-ended (max=None) tier
    # if present, else the last one.
    open_ended = next((t for t in tiers if t.max is None), None)
    last = open_ended or tiers[-1]
    return price_usd * last.multiplier


def calculate_shipping_sea(cbm: float, shipping: ShippingConstants) -> float:
    return round(cbm * 1.10 * shipping.sea_usd_per_cbm, 2)


def calculate_shipping_air(weight_kg: float, shipping: ShippingConstants) -> float:
    return round(
        max(shipping.min_air_usd, weight_kg * 1.10 * shipping.air_usd_per_kg), 2
    )


def _parse_tier(raw: Dict[str, Any]) -> CommissionTier:
    return CommissionTier(
        min=float(raw.get("min") or 0),
        max=(float(raw["max"]) if raw.get("max") is not None else None),
        multiplier=float(raw.get("multiplier") or 0),
    )


def _parse_category(raw: Dict[str, Any]) -> CategoryEntry:
    tiers_raw = raw.get("tiers") or []
    tiers = tuple(_parse_tier(t) for t in tiers_raw)
    return CategoryEntry(
        parent_name=raw.get("parent_name"),
        child_name=raw.get("child_name"),
        full_path=str(raw.get("full_path") or "").strip(),
        slug=str(raw.get("slug") or "").strip(),
        is_subcategory=bool(raw.get("is_subcategory")),
        weight=float(raw.get("weight") or 0),
        cbm=float(raw.get("cbm") or 0),
        tiers=tiers,
    )


async def _get_with_retries(
    client: httpx.AsyncClient, url: str, headers: Dict[str, str]
) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)
                logger.warning(
                    f"pricing_config: GET {url} attempt {attempt}/{MAX_RETRIES} "
                    f"failed: {exc}; retrying"
                )
    raise RuntimeError(
        f"pricing_config: GET {url} failed after {MAX_RETRIES} attempts: {last_exc}"
    )


def _auth_login_url(backend_url: str) -> str:
    """Derive {scheme}://{host}/auth/login from AUTOMATION_BACKEND_URL.

    The commission endpoints sit under `/api/v1/...` but the login endpoint
    lives at the host root (`/auth/login`), so we strip the path prefix.
    """
    parsed = urlparse(backend_url)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError(
            f"pricing_config: AUTOMATION_BACKEND_URL is malformed: {backend_url!r}"
        )
    return f"{parsed.scheme}://{parsed.netloc}/auth/login"


async def _login(client: httpx.AsyncClient, login_url: str, email: str, password: str) -> str:
    """POST credentials to /auth/login and return the access_token.

    Retries on transient failures the same way as the data calls. Raises on
    bad credentials / persistent failure — callers should surface that loudly
    rather than silently degrade.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.post(
                login_url,
                json={"email": email, "password": password},
                headers={"accept": "application/json", "Content-Type": "application/json"},
            )
            # 401/422 mean creds are wrong — no point retrying.
            if resp.status_code in (401, 403, 422):
                raise RuntimeError(
                    f"pricing_config: /auth/login rejected credentials "
                    f"(status {resp.status_code}): {resp.text[:200]}"
                )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("access_token")
            if not token:
                raise RuntimeError(
                    f"pricing_config: /auth/login response missing access_token: {data}"
                )
            return token
        except RuntimeError:
            raise
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)
                logger.warning(
                    f"pricing_config: /auth/login attempt {attempt}/{MAX_RETRIES} "
                    f"failed: {exc}; retrying"
                )
    raise RuntimeError(
        f"pricing_config: /auth/login failed after {MAX_RETRIES} attempts: {last_exc}"
    )


async def fetch_pricing_config() -> PricingConfig:
    """Fetch all dynamic pricing data once. Raises on persistent failure.

    Flow per call: login → fetch categories + shipping in parallel.

    Cron entrypoints should call this at the start of a run and pass the
    returned `PricingConfig` through the pipeline. We deliberately do NOT
    fall back to stale/hardcoded constants — wrong prices going to production
    are worse than skipping a run.
    """
    cfg = get_config()
    base = (cfg.AUTOMATION_BACKEND_URL or "").rstrip("/")
    if not base:
        raise RuntimeError("pricing_config: AUTOMATION_BACKEND_URL is not configured")
    if not cfg.COMMISSION_API_EMAIL or not cfg.COMMISSION_API_PASSWORD:
        raise RuntimeError(
            "pricing_config: COMMISSION_API_EMAIL / COMMISSION_API_PASSWORD not configured"
        )

    login_url = _auth_login_url(base)
    cats_url = f"{base}/commission/get_all_categories_weight?page=1&limit={CATEGORY_LIMIT}"
    ship_url = f"{base}/commission/shipping-constants"

    timeout = httpx.Timeout(HTTP_TIMEOUT)
    async with httpx.AsyncClient(timeout=timeout) as client:
        token = await _login(
            client, login_url, cfg.COMMISSION_API_EMAIL, cfg.COMMISSION_API_PASSWORD
        )
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        cats_data, ship_data = await asyncio.gather(
            _get_with_retries(client, cats_url, headers),
            _get_with_retries(client, ship_url, headers),
        )

    items: List[Dict[str, Any]] = cats_data.get("items") or []
    if not items:
        raise RuntimeError("pricing_config: categories endpoint returned no items")

    categories = tuple(_parse_category(c) for c in items)

    # Pick a default tier ladder from the first category that has non-zero
    # multipliers, so unmatched categories still get sensible commission.
    default_tiers: Tuple[CommissionTier, ...] = ()
    for entry in categories:
        if entry.tiers and any(t.multiplier > 0 for t in entry.tiers):
            default_tiers = entry.tiers
            break

    shipping = ShippingConstants(
        sea_usd_per_cbm=float(ship_data.get("sea_usd_per_cbm") or 0),
        air_usd_per_kg=float(ship_data.get("air_usd_per_kg") or 0),
        min_air_usd=float(ship_data.get("min_air_usd") or 0),
    )

    config = PricingConfig(
        categories=categories,
        shipping=shipping,
        default_tiers=default_tiers,
    )
    logger.info(
        f"pricing_config: loaded {len(categories)} categories, "
        f"shipping=(sea={shipping.sea_usd_per_cbm}/cbm, "
        f"air={shipping.air_usd_per_kg}/kg, min_air={shipping.min_air_usd})"
    )
    return config
