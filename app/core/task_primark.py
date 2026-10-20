from app.core.inventory import source_quantity, mark_missing_variants
import asyncio
from contextvars import ContextVar
import logging
import os
import random
import requests
import time
import math
import json
import re
import threading
from typing import Optional, Dict, Any, List
from collections import defaultdict
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import httpx

def remove_utm_source(url: str) -> str:
    """Removes all utm_ parameters from URL."""
    if not url:
        return url
    parsed = urlparse(url)
    query_params = parse_qsl(parsed.query)
    filtered_params = [(k, v) for k, v in query_params if not k.lower().startswith("utm_")]
    new_query = urlencode(filtered_params)
    return urlunparse(parsed._replace(query=new_query))

from rnet import Client, Impersonate

logger = logging.getLogger(__name__)


def _create_rnet_client() -> Client:
    """Build a fresh rnet client — starts a new TCP+TLS+HTTP/2 session with a reset stream-ID counter."""
    return Client(impersonate=Impersonate.Chrome128)


# Rotate the underlying rnet client after this many requests. Akamai-fronted edges reset
# HTTP/2 streams with INTERNAL_ERROR once a single connection has served too many streams; forcing
# a fresh handshake avoids the server-side reset wave that drops variants.
CLIENT_MAX_REQUESTS = 500

# Substrings that indicate the underlying connection is poisoned and must be rebuilt before retry.
_CONNECTION_POISON_PATTERNS = (
    "Reset",
    "INTERNAL_ERROR",
    "GOAWAY",
    "BrokenPipe",
    "connection closed",
    "stream closed",
    "TimedOut",
    "timeout",
)


class ScraperClient:
    """rnet.Client wrapper that rotates itself after N requests or on poisoned-connection errors."""

    def __init__(self, max_requests: int = CLIENT_MAX_REQUESTS, name: str = ""):
        self._client = _create_rnet_client()
        self._count = 0
        self._max = max_requests
        self._name = name or "default"

    def _rotate(self, reason: str) -> None:
        prev = self._count
        self._client = _create_rnet_client()
        self._count = 0
        logger.info(f"[ScraperClient:{self._name}] rotated after {prev} reqs (reason: {reason})")

    async def get(self, url: str, **kwargs):
        if self._count >= self._max:
            self._rotate("request-count threshold")
        self._count += 1
        return await self._client.get(url, **kwargs)

    def handle_exception(self, exc: BaseException) -> bool:
        """Rotate the client if the exception looks like a poisoned HTTP/2 connection. Returns True if rotated."""
        msg = repr(exc)
        if any(p in msg for p in _CONNECTION_POISON_PATTERNS):
            self._rotate(f"connection poisoned ({type(exc).__name__})")
            return True
        return False

    def rotate_on_server_throttle(self, status: int) -> None:
        """Rotate when the server returned a throttling/unavailable status — reuses the same connection are likely to get the same treatment."""
        self._rotate(f"server throttle HTTP {status}")


RNET_CLIENT: ScraperClient = ScraperClient(name="global")

from app.utils.primark_utils import (
    append_log,
    patch_product_to_api,
    delete_product_from_api,
    get_product_searching_url,
)
from app.core.primark_scraper_api import (
    PrimarkScraper,
    _parse_price_number,
    get_style_code_by_url,
    gql_get,
    get_attr,
    GET_PRODUCT_SEARCH_DETAILS_QUERY,
    SEARCH_HEADERS,
)
from app.core.product_bulk_sync import BulkProductSync
from app.core.pricing_config import (
    PricingConfig,
    fetch_pricing_config,
    calculate_commission,
    calculate_shipping_air,
    calculate_shipping_sea,
)

USD_TO_IQD = 1500.0
USD_TO_AED = 3.67

# Browser-like headers
PRIMARK_REQUEST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# Minimal projection — only fields needed for price update logic
FETCH_PROJECTION = {
    "ASIN": 1,
    "SKU": 1,
    "link": 1,
    "source_color": 1,
    "size": 1,
    "suggested_category": 1,
    "category.name.en": 1,
    "price.hanooot_price": 1,
    "price.hanooot_discount": 1,
    "availability.status": 1,
    "availability.quantity": 1,
}

def group_variants_by_url(variants: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group variants by their URL (same product/color = same URL).
    Returns: {url: [variant1, variant2, ...]}
    """
    url_groups = defaultdict(list)
    for variant in variants:
        url = variant.get("link", "")
        if url:
            url = remove_utm_source(url)
            url_groups[url].append(variant)
    return dict(url_groups)


def _pack_primark_color(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Turn one accumulated color entry into the scraped-data shape the patcher
    expects: {variant_sku, original_price, discounted_price, discount_percentage,
    size_stock, color_links}."""
    regular = float(entry.get("regular") or 0)
    final = float(entry.get("final") or 0)
    has_discount = regular > 0 and final > 0 and final < regular
    discount_pct = f"{round((regular - final) / regular * 100)}%" if has_discount else ""
    return {
        "variant_sku": "",
        "original_price": regular if regular > 0 else final,
        "discounted_price": final if has_discount else 0,
        "discount_percentage": discount_pct,
        "size_stock": entry.get("sizes", []),
        "color_links": [],
    }


async def scrape_product_data(
    url: str,
    timeout: int = 15,
    client: Optional[ScraperClient] = None,
) -> Optional[Dict[str, Any]]:
    """
    Re-fetch fresh Primark price/stock via the Magento GraphQL API.

    Primark serves no scrapeable PDP HTML (the storefront is GraphQL/Algolia
    backed), so a price refresh resolves the product's style_code from its
    url_key and queries `productSearch` for current per-size price and stock.

    For multi-color products every color shares one url_key, so the first call
    returns one synthetic per-color URL (`?clr=<color>`) in `color_links`;
    `apply_scraped_to_variants` then re-calls this function once per color so
    each color's variants are priced/stocked from their own GraphQL variant.

    Returns: {variant_sku, original_price, discounted_price,
              discount_percentage, size_stock, color_links}
    """
    parsed = urlparse(url)
    qs = dict(parse_qsl(parsed.query))
    color_filter = (qs.get("clr") or "").strip().lower()
    url_key = parsed.path.rstrip("/").split("/")[-1]
    if not url_key:
        logger.warning(f"Primark scrape: could not derive url_key from {url}")
        return None

    try:
        style_code = await asyncio.to_thread(get_style_code_by_url, url_key)
    except Exception as e:
        logger.warning(f"Primark GraphQL style lookup failed for {url_key}: {e}")
        return None
    if not style_code:
        logger.warning(f"Primark scrape: no style_code for url_key {url_key}")
        return None

    def _gql_fetch_items():
        variables = {
            "phrase": "",
            "filter": [{"attribute": "style_code", "eq": style_code}],
            "sort": [{"attribute": "price", "direction": "DESC"}],
            "pageSize": 100,
        }
        data = gql_get(GET_PRODUCT_SEARCH_DETAILS_QUERY, variables, SEARCH_HEADERS)
        return data.get("data", {}).get("productSearch", {}).get("items", []) or []

    try:
        items = await asyncio.to_thread(_gql_fetch_items)
    except Exception as e:
        logger.warning(f"Primark GraphQL fetch failed for style {style_code}: {e}")
        return None

    # Style no longer returns any product → treat as removed (patcher deletes).
    if not items:
        logger.warning(f"Primark scrape: style {style_code} returned no items (404).")
        return {"rnet_status_404": True}

    # Accumulate per-color: each color carries its size/stock list plus the
    # regular/final price (max across its variants).
    colors: Dict[str, Dict[str, Any]] = {}
    for item in items:
        pv = item.get("productView") or {}
        pv_attrs = pv.get("attributes") or []
        for vv in (pv.get("variants") or {}).get("variants") or []:
            prod = vv.get("product") or {}
            attrs = prod.get("attributes") or []
            color = get_attr(attrs, "color") or get_attr(pv_attrs, "color") or "Unknown"
            size = get_attr(attrs, "size") or ""
            in_stock = bool(prod.get("inStock"))
            price_obj = prod.get("price") or {}
            final_v = ((price_obj.get("final") or {}).get("amount") or {}).get("value")
            reg_v = ((price_obj.get("regular") or {}).get("amount") or {}).get("value")
            final_num = float(final_v) if final_v not in (None, "") else 0.0
            reg_num = float(reg_v) if reg_v not in (None, "") else 0.0

            ck = color.strip().lower()
            entry = colors.setdefault(
                ck, {"color": color, "sizes": [], "regular": 0.0, "final": 0.0}
            )
            entry["sizes"].append({
                "size": size,
                "stock": source_quantity(prod.get("stock_quantity"), "Primark variant stock_quantity") if in_stock else 0,
                "stock_status": "in_stock" if in_stock else "out_of_stock",
                "sku": prod.get("sku", ""),
            })
            entry["regular"] = max(entry["regular"], reg_num)
            entry["final"] = max(entry["final"], final_num)

    if not colors:
        logger.warning(f"Primark scrape: style {style_code} had no variant data.")
        return None

    # Recursive per-color call → return only that color's data.
    if color_filter:
        entry = colors.get(color_filter) or next(iter(colors.values()))
        return _pack_primark_color(entry)

    # First call: return the first color's data, and (for multi-color products)
    # advertise per-color URLs so the patcher re-scrapes each color separately.
    base = urlunparse(parsed._replace(query="", fragment=""))
    result = _pack_primark_color(next(iter(colors.values())))
    if len(colors) > 1:
        result["color_links"] = [
            {"color": e["color"], "url": f"{base}?clr={e['color']}"}
            for e in colors.values()
        ]
    return result


# Search-index sync for the running price update (app/core/product_bulk_sync.py):
# every parent whose write succeeded is sent to the AI-stylist /product/update/bulk
# in batches DURING the run, confirmed, and spooled to Redis if undeliverable.
# A ContextVar, not a module global: the tasks a run spawns inherit it, so two runs
# in one process never share or close each other's sync.
_PRODUCT_SYNC: ContextVar[Optional[BulkProductSync]] = ContextVar("primark_product_sync", default=None)


def _queue_search_update(parent_asin: str) -> None:
    """Queue a parent ASIN for the search index. Call only AFTER its write succeeded."""
    sync = _PRODUCT_SYNC.get()
    if sync is not None:
        sync.add(parent_asin)
    else:
        logger.warning(f"search update for {parent_asin} requested outside a price run — not queued")

# Collector for post-cron price-drop alerts. {ASIN: {"old_price": str, "new_price": str}}.
# Populated on `hanooot_price` change; flushed in product_worker.scheduled_price_update.
_PRICE_ALERTS: Dict[str, Dict[str, str]] = {}


def _derive_parent_asin(variant_asin: str) -> str:
    """Strip trailing -{idx} from a variant ASIN.
    e.g., PRM-3009778053-1 → PRM-3009778053."""
    if not variant_asin:
        return variant_asin
    parts = variant_asin.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0]
    return variant_asin


def find_stock_for_size(size_stock_list: List[Dict], size: str) -> Dict[str, Any]:
    """Find stock info for a specific size."""
    if not size_stock_list or not size:
        return {"size": size, "stock": 0, "stock_status": "out_of_stock", "sku": ""}

    size_norm = size.lower().strip()

    for stock_item in size_stock_list:
        if not isinstance(stock_item, dict):
            continue
        if str(stock_item.get("size", "")).lower().strip() == size_norm:
            return {
                "size": size,
                "stock": stock_item.get("stock", 0),
                "stock_status": stock_item.get("stock_status", "out_of_stock"),
                "sku": stock_item.get("sku", ""),
            }

    return {"size": size, "stock": 0, "stock_status": "out_of_stock", "sku": ""}


def build_price_payload(
    original_price: float,
    discounted_price: float,
    discount_percentage: str,
    category_name: str,
    pricing_config: PricingConfig,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Build price/price_iqd blocks with commission and shipping."""
    base_price = original_price if original_price > 0 else discounted_price
    if base_price <= 0:
        raise ValueError("Invalid price: must be > 0")

    price_usd = round(base_price / usd_to_aed, 2)
    has_discount = discounted_price > 0 and discounted_price < base_price
    discount_usd = round(discounted_price / usd_to_aed, 2) if has_discount else 0

    weight_kg, cbm, tiers = pricing_config.get_weight_cbm_tiers(category_name)
    commission = round(calculate_commission(price_usd, tiers), 2)
    commission_discount = round(calculate_commission(discount_usd, tiers), 2) if has_discount else 0
    shipping_sea = calculate_shipping_sea(cbm, pricing_config.shipping)
    shipping_air = calculate_shipping_air(weight_kg, pricing_config.shipping)

    total_usd = price_usd + commission + shipping_air
    iqd_price = int(math.ceil((total_usd * usd_to_iqd) / 250) * 250)

    # When there's no real discount, force iqd_discount equal to iqd_price and
    # discount_percentage to 0 — otherwise the residual shipping/commission flow
    # makes a no-discount product look ~90% off, which is what surfaced in
    # discount_percentage = 90 with amazon_discount_uae = null.
    if has_discount:
        total_discount_usd = discount_usd + commission_discount + shipping_air
        iqd_discount = int(math.ceil((total_discount_usd * usd_to_iqd) / 250) * 250)
        discount_pct_value = (
            round(((iqd_price - iqd_discount) / iqd_price) * 100, 2)
            if iqd_price and iqd_discount < iqd_price else 0
        )
    else:
        iqd_discount = iqd_price
        discount_pct_value = 0

    # When there's no real discount, mirror the price into the discount slot
    # (instead of leaving 0/empty). API consumers prefer a numeric discount that
    # equals the regular price over a sentinel zero or null.
    block = {
        "amazon_price_uae": base_price,
        "amazon_discount_uae": discounted_price if has_discount else base_price,
        "amazon_price": price_usd,
        "amazon_discount": discount_usd if has_discount else price_usd,
        "hanooot_price": iqd_price,
        "hanooot_discount": iqd_discount,
        "commission": commission,
        "commission_discount": commission_discount,
        "shipping_cost": shipping_sea,
        "shipping_cost_by_air": shipping_air,
        "discount_percentage": round(discount_pct_value),
    }

    # price and price_iqd have identical structure; return same block for both
    return block, block.copy()


_PRICE_FIELDS_TO_COMPARE = (
    "amazon_price_uae",
    "amazon_discount_uae",
    "amazon_price",
    "amazon_discount",
    "hanooot_price",
    "hanooot_discount",
    "commission",
    "commission_discount",
    "shipping_cost",
    "shipping_cost_by_air",
    "discount_percentage",
)


def _to_rounded_float(value: Any) -> float:
    """Coerce stored or computed price values to float rounded to 2 decimals.
    Tolerates legacy empty-string values (commission/shipping pre-dynamic-config)."""
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _price_block_changed(old_price: Dict[str, Any], new_price: Dict[str, Any]) -> bool:
    """Compare every field that build_price_payload produces, rounded to 2 decimals.
    Catches drift from dynamic-config changes (commission tier, shipping rates) even
    when the scraped raw AED price is unchanged."""
    for field in _PRICE_FIELDS_TO_COMPARE:
        if _to_rounded_float(old_price.get(field)) != _to_rounded_float(new_price.get(field)):
            return True
    return False


def has_data_changed(
    current_variant: Dict[str, Any],
    new_price_block: Dict[str, Any],
    new_quantity: int,
    new_availability_status: str,
) -> bool:
    """Return True if PATCH is needed.
    Compares the full price block plus availability — so dynamic-config changes
    (commission tier, shipping constants) propagate to all variants on the next
    cycle even if the scraped raw price is identical."""
    if _price_block_changed(current_variant.get("price") or {}, new_price_block):
        return True

    current_availability = current_variant.get("availability", {})
    if current_availability.get("quantity") != new_quantity:
        return True
    if current_availability.get("status") != new_availability_status:
        return True

    return False


def extract_category_name(variant: Dict[str, Any]) -> str:
    """Extract category name from variant data."""
    cat_data = variant.get("suggested_category")
    if cat_data:
        return str(cat_data)

    cat_obj = variant.get("category")
    if isinstance(cat_obj, list) and cat_obj:
        return ">".join(
            cat.get("name", {}).get("en", "")
            for cat in cat_obj
            if cat.get("name", {}).get("en")
        )
    return ""


def get_variant_color_label(variant: Dict[str, Any]) -> str:
    """Extract display color from source_color field (replaces 'color')."""
    source_color = variant.get("source_color")
    if isinstance(source_color, dict):
        return source_color.get("en") or source_color.get("ar") or "Unknown"
    if isinstance(source_color, str):
        return source_color
    return "Unknown"


async def apply_scraped_to_variants(
    url: str,
    variants: List[Dict[str, Any]],
    scraped_data: Dict[str, Any],
    task_id: str,
    start_index: int,
    pricing_config: PricingConfig,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
    client: Optional[ScraperClient] = None,
) -> tuple[int, int]:
    """
    Apply already-scraped data to variants and send PATCH requests.
    Returns (updated_count, skipped_count).
    """
    if not variants or not scraped_data:
        return 0, 0

    if scraped_data.get("rnet_status_404"):
        deleted_count = 0
        for idx, variant in enumerate(variants, start=start_index):
            asin = variant.get("ASIN")
            if not asin:
                continue

            def _delete_sync():
                return delete_product_from_api(asin)

            try:
                success, error = await asyncio.to_thread(_delete_sync)
            except Exception as e:
                success, error = False, str(e)

            if success:
                deleted_count += 1
                logger.info(f"[{task_id}] DELETED {asin} (404)")
                _queue_search_update(_derive_parent_asin(asin))   # deletion is a search change too: the parent's any_not_deleted / any_in_stock / skus move
                append_log(task_id, f"[{idx}] 🗑️ Deleted {asin} due to 404", "info")
            else:
                logger.warning(f"[{task_id}] DELETE FAILED {asin}: {error}")
                append_log(task_id, f"[{idx}] ❌ DELETE failed for {asin}: {error}", "error")
        logger.info(f"[{task_id}] Batch complete: 0 updated, 0 skipped, {deleted_count} deleted (404)")
        return deleted_count, 0

    # --- Handle multi-color groups: split by color and scrape each color's correct URL ---
    color_links = scraped_data.get("color_links") or []
    if color_links:
        color_url_map = {}
        for cl in color_links:
            cn = (cl.get("color") or "").strip().lower()
            cu = remove_utm_source(cl.get("url") or "")
            if cn and cu:
                color_url_map[cn] = cu

        if color_url_map:
            color_groups = defaultdict(list)
            for v in variants:
                vlabel = get_variant_color_label(v).strip().lower()
                color_groups[vlabel].append(v)

            base_url = remove_utm_source(url)
            has_different_urls = any(
                c not in color_url_map or color_url_map[c] != base_url
                for c in color_groups
            )

            if has_different_urls:
                scraped_cache = {base_url: scraped_data}
                total_updated = 0
                total_skipped = 0
                idx = start_index

                for clabel, cvariants in color_groups.items():
                    if clabel not in color_url_map:
                        total_updated += await mark_missing_variants(
                            cvariants, patch_product_to_api, task_id, _queue_search_update)
                        idx += len(cvariants)
                        continue
                    correct_url = color_url_map[clabel]
                    clean_url = remove_utm_source(correct_url)

                    if clean_url not in scraped_cache:
                        append_log(task_id, f"[{idx}] Scraping color URL: {correct_url}", "debug")
                        cs = await scrape_product_data(correct_url, client=client)
                        scraped_cache[clean_url] = cs

                    cdata = scraped_cache.get(clean_url)
                    if not cdata:
                        logger.warning("[%s] colour fetch failed; keeping previous data: %s", task_id, clean_url)
                        idx += len(cvariants)
                        continue
                    # Strip color_links to prevent re-splitting on recursive call
                    cdata_single = {k: v for k, v in cdata.items() if k != "color_links"}

                    u, s = await apply_scraped_to_variants(
                        correct_url, cvariants, cdata_single,
                        task_id, idx, pricing_config,
                        usd_to_aed=usd_to_aed, usd_to_iqd=usd_to_iqd,
                        client=client,
                    )
                    total_updated += u
                    total_skipped += s
                    idx += len(cvariants)

                logger.info(f"[{task_id}] Multi-color batch: {total_updated} updated, {total_skipped} skipped")
                append_log(task_id, f"[{start_index}] Multi-color batch: {total_updated} updated, {total_skipped} skipped", "info")
                return total_updated, total_skipped

    sample_variant = variants[0]
    variant_color = get_variant_color_label(sample_variant)
    original_price = scraped_data.get("original_price", 0)
    discounted_price = scraped_data.get("discounted_price", 0)
    discount_percentage = scraped_data.get("discount_percentage", 0)
    size_stock_list = scraped_data.get("size_stock") or []

    if original_price <= 0 and discounted_price <= 0:
        append_log(task_id, f"[{start_index}] ❌ Invalid price for {url}", "warning")
        return 0, 0

    category_name = extract_category_name(sample_variant)
    try:
        price_block, price_iqd_block = build_price_payload(
            original_price,
            discounted_price,
            str(discount_percentage) if discount_percentage is not None else "0",
            category_name,
            pricing_config,
            usd_to_aed=usd_to_aed,
            usd_to_iqd=usd_to_iqd,
        )
    except ValueError as e:
        append_log(task_id, f"[{start_index}] ❌ Price calculation failed: {e}", "error")
        return 0, 0

    updated_count = 0
    skipped_count = 0

    for idx, variant in enumerate(variants, start=start_index):
        asin = variant.get("ASIN")
        sku = variant.get("SKU")
        variant_size = variant.get("size", "") or ""
        if not asin:
            continue

        # Determine stock from scraped page
        stock_info = find_stock_for_size(size_stock_list, variant_size)
        stock_quantity = stock_info["stock"]
        stock_status = stock_info["stock_status"]

        if stock_status == "low_stock":
            availability_status = "Low Stock"
        elif stock_status == "out_of_stock":
            availability_status = "Out of Stock"
        else:
            availability_status = "In Stock"

        # Skip if nothing changed (uses availability.quantity now)
        if not has_data_changed(variant, price_block, stock_quantity, availability_status):
            skipped_count += 1
            logger.info(f"[{task_id}] SKIPPED {asin}")
            append_log(task_id, f"[{idx}] ⏭️  {asin} | {variant_color}/{variant_size} | No changes, skipped", "debug")
            continue

        old_hanoot = float(variant.get("price", {}).get("hanooot_price", 0) or 0)
        new_hanoot = float(price_block.get("hanooot_price", 0) or 0)
        if old_hanoot != new_hanoot:
            _PRICE_ALERTS[asin] = {
                "old_price": str(int(old_hanoot)),
                "new_price": str(int(new_hanoot)),
            }

        patch_payload = {
            "data": {
                "price": price_block,
                "price_iqd": price_iqd_block,
                "stock": {
                    "quantity": stock_quantity
                },
                "availability": {
                    "status": availability_status,
                    "quantity": stock_quantity,
                },
                "SKU": sku,
            }
        }

        # with open("patch_payload.json", "a", encoding="utf-8") as f:
        #     f.write(json.dumps(patch_payload, indent=4, ensure_ascii=False))
        #     f.write("\n")

        def _patch_sync():
            # return True, None
            return patch_product_to_api(asin, patch_payload)

        try:
            success, error = await asyncio.to_thread(_patch_sync)
        except Exception as e:
            success, error = False, str(e)

        if success:
            updated_count += 1
            _queue_search_update(_derive_parent_asin(asin))
            logger.info(f"[{task_id}] UPDATED {asin}")
            append_log(task_id, f"[{idx}] ✅ {asin} | {variant_color}/{variant_size} | Stock: {stock_quantity} ({availability_status})", "info")
        else:
            logger.warning(f"[{task_id}] PATCH FAILED {asin}: {error}")
            append_log(task_id, f"[{idx}] ❌ PATCH failed for {asin}: {error}", "error")

    logger.info(f"[{task_id}] Batch complete: {updated_count} updated, {skipped_count} skipped")
    append_log(task_id, f"[{start_index}] Batch complete: {updated_count} updated, {skipped_count} skipped (no changes)", "info")
    return updated_count, skipped_count


# ---------------------------------------------------------------------------
# Legacy helper kept for backward-compat — delegates to apply_scraped_to_variants
# after an inline scrape. Prefer the pipeline for production use.
# ---------------------------------------------------------------------------
async def update_variant_batch(
    url: str,
    variants: List[Dict[str, Any]],
    task_id: str,
    start_index: int,
    pricing_config: PricingConfig,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
) -> tuple[int, int]:
    """
    Update all variants sharing the same URL. Scrapes page once, then patches.
    Returns: (updated_count, skipped_count)
    """
    if not variants:
        return 0, 0

    variant_color = get_variant_color_label(variants[0])
    append_log(task_id, f"[{start_index}] Scraping {url} ({variant_color}) for {len(variants)} variants...", "debug")

    scraped_data = await scrape_product_data(url)
    if not scraped_data:
        append_log(task_id, f"[{start_index}] ❌ Failed to scrape {url}", "error")
        return 0, 0

    if scraped_data.get("rnet_status_404"):
        pass
    elif scraped_data.get("original_price", 0) <= 0 and scraped_data.get("discounted_price", 0) <= 0:
        append_log(task_id, f"[{start_index}] ❌ Invalid price for {url}", "warning")
        return 0, 0

    return await apply_scraped_to_variants(
        url, variants, scraped_data, task_id, start_index,
        pricing_config,
        usd_to_aed=usd_to_aed, usd_to_iqd=usd_to_iqd,
    )


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _build_fetch_payload(filter_criteria: dict, last_seen_asin: Optional[str], batch_size: int) -> dict:
    payload = {
        "paginationMode": "cursor",
        "filters": [filter_criteria],
        "limit": batch_size,
        "projection": FETCH_PROJECTION,
        "returnFilters": False,
        # "includeDeleted": True  
    }
    if last_seen_asin:
        payload["lastSeenASIN"] = last_seen_asin
    return payload


async def fetch_all_variants(
    task_id: str,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    batch_size: int = 100,
) -> List[Dict[str, Any]]:
    """Fetch ALL variants from API in batches."""
    search_url = get_product_searching_url()
    logger.info(f"[{task_id}] Fetching variants from API (configured: {bool(search_url)})")
    if not search_url:
        append_log(task_id, "ERROR: Product search URL not configured", "critical")
        return []

    filter_criteria = _build_filter_criteria(filter_task_id, filter_sku)
    all_variants: List[Dict[str, Any]] = []
    last_seen_asin = None
    append_log(task_id, "Fetching all variants from API...", "info")

    def _fetch_batch_sync(last_seen_asin: Optional[str]):
        payload = _build_fetch_payload(filter_criteria, last_seen_asin, batch_size)
        response = requests.post(search_url, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json().get("data", {})
        return data.get("records", []), data.get("lastSeenASIN"), data.get("hasMore", False)

    while True:
        try:
            variants, retrieved_last_seen_asin, has_more = await asyncio.to_thread(_fetch_batch_sync, last_seen_asin)
        except Exception as e:
            logger.warning(f"[{task_id}] API request failed: {e}")
            append_log(task_id, f"API request failed: {e}", "warning")
            break

        if not variants:
            break

        all_variants.extend(variants)
        batch_asins = ", ".join(v.get("ASIN", "N/A") for v in variants)
        logger.info(f"[{task_id}] Fetched batch: {len(variants)} variants (total: {len(all_variants)}) | ASINs: {batch_asins}")
        append_log(task_id, f"Fetched {len(variants)} variants (total: {len(all_variants)}) | ASINs: {batch_asins}", "debug")

        if not has_more or not retrieved_last_seen_asin:
            break

        last_seen_asin = retrieved_last_seen_asin

    append_log(task_id, f"✅ Fetched {len(all_variants)} total variants", "info")
    return all_variants


def _build_filter_criteria(filter_task_id: Optional[str], filter_sku: Optional[str]) -> dict:
    if filter_sku:
        return {"SKU": filter_sku}
    if filter_task_id:
        return {"task": filter_task_id, "source_type": "PRIMARK"}
    return {"source_type": "PRIMARK"}


# ---------------------------------------------------------------------------
# Pipeline: Fetcher → Scraper → Patcher
# ---------------------------------------------------------------------------

async def _fetcher_into_queue(
    scrape_queue: asyncio.Queue,
    task_id: str,
    filter_task_id: Optional[str],
    filter_sku: Optional[str],
    batch_size: int,
    concurrency: int,
    stop_event: Optional[threading.Event] = None,
    total_records_ref: Optional[List[Optional[int]]] = None,
) -> int:
    """Fetch variants directly from MongoDB in ASIN-ordered batches, group by URL,
    put (url, variants, start_index) into scrape_queue.

    Reads product docs straight from Mongo and explodes their embedded `variants`
    arrays into flat variant records (the shape the pipeline expects), instead of
    the HTTP `/api/v1/products/search` API — that endpoint returns 0 records for
    every query."""
    if total_records_ref is None:
        total_records_ref = [None]

    from app.utils.mongo import get_products_collection_safe

    mongo_filter = _build_filter_criteria(filter_task_id, filter_sku)
    if "source_type" not in mongo_filter:
        mongo_filter["source_type"] = "PRIMARK"

    try:
        col = await get_products_collection_safe()
    except Exception as e:
        logger.error(f"[{task_id}] Fetcher: Mongo connection failed: {e}")
        append_log(task_id, f"Fetcher: Mongo connection failed: {e}", "critical")
        for _ in range(concurrency):
            await scrape_queue.put(None)
        return 0

    if total_records_ref[0] is None:
        try:
            total_records_ref[0] = await col.count_documents(mongo_filter)
        except Exception as e:
            logger.warning(f"[{task_id}] Fetcher: count_documents failed: {e}")

    async def _fetch_batch(last_seen_asin: Optional[str]):
        """Read one ASIN-ordered page of product docs from MongoDB and explode
        their embedded `variants` arrays into flat variant records. Skips ahead
        until at least one variant is found so an all-empty product page can't
        end the run early.
        Returns (variant_records, total_records, lastSeenASIN, hasMore)."""
        records: List[Dict[str, Any]] = []
        cursor_asin = last_seen_asin
        has_more = True
        while not records and has_more:
            batch_filter = dict(mongo_filter)
            if cursor_asin:
                batch_filter["ASIN"] = {"$gt": cursor_asin}
            docs = await col.find(
                batch_filter, {"ASIN": 1, "variants": 1, "_id": 0}
            ).sort("ASIN", 1).limit(batch_size).to_list(length=batch_size)
            if not docs:
                has_more = False
                break
            for doc in docs:
                for v in doc.get("variants") or []:
                    if isinstance(v, dict) and v.get("ASIN"):
                        records.append(v)
            cursor_asin = docs[-1].get("ASIN")
            has_more = len(docs) == batch_size
        return records, total_records_ref[0], cursor_asin, has_more

    MAX_FETCH_RETRIES = 3
    FETCH_RETRY_DELAY = 5

    total_enqueued = 0
    last_seen_asin = None
    next_index = 1
    consecutive_failures = 0
    append_log(task_id, "Fetcher: starting (batches → scrape queue)", "info")

    while True:
        if stop_event and stop_event.is_set():
            break

        variants = None
        total_records = None
        retrieved_last_seen_asin = None
        has_more = False

        for attempt in range(1, MAX_FETCH_RETRIES + 1):
            try:
                variants, total_records, retrieved_last_seen_asin, has_more = await _fetch_batch(last_seen_asin)
                consecutive_failures = 0
                break
            except Exception as e:
                logger.warning(f"[{task_id}] Fetcher: attempt {attempt}/{MAX_FETCH_RETRIES} failed: {e}")
                if attempt < MAX_FETCH_RETRIES:
                    await asyncio.sleep(FETCH_RETRY_DELAY * attempt)

        if variants is None:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                logger.error(f"[{task_id}] Fetcher: 3 consecutive batch failures, stopping")
                append_log(task_id, f"Fetcher: stopping after 3 consecutive batch failures", "error")
                break
            continue

        if not variants:
            break

        if total_records is not None and total_records_ref[0] is None:
            total_records_ref[0] = total_records
            append_log(task_id, f"Fetcher: total_records from API = {total_records}", "info")
            logger.info(f"[{task_id}] Fetcher: total_records from API = {total_records}")

        url_groups = group_variants_by_url(variants)
        for url, group_variants in url_groups.items():
            await scrape_queue.put((url, group_variants, next_index))
            next_index += len(group_variants)
            total_enqueued += len(group_variants)

        total_records_val = total_records_ref[0]
        progress_str = f" ({total_enqueued}/{total_records_val})" if total_records_val is not None else ""
        logger.info(f"[{task_id}] Fetcher: {len(variants)} variants, {len(url_groups)} URL groups (total: {total_enqueued}{progress_str})")
        append_log(task_id, f"Fetcher: enqueued {total_enqueued} variants{progress_str}", "debug")

        if not has_more or not retrieved_last_seen_asin:
            break

        last_seen_asin = retrieved_last_seen_asin

    for _ in range(concurrency):
        await scrape_queue.put(None)

    total_records_val = total_records_ref[0]
    progress_str = f" ({total_enqueued}/{total_records_val})" if total_records_val is not None else ""
    append_log(task_id, f"Fetcher: done. Total variants enqueued: {total_enqueued}{progress_str}", "info")
    logger.info(f"[{task_id}] Fetcher: done. Total variants enqueued: {total_enqueued}{progress_str}")
    return total_enqueued


async def _scraper_worker(
    scrape_queue: asyncio.Queue,
    patch_queue: asyncio.Queue,
    task_id: str,
    concurrency: int,
    scrape_failed_ref: Optional[List[int]] = None,
) -> None:
    """Consume (url, variants, start_index) from scrape_queue; scrape; put result into patch_queue."""
    MAX_SCRAPE_RETRIES = 2
    while True:
        item = await scrape_queue.get()
        if item is None:
            await patch_queue.put(None)
            return
        try:
            url, variants, start_index = item
            scraped_data = None
            for attempt in range(1, MAX_SCRAPE_RETRIES + 1):
                scraped_data = await scrape_product_data(url)
                if scraped_data:
                    break
                if attempt < MAX_SCRAPE_RETRIES:
                    await asyncio.sleep(2 * attempt)
            if scraped_data:
                await patch_queue.put((url, variants, scraped_data, start_index))
            else:
                if scrape_failed_ref is not None:
                    scrape_failed_ref[0] += len(variants)
                logger.warning(f"[{task_id}] Scraper: failed to scrape {url} after {MAX_SCRAPE_RETRIES} attempts, {len(variants)} variants dropped")
        except Exception as e:
            logger.exception(f"[{task_id}] Scraper worker error: {e}")
        finally:
            scrape_queue.task_done()


async def _patcher_worker(
    patch_queue: asyncio.Queue,
    task_id: str,
    counts: List[int],
    usd_to_aed: float,
    usd_to_iqd: float,
    pricing_config: PricingConfig,
    total_records_ref: Optional[List[Optional[int]]] = None,
    variants_completed_ref: Optional[List[int]] = None,
    on_progress_cb: Optional[Any] = None,
) -> None:
    """Consume from patch_queue; call apply_scraped_to_variants; update counts and progress."""
    if variants_completed_ref is None:
        variants_completed_ref = [0]
    while True:
        item = await patch_queue.get()
        if item is None:
            patch_queue.task_done()
            return
        try:
            url, variants, scraped_data, start_index = item
            updated, skipped = await apply_scraped_to_variants(
                url, variants, scraped_data, task_id, start_index,
                pricing_config,
                usd_to_aed=usd_to_aed, usd_to_iqd=usd_to_iqd,
            )
            counts[0] += updated
            counts[1] += skipped
            variants_completed_ref[0] += len(variants)
            completed = variants_completed_ref[0]
            total_records_val = total_records_ref[0] if total_records_ref else None
            if total_records_val is not None:
                progress_str = f" {completed}/{total_records_val} completed"
                logger.info(f"[{task_id}] Patcher:{progress_str} (updated: {counts[0]}, skipped: {counts[1]})")
                append_log(task_id, progress_str.strip(), "debug")
            if on_progress_cb:
                progress = {
                    "total_records": total_records_val,
                    "updated": counts[0],
                    "skipped": counts[1],
                    "variants_completed": completed,
                }
                if asyncio.iscoroutinefunction(on_progress_cb):
                    await on_progress_cb(progress)
                else:
                    on_progress_cb(progress)
        except Exception as e:
            logger.exception(f"[{task_id}] Patcher worker error: {e}")
        finally:
            patch_queue.task_done()


async def update_primark_prices_pipeline(
    task_id: str = "primark-batch-update-pricing",
    batch_size: int = 100,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    concurrency: int = 5,
    stop_event: Optional[threading.Event] = None,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
    queue_max_size: int = 500,
    on_progress: Optional[Any] = None,
    pricing_config: Optional[PricingConfig] = None,
) -> Optional[Dict[str, Any]]:
    """
    Run price update as a pipeline: Fetcher | Scraper | Patcher running in parallel.

    `pricing_config` is fetched once at the start of a run and held constant for
    the whole pipeline. Cron entrypoints fetch it explicitly so all pipeline
    stages see the same snapshot; if not supplied, this function will fetch it
    here (raising on failure — we never silently fall back to stale prices).
    """
    if pricing_config is None:
        pricing_config = await fetch_pricing_config()
        append_log(
            task_id,
            f"Pricing config loaded: {len(pricing_config.categories)} categories, "
            f"sea={pricing_config.shipping.sea_usd_per_cbm}/cbm, "
            f"air={pricing_config.shipping.air_usd_per_kg}/kg",
            "info",
        )

    logger.info(f"[{task_id}] Starting pipeline (Fetcher → Scraper → Patcher). Concurrency: {concurrency}")
    append_log(
        task_id,
        f"Starting Primark price update (PIPELINE). "
        f"Filter task: {filter_task_id}, SKU: {filter_sku}, Concurrency: {concurrency}",
        "info",
    )

    scrape_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_size)
    patch_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_size)
    counts: List[int] = [0, 0]  # [total_updated, total_skipped]
    total_records_ref: List[Optional[int]] = [None]
    variants_completed_ref: List[int] = [0]
    scrape_failed_ref: List[int] = [0]

    # Search-index sync: started just before the workers that feed it; it first
    # replays anything a previous run could not deliver.
    product_sync = BulkProductSync(source="primark", task_id=task_id)
    await product_sync.start()
    sync_token = _PRODUCT_SYNC.set(product_sync)   # inherited by the tasks created below

    fetcher_task = asyncio.create_task(
        _fetcher_into_queue(
            scrape_queue, task_id, filter_task_id, filter_sku, batch_size, concurrency,
            stop_event, total_records_ref=total_records_ref,
        )
    )
    scraper_tasks = [
        asyncio.create_task(_scraper_worker(scrape_queue, patch_queue, task_id, concurrency, scrape_failed_ref))
        for _ in range(concurrency)
    ]
    patcher_tasks = [
        asyncio.create_task(_patcher_worker(
            patch_queue, task_id, counts, usd_to_aed, usd_to_iqd,
            pricing_config,
            total_records_ref=total_records_ref,
            variants_completed_ref=variants_completed_ref,
            on_progress_cb=on_progress,
        ))
        for _ in range(concurrency)
    ]

    try:
        await fetcher_task
        await asyncio.gather(*scraper_tasks)
        for _ in range(concurrency):
            await patch_queue.put(None)
        await asyncio.gather(*patcher_tasks)
    except BaseException:
        # Cancelled or crashed mid-run: stop the workers first (a cancelled run must not
        # keep patching in the background), then save every queued / in-flight search
        # update to Redis so the next run sends it first. Nothing is dropped.
        workers = [fetcher_task, *scraper_tasks, *patcher_tasks]
        for t in workers:
            t.cancel()
        await asyncio.wait(workers, timeout=30)
        await product_sync.abort()
        raise
    finally:
        _PRODUCT_SYNC.reset(sync_token)

    # Final flush + confirmation of this run's search-index updates. Anything that
    # cannot be delivered or confirmed is spooled to Redis and sent first next run.
    sync_stats = await product_sync.close()
    logger.info(f"[{task_id}] Search index sync: {sync_stats}")
    append_log(
        task_id,
        f"Search index sync: {sync_stats['asins_sent']} sent in {sync_stats['batches_sent']} batches, "
        f"{sync_stats['asins_confirmed']} confirmed, {sync_stats['asins_spooled']} spooled for next run"
        + (f", {sync_stats['replayed']} replayed from an earlier run" if sync_stats["replayed"] else ""), "info",
    )

    total_updated, total_skipped = counts[0], counts[1]
    completed = variants_completed_ref[0]
    total_records_val = total_records_ref[0]
    scrape_failures = scrape_failed_ref[0]
    progress_str = f" {completed}/{total_records_val}" if total_records_val is not None else f" {completed} variants"

    append_log(task_id, f"✅ Pipeline complete.{progress_str} Updated: {total_updated}, Skipped: {total_skipped}, Scrape failed: {scrape_failures}", "info")
    logger.info(f"[{task_id}] Pipeline complete.{progress_str} Updated: {total_updated}, Skipped: {total_skipped}, Scrape failed: {scrape_failures}")

    return {
        "total_records": total_records_val,
        "updated": total_updated,
        "skipped": total_skipped,
        "variants_completed": completed,
        "scrape_failed": scrape_failures,
    }


async def update_primark_prices(
    task_id: str = "primark-batch-update-pricing",
    batch_size: int = 100,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    concurrency: int = 5,
    stop_event: Optional[threading.Event] = None,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
    on_progress: Optional[Any] = None,
    pricing_config: Optional[PricingConfig] = None,
) -> Optional[Dict[str, Any]]:
    """Main entry: run Fetcher → Scraper → Patcher pipeline."""
    return await update_primark_prices_pipeline(
        task_id=task_id,
        batch_size=batch_size,
        filter_task_id=filter_task_id,
        filter_sku=filter_sku,
        concurrency=concurrency,
        stop_event=stop_event,
        usd_to_aed=usd_to_aed,
        usd_to_iqd=usd_to_iqd,
        on_progress=on_progress,
        pricing_config=pricing_config,
    )


def run_update_primark_prices(
    task_id: str = "primark-batch-update-pricing",
    batch_size: int = 100,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    concurrency: int = 5,
    stop_event: Optional[threading.Event] = None,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
) -> None:
    """Sync wrapper to run update_primark_prices from a sync context."""
    asyncio.run(update_primark_prices(
        task_id=task_id,
        batch_size=batch_size,
        filter_task_id=filter_task_id,
        filter_sku=filter_sku,
        concurrency=concurrency,
        stop_event=stop_event,
        usd_to_aed=usd_to_aed,
        usd_to_iqd=usd_to_iqd,
    ))


if __name__ == "__main__":
    run_update_primark_prices(
        task_id="primark-batch-update-pricing",
        batch_size=100,
        filter_task_id="c914ba22-c6aa-4f20-af90-6044b7113bee",
        concurrency=8,
    )