"""Turn a job's chosen filters into something this source's listing API accepts.

Why this exists
---------------
Four sources (Namshi, noon, Splash, BFL) keep their filter state in the address
bar, so a job that wants "dresses, size S, under 200 AED" just carries a longer
link and the scraper needs to know nothing. The other five — BBW, Victoria's
Secret, Primark, Sephora and Zara — have no filterable URL at all: the listing
is a client-side widget that talks to a search API and leaves the address bar
alone. Clicking a filter on those sites genuinely does not change the URL, which
was verified in a browser rather than assumed.

So a job for one of those five carries its selection in ONE extra query
parameter on `scraping_url`:

    https://www.bathandbodyworks.ae/en/shop-body-care?hx_filters=<selection>

`hx_filters` is ours, not the retailer's. This module strips it back off (the
rest of the scraper must never see it — several of these scrapers parse the URL
as a raw string and would swallow it into a slug) and hands back a selection the
call site can translate into `facetFilters`, `refine`, or a product-id
intersection, whichever that source speaks.

The wire format
---------------
CANONICAL — a percent-encoded JSON object mapping a filter key to its chosen
values. The keys and value codes are exactly those stored in `source_filters`;
nothing here invents or maps names.

    {"attr_collection_1.en": ["Body Cream", "Body Lotion"],
     "final_price.AED.default": {"min": 10, "max": 50}}

Values within one key are OR-ed, separate keys are AND-ed — the same logic the
sites' own filter panels apply. A bare scalar is accepted as a one-value list.
A `{"min":…, "max":…}` object (either bound optional) is a numeric range.

ALSO ACCEPTED, because a URL survives a lot of hands between Spark and here:
  • base64url of that same JSON, for callers that would rather not nest
    percent-encoding inside a query string;
  • a compact `key:v1,v2;key2:10..50` form for simple selections typed by hand.
    Values containing a comma, semicolon or colon MUST use the JSON form — the
    compact parser cannot see the difference and will split them.

An unparseable value is not a silent no-op: it raises, so a malformed job fails
loudly at the start instead of quietly scraping the whole unfiltered category
and looking like a success.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

logger = logging.getLogger(__name__)

#: The query parameter a job uses to carry its filter selection.
HX_PARAM = "hx_filters"


def _num(value: Optional[float]) -> str:
    """Render a bound the way the site would: 10, not 10.0, and "" for unbounded."""
    if value is None:
        return ""
    return str(int(value)) if float(value).is_integer() else str(value)


class FilterSelectionError(ValueError):
    """The job carried an `hx_filters` value that could not be understood."""


class FilterSelection:
    """A chosen set of filters, in whatever shape the caller's API wants.

    `terms` holds the multi-select choices ({key: [codes]}); `ranges` holds the
    numeric ones ({key: {"min": x, "max": y}}). Both keep the key exactly as the
    source published it, so the translation methods below never have to guess.
    """

    __slots__ = ("terms", "ranges")

    def __init__(self, terms: Optional[dict[str, list[str]]] = None,
                 ranges: Optional[dict[str, dict[str, float]]] = None) -> None:
        self.terms: dict[str, list[str]] = terms or {}
        self.ranges: dict[str, dict[str, float]] = ranges or {}

    def __bool__(self) -> bool:
        return bool(self.terms or self.ranges)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"FilterSelection(terms={self.terms!r}, ranges={self.ranges!r})"

    def describe(self) -> str:
        """One human line for the job log, so an operator can see what was applied."""
        bits = [f"{k}={'|'.join(v)}" for k, v in self.terms.items()]
        for k, r in self.ranges.items():
            bits.append(f"{k}={_num(r.get('min'))}..{_num(r.get('max'))}")
        return ", ".join(bits) or "none"

    # -- per-source translations -------------------------------------------
    def algolia_facet_filters(self) -> list[list[str]]:
        """Algolia's `facetFilters`: inner list ORs, outer list ANDs.

        Matches the `apply` recipe stored for BBW, Victoria and Primark.
        """
        return [[f"{key}:{value}" for value in values]
                for key, values in self.terms.items() if values]

    def algolia_numeric_filters(self) -> list[str]:
        """Algolia's `numericFilters` — ranges cannot ride in `facetFilters`."""
        out: list[str] = []
        for key, r in self.ranges.items():
            if r.get("min") is not None:
                out.append(f"{key}>={_num(r['min'])}")
            if r.get("max") is not None:
                out.append(f"{key}<={_num(r['max'])}")
        return out

    def sephora_refine(self) -> list[str]:
        """Sephora's `refine`: one `key=value` string per chosen value.

        Sephora models price as an ordinary value whose code is already a range
        expression (`(0.01..8943)`), so a range here is emitted in that shape
        rather than as a separate parameter.
        """
        out = [f"{key}={value}" for key, values in self.terms.items() for value in values]
        for key, r in self.ranges.items():
            out.append(f"{key}=({_num(r.get('min'))}..{_num(r.get('max'))})")
        return out


def _coerce_values(key: str, raw: Any) -> tuple[list[str], Optional[dict[str, float]]]:
    """One key's value, as either a list of codes or a numeric range."""
    if isinstance(raw, dict):
        if not ({"min", "max"} & set(raw)):
            raise FilterSelectionError(
                f"{HX_PARAM}: {key!r} is an object but carries neither 'min' nor 'max'")
        out: dict[str, float] = {}
        for bound in ("min", "max"):
            if raw.get(bound) is None:
                continue
            try:
                out[bound] = float(raw[bound])
            except (TypeError, ValueError):
                raise FilterSelectionError(
                    f"{HX_PARAM}: {key!r} {bound} is not a number: {raw[bound]!r}") from None
        return [], out
    if isinstance(raw, (list, tuple, set)):
        values = [str(v).strip() for v in raw if str(v).strip() != ""]
    elif raw is None:
        values = []
    else:
        values = [str(raw).strip()]
    if not values:
        raise FilterSelectionError(f"{HX_PARAM}: {key!r} was given no values")
    return values, None


def _parse_compact(blob: str) -> dict[str, Any]:
    """`key:v1,v2;key2:10..50` -> the same mapping the JSON form produces."""
    mapping: dict[str, Any] = {}
    for clause in blob.split(";"):
        clause = clause.strip()
        if not clause:
            continue
        key, sep, rest = clause.partition(":")
        if not sep:
            raise FilterSelectionError(
                f"{HX_PARAM}: {clause!r} is not 'key:value' — use the JSON form for "
                f"values containing ':' or ','")
        key = key.strip()
        rest = rest.strip()
        if ".." in rest:
            lo, _, hi = rest.partition("..")
            mapping[key] = {"min": lo.strip() or None, "max": hi.strip() or None}
        else:
            mapping[key] = [v.strip() for v in rest.split(",") if v.strip()]
    return mapping


def parse_selection(blob: str) -> FilterSelection:
    """Read one `hx_filters` value in any accepted encoding."""
    blob = (blob or "").strip()
    if not blob:
        return FilterSelection()

    mapping: Optional[dict[str, Any]] = None
    if blob[0] in "{[":
        try:
            mapping = json.loads(blob)
        except ValueError as e:
            raise FilterSelectionError(f"{HX_PARAM}: not valid JSON ({e})") from None
    else:
        # base64url before compact: a base64 blob can contain ':' and would
        # otherwise be mangled into a nonsense key.
        padded = blob + "=" * (-len(blob) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded.encode()).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            decoded = ""
        if decoded.strip().startswith("{"):
            try:
                mapping = json.loads(decoded)
            except ValueError as e:
                raise FilterSelectionError(f"{HX_PARAM}: base64 payload is not valid JSON ({e})") from None
        else:
            mapping = _parse_compact(blob)

    if not isinstance(mapping, dict):
        raise FilterSelectionError(f"{HX_PARAM}: expected an object of key -> values")

    terms: dict[str, list[str]] = {}
    ranges: dict[str, dict[str, float]] = {}
    for key, raw in mapping.items():
        key = str(key).strip()
        if not key:
            continue
        values, rng = _coerce_values(key, raw)
        if rng is not None:
            if rng:
                ranges[key] = rng
        else:
            terms[key] = values
    return FilterSelection(terms, ranges)


def split_filters(url: str) -> tuple[str, FilterSelection]:
    """Strip `hx_filters` off a job URL and return (clean url, selection).

    The clean URL is what the rest of the scraper sees — it is byte-identical to
    the link a shopper would use, so URL parsing downstream (slugs, category
    ids, PDP detection) behaves exactly as it did before filters existed.
    """
    raw = (url or "").strip()
    if not raw or HX_PARAM not in raw:
        return raw, FilterSelection()

    parts = urlparse(raw)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    kept = [(k, v) for k, v in pairs if k != HX_PARAM]
    chosen = [v for k, v in pairs if k == HX_PARAM]

    clean = urlunparse(parts._replace(query=urlencode(kept, doseq=True)))
    if not kept and raw.endswith("?" + parts.query):
        clean = clean.rstrip("?")

    merged: dict[str, Any] = {}
    for blob in chosen:
        sel = parse_selection(blob)
        for k, v in sel.terms.items():
            merged.setdefault(k, []).extend(v)
        for k, r in sel.ranges.items():
            merged[k] = r
    selection = parse_selection(json.dumps(merged, ensure_ascii=False)) if merged else FilterSelection()
    return clean, selection
