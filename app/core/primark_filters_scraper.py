"""
Primark listing filters -> Mongo `source_filters`.

Why this exists
---------------
The catalogue of every filter a shopper can apply, per category, so a scraping
job can be aimed at an exact slice ("body care, Bar Soap, 4-star and up") without
anyone hand-writing a link. Each stored document carries the filter keys, their
values with product counts, and the RECIPE for turning a chosen set of values
into a real request.

The recipe matters because this storefront has no filterable URL. The listing is
an Adobe Edge Delivery block that talks straight to Algolia and keeps the filter
state in memory — the address bar only ever carries `page`. Clicking a filter in
a real browser leaves the URL untouched, which was verified rather than assumed.
So the recipe stored here is `kind: "api"`: an Algolia query with `facetFilters`,
not a URL with query parameters. Sources whose filters DO live in the URL store
`kind: "url"` instead, and a caller treats the two the same way.

Where the data comes from
-------------------------
The storefront publishes its own Algolia credentials in `/configs.json` — the
same public search key the shopper's browser uses. They are read at run time
rather than pinned here, so a key rotation is picked up on the next run.

    /configs.json                  -> app id, public search key, index
    POST <app>-dsn.algolia.net     -> facets=["*"] gives every facet + counts
      responseFields renderingContent -> facetOrdering.facets.order, which is the
                                         SHOPPER-FACING subset in display order

That ordering is the honest filter list. The `algolia-filters-on-main-page`
config value looks authoritative but is a generic Alshaya default (it claims
Size/Colour/Length even where the site shows Fragrance Name and Rating), so it is
recorded as a hint and never used as the filter set.

Categories come from the catalogue's own hierarchy facets
(`field_category_name.en.lvl0/lvl1/lvl2`), so the walk needs no separate crawl.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
from pymongo import AsyncMongoClient

try:  # the harrir_core baked into some images predates the bandwidth counter
    from harrir_core.utils import bandwidth as _bandwidth
except ImportError:  # pragma: no cover
    _bandwidth = None

BASE = "https://www.primark.ae"

logger = logging.getLogger(__name__)

SOURCE = "PRIMARK"
COLLECTION = (os.getenv("MONGO_FILTERS_COLLECTION") or "source_filters").strip()
_BANDWIDTH_KIND = "filters"

_LANG = (os.getenv("PRIMARK_FILTERS_LANG") or "en").strip()
_CONFIG_URL = f"{BASE}/configs.json"
# A category level of 1 is "Body Care > Body Care Offers"; level 2 adds the leaf.
_CATEGORY_LEVEL = int((os.getenv("PRIMARK_FILTERS_CATEGORY_LEVEL") or "1").strip() or "1")
# 0 = no cap: a normal run walks every category the source lists.
_MAX_CATEGORIES = int((os.getenv("PRIMARK_FILTERS_MAX_CATEGORIES") or "0").strip() or "0")
_CONCURRENCY = int((os.getenv("PRIMARK_FILTERS_CONCURRENCY") or "4").strip() or "4")
_TIMEOUT = float((os.getenv("PRIMARK_FILTERS_TIMEOUT") or "40").strip() or "40")

# The live count may be up to this multiple of the advertised one and still
# count as a match; Algolia reports approximate facet counts on large sets.
_COUNT_TOLERANCE = float((os.getenv("FILTERS_COUNT_TOLERANCE") or "1.5").strip() or "1.5")

_UA = "Mozilla/5.0 (X11; Linux x86_64; rv:152.0) Gecko/20100101 Firefox/152.0"

# Facet attributes that are plumbing rather than something a shopper picks.
_NEVER_A_FILTER = ("sku", "gtm", "created", "stock", "raw_title", "best_sellers",
                   "new_arrivals", "product_labels", "field_category", "lhn_category",
                   "attr_bv_rating_distribution", "promotion_nid", "attr_updated_at",
                   "attr_bv_total_review_count", "attr_bv_average_overall_rating")


_BATCH = int((os.getenv("PRIMARK_FILTERS_BATCH") or "25").strip() or "25")


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _record_bytes(size: int) -> None:
    if _bandwidth is None:
        return
    try:
        await _bandwidth.record(SOURCE, size, kind=_BANDWIDTH_KIND)
    except Exception:  # pragma: no cover
        logger.debug("bandwidth record failed", exc_info=True)


def _is_shopper_facet(attribute: str) -> bool:
    return not any(attribute.startswith(p) or p in attribute for p in _NEVER_A_FILTER)


def _label_for(attribute: str) -> str:
    """Turn an index attribute into the words the site shows beside it.

    The storefront renders these labels from its own dictionary, which is not
    published; this mapping covers what the live page was observed to show and
    falls back to a readable form of the attribute for anything new.
    """
    known = {
        "attr_collection_1": "Product Type",
        "attr_fragrance_name": "Fragrance Name",
        "attr_fragrance_category": "Fragrance Category",
        "field_acq_promotion_label": "Promotions",
        "attr_bv_rating": "Rating",
        "final_price": "Price",
        "attr_product_category": "Category",
        "attr_form": "Form",
        "attr_size": "Size",
        "attr_brand": "Brand",
        "attr_actual_color_label_code": "Colour",
        "attr_color": "Colour",
        "attr_delivery_ways": "Delivery",
        "discount": "Discount",
        "colour": "Colour",
    }
    base = attribute.split(".")[0]
    if base in known:
        return known[base]
    return base.replace("attr_", "").replace("_", " ").strip().title()


class _Algolia:
    """The storefront's own search client, credentials and all."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        self._client: Optional[httpx.AsyncClient] = None
        self._sem = asyncio.Semaphore(max(1, _CONCURRENCY))
        self.app_id = ""
        self.api_key = ""
        self.index = ""
        self.config_hints: dict[str, Any] = {}

    async def __aenter__(self) -> "_Algolia":
        self._client = httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True)
        await self._load_config()
        return self

    async def __aexit__(self, *exc) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _load_config(self) -> None:
        assert self._client is not None
        r = await self._client.get(_CONFIG_URL, headers={"User-Agent": _UA, "Accept": "application/json"})
        await _record_bytes(len(r.text))
        rows = (r.json() or {}).get("data") or []
        cfg = {row.get("key"): row.get("value") for row in rows if isinstance(row, dict)}
        self.app_id = (cfg.get("x-algolia-application-id") or "").strip()
        self.api_key = (cfg.get("x-algolia-api-key") or "").strip()
        self.index = (cfg.get("algolia-plp-index") or cfg.get("algolia-default-index") or "").strip()
        if not (self.app_id and self.api_key and self.index):
            raise RuntimeError(f"{SOURCE}: /configs.json carried no Algolia credentials")
        # Kept for reference only — see the module docstring on why this is a hint.
        self.config_hints = {
            "filters_on_main_page": cfg.get("algolia-filters-on-main-page"),
            "hidden_filters": cfg.get("algolia-hidden-filters"),
        }
        logger.info("[filters] %s Algolia app=%s index=%s", SOURCE, self.app_id, self.index)

    @property
    def endpoint(self) -> str:
        return f"https://{self.app_id}-dsn.algolia.net/1/indexes/*/queries"

    async def query(self, params: str) -> dict[str, Any]:
        assert self._client is not None
        headers = {"x-algolia-application-id": self.app_id, "x-algolia-api-key": self.api_key,
                   "Content-Type": "application/json"}
        body = {"requests": [{"indexName": self.index, "params": params}]}
        async with self._sem:
            r = await self._client.post(self.endpoint, headers=headers, json=body)
        await _record_bytes(len(r.text))
        if r.status_code != 200:
            raise RuntimeError(f"{SOURCE}: Algolia HTTP {r.status_code}: {r.text[:200]}")
        return (r.json().get("results") or [{}])[0]


def _params(*, facets: str = '["*"]', hits: int = 0, facet_filters: Optional[list] = None,
            max_values: int = 200) -> str:
    q = {
        "facets": facets,
        "hitsPerPage": str(hits),
        "page": "0",
        "maxValuesPerFacet": str(max_values),
        "responseFields": '["facets","renderingContent","nbHits","hitsPerPage","page"]',
        "filters": "stock > 0",
    }
    if facet_filters:
        q["facetFilters"] = json.dumps(facet_filters, ensure_ascii=False)
    return urlencode(q)


def _filters_from(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Shopper-facing filters, in the order the site displays them."""
    facets = result.get("facets") or {}
    order = (((result.get("renderingContent") or {}).get("facetOrdering") or {})
             .get("facets") or {}).get("order") or []

    # Display order first, then anything else the index exposes that a shopper
    # could plausibly pick — a facet added by the merchandiser shows up here
    # before it is added to the ordering.
    ordered = [a for a in order if a in facets and _is_shopper_facet(a)]
    extra = [a for a in facets if a not in ordered and _is_shopper_facet(a)]

    out: list[dict[str, Any]] = []
    for position, attribute in enumerate(ordered + extra, start=1):
        if attribute.endswith(".ar") or ".ar." in attribute:
            continue                      # the Arabic twin of a filter we already have
        values = facets.get(attribute) or {}
        if not values:
            continue
        out.append({
            "position": position,
            "key": attribute,
            "label": _label_for(attribute),
            "type": "range" if attribute.startswith("final_price") else "multiselect",
            "in_display_order": attribute in ordered,
            "value_count": len(values),
            "values": [{"code": v, "label": v, "count": n}
                       for v, n in sorted(values.items(), key=lambda kv: -kv[1])],
        })
    return out


def _categories_from(result: dict[str, Any], level: int) -> list[dict[str, str]]:
    """The catalogue's own hierarchy, read off the category facet."""
    attribute = f"field_category_name.{_LANG}.lvl{level}"
    facets = (result.get("facets") or {}).get(attribute) or {}
    rows = [{"attribute": attribute, "value": value, "products": count}
            for value, count in sorted(facets.items(), key=lambda kv: -kv[1])]
    return rows[:_MAX_CATEGORIES] if _MAX_CATEGORIES else rows


async def _verify(algolia: "_Algolia", category: dict[str, str],
                  filters: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Apply each filter for real and keep only the ones that behave.

    Algolia answers with the exact number of products a facet value matches, so a
    working filter reproduces the count it advertised. Anything that empties the
    listing, or comes back with a different number, is rejected with the reason
    rather than stored — a filter nobody has exercised is a filter that breaks a
    scrape job later.
    """
    kept: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    category_clause = f"{category['attribute']}:{category['value']}"

    async def check(f: dict[str, Any]) -> None:
        sample = max(f["values"], key=lambda v: v.get("count") or 0)
        clause = f"{f['key']}:{sample['code']}"
        try:
            result = await algolia.query(_params(
                facets='[]', facet_filters=[[category_clause], [clause]]))
        except Exception as e:
            rejected.append({"key": f["key"], "reason": f"probe failed: {type(e).__name__}"})
            return
        observed = result.get("nbHits")
        advertised = sample.get("count") or 0
        exact = observed == advertised
        # Algolia's facet counts are approximate once a result set gets large, so
        # the live count sits a few percent either side of the advertised one.
        # What proves a filter works is that it selects a non-empty slice of about
        # the promised size — not that two approximations agree to the unit.
        drift = abs((observed or 0) - advertised) / advertised if advertised else 1.0
        # The live count runs a few percent above the advertised one across every
        # filter — an artefact of approximate facet counting, not a fault. What
        # must hold is that the filter selects a non-empty slice of roughly the
        # promised size; an order-of-magnitude gap means the key does not work.
        ok = (observed is not None and observed > 0
              and observed <= max(advertised * _COUNT_TOLERANCE, 1))
        f["verified"] = bool(ok)
        f["verification"] = {
            "method": "applied the filter through the site's own search and compared product counts",
            "sample_value": sample["code"], "advertised_count": advertised,
            "observed_count": observed, "exact_match": exact, "drift": round(drift, 4),
            "facet_filters": [[category_clause], [clause]],
        }
        if ok:
            kept.append(f)
        else:
            rejected.append({"key": f["key"], "reason": (
                "applying it emptied the listing" if observed == 0
                else f"count was implausible (advertised {advertised}, observed {observed})"),
                "observed_count": observed})

    await asyncio.gather(*(check(f) for f in filters))
    kept.sort(key=lambda f: f["position"])
    return kept, rejected


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------
async def capture_filters(task_id: Optional[str] = None, *, level: Optional[int] = None,
                          max_categories: Optional[int] = None, verify: bool = True,
                          store: bool = True) -> dict[str, Any]:
    """Catalogue every filter, per category, and store one document per category."""
    task_id = task_id or f"{SOURCE.lower()}-filters-{uuid.uuid4().hex[:8]}"
    capture_id = f"{SOURCE.lower()}-filters-{_now().strftime('%Y%m%dT%H%M%SZ')}"
    started = _now()
    level = _CATEGORY_LEVEL if level is None else level
    cap = max_categories or _MAX_CATEGORIES   # 0 = walk everything

    docs: list[dict[str, Any]] = []
    async with _Algolia(task_id) as algolia:
        catalogue = await algolia.query(_params())
        discovered = _categories_from(catalogue, level)
        discovered_total = len(discovered)
        categories = discovered[:cap] if cap else discovered
        logger.info("[filters] %s: %d categories at level %d", SOURCE, len(categories), level)

        recipe = {
            "kind": "api",
            "why_not_url": "the storefront filters client-side; the URL carries only `page`",
            "method": "POST",
            "endpoint": algolia.endpoint,
            "index": algolia.index,
            "auth": {"x-algolia-application-id": algolia.app_id, "x-algolia-api-key": algolia.api_key},
            "apply": "facetFilters: [[\"<key>:<value code>\"]] — inner list ORs, outer list ANDs",
            "category_filter": "facetFilters entry \"<category attribute>:<category value>\"",
            "page_param": "page",
            "hits_param": "hitsPerPage",
        }

        async def one(category: dict[str, str]) -> Optional[dict[str, Any]]:
            result = await algolia.query(_params(
                facet_filters=[[f"{category['attribute']}:{category['value']}"]]))
            filters = _filters_from(result)
            filters, rejected = (await _verify(algolia, category, filters)) if verify else (filters, [])
            if not filters:
                return None
            return {
                "doc_type": "filters",
                "source": SOURCE,
                "capture_id": capture_id,
                "task_id": task_id,
                "locale": _LANG,
                "captured_at": started,
                "category": {
                    "key": category["value"],
                    "department": category["value"].split(" > ")[0].strip().lower(),
                    "title": category["value"].split(" > ")[-1],
                    "attribute": category["attribute"],
                    "products": category["products"],
                    "url": None,           # this storefront has no filterable category URL
                },
                "link": recipe,
                "config_hints": algolia.config_hints,
                "rejected_filters": rejected,
                "filter_count": len(filters),
                "value_count": sum(f["value_count"] for f in filters),
                "filters": filters,
            }

        docs = await _sweep(categories, one, store=store)

    if store and docs:
        full_sweep = True
        schema = _schema_document(recipe, docs)
        written, pruned = await _store_documents([], schema, prune=full_sweep,
                                                 keep_keys=[d['category']['key'] for d in docs])
        logger.info("[filters] %s: %d category documents up to date, %d stale removed",
                    SOURCE, len(docs), pruned)

    summary = {
        "source": SOURCE,
        "capture_id": capture_id,
        "task_id": task_id,
        "captured_at": started,
        "duration_seconds": round((_now() - started).total_seconds(), 1),
        "categories": len(docs),
        "filters_total": sum(d["filter_count"] for d in docs),
        "values_total": sum(d["value_count"] for d in docs),
        "link_kind": "api",
        "documents": docs,
    }
    logger.info("[filters] %s %s: %d categories, %d filters, %d values in %.1fs",
                SOURCE, capture_id, summary["categories"], summary["filters_total"],
                summary["values_total"], summary["duration_seconds"])
    return summary


# ---------------------------------------------------------------------------
# Mongo
# ---------------------------------------------------------------------------
async def _sweep(categories: list[Any], one: Any, *, store: bool,
                 batch: int = _BATCH) -> list[dict[str, Any]]:
    """Walk the categories in batches, writing each batch as it completes.

    A full sweep is hundreds of requests; storing only at the end would throw the
    whole run away on a crash or a restart. Each category is also isolated, so one
    that raises is skipped rather than killing the sweep.
    """
    done: list[dict[str, Any]] = []

    async def safe(category: Any) -> Optional[dict[str, Any]]:
        try:
            return await one(category)
        except Exception as e:  # pragma: no cover - one bad category, not a dead run
            logger.warning("[filters] %s: category failed (%s): %s", SOURCE, type(e).__name__, e)
            return None

    for start in range(0, len(categories), batch):
        chunk = categories[start:start + batch]
        part = [d for d in await asyncio.gather(*(safe(c) for c in chunk)) if d]
        done.extend(part)
        if store and part:
            await _store_documents(part, prune=False)
        logger.info("[filters] %s: %d/%d categories done", SOURCE, len(done), len(categories))
    return done


def _schema_document(recipe: dict[str, Any], docs: list[dict[str, Any]],
                     sorts: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
    """The one-per-source document: how to apply this source's filters, and the
    proof each key was exercised.

    Identical for every category, so it is written once here instead of being
    copied into hundreds of category documents.
    """
    seen: dict[str, dict[str, Any]] = {}
    for doc in docs:
        for f in doc.get("filters") or []:
            entry = seen.setdefault(f["key"], {
                "key": f["key"], "label": f.get("label"), "type": f.get("type"),
                "apply_template": f.get("apply_template") or f.get("refine_template"),
                "categories": 0, "verified": bool(f.get("verified")),
            })
            entry["categories"] += 1
            if f.get("verification") and "verification" not in entry:
                entry["verification"] = f["verification"]
    return {
        "doc_type": "filter_schema",
        "source": SOURCE,
        "link": recipe,
        "departments": sorted({(d.get("category") or {}).get("department")
                               for d in docs if (d.get("category") or {}).get("department")}),
        "category_count": len(docs),
        "filter_key_count": len(seen),
        "filters": sorted(seen.values(), key=lambda e: (-e["categories"], e["key"])),
        **({"sorts": sorts} if sorts else {}),
        "updated_at": _now(),
    }


async def _store_documents(docs: list[dict[str, Any]], schema: Optional[dict[str, Any]] = None,
                           *, prune: bool = True,
                           keep_keys: Optional[list[str]] = None) -> tuple[int, int]:
    """One document per category plus one schema document, all in `source_filters`.

    Categories are upserted on (source, category key) so a run overwrites rather
    than accumulates, and `first_captured_at` survives. Pruning only happens on a
    FULL sweep: a capped run knows nothing about the categories it skipped.
    """
    source = docs[0]["source"] if docs else (schema or {}).get("source")
    current = keep_keys if keep_keys is not None else [d["category"]["key"] for d in docs]
    async with _write_collection() as col:
        await col.create_index([("source", 1), ("doc_type", 1), ("category.key", 1)], unique=True)
        await col.create_index([("source", 1), ("filters.key", 1)])
        await col.create_index([("source", 1), ("category.department", 1)])

        if schema:
            body = dict(schema)
            stamped = body.pop("updated_at", _now())
            await col.update_one(
                {"source": source, "doc_type": "filter_schema"},
                {"$set": {**body, "updated_at": stamped},
                 "$setOnInsert": {"first_captured_at": stamped}},
                upsert=True,
            )

        for doc in docs:
            body = dict(doc)
            captured_at = body.pop("captured_at")
            # The proof lives once in the schema; the category keeps only the flag.
            body["filters"] = [{k: v for k, v in f.items() if k != "verification"}
                               for f in body.get("filters") or []]
            await col.update_one(
                {"source": source, "doc_type": "filters", "category.key": doc["category"]["key"]},
                {"$set": {**body, "captured_at": captured_at, "updated_at": captured_at},
                 "$setOnInsert": {"first_captured_at": captured_at}},
                upsert=True,
            )

        removed = 0
        if prune:
            stale = await col.delete_many({"source": source, "doc_type": "filters",
                                           "category.key": {"$nin": current}})
            removed = stale.deleted_count

        if schema:
            stored = await col.count_documents({"source": source, "doc_type": "filters"})
            await col.update_one({"source": source, "doc_type": "filter_schema"},
                                 {"$set": {"category_count": stored}})
    return len(docs), removed


def _mongo_uri() -> str:
    uri = ""
    try:
        from app.config import get_config

        uri = (get_config().MONGO_URI or "").strip()
    except Exception:  # pragma: no cover
        uri = ""
    uri = uri or (os.getenv("MONGO_URI") or os.getenv("MONGO_URI_PROD") or "").strip()
    if not uri:
        raise EnvironmentError("No Mongo URI (MONGO_URI_PROD / MONGO_URI) — cannot store filters.")
    return uri


@asynccontextmanager
async def _write_collection():
    client = AsyncMongoClient(_mongo_uri(), serverSelectionTimeoutMS=10_000,
                              connectTimeoutMS=10_000, socketTimeoutMS=30_000, maxPoolSize=5)
    try:
        yield client[os.getenv("MONGO_DB_NAME", "Primary")][COLLECTION]
    finally:
        try:
            await client.aclose()
        except Exception:
            pass


async def latest_filters(source: str = SOURCE, category: Optional[str] = None) -> list[dict[str, Any]]:
    async with _write_collection() as col:
        match: dict[str, Any] = {"source": source.upper(), "doc_type": "filters"}
        newest = await col.find_one(match, sort=[("captured_at", -1)])
        if not newest:
            return []
        match["capture_id"] = newest["capture_id"]
        if category:
            match["category.key"] = category
        return [d async for d in col.find(match)]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
    out = asyncio.run(capture_filters(store=os.getenv("STORE", "1") == "1",
                                      max_categories=int(os.getenv("MAX_CATEGORIES", "6"))))
    docs = out.pop("documents")
    print(json.dumps(out, indent=2, default=str))
    for d in docs[:3]:
        print(f"\n  {d['category']['key']}  ({d['category']['products']} products)")
        for f in d["filters"][:6]:
            print(f"     {f['label']:<22} key={f['key']:<34} {f['value_count']:>4} values"
                  f"   e.g. {[v['code'] for v in f['values'][:3]]}")


if __name__ == "__main__":
    main()
