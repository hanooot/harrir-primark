"""
Primark homepage banner capture -> Mongo `source_home`.

Why this exists
---------------
The eighth source in the cross-store merchandising view: every banner the
Primark homepage renders, flattened into one normalised row with the listing page each
one links to. The row schema matches the Namshi, Splash, BFL and Zara banner
rows field for field, so `source_home` reads the same whichever store you query.

Where the data comes from
-------------------------
Primark runs on Adobe Edge Delivery (the `/scripts/aem.js` storefront), NOT on
the Magento GraphQL surface the product scraper uses. There is no CMS API to query:
the homepage is authored as a document, and its HTML *is* the content.

The page is a list of references, not of creative. Each homepage module is a
`fragment` block naming another authored page:

    /en/                      -> div.fragment.02-hp-hero-banner
                                   -> /en/fragments/banners/offers/02-hp-hero-banner/...
    that fragment             -> four more fragments, one per booked campaign
    those leaf fragments      -> div.promo-banner.image-banner  <- the actual banner

So a capture walks that tree: fetch the homepage, follow every fragment to its
`.plain.html` (the authored body without the page chrome), and recurse until a
block holds creative instead of another reference. Leaves carry three authored
links — desktop image, mobile image, and the page the banner opens:

    <p><a href="https://media.alshaya.com/.../w38-hero-uae-en-d.avif">
    <p><a href="https://media.alshaya.com/.../w38-hero-uae-en-m.avif">
    <p><a href="/en/shop-promotions/big-event">

Market note: the storefront ships ONE global document to every Alshaya market
and hides the rows that do not apply, so a UAE capture legitimately sees rows
authored for KSA/KWT. The slug names the market when it has one, so it is parsed
into `market` rather than dropped — filtering here would silently lose the row
the day a merchandiser renames a slug.

Transport: plain httpx on the host IP, the same as the product scraper. The
homepage answers unchallenged, so no proxy is needed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any, Iterator, Optional

import httpx
from pymongo import AsyncMongoClient

try:  # the harrir_core baked into some images predates the bandwidth counter
    from harrir_core.utils import bandwidth as _bandwidth
except ImportError:  # pragma: no cover - counted bytes are nice to have, not load-bearing
    _bandwidth = None

# Primark's product scraper talks GraphQL and exposes no shared header helper, so
# the storefront header set lives here. The homepage is public HTML behind no
# challenge — the browser shape is politeness, not evasion.
BASE = "https://www.primark.ae"
_UA = "Mozilla/5.0 (X11; Linux x86_64; rv:152.0) Gecko/20100101 Firefox/152.0"


def _headers(lang: str = "en", *, referer: str | None = None) -> dict[str, str]:
    return {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9" if lang == "en" else "ar,en-US;q=0.9,en;q=0.8",
        "Referer": referer or f"{BASE}/{lang}/",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Endpoints / knobs
# ---------------------------------------------------------------------------
SOURCE = "PRIMARK"
COLLECTION = (os.getenv("MONGO_HOME_COLLECTION") or "source_home").strip()

# Retention for `source_home`: captures older than this are expired by Mongo
# itself (see `_ensure_ttl_index`). Raise it if a report ever needs a longer
# look-back than a fortnight.
_HOME_TTL_DAYS = int((os.getenv("HOME_TTL_DAYS") or "14").strip() or "14")
_TTL_INDEX_NAME = "captured_at_ttl"

_BANDWIDTH_KIND = "home"

_LANG = (os.getenv("PRIMARK_HOME_LANG") or "en").strip("/")
_HOME_PATH = (os.getenv("PRIMARK_HOME_PATH") or f"/{_LANG}/").strip()

# The fragment tree is three deep in practice (homepage -> slot -> campaign);
# the cap stops a mis-authored self-reference from walking forever.
_MAX_DEPTH = int((os.getenv("PRIMARK_HOME_MAX_DEPTH") or "4").strip() or "4")
_MAX_FRAGMENTS = int((os.getenv("PRIMARK_HOME_MAX_FRAGMENTS") or "250").strip() or "250")
_CONCURRENCY = int((os.getenv("PRIMARK_HOME_CONCURRENCY") or "8").strip() or "8")
_TIMEOUT = float((os.getenv("PRIMARK_HOME_TIMEOUT") or "30").strip() or "30")
_FETCH_RETRIES = int((os.getenv("PRIMARK_HOME_RETRIES") or "3").strip() or "3")

# Block classes that hold creative rather than another reference.
_BANNER_BLOCKS = ("promo-banner", "image-banner", "video-banner", "hero-banner",
                  "banner", "teaser", "card-banner")
# Blocks that render products, USP strips or personalisation — real page modules,
# but not merchandising creative. Named so the skip is honest in the stored doc.
_NON_BANNER_BLOCKS = ("recommendations", "carousel", "product-list", "columns",
                      "cart", "search", "footer", "header", "section-metadata")

_IMAGE_EXT = (".avif", ".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg")
_VIDEO_EXT = (".mp4", ".webm", ".m3u8")
_MARKETS = ("uae", "ksa", "kwt", "bhr", "qat", "omn", "egy", "jor", "lbn")


async def _record_bytes(size: int) -> None:
    """Count fetched bytes when the image's harrir_core is new enough to have the
    counter; a capture is never worth failing over its own telemetry."""
    if _bandwidth is None:
        return
    try:
        await _bandwidth.record(SOURCE, size, kind=_BANDWIDTH_KIND)
    except Exception:  # pragma: no cover
        logger.debug("bandwidth record failed", exc_info=True)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _abs_url(path: Optional[str]) -> Optional[str]:
    if not path or not isinstance(path, str):
        return None
    p = path.strip()
    if not p:
        return None
    if p.startswith("http://") or p.startswith("https://"):
        return p
    return f"{BASE}/{p.lstrip('/')}"


def _site_path(href: Optional[str]) -> Optional[str]:
    """Authored links come in two shapes — a site path, or the absolute aem.live
    preview URL of the same page. Normalise both to the site path so a capture
    compares cleanly against yesterday's."""
    if not href:
        return None
    h = href.strip()
    if not h:
        return None
    if h.startswith("http"):
        m = re.match(r"https?://[^/]+(/.*)$", h)
        if not m:
            return None
        h = m.group(1)
    return h.split("#")[0] or None


def _is_asset(href: str) -> bool:
    low = href.lower().split("?")[0]
    return low.endswith(_IMAGE_EXT + _VIDEO_EXT) or "media.alshaya.com" in low


def _market_of(slug: str) -> Optional[str]:
    """Slugs name the market they were booked for ("...-bodycare-ksa"); the rest
    are global."""
    parts = re.split(r"[-_/]", slug.lower())
    for m in _MARKETS:
        if m in parts:
            return m.upper()
    return None


# ---------------------------------------------------------------------------
# Minimal DOM — html.parser only
# ---------------------------------------------------------------------------
# The image carries no bs4/lxml and this runs three times a day, so a ~40-line
# tree beats adding a dependency to the fleet.
class _Node:
    __slots__ = ("tag", "attrs", "children", "parent", "text")

    def __init__(self, tag: str, attrs: Optional[dict[str, str]] = None,
                 parent: Optional["_Node"] = None):
        self.tag = tag
        self.attrs = attrs or {}
        self.children: list["_Node"] = []
        self.parent = parent
        self.text = ""

    @property
    def classes(self) -> list[str]:
        return (self.attrs.get("class") or "").split()

    def descendants(self) -> Iterator["_Node"]:
        for child in self.children:
            yield child
            yield from child.descendants()

    def find_all(self, tag: str) -> list["_Node"]:
        return [n for n in self.descendants() if n.tag == tag]

    def first(self, tag: str) -> Optional["_Node"]:
        return next((n for n in self.descendants() if n.tag == tag), None)

    def all_text(self) -> str:
        parts = [self.text] + [n.text for n in self.descendants()]
        return re.sub(r"\s+", " ", " ".join(parts)).strip()


class _DOM(HTMLParser):
    _VOID = {"img", "br", "hr", "meta", "link", "input", "source", "area", "base", "col"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _Node("#root")
        self._stack: list[_Node] = [self.root]

    def handle_starttag(self, tag: str, attrs) -> None:
        node = _Node(tag, {k: (v or "") for k, v in attrs}, self._stack[-1])
        self._stack[-1].children.append(node)
        if tag not in self._VOID:
            self._stack.append(node)

    def handle_startendtag(self, tag: str, attrs) -> None:
        self._stack[-1].children.append(_Node(tag, {k: (v or "") for k, v in attrs}, self._stack[-1]))

    def handle_endtag(self, tag: str) -> None:
        if tag in self._VOID:
            return
        for i in range(len(self._stack) - 1, 0, -1):
            if self._stack[i].tag == tag:
                del self._stack[i:]
                return

    def handle_data(self, data: str) -> None:
        self._stack[-1].text += data


def _parse(html: str) -> _Node:
    dom = _DOM()
    dom.feed(html)
    return dom.root


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------
class _Fetcher:
    """One pooled session for the whole walk, with the product scraper's headers."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        self._sem = asyncio.Semaphore(max(1, _CONCURRENCY))
        self._client: Optional[httpx.AsyncClient] = None
        self.fetched = 0

    async def __aenter__(self) -> "_Fetcher":
        self._client = httpx.AsyncClient(
            timeout=_TIMEOUT, follow_redirects=True,
            limits=httpx.Limits(max_connections=max(4, _CONCURRENCY * 2)),
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def html(self, url: str) -> Optional[str]:
        assert self._client is not None, "_Fetcher must be used as an async context manager"
        headers = dict(_headers(_LANG, referer=f"{BASE}/{_LANG}/"))
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        headers.pop("Content-Type", None)
        last = ""
        for attempt in range(1, _FETCH_RETRIES + 1):
            try:
                async with self._sem:
                    r = await self._client.get(url, headers=headers)
                body = r.text
                await _record_bytes(len(body))
                if r.status_code == 404:
                    return None          # an unpublished fragment, not a failure
                if r.status_code != 200:
                    last = f"HTTP {r.status_code}"
                else:
                    self.fetched += 1
                    return body
            except Exception as e:
                last = f"{type(e).__name__}: {e}"
            if attempt < _FETCH_RETRIES:
                await asyncio.sleep(attempt * 1.0)
        logger.warning("[%s] Primark home: %s failed after %d attempts: %s",
                       self.task_id, url, _FETCH_RETRIES, last)
        return None


# ---------------------------------------------------------------------------
# Blocks -> banner rows
# ---------------------------------------------------------------------------
def _blocks_of(root: _Node) -> list[_Node]:
    """Every authored block on a page, in scroll order.

    Edge Delivery wraps each block in an anonymous section div, so blocks are the
    grandchildren of <main> on a rendered page and of the body on a .plain.html
    fragment. Taking any div that carries a class covers both without special-casing.
    """
    main = next((n for n in root.descendants() if n.tag == "main"), root)
    return [n for n in main.descendants() if n.tag == "div" and n.attrs.get("class")]


def _block_kind(block: _Node) -> str:
    return (block.classes[0] if block.classes else "").lower()


def _block_name(block: _Node) -> str:
    """The authored name of a block: `<div class="fragment 02-hp-hero-banner">`."""
    return " ".join(block.classes[1:]).strip() or _block_kind(block)


def _fragment_target(block: _Node) -> Optional[str]:
    """A fragment block holds exactly one link: the page it pulls in."""
    a = block.first("a")
    return _site_path(a.attrs.get("href")) if a else None


def _banner_from(block: _Node, *, section: str, name: str,
                 position: int, block_index: int) -> Optional[dict[str, Any]]:
    """Turn one leaf creative block into a banner row.

    Authored as a list of links: the assets first (desktop, then mobile), then the
    page the banner opens. Text in the block is the headline/CTA a shopper reads.
    """
    assets: list[str] = []
    target: Optional[str] = None
    for a in block.find_all("a"):
        href = (a.attrs.get("href") or "").strip()
        if not href:
            continue
        if _is_asset(href):
            assets.append(href)
        elif target is None:
            target = _site_path(href)

    images = [a for a in assets if not a.lower().split("?")[0].endswith(_VIDEO_EXT)]
    videos = [a for a in assets if a.lower().split("?")[0].endswith(_VIDEO_EXT)]
    if not images and not videos and not target:
        return None

    # The headline a shopper reads is an <h*>; the <p>s are a mix of the asset
    # links themselves and the authored style config ("color-bg-cta: #000"), so
    # both of those shapes drop out and whatever is left is real copy.
    headings = [n.all_text() for n in block.descendants() if n.tag in ("h1", "h2", "h3", "h4", "h5", "h6")]
    copy = [p.all_text() for p in block.find_all("p")]
    copy = [c for c in copy if c and not c.startswith("http") and not c.startswith("/")
            and ":" not in c.split(" ")[0]]

    return {
        "position": position,
        "block": block_index,
        "section": section,
        "section_type": _block_kind(block),
        "name": name,
        "plp_url": _abs_url(target),
        "plp_path": target,
        "image_url": images[0] if images else None,
        "mobile_image_url": images[1] if len(images) > 1 else None,
        "video_url": videos[0] if videos else None,
        "campaign": None,
        "asset_code": name,
        "countdown_ends_at": None,
        "scheduled_from": None,
        "scheduled_until": None,
        "media_type": "video" if videos and not images else "image",
        "title": next((t for t in headings + copy if t), None),
        "market": _market_of(name),
    }


async def _walk(fetcher: _Fetcher, path: str, *, section: str, depth: int,
                block_index: int, rows: list[dict[str, Any]], skipped: list[str],
                seen: set[str]) -> None:
    """Read one fragment page and either collect its creative or follow it deeper."""
    if depth > _MAX_DEPTH or len(seen) >= _MAX_FRAGMENTS or path in seen:
        if path in seen:
            skipped.append(f"{section} ({path}: already visited)")
        return
    seen.add(path)

    html = await fetcher.html(_abs_url(f"{path}.plain.html") or "")
    if not html:
        skipped.append(f"{section} ({path}: not published)")
        return

    for block in _blocks_of(_parse(html)):
        kind = _block_kind(block)
        if kind == "fragment":
            target = _fragment_target(block)
            if target:
                await _walk(fetcher, target, section=section, depth=depth + 1,
                            block_index=block_index, rows=rows, skipped=skipped, seen=seen)
            continue
        if kind in _NON_BANNER_BLOCKS:
            continue
        if kind not in _BANNER_BLOCKS and not any(b in " ".join(block.classes) for b in _BANNER_BLOCKS):
            continue
        # Name the row after the fragment it was authored in ("w38-160926-hero-
        # the-big-event"), not after its block class ("image-banner"): the slug is
        # the campaign identity the weekly diff follows week to week.
        row = _banner_from(block, section=section, name=path.rsplit("/", 1)[-1],
                           position=len(rows) + 1, block_index=block_index)
        if row:
            rows.append(row)


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------
async def capture_home_banners(task_id: Optional[str] = None, *,
                               page_path: Optional[str] = None,
                               store: bool = True) -> dict[str, Any]:
    """Capture the Primark homepage's banners as a single normalised row.

    Walks the homepage's fragment tree and writes one `doc_type="banners"`
    document to `source_home`. Pass `page_path` to capture a different authored
    page (a campaign landing, say) instead of the homepage.
    """
    task_id = task_id or f"primark-banners-{uuid.uuid4().hex[:8]}"
    capture_id = f"{SOURCE.lower()}-banners-{_now().strftime('%Y%m%dT%H%M%SZ')}"
    started = _now()
    entry = (page_path or _HOME_PATH).strip()

    rows: list[dict[str, Any]] = []
    skipped: list[str] = []
    seen: set[str] = set()

    async with _Fetcher(task_id) as fetcher:
        html = await fetcher.html(_abs_url(entry) or "")
        if not html:
            raise RuntimeError(f"Could not fetch {_abs_url(entry)} (Fastly challenge?)")

        root = _parse(html)
        title = (root.first("title").all_text() if root.first("title") else None) or "Homepage"

        for index, block in enumerate(_blocks_of(root), start=1):
            kind, name = _block_kind(block), _block_name(block)
            if kind == "fragment":
                target = _fragment_target(block)
                if not target:
                    skipped.append(f"{name} (fragment with no target)")
                    continue
                await _walk(fetcher, target, section=name, depth=1, block_index=index,
                            rows=rows, skipped=skipped, seen=seen)
            elif kind in _NON_BANNER_BLOCKS:
                skipped.append(f"{name or kind} ({kind}, no creative)")
            else:
                row = _banner_from(block, section=name, name=name,
                                   position=len(rows) + 1, block_index=index)
                if row:
                    rows.append(row)

        fetches = fetcher.fetched

    linked = [r for r in rows if r["plp_url"]]
    doc: dict[str, Any] = {
        "doc_type": "banners",
        "source": SOURCE,
        "capture_id": capture_id,
        "task_id": task_id,
        "locale": _LANG,
        "captured_at": started,
        "duration_seconds": round((_now() - started).total_seconds(), 1),
        "home_url": _abs_url(_HOME_PATH),
        "page": entry,
        "page_url": _abs_url(entry),
        "page_title": title,
        "banner_count": len(rows),
        "linked_count": len(linked),
        "fragments_read": len(seen),
        "pages_fetched": fetches,
        "campaigns": sorted({r["section"] for r in rows if r["section"]}),
        "skipped_blocks": skipped,
        "banners": rows,
    }

    if store:
        async with _write_collection() as col:
            await col.create_index([("source", 1), ("doc_type", 1), ("captured_at", -1)])
            await _ensure_ttl_index(col)
            await col.insert_one(dict(doc))
        logger.info("[home] Stored 1 banners row in '%s'", COLLECTION)

    logger.info("[home] Banner capture %s: %d banners (%d linked) from %d fragments in %.1fs",
                capture_id, len(rows), len(linked), len(seen), doc["duration_seconds"])
    return doc


# ---------------------------------------------------------------------------
# Mongo
# ---------------------------------------------------------------------------
def _mongo_uri() -> str:
    """The cluster this service already writes products to.

    These services pick prod/stage through `app.config` (MONGO_URI_PROD /
    MONGO_URI_STAGE), so the config wins and the bare env var is only a fallback
    for a standalone run outside the app.
    """
    uri = ""
    try:
        from app.config import get_config

        uri = (get_config().MONGO_URI or "").strip()
    except Exception:  # pragma: no cover - standalone use without the app config
        uri = ""
    uri = uri or (os.getenv("MONGO_URI") or os.getenv("MONGO_URI_PROD") or "").strip()
    if not uri:
        raise EnvironmentError("No Mongo URI (MONGO_URI_PROD / MONGO_URI) — cannot store the banner row.")
    return uri


async def _ensure_ttl_index(col) -> None:
    """Keep only the last `HOME_TTL_DAYS` of captures; Mongo deletes the rest.

    Each capture appends a document and nothing ever removed them, so this
    collection only grew — the banner arrays make the average document ~100 KB,
    so it adds up far faster than the document count suggests. A TTL index on
    `captured_at` (a real BSON Date on every row) lets Mongo expire old captures
    on its own, with no cron and nothing to remember to run.

    Created here rather than in a migration so it is self-healing: if the index
    is ever dropped, the next capture puts it back.

    Failures are logged and swallowed on purpose. If someone widens the window
    later with `collMod`, this call would otherwise raise IndexOptionsConflict
    and take the whole nightly capture down with it — retention is not worth
    losing the capture over.
    """
    try:
        await col.create_index([("captured_at", 1)],
                               name=_TTL_INDEX_NAME,
                               expireAfterSeconds=_HOME_TTL_DAYS * 86400)
    except Exception as e:  # pragma: no cover - never fail a capture over retention
        logger.warning("[home] TTL index not applied to '%s' (%s: %s); "
                       "captures are unaffected", COLLECTION, type(e).__name__, e)


@asynccontextmanager
async def _write_collection():
    """A short-lived client bound to the loop the capture runs on — pymongo's async
    client refuses to be used from a loop other than the one that created it."""
    client = AsyncMongoClient(
        _mongo_uri(),
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=30_000,
        maxPoolSize=5,
    )
    try:
        yield client[os.getenv("MONGO_DB_NAME", "Primary")][COLLECTION]
    finally:
        try:
            await client.aclose()
        except Exception:
            pass


async def latest_banners(source: str = SOURCE) -> Optional[dict[str, Any]]:
    async with _write_collection() as col:
        return await col.find_one({"source": source, "doc_type": "banners"},
                                  sort=[("captured_at", -1)])


# ---------------------------------------------------------------------------
# Standalone: docker exec primark-web python -m app.core.primark_home_scraper
# ---------------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
    doc = asyncio.run(capture_home_banners(store=os.getenv("STORE", "1") == "1"))
    print(json.dumps({k: v for k, v in doc.items() if k != "banners"}, indent=2, default=str))
    for b in doc["banners"][:8]:
        print(f"  {b['position']:>2}. [{b['section']}] {b['name']} -> {b['plp_path']}")


if __name__ == "__main__":
    main()
