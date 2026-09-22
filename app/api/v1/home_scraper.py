"""
Homepage-merchandising API for Primark: capture the homepage's banners and read
the row back.

Mirrors the Namshi, Splash, BFL and Zara /home-* endpoints so one dashboard can
hit the same paths on any service and get the same row shape.
"""
import logging
import uuid
from datetime import datetime
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.core import primark_home_scraper as home

logger = logging.getLogger(__name__)

router = APIRouter()


class HomeBannersRequest(BaseModel):
    task_id: Optional[str] = Field(default=None, description="Caller-supplied id; generated when omitted")
    page_path: Optional[str] = Field(
        default=None,
        description="Capture a specific authored page instead of the homepage.",
    )
    # The capture is synchronous — accepted and ignored so the cross-source hub
    # and the nightly job can send one body shape to every service.
    wait: bool = Field(default=True, description="Accepted for symmetry; the capture always blocks")
    environment: str = Field(default="prod", description="'stage' or 'prod'")


def _serialise(doc: Any) -> Any:
    """Mongo docs carry ObjectId/datetime — make them JSON-safe."""
    if isinstance(doc, dict):
        return {k: _serialise(v) for k, v in doc.items() if k != "_id"}
    if isinstance(doc, list):
        return [_serialise(v) for v in doc]
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, datetime):
        return doc.isoformat()
    return doc


@router.post("/home-banners")
async def scrape_home_banners(request: HomeBannersRequest):
    """Capture the homepage's banners as one normalised row."""
    task_id = request.task_id or f"primark-banners-{uuid.uuid4().hex[:8]}"
    try:
        doc = await home.capture_home_banners(task_id=task_id, page_path=request.page_path)
    except Exception as e:
        logger.exception("[%s] Primark banner capture failed", task_id)
        raise HTTPException(status_code=502, detail=str(e))
    return _serialise(doc)


@router.get("/home-banners")
async def get_home_banners(
    source: str = Query("PRIMARK", description="Source whose banner row to return"),
):
    """The latest banner row."""
    doc = await home.latest_banners(source.upper())
    if not doc:
        raise HTTPException(
            status_code=404,
            detail=f"No banner row stored for '{source.upper()}'. Run POST /api/v1/home-banners first.",
        )
    return _serialise(doc)


# ---------------------------------------------------------------------------
# Uniform surface — every source answers these same four paths
# ---------------------------------------------------------------------------
# `/home-scrape` is the path the cross-source hub, the nightly capture job and any
# dashboard call on EVERY service; here it is the same capture as POST
# /home-banners under the name the multi-page stores (Namshi, Sephora) use. The
# two read endpoints delegate to harrir_core so the report/history logic exists
# once, not once per source.
try:
    from harrir_core.utils import home_hub as _hub
except ImportError:  # pragma: no cover - older harrir_core baked into the image
    _hub = None
    logger.warning("harrir_core.utils.home_hub missing — /home-report and /home-captures not mounted")


@router.post("/home-scrape")
async def scrape_home(request: HomeBannersRequest):
    """Capture the homepage's banners now — the same run as POST /home-banners,
    under the path every source shares."""
    return await scrape_home_banners(request)


if _hub is not None:

    @router.get("/home-report")
    async def home_report(
        source: str = Query("PRIMARK", description="Source to report on"),
        capture_id: Optional[str] = Query(None, description="Specific capture; defaults to the newest"),
    ):
        """This capture's pages, banners and the PLPs they point at."""
        data = await _hub.source_report(source, capture_id)
        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No capture stored for '{source.upper()}'. Run POST /api/v1/home-scrape first.",
            )
        return _serialise(data)

    @router.get("/home-captures")
    async def list_home_captures(
        source: str = Query("PRIMARK"),
        limit: int = Query(20, ge=1, le=200),
    ):
        """Capture history — one row per capture, newest first."""
        return _serialise({"source": source.upper(), "captures": await _hub.captures_for(source, limit)})
