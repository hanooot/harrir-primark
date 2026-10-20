"""
Filter-catalogue API for Primark.

One route, mirroring the homepage capture the daily scheduler already fires:

    POST /api/v1/filters-scrape

It walks the source's categories, proves every filter against the live site and
writes the result into the shared `source_filters` collection, overwriting each
category in place so the collection stays one document per category.
The response carries the counts the scheduler prints in its completion line.

This storefront has no filterable URL — its listing talks straight to a
search index — so each stored filter carries an `api` recipe (`facetFilters`)
rather than a URL template. The scraper applies that recipe itself when a job
arrives with an `hx_filters` selection.

There is deliberately NO read route. The consumer of this data (the sourcing
scheduler) has direct Mongo access and reads `source_filters` itself, so an HTTP
reader would be a second way to say the same thing — and a second thing to keep
in step.
"""
import logging
from datetime import datetime
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core import primark_filters_scraper as filters

logger = logging.getLogger(__name__)

router = APIRouter()


class FiltersScrapeRequest(BaseModel):
    task_id: Optional[str] = Field(default=None, description="Caller-supplied id; generated when omitted")
    max_categories: Optional[int] = Field(
        default=None,
        description="Cap the walk. A capped run updates what it sees and never retires "
                    "the categories it skipped; only a full sweep prunes.",
    )
    verify: bool = Field(default=True, description="Prove every filter against the live site before storing it")
    store: bool = Field(default=True, description="Write to the shared filter catalogue")


def _serialise(doc: Any) -> Any:
    if isinstance(doc, dict):
        return {k: _serialise(v) for k, v in doc.items() if k != "_id"}
    if isinstance(doc, list):
        return [_serialise(v) for v in doc]
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, datetime):
        return doc.isoformat()
    return doc


@router.post("/filters-scrape")
async def scrape_filters(request: FiltersScrapeRequest):
    """Capture every filter, per category, verifying each against the live site."""
    try:
        summary = await filters.capture_filters(
            task_id=request.task_id, max_categories=request.max_categories,
            verify=request.verify, store=request.store,
        )
    except Exception as e:
        logger.exception("Primark filter capture failed")
        raise HTTPException(status_code=502, detail=str(e))

    # The documents are large and the reader goes straight to Mongo; the response
    # is what the scheduler needs to report the run.
    documents = summary.pop("documents", [])
    summary["categories_captured"] = [d["category"]["key"] for d in documents]
    summary["rejected_total"] = sum(len(d.get("rejected_filters") or []) for d in documents)
    summary["collection"] = filters.COLLECTION
    return _serialise(summary)
