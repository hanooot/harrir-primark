import threading
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from pydantic.config import ConfigDict

from app.config import set_environment
from app.utils.primark_utils import append_log, complete_task
from app.core.primark_scraper_api import run_scraper as run_scraper_api
from app.core.task_primark import run_update_primark_prices
import requests

router = APIRouter()
TASKS: dict[str, dict] = {}  # task_id -> {"thread": Thread, "stop_event": Event, "status": str}


class ScrapeRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    website_source: str = Field(default="primark")
    choose_method: str = Field(default="default")
    sku: Optional[str] = None
    job_name: Optional[str] = None
    url: str = Field(..., description="Primark category/listing URL")
    task_id: str
    product_target: Optional[int] = Field(default=None, description="Target number of products to scrape; omit or 0 to scrape until end")
    method: str = Field(default="api", description="Scraping method: 'api' or 'browser'")
    usd_to_aed: float = Field(default=3.67, description="USD to AED conversion rate")
    usd_to_iqd: float = Field(default=1500, description="USD to IQD conversion rate")
    environment: str = Field(default="prod", description="Environment: 'stage' or 'prod'")


class PricingUpdateRequest(BaseModel):
    task_id: str
    batch_size: int = Field(default=40, description="Batch size for pricing update")
    filter_task_id: Optional[str] = Field(default=None, description="Filter task ID")
    filter_sku: Optional[str] = Field(default=None, description="Filter SKU")
    usd_to_aed: float = Field(default=3.67, description="USD to AED conversion rate")
    usd_to_iqd: float = Field(default=1500, description="USD to IQD conversion rate")
    environment: str = Field(default="prod", description="Environment: 'stage' or 'prod'")


class KillRequest(BaseModel):
    task_id: str


def _scraper_worker(
    url: str,
    task_id: str,
    sku: Optional[str],
    method: str,
    product_target: Optional[int],
    usd_to_aed: float,
    usd_to_iqd: float,
    stop_event: threading.Event,
    environment: str = "prod",
):
    # Set context once for this thread — all downstream code reads it automatically
    set_environment(environment)
    try:
        run_scraper_api(
            target_url=url,
            task_id=task_id,
            sku=sku,
            stop_event=stop_event,
            product_target=product_target,
            usd_to_aed=usd_to_aed,
            usd_to_iqd=usd_to_iqd,
        )
        # NOTE: completion is now signalled by the last ARQ worker job
        # (or by run_scraper itself if 0 products were queued).
        # Do NOT call complete_task here — products are still being processed.
    except Exception as e:
        append_log(task_id, f"ERROR: {e}", "error")
        complete_task(task_id, "failed")


def _pricing_worker(
    task_id: str,
    batch_size: int,
    filter_task_id: Optional[str],
    filter_sku: Optional[str],
    usd_to_aed: float,
    usd_to_iqd: float,
    stop_event: threading.Event,
    environment: str = "prod",
):
    # Set context once for this thread
    set_environment(environment)
    try:
        run_update_primark_prices(
            task_id=task_id,
            batch_size=batch_size,
            filter_task_id=filter_task_id,
            filter_sku=filter_sku,
            stop_event=stop_event,
            usd_to_aed=usd_to_aed,
            usd_to_iqd=usd_to_iqd,
        )
    except Exception as e:
        append_log(task_id, f"Pricing update failed: {e}", "error")
        complete_task(task_id, "failed")
    else:
        status = "stopped" if stop_event.is_set() else "completed"
        complete_task(task_id, status)


@router.post("/scrape")
async def start_scraping_task(request: ScrapeRequest):
    print(f"Received scrape request: {request}")
    if request.website_source.lower() != "primark":
        raise HTTPException(status_code=400, detail="Only 'primark' allowed.")

    if request.task_id in TASKS and TASKS[request.task_id]["status"] == "running":
        raise HTTPException(status_code=409, detail="Task already running with this task_id.")

    if request.method.lower() not in ["api"]:
        raise HTTPException(status_code=400, detail="Method must be 'api' or 'browser'.")

    stop_event = threading.Event()
    worker_thread = threading.Thread(
        target=_scraper_worker,
        args=(
            request.url,
            request.task_id,
            request.sku,
            request.method,
            request.product_target,
            request.usd_to_aed,
            request.usd_to_iqd,
            stop_event,
            request.environment,
        ),
        daemon=True,
    )
    TASKS[request.task_id] = {"thread": worker_thread, "stop_event": stop_event, "status": "running"}
    worker_thread.start()

    target_msg = "until end" if (request.product_target is None or request.product_target == 0) else str(request.product_target)
    append_log(request.task_id, f"Scraping task accepted and started (method: {request.method}, target: {target_msg}).", "info")
    return {"status": "success", "message": "Scraping started.", "task_id": request.task_id}


@router.post("/scrape/kill")
async def kill_scraping_task(request: KillRequest):
    task = TASKS.get(request.task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found.")

    task["stop_event"].set()
    append_log(request.task_id, "Kill request received; attempting to stop task.", "warning")
    return {"status": "stopping", "task_id": request.task_id}


@router.post("/update-prices")
async def start_pricing_update_task(request: PricingUpdateRequest):
    if request.task_id in TASKS and TASKS[request.task_id]["status"] == "running":
        raise HTTPException(status_code=409, detail="Task already running with this task_id.")

    stop_event = threading.Event()
    worker_thread = threading.Thread(
        target=_pricing_worker,
        args=(
            request.task_id,
            request.batch_size,
            request.filter_task_id,
            request.filter_sku,
            request.usd_to_aed,
            request.usd_to_iqd,
            stop_event,
            request.environment,
        ),
        daemon=True,
    )
    TASKS[request.task_id] = {"thread": worker_thread, "stop_event": stop_event, "status": "running"}
    worker_thread.start()

    append_log(request.task_id, "Pricing update task accepted and started.", "info")
    return {"status": "success", "message": "Pricing update started.", "task_id": request.task_id}
