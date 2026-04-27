"""
API endpoints for continuous price/stock updates.
Auto-starts on server startup.
"""
import asyncio
import logging
import os
import time
from typing import Optional

import requests
from arq import create_pool
from arq.connections import RedisSettings
from arq.jobs import Job as ArqJob
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException

from app.utils.primark_utils import get_product_searching_url

load_dotenv()

logger = logging.getLogger(__name__)

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_SETTINGS = RedisSettings(host=REDIS_HOST, port=REDIS_PORT, database=0)

# Status keys (per-job key with TTL; no single hash to avoid unbounded growth)
STATUS_PREFIX = "primark_price_update"
CURRENT_JOB_KEY = f"{STATUS_PREFIX}:current_job_id"
STATS_KEY_PATTERN = f"{STATUS_PREFIX}:job:*:stats"

PRICE_UPDATER_QUEUE = "arq:product"
SCHEDULED_PRICE_UPDATE_FUNC = "scheduled_price_update"

SOURCE_TYPE = "PRIMARK"

# Cache the search-API-derived totals so /status and /history don't each re-query.
_SOURCE_TOTALS_CACHE: dict = {}  # {"value": int | None, "expires_at": float}
_SOURCE_TOTALS_TTL_SECONDS = 60


def _job_stats_key(job_id: str) -> str:
    return f"{STATUS_PREFIX}:job:{job_id}:stats"


router = APIRouter()


async def get_arq_pool():
    """Get or create ARQ connection pool"""
    return await create_pool(REDIS_SETTINGS)


async def auto_start_price_updater():
    """No-op: price updates are now scheduled via ARQ cron (00:00 & 12:00 UTC).
    Retained so main.py startup imports don't break."""
    logger.info("🕑 Price updater: cron-scheduled (00:00 & 12:00 UTC); auto-start skipped")


@router.post("/price-updater/start")
async def start_price_updater():
    """Trigger an ad-hoc price-update run now (in addition to the cron schedule)."""
    arq_pool = await get_arq_pool()

    try:
        async with arq_pool.client() as conn:
            current_job = await conn.get(CURRENT_JOB_KEY)
            if current_job:
                stats_data = await conn.get(_job_stats_key(current_job.decode()))
                stats = _parse_stats(stats_data) if stats_data else {}
                if stats.get("status") == "running":
                    return {
                        "message": "Price updater already running",
                        "current_job_id": current_job.decode(),
                    }

        job = await arq_pool.enqueue_job(
            SCHEDULED_PRICE_UPDATE_FUNC,
            _queue_name=PRICE_UPDATER_QUEUE,
        )

        logger.info(f"✅ Ad-hoc price updater enqueued: {job.job_id}")

        return {
            "message": "Price updater triggered",
            "job_id": job.job_id,
            "schedule": "Cron: 00:00 & 12:00 UTC",
        }
    finally:
        await arq_pool.close()


@router.post("/price-updater/stop")
async def stop_price_updater():
    """Abort the currently running price-update job (ad-hoc or cron).
    Worker must be started with allow_abort_jobs=True for this to take effect."""
    arq_pool = await get_arq_pool()
    try:
        async with arq_pool.client() as conn:
            current_job_raw = await conn.get(CURRENT_JOB_KEY)
            if not current_job_raw:
                return {"message": "No price updater currently running", "stopped": False}
            job_id = current_job_raw.decode()

        job = ArqJob(job_id, arq_pool, _queue_name=PRICE_UPDATER_QUEUE)
        try:
            aborted = await job.abort(timeout=5)
        except Exception as e:
            logger.warning(f"abort() error for {job_id}: {e}")
            aborted = False

        async with arq_pool.client() as conn:
            await conn.delete(CURRENT_JOB_KEY)

        logger.info(f"🛑 Price updater stop requested: {job_id} (aborted={aborted})")
        return {
            "message": "Price updater stop requested",
            "job_id": job_id,
            "aborted": aborted,
        }
    finally:
        await arq_pool.close()


@router.post("/scraper/stop-all")
async def stop_all_scraping():
    """
    Stop all scraping work:
    1. Signal stop_event on every threaded scrape/pricing task in the web process.
    2. Abort any in-progress arq jobs (process_full_product, finalize_task, etc.).
    3. Drop queued arq jobs.
    Preserves cron:scheduled_price_update — use /price-updater/stop for that.
    """
    threads_signalled = 0
    try:
        from app.api.v1.primark_scraper import TASKS as SCRAPE_TASKS
        for tid, task in list(SCRAPE_TASKS.items()):
            ev = task.get("stop_event") if isinstance(task, dict) else None
            if ev is not None and not ev.is_set():
                ev.set()
                threads_signalled += 1
    except Exception as e:
        logger.warning(f"could not iterate threaded scrape tasks: {e}")

    arq_pool = await get_arq_pool()
    try:
        removed_queued = 0
        aborted_running = 0
        preserved_cron = 0

        async with arq_pool.client() as conn:
            queued_raw = await conn.zrange(PRICE_UPDATER_QUEUE, 0, -1)
            queued_ids: set[str] = {(r.decode() if isinstance(r, bytes) else r) for r in queued_raw}

            in_progress_ids: set[str] = set()
            cursor = 0
            while True:
                cursor, batch = await conn.scan(cursor, match="arq:in-progress:*", count=200)
                for raw in batch:
                    key = raw.decode() if isinstance(raw, bytes) else raw
                    in_progress_ids.add(key.rsplit(":", 1)[-1])
                if cursor == 0:
                    break

            for job_id in in_progress_ids:
                if job_id.startswith("cron:"):
                    preserved_cron += 1
                    continue
                try:
                    job = ArqJob(job_id, arq_pool, _queue_name=PRICE_UPDATER_QUEUE)
                    if await job.abort(timeout=2):
                        aborted_running += 1
                except Exception:
                    pass
                await conn.delete(f"arq:in-progress:{job_id}", f"arq:job:{job_id}")

            for job_id in queued_ids - in_progress_ids:
                if job_id.startswith("cron:"):
                    preserved_cron += 1
                    continue
                await conn.zrem(PRICE_UPDATER_QUEUE, job_id)
                await conn.delete(f"arq:job:{job_id}")
                removed_queued += 1

        logger.info(
            f"🛑 /scraper/stop-all: threads_signalled={threads_signalled} removed={removed_queued} aborted={aborted_running} cron_preserved={preserved_cron}"
        )
        return {
            "message": "Product scraping stopped",
            "threads_signalled": threads_signalled,
            "removed_queued": removed_queued,
            "aborted_running": aborted_running,
            "preserved_cron": preserved_cron,
        }
    finally:
        await arq_pool.close()


def _parse_stats(stats_data) -> dict:
    """Parse stats string from Redis into dict."""
    if not stats_data:
        return {}
    import ast
    try:
        return ast.literal_eval(stats_data.decode())
    except Exception:
        return {}


def _int_or_none(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


async def _fetch_source_total() -> Optional[int]:
    """Authoritative count of records for SOURCE_TYPE via the central products search API.
    Cached for _SOURCE_TOTALS_TTL_SECONDS. Returns None on failure."""
    now = time.time()
    if _SOURCE_TOTALS_CACHE.get("expires_at", 0) > now:
        return _SOURCE_TOTALS_CACHE.get("value")

    search_url = get_product_searching_url()
    if not search_url:
        return None

    payload = {
        "paginationMode": "cursor",
        "filters": [{"source_type": SOURCE_TYPE}],
        "limit": 1,
        "projection": {"ASIN": 1},
        "returnFilters": False,
    }

    def _post():
        resp = requests.post(search_url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json().get("data", {}).get("total_records")

    try:
        total = await asyncio.to_thread(_post)
        value = _int_or_none(total)
        _SOURCE_TOTALS_CACHE["value"] = value
        _SOURCE_TOTALS_CACHE["expires_at"] = now + _SOURCE_TOTALS_TTL_SECONDS
        return value
    except Exception as exc:
        logger.warning("Failed to fetch source total from search API: %s", exc)
        return None


async def _enrich_status(stats: dict) -> dict:
    """Add total_products, updated, remaining and progress fields for API response.
    total_products is fetched live from the central search API (source_type filter),
    so it's populated even if the worker never wrote total_records to Redis."""
    worker_total = _int_or_none(stats.get("total_records"))
    updated = _int_or_none(stats.get("updated"))
    skipped = _int_or_none(stats.get("skipped"))
    variants_completed = _int_or_none(stats.get("variants_completed")) or 0

    authoritative_total = await _fetch_source_total()
    denom = authoritative_total if authoritative_total is not None else worker_total

    stats["total_products"] = authoritative_total if authoritative_total is not None else worker_total
    stats["updated"] = updated if updated is not None else 0
    stats["skipped"] = skipped if skipped is not None else 0

    if denom is not None and denom > 0:
        stats["remaining"] = max(0, denom - variants_completed)
        stats["progress"] = f"{variants_completed}/{denom}"
    else:
        stats["remaining"] = None
        stats["progress"] = str(variants_completed) if stats.get("variants_completed") is not None else None
    return stats


@router.get("/price-updater/status")
async def get_updater_status():
    """Get current job status with total_products, updated, remaining in separate fields."""
    arq_pool = await get_arq_pool()

    try:
        async with arq_pool.client() as conn:
            current_job_id = await conn.get(CURRENT_JOB_KEY)

            if not current_job_id:
                return {
                    "status": "idle",
                    "message": "No active job",
                    "current_job": None,
                    "total_products": None,
                    "updated": None,
                    "remaining": None,
                    "progress": None,
                }

            job_id = current_job_id.decode()
            stats_key = _job_stats_key(job_id)
            stats_data = await conn.get(stats_key)
            stats = _parse_stats(stats_data) if stats_data else {"job_id": job_id, "status": "running", "timestamp": None}
            stats = await _enrich_status(stats)

            return {
                "status": "active",
                "current_job": stats,
                "total_products": stats.get("total_products"),
                "updated": stats.get("updated"),
                "remaining": stats.get("remaining"),
                "progress": stats.get("progress"),
            }
    finally:
        await arq_pool.close()


@router.get("/price-updater/history")
async def get_updater_history(limit: int = 10):
    """Get job history with updated count (and total_products, remaining) for each job. Uses per-job keys with TTL."""
    arq_pool = await get_arq_pool()

    try:
        async with arq_pool.client() as conn:
            # Per-job keys with TTL (only non-expired keys exist)
            keys = await conn.keys(STATS_KEY_PATTERN)
            if not keys:
                return {"jobs": [], "message": "No history"}

            jobs = []
            for key in keys:
                key_str = key.decode() if isinstance(key, bytes) else key
                # Key format: primark_price_update:job:{job_id}:stats
                parts = key_str.split(":")
                job_id_str = parts[2] if len(parts) >= 4 else key_str
                stats_data = await conn.get(key)
                stats = _parse_stats(stats_data)
                if not stats:
                    continue
                stats = await _enrich_status(stats.copy())
                jobs.append({
                    "job_id": stats.get("job_id", job_id_str),
                    "status": stats.get("status"),
                    "timestamp": stats.get("timestamp"),
                    "start_time": stats.get("start_time"),
                    "end_time": stats.get("end_time"),
                    "duration_seconds": stats.get("duration_seconds"),
                    "total_products": stats.get("total_products"),
                    "updated": stats.get("updated"),
                    "skipped": stats.get("skipped"),
                    "remaining": stats.get("remaining"),
                    "progress": stats.get("progress"),
                })

            jobs.sort(key=lambda x: x.get("timestamp") or x.get("end_time") or "", reverse=True)
            return {"jobs": jobs[:limit]}
    finally:
        await arq_pool.close()
