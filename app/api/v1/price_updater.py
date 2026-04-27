"""
API endpoints for continuous price/stock updates.
Auto-starts on server startup.
"""
import logging
import os
from typing import Optional

from arq import create_pool
from arq.connections import RedisSettings
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException

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

PRICE_UPDATER_QUEUE = "arq:price-updater"


def _job_stats_key(job_id: str) -> str:
    return f"{STATUS_PREFIX}:job:{job_id}:stats"


router = APIRouter()


async def get_arq_pool():
    """Get or create ARQ connection pool"""
    return await create_pool(REDIS_SETTINGS)


async def auto_start_price_updater():
    """
    Auto-start price updater on server startup.
    Called from main app lifespan/startup event.
    Clears stale job key from previous runs so a fresh cycle is always enqueued.
    """
    arq_pool = await get_arq_pool()
    
    try:
        async with arq_pool.client() as conn:
            await conn.delete(CURRENT_JOB_KEY)
        
        job = await arq_pool.enqueue_job(
            'continuous_price_update_task',
            _queue_name=PRICE_UPDATER_QUEUE,
            task_id="price-updater-auto",
            batch_size=50,
            filter_task_id=None,
            filter_sku=None,
            concurrency=5,
            delay_between_cycles=0,
        )
        
        logger.info(f"🚀 Price updater auto-started: {job.job_id}")
        
    except Exception as e:
        logger.error(f"❌ Failed to auto-start price updater: {e}")
    finally:
        await arq_pool.close()


@router.post("/price-updater/start")
async def start_price_updater(
    task_id: Optional[str] = None,
    batch_size: int = 100,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    concurrency: int = 5,
    delay_between_cycles: int = 0,
):
    """Start continuous price/stock updates (infinite loop)"""
    arq_pool = await get_arq_pool()
    
    try:
        # Check if already running
        async with arq_pool.client() as conn:
            current_job = await conn.get(CURRENT_JOB_KEY)
        
        if current_job:
            return {
                "message": "Price updater already running",
                "current_job_id": current_job.decode()
            }
        
        # Start new job
        job = await arq_pool.enqueue_job(
            'continuous_price_update_task',
            _queue_name=PRICE_UPDATER_QUEUE,
            task_id=task_id,
            batch_size=batch_size,
            filter_task_id=filter_task_id,
            filter_sku=filter_sku,
            concurrency=concurrency,
            delay_between_cycles=delay_between_cycles,
        )
        
        logger.info(f"✅ Price updater started: {job.job_id}")
        
        return {
            "message": "Price updater started",
            "job_id": job.job_id,
            "parameters": {
                "batch_size": batch_size,
                "filter_task_id": filter_task_id,
                "concurrency": concurrency,
                "delay_between_cycles": delay_between_cycles,
            }
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


def _enrich_status(stats: dict) -> dict:
    """Add total_products, updated, remaining and progress fields for API response."""
    total_records = _int_or_none(stats.get("total_records"))
    updated = _int_or_none(stats.get("updated"))
    skipped = _int_or_none(stats.get("skipped"))
    variants_completed = _int_or_none(stats.get("variants_completed"))
    if total_records is not None:
        remaining = max(0, total_records - (variants_completed or 0))
        stats["total_products"] = total_records
        stats["updated"] = updated if updated is not None else 0
        stats["skipped"] = skipped if skipped is not None else 0
        stats["remaining"] = remaining
        stats["progress"] = f"{variants_completed or 0}/{total_records}"
    else:
        stats["total_products"] = None
        stats["updated"] = updated
        stats["skipped"] = skipped
        stats["remaining"] = None
        stats["progress"] = str(variants_completed) if variants_completed is not None else None
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
            stats = _enrich_status(stats)
            
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
                stats = _enrich_status(stats.copy())
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
