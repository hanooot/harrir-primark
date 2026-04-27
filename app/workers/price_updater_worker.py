"""
ARQ Worker for continuous Primark price and stock updates.
Automatically re-enqueues after each cycle for infinite operation.
"""
import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

from arq import create_pool
from arq.connections import RedisSettings
from dotenv import load_dotenv

from app.core.task_primark import update_primark_prices

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_SETTINGS = RedisSettings(host=REDIS_HOST, port=REDIS_PORT, database=0)

QUEUE_NAME = "arq:price-updater"

# Status tracking keys (per-job key with TTL to avoid unbounded growth)
STATUS_PREFIX = "primark_price_update"
CURRENT_JOB_KEY = f"{STATUS_PREFIX}:current_job_id"
STATS_TTL_SECONDS = 604800  # 7 days


def _job_stats_key(job_id: str) -> str:
    return f"{STATUS_PREFIX}:job:{job_id}:stats"


async def update_job_stats(redis_pool, job_id: str, status: str, **kwargs):
    """Update job statistics in Redis (per-job key with TTL)."""
    async with redis_pool.client() as conn:
        await conn.set(CURRENT_JOB_KEY, job_id)
        stats = {
            "job_id": job_id,
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            **kwargs
        }
        key = _job_stats_key(job_id)
        await conn.set(key, str(stats), ex=STATS_TTL_SECONDS)


async def continuous_price_update_task(ctx, **kwargs) -> dict:
    """
    Main ARQ task for continuous price/stock updates.
    Automatically re-enqueues itself for infinite operation.
    
    Parameters:
        task_id: Task identifier for logging (optional)
        batch_size: Number of variants to fetch per batch (default: 100)
        filter_task_id: Filter by specific task ID (optional)
        filter_sku: Filter by specific SKU (optional)
        concurrency: Number of parallel scraping operations (default: 5)
        delay_between_cycles: Seconds to wait before next cycle (default: 0)
    """
    job_id = ctx.get("job_id", "unknown")
    redis_pool = ctx.get("redis")
    
    # Extract parameters (task_id must be non-None for append_log)
    task_id = kwargs.get("task_id") or f"price-update-{job_id[:8]}"
    batch_size = kwargs.get("batch_size", 100)
    filter_task_id = kwargs.get("filter_task_id")
    filter_sku = kwargs.get("filter_sku")
    concurrency = kwargs.get("concurrency", 5)
    delay_between_cycles = kwargs.get("delay_between_cycles", 0)
    
    start_time = datetime.utcnow()
    
    logger.info("=" * 80)
    logger.info(f"🚀 Starting Price Update Cycle - Job ID: {job_id}")
    logger.info(f"   Task: {task_id}")
    logger.info(f"   Filter Task: {filter_task_id or 'None'}")
    logger.info(f"   Filter SKU: {filter_sku or 'None'}")
    logger.info(f"   Concurrency: {concurrency}, Batch Size: {batch_size}")
    logger.info("=" * 80)
    
    await update_job_stats(
        redis_pool, job_id, "running",
        start_time=start_time.isoformat(),
        task_id=task_id
    )
    
    try:
        async def on_progress(progress: dict):
            await update_job_stats(
                redis_pool, job_id, "running",
                start_time=start_time.isoformat(),
                task_id=task_id,
                total_records=progress.get("total_records"),
                updated=progress.get("updated"),
                skipped=progress.get("skipped"),
                variants_completed=progress.get("variants_completed"),
            )

        logger.info(f"Calling update_primark_prices (task_id={task_id})...")
        # Run the actual price update function
        pipeline_result = await update_primark_prices(
            task_id=task_id,
            batch_size=batch_size,
            filter_task_id=filter_task_id,
            filter_sku=filter_sku,
            concurrency=concurrency,
            stop_event=None,
            on_progress=on_progress,
        )
        
        # Calculate duration
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        # Update stats with final counts from pipeline
        stats_kwargs = {
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
        }
        if pipeline_result:
            stats_kwargs["total_records"] = pipeline_result.get("total_records")
            stats_kwargs["updated"] = pipeline_result.get("updated")
            stats_kwargs["skipped"] = pipeline_result.get("skipped")
            stats_kwargs["variants_completed"] = pipeline_result.get("variants_completed")
        await update_job_stats(redis_pool, job_id, "completed", **stats_kwargs)
        
        logger.info("=" * 80)
        logger.info(f"✅ Price Update Cycle COMPLETED - Job ID: {job_id}")
        logger.info(f"   Duration: {duration:.2f} seconds")
        if pipeline_result:
            logger.info(f"   Total: {pipeline_result.get('total_records')}, Updated: {pipeline_result.get('updated')}, Skipped: {pipeline_result.get('skipped')}")
        logger.info("=" * 80)
        
        result = {
            "status": "completed",
            "job_id": job_id,
            "duration_seconds": duration,
            "task_id": task_id,
        }
        if pipeline_result:
            result.update(pipeline_result)
        
        # Wait before next cycle if configured
        if delay_between_cycles > 0:
            logger.info(f"⏳ Waiting {delay_between_cycles} seconds before next cycle...")
            await asyncio.sleep(delay_between_cycles)
        
        # Re-enqueue for next cycle (INFINITE LOOP)
        logger.info("🔄 Enqueueing next price update cycle...")
        pool = await create_pool(REDIS_SETTINGS)
        next_job = await pool.enqueue_job('continuous_price_update_task', _queue_name=QUEUE_NAME, **kwargs)
        await pool.close()
        
        result["next_job_id"] = next_job.job_id
        logger.info(f"✅ Next cycle enqueued: {next_job.job_id}\n")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in price update cycle: {str(e)}", exc_info=True)
        
        # Update stats with error
        await update_job_stats(
            redis_pool, job_id, "failed",
            error=str(e),
            end_time=datetime.utcnow().isoformat()
        )
        
        # Re-enqueue despite error to ensure continuous operation
        logger.info("🔄 Re-enqueueing despite error...")
        try:
            if delay_between_cycles > 0:
                await asyncio.sleep(delay_between_cycles)
            
            pool = await create_pool(REDIS_SETTINGS)
            await pool.enqueue_job('continuous_price_update_task', _queue_name=QUEUE_NAME, **kwargs)
            await pool.close()
            logger.info("✅ Next cycle enqueued after error\n")
        except Exception as retry_error:
            logger.error(f"❌ Failed to re-enqueue: {str(retry_error)}")
        
        return {"status": "failed", "error": str(e)}


class Settings:
    """ARQ Worker Configuration"""
    functions = [continuous_price_update_task]
    redis_settings = REDIS_SETTINGS
    queue_name = QUEUE_NAME
    max_jobs = 3
    job_timeout = 14400  # 4 hours per job
    max_tries = 1  # Don't retry - we re-enqueue manually
    retry_jobs = False
    health_check_interval = 60
    keep_result = 86400  # Keep results for 24 hours
