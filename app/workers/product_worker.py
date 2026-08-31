"""
Product Worker - Handles image upload, color generation, and product posting.
Moved from app.core.worker.py to app.workers.product_worker.py
"""
import asyncio
import logging
import os
import json
from typing import Any, Dict, List

from datetime import datetime

from arq import create_pool, cron
from arq.connections import RedisSettings
from dotenv import load_dotenv

from app.config import set_environment
from app.core.pricing_config import fetch_pricing_config
from app.core.task_primark import update_primark_prices
from app.utils.color_utils import get_color_hexes_from_variants
from app.utils.image_processing import DownloadImage
from app.utils.primark_utils import post_product_to_api, append_log, complete_task
from app.workers.price_updater_worker import update_job_stats, is_previous_run_active

load_dotenv()

# So that upload/color logs show when running: arq app.workers.product_worker.Settings
_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
logging.basicConfig(
    level=getattr(logging, _level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


async def _upload_variant_images(
    task_id: str,
    product_id: str,
    product_data: Dict[str, Any],
) -> None:
    """Upload each variant's images to images/PRIMARK/{product_id}/{variant_asin}/ and
    replace `variant["product_images"]` with the returned CloudFront URLs. After all
    variants are uploaded, product-level `images_gallery` / `Product_images` are set
    to a copy of variant[0]'s uploaded URLs so the product doc never carries a union
    of every colour's images."""
    variants = product_data.get("variants") or []
    if not variants:
        return

    uploader = DownloadImage(task_id=task_id)

    for variant in variants:
        v_asin = (variant.get("ASIN") or variant.get("SKU") or variant.get("variant_id") or "unknown").strip() or "unknown"
        v_images = variant.get("product_images") or variant.get("images") or []
        if not v_images:
            variant["product_images"] = []
            continue
        try:
            v_s3_urls = await asyncio.to_thread(
                uploader.upload_images_to_s3, v_images, "PRIMARK", product_id, v_asin
            )
            variant["product_images"] = list(v_s3_urls)
            logger.info(f"[{task_id}] ✅ Uploaded {len(v_s3_urls)} images for variant {v_asin}")
        except Exception as e:
            logger.error(f"[{task_id}] ❌ Variant {v_asin} image upload failed: {e}")
            variant["product_images"] = list(v_images)

    v0_images = list(variants[0].get("product_images") or [])
    product_data["images_gallery"] = list(v0_images)
    product_data["Product_images"] = list(v0_images)


def _color_to_first_image_url(variants: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Build source_color -> first image URL from variants (one entry per color).
    Schema: variant has color (LocalizedText), source_color (str), size (str), product_images.
    Uses source_color when present, else color.en/ar, so keys match color_hexes lookup.
    """
    out: Dict[str, str] = {}
    for v in variants or []:
        if not isinstance(v, dict):
            continue
        source = (v.get("source_color") or "").strip()
        if not source:
            color_val = v.get("color")
            if isinstance(color_val, dict):
                source = (color_val.get("en") or color_val.get("ar") or "").strip()
            else:
                source = (color_val or "").strip()
        if not source or source in out:
            continue
        images = v.get("product_images") or v.get("images") or []
        if not isinstance(images, list):
            images = []
        url = next((u for u in images if isinstance(u, str) and u.strip()), None)
        if url:
            out[source] = url.strip()
    return out


async def generate_color_hexes(
    ctx,
    task_id: str,
    product_id: str,
    product_title: str,
    variants: List[Dict[str, Any]],
) -> Dict[str, Dict[str, str]]:
    """Resolve color + hex per variant. Returns source_color (str) -> {color, color_hex, source_color} for variant schema."""
    color_to_image = _color_to_first_image_url(variants or [])
    try:
        logger.info(f"[{task_id}] 🎨 Generating color hexes for {product_id} ({len(color_to_image)} colors)")
        result = await get_color_hexes_from_variants(product_title, color_to_image)
        logger.info(f"[{task_id}] ✅ Generated {len(result)} color entries for {product_id}")
        return result
    except Exception as e:
        logger.error(f"[{task_id}] ❌ Color generation failed for {product_id}: {e}")
        return {
            c: {"color": c, "color_ar": "", "color_hex": "#000000", "source_color": c}
            for c in color_to_image
        }


async def apply_data_and_post(
    task_id: str,
    product_id: str,
    product_data: Dict[str, Any],
    color_hexes: Dict[str, Dict[str, str]],
) -> bool:
    """
    Apply color results to product_data and POST to API.
    Variant schema: color (LocalizedText), color_hex (str), source_color (str), size (str).
    Leaves variant.size and other variant fields unchanged. Image URLs are mapped to
    S3 upstream in process_full_product.
    """
    try:
        # Shallow copy so we don't mutate the original
        data = product_data.copy()

        # Apply color: keep source_color as exact extracted; color (en/ar) and color_hex from API or created.
        # Case-insensitive lookup so "Bordeaux" and "BORDEAUX" both match.
        color_hexes_by_lower = {k.strip().lower(): (k, v) for k, v in color_hexes.items()}

        # First pass: collect unique Gemini-generated color names
        unique_colors_en = []
        unique_colors_ar = []
        seen_colors = set()

        for variant in data.get("variants", []):
            source_key = (variant.get("source_color") or "").strip()
            if not source_key:
                color_val = variant.get("color")
                if isinstance(color_val, dict):
                    source_key = (color_val.get("en") or color_val.get("ar") or "").strip()
                else:
                    source_key = (color_val or "").strip()
            if not source_key:
                continue

            lookup_key = source_key.lower()
            if lookup_key in color_hexes_by_lower:
                _canonical, resolved = color_hexes_by_lower[lookup_key]
                color_en = resolved.get("color", source_key)
                color_ar = resolved.get("color_ar", "")

                if color_en and color_en not in seen_colors:
                    unique_colors_en.append(color_en)
                    unique_colors_ar.append(color_ar)
                    seen_colors.add(color_en)

        # Second pass: apply colors to variants
        for variant in data.get("variants", []):
            source_key = (variant.get("source_color") or "").strip()
            if not source_key:
                color_val = variant.get("color")
                if isinstance(color_val, dict):
                    source_key = (color_val.get("en") or color_val.get("ar") or "").strip()
                else:
                    source_key = (color_val or "").strip()
            if not source_key:
                continue

            lookup_key = source_key.lower()
            if lookup_key in color_hexes_by_lower:
                _canonical, resolved = color_hexes_by_lower[lookup_key]
                variant["color"] = {
                    "en": resolved.get("color", source_key),
                    "ar": resolved.get("color_ar", ""),
                }
                variant["color_hex"] = resolved.get("color_hex", "#000000")
                variant["source_color"] = source_key
            else:
                variant["color"] = {"en": source_key, "ar": ""}
                variant["color_hex"] = "#000000"
                variant["source_color"] = source_key

            if "attributes" not in variant:
                variant["attributes"] = {}
            if "colors" not in variant["attributes"]:
                variant["attributes"]["colors"] = {"en": [], "ar": []}
            variant["attributes"]["colors"]["en"] = unique_colors_en.copy()
            variant["attributes"]["colors"]["ar"] = unique_colors_ar.copy()

        # Copy size_guide_chart, conversion_guide_chart, model_measurements from product to each variant
        product_attrs = data.get("attributes") or {}
        for variant in data.get("variants", []):
            if "attributes" not in variant:
                variant["attributes"] = {}
            for key in ("size_guide_chart", "conversion_guide_chart", "model_measurements"):
                if product_attrs.get(key):
                    variant["attributes"][key] = product_attrs[key]

        if "attributes" not in data:
            data["attributes"] = {}
        if "colors" not in data["attributes"]:
            data["attributes"]["colors"] = {"en": [], "ar": []}
        data["attributes"]["colors"]["en"] = unique_colors_en
        data["attributes"]["colors"]["ar"] = unique_colors_ar

        # POST to API
        asin = data.get("ASIN", "?")
        logger.info(f"[{task_id}] 🚀 Posting product {product_id} (ASIN={asin}) to API")
        success, error = await asyncio.to_thread(post_product_to_api, {"data": data})

        if success:
            logger.info(f"[{task_id}] ✅ Successfully posted product {product_id} (ASIN={asin})")
            append_log(task_id, f"✅ Product {product_id} (ASIN={asin}) posted successfully", "info")
            return True
        else:
            logger.error(f"[{task_id}] ❌ Failed to post product {product_id} (ASIN={asin}): {error}")
            append_log(task_id, f"❌ Failed to post product {product_id} (ASIN={asin}): {error}", "error")
            return False

    except Exception as e:
        logger.error(f"[{task_id}] ❌ Assembly/post failed for {product_id}: {e}")
        append_log(task_id, f"❌ Assembly/post failed for {product_id}: {e}", "error")
        return False


async def process_full_product(
    ctx: Dict[str, Any],
    task_id: str,
    product_id: str,
    raw_product_data: Dict[str, Any],
    all_images: List[str],
    all_colors: List[str],
    environment: str = "prod",
) -> bool:
    """
    MASTER TASK: One job per product.
    Runs color → upload → apply+POST.
    `environment` arrives via ARQ job args (crosses process boundary).
    """
    # Set context once — all downstream code reads it automatically
    set_environment(environment)

    try:
        logger.info(f"[{task_id}] 🚀 Starting Full Pipeline for {product_id} (env={environment})")

        product_title = (
            (raw_product_data.get("original_name") or {}).get("en")
            or (raw_product_data.get("name") or {}).get("en")
            or ""
        )
        variants = raw_product_data.get("variants") or []

        color_hexes = await generate_color_hexes(
            ctx, task_id, product_id, product_title, variants=variants
        )
        await _upload_variant_images(task_id, product_id, raw_product_data)
        success = await apply_data_and_post(
            task_id, product_id, raw_product_data, color_hexes
        )
        return success

    except Exception as e:
        logger.error(f"[{task_id}] ❌ Master Task failed for {product_id}: {e}")
        append_log(task_id, f"❌ Master Task failed for {product_id}: {e}", "error")
        return False


async def finalize_task(
    ctx: Dict[str, Any],
    task_id: str,
    job_ids: List[str],
    final_status: str = "completed",
) -> bool:
    """
    Sentinel job: waits for all product jobs to finish, then signals completion.
    Runs inside the ARQ worker so awaiting is non-blocking async.
    """
    from arq.jobs import Job as ArqJob

    redis = ctx["redis"]
    logger.info(f"[{task_id}] 🔎 Sentinel waiting for {len(job_ids)} product jobs...")

    try:
        succeeded = 0
        failed = 0
        for job_id in job_ids:
            try:
                job = ArqJob(job_id, redis, _queue_name="arq:product")
                result = await job.result(timeout=600, poll_delay=2)
                if result is True:
                    succeeded += 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning(f"[{task_id}] Job {job_id} failed or timed out: {e}")
                failed += 1

        logger.info(f"[{task_id}] 🏁 All jobs done: {succeeded} succeeded, {failed} failed")
        append_log(task_id, f"Processing complete: {succeeded}/{len(job_ids)} products posted.", "info")
        return failed == 0

    except Exception as e:
        logger.error(f"[{task_id}] ❌ Sentinel crashed: {e}")
        append_log(task_id, f"❌ Finalization failed: {e}", "error")
        return False

    finally:
        complete_task(task_id, final_status)


async def scheduled_price_update(ctx):
    """Cron-triggered Primark price/stock update. Fires every 12 hours.
    Publishes progress/status to Redis so /price-updater/status and /history see it."""
    job_id = ctx.get("job_id", "unknown")
    redis_pool = ctx.get("redis")
    task_id = job_id
    start_time = datetime.utcnow()

    active, prev_job_id = await is_previous_run_active(redis_pool, job_id)
    if active:
        logger.warning(
            f"[cron] Previous Primark price-update run {prev_job_id} still active; skipping this fire"
        )
        return {"status": "skipped", "reason": "previous_run_in_progress", "previous_job_id": prev_job_id}

    logger.info(f"[cron] Starting Primark price update (task_id={task_id})")
    await update_job_stats(
        redis_pool, job_id, "running",
        start_time=start_time.isoformat(),
        task_id=task_id,
    )

    # Fetch the pricing config (categories/tiers/shipping constants) ONCE at
    # the start of this run. The same snapshot is reused for every variant so
    # prices within a single cron run are computed against a consistent ruleset
    # even if the remote config changes mid-run. If the fetch fails after
    # retries, abort — never silently fall back to stale constants in a
    # pricing job (mirrors namshi's scheduled_price_update).
    try:
        pricing_config = await fetch_pricing_config()
    except Exception as exc:
        logger.error(f"[cron] Failed to fetch pricing config: {exc}", exc_info=True)
        await update_job_stats(
            redis_pool, job_id, "failed",
            error=f"pricing_config fetch failed: {exc}",
            start_time=start_time.isoformat(),
            end_time=datetime.utcnow().isoformat(),
            task_id=task_id,
        )
        raise

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

    try:
        result = await update_primark_prices(
            task_id=task_id,
            batch_size=100,
            concurrency=5,
            on_progress=on_progress,
            pricing_config=pricing_config,
        )

        if job_id.startswith("cron:"):
            from app.core.task_primark import _PRICE_ALERTS
            from app.utils.price_alerts import post_price_drops
            if _PRICE_ALERTS:
                alerts_snapshot = dict(_PRICE_ALERTS)
                _PRICE_ALERTS.clear()
                try:
                    summary = await post_price_drops(alerts_snapshot)
                    logger.info(f"[cron] Primark price-drops posted: {summary}")
                except Exception as exc:
                    logger.error(f"[cron] Primark price-drops post failed: {exc}", exc_info=True)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        final_stats = {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "task_id": task_id,
        }
        if result:
            final_stats["total_records"] = result.get("total_records")
            final_stats["updated"] = result.get("updated")
            final_stats["skipped"] = result.get("skipped")
            final_stats["variants_completed"] = result.get("variants_completed")
        await update_job_stats(redis_pool, job_id, "completed", **final_stats)

        logger.info(f"[cron] Primark price update complete in {duration:.1f}s: {result}")
        return result
    except Exception as e:
        logger.error(f"[cron] Primark price update failed: {e}", exc_info=True)
        await update_job_stats(
            redis_pool, job_id, "failed",
            error=str(e),
            start_time=start_time.isoformat(),
            end_time=datetime.utcnow().isoformat(),
            task_id=task_id,
        )
        raise


class Settings:
    redis_settings = RedisSettings(host=REDIS_HOST, port=REDIS_PORT)
    queue_name = "arq:product"
    functions = [process_full_product, finalize_task, scheduled_price_update]
    # Primark price-update fires at 06:00 and 18:00 UTC.
    # Schedules across the projects are staggered 2 hours apart so the
    # commission API, source-store APIs and our own backend don't all get hit
    # simultaneously: Namshi 00/12 → Sephora 02/14 → Splash 04/16 →
    # Primark 06/18 → BrandsForLess 08/20.
    cron_jobs = [
        cron(
            scheduled_price_update,
            hour={6, 18},
            minute=0,
            run_at_startup=False,
            max_tries=1,
            timeout=86400,  # 24h — effectively no timeout; run until all variants processed
        ),
    ]
    job_timeout = 1800  # sentinel may wait a long time for all products
    max_jobs = 10
    allow_abort_jobs = True
