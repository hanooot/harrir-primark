"""Post price-drop alerts to the hanooot express API.

Shape per the backend contract:
    POST {EXPRESS_API_URL}/product/alerts/price-drops
    Headers: x-admin-secret, Content-Type: application/json
    Body: {"<ASIN>": {"old_price": "<str>", "new_price": "<str>"}, ...}

Batched at POST_BATCH_SIZE per request with bounded concurrency so a 30k-variant
cron tick doesn't fire a single multi-megabyte POST.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Dict, List

import httpx

from app.config import get_config

logger = logging.getLogger(__name__)

POST_BATCH_SIZE = 500
MAX_CONCURRENCY = 5
REQUEST_TIMEOUT_SECONDS = 30.0


def _chunks(items: List[tuple], size: int):
    for i in range(0, len(items), size):
        yield dict(items[i:i + size])


async def _post_batch(client: httpx.AsyncClient, url: str, headers: dict, batch: Dict[str, Dict[str, str]]) -> bool:
    try:
        response = await client.post(url, headers=headers, json=batch)
        if response.status_code >= 400:
            logger.warning("price-drops batch failed status=%s body=%s", response.status_code, response.text[:500])
            return False
        return True
    except Exception as exc:
        logger.warning("price-drops batch error: %s", exc)
        return False


async def post_price_drops(alerts: Dict[str, Dict[str, str]]) -> Dict[str, int]:
    """POST collected price-drop alerts. Returns {'sent': int, 'failed': int, 'batches': int}.
    Never raises — failures are logged and counted so the caller's cron run isn't killed."""
    if not alerts:
        return {"sent": 0, "failed": 0, "batches": 0}

    base = get_config().EXPRESS_API_URL or os.getenv("EXPRESS_API_URL_PROD") or ""
    secret = os.getenv("HARRIR_ADMIN_SECRET", "").strip()

    if not base:
        logger.warning("price-drops: EXPRESS_API_URL not configured; skipping %d alert(s)", len(alerts))
        return {"sent": 0, "failed": len(alerts), "batches": 0}
    if not secret:
        logger.warning("price-drops: HARRIR_ADMIN_SECRET not set; skipping %d alert(s)", len(alerts))
        return {"sent": 0, "failed": len(alerts), "batches": 0}

    url = f"{base.rstrip('/')}/product/alerts/price-drops"
    headers = {"Content-Type": "application/json", "x-admin-secret": secret}

    items = list(alerts.items())
    batches = list(_chunks(items, POST_BATCH_SIZE))

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    sent = 0
    failed = 0

    async def _bounded(client: httpx.AsyncClient, batch: Dict[str, Dict[str, str]]):
        nonlocal sent, failed
        async with sem:
            ok = await _post_batch(client, url, headers, batch)
            if ok:
                sent += len(batch)
            else:
                failed += len(batch)

    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)
    async with httpx.AsyncClient(timeout=timeout) as client:
        await asyncio.gather(*(_bounded(client, batch) for batch in batches))

    logger.info("price-drops: posted %d batch(es) → sent=%d failed=%d", len(batches), sent, failed)
    return {"sent": sent, "failed": failed, "batches": len(batches)}
