"""
Loss-free, batched delivery of updated parent ASINs to the AI-stylist search API.

Replaces the old per-product fan-out (`_call_embed_service`): one
`PATCH /api/v1/product/update` per ASIN, all fired at the END of a run. On
2026-09-22 namshi fired 8,152 of those in 0.4s; 1,389 died on connect/pool
timeouts and were never retried, 5,507 hit a 2s read timeout that was logged as
"dispatched", and the burst itself kept jamming the API's MongoDB connection.

API contract (verified against the live OpenAPI):
    PATCH <bulk url>   {"asins": [...]}            1..5000 parent or variant ASINs
      202 {"status": "queued", "count": n, "task_id": "...", "check_status_at": "..."}
      422 validation error
    GET <update url>/status/{task_id}
      200 {"task_id", "state": PENDING|RETRY|SUCCESS|FAILURE, "ready", "result"?}
The API needs ONLY ASINs: its worker re-reads each product from MongoDB. So an ASIN
must be added only AFTER its product write succeeded (same rule as before), and
re-sending is always safe (idempotent).

Guarantees
  * Load is spread over the run: a background flusher sends a batch when
    `batch_size` ASINs are waiting or the oldest has waited `max_wait_s`.
  * One keep-alive HTTP client, 30s timeout. ONLY a 202 counts as delivered.
    Transport errors, 5xx and 429 are retried with backoff; other 4xx are not.
  * Nothing is dropped. Every accepted batch's task_id is confirmed via the status
    endpoint; a FAILURE is re-sent. Whatever still can't be delivered or confirmed
    when the run ends — or is waiting when the run is cancelled — is saved to this
    scraper's Redis (`<source>:product_bulk_sync:spool`) and sent FIRST next run.

Usage (inside the async pipeline)
    sync = BulkProductSync(source="namshi", task_id=task_id)
    await sync.start()            # also replays last run's spool
    sync.add(parent_asin)         # sync, O(1), never raises — after the write succeeded
    stats = await sync.close()    # end of run: final flush + confirm + spool leftovers
    await sync.abort()            # on cancellation: spool everything, no network

Env: PRODUCT_BULK_UPDATE_URL (else derived as PRODUCT_UPDATE_URL + "/bulk" — never
repoint PRODUCT_UPDATE_URL itself: other callers still send {"asin": ...} to it),
PRODUCT_BULK_BATCH_SIZE (500), PRODUCT_BULK_MAX_WAIT_S (120),
PRODUCT_BULK_CONFIRM_TIMEOUT_S (180), REDIS_HOST / REDIS_PORT.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from itertools import islice
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

API_MAX_BATCH = 5000                       # server-side maxItems
_RETRY_DELAYS_S = (2, 5, 10, 20, 40, 60)   # ~2.3 min of retries per batch
_STATUS_POLL_EVERY_S = 60.0                # opportunistic confirmation during the run


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def bulk_update_url() -> Optional[str]:
    explicit = (os.getenv("PRODUCT_BULK_UPDATE_URL") or "").strip()
    if explicit:
        return explicit.rstrip("/")
    single = (os.getenv("PRODUCT_UPDATE_URL") or "").strip()
    return single.rstrip("/") + "/bulk" if single else None


class BulkProductSync:
    def __init__(
        self,
        source: str,
        task_id: str = "",
        batch_size: Optional[int] = None,
        max_wait_s: Optional[float] = None,
        confirm_timeout_s: Optional[float] = None,
        url: Optional[str] = None,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        retry_delays_s: tuple = _RETRY_DELAYS_S,
    ):
        self.source = source
        self.task_id = task_id
        self.batch_size = min(max(1, batch_size or _env_int("PRODUCT_BULK_BATCH_SIZE", 500)), API_MAX_BATCH)
        self.max_wait_s = max_wait_s if max_wait_s is not None else _env_float("PRODUCT_BULK_MAX_WAIT_S", 120.0)
        self.confirm_timeout_s = (confirm_timeout_s if confirm_timeout_s is not None
                                  else _env_float("PRODUCT_BULK_CONFIRM_TIMEOUT_S", 180.0))
        self.url = url or bulk_update_url()
        self.status_base = (self.url[: -len("/bulk")] + "/status"
                            if self.url and self.url.endswith("/bulk") else None)
        self._redis_host = redis_host or os.getenv("REDIS_HOST", "localhost")
        self._redis_port = int(redis_port or os.getenv("REDIS_PORT", "6379"))
        self._retry_delays = retry_delays_s
        self.spool_key = f"{source}:product_bulk_sync:spool"
        self._replaying_key: Optional[str] = None

        self._pending: dict[str, None] = {}          # ordered set of not-yet-sent ASINs
        self._oldest_pending_at: Optional[float] = None
        self._inflight: dict[str, list[str]] = {}    # task_id -> ASINs awaiting confirmation
        self._sending: list[str] = []                # the batch currently on the wire
        self._api_down = False                       # last batch exhausted its retries
        self._client: Optional[httpx.AsyncClient] = None
        self._redis = None
        self._wake: Optional[asyncio.Event] = None
        self._flusher: Optional[asyncio.Task] = None
        self._closing = False
        self._finished = False                       # close()/abort() has handed off everything
        self._late_tasks: set = set()
        self._last_poll = 0.0
        self.stats = {"added": 0, "replayed": 0, "batches_sent": 0, "asins_sent": 0,
                      "asins_confirmed": 0, "asins_resent_after_failure": 0,
                      "asins_spooled": 0, "send_failures": 0}

    # ── lifecycle ─────────────────────────────────────────────────────────
    async def start(self) -> None:
        self._wake = asyncio.Event()
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
            headers={"accept": "application/json"},
        )
        await self._open_redis()
        await self._replay_spool()
        if not self.url:
            logger.error("[%s] PRODUCT_UPDATE_URL / PRODUCT_BULK_UPDATE_URL not set — "
                         "search updates will be spooled, not sent", self.task_id)
        self._flusher = asyncio.create_task(self._run_flusher())
        logger.info("[%s] product bulk sync started url=%s batch=%d max_wait=%.0fs replayed=%d",
                    self.task_id, self.url, self.batch_size, self.max_wait_s, self.stats["replayed"])

    def add(self, asin: Optional[str]) -> None:
        """Queue one ASIN. Synchronous, O(1), never raises."""
        try:
            if not asin:
                return
            if self._finished:                          # a straggler write after the run stopped
                self._spool_late(asin)
                return
            if asin not in self._pending:
                self._pending[asin] = None
                self.stats["added"] += 1
            if self._oldest_pending_at is None:
                self._oldest_pending_at = time.monotonic()
            if len(self._pending) >= self.batch_size and self._wake is not None:
                self._wake.set()
        except Exception:  # noqa: BLE001 — must never break the scrape
            logger.exception("[%s] product bulk sync add() failed for %s", self.task_id, asin)

    async def close(self) -> dict:
        """End of run: flush everything, confirm, re-send failures, spool leftovers."""
        self._closing = True
        await self._stop_flusher()
        while self._pending:
            await self._flush_once()
        deadline = time.monotonic() + self.confirm_timeout_s
        while self._inflight:
            await self._poll_inflight()
            while self._pending:                       # FAILUREs re-queued by the poll
                await self._flush_once()
            remaining = deadline - time.monotonic()
            if not self._inflight or remaining <= 0:
                break
            await asyncio.sleep(min(2.0, remaining))   # never sleep past the deadline
        self._finished = True                           # from here add() spools directly
        leftover = [a for asins in self._inflight.values() for a in asins] + list(self._pending)
        self._inflight.clear()
        self._pending.clear()
        await self._spool(leftover, "unconfirmed at end of run")   # accepted but unconfirmed
        await self._finish_replay()
        await self._shutdown()
        logger.info("[%s] product bulk sync done: %s", self.task_id, self.stats)
        return dict(self.stats)

    async def abort(self) -> None:
        """Cancellation path: spool everything unsent/unconfirmed. No HTTP, no waiting."""
        self._closing = True
        self._finished = True
        if self._flusher is not None:
            self._flusher.cancel()
            self._flusher = None
        leftover = (list(self._sending) + list(self._pending)
                    + [a for asins in self._inflight.values() for a in asins])
        self._sending = []
        self._pending.clear()
        self._inflight.clear()
        if leftover:
            await self._spool(leftover, "run cancelled")
        await self._shutdown()

    # ── flushing ──────────────────────────────────────────────────────────
    async def _run_flusher(self) -> None:
        while not self._closing:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=1.0)   # plain Event, safe to cancel
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if self._closing:
                break
            try:
                due = (self._pending and (len(self._pending) >= self.batch_size or
                       time.monotonic() - (self._oldest_pending_at or 0) >= self.max_wait_s))
                if due:
                    await self._flush_once()
                if self._inflight and time.monotonic() - self._last_poll >= _STATUS_POLL_EVERY_S:
                    await self._poll_inflight()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — keep flushing no matter what
                logger.exception("[%s] product bulk sync flusher error", self.task_id)
                await asyncio.sleep(5)

    async def _flush_once(self) -> None:
        batch = list(islice(self._pending, self.batch_size))
        for a in batch:
            del self._pending[a]
        self._oldest_pending_at = time.monotonic() if self._pending else None
        if not batch:
            return
        if self._closing and self._api_down:
            # Closing with the API unreachable: don't spend ~2 min of retries per
            # remaining batch (that could push the run past ARQ's job_timeout).
            await self._spool(batch, "API unreachable at end of run")
            return
        self._sending = batch
        try:
            task_id = await self._send(batch)
        finally:
            self._sending = []
        if task_id:
            self._inflight[task_id] = batch
            self.stats["batches_sent"] += 1
            self.stats["asins_sent"] += len(batch)
        else:
            self.stats["send_failures"] += 1
            await self._spool(batch, "could not be delivered")

    async def _send(self, batch: list[str]) -> Optional[str]:
        """PATCH one batch. Returns the task_id on 202, else None."""
        if not self.url or self._client is None:
            return None
        attempts = len(self._retry_delays) + 1
        for attempt in range(attempts):
            try:
                r = await self._client.patch(self.url, json={"asins": batch, "mode": "auto"})
                if r.status_code == 202:
                    self._api_down = False
                    task_id = (r.json() or {}).get("task_id") or f"untracked-{time.time()}"
                    logger.info("[%s] bulk update queued n=%d task_id=%s", self.task_id, len(batch), task_id)
                    return task_id
                if r.status_code != 429 and r.status_code < 500:
                    logger.error("[%s] bulk update rejected HTTP %s (not retried): %s",
                                 self.task_id, r.status_code, r.text[:300])
                    return None
                reason = f"HTTP {r.status_code}"
            except httpx.HTTPError as exc:
                reason = f"{type(exc).__name__}: {exc}"
            if attempt < attempts - 1:
                delay = self._retry_delays[attempt]
                logger.warning("[%s] bulk update attempt %d/%d failed (%s) — retrying in %ss",
                               self.task_id, attempt + 1, attempts, reason, delay)
                await asyncio.sleep(delay)
            else:
                self._api_down = True
                logger.error("[%s] bulk update failed after %d attempts (%s)", self.task_id, attempts, reason)
        return None

    async def _poll_inflight(self) -> None:
        self._last_poll = time.monotonic()
        if not self.status_base or self._client is None:
            return
        for task_id in list(self._inflight):
            if task_id.startswith("untracked-"):
                self.stats["asins_confirmed"] += len(self._inflight.pop(task_id))
                continue
            try:
                r = await self._client.get(f"{self.status_base}/{task_id}", timeout=10.0)
                if r.status_code != 200:
                    continue
                state = (r.json() or {}).get("state")
            except httpx.HTTPError:
                continue
            if state == "SUCCESS":
                self.stats["asins_confirmed"] += len(self._inflight.pop(task_id))
            elif state == "FAILURE":
                asins = self._inflight.pop(task_id)
                logger.warning("[%s] bulk task %s FAILED server-side — re-sending %d ASINs",
                               self.task_id, task_id, len(asins))
                self.stats["asins_resent_after_failure"] += len(asins)
                for a in asins:
                    self.add(a)
            # PENDING / RETRY / STARTED: keep waiting

    # ── spool (this scraper's Redis) ──────────────────────────────────────
    async def _open_redis(self) -> None:
        try:
            import redis.asyncio as aioredis
            self._redis = aioredis.Redis(host=self._redis_host, port=self._redis_port,
                                         decode_responses=True, socket_timeout=10,
                                         socket_connect_timeout=10)
            await self._redis.ping()
        except Exception as exc:  # noqa: BLE001
            logger.error("[%s] product bulk sync: Redis unavailable (%s) — undeliverable "
                         "ASINs will be written to the log instead", self.task_id, exc)
            self._redis = None

    async def _replay_spool(self) -> None:
        """Take over last run's spool (plus any replay left by a crashed run)."""
        if self._redis is None:
            return
        try:
            keys = [self.spool_key] if await self._redis.exists(self.spool_key) else []
            keys += [k async for k in self._redis.scan_iter(match=f"{self.spool_key}:replaying:*")]
            if not keys:
                return
            self._replaying_key = f"{self.spool_key}:replaying:{int(time.time())}"
            for k in keys:                              # merge into one replaying set atomically-ish
                if k == self._replaying_key:
                    continue
                await self._redis.sunionstore(self._replaying_key, [self._replaying_key, k])
                await self._redis.delete(k)
            members = await self._redis.smembers(self._replaying_key)
            for a in sorted(members):
                self.add(a)
            self.stats["replayed"] = len(members)
            logger.info("[%s] product bulk sync: replaying %d ASINs spooled by an earlier run",
                        self.task_id, len(members))
        except Exception as exc:  # noqa: BLE001
            logger.error("[%s] product bulk sync: spool replay failed (%s)", self.task_id, exc)

    async def _finish_replay(self) -> None:
        # Everything replayed has now been sent+confirmed or re-spooled under spool_key.
        if self._redis is not None and self._replaying_key:
            try:
                await self._redis.delete(self._replaying_key)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[%s] could not delete %s (%s) — it will be replayed again (safe)",
                               self.task_id, self._replaying_key, exc)

    async def _spool(self, asins: list[str], reason: str) -> None:
        if not asins:
            return
        self.stats["asins_spooled"] += len(asins)
        if self._redis is not None:
            try:
                await self._redis.sadd(self.spool_key, *asins)
                logger.warning("[%s] spooled %d ASINs to Redis %s (%s) — sent first next run",
                               self.task_id, len(asins), self.spool_key, reason)
                return
            except Exception as exc:  # noqa: BLE001
                logger.error("[%s] Redis spool failed (%s)", self.task_id, exc)
        logger.error("[%s] UNDELIVERED ASINs (%s), re-send manually: %s", self.task_id, reason, asins)

    def _spool_late(self, asin: str) -> None:
        """add() after close()/abort(): spool on a fresh connection (the run's own one may
        already be closed) so the ASIN is still sent first next run."""
        self.stats["asins_spooled"] += 1

        async def _go() -> None:
            try:
                import redis.asyncio as aioredis
                r = aioredis.Redis(host=self._redis_host, port=self._redis_port,
                                   socket_timeout=10, socket_connect_timeout=10)
                try:
                    await r.sadd(self.spool_key, asin)
                finally:
                    await r.aclose()
                logger.warning("[%s] late ASIN %s spooled after the run stopped", self.task_id, asin)
            except Exception as exc:  # noqa: BLE001
                logger.error("[%s] UNDELIVERED ASIN (late, %s), re-send manually: %s",
                             self.task_id, exc, asin)

        try:
            task = asyncio.get_running_loop().create_task(_go())
            self._late_tasks.add(task)
            task.add_done_callback(self._late_tasks.discard)
        except RuntimeError:
            logger.error("[%s] UNDELIVERED ASIN (late, no event loop), re-send manually: %s",
                         self.task_id, asin)

    # ── teardown ──────────────────────────────────────────────────────────
    async def _stop_flusher(self) -> None:
        if self._wake is not None:
            self._wake.set()
        if self._flusher is not None:
            try:
                await asyncio.wait_for(self._flusher, timeout=self.confirm_timeout_s + 180)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._flusher.cancel()
            except Exception:  # noqa: BLE001
                pass
            self._flusher = None

    async def _shutdown(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._client = None
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._redis = None
