from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis

from src.common.config import Settings, load_settings
from src.common.logger import get_logger

log = get_logger("aether.heartbeat")

HEARTBEAT_HASH = "aether:worker_heartbeats"
LEASE_PREFIX = "aether:lease:"


OnWorkerFailure = Callable[[str], Awaitable[None]]


def lease_key(worker_id: str) -> str:
    return f"{LEASE_PREFIX}{worker_id}"


class HeartbeatMonitor:
    """Tracks worker liveness via Redis hashes and short-lived lease keys."""

    def __init__(
        self,
        redis: Redis,
        on_worker_failure: OnWorkerFailure,
        settings: Settings | None = None,
    ):
        self.redis = redis
        self.on_worker_failure = on_worker_failure
        self.settings = settings or load_settings()
        self.timeout_s = self.settings.lease_s
        self.poll_interval_s = self.settings.monitor_poll_s
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._seeing_since: float | None = None

    def _lease_key(self, worker_id: str) -> str:
        return lease_key(worker_id)

    async def beat(self, worker_id: str) -> None:
        now = str(time.time())
        await self.redis.hset(HEARTBEAT_HASH, worker_id, now)
        await self.redis.set(self._lease_key(worker_id), "alive", px=int(self.settings.lease_s * 1000))

    async def deregister(self, worker_id: str) -> None:
        await self.redis.delete(self._lease_key(worker_id))
        await self.redis.hdel(HEARTBEAT_HASH, worker_id)

    async def is_alive(self, worker_id: str) -> bool:
        return bool(await self.redis.exists(self._lease_key(worker_id)))

    async def start(self) -> None:
        self._stop.clear()
        self._seeing_since = None
        self._task = asyncio.create_task(self._loop(), name="heartbeat-monitor")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def can_judge(self) -> bool:
        """Whether a lapsed lease proves its worker dead.

        Only once this monitor has seen Redis continuously for a whole lease: at start, or
        just after a Redis outage or failover, leases may have lapsed because nobody could
        reach Redis to renew them, and their workers are about to renew.
        """
        return self._seeing_since is not None and time.monotonic() - self._seeing_since >= self.timeout_s

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._scan_dead_workers()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._seeing_since = None
                log.exception("heartbeat.scan_failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval_s)
            except asyncio.TimeoutError:
                continue

    async def _scan_dead_workers(self) -> None:
        asked = time.monotonic()
        heartbeats = await self.redis.hgetall(HEARTBEAT_HASH)
        # A slow answer means Redis stalled (a frozen primary answers late rather than failing),
        # and renewals stalled with it.
        if self._seeing_since is None or time.monotonic() - asked > self.settings.heartbeat_interval_s:
            self._seeing_since = time.monotonic()
        if not self.can_judge():
            return
        now = time.time()
        for worker_id, ts_raw in heartbeats.items():
            worker_id = _as_str(worker_id)
            try:
                last = float(_as_str(ts_raw))
            except (TypeError, ValueError):
                last = 0.0
            lease_alive = await self.redis.exists(self._lease_key(worker_id))
            stale = (now - last) > self.timeout_s or not lease_alive
            if not stale:
                continue
            claimed = await self.redis.set(
                f"aether:failclaim:{worker_id}",
                "1",
                nx=True,
                px=int(self.timeout_s * 1000),
            )
            if not claimed:
                continue
            log.info(
                "worker.dead",
                extra={"extra_fields": {"worker_id": worker_id, "age_s": now - last}},
            )
            await self.redis.hdel(HEARTBEAT_HASH, worker_id)
            await self.on_worker_failure(worker_id)


def _as_str(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
