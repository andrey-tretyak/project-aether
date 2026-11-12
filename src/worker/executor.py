from __future__ import annotations

import asyncio
import json
import os
import socket
import time
from typing import Any

from redis import Redis as SyncRedis
from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.common.config import Settings, load_settings
from src.common.logger import get_logger
from src.common.models import TaskStatus
from src.control_plane.heartbeat import HeartbeatMonitor
from src.control_plane.scheduler import RESULTS_CHANNEL, TASK_OWNER_PREFIX, TASK_QUEUE
from src.control_plane.wal import ResultCommitted, StaleFenceError, WriteAheadLog, result_op
from src.worker.effects import MockExternalAPI, perform_effects
from src.worker.heartbeat_thread import HeartbeatThread
from src.worker.mock_agent import run_mock_agent

log = get_logger("aether.worker")

LOCK_PREFIX = "aether:lock:"
COMMIT_RETRY_S = 30.0


class WorkerExecutor:
    def __init__(
        self,
        redis: Redis,
        worker_id: str | None = None,
        heartbeat_client: SyncRedis | None = None,
        settings: Settings | None = None,
        external_api_redis: Redis | None = None,
    ):
        self.redis = redis
        self.worker_id = worker_id or os.environ.get("WORKER_ID") or _default_worker_id()
        self.settings = settings or load_settings()
        self.wal = WriteAheadLog(redis)
        self.heartbeat = HeartbeatMonitor(redis, self._ignore_failure, self.settings)
        self.heartbeat_client = heartbeat_client
        self.external_api = MockExternalAPI(external_api_redis or redis)
        self._lock_ttl_ms = int(self.settings.lock_ttl_s * 1000)
        self._last_loop_tick = time.monotonic()
        self._stop = asyncio.Event()

    async def _ignore_failure(self, worker_id: str) -> None:
        return None

    async def run(self) -> None:
        log.info(
            "worker.started",
            extra={
                "extra_fields": {
                    "worker_id": self.worker_id,
                    "heartbeat_mode": self.settings.heartbeat_mode,
                    "lease_s": self.settings.lease_s,
                }
            },
        )
        background: list[asyncio.Task[None]] = []
        thread: HeartbeatThread | None = None
        if self.settings.heartbeat_mode == "thread":
            if self.heartbeat_client is None:
                raise ValueError("thread heartbeat mode needs a synchronous Redis client")
            thread = HeartbeatThread(
                self.heartbeat_client,
                self.worker_id,
                self.settings.heartbeat_interval_s,
                self.settings.lease_s,
                lambda: self._last_loop_tick,
                self.settings.max_loop_stall_s,
            )
            thread.start()
            background.append(asyncio.create_task(self._tick_loop(), name="worker-loop-tick"))
        else:
            background.append(asyncio.create_task(self._heartbeat_loop(), name="worker-heartbeat"))
        try:
            while not self._stop.is_set():
                try:
                    item = await self.redis.brpop(TASK_QUEUE, timeout=1)
                except RedisError as exc:
                    log.info("worker.redis_unavailable", extra={"extra_fields": {"error": str(exc)}})
                    await asyncio.sleep(0.5)
                    continue
                if item is None:
                    continue
                _, raw = item
                try:
                    envelope = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                await self._execute(envelope)
        finally:
            self._stop.set()
            for task in background:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            if thread is not None:
                thread.stop()
            try:
                await self.heartbeat.deregister(self.worker_id)
            except RedisError:
                log.info("worker.deregister_failed", extra={"extra_fields": {"worker_id": self.worker_id}})
            else:
                log.info("worker.deregistered", extra={"extra_fields": {"worker_id": self.worker_id}})

    def request_stop(self) -> None:
        """Drain: finish the current task, take no new ones, then give up the lease."""
        if not self._stop.is_set():
            log.info("worker.draining", extra={"extra_fields": {"worker_id": self.worker_id}})
        self._stop.set()

    async def stop(self) -> None:
        self.request_stop()

    async def _tick_loop(self) -> None:
        while not self._stop.is_set():
            self._last_loop_tick = time.monotonic()
            await asyncio.sleep(self.settings.heartbeat_interval_s)

    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.heartbeat.beat(self.worker_id)
            except Exception:
                log.exception("worker.heartbeat_failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.settings.heartbeat_interval_s)
            except asyncio.TimeoutError:
                continue

    async def _execute(self, envelope: dict[str, Any]) -> None:
        task_id = str(envelope["task_id"])
        workflow_id = str(envelope["workflow_id"])
        fence = int(envelope["fence_token"])
        lock_key = f"{LOCK_PREFIX}{task_id}"
        token = f"{self.worker_id}:{fence}"
        try:
            if not await self._claim(workflow_id, task_id, fence, lock_key, token):
                return
        except RedisError as exc:
            # Whether or not the claim landed, the control plane requeues a task nobody holds.
            log.info("worker.abandoned", extra={"extra_fields": {"task_id": task_id, "error": str(exc)}})
            return

        refresh = asyncio.create_task(self._refresh_lock(lock_key, token), name=f"lock-{task_id}")
        try:
            outcome = {"fence_token": fence, "assigned_worker": self.worker_id}
            try:
                payload = envelope.get("payload") or {}
                result = await run_mock_agent(envelope.get("agent_prompt", ""), payload)
                effects = payload.get("effects") or []
                if effects:
                    result["effects"] = await perform_effects(
                        self.external_api, workflow_id, task_id, effects, keyed=self.settings.idempotency_keys
                    )
                outcome.update(type="task_completed", status=TaskStatus.COMPLETED.value, result=result)
            except Exception as exc:
                outcome.update(type="task_failed", status=TaskStatus.FAILED.value, error=str(exc))
            await self._commit({"workflow_id": workflow_id, "task_id": task_id, **outcome})
        finally:
            refresh.cancel()
            try:
                await refresh
            except asyncio.CancelledError:
                pass
            try:
                if await self.redis.get(lock_key) == token:
                    await self.redis.delete(lock_key)
            except RedisError:
                pass

    async def _claim(self, workflow_id: str, task_id: str, fence: int, lock_key: str, token: str) -> bool:
        current = await self.wal.current_fence(task_id)
        if current != fence:
            log.info(
                "worker.reject_stale_envelope",
                extra={"extra_fields": {"task_id": task_id, "fence": fence, "current": current}},
            )
            return False

        acquired = await self.redis.set(lock_key, token, nx=True, px=self._lock_ttl_ms)
        if not acquired:
            held = await self.redis.get(lock_key)
            log.info(
                "worker.lock_miss",
                extra={"extra_fields": {"task_id": task_id, "held": held}},
            )
            return False

        await self.redis.set(f"{TASK_OWNER_PREFIX}{task_id}", self.worker_id, ex=30)
        started = {
            "type": "task_started",
            "workflow_id": workflow_id,
            "task_id": task_id,
            "status": TaskStatus.RUNNING.value,
            "fence_token": fence,
            "assigned_worker": self.worker_id,
        }
        try:
            # A Redis failover can hand a second worker an envelope another worker already finished.
            await self.wal.append_delta(
                workflow_id, started, op_id=f"started:{task_id}:{fence}:{self.worker_id}", unless_committed=True
            )
        except StaleFenceError:
            log.info("worker.stale_start", extra={"extra_fields": {"task_id": task_id, "fence": fence}})
            return False
        except ResultCommitted:
            log.info("worker.already_committed", extra={"extra_fields": {"task_id": task_id, "fence": fence}})
            return False
        log.info(
            "worker.claimed",
            extra={"extra_fields": {"task_id": task_id, "fence_token": fence, "worker_id": self.worker_id}},
        )
        return True

    async def _commit(self, delta: dict[str, Any]) -> None:
        """Commit a result, retrying through Redis outages until the outcome is known.

        The op id names the result slot for this task and fence, not this attempt, so a
        retry after an ambiguous failure, or a second worker holding the same envelope,
        cannot append a second result.
        """
        task_id, fence = delta["task_id"], delta["fence_token"]
        deadline = time.monotonic() + COMMIT_RETRY_S
        while True:
            try:
                await self.wal.append_delta(delta["workflow_id"], delta, op_id=result_op(task_id, fence))
                await self.redis.publish(RESULTS_CHANNEL, json.dumps(delta))
                return
            except StaleFenceError:
                event = "worker.stale_commit" if delta["type"] == "task_completed" else "worker.stale_fail_commit"
                log.info(event, extra={"extra_fields": {"task_id": task_id, "fence": fence}})
                return
            except RedisError as exc:
                if time.monotonic() > deadline:
                    log.info("worker.abandoned", extra={"extra_fields": {"task_id": task_id, "error": str(exc)}})
                    return
                log.info("worker.commit_retry", extra={"extra_fields": {"task_id": task_id, "error": str(exc)}})
                await asyncio.sleep(0.2)

    async def _refresh_lock(self, lock_key: str, token: str) -> None:
        while True:
            await asyncio.sleep(self.settings.lock_ttl_s / 2)
            try:
                if await self.redis.get(lock_key) != token:
                    return
                await self.redis.pexpire(lock_key, self._lock_ttl_ms)
            except RedisError:
                continue


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{int(time.time())}"
