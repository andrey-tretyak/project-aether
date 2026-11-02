from __future__ import annotations

import asyncio
import json
import os
import socket
import time
from typing import Any

from redis import Redis as SyncRedis
from redis.asyncio import Redis

from src.common.config import Settings, load_settings
from src.common.logger import get_logger
from src.common.models import TaskStatus
from src.control_plane.heartbeat import HeartbeatMonitor
from src.control_plane.scheduler import RESULTS_CHANNEL, TASK_OWNER_PREFIX, TASK_QUEUE
from src.control_plane.wal import StaleFenceError, WriteAheadLog
from src.worker.heartbeat_thread import HeartbeatThread
from src.worker.mock_agent import run_mock_agent

log = get_logger("aether.worker")

LOCK_PREFIX = "aether:lock:"


class WorkerExecutor:
    def __init__(
        self,
        redis: Redis,
        worker_id: str | None = None,
        heartbeat_client: SyncRedis | None = None,
        settings: Settings | None = None,
    ):
        self.redis = redis
        self.worker_id = worker_id or os.environ.get("WORKER_ID") or _default_worker_id()
        self.settings = settings or load_settings()
        self.wal = WriteAheadLog(redis)
        self.heartbeat = HeartbeatMonitor(redis, self._ignore_failure, self.settings)
        self.heartbeat_client = heartbeat_client
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
                item = await self.redis.brpop(TASK_QUEUE, timeout=1)
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
            await self.heartbeat.deregister(self.worker_id)
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
        current = await self.wal.current_fence(task_id)
        if current > fence:
            log.info(
                "worker.reject_stale_envelope",
                extra={"extra_fields": {"task_id": task_id, "fence": fence, "current": current}},
            )
            return

        lock_key = f"{LOCK_PREFIX}{task_id}"
        token = f"{self.worker_id}:{fence}"
        acquired = await self.redis.set(lock_key, token, nx=True, px=self._lock_ttl_ms)
        if not acquired:
            held = await self.redis.get(lock_key)
            log.info(
                "worker.lock_miss",
                extra={"extra_fields": {"task_id": task_id, "held": held}},
            )
            return

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
            await self.wal.append_delta(workflow_id, started)
        except StaleFenceError:
            log.info("worker.stale_start", extra={"extra_fields": {"task_id": task_id, "fence": fence}})
            return
        log.info(
            "worker.claimed",
            extra={"extra_fields": {"task_id": task_id, "fence_token": fence, "worker_id": self.worker_id}},
        )

        refresh = asyncio.create_task(self._refresh_lock(lock_key, token), name=f"lock-{task_id}")
        try:
            result = await run_mock_agent(envelope.get("agent_prompt", ""), envelope.get("payload") or {})
            delta = {
                "type": "task_completed",
                "workflow_id": workflow_id,
                "task_id": task_id,
                "status": TaskStatus.COMPLETED.value,
                "fence_token": fence,
                "assigned_worker": self.worker_id,
                "result": result,
            }
            await self.wal.append_delta(workflow_id, delta)
            await self.redis.publish(RESULTS_CHANNEL, json.dumps(delta))
        except StaleFenceError:
            log.info("worker.stale_commit", extra={"extra_fields": {"task_id": task_id, "fence": fence}})
        except Exception as exc:
            delta = {
                "type": "task_failed",
                "workflow_id": workflow_id,
                "task_id": task_id,
                "status": TaskStatus.FAILED.value,
                "fence_token": fence,
                "assigned_worker": self.worker_id,
                "error": str(exc),
            }
            try:
                await self.wal.append_delta(workflow_id, delta)
                await self.redis.publish(RESULTS_CHANNEL, json.dumps(delta))
            except StaleFenceError:
                log.info("worker.stale_fail_commit", extra={"extra_fields": {"task_id": task_id}})
        finally:
            refresh.cancel()
            try:
                await refresh
            except asyncio.CancelledError:
                pass
            held = await self.redis.get(lock_key)
            if held == token:
                await self.redis.delete(lock_key)

    async def _refresh_lock(self, lock_key: str, token: str) -> None:
        while True:
            await asyncio.sleep(self.settings.lock_ttl_s / 2)
            held = await self.redis.get(lock_key)
            if held != token:
                return
            await self.redis.pexpire(lock_key, self._lock_ttl_ms)


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{int(time.time())}"
