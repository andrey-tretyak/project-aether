from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from redis.asyncio import Redis

from src.common.logger import get_logger
from src.common.models import DAGWorkflow, TaskStatus
from src.control_plane.heartbeat import WORKER_TIMEOUT_S, HeartbeatMonitor
from src.control_plane.wal import WriteAheadLog

log = get_logger("aether.scheduler")

TASK_QUEUE = "aether:queue:tasks"
ACTIVE_WORKFLOWS = "aether:workflows:active"
TASK_OWNER_PREFIX = "aether:task_owner:"


class Scheduler:
    """DAG resolver and state machine. Dispatches ready tasks onto the Redis queue."""

    def __init__(self, redis: Redis):
        self.redis = redis
        self.wal = WriteAheadLog(redis)
        self.heartbeat = HeartbeatMonitor(redis, self.on_worker_failure)
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._stop.clear()
        await self.heartbeat.start()
        self._orphan_task = asyncio.create_task(self._orphan_loop(), name="orphan-reclaim")
        log.info("control_plane.started")

    async def stop(self) -> None:
        self._stop.set()
        await self.heartbeat.stop()
        task = getattr(self, "_orphan_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _orphan_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._reclaim_orphans()
            except Exception:
                log.exception("scheduler.orphan_scan_failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

    async def submit_workflow(self, workflow: DAGWorkflow) -> None:
        missing = workflow.missing_dependencies()
        if missing:
            raise ValueError(f"unknown dependencies: {missing}")
        if workflow.has_cycle():
            raise ValueError(f"cycle detected in workflow {workflow.workflow_id}")

        await self.wal.append_delta(
            workflow.workflow_id,
            {"type": "workflow_submitted", "workflow": workflow.model_dump(mode="json")},
        )
        await self.wal.checkpoint(workflow.workflow_id, {"workflow": workflow.model_dump(mode="json")})
        await self.redis.sadd(ACTIVE_WORKFLOWS, workflow.workflow_id)
        await self._dispatch_ready(workflow)

    async def apply_task_update(self, delta: dict[str, Any]) -> None:
        """Wake after a worker WAL commit: recover, checkpoint, dispatch newly ready tasks."""
        workflow_id = str(delta["workflow_id"])
        recovered = await self.wal.recover_latest_state(workflow_id)
        if recovered is None:
            return
        workflow = recovered.workflow
        await self.wal.checkpoint(workflow_id, {"workflow": workflow.model_dump(mode="json")})

        if workflow.is_complete() or workflow.has_failed():
            await self.redis.srem(ACTIVE_WORKFLOWS, workflow_id)
            log.info(
                "workflow.terminal",
                extra={
                    "extra_fields": {
                        "workflow_id": workflow_id,
                        "complete": workflow.is_complete(),
                        "failed": workflow.has_failed(),
                    }
                },
            )
            return
        await self._dispatch_ready(workflow)

    async def on_worker_failure(self, worker_id: str) -> None:
        """Reassign RUNNING tasks owned by a dead worker with a new fence token."""
        workflow_ids = await self.redis.smembers(ACTIVE_WORKFLOWS)
        for raw_id in workflow_ids:
            workflow_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            recovered = await self.wal.recover_latest_state(workflow_id)
            if recovered is None:
                continue
            workflow = recovered.workflow
            reassigned = False
            for task in workflow.tasks:
                if task.status != TaskStatus.RUNNING:
                    continue
                owner = await self.redis.get(f"{TASK_OWNER_PREFIX}{task.id}")
                if task.assigned_worker != worker_id and owner != worker_id:
                    continue
                await self._requeue_task(workflow, task.id, reason="worker_dead", worker_id=worker_id)
                reassigned = True
            if reassigned:
                recovered = await self.wal.recover_latest_state(workflow_id)
                if recovered:
                    await self.wal.checkpoint(
                        workflow_id, {"workflow": recovered.workflow.model_dump(mode="json")}
                    )
                    await self._dispatch_ready(recovered.workflow)

    async def _dispatch_ready(self, workflow: DAGWorkflow) -> None:
        for task in workflow.ready_tasks():
            fence = await self.wal.bump_fence(task.id)
            delta = {
                "type": "task_dispatched",
                "workflow_id": workflow.workflow_id,
                "task_id": task.id,
                "status": TaskStatus.RUNNING.value,
                "fence_token": fence,
            }
            await self.wal.append_delta(workflow.workflow_id, delta)
            envelope = {
                "workflow_id": workflow.workflow_id,
                "task_id": task.id,
                "agent_prompt": task.agent_prompt,
                "payload": task.payload,
                "fence_token": fence,
            }
            await self.redis.set(f"aether:dispatched:{task.id}", str(time.time()))
            await self.redis.lpush(TASK_QUEUE, json.dumps(envelope))
            log.info(
                "task.dispatched",
                extra={
                    "extra_fields": {
                        "workflow_id": workflow.workflow_id,
                        "task_id": task.id,
                        "fence_token": fence,
                    }
                },
            )

    async def _requeue_task(
        self,
        workflow: DAGWorkflow,
        task_id: str,
        reason: str,
        worker_id: str | None = None,
    ) -> None:
        task = workflow.task_map()[task_id]
        fence = await self.wal.bump_fence(task.id)
        delta = {
            "type": "task_reassigned",
            "workflow_id": workflow.workflow_id,
            "task_id": task.id,
            "status": TaskStatus.PENDING.value,
            "fence_token": fence,
            "assigned_worker": None,
            "retry_count": task.retry_count + 1,
        }
        await self.wal.append_delta(workflow.workflow_id, delta)
        await self.redis.delete(f"{TASK_OWNER_PREFIX}{task.id}")
        await self.redis.delete(f"aether:lock:{task.id}")
        log.info(
            "task.reassigned",
            extra={
                "extra_fields": {
                    "workflow_id": workflow.workflow_id,
                    "task_id": task.id,
                    "dead_worker": worker_id,
                    "fence_token": fence,
                    "reason": reason,
                }
            },
        )

    async def _reclaim_orphans(self) -> None:
        workflow_ids = await self.redis.smembers(ACTIVE_WORKFLOWS)
        for raw_id in workflow_ids:
            workflow_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            recovered = await self.wal.recover_latest_state(workflow_id)
            if recovered is None:
                continue
            workflow = recovered.workflow
            changed = False
            for task in workflow.tasks:
                if task.status != TaskStatus.RUNNING:
                    continue
                owner = await self.redis.get(f"{TASK_OWNER_PREFIX}{task.id}") or task.assigned_worker
                if owner and await self.heartbeat.is_alive(owner):
                    continue
                if owner and not await self.heartbeat.is_alive(owner):
                    await self._requeue_task(workflow, task.id, reason="orphan_dead_owner")
                    changed = True
                    continue
                dispatched_at = await self.redis.get(f"aether:dispatched:{task.id}")
                age = time.time() - float(dispatched_at) if dispatched_at else WORKER_TIMEOUT_S + 1
                if age <= WORKER_TIMEOUT_S:
                    continue
                await self._requeue_task(workflow, task.id, reason="orphan")
                changed = True
            if changed:
                recovered = await self.wal.recover_latest_state(workflow_id)
                if recovered:
                    await self.wal.checkpoint(
                        workflow_id, {"workflow": recovered.workflow.model_dump(mode="json")}
                    )
                    await self._dispatch_ready(recovered.workflow)

    async def consume_results(self) -> None:
        """Listen for worker result publications on Redis Pub/Sub."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("aether:results")
        try:
            while not self._stop.is_set():
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if not message or message.get("type") != "message":
                    continue
                data = message.get("data")
                if data is None:
                    continue
                if isinstance(data, bytes):
                    data = data.decode()
                try:
                    delta = json.loads(data)
                except json.JSONDecodeError:
                    continue
                await self.apply_task_update(delta)
        finally:
            await pubsub.unsubscribe("aether:results")
            await pubsub.aclose()
