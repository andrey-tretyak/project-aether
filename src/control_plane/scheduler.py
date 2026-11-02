from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from redis.asyncio import Redis

from src.common.logger import get_logger
from src.common.models import DAGWorkflow, TaskStatus, WorkflowState
from src.common.config import Settings, load_settings
from src.control_plane.heartbeat import HeartbeatMonitor
from src.control_plane.wal import WriteAheadLog

log = get_logger("aether.scheduler")

TASK_QUEUE = "aether:queue:tasks"
RESULTS_CHANNEL = "aether:results"
ACTIVE_WORKFLOWS = "aether:workflows:active"
TASK_OWNER_PREFIX = "aether:task_owner:"
MAX_RETRIES = 3
RECONCILE_INTERVAL_S = 1.0


class Scheduler:
    """DAG resolver and state machine. Dispatches ready tasks onto the Redis queue.

    The control plane is the only dispatcher, and every state transition it makes
    (worker result, dead worker, orphan reclaim, reconcile) runs under one lock, so
    each transition starts from the latest WAL state and two of them never race to
    bump the same task's fence.
    """

    def __init__(self, redis: Redis, settings: Settings | None = None):
        self.redis = redis
        self.settings = settings or load_settings()
        self.wal = WriteAheadLog(redis)
        self.heartbeat = HeartbeatMonitor(redis, self.on_worker_failure, self.settings)
        self._transitions = asyncio.Lock()
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._stop.clear()
        await self.heartbeat.start()
        self._reconcile_task = asyncio.create_task(self._reconcile_loop(), name="reconcile")
        log.info("control_plane.started")

    async def stop(self) -> None:
        self._stop.set()
        await self.heartbeat.stop()
        task = getattr(self, "_reconcile_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _reconcile_loop(self) -> None:
        """Reclaim orphans and dispatch anything ready that no event has dispatched yet.

        Also how the control plane catches up after a restart or a missed wake-up message.
        """
        while not self._stop.is_set():
            try:
                await self._reclaim_orphans()
                for workflow_id in await self._active_workflow_ids():
                    async with self._transitions:
                        await self._advance(workflow_id)
            except Exception:
                log.exception("scheduler.reconcile_failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=RECONCILE_INTERVAL_S)
            except asyncio.TimeoutError:
                continue

    async def _active_workflow_ids(self) -> list[str]:
        return [
            raw.decode() if isinstance(raw, bytes) else str(raw)
            for raw in await self.redis.smembers(ACTIVE_WORKFLOWS)
        ]

    async def submit_workflow(self, workflow: DAGWorkflow, dispatch: bool = True) -> None:
        """Record a workflow. With dispatch=False (clients outside the control plane),
        only wake the control plane, which then dispatches."""
        missing = workflow.missing_dependencies()
        if missing:
            raise ValueError(f"unknown dependencies: {missing}")
        if workflow.has_cycle():
            raise ValueError(f"cycle detected in workflow {workflow.workflow_id}")

        seq = await self.wal.append_delta(
            workflow.workflow_id,
            {"type": "workflow_submitted", "workflow": workflow.model_dump(mode="json")},
        )
        await self.wal.checkpoint(
            workflow.workflow_id, {"workflow": workflow.model_dump(mode="json")}, wal_index=seq
        )
        await self.redis.sadd(ACTIVE_WORKFLOWS, workflow.workflow_id)
        if dispatch:
            async with self._transitions:
                await self._advance(workflow.workflow_id)
        else:
            await self.redis.publish(
                RESULTS_CHANNEL,
                json.dumps({"type": "workflow_submitted", "workflow_id": workflow.workflow_id}),
            )

    async def _checkpoint(self, recovered: WorkflowState) -> None:
        await self.wal.checkpoint(
            recovered.workflow.workflow_id,
            {"workflow": recovered.workflow.model_dump(mode="json")},
            wal_index=recovered.wal_seq,
        )

    async def _advance(self, workflow_id: str) -> None:
        """Recover, checkpoint, then either retire the workflow or dispatch newly ready tasks."""
        recovered = await self.wal.recover_latest_state(workflow_id)
        if recovered is None:
            return
        workflow = recovered.workflow
        await self._checkpoint(recovered)

        if workflow.is_complete() or workflow.has_failed():
            if await self.redis.srem(ACTIVE_WORKFLOWS, workflow_id):
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

    async def apply_task_update(self, delta: dict[str, Any]) -> None:
        """Wake after a worker WAL commit or a client submission."""
        async with self._transitions:
            await self._advance(str(delta["workflow_id"]))

    async def on_worker_failure(self, worker_id: str) -> None:
        """Reassign RUNNING tasks owned by a dead worker with a new fence token."""
        async with self._transitions:
            for workflow_id in await self._active_workflow_ids():
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
                    await self._advance(workflow_id)

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
            # Workers BRPOP from the right, so RPUSH puts a retry at the head of the line.
            if task.retry_count > 0:
                await self.redis.rpush(TASK_QUEUE, json.dumps(envelope))
            else:
                await self.redis.lpush(TASK_QUEUE, json.dumps(envelope))
            log.info(
                "task.dispatched",
                extra={
                    "extra_fields": {
                        "workflow_id": workflow.workflow_id,
                        "task_id": task.id,
                        "fence_token": fence,
                        "retry_count": task.retry_count,
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
        retries = task.retry_count + 1
        exhausted = retries > MAX_RETRIES
        delta = {
            "type": "task_failed" if exhausted else "task_reassigned",
            "workflow_id": workflow.workflow_id,
            "task_id": task.id,
            "status": (TaskStatus.FAILED if exhausted else TaskStatus.PENDING).value,
            "fence_token": fence,
            "assigned_worker": None,
            "retry_count": retries,
        }
        if exhausted:
            delta["error"] = f"gave up after {MAX_RETRIES} retries (last: {reason})"
        await self.wal.append_delta(workflow.workflow_id, delta)
        await self.redis.delete(f"{TASK_OWNER_PREFIX}{task.id}")
        await self.redis.delete(f"aether:lock:{task.id}")
        log.info(
            "task.retries_exhausted" if exhausted else "task.reassigned",
            extra={
                "extra_fields": {
                    "workflow_id": workflow.workflow_id,
                    "task_id": task.id,
                    "dead_worker": worker_id,
                    "fence_token": fence,
                    "retry_count": retries,
                    "reason": reason,
                }
            },
        )

    async def _queued_envelopes(self) -> set[tuple[str, int]]:
        queued: set[tuple[str, int]] = set()
        for raw in await self.redis.lrange(TASK_QUEUE, 0, -1):
            try:
                env = json.loads(raw)
                queued.add((str(env["task_id"]), int(env["fence_token"])))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
        return queued

    async def _reclaim_orphans(self) -> None:
        """Requeue RUNNING tasks that no live worker holds and that are no longer waiting in the queue."""
        async with self._transitions:
            workflow_ids = await self._active_workflow_ids()
            if not workflow_ids:
                return
            queued = await self._queued_envelopes()
            for workflow_id in workflow_ids:
                recovered = await self.wal.recover_latest_state(workflow_id)
                if recovered is None:
                    continue
                workflow = recovered.workflow
                changed = False
                for task in workflow.tasks:
                    if task.status != TaskStatus.RUNNING:
                        continue
                    if (task.id, task.fence_token) in queued:
                        continue
                    owner = await self.redis.get(f"{TASK_OWNER_PREFIX}{task.id}") or task.assigned_worker
                    if owner:
                        if await self.heartbeat.is_alive(owner):
                            continue
                        await self._requeue_task(workflow, task.id, reason="orphan_dead_owner")
                        changed = True
                        continue
                    if await self.redis.exists(f"aether:lock:{task.id}"):
                        continue
                    dispatched_at = await self.redis.get(f"aether:dispatched:{task.id}")
                    timeout = self.settings.lease_s
                    age = time.time() - float(dispatched_at) if dispatched_at else timeout + 1
                    if age <= timeout:
                        continue
                    await self._requeue_task(workflow, task.id, reason="orphan_lost_envelope")
                    changed = True
                if changed:
                    await self._advance(workflow_id)

    async def consume_results(self) -> None:
        """Listen for worker results and client submissions on Redis Pub/Sub."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(RESULTS_CHANNEL)
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
                try:
                    await self.apply_task_update(delta)
                except Exception:
                    log.exception(
                        "scheduler.apply_failed",
                        extra={"extra_fields": {"workflow_id": delta.get("workflow_id")}},
                    )
        finally:
            await pubsub.unsubscribe(RESULTS_CHANNEL)
            await pubsub.aclose()
