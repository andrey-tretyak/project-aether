from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

from src.common.logger import get_logger
from src.common.models import DAGWorkflow, TaskStatus, WorkflowState

log = get_logger("aether.wal")


class StaleFenceError(Exception):
    """Raised when a zombie worker tries to commit with an outdated fence token."""


class WriteAheadLog:
    """Redis-backed append-only WAL with snapshot + replay recovery."""

    def __init__(self, redis: Redis):
        self.redis = redis

    def _wal_key(self, workflow_id: str) -> str:
        return f"aether:wal:{workflow_id}"

    def _snap_key(self, workflow_id: str) -> str:
        return f"aether:snapshot:{workflow_id}"

    def _fence_key(self, task_id: str) -> str:
        return f"aether:fence:{task_id}"

    async def current_fence(self, task_id: str) -> int:
        raw = await self.redis.get(self._fence_key(task_id))
        return int(raw) if raw is not None else 0

    async def bump_fence(self, task_id: str) -> int:
        return int(await self.redis.incr(self._fence_key(task_id)))

    async def append_delta(self, workflow_id: str, delta: dict[str, Any]) -> int:
        """Append an immutable state delta. Rejects stale fence tokens."""
        task_id = delta.get("task_id")
        fence = delta.get("fence_token")
        if task_id is not None and fence is not None:
            current = await self.current_fence(str(task_id))
            if current > int(fence):
                raise StaleFenceError(
                    f"stale fence for {task_id}: got {fence}, current {current}"
                )
        try:
            seq = await self.redis.rpush(self._wal_key(workflow_id), json.dumps(delta))
            log.info(
                "wal.append",
                extra={"extra_fields": {"workflow_id": workflow_id, "seq": seq}},
            )
            return int(seq)
        except Exception:
            log.exception("wal.append_failed", extra={"extra_fields": {"workflow_id": workflow_id}})
            raise

    async def checkpoint(self, workflow_id: str, state_snapshot: dict[str, Any]) -> None:
        try:
            wal_len = int(await self.redis.llen(self._wal_key(workflow_id)))
            payload = json.dumps({"wal_index": wal_len, "state": state_snapshot})
            await self.redis.set(self._snap_key(workflow_id), payload)
            log.info(
                "wal.checkpoint",
                extra={"extra_fields": {"workflow_id": workflow_id, "wal_index": wal_len}},
            )
        except Exception:
            log.exception("wal.checkpoint_failed", extra={"extra_fields": {"workflow_id": workflow_id}})
            raise

    async def recover_latest_state(self, workflow_id: str) -> WorkflowState | None:
        """Load the latest snapshot and replay WAL deltas after it."""
        try:
            raw_snap = await self.redis.get(self._snap_key(workflow_id))
            start = 0
            state: dict[str, Any] | None = None
            if raw_snap:
                parsed = json.loads(raw_snap)
                start = int(parsed.get("wal_index", 0))
                state = parsed.get("state")

            entries = await self.redis.lrange(self._wal_key(workflow_id), start, -1)
            if state is None and not entries:
                return None

            if state is None:
                workflow = _workflow_from_first_delta(entries)
            else:
                workflow = DAGWorkflow.model_validate(state["workflow"] if "workflow" in state else state)

            for raw in entries:
                delta = json.loads(raw)
                _apply_delta(workflow, delta)

            wal_seq = int(await self.redis.llen(self._wal_key(workflow_id)))
            recovered = WorkflowState(workflow=workflow, wal_seq=wal_seq)
            log.info(
                "Recovering state",
                extra={
                    "extra_fields": {
                        "workflow_id": workflow_id,
                        "wal_seq": wal_seq,
                        "replayed": len(entries),
                    }
                },
            )
            return recovered
        except Exception:
            log.exception("wal.recover_failed", extra={"extra_fields": {"workflow_id": workflow_id}})
            raise


def _workflow_from_first_delta(entries: list[Any]) -> DAGWorkflow:
    for raw in entries:
        delta = json.loads(raw)
        if delta.get("type") == "workflow_submitted":
            return DAGWorkflow.model_validate(delta["workflow"])
    raise ValueError("WAL has no workflow_submitted delta and no snapshot")


def _apply_delta(workflow: DAGWorkflow, delta: dict[str, Any]) -> None:
    kind = delta.get("type")
    if kind == "workflow_submitted":
        return
    task_id = delta.get("task_id")
    if not task_id:
        return
    by_id = workflow.task_map()
    task = by_id.get(task_id)
    if task is None:
        return
    if "status" in delta:
        task.status = TaskStatus(delta["status"])
    if "fence_token" in delta:
        task.fence_token = int(delta["fence_token"])
    if "assigned_worker" in delta:
        task.assigned_worker = delta["assigned_worker"]
    if "result" in delta:
        task.result = delta["result"]
    if "error" in delta:
        task.error = delta["error"]
    if "retry_count" in delta:
        task.retry_count = int(delta["retry_count"])
