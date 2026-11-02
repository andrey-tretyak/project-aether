from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import WatchError

from src.common.logger import get_logger
from src.common.models import DAGWorkflow, TaskStatus, WorkflowState
from src.control_plane.leader import LEADER_KEY, LeaderElection, NotLeaderError

log = get_logger("aether.wal")


class StaleFenceError(Exception):
    """Raised when a zombie worker tries to commit with an outdated fence token."""


class WriteAheadLog:
    """Redis-backed append-only WAL with snapshot + replay recovery.

    With `leader` set (the control plane), every write checks the leader token in the
    same transaction and raises NotLeaderError if this node has been deposed, and each
    delta records the leader's epoch. Workers pass no leader; their commits are guarded
    by the task's fence instead.
    """

    def __init__(self, redis: Redis, leader: LeaderElection | None = None):
        self.redis = redis
        self.leader = leader

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
        if self.leader is None:
            return int(await self.redis.incr(self._fence_key(task_id)))
        (fence,) = await self._transact([], lambda pipe: pipe.incr(self._fence_key(task_id)))
        return int(fence)

    async def append_delta(
        self,
        workflow_id: str,
        delta: dict[str, Any],
        also: Callable[[Pipeline], Any] | None = None,
    ) -> int:
        """Append an immutable state delta. Rejects stale fence tokens.

        `also` queues extra commands into the same transaction, so they happen if and
        only if the delta is appended.
        """
        task_id = delta.get("task_id")
        fence = delta.get("fence_token")
        if self.leader is not None:
            delta = {**delta, "leader_epoch": self.leader.epoch}
        payload = json.dumps(delta)
        wal_key = self._wal_key(workflow_id)

        def append(pipe: Pipeline) -> None:
            pipe.rpush(wal_key, payload)
            if also is not None:
                also(pipe)

        try:
            if task_id is None or fence is None:
                if self.leader is None and also is None:
                    seq = await self.redis.rpush(wal_key, payload)
                else:
                    seq, *_ = await self._transact([], append)
            else:
                seq, *_ = await self._transact([(str(task_id), int(fence))], append)
            log.info(
                "wal.append",
                extra={"extra_fields": {"workflow_id": workflow_id, "seq": seq}},
            )
            return int(seq)
        except (StaleFenceError, NotLeaderError):
            raise
        except Exception:
            log.exception("wal.append_failed", extra={"extra_fields": {"workflow_id": workflow_id}})
            raise

    async def _transact(
        self, fences: list[tuple[str, int]], queue: Callable[[Pipeline], Any]
    ) -> list[Any]:
        """Run `queue`'s commands in one MULTI, after checking each (task, fence) is current
        and, for a control plane, that it still holds the leader lease. WATCH aborts the
        transaction if any of those keys change between the check and EXEC."""
        watched = [self._fence_key(task_id) for task_id, _ in fences]
        if self.leader is not None:
            watched.append(LEADER_KEY)
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    if watched:
                        await pipe.watch(*watched)
                    for task_id, fence in fences:
                        raw = await pipe.get(self._fence_key(task_id))
                        current = int(raw) if raw is not None else 0
                        if current > fence:
                            raise StaleFenceError(
                                f"stale fence for {task_id}: got {fence}, current {current}"
                            )
                    if self.leader is not None:
                        holder = await pipe.get(LEADER_KEY)
                        if isinstance(holder, bytes):
                            holder = holder.decode()
                        if holder != self.leader.token:
                            raise NotLeaderError(f"leader is {holder!r}, not {self.leader.token!r}")
                    pipe.multi()
                    queue(pipe)
                    return await pipe.execute()
                except WatchError:
                    continue

    async def checkpoint(self, workflow_id: str, state_snapshot: dict[str, Any], wal_index: int) -> None:
        """Persist a snapshot. `wal_index` must be the number of WAL entries the snapshot reflects."""
        try:
            payload = json.dumps({"wal_index": wal_index, "state": state_snapshot})
            if self.leader is None:
                await self.redis.set(self._snap_key(workflow_id), payload)
            else:
                await self._transact([], lambda pipe: pipe.set(self._snap_key(workflow_id), payload))
            log.info(
                "wal.checkpoint",
                extra={"extra_fields": {"workflow_id": workflow_id, "wal_index": wal_index}},
            )
        except NotLeaderError:
            raise
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

            wal_seq = start + len(entries)
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
