from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Callable
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.exceptions import WatchError

from src.common.logger import get_logger
from src.common.models import DAGWorkflow, TaskStatus, WorkflowState
from src.common.redis_client import NotReplicatedError, confirm_replicated
from src.common.redis_client import replica_acks as _default_replica_acks
from src.control_plane.leader import LEADER_KEY, LeaderElection, NotLeaderError

log = get_logger("aether.wal")

OP_PREFIX = "aether:op:"
OP_RETENTION_S = 3600
WRITE_RETRY_S = 10.0


class StaleFenceError(Exception):
    """Raised when a zombie worker tries to commit with an outdated fence token."""


class ResultCommitted(Exception):
    """Raised when a task the control plane is about to reassign has already committed."""


def result_op(task_id: str, fence: int) -> str:
    """Op id of a task's result under one fence; at most one result per fence can land."""
    return f"result:{task_id}:{fence}"


class WriteAheadLog:
    """Redis-backed append-only WAL with snapshot + replay recovery.

    With `leader` set (the control plane), every write checks the leader token in the
    same transaction and raises NotLeaderError if this node has been deposed, and each
    delta records the leader's epoch. Workers pass no leader; their commits are guarded
    by the task's fence instead.

    With `replica_acks` (default from REDIS_REPLICA_ACKS), a transactional write returns
    only once that many replicas hold it, so a Redis failover cannot lose it afterwards.
    """

    def __init__(self, redis: Redis, leader: LeaderElection | None = None, replica_acks: int | None = None):
        self.redis = redis
        self.leader = leader
        self.replica_acks = _default_replica_acks() if replica_acks is None else replica_acks

    def _wal_key(self, workflow_id: str) -> str:
        return f"aether:wal:{workflow_id}"

    def _snap_key(self, workflow_id: str) -> str:
        return f"aether:snapshot:{workflow_id}"

    def _fence_key(self, task_id: str) -> str:
        return f"aether:fence:{task_id}"

    async def current_fence(self, task_id: str) -> int:
        raw = await self.redis.get(self._fence_key(task_id))
        return int(raw) if raw is not None else 0

    async def bump_fence(self, task_id: str, unless_result_at: int | None = None) -> int | None:
        """Issue the task's next fence, which makes every older envelope stale.

        With `unless_result_at`, issue nothing and return None if a result was already
        committed under that fence: reassigning a task and committing it exclude each other.
        """
        if self.leader is None and unless_result_at is None and not self.replica_acks:
            return int(await self.redis.incr(self._fence_key(task_id)))
        absent = [f"{OP_PREFIX}{result_op(task_id, unless_result_at)}"] if unless_result_at is not None else []
        try:
            applied = await self._transact([], lambda pipe: pipe.incr(self._fence_key(task_id)), absent=absent)
        except ResultCommitted:
            return None
        if applied is None:
            # An earlier attempt of this call landed; only the leader bumps fences, so it is still current.
            return await self.current_fence(task_id)
        return int(applied[0])

    async def append_delta(
        self,
        workflow_id: str,
        delta: dict[str, Any],
        also: Callable[[Pipeline], Any] | None = None,
        op_id: str | None = None,
        unless_committed: bool = False,
    ) -> int:
        """Append an immutable state delta. Rejects stale fence tokens.

        `also` queues extra commands into the same transaction, so they happen if and
        only if the delta is appended. Passing the same `op_id` again appends nothing
        if the first call landed; returns the delta's position, or 0 in that case.
        With `unless_committed`, raises ResultCommitted if a result already landed
        under the delta's fence.
        """
        task_id = delta.get("task_id")
        fence = delta.get("fence_token")
        if self.leader is not None:
            delta = {**delta, "leader_epoch": self.leader.epoch}
        payload = json.dumps(delta)
        wal_key = self._wal_key(workflow_id)
        op_id = op_id or uuid.uuid4().hex

        def append(pipe: Pipeline) -> None:
            pipe.rpush(wal_key, payload)
            if also is not None:
                also(pipe)

        try:
            if task_id is None or fence is None:
                if self.leader is None and also is None and not self.replica_acks:
                    applied: list[Any] | None = [await self.redis.rpush(wal_key, payload)]
                else:
                    applied = await self._transact([], append, op_id)
            else:
                absent = [f"{OP_PREFIX}{result_op(str(task_id), int(fence))}"] if unless_committed else []
                applied = await self._transact([(str(task_id), int(fence))], append, op_id, absent)
            seq = 0 if applied is None else int(applied[0])
            log.info(
                "wal.append",
                extra={"extra_fields": {"workflow_id": workflow_id, "seq": seq}},
            )
            return seq
        except (StaleFenceError, NotLeaderError, ResultCommitted):
            raise
        except Exception:
            log.exception("wal.append_failed", extra={"extra_fields": {"workflow_id": workflow_id}})
            raise

    async def _transact(
        self,
        fences: list[tuple[str, int]],
        queue: Callable[[Pipeline], Any],
        op_id: str | None = None,
        absent: list[str] = (),
    ) -> list[Any] | None:
        """Run `queue`'s commands in one MULTI, after checking each (task, fence) is current
        and, for a control plane, that it still holds the leader lease. WATCH aborts the
        transaction if any of those keys change between the check and EXEC.

        The fence must match exactly. An envelope's fence can only be ahead of Redis if a
        failover lost the dispatch that issued it, and that fence may be issued again.

        Connection errors are retried for up to WRITE_RETRY_S, which covers a Redis
        failover. redis-py reports a connection lost mid-transaction as a WatchError
        even if EXEC went through, so the transaction also records `op_id` (random if not
        given), and a retry that finds it returns None instead of applying it twice. With
        replica_acks, the write is then confirmed on replicas; one lost in a failover is
        redone. Raises ResultCommitted if any `absent` key exists.
        """
        op_key = f"{OP_PREFIX}{op_id or uuid.uuid4().hex}"
        watched = [self._fence_key(task_id) for task_id, _ in fences] + list(absent) + [op_key]
        if self.leader is not None:
            watched.append(LEADER_KEY)
        deadline = time.monotonic() + WRITE_RETRY_S
        attempt = 0
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(*watched)
                    if await pipe.exists(op_key):
                        await pipe.reset()
                        results = None
                    else:
                        if absent and await pipe.exists(*absent):
                            raise ResultCommitted(f"already committed: {list(absent)}")
                        if self.leader is not None:
                            holder = await pipe.get(LEADER_KEY)
                            if isinstance(holder, bytes):
                                holder = holder.decode()
                            if holder != self.leader.token:
                                raise NotLeaderError(f"leader is {holder!r}, not {self.leader.token!r}")
                        for task_id, fence in fences:
                            raw = await pipe.get(self._fence_key(task_id))
                            current = int(raw) if raw is not None else 0
                            if current != fence:
                                raise StaleFenceError(
                                    f"stale fence for {task_id}: got {fence}, current {current}"
                                )
                        pipe.multi()
                        queue(pipe)
                        pipe.set(op_key, "1", ex=OP_RETENTION_S)
                        results = (await pipe.execute())[:-1]
                    if self.replica_acks:
                        await confirm_replicated(self.redis, op_key, "1", self.replica_acks)
                    return results
                except (WatchError, RedisConnectionError, RedisTimeoutError, NotReplicatedError):
                    if time.monotonic() > deadline:
                        raise
                    attempt += 1
                    await asyncio.sleep(min(0.2, 0.005 * 2**attempt))

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
