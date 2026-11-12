import json

import fakeredis
import pytest

from src.common.models import DAGWorkflow, TaskNode, TaskStatus
from src.control_plane.audit import audit_wal
from src.control_plane.scheduler import Scheduler
from src.control_plane.wal import StaleFenceError, WriteAheadLog

DEPENDENCIES = {"a": [], "b": ["a"]}


async def _write(wal, kind: str, task_id: str, fence: int, worker: str) -> None:
    status = TaskStatus.COMPLETED if kind == "task_completed" else TaskStatus.RUNNING
    await wal.append_delta(
        "audit",
        {"type": kind, "task_id": task_id, "status": status.value, "fence_token": fence, "assigned_worker": worker},
    )


@pytest.fixture
async def history() -> list[dict]:
    """A real run of a -> b in which the worker holding `a` is declared dead and later wakes up."""
    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    wal = WriteAheadLog(redis)
    scheduler = Scheduler(redis)
    workflow = DAGWorkflow(
        workflow_id="audit",
        tasks=[TaskNode(id="a", agent_prompt="a"), TaskNode(id="b", agent_prompt="b", dependencies=["a"])],
    )
    await scheduler.submit_workflow(workflow)

    await redis.set("aether:task_owner:a", "worker-1")
    await _write(wal, "task_started", "a", 1, "worker-1")
    await scheduler.on_worker_failure("worker-1")
    fence = await wal.current_fence("a")
    await _write(wal, "task_started", "a", fence, "worker-2")
    await _write(wal, "task_completed", "a", fence, "worker-2")
    with pytest.raises(StaleFenceError):
        await _write(wal, "task_completed", "a", 1, "worker-1")

    await scheduler._advance("audit")
    fence = await wal.current_fence("b")
    await _write(wal, "task_started", "b", fence, "worker-2")
    await _write(wal, "task_completed", "b", fence, "worker-2")
    return [json.loads(raw) for raw in await redis.lrange("aether:wal:audit", 0, -1)]


def _index(history: list[dict], kind: str, task_id: str) -> int:
    return next(i for i, d in enumerate(history) if d.get("type") == kind and d.get("task_id") == task_id)


def test_a_run_through_a_worker_failure_passes(history):
    kinds = [d["type"] for d in history]
    assert "task_reassigned" in kinds
    assert audit_wal(history, DEPENDENCIES) == []


def test_a_second_commit_is_reported(history):
    history.append(history[_index(history, "task_completed", "b")])
    assert any("already committed" in v for v in audit_wal(history, DEPENDENCIES))
    assert any("b: committed 2 times" in v for v in audit_wal(history, DEPENDENCIES))


def test_a_commit_under_a_superseded_fence_is_reported(history):
    zombie = {**history[_index(history, "task_started", "a")], "type": "task_completed"}
    history.insert(_index(history, "task_completed", "a"), zombie)
    assert any("is not the current fence" in v for v in audit_wal(history, DEPENDENCIES))


def test_dispatch_before_a_dependency_commits_is_reported(history):
    dispatch = history.pop(_index(history, "task_dispatched", "b"))
    history.insert(_index(history, "task_completed", "a"), dispatch)
    assert any("before its dependencies committed: ['a']" in v for v in audit_wal(history, DEPENDENCIES))


def test_a_fence_that_does_not_advance_is_reported(history):
    reassign = history[_index(history, "task_reassigned", "a")]
    reassign["fence_token"] = 1
    assert any("does not advance" in v for v in audit_wal(history, DEPENDENCIES))


def test_a_deposed_leader_writing_after_its_successor_is_reported(history):
    for delta in history:
        if delta["type"] in ("task_dispatched", "task_reassigned"):
            delta["leader_epoch"] = 2
    history[_index(history, "task_dispatched", "b")]["leader_epoch"] = 1
    assert any("leader epoch 1 after epoch 2" in v for v in audit_wal(history, DEPENDENCIES))


def test_a_task_that_never_committed_is_reported(history):
    history = history[: _index(history, "task_completed", "b")]
    assert audit_wal(history, DEPENDENCIES) == ["b: committed 0 times, expected exactly once"]
