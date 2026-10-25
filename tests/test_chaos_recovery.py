import json

import fakeredis
import pytest

from src.common.models import DAGWorkflow, TaskNode, TaskStatus
from src.control_plane.scheduler import Scheduler
from src.control_plane.wal import StaleFenceError, WriteAheadLog


@pytest.fixture
async def redis():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


@pytest.fixture
async def wal(redis):
    return WriteAheadLog(redis)


def _dag() -> DAGWorkflow:
    return DAGWorkflow(
        workflow_id="chaos",
        tasks=[
            TaskNode(id="root", agent_prompt="root"),
            TaskNode(id="leaf", agent_prompt="leaf", dependencies=["root"]),
        ],
    )


@pytest.mark.asyncio
async def test_wal_replays_deltas_after_snapshot(wal):
    workflow = _dag()
    await wal.append_delta(
        "chaos",
        {"type": "workflow_submitted", "workflow": workflow.model_dump(mode="json")},
    )
    await wal.checkpoint("chaos", {"workflow": workflow.model_dump(mode="json")})
    fence = await wal.bump_fence("root")
    await wal.append_delta(
        "chaos",
        {
            "type": "task_completed",
            "task_id": "root",
            "status": TaskStatus.COMPLETED.value,
            "fence_token": fence,
            "result": {"ok": True},
        },
    )
    recovered = await wal.recover_latest_state("chaos")
    assert recovered is not None
    assert recovered.workflow.task_map()["root"].status == TaskStatus.COMPLETED
    assert recovered.workflow.task_map()["root"].result == {"ok": True}
    assert [t.id for t in recovered.workflow.ready_tasks()] == ["leaf"]


@pytest.mark.asyncio
async def test_stale_fence_is_rejected(wal):
    await wal.bump_fence("root")
    await wal.bump_fence("root")
    with pytest.raises(StaleFenceError):
        await wal.append_delta(
            "chaos",
            {
                "type": "task_completed",
                "task_id": "root",
                "status": TaskStatus.COMPLETED.value,
                "fence_token": 1,
            },
        )


@pytest.mark.asyncio
async def test_worker_failure_reassigns_running_task(redis, wal):
    scheduler = Scheduler(redis)
    workflow = _dag()
    await scheduler.submit_workflow(workflow)

    recovered = await wal.recover_latest_state("chaos")
    assert recovered is not None
    root = recovered.workflow.task_map()["root"]
    assert root.status == TaskStatus.RUNNING

    await redis.set("aether:task_owner:root", "worker-1")
    await wal.append_delta(
        "chaos",
        {
            "type": "task_started",
            "task_id": "root",
            "status": TaskStatus.RUNNING.value,
            "fence_token": root.fence_token,
            "assigned_worker": "worker-1",
        },
    )

    await scheduler.on_worker_failure("worker-1")
    recovered = await wal.recover_latest_state("chaos")
    assert recovered is not None
    root = recovered.workflow.task_map()["root"]
    # requeue then immediate redispatch
    assert root.status == TaskStatus.RUNNING
    assert root.assigned_worker is None
    assert root.retry_count == 1
    assert root.fence_token > 1

    queued = await redis.lrange("aether:queue:tasks", 0, -1)
    envelopes = [json.loads(item) for item in queued]
    assert any(env["task_id"] == "root" and env["fence_token"] == root.fence_token for env in envelopes)
