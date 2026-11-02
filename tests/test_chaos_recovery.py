import asyncio
import json

import fakeredis
import pytest

from src.common.models import DAGWorkflow, TaskNode, TaskStatus
from src.control_plane.scheduler import MAX_RETRIES, Scheduler
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
    seq = await wal.append_delta(
        "chaos",
        {"type": "workflow_submitted", "workflow": workflow.model_dump(mode="json")},
    )
    await wal.checkpoint("chaos", {"workflow": workflow.model_dump(mode="json")}, wal_index=seq)
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


@pytest.mark.asyncio
async def test_checkpoint_keeps_commit_that_lands_after_recover(redis, wal):
    scheduler = Scheduler(redis)
    await scheduler.submit_workflow(_dag())
    recovered = await wal.recover_latest_state("chaos")
    root = recovered.workflow.task_map()["root"]

    await wal.append_delta(
        "chaos",
        {
            "type": "task_completed",
            "task_id": "root",
            "status": TaskStatus.COMPLETED.value,
            "fence_token": root.fence_token,
        },
    )
    await scheduler._checkpoint(recovered)

    after = await wal.recover_latest_state("chaos")
    assert after.workflow.task_map()["root"].status == TaskStatus.COMPLETED


async def _age_dispatch(redis, task_id: str) -> None:
    await redis.set(f"aether:dispatched:{task_id}", "0")


@pytest.mark.asyncio
async def test_orphan_reclaim_ignores_tasks_still_in_queue(redis, wal):
    scheduler = Scheduler(redis)
    await scheduler.submit_workflow(_dag())
    await _age_dispatch(redis, "root")

    await scheduler._reclaim_orphans()

    root = (await wal.recover_latest_state("chaos")).workflow.task_map()["root"]
    assert root.retry_count == 0
    assert root.fence_token == 1
    assert await redis.llen("aether:queue:tasks") == 1


@pytest.mark.asyncio
async def test_orphan_reclaim_requeues_lost_envelope(redis, wal):
    scheduler = Scheduler(redis)
    await scheduler.submit_workflow(_dag())
    await redis.rpop("aether:queue:tasks")
    await _age_dispatch(redis, "root")

    await scheduler._reclaim_orphans()

    root = (await wal.recover_latest_state("chaos")).workflow.task_map()["root"]
    assert root.retry_count == 1
    envelopes = [json.loads(e) for e in await redis.lrange("aether:queue:tasks", 0, -1)]
    assert [(e["task_id"], e["fence_token"]) for e in envelopes] == [("root", root.fence_token)]


async def _claim(redis, wal, workflow_id: str, task_id: str, worker_id: str) -> None:
    task = (await wal.recover_latest_state(workflow_id)).workflow.task_map()[task_id]
    await redis.set(f"aether:task_owner:{task_id}", worker_id)
    await wal.append_delta(
        workflow_id,
        {
            "type": "task_started",
            "task_id": task_id,
            "status": TaskStatus.RUNNING.value,
            "fence_token": task.fence_token,
            "assigned_worker": worker_id,
        },
    )


@pytest.mark.asyncio
async def test_concurrent_recovery_paths_reassign_a_task_once(redis, wal):
    scheduler = Scheduler(redis)
    await scheduler.submit_workflow(_dag())
    await redis.rpop("aether:queue:tasks")
    await _claim(redis, wal, "chaos", "root", "worker-1")

    # fakeredis never yields to the event loop; widen the read-then-bump window like real I/O does.
    bump_fence = scheduler.wal.bump_fence

    async def slow_bump_fence(task_id: str) -> int:
        await asyncio.sleep(0.01)
        return await bump_fence(task_id)

    scheduler.wal.bump_fence = slow_bump_fence

    await asyncio.gather(scheduler.on_worker_failure("worker-1"), scheduler._reclaim_orphans())

    root = (await wal.recover_latest_state("chaos")).workflow.task_map()["root"]
    assert root.retry_count == 1
    envelopes = [json.loads(e) for e in await redis.lrange("aether:queue:tasks", 0, -1)]
    assert [(e["task_id"], e["fence_token"]) for e in envelopes] == [("root", root.fence_token)]


@pytest.mark.asyncio
async def test_client_submission_leaves_dispatch_to_the_control_plane(redis, wal):
    client = Scheduler(redis)
    await client.submit_workflow(_dag(), dispatch=False)
    assert await redis.llen("aether:queue:tasks") == 0

    control_plane = Scheduler(redis)
    await control_plane.apply_task_update({"type": "workflow_submitted", "workflow_id": "chaos"})

    envelopes = [json.loads(e) for e in await redis.lrange("aether:queue:tasks", 0, -1)]
    assert [e["task_id"] for e in envelopes] == ["root"]


@pytest.mark.asyncio
async def test_retried_task_jumps_the_queue(redis, wal):
    scheduler = Scheduler(redis)
    await scheduler.submit_workflow(
        DAGWorkflow(
            workflow_id="backlog",
            tasks=[TaskNode(id=name, agent_prompt=name) for name in ("a", "b", "c")],
        )
    )
    first = json.loads(await redis.rpop("aether:queue:tasks"))
    await _claim(redis, wal, "backlog", first["task_id"], "worker-1")

    await scheduler.on_worker_failure("worker-1")

    retried = json.loads(await redis.rpop("aether:queue:tasks"))
    assert retried["task_id"] == first["task_id"]
    assert retried["fence_token"] > first["fence_token"]


@pytest.mark.asyncio
async def test_task_fails_after_max_retries(redis, wal):
    scheduler = Scheduler(redis)
    await scheduler.submit_workflow(_dag())

    for attempt in range(MAX_RETRIES + 1):
        await _claim(redis, wal, "chaos", "root", f"worker-{attempt}")
        await scheduler.on_worker_failure(f"worker-{attempt}")

    workflow = (await wal.recover_latest_state("chaos")).workflow
    root = workflow.task_map()["root"]
    assert root.status == TaskStatus.FAILED
    assert root.retry_count == MAX_RETRIES + 1
    assert workflow.has_failed()
    assert not await redis.sismember("aether:workflows:active", "chaos")
    envelopes = [json.loads(e) for e in await redis.lrange("aether:queue:tasks", 0, -1)]
    assert all(e["fence_token"] < root.fence_token for e in envelopes)
