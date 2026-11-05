import asyncio
import json

import fakeredis
import pytest

from src.common.config import Settings
from src.common.models import DAGWorkflow, TaskNode, TaskStatus
from src.control_plane.leader import LEADER_KEY, LeaderElection, NotLeaderError
from src.control_plane.scheduler import TASK_QUEUE, Scheduler
from src.control_plane.wal import WriteAheadLog

SETTINGS = Settings(leader_lease_s=0.4)


@pytest.fixture
async def redis():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


def _dag() -> DAGWorkflow:
    return DAGWorkflow(workflow_id="ha", tasks=[TaskNode(id="root", agent_prompt="root")])


async def _deposed_leader(redis) -> LeaderElection:
    old = LeaderElection(redis, "cp-1", SETTINGS)
    await old.campaign()
    await redis.delete(LEADER_KEY)
    await LeaderElection(redis, "cp-2", SETTINGS).campaign()
    return old


async def test_one_leader_at_a_time_and_epochs_increase(redis):
    first = LeaderElection(redis, "cp-1", SETTINGS)
    second = LeaderElection(redis, "cp-2", SETTINGS)
    assert await first.campaign() == "cp-1:1"

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(second.campaign(), timeout=0.3)

    await redis.delete(LEADER_KEY)
    assert await asyncio.wait_for(second.campaign(), timeout=1) == "cp-2:2"
    assert not await first.renew()
    assert await second.renew()


async def test_hold_returns_once_the_lease_is_taken(redis):
    leader = LeaderElection(redis, "cp-1", SETTINGS)
    await leader.campaign()
    holding = asyncio.create_task(leader.hold())
    await asyncio.sleep(0.15)
    assert not holding.done()

    await redis.set(LEADER_KEY, "cp-2:9")
    await asyncio.wait_for(holding, timeout=1)


async def test_resign_only_releases_its_own_lease(redis):
    old = await _deposed_leader(redis)
    await old.resign()
    assert await redis.get(LEADER_KEY) == "cp-2:2"


async def test_deposed_leader_cannot_write(redis):
    old = await _deposed_leader(redis)
    wal = WriteAheadLog(redis, old)

    with pytest.raises(NotLeaderError):
        await wal.bump_fence("root")
    with pytest.raises(NotLeaderError):
        await wal.append_delta("ha", {"type": "task_dispatched", "task_id": "root", "fence_token": 1})
    with pytest.raises(NotLeaderError):
        await wal.checkpoint("ha", {"workflow": _dag().model_dump(mode="json")}, wal_index=0)

    assert await redis.get("aether:fence:root") is None
    assert await redis.llen("aether:wal:ha") == 0
    assert await redis.get("aether:snapshot:ha") is None


async def test_leader_dispatch_is_atomic_and_records_the_epoch(redis):
    leader = LeaderElection(redis, "cp-1", SETTINGS)
    await leader.campaign()
    await Scheduler(redis, SETTINGS).submit_workflow(_dag(), dispatch=False)

    scheduler = Scheduler(redis, SETTINGS, leader=leader)
    await scheduler.apply_task_update({"workflow_id": "ha"})

    deltas = [json.loads(d) for d in await redis.lrange("aether:wal:ha", 0, -1)]
    dispatched = [d for d in deltas if d["type"] == "task_dispatched"]
    assert [(d["task_id"], d["leader_epoch"]) for d in dispatched] == [("root", 1)]
    (envelope,) = await redis.lrange(TASK_QUEUE, 0, -1)
    assert json.loads(envelope)["fence_token"] == dispatched[0]["fence_token"]


async def test_deposed_scheduler_steps_down_without_dispatching(redis):
    await Scheduler(redis, SETTINGS).submit_workflow(_dag(), dispatch=False)
    old = await _deposed_leader(redis)
    scheduler = Scheduler(redis, SETTINGS, leader=old)

    with pytest.raises(NotLeaderError):
        await scheduler.apply_task_update({"workflow_id": "ha"})

    assert await redis.llen(TASK_QUEUE) == 0
    state = await WriteAheadLog(redis).recover_latest_state("ha")
    assert state.workflow.task_map()["root"].status == TaskStatus.PENDING
