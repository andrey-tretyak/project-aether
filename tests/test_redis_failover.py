import asyncio
import json

import fakeredis
import pytest
from redis.asyncio.sentinel import SentinelConnectionPool

from src.common import redis_client
from src.common.config import Settings
from src.common.models import DAGWorkflow, TaskNode, TaskStatus
from src.common.redis_client import NotReplicatedError
from src.control_plane import leader as leader_module
from src.control_plane import wal as wal_module
from src.control_plane.heartbeat import HeartbeatMonitor
from src.control_plane.leader import LeaderElection
from src.control_plane.scheduler import TASK_QUEUE, Scheduler
from src.control_plane.wal import ResultCommitted, StaleFenceError, WriteAheadLog, result_op

SETTINGS = Settings(leader_lease_s=0.4, lease_s=0.2, heartbeat_interval_s=0.05)


@pytest.fixture
async def redis():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


def _completed(fence: int) -> dict:
    return {"type": "task_completed", "task_id": "root", "status": TaskStatus.COMPLETED.value, "fence_token": fence}


async def test_a_repeated_op_id_appends_once(redis):
    wal = WriteAheadLog(redis)
    fence = await wal.bump_fence("root")

    first = await wal.append_delta("wf", _completed(fence), op_id=result_op("root", fence))
    second = await wal.append_delta("wf", _completed(fence), op_id=result_op("root", fence))

    assert (first, second) == (1, 0)
    assert await redis.llen("aether:wal:wf") == 1


async def test_a_fence_ahead_of_redis_is_rejected(redis):
    """An envelope can be ahead of Redis only if a failover lost the dispatch that issued it."""
    wal = WriteAheadLog(redis)
    await wal.bump_fence("root")

    with pytest.raises(StaleFenceError):
        await wal.append_delta("wf", _completed(2))


async def test_reassigning_a_committed_task_issues_no_fence(redis):
    wal = WriteAheadLog(redis)
    fence = await wal.bump_fence("root")
    await wal.append_delta("wf", _completed(fence), op_id=result_op("root", fence))

    assert await wal.bump_fence("root", unless_result_at=fence) is None
    assert await wal.current_fence("root") == fence


async def test_a_worker_cannot_start_a_fence_that_already_committed(redis):
    wal = WriteAheadLog(redis)
    fence = await wal.bump_fence("root")
    await wal.append_delta("wf", _completed(fence), op_id=result_op("root", fence))

    started = {"type": "task_started", "task_id": "root", "status": TaskStatus.RUNNING.value, "fence_token": fence}
    with pytest.raises(ResultCommitted):
        await wal.append_delta("wf", started, unless_committed=True)
    assert await redis.llen("aether:wal:wf") == 1


async def test_a_write_lost_in_a_failover_is_redone_once(redis, monkeypatch):
    """The primary takes the write, then dies before its replica has it; the retry lands it again."""
    wal = WriteAheadLog(redis, replica_acks=1)
    calls = []

    async def failover_after_first_write(client, key, value, replicas):
        calls.append(key)
        if len(calls) == 1:
            await client.flushall()
            raise NotReplicatedError(f"{key} is not on the current primary")

    monkeypatch.setattr(wal_module, "confirm_replicated", failover_after_first_write)
    seq = await wal.append_delta("wf", {"type": "workflow_note"}, op_id="note-1")

    assert seq == 1
    assert len(calls) == 2
    assert await redis.lrange("aether:wal:wf", 0, -1) == [json.dumps({"type": "workflow_note"})]


async def test_an_unreplicated_write_is_retried_without_applying_it_twice(redis, monkeypatch):
    wal = WriteAheadLog(redis, replica_acks=1)
    calls = []

    async def lagging_replica(client, key, value, replicas):
        calls.append(key)
        if len(calls) < 3:
            raise NotReplicatedError(f"{key} reached 0 of 1 replicas")

    monkeypatch.setattr(wal_module, "confirm_replicated", lagging_replica)
    fence = await wal.bump_fence("root")

    assert fence == 1
    assert len(calls) == 3
    assert await wal.current_fence("root") == 1


async def test_a_write_no_replica_confirms_fails_after_the_retry_window(redis, monkeypatch):
    async def no_replica(client, key, value, replicas):
        raise NotReplicatedError(f"{key} reached 0 of 1 replicas")

    monkeypatch.setattr(wal_module, "confirm_replicated", no_replica)
    monkeypatch.setattr(wal_module, "WRITE_RETRY_S", 0.1)

    with pytest.raises(NotReplicatedError):
        await WriteAheadLog(redis, replica_acks=1).bump_fence("root")


async def test_the_envelope_goes_out_only_after_the_dispatch_is_durable(redis, monkeypatch):
    queue_at_confirm = []

    async def record_queue(client, key, value, replicas):
        queue_at_confirm.append(await client.llen(TASK_QUEUE))

    monkeypatch.setattr(wal_module, "confirm_replicated", record_queue)
    dag = DAGWorkflow(workflow_id="wf", tasks=[TaskNode(id="root", agent_prompt="root")])
    scheduler = Scheduler(redis, SETTINGS)
    scheduler.wal = WriteAheadLog(redis, replica_acks=1)
    await scheduler.submit_workflow(dag)

    assert queue_at_confirm and set(queue_at_confirm) == {0}
    (envelope,) = await redis.lrange(TASK_QUEUE, 0, -1)
    assert json.loads(envelope)["fence_token"] == 1


async def test_an_election_no_replica_confirms_does_not_count(redis, monkeypatch):
    async def no_replica(client, key, value, replicas):
        raise NotReplicatedError(f"{key} reached 0 of 1 replicas")

    monkeypatch.setattr(leader_module, "confirm_replicated", no_replica)
    election = LeaderElection(redis, "cp-1", SETTINGS, replica_acks=1)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(election.campaign(), timeout=0.5)
    assert election.token is None


async def test_an_election_counts_once_a_replica_confirms_it(redis, monkeypatch):
    confirmed = []

    async def replica_has_it(client, key, value, replicas):
        confirmed.append((key, value))

    monkeypatch.setattr(leader_module, "confirm_replicated", replica_has_it)
    token = await LeaderElection(redis, "cp-1", SETTINGS, replica_acks=1).campaign()

    assert confirmed == [("aether:leader", token)]


async def _expire_lease(redis, monitor):
    await monitor.beat("worker-1")
    await redis.delete("aether:lease:worker-1")


async def test_the_monitor_waits_a_lease_after_seeing_redis_again(redis):
    """Right after a Redis outage, a lapsed lease means nobody could renew it, not that its worker died."""
    dead = []

    async def on_failure(worker_id):
        dead.append(worker_id)

    monitor = HeartbeatMonitor(redis, on_failure, SETTINGS)
    await _expire_lease(redis, monitor)

    await monitor._scan_dead_workers()
    assert dead == [] and not monitor.can_judge()

    await asyncio.sleep(SETTINGS.lease_s)
    await monitor._scan_dead_workers()
    assert dead == ["worker-1"]


async def test_a_stalled_scan_restarts_the_grace_period(redis, monkeypatch):
    """A frozen primary answers late rather than failing; renewals stalled for as long."""
    monitor = HeartbeatMonitor(redis, lambda worker_id: asyncio.sleep(0), SETTINGS)
    await _expire_lease(redis, monitor)
    await monitor._scan_dead_workers()
    await asyncio.sleep(SETTINGS.lease_s)
    assert monitor.can_judge()

    hgetall = redis.hgetall

    async def stalled_hgetall(key):
        await asyncio.sleep(SETTINGS.heartbeat_interval_s * 2)
        return await hgetall(key)

    monkeypatch.setattr(redis, "hgetall", stalled_hgetall)
    await monitor._scan_dead_workers()
    assert not monitor.can_judge()


def test_sentinels_switch_clients_to_discovery(monkeypatch):
    monkeypatch.setenv("REDIS_SENTINELS", "127.0.0.1:26379, 127.0.0.1:26380")
    monkeypatch.setenv("REDIS_SENTINEL_MASTER", "orders")
    monkeypatch.delenv("REDIS_REPLICA_ACKS", raising=False)

    client = redis_client.create_redis()

    assert isinstance(client.connection_pool, SentinelConnectionPool)
    assert client.connection_pool.service_name == "orders"
    assert redis_client.replica_acks() == 1


def test_replica_acks_default_to_none_without_sentinels(monkeypatch):
    monkeypatch.delenv("REDIS_SENTINELS", raising=False)
    monkeypatch.delenv("REDIS_REPLICA_ACKS", raising=False)
    assert redis_client.replica_acks() == 0
    monkeypatch.setenv("REDIS_REPLICA_ACKS", "2")
    assert redis_client.replica_acks() == 2
