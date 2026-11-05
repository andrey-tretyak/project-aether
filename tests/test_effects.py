import asyncio
import json

import fakeredis
import pytest

from src.common.config import Settings, load_settings
from src.control_plane.wal import WriteAheadLog
from src.worker.effects import (
    LEDGER_KEY,
    REPLAYS_KEY,
    IdempotencyConflict,
    MockExternalAPI,
    idempotency_key,
    perform_effects,
)
from src.worker.executor import WorkerExecutor

NOTIFY = {"kind": "notify", "channel": "#research", "text": "done"}


@pytest.fixture
async def redis():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


async def _ledger(redis) -> list[dict]:
    return [json.loads(e) for e in await redis.lrange(LEDGER_KEY, 0, -1)]


def test_key_names_the_effect_not_the_attempt():
    assert idempotency_key("wf", "t", 0) == idempotency_key("wf", "t", 0)
    assert len({idempotency_key("wf", "t", 0), idempotency_key("wf", "t", 1), idempotency_key("wf", "u", 0)}) == 3


async def test_repeated_key_returns_the_first_receipt_without_a_second_effect(redis):
    api = MockExternalAPI(redis)
    first = await api.call(NOTIFY, "k1")
    again = await api.call(NOTIFY, "k1")

    assert again["effect_id"] == first["effect_id"]
    assert (first["replayed"], again["replayed"]) == (False, True)
    assert len(await _ledger(redis)) == 1
    assert await redis.get(REPLAYS_KEY) == "1"


async def test_key_reused_with_a_different_request_is_rejected(redis):
    api = MockExternalAPI(redis)
    await api.call(NOTIFY, "k1")
    with pytest.raises(IdempotencyConflict):
        await api.call({**NOTIFY, "text": "something else"}, "k1")
    assert len(await _ledger(redis)) == 1


async def test_without_a_key_every_call_is_a_new_effect(redis):
    api = MockExternalAPI(redis)
    await api.call(NOTIFY, None)
    await api.call(NOTIFY, None)
    assert len(await _ledger(redis)) == 2


async def test_concurrent_calls_with_one_key_perform_one_effect(redis):
    api = MockExternalAPI(redis)
    receipts = await asyncio.gather(*(api.call(NOTIFY, "k1") for _ in range(5)))
    assert len({r["effect_id"] for r in receipts}) == 1
    assert len(await _ledger(redis)) == 1


@pytest.mark.parametrize("keyed, expected_effects", [(True, 1), (False, 2)])
async def test_a_reassigned_task_repeats_its_effect_only_without_keys(redis, keyed, expected_effects):
    """The original holder and the worker that took over both run the task to the effect."""
    api = MockExternalAPI(redis)
    for _attempt in range(2):
        await perform_effects(api, "wf", "step-1", [NOTIFY], keyed=keyed)
    assert len(await _ledger(redis)) == expected_effects


async def test_executor_retry_under_a_new_fence_reuses_the_key(redis):
    wal = WriteAheadLog(redis)
    executor = WorkerExecutor(redis, worker_id="worker-1", settings=Settings())
    envelope = {
        "workflow_id": "wf",
        "task_id": "step-1",
        "agent_prompt": "step 1",
        "payload": {"effects": [NOTIFY]},
    }
    for _attempt in range(2):
        fence = await wal.bump_fence("step-1")
        await executor._execute({**envelope, "fence_token": fence})

    ledger = await _ledger(redis)
    assert len(ledger) == 1
    assert ledger[0]["key"] == idempotency_key("wf", "step-1", 0)
    commits = [json.loads(d) for d in await redis.lrange("aether:wal:wf", 0, -1)]
    receipts = [d["result"]["effects"][0] for d in commits if d["type"] == "task_completed"]
    assert [r["replayed"] for r in receipts] == [False, True]
    assert len({r["effect_id"] for r in receipts}) == 1


def test_idempotency_keys_can_be_turned_off(monkeypatch):
    assert load_settings().idempotency_keys is True
    monkeypatch.setenv("AETHER_IDEMPOTENCY_KEYS", "false")
    assert load_settings().idempotency_keys is False
