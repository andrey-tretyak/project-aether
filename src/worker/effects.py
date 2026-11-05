from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import WatchError

from src.common.logger import get_logger

log = get_logger("aether.effects")

EXTERNAL_PREFIX = "extapi:"
LEDGER_KEY = "extapi:ledger"
REPLAYS_KEY = "extapi:replays"
KEY_RETENTION_S = 24 * 60 * 60


class IdempotencyConflict(Exception):
    """The same idempotency key was sent with a different request."""


def idempotency_key(workflow_id: str, task_id: str, index: int) -> str:
    """Stable across retries and reassignments: it names the effect, not the attempt."""
    return hashlib.sha256(f"{workflow_id}/{task_id}/{index}".encode()).hexdigest()[:32]


class MockExternalAPI:
    """Stand-in for a third-party API with Stripe-style idempotency keys.

    It lives under extapi:* in Redis, outside Aether's own state, and records every
    effect it actually performs in a ledger. A request with a key it has already seen
    returns the stored response without performing the effect again; the same key with
    a different request is rejected. A request without a key is always performed.
    """

    def __init__(self, redis: Redis):
        self.redis = redis

    async def call(self, request: dict[str, Any], key: str | None) -> dict[str, Any]:
        if key is None:
            receipt = self._receipt(request)
            await self.redis.rpush(LEDGER_KEY, json.dumps({**receipt, "request": request, "key": None}))
            return receipt

        body = json.dumps(request, sort_keys=True)
        stored_key = f"{EXTERNAL_PREFIX}idem:{key}"
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(stored_key)
                    stored = await pipe.get(stored_key)
                    if stored is not None:
                        record = json.loads(stored)
                        if record["request"] != body:
                            raise IdempotencyConflict(f"key {key} was first used with a different request")
                        await self.redis.incr(REPLAYS_KEY)
                        return {**record["response"], "replayed": True}
                    receipt = self._receipt(request)
                    pipe.multi()
                    pipe.rpush(LEDGER_KEY, json.dumps({**receipt, "request": request, "key": key}))
                    pipe.set(stored_key, json.dumps({"request": body, "response": receipt}), ex=KEY_RETENTION_S)
                    await pipe.execute()
                    return receipt
                except WatchError:
                    continue

    @staticmethod
    def _receipt(request: dict[str, Any]) -> dict[str, Any]:
        return {"effect_id": uuid.uuid4().hex, "kind": request.get("kind"), "replayed": False}


async def perform_effects(
    api: MockExternalAPI,
    workflow_id: str,
    task_id: str,
    effects: list[dict[str, Any]],
    keyed: bool = True,
) -> list[dict[str, Any]]:
    """Call the external API once per declared effect, in order, and return the receipts."""
    receipts = []
    for index, effect in enumerate(effects):
        request = {**effect, "workflow_id": workflow_id, "task_id": task_id, "index": index}
        key = idempotency_key(workflow_id, task_id, index) if keyed else None
        receipt = await api.call(request, key)
        log.info(
            "worker.effect",
            extra={
                "extra_fields": {
                    "task_id": task_id,
                    "index": index,
                    "kind": effect.get("kind"),
                    "effect_id": receipt["effect_id"],
                    "replayed": receipt["replayed"],
                    "keyed": keyed,
                }
            },
        )
        receipts.append(receipt)
    return receipts
