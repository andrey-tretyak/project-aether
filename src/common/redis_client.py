from __future__ import annotations

import os

from redis import Redis as SyncRedis
from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel
from redis.exceptions import RedisError
from redis.sentinel import Sentinel as SyncSentinel

# A frozen primary never answers, so every call needs a deadline; it must outlast the
# 1s blocking pop workers use to wait for tasks.
SOCKET_TIMEOUT_S = 2.0
CONNECT_TIMEOUT_S = 1.0

REPLICA_ACK_TIMEOUT_MS = 500
BARRIER_KEY = "aether:replication_barrier"
# Bump the barrier only if the write being confirmed is on this server.
_CONFIRM_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('incr', KEYS[2]) end return 0"


class NotReplicatedError(RedisError):
    """A write is not yet held by enough replicas, so a failover could still lose it."""


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def _sentinels() -> list[tuple[str, int]]:
    """REDIS_SENTINELS="host:port,host:port" switches clients to Sentinel discovery."""
    addrs = []
    for item in filter(None, os.environ.get("REDIS_SENTINELS", "").split(",")):
        host, _, port = item.strip().rpartition(":")
        addrs.append((host, int(port)))
    return addrs


def _master_name() -> str:
    return os.environ.get("REDIS_SENTINEL_MASTER", "aether")


def replica_acks() -> int:
    """Replicas that must hold a critical write before it counts (REDIS_REPLICA_ACKS; 1 with Sentinel)."""
    return int(os.environ.get("REDIS_REPLICA_ACKS", "1" if _sentinels() else "0"))


async def confirm_replicated(redis: Redis, key: str, value: str, replicas: int) -> None:
    """Return once `replicas` replicas hold `key` == `value`; raise NotReplicatedError otherwise.

    Sentinel promotes a replica with whatever it had received, so an acknowledged write
    can vanish in a failover. The script bumps a barrier key only if the write is on this
    server; replication is ordered, so a replica that has the barrier has the write too,
    and WAIT on the same connection waits for exactly that. A missing key means the write
    was lost (or the client has moved to a new primary) and must be redone.
    """
    async with redis.pipeline(transaction=False) as pipe:
        pipe.eval(_CONFIRM_SCRIPT, 2, key, BARRIER_KEY, value)
        pipe.wait(replicas, REPLICA_ACK_TIMEOUT_MS)
        present, acked = await pipe.execute()
    if not present:
        raise NotReplicatedError(f"{key} is not on the current primary")
    if acked < replicas:
        raise NotReplicatedError(f"{key} reached {acked} of {replicas} replicas")


def _timeouts() -> dict[str, float]:
    return {"socket_timeout": SOCKET_TIMEOUT_S, "socket_connect_timeout": CONNECT_TIMEOUT_S}


def create_redis() -> Redis:
    sentinels = _sentinels()
    if sentinels:
        return Sentinel(sentinels, **_timeouts()).master_for(_master_name(), decode_responses=True, **_timeouts())
    return Redis.from_url(redis_url(), decode_responses=True)


def create_sync_redis() -> SyncRedis:
    sentinels = _sentinels()
    if sentinels:
        return SyncSentinel(sentinels, **_timeouts()).master_for(_master_name(), decode_responses=True, **_timeouts())
    return SyncRedis.from_url(redis_url(), decode_responses=True)


def create_external_api_redis() -> Redis | None:
    """The mock external API's own store, when it should not share Aether's Redis (and its failures)."""
    url = os.environ.get("EXTERNAL_API_REDIS_URL")
    return Redis.from_url(url, decode_responses=True) if url else None
