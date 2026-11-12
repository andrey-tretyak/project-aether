from __future__ import annotations

import asyncio
import time

from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from src.common.config import Settings, load_settings
from src.common.logger import get_logger
from src.common.redis_client import NotReplicatedError, confirm_replicated
from src.common.redis_client import replica_acks as _default_replica_acks

log = get_logger("aether.leader")

LEADER_KEY = "aether:leader"
LEADER_EPOCH_KEY = "aether:leader_epoch"


class NotLeaderError(Exception):
    """Raised when a control plane that no longer holds the leader lease tries to write."""


class LeaderElection:
    """Single-leader election over one Redis lease key.

    The key holds "<node_id>:<epoch>". The epoch increases with every election, so a
    leader that stalls past its lease and wakes up can never pass for the current one,
    even if it is re-elected later. Control-plane writes check the token in the same
    transaction (see WriteAheadLog), which is what makes a deposed leader harmless.

    With `replica_acks`, a node counts as elected only once replicas hold its lease (and
    so the epoch increment before it); otherwise a Redis failover could lose the election
    and hand the same epoch to a second leader.
    """

    def __init__(
        self, redis: Redis, node_id: str, settings: Settings | None = None, replica_acks: int | None = None
    ):
        self.redis = redis
        self.node_id = node_id
        self.replica_acks = _default_replica_acks() if replica_acks is None else replica_acks
        self.settings = settings or load_settings()
        self.lease_s = self.settings.leader_lease_s
        self.renew_interval_s = self.lease_s / 4
        self.token: str | None = None
        self.epoch: int | None = None
        self._lost = asyncio.Event()

    async def campaign(self) -> str:
        """Block until this node holds the leader lease; return its token."""
        log.info("control_plane.standby", extra={"extra_fields": {"node_id": self.node_id}})
        while True:
            try:
                if not await self.redis.exists(LEADER_KEY):
                    epoch = int(await self.redis.incr(LEADER_EPOCH_KEY))
                    token = f"{self.node_id}:{epoch}"
                    if await self.redis.set(LEADER_KEY, token, nx=True, px=self._lease_ms) and (
                        await self._confirmed(token)
                    ):
                        self.token, self.epoch = token, epoch
                        self._lost.clear()
                        log.info(
                            "control_plane.elected",
                            extra={"extra_fields": {"node_id": self.node_id, "epoch": epoch}},
                        )
                        return token
            except RedisError as exc:
                log.info("control_plane.redis_unavailable", extra={"extra_fields": {"error": str(exc)}})
            await asyncio.sleep(self.renew_interval_s)

    async def _confirmed(self, token: str) -> bool:
        if not self.replica_acks:
            return True
        deadline = time.monotonic() + self.lease_s / 2
        while True:
            try:
                await confirm_replicated(self.redis, LEADER_KEY, token, self.replica_acks)
                return True
            except NotReplicatedError as exc:
                if time.monotonic() > deadline:
                    log.info("control_plane.election_unconfirmed", extra={"extra_fields": {"error": str(exc)}})
                    return False
                await asyncio.sleep(0.05)

    async def renew(self) -> bool:
        """Extend the lease if this node still holds it. False means it has been lost."""
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(LEADER_KEY)
                    if _as_str(await pipe.get(LEADER_KEY)) != self.token:
                        return False
                    pipe.multi()
                    pipe.pexpire(LEADER_KEY, self._lease_ms)
                    await pipe.execute()
                    return True
                except WatchError:
                    continue

    async def hold(self) -> None:
        """Renew the lease until it is lost, then return."""
        valid_until = time.monotonic() + self.lease_s
        while not self._lost.is_set():
            try:
                if not await self.renew():
                    self.mark_lost("lease taken or expired")
                    break
                valid_until = time.monotonic() + self.lease_s
            except Exception:
                log.exception("control_plane.renew_failed", extra={"extra_fields": {"node_id": self.node_id}})
                if time.monotonic() > valid_until:
                    self.mark_lost("could not renew within the lease")
                    break
            try:
                await asyncio.wait_for(self._lost.wait(), timeout=self.renew_interval_s)
            except asyncio.TimeoutError:
                continue

    def mark_lost(self, reason: str) -> None:
        if self._lost.is_set():
            return
        self._lost.set()
        log.info(
            "control_plane.lost_leadership",
            extra={"extra_fields": {"node_id": self.node_id, "epoch": self.epoch, "reason": reason}},
        )

    async def resign(self) -> None:
        """Give up the lease early (planned shutdown) so a standby can take over at once."""
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(LEADER_KEY)
                    if _as_str(await pipe.get(LEADER_KEY)) != self.token:
                        return
                    pipe.multi()
                    pipe.delete(LEADER_KEY)
                    await pipe.execute()
                    log.info(
                        "control_plane.resigned",
                        extra={"extra_fields": {"node_id": self.node_id, "epoch": self.epoch}},
                    )
                    return
                except WatchError:
                    continue

    @property
    def _lease_ms(self) -> int:
        return int(self.lease_s * 1000)


def _as_str(value: object) -> str | None:
    if isinstance(value, bytes):
        return value.decode()
    return None if value is None else str(value)
