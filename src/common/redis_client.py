from __future__ import annotations

import os

from redis.asyncio import Redis


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def create_redis() -> Redis:
    return Redis.from_url(redis_url(), decode_responses=True)
