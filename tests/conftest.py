import pytest


@pytest.fixture(autouse=True)
def _single_redis(monkeypatch):
    """fakeredis has no replicas, so a shell pointed at Sentinel must not make writes wait for one."""
    monkeypatch.delenv("REDIS_SENTINELS", raising=False)
    monkeypatch.delenv("REDIS_REPLICA_ACKS", raising=False)
