import time

import fakeredis
import pytest

from src.common.config import Settings, load_settings
from src.control_plane.heartbeat import HEARTBEAT_HASH, lease_key
from src.worker.heartbeat_thread import HeartbeatThread


def test_settings_come_from_cluster_yaml():
    assert load_settings() == Settings()


def test_environment_overrides_yaml(monkeypatch):
    monkeypatch.setenv("AETHER_LEASE_S", "1.5")
    monkeypatch.setenv("AETHER_HEARTBEAT_MODE", "loop")
    settings = load_settings()
    assert settings.lease_s == 1.5
    assert settings.heartbeat_mode == "loop"


def test_lease_must_outlast_heartbeat_interval():
    with pytest.raises(ValueError):
        Settings(heartbeat_interval_s=1.0, lease_s=1.0)


def _thread(client, last_tick):
    return HeartbeatThread(
        client,
        "worker-1",
        interval_s=0.05,
        lease_s=0.3,
        last_loop_tick=last_tick,
        max_loop_stall_s=0.5,
    )


def test_thread_renews_lease_while_the_loop_is_healthy():
    client = fakeredis.FakeRedis(decode_responses=True)
    thread = _thread(client, time.monotonic)
    thread.start()
    try:
        time.sleep(0.6)
        assert client.exists(lease_key("worker-1"))
        assert client.hexists(HEARTBEAT_HASH, "worker-1")
    finally:
        thread.stop()


def test_thread_lets_the_lease_lapse_when_the_loop_is_wedged():
    client = fakeredis.FakeRedis(decode_responses=True)
    wedged_since = time.monotonic()
    thread = _thread(client, lambda: wedged_since)
    thread.start()
    try:
        time.sleep(0.2)
        assert client.exists(lease_key("worker-1"))
        time.sleep(0.8)
        assert not client.exists(lease_key("worker-1"))
    finally:
        thread.stop()
