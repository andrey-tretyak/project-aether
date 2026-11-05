from __future__ import annotations

import os
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Optional

import yaml

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "config" / "cluster.yaml"

# Settings field -> (YAML path, environment override)
_SOURCES = {
    "heartbeat_interval_s": (("heartbeat", "interval_s"), "AETHER_HEARTBEAT_INTERVAL_S"),
    "lease_s": (("heartbeat", "lease_s"), "AETHER_LEASE_S"),
    "monitor_poll_s": (("heartbeat", "monitor_poll_s"), "AETHER_MONITOR_POLL_S"),
    "heartbeat_mode": (("heartbeat", "mode"), "AETHER_HEARTBEAT_MODE"),
    "max_loop_stall_s": (("heartbeat", "max_loop_stall_s"), "AETHER_MAX_LOOP_STALL_S"),
    "lock_ttl_s": (("locks", "ttl_s"), "AETHER_LOCK_TTL_S"),
    "leader_lease_s": (("control_plane", "leader_lease_s"), "AETHER_LEADER_LEASE_S"),
    "idempotency_keys": (("effects", "idempotency_keys"), "AETHER_IDEMPOTENCY_KEYS"),
}

HEARTBEAT_MODES = ("thread", "loop")


@dataclass(frozen=True)
class Settings:
    heartbeat_interval_s: float = 0.5
    lease_s: float = 1.5
    monitor_poll_s: float = 0.5
    heartbeat_mode: str = "thread"
    max_loop_stall_s: float = 30.0
    lock_ttl_s: float = 5.0
    leader_lease_s: float = 2.0
    idempotency_keys: bool = True

    def __post_init__(self) -> None:
        if self.heartbeat_mode not in HEARTBEAT_MODES:
            raise ValueError(f"heartbeat mode must be one of {HEARTBEAT_MODES}, got {self.heartbeat_mode!r}")
        if self.lease_s <= self.heartbeat_interval_s:
            raise ValueError("lease_s must be longer than heartbeat_interval_s")
        if self.leader_lease_s <= 0:
            raise ValueError("leader_lease_s must be positive")


def load_settings(path: Optional[Path] = None) -> Settings:
    """Read config/cluster.yaml (or AETHER_CONFIG), then apply AETHER_* environment overrides."""
    config_path = Path(path or os.environ.get("AETHER_CONFIG") or DEFAULT_PATH)
    raw: dict[str, Any] = {}
    if config_path.exists():
        raw = yaml.safe_load(config_path.read_text()) or {}

    types = {f.name: f.type for f in fields(Settings)}
    values: dict[str, Any] = {}
    for name, (yaml_path, env_var) in _SOURCES.items():
        value: Any = raw
        for key in yaml_path:
            value = value.get(key) if isinstance(value, dict) else None
        if env_var in os.environ:
            value = os.environ[env_var]
        if value is None:
            continue
        if types[name] == "str":
            values[name] = str(value)
        elif types[name] == "bool":
            values[name] = _parse_bool(value, env_var)
        else:
            values[name] = float(value)
    return Settings(**values)


def _parse_bool(value: Any, source: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"{source}: expected a boolean, got {value!r}")
