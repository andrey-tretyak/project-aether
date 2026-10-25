from __future__ import annotations

import asyncio
import hashlib
import random
from typing import Any


async def run_mock_agent(prompt: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Deterministic mock LLM: synthetic latency, no external API calls."""
    payload = payload or {}
    digest = hashlib.sha256(prompt.encode()).hexdigest()[:12]
    delay = 0.1 + (int(digest[:2], 16) / 255.0) * 0.4
    await asyncio.sleep(delay)
    return {
        "ok": True,
        "backend": "mock",
        "latency_ms": int(delay * 1000),
        "echo": prompt[:160],
        "token": digest,
        "payload": payload,
        "jitter": random.random(),
    }
