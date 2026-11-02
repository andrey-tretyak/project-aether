from __future__ import annotations

import asyncio
import hashlib
import os
import random
import time
from typing import Any


async def run_mock_agent(prompt: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Deterministic mock LLM: synthetic latency, no external API calls.

    AETHER_MOCK_STALL_RATE / AETHER_MOCK_STALL_S make a deterministic subset of
    prompts block the event loop, the way a synchronous client or a large parse would.
    """
    payload = payload or {}
    digest = hashlib.sha256(prompt.encode()).hexdigest()[:12]
    delay = 0.1 + (int(digest[:2], 16) / 255.0) * 0.4

    stall_rate = float(os.environ.get("AETHER_MOCK_STALL_RATE", "0"))
    stall_s = float(os.environ.get("AETHER_MOCK_STALL_S", "0"))
    stalled = stall_rate > 0 and stall_s > 0 and int(digest[2:4], 16) / 255.0 < stall_rate
    if stalled:
        time.sleep(stall_s)

    await asyncio.sleep(delay)
    return {
        "ok": True,
        "backend": "mock",
        "latency_ms": int((delay + (stall_s if stalled else 0)) * 1000),
        "echo": prompt[:160],
        "token": digest,
        "payload": payload,
        "jitter": random.random(),
    }
