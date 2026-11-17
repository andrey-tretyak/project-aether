from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request
from typing import Any


async def run_ollama_agent(
    prompt: str,
    payload: dict[str, Any] | None = None,
    *,
    url: str,
    model: str,
    timeout_s: float,
) -> dict[str, Any]:
    """Call a local Ollama server. Runs the HTTP request off the event loop.

    The worker's lease is renewed on a dedicated thread, so a slow model call does
    not look like a dead worker. A failed or timed-out call raises and the task is
    retried under a new fence, same as any other agent failure.
    """
    payload = payload or {}
    started = time.monotonic()
    text = await asyncio.to_thread(_generate, url, model, prompt, payload, timeout_s)
    return {
        "ok": True,
        "backend": "ollama",
        "model": model,
        "latency_ms": int((time.monotonic() - started) * 1000),
        "response": text,
        "echo": prompt[:160],
        "payload": payload,
    }


def _generate(url: str, model: str, prompt: str, payload: dict[str, Any], timeout_s: float) -> str:
    user = prompt if not payload else f"{prompt}\n\nTask payload: {json.dumps(payload)}"
    body = json.dumps(
        {
            "model": model,
            "prompt": user,
            "stream": False,
            "system": "You are a step in a multi-agent workflow. Answer the prompt directly and briefly.",
            "options": {"temperature": 0.2},
        }
    ).encode()
    request = urllib.request.Request(
        url.rstrip("/") + "/api/generate",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            data = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"ollama HTTP {exc.code}: {detail[:200]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"ollama unreachable at {url}: {exc.reason}") from exc
    text = data.get("response")
    if not isinstance(text, str) or not text.strip():
        raise RuntimeError(f"ollama returned no response text: {data!r}"[:300])
    return text.strip()
