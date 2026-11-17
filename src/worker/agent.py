from __future__ import annotations

from typing import Any

from src.common.config import Settings
from src.worker.mock_agent import run_mock_agent
from src.worker.ollama_agent import run_ollama_agent


async def run_agent(prompt: str, payload: dict[str, Any] | None, settings: Settings) -> dict[str, Any]:
    """Run the configured agent backend. Default is the deterministic mock."""
    if settings.agent_backend == "ollama":
        return await run_ollama_agent(
            prompt,
            payload,
            url=settings.ollama_url,
            model=settings.ollama_model,
            timeout_s=settings.ollama_timeout_s,
        )
    return await run_mock_agent(prompt, payload)
