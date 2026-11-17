import json
from unittest.mock import patch

import pytest

from src.common.config import Settings, load_settings
from src.worker.agent import run_agent
from src.worker.ollama_agent import _generate, run_ollama_agent


@pytest.mark.asyncio
async def test_default_backend_is_the_mock():
    result = await run_agent("summarize the lease", {"k": 1}, Settings())
    assert result["backend"] == "mock"
    assert result["ok"] is True
    assert result["echo"].startswith("summarize")


@pytest.mark.asyncio
async def test_ollama_backend_is_selected():
    settings = Settings(agent_backend="ollama", ollama_model="tiny")

    async def fake_ollama(prompt, payload, *, url, model, timeout_s):
        assert prompt == "ping" and model == "tiny" and timeout_s > 0
        return {"ok": True, "backend": "ollama", "model": model, "response": "hello", "echo": prompt}

    with patch("src.worker.agent.run_ollama_agent", fake_ollama):
        result = await run_agent("ping", {}, settings)
    assert result["backend"] == "ollama"
    assert result["response"] == "hello"


def test_generate_sends_non_streaming_request():
    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps({"response": "  done  "}).encode()

    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["body"] = json.loads(request.data.decode())
        captured["timeout"] = timeout
        return _Response()

    with patch("urllib.request.urlopen", fake_urlopen):
        text = _generate("http://127.0.0.1:11434", "llama3.2", "say hi", {"n": 1}, 30.0)

    assert text == "done"
    assert captured["url"].endswith("/api/generate")
    assert captured["body"]["stream"] is False
    assert captured["body"]["model"] == "llama3.2"
    assert captured["body"]["prompt"].startswith("say hi")
    assert "Task payload:" in captured["body"]["prompt"]
    assert captured["timeout"] == 30.0


def test_generate_raises_when_ollama_is_down():
    import urllib.error

    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")):
        with pytest.raises(RuntimeError, match="unreachable"):
            _generate("http://127.0.0.1:11434", "llama3.2", "x", {}, 5.0)


@pytest.mark.asyncio
async def test_ollama_agent_records_latency():
    with patch("src.worker.ollama_agent._generate", return_value="ok"):
        result = await run_ollama_agent(
            "q", {}, url="http://127.0.0.1:11434", model="m", timeout_s=10.0
        )
    assert result["backend"] == "ollama"
    assert result["response"] == "ok"
    assert result["latency_ms"] >= 0


def test_agent_settings_come_from_env(monkeypatch):
    monkeypatch.setenv("AETHER_AGENT_BACKEND", "ollama")
    monkeypatch.setenv("AETHER_OLLAMA_MODEL", "mistral")
    monkeypatch.setenv("AETHER_OLLAMA_URL", "http://ollama:11434")
    settings = load_settings()
    assert settings.agent_backend == "ollama"
    assert settings.ollama_model == "mistral"
    assert settings.ollama_url == "http://ollama:11434"


def test_unknown_agent_backend_is_rejected():
    with pytest.raises(ValueError, match="agent backend"):
        Settings(agent_backend="openai")
