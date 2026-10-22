from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from http.server import BaseHTTPRequestHandler
from pathlib import Path
import shutil
import socket
from socketserver import TCPServer
import threading
from types import SimpleNamespace

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run provider/tool matrix e2e tests",
)


CHAT_PROVIDERS = (
    "openai",
    "codex",
    "anthropic",
    "openrouter",
    "gemini",
    "ollama",
    "groq",
    "mistral",
    "deepseek",
    "xai",
    "together",
    "custom",
)

OPENAI_COMPAT_MODEL_TEST_PROVIDER_CASES = (
    "openai",
    "gemini",
    "ollama",
    "groq",
    "mistral",
    "deepseek",
    "xai",
    "together",
    "custom",
)

OPENAI_COMPAT_REASONING_PROVIDER_CASES = (
    "openai",
    "openrouter",
    "gemini",
    "ollama",
    "groq",
    "mistral",
    "deepseek",
    "xai",
    "together",
    "custom",
)

PLUGIN_PROVIDER_MATRIX = {
    "search_plugin": (
        "builtin_search_provider",
        "brave_search_provider",
        "google_custom_search_provider",
        "google_search_provider",
        "perplexity_search_provider",
        "duckduckgo_instant_answer_provider",
        "duckduckgo_search_provider",
    ),
    "email_plugin": (
        "google_workspace_provider",
        "custom_api_provider",
        "imap_smtp_provider",
    ),
    "calendar_plugin": ("google_workspace_provider",),
    "media_plugin": ("local_media_provider",),
}

EXPECTED_CORE_TOOLS = {
    "file_read",
    "file_write",
    "pdf_create",
    "pdf_edit",
    "presentation_create",
    "terminal_exec",
}

EXPECTED_PLUGIN_TOOLS = {
    "web_search",
    "email_search",
    "email_read",
    "calendar_list",
    "audio_transcribe",
    "image_extract_text",
    "image_generate",
    "connector_request",
    "skill_pack_read",
}


def _web_app(tmp_path: Path):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    app = create_app(runtime, orchestrator=None, registry=registry)
    return app, runtime, registry


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _ThreadedTCPServer(TCPServer):
    allow_reuse_address = True


class _MockApiServer:
    def __init__(self, handler_cls, port: int) -> None:  # noqa: ANN001
        self.base_url = f"http://127.0.0.1:{port}"
        self.server = _ThreadedTCPServer(("127.0.0.1", port), handler_cls)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise AssertionError("mock API server thread did not stop after e2e")


class _MockToolWebServer:
    def __init__(self, handler_cls, port: int) -> None:  # noqa: ANN001
        self.server = _ThreadedTCPServer(("127.0.0.1", port), handler_cls)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise AssertionError("mock web server did not stop after e2e")


class _OpenAICompatibleHandler(BaseHTTPRequestHandler):
    calls: list[dict[str, object]] = []
    response_builder = None

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8") or "{}")
        self.__class__.calls.append(
            {
                "path": self.path,
                "authorization": self.headers.get("Authorization", ""),
                "payload": payload,
            }
        )
        response = self.__class__.response_builder(payload) if callable(self.__class__.response_builder) else {
            "id": "chatcmpl-e2e",
            "object": "chat.completion",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }
            ],
        }
        raw = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format, *args):  # noqa: A002, ANN001
        return


def _openai_compatible_response_for_variant(variant: str, provider: str, model_name: str) -> dict[str, object]:
    if variant == "tool_call":
        return {
            "id": "chatcmpl-e2e",
            "object": "chat.completion",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "reasoning_content": "trace",
                        "tool_calls": [
                            {
                                "id": "tool-1",
                                "type": "function",
                                "function": {"name": "web_fetch", "arguments": "{\"url\":\"https://example.com\"}"},
                            }
                        ],
                    },
                    "finish_reason": "tool_calls",
                }
            ],
        }
    if variant == "empty":
        return {
            "id": "chatcmpl-e2e",
            "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": ""}, "finish_reason": "stop"}],
        }
    return {
        "id": "chatcmpl-e2e",
        "object": "chat.completion",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": "stop",
            }
        ],
    }


class _OpenAICompatibleResponseVariantsHandler(BaseHTTPRequestHandler):
    calls: list[dict[str, object]] = []
    variant = "standard"
    provider = "openai"
    model_name = "model"

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8") or "{}")
        self.__class__.calls.append({"path": self.path, "payload": payload})

        response = _openai_compatible_response_for_variant(
            self.__class__.variant,
            self.__class__.provider,
            self.__class__.model_name,
        )
        raw = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format, *args):  # noqa: A002, ANN001
        return


class _CodexResponsesHandler(BaseHTTPRequestHandler):
    calls: list[dict[str, object]] = []

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length)
        payload = json.loads(body.decode("utf-8") or "{}")
        self.__class__.calls.append(
            {
                "path": self.path,
                "authorization": self.headers.get("Authorization", ""),
                "account_id": self.headers.get("ChatGPT-Account-Id", ""),
                "payload": payload,
            }
        )
        events = [
            {"type": "response.reasoning_summary_text.delta", "delta": "thinking"},
            {"type": "response.output_text.delta", "delta": "ok"},
        ]
        chunks = "".join(f"data: {json.dumps(event)}\n\n" for event in events) + "data: [DONE]\n\n"
        raw = chunks.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format, *args):  # noqa: A002, ANN001
        return


class _ConnectorGatewayHandler(BaseHTTPRequestHandler):
    calls: list[dict[str, object]] = []

    def do_GET(self):  # noqa: N802
        self._handle()

    def do_POST(self):  # noqa: N802
        self._handle()

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length).decode("utf-8")
        self.__class__.calls.append(
            {
                "path": self.path,
                "method": self.command,
                "authorization": self.headers.get("Authorization", ""),
                "body": body,
            }
        )
        raw = json.dumps({"ok": True, "path": self.path, "method": self.command}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format, *args):  # noqa: A002, ANN001
        return


def test_config_api_exposes_every_supported_provider_and_runtime_toggle(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.setenv("NULLION_MODEL_PROVIDER", "openai")
    monkeypatch.setenv("NULLION_MODEL", "gpt-e2e")
    monkeypatch.delenv("NULLION_MEMORY_LONG_TERM_LIMIT", raising=False)
    monkeypatch.delenv("NULLION_MEMORY_MID_TERM_LIMIT", raising=False)
    monkeypatch.delenv("NULLION_MEMORY_SHORT_TERM_LIMIT", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-e2e")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-e2e")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-e2e")
    monkeypatch.setenv("GEMINI_API_KEY", "sk-gemini-e2e")
    monkeypatch.setenv("GROQ_API_KEY", "sk-groq-e2e")
    monkeypatch.setenv("MISTRAL_API_KEY", "sk-mistral-e2e")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-deepseek-e2e")
    monkeypatch.setenv("XAI_API_KEY", "sk-xai-e2e")
    monkeypatch.setenv("TOGETHER_API_KEY", "sk-together-e2e")
    monkeypatch.setenv("NULLION_OLLAMA_API_KEY", "ollama-e2e")
    monkeypatch.setenv("NULLION_ENABLED_PLUGINS", "search_plugin,email_plugin,calendar_plugin,media_plugin")
    monkeypatch.setenv("NULLION_ENABLED_SKILL_PACKS", "nullion/connector-skills")

    app, _runtime, _registry = _web_app(tmp_path)
    with TestClient(app) as client:
        payload = client.get("/api/config").json()

    assert set(CHAT_PROVIDERS).issubset(set(payload["providers_configured"]))
    assert payload["providers_configured"]["openai"] is True
    assert payload["providers_configured"]["anthropic"] is True
    assert payload["providers_configured"]["openrouter"] is True
    assert payload["providers_configured"]["gemini"] is True
    assert payload["providers_configured"]["groq"] is True
    assert payload["providers_configured"]["mistral"] is True
    assert payload["providers_configured"]["deepseek"] is True
    assert payload["providers_configured"]["xai"] is True
    assert payload["providers_configured"]["together"] is True
    assert payload["providers_configured"]["ollama"] is True
    assert payload["model_provider"] == "openai"
    assert payload["memory_long_term_limit"] == 25
    assert payload["memory_mid_term_limit"] == 15
    assert payload["memory_short_term_limit"] == 10
    for key in (
        "doctor_enabled",
        "smart_cleanup_enabled",
        "memory_enabled",
        "skill_learning",
        "web_access",
        "browser_enabled",
        "file_access",
        "terminal_enabled",
        "task_decomposition",
        "multi_agent",
    ):
        assert isinstance(payload[key], bool), key
    assert "skill_pack_catalog" in payload
    assert "skill_auth_providers" in payload


def test_codex_oauth_refresh_and_responses_api_are_mockable(tmp_path: Path, monkeypatch) -> None:
    from nullion import model_clients
    from nullion.config import ModelSettings, NullionSettings
    from nullion.model_clients import build_model_client_from_settings

    def jwt_with_account(account_id: str) -> str:
        header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode("ascii").rstrip("=")
        body = base64.urlsafe_b64encode(
            json.dumps({"https://api.openai.com/auth": {"chatgpt_account_id": account_id}}).encode("utf-8")
        ).decode("ascii").rstrip("=")
        return f"{header}.{body}.sig"

    refreshed_token = jwt_with_account("acct-e2e")
    refresh_calls: list[dict[str, object]] = []

    class RefreshResponse:
        status_code = 200
        text = '{"access_token":"token"}'

        def json(self):
            return {"access_token": refreshed_token}

    def fake_refresh_post(url, *, data, headers, timeout):  # noqa: ANN001
        refresh_calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        return RefreshResponse()

    monkeypatch.setattr(model_clients._httpx, "post", fake_refresh_post)
    monkeypatch.setattr(model_clients, "_load_codex_auth_json_tokens", lambda: None)
    monkeypatch.setattr(model_clients, "_load_persisted_codex_tokens", lambda: None)
    monkeypatch.setattr(model_clients, "_codex_refresh_lock_path", lambda: tmp_path / "codex-refresh.lock")
    settings = NullionSettings()
    settings.model = ModelSettings(
        provider="codex",
        openai_api_key="expired-token-without-account-claim",
        codex_refresh_token="refresh-e2e",
        openai_model="gpt-e2e",
        reasoning_effort="low",
    )
    client = build_model_client_from_settings(settings)

    _CodexResponsesHandler.calls = []
    port = _free_port()
    client._ENDPOINT = f"http://127.0.0.1:{port}/backend-api/codex/responses"
    with _MockApiServer(_CodexResponsesHandler, port):
        result = client.create(
            messages=[{"role": "user", "content": "Say ok"}],
            tools=[{"name": "web_fetch", "description": "Fetch", "input_schema": {"type": "object"}}],
        )

    assert refresh_calls
    assert refresh_calls[0]["url"] == "https://auth.openai.com/oauth/token"
    assert result["stop_reason"] == "end_turn"
    assert result["content"] == [
        {"type": "thinking", "text": "thinking"},
        {"type": "text", "text": "ok"},
    ]
    assert _CodexResponsesHandler.calls
    call = _CodexResponsesHandler.calls[0]
    assert call["authorization"] == f"Bearer {refreshed_token}"
    assert call["account_id"] == "acct-e2e"
    payload = call["payload"]
    assert isinstance(payload, dict)
    assert payload["model"] == "gpt-e2e"
    assert payload["reasoning"] == {"effort": "low"}
    assert payload["tools"][0]["name"] == "web_fetch"


def test_concurrent_runtime_clients_share_one_codex_oauth_rotation(tmp_path: Path, monkeypatch) -> None:
    from nullion import model_clients
    from nullion.config import ModelSettings, NullionSettings

    def jwt_with_account(account_id: str, exp: int) -> str:
        header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode("ascii").rstrip("=")
        body = base64.urlsafe_b64encode(
            json.dumps(
                {
                    "https://api.openai.com/auth": {"chatgpt_account_id": account_id},
                    "exp": exp,
                }
            ).encode("utf-8")
        ).decode("ascii").rstrip("=")
        return f"{header}.{body}.sig"

    expired_access = jwt_with_account("acct-e2e", 1)
    fresh_access = jwt_with_account("acct-e2e", 9_999_999_999)
    shared: dict[str, model_clients.CodexTokenRefresh | None] = {"tokens": None}
    refresh_calls: list[str] = []
    start = threading.Barrier(2)

    monkeypatch.setattr(model_clients, "_load_codex_auth_json_tokens", lambda: None)
    monkeypatch.setattr(model_clients, "_load_persisted_codex_tokens", lambda: shared["tokens"])
    monkeypatch.setattr(model_clients, "_codex_refresh_lock_path", lambda: tmp_path / "codex-refresh.lock")

    def refresh(token: str) -> model_clients.CodexTokenRefresh:
        refresh_calls.append(token)
        return model_clients.CodexTokenRefresh(fresh_access, "rotated-refresh")

    def persist(access_token: str, refresh_token: str | None = None) -> None:
        shared["tokens"] = model_clients.CodexTokenRefresh(access_token, refresh_token)

    monkeypatch.setattr(model_clients, "_refresh_codex_access_token", refresh)
    monkeypatch.setattr(model_clients, "_persist_codex_oauth_tokens", persist)

    settings = NullionSettings()
    settings.model = ModelSettings(
        provider="codex",
        openai_api_key=expired_access,
        codex_refresh_token="single-use-refresh",
        openai_model="gpt-e2e",
        reasoning_effort="low",
    )

    def build_client() -> model_clients.CodexResponsesModelClient:
        start.wait(timeout=2)
        client = model_clients.build_model_client_from_settings(settings)
        assert isinstance(client, model_clients.CodexResponsesModelClient)
        return client

    with ThreadPoolExecutor(max_workers=2) as pool:
        clients = list(pool.map(lambda _: build_client(), range(2)))

    assert refresh_calls == ["single-use-refresh"]
    assert [client.api_key for client in clients] == [fresh_access, fresh_access]
    assert [client.refresh_token for client in clients] == ["rotated-refresh", "rotated-refresh"]


def test_anthropic_provider_serializes_messages_tools_and_thinking_without_live_api() -> None:
    from nullion.model_clients import AnthropicMessagesModelClient

    captured: dict[str, object] = {}

    class Messages:
        def create(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            return SimpleNamespace(
                stop_reason="end_turn",
                content=[
                    SimpleNamespace(type="thinking", thinking="private chain"),
                    SimpleNamespace(type="text", text="ok"),
                ],
            )

    client = AnthropicMessagesModelClient(
        client=SimpleNamespace(messages=Messages()),
        model="claude-e2e",
        reasoning_effort="low",
    )
    result = client.create(
        messages=[
            {"role": "system", "content": "System instruction"},
            {"role": "user", "content": [{"type": "text", "text": "Say ok"}]},
        ],
        tools=[{"name": "web_fetch", "description": "Fetch", "input_schema": {"type": "object"}}],
        max_tokens=4096,
    )

    assert captured["model"] == "claude-e2e"
    assert captured["system"] == "System instruction"
    assert captured["thinking"] == {"type": "enabled", "budget_tokens": 1024}
    assert captured["tools"][0]["name"] == "web_fetch"
    assert result["content"] == [
        {"type": "thinking", "text": "private chain"},
        {"type": "text", "text": "ok"},
    ]


@pytest.mark.parametrize("provider", OPENAI_COMPAT_MODEL_TEST_PROVIDER_CASES)
def test_model_test_exercises_openai_compatible_provider_against_mock_api(
    tmp_path: Path,
    monkeypatch,
    provider: str,
) -> None:
    from fastapi.testclient import TestClient

    _OpenAICompatibleHandler.calls = []
    port = _free_port()
    monkeypatch.setenv("NULLION_OPENAI_BASE_URL", f"http://127.0.0.1:{port}/v1")

    app, _runtime, _registry = _web_app(tmp_path)
    with _MockApiServer(_OpenAICompatibleHandler, port):
        with TestClient(app) as client:
            response = client.post(
                "/api/config/model-test",
                json={
                    "model_provider": provider,
                    "model_name": f"{provider}-model-e2e",
                    "api_key": f"sk-{provider}-e2e",
                    "reasoning_effort": "low",
                },
            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["provider"] == provider
    assert payload["model"] == f"{provider}-model-e2e"
    assert _OpenAICompatibleHandler.calls
    last_call = _OpenAICompatibleHandler.calls[-1]
    assert last_call["path"] == "/v1/chat/completions"
    assert last_call["authorization"] == f"Bearer sk-{provider}-e2e"
    request_payload = last_call["payload"]
    assert isinstance(request_payload, dict)
    assert request_payload["model"] == f"{provider}-model-e2e"
    if provider in {"openai", "gemini", "ollama"}:
        assert request_payload["reasoning_effort"] == "low"
    elif provider == "openrouter":
        assert "reasoning_effort" not in request_payload
        assert "reasoning" not in request_payload
        assert "extra_body" not in request_payload
    else:
        assert "reasoning_effort" not in request_payload
        assert "reasoning" not in request_payload


@pytest.mark.parametrize(
    ("provider", "model_variant", "expected_stop_reason"),
    (
    ("openai", "standard", "end_turn"),
    ("gemini", "tool_call", "tool_use"),
    ("openai", "tool_call", "tool_use"),
    ("openrouter", "standard", "end_turn"),
    ("groq", "standard", "end_turn"),
    ("together", "standard", "end_turn"),
    ("custom", "standard", "end_turn"),
    ),
)
def test_model_test_exercises_openai_compatible_provider_output_variants(
    tmp_path: Path,
    monkeypatch,
    provider: str,
    model_variant: str,
    expected_stop_reason: str | None,
) -> None:
    from fastapi.testclient import TestClient

    _OpenAICompatibleResponseVariantsHandler.variant = model_variant
    _OpenAICompatibleResponseVariantsHandler.provider = provider
    _OpenAICompatibleResponseVariantsHandler.calls = []

    port = _free_port()
    monkeypatch.setenv("NULLION_OPENAI_BASE_URL", f"http://127.0.0.1:{port}/v1")

    app, _runtime, _registry = _web_app(tmp_path)
    with _MockApiServer(_OpenAICompatibleResponseVariantsHandler, port):
        with TestClient(app) as client:
            response = client.post(
                "/api/config/model-test",
                json={
                    "model_provider": provider,
                    "model_name": f"{provider}-model-e2e",
                    "api_key": f"sk-{provider}-e2e",
                    "reasoning_effort": "low",
                },
            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["provider"] == provider
    assert payload["model"] == f"{provider}-model-e2e"
    assert payload["stop_reason"] == expected_stop_reason
    assert _OpenAICompatibleResponseVariantsHandler.calls
    request_payload = _OpenAICompatibleResponseVariantsHandler.calls[-1]["payload"]
    assert request_payload["model"] == f"{provider}-model-e2e"
    if provider in {"openai", "gemini", "ollama"}:
        assert request_payload["reasoning_effort"] == "low"
    elif provider == "openrouter":
        assert "reasoning_effort" not in request_payload
        assert "reasoning" not in request_payload
        assert "extra_body" not in request_payload
    else:
        assert "reasoning_effort" not in request_payload
        assert "reasoning" not in request_payload



def test_model_test_exercises_provider_specific_outputs_using_fake_clients(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient
    import nullion.model_clients as model_clients

    calls: list[str] = []

    outputs_by_provider = {
        "anthropic": {
            "stop_reason": "tool_use",
            "content": [
                {"type": "thinking", "text": "planning"},
                {"type": "text", "text": "summary"},
                {"type": "tool_use", "id": "anth-1", "name": "search", "input": {"q": "news"}},
            ],
        },
        "codex": {
            "content": [{"type": "text", "text": "done"}],
        },
        "openrouter": {
            "stop_reason": "tool_use",
            "content": [
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "type": "function",
                            "id": "or-1",
                            "function": {
                                "name": "web_fetch",
                                "arguments": "{\"url\":\"https://example.com/report\"}",
                            },
                        }
                    ],
                },
            ],
        },
        "gemini": {
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": "done with no tool call"}],
        },
    }

    class FakeModelClient:
        def __init__(self, provider: str) -> None:
            self.model = f"{provider}-model-e2e"
            self.provider = provider
            self.create_called = False

        def create(self, **kwargs):
            del kwargs
            self.create_called = True
            return outputs_by_provider[self.provider]

    def fake_builder(settings):
        provider = str(getattr(settings.model, "provider", "")).strip().lower()
        calls.append(provider)
        return FakeModelClient(provider)

    monkeypatch.setattr(model_clients, "build_model_client_from_settings", fake_builder)

    app, _runtime, _registry = _web_app(tmp_path)
    with TestClient(app) as client:
        for provider, expected in outputs_by_provider.items():
            response = client.post(
                "/api/config/model-test",
                json={
                    "model_provider": provider,
                    "model_name": f"{provider}-model-e2e",
                    "api_key": f"{provider}-key-e2e",
                    "reasoning_effort": "high",
                },
            )
            assert response.status_code == 200
            payload = response.json()
            assert payload["ok"] is True
            assert payload["provider"] == provider
            assert payload["model"] == f"{provider}-model-e2e"
            assert payload["stop_reason"] == expected.get("stop_reason")

    assert calls == ["anthropic", "codex", "openrouter", "gemini"]


def test_settings_save_hot_swaps_running_messaging_service_config(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    import nullion.telegram_app as telegram_app
    import nullion.web_app as web_app
    from nullion.config import load_settings
    from nullion.telegram_app import build_telegram_operator_service

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "NULLION_TELEGRAM_BOT_TOKEN=123456:abcdefghijklmnopqrstuvwxyz",
                "NULLION_TELEGRAM_OPERATOR_CHAT_ID=123",
                "NULLION_TELEGRAM_CHAT_ENABLED=true",
                "NULLION_MODEL_PROVIDER=openai",
                "NULLION_MODEL=gpt-initial-e2e",
                "NULLION_OPENAI_API_KEY=sk-initial-e2e",
            ]
        ),
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    built_clients: list[tuple[str | None, str | None]] = []

    def fake_build_chat_model_client(candidate_settings, *, surface):  # noqa: ANN001, ARG001
        built_clients.append((candidate_settings.model.provider, candidate_settings.model.openai_model))
        return SimpleNamespace(
            provider=candidate_settings.model.provider,
            model=candidate_settings.model.openai_model,
        )

    monkeypatch.setattr(telegram_app, "_build_chat_model_client_with_fallback", fake_build_chat_model_client)
    monkeypatch.setattr(telegram_app, "AgentOrchestrator", lambda model_client: SimpleNamespace(model_client=model_client))

    app, runtime, _registry = _web_app(tmp_path)
    service = build_telegram_operator_service(runtime, settings=load_settings(env_path=env_file))
    assert service.model_client.model == "gpt-initial-e2e"

    with TestClient(app) as client:
        disabled = client.post(
            "/api/config",
            json={
                "tg_token": "654321:abcdefghijklmnopqrstuvwxyz",
                "tg_chat_id": "456",
                "chat_enabled": False,
                "model_provider": "groq",
                "model_name": "llama-disabled-e2e",
                "api_key": "sk-groq-e2e",
                "web_access": False,
                "browser_enabled": False,
                "tg_streaming_enabled": False,
                "activity_trace": False,
                "enabled_skill_packs": "nullion/example-skill",
            },
        )
        assert disabled.status_code == 200
        assert disabled.json()["ok"] is True

        assert service.refresh_live_configuration() is True
        assert service.bot_token == "654321:abcdefghijklmnopqrstuvwxyz"
        assert service.operator_chat_id == "456"
        assert service.settings.telegram.chat_enabled is False
        assert service.settings.enabled_skill_packs == ("nullion/example-skill",)
        assert service.model_client is None
        assert service.agent_orchestrator is None
        assert service.handle_text_message(text="hello", chat_id="456") is None
        assert os.environ["NULLION_WEB_ACCESS_ENABLED"] == "false"
        assert os.environ["NULLION_BROWSER_ENABLED"] == "false"
        assert os.environ["NULLION_TELEGRAM_CHAT_STREAMING_ENABLED"] == "false"
        assert os.environ["NULLION_ACTIVITY_TRACE_ENABLED"] == "false"

        reenabled = client.post(
            "/api/config",
            json={
                "tg_token": "654321:abcdefghijklmnopqrstuvwxyz",
                "tg_chat_id": "789",
                "chat_enabled": True,
                "model_provider": "openai",
                "model_name": "gpt-reenabled-e2e",
                "api_key": "sk-openai-next-e2e",
                "web_access": True,
                "browser_enabled": True,
                "tg_streaming_enabled": True,
                "activity_trace": True,
                "enabled_skill_packs": "",
            },
        )
        assert reenabled.status_code == 200
        assert reenabled.json()["ok"] is True

        assert service.refresh_live_configuration() is True
        assert service.operator_chat_id == "789"
        assert service.settings.telegram.chat_enabled is True
        assert service.settings.enabled_skill_packs == ()
        assert service.model_client.provider == "openai"
        assert service.model_client.model == "gpt-reenabled-e2e"
        assert service.agent_orchestrator.model_client is service.model_client
        assert os.environ["NULLION_WEB_ACCESS_ENABLED"] == "true"
        assert os.environ["NULLION_BROWSER_ENABLED"] == "true"
        assert os.environ["NULLION_TELEGRAM_CHAT_STREAMING_ENABLED"] == "true"
        assert os.environ["NULLION_ACTIVITY_TRACE_ENABLED"] == "true"

    assert built_clients[0] == ("openai", "gpt-initial-e2e")
    assert built_clients[-1] == ("openai", "gpt-reenabled-e2e")


def test_media_model_config_api_add_defaults_and_delete(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app

    env_file = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with TestClient(app) as client:
        setup = client.post(
            "/api/config",
            json={
                "model_provider": "openai",
                "model_name": "gpt-4o-e2e",
                "media_models": {
                    "openai": [
                        {"model": "whisper-1", "capabilities": ["audio_input"]},
                        {"model": "gpt-4o-e2e", "capabilities": ["image_input"]},
                    ],
                },
                "media_providers_enabled": {"openai": True},
                "audio_transcribe_provider": "openai",
                "audio_transcribe_model": "whisper-1",
                "image_ocr_provider": "openai",
                "image_ocr_model": "gpt-4o-e2e",
            },
        )
        assert setup.status_code == 200
        assert setup.json()["ok"] is True

        initial_payload = client.get("/api/config").json()
        assert initial_payload["media_models"]["openai"] == [
            {"model": "whisper-1", "capabilities": ["audio_input"]},
            {"model": "gpt-4o-e2e", "capabilities": ["image_input"]},
        ]
        assert initial_payload["media_providers_enabled"]["openai"] is True
        assert initial_payload["audio_transcribe_provider"] == "openai"
        assert initial_payload["audio_transcribe_model"] == "whisper-1"
        assert initial_payload["image_ocr_provider"] == "openai"
        assert initial_payload["image_ocr_model"] == "gpt-4o-e2e"

        removed = client.post(
            "/api/config",
            json={
                "model_provider": "openai",
                "model_name": "gpt-4o-e2e",
                "media_models": {
                    "openai": [
                        {"model": "gpt-4o-e2e", "capabilities": ["image_input"]},
                    ],
                },
                "media_providers_enabled": {"openai": True},
                "audio_transcribe_provider": "",
                "audio_transcribe_model": "",
                "image_ocr_provider": "openai",
                "image_ocr_model": "gpt-4o-e2e",
            },
        )
        assert removed.status_code == 200
        assert removed.json()["ok"] is True

        updated_payload = client.get("/api/config").json()
        assert updated_payload["media_models"]["openai"] == [
            {"model": "gpt-4o-e2e", "capabilities": ["image_input"]},
        ]
        assert updated_payload["audio_transcribe_provider"] == ""
        assert updated_payload["audio_transcribe_model"] == ""


@pytest.mark.parametrize("provider", OPENAI_COMPAT_REASONING_PROVIDER_CASES)
def test_openai_compatible_provider_reasoning_translation_is_covered(provider: str) -> None:
    from nullion.model_clients import _chat_reasoning_kwargs

    model = "mistral-small-latest" if provider == "mistral" else f"{provider}-model-e2e"
    kwargs = _chat_reasoning_kwargs(provider, model, "high")

    if provider == "openrouter":
        assert kwargs == {}
    elif provider in {"openai", "gemini", "ollama"}:
        assert kwargs == {"reasoning_effort": "high"}
    elif provider == "mistral":
        assert kwargs == {"reasoning_effort": "high"}
    else:
        assert kwargs == {}


def test_core_plugin_connector_and_skill_tools_have_structured_metadata(tmp_path: Path, monkeypatch) -> None:
    from nullion.tools import (
        ToolRegistry,
        create_core_tool_registry,
        register_calendar_plugin,
        register_email_plugin,
        register_media_plugin,
        register_search_plugin,
    )

    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_CONNECTOR_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_CONNECTOR_GATEWAY", "custom")
    monkeypatch.setenv("NULLION_SKILL_PACK_ACCESS_ENABLED", "1")
    core = create_core_tool_registry(workspace_root=tmp_path)
    core_tool_names = {spec.name for spec in core.list_specs()}
    assert EXPECTED_CORE_TOOLS.issubset(core_tool_names)
    assert {"connector_request", "skill_pack_read"}.issubset(core_tool_names)
    assert "web_fetch" not in core_tool_names

    direct_fetch_core = create_core_tool_registry(workspace_root=tmp_path, direct_web_fetch_enabled=True)
    assert "web_fetch" in {spec.name for spec in direct_fetch_core.list_specs()}

    plugin = ToolRegistry(plugin_registration_allowed=True, filesystem_allowed_roots=[tmp_path])
    register_search_plugin(plugin, web_searcher=lambda query, limit: [])
    register_email_plugin(plugin, email_searcher=lambda query, limit: [], email_reader=lambda message_id: {})
    register_calendar_plugin(plugin, calendar_lister=lambda start, end, max: [])
    register_media_plugin(
        plugin,
        audio_transcriber=lambda path, language: {"text": "audio"},
        image_text_extractor=lambda path: {"text": "image"},
        image_generator=lambda prompt, output_path, size: {"path": output_path},
    )
    plugin_tool_names = {spec.name for spec in plugin.list_specs()}
    assert EXPECTED_PLUGIN_TOOLS - {"connector_request", "skill_pack_read"} <= plugin_tool_names

    for registry in (core, plugin):
        for definition in registry.list_tool_definitions():
            assert definition["name"]
            assert definition["description"]
            assert definition["input_schema"]["type"] == "object"


def test_disabled_image_generation_is_hidden_from_agent_tool_contract(tmp_path: Path, monkeypatch) -> None:
    from nullion.providers import resolve_plugin_provider_kwargs
    from nullion.system_context import build_system_context_snapshot, format_system_context_for_prompt
    from nullion.tools import ToolRegistry, register_media_plugin

    for name in (
        "NULLION_IMAGE_GENERATE_COMMAND",
        "NULLION_IMAGE_GENERATE_PROVIDER",
        "NULLION_IMAGE_GENERATE_MODEL",
        "NULLION_IMAGE_GENERATE_ENABLED",
        "NULLION_MEDIA_GEMINI_API_KEY",
        "GEMINI_API_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NULLION_IMAGE_GENERATE_ENABLED", "false")
    monkeypatch.setenv("NULLION_IMAGE_GENERATE_PROVIDER", "gemini")
    monkeypatch.setenv("NULLION_IMAGE_GENERATE_MODEL", "gemini-3.1-flash-image-preview")
    monkeypatch.setenv("NULLION_MEDIA_GEMINI_API_KEY", "key")

    registry = ToolRegistry(plugin_registration_allowed=True, filesystem_allowed_roots=[tmp_path])
    register_media_plugin(
        registry,
        **resolve_plugin_provider_kwargs(
            plugin_name="media_plugin",
            provider_name="local_media_provider",
        ),
    )

    tool_names = {definition["name"] for definition in registry.list_tool_definitions()}
    assert "audio_transcribe" in tool_names
    assert "image_extract_text" in tool_names
    assert "image_generate" not in tool_names

    context = format_system_context_for_prompt(build_system_context_snapshot(tool_registry=registry))
    assert "image_generate [plugin path:media_plugin • unavailable]" in context
    assert "local fallback images" in context
    assert "💡 Setup tip:" in context
    assert "image generation provider in Settings" in context


def test_core_tool_user_paths_are_executable_in_e2e_runtime(tmp_path: Path, monkeypatch) -> None:
    import socket

    from nullion import tools
    from nullion.tools import ToolInvocation, create_core_tool_registry

    class _MockFetchHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path != "/report":
                self.send_response(404)
                self.end_headers()
                return
            body = (
                "<!doctype html><html><body><h1>Q4</h1><p>Revenue rose 12 percent</p></body></html>"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: A002, ANN001
            return

    workspace = tmp_path / "workspace"
    workspace.mkdir()

    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    registry = create_core_tool_registry(workspace_root=workspace, direct_web_fetch_enabled=True)
    principal = "web:e2e"

    file_path = workspace / "notes.txt"
    write_result = registry.invoke(
        ToolInvocation(
            "e2e-file-write",
            "file_write",
            principal,
            {"path": str(file_path), "content": "hello from e2e"},
        )
    )
    assert write_result.status == "completed"
    assert file_path.exists()

    read_result = registry.invoke(
        ToolInvocation(
            "e2e-file-read",
            "file_read",
            principal,
            {"path": str(file_path)},
        )
    )
    assert read_result.status == "completed"
    assert read_result.output["content"] == "hello from e2e"

    pdf_path = workspace / "report.pdf"
    pdf_create_result = registry.invoke(
        ToolInvocation(
            "e2e-pdf-create",
            "pdf_create",
            principal,
            {
                "image_paths": [],
                "text_pages": ["E2E report"],
                "output_path": str(pdf_path),
            },
        )
    )
    assert pdf_create_result.status == "completed"
    assert Path(str(pdf_create_result.output["path"])).exists()
    assert int(pdf_create_result.output["bytes_written"]) > 0

    edited_pdf_path = workspace / "report-edited.pdf"
    pdf_edit_result = registry.invoke(
        ToolInvocation(
            "e2e-pdf-edit",
            "pdf_edit",
            principal,
            {
                "input_path": str(pdf_path),
                "append_text_pages": ["Edited version"],
                "output_path": str(edited_pdf_path),
            },
        )
    )
    assert pdf_edit_result.status == "completed"
    assert Path(str(pdf_edit_result.output["path"])).exists()

    port = _free_port()
    address = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", port))
    fetch_url = f"http://mocked-web.test:{port}/report"

    def fake_resolution(url: str):  # noqa: ANN001
        if url == fetch_url:
            return tools._WebFetchResolution(host="mocked-web.test", address_infos=(address,))
        return None

    monkeypatch.setattr(tools, "_resolve_web_fetch_resolution", fake_resolution)

    with _MockToolWebServer(_MockFetchHandler, port):
        web_fetch_result = registry.invoke(
            ToolInvocation(
                "e2e-web-fetch",
                "web_fetch",
                principal,
                {"url": fetch_url},
            )
        )
    assert web_fetch_result.status == "completed"
    assert web_fetch_result.output["text"].startswith("Q4")

    shell_command_rows = [
        ("cmd", "echo terminal-ok") if os.name == "nt" else ("/bin/sh", "printf 'terminal-ok'"),
        ("/bin/bash", "printf 'terminal-ok'"),
        ("/bin/dash", "printf 'terminal-ok'"),
    ]
    terminal_ok_executed = 0
    for index, (terminal_shell, command) in enumerate(shell_command_rows, start=1):
        if os.path.isabs(terminal_shell):
            if not Path(terminal_shell).exists():
                continue
        elif not shutil.which(terminal_shell):
            continue
        monkeypatch.setenv("NULLION_TERMINAL_SHELL", terminal_shell)
        terminal_result = registry.invoke(
            ToolInvocation(
                f"e2e-terminal-exec-{index}",
                "terminal_exec",
                principal,
                {"command": command},
            )
        )
        assert terminal_result.status == "completed"
        assert "terminal-ok" in terminal_result.output["stdout"]
        terminal_ok_executed += 1
    assert terminal_ok_executed >= 1


def test_connector_request_uses_mock_gateway_credentials_and_write_permissions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import tools
    from nullion.connections import ProviderConnection
    from nullion.tools import ToolInvocation, _WebFetchResolution, create_core_tool_registry

    port = _free_port()
    address_infos = ((socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", port)),)
    provider_id = "acme_connector_provider"
    connection = ProviderConnection(
        connection_id="conn-e2e",
        workspace_id="workspace_admin",
        provider_id=provider_id,
        display_name="ACME",
        provider_profile=f"http://mocked-connector.test:{port}/api",
        credential_ref="ACME_TOKEN",
        credential_scope="shared",
        permission_mode="write",
    )

    def fake_resolution(url: str):  # noqa: ANN001
        if url.startswith(f"http://mocked-connector.test:{port}/"):
            return _WebFetchResolution(host="mocked-connector.test", address_infos=address_infos)
        return None

    monkeypatch.setenv("NULLION_CONNECTOR_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_CONNECTOR_GATEWAY", "custom")
    monkeypatch.setenv("ACME_TOKEN", "secret-e2e")
    monkeypatch.setattr(tools, "_resolve_web_fetch_resolution", fake_resolution)
    monkeypatch.setattr("nullion.connections.require_workspace_connection_for_principal", lambda principal_id, candidate: connection)
    registry = create_core_tool_registry(workspace_root=tmp_path)

    _ConnectorGatewayHandler.calls = []
    with _MockApiServer(_ConnectorGatewayHandler, port):
        result = registry.invoke(
            ToolInvocation(
                "connector-post",
                "connector_request",
                "web:e2e",
                {
                    "provider_id": provider_id,
                    "url": f"http://mocked-connector.test:{port}/api/messages",
                    "method": "POST",
                    "json": {"message": "hello"},
                },
            )
        )

    assert result.status == "completed"
    assert result.output["json"]["ok"] is True
    assert _ConnectorGatewayHandler.calls
    call = _ConnectorGatewayHandler.calls[0]
    assert call["path"] == "/api/messages"
    assert call["method"] == "POST"
    assert call["authorization"] == "Bearer secret-e2e"
    assert json.loads(str(call["body"])) == {"message": "hello"}


def test_connections_api_persists_provider_connections_and_enables_connector_access(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import connections

    env_file = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(connections, "_CONNECTIONS_PATH", tmp_path / "connections.json")
    app, _runtime, registry = _web_app(tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/api/connections",
            json={
                "connections": [
                    {
                        "connection_id": "conn-acme",
                        "workspace_id": "workspace_admin",
                        "provider_id": "acme_connector_provider",
                        "display_name": "ACME",
                        "provider_profile": "https://api.acme.test/v1",
                        "credential_ref": "ACME_TOKEN",
                        "credential_scope": "shared",
                        "permission_mode": "read_write",
                        "_credential_value": "secret-e2e",
                    }
                ]
            },
        )
        listed = client.get("/api/connections")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert os.environ["NULLION_CONNECTOR_ACCESS_ENABLED"] == "true"
    assert os.environ["ACME_TOKEN"] == "secret-e2e"
    env_contents = env_file.read_text(encoding="utf-8")
    assert 'NULLION_CONNECTOR_ACCESS_ENABLED="true"' in env_contents
    assert 'ACME_TOKEN="secret-e2e"' in env_contents
    assert registry.is_plugin_installed("connector_plugin") is True
    assert listed.json()["connections"][0]["provider_id"] == "acme_connector_provider"


def test_every_declared_plugin_provider_binding_resolves_to_callable_adapters() -> None:
    from nullion.providers import resolve_plugin_provider_kwargs

    for plugin_name, provider_names in PLUGIN_PROVIDER_MATRIX.items():
        for provider_name in provider_names:
            kwargs = resolve_plugin_provider_kwargs(plugin_name=plugin_name, provider_name=provider_name)
            assert kwargs, f"{plugin_name}/{provider_name} returned no adapters"
            if plugin_name == "media_plugin" and provider_name == "local_media_provider":
                assert callable(kwargs.get("audio_transcriber")), f"{plugin_name}/{provider_name}"
                assert callable(kwargs.get("image_text_extractor")), f"{plugin_name}/{provider_name}"
                assert kwargs.get("image_generator") is None or callable(kwargs.get("image_generator")), (
                    f"{plugin_name}/{provider_name}"
                )
                continue
            assert all(callable(value) for value in kwargs.values()), f"{plugin_name}/{provider_name}"


def test_openrouter_model_test_endpoint_is_responsive_to_configured_openai_base_url(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}/e2e-openrouter/v1"
    monkeypatch.setenv("NULLION_OPENAI_BASE_URL", base_url)

    _OpenAICompatibleHandler.calls = []
    with _MockApiServer(_OpenAICompatibleHandler, port):
        app, _runtime, _registry = _web_app(tmp_path)
        with TestClient(app) as client:
            response = client.post(
                "/api/config/model-test",
                json={
                    "model_provider": "openrouter",
                    "model_name": "openrouter-e2e",
                    "api_key": "openrouter-e2e-key",
                    "reasoning_effort": "low",
                },
            )
            assert response.status_code == 200
            assert response.json()["ok"] is True

    assert _OpenAICompatibleHandler.calls
    call = _OpenAICompatibleHandler.calls[-1]
    assert call["path"] == "/e2e-openrouter/v1/chat/completions"


def test_plugin_provider_google_workspace_email_behavior_is_fully_mockable(tmp_path: Path, monkeypatch) -> None:
    import subprocess

    from nullion import providers

    captured: list[list[str]] = []

    class FakeCompleted(subprocess.CompletedProcess):
        def __init__(self, command: list[str], output: str) -> None:
            self.returncode = 0
            self.stdout = output
            self.stderr = ""
            self.args = command

    def fake_run(command: list[str], **_kwargs):  # noqa: ANN001, ANN002
        captured.append(list(command))
        if command[1] == "envelope":
            return FakeCompleted(
                command,
                '[{"id":"m1","subject":"Status report","from":"manager@example.test"}]',
            )
        if command[1] == "message":
            return FakeCompleted(command, '{"id":"m1","subject":"Status report","from":"manager"}')
        raise AssertionError(f"unexpected google workspace command: {command}")

    class _Connection:
        provider_profile = "workspace-user"

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(providers.shutil, "which", lambda name: "/usr/bin/himalaya" if name == "himalaya" else None)
    monkeypatch.setattr(providers, "subprocess", SimpleNamespace(run=fake_run))
    monkeypatch.setattr(
        "nullion.connections.require_workspace_connection_for_principal",
        lambda principal_id, provider_name: _Connection(),
    )

    search_results = providers._google_workspace_email_search("report", 5, principal_id="user:web")
    assert search_results == [{"id": "m1", "subject": "Status report", "from": "manager@example.test"}]
    read_result = providers._google_workspace_email_read("m1", principal_id="user:web")
    assert read_result == {"id": "m1", "subject": "Status report", "from": "manager"}

    assert any(row[:2] == ["himalaya", "envelope"] for row in captured)
    assert any(row[:2] == ["himalaya", "message"] for row in captured)


def test_plugin_provider_custom_api_email_behavior_is_fully_mockable(monkeypatch) -> None:
    from nullion import providers

    seen: list[tuple[str, dict[str, str]]] = []

    def fake_request_json(url: str, *, headers: dict[str, str]) -> dict[str, object]:  # noqa: ANN001
        seen.append((url, headers))
        if "/email/search" in url:
            return {"messages": [{"id": "custom-1", "subject": "invoice"}]}
        if "/email/read/" in url:
            return {"message": {"id": "custom-1", "subject": "invoice"}}
        raise AssertionError(f"unexpected custom API URL: {url}")

    monkeypatch.setenv("NULLION_CUSTOM_API_BASE_URL", "http://mocked-custom.test/v2")
    monkeypatch.setenv("NULLION_CUSTOM_API_TOKEN", "custom-token")
    monkeypatch.setattr(providers, "_request_json", fake_request_json)
    monkeypatch.setattr(
        "nullion.connections.require_workspace_connection_for_principal",
        lambda principal_id, provider_name: None,
    )

    search_results = providers._custom_api_email_search("invoice", 3, principal_id="user:web")
    assert search_results == [{"id": "custom-1", "subject": "invoice"}]
    read_result = providers._custom_api_email_read("custom-1", principal_id="user:web")
    assert read_result == {"id": "custom-1", "subject": "invoice"}

    assert seen[0][0].startswith("http://mocked-custom.test/v2/email/search?")
    assert seen[1][0].startswith("http://mocked-custom.test/v2/email/read/custom-1")
    assert seen[0][1]["Authorization"] == "Bearer custom-token"


def test_plugin_provider_imap_smtp_email_behavior_is_fully_mockable(tmp_path: Path, monkeypatch) -> None:
    from nullion.providers import _imap_connect, _imap_smtp_email_read, _imap_smtp_email_search
    from nullion import providers

    class FakeImapClient:
        def __init__(self, *args):
            self.calls: list[tuple] = []

        def select(self, mailbox: str, readonly: bool = True):  # noqa: ANN001
            return ("OK", [f"[{mailbox}]".encode()])

        def uid(self, *args):  # noqa: ANN001
            self.calls.append(args)
            if args[0] == "SEARCH":
                return ("OK", [b"11 10"])
            if args[0] == "FETCH":
                message_id = str(args[1])
                payload = b"From: sender@example.test\r\nTo: user@example.test\r\nDate: Mon, 1 Jan 2000 00:00:00 +0000\r\nSubject: test\r\n\r\nBody"
                if "RFC822" in args[2]:
                    return ("OK", [(b"FETCH", payload)])
                return ("OK", [(b"FETCH", b"Subject: test\r\nFrom: sender@example.test\r\nTo: user@example.test\r\nDate: now\r\n")])
            raise AssertionError(f"unexpected imap command: {args}")

        def logout(self) -> None:
            pass

    monkeypatch.setenv("NULLION_IMAP_ACCOUNT_HOST", "imap.example.test")
    monkeypatch.setenv("NULLION_IMAP_ACCOUNT_USERNAME", "smtp-user")
    monkeypatch.setenv("NULLION_IMAP_ACCOUNT_PASSWORD", "smtp-pass")
    monkeypatch.setattr(providers, "_imap_connect", lambda connection: FakeImapClient())
    monkeypatch.setattr(
        "nullion.connections.require_workspace_connection_for_principal",
        lambda principal_id, provider_name: None,
    )

    search_results = _imap_smtp_email_search("inbox", 1, principal_id="user:web")
    assert len(search_results) == 1
    assert search_results[0]["id"] == "10"

    read_result = _imap_smtp_email_read("10", principal_id="user:web")
    assert read_result["id"] == "10"
    assert read_result["subject"] == "test"
    assert read_result["from"] == "sender@example.test"


def test_plugin_provider_local_media_command_hooks_are_fully_mockable(monkeypatch, tmp_path: Path) -> None:
    import subprocess

    from nullion import providers

    captured: list[tuple[str, dict[str, str]]] = []

    def fake_run_command(
        template: str,
        *,
        substitutions: dict[str, str],
        timeout_seconds: int = 120,
    ) -> subprocess.CompletedProcess[str]:  # noqa: ANN001
        captured.append((template, dict(substitutions)))
        output_path = substitutions.get("output")
        if output_path:
            Path(output_path).write_text("ok", encoding="utf-8")
        return subprocess.CompletedProcess(args=template, returncode=0, stdout="ok")

    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_COMMAND", "audio-cmd {input}")
    monkeypatch.setenv("NULLION_IMAGE_OCR_COMMAND", "ocr-cmd {input}")
    monkeypatch.setenv("NULLION_IMAGE_GENERATE_COMMAND", "img-cmd {prompt_file} {output}")
    monkeypatch.setattr(providers, "_run_media_command_template", fake_run_command)

    audio = providers._local_audio_transcribe(str(tmp_path / "audio.wav"), "en")
    assert audio["text"] == "ok"

    ocr = providers._local_image_extract_text(str(tmp_path / "image.png"))
    assert ocr["text"] == "ok"

    generated = providers._local_image_generate("draw a thing", str(tmp_path / "out.png"), None)
    assert generated["path"] == str(tmp_path / "out.png")

    assert captured[0][0] == "audio-cmd {input}"
    assert captured[1][0] == "ocr-cmd {input}"
    assert captured[2][0] == "img-cmd {prompt_file} {output}"
