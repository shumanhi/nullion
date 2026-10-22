from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from nullion import model_clients, providers


def _jwt(payload: dict) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"header.{encoded}.sig"


def test_model_client_small_helpers_cover_jwt_reasoning_and_tool_parsing(monkeypatch) -> None:
    token = _jwt({"sub": "user", "exp": 100, "https://api.openai.com/auth": {"chatgpt_account_id": "acct"}})
    monkeypatch.setattr(model_clients._time, "time", lambda: 200) if hasattr(model_clients, "_time") else None

    assert model_clients._extract_jwt_claim(token, "sub") == "user"
    assert model_clients._extract_jwt_claim("bad", "sub") is None
    assert model_clients._extract_chatgpt_account_id(token) == "acct"
    assert model_clients._jwt_is_expired(_jwt({"exp": 1}), leeway_seconds=0) is True
    assert model_clients._jwt_is_expired("bad") is False
    assert model_clients._image_block_data_url({"source": {"type": "base64", "media_type": "image/webp", "data": "abc"}}) == "data:image/webp;base64,abc"
    assert model_clients._image_block_data_url({"source": {"type": "url"}}) is None

    assert model_clients.parse_tool_arguments('{"x": 1}') == {"x": 1}
    assert model_clients.parse_tool_arguments("[1, 2]") == {"arguments": [1, 2]}
    assert model_clients.parse_tool_arguments("not-json") == {"arguments": "not-json"}
    assert model_clients.parse_tool_arguments(3) == {"arguments": 3}
    assert model_clients._chat_reasoning_kwargs("openrouter", "any", "HIGH") == {}
    assert model_clients._chat_reasoning_kwargs("gemini", "gemini-2.5-flash", "medium") == {"reasoning_effort": "medium"}
    assert model_clients._chat_reasoning_kwargs("mistral", "mistral-small-latest", "low") == {"reasoning_effort": "low"}
    assert model_clients._chat_reasoning_kwargs("unknown", "m", "high") == {}
    assert model_clients._anthropic_thinking_kwargs("medium", 4096) == {"thinking": {"type": "enabled", "budget_tokens": 2048}}
    assert model_clients._anthropic_thinking_kwargs("high", 1200) == {"thinking": {"type": "enabled", "budget_tokens": 1024}}
    assert model_clients._anthropic_thinking_kwargs("high", 1024) == {}


def test_image_generation_provider_resolution_uses_svg_fallback_without_real_setup(tmp_path, monkeypatch) -> None:
    for name in (
        "NULLION_IMAGE_GENERATE_COMMAND",
        "NULLION_IMAGE_GENERATE_PROVIDER",
        "NULLION_IMAGE_GENERATE_MODEL",
        "NULLION_IMAGE_GENERATE_ENABLED",
        "NULLION_MEDIA_GEMINI_API_KEY",
        "GEMINI_API_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    kwargs = providers.resolve_plugin_provider_kwargs(
        plugin_name="media_plugin",
        provider_name="local_media_provider",
    )
    assert kwargs["image_generator"] is providers._local_image_generate
    result = kwargs["image_generator"]("draw a peacock", str(tmp_path / "peacock.png"), None)
    assert result["provider"] == "local_svg_fallback"
    assert Path(result["path"]).is_file()

    monkeypatch.setenv("NULLION_IMAGE_GENERATE_ENABLED", "false")
    monkeypatch.setenv("NULLION_IMAGE_GENERATE_COMMAND", "legacy-image-command {output}")
    monkeypatch.setenv("NULLION_IMAGE_GENERATE_PROVIDER", "gemini")
    monkeypatch.setenv("NULLION_IMAGE_GENERATE_MODEL", "gemini-3.1-flash-image-preview")
    monkeypatch.setenv("NULLION_MEDIA_GEMINI_API_KEY", "secret")
    kwargs = providers.resolve_plugin_provider_kwargs(
        plugin_name="media_plugin",
        provider_name="local_media_provider",
    )
    assert kwargs["image_generator"] is None

    monkeypatch.setenv("NULLION_IMAGE_GENERATE_ENABLED", "true")
    kwargs = providers.resolve_plugin_provider_kwargs(
        plugin_name="media_plugin",
        provider_name="local_media_provider",
    )
    assert kwargs["image_generator"] is providers._local_image_generate


def test_openai_chat_adapter_serializes_messages_tools_and_parses_tool_calls() -> None:
    messages = [
        {"role": "system", "content": [{"type": "text", "text": "sys"}]},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "look"},
                {"type": "image", "source": {"type": "base64", "data": "aaa"}},
                {"type": "tool_result", "tool_use_id": "call-1", "content": [{"type": "text", "text": "done"}]},
            ],
        },
        {"role": "assistant", "content": [{"type": "text", "text": "thinking"}, {"type": "tool_use", "id": "call-2", "name": "search", "input": {"q": "x"}}]},
        {"role": "tool", "tool_call_id": "call-2", "content": {"ok": True}},
        {"role": "custom", "content": None},
    ]
    serialized = model_clients.OpenAIChatCompletionsModelClient._serialize_messages(messages)

    assert serialized[0] == {"role": "system", "content": "sys"}
    assert serialized[1]["role"] == "tool"
    assert serialized[1]["content"] == "done"
    assert serialized[2]["content"][1]["image_url"]["url"].startswith("data:image/png;base64,")
    assert serialized[3]["tool_calls"][0]["function"]["name"] == "search"
    assert serialized[4]["content"] == "{'ok': True}"
    assert serialized[5] == {"role": "custom", "content": ""}

    repaired = model_clients.OpenAIChatCompletionsModelClient._serialize_messages(
        [
            {"role": "user", "content": "hi"},
            {"role": "system", "content": "late sys"},
            {"role": "assistant", "content": "hello"},
        ]
    )
    assert [message["role"] for message in repaired] == ["system", "user", "assistant"]

    tools = model_clients.OpenAIChatCompletionsModelClient._serialize_tool_definitions(
        [{"name": "search", "description": "Search", "input_schema": {"type": "object"}}, {"bad": True}]
    )
    assert tools == [{"type": "function", "function": {"name": "search", "description": "Search", "parameters": {"type": "object"}}}]

    choice = SimpleNamespace(
        finish_reason="tool_calls",
        message=SimpleNamespace(
            content=None,
            reasoning_content="why",
            tool_calls=[
                SimpleNamespace(id="call-3", function=SimpleNamespace(name="lookup", arguments='{"q":"x"}')),
                SimpleNamespace(id=None, function=SimpleNamespace(name="skip", arguments="{}")),
            ],
        ),
    )
    stop, blocks = model_clients.OpenAIChatCompletionsModelClient._content_blocks_from_choice(choice)
    assert stop == "tool_use"
    assert blocks[0] == {"type": "thinking", "text": "why"}
    assert blocks[1] == {"type": "tool_use", "id": "call-3", "name": "lookup", "input": {"q": "x"}}


def test_openai_chat_create_only_attaches_tools_when_present() -> None:
    calls: list[dict] = []

    class Completions:
        def create(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok", tool_calls=[]), finish_reason="stop")])

    client = SimpleNamespace(chat=SimpleNamespace(completions=Completions()))
    adapter = model_clients.OpenAIChatCompletionsModelClient(client=client, model="m", provider="openai", reasoning_effort="low")

    assert adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[]) == {"stop_reason": "end_turn", "content": [{"type": "text", "text": "ok"}]}
    assert "tools" not in calls[-1]
    adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[], max_tokens=123, system="sys2")
    assert calls[-1]["messages"][0] == {"role": "system", "content": "sys2"}
    adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[{"name": "t"}])
    assert calls[-1]["tool_choice"] == "auto"
    assert calls[-1]["reasoning_effort"] == "low"


def test_openai_chat_adapter_trims_oversized_serialized_messages(monkeypatch) -> None:
    calls: list[dict] = []

    class Completions:
        def create(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="ok", tool_calls=[]), finish_reason="stop")])

    monkeypatch.setenv("NULLION_CHAT_COMPLETIONS_INPUT_BUDGET_CHARS", "1200")
    client = SimpleNamespace(chat=SimpleNamespace(completions=Completions()))
    adapter = model_clients.OpenAIChatCompletionsModelClient(client=client, model="m")
    adapter.create(
        messages=[
            {"role": "system", "content": "sys" * 1000},
            {"role": "user", "content": "old" * 1000},
            {"role": "assistant", "content": "older" * 1000},
            {"role": "user", "content": "new" * 1000},
        ],
        tools=[],
    )

    assert len(json.dumps(calls[-1]["messages"], ensure_ascii=False)) <= 1200
    assert calls[-1]["messages"][0]["role"] == "system"
    assert calls[-1]["messages"][-1]["role"] == "user"


def test_anthropic_adapter_serializes_parses_and_creates() -> None:
    system, messages = model_clients.AnthropicMessagesModelClient._serialize_messages(
        [
            {"role": "system", "content": "sys"},
            {"role": "tool", "content": "ignored"},
            {"role": "user", "content": [{"type": "text", "text": "hi"}]},
        ]
    )
    assert system == "sys"
    assert messages == [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]

    response = SimpleNamespace(
        stop_reason="tool_use",
        content=[
            SimpleNamespace(type="thinking", thinking="plan"),
            SimpleNamespace(type="text", text="answer"),
            SimpleNamespace(type="tool_use", id="tu", name="search", input={"q": "x"}),
        ],
    )
    assert model_clients.AnthropicMessagesModelClient._parse_response(response)["content"][-1]["input"] == {"q": "x"}

    class Messages:
        def __init__(self) -> None:
            self.kwargs = None

        def create(self, **kwargs):
            self.kwargs = kwargs
            return response

    messages_api = Messages()
    adapter = model_clients.AnthropicMessagesModelClient(client=SimpleNamespace(messages=messages_api), model="claude", reasoning_effort="low")
    result = adapter.create(messages=[{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}], tools=[{"name": "tool"}], max_tokens=4096)
    assert result["stop_reason"] == "tool_use"
    assert messages_api.kwargs["system"] == "sys"
    assert messages_api.kwargs["tools"][0]["name"] == "tool"
    assert messages_api.kwargs["thinking"]["budget_tokens"] == 1024


def test_anthropic_adapter_handles_none_max_tokens() -> None:
    class Messages:
        def __init__(self) -> None:
            self.kwargs: dict | None = None

        def create(self, **kwargs):
            self.kwargs = dict(kwargs)
            return SimpleNamespace(stop_reason="end_turn", content=[SimpleNamespace(type="text", text="ok")])

    messages_api = Messages()
    adapter = model_clients.AnthropicMessagesModelClient(
        client=SimpleNamespace(messages=messages_api),
        model="claude",
        reasoning_effort="low",
    )
    adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[], max_tokens=None)

    assert messages_api.kwargs is not None
    assert messages_api.kwargs["max_tokens"] == 4096
    assert messages_api.kwargs["thinking"]["budget_tokens"] == 1024

    adapter = model_clients.AnthropicMessagesModelClient(
        client=SimpleNamespace(messages=messages_api),
        model="claude",
        max_tokens=1536,
        reasoning_effort="medium",
    )
    adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[], max_tokens=None)
    assert messages_api.kwargs["max_tokens"] == 1536
    assert messages_api.kwargs["thinking"]["budget_tokens"] == 1024


def test_codex_adapter_converts_history_parses_output_and_streams(monkeypatch) -> None:
    messages = [
        {"role": "system", "content": "sys"},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "before"},
                {"type": "tool_result", "tool_use_id": "call-ok", "content": [{"type": "text", "text": "result"}]},
                {"type": "image", "source": {"type": "base64", "data": "abc"}},
            ],
        },
        {"role": "assistant", "content": [{"type": "text", "text": "I will"}, {"type": "tool_use", "id": "call-ok", "name": "search", "input": {"q": "x"}}, {"type": "tool_use", "id": "missing", "name": "skip"}]},
    ]
    converted = model_clients.CodexResponsesModelClient._to_input(messages[1:])
    assert {"type": "function_call_output", "call_id": "call-ok", "output": "result"} in converted
    assert any(item.get("type") == "function_call" and item["call_id"] == "call-ok" for item in converted)
    assert not any(item.get("call_id") == "missing" for item in converted)
    assert model_clients.CodexResponsesModelClient._to_tools([{"name": "t"}, {"no": "name"}])[0]["strict"] is False

    parsed = model_clients.CodexResponsesModelClient._parse(
        {
            "output": [
                {"type": "reasoning", "summary": [{"text": "think"}]},
                {"type": "message", "content": [{"type": "output_text", "text": "hi"}]},
                {"type": "function_call", "call_id": "c", "name": "tool", "arguments": '{"a":1}'},
            ]
        }
    )
    assert parsed["stop_reason"] == "tool_use"
    assert parsed["content"][-1]["input"] == {"a": 1}

    class Response:
        status_code = 200
        reason_phrase = "OK"

        def raise_for_status(self):
            return None

        def iter_lines(self):
            yield 'data: {"type":"response.reasoning_summary_text.delta","delta":"think"}'
            yield 'data: {"type":"response.output_text.delta","delta":"hello"}'
            yield 'data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"tool","arguments":""}}'
            yield 'data: {"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\\"x\\":2}"}'
            yield "data: [DONE]"

    class Stream:
        def __enter__(self):
            return Response()

        def __exit__(self, *args):
            return False

    class Client:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, *args, **kwargs):
            return Stream()

    monkeypatch.setattr(model_clients, "_httpx", SimpleNamespace(Client=Client))
    adapter = model_clients.CodexResponsesModelClient(api_key="tok", account_id="acct", model="gpt", reasoning_effort="high")
    result = adapter.create(messages=messages, tools=[{"name": "tool"}], system="extra")
    assert result["stop_reason"] == "tool_use"
    assert result["content"][0]["type"] == "thinking"
    assert result["content"][1]["text"] == "hello"
    assert result["content"][2]["input"] == {"x": 2}


def test_codex_adapter_stops_stream_on_response_completed(monkeypatch) -> None:
    class Response:
        status_code = 200
        reason_phrase = "OK"

        def raise_for_status(self):
            return None

        def iter_lines(self):
            yield 'data: {"type":"response.output_text.delta","delta":"hello"}'
            yield 'data: {"type":"response.completed"}'
            yield 'data: {"type":"response.output_text.delta","delta":"-late"}'

    class Stream:
        def __enter__(self):
            return Response()

        def __exit__(self, *args):
            return False

    class Client:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, *args, **kwargs):
            return Stream()

    monkeypatch.setattr(model_clients, "_httpx", SimpleNamespace(Client=Client))
    adapter = model_clients.CodexResponsesModelClient(api_key="tok", account_id="acct", model="gpt", reasoning_effort="low")
    result = adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[], timeout=5)
    assert result["stop_reason"] == "end_turn"
    assert result["content"][-1]["text"] == "hello"


def test_codex_adapter_enforces_wall_clock_timeout_budget(monkeypatch) -> None:
    clock = iter([0.0, 0.5, 1.2])
    monkeypatch.setattr(model_clients._time, "monotonic", lambda: next(clock))

    class Response:
        status_code = 200
        reason_phrase = "OK"

        def raise_for_status(self):
            return None

        def iter_lines(self):
            yield 'data: {"type":"response.output_text.delta","delta":"hello"}'
            yield 'data: {"type":"response.output_text.delta","delta":"still-going"}'

    class Stream:
        def __enter__(self):
            return Response()

        def __exit__(self, *args):
            return False

    class Client:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, *args, **kwargs):
            return Stream()

    monkeypatch.setattr(model_clients, "_httpx", SimpleNamespace(Client=Client))
    adapter = model_clients.CodexResponsesModelClient(api_key="tok", account_id="acct", model="gpt", reasoning_effort="low")
    with pytest.raises(model_clients.ModelClientConfigurationError, match="timeout budget"):
        adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[], timeout=1)


def test_codex_adapter_retries_transient_read_timeout(monkeypatch) -> None:
    calls: list[int] = []

    class FakeTimeout(Exception):
        pass

    class FakeReadTimeout(FakeTimeout):
        pass

    class Response:
        status_code = 200
        reason_phrase = "OK"

        def read(self):
            return b""

        def raise_for_status(self):
            return None

        def iter_lines(self):
            yield 'data: {"type":"response.output_text.delta","delta":"recovered"}'
            yield "data: [DONE]"

    class Stream:
        def __enter__(self):
            return Response()

        def __exit__(self, *args):
            return False

    class Client:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, *args, **kwargs):
            _ = (args, kwargs)
            calls.append(1)
            if len(calls) == 1:
                raise FakeReadTimeout("read timed out")
            return Stream()

    monkeypatch.setattr(
        model_clients,
        "_httpx",
        SimpleNamespace(Client=Client, TimeoutException=FakeTimeout, ReadTimeout=FakeReadTimeout),
    )
    adapter = model_clients.CodexResponsesModelClient(api_key="tok", account_id="acct", model="gpt", reasoning_effort="low")
    result = adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[], timeout=1)

    assert len(calls) == 2
    assert result["content"] == [{"type": "text", "text": "recovered"}]


def test_codex_adapter_retries_transient_remote_protocol_error(monkeypatch) -> None:
    calls: list[int] = []

    class FakeTransportError(Exception):
        pass

    class FakeRemoteProtocolError(FakeTransportError):
        pass

    class Response:
        status_code = 200
        reason_phrase = "OK"

        def __init__(self, *, fail: bool):
            self.fail = fail

        def read(self):
            return b""

        def raise_for_status(self):
            return None

        def iter_lines(self):
            if self.fail:
                raise FakeRemoteProtocolError("peer closed connection")
            yield 'data: {"type":"response.output_text.delta","delta":"recovered"}'
            yield "data: [DONE]"

    class Stream:
        def __init__(self, *, fail: bool):
            self.fail = fail

        def __enter__(self):
            return Response(fail=self.fail)

        def __exit__(self, *args):
            return False

    class Client:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, *args, **kwargs):
            _ = (args, kwargs)
            calls.append(1)
            return Stream(fail=len(calls) == 1)

    monkeypatch.setattr(
        model_clients,
        "_httpx",
        SimpleNamespace(Client=Client, TransportError=FakeTransportError, RemoteProtocolError=FakeRemoteProtocolError),
    )
    adapter = model_clients.CodexResponsesModelClient(api_key="tok", account_id="acct", model="gpt", reasoning_effort="low")
    result = adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[], timeout=1)

    assert len(calls) == 2
    assert result["content"] == [{"type": "text", "text": "recovered"}]


def test_codex_adapter_retries_with_unsupported_max_output_tokens(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    class Response:
        def __init__(self, status_code: int, body: bytes, lines: list[str] | None = None) -> None:
            self.status_code = status_code
            self._body = body
            self._lines = lines or []
            self.reason_phrase = "Bad Request" if status_code >= 400 else "OK"

        def read(self):
            return self._body

        def raise_for_status(self):
            return None

        def iter_lines(self):
            for raw in self._lines:
                yield raw

    class Stream:
        def __init__(self, response: Response):
            self._response = response

        def __enter__(self):
            return self._response

        def __exit__(self, *args):
            return False

    class Client:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, *args, **kwargs):
            calls.append(kwargs)
            responses = [
                Response(
                    400,
                    b'{"error":"Unsupported parameter: max_output_tokens"}',
                ),
                Response(
                    200,
                    b"",
                    [
                        'data: {"type":"response.output_text.delta","delta":"fallback"}',
                        "data: [DONE]",
                    ],
                ),
            ]
            idx = len(calls) - 1
            idx = min(idx, len(responses) - 1)
            return Stream(responses[idx])

    monkeypatch.setattr(model_clients, "_httpx", SimpleNamespace(Client=Client))
    adapter = model_clients.CodexResponsesModelClient(api_key="tok", account_id="acct", model="gpt", reasoning_effort="medium")
    result = adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[], max_tokens=9999)

    assert len(calls) == 2
    assert calls[0]["json"]["max_output_tokens"] == 9999
    assert "max_output_tokens" not in calls[1]["json"]
    assert result["stop_reason"] == "end_turn"
    assert result["content"] == [{"type": "text", "text": "fallback"}]


@pytest.mark.parametrize("error_code", ["token_expired", "token_invalidated"])
@pytest.mark.parametrize("retry_succeeds", [True, False])
def test_codex_adapter_refreshes_and_retries_once_on_token_expired(monkeypatch, error_code, retry_succeeds) -> None:
    calls: list[dict[str, Any]] = []
    refreshed_token = _jwt({"https://api.openai.com/auth": {"chatgpt_account_id": "acct-refreshed"}, "exp": 9_999_999_999})
    rotated_refresh = "refresh-rotated"
    persisted: list[tuple[str, str | None]] = []

    class Response:
        def __init__(self, status_code: int, body: bytes, lines: list[str] | None = None) -> None:
            self.status_code = status_code
            self._body = body
            self._lines = lines or []
            self.reason_phrase = "Unauthorized" if status_code >= 400 else "OK"

        def read(self):
            return self._body

        def raise_for_status(self):
            return None

        def json(self):
            return {"access_token": refreshed_token, "refresh_token": rotated_refresh}

        def iter_lines(self):
            for raw in self._lines:
                yield raw

    class Stream:
        def __init__(self, response: Response):
            self._response = response

        def __enter__(self):
            return self._response

        def __exit__(self, *args):
            return False

    class Client:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def stream(self, *args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1 or not retry_succeeds:
                return Stream(Response(401, json.dumps({"error": {"code": error_code, "message": "認証エラー"}}).encode()))
            return Stream(
                Response(
                    200,
                    b"",
                    [
                        'data: {"type":"response.output_text.delta","delta":"recovered"}',
                        "data: [DONE]",
                    ],
                )
            )

    def post(*args, **kwargs):
        return Response(200, b"")

    monkeypatch.setattr(model_clients, "_httpx", SimpleNamespace(Client=Client, post=post))
    monkeypatch.setattr(model_clients, "_shared_codex_token_sources", lambda: ())
    monkeypatch.setattr(
        model_clients,
        "_persist_codex_oauth_tokens",
        lambda access_token, refresh_token=None: persisted.append((access_token, refresh_token)),
    )
    adapter = model_clients.CodexResponsesModelClient(
        api_key="stale",
        account_id="acct-old",
        model="gpt",
        reasoning_effort="medium",
        refresh_token="refresh",
    )

    if retry_succeeds:
        result = adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[])
    else:
        with pytest.raises(model_clients.ModelClientConfigurationError, match="HTTP 401"):
            adapter.create(messages=[{"role": "user", "content": "hi"}], tools=[])

    assert len(calls) == 2
    assert calls[0]["headers"]["Authorization"] == "Bearer stale"
    assert calls[1]["headers"]["Authorization"] == f"Bearer {refreshed_token}"
    assert calls[1]["headers"]["ChatGPT-Account-Id"] == "acct-refreshed"
    assert persisted == [(refreshed_token, rotated_refresh)]
    assert adapter.refresh_token == rotated_refresh
    if retry_succeeds:
        assert result["content"] == [{"type": "text", "text": "recovered"}]


def test_codex_refresh_contract_preserves_rotated_refresh_token(monkeypatch) -> None:
    refreshed_token = _jwt({"https://api.openai.com/auth": {"chatgpt_account_id": "acct-refreshed"}, "exp": 9_999_999_999})
    calls: list[dict[str, Any]] = []

    class Response:
        status_code = 200
        text = ""

        def json(self):
            return {"access_token": refreshed_token, "refresh_token": "new-refresh-token"}

    def post(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return Response()

    monkeypatch.setattr(model_clients, "_httpx", SimpleNamespace(post=post))

    refreshed = model_clients._refresh_codex_access_token("old-refresh-token")

    assert refreshed.access_token == refreshed_token
    assert refreshed.refresh_token == "new-refresh-token"
    assert calls[0]["kwargs"]["data"]["refresh_token"] == "old-refresh-token"


def test_codex_factory_falls_back_to_local_auth_refresh_when_configured_refresh_is_stale(
    monkeypatch,
    tmp_path,
) -> None:
    fallback_auth_path = tmp_path / "auth.json"
    fallback_auth_path.write_text(
        json.dumps(
            {
                "tokens": {
                    "access_token": "expired-fallback-access",
                    "refresh_token": "fresh-refresh",
                }
            }
        ),
        encoding="utf-8",
    )
    refreshed_token = _jwt(
        {
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct-refreshed"},
            "exp": 9_999_999_999,
        }
    )
    persisted: list[tuple[str, str | None]] = []
    calls: list[str] = []

    def refresh(token: str):
        calls.append(token)
        if token == "stale-refresh":
            raise model_clients.ModelClientConfigurationError("HTTP 401 refresh_token_reused")
        return model_clients.CodexTokenRefresh(
            access_token=refreshed_token,
            refresh_token="rotated-refresh",
        )

    monkeypatch.setenv("NULLION_CODEX_AUTH_JSON", str(fallback_auth_path))
    monkeypatch.setattr(model_clients, "_load_persisted_codex_tokens", lambda: None)
    monkeypatch.setattr(model_clients, "_refresh_codex_access_token", refresh)
    monkeypatch.setattr(
        model_clients,
        "_persist_codex_oauth_tokens",
        lambda access_token, refresh_token=None: persisted.append((access_token, refresh_token)),
    )

    client = model_clients.build_model_client_from_settings(
        SimpleNamespace(
            model=SimpleNamespace(
                provider="codex",
                openai_api_key="",
                codex_refresh_token="stale-refresh",
                openai_model="gpt",
                reasoning_effort=None,
            )
        )
    )

    assert isinstance(client, model_clients.CodexResponsesModelClient)
    assert calls == ["fresh-refresh"]
    assert client.api_key == refreshed_token
    assert client.account_id == "acct-refreshed"
    assert client.refresh_token == "rotated-refresh"
    assert persisted == [(refreshed_token, "rotated-refresh")]


def test_codex_oauth_rotation_persists_for_legacy_openai_credentials(monkeypatch) -> None:
    from nullion import credential_store

    old_access = _jwt(
        {
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct"},
            "exp": 1,
        }
    )
    fresh_access = _jwt(
        {
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct"},
            "exp": 9_999_999_999,
        }
    )
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(
        credential_store,
        "load_encrypted_credentials",
        lambda: {
            "provider": "openai",
            "api_key": old_access,
            "refresh_token": "consumed-refresh",
            "keys": {"openai": old_access},
        },
    )
    monkeypatch.setattr(credential_store, "save_encrypted_credentials", lambda creds: saved.append(dict(creds)))

    model_clients._persist_codex_oauth_tokens(fresh_access, "rotated-refresh")

    assert saved[0]["provider"] == "openai"
    assert saved[0]["api_key"] == fresh_access
    assert saved[0]["keys"]["codex"] == fresh_access
    assert saved[0]["refresh_token"] == "rotated-refresh"


def test_codex_oauth_rotation_does_not_replace_platform_openai_key(monkeypatch) -> None:
    from nullion import credential_store

    fresh_access = _jwt(
        {
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct"},
            "exp": 9_999_999_999,
        }
    )
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(
        credential_store,
        "load_encrypted_credentials",
        lambda: {
            "provider": "openai",
            "api_key": "sk-platform-key",
            "keys": {"openai": "sk-platform-key"},
        },
    )
    monkeypatch.setattr(credential_store, "save_encrypted_credentials", lambda creds: saved.append(dict(creds)))

    model_clients._persist_codex_oauth_tokens(fresh_access, "rotated-refresh")

    assert saved[0]["provider"] == "openai"
    assert saved[0]["api_key"] == "sk-platform-key"
    assert saved[0]["keys"]["openai"] == "sk-platform-key"
    assert saved[0]["keys"]["codex"] == fresh_access
    assert saved[0]["refresh_token"] == "rotated-refresh"


def test_codex_oauth_rotation_is_single_use_across_concurrent_workers(monkeypatch) -> None:
    expired_access = _jwt(
        {
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct"},
            "exp": 1,
        }
    )
    fresh_access = _jwt(
        {
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct"},
            "exp": 9_999_999_999,
        }
    )
    shared: dict[str, model_clients.CodexTokenRefresh | None] = {"tokens": None}
    network_calls: list[str] = []
    start = threading.Barrier(2)

    monkeypatch.setattr(model_clients, "_load_codex_auth_json_tokens", lambda: None)
    monkeypatch.setattr(model_clients, "_load_persisted_codex_tokens", lambda: shared["tokens"])

    def refresh(token: str) -> model_clients.CodexTokenRefresh:
        network_calls.append(token)
        return model_clients.CodexTokenRefresh(fresh_access, "rotated-refresh")

    def persist(access_token: str, refresh_token: str | None = None) -> None:
        shared["tokens"] = model_clients.CodexTokenRefresh(access_token, refresh_token)

    monkeypatch.setattr(model_clients, "_refresh_codex_access_token", refresh)
    monkeypatch.setattr(model_clients, "_persist_codex_oauth_tokens", persist)

    def worker() -> model_clients.CodexTokenRefresh:
        start.wait(timeout=2)
        return model_clients._refresh_codex_oauth_with_recovery(
            expired_access,
            ("single-use-refresh",),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: worker(), range(2)))

    assert network_calls == ["single-use-refresh"]
    assert [result.access_token for result in results] == [fresh_access, fresh_access]
    assert [result.refresh_token for result in results] == ["rotated-refresh", "rotated-refresh"]


def test_model_client_factory_paths_and_clone(monkeypatch) -> None:
    codex_token = _jwt({"https://api.openai.com/auth": {"chatgpt_account_id": "acct"}, "exp": 9_999_999_999})
    cfg = SimpleNamespace(model=SimpleNamespace(provider="codex", openai_api_key=codex_token, codex_refresh_token="", openai_model="gpt", reasoning_effort="low"))
    codex = model_clients.build_model_client_from_settings(cfg)
    assert isinstance(codex, model_clients.CodexResponsesModelClient)
    assert codex.account_id == "acct"

    with pytest.raises(model_clients.ModelClientConfigurationError, match="no model"):
        model_clients.build_model_client_from_settings(SimpleNamespace(model=SimpleNamespace(provider="codex", openai_api_key=codex_token, codex_refresh_token="", openai_model="")))
    with pytest.raises(model_clients.ModelClientConfigurationError, match="no Codex OAuth"):
        model_clients.build_model_client_from_settings(SimpleNamespace(model=SimpleNamespace(provider="codex", openai_api_key="sk-platform", codex_refresh_token="", openai_model="gpt", reasoning_effort=None)))

    monkeypatch.setattr(model_clients, "_anthropic", SimpleNamespace(Anthropic=lambda api_key: SimpleNamespace(api_key=api_key)))
    anthropic = model_clients.build_model_client_from_settings(
        SimpleNamespace(model=SimpleNamespace(provider="anthropic", openai_api_key="", anthropic_api_key="ak", anthropic_model="claude", openai_model="", reasoning_effort=None))
    )
    assert isinstance(anthropic, model_clients.AnthropicMessagesModelClient)

    monkeypatch.setattr(model_clients, "OpenAI", lambda **kwargs: SimpleNamespace(**kwargs))
    openai = model_clients.build_model_client_from_settings(
        SimpleNamespace(model=SimpleNamespace(provider="openai", openai_api_key="sk", openai_base_url=" http://base ", openai_model="gpt", reasoning_effort="medium"))
    )
    assert isinstance(openai, model_clients.OpenAIChatCompletionsModelClient)
    assert model_clients.clone_model_client_with_model(openai, "new").model == "new"
    with pytest.raises(ValueError):
        model_clients.clone_model_client_with_model(object(), "new")


def test_provider_env_search_and_media_helpers(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(providers, "_read_credentials_json", lambda: {})
    monkeypatch.delenv("NULLION_OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("NULLION_MEDIA_OPENAI_API_KEY", "media")
    monkeypatch.setenv("OPENAI_API_KEY", "fallback")
    assert providers._provider_key_for_media("openai") == "media"
    monkeypatch.delenv("NULLION_MEDIA_OPENAI_API_KEY")
    assert providers._provider_key_for_media("openai") == "fallback"
    assert providers._media_model_selection("P", "M", "E") is None
    monkeypatch.setenv("P", "openai")
    monkeypatch.setenv("M", "gpt")
    assert providers._media_model_selection("P", "M", "E") == ("openai", "gpt")
    monkeypatch.setenv("E", "off")
    assert providers._media_model_selection("P", "M", "E") is None

    assert providers._first_env("NOPE", "OPENAI_API_KEY") == "fallback"
    with pytest.raises(RuntimeError, match="missing_provider requires"):
        providers._require_env("NOPE", provider_name="missing_provider")
    assert providers._clamped_limit(99, max_limit=10) == 10
    assert providers._clamped_limit(0) == 1
    assert providers._result(" title ", " http://x ", 123) == {"title": "title", "url": "http://x", "snippet": ""}
    assert providers._result("", "url") is None
    assert providers._extract_rows({"items": [1]}, provider_name="p") == [1]
    with pytest.raises(RuntimeError):
        providers._extract_rows({}, provider_name="p")

    monkeypatch.setattr(providers, "_request_json", lambda url, **kwargs: {"web": {"results": [{"title": "A", "url": "u", "description": "s"}, "bad"]}})
    monkeypatch.setenv("BRAVE_SEARCH_API_KEY", "brave")
    assert providers._brave_web_search("q", 2) == [{"title": "A", "url": "u", "snippet": "s"}]
    monkeypatch.setattr(providers, "_request_json", lambda url, **kwargs: {"items": [{"title": "G", "link": "u", "snippet": "s"}]})
    monkeypatch.setenv("GOOGLE_SEARCH_API_KEY", "g")
    monkeypatch.setenv("GOOGLE_SEARCH_CX", "cx")
    assert providers._google_custom_search("q", 12)[0]["title"] == "G"
    monkeypatch.setattr(providers, "_request_json", lambda url, **kwargs: {"results": [{"title": "P", "url": "u", "snippet": "s", "date": "2026"}]})
    monkeypatch.setenv("PERPLEXITY_API_KEY", "p")
    assert providers._perplexity_search("q", 1)[0]["date"] == "2026"
    monkeypatch.setattr(providers, "_request_json", lambda url, **kwargs: {"Heading": "D", "AbstractURL": "u", "AbstractText": "s", "RelatedTopics": [{"Text": "R", "FirstURL": "r"}]})
    assert len(providers._duckduckgo_instant_answer_search("q", 2)) == 2

    assert providers._openrouter_image_config("1024x1024") == {"image_size": "1K", "aspect_ratio": "1:1"}
    assert providers._openrouter_image_config("1200x800") == {"aspect_ratio": "3:2"}
    assert providers._openrouter_image_config("bad") == {}
    out = tmp_path / "image.bin"
    providers._write_image_url_to_path("data:image/png;base64," + base64.b64encode(b"img").decode(), out)
    assert out.read_bytes() == b"img"

    monkeypatch.setattr(providers, "_request_gemini_json", lambda *args, **kwargs: {"predictions": [{"bytesBase64Encoded": base64.b64encode(b"g").decode()}]})
    gemini_out = tmp_path / "gemini.bin"
    assert providers._gemini_imagen_generate("key", "models/imagen-3", "prompt", str(gemini_out), "1k")["path"] == str(gemini_out)
    assert gemini_out.read_bytes() == b"g"
    with pytest.raises(RuntimeError, match="text-to-image only"):
        providers._gemini_imagen_generate("key", "imagen", "prompt", str(gemini_out), None, source_path=str(gemini_out))

    source = tmp_path / "source.png"
    source.write_bytes(b"source")
    monkeypatch.setattr(providers, "_request_gemini_json", lambda *args, **kwargs: {"candidates": [{"content": {"parts": [{"inlineData": {"data": base64.b64encode(b"n").decode()}}]}}]})
    native_out = tmp_path / "native.bin"
    assert providers._gemini_native_image_generate("key", "gemini-image", "prompt", str(native_out), "16x9", source_path=str(source))["provider"] == "gemini:gemini-image"
    assert native_out.read_bytes() == b"n"


def test_provider_commands_custom_api_and_resolution(monkeypatch, tmp_path) -> None:
    completed = SimpleNamespace(returncode=0, stdout="ok\n", stderr="")
    monkeypatch.setattr(providers.subprocess, "run", lambda *args, **kwargs: completed)
    assert providers._run_media_command_template("/bin/echo {code}", substitutions={"code": "print(1)"}).stdout == "ok\n"
    with pytest.raises(RuntimeError, match="missing placeholder"):
        providers._run_media_command_template("{missing}", substitutions={})
    with pytest.raises(RuntimeError, match="empty"):
        providers._run_media_command_template(" ", substitutions={})

    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_COMMAND", "/bin/echo ok")
    assert providers._local_audio_transcribe(str(tmp_path / "a.wav"), "en")["text"] == "ok"
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"ogg")
    converted_inputs: list[str] = []

    def fake_audio_run(argv, **kwargs):
        if "-i" in argv:
            Path(argv[-1]).write_bytes(b"wav")
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        converted_inputs.append(argv[argv.index("-f") + 1])
        return SimpleNamespace(returncode=0, stdout="transcribed\n", stderr="")

    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_COMMAND", "/bin/echo -f {input}")
    monkeypatch.setattr(providers, "_which_local_tool", lambda name: "/usr/bin/ffmpeg" if name == "ffmpeg" else None)
    monkeypatch.setattr(providers.subprocess, "run", fake_audio_run)
    assert providers._local_audio_transcribe(str(audio), None)["text"] == "transcribed"
    assert converted_inputs and converted_inputs[-1].endswith("input.wav")
    monkeypatch.setattr(providers.subprocess, "run", lambda *args, **kwargs: completed)
    monkeypatch.setenv("NULLION_IMAGE_OCR_COMMAND", "/bin/echo ok")
    assert providers._local_image_extract_text(str(tmp_path / "i.png"))["text"] == "ok"
    image_out = tmp_path / "generated.png"
    monkeypatch.setattr(providers, "messaging_media_scratch_root", lambda: tmp_path)

    def fake_run_template(template, *, substitutions, timeout_seconds=0):
        Path(substitutions["output"]).write_bytes(b"png")
        return completed

    monkeypatch.setattr(providers, "_run_media_command_template", fake_run_template)
    assert providers._command_image_generate("tool {output}", "prompt", str(image_out), "512x512")["path"] == str(image_out)

    connection = SimpleNamespace(provider_profile="https://api.example.test/root", credential_ref="env:CUSTOM_TOKEN")
    monkeypatch.setattr(providers, "_custom_api_connection", lambda principal_id: connection)
    monkeypatch.setenv("CUSTOM_TOKEN", "tok")
    monkeypatch.setattr(providers, "_request_json", lambda url, **kwargs: {"results": [{"id": 1}], "message": {"id": "m"}})
    assert providers._custom_api_base_url(connection) == "https://api.example.test/root"
    assert providers._custom_api_headers(connection)["Authorization"] == "Bearer tok"
    assert providers._custom_api_email_search("hello", 30, principal_id="p") == [{"id": 1}]
    assert providers._custom_api_email_read("m/id", principal_id="p") == {"id": "m"}

    class FakeImap:
        def __init__(self, host, port):
            self.host = host
            self.port = port
            self.logged_out = False

        def login(self, username, password):
            assert (username, password) == ("agent@example.com", "pw")

        def select(self, mailbox, readonly=True):
            assert mailbox == "INBOX"
            assert readonly is True
            return "OK", [b"1"]

        def uid(self, command, *args):
            if command == "SEARCH":
                return "OK", [b"42"]
            if command == "FETCH" and args[0] == "42" and args[1] == "(BODY.PEEK[HEADER])":
                return "OK", [(b"42", b"Subject: Hello\r\nFrom: Ada <ada@example.com>\r\nTo: Agent <agent@example.com>\r\nDate: Thu, 30 Apr 2026 10:00:00 -0400\r\n\r\n")]
            if command == "FETCH" and args[0] == "42" and args[1] == "(RFC822)":
                return "OK", [(b"42", b"Subject: Hello\r\nFrom: Ada <ada@example.com>\r\nTo: Agent <agent@example.com>\r\n\r\nBody text")]
            raise AssertionError((command, args))

        def logout(self):
            self.logged_out = True

    monkeypatch.setattr(providers, "_imap_smtp_connection", lambda principal_id: SimpleNamespace(credential_ref="AGENT"))
    monkeypatch.setattr(providers.imaplib, "IMAP4_SSL", FakeImap)
    monkeypatch.setenv("NULLION_IMAP_AGENT_HOST", "imap.example.com")
    monkeypatch.setenv("NULLION_IMAP_AGENT_USERNAME", "agent@example.com")
    monkeypatch.setenv("NULLION_IMAP_AGENT_PASSWORD", "pw")
    assert providers._imap_smtp_email_search("hello", 5, principal_id="p")[0]["id"] == "42"
    assert providers._imap_smtp_email_read("42", principal_id="p")["body"] == "Body text"

    assert "web_searcher" in providers.resolve_plugin_provider_kwargs(plugin_name="search_plugin", provider_name="builtin_search_provider")
    assert "email_reader" in providers.resolve_plugin_provider_kwargs(plugin_name="email_plugin", provider_name="custom_api_provider")
    assert "email_reader" in providers.resolve_plugin_provider_kwargs(plugin_name="email_plugin", provider_name="imap_smtp_provider")
    assert "calendar_lister" in providers.resolve_plugin_provider_kwargs(plugin_name="calendar_plugin", provider_name="google_workspace_provider")
    assert "image_generator" in providers.resolve_plugin_provider_kwargs(plugin_name="media_plugin", provider_name="local_media_provider")
    with pytest.raises(ValueError):
        providers.resolve_plugin_provider_kwargs(plugin_name="unknown", provider_name="x")
