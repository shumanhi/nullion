from __future__ import annotations

import gzip
import socket
import zlib
from types import SimpleNamespace

import pytest

from nullion.tools import ToolRegistry


class ResponseDouble:
    def __init__(self, body: bytes, *, encoding: str | None = None, content_type: str = "text/html", status: int = 200):
        self._body = body
        self._offset = 0
        self.status = status
        self.headers = SimpleNamespace(
            get=lambda name, default=None: encoding if name == "Content-Encoding" else default,
            get_content_type=lambda: content_type,
        )

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._body) - self._offset
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_read_response_decodes_gzip_deflate_and_plain_payloads() -> None:
    from nullion.chat_backend import _read_response

    assert _read_response(ResponseDouble(b"plain text"), max_bytes=20) == b"plain text"
    assert _read_response(ResponseDouble(gzip.compress(b"hello gzip"), encoding="gzip"), max_bytes=20) == b"hello gzip"

    compressed = zlib.compress(b"hello deflate")
    assert _read_response(ResponseDouble(compressed, encoding="deflate"), max_bytes=20) == b"hello deflate"

    raw_deflate = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    raw = raw_deflate.compress(b"raw deflate") + raw_deflate.flush()
    assert _read_response(ResponseDouble(raw, encoding="deflate"), max_bytes=20) == b"raw deflate"


def test_validate_fetch_url_rejects_unsafe_hosts_and_rewrites_resolved_host(monkeypatch) -> None:
    from nullion import chat_backend

    assert chat_backend._validate_fetch_url("https://93.184.216.34/path") == "https://93.184.216.34/path"

    with pytest.raises(ValueError, match="Only http://"):
        chat_backend._validate_fetch_url("file:///tmp/x")
    with pytest.raises(ValueError, match="Localhost"):
        chat_backend._validate_fetch_url("https://localhost")
    with pytest.raises(ValueError, match="globally routable"):
        chat_backend._validate_fetch_url("https://127.0.0.1")

    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda host, port, proto: [(socket.AF_INET, socket.SOCK_STREAM, proto, "", ("93.184.216.34", port or 443))],
    )
    assert chat_backend._validate_fetch_url("https://example.com/a") == "https://93.184.216.34/a"

    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda host, port, proto: [(socket.AF_INET, socket.SOCK_STREAM, proto, "", ("10.0.0.5", port or 443))],
    )
    with pytest.raises(ValueError, match="globally routable"):
        chat_backend._validate_fetch_url("https://internal.example")

    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [])
    with pytest.raises(ValueError, match="routable address"):
        chat_backend._validate_fetch_url("https://empty.example")

    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("dns")))
    with pytest.raises(ValueError, match="Unable to resolve"):
        chat_backend._validate_fetch_url("https://bad.example")


def test_fetch_url_snapshot_and_external_context(monkeypatch) -> None:
    from nullion import chat_backend

    opened: list[tuple[str, int]] = []

    def fake_urlopen(request, timeout):
        opened.append((request.full_url, timeout))
        html = b"<html><title>Example</title><body><h1>Hello</h1><p>World</p></body></html>"
        return ResponseDouble(html, status=201)

    monkeypatch.setattr(chat_backend, "_validate_fetch_url", lambda url: url)
    monkeypatch.setattr(chat_backend.urllib.request, "urlopen", fake_urlopen)

    snapshot = chat_backend.fetch_url_snapshot("https://example.com", timeout_seconds=3)

    assert opened == [("https://example.com", 3)]
    assert snapshot["status_code"] == 201
    assert snapshot["title"] == "Example"
    assert "Hello World" in snapshot["text"]
    assert "Title: Example" in chat_backend.fetch_external_context("see https://example.com")

    monkeypatch.setattr(chat_backend, "search_web", lambda message, limit: [{"title": "A", "url": "https://a.example", "snippet": "S"}])
    assert "A" in chat_backend.fetch_external_context("news please", limit=1)


def test_search_web_uses_fallbacks_filters_unsafe_urls_and_raises_when_blocked(monkeypatch) -> None:
    from nullion import chat_backend

    monkeypatch.setattr(chat_backend, "_ddg_html_search", lambda query, limit: [])
    monkeypatch.setattr(
        chat_backend,
        "_bing_html_search",
        lambda query, limit: [
            {"title": "Unsafe", "url": "http://127.0.0.1", "snippet": ""},
            {"title": "Safe", "url": "https://example.com", "snippet": "ok"},
        ],
    )
    monkeypatch.setattr(
        chat_backend,
        "_validate_fetch_url",
        lambda url: (_ for _ in ()).throw(ValueError("unsafe")) if "127.0.0.1" in url else url,
    )

    assert chat_backend.search_web("query", limit=5) == [{"title": "Safe", "url": "https://example.com", "snippet": "ok"}]

    monkeypatch.setattr(chat_backend, "_bing_html_search", lambda query, limit: [])
    with pytest.raises(RuntimeError, match="no usable results"):
        chat_backend.search_web("query", limit=5)


def test_generate_chat_reply_dispatches_orchestrator_and_handles_approval_errors(monkeypatch) -> None:
    from nullion import chat_backend

    class Orchestrator:
        def __init__(self):
            self.calls = []

        def dispatch_request_sync(self, **kwargs):
            return SimpleNamespace(dispatched=False)

        def run_turn(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(final_text="  done  ")

    orchestrator = Orchestrator()
    reply = chat_backend.generate_chat_reply(
        message="hello",
        conversation_context="previous",
        memory_context="likes tests",
        active_tool_registry=ToolRegistry(),
        agent_orchestrator=orchestrator,
        conversation_id="conv",
        principal_id="operator",
    )
    assert reply == "done"
    assert orchestrator.calls[0]["conversation_history"][0]["role"] == "system"

    class ApprovalOrchestrator:
        def run_turn(self, **kwargs):
            return SimpleNamespace(final_text="", suspended_for_approval=True, approval_id="ap-1")

    assert (
        chat_backend.generate_chat_reply(message="hello", agent_orchestrator=ApprovalOrchestrator())
        == "Tool approval requested: ap-1"
    )

    class EmptyOrchestrator:
        def run_turn(self, **kwargs):
            return SimpleNamespace(final_text="")

    with pytest.raises(chat_backend.ChatBackendUnavailableError, match="empty reply"):
        chat_backend.generate_chat_reply(message="hello", agent_orchestrator=EmptyOrchestrator())

    class ExplodingOrchestrator:
        def run_turn(self, **kwargs):
            raise RuntimeError("boom")

    with pytest.raises(chat_backend.ChatBackendUnavailableError, match="boom"):
        chat_backend.generate_chat_reply(message="hello", agent_orchestrator=ExplodingOrchestrator())


def test_generate_chat_reply_uses_dispatch_fast_path(monkeypatch) -> None:
    from nullion import chat_backend

    class DispatchingOrchestrator:
        def dispatch_request_sync(self, **kwargs):
            return SimpleNamespace(dispatched=True, task_count=2, acknowledgment="")

        def run_turn(self, **kwargs):
            raise AssertionError("dispatch fast path should return first")

    monkeypatch.setattr(chat_backend, "_feature_enabled", lambda name: True)
    monkeypatch.setattr(
        chat_backend.TaskPlanner,
        "build_execution_plan",
        lambda self, **kwargs: SimpleNamespace(can_dispatch_mini_agents=True),
    )

    assert (
        chat_backend.generate_chat_reply(message="please research", agent_orchestrator=DispatchingOrchestrator())
        == "Working on 2 task(s)."
    )
