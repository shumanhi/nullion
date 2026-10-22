from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
import json
import os
from types import SimpleNamespace
import re
from pathlib import Path
import socket
from socketserver import TCPServer
import threading
import time
import urllib.error
import urllib.request

import pytest
from playwright.sync_api import expect


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run web surface e2e tests",
)


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


def _write_blank_pdf(path: Path) -> None:
    from pypdf import PdfWriter

    path.parent.mkdir(parents=True, exist_ok=True)
    writer = PdfWriter()
    writer.pdf_header = "%PDF-1.4"
    writer.add_blank_page(width=200, height=200)
    with path.open("wb") as fh:
        writer.write(fh)


class _ThreadedTCPServer(TCPServer):
    allow_reuse_address = True


class _MockWebServer:
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
            raise AssertionError("mock web server thread did not stop after e2e")


class _UvicornThread:
    def __init__(self, app, port: int) -> None:  # noqa: ANN001
        import uvicorn

        self.base_url = f"http://127.0.0.1:{port}"
        self.server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        deadline = time.monotonic() + 15
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{self.base_url}/api/health", timeout=1) as response:
                    if response.status == 200:
                        return self
            except (OSError, TimeoutError, urllib.error.URLError) as exc:
                last_error = exc
            time.sleep(0.1)
        raise AssertionError(f"web app did not become healthy for browser e2e: {last_error!r}")

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        self.server.should_exit = True
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise AssertionError("web app thread did not stop after browser e2e")


def _open_page(base_url: str, *, configure_page=None):
    from playwright.sync_api import sync_playwright

    manager = sync_playwright()
    playwright = None
    browser = None
    try:
        playwright = manager.start()
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        context.add_init_script(
            """
            localStorage.setItem('nullion_chat_restore_suppressed', 'true');
            localStorage.setItem('nullion_conv_id', 'web:playwright:' + Math.random().toString(36).slice(2));
            """
        )
        page = context.new_page()
        errors: list[str] = []
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.on(
            "console",
            lambda msg: errors.append(msg.text) if msg.type == "error" else None,
        )
        if configure_page is not None:
            configure_page(page)
        page.goto(base_url, wait_until="domcontentloaded")
        page.locator("#user-input").wait_for(state="visible", timeout=10000)
        return playwright, browser, page, errors
    except Exception:
        if browser is not None:
            browser.close()
        if playwright is not None:
            playwright.stop()
        raise


def _wait_for_settings_tab_loaded(page, tab_name: str):
    if tab_name in {"Preferences", "Security"}:
        # A visible form can still be awaiting its real preferences response.
        # Wait for hydration before editing; no sleeps or mocked UI state.
        from playwright.sync_api import expect
        expect(page.locator("#pref-timezone")).not_to_have_value("")


def _open_settings_tab(page, tab_name: str):
    from playwright.sync_api import expect

    page.locator("#settings-btn").click()
    expect(page.locator("#settings-overlay")).to_be_visible()
    page.locator(".settings-nav button", has_text=tab_name).click()
    _wait_for_settings_tab_loaded(page, tab_name)


def _request_post_json(request):
    post_data_json = request.post_data_json
    return post_data_json() if callable(post_data_json) else post_data_json


@pytest.mark.parametrize('width', [400, 1280])
def test_output_lists_links_and_literal_code_render_through_real_web_chat(tmp_path, monkeypatch, width):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    report='1. **公園** — 19:30\n[Source](https://example.com)\n\n2. **Plaza** — 20:00\n\n| Name | Time |\n| --- | --- |\n| | 19:30 | 未対応 |\n\n```python\n    <b>literal</b>\n```'
    class Model:
        def create(self, **kwargs):
            return {'stop_reason':'end_turn', 'content':[{'type':'text','text':report}]}
    runtime=bootstrap_persistent_runtime(tmp_path/'output.db')
    model=Model(); runtime.model_client=model
    app=create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=ToolRegistry())
    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors=_open_page(server.base_url)
        try:
            page.set_viewport_size({'width':width,'height':800})
            request='形式を確認してください\n\n```python\n    <b>literal</b>\n```'
            page.locator('#user-input').fill(request)
            page.locator('#send-btn').click()
            bubble=page.locator('.msg.bot .bubble').last
            expect(bubble.locator('strong', has_text='公園')).to_be_visible()
            expect(bubble.locator('strong', has_text='Plaza')).to_be_visible()
            code=bubble.locator('pre code')
            expect(code).to_have_text('    <b>literal</b>')
            expect(code).to_have_attribute('class','language-python')
            anchor=bubble.get_by_role('link',name='Source',exact=True)
            expect(anchor).to_have_attribute('href','https://example.com')
            assert anchor.evaluate('e => getComputedStyle(e).color') in {'rgb(147, 197, 253)','rgb(196, 181, 253)'}
            assert code.evaluate('e => getComputedStyle(e.parentElement).whiteSpace')=='pre'
            expect(bubble.locator('tbody tr td')).to_have_count(3)
            assert bubble.locator('tbody tr td').all_text_contents() == ['', '19:30', '未対応']
            assert bubble.evaluate('e => e.scrollWidth <= e.clientWidth')
            stored=[e for e in runtime.store.list_conversation_events(page.evaluate("localStorage.getItem('nullion_conv_id')")) if e.get('event_type')=='conversation.chat_turn']
            assert stored[-1]['assistant_reply']==report
            assert stored[-1]['user_message']==request
            screenshots=Path(__file__).resolve().parents[2]/'.e2e-artifacts/full-output-review-20261010'
            screenshots.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(screenshots/f'web-output-{width}.png'))
            assert not errors
        finally:
            browser.close(); playwright.stop()


def test_web_chat_history_collapses_runtime_and_browser_duplicate_bot_reply(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    import nullion.chat_store as chat_store_module
    from nullion.web_app import _remember_web_chat_turn

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    chat_store_module._store = None
    app, runtime, _registry = _web_app(tmp_path)
    client = TestClient(app)
    conversation_id = "web:e2e-dup"
    reply = "Here’s the screenshot of **cnn.com**:\n\n`artifacts/screenshot.png`"
    artifact_metadata = {
        "artifacts": [
            {
                "name": "screenshot.png",
                "url": "/api/artifacts/abc123",
                "media_type": "image/png",
            }
        ],
        "activity": [{"id": "respond", "label": "Writing response", "status": "done"}],
    }

    _remember_web_chat_turn(
        runtime,
        conversation_id=conversation_id,
        user_message="take a screenshot of cnn.com",
        assistant_reply=reply,
    )
    browser_save = client.post(
        f"/api/chat/history/{conversation_id}/message",
        json={"role": "bot", "text": reply, "metadata": artifact_metadata},
    )
    history = client.get(f"/api/chat/history/{conversation_id}")

    assert browser_save.status_code == 200
    assert history.status_code == 200
    messages = history.json()["messages"]
    assert [(message["role"], message["text"]) for message in messages] == [
        ("user", "take a screenshot of cnn.com"),
        ("bot", reply),
    ]
    assert messages[-1]["metadata"] == artifact_metadata


@pytest.mark.parametrize(
    ("reset_button", "confirm_text"),
    [
        ("#composer-archive-btn", "Archive"),
        ("#composer-clear-btn", "Delete"),
    ],
)
def test_browser_reopen_after_conversation_reset_stays_blank(
    tmp_path: Path,
    monkeypatch,
    reset_button: str,
    confirm_text: str,
) -> None:
    from fastapi.testclient import TestClient
    from playwright.sync_api import expect, sync_playwright

    import nullion.chat_store as chat_store_module

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    chat_store_module._store = None
    app, _runtime, _registry = _web_app(tmp_path)
    older_id = "web:e2e-older-reset-thread"
    current_id = f"web:e2e-current-reset-{confirm_text.lower()}"

    with TestClient(app) as client:
        assert client.post(
            f"/api/chat/history/{older_id}/message",
            json={"role": "user", "text": "older thread should stay hidden after reset"},
        ).status_code == 200
        assert client.post(
            f"/api/chat/history/{current_id}/message",
            json={"role": "user", "text": "current thread being reset"},
        ).status_code == 200

    with _UvicornThread(app, _free_port()) as server:
        manager = sync_playwright()
        playwright = manager.start()
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        errors: list[str] = []
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        try:
            page.goto(server.base_url, wait_until="domcontentloaded")
            page.locator("#user-input").wait_for(state="visible", timeout=10000)
            page.evaluate(
                f"""
                () => {{
                  localStorage.setItem('nullion_conv_id', {json.dumps(current_id)});
                  localStorage.removeItem('nullion_chat_restore_suppressed');
                }}
                """
            )
            page.reload(wait_until="domcontentloaded")
            page.locator("#user-input").wait_for(state="visible", timeout=10000)
            assert page.evaluate("refreshLocalCsrfToken()") is True
            expect(page.locator("#messages")).to_contain_text("current thread being reset", timeout=5000)

            page.locator(reset_button).click()
            expect(page.locator("#confirm-ok")).to_have_text(confirm_text)
            method = "POST" if confirm_text == "Archive" else "DELETE"
            with page.expect_response(
                lambda response: "/api/chat/history/" in response.url
                and response.request.method == method
            ) as reset_response_info:
                page.locator("#confirm-ok").click()
            assert reset_response_info.value.status == 200
            expect(page.locator("#messages")).to_contain_text("Runtime session opened", timeout=5000)
            expect(page.locator("#messages")).not_to_contain_text("current thread being reset", timeout=1000)

            page.evaluate("localStorage.removeItem('nullion_chat_restore_suppressed')")
            page.reload(wait_until="domcontentloaded")
            page.locator("#user-input").wait_for(state="visible", timeout=10000)
            expect(page.locator("#messages")).to_contain_text("Runtime session opened", timeout=5000)
            expect(page.locator("#messages")).not_to_contain_text("older thread should stay hidden after reset", timeout=1000)
            expect(page.locator("#messages")).not_to_contain_text("current thread being reset", timeout=1000)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_backend_setting_hot_reloads_live_tool_registry(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    import nullion.auth as auth_module
    import nullion.plugins.browser_plugin as browser_plugin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolInvocation,
        ToolRegistry,
        ToolResult,
        ToolRiskLevel,
        ToolSideEffectClass,
        ToolSpec,
    )

    env_path = tmp_path / ".env"
    saved_credentials = {"browser_backend": "auto"}
    registered_backends: list[str] = []
    visible_open_calls: list[str] = []

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "auto")
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "true")
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.setattr(web_app, "_read_credentials_json", lambda: dict(saved_credentials))
    monkeypatch.setattr(auth_module, "load_stored_credentials", lambda: dict(saved_credentials))

    def write_credentials(creds):
        saved_credentials.clear()
        saved_credentials.update(dict(creds))

    monkeypatch.setattr(web_app, "_write_credentials_json", write_credentials)

    def browser_spec() -> ToolSpec:
        return ToolSpec(
            name="browser_navigate",
            description="Fake browser navigate",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=30,
        )

    def browser_open_spec() -> ToolSpec:
        return ToolSpec(
            name="browser_open",
            description="Fake browser open",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=10,
        )

    def fake_register_browser_tools(registry: ToolRegistry, *, policy=None, model_client_getter=None) -> None:  # noqa: ANN001
        backend = web_app.os.environ.get("NULLION_BROWSER_BACKEND", "")
        registered_backends.append(backend)
        registry.mark_plugin_installed("browser_plugin")

        def navigate(invocation: ToolInvocation) -> ToolResult:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name="browser_navigate",
                status="completed",
                output={
                    "backend": backend,
                    "url": invocation.arguments.get("url"),
                    "session_id": invocation.arguments.get("session_id", "default"),
                },
            )

        registry.register(browser_spec(), navigate)
        registry.register(
            browser_open_spec(),
            lambda invocation: ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name="browser_open",
                status="completed",
                output={
                    "backend": backend,
                    "session_id": invocation.arguments.get("session_id", "default"),
                },
            ),
        )

    monkeypatch.setattr(browser_plugin, "register_browser_tools", fake_register_browser_tools)

    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])

    def stale_visible_navigate(invocation: ToolInvocation) -> ToolResult:
        visible_open_calls.append(str(invocation.arguments.get("url") or ""))
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name="browser_navigate",
            status="completed",
            output={"backend": "auto-stale", "url": invocation.arguments.get("url")},
        )

    registry.mark_plugin_installed("browser_plugin")
    registry.register(browser_spec(), stale_visible_navigate)
    registry.register(
        browser_open_spec(),
        lambda invocation: ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name="browser_open",
            status="completed",
            output={
                "backend": "auto-stale",
                "session_id": invocation.arguments.get("session_id", "default"),
            },
        ),
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(runtime, orchestrator=None, registry=registry)

    with TestClient(app) as client:
        before = client.post("/api/browser/open", json={"url": "https://example.com"}).json()
        quick_open = client.post("/api/browser/open", json={}).json()
        save = client.post(
            "/api/config",
            json={"browser_backend": "playwright", "browser_enabled": True},
        )
        direct_tool_result = registry.invoke(
            ToolInvocation(
                invocation_id="after-save",
                tool_name="browser_navigate",
                principal_id="web:e2e",
                arguments={"url": "https://example.com", "session_id": "default"},
            )
        )
        visible_open = client.post("/api/browser/open", json={"url": "https://example.com"})
        config = client.get("/api/config").json()

    assert before["ok"] is True
    assert before["result"]["backend"] == "auto-stale"
    assert quick_open["ok"] is True
    assert quick_open["result"]["session_id"] == "default"
    assert save.status_code == 200
    assert save.json()["ok"] is True
    assert registered_backends == ["playwright"]
    assert direct_tool_result.status == "completed"
    assert direct_tool_result.output["backend"] == "playwright"
    assert visible_open.status_code == 400
    assert visible_open.json()["backend"] == "playwright"
    assert "headlessly" in visible_open.json()["error"]
    assert config["browser_backend"] == "playwright"
    assert config["browser_tools_available"] is True
    assert visible_open_calls == ["https://example.com"]


def test_browser_ui_auto_preferred_browser_is_saved_and_used_for_open(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app
    import nullion.auth as auth_module
    import nullion.plugins.browser_plugin as browser_plugin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolInvocation,
        ToolRegistry,
        ToolResult,
        ToolRiskLevel,
        ToolSideEffectClass,
        ToolSpec,
    )

    env_path = tmp_path / ".env"
    saved_credentials = {
        "browser_backend": "auto",
        "browser_preferred": "brave",
        "browser_cdp_url": "http://127.0.0.1:9222",
    }

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "auto")
    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "brave")
    monkeypatch.setenv("NULLION_BROWSER_CDP_URL", "http://127.0.0.1:9222")
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "true")
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.setattr(web_app, "_read_credentials_json", lambda: dict(saved_credentials))
    monkeypatch.setattr(auth_module, "load_stored_credentials", lambda: dict(saved_credentials))

    def write_credentials(creds):
        saved_credentials.clear()
        saved_credentials.update(dict(creds))

    monkeypatch.setattr(web_app, "_write_credentials_json", write_credentials)

    def fake_register_browser_tools(registry: ToolRegistry, *, policy=None, model_client_getter=None) -> None:  # noqa: ANN001
        registry.mark_plugin_installed("browser_plugin")

        def navigate(invocation: ToolInvocation) -> ToolResult:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name="browser_navigate",
                status="completed",
                output={
                    "backend": web_app.os.environ.get("NULLION_BROWSER_BACKEND"),
                    "preferred": web_app.os.environ.get("NULLION_BROWSER_PREFERRED"),
                    "url": invocation.arguments.get("url"),
                },
            )

        registry.register(
            ToolSpec(
                name="browser_navigate",
                description="Fake browser navigate",
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=30,
            ),
            navigate,
        )

    monkeypatch.setattr(browser_plugin, "register_browser_tools", fake_register_browser_tools)

    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(runtime, orchestrator=None, registry=registry)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "General")
            mode = page.locator("#cfg-browser-backend")
            preferred = page.locator("#cfg-browser-preferred")
            cdp_url = page.locator("#cfg-browser-cdp-url")

            expect(preferred).to_have_value("brave")
            expect(cdp_url).to_have_value("http://127.0.0.1:9223")
            mode.select_option("auto")
            preferred.select_option("chrome")
            _close_settings_tab_after_save(page, "General")

            config = page.evaluate("fetch('/api/config').then(r => r.json())")
            opened = page.evaluate(
                "fetch('/api/browser/open', {method: 'POST', headers: {'Content-Type': 'application/json'}, "
                "body: JSON.stringify({url: 'https://example.com'})}).then(r => r.json())"
            )

            assert config["browser_backend"] == "auto"
            assert config["browser_preferred"] == "chrome"
            assert config["browser_cdp_url"] == "http://127.0.0.1:9222"
            assert opened["ok"] is True
            assert opened["result"]["backend"] == "auto"
            assert opened["result"]["preferred"] == "chrome"
            assert saved_credentials["browser_preferred"] == "chrome"
            assert saved_credentials["browser_cdp_url"] == "http://127.0.0.1:9222"
            assert 'NULLION_BROWSER_PREFERRED="chrome"' in env_path.read_text()
            assert 'NULLION_BROWSER_CDP_URL="http://127.0.0.1:9222"' in env_path.read_text()
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_search_provider_without_api_hot_reloads_to_headless_browser_not_direct_fetch(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    import nullion.auth as auth_module
    import nullion.plugins.browser_plugin as browser_plugin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolInvocation,
        ToolRegistry,
        ToolResult,
        ToolRiskLevel,
        ToolSideEffectClass,
        ToolSpec,
        register_search_plugin,
        register_web_fetch_tool,
    )

    env_path = tmp_path / ".env"
    registered_backends: list[str] = []

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "true")
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "true")
    monkeypatch.delenv("NULLION_BROWSER_BACKEND", raising=False)
    monkeypatch.delenv("NULLION_DIRECT_WEB_FETCH_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_BRAVE_SEARCH_API_KEY", raising=False)
    monkeypatch.delenv("BRAVE_SEARCH_API_KEY", raising=False)
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.setattr(auth_module, "load_stored_credentials", lambda: {})

    def fake_register_browser_tools(registry: ToolRegistry, *, policy=None, model_client_getter=None) -> None:  # noqa: ANN001
        backend = web_app.os.environ.get("NULLION_BROWSER_BACKEND", "")
        registered_backends.append(backend)
        registry.mark_plugin_installed("browser_plugin")

        def navigate(invocation: ToolInvocation) -> ToolResult:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name="browser_navigate",
                status="completed",
                output={"backend": backend, "url": invocation.arguments.get("url")},
            )

        registry.register(
            ToolSpec(
                name="browser_navigate",
                description="Fake browser navigate",
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=30,
            ),
            navigate,
        )

    monkeypatch.setattr(browser_plugin, "register_browser_tools", fake_register_browser_tools)

    registry = ToolRegistry(plugin_registration_allowed=True, filesystem_allowed_roots=[tmp_path])
    register_web_fetch_tool(registry, web_fetcher=lambda url, timeout: {"url": url, "text": "stale direct fetch"})
    register_search_plugin(registry, web_searcher=lambda query, limit: [{"title": query, "url": "https://example.com"}])
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(runtime, orchestrator=None, registry=registry)

    with TestClient(app) as client:
        save = client.post("/api/config", json={"search_provider": "builtin_search_provider"})
        config = client.get("/api/config").json()

    tool_names = {spec.name for spec in registry.list_specs()}
    assert save.status_code == 200
    assert save.json()["ok"] is True
    assert "web_fetch" not in tool_names
    assert "web_search" not in tool_names
    assert "browser_navigate" in tool_names
    assert registered_backends[-1] == "playwright"
    assert config["browser_tools_available"] is True


def test_search_provider_with_api_hot_reloads_to_search_tool_and_browser_evidence(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    import nullion.auth as auth_module
    import nullion.plugins.browser_plugin as browser_plugin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolInvocation,
        ToolRegistry,
        ToolResult,
        ToolRiskLevel,
        ToolSideEffectClass,
        ToolSpec,
        register_web_fetch_tool,
    )

    env_path = tmp_path / ".env"
    registered_backends: list[str] = []

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "true")
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "true")
    monkeypatch.setenv("NULLION_BRAVE_SEARCH_API_KEY", "brave-test-key")
    monkeypatch.delenv("NULLION_DIRECT_WEB_FETCH_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_BROWSER_BACKEND", raising=False)
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.setattr(auth_module, "load_stored_credentials", lambda: {})

    def fake_register_browser_tools(registry: ToolRegistry, *, policy=None, model_client_getter=None) -> None:  # noqa: ANN001
        backend = web_app.os.environ.get("NULLION_BROWSER_BACKEND", "")
        registered_backends.append(backend)
        registry.mark_plugin_installed("browser_plugin")
        registry.register(
            ToolSpec(
                name="browser_navigate",
                description="Fake browser navigate",
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=30,
            ),
            lambda invocation: ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={"backend": backend, "url": invocation.arguments.get("url")},
            ),
        )

    monkeypatch.setattr(browser_plugin, "register_browser_tools", fake_register_browser_tools)

    registry = ToolRegistry(plugin_registration_allowed=True, filesystem_allowed_roots=[tmp_path])
    register_web_fetch_tool(registry, web_fetcher=lambda url, timeout: {"url": url, "text": "stale direct fetch"})
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(runtime, orchestrator=None, registry=registry)

    with TestClient(app) as client:
        save = client.post("/api/config", json={"search_provider": "brave_search_provider"})
        config = client.get("/api/config").json()

    tool_names = {spec.name for spec in registry.list_specs()}
    assert save.status_code == 200
    assert save.json()["ok"] is True
    assert "web_fetch" not in tool_names
    assert "web_search" in tool_names
    assert "browser_navigate" in tool_names
    assert registered_backends[-1] == "playwright"
    assert config["search_provider"] == "brave_search_provider"
    assert config["browser_tools_available"] is True


def _set_checked(locator, checked: bool) -> None:
    locator.evaluate(
        """(element, checked) => {
            element.checked = checked;
            element.dispatchEvent(new Event('input', { bubbles: true }));
            element.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        checked,
    )


def _select_option(locator, value: str) -> None:
    locator.evaluate(
        """(element, value) => {
            element.value = value;
            element.dispatchEvent(new Event('input', { bubbles: true }));
            element.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        value,
    )


def _close_settings_tab_after_save(page, tab_name: str):
    from playwright.sync_api import expect

    page.locator('.modal-footer button:has-text("Save changes")').click()
    expect(page.locator("#save-feedback")).to_have_text("✓ Saved", timeout=5000)
    page.locator("#settings-overlay .modal-close").click()
    expect(page.locator("#settings-overlay")).not_to_be_visible()
    page.locator("#settings-btn").click()
    expect(page.locator("#settings-overlay")).to_be_visible()
    page.locator(".settings-nav button", has_text=tab_name).click()
    _wait_for_settings_tab_loaded(page, tab_name)


def _set_pref_chip(page, group_id: str, value: str):
    page.locator(f"#{group_id} .pref-chip[data-val='{value}']").click()


def _assert_pref_chip(page, group_id: str, value: str):
    from playwright.sync_api import expect

    expect(page.locator(f"#{group_id} .pref-chip[data-val='{value}']")).to_have_class(re.compile(r"\bactive\b"))


def test_web_ui_shell_exposes_chat_upload_tools_and_approval_controls(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient

    app, _runtime, _registry = _web_app(tmp_path)

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    html = response.text
    for marker in (
        'id="chat-panel"',
        'id="user-input"',
        'id="send-btn"',
        'id="file-input"',
        'id="tool-chip"',
        'id="approvals-list"',
        "async function approveRequest(",
        "async function rejectRequest(",
        "fetch('/api/upload'",
        "Upload failed:",
        "Attachment upload failed.",
    ):
        assert marker in html


def test_http_chat_response_contract_and_empty_message_errors(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import platform_chat

    artifact = {
        "id": "artifact-1",
        "name": "answer.txt",
        "path": str(tmp_path / "answer.txt"),
        "media_type": "text/plain",
        "size_bytes": 11,
        "url": "/api/artifacts/artifact-1",
    }
    seen_requests = []

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        del runtime_arg
        seen_requests.append(request)
        assert request.platform == "web"
        assert request.conversation_id == "web:e2e"
        assert request.attachments == [{"name": "notes.txt", "path": "/tmp/notes.txt"}]
        return platform_chat.PlatformChatResponse(
            text=f"reply:{request.text}",
            artifacts=[artifact],
            thinking="private notes",
        )

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    app, _runtime, _registry = _web_app(tmp_path)

    with TestClient(app) as client:
        empty = client.post("/api/chat", json={"text": "   "})
        reset = client.post("/api/chat", json={"text": "/new", "conversation_id": "web:e2e"})
        response = client.post(
            "/api/chat",
            json={
                "text": "make a file",
                "conversation_id": "web:e2e",
                "stream": False,
                "show_thinking": True,
                "attachments": [{"name": "notes.txt", "path": "/tmp/notes.txt"}],
            },
        )

    assert empty.status_code == 400
    assert empty.json() == {"type": "error", "text": "Message is empty."}
    assert reset.json()["type"] == "conversation_reset"
    payload = response.json()
    assert payload["type"] == "message"
    assert payload["text"] == "reply:make a file"
    assert payload["thinking"] == "private notes"
    assert payload["artifacts"] == [artifact]
    assert [request.text for request in seen_requests] == ["make a file"]


def test_http_chat_fast_fails_on_pending_model_health_issue(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    class Orchestrator:
        model_client = None

        def run_turn(self, **kwargs):  # noqa: ANN003
            raise AssertionError("known model outage should not invoke the model turn")

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_doctor_action(
        {
            "action_id": "act-model-api",
            "owner": "system",
            "status": "pending",
            "action_type": "runtime",
            "recommendation_code": "model_api_unreachable",
            "summary": "Model API is unreachable",
            "severity": "high",
            "reason": None,
            "error": None,
            "details": {"service_id": "model_api"},
        }
    )
    app = create_app(
        runtime,
        orchestrator=Orchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        response = client.post("/api/chat", json={"text": "hello", "conversation_id": "web:e2e"})

    assert response.status_code == 200
    assert response.json()["text"] == (
        "Chat backend is temporarily unavailable due to model API connectivity. "
        "Try again after the service recovers."
    )
    assert runtime.store.get_doctor_action("act-model-api")["status"] == "pending"


def test_web_chat_plain_turn_latency_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import crons, turn_context_policy, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "web-latency-home"))

    runtime = bootstrap_persistent_runtime(tmp_path / "web-latency-runtime.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    model_start_ms: list[float] = []
    total_ms: list[float] = []
    scoped_tool_counts: list[int] = []
    attachment_model_calls: list[dict[str, object]] = []
    turn_started_at = 0.0

    class SlowAttachmentModel:
        def create(self, **kwargs):
            if any(tool.get("name") == "select_attachment_format" for tool in kwargs.get("tools", [])):
                attachment_model_calls.append(kwargs)
                time.sleep(1.5)
                return {"content": [{"type": "tool_use", "name": "select_attachment_format", "input": {"extension": None, "embedded_media_extensions": []}}]}
            return {"content": [{"type": "text", "text": '{"disposition":"independent","confidence":0.99}'}]}

    class InstantOrchestrator:
        model_client = SlowAttachmentModel()

        def run_turn(self, **kwargs):  # noqa: ANN001
            model_start_ms.append((time.perf_counter() - turn_started_at) * 1000)
            scoped_tool_counts.append(len(kwargs["tool_registry"].list_tool_definitions()))
            return SimpleNamespace(
                final_text="hello",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text="",
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_enforce_web_response_fulfillment", lambda runtime, **kwargs: (kwargs["reply"], True))
    monkeypatch.setattr(crons, "list_crons", lambda *args, **kwargs: [])

    def fail_slow_scope_dependency() -> list[dict[str, object]]:
        raise AssertionError("plain independent web turns must not enter the tool-scope classifier path")

    monkeypatch.setattr(turn_context_policy, "_active_connector_provider_context", fail_slow_scope_dependency)

    app = web_app.create_app(runtime, orchestrator=InstantOrchestrator(), registry=registry)
    with TestClient(app) as client:
        for index, prompt in enumerate(("hi", "こんにちは", "مرحبا", "bonjour", "hola")):
            conversation_id = f"web:latency-budget-{index}"
            runtime.store.add_conversation_event(
                {
                    "conversation_id": conversation_id,
                    "event_type": "conversation.chat_turn",
                    "user_message": "prior structured tool turn",
                    "assistant_reply": "done",
                    "tool_results": [{"tool_name": "connector_request", "status": "completed", "output": {}}],
                }
            )
            turn_started_at = time.perf_counter()
            response = client.post(
                "/api/chat",
                json={"text": prompt, "conversation_id": conversation_id},
            )
            total_ms.append((time.perf_counter() - turn_started_at) * 1000)
            assert response.status_code == 200
            assert response.json()["text"] == "hello"
            assert response.json().get("artifacts", []) == []
            turns = [event for event in runtime.store.list_conversation_events(conversation_id) if event.get("event_type") == "conversation.chat_turn" and event.get("user_message") == prompt]
            assert turns and turns[-1].get("assistant_reply") == "hello"
            assert turns[-1].get("tool_results", []) == []

    assert attachment_model_calls == []
    average_model_start_ms = sum(model_start_ms) / len(model_start_ms)
    average_total_ms = sum(total_ms) / len(total_ms)
    assert average_model_start_ms < 350, f"web plain-chat pre-model latency regressed: avg={average_model_start_ms:.1f}ms samples={model_start_ms!r}"
    assert max(model_start_ms) < 700, f"web plain-chat pre-model spike: samples={model_start_ms!r}"
    assert average_total_ms < 650, f"web plain-chat average latency regressed: avg={average_total_ms:.1f}ms samples={total_ms!r}"
    assert max(total_ms) < 1200, f"web plain-chat latency spike: samples={total_ms!r}"
    assert max(scoped_tool_counts) <= 10, f"web plain-chat carried too many tools: {scoped_tool_counts!r}"


def test_web_chat_delivers_browser_summary_without_verification_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    raw_text = (
        "Sign in\n"
        "$70 Cheap Route A to Route B\n"
        "Cheap Route A to Route B\n"
        "$70\n"
        "Route A\n"
        "$186 Roundtrip, found 2 hours ago\n"
        "Roundtrip\n"
        "$186\n"
        "just found\n"
        "$187"
    )

    class RawBrowserSnippetOrchestrator:
        model_client = SimpleNamespace(name="raw-browser-snippet-e2e")

        def run_turn(self, **kwargs):  # noqa: ANN001
            return SimpleNamespace(
                final_text="The page lists fares from $70; a round trip was listed at $186.",
                tool_results=[
                    ToolResult(
                        "extract",
                        "browser_extract_text",
                        "completed",
                        {"text": raw_text},
                    )
                ],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text="",
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-browser-price-snippet.db")
    app = web_app.create_app(
        runtime,
        orchestrator=RawBrowserSnippetOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={
                "text": "Find live options for this route and date range.",
                "conversation_id": "web:browser-price-snippet",
            },
        )

    assert response.status_code == 200
    text = response.json()["text"]
    assert text == "The page lists fares from $70; a round trip was listed at $186."
    events = runtime.store.list_conversation_events("web:browser-price-snippet")
    turns = [e for e in events if e.get("event_type") == "conversation.chat_turn"]
    assert turns[-1]["assistant_reply"] == text
    assert turns[-1]["tool_results"][0]["tool_name"] == "browser_extract_text"


def test_web_chat_scheduler_update_reply_overrides_stale_cron_listing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    cron_list_message = "\n".join(
        [
            '1. Email deadline triage · Status: enabled · Run by name: run_cron name="Email deadline triage"',
            '2. OpenClaw import — Provincetown Airbnb Monitor · Status: enabled · Run by name: run_cron name="OpenClaw import — Provincetown Airbnb Monitor"',
        ]
    )
    update_message = "Cron updated: 'OpenClaw import — Provincetown Airbnb Monitor' (id=afc3cfd5)."

    class StaleListOrchestrator:
        model_client = SimpleNamespace(name="stale-list-e2e")

        def run_turn(self, **kwargs):  # noqa: ANN001
            return SimpleNamespace(
                final_text=cron_list_message,
                tool_results=[
                    ToolResult(
                        "list-1",
                        "list_crons",
                        "completed",
                        {
                            "crons": [
                                {"name": "Email deadline triage", "selection_index": 1},
                                {"name": "OpenClaw import — Provincetown Airbnb Monitor", "selection_index": 2},
                            ],
                            "message": cron_list_message,
                        },
                    ),
                    ToolResult(
                        "update-1",
                        "update_cron",
                        "completed",
                        {
                            "id": "afc3cfd5",
                            "name": "OpenClaw import — Provincetown Airbnb Monitor",
                            "html_image_delivery_mode": "self_contained",
                            "message": update_message,
                        },
                    ),
                    ToolResult("reminders-1", "list_reminders", "completed", {"count": 0, "reminders": []}),
                ],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text="",
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_enforce_web_response_fulfillment", lambda runtime, **kwargs: (kwargs["reply"], True))

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-cron-update-reply.db")
    app = web_app.create_app(
        runtime,
        orchestrator=StaleListOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={
                "text": "Apply the structured image-delivery change to the selected scheduled task.",
                "conversation_id": "web:cron-update-reply",
            },
        )

    assert response.status_code == 200
    assert response.json()["text"] == update_message


def test_web_chat_action_receipt_details_override_stale_tool_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    cron_list_message = "\n".join(
        [
            '1. Email deadline triage · Status: enabled · Run by name: run_cron name="Email deadline triage"',
            '2. OpenClaw import — Provincetown Airbnb Monitor · Status: enabled · Run by name: run_cron name="OpenClaw import — Provincetown Airbnb Monitor"',
        ]
    )
    receipt_summary = (
        "Cron updated: 'OpenClaw import — Provincetown Airbnb Monitor' (id=afc3cfd5). "
        "Changed: Task instructions."
    )

    class StaleListWithReceiptOrchestrator:
        model_client = SimpleNamespace(name="action-receipt-e2e")

        def run_turn(self, **kwargs):  # noqa: ANN001
            return SimpleNamespace(
                final_text=cron_list_message,
                tool_results=[
                    ToolResult(
                        "list-1",
                        "list_crons",
                        "completed",
                        {"crons": [], "message": cron_list_message},
                    ),
                    ToolResult(
                        "update-1",
                        "update_cron",
                        "completed",
                        {
                            "id": "afc3cfd5",
                            "name": "OpenClaw import — Provincetown Airbnb Monitor",
                            "message": "Cron updated: 'OpenClaw import — Provincetown Airbnb Monitor' (id=afc3cfd5).",
                            "action_receipt": {
                                "type": "action_receipt",
                                "action": "updated",
                                "object_type": "cron",
                                "object_id": "afc3cfd5",
                                "object_name": "OpenClaw import — Provincetown Airbnb Monitor",
                                "summary": receipt_summary,
                                "details": [
                                    "Task instructions: Use current listing images from the listing pages themselves.",
                                ],
                            },
                        },
                    ),
                ],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text="",
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_enforce_web_response_fulfillment", lambda runtime, **kwargs: (kwargs["reply"], True))

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-action-receipt-reply.db")
    app = web_app.create_app(
        runtime,
        orchestrator=StaleListWithReceiptOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={
                "text": "Apply the listing-image change to the scheduled task.",
                "conversation_id": "web:action-receipt-reply",
            },
        )

    assert response.status_code == 200
    assert response.json()["text"] == (
        f"{receipt_summary}\n\n"
        "Details:\n"
        "- Task instructions: Use current listing images from the listing pages themselves."
    )


def test_web_chat_multiple_cron_delete_receipts_override_tool_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    class ToggleDriftAfterDeleteOrchestrator:
        model_client = SimpleNamespace(name="delete-receipt-drift-e2e")

        def run_turn(self, **kwargs):  # noqa: ANN001
            return SimpleNamespace(
                final_text=(
                    "I ran toggle_cron as requested, but both toggles failed because those cron jobs no longer exist."
                ),
                tool_results=[
                    ToolResult(
                        "delete-1",
                        "delete_cron",
                        "completed",
                        {
                            "id": "ca253e87",
                            "name": "PR #30 readiness monitor",
                            "message": "Cron deleted: 'PR #30 readiness monitor' (id=ca253e87).",
                            "action_receipt": {
                                "type": "action_receipt",
                                "action": "deleted",
                                "object_type": "cron",
                                "object_id": "ca253e87",
                                "object_name": "PR #30 readiness monitor",
                                "summary": "Cron deleted: 'PR #30 readiness monitor' (id=ca253e87).",
                            },
                        },
                    ),
                    ToolResult(
                        "delete-2",
                        "delete_cron",
                        "completed",
                        {
                            "id": "e1f47ad0",
                            "name": "nullion-test PR #32 merge monitor",
                            "message": "Cron deleted: 'nullion-test PR #32 merge monitor' (id=e1f47ad0).",
                            "action_receipt": {
                                "type": "action_receipt",
                                "action": "deleted",
                                "object_type": "cron",
                                "object_id": "e1f47ad0",
                                "object_name": "nullion-test PR #32 merge monitor",
                                "summary": "Cron deleted: 'nullion-test PR #32 merge monitor' (id=e1f47ad0).",
                            },
                        },
                    ),
                ],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text="",
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_enforce_web_response_fulfillment", lambda runtime, **kwargs: (kwargs["reply"], True))

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-cron-delete-receipts.db")
    app = web_app.create_app(
        runtime,
        orchestrator=ToggleDriftAfterDeleteOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={
                "text": "Cancel both selected scheduled monitors.",
                "conversation_id": "web:cron-delete-receipts",
            },
        )

    assert response.status_code == 200
    assert response.json()["text"] == (
        "Cron deleted: 'PR #30 readiness monitor' (id=ca253e87).\n\n"
        "Cron deleted: 'nullion-test PR #32 merge monitor' (id=e1f47ad0)."
    )


def test_web_chat_scheduler_setup_scopes_future_artifact_work_to_cron_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    class SchedulerSetupClient:
        def __init__(self) -> None:
            self.calls = 0
            self.visible_tools_by_call: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN001
            if any(tool['name']=='select_attachment_format' for tool in kwargs.get('tools',[])):
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format',
                    'name':'select_attachment_format','input':{'extension':None,'output_kind':'internal',
                        'embedded_media_extensions':[],'confidence':1}}]}
            self.calls += 1
            self.visible_tools_by_call.append({tool["name"] for tool in kwargs.get("tools", [])})
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-1",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["scheduler_mutate", "web", "local_files"],
                                "artifact_extensions": [".docx", ".pptx", ".xlsx", ".csv", ".pdf", ".html"],
                                "embedded_media_artifact_extensions": [".xlsx"],
                                "tool_names": [
                                    "create_cron",
                                    "browser_navigate",
                                    "browser_image_collect",
                                    "spreadsheet_create",
                                    "document_create",
                                    "presentation_create",
                                    "pdf_create",
                                    "html_create",
                                    "file_write",
                                ],
                            },
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "cron-1",
                            "name": "create_cron",
                            "input": {
                                "name": "Random website artifact stress test",
                                "schedule": "0 */3 * * *",
                                "task": (
                                    "When the cron runs, research random websites and generate docx, pptx, "
                                    "xlsx, csv, pdf, self-contained html, linked html, and image artifacts."
                                ),
                            },
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Cron created."}]}

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-scheduler-scope.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    handler_calls: list[str] = []

    for name, tags in (
        ("list_crons", ("scheduler", "cron")),
        ("create_cron", ("scheduler", "cron")),
        ("browser_navigate", ("web",)),
        ("browser_image_collect", ("web",)),
        ("spreadsheet_create", ("local_files",)),
        ("document_create", ("local_files",)),
        ("presentation_create", ("local_files",)),
        ("pdf_create", ("local_files",)),
        ("html_create", ("local_files",)),
        ("file_write", ("local_files",)),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: (
                handler_calls.append(invocation.tool_name)
                or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "Cron created."})
            ),
        )

    model_client = SchedulerSetupClient()
    app = web_app.create_app(
        runtime,
        orchestrator=AgentOrchestrator(model_client=model_client),
        registry=registry,
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={
                "text": (
                    "Set up a cron every 3 hours. When it runs, research random websites and generate "
                    "docx, pptx, xlsx, csv, pdf, self-contained html, linked html, and images."
                ),
                "conversation_id": "web:scheduler-future-artifacts",
                "stream": False,
            },
        )

    assert response.status_code == 200
    assert response.json()["text"] == "Cron created."
    assert "couldn't attach" not in response.json()["text"]
    assert handler_calls == ["create_cron"]
    assert model_client.visible_tools_by_call[1] == {"request_tool_scope", "list_crons", "create_cron", "chat_history_search", "memory_search"}


def test_web_chat_exposes_multiple_artifact_tools_only_after_a_structured_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    class MultiArtifactClient:
        def __init__(self) -> None:
            self.visible_tools_by_call: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN001
            if any(tool['name']=='select_attachment_format' for tool in kwargs.get('tools',[])):
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format',
                    'name':'select_attachment_format','input':{'extension':'.pdf','output_kind':'attachment',
                        'embedded_media_extensions':[],'confidence':1}}]}
            self.visible_tools_by_call.append({tool["name"] for tool in kwargs.get("tools", [])})
            if len(self.visible_tools_by_call)==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                    'input':{'capabilities':['local_files'],
                        'artifact_extensions':['.docx','.pptx','.xlsx','.csv','.pdf','.html'],
                        'tool_names':['document_create','presentation_create','spreadsheet_create','pdf_create','file_write']}}]}
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Ready to create the pack."}]}

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-web-multi-artifact-scope.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    for name, tags in (
        ("browser_image_collect", ("web",)),
        ("spreadsheet_create", ("local_files",)),
        ("document_create", ("local_files",)),
        ("presentation_create", ("local_files",)),
        ("pdf_create", ("local_files",)),
        ("file_write", ("local_files",)),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    model_client = MultiArtifactClient()
    app = web_app.create_app(
        runtime,
        orchestrator=AgentOrchestrator(model_client=model_client),
        registry=registry,
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={
                "text": (
                    "Create a source-backed package: DOCX report, PPTX deck, XLSX workbook, "
                    "CSV export, PDF summary, and HTML page."
                ),
                "conversation_id": "web:multi-artifact-scope",
                "stream": False,
            },
        )

    assert response.status_code == 200
    assert model_client.visible_tools_by_call
    assert "document_create" not in model_client.visible_tools_by_call[0]
    widened_tools = model_client.visible_tools_by_call[1]
    assert {"document_create","presentation_create","spreadsheet_create","pdf_create","file_write"} <= widened_tools
    assert "couldn't complete" in response.json()['text']
    assert not response.json().get('artifacts')
    turn=next(e for e in reversed(runtime.store.list_conversation_events('web:multi-artifact-scope'))
        if e.get('event_type')=='conversation.chat_turn')
    assert any(r['tool_name']=='request_tool_scope' and r['status']=='completed' for r in turn['tool_results'])


def test_web_chat_bulk_cron_run_reply_overrides_stale_cron_listing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    cron_list_message = "\n".join(
        [
            '1. Email deadline triage · Status: enabled · Run by name: run_cron name="Email deadline triage"',
            '2. OpenClaw import — Provincetown Airbnb Monitor · Status: enabled · Run by name: run_cron name="OpenClaw import — Provincetown Airbnb Monitor"',
        ]
    )
    run_message = "Started 2 cron runs."

    class StaleListAfterRunOrchestrator:
        model_client = SimpleNamespace(name="stale-list-run-e2e")

        def run_turn(self, **kwargs):  # noqa: ANN001
            return SimpleNamespace(
                final_text=cron_list_message,
                tool_results=[
                    ToolResult(
                        "list-1",
                        "list_crons",
                        "completed",
                        {
                            "crons": [
                                {"name": "Email deadline triage", "selection_index": 1},
                                {"name": "OpenClaw import — Provincetown Airbnb Monitor", "selection_index": 2},
                            ],
                            "message": cron_list_message,
                        },
                    ),
                    ToolResult(
                        "run-1",
                        "run_cron",
                        "completed",
                        {
                            "all_enabled": True,
                            "started_count": 2,
                            "failed_count": 0,
                            "ids": ["c4cb766a", "afc3cfd5"],
                            "message": run_message,
                            "results": [
                                {"id": "c4cb766a", "name": "Email deadline triage", "status": "completed"},
                                {"id": "afc3cfd5", "name": "OpenClaw import — Provincetown Airbnb Monitor", "status": "completed"},
                            ],
                        },
                    ),
                    ToolResult("reminders-1", "list_reminders", "completed", {"count": 0, "reminders": []}),
                ],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text="",
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_enforce_web_response_fulfillment", lambda runtime, **kwargs: (kwargs["reply"], True))

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-bulk-cron-run-reply.db")
    app = web_app.create_app(
        runtime,
        orchestrator=StaleListAfterRunOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={
                "text": "Run every selected scheduled task.",
                "conversation_id": "web:bulk-cron-run-reply",
            },
        )

    assert response.status_code == 200
    assert response.json()["text"] == run_message


def test_web_startup_runs_chat_warmup_without_blocking_first_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import startup_warmup, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "web-startup-warmup-home"))
    runtime = bootstrap_persistent_runtime(tmp_path / "web-startup-warmup.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    warmed: list[dict[str, object]] = []

    def fake_run(runtime, **kwargs):  # noqa: ANN001
        warmed.append(kwargs)
        return startup_warmup.StartupWarmupResult(
            surface=str(kwargs.get("surface") or ""),
            ok=True,
            elapsed_ms=0.1,
            warmed=("fake",),
        )

    class InstantOrchestrator:
        model_client = SimpleNamespace(name="instant-web-e2e")

        def run_turn(self, **kwargs):  # noqa: ANN001
            return SimpleNamespace(
                final_text="hello",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text="",
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(startup_warmup, "run_chat_startup_warmup", fake_run)
    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_enforce_web_response_fulfillment", lambda runtime, **kwargs: (kwargs["reply"], True))

    app = web_app.create_app(runtime, orchestrator=InstantOrchestrator(), registry=registry)
    started = time.perf_counter()
    with TestClient(app) as client:
        response = client.post("/api/chat", json={"text": "hi", "conversation_id": "web:startup-warmup"})
    elapsed_ms = (time.perf_counter() - started) * 1000

    assert response.status_code == 200
    assert response.json()["text"] == "hello"
    assert warmed and warmed[0]["surface"] == "web"
    assert warmed[0]["principal_ids"] == ("web:admin",)
    context_warmers = warmed[0]["context_warmers"]
    assert isinstance(context_warmers, tuple)
    assert len(context_warmers) == 1
    warmer_name, warmer = context_warmers[0]
    assert warmer_name == "web_first_turn_context"
    assert callable(warmer)
    warmer()
    assert elapsed_ms < 1200


def test_web_chat_builder_memory_reaches_dashboard_without_runtime_id_noise(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_web_admin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "true")
    monkeypatch.setenv("NULLION_SKILL_LEARNING_ENABLED", "false")
    monkeypatch.setenv("NULLION_MEMORY_NO_TOOL_CAPTURE_ENABLED", "1")
    monkeypatch.setenv("NULLION_MEMORY_NO_TOOL_SYNC_CAPTURE", "true")

    owner = memory_owner_for_web_admin()
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_user_memory_entry(
        UserMemoryEntry(
            entry_id=f"{owner}:cnn_cron_current_id",
            owner=owner,
            kind=UserMemoryKind.ENVIRONMENT_FACT,
            key="cnn_cron_current_id",
            value='CNN cron current ID 93570269 is named "Hourly CNN latest news PDF".',
            source="e2e-seed",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
    )
    runtime.store.add_user_memory_entry(
        UserMemoryEntry(
            entry_id=f"{owner}:nullion_monitor_schedule",
            owner=owner,
            kind=UserMemoryKind.PREFERENCE,
            key="nullion_monitor_schedule",
            value="Active Nullion AI social + GitHub detailed-mentions monitor d4f7034e is enabled hourly on cron 0 * * * *.",
            source="e2e-seed",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            use_score=25,
        )
    )
    runtime.store.add_user_memory_entry(
        UserMemoryEntry(
            entry_id=f"{owner}:telegram_unavailable_runtime",
            owner=owner,
            kind=UserMemoryKind.ENVIRONMENT_FACT,
            key="telegram_unavailable_runtime",
            value="Telegram delivery is unavailable in the runtime.",
            source="e2e-seed",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            use_score=25,
        )
    )
    runtime.store.add_user_memory_entry(
        UserMemoryEntry(
            entry_id=f"{owner}:scheduled_email_deadline_triage_maton_gmail",
            owner=owner,
            kind=UserMemoryKind.ENVIRONMENT_FACT,
            key="scheduled_email_deadline_triage_maton_gmail",
            value='Scheduled task "Email deadline triage — Maton Gmail" can be triggered on request; delivery saves to configured destination.',
            source="e2e-seed",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            use_score=100,
        )
    )

    class MemoryModel:
        def __init__(self) -> None:
            self.memory_calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            tools = kwargs.get("tools") or []
            if tools and tools[0].get("name") == "select_attachment_format":
                return {"content": [{"type": "tool_use", "name": "select_attachment_format", "input": {"extension": None}}]}
            self.memory_calls += 1
            if self.memory_calls == 1:
                return {"content": [{"type": "text", "text": json.dumps({"entries": []})}]}
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "entries": [
                                    {
                                        "key": "color_favorito",
                                        "value": "El color favorito del usuario es verde.",
                                        "kind": "preference",
                                        "confidence": 0.99,
                                    }
                                ]
                            }
                        ),
                    }
                ]
            }

    class MemoryOrchestrator:
        def __init__(self) -> None:
            self.model_client = MemoryModel()

        def run_turn(self, **kwargs):  # noqa: ANN003
            assert kwargs["conversation_id"] == "web:e2e-memory"
            assert kwargs["user_message"] == "mi color favorito es verde"
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="Entendido, lo recordaré.",
                tool_results=[],
                artifacts=[],
                thinking_text="",
                reached_iteration_limit=False,
            )

    orchestrator = MemoryOrchestrator()
    app = create_app(
        runtime,
        orchestrator=orchestrator,
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        chat = client.post(
            "/api/chat",
            json={
                "text": "mi color favorito es verde",
                "conversation_id": "web:e2e-memory",
                "stream": False,
            },
        )
        status = client.get("/api/status?full=1")
        saved = None
        for _ in range(100):
            saved = runtime.store.get_user_memory_entry(f"{owner}:color_favorito")
            values = [item["value"] for item in status.json()["memory"]]
            if saved is not None and "El color favorito del usuario es verde." in values:
                break
            time.sleep(0.05)
            status = client.get("/api/status?full=1")

    assert chat.status_code == 200
    assert chat.json()["text"] == "Entendido, lo recordaré."
    assert orchestrator.model_client.memory_calls >= 2

    assert saved is not None
    assert saved.value == "El color favorito del usuario es verde."

    dashboard_memory = status.json()["memory"]
    values = [item["value"] for item in dashboard_memory]
    keys = [item["key"] for item in dashboard_memory]
    assert "El color favorito del usuario es verde." in values
    assert "cnn_cron_current_id" not in keys
    assert "nullion_monitor_schedule" not in keys
    assert "telegram_unavailable_runtime" not in keys
    assert "scheduled_email_deadline_triage_maton_gmail" not in keys
    assert all("93570269" not in value for value in values)
    assert all("d4f7034e" not in value for value in values)
    assert all("unavailable in the runtime" not in value for value in values)
    assert all("Scheduled task" not in value for value in values)


def test_browser_ui_memory_graph_hides_operational_runtime_state(tmp_path: Path) -> None:
    from playwright.sync_api import expect
    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_web_admin

    app, runtime, _registry = _web_app(tmp_path)
    owner = memory_owner_for_web_admin()
    now = datetime.now(timezone.utc)
    for entry_id, kind, key, value, score in [
        (
            f"{owner}:report_style",
            UserMemoryKind.PREFERENCE,
            "report_style",
            "Prefiere informes concisos.",
            1,
        ),
        (
            f"{owner}:public_repo",
            UserMemoryKind.ENVIRONMENT_FACT,
            "public_repo",
            "Repo publico: sample-team/sample-app.",
            1,
        ),
        (
            f"{owner}:nullion_monitor_schedule",
            UserMemoryKind.PREFERENCE,
            "nullion_monitor_schedule",
            "Active monitor d4f7034e is enabled hourly on cron 0 * * * *.",
            50,
        ),
        (
            f"{owner}:telegram_unavailable_runtime",
            UserMemoryKind.ENVIRONMENT_FACT,
            "telegram_unavailable_runtime",
            "Telegram delivery is unavailable in the runtime.",
            50,
        ),
        (
            f"{owner}:nullion_report_format_preferences",
            UserMemoryKind.PREFERENCE,
            "nullion_report_format_preferences",
            "Nullion scheduled monitor should produce complete mobile HTML with no empty graphs and clearly report failed metric sources.",
            80,
        ),
        (
            f"{owner}:nullion_monitor_use_gh_cli",
            UserMemoryKind.PREFERENCE,
            "nullion_monitor_use_gh_cli",
            "For Nullion GitHub audience metrics, use local GitHub CLI read-only `gh api` calls, not web scraping.",
            80,
        ),
        (
            f"{owner}:gmail_account_address",
            UserMemoryKind.PREFERENCE,
            "gmail_account_address",
            "User's Gmail account is profile@example.com.",
            90,
        ),
        (
            f"{owner}:scheduled_email_deadline_triage_maton_gmail",
            UserMemoryKind.ENVIRONMENT_FACT,
            "scheduled_email_deadline_triage_maton_gmail",
            'Scheduled task "Email deadline triage — Maton Gmail" can be triggered on request; delivery saves to configured destination.',
            100,
        ),
    ]:
        runtime.store.add_user_memory_entry(
            UserMemoryEntry(
                entry_id=entry_id,
                owner=owner,
                kind=kind,
                key=key,
                value=value,
                source="browser-e2e-seed",
                created_at=now,
                updated_at=now,
                use_score=score,
            )
        )

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.evaluate("refreshDashboard()")
            expect(page.locator("#memory-count")).to_have_text("2", timeout=10000)
            expect(page.locator("#memory-list")).to_contain_text("Prefiere informes concisos", timeout=10000)
            expect(page.locator("#memory-list")).to_contain_text("Repo publico", timeout=10000)
            expect(page.locator("#memory-list")).not_to_contain_text("d4f7034e")
            expect(page.locator("#memory-list")).not_to_contain_text("unavailable in the runtime")
            expect(page.locator("#memory-list")).not_to_contain_text("audience metrics")
            expect(page.locator("#memory-list")).not_to_contain_text("Gmail account")
            expect(page.locator("#memory-list")).not_to_contain_text("Scheduled task")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_delete_all_memory_updates_open_modal_immediately(tmp_path: Path) -> None:
    from playwright.sync_api import expect
    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_entries_for_owner, memory_owner_for_web_admin

    app, runtime, _registry = _web_app(tmp_path)
    owner = memory_owner_for_web_admin()
    now = datetime.now(timezone.utc)
    for entry_id, kind, key, value in [
        (
            f"{owner}:report_style",
            UserMemoryKind.PREFERENCE,
            "report_style",
            "Prefiere informes concisos.",
        ),
        (
            f"{owner}:public_repo",
            UserMemoryKind.ENVIRONMENT_FACT,
            "public_repo",
            "Repo publico: sample-team/sample-app.",
        ),
    ]:
        runtime.store.add_user_memory_entry(
            UserMemoryEntry(
                entry_id=entry_id,
                owner=owner,
                kind=kind,
                key=key,
                value=value,
                source="browser-e2e-seed",
                created_at=now,
                updated_at=now,
            )
        )

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.evaluate("refreshDashboard()")
            expect(page.locator("#memory-count")).to_have_text("2", timeout=10000)
            page.locator("#memory-list .view-all-btn").click()
            expect(page.locator("#full-list-overlay")).to_be_visible()
            expect(page.locator("#full-list-title")).to_have_text("Memory · 2")
            expect(page.locator("#full-list-body")).to_contain_text("Prefiere informes concisos")

            page.locator("#full-list-body button", has_text="Delete all").click()
            expect(page.locator("#confirm-overlay")).to_be_visible()
            page.locator("#confirm-ok").click()

            expect(page.locator("#memory-count")).to_have_text("0", timeout=10000)
            expect(page.locator("#full-list-title")).to_have_text("Memory · 0", timeout=10000)
            expect(page.locator("#full-list-body")).to_contain_text("Memory quiet")
            expect(page.locator("#full-list-body")).not_to_contain_text("Prefiere informes concisos")
            assert memory_entries_for_owner(runtime.store, owner) == []
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_web_config_memory_limits_apply_to_next_builder_memory_turn(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion.memory import memory_owner_for_web_admin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.web_app import create_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "true")
    monkeypatch.setenv("NULLION_SKILL_LEARNING_ENABLED", "false")
    monkeypatch.setenv("NULLION_MEMORY_LONG_TERM_LIMIT", "1")
    monkeypatch.setenv("NULLION_MEMORY_MID_TERM_LIMIT", "1")
    monkeypatch.setenv("NULLION_MEMORY_SHORT_TERM_LIMIT", "1")
    monkeypatch.setenv("NULLION_MEMORY_SMART_CLEANUP_ENABLED", "false")
    monkeypatch.setattr("nullion.builder_background.schedule_builder_background_task", lambda _name, fn: fn())

    owner = memory_owner_for_web_admin()
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")

    class CapturingMemoryModel:
        def __init__(self) -> None:
            self.builder_payloads: list[dict] = []

        def create(self, **kwargs):  # noqa: ANN003
            messages = kwargs.get("messages") or []
            user_text = messages[-1]["content"][0]["text"]
            payload = json.loads(user_text)
            if "existing_memory" not in payload:
                return {"content": [{"type": "text", "text": json.dumps({"entries": []})}]}
            self.builder_payloads.append(payload)
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "entries": [
                                    {
                                        "key": "preference_one",
                                        "value": "User prefers concise summaries.",
                                        "kind": "preference",
                                        "confidence": 0.99,
                                    },
                                    {
                                        "key": "preference_two",
                                        "value": "User prefers direct test evidence.",
                                        "kind": "preference",
                                        "confidence": 0.99,
                                    },
                                    {
                                        "key": "preference_three",
                                        "value": "User wants settings live immediately.",
                                        "kind": "preference",
                                        "confidence": 0.99,
                                    },
                                    {
                                        "key": "workspace_one",
                                        "value": "Workspace memory settings are under active E2E validation.",
                                        "kind": "environment_fact",
                                        "confidence": 0.99,
                                    },
                                    {
                                        "key": "workspace_two",
                                        "value": "Private E2E runs against this app checkout.",
                                        "kind": "environment_fact",
                                        "confidence": 0.99,
                                    },
                                    {
                                        "key": "short_note",
                                        "value": "Next turn should see fresh memory caps.",
                                        "kind": "fact",
                                        "confidence": 0.99,
                                    },
                                ]
                            }
                        ),
                    }
                ]
            }

    model = CapturingMemoryModel()

    class MemoryOrchestrator:
        @property
        def model_client(self):  # noqa: ANN201
            return model

        def run_turn(self, **kwargs):  # noqa: ANN003
            assert kwargs["conversation_id"] == "web:e2e-memory-settings"
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="Settings-aware reply.",
                tool_results=[ToolResult("tool-1", "file_read", "completed", {"path": "notes.txt"})],
                artifacts=[],
                thinking_text="",
                reached_iteration_limit=False,
            )

    app = create_app(
        runtime,
        orchestrator=MemoryOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        saved = client.post(
            "/api/config",
            json={
                "memory_long_term_limit": 3,
                "memory_mid_term_limit": 2,
                "memory_short_term_limit": 1,
                "memory_smart_cleanup": False,
            },
        )
        assert saved.status_code == 200
        assert {
            "NULLION_MEMORY_LONG_TERM_LIMIT",
            "NULLION_MEMORY_MID_TERM_LIMIT",
            "NULLION_MEMORY_SHORT_TERM_LIMIT",
            "NULLION_MEMORY_SMART_CLEANUP_ENABLED",
        }.issubset(set(saved.json()["updated"]))

        config = client.get("/api/config")
        assert config.status_code == 200
        assert config.json()["memory_long_term_limit"] == 3
        assert config.json()["memory_mid_term_limit"] == 2
        assert config.json()["memory_short_term_limit"] == 1
        assert config.json()["memory_smart_cleanup"] is False

        chat = client.post(
            "/api/chat",
            json={
                "text": "Guarda estas preferencias nuevas.",
                "conversation_id": "web:e2e-memory-settings",
                "stream": False,
            },
        )

    assert chat.status_code == 200
    assert model.builder_payloads
    limits = model.builder_payloads[0]["limits"]
    assert limits["max_entries"] == 6
    assert limits["long_term_memory"] == 3
    assert limits["mid_term_memory"] == 2
    assert limits["short_term_memory"] == 1
    assert os.environ["NULLION_MEMORY_LONG_TERM_LIMIT"] == "3"
    assert os.environ["NULLION_MEMORY_MID_TERM_LIMIT"] == "2"
    assert os.environ["NULLION_MEMORY_SHORT_TERM_LIMIT"] == "1"
    assert os.environ["NULLION_MEMORY_SMART_CLEANUP_ENABLED"] == "false"
    assert env_path.read_text(encoding="utf-8").count("NULLION_MEMORY_LONG_TERM_LIMIT") == 1
    assert len([entry for entry in runtime.store.list_user_memory_entries() if entry.owner == owner]) == 6


def test_upload_file_delivery_and_artifact_download_paths(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient

    app, _runtime, _registry = _web_app(tmp_path)

    with TestClient(app) as client:
        upload = client.post(
            "/api/upload",
            files={"file": ("../report.txt", b"artifact bytes", "text/plain")},
        )
        payload = upload.json()
        artifact = payload["artifact"]
        download = client.get(artifact["url"])

    assert upload.status_code == 200
    assert payload["ok"] is True
    assert payload["name"] == "report.txt"
    assert Path(payload["path"]).name == "report.txt"
    assert artifact["name"] == "report.txt"
    assert artifact["media_type"] == "text/plain"
    assert download.status_code == 200
    assert download.content == b"artifact bytes"


def test_upload_size_limit_is_configurable_and_reports_error(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    monkeypatch.setenv("NULLION_MAX_UPLOAD_MB", "1")
    app, _runtime, _registry = _web_app(tmp_path)

    with TestClient(app) as client:
        upload = client.post(
            "/api/upload",
            files={"file": ("clip.mov", b"x" * (1024 * 1024 + 1), "video/quicktime")},
        )

    payload = upload.json()
    assert upload.status_code == 413
    assert payload["ok"] is False
    assert payload["error"] == "File exceeds maximum size of 1 MB."


def test_upload_accepts_mov_above_legacy_50mb_limit(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    data_dir = tmp_path / "data"
    monkeypatch.setenv("NULLION_DATA_DIR", str(data_dir))
    monkeypatch.delenv("NULLION_MAX_UPLOAD_MB", raising=False)
    app, _runtime, _registry = _web_app(tmp_path)

    with TestClient(app) as client:
        upload = client.post(
            "/api/upload",
            files={"file": ("screen-recording.mov", b"x" * (50 * 1024 * 1024 + 1), "video/quicktime")},
        )

    payload = upload.json()
    uploaded_path = Path(payload["path"])
    assert upload.status_code == 200
    assert payload["ok"] is True
    assert payload["name"] == "screen-recording.mov"
    assert payload["media_type"] == "video/quicktime"
    assert uploaded_path.parent == data_dir / "uploads"
    assert uploaded_path.stat().st_size == 50 * 1024 * 1024 + 1


def test_browser_ui_workspace_folder_button_opens_configured_root(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    storage_root = tmp_path / "storage"
    workspace = storage_root / "workspace_admin"
    opened: list[Path] = []

    def fake_open(path: Path) -> None:
        opened.append(path)

    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(storage_root))
    monkeypatch.setattr(web_app, "_open_local_directory", fake_open)
    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            workspace_box = page.locator("#workspace-folder-btn").bounding_box()
            browser_box = page.locator("#browser-btn").bounding_box()
            assert workspace_box is not None
            if browser_box is not None:
                assert workspace_box["width"] == browser_box["width"]
                assert workspace_box["height"] == browser_box["height"]
            page.locator("#workspace-folder-btn").click()
            expect(page.locator("#status-label")).to_contain_text("Workspace opened")
            assert opened == [workspace.resolve()]
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_background_file_delivery_broadcasts_downloadable_artifact(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    class CapturingOrchestrator:
        deliver_fn = None

        def set_deliver_fn(self, deliver_fn):
            self.deliver_fn = deliver_fn

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    artifact_dir = artifact_root_for_principal("web:operator")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "delivered.txt"
    artifact_path.write_text("delivered bytes", encoding="utf-8")
    orchestrator = CapturingOrchestrator()
    app = create_app(runtime, orchestrator=orchestrator, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            assert orchestrator.deliver_fn is not None
            orchestrator.deliver_fn("web:operator", str(artifact_path), is_artifact=True)
            event = websocket.receive_json()
        downloaded = client.get(event["artifacts"][0]["url"])

    assert event["type"] == "background_message"
    assert event["conversation_id"] == "web:operator"
    assert event["text"] == "Attached the requested file."
    assert event["artifacts"][0]["name"] == "delivered.txt"
    assert downloaded.status_code == 200
    assert downloaded.text == "delivered bytes"


def test_browser_planner_status_is_conversation_scoped_and_activity_is_group_scoped(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    class CapturingOrchestrator:
        deliver_fn = None

        def set_deliver_fn(self, deliver_fn):
            self.deliver_fn = deliver_fn

    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "true")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    orchestrator = CapturingOrchestrator()
    app = create_app(runtime, orchestrator=orchestrator, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            expect(page.locator("#status-label")).to_contain_text("Connected", timeout=10000)
            assert orchestrator.deliver_fn is not None
            conversation_id = page.evaluate("localStorage.getItem('nullion_conv_id')")
            assert isinstance(conversation_id, str) and conversation_id.startswith("web:")

            orchestrator.deliver_fn(
                "web:other-conversation",
                "Planner: Parallel Mission * 1 task\n-> Running 1 task in parallel:\n  ◐ Other Conversation Task",
                is_status=True,
                group_id="grp-other",
                status_kind="task_summary",
            )
            expect(page.locator("body")).not_to_contain_text("Other Conversation Task", timeout=1000)

            page.evaluate(
                """
                () => {
                  updateRunActivity({
                    id: 'tool-leak',
                    label: 'file_write',
                    status: 'done',
                    detail: '/tmp/deny-pass.txt',
                    turn_id: 'turn-leak'
                  });
                  updateTaskStatusCard({
                    conversation_id: localStorage.getItem('nullion_conv_id'),
                    group_id: 'grp-clean',
                    turn_id: 'turn-leak',
                    status_kind: 'task_summary',
                    text: 'Planner: Parallel Mission * 2 tasks\\n-> Running 2 tasks in parallel:\\n  ◐ Search Gmail\\n  ☐ Deliver report'
                  });
                }
                """
            )
            clean_card = page.locator('.task-status-bubble[data-task-group-id="grp-clean"]')
            expect(clean_card).to_be_visible(timeout=5000)
            expect(clean_card).not_to_contain_text("deny-pass", timeout=1000)

            orchestrator.deliver_fn(
                conversation_id,
                "Planner: Parallel Mission * 2 tasks\nFor: Email deadline triage\n-> Running 2 tasks in parallel:\n  ◐ Search Gmail\n  ☐ Deliver report",
                is_status=True,
                group_id="grp-live",
                status_kind="task_summary",
            )
            live_card = page.locator('.task-status-bubble[data-task-group-id="grp-live"]')
            expect(live_card).to_be_visible(timeout=5000)
            expect(live_card).not_to_contain_text("* 2 tasks", timeout=1000)
            expect(live_card).to_contain_text("Email deadline triage", timeout=5000)
            orchestrator.deliver_fn(
                conversation_id,
                "Tools\n"
                "✓ connector_request — api.example.com\n"
                "✓ connector_request — api.example.com\n"
                "✓ file_write — deny-pass.txt\n"
                "✓ file_write — deny-pass.txt\n"
                "✓ run_cron — delivery saved",
                is_status=True,
                group_id="grp-live",
                status_kind="tool_activity",
                activity_id="cron-tools",
                activity_label="Tools",
            )
            expect(live_card.locator(".run-activity")).to_be_visible(timeout=5000)
            expect(live_card).to_contain_text("connector_request", timeout=5000)
            card_text = live_card.inner_text(timeout=5000)
            assert card_text.count("connector_request") == 1
            assert card_text.count("file_write") == 1
            assert "api.example.com" not in card_text
            assert "deny-pass.txt" not in card_text
            orchestrator.deliver_fn(
                conversation_id,
                "Research Boston day trip\n✓ browser_extract_text",
                is_status=True,
                group_id="grp-live",
                status_kind="tool_activity",
                activity_id="research-tools",
                activity_label="Research Boston day trip tools",
            )
            expect(live_card).to_contain_text("Research Boston day trip tools", timeout=5000)
            expect(live_card).to_contain_text("browser_extract_text", timeout=5000)
            deduped_card_text = live_card.inner_text(timeout=5000)
            assert deduped_card_text.count("Research Boston day trip") == 1

            orchestrator.deliver_fn(
                conversation_id,
                "Planner: Scheduled task\nFor: 確認\n⏰ **確認 <script>**\n  ✓ **完了** — `literal`",
                is_status=True,
                group_id="grp-formatted",
                status_kind="task_summary",
            )
            formatted_card = page.locator('.task-status-bubble[data-task-group-id="grp-formatted"]')
            expect(formatted_card.locator('.task-status-title strong')).to_have_text("確認 <script>")
            expect(formatted_card.locator('.task-status-label strong')).to_have_text("完了")
            expect(formatted_card.locator('.task-status-label code')).to_have_text("literal")
            expect(formatted_card).not_to_contain_text("**")
            assert formatted_card.locator('script').count() == 0

            monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "false")
            orchestrator.deliver_fn(
                conversation_id,
                "Planner: Sequential Mission * 2 tasks\nFor: Boston day trip from NYC by train\n-> Working on 2 tasks:\n  ◐ Research Boston day trip options\n  ☐ Create Boston day itinerary",
                is_status=True,
                group_id="grp-quiet",
                status_kind="task_summary",
            )
            quiet_card = page.locator('.task-status-bubble[data-task-group-id="grp-quiet"]')
            expect(quiet_card).to_be_visible(timeout=5000)
            expect(quiet_card).to_contain_text("Boston day trip from NYC by train", timeout=5000)
            expect(quiet_card).not_to_contain_text("* 2 tasks", timeout=1000)
            orchestrator.deliver_fn(
                conversation_id,
                "Tools\n✓ browser_extract_text — Boston guide",
                is_status=True,
                group_id="grp-quiet",
                status_kind="tool_activity",
                activity_id="quiet-tools",
                activity_label="Tools",
            )
            expect(quiet_card.locator(".run-activity")).not_to_be_visible(timeout=1000)
            orchestrator.deliver_fn(
                conversation_id,
                'Result for "Boston day trip from NYC by train":\nCompleted 0/2 task(s). 2 failed.',
                group_id="grp-quiet",
            )
            expect(page.locator("#messages")).to_contain_text("Completed 0/2 task(s). 2 failed.", timeout=5000)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_websocket_slash_planner_forces_single_task_planner_card(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    captured: dict[str, object] = {}

    class FakeStructuredPlannerModel:
        def create(self, **kwargs):  # noqa: ANN003
            captured["model_messages"] = kwargs.get("messages")
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "disposition": "single_turn",
                                "needs_clarification": False,
                                "clarification_question": None,
                                "routing_evidence": [],
                                "tasks": [
                                    {
                                        "title": "Build requested PDF",
                                        "description": "Build the requested planner PDF.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [],
                                        "context_key_in": None,
                                        "context_key_out": None,
                                        "required_inputs": [],
                                        "can_start": True,
                                    }
                                ],
                            }
                        ),
                    }
                ]
            }

    class DispatchingOrchestrator:
        model_client = FakeStructuredPlannerModel()

        def dispatch_request_sync(self, **kwargs):  # noqa: ANN003
            captured["dispatch_kwargs"] = kwargs
            dag_plan = kwargs["dag_plan"]
            assert dag_plan.can_dispatch is False
            assert dag_plan.can_dispatch_when_requested is True
            return SimpleNamespace(
                dispatched=True,
                task_count=1,
                group_id="grp-forced-planner",
                acknowledgment=(
                    "Planner: Single Turn • 1 task\n"
                    "For: Build the requested planner PDF\n"
                    "→ Working on 1 task:\n"
                    "  ☐ Build requested PDF"
                ),
                planner_summary="Single Turn • 1 task",
            )

        def run_turn(self, **kwargs):  # noqa: ANN003
            raise AssertionError("explicit /planner should not fall back to the normal web tool loop")

    monkeypatch.setenv("NULLION_TASK_DECOMPOSITION_ENABLED", "true")
    monkeypatch.setenv("NULLION_MULTI_AGENT_ENABLED", "true")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "true")

    app = create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=DispatchingOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "/planner Build the requested planner PDF",
                    "conversation_id": "web:e2e-planner",
                    "turn_id": "turn:e2e-planner",
                    "stream": True,
                    "activity_trace": True,
                }
            )
            events: list[dict[str, object]] = []
            while True:
                event = websocket.receive_json()
                events.append(event)
                if event.get("type") == "done":
                    break

    dispatch_kwargs = captured["dispatch_kwargs"]
    assert dispatch_kwargs["user_message"] == "Build the requested planner PDF"
    assert dispatch_kwargs["single_task_fast_path"] is False
    assert any(
        event.get("type") == "task_status"
        and event.get("turn_id") == "turn:e2e-planner"
        and event.get("group_id") == "grp-forced-planner"
        and "Planner: Single Turn" in str(event.get("text", ""))
        for event in events
    )
    assert not any(event.get("turn_id") == "turn:e2e-planner" and event.get("type") == "chunk" for event in events)


def test_websocket_can_generate_txt_artifacts_from_fetched_content(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.web_app import create_app

    artifact_root = artifact_root_for_principal("web:e2e")
    artifact_root.mkdir(parents=True, exist_ok=True)
    txt_path = artifact_root / "quarterly_report.txt"

    def run_turn(
        conversation_id: str,
        *,
        user_message: str,
        **kwargs,
    ) -> object:
        del kwargs
        txt_path.write_text("Quarterly report values: 18.4k", encoding="utf-8")
        return SimpleNamespace(
            suspended_for_approval=False,
            final_text=f"Created requested file for {conversation_id}: {user_message[:40]}",
            artifacts=[str(txt_path)],
            tool_results=[
                ToolResult(
                    "fetch-text",
                    "web_fetch",
                    "completed",
                    {"url": "https://example.com/report", "body": "Quarterly report values: 18.4k"},
                )
            ],
            reached_iteration_limit=False,
        )

    class Orchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            return run_turn(
                conversation_id=kwargs["conversation_id"],
                user_message=kwargs["user_message"],
            )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(
        runtime,
        orchestrator=Orchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[artifact_root, tmp_path]),
    )

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "Fetch the report and save it as .txt",
                    "conversation_id": "web:e2e",
                    "stream": False,
                    "turn_id": "turn-txt",
                }
            )
            done = {}
            while True:
                event = websocket.receive_json()
                assert event.get("type") not in {"error", "approval_required", "clarification"}, event
                if event.get("type") == "done":
                    done = event
                    break
    artifact = done["artifacts"][0]
    downloaded = client.get(artifact["url"])

    assert done["type"] == "done"
    assert artifact["name"].endswith(".txt")
    assert downloaded.status_code == 200
    assert downloaded.text == "Quarterly report values: 18.4k"


def test_websocket_can_generate_pdf_artifacts_from_fetched_content(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolInvocation,
        ToolRegistry,
        ToolResult,
        ToolRiskLevel,
        ToolSideEffectClass,
        ToolSpec,
    )
    from nullion.web_app import create_app

    def file_write_handler(invocation: ToolInvocation) -> ToolResult:
        path = Path(invocation.arguments["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(invocation.arguments["content"]), encoding="utf-8")
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"path": str(path)})

    def pdf_create_handler(invocation: ToolInvocation) -> ToolResult:
        path = Path(invocation.arguments["output_path"])
        _write_blank_pdf(path)
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"artifact_path": str(path)},
        )

    def run_turn(
        conversation_id: str,
        *,
        user_message: str,
        **kwargs,
    ) -> object:
        del kwargs
        return SimpleNamespace(
            suspended_for_approval=False,
            final_text=f"Created requested file for {conversation_id}: {user_message[:40]}",
            artifacts=[],
            tool_results=[
                ToolResult(
                    "fetch-pdf",
                    "web_fetch",
                    "completed",
                    {"url": "https://example.com/report", "text": "Quarterly report values: 18.4k"},
                )
            ],
            reached_iteration_limit=False,
        )

    class Orchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            return run_turn(
                conversation_id=kwargs["conversation_id"],
                user_message=kwargs["user_message"],
            )

    registry = ToolRegistry(filesystem_allowed_roots=[artifact_root_for_principal("web:e2e"), tmp_path])
    registry.register(
        ToolSpec(
            name="file_write",
            description="Write a local file",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=20,
        ),
        file_write_handler,
    )
    registry.register(
        ToolSpec(
            name="pdf_create",
            description="Create pdf",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=30,
        ),
        pdf_create_handler,
    )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=Orchestrator(), registry=registry)

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "Fetch the report and save it as .pdf",
                    "conversation_id": "web:e2e",
                    "stream": False,
                    "turn_id": "turn-pdf",
                }
            )
            done = {}
            while True:
                event = websocket.receive_json()
                if event.get("type") == "done":
                    done = event
                    break
    artifact = done["artifacts"][0]
    downloaded = client.get(artifact["url"])

    assert done["type"] == "done"
    assert artifact["name"].endswith(".pdf")
    assert downloaded.status_code == 200
    assert downloaded.content.startswith(b"%PDF-1.4")


def test_websocket_can_deliver_attachment_format_filtered_workspace_artifacts(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.web_app import create_app

    worksheet_root = artifact_root_for_principal("web:e2e")
    worksheet_root.mkdir(parents=True, exist_ok=True)
    xlsx_path = worksheet_root / "insights.xlsx"
    xlsx_path.write_bytes(b"PK\x03\x04\x14dummy-xlsx")

    class Orchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            conversation_id = kwargs["conversation_id"]
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text=f"Created requested file for {conversation_id}: workbook ready",
                artifacts=[str(xlsx_path)],
                tool_results=[
                    ToolResult(
                        "artifact-xlsx",
                        "file_write",
                        "completed",
                        {"artifact_path": str(xlsx_path)},
                    )
                ],
                reached_iteration_limit=False,
            )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=Orchestrator(), registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "Please fetch the report and write a spreadsheet .xlsx",
                    "conversation_id": "web:e2e",
                    "stream": False,
                    "turn_id": "turn-xlsx",
                }
            )
            done = {}
            while True:
                event = websocket.receive_json()
                if event.get("type") == "done":
                    done = event
                    break
    artifact = done["artifacts"][0]
    downloaded = client.get(artifact["url"])

    assert artifact["name"] == "insights.xlsx"
    assert artifact["media_type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert downloaded.status_code == 200
    assert downloaded.content == b"PK\x03\x04\x14dummy-xlsx"


def test_websocket_can_deliver_screenshot_like_image_artifacts(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.web_app import create_app

    artifact_root = artifact_root_for_principal("web:e2e")
    artifact_root.mkdir(parents=True, exist_ok=True)
    image_path = artifact_root / "capture.png"
    image_path.write_bytes(b"\x89PNG\r\nfake-png")

    class Orchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            conversation_id = kwargs["conversation_id"]
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text=f"Created requested file for {conversation_id}: capture ready",
                artifacts=[str(image_path)],
                tool_results=[
                    ToolResult(
                        "artifact-png",
                        "terminal_exec",
                        "completed",
                        {"artifact_paths": [str(image_path)]},
                    )
                ],
                reached_iteration_limit=False,
            )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=Orchestrator(), registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "Take a screenshot and save it as a .png",
                    "conversation_id": "web:e2e",
                    "stream": False,
                    "turn_id": "turn-png",
                }
            )
            done = {}
            while True:
                event = websocket.receive_json()
                if event.get("type") == "done":
                    done = event
                    break
    artifact = done["artifacts"][0]
    downloaded = client.get(artifact["url"])

    assert artifact["name"] == "capture.png"
    assert artifact["media_type"] == "image/png"
    assert downloaded.status_code == 200
    assert downloaded.content == b"\x89PNG\r\nfake-png"


def test_websocket_can_deliver_multi_format_artifacts_from_a_multi_step_turn(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.web_app import create_app

    artifact_root = artifact_root_for_principal("web:e2e")
    artifact_root.mkdir(parents=True, exist_ok=True)
    txt_path = artifact_root / "quarterly.txt"
    xlsx_path = artifact_root / "ledger.xlsx"
    txt_path.write_text("Revenue: 18.4k", encoding="utf-8")
    xlsx_path.write_bytes(b"PK\x03\x04\x14dummy-xlsx")

    class Orchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            conversation_id = kwargs["conversation_id"]
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text=f"{conversation_id}: multi-format artifacts generated",
                artifacts=[str(txt_path), str(xlsx_path)],
                tool_results=[
                    ToolResult(
                        "artifact-txt",
                        "web_fetch",
                        "completed",
                        {"url": "https://example.com/report", "text": "Revenue: 18.4k"},
                    ),
                    ToolResult(
                        "artifact-xlsx",
                        "file_write",
                        "completed",
                        {"path": str(xlsx_path)},
                    ),
                ],
                reached_iteration_limit=False,
            )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=Orchestrator(), registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "Get report as .txt and then as .xlsx in one workflow",
                    "conversation_id": "web:e2e",
                    "stream": False,
                    "turn_id": "turn-multi",
                }
            )
            done = {}
            while True:
                event = websocket.receive_json()
                if event.get("type") == "done":
                    done = event
                    break

    artifacts = done["artifacts"]
    assert len(artifacts) == 2
    downloaded_txt = client.get(next(item["url"] for item in artifacts if item["name"] == "quarterly.txt"))
    downloaded_xlsx = client.get(next(item["url"] for item in artifacts if item["name"] == "ledger.xlsx"))
    assert downloaded_txt.status_code == 200
    assert downloaded_txt.text == "Revenue: 18.4k"
    assert downloaded_xlsx.status_code == 200
    assert downloaded_xlsx.content == b"PK\x03\x04\x14dummy-xlsx"
    names = sorted(item["name"] for item in artifacts)
    media_types = sorted(item["media_type"] for item in artifacts)
    assert names == ["ledger.xlsx", "quarterly.txt"]
    assert media_types == [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "text/plain",
    ]


def test_websocket_can_chain_web_fetch_into_text_and_pdf_artifacts(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.web_app import create_app
    import urllib.request

    class MockOriginHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            assert self.path == "/quarterly"
            content = (
                "<!doctype html><html><body><h1>Q4 Revenue</h1><p>Sales reached 18.4k units.</p></body></html>"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, format, *args):  # noqa: A002, ANN001
            return

    artifact_root = artifact_root_for_principal("web:e2e")
    artifact_root.mkdir(parents=True, exist_ok=True)
    txt_path = artifact_root / "quarterly_report.txt"
    pdf_path = artifact_root / "quarterly_report.pdf"

    port = _free_port()
    report_url = f"http://127.0.0.1:{port}/quarterly"

    class Orchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            del kwargs
            with urllib.request.urlopen(report_url, timeout=5) as response:
                body = response.read().decode("utf-8", errors="replace")
            txt_payload = re.sub("<[^>]+>", "", body)
            txt_path.write_text(f"Revenue summary: {txt_payload}", encoding="utf-8")
            _write_blank_pdf(pdf_path)

            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="Fetched report and wrote text + pdf artifacts.",
                artifacts=[str(txt_path), str(pdf_path)],
                tool_results=[
                    ToolResult(
                        "fetch-quarterly",
                        "web_fetch",
                        "completed",
                        {"url": report_url, "status_code": 200, "text": txt_payload},
                    ),
                    ToolResult(
                        "write-txt",
                        "file_write",
                        "completed",
                        {"path": str(txt_path)},
                    ),
                    ToolResult(
                        "write-pdf",
                        "pdf_create",
                        "completed",
                        {"artifact_path": str(pdf_path)},
                    ),
                ],
                reached_iteration_limit=False,
            )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(
        runtime,
        orchestrator=Orchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[artifact_root, tmp_path]),
    )

    with _MockWebServer(MockOriginHandler, port), TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "Fetch quarterly report and put it in text and pdf files",
                    "conversation_id": "web:e2e",
                    "stream": False,
                    "turn_id": "turn-multi-step-fetch",
                }
            )
            done = {}
            while True:
                event = websocket.receive_json()
                if event.get("type") == "done":
                    done = event
                    break

    artifact_map = {item["name"]: item["url"] for item in done["artifacts"]}
    txt_download = client.get(artifact_map["quarterly_report.txt"])
    pdf_download = client.get(artifact_map["quarterly_report.pdf"])

    assert done["type"] == "done"
    assert artifact_map.keys() == {"quarterly_report.txt", "quarterly_report.pdf"}
    assert txt_download.status_code == 200
    assert txt_download.text.startswith("Revenue summary: ")
    assert "Q4 RevenueSales reached 18.4k units." in txt_download.text
    assert pdf_download.status_code == 200
    assert pdf_download.content.startswith(b"%PDF-1.4")
    media_types = sorted(item["media_type"] for item in done["artifacts"])
    assert media_types == ["application/pdf", "text/plain"]


def test_websocket_pdf_request_only_surfaces_requested_pdf_artifact(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.web_app import create_app

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    conversation_id = "web:e2e-pdf-filter"
    artifact_root = artifact_root_for_principal(conversation_id)
    artifact_root.mkdir(parents=True, exist_ok=True)
    html_page = artifact_root / "boston_day_trip_page1.html"
    html_page_2 = artifact_root / "boston_day_trip_page2.html"
    screenshot = artifact_root / "screenshot-f3764f0469ca.png"
    pdf_path = artifact_root / "boston_day_trip_from_nyc_by_train.pdf"
    html_page.write_text("<html><body>draft page 1</body></html>", encoding="utf-8")
    html_page_2.write_text("<html><body>draft page 2</body></html>", encoding="utf-8")
    screenshot.write_bytes(b"\x89PNG\r\nintermediate screenshot")
    _write_blank_pdf(pdf_path)

    class StructuredFormatModel:
        def create(self, **kwargs):  # noqa: ANN003
            tools = kwargs.get("tools") or []
            if tools and tools[0].get("name") == "select_attachment_format":
                return {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "select_attachment_format",
                            "input": {"extension": ".pdf", "confidence": 0.97},
                        }
                    ]
                }
            return {"content": [{"type": "text", "text": '{"disposition":"independent","confidence":0.95}'}]}

    class Orchestrator:
        model_client = StructuredFormatModel()

        def run_turn(self, **kwargs):  # noqa: ANN003
            assert kwargs["conversation_id"] == conversation_id
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="Created the requested guide as a PDF.",
                artifacts=[str(html_page), str(html_page_2), str(screenshot), str(pdf_path)],
                tool_results=[
                    ToolResult("page-1", "file_write", "completed", {"path": str(html_page)}),
                    ToolResult("page-2", "file_write", "completed", {"path": str(html_page_2)}),
                    ToolResult("shot", "browser_screenshot", "completed", {"artifact_paths": [str(screenshot)]}),
                    ToolResult("pdf", "pdf_create", "completed", {"artifact_path": str(pdf_path)}),
                ],
                reached_iteration_limit=False,
            )

    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "true")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(
        runtime,
        orchestrator=Orchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[artifact_root, tmp_path]),
    )

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "作ったガイドをきれいな添付資料にして",
                    "conversation_id": conversation_id,
                    "stream": False,
                    "activity_trace": True,
                    "turn_id": "turn-pdf-filter",
                }
            )
            events: list[dict[str, object]] = []
            while True:
                event = websocket.receive_json()
                events.append(event)
                if event.get("type") == "done":
                    done = event
                    break

    assert done["type"] == "done"
    assert [artifact["name"] for artifact in done["artifacts"]] == [pdf_path.name]
    assert all(not artifact["name"].endswith((".html", ".png")) for artifact in done["artifacts"])
    artifact_activity = [
        event
        for event in events
        if event.get("type") == "activity"
        and event.get("id") == "artifacts"
        and event.get("status") == "done"
    ]
    assert artifact_activity[-1]["detail"] == "1 artifact"
    downloaded = client.get(done["artifacts"][0]["url"])
    assert downloaded.status_code == 200
    assert downloaded.content.startswith(b"%PDF-1.4")


def test_websocket_can_build_excel_artifact_with_embedded_image_data_after_web_fetch(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult
    from nullion.web_app import create_app
    import base64
    import urllib.request

    class MockOriginHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path == "/report":
                body = "<!doctype html><p>Revenue 11.2k, Growth 3.4%</p>".encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-Image", "from-origin")
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/chart.png":
                image = b"\x89PNG\r\n\x1A\nfake-chart-data"
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(image)))
                self.end_headers()
                self.wfile.write(image)
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format, *args):  # noqa: A002, ANN001
            return

    class EmbeddedXlsxBuilder:
        def build(self, report_text: str, image_bytes: bytes) -> bytes:
            encoded_image = base64.b64encode(image_bytes).decode("ascii")
            return (
                b"PK\x03\x04\x14\x00\x00\x00\x00\x00\x00\x00!\x00"
                + f"excel-sheet\\n{report_text}\\nimage-b64:{encoded_image}".encode("utf-8")
            )

    artifact_root = artifact_root_for_principal("web:e2e")
    artifact_root.mkdir(parents=True, exist_ok=True)
    xlsx_path = artifact_root / "quarterly.xlsx"

    port = _free_port()
    report_url = f"http://127.0.0.1:{port}/report"
    chart_url = f"http://127.0.0.1:{port}/chart.png"

    class Orchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            del kwargs
            with urllib.request.urlopen(report_url, timeout=5) as report_response:
                report_body = report_response.read().decode("utf-8", errors="replace")
            with urllib.request.urlopen(chart_url, timeout=5) as chart_response:
                image_body = chart_response.read()

            report_text = re.sub("<[^>]+>", "", report_body)
            xlsx_path.write_bytes(EmbeddedXlsxBuilder().build(report_text, image_body))

            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="Fetched report and generated spreadsheet with embedded chart bytes.",
                artifacts=[str(xlsx_path)],
                tool_results=[
                    ToolResult(
                        "fetch-report",
                        "web_fetch",
                        "completed",
                        {"url": report_url, "status_code": 200, "text": report_text},
                    ),
                    ToolResult(
                        "fetch-chart",
                        "web_fetch",
                        "completed",
                        {"url": chart_url, "status_code": 200, "content_type": "image/png"},
                    ),
                    ToolResult(
                        "write-xlsx",
                        "file_write",
                        "completed",
                        {"path": str(xlsx_path)},
                    ),
                ],
                reached_iteration_limit=False,
            )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(
        runtime,
        orchestrator=Orchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[artifact_root, tmp_path]),
    )

    with _MockWebServer(MockOriginHandler, port), TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "Fetch report and chart image, then generate spreadsheet",
                    "conversation_id": "web:e2e",
                    "stream": False,
                    "turn_id": "turn-xlsx-embedded",
                }
            )
            done = {}
            while True:
                event = websocket.receive_json()
                if event.get("type") == "done":
                    done = event
                    break

    artifact = done["artifacts"][0]
    downloaded = client.get(artifact["url"])
    assert done["type"] == "done"
    assert artifact["name"] == "quarterly.xlsx"
    assert artifact["media_type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert downloaded.status_code == 200
    assert b"image-b64:" in downloaded.content
    assert b"Revenue 11.2k" in downloaded.content


def test_http_chat_does_not_deliver_csv_when_scope_requires_xlsx(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    conversation_id = "web:e2e-scope-xlsx"
    artifact_root = artifact_root_for_principal(conversation_id)
    artifact_root.mkdir(parents=True, exist_ok=True)
    csv_path = artifact_root / "topaz.csv"
    csv_path.write_text("name,image\nring,$PLACEHOLDER_IMAGE\n", encoding="utf-8")

    class CsvOnlyOrchestrator:
        model_client = SimpleNamespace(name="csv-only")

        def run_turn(self, **kwargs):  # noqa: ANN003
            del kwargs
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="Created the spreadsheet.",
                artifacts=[str(csv_path)],
                tool_results=[
                    ToolResult(
                        "scope",
                        "request_tool_scope",
                        "completed",
                        {
                            "capabilities": ["local_files"],
                            "artifact_extensions": [".xlsx"],
                            "available_tools": ["spreadsheet_create"],
                        },
                    ),
                    ToolResult("write-csv", "file_write", "completed", {"path": str(csv_path)}),
                ],
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(
        runtime,
        orchestrator=CsvOnlyOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[artifact_root, tmp_path]),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={"text": "structured artifact request", "conversation_id": conversation_id, "stream": False},
        )

    assert response.status_code == 200
    payload = response.json()
    assert "couldn't attach the requested file" in payload["text"]
    assert payload["artifacts"] == []


def test_http_chat_delivers_complex_research_workbook_from_nullion_home_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import base64

    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    monkeypatch.delenv("NULLION_DATA_DIR", raising=False)
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "stage-home"))
    conversation_id = "web:e2e-complex-research"
    artifact_root = artifact_root_for_principal(conversation_id)
    artifact_root.mkdir(parents=True, exist_ok=True)
    source_listing_image = artifact_root / "etsy-source-listing.png"
    generated_style_badge = artifact_root / "generated-style-badge.png"
    html_preview = artifact_root / "research-preview.html"
    workbook_path = artifact_root / "etsy_witches_comparison.xlsx"
    source_listing_image.write_bytes(b"\x89PNG\r\nsource listing bytes")
    generated_style_badge.write_bytes(b"\x89PNG\r\ngenerated badge bytes")
    html_preview.write_text(
        "<html><body><h1>Research</h1><table><tr><td>Moon Witch Print</td></tr></table></body></html>",
        encoding="utf-8",
    )

    class ComplexResearchOrchestrator:
        model_client = SimpleNamespace(name="complex-research")

        def run_turn(self, **kwargs):  # noqa: ANN003
            del kwargs
            workbook_path.write_bytes(
                b"PK\x03\x04"
                + b"formatted workbook\n"
                + b"Moon Witch Print,$18.00,https://example.test/moon\n"
                + b"Kitchen Witch Doll,$34.00,https://example.test/doll\n"
                + b"Forest Witch Sticker,$5.00,https://example.test/sticker\n"
                + b"source-image:"
                + base64.b64encode(source_listing_image.read_bytes())
                + b"\n"
                + b"generated-image:"
                + base64.b64encode(generated_style_badge.read_bytes())
            )
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text=f"Created the formatted comparison workbook.\nMEDIA:{workbook_path}",
                artifacts=[str(workbook_path)],
                tool_results=[
                    ToolResult(
                        "scope",
                        "request_tool_scope",
                        "completed",
                        {
                            "capabilities": ["browser", "local_files"],
                            "artifact_extensions": [".xlsx"],
                            "embedded_media_artifact_extensions": [".xlsx"],
                            "required_tool_names": [
                                "browser_extract_items",
                                "browser_image_collect",
                                "spreadsheet_create",
                            ],
                        },
                    ),
                    ToolResult(
                        "nav",
                        "browser_navigate",
                        "completed",
                        {"url": "https://www.etsy.com/search?q=witches"},
                    ),
                    ToolResult(
                        "items",
                        "browser_extract_items",
                        "completed",
                        {
                            "items": [
                                {
                                    "title": "Moon Witch Print",
                                    "price": "$18.00",
                                    "url": "https://example.test/moon",
                                    "image_url": "https://example.test/moon.png",
                                },
                                {
                                    "title": "Kitchen Witch Doll",
                                    "price": "$34.00",
                                    "url": "https://example.test/doll",
                                    "image_url": "https://example.test/doll.png",
                                },
                                {
                                    "title": "Forest Witch Sticker",
                                    "price": "$5.00",
                                    "url": "https://example.test/sticker",
                                    "image_url": "https://example.test/sticker.png",
                                },
                            ],
                            "source": "rendered_page",
                        },
                    ),
                    ToolResult(
                        "collect",
                        "browser_image_collect",
                        "completed",
                        {
                            "artifact_paths": [str(source_listing_image)],
                            "image_paths": [str(source_listing_image)],
                            "saved_count": 1,
                        },
                    ),
                    ToolResult(
                        "generated",
                        "image_generate",
                        "completed",
                        {
                            "artifact_path": str(generated_style_badge),
                            "artifact_paths": [str(generated_style_badge)],
                        },
                    ),
                    ToolResult(
                        "html",
                        "html_create",
                        "completed",
                        {
                            "artifact_path": str(html_preview),
                            "artifact_paths": [str(html_preview)],
                        },
                    ),
                    ToolResult(
                        "sheet",
                        "spreadsheet_create",
                        "completed",
                        {
                            "artifact_path": str(workbook_path),
                            "artifact_paths": [str(workbook_path)],
                            "embedded_images": [str(source_listing_image), str(generated_style_badge)],
                            "embedded_screenshots": [],
                            "formatting": {"currency_columns": ["price"], "frozen_header": True},
                        },
                    ),
                ],
                reached_iteration_limit=False,
            )

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(
        runtime,
        orchestrator=ComplexResearchOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[artifact_root, tmp_path]),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat",
            json={
                "text": "compare researched products with website images, generated media, and formatting",
                "conversation_id": conversation_id,
                "stream": False,
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert "couldn't attach" not in payload["text"]
        assert len(payload["artifacts"]) == 1
        artifact = payload["artifacts"][0]
        downloaded = client.get(artifact["url"])

    assert artifact["name"] == "etsy_witches_comparison.xlsx"
    assert artifact["media_type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert downloaded.status_code == 200
    assert b"Moon Witch Print" in downloaded.content
    assert b"source-image:" in downloaded.content
    assert b"generated-image:" in downloaded.content


def test_tool_file_web_permission_and_approval_rejection_workflows(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion.policy import BoundaryKind
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolExecutor,
        ToolInvocation,
        ToolRegistry,
        ToolResult,
        ToolSideEffectClass,
        ToolSpec,
        ToolRiskLevel,
    )
    from nullion.web_app import create_app

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("outside content", encoding="utf-8")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry(filesystem_allowed_roots=[workspace])
    monkeypatch.setenv("NULLION_DIRECT_WEB_FETCH_ENABLED", "true")

    def risky_handler(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True})

    def file_read_handler(invocation: ToolInvocation) -> ToolResult:
        path = Path(str(invocation.arguments["path"])).resolve()
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"path": str(path), "content": path.read_text(encoding="utf-8")},
        )

    def web_fetch_handler(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"url": invocation.arguments["url"], "status_code": 200, "text": "ok"},
        )

    registry.register(
        ToolSpec(
            name="dangerous_task",
            description="Risky test action",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
        ),
        risky_handler,
    )
    registry.register(
        ToolSpec(
            name="file_read",
            description="Read a file",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        file_read_handler,
    )
    executor = ToolExecutor(store=runtime.store, registry=registry)
    app = create_app(runtime, orchestrator=None, registry=registry)

    with TestClient(app) as client:
        registry.unregister("web_fetch")
        registry.register(
            ToolSpec(
                name="web_fetch",
                description="Fetch a URL",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
            ),
            web_fetch_handler,
        )
        risky_denied = executor.invoke(
            ToolInvocation("risk-1", "dangerous_task", "web:operator", {"value": "go"})
        )
        risky_approval_id = str(risky_denied.output["approval_id"])
        risky_approval = client.post(f"/api/approve/{risky_approval_id}", json={"mode": "always"})
        risky_allowed = executor.invoke(
            ToolInvocation("risk-2", "dangerous_task", "web:operator", {"value": "go"})
        )

        file_denied = executor.invoke(
            ToolInvocation("file-1", "file_read", "web:operator", {"path": str(outside)})
        )
        file_approval_id = str(file_denied.output["approval_id"])
        file_reject = client.post(f"/api/reject/{file_approval_id}")
        file_reapprove = client.post(f"/api/approve/{file_approval_id}", json={"mode": "once"})

        web_denied = executor.invoke(
            ToolInvocation("web-1", "web_fetch", "web:operator", {"url": "https://example.com/report"})
        )
        web_approval_id = str(web_denied.output["approval_id"])
        web_approval = client.post(f"/api/approve/{web_approval_id}", json={"mode": "once"})
        web_allowed = executor.invoke(
            ToolInvocation("web-2", "web_fetch", "web:operator", {"url": "https://example.com/report"})
        )
        status = client.get("/api/status?full=1").json()

    assert risky_denied.status == "denied"
    assert risky_denied.output["requires_approval"] is True
    assert risky_approval.status_code == 200
    assert risky_approval.json()["ok"] is True
    assert risky_allowed.status == "completed"

    assert file_denied.status == "denied"
    assert file_denied.output["boundary_kind"] == BoundaryKind.FILESYSTEM_ACCESS.value
    assert file_reject.status_code == 200
    assert file_reject.json()["ok"] is True
    assert file_reapprove.status_code == 409

    assert web_denied.status == "denied"
    assert web_denied.output["boundary_kind"] == BoundaryKind.OUTBOUND_NETWORK.value
    assert web_approval.status_code == 200
    assert web_approval.json()["ok"] is True
    assert web_allowed.status == "completed"
    assert web_allowed.output["status_code"] == 200

    approvals_by_id = {item["approval_id"]: item for item in status["approvals"]}
    assert approvals_by_id[risky_approval_id]["status"] == "approved"
    assert approvals_by_id[file_approval_id]["status"] == "denied"
    assert approvals_by_id[web_approval_id]["status"] == "approved"
    assert any(permit["approval_id"] == web_approval_id for permit in status["boundary_permits"])


def test_default_web_fetch_can_be_exercised_against_mocked_http_origin(tmp_path: Path, monkeypatch) -> None:
    from nullion import tools
    from nullion.approvals import create_boundary_permit
    from nullion.policy import BoundaryKind
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolExecutor,
        ToolInvocation,
        _WebFetchResolution,
        create_core_tool_registry,
    )

    class MockOriginHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            assert self.path == "/report"
            body = (
                "<!doctype html><html><head><title>Mocked market report</title></head>"
                "<body><h1>Revenue rose 12 percent</h1></body></html>"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: A002, ANN001
            return

    port = _free_port()
    address_infos = ((socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", port)),)

    def fake_resolution(url: str):  # noqa: ANN001
        if url == f"http://mocked-web.test:{port}/report":
            return _WebFetchResolution(host="mocked-web.test", address_infos=address_infos)
        return None

    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    monkeypatch.setattr(tools, "_resolve_web_fetch_resolution", fake_resolution)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = create_core_tool_registry(workspace_root=tmp_path, direct_web_fetch_enabled=True)
    executor = ToolExecutor(store=runtime.store, registry=registry)

    with _MockWebServer(MockOriginHandler, port):
        denied = executor.invoke(
            ToolInvocation(
                "fetch-before-approval",
                "web_fetch",
                "web:e2e",
                {"url": f"http://mocked-web.test:{port}/report"},
            )
        )
        approval_id = str(denied.output["approval_id"])
        runtime.store.add_boundary_permit(
            create_boundary_permit(
                approval_id=approval_id,
                principal_id="web:e2e",
                boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
                selector=f"http://mocked-web.test:{port}/*",
                granted_by="doctor:e2e",
                permit_id="permit-e2e-web",
            )
        )
        fetched = executor.invoke(
            ToolInvocation(
                "fetch-after-approval",
                "web_fetch",
                "web:e2e",
                {"url": f"http://mocked-web.test:{port}/report"},
            )
        )

    assert denied.status == "denied"
    assert fetched.status == "completed"
    assert fetched.output["status_code"] == 200
    assert fetched.output["content_type"] == "text/html"
    assert fetched.output["title"] == "Mocked market report"
    assert "Revenue rose 12 percent" in str(fetched.output["text"])


def test_browser_ui_uses_saved_history_before_asking_numbered_clarification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from playwright.sync_api import expect
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from nullion.web_app import create_app

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "web-history-home"))
    runtime = bootstrap_persistent_runtime(tmp_path / "web-history-runtime.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(
        ToolSpec(
            name="terminal_exec",
            description="Run a local read-only command.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            capability_tags=("local_shell",),
        ),
        lambda invocation: ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"stdout": "sample-team/sample-app #28 Prepare release reliability fixes\n"},
        ),
    )
    runtime.active_tool_registry = registry

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_remember_web_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_enforce_web_response_fulfillment", lambda runtime, **kwargs: (kwargs["reply"], True))

    from nullion.agent_orchestrator import AgentOrchestrator

    class HistoryModel:
        calls = 0

        def create(self, **kwargs):
            names = {tool['name'] for tool in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return {'content': [{'type': 'tool_use', 'id': 'format', 'name': 'select_attachment_format',
                                     'input': {'extension': None, 'confidence': 1}}]}
            if not names:
                return {'content': [{'type': 'text', 'text': '{}'}]}
            self.calls += 1
            steps = [
                ('request_tool_scope', {'capabilities': ['conversation_history'], 'tool_names': ['chat_history_search']}),
                ('chat_history_search', {'query': 'sample-team/sample-app', 'limit': 12}),
                ('request_tool_scope', {'capabilities': ['local_shell'], 'tool_names': ['terminal_exec'], 'required_tool_names': ['terminal_exec']}),
                ('terminal_exec', {}),
            ]
            if self.calls <= len(steps):
                name, arguments = steps[self.calls - 1]
                if name == 'terminal_exec':
                    assert 'sample-team/sample-app #28' in str(kwargs['messages'])
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': f'history-{self.calls}',
                                                               'name': name, 'input': arguments}]}
            assert self.calls == 5
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text',
                    'text': 'Usé el historial guardado y revisé sample-team/sample-app #28.'}]}

    model = HistoryModel()
    orchestrator = AgentOrchestrator(model_client=model)
    app = create_app(runtime, orchestrator=orchestrator, registry=registry)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.wait_for_function(
                "() => typeof ws !== 'undefined' && ws && ws.readyState === WebSocket.OPEN",
                timeout=5000,
            )
            conversation_id = page.evaluate("localStorage.getItem('nullion_conv_id')")
            assert isinstance(conversation_id, str) and conversation_id.startswith("web:")
            runtime.store.add_conversation_event(
                {
                    "conversation_id": conversation_id,
                    "event_type": "conversation.chat_turn",
                    "created_at": datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc).isoformat(),
                    "turn_id": "turn-pr-cli",
                    "user_message": "repo sample-team/sample-app",
                    "assistant_reply": "Checked the saved pull request target with local CLI evidence.",
                    "tool_results": [
                        {
                            "tool_name": "terminal_exec",
                            "status": "completed",
                            "output": {"stdout": "sample-team/sample-app #28 Prepare release reliability fixes\n"},
                        }
                    ],
                }
            )
            for index in range(8):
                runtime.store.add_conversation_event(
                    {
                        "conversation_id": conversation_id,
                        "event_type": "conversation.chat_turn",
                        "created_at": datetime(2026, 5, 28, 12, index + 1, tzinfo=timezone.utc).isoformat(),
                        "turn_id": f"turn-filler-{index}",
                        "user_message": f"mensaje intermedio {index}",
                        "assistant_reply": f"respuesta intermedia {index}",
                    }
                )
            for index in range(2):
                runtime.store.add_conversation_event(
                    {
                        "conversation_id": conversation_id,
                        "event_type": "conversation.chat_turn",
                        "created_at": datetime(2026, 5, 28, 12, 20 + index, tzinfo=timezone.utc).isoformat(),
                        "turn_id": f"turn-scope-only-{index}",
                        "user_message": "sample-team/sample-app?",
                        "assistant_reply": "Necesito elegir una fuente:\n\n1. Sistema A\n2. Sistema B",
                        "tool_results": [
                            {
                                "tool_name": "request_tool_scope",
                                "status": "completed",
                                "output": {
                                    "scope_requested": True,
                                    "available_tools": [],
                                    "message": "No registered tools matched the requested capability scope.",
                                },
                            }
                        ],
                    }
                )

            page.locator("#user-input").fill("sample-team/sample-app?")
            page.locator("#send-btn").click()
            expect(page.locator(".msg.bot .bubble").filter(has_text="sample-team/sample-app #28")).to_have_count(1, timeout=5000)
            expect(page.locator("#messages")).not_to_contain_text("Sistema A", timeout=1000)
            assert model.calls == 5
            turns = [e for e in runtime.store.list_conversation_events(conversation_id) if e.get("event_type") == "conversation.chat_turn"]
            assert {r["tool_name"] for r in turns[-1]["tool_results"]} >= {"chat_history_search", "terminal_exec"}
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_shows_reply_before_slow_persistence_cleanup(tmp_path: Path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    class Orchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[],
                final_text="visible before save",
                artifacts=[],
                thinking_text="",
            )

    save_started = threading.Event()
    save_finished = threading.Event()

    def slow_save(*args, **kwargs):  # noqa: ANN002, ANN003
        save_started.set()
        time.sleep(1.0)
        save_finished.set()

    monkeypatch.setattr(web_app, "_try_dispatch_web_mini_agents", lambda **kwargs: None)
    monkeypatch.setattr(web_app, "_orchestrator_for_admin_forced_model", lambda orchestrator, runtime: orchestrator)
    monkeypatch.setattr(web_app, "_build_web_skill_hint", lambda runtime, user_text: None)
    monkeypatch.setattr(web_app, "_recent_web_tool_context_prompt", lambda runtime, conv_id: None)
    monkeypatch.setattr(web_app, "_remember_web_chat_turn", slow_save)
    monkeypatch.setattr(chat_operator, "_remember_chat_turn", slow_save)
    monkeypatch.setattr(web_app, "_remember_web_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        web_app,
        "_enforce_web_response_fulfillment",
        lambda runtime, **kwargs: (kwargs["reply"], True),
    )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    app = create_app(runtime, orchestrator=Orchestrator(), registry=registry)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.wait_for_function(
                "() => typeof ws !== 'undefined' && ws && ws.readyState === WebSocket.OPEN",
                timeout=5000,
            )
            page.locator("#user-input").fill("latency-e2e-42")
            page.locator("#send-btn").click()
            expect(page.locator(".msg.bot .bubble").filter(has_text="visible before save")).to_have_count(1, timeout=800)
            assert save_started.wait(timeout=1)
            assert not save_finished.is_set()
            assert save_finished.wait(timeout=3)
            expect(page.locator("#send-btn")).to_be_enabled(timeout=3000)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_modal_can_navigate_tabs_and_render_feedback(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#settings-btn").click()
            expect(page.locator("#settings-overlay")).to_be_visible()
            expect(page.locator("#tab-general")).to_have_class(re.compile(r"\bactive\b"))
            page.locator(".settings-nav button", has_text="Model").click()
            expect(page.locator("#tab-model")).to_have_class(re.compile(r"\bactive\b"))
            page.locator(".settings-nav button", has_text="Security").click()
            expect(page.locator("#tab-security")).to_have_class(re.compile(r"\bactive\b"))
            page.locator("#settings-overlay .modal-close").click()
            expect(page.locator("#settings-overlay")).not_to_be_visible()
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_general_exposes_memory_bucket_caps(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#settings-btn").click()
            expect(page.locator("#settings-overlay")).to_be_visible()
            expect(page.locator("#tab-general")).to_have_class(re.compile(r"\bactive\b"))
            expect(page.locator("#tab-general")).to_contain_text("Memory & Learning")
            assert page.locator("#cfg-memory-enabled").count() == 1
            expect(page.locator("#cfg-memory-long-term-limit")).to_be_visible()
            expect(page.locator("#cfg-memory-mid-term-limit")).to_be_visible()
            expect(page.locator("#cfg-memory-short-term-limit")).to_be_visible()
            expect(page.locator("#tab-general")).to_contain_text("Long-term")
            expect(page.locator("#tab-general")).to_contain_text("Mid-term")
            expect(page.locator("#tab-general")).to_contain_text("Short-term")
            expect(page.locator("#tab-general")).to_contain_text("Smart cleanup")
            expect(page.locator("#tab-general")).to_contain_text("Maximum saved memories per workspace")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_memory_modal_shows_score_legend_and_spaced_capsules(tmp_path: Path) -> None:
    from datetime import UTC, datetime

    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_web_admin
    from playwright.sync_api import expect

    app, runtime, _registry = _web_app(tmp_path)
    owner = memory_owner_for_web_admin()
    now = datetime.now(UTC)
    for key, kind, value, score in (
        ("stable_preference", UserMemoryKind.PREFERENCE, "User prefers short watcher alerts.", 8.0),
        ("workspace_folder", UserMemoryKind.ENVIRONMENT_FACT, "Open workspace_admin from the folder button.", 6.0),
        ("folder_note", UserMemoryKind.ENVIRONMENT_FACT, "Use the workspace folder view for project files.", 1.0),
        ("fresh_note", UserMemoryKind.FACT, "A fresh memory starts lighter.", 0.0),
    ):
        runtime.store.add_user_memory_entry(
            UserMemoryEntry(
                entry_id=f"{owner}:{key}",
                owner=owner,
                kind=kind,
                key=key,
                value=value,
                created_at=now,
                updated_at=now,
                use_score=score,
                use_count=int(score),
            )
        )

    with _UvicornThread(app, _free_port()) as server:
        with urllib.request.urlopen(f"{server.base_url}/assets/d3-force.bundle.min.js", timeout=2) as response:
            assert response.status == 200
            assert b"d3Force" in response.read()
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            view_all = page.locator("button", has_text="View all memories")
            expect(view_all).to_be_visible()
            view_all.click()
            expect(page.locator(".memory-map-legend")).to_be_visible()
            expect(page.locator(".memory-map-legend")).to_contain_text("More permanent")
            expect(page.locator(".memory-map-legend")).to_contain_text("Recent")
            expect(page.locator(".memory-legend-gradient")).to_be_visible()
            expect(page.locator(".memory-brain-node.score-4").first).to_have_css("padding-left", "12px")
            expect(page.locator(".memory-brain-node").first).not_to_contain_text("SCORE")
            expect(page.locator(".memory-brain-node")).to_have_count(4)
            page.wait_for_function(
                "() => [...document.querySelectorAll('.memory-brain-node')].every(node => node.style.left && node.style.top)"
            )
            overlaps = page.evaluate(
                """() => {
                  const nodes = [...document.querySelectorAll('.memory-brain-node')];
                  const boxes = nodes.map((node, index) => ({ index, rect: node.getBoundingClientRect() }));
                  const overlapPairs = [];
                  for (let i = 0; i < boxes.length; i += 1) {
                    for (let j = i + 1; j < boxes.length; j += 1) {
                      const a = boxes[i].rect;
                      const b = boxes[j].rect;
                      const separated = a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top;
                      if (!separated) overlapPairs.push([boxes[i].index, boxes[j].index]);
                    }
                  }
                  return overlapPairs;
                }"""
            )
            assert overlaps == []
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_builder_memory_full_card_runs_smart_cleanup(tmp_path: Path, monkeypatch) -> None:
    from datetime import UTC, datetime

    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_web_admin
    from playwright.sync_api import expect

    monkeypatch.setenv("NULLION_MEMORY_LONG_TERM_LIMIT", "2")
    monkeypatch.setenv("NULLION_MEMORY_MID_TERM_LIMIT", "10")
    monkeypatch.setenv("NULLION_MEMORY_SHORT_TERM_LIMIT", "10")
    monkeypatch.setenv("NULLION_MEMORY_SMART_CLEANUP_ENABLED", "false")
    app, runtime, _registry = _web_app(tmp_path)
    owner = memory_owner_for_web_admin()
    now = datetime.now(UTC)
    for key, value, score in (
        ("watcher_alerts", "User prefers watcher alerts only when meaningful.", 9.0),
        ("stale_preference", "Old preference that can be cleaned up.", 0.0),
    ):
        runtime.store.add_user_memory_entry(
            UserMemoryEntry(
                entry_id=f"{owner}:{key}",
                owner=owner,
                kind=UserMemoryKind.PREFERENCE,
                key=key,
                value=value,
                created_at=now,
                updated_at=now,
                use_score=score,
                use_count=int(score),
            )
        )

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator(".control-tab[data-pane='builder']").click()
            builder_pane = page.locator("#control-pane-builder")
            expect(builder_pane).to_contain_text("Agent memory is full")
            expect(builder_pane).to_contain_text("Increase memory")
            expect(builder_pane).to_contain_text("Smart cleanup")
            expect(builder_pane).to_contain_text("token usage")
            expect(builder_pane).to_contain_text("Enable it so Builder can remove lower-strength memories automatically")

            builder_pane.locator("button", has_text="Smart cleanup").click()
            expect(page.locator("#confirm-overlay")).to_be_visible()
            expect(page.locator("#confirm-message")).to_contain_text("lower-strength memories")
            page.locator("#confirm-ok").click()
            expect(builder_pane).not_to_contain_text("Agent memory is full")
            assert runtime.store.get_user_memory_entry(f"{owner}:watcher_alerts") is not None
            assert runtime.store.get_user_memory_entry(f"{owner}:stale_preference") is None
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_history_and_allhistory_modals_open_and_close(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#history-btn").click()
            history_overlay = page.locator("#history-overlay")
            expect(history_overlay).to_be_visible()
            expect(page.locator("#history-list")).to_be_visible()
            history_overlay.locator(".modal-close").click()
            expect(history_overlay).not_to_be_visible()

            page.locator("#allhistory-btn").click()
            allhistory_overlay = page.locator("#allhist-overlay")
            expect(allhistory_overlay).to_be_visible()
            expect(page.locator("#allhist-cal-grid")).to_be_visible()
            page.locator("#allhist-close").click()
            expect(allhistory_overlay).not_to_be_visible()
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_allhistory_rows_are_clickable_and_readable(tmp_path: Path, monkeypatch) -> None:
    import nullion.chat_store as chat_store_module

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    chat_store_module._store = None
    store = chat_store_module.get_chat_store()
    conversation_id = "telegram:shashank-history-e2e"
    store.save_message(
        conversation_id,
        "user",
        "Latest Shashank thread ping",
        channel="telegram:shashank-history-e2e",
        channel_label="Telegram · Shashank Hazrati",
    )
    store.save_message(
        conversation_id,
        "bot",
        "Shashank replied with the stage history contract message.",
        channel="telegram:shashank-history-e2e",
        channel_label="Telegram · Shashank Hazrati",
    )
    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.set_viewport_size({"width": 509, "height": 875})
            page.reload(wait_until="domcontentloaded")
            page.locator("#user-input").wait_for(state="visible", timeout=10000)
            page.locator("#allhistory-btn").click()
            expect(page.locator("#allhist-overlay")).to_be_visible()

            channel = page.get_by_role("button", name=re.compile("Open history channel Telegram .* Shashank Hazrati"))
            expect(channel).to_be_visible()
            assert channel.get_attribute("data-history-channel") == "telegram:shashank-history-e2e"
            channel.click()

            page.locator("#allhist-cal-days .ahcal-day.has-data").first.click()
            conversation = page.get_by_role("button", name=re.compile("Open history conversation"))
            expect(conversation).to_be_visible()
            assert conversation.get_attribute("data-history-conversation-id") == conversation_id
            conversation_box = conversation.bounding_box()
            assert conversation_box is not None
            assert conversation_box["width"] >= 300
            conversation.click()

            expect(page.locator("#allhist-messages")).to_contain_text("Latest Shashank thread ping")
            expect(page.locator("#allhist-messages")).to_contain_text(
                "Shashank replied with the stage history contract message."
            )
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_control_center_tabs_switch_between_sections(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            expect(page.locator("#control-pane-permissions")).to_have_class(re.compile(r"\bactive\b"))
            page.locator(".control-tab[data-pane='decisions']").click()
            expect(page.locator("#control-pane-decisions")).to_have_class(re.compile(r"\bactive\b"))
            expect(page.locator("#control-pane-permissions")).not_to_have_class(re.compile(r"\bactive\b"))
            page.locator(".control-tab[data-pane='builder']").click()
            expect(page.locator("#control-pane-builder")).to_have_class(re.compile(r"\bactive\b"))
            page.locator(".control-tab[data-pane='doctor']").click()
            expect(page.locator("#control-pane-doctor")).to_have_class(re.compile(r"\bactive\b"))
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_general_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "General")
            page.locator("#cfg-data-dir").fill("/tmp/nullion-e2e-general")
            _set_checked(page.locator("#cfg-memory-enabled"), False)
            _set_checked(page.locator("#cfg-web-search"), False)
            _set_checked(page.locator("#cfg-browser-enabled"), True)
            _set_checked(page.locator("#cfg-file-access"), False)
            _set_checked(page.locator("#cfg-terminal-enabled"), False)
            _set_checked(page.locator("#cfg-memory-smart-cleanup"), True)
            page.locator("#cfg-memory-long-term-limit").fill("20")
            page.locator("#cfg-memory-mid-term-limit").fill("10")
            page.locator("#cfg-memory-short-term-limit").fill("10")
            page.locator("#cfg-browser-backend").select_option("playwright")

            _close_settings_tab_after_save(page, "General")
            expect(page.locator("#cfg-data-dir")).to_have_value("/tmp/nullion-e2e-general")
            expect(page.locator("#cfg-memory-enabled")).not_to_be_checked()
            expect(page.locator("#cfg-web-search")).not_to_be_checked()
            expect(page.locator("#cfg-browser-enabled")).to_be_checked()
            expect(page.locator("#cfg-file-access")).not_to_be_checked()
            expect(page.locator("#cfg-terminal-enabled")).not_to_be_checked()
            expect(page.locator("#cfg-memory-smart-cleanup")).to_be_checked()
            expect(page.locator("#cfg-memory-long-term-limit")).to_have_value("20")
            expect(page.locator("#cfg-memory-mid-term-limit")).to_have_value("10")
            expect(page.locator("#cfg-memory-short-term-limit")).to_have_value("10")
            expect(page.locator("#cfg-browser-backend")).to_have_value("playwright")
            expect(page.locator("#browser-btn")).to_be_hidden()
            env_text = env_path.read_text(encoding="utf-8")
            assert 'NULLION_MEMORY_SMART_CLEANUP_ENABLED="true"' in env_text
            assert 'NULLION_MEMORY_LONG_TERM_LIMIT="20"' in env_text
            assert 'NULLION_MEMORY_MID_TERM_LIMIT="10"' in env_text
            assert 'NULLION_MEMORY_SHORT_TERM_LIMIT="10"' in env_text
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_messaging_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Messaging")
            _set_checked(page.locator("#cfg-chat-enabled"), True)
            _set_checked(page.locator("#cfg-tg-streaming-enabled"), True)
            page.locator("#cfg-tg-token").fill("tg-token-e2e")
            page.locator("#cfg-tg-chat-id").fill("123456789")
            _set_checked(page.locator("#cfg-slack-enabled"), True)
            page.locator("#cfg-slack-bot-token").fill("slack-token-e2e")
            _set_checked(page.locator("#cfg-discord-enabled"), True)
            page.locator("#cfg-discord-bot-token").fill("discord-token-e2e")
            page.locator("#cfg-discord-operator-user-id").fill("2001")

            _close_settings_tab_after_save(page, "Messaging")
            expect(page.locator("#cfg-chat-enabled")).to_be_checked()
            expect(page.locator("#cfg-tg-streaming-enabled")).to_be_checked()
            expect(page.locator("#cfg-tg-token")).to_have_value("••••••••")
            expect(page.locator("#cfg-tg-chat-id")).to_have_value("123456789")
            expect(page.locator("#cfg-slack-enabled")).to_be_checked()
            expect(page.locator("#cfg-slack-bot-token")).to_have_value("••••••••")
            expect(page.locator("#cfg-discord-enabled")).to_be_checked()
            expect(page.locator("#cfg-discord-bot-token")).to_have_value("••••••••")
            expect(page.locator("#cfg-discord-operator-user-id")).to_have_value("2001")
            expect(page.locator("#discord-status")).to_have_text("Configured")
            assert 'NULLION_DISCORD_OPERATOR_USER_ID="2001"' in env_path.read_text()
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_profile_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Profile")
            page.locator("#cfg-profile-name").fill("E2E Operator")
            page.locator("#cfg-profile-email").fill("operator@example.com")
            page.locator("#cfg-profile-phone").fill("+15550123456")
            page.locator("#cfg-profile-address").fill("101 E2E Road")
            page.locator("#cfg-profile-notes").fill("Focus on deterministic test delivery")

            _close_settings_tab_after_save(page, "Profile")
            expect(page.locator("#cfg-profile-name").first).to_have_value("E2E Operator")
            expect(page.locator("#cfg-profile-email")).to_have_value("operator@example.com")
            expect(page.locator("#cfg-profile-phone")).to_have_value("+15550123456")
            expect(page.locator("#cfg-profile-address")).to_have_value("101 E2E Road")
            expect(page.locator("#cfg-profile-notes")).to_have_value("Focus on deterministic test delivery")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_preferences_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Preferences")
            page.locator("#pref-persona").fill("Direct, practical style for validation checks.")
            _set_pref_chip(page, "pref-tone", "professional")
            _set_pref_chip(page, "pref-complexity", "technical")
            _set_pref_chip(page, "pref-length", "detailed")
            _set_pref_chip(page, "pref-structure", "bullets")
            _set_pref_chip(page, "pref-markdown", "full")
            _set_pref_chip(page, "pref-emoji", "minimal")
            _set_pref_chip(page, "pref-code", "always")
            _set_checked(page.locator("#pref-proactive"), False)
            _set_checked(page.locator("#pref-reasoning"), True)
            page.locator("#pref-timezone").fill("Europe/Paris")
            page.locator("#pref-dateformat").select_option("MM/DD/YYYY")
            page.locator("#pref-timeformat").select_option("24h")

            _close_settings_tab_after_save(page, "Preferences")
            expect(page.locator("#pref-persona")).to_have_value("Direct, practical style for validation checks.")
            _assert_pref_chip(page, "pref-tone", "professional")
            _assert_pref_chip(page, "pref-complexity", "technical")
            _assert_pref_chip(page, "pref-length", "detailed")
            _assert_pref_chip(page, "pref-structure", "bullets")
            _assert_pref_chip(page, "pref-markdown", "full")
            _assert_pref_chip(page, "pref-emoji", "minimal")
            _assert_pref_chip(page, "pref-code", "always")
            expect(page.locator("#pref-proactive")).not_to_be_checked()
            expect(page.locator("#pref-reasoning")).to_be_checked()
            expect(page.locator("#pref-timezone")).to_have_value("Europe/Paris")
            expect(page.locator("#pref-dateformat")).to_have_value("MM/DD/YYYY")
            expect(page.locator("#pref-timeformat")).to_have_value("24h")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_setup_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Setup")
            page.locator("#cfg-workspace-root").fill(str(tmp_path / "workspace"))
            page.locator("#cfg-allowed-roots").fill("/tmp/e2e-allow")
            page.locator("#cfg-search-provider").select_option("duckduckgo_instant_answer_provider")
            _select_option(page.locator("#cfg-audio-transcribe-provider"), "local_auto")
            _select_option(page.locator("#cfg-image-ocr-provider"), "local_auto")
            _select_option(page.locator("#cfg-image-generate-provider"), "")

            _close_settings_tab_after_save(page, "Setup")
            expect(page.locator("#cfg-workspace-root")).to_have_value(str(tmp_path / "workspace"))
            expect(page.locator("#cfg-allowed-roots")).to_have_value("/tmp/e2e-allow")
            expect(page.locator("#cfg-search-provider")).to_have_value("duckduckgo_instant_answer_provider")
            expect(page.locator("#cfg-audio-transcribe-provider")).to_have_value("")
            expect(page.locator("#cfg-image-ocr-provider")).to_have_value("")
            expect(page.locator("#cfg-image-generate-provider")).to_have_value("")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_users_tab_can_test_and_edit_imap_connection(tmp_path: Path, monkeypatch) -> None:
    import imaplib

    from nullion import connections, web_app

    class FakeImap:
        def __init__(self, host, port, timeout=None):  # noqa: ANN001
            self.host = host
            self.port = port
            self.timeout = timeout

        def login(self, username, password):  # noqa: ANN001
            assert username == "profile@example.com"
            assert password == "app-password"

        def select(self, mailbox, readonly=True):  # noqa: ANN001
            assert mailbox == "INBOX"
            assert readonly is True
            return "OK", [b"1"]

        def logout(self):
            pass

    env_path = tmp_path / ".env"
    home = tmp_path / "home"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("NULLION_HOME", str(home))
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)
    monkeypatch.setattr(connections, "_CONNECTIONS_PATH", tmp_path / "connections.json", raising=False)
    monkeypatch.setattr(imaplib, "IMAP4_SSL", FakeImap)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Users")
            page.locator("#new-connection-provider option[value='imap_smtp_provider']").wait_for(
                state="attached",
                timeout=5000,
            )
            page.locator("#new-connection-provider").select_option("imap_smtp_provider")
            expect(page.locator("#connection-test-btn")).to_be_visible()
            page.locator("#new-connection-profile").fill("FIXTURE_ACCOUNT")
            page.locator("#new-connection-imap-host").fill("imap.gmail.com")
            page.locator("#new-connection-imap-port").fill("993")
            page.locator("#new-connection-smtp-host").fill("smtp.gmail.com")
            page.locator("#new-connection-smtp-port").fill("587")
            page.locator("#new-connection-username").fill("profile@example.com")
            page.locator("#new-connection-password").fill("app-password")
            page.locator("#new-connection-label").fill("Example Gmail")

            page.locator("#connection-test-btn").click()
            expect(page.locator("#connection-test-feedback")).to_contain_text("IMAP connected", timeout=5000)
            page.locator("#connection-submit-btn").click()
            expect(page.locator(".connection-card").filter(has_text="Example Gmail")).to_be_visible(timeout=5000)

            _close_settings_tab_after_save(page, "Users")
            card = page.locator(".connection-card").filter(has_text="Example Gmail")
            card.locator("button", has_text="Edit").click()
            expect(page.locator("#connection-submit-btn")).to_have_text("Update connection")
            expect(page.locator("#new-connection-imap-host")).to_have_value("imap.gmail.com")
            expect(page.locator("#new-connection-username")).to_have_value("profile@example.com")
            expect(page.locator("#new-connection-password")).to_have_value("")
            expect(page.locator("#connection-test-feedback")).to_contain_text("Password is stored", timeout=5000)

            page.locator("#new-connection-label").fill("Example Gmail IMAP")
            page.locator("#connection-submit-btn").click()
            expect(page.locator(".connection-card").filter(has_text="Example Gmail IMAP")).to_be_visible(timeout=5000)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_setup_tab_search_keys_and_media_modes_are_saved(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)
    saved_config_payload: dict[str, object] = {}

    def _record_config(route):
        if route.request.method == "POST":
            saved_config_payload.update(_request_post_json(route.request) or {})
        route.continue_()

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/config", _record_config)

            _open_settings_tab(page, "Setup")
            page.locator("#cfg-search-provider").select_option("brave_search_provider")
            page.locator("#cfg-brave-search-key").fill("brave-e2e-key")
            _select_option(page.locator("#cfg-audio-transcribe-provider"), "local_auto")
            _select_option(page.locator("#cfg-image-ocr-provider"), "model")

            _close_settings_tab_after_save(page, "Setup")

            assert saved_config_payload["search_provider"] == "brave_search_provider"
            assert saved_config_payload["brave_search_key"] == "brave-e2e-key"
            assert saved_config_payload["audio_transcribe_provider"] == ""
            assert saved_config_payload["audio_transcribe_model"] == ""
            assert saved_config_payload["image_ocr_provider"] == ""
            assert saved_config_payload["image_ocr_model"] == ""

            page.locator("#settings-overlay .modal-close").click()
            expect(page.locator("#settings-overlay")).not_to_be_visible()
            _open_settings_tab(page, "Setup")
            expect(page.locator("#cfg-search-provider")).to_have_value("brave_search_provider", timeout=5000)
            expect(page.locator("#cfg-audio-transcribe-provider")).to_have_value("")
            expect(page.locator("#cfg-image-ocr-provider")).to_have_value("")
            expect(page.locator("#cfg-brave-search-key")).to_have_value("••••••••")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_execution_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Execution")
            _set_checked(page.locator("#cfg-task-decomposition"), False)
            _set_checked(page.locator("#cfg-multi-agent"), False)
            _set_checked(page.locator("#cfg-background-tasks"), False)
            _set_checked(page.locator("#tab-execution #cfg-verbose-enabled"), False)
            _set_checked(page.locator("#tab-execution #cfg-show-thinking"), True)
            page.locator("#cfg-mini-agent-timeout").fill("360")
            page.locator("#cfg-mini-agent-max-iterations").fill("20")
            page.locator("#cfg-mini-agent-max-continuations").fill("2")
            page.locator("#cfg-repeated-tool-failure-limit").fill("7")
            page.locator("#cfg-mini-agent-stale-after").fill("999")

            _close_settings_tab_after_save(page, "Execution")
            expect(page.locator("#cfg-task-decomposition")).not_to_be_checked()
            expect(page.locator("#cfg-multi-agent")).not_to_be_checked()
            expect(page.locator("#cfg-background-tasks")).not_to_be_checked()
            expect(page.locator("#tab-execution #cfg-verbose-enabled")).not_to_be_checked()
            expect(page.locator("#tab-execution #cfg-task-planner-feed-enabled")).to_have_count(0)
            expect(page.locator("#tab-execution #cfg-show-thinking")).to_be_checked()
            expect(page.locator("#cfg-mini-agent-timeout")).to_have_value("360")
            expect(page.locator("#cfg-mini-agent-max-iterations")).to_have_value("20")
            expect(page.locator("#cfg-mini-agent-max-continuations")).to_have_value("2")
            expect(page.locator("#cfg-repeated-tool-failure-limit")).to_have_value("7")
            expect(page.locator("#cfg-mini-agent-stale-after")).to_have_value("999")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_doctor_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Doctor")
            _set_checked(page.locator("#cfg-doctor-enabled"), False)
            _set_checked(page.locator("#cfg-smart-cleanup-enabled"), False)
            expect(page.locator("#tab-doctor #cfg-verbose-enabled")).to_have_count(0)
            expect(page.locator("#tab-doctor #cfg-task-planner-feed-enabled")).to_have_count(0)
            expect(page.locator("#tab-doctor #cfg-show-thinking")).to_have_count(0)
            _set_checked(page.locator("#cfg-proactive-reminders"), False)

            _close_settings_tab_after_save(page, "Doctor")
            expect(page.locator("#cfg-doctor-enabled")).not_to_be_checked()
            expect(page.locator("#cfg-smart-cleanup-enabled")).not_to_be_checked()
            expect(page.locator("#tab-doctor #cfg-verbose-enabled")).to_have_count(0)
            expect(page.locator("#tab-doctor #cfg-task-planner-feed-enabled")).to_have_count(0)
            expect(page.locator("#tab-doctor #cfg-show-thinking")).to_have_count(0)
            expect(page.locator("#cfg-proactive-reminders")).not_to_be_checked()
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_security_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Security")
            _set_checked(page.locator("#pref-auto-mode"), False)
            page.locator("#pref-approval-strictness").select_option("relaxed")
            _set_checked(page.locator("#pref-confirm"), True)
            _set_pref_chip(page, "pref-sentinel-mode", "ask_all")
            _set_pref_chip(page, "pref-outbound-mode", "ask_all")
            page.locator("#cfg-web-session-allow-duration").select_option("today")

            _close_settings_tab_after_save(page, "Security")
            expect(page.locator("#pref-auto-mode")).not_to_be_checked()
            expect(page.locator("#pref-approval-strictness")).to_have_value("relaxed")
            expect(page.locator("#pref-confirm")).to_be_checked()
            _assert_pref_chip(page, "pref-sentinel-mode", "ask_all")
            _assert_pref_chip(page, "pref-outbound-mode", "ask_all")
            expect(page.locator("#cfg-web-session-allow-duration")).to_have_value("today")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_builder_tab_persistence(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Builder")
            _set_checked(page.locator("#cfg-skill-learning"), False)
            _set_checked(page.locator(".cfg-skill-pack-choice[value='nullion/web-research']"), True)

            _close_settings_tab_after_save(page, "Builder")
            expect(page.locator("#cfg-skill-learning")).not_to_be_checked()
            enabled_skill_packs = page.locator("#cfg-enabled-skill-packs").input_value()
            assert "nullion/web-research" in enabled_skill_packs
            assert "nullion/connector-skills" in enabled_skill_packs
            assert not errors
        finally:
            browser.close()
            playwright.stop()


@pytest.mark.parametrize("member_name", ["エイダ", "آدا"])
def test_browser_ui_users_waits_for_one_registry_load_before_editing(tmp_path: Path, monkeypatch, member_name) -> None:
    from nullion import users

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "users.json")
    app, _runtime, _registry = _web_app(tmp_path)
    release_response = threading.Event()
    user_loads = []

    @app.middleware("http")
    async def delay_registry_response(request, call_next):
        if request.method == "GET" and request.url.path == "/api/users":
            user_loads.append(request.url.path)
            await asyncio.to_thread(release_response.wait, 10)
        return await call_next(request)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Users")
            page.locator(".settings-nav button", has_text="Users").click()
            add_button = page.get_by_role("button", name="Add member", exact=True)
            expect(add_button).to_be_disabled()
            expect(page.locator("#users-enabled")).to_be_disabled()
            page.locator("#new-user-name").fill(member_name)
            page.locator("#new-user-telegram-chat").fill("123456789")
            release_response.set()
            expect(add_button).to_be_enabled(timeout=5000)
            assert len(user_loads) == 1
            add_button.click()
            expect(page.locator(".user-card").filter(has_text=member_name)).to_be_visible()
            _close_settings_tab_after_save(page, "Users")
            saved = json.loads(users._USERS_PATH.read_text())
            assert any(user["display_name"] == member_name for user in saved["users"])
            expect(page.locator(".user-card").filter(has_text=member_name)).to_be_visible()
            assert not errors
        finally:
            release_response.set()
            browser.close()
            playwright.stop()


def test_browser_ui_settings_users_tab_member_crud_and_toggle(tmp_path: Path, monkeypatch) -> None:
    from nullion import users, web_app
    from playwright.sync_api import expect

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "nullion-home" / "users.json")
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            user_loads = []
            page.on("request", lambda request: user_loads.append(request.url)
                    if request.method == "GET" and request.url.endswith("/api/users") else None)
            _open_settings_tab(page, "Users")
            page.locator("#new-user-name").fill("Ada Lovelace")
            page.locator("#new-user-channel").select_option("telegram")
            page.locator("#new-user-telegram-chat").fill("123456789")
            page.locator("#new-user-notes").fill("Builds tooling around schedule workflows.")
            page.locator("button", has_text="Add member").click()

            member_card = page.locator(".user-card").filter(has_text="Ada Lovelace")
            expect(member_card).to_be_visible(timeout=5000)
            assert len(user_loads) == 1, "Concurrent registry loads can overwrite unsaved members"
            member_card.locator("input[id^='user-name-']").fill("Ada L")
            member_card.locator("input[id^='user-identity-']").fill("987654321")
            member_card.locator("textarea").fill("Builds tooling around schedule workflows.")
            _set_checked(member_card.locator("input[aria-label^='Active member']"), False)

            _close_settings_tab_after_save(page, "Users")
            expect(page.locator("#users-list")).to_contain_text("Ada L", timeout=5000)

            reloaded_card = page.locator(".user-card").filter(has_text="Ada L")
            expect(reloaded_card.locator("input[id^='user-name-']")).to_have_value("Ada L")
            expect(reloaded_card.locator("input[id^='user-identity-']")).to_have_value("987654321")
            expect(reloaded_card.locator("textarea")).to_have_value("Builds tooling around schedule workflows.")
            saved_users = json.loads(users._USERS_PATH.read_text())
            saved_member = next(user for user in saved_users["users"] if user["display_name"] == "Ada L")
            assert saved_member["messaging_user_id"] == "987654321"
            assert saved_member["active"] is False

            reloaded_card.locator(".member-remove-btn").click()
            expect(page.locator("#confirm-overlay")).to_be_visible()
            page.locator("#confirm-ok").click()
            expect(page.locator("#save-feedback")).to_contain_text("Member removed", timeout=5000)
            expect(page.locator(".user-card")).to_have_count(1)
            expect(page.locator(".user-card").filter(has_text="Ada L")).to_have_count(0)
            _close_settings_tab_after_save(page, "Users")
            expect(page.locator(".user-card")).to_have_count(1)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_crons_tab_add_update_delete_and_workspace_scoped_rules(tmp_path: Path, monkeypatch) -> None:
    from nullion import users, web_app
    from playwright.sync_api import expect

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "nullion-home" / "users.json")
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    posted: dict[str, object] = {}
    patched: dict[str, object] = {}
    deleted: dict[str, bool] = {}

    def _record_crons(route):
        if route.request.url.endswith("/api/crons"):
            if route.request.method == "POST":
                posted.update(_request_post_json(route.request) or {})
            if route.request.method == "PATCH":
                patched.update(_request_post_json(route.request) or {})
        elif "/api/crons/" in route.request.url:
            if route.request.method == "PATCH":
                patched.update(_request_post_json(route.request) or {})
            if route.request.method == "DELETE":
                deleted["method"] = True
        route.continue_()

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/crons*", _record_crons)

            _open_settings_tab(page, "Users")
            page.locator("#new-user-name").fill("E2E Reminder")
            page.locator("#new-user-channel").select_option("telegram")
            page.locator("#new-user-telegram-chat").fill("123456789")
            page.locator("button", has_text="Add member").click()
            expect(page.locator(".user-card")).to_have_count(2, timeout=5000)

            workspace_id = page.locator(".user-card").filter(has_text="E2E Reminder").locator("input[id^='user-workspace-']").input_value()
            _close_settings_tab_after_save(page, "Users")

            page.locator(".settings-nav button", has_text="Crons").click()
            page.locator("#cron-add-btn").click()
            page.locator("#cron-form-name").fill("E2E Reminder")
            page.locator("#cron-form-workspace").select_option(workspace_id)
            page.locator("#cron-form-schedule").fill("0 9 * * 1-5")
            page.locator("#cron-form-task").fill("Run daily status check and report blockers.")
            page.locator("#cron-form-delivery").select_option("web")
            page.locator("#cron-form").locator("button", has_text="Save cron").click()

            cron_row = page.locator("#crons-tab-list .cron-row").filter(has_text="E2E Reminder")
            expect(cron_row).to_be_visible(timeout=5000)
            expect(cron_row.locator(".cron-meta").first).to_contain_text(workspace_id)

            cron_row_id = cron_row.get_attribute("id") or ""
            cron_id = cron_row_id.removeprefix("crow-")
            with page.expect_request(lambda request: f"/api/crons/{cron_id}" in request.url and request.method == "PATCH") as patch_request:
                page.evaluate("(id) => toggleCronItem(id, false)", cron_id)
            patch_payload = _request_post_json(patch_request.value) or {}
            expect(cron_row.get_by_role("checkbox", name="Disabled — click to enable", exact=True)).not_to_be_checked()

            cron_row.locator("button.cron-del").click()
            expect(page.locator("#confirm-overlay")).to_be_visible()
            with page.expect_request(lambda request: f"/api/crons/{cron_id}" in request.url and request.method == "DELETE") as delete_request:
                page.locator("#confirm-ok").click()
            expect(cron_row).to_have_count(0, timeout=5000)

            assert posted["workspace_id"] == workspace_id
            assert posted["delivery_channel"] == "web"
            assert posted["delivery_target"] == "web:operator"
            assert posted["name"] == "E2E Reminder"
            assert posted["enabled"] is True
            assert patch_payload["enabled"] is False
            assert delete_request.value.method == "DELETE"
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_logs_tab_load_and_source_switch(tmp_path: Path) -> None:
    from playwright.sync_api import expect
    from urllib.parse import parse_qs, urlsplit

    app, _runtime, _registry = _web_app(tmp_path)

    sources_seen: set[str] = set()

    def _fake_logs_route(route):
        parsed = parse_qs(urlsplit(route.request.url).query)
        source = parsed.get("source", ["memory"])[0]
        sources_seen.add(source)
        if source == "memory":
            payload = {
                "ok": True,
                "source": "memory",
                "sources": [
                    {"value": "memory", "label": "Memory"},
                    {"value": "runtime", "label": "runtime.log"},
                ],
                "text": "memory log stream",
                "entry_count": 3,
            }
        else:
            payload = {
                "ok": True,
                "source": source,
                "sources": [
                    {"value": "memory", "label": "Memory"},
                    {"value": "runtime", "label": "runtime.log"},
                ],
                "text": "runtime log stream",
                "truncated": False,
                "bytes_returned": 32,
                "file_size": 32,
            }
        route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/logs*", _fake_logs_route)

            _open_settings_tab(page, "Logs")
            expect(page.locator("#logs-output")).to_have_text("memory log stream", timeout=5000)
            page.locator("#logs-source").select_option("runtime")
            expect(page.locator("#logs-output")).to_have_text("runtime log stream", timeout=5000)

            assert "memory" in sources_seen
            assert "runtime" in sources_seen
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_logs_tab_copy_action(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    def _fake_logs_route(route):
        payload = {
            "ok": True,
            "source": "memory",
            "sources": [
                {"value": "memory", "label": "Memory"},
            ],
            "text": "memory copy e2e text",
            "entry_count": 4,
        }
        route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/logs*", _fake_logs_route)

            _open_settings_tab(page, "Logs")
            expect(page.locator("#logs-output")).to_have_text("memory copy e2e text", timeout=5000)

            page.evaluate(
                """
                window.__nullionCopiedText = null;
                if (!window.navigator.clipboard) {
                    window.navigator.clipboard = {};
                }
                window.navigator.clipboard.writeText = async (value) => {
                    window.__nullionCopiedText = value;
                };
                """
            )
            page.locator("#tab-logs button", has_text="Copy").click()
            page.wait_for_function("window.__nullionCopiedText === 'memory copy e2e text'", timeout=2000)
            assert page.evaluate("window.__nullionCopiedText") == "memory copy e2e text"
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_security_domain_policy_add_and_remove(tmp_path: Path, monkeypatch) -> None:
    from nullion import web_app
    from playwright.sync_api import expect

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Security")
            page.locator("#pref-domain-allow-input").fill("allow-domain.example")
            page.locator(".domain-policy-list.allow .domain-add-btn").click()
            page.locator("#pref-domain-block-input").fill("blocked-domain.example")
            page.locator(".domain-policy-list.block .domain-add-btn").click()

            expect(page.locator("#pref-domain-allow-list .domain-chip-text")).to_contain_text("allow-domain.example", timeout=5000)
            expect(page.locator("#pref-domain-block-list .domain-chip-text")).to_contain_text("blocked-domain.example", timeout=5000)

            allow_card = page.locator("#pref-domain-allow-list .domain-chip").filter(has_text="allow-domain.example").first
            allow_card.locator(".domain-remove-btn").click()
            expect(page.locator("#pref-domain-allow-list")).to_contain_text("No saved")
            block_card = page.locator("#pref-domain-block-list .domain-chip").filter(has_text="blocked-domain.example").first
            block_card.locator(".domain-remove-btn").click()
            expect(page.locator("#pref-domain-block-list")).to_contain_text("No saved")
            expect(page.locator("#pref-domain-allow-count")).to_have_text("0")
            expect(page.locator("#pref-domain-block-count")).to_have_text("0")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_security_sentinel_modes_and_permissions_are_saved(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)
    posted_prefs: dict[str, object] = {}

    def _record_preferences(route):
        if route.request.method == "POST" and route.request.url.endswith("/api/preferences"):
            posted_prefs.update(_request_post_json(route.request) or {})
        route.continue_()

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/preferences", _record_preferences)

            _open_settings_tab(page, "Security")
            _set_pref_chip(page, "pref-sentinel-mode", "ask_all")
            _set_pref_chip(page, "pref-outbound-mode", "allow_all")
            _set_checked(page.locator("#pref-auto-mode"), False)
            _set_checked(page.locator("#pref-confirm"), False)
            page.locator("#pref-approval-strictness").select_option("relaxed")
            page.locator("#cfg-web-session-allow-duration").select_option("2h")

            _close_settings_tab_after_save(page, "Security")

            assert posted_prefs["sentinel_mode"] == "ask_all"
            assert posted_prefs["outbound_request_mode"] == "allow_all"
            assert posted_prefs["auto_mode"] is False
            assert posted_prefs["approval_strictness"] == "relaxed"
            assert posted_prefs["confirm_before_action"] is False
            page.locator("#settings-overlay .modal-close").click()
            expect(page.locator("#settings-overlay")).not_to_be_visible()

            _open_settings_tab(page, "Security")
            expect(page.locator("#cfg-web-session-allow-duration")).to_have_value("2h", timeout=5000)
            expect(page.locator("#pref-auto-mode")).not_to_be_checked()
            expect(page.locator("#pref-confirm")).not_to_be_checked()
            expect(page.locator("#pref-approval-strictness")).to_have_value("relaxed")
            _assert_pref_chip(page, "pref-sentinel-mode", "ask_all")
            _assert_pref_chip(page, "pref-outbound-mode", "allow_all")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_builder_tab_skill_pack_install_action(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)
    seen_payload = {}

    def _fake_install(route):
        seen_payload["body"] = _request_post_json(route.request)
        payload = {
            "ok": True,
            "pack": {
                "pack_id": "owner/e2e-pack",
                "source": "https://example.com/owner/e2e-pack",
                "installed_at": "2026-01-01T00:00:00Z",
                "skills_count": 2,
                "warnings": [],
                "path": "/tmp/e2e-pack",
            },
            "enabled": True,
            "updated": ["owner/e2e-pack"],
        }
        route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/skill-packs/install", _fake_install)

            _open_settings_tab(page, "Builder")
            page.locator("#cfg-skill-pack-source").fill("https://example.com/owner/e2e-pack")
            page.locator("#cfg-skill-pack-install-id").fill("owner/e2e-pack")
            page.locator("button", has_text="Install and enable").click()

            expect(page.locator("#skill-pack-install-status")).to_contain_text("Installed owner/e2e-pack", timeout=5000)
            assert seen_payload["body"]["source"] == "https://example.com/owner/e2e-pack"
            assert seen_payload["body"]["pack_id"] == "owner/e2e-pack"
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_messaging_tab_restart_services_action(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)
    called: dict[str, bool] = {}

    def _fake_restart_services(route):
        called["restarted"] = True
        payload = {
            "ok": True,
            "message": "Chat services restarted (test).",
            "services": {"telegram": True},
        }
        route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/chat-services/restart", _fake_restart_services)

            _open_settings_tab(page, "Messaging")
            page.locator("#restart-chat-services-btn").click()

            expect(page.locator("#restart-chat-services-status")).to_have_text("Chat services restarted (test).", timeout=5000)
            assert called.get("restarted", False)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_doctor_tab_run_diagnose_action(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    def _fake_diagnose(route):
        payload = {
            "ok": True,
            "report": {
                "summary": "Doctor diagnosis completed for e2e path.",
                "repaired_mini_agent_run_ids": ["agent-1"],
            },
        }
        route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/doctor/diagnose", _fake_diagnose)

            _open_settings_tab(page, "Doctor")
            page.locator("button", has_text="Run diagnose").click()

            expect(page.locator("#save-feedback")).to_contain_text("Doctor diagnosis completed for e2e path", timeout=5000)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_doctor_tab_cleanup_uses_retention_controls(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)
    cleanup_requests = []

    def _fake_cleanup(route):  # noqa: ANN001
        cleanup_requests.append(_request_post_json(route.request))
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(
                {
                    "ok": True,
                    "removed": {"chat_events": 1},
                    "before_storage": {"total_bytes": 2048},
                    "storage": {"total_bytes": 1024},
                    "bytes_freed": 1024,
                }
            ),
        )

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/doctor/cleanup", _fake_cleanup)

            _open_settings_tab(page, "Doctor")
            page.locator("#cfg-data-retention-days").select_option("7")
            for value in ("builder", "doctor", "tasks", "approvals", "logs"):
                _set_checked(page.locator(f"#doctor-data-types input[value='{value}']"), False)
            expect(page.locator("#cfg-data-retention-days")).to_have_value("7")
            assert page.evaluate("dataRetentionDaysPayloadValue()") == 7
            assert page.evaluate("dataRetentionTypesPayloadValue()") == ["chat"]

            page.locator("#tab-doctor button", has_text="Clean up now").click()

            expect(page.locator("#save-feedback")).to_contain_text("Cleaned 1 stale or duplicate item", timeout=5000)
            expect(page.locator("#confirm-title")).to_have_text("Cleanup complete")
            expect(page.locator("#confirm-message")).to_contain_text("1 chat records")
            expect(page.locator("#confirm-message")).to_contain_text("1 KB reclaimed")
            page.locator("#confirm-ok").click()
            expect(page.locator("#confirm-overlay")).not_to_be_visible()
            assert cleanup_requests == [{"data_retention_days": 7, "data_retention_types": ["chat"]}]
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_setup_tab_does_not_expose_obsolete_workspace_restart(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Setup")
            expect(page.locator("#workspace-restart-btn")).to_have_count(0)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_discard_unsaved_changes_prompt(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#settings-btn").click()
            expect(page.locator("#settings-overlay")).to_be_visible()

            page.locator("#cfg-data-dir").fill("/tmp/nullion-test")
            page.locator("#settings-overlay .modal-close").click()
            confirm_overlay = page.locator("#confirm-overlay")
            expect(confirm_overlay).to_be_visible()
            expect(page.locator("#confirm-message")).to_contain_text("Close Settings and lose those changes?")
            page.locator("#confirm-ok").click()
            expect(page.locator("#settings-overlay")).not_to_be_visible()
            expect(confirm_overlay).not_to_be_visible()
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_model_test_requires_model_input(tmp_path: Path) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#settings-btn").click()
            page.locator(".settings-nav button", has_text="Model").click()
            page.locator("#cfg-model-provider").select_option("custom")
            while page.locator("#chat-model-list .chat-model-remove-btn").count():
                page.locator("#chat-model-list .chat-model-remove-btn").first.click()
            page.locator("#model-test-btn").click()
            expect(page.locator("#model-test-feedback")).to_have_text(
                "Enter at least one chat model for the selected provider.",
                timeout=2000,
            )
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_model_settings_save_is_persisted_and_reloaded(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#settings-btn").click()
            page.locator(".settings-nav button", has_text="Model").click()

            page.locator("#cfg-model-provider").select_option("groq")
            expect(page.locator("#cfg-model-provider")).to_have_value("groq")
            page.locator("#cfg-chat-model-name").fill("llama-3.1-e2e")
            page.locator("#model-add-btn").click()
            expect(page.locator("#chat-model-list")).to_contain_text("llama-3.1-e2e", timeout=5000)
            page.locator("#cfg-api-key").fill("sk-groq-test-e2e")

            page.locator('.modal-footer button:has-text("Save changes")').click()
            expect(page.locator("#save-feedback")).to_have_text("✓ Saved", timeout=5000)

            page.locator("#settings-overlay .modal-close").click()
            expect(page.locator("#settings-overlay")).not_to_be_visible()

            page.locator("#settings-btn").click()
            expect(page.locator("#settings-overlay")).to_be_visible()
            page.locator(".settings-nav button", has_text="Model").click()
            expect(page.locator("#cfg-model-provider")).to_have_value("groq", timeout=5000)
            expect(page.locator("#cfg-model-name")).to_have_value("llama-3.1-e2e")
            expect(page.locator("#chat-model-list")).to_contain_text("llama-3.1-e2e")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_chat_models_can_be_added_defaulted_and_deleted(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#settings-btn").click()
            page.locator(".settings-nav button", has_text="Model").click()
            page.locator("#cfg-model-provider").select_option("codex")

            while page.locator("#chat-model-list .chat-model-remove-btn").count():
                page.locator("#chat-model-list .chat-model-remove-btn").first.click()
            expect(page.locator("#chat-model-list")).to_contain_text("No chat models added")

            page.locator("#cfg-chat-model-name").fill("gpt-5.4-e2e")
            page.locator("#model-add-btn").click()
            page.locator("#cfg-chat-model-name").fill("gpt-5.5-e2e")
            page.locator("#model-add-btn").click()
            first = page.locator("#chat-model-list .chat-model-item").filter(has_text="gpt-5.4-e2e")
            second = page.locator("#chat-model-list .chat-model-item").filter(has_text="gpt-5.5-e2e")
            expect(first).to_have_count(1, timeout=5000)
            expect(second).to_have_count(1, timeout=5000)
            expect(first).to_contain_text("gpt-5.4-e2e")
            expect(second).to_contain_text("gpt-5.5-e2e")
            second.locator("button", has_text="Make default").click()
            expect(page.locator("#chat-model-list .chat-model-item").first).to_contain_text("gpt-5.5-e2e")
            expect(page.locator("#cfg-model-name")).to_have_value("gpt-5.5-e2e, gpt-5.4-e2e")

            page.locator("#chat-model-list .chat-model-item").filter(has_text="gpt-5.4-e2e").locator(".chat-model-remove-btn").click(force=True)
            expect(page.locator("#cfg-model-name")).to_have_value("gpt-5.5-e2e")
            expect(page.locator("#chat-model-list")).not_to_contain_text("gpt-5.4-e2e")

            page.locator('.modal-footer button:has-text("Save changes")').click()
            expect(page.locator("#save-feedback")).to_have_text("✓ Saved", timeout=5000)

            page.locator("#settings-overlay .modal-close").click()
            expect(page.locator("#settings-overlay")).not_to_be_visible()
            page.locator("#settings-btn").click()
            expect(page.locator("#settings-overlay")).to_be_visible()
            page.locator(".settings-nav button", has_text="Model").click()
            page.locator("#cfg-model-provider").select_option("codex")
            expect(page.locator("#chat-model-list")).to_contain_text("gpt-5.5-e2e")
            expect(page.locator("#chat-model-list")).not_to_contain_text("gpt-5.4-e2e")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_media_models_can_be_added_defaulted_and_deleted(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#settings-btn").click()
            page.locator(".settings-nav button", has_text="Model").click()
            page.locator("#cfg-model-provider").select_option("openai")
            page.locator("#cfg-api-key").fill("sk-openai-e2e")

            page.locator("#cfg-media-model-name").fill("whisper-1")
            page.locator("#cfg-media-model-type").select_option("audio_input")
            page.locator(".media-model-form-grid .card-btn.secondary", has_text="Add").click()
            expect(page.locator("#media-model-list")).to_contain_text("whisper-1", timeout=5000)

            page.locator("#cfg-media-model-name").fill("gpt-4o-e2e")
            page.locator("#cfg-media-model-type").select_option("image_input")
            page.locator(".media-model-form-grid .card-btn.secondary", has_text="Add").click()
            row = page.locator("#media-model-list .media-model-item").filter(has_text="gpt-4o-e2e")
            expect(row).to_have_count(1, timeout=5000)
            if row.locator("button.media-default-btn").filter(has_text="Make default").count():
                row.locator("button.media-default-btn").filter(has_text="Make default").click()
            expect(page.locator("#cfg-image-ocr-provider")).to_have_value("model")
            expect(page.locator("#cfg-image-ocr-model")).to_have_value("openai::gpt-4o-e2e")

            whisper_row = page.locator("#media-model-list .media-model-item").filter(has_text="whisper-1")
            if whisper_row.locator("button.media-default-btn").filter(has_text="Make default").count():
                whisper_row.locator("button.media-default-btn").filter(has_text="Make default").click()
            expect(page.locator("#cfg-audio-transcribe-provider")).to_have_value("model")
            expect(page.locator("#cfg-audio-transcribe-model")).to_have_value("openai::whisper-1")

            whisper_row.locator(".media-remove-btn").click()
            expect(page.locator("#cfg-audio-transcribe-provider")).to_have_value("")
            expect(page.locator("#cfg-audio-transcribe-model")).to_have_value("")
            expect(page.locator("#media-model-list .media-model-item")).to_have_count(1, timeout=5000)
            expect(page.locator("#media-model-list")).to_contain_text("gpt-4o-e2e")

            page.locator('.modal-footer button:has-text("Save changes")').click()
            expect(page.locator("#save-feedback")).to_have_text("✓ Saved", timeout=5000)

            page.locator("#settings-overlay .modal-close").click()
            expect(page.locator("#settings-overlay")).not_to_be_visible()

            page.locator("#settings-btn").click()
            page.locator(".settings-nav button", has_text="Model").click()
            expect(page.locator("#cfg-audio-transcribe-provider")).to_have_value("")
            expect(page.locator("#cfg-audio-transcribe-model")).to_have_value("")
            expect(page.locator("#cfg-image-ocr-provider")).to_have_value("model")
            expect(page.locator("#cfg-image-ocr-model")).to_have_value("openai::gpt-4o-e2e")
            expect(page.locator("#media-model-list .media-model-item")).to_have_count(1, timeout=5000)
            expect(page.locator("#media-model-list")).to_contain_text("gpt-4o-e2e")
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_config_save_failure_feedback(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect

    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    def _fake_config_route(route):
        if route.request.method == "GET":
            route.continue_()
            return
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"ok": False, "error": "provider policy blocks this update"}),
        )

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/config", _fake_config_route)

            _open_settings_tab(page, "Model")
            page.locator("#cfg-model-provider").select_option("openai")
            page.locator('.modal-footer button:has-text("Save changes")').click()
            expect(page.locator("#save-feedback")).to_have_text("✗ provider policy blocks this update", timeout=5000)

            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_settings_logs_tab_load_and_source_switch_failures(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    def _fake_logs_route(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"ok": False, "error": "log sink missing"}),
        )

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/logs*", _fake_logs_route)

            _open_settings_tab(page, "Logs")
            expect(page.locator("#logs-output")).to_have_text("Could not load logs: log sink missing", timeout=5000)

            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_model_oauth_reauth_sse_stream_and_feedback(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)

    async def _fake_stream(*, request):  # noqa: ANN001
        yield {"type": "output", "text": "token-refresh-line-1\n"}
        yield {"type": "output", "text": "token-refresh-line-2\n"}
        yield {"type": "complete", "ok": True}

    monkeypatch.setattr(web_app, "_stream_codex_reauth_events", _fake_stream)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            _open_settings_tab(page, "Model")
            page.locator("#cfg-model-provider").select_option("codex")

            page.locator("#oauth-reauth-btn").click()
            expect(page.locator("#oauth-reauth-output")).to_contain_text("token-refresh-line-1", timeout=5000)
            expect(page.locator("#oauth-reauth-output")).to_contain_text("token-refresh-line-2")
            expect(page.locator("#oauth-reauth-feedback")).to_have_text("Codex OAuth refreshed.", timeout=5000)
            expect(page.locator("#oauth-reauth-btn")).to_have_text("Re-authenticate")

            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_codex_reauth_stream_reports_process_output_failure_and_disconnect(monkeypatch) -> None:
    from nullion import web_app

    class Request:
        def __init__(self, disconnected: tuple[bool, ...] = ()) -> None:
            self._disconnected = list(disconnected)

        async def is_disconnected(self) -> bool:
            return self._disconnected.pop(0) if self._disconnected else False

    class Stdout:
        def __init__(self, lines: tuple[bytes, ...]) -> None:
            self._lines = list(lines)

        async def readline(self) -> bytes:
            return self._lines.pop(0) if self._lines else b""

    class Process:
        def __init__(self, *, lines: tuple[bytes, ...], returncode: int) -> None:
            self.stdout = Stdout(lines)
            self._final_returncode = returncode
            self.returncode: int | None = None
            self.terminated = False
            self.killed = False

        async def wait(self) -> int:
            self.returncode = self._final_returncode
            return self.returncode

        def terminate(self) -> None:
            self.terminated = True
            self.returncode = self._final_returncode

        def kill(self) -> None:
            self.killed = True
            self.returncode = self._final_returncode

    async def collect(request: Request):
        return [event async for event in web_app._stream_codex_reauth_events(request=request)]

    failed = Process(lines=(b"\x1b[31mrefresh failed\x1b[0m\n",), returncode=17)

    async def fake_failed_process(*args, **kwargs):  # noqa: ANN001, ANN003
        return failed

    monkeypatch.setattr(web_app.asyncio, "create_subprocess_exec", fake_failed_process)
    failed_events = asyncio.run(collect(Request()))

    assert failed_events == [
        {"type": "output", "text": "refresh failed\n"},
        {"type": "complete", "ok": False, "error": "Codex re-authentication exited with status 17."},
    ]

    disconnected = Process(lines=(b"never read\n",), returncode=0)

    async def fake_disconnected_process(*args, **kwargs):  # noqa: ANN001, ANN003
        return disconnected

    monkeypatch.setattr(web_app.asyncio, "create_subprocess_exec", fake_disconnected_process)
    disconnected_events = asyncio.run(collect(Request((True,))))

    assert disconnected.terminated is True
    assert disconnected_events == [
        {"type": "complete", "ok": False, "error": "Browser disconnected from re-authentication stream."}
    ]

    async def fake_spawn_error(*args, **kwargs):  # noqa: ANN001, ANN003
        raise OSError("missing auth command")

    monkeypatch.setattr(web_app.asyncio, "create_subprocess_exec", fake_spawn_error)
    assert asyncio.run(collect(Request())) == [
        {"type": "complete", "ok": False, "error": "missing auth command"}
    ]


def test_browser_ui_model_tab_set_and_clear_default_model_for_all_sessions(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)
    posted: dict[str, object] = {}
    delete_calls: dict[str, bool] = {}

    def _fake_model_force(route):
        if route.request.method == "POST":
            posted.update(_request_post_json(route.request) or {})
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"ok": True, "admin_forced_model": "gpt-5.4-e2e", "admin_forced_provider": "openai"}),
            )
            return
        if route.request.method == "DELETE":
            delete_calls["called"] = True
            route.fulfill(status=200, content_type="application/json", body='{"ok": true}')
            return
        route.continue_()

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/config/model-force", _fake_model_force)

            _open_settings_tab(page, "Model")
            page.locator("#cfg-model-provider").select_option("openai")
            while page.locator("#chat-model-list .chat-model-remove-btn").count():
                page.locator("#chat-model-list .chat-model-remove-btn").first.click()

            page.locator("#cfg-chat-model-name").fill("gpt-5.4-e2e")
            page.locator("#model-add-btn").click()

            page.locator(".model-default-row button", has_text="Set default").click()
            expect(page.locator("#admin-forced-strip")).to_be_visible()
            expect(page.locator("#admin-forced-value")).to_have_text("openai · gpt-5.4-e2e")
            page.locator("#admin-forced-strip .ams-clear").click()
            expect(page.locator("#admin-forced-strip")).not_to_be_visible()

            assert posted["model_name"] == "gpt-5.4-e2e"
            assert posted["model_provider"] == "openai"
            assert delete_calls.get("called") is True
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_model_tab_oauth_disconnect_and_ui_refresh(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    env_path = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app, _runtime, _registry = _web_app(tmp_path)
    observed_payload: dict[str, object] = {}

    def _fake_disconnect(route):
        observed_payload["body"] = _request_post_json(route.request)
        route.fulfill(status=200, content_type="application/json", body='{"ok": true, "removed": ["codex_token"]}')

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.route("**/api/config/oauth/disconnect", _fake_disconnect)

            _open_settings_tab(page, "Model")
            page.locator("#cfg-model-provider").select_option("codex")
            expect(page.locator("#oauth-status-row")).to_be_visible()

            page.locator("#tab-model button", has_text="Disconnect").click()
            expect(page.locator("#confirm-overlay")).to_be_visible()
            page.locator("#confirm-ok").click()

            expect(page.locator("#oauth-status-hint")).to_have_text("Click Re-authenticate to connect Codex.", timeout=5000)

            assert observed_payload["body"] == {"provider": "codex"}
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_dashboard_task_kill_actions_cover_frame_and_mini_agent(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, _runtime, _registry = _web_app(tmp_path)

    status_payload = {
        "approvals": [],
        "task_frames": [
            {
                "frame_id": "frame-e2e",
                "status": "running",
                "summary": "Frame cleanup",
                "target": "web:operator",
                "created_at": "2026-05-01T00:00:00Z",
            },
        ],
        "mini_agent_tasks": [
            {
                "task_id": "mini-e2e",
                "status": "running",
                "title": "Mini Agent cleanup",
                "agent_id": "agent-operator",
                "group_id": "grp-e2e",
                "allowed_tools": ["web_fetch"],
                "created_at": "2026-05-01T00:00:00Z",
            },
        ],
        "permissions": [],
        "decisions": [],
        "builder_proposals": [],
        "doctor": [],
        "skills": [],
        "memory": [],
        "delivery_receipts": [],
        "health": {},
        "delivery_health": {},
        "tool_count": 0,
    }

    called_kills: list[str] = []

    def _status_route(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(status_payload),
        )

    def _frame_kill_route(route):
        called_kills.append("frame")
        route.fulfill(status=200, content_type="application/json", body='{"ok": true}')

    def _mini_agent_kill_route(route):
        called_kills.append("mini-agent")
        route.fulfill(status=200, content_type="application/json", body='{"ok": true}')

    def _configure_page(page):
        # Install receipts before navigation: refreshDashboard coalesces concurrent
        # calls, so a late route can leave this assertion observing the real
        # startup request instead of this test's injected task state.
        page.route("**/api/status?full=1", _status_route)
        page.route("**/api/tasks/frame/*/kill", _frame_kill_route)
        page.route("**/api/tasks/mini-agent/*/kill", _mini_agent_kill_route)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(
            server.base_url, configure_page=_configure_page,
        )
        try:
            page.evaluate("refreshDashboard()")
            page.locator("#tasks-list").wait_for(state="visible", timeout=10000)
            expect(page.locator("#tasks-list .task-card").first).to_be_visible(timeout=10000)

            page.locator("#tasks-list .task-card").first.locator("button", has_text="Kill").click()
            page.locator("#confirm-ok").click()

            page.locator("#tasks-list .task-card").nth(1).locator("button", has_text="Kill").click()
            page.locator("#confirm-ok").click()

            assert sorted(called_kills) == ["frame", "mini-agent"]
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_attention_doctor_dismiss_removes_card_from_open_modal(tmp_path: Path) -> None:
    from playwright.sync_api import expect

    app, runtime, _registry = _web_app(tmp_path)
    runtime.store.add_doctor_action(
        {
            "action_id": "act-attention-e2e",
            "owner": "doctor",
            "status": "pending",
            "action_type": "inspect",
            "recommendation_code": "review_doctor_route",
            "summary": "Review routed health issue",
            "severity": "critical",
            "reason": "source=telegram_chat;issue_type=error;detail=No model client configured for chat backend.",
            "source_reason": "source=telegram_chat;issue_type=error;detail=No model client configured for chat backend.",
            "error": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        dialogs: list[str] = []
        page.on("dialog", lambda dialog: (dialogs.append(dialog.message), dialog.dismiss()))
        try:
            page.evaluate("refreshDashboard()")
            expect(page.locator("#attention-chip .chip-count")).to_have_text("1", timeout=10000)

            page.locator("#attention-chip").click()
            expect(page.locator("#attention-overlay")).to_be_visible(timeout=10000)
            expect(page.locator("#attention-body .control-card")).to_have_count(1)
            expect(page.locator("#attention-body")).to_contain_text("No model client configured for chat backend")

            page.locator("#attention-body .control-card").first.locator(
                "button",
                has_text="Dismiss",
            ).evaluate("(btn) => { btn.click(); btn.click(); }")

            expect(page.locator("#attention-body .control-card")).to_have_count(0, timeout=10000)
            expect(page.locator("#attention-body")).to_contain_text("No attention items", timeout=10000)
            expect(page.locator("#attention-chip .chip-count")).to_have_text("0", timeout=10000)
            assert runtime.store.get_doctor_action("act-attention-e2e")["status"] == "cancelled"
            assert dialogs == []
            assert not errors
        finally:
            browser.close()
            playwright.stop()


@pytest.mark.parametrize('prompt,answer', [('Give me ten headlines.', 'Fresh headlines delivered.'), ('最新のニュースを十件教えて。', '新しいニュースを十件届けました。')])
def test_pending_web_reply_recovery_uses_turn_identity(tmp_path, monkeypatch, prompt, answer):
    from fastapi.testclient import TestClient
    from nullion import chat_store as chat_store_module
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_BUILDER_ENABLED', '0')
    chat_store_module._store = None
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    entered, release = threading.Event(), threading.Event()

    class Model:
        calls = 0
        def create(self, **kwargs):
            names = {tool['name'] for tool in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return {'content': [{'type': 'tool_use', 'id': 'format', 'name': 'select_attachment_format', 'input': {'extension': None, 'confidence': 1}}]}
            if not names:
                return {'content': [{'type': 'text', 'text': '{}'}]}
            self.calls += 1
            entered.set()
            assert release.wait(20), 'external provider release timed out'
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': answer}]}

    model = Model()
    app = create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))
    with _UvicornThread(app, _free_port()) as server:
        from playwright.sync_api import sync_playwright
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        errors = []
        page.on('pageerror', lambda exc: errors.append(str(exc)))
        page.goto(server.base_url, wait_until='domcontentloaded')
        try:
            page.wait_for_function("() => ws && ws.readyState === WebSocket.OPEN", timeout=5000)
            conv = page.evaluate('conversationId')
            with TestClient(app) as client:
                for role, text in [('user', 'Earlier weather request.'), ('bot', 'Old weather: 65 F.')]:
                    assert client.post(f'/api/chat/history/{conv}/message', json={'role': role, 'text': text}).status_code == 200
                page.reload(wait_until='domcontentloaded')
                expect(page.locator('#messages')).to_contain_text('Old weather: 65 F.', timeout=5000)
                runtime.store.add_conversation_event({'conversation_id': conv, 'event_type': 'conversation.chat_turn',
                    'created_at': datetime.now(timezone.utc).isoformat(), 'turn_id': 'old-weather-turn',
                    'user_message': 'Earlier weather request.', 'assistant_reply': 'Old weather: 65 F. Additional prior detail.'})
                runtime.checkpoint(force=True)
                page.wait_for_function("() => ws && ws.readyState === WebSocket.OPEN", timeout=5000)
                page.locator('#user-input').fill(prompt)
                page.locator('#send-btn').click()
                assert entered.wait(8)
                # Let normal dashboard and terminal recovery polling run while
                # the real product turn is waiting at the provider boundary.
                page.wait_for_timeout(6000)
                expect(page.locator('#messages')).not_to_contain_text('Additional prior detail.')
                expect(page.locator('#messages .typing-bubble, #messages .status-bubble')).to_have_count(1)
                turn_ids = page.evaluate('Array.from(_pendingTerminalReplyRecoveries.keys())')
                assert len(turn_ids) == 1
                pending = client.get(f'/api/chat/history/{conv}', params={'turn_ids': json.dumps(turn_ids)}).json()
                assert pending['terminal_replies'] == []
                release.set()
                expect(page.locator('#messages')).to_contain_text(answer, timeout=10000)
                page.wait_for_timeout(2000)
                expect(page.locator('#messages .msg.bot .bubble').filter(has_text=answer)).to_have_count(1)
                recovered = client.get(f'/api/chat/history/{conv}', params={'turn_ids': json.dumps(turn_ids)}).json()
                assert [(r['turn_id'], r['text']) for r in recovered['terminal_replies']] == [(turn_ids[0], answer)]
                other = client.get('/api/chat/history/web:other', params={'turn_ids': json.dumps(turn_ids)}).json()
                assert other['terminal_replies'] == []
                assert model.calls == 1
                assert not errors
        finally:
            release.set()
            browser.close()
            playwright.stop()


@pytest.mark.parametrize('prompt,answer', [('Continue collecting observations.', 'All observations collected.'), ('観測収集を続けて。', 'すべての観測を収集しました。')])
def test_web_budget_approval_preserves_turn_and_keeps_server_responsive(tmp_path, monkeypatch, prompt, answer):
    from nullion import web_app, chat_store as chat_store_module
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from datetime import UTC, datetime
    from nullion.tools import ToolRegistry, register_web_fetch_tool
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED', '1')
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'home'/'.env'))
    from nullion import preferences
    monkeypatch.setattr(preferences, '_PREFS_PATH', tmp_path/'home'/'preferences.json')
    monkeypatch.setenv('NULLION_AGENT_TURN_MAX_ITERATIONS', '2')
    monkeypatch.setenv('NULLION_BROWSER_AGENT_TURN_MAX_ITERATIONS', '2')
    for key in ('NULLION_TASK_DECOMPOSITION_ENABLED', 'NULLION_MULTI_AGENT_ENABLED',
                'NULLION_MEMORY_ENABLED', 'NULLION_BUILDER_ENABLED', 'NULLION_DOCTOR_ENABLED'):
        monkeypatch.setenv(key, '0')
    chat_store_module._store = None
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *args, **kwargs: None)
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        rule_id='research-network', principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector='*',
        created_by='fixture-user', created_at=datetime.now(UTC)))
    registry = ToolRegistry()
    from nullion import tools as tools_module
    fetcher = lambda url, timeout: {'url':url, 'status_code':200, 'text':'Observation: 18°C.'}
    monkeypatch.setattr(tools_module, '_default_web_fetcher', fetcher)
    register_web_fetch_tool(registry, fetcher)
    entered, release = threading.Event(), threading.Event()
    class Model:
        rounds = 0
        def create(self, **kwargs):
            names = {t['name'] for t in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return {'content':[{'type':'tool_use','id':'format','name':'select_attachment_format',
                    'input':{'extension':None,'confidence':1}}]}
            if not names:
                return {'content':[{'type':'text','text':'{}'}]}
            self.rounds += 1
            if self.rounds == 1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                    'input':{'capabilities':['web'],'tool_names':['web_fetch'],'required_tool_names':['web_fetch']}}]}
            if self.rounds == 2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':'web_fetch','input':{'url':'https://example.org/observations/1'}}]}
            entered.set()
            assert release.wait(20), 'Provider fixture was not released'
            return {'stop_reason':'end_turn','content':[{'type':'text','text':answer}]}
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=Model()), registry=registry)
    with _UvicornThread(app, _free_port()) as server:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            errors = []
            page.on('pageerror', lambda exc: errors.append(str(exc)))
            try:
                page.goto(server.base_url, wait_until='domcontentloaded')
                page.wait_for_function('() => ws && ws.readyState === WebSocket.OPEN')
                conv = page.evaluate('conversationId')
                page.locator('#user-input').fill(prompt)
                page.locator('#send-btn').click()
                expect(page.locator('#messages .approval-bubble')).to_have_count(1, timeout=10000)
                turn_id = page.locator('#messages .status-bubble').get_attribute('data-turn-id')
                approval_id = page.locator('#messages .approval-bubble').get_attribute('data-approval-id')
                saved = runtime.store.get_suspended_turn(approval_id)
                assert saved.resume_token['conversation_turn_id'] == turn_id
                assert 'Observation: 18' in json.dumps(saved.messages_snapshot), json.dumps(saved.messages_snapshot[-2:])
                runtime.store.add_conversation_event({'conversation_id':conv,'event_type':'conversation.chat_turn',
                    'created_at':datetime.now(timezone.utc).isoformat(),'turn_id':'old-weather-turn',
                    'user_message':'Earlier weather','assistant_reply':'Old weather: additional prior detail.'})
                runtime.checkpoint(force=True)
                page.wait_for_timeout(2500)
                expect(page.locator('#messages')).not_to_contain_text('additional prior detail')
                page.locator('#messages .approval-bubble').get_by_role('button', name='2x', exact=True).click()
                assert entered.wait(6)
                started = time.perf_counter()
                with urllib.request.urlopen(server.base_url+'/api/health', timeout=2) as response:
                    assert response.status == 200
                assert time.perf_counter()-started < .8
                page.wait_for_timeout(2000)
                expect(page.locator('#messages')).not_to_contain_text('additional prior detail')
                release.set()
                expect(page.locator('#messages')).to_contain_text(answer, timeout=10000)
                expect(page.locator('#messages .bubble').filter(has_text=answer)).to_have_count(1)
                turns = runtime.store.list_recent_conversation_events(conv, event_type='conversation.chat_turn')
                assert any(r['turn_id'] == turn_id and r['assistant_reply'] == answer for r in turns)
                assert runtime.store.get_suspended_turn(approval_id) is None
                assert not errors
            finally:
                release.set()
                browser.close()


@pytest.mark.parametrize('label', ['Booking page', 'Reservas', '予約ページ'])
def test_web_reply_links_preserve_urls_and_keep_code_and_html_inert(tmp_path, monkeypatch, label):
    from nullion.agent_orchestrator import AgentOrchestrator
    import nullion.chat_store as chat_store_module
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    chat_store_module._store = None
    long_url = 'https://example.com/reserve?date=2026-10-10&party=2&state=' + 'a' * 6000
    plain_url = 'https://example.org/catalogue/item.html?price=84&stock=12'
    reply = (f'[{label}]({long_url})\n\n{plain_url}\n\n'
             '`https://example.net/inline-code`\n\n'
             '```\nhttps://example.net/fenced-code\n```\n\n'
             '[Unsafe](javascript:alert(1))\n<img src=x onerror="alert(1)">')

    class Model:
        def create(self, **kwargs):
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': reply}]}

    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    app = create_app(runtime, orchestrator=AgentOrchestrator(model_client=Model()), registry=ToolRegistry())
    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator('#user-input').fill('Demonstration')
            page.locator('#send-btn').click()
            booking = page.get_by_role('link', name=label, exact=True)
            expect(booking).to_be_visible(timeout=15000)
            assert booking.get_attribute('href') == long_url
            assert booking.get_attribute('rel') == 'noopener noreferrer'
            expect(page.get_by_role('link', name=plain_url, exact=True)).to_be_visible()
            assert page.get_by_role('link', name=plain_url, exact=True).get_attribute('href') == plain_url
            assert page.locator('a[href^="javascript:"]').count() == 0
            assert page.locator('a[href*="example.net"]').count() == 0
            assert page.locator('img[onerror]').count() == 0
            assert page.locator('code').filter(has_text='inline-code').count() == 1
            turns = [e for e in runtime.store.list_conversation_events() if e.get('event_type') == 'conversation.chat_turn' and e.get('user_message') == 'Demonstration']
            assert turns and turns[-1]['assistant_reply'] == reply
            assert not turns[-1].get('artifacts')
            assert errors == []
        finally:
            browser.close()
            playwright.stop()


@pytest.mark.parametrize('followup', [
    'What date and party size was that search for?',
    '¿Para qué fecha y cuántas personas fue esa búsqueda?',
    'その検索の日付と人数は？',
])
def test_web_followup_is_not_replaced_by_connector_builder_suggestion(tmp_path, monkeypatch, followup):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion import chat_operator, turn_context_policy
    import nullion.chat_store as chat_store_module
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    from nullion.web_app import create_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    chat_store_module._store = None
    providers = ({'active_app_ids': ('brave-search', 'google-calendar', 'google-mail'), 'structured_tools': ('calendar_list', 'email_search', 'email_read')},)
    for module in (chat_operator, turn_context_policy):
        monkeypatch.setattr(module, 'cached_active_connector_app_id_providers', lambda *args, **kwargs: providers)
    inventory_calls = []
    def reject_inventory():
        inventory_calls.append(True)
        raise AssertionError('A plain follow-up must not load connector inventory')
    monkeypatch.setattr(turn_context_policy, '_active_connector_provider_context', reject_inventory)
    answer = 'Saturday, October 10, 2026, for 2 people.'
    class Model:
        scope_calls = 0
        def create(self, **kwargs):
            if 'Return only a JSON object matching this schema: {"requested_outcome"' in str(kwargs.get('system', '')):
                self.scope_calls += 1
                time.sleep(3)
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': answer}]}
    model = Model()
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    registry = ToolRegistry()
    def reject_scope(invocation):
        raise AssertionError('A plain follow-up must not request connector scope')
    registry.register(ToolSpec('request_tool_scope', 'Scope request', ToolRiskLevel.LOW,
                              ToolSideEffectClass.READ, False, 1, capability_tags=('scope_request',)), reject_scope)
    app = create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            elapsed = []
            for prompt in ('Reservation search for October 10, 2026, party size 2.', followup):
                started = time.monotonic()
                page.locator('#user-input').fill(prompt)
                page.locator('#send-btn').click()
                expect(page.locator('#messages .bubble').filter(has_text=answer)).to_have_count(len(elapsed) + 1, timeout=10000)
                elapsed.append(time.monotonic() - started)
            assert sum(elapsed) / len(elapsed) < 5
            assert model.scope_calls == 0, 'No extra scope classifier belongs on the plain follow-up path'
            assert inventory_calls == []
            turns = [e for e in runtime.store.list_conversation_events() if e.get('event_type') == 'conversation.chat_turn']
            assert len(turns) == 2
            assert all(e['assistant_reply'] == answer and not e.get('artifacts') for e in turns)
            assert all(not e.get('tool_results') for e in turns)
            assert runtime.list_pending_builder_proposals() == []
            assert 'Builder suggestion' not in page.locator('#messages').inner_text()
            assert errors == []
        finally:
            browser.close()
            playwright.stop()
