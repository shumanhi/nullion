from __future__ import annotations

import os
from pathlib import Path
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest


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


def _open_page(base_url: str):
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
        page.goto(base_url, wait_until="domcontentloaded")
        page.locator("#user-input").wait_for(state="visible", timeout=10000)
        return playwright, browser, page, errors
    except Exception:
        if browser is not None:
            browser.close()
        if playwright is not None:
            playwright.stop()
        raise


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
    ):
        assert marker in html


def test_http_chat_response_contract_and_empty_message_errors(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app

    artifact = {
        "id": "artifact-1",
        "name": "answer.txt",
        "path": str(tmp_path / "answer.txt"),
        "media_type": "text/plain",
        "size_bytes": 11,
        "url": "/api/artifacts/artifact-1",
    }

    def fake_run_turn_sync(user_text, conv_id, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        assert conv_id == "web:e2e"
        assert kwargs.get("attachments") == [{"name": "notes.txt", "path": "/tmp/notes.txt"}]
        return {"text": f"reply:{user_text}", "artifacts": [artifact], "thinking": "private notes"}

    monkeypatch.setattr(web_app, "_run_turn_sync", fake_run_turn_sync)
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


def test_websocket_chat_handles_multiple_requests_without_serializing_all_turns(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import web_app

    def fake_run_turn_sync(user_text, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if user_text == "slow":
            time.sleep(0.2)
        return {"text": f"{user_text} done", "artifacts": []}

    monkeypatch.setattr(web_app, "_run_turn_sync", fake_run_turn_sync)
    app, _runtime, _registry = _web_app(tmp_path)

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({"text": "slow", "conversation_id": "web:e2e", "turn_id": "turn:slow", "stream": False})
            websocket.send_json({"text": "fast", "conversation_id": "web:e2e", "turn_id": "turn:fast", "stream": False})

            assert websocket.receive_json() == {"turn_id": "turn:fast", "type": "chunk", "text": "fast done"}

            done_turns = set()
            while done_turns != {"turn:fast", "turn:slow"}:
                event = websocket.receive_json()
                if event["type"] == "done":
                    done_turns.add(event["turn_id"])


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


def test_tool_file_web_permission_and_approval_rejection_workflows(tmp_path: Path) -> None:
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
    executor = ToolExecutor(store=runtime.store, registry=registry)
    app = create_app(runtime, orchestrator=None, registry=registry)

    with TestClient(app) as client:
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
        status = client.get("/api/status").json()

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


def test_browser_ui_can_send_multiple_requests_without_blocking_fast_reply(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    def fake_run_turn_sync(user_text, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if user_text == "slow":
            time.sleep(0.35)
        return {"text": f"{user_text} done", "artifacts": []}

    monkeypatch.setattr(web_app, "_run_turn_sync", fake_run_turn_sync)
    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.wait_for_function(
                "() => typeof ws !== 'undefined' && ws && ws.readyState === WebSocket.OPEN",
                timeout=5000,
            )
            composer = page.locator("#user-input")
            composer.fill("slow")
            page.locator("#send-btn").click()
            slow_user = page.locator(".msg.user").filter(has_text="slow")
            expect(slow_user).to_have_count(1)

            composer.fill("fast")
            page.locator("#send-btn").click()
            fast_user = page.locator(".msg.user").filter(has_text="fast")
            expect(fast_user).to_have_count(1)

            fast_reply = page.locator(".msg.bot .bubble").filter(has_text="fast done")
            expect(fast_reply).to_have_count(1, timeout=3000)
            slow_reply = page.locator(".msg.bot .bubble").filter(has_text="slow done")
            expect(slow_reply).to_have_count(1, timeout=5000)
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_browser_ui_uploads_file_delivers_download_row_and_saves_it(tmp_path: Path, monkeypatch) -> None:
    from playwright.sync_api import expect
    from nullion import web_app

    upload_source = tmp_path / "playwright-note.txt"
    upload_source.write_text("hello from playwright", encoding="utf-8")

    def fake_run_turn_sync(user_text, conv_id, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        attachments = kwargs.get("attachments") or []
        assert attachments and attachments[0]["name"] == "playwright-note.txt"
        return {
            "text": f"Received {attachments[0]['name']} for {conv_id}.",
            "artifacts": attachments,
        }

    monkeypatch.setattr(web_app, "_run_turn_sync", fake_run_turn_sync)
    app, _runtime, _registry = _web_app(tmp_path)

    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#file-input").set_input_files(str(upload_source))
            expect(page.locator("#attachments-bar")).to_contain_text("playwright-note.txt")
            page.locator("#user-input").fill("Please send this back as a downloadable file")
            page.locator("#send-btn").click()

            bot_link = page.locator(".msg.bot .artifact-link").first
            expect(bot_link).to_have_text("Download playwright-note.txt", timeout=5000)
            bot_link.click()
            expect(bot_link).to_contain_text("Saved playwright-note", timeout=5000)
            assert not errors
        finally:
            browser.close()
            playwright.stop()
