"""Real browser, agent graph, SQLite and platform artifacts after correctable failures."""
from __future__ import annotations

import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E only")


def text_response(value):
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": value}]}


@pytest.mark.parametrize("explain", [False, True])
def test_correctable_browser_calls_recover_within_normal_budget(tmp_path, monkeypatch, record_property, explain):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_tools import _run
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision
    from PIL import Image

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(403)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b'<html><body style="background:#142f40;color:white"><h1>Access unavailable</h1><p>Reference A17</p></body></html>')
        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://repair.localhost:{server.server_port}/"
    dns = socket.getaddrinfo
    monkeypatch.setattr(socket, "getaddrinfo", lambda host, *args, **kw: dns("127.0.0.1" if host == "repair.localhost" else host, *args, **kw))
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "playwright")
    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "true")
    registry = ToolRegistry()
    register_browser_tools(registry)
    backend = registry._handlers["browser_navigate"].__self__._backend
    elapsed = []
    try:
        for index, prompt in enumerate(["ページの画像をください", "Muéstrame la captura", "أرسل صورة الصفحة"]):
            runtime = bootstrap_persistent_runtime(tmp_path / f"runtime-{index}.db")
            scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(has_url_target=True, requested_extensions=(".png",)),
                tool_scope_decision=TurnToolScopeDecision(valid=True, confidence=1,
                    web_action="browser_interaction", requested_artifact_extensions=(".png",),
                    requested_tool_names=("browser_open", "browser_navigate", "browser_wait_for", "browser_screenshot") + (("browser_extract_text",) if explain else ()),
                    required_tool_names=("browser_screenshot", "browser_wait_for")))

            class Model:
                calls = 0
                session = None
                def create(self, **kwargs):
                    names = {tool["name"] for tool in kwargs.get("tools", [])}
                    self.calls += 1
                    assert "browser_screenshot" in names, "Correction must not remove capture access"
                    assert "browser_wait_for" in names, "Argument errors must not remove wait access"
                    if self.calls > 6:
                        time.sleep(2)  # Slow model verification must stay off this completed path.
                        raise AssertionError("Browser recovery exceeded its contract")
                    if self.calls == 1:
                        calls = [("browser_open", {"session_id": "view-tab"}), ("browser_screenshot", {})]
                    else:
                        evidence = []
                        for message in kwargs["messages"]:
                            for block in message.get("content", []):
                                if isinstance(block, dict) and block.get("type") == "tool_result":
                                    for part in block.get("content", []):
                                        try:
                                            evidence.append(json.loads(part.get("text", "")))
                                        except (ValueError, AttributeError):
                                            pass
                        # Tool output is serialized through the actual model adapter.
                        def visit(value):
                            if isinstance(value, dict):
                                if value.get("reason") == "tool_precondition_unmet":
                                    self.session = value["session_id"]
                                for sub in value.values(): visit(sub)
                            elif isinstance(value, list):
                                for sub in value: visit(sub)
                        visit(evidence)
                        assert self.session
                        if self.calls == 2:
                            calls = [("browser_navigate", {"url": url, "session_id": self.session})]
                            if explain:
                                calls.append(("browser_extract_text", {"session_id": self.session}))
                        elif self.calls == 3:
                            calls = [("browser_wait_for", {"timeout": 1, "session_id": self.session})]
                        elif self.calls == 4:
                            calls = [("browser_wait_for", {"text": "Reference A17", "session_id": self.session})]
                        elif self.calls == 5:
                            calls = [("browser_screenshot", {})]
                        else:
                            paths = []
                            def capture(value):
                                if isinstance(value, dict):
                                    if value.get("format") == "png" and value.get("path"): paths.append(value["path"])
                                    for sub in value.values(): capture(sub)
                                elif isinstance(value, list):
                                    for sub in value: capture(sub)
                            capture(evidence)
                            assert paths
                            return text_response(f"Reference A17: このページはアクセスを制限しています。\nMEDIA:{paths[-1]}")
                    return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": f"call-{self.calls}-{i}",
                        "name": name, "input": args} for i, (name, args) in enumerate(calls)]}

            model = Model()
            started = time.perf_counter()
            result = AgentOrchestrator(model_client=model).run_turn(
                conversation_id=f"telegram:member-{index}", principal_id="operator", user_message=f"{prompt} {url}",
                conversation_history=[], tool_registry=scoped, policy_store=runtime.store, approval_store=runtime.store,
                max_iterations=8)
            elapsed.append(time.perf_counter() - started)
            assert not result.reached_iteration_limit and not result.suspended_for_approval
            assert result.response_fulfilled is not False
            if explain:
                assert "Reference A17" in result.final_text
                assert model.calls == 6
            assert 5 <= model.calls <= 6
            assert len(result.artifacts) == 1, result.final_text
            capture = next(r for r in reversed(result.tool_results) if r.tool_name == "browser_screenshot")
            assert capture.status == "completed" and capture.output["session_id"] == model.session
            with Image.open(result.artifacts[0]) as image:
                assert image.format == "PNG" and image.width > 100 and image.height > 100
            delivery = prepare_reply_for_platform_delivery(result.final_text, principal_id="operator")
            assert Path(result.artifacts[0]) in delivery.attachments, delivery
            assert Path(result.artifacts[0]).read_bytes() == delivery.attachments[0].read_bytes()
            assert runtime.store.list_approval_requests() == []
            runtime.checkpoint()
            assert (tmp_path / f"runtime-{index}.db").stat().st_size > 0
        record_property("browser_repair_seconds", elapsed)
        assert sum(elapsed) / len(elapsed) < 4, elapsed
        assert max(elapsed) < 6, elapsed
    finally:
        _run(backend.shutdown())
        server.shutdown()
        server.server_close()


def test_approved_budget_is_persisted_and_visible_at_telegram_boundary(tmp_path):
    from nullion.chat_operator import _store_agent_turn_limit_extension_suspended_turn
    from nullion.telegram_app import _execute_decision_action
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_turn_limits import max_iterations_from_resume_token
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    runtime = bootstrap_persistent_runtime(tmp_path / "budget.db")
    approval_id = _store_agent_turn_limit_extension_suspended_turn(runtime,
        conversation_id="telegram:member", chat_id="member", message="次へ", request_id="budget-run", message_id="budget-message",
        messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "次へ"}]}],
        current_max_iterations=24, tool_results=[], required_attachment_extensions=(".png",))
    _, reply = _execute_decision_action(SimpleNamespace(runtime=runtime), kind="approval", action="extend_limit_5x", record_id=approval_id)
    assert "Approved continuation budget: 120 model rounds" in reply
    assert "Previous budget: 24 model rounds" in reply
    assert "Current turn budget: 24" not in reply
    assert max_iterations_from_resume_token(runtime.store.get_suspended_turn(approval_id).resume_token) == 120
    runtime.checkpoint()
    fresh = bootstrap_persistent_runtime(tmp_path / "budget.db")
    assert fresh.store.get_approval_request(approval_id).context["approved_max_iterations"] == 120
    assert "120 model rounds" in str(prepare_reply_for_platform_delivery(reply, principal_id="operator"))


def test_repair_retries_are_bounded_and_plain_turns_stay_fast(tmp_path, monkeypatch, record_property):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "playwright")
    monkeypatch.setenv("NULLION_REPEATED_TOOL_FAILURE_LIMIT", "2")
    registry = ToolRegistry()
    register_browser_tools(registry)
    runtime = bootstrap_persistent_runtime(tmp_path / "bounded.db")
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(valid=True, web_action="browser_interaction", requested_tool_names=("browser_wait_for",)))

    class WrongArguments:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            assert self.calls <= 2
            assert "browser_wait_for" in {tool["name"] for tool in kwargs["tools"]}
            return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": f"wrong-{self.calls}",
                "name": "browser_wait_for", "input": {"timeout": self.calls}}]}
    bad = WrongArguments()
    result = AgentOrchestrator(model_client=bad).run_turn(conversation_id="web:bounded", principal_id="operator",
        user_message="次の操作", conversation_history=[], tool_registry=scoped, policy_store=runtime.store,
        approval_store=runtime.store, max_iterations=8)
    assert bad.calls == 2 and not result.reached_iteration_limit
    assert result.final_text and not result.suspended_for_approval
    assert [r.tool_name for r in result.tool_results] == ["browser_wait_for", "browser_wait_for"]

    class Plain:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            if self.calls != 1:
                time.sleep(2)
                raise AssertionError("Installed browser tools must not add a plain-turn decision")
            return text_response("こんにちは")
    elapsed = []
    for index, prompt in enumerate(["Hola", "مرحبا", "こんにちは", "Bonjour", "नमस्ते"]):
        model = Plain()
        start = time.perf_counter()
        turn = AgentOrchestrator(model_client=model).run_turn(conversation_id=f"web:plain-{index}", principal_id="operator",
            user_message=prompt, conversation_history=[], tool_registry=registry, policy_store=runtime.store,
            approval_store=runtime.store, max_iterations=2)
        elapsed.append(time.perf_counter() - start)
        assert model.calls == 1 and turn.final_text == "こんにちは" and not turn.tool_results
    record_property("plain_seconds_with_browser_installed", elapsed)
    assert sum(elapsed) / len(elapsed) < .25 and max(elapsed) < .4, elapsed


def test_web_approval_route_shows_actual_resume_budget(tmp_path):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _store_agent_turn_limit_extension_suspended_turn
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    runtime = bootstrap_persistent_runtime(tmp_path / "web-budget.db")
    approval_id = _store_agent_turn_limit_extension_suspended_turn(runtime,
        conversation_id="web:member", chat_id="member", message="次へ", request_id="web-budget-run", message_id="web-budget-message",
        messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "次へ"}]}], current_max_iterations=24, tool_results=[])
    class Model:
        def create(self, **kwargs):
            return text_response("完了しました。")
    app = create_app(runtime, orchestrator=AgentOrchestrator(model_client=Model()), registry=ToolRegistry())
    with TestClient(app) as client:
        response = client.post(f"/api/approve/{approval_id}", json={"mode": "limit_5x"})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert "Approved continuation budget: 120 model rounds" in payload["confirmation_text"]
    assert "Current turn budget: 24" not in payload["confirmation_text"]
    assert runtime.store.get_approval_request(approval_id).context["approved_max_iterations"] == 120
