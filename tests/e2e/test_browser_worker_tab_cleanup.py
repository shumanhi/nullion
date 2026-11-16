"""Real isolated Browser Use/CDP and request cancellation, preserving user tabs."""
import json
import os
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


@pytest.mark.parametrize("outcome", ["completed", "cancelled"])
def test_browser_worker_closes_created_tabs_and_preserves_concurrent_user_tab(tmp_path, monkeypatch, outcome, record_property):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.browser_model_protocol import browser_schema_messages
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.source_answer_fixture import native_png_directives, source_presentation_response
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({"outbound_request_mode": "allow_all"})
    worker = os.environ.get("NULLION_BROWSER_USE_TEST_PYTHON")
    assert worker, "This regression requires the isolated Browser Use SDK"
    port = _free_port()
    endpoint = f"http://127.0.0.1:{port}"
    for key, value in {"NULLION_CODE_MODE": "on", "NULLION_BROWSER_BACKEND": "cdp",
            "NULLION_BROWSER_CDP_URL": endpoint, "NULLION_BROWSER_USE_CDP_URL": endpoint,
            "NULLION_BROWSER_USE_PYTHON": worker, "NULLION_BROWSER_BLOCK_PRIVATE": "false"}.items():
        monkeypatch.setenv(key, value)
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers()
            self.wfile.write(b"<html><title>Owned source</title><main>Verified value: 37</main></html>")
        def log_message(self, *args): pass
    source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f"http://localhost:{source.server_port}/one"
    reached = threading.Event(); release = threading.Event(); user_targets = []
    class Model:
        model = "scripted-external-provider"
        phase = 0
        browser_calls = 0
        def create(self, **kw):
            schema_messages = browser_schema_messages(kw)
            if schema_messages:
                self.browser_calls += 1
                schema = json.loads(schema_messages[-1]["content"].split(": ", 1)[1])
                if self.browser_calls == 1:
                    # A user-created tab during the job must not be swept up
                    # by a before/after target inventory difference.
                    request = urllib.request.Request(endpoint+"/json/new?about:blank", method="PUT")
                    user_targets.append(json.load(urllib.request.urlopen(request))["id"])
                    reached.set()
                    if outcome == "cancelled":
                        release.wait(25)
                    actions = {"navigate": {"url": url.replace("/one", "/two"), "new_tab": True}}
                else:
                    report = {"goal_status": "answered", "next_action": "report", "observations": [
                        {"label": "値", "value": "37", "quote": "Verified value: 37"}], "unknowns": []}
                    actions = {"done": {"data": report, "success": True}}
                value = {"evaluation_previous_goal": "Captured source", "memory": "37", "next_goal": "Report", "action": [actions]}
                for key in ("thinking", "current_plan_item", "plan_update"):
                    if key in schema.get("properties", {}): value[key] = "Inspect" if key == "thinking" else None
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "browser-"+str(self.browser_calls),
                    "name": "browser_decision", "input": value}]}
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            names = {t["name"] for t in kw.get("tools", [])}
            if "present_source_answer" in names: return source_presentation_response(kw)
            if not names: return text("{}")
            self.phase += 1
            if self.phase == 1:
                return call("request_tool_scope", {"capabilities": ["web"], "tool_names": ["browser_run_task", "browser_read_pages"],
                    "required_tool_names": ["browser_run_task"], "artifact_extensions": [".png"],
                    "source_artifact_delivery_requested": True, "source_artifact_request_evidence": url+" .png"})
            if self.phase == 2:
                return call("code_mode", {"tool_names": ["browser_read_pages", "browser_run_task"], "code":
                    "call_tool('browser_read_pages', "+repr({"pages": [{"url": url, "read_kind": "text"}]})+")\n"+
                    "call_tool('browser_run_task', "+repr({"task": "Capture the requested displayed value", "url": url, "max_steps": 4})+")"})
            return text("Verified value: 37. "+native_png_directives(kw, latest_only=True))
    model = Model(); registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    started = time.monotonic()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch_persistent_context(str(tmp_path/"profile"), headless=True,
                args=[f"--remote-debugging-port={port}"])
            try:
                original = browser.pages[0]
                original.goto("data:text/html,<title>User original</title>Untouched")
                blank = browser.new_page()
                baseline = {t["id"] for t in json.load(urllib.request.urlopen(endpoint+"/json/list")) if t["type"] == "page"}
                with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        pending = pool.submit(client.post, "/api/chat", json={"text": "選択した値をPNGで確認してください。 "+url+" .png",
                            "conversation_id": "web:worker-cleanup", "turn_id": "worker-cleanup"})
                        if not reached.wait(35):
                            client.post("/api/chat", json={"text": "/stop", "conversation_id": "web:worker-cleanup"})
                            release.set()
                            turns = runtime.store.list_recent_conversation_events("web:worker-cleanup", event_type="conversation.chat_turn")
                            pytest.fail("The isolated worker did not reach its model: "+repr([e.get('tool_results') for e in turns]))
                        if outcome == "cancelled":
                            stopped = client.post("/api/chat", json={"text": "/stop", "conversation_id": "web:worker-cleanup"})
                            assert stopped.status_code == 200
                        response = pending.result(timeout=45)
                    assert response.status_code == 200, response.text
                    if outcome == "completed":
                        assert "37" in response.json()["text"] and response.json().get("artifacts"), response.json()
                        turns = runtime.store.list_recent_conversation_events("web:worker-cleanup", event_type="conversation.chat_turn")
                        results = [r for r in turns[-1]["tool_results"] if r["tool_name"] == "browser_run_task"]
                        assert results[-1]["output"]["browser_tab_cleanup"]["verified"] is True, results
                    else:
                        assert response.json().get("superseded") is True, response.json()
                    deadline = time.monotonic()+12
                    expected = baseline | set(user_targets)
                    while True:
                        actual = {t["id"] for t in json.load(urllib.request.urlopen(endpoint+"/json/list")) if t["type"] == "page"}
                        if actual == expected or time.monotonic() >= deadline: break
                        time.sleep(.1)
                    assert actual == expected, (actual, expected)
                    assert original.title() == "User original" and blank.url == "about:blank"
                    assert not list((tmp_path/"NULLION_HOME"/"browser-tab-leases").glob("*.json"))
            finally:
                release.set()
                browser.close()
    finally:
        source.shutdown(); source.server_close()
    record_property(outcome+"_seconds", time.monotonic()-started)
    assert time.monotonic()-started < 45
