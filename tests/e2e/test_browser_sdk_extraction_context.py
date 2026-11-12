"""Actual SDK actions must expose extracted DOM values to the next model step."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pytest
pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker browser boundary E2E")

@pytest.mark.parametrize("prompt", ["Read the page and its image attribute.", "画像の属性とページを読んでください。", "Lee la página y el atributo de imagen."])
def test_real_sdk_extraction_values_reach_next_step_without_a_repeated_query(tmp_path, monkeypatch, prompt):
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask
    from nullion.plugins.browser_plugin.browser_use_worker import run_isolated_browser_task
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    worker = os.environ.get("NULLION_BROWSER_USE_TEST_PYTHON", "/tmp/nullion-browser-sdk/bin/python")
    if not __import__("pathlib").Path(worker).is_file(): pytest.skip("Installed SDK worker required")
    for key, value in {"NULLION_HOME":str(tmp_path), "NULLION_BROWSER_BLOCK_PRIVATE":"false", "NULLION_BROWSER_HEADLESS":"true", "NULLION_BROWSER_USE_SANDBOX":"false", "ANONYMIZED_TELEMETRY":"false"}.items(): monkeypatch.setenv(key, value)
    monkeypatch.delenv("NULLION_BROWSER_USE_CDP_URL", raising=False)
    from playwright.sync_api import sync_playwright
    with sync_playwright() as playwright:
        monkeypatch.setenv("NULLION_BROWSER_USE_EXECUTABLE_PATH", playwright.chromium.executable_path)
    marker = "sdk-extracted-attribute-73c81"
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.send_header("Content-Type", "text/html"); self.end_headers()
            self.wfile.write(('<html><body><h1>Verified item</h1><p>Total $37</p><img data-evidence="'+marker+'" alt="Illustration"></body></html>').encode())
        def log_message(self, *_): pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Page); threading.Thread(target=server.serve_forever, daemon=True).start()
    url = "http://localhost:"+str(server.server_port)+"/"
    class Model:
        model = "deterministic-external-provider"; calls = 0
        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            self.calls += 1
            schema_message = next(m["content"] for m in kwargs["messages"] if isinstance(m.get("content"), str) and m["content"].startswith("Return valid JSON matching this schema: "))
            schema = json.loads(schema_message.split(": ", 1)[1])
            if self.calls == 1:
                action = {"find_elements":{"selector":"img", "attributes":["data-evidence"], "max_results":5, "include_text":False}}
            else:
                assert self.calls == 2, "The extraction query was repeated"
                assert marker in json.dumps(kwargs["messages"]), "SDK replaced actual extracted DOM fields with a count-only summary"
                action = {"done":{"data":{"goal_status":"answered", "next_action":"report", "observations":[{"label":"Item total", "value":"$37", "quote":"Total $37", "source_url":url}], "unknowns":[], "blocker":None}, "success":True}}
            value = {"evaluation_previous_goal":"Page read", "memory":"Retain the captured item", "next_goal":"Return findings", "action":[action]}
            if "thinking" in schema.get("properties", {}): value["thinking"] = "Use captured fields"
            for key in ("current_plan_item", "plan_update"):
                if key in schema.get("properties", {}): value[key] = None
            return {"stop_reason":"end_turn", "content":[{"type":"text", "text":json.dumps(value)}]}
    model = Model(); started = time.monotonic()
    try:
        result = run_isolated_browser_task(BrowserTask(task=prompt, url=url, max_steps=3), client=model, principal_id="web:sdk-fields", policy=BrowserPolicy(), timeout=45, python=worker)
        assert model.calls == 2 and result["report_status"] == "completed", result
        assert result["observations"][0]["value"] == "$37"
        assert "find_elements" in result["execution_audit"]["action_names"]
        assert time.monotonic()-started < 35
    finally: server.shutdown(); server.server_close()
