"""Actual SDK actions must expose extracted DOM values to the next model step."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pytest
pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker browser boundary E2E")

@pytest.mark.parametrize("interrupt", [False,True])
@pytest.mark.parametrize("prompt", ["Read the price and retain the captured findings.", "価格を読み取って記録してください。", "Lee el precio y conserva lo observado."])
def test_completed_sdk_history_is_auditable_after_step_limit(tmp_path, monkeypatch, prompt,interrupt):
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
        model = "deterministic-external-provider"; calls = 0;errors=[]
        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kwargs)
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self, **kwargs):
            self.calls += 1
            schema_message = next(m["content"] for m in kwargs["messages"] if isinstance(m.get("content"), str) and m["content"].startswith("Return valid JSON matching this schema: "))
            schema = json.loads(schema_message.split(": ", 1)[1])
            if self.calls == 1:
                action = {"record_observations":{"goal_status":"partial", "next_action":"report", "observations":[
                    {"label":"Item price", "value":"$37", "quote":"Total $37", "source_url":url,"semantic_role":"attribute"}],
                    "unknowns":["An additional field was not displayed"], "blocker":None}}
            else:
                assert self.calls <= 3
                # A valid schema with an unsupported quote must be rejected by
                # the real evidence controller, retaining the earlier $37.
                action = {"done":{"data":{"goal_status":"answered", "next_action":"report", "observations":[
                    {"label":"Unsupported price", "value":"$99", "quote":"Total $99", "source_url":url,"semantic_role":"attribute"}],
                    "unknowns":[], "blocker":None}, "success":True}}
            value = {"evaluation_previous_goal":"Page read", "memory":"Retain the captured item", "next_goal":"Return findings", "action":[action]}
            if "thinking" in schema.get("properties", {}): value["thinking"] = "Use captured fields"
            for key in ("current_plan_item", "plan_update"):
                if key in schema.get("properties", {}): value[key] = None
            return {"stop_reason":"end_turn", "content":[{"type":"text", "text":json.dumps(value)}]}
    model = Model(); started = time.monotonic()
    try:
        result = run_isolated_browser_task(BrowserTask(task=prompt, url=url, max_steps=2), client=model, principal_id="web:sdk-fields", policy=BrowserPolicy(), timeout=0.02 if interrupt else 45, python=worker)
        assert not model.errors,model.errors
        if interrupt:
            assert result["execution_audit"]["complete"] is False,result
            assert result["execution_audit"]["url_inventory_complete"] is False,result
            assert result["report_status"] in {"blocked","partial"},result
            return
        assert 2 <= model.calls <= 3 and result["report_status"] == "partial",result
        assert any(row["value"]=="$37" for row in result["observations"]),result
        assert all(row["value"]!="$99" for row in result["observations"]),result
        assert result["blocker"] and result["rejected_observations"]>=1,result
        assert result["execution_audit"]["complete"] is True,result
        assert result["execution_audit"]["read_only_actions_only"] is True,result
        assert url in result["execution_audit"]["visited_urls"]
        assert time.monotonic()-started < 35
    finally: server.shutdown(); server.server_close()
