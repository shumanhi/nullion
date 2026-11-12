"""HTTP/WS stop cancels the owning real process through the actual chat routes."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E only")


@pytest.mark.parametrize("stop_transport", ["http", "websocket"])
def test_http_browser_request_cancellation_releases_worker_and_preserves_other_turns(tmp_path, monkeypatch, stop_transport):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, OPERATOR_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry
    from tests.test_tool_execution_cancellation import fake_sdk_worker, wait_worker, wait_dead
    monkeypatch.setenv("HOME", str(tmp_path)); monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0"); monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off")
    chat_store._store = None
    executable, marker = fake_sdk_worker(tmp_path)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available', lambda:True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python', lambda:str(executable))
    class Provider:
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names = {tool["name"] for tool in kw.get("tools", [])}
            messages = str(kw.get("messages", []))
            if "Return only the number four" in messages:
                return {"stop_reason":"end_turn","content":[{"type":"text","text":"4"}]}
            if not names:
                return {"stop_reason":"end_turn","content":[{"type":"text","text":json.dumps({"confidence":1})}]}
            name = "browser_run_task" if "browser_run_task" in names else "request_tool_scope"
            arguments = ({"task":"Read the public page", "url":"https://example.com", "max_steps":2}
                if name == "browser_run_task" else {"capabilities":["web"],
                    "tool_names":["browser_run_task"],"required_tool_names":["browser_run_task"]})
            return {"stop_reason":"tool_use","content":[{"type":"tool_use","id":name,"name":name,"input":arguments}]}
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule("fixture", OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, "*", "fixture", datetime.now(UTC)))
    runtime.checkpoint(); model = Provider(); runtime.model_client = model
    registry = ToolRegistry(); register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda:model)
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(client.post, "/api/chat", json={"text":"Perform the prepared public observation.",
            "conversation_id":"web:cancel-own", "turn_id":"cancel-owned-turn"})
        pid = wait_worker(marker)
        started = time.monotonic()
        if stop_transport == "http":
            stopped = client.post("/api/chat", json={"text":"/stop", "conversation_id":"web:cancel-own"}).json()
        else:
            with client.websocket_connect("/ws/chat") as socket:
                socket.send_json({"text":"/stop", "conversation_id":"web:cancel-own", "turn_id":"stop-ui"})
                stopped = socket.receive_json()
                assert stopped["type"] == "session_stopped", stopped
        assert stopped["cancelled_turn_ids"] == ["cancel-owned-turn"], stopped
        assert "Stopped" in stopped["text"]
        original = pending.result(timeout=3)
        assert original.status_code == 200 and original.json()["superseded"] is True, original.text
        wait_dead(pid)
        assert time.monotonic() - started < 3
        ordinary = client.post("/api/chat", json={"text":"Return only the number four", "conversation_id":"web:other"})
        assert ordinary.status_code == 200 and ordinary.json()["text"] == "4", ordinary.text
