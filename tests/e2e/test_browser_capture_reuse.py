"""Saved captures pass through real chat history, source persistence and API delivery."""
import json
import os
import statistics
import time

import pytest

from tests.e2e.test_tracker_status_delivery import setup
from tests.test_browser_capture_reuse_contract import capture
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


@pytest.mark.parametrize("linked", [False, True])
def test_followups_read_verified_capture_without_reentering_browser(tmp_path, monkeypatch, linked):
    from fastapi.testclient import TestClient
    from nullion import chat_operator
    from nullion.tools import ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    # Model reasoning can only use the durable capture when prior transcript
    # structure is unavailable to the lightweight relationship check.
    if linked:
        monkeypatch.setattr(chat_operator, "_chat_thread_has_structured_relationship_evidence",
                            lambda *args, **kwargs: False)
    url = "https://example.com/results"
    browser_calls = {}
    current = ""
    cid = ""
    prompts = ("¿Y la otra opción?", "もう一つの選択肢は？", "والخيار الآخر؟")

    def call(name, args):
        return {"stop_reason": "tool_use", "content": [
            {"type": "tool_use", "id": name, "name": name, "input": args}]}

    def text(value):
        return {"stop_reason": "end_turn", "content": [{"type": "text", "text": value}]}

    class Model:
        def create(self, **kwargs):
            if "keys relationship, effect" in kwargs.get("system", ""):
                continue_turn = linked and current in prompts
                return text(json.dumps({"relationship": "follow_up" if continue_turn else "separate",
                    "effect": "continue", "confidence": 1, "target_index": 0 if continue_turn else None,
                    "target_is_explicit": continue_turn}))
            protocol = workflow_provider_reply(kwargs, completion=False)
            if protocol is not None:
                return protocol
            names = {tool["name"] for tool in kwargs.get("tools", [])}
            if not names:
                return text("{}")
            messages = json.dumps(kwargs["messages"], ensure_ascii=False)
            if current == "独立した質問":
                return text("独立した回答")
            if current == "2 + 2":
                assert "retained_browser_snapshot" not in messages
                return text("4")
            if current in prompts:
                if linked:
                    assert "retained_browser_snapshot" in messages and "Otra opción $35" in messages
                    assert "[BEGIN UNTRUSTED TOOL OUTPUT]" in messages
                    return text("Otra opción $35. " + url)
                if "retained_browser_snapshot" not in messages:
                    return call("chat_history_search", {"browser_snapshot_max_age_seconds": 120})
                assert "Otra opción $35" in messages
                assert "untrusted_tool_output" in messages
                return text("Otra opción $35. " + url)
            if cid not in browser_calls:
                if "browser_run_task" not in names:
                    return call("request_tool_scope", {"capabilities": ["web"], "tool_names": ["browser_run_task"],
                                                       "required_tool_names": ["browser_run_task"]})
                return call("browser_run_task", {"url": url, "task": "Consulta precios"})
            return text("Precio $20. " + url)

    def browser(invocation):
        if cid in browser_calls:
            time.sleep(2)
            pytest.fail("The follow-up reopened the browser")
        browser_calls[cid] = invocation.invocation_id
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed",
                          capture(url, time.time(), "$20"))

    app, runtime, db = setup(tmp_path, monkeypatch, Model())
    app.state.live_tracker_engine.registry.register(ToolSpec(
        "browser_run_task", "External browser capture", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 10), browser)
    times = []
    cases = []
    with TestClient(app) as client:
        for index, followup in enumerate(prompts):
            cid = f"web:captured-price-{index}"
            cases.append(cid)
            requests = [("Consulta precios " + url, "$20"), (followup, "$35"), ("2 + 2", "4")]
            if linked:
                requests.insert(1, ("独立した質問", "独立した回答"))
            for prompt, expected in requests:
                current = prompt
                start = time.monotonic()
                response = client.post("/api/chat", json={"text": prompt, "conversation_id": cid})
                if expected == "$35":
                    times.append(time.monotonic() - start)
                assert response.status_code == 200 and expected in response.json()["text"], response.text
    from nullion.runtime import bootstrap_persistent_runtime

    assert len(browser_calls) == len(prompts)
    store = bootstrap_persistent_runtime(db).store
    for cid in cases:
        events = store.list_recent_conversation_events(cid, event_type="conversation.chat_turn", limit=4)
        assert len(events) == (4 if linked else 3)
        turn = events[-2]
        if linked:
            assert turn["parent_turn_id"] == events[0]["turn_id"]
        audit = store.list_recent_conversation_events(cid, event_type="conversation.retained_browser_snapshot")
        if linked:
            event, = audit
            assert event["turn_id"] == turn["turn_id"] and event["source_invocation_id"] == browser_calls[cid]
            assert not any(r["tool_name"] == "chat_history_search" for r in turn["tool_results"])
        else:
            result, = [r for r in turn["tool_results"] if r["tool_name"] == "chat_history_search"]
            assert result["output"]["source_invocation_id"] == browser_calls[cid]
        assert not any(r["tool_name"] == "browser_run_task" for event in events[1:] for r in event["tool_results"])
    assert statistics.mean(times) < 1.5 and max(times) < 2, times
