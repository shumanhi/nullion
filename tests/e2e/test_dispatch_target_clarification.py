"""Real HTTP ingress, concurrent routing, persisted choices and publication."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


@pytest.mark.parametrize("message", ["Fix it", "Corrígelo", "修正して"])
@pytest.mark.parametrize("late_result", [False, True])
def test_unquoted_ambiguous_message_asks_before_touching_running_request(tmp_path, monkeypatch, message, late_result):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _remember_chat_turn
    from nullion.runtime_persistence import load_runtime_store
    from tests.e2e.test_inflight_amendment_delivery import _runtime_fixture
    from tests.workflow_provider_protocol import workflow_provider_reply

    fixture = _runtime_fixture(tmp_path, monkeypatch, "not-used")
    runtime = fixture.runtime
    cid = "web:separate-request-families"
    _remember_chat_turn(runtime, chat_id=cid, conversation_id=cid, conversation_turn_id="invitation",
        user_message="Create an invitation for 8 PM at my place", assistant_reply="Invitation preview")

    class Model:
        classifications = 0
        primary_calls = 0
        def create(self, **kwargs):
            encoded = json.dumps(kwargs.get("messages", []), ensure_ascii=False)
            if "allowed_target_indexes" in encoded:
                self.classifications += 1
                return {"content": [{"type": "text", "text": json.dumps({"relationship": "ambiguous",
                    "effect": "revise", "confidence": 1})}]}
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            self.primary_calls += 1
            if message in encoded:
                assert "8 PM at my place" in encoded
                return {"content": [{"type": "text", "text": "INVITATION_UPDATED: 8 PM at my place"}]}
            return fixture.model.create(**kwargs)

    model = Model()
    app = web_app.create_app(runtime, AgentOrchestrator(model_client=model), fixture.registry)
    with TestClient(app) as client, ThreadPoolExecutor() as pool:
        running = pool.submit(client.post, "/api/chat", json={"text": "Inspect the current departure board.",
            "conversation_id": cid, "turn_id": "aviation"})
        try:
            assert fixture.entered.wait(10)
            before = model.primary_calls
            started = time.monotonic()
            question = client.post("/api/chat", json={"text": message, "conversation_id": cid, "turn_id": "question"})
            assert time.monotonic() - started < 5
            assert question.status_code == 200, question.text
            body = question.json()["text"]
            assert "Which request" in body and "1." in body and "2." in body
            assert "invitation" in body.lower() and "departure board" in body
            assert model.primary_calls == before, "Clarification executed the ambiguous request"
            assert not any(event.get("event_type") == "conversation.reply_superseded"
                and event.get("turn_id") == "aviation" for event in runtime.store.list_conversation_events(cid))
            if late_result:
                fixture.release.set()
                assert not running.result(timeout=20).json().get("superseded")
                before_repeat = model.primary_calls
                repeated = client.post("/api/chat", json={"text":"2", "conversation_id":cid, "turn_id":"repeated-question"})
                assert "Which request" in repeated.json()["text"], repeated.text
                assert model.primary_calls == before_repeat
            chosen = client.post("/api/chat", json={"text": "2", "conversation_id": cid, "turn_id": "chosen"})
            assert chosen.status_code == 200, chosen.text
            assert "INVITATION_UPDATED" in chosen.json()["text"], chosen.text
            assert model.classifications == 1, "Numeric UI selection must not run another classifier"
            assert runtime.store.get_conversation_turn("chosen").parent_turn_id == "invitation"
        finally:
            fixture.release.set()
            response = running.result(timeout=20)
        assert not response.json().get("superseded"), response.text
    deadline = time.monotonic() + 5
    while True:
        restored = load_runtime_store(tmp_path / "runtime.db")
        if restored.get_conversation_turn("chosen") is not None or time.monotonic() >= deadline:
            break
        time.sleep(.02)
    assert restored.get_conversation_turn("chosen") is not None, {
        "checkpoint":str(runtime.checkpoint_path), "memory_head":runtime.store.get_conversation_head(cid),
        "disk_head":restored.get_conversation_head(cid), "disk_turns":list(restored.conversation_turns),
        "disk_events":[(row.get("event_type"),row.get("turn_id")) for row in restored.list_conversation_events(cid)]}
    assert restored.get_conversation_turn("chosen").parent_turn_id == "invitation"
    assert restored.get_conversation_turn("question").parent_turn_id is None


def test_plain_chat_does_not_pay_for_slow_concurrent_relationship_model(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path, monkeypatch)
    class Model:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            if "allowed_target_indexes" in json.dumps(kwargs.get("messages", [])):
                time.sleep(1)
                pytest.fail("Concurrent classifier entered the ordinary chat path")
            return {"stop_reason":"end_turn", "content":[{"type":"text", "text":"Hello."}]}
    model = Model()
    elapsed = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), ToolRegistry())) as client:
        for index, message in enumerate(["Bonjour", "こんにちは", "Hola", "नमस्ते"]):
            started = time.monotonic()
            reply = client.post("/api/chat", json={"text":message, "conversation_id":f"web:fast-{index}"})
            elapsed.append(time.monotonic() - started)
            assert reply.status_code == 200 and "Hello" in reply.json()["text"], reply.text
    assert model.calls == 4
    assert sum(elapsed) / len(elapsed) < .75, elapsed
    assert max(elapsed) < 1.5, elapsed
