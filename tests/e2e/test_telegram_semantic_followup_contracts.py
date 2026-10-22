"""Real Telegram handler, graph, model loop and SQLite; fake network boundaries."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="opt-in E2E")


@pytest.mark.parametrize("followup,relationship", [("Find me the best one on amazon", "follow_up"), ("Busca el mejor en esa tienda", "follow_up"), ("その店で一番いいものを探して", "follow_up"), ("Tell me a joke", "separate"), ("Cuéntame un chiste", "separate"), ("冗談を聞かせて", "separate")])
def test_telegram_completed_context_and_overlapping_semantic_followup(tmp_path, monkeypatch, followup, relationship):
    from nullion import chat_store, telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "false")
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "false")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off")
    chat_store._store = None
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(name="web_fetch", description="Read product listing", risk_level=ToolRiskLevel.LOW,
                 side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=5,
                 capability_tags=("web", "research"), input_schema={"type": "object", "properties": {"url": {"type": "string"}}}),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed",
                                      {"url": "https://example.test/product", "title": "OxiClean Max Force", "text": "OxiClean Max Force laundry stain remover listing is available."}),
    )
    runtime.active_tool_registry = registry
    second_ready, release_second, classified = threading.Event(), threading.Event(), threading.Event()

    class Model:
        def __init__(self):
            self.requests = []
            self.relationship_calls = []

        def create(self, **kwargs):
            if "allowed_target_indexes" in json.dumps(kwargs.get("messages", [])):
                self.relationship_calls.append(kwargs)
                time.sleep(0.5)  # Previously slow relationship dependency.
                classified.set()
                if kwargs.get("max_tokens", 0) < 512:
                    # Simulate reasoning consuming the old 120-token budget.
                    return {"completion": {"status": "incomplete", "reason": "max_output_tokens"},
                            "content": [{"type": "text", "text": '{"relationship":"follow_up",'}]}
                return {"content": [{"type": "text", "text": json.dumps({"relationship": relationship, "effect": "continue", "confidence": 0.98, "target_index": 0 if relationship == "follow_up" else None, "target_is_explicit": relationship == "follow_up"})}]}
            if not kwargs.get("tools") and "timeout" not in kwargs:
                # Off-path skill learning shares the provider; it must not
                # consume a scripted foreground response.
                return {"content": [{"type": "text", "text": "{}"}]}
            self.requests.append(kwargs)
            count = len(self.requests)
            if count == 2:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "scope", "name": "request_tool_scope", "input": {"tool_names": ["web_fetch"]}}]}
            if count == 3:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "lookup", "name": "web_fetch", "input": {"url": "https://example.test/product"}}]}
            if count == 4:
                second_ready.set()
                assert release_second.wait(15), "test must release in-flight lookup"
            replies = {1: "I recommend OxiClean Max Force for laundry stains.", 4: "The OxiClean Max Force listing is available: https://example.test/product", 5: "For your laundry stains, choose OxiClean Max Force: https://example.test/product"}
            if relationship == "separate":
                replies[5] = "Why did the bicycle fall over? It was two tired."
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": replies.get(count, "Hello.")}]}

    model = Model()
    settings = load_settings(env={"NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz", "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
                                               operator_chat_id="123", model_client=model,
                                               agent_orchestrator=AgentOrchestrator(model_client=model))
    sent = []
    idle_durations = []

    class Message:
        caption = None
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")
        reply_to_message = None

        def __init__(self, text, message_id):
            self.text, self.message_id = text, message_id
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)

        async def send_action(self, action):
            return None

        async def reply_text(self, text, **kwargs):
            sent.append((self.message_id, text))
            return SimpleNamespace(message_id=1000 + len(sent))

    async def run():
        async def deliver(text, mid):
            await service.on_message(SimpleNamespace(update_id=mid, message=Message(text, mid)), SimpleNamespace(bot=None))

        await deliver("Find me the best stain remover", 1)
        second = asyncio.create_task(deliver("Is this on amazon?", 2))
        assert await asyncio.to_thread(second_ready.wait, 15)
        third = asyncio.create_task(deliver(followup, 3))
        try:
            assert await asyncio.to_thread(classified.wait, 15)
            expected = 4 if relationship == "follow_up" else 5
            deadline = time.perf_counter() + 2
            while len(model.requests) < expected and time.perf_counter() < deadline:
                await asyncio.sleep(0.01)
            assert len(model.requests) == expected
        finally:
            release_second.set()
        await asyncio.gather(second, third)
        await asyncio.sleep(0.05)
        for mid, prompt in enumerate(["Hello", "こんにちは", "¿Qué tal?", "Salut", "مرحبًا"], start=4):
            start = time.perf_counter()
            await deliver(prompt, mid)
            idle_durations.append(time.perf_counter() - start)

    asyncio.run(run())
    assert len(model.relationship_calls) == 1
    request_diagnostics = [
        {"max_tokens": request.get("max_tokens"), "timeout": request.get("timeout"),
         "tools": [tool.get("name") for tool in request.get("tools", [])],
         "system": str(request.get("system") or request.get("messages", [{}])[0].get("content", ""))[:240]}
        for request in model.requests
    ]
    assert len(model.requests) == 10, "\n" + "\n".join(
        json.dumps({"call": index, **request}, ensure_ascii=False)
        for index, request in enumerate(request_diagnostics, start=1)
    )
    assert sum(idle_durations) / len(idle_durations) < 0.4
    assert max(idle_durations) < 0.5
    last_context = json.dumps(model.requests[4]["messages"], ensure_ascii=False)
    if relationship == "follow_up":
        assert "OxiClean Max Force" in last_context
        assert "Find me the best stain remover" in last_context
        assert "https://example.test/product" in last_context
        assert any(mid == 3 and text == "Got it — I’ll continue from the current request." for mid, text in sent)
        assert any(mid == 3 and "For your laundry stains" in text for mid, text in sent)
    else:
        assert any(mid == 3 and "bicycle" in text for mid, text in sent)
        assert all(mid != 3 or text != "Got it — I’ll continue from the current request." for mid, text in sent)
    assert any(mid == 2 and text == "⌛ On it! Feel free to send other tasks — I can handle multiple at once." for mid, text in sent)
    loaded = load_runtime_store(tmp_path / "runtime.db")
    child = loaded.get_conversation_turn("telegram-update:3")
    assert child.parent_turn_id == ("telegram-update:2" if relationship == "follow_up" else None)
    assert child.disposition.value == ("continue" if relationship == "follow_up" else "independent")
    assert child.disposition_reason == ("dispatch:model_structured_follow_up" if relationship == "follow_up" else "dispatch:model_structured_separate")


@pytest.mark.parametrize("question,choices,recommendation", [
    ("Find me the best stain remover", "What kind of stain?\n1. Clothing/laundry\n2. Carpet", "OxiClean Max Force for laundry stains."),
    ("Busca un quitamanchas", "¿Qué superficie?\n1. Ropa\n2. Alfombra", "OxiClean Max Force para la ropa."),
])
def test_web_numeric_followup_does_not_revive_old_scheduler_scope(tmp_path, monkeypatch, question, choices, recommendation):
    from fastapi.testclient import TestClient
    from nullion import chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from nullion.web_app import create_app

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "false")
    chat_store._store = None
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    cid = "web:numeric-followup"
    runtime.process_conversation_message(conversation_id=cid, user_message="Run the previous saved job", turn_id="old")
    runtime.store.add_conversation_event({
        "conversation_id": cid, "event_type": "conversation.chat_turn", "turn_id": "old",
        "user_message": "Run the previous saved job", "assistant_reply": "Previous report started.",
        "tool_results": [{"tool_name": "run_cron", "status": "completed", "output": {"id": "old-job", "name": "Old report"}}],
    })
    invoked = []
    registry = ToolRegistry()
    registry.register(ToolSpec(name="run_cron", description="Run a saved scheduled job", risk_level=ToolRiskLevel.LOW,
                              side_effect_class=ToolSideEffectClass.WRITE, requires_approval=False, timeout_seconds=5),
                      lambda invocation: invoked.append(invocation.tool_name))

    class Model:
        def __init__(self):
            self.foreground = []

        def create(self, **kwargs):
            if kwargs.get("system") or (not kwargs.get("tools") and "timeout" not in kwargs):
                return {"content": [{"type": "text", "text": json.dumps({"web_action": "none", "scheduler_action": "none", "skill_pack_action": "none", "requested_tool_names": [], "required_tool_names": [], "confidence": 0.99})}]}
            self.foreground.append(kwargs)
            if len(self.foreground) == 2:
                assert "run_cron" not in {tool["name"] for tool in kwargs.get("tools", [])}
                assert "old-job" not in json.dumps(kwargs["messages"])
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": choices if len(self.foreground) == 1 else recommendation}]}

    model = Model()
    app = create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    with TestClient(app) as client:
        first = client.post("/api/chat", json={"text": question, "conversation_id": cid, "turn_id": "current"})
        assert first.status_code == 200
        assert choices in first.json()["text"]
        second = client.post("/api/chat", json={"text": "1", "conversation_id": cid, "turn_id": "choice"})
        assert second.status_code == 200
        assert recommendation in second.json()["text"]
    assert invoked == []
    assert len(model.foreground) == 2
    choice = runtime.store.get_conversation_turn("choice")
    assert choice.parent_turn_id == "current"
    assert choice.disposition_reason == "dispatch:numbered_option_reply"
