"""Real Telegram handler, graph, model loop and SQLite; fake network boundaries."""

from __future__ import annotations

from tests.workflow_provider_protocol import workflow_provider_reply
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
        ToolSpec(name="listing_read", description="Read product listing", risk_level=ToolRiskLevel.LOW,
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            if "allowed_target_indexes" in json.dumps(kwargs.get("messages", [])):
                self.relationship_calls.append(kwargs)
                time.sleep(0.5)  # Previously slow relationship dependency.
                classified.set()
                if kwargs.get("max_tokens", 0) < 512:
                    # Simulate reasoning consuming the old 120-token budget.
                    return {"completion": {"status": "incomplete", "reason": "max_output_tokens"},
                            "content": [{"type": "text", "text": '{"relationship":"follow_up",'}]}
                return {"content": [{"type": "text", "text": json.dumps({"relationship": relationship, "effect": "continue", "confidence": 0.98, "target_index": 0 if relationship == "follow_up" else None, "target_is_explicit": relationship == "follow_up"})}]}
            if not kwargs.get("tools") and 'web_action' in str(kwargs.get('system') or ''):
                return {"content": [{"type": "text", "text": json.dumps({'web_action':'live_research','scheduler_action':'none',
                    'skill_pack_action':'none','requested_tool_names':['listing_read'],'required_tool_names':['listing_read'],'confidence':1})}]}
            if not kwargs.get("tools") and "timeout" not in kwargs:
                # Off-path skill learning shares the provider; it must not
                # consume a scripted foreground response.
                return {"content": [{"type": "text", "text": "{}"}]}
            self.requests.append(kwargs)
            count = len(self.requests)
            if count == 2:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "scope", "name": "request_tool_scope", "input": {"capabilities": ["web"], "tool_names": ["listing_read"]}}]}
            if count == 3:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "lookup", "name": "listing_read", "input": {"url": "https://example.test/product"}}]}
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
    assert sum(mid == 2 and "listing is available" in text for mid, text in sent) == 1
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            if not kwargs.get("tools") and 'web_action' in str(kwargs.get('system') or ''):
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
    assert choice.disposition_reason in {"dispatch:numbered_option_reply", "dispatch:numbered_conversation_choice"}


@pytest.mark.parametrize("open_task", [False, True])
def test_idle_telegram_links_completed_source_around_unrelated_turn(tmp_path, monkeypatch, open_task):
    from nullion import telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("NULLION_CODE_MODE", "off")
    source = "https://example.com/results"
    first, unrelated, followup = "Consulta el precio", "Cuéntame un chiste", "ほかの便は？"
    if open_task:
        unrelated, followup = "日付を確認してから予定を作ってください。", "明日です。"
    reads, decisions, sent = [], [], []
    registry = ToolRegistry()

    def read(invocation):
        reads.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed",
                          {"result_kind": "browser_page_collection", "completed_pages": 1,
                           "documents": [{"url": source, "text": "$20" if len(reads) == 1 else "$35"}]})

    registry.register(ToolSpec("browser_read_pages", "Read source", ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, capability_tags=("web",), input_schema={"type": "object"}), read)
    runtime.active_tool_registry = registry

    class Model:
        current = first

        def create(self, **kwargs):
            system = str(kwargs.get("system", ""))
            messages = json.dumps(kwargs.get("messages", []), ensure_ascii=False)
            if "keys relationship, effect" in system:
                request = json.loads(kwargs["messages"][0]["content"][0]["text"])
                decisions.append(request)
                if self.current == followup:
                    target = (0 if open_task else next(item["index"] for item in request["active_requests"]
                                                       if item["user_request"] == first))
                    return text(json.dumps({"relationship": "follow_up", "effect": "continue", "confidence": 1,
                                            "target_index": target, "target_is_explicit": True}))
                return text(json.dumps({"relationship": "separate", "confidence": 1, "target_index": None}))
            protocol = workflow_provider_reply(kwargs, completion=False)
            if protocol is not None:
                return protocol
            names = {tool["name"] for tool in kwargs.get("tools", [])}
            if not names:
                return text("{}")
            if self.current == unrelated:
                return text("何日ですか？" if open_task else "Un chiste breve.")
            if open_task and self.current == followup:
                return text("日付を確認しました。")
            target_count = 1 if self.current == first else 2
            if len(reads) < target_count:
                assert "retained_browser_snapshot" not in messages
                if "browser_read_pages" not in names:
                    return call("request_tool_scope", {"capabilities": ["web"],
                                "tool_names": ["browser_read_pages"], "required_tool_names": ["browser_read_pages"]})
                return call("browser_read_pages", {"url": source})
            return text("$20" if self.current == first else "$35")

    model = Model()
    settings = load_settings(env={"NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
                                 "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
        operator_chat_id="123", model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))

    class Message:
        caption = None
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")
        reply_to_message = None

        def __init__(self, prompt, mid):
            self.text, self.message_id = prompt, mid
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)

        async def send_action(self, action):
            return None

        async def reply_text(self, reply, **kwargs):
            sent.append((self.message_id, reply))
            return SimpleNamespace(message_id=1000 + len(sent))

    async def run():
        for mid, prompt in enumerate((first, unrelated, followup), 1):
            model.current = prompt
            await service.on_message(SimpleNamespace(update_id=mid, message=Message(prompt, mid)), SimpleNamespace(bot=None))
            if open_task and mid == 2:
                from datetime import UTC, datetime
                from nullion import turn_dispatch_choices
                from nullion.task_frames import (TaskFrame, TaskFrameExecutionContract, TaskFrameFinishCriteria,
                    TaskFrameOperation, TaskFrameOutputContract, TaskFrameStatus)

                active = runtime.store.get_conversation_turn("telegram-update:2")
                now = datetime.now(UTC)
                frame = TaskFrame("fixture-open-task", active.conversation_id, active.branch_id, active.turn_id,
                    None, TaskFrameStatus.WAITING_INPUT, TaskFrameOperation.ANSWER_WITH_CONTEXT, None,
                    TaskFrameExecutionContract(), TaskFrameOutputContract(), TaskFrameFinishCriteria(False),
                    unrelated, now, now)
                runtime.store.add_task_frame(frame)
                runtime.store.set_active_task_frame_id(active.conversation_id, frame.frame_id)

                def reject_browser_fallback(*args, **kwargs):
                    pytest.fail("Open task context must take priority over the older browser turn")

                monkeypatch.setattr(turn_dispatch_choices, "completed_browser_dispatch_candidates", reject_browser_fallback)

    asyncio.run(run())
    assert len(reads) == (1 if open_task else 2)
    expected = "日付を確認しました。" if open_task else "$35"
    assert any(mid == 3 and expected in reply for mid, reply in sent)
    assert decisions[-1]["current_message"] == followup
    turn = runtime.store.get_conversation_turn("telegram-update:3")
    assert turn.parent_turn_id == ("telegram-update:2" if open_task else "telegram-update:1")
    assert turn.disposition.value == "continue"
    assert not runtime.store.list_recent_conversation_events("telegram:123", event_type="conversation.retained_browser_snapshot")
