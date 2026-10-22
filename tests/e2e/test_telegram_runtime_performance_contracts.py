from __future__ import annotations

import asyncio
import os
from pathlib import Path
import time
from types import SimpleNamespace

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run Telegram runtime performance e2e tests",
)


def test_telegram_chat_turn_persists_dedupe_and_ignores_retry_without_extra_user_reply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import chat_operator, chat_store as chat_store_module, telegram_app
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolResult

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    monkeypatch.setenv("NULLION_CHAT_TURN_SLOW_LOG_MS", "999999")
    monkeypatch.setenv("NULLION_TELEGRAM_TURN_SLOW_LOG_MS", "999999")
    chat_store_module._store = None

    checkpoint = tmp_path / "runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )

    class FakeOrchestrator:
        model_client = SimpleNamespace(name="fake")

        def run_turn(self, **kwargs):  # noqa: ANN001
            assert kwargs["conversation_id"] == "telegram:123"
            return SimpleNamespace(
                final_text="返信できました",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text=None,
            )

    class Message:
        text = "こんにちは"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")

        async def send_action(self, action):  # noqa: ANN001
            return None

        chat = SimpleNamespace(id=123, send_action=send_action)

    deliveries: list[str] = []

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    async def capture_reply(message, reply, **kwargs) -> None:  # noqa: ANN001, ANN003
        deliveries.append(str(reply))

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_reply)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_append_chat_artifacts_to_reply", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(
        chat_operator,
        "format_tool_results_activity_detail",
        lambda tool_results: "",
    )

    service = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
        agent_orchestrator=FakeOrchestrator(),
    )
    update = SimpleNamespace(update_id=99, message=Message())

    asyncio.run(service.on_message(update, SimpleNamespace(bot=None)))
    asyncio.run(service.on_message(update, SimpleNamespace(bot=None)))

    assert deliveries == ["返信できました"]
    loaded = load_runtime_store(checkpoint)
    assert loaded.has_conversation_ingress_id("telegram:123", "telegram-update:99")
    assert [event["user_message"] for event in loaded.list_conversation_events("telegram:123") if event["event_type"] == "conversation.chat_turn"] == [
        "こんにちは"
    ]

    store = chat_store_module.get_chat_store()
    assert [(message["role"], message["text"]) for message in store.load_messages("telegram:123")] == [
        ("user", "こんにちは"),
        ("bot", "返信できました"),
    ]


def test_telegram_planner_command_renders_task_status_card_not_plain_reply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import telegram_app
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    monkeypatch.setenv("NULLION_TELEGRAM_TURN_SLOW_LOG_MS", "999999")

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
            "NULLION_TASK_PLANNER_FEED_MODE": "task",
        }
    )
    service = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )

    class Message:
        text = "/planner Build the requested planner PDF"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")
        replies: list[str] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append(text)
            return None

        chat = SimpleNamespace(id=123, send_action=send_action)

    status_messages: list[dict[str, object]] = []

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    def fake_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        assert kwargs["activity_callback"] is not None
        assert kwargs["append_activity_trace"] is False
        return (
            "Planner: Single Turn • 1 task\n"
            "For: Build the requested planner PDF\n"
            "→ Working on 1 task:\n"
            "  ☐ Build requested PDF"
        )

    async def capture_status(bot, status_store, **kwargs):  # noqa: ANN001, ANN003
        status_messages.append({"bot": bot, "status_store": status_store, **kwargs})
        return True

    async def fail_plain_reply(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("/planner should not be delivered as a normal chat reply")

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", fake_handle_update)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", capture_status)
    monkeypatch.setattr(telegram_app, "_deliver_reply", fail_plain_reply)

    message = Message()
    asyncio.run(service.on_message(SimpleNamespace(update_id=99, message=message), SimpleNamespace(bot=None)))

    assert message.replies == [
        "Activity\n→ Planner — Building and running the plan",
    ]
    assert len(status_messages) == 1
    assert status_messages[0]["chat_id"] == "123"
    assert status_messages[0]["group_id"]
    assert "🎯 **PLANNER" in str(status_messages[0]["text"])
    assert "SINGLE TURN" in str(status_messages[0]["text"])
    assert "Build the requested planner PDF" in str(status_messages[0]["text"])


@pytest.mark.parametrize("channel,user_id", [("telegram", "123"), ("slack", "U1"), ("discord", "D1")])
def test_plain_chat_turn_latency_budget_for_messaging_surfaces(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    channel: str,
    user_id: str,
) -> None:
    from nullion import chat_operator, chat_store as chat_store_module, crons, telegram_app, turn_context_policy
    from nullion.config import load_settings
    from nullion.messaging_adapters import MessagingIngress
    from nullion.messaging_turn_graph import run_messaging_turn_graph
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / f"{channel}-home"))
    monkeypatch.setenv("NULLION_CHAT_TURN_SLOW_LOG_MS", "999999")
    monkeypatch.setenv("NULLION_TELEGRAM_TURN_SLOW_LOG_MS", "999999")
    chat_store_module._store = None

    runtime = bootstrap_persistent_runtime(tmp_path / f"{channel}-runtime.db")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": str(channel == "telegram").lower(),
            "NULLION_SLACK_ENABLED": str(channel == "slack").lower(),
            "NULLION_DISCORD_ENABLED": str(channel == "discord").lower(),
        }
    )

    model_start_ms: list[float] = []
    total_ms: list[float] = []
    scoped_tool_counts: list[int] = []
    turn_started_at = 0.0

    class InstantOrchestrator:
        model_client = SimpleNamespace(name="instant-e2e")

        def run_turn(self, **kwargs):  # noqa: ANN001
            model_start_ms.append((time.perf_counter() - turn_started_at) * 1000)
            scoped_tool_counts.append(len(kwargs["tool_registry"].list_tool_definitions()))
            return SimpleNamespace(
                final_text="hello",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text=None,
                reached_iteration_limit=False,
            )

    def fail_plain_chat_checkpoint_refresh(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("plain chat must not block on runtime checkpoint refresh")

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    async def capture_reply(message, reply, **kwargs) -> None:  # noqa: ANN001, ANN003
        assert str(reply).strip()

    monkeypatch.setattr(telegram_app, "_refresh_runtime_from_checkpoint", fail_plain_chat_checkpoint_refresh)
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_reply)
    monkeypatch.setattr(chat_operator, "is_authorized_messaging_identity", lambda *args, **kwargs: True)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_append_chat_artifacts_to_reply", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(crons, "list_crons", lambda *args, **kwargs: [])

    def fail_slow_scope_dependency() -> list[dict[str, object]]:
        raise AssertionError("plain independent turns must not enter the tool-scope classifier path")

    def fail_plain_capability_context(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("plain independent turns must not build live capability/tool context")

    def fail_plain_fulfillment_evaluation(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("plain independent turns must not run deliverable fulfillment evaluation")

    monkeypatch.setattr(turn_context_policy, "_active_connector_provider_context", fail_slow_scope_dependency)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", fail_plain_capability_context)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_index_prompt", fail_plain_capability_context)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", fail_plain_capability_context)
    monkeypatch.setattr(chat_operator, "_builder_route_hints_prompt", fail_plain_capability_context)
    monkeypatch.setattr(chat_operator, "evaluate_response_execution_outcome", fail_plain_fulfillment_evaluation)

    service = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123" if channel == "telegram" else None,
        settings=settings,
        agent_orchestrator=InstantOrchestrator(),
    )

    class Message:
        text = "hi"
        caption = None
        from_user = SimpleNamespace(id=int(user_id) if user_id.isdigit() else 123, first_name="Ada", last_name="", username="ada")

        def __init__(self, message_id: int) -> None:
            self.message_id = message_id
            self.chat = SimpleNamespace(id=int(user_id) if user_id.isdigit() else 123, send_action=self.send_action)

        async def send_action(self, action):  # noqa: ANN001
            return None

    conversation_id = f"{channel}:{'123' if channel == 'telegram' else user_id}"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "user_message": "prior structured tool turn",
            "assistant_reply": "done",
            "tool_results": [{"tool_name": "connector_request", "status": "completed", "output": {}}],
        }
    )

    for index in range(5):
        turn_started_at = time.perf_counter()
        if channel == "telegram":
            update = SimpleNamespace(update_id=10_000 + index, message=Message(200 + index))
            asyncio.run(service.on_message(update, SimpleNamespace(bot=None)))
        else:
            ingress = MessagingIngress(
                channel=channel,
                user_id=user_id,
                text="hi",
                request_id=f"{channel}-latency-{index}",
                message_id=f"{channel}-message-{index}",
                delivery_target_id=user_id,
            )
            result = run_messaging_turn_graph(service, ingress)
            assert result.reply == "hello"
        total_ms.append((time.perf_counter() - turn_started_at) * 1000)

    average_model_start_ms = sum(model_start_ms) / len(model_start_ms)
    average_total_ms = sum(total_ms) / len(total_ms)
    assert average_model_start_ms < 350, (
        f"{channel} plain-chat app pre-model latency regressed: "
        f"avg={average_model_start_ms:.1f}ms samples={model_start_ms!r}"
    )
    assert max(model_start_ms) < 700, f"{channel} plain-chat pre-model spike: samples={model_start_ms!r}"
    assert average_total_ms < 650, f"{channel} plain-chat average latency regressed: avg={average_total_ms:.1f}ms samples={total_ms!r}"
    assert max(total_ms) < 1200, f"{channel} plain-chat latency spike: samples={total_ms!r}"
    assert max(scoped_tool_counts) <= 10, f"{channel} plain-chat carried too many tools: {scoped_tool_counts!r}"


@pytest.mark.parametrize("channel,user_id", [("slack", "U1"), ("discord", "D1")])
def test_shared_messaging_surfaces_coalesce_ingress_marker_into_chat_save(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    channel: str,
    user_id: str,
) -> None:
    from nullion import chat_operator, chat_store as chat_store_module
    from nullion.config import load_settings
    from nullion.messaging_adapters import MessagingIngress
    from nullion.messaging_turn_graph import run_messaging_turn_graph
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.telegram_app import ChatOperatorService

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    monkeypatch.setenv("NULLION_CHAT_TURN_SLOW_LOG_MS", "999999")
    chat_store_module._store = None

    checkpoint = tmp_path / f"{channel}-runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)
    settings = load_settings(env={
        "NULLION_TELEGRAM_CHAT_ENABLED": str(channel == "telegram").lower(),
        "NULLION_SLACK_ENABLED": str(channel == "slack").lower(),
        "NULLION_DISCORD_ENABLED": str(channel == "discord").lower(),
    })

    class FakeOrchestrator:
        model_client = SimpleNamespace(name="fake")

        def run_turn(self, **kwargs):  # noqa: ANN001
            assert kwargs["conversation_id"] == f"{channel}:{user_id}"
            return SimpleNamespace(
                final_text=f"{channel} reply",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                thinking_text=None,
            )

    monkeypatch.setattr(chat_operator, "is_authorized_messaging_identity", lambda *args, **kwargs: True)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_append_chat_artifacts_to_reply", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])

    service = ChatOperatorService(
        runtime=runtime,
        bot_token="unused",
        operator_chat_id=None,
        settings=settings,
        agent_orchestrator=FakeOrchestrator(),
    )
    request_id = f"{channel}-request-1"
    ingress = MessagingIngress(
        channel=channel,
        user_id=user_id,
        text="hola",
        request_id=request_id,
        message_id=f"{channel}-message-1",
        delivery_target_id=user_id,
    )

    result = run_messaging_turn_graph(service, ingress)

    assert result.reply == f"{channel} reply"
    loaded = load_runtime_store(checkpoint)
    assert loaded.has_conversation_ingress_id(f"{channel}:{user_id}", request_id)
    assert [event["user_message"] for event in loaded.list_conversation_events(f"{channel}:{user_id}") if event["event_type"] == "conversation.chat_turn"] == [
        "hola"
    ]
