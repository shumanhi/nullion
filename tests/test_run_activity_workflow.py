from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion.run_activity import (
    RunActivityPhase,
    activity_trace_enabled,
    activity_trace_status_text,
    append_activity_trace_to_reply,
    classify_run_activity_phase,
    format_activity_detail_lines,
    format_activity_sublist_line,
    format_mini_agent_activity_detail,
    format_run_activity_trace,
    format_skill_usage_activity_detail,
    format_skill_usage_activity_line,
    format_tool_activity_detail,
    format_tool_activity_line,
    format_tool_inventory_activity_detail,
    set_activity_trace_enabled,
    set_task_planner_feed_enabled,
    set_task_planner_feed_mode,
    set_verbose_mode,
    task_planner_feed_enabled,
    task_planner_feed_mode,
    task_planner_feed_status_text,
    verbose_mode,
    verbose_mode_status_text,
)
from nullion.task_status_format import compact_task_status_subject
from nullion.tools import ToolResult


def test_operator_verbose_command_uses_activity_on_off(monkeypatch) -> None:
    from nullion.operator_commands import (
        handle_operator_command,
        is_new_command_text,
        is_operator_command_text,
        is_stop_command_text,
        operator_command_suggestions,
    )

    monkeypatch.delenv("NULLION_ACTIVITY_TRACE_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_MODE", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_ENABLED", raising=False)

    assert ("/stop", "Stop active work in this session") in operator_command_suggestions()
    assert is_operator_command_text("/stop")
    assert is_operator_command_text("/status@NullionBot")
    assert is_operator_command_text("/stream off")
    assert not is_operator_command_text("/Users/example/Downloads/file.txt")
    assert not is_operator_command_text("/tmp/資料.txt")
    assert is_stop_command_text("/stop")
    assert is_stop_command_text("/stop@NullionBot")
    assert not is_stop_command_text("stop")
    assert is_new_command_text("/new")
    assert is_new_command_text("/new@NullionBot extra")
    assert not is_new_command_text("/new@")
    assert not is_new_command_text("new")
    assert handle_operator_command(None, "/stop") == "No active tasks are running in this session."
    assert handle_operator_command(None, "/stop now") == "Usage: /stop"
    assert ("/verbose [on|off|status]", "Toggle activity details") in operator_command_suggestions()
    assert handle_operator_command(None, "/verbose status") == "Verbose mode is off."
    assert handle_operator_command(None, "/verbose off") == "Verbose mode is off."
    assert task_planner_feed_mode() == "task"
    assert handle_operator_command(None, "/verbose on") == "Verbose mode is on."
    assert task_planner_feed_mode() == "task"
    assert handle_operator_command(None, "/verbose activity") == "Usage: /verbose [on|off|status]"
    assert handle_operator_command(None, "/verbose planner") == "Usage: /verbose [on|off|status]"
    assert handle_operator_command(None, "/verbose full") == "Usage: /verbose [on|off|status]"
    assert handle_operator_command(None, "/planner-feed status").startswith("Unknown command.")


def test_image_generation_setup_activity_hides_internal_env_var() -> None:
    detail = format_tool_activity_detail(
        [
            ToolResult(
                "inv-img",
                "image_generate",
                "failed",
                {},
                error="local_media_provider requires NULLION_IMAGE_GENERATE_COMMAND for image_generate",
            )
        ]
    )

    assert "image_generate — provider not configured" in detail
    assert "NULLION_IMAGE_GENERATE_COMMAND" not in detail


def test_browser_tool_activity_hides_locator_errors_and_artifact_paths() -> None:
    detail = format_tool_activity_detail(
        [
            ToolResult(
                "click",
                "browser_click_element",
                "failed",
                {},
                error="Click failed: Locator.click: Timeout 5000ms exceeded.\nwaiting for get_by_text(\"dummy\")",
            ),
            ToolResult(
                "shot",
                "browser_screenshot",
                "completed",
                {"artifact_path": "/Users/example/.nullion-stage/workspaces/ws/artifacts/screenshot-123.png"},
            ),
        ]
    )

    assert "browser_click_element — failed" in detail
    assert "browser_screenshot" in detail
    assert "Locator.click" not in detail
    assert "dummy" not in detail
    assert "screenshot-123.png" not in detail


def test_tool_activity_hides_failed_retry_when_same_tool_later_succeeds() -> None:
    detail = format_tool_activity_detail(
        [
            ToolResult(
                "sheet-1",
                "spreadsheet_create",
                "failed",
                {},
                error="spreadsheet_create could not embed all requested image paths",
            ),
            ToolResult(
                "sheet-2",
                "spreadsheet_create",
                "completed",
                {"artifact_path": "/tmp/stage_browser_image_manifest.xlsx"},
            ),
        ]
    )

    assert "spreadsheet_create" in detail
    assert "could not embed" not in detail
    assert "failed" not in detail


def test_account_tool_activity_does_not_leak_message_or_calendar_payloads() -> None:
    detail = format_tool_activity_detail(
        [
            ToolResult(
                "search",
                "email_search",
                "completed",
                {
                    "resultSizeEstimate": 2,
                    "results": [
                        {
                            "id": "msg-1",
                            "threadId": "thread-1",
                            "from": "Private Sender <private@example.com>",
                            "subject": "Sensitive subject",
                        },
                        {"id": "msg-2", "subject": "Another sensitive subject"},
                    ],
                },
            ),
            ToolResult(
                "read",
                "email_read",
                "completed",
                {
                    "id": "msg-1",
                    "message": {
                        "headers": {"from": "Private Sender <private@example.com>"},
                        "body": "Sensitive email body",
                    },
                },
            ),
            ToolResult(
                "calendar",
                "calendar_list",
                "completed",
                {
                    "result_count": 1,
                    "results": [
                        {
                            "summary": "Private appointment",
                            "description": "Sensitive details",
                        }
                    ],
                },
            ),
            ToolResult(
                "skill",
                "skill_pack_read",
                "completed",
                {"path": "/skills/private/SKILL.md", "text": "internal instructions"},
            ),
        ]
    )

    assert "✓ email_search — ✉️ 2 messages" in detail
    assert "✓ email_read — ✉️ message read" in detail
    assert "✓ calendar_list — 📅 1 event" in detail
    assert "✓ skill_pack_read — reference loaded" in detail
    assert "Private Sender" not in detail
    assert "Sensitive" not in detail
    assert "msg-1" not in detail
    assert "/skills/private/SKILL.md" not in detail


def test_chat_verbose_command_uses_activity_on_off_per_session(tmp_path, monkeypatch) -> None:
    from nullion.chat_operator import _handle_verbose_command_for_chat, verbose_mode_status_text_for_chat
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.delenv("NULLION_ACTIVITY_TRACE_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_MODE", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_ENABLED", raising=False)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")

    assert _handle_verbose_command_for_chat(runtime, "/verbose status", chat_id="chat-1") == "Verbose mode is off."
    assert _handle_verbose_command_for_chat(runtime, "/verbose off", chat_id="chat-1") == "Verbose mode is off."
    assert task_planner_feed_mode() == "task"
    assert verbose_mode_status_text_for_chat(runtime, chat_id="chat-1") == "off"
    assert _handle_verbose_command_for_chat(runtime, "/verbose on", chat_id="chat-1") == "Verbose mode is on."
    assert task_planner_feed_mode() == "task"
    assert _handle_verbose_command_for_chat(runtime, "/verbose activity", chat_id="chat-1") == "Usage: /verbose [on|off|status]"
    assert _handle_verbose_command_for_chat(runtime, "/verbose planner", chat_id="chat-1") == "Usage: /verbose [on|off|status]"
    assert _handle_verbose_command_for_chat(runtime, "/verbose full", chat_id="chat-1") == "Usage: /verbose [on|off|status]"
    assert _handle_verbose_command_for_chat(runtime, "/verbos off", chat_id="chat-1") is None


def test_telegram_verbose_card_uses_activity_on_off() -> None:
    source = Path("src/nullion/telegram_app.py").read_text(encoding="utf-8")

    assert 'record_id="verbose_on"' in source
    assert 'record_id="verbose_off"' in source
    assert 'record_id="verbose_planner"' not in source
    assert 'record_id="verbose_full"' not in source
    assert "Verbose mode is {status}." in source
    assert 'verbose_head == "/verbose"' in source
    assert '"/verbos"' not in source


def test_telegram_planner_status_requires_streaming_mode(tmp_path, monkeypatch) -> None:
    from nullion.chat_operator import set_chat_streaming_enabled_for_chat
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import _telegram_allows_status_streaming

    monkeypatch.delenv("NULLION_TELEGRAM_CHAT_STREAMING_ENABLED", raising=False)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")

    assert _telegram_allows_status_streaming(runtime, chat_id="chat-1") is True
    set_chat_streaming_enabled_for_chat(runtime, chat_id="chat-1", enabled=False)
    assert _telegram_allows_status_streaming(runtime, chat_id="chat-1") is True
    set_chat_streaming_enabled_for_chat(runtime, chat_id="chat-1", enabled=True)
    assert _telegram_allows_status_streaming(runtime, chat_id="chat-1") is True


def test_telegram_status_delivery_runs_without_existing_event_loop() -> None:
    from nullion.telegram_app import _schedule_or_run_telegram_status_delivery

    calls: list[bool] = []

    async def deliver(use_loop_bound_state: bool) -> None:
        calls.append(use_loop_bound_state)

    assert _schedule_or_run_telegram_status_delivery(deliver) is True
    assert calls == [False]


@pytest.mark.asyncio
async def test_telegram_status_delivery_schedules_on_running_event_loop() -> None:
    from nullion.telegram_app import _schedule_or_run_telegram_status_delivery

    calls: list[bool] = []

    async def deliver(use_loop_bound_state: bool) -> None:
        calls.append(use_loop_bound_state)

    assert _schedule_or_run_telegram_status_delivery(deliver) is True
    await asyncio.sleep(0)
    assert calls == [True]


@pytest.mark.asyncio
async def test_telegram_task_summary_status_delivery_can_confirm_on_running_event_loop() -> None:
    from nullion.telegram_app import _schedule_or_run_telegram_status_delivery

    calls: list[bool] = []

    async def deliver(use_loop_bound_state: bool) -> None:
        calls.append(use_loop_bound_state)

    assert _schedule_or_run_telegram_status_delivery(deliver, confirm=True) is True
    assert calls == [False]


@pytest.mark.asyncio
async def test_telegram_task_summary_status_delivery_confirmation_reports_failure_on_running_loop() -> None:
    from nullion.telegram_app import _schedule_or_run_telegram_status_delivery

    calls: list[bool] = []

    async def deliver(use_loop_bound_state: bool) -> bool:
        calls.append(use_loop_bound_state)
        return False

    assert _schedule_or_run_telegram_status_delivery(deliver, confirm=True) is False
    assert calls == [False]


@pytest.mark.asyncio
async def test_telegram_planner_status_reuses_editable_message() -> None:
    from nullion.telegram_app import _send_or_edit_telegram_status_message

    class Bot:
        def __init__(self) -> None:
            self.sent: list[tuple[str, str, dict]] = []
            self.edited: list[tuple[str, int, str, dict]] = []

        def send_message(self, chat_id, text, **kwargs):
            self.sent.append((chat_id, text, kwargs))
            return SimpleNamespace(message_id=42)

        def edit_message_text(self, *, text, chat_id, message_id, **kwargs):
            self.edited.append((chat_id, message_id, text, kwargs))
            return SimpleNamespace(message_id=message_id)

    bot = Bot()
    status_messages: dict[tuple[str, str], int] = {}

    await _send_or_edit_telegram_status_message(
        bot,
        status_messages,
        chat_id="123",
        group_id="grp-1",
        text="→ Working on 1 task:\n  ☐ Find data",
    )
    await _send_or_edit_telegram_status_message(
        bot,
        status_messages,
        chat_id="123",
        group_id="grp-1",
        text="→ Working on 1 task:\n  ☑ Find data",
    )

    assert len(bot.sent) == 1
    assert status_messages == {("123", "grp-1"): 42}
    assert len(bot.edited) == 1
    assert bot.edited[0][1] == 42
    assert "☑ Find data" in bot.edited[0][2]


@pytest.mark.asyncio
async def test_telegram_task_status_does_not_keep_platform_typing_alive(tmp_path, monkeypatch) -> None:
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import (
        _send_or_edit_telegram_task_status_message,
        _telegram_task_status_has_active_work,
    )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    started: list[tuple[str, str]] = []
    refreshed: list[str] = []
    cancelled: list[str] = []

    async def fake_keepalive(bot_token, *, chat_id, runtime, text):
        del runtime
        started.append((bot_token, chat_id))
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(chat_id)
            raise

    async def fake_refresh(bot_token, *, chat_id, runtime, text):
        del bot_token, runtime, text
        refreshed.append(chat_id)

    monkeypatch.setattr("nullion.telegram_app._run_telegram_chat_typing_keepalive", fake_keepalive)
    monkeypatch.setattr("nullion.telegram_app._send_telegram_chat_typing_indicator_by_token", fake_refresh)

    class Bot:
        def __init__(self) -> None:
            self.sent: list[tuple[str, str, dict]] = []
            self.edited: list[dict] = []

        def send_message(self, chat_id, text, **kwargs):
            self.sent.append((chat_id, text, kwargs))
            return SimpleNamespace(message_id=42)

        def edit_message_text(self, **kwargs):
            self.edited.append(kwargs)
            return SimpleNamespace(message_id=kwargs["message_id"])

    bot = Bot()
    status_messages: dict[tuple[str, str], int] = {}
    typing_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}

    active_text = "→ Working on 1 task:\n  ◐ Find data"
    done_text = "→ Working on 1 task:\n  ☑ Find data"
    terminal_with_activity_text = (
        "Planner: Sequential mission\n"
        "→ Working on 2 tasks:\n"
        "  ✕ Search mail\n"
        "  ✕ Decide whether to report\n\n"
        "Activity\n"
        "✓ Search mail tools\n"
        "  ✓ browser_extract_text"
    )
    assert _telegram_task_status_has_active_work(active_text)
    assert not _telegram_task_status_has_active_work(done_text)
    assert not _telegram_task_status_has_active_work(terminal_with_activity_text)

    await _send_or_edit_telegram_task_status_message(
        bot,
        status_messages,
        chat_id="123",
        group_id="grp-1",
        text=active_text,
        runtime=runtime,
        bot_token="token",
        typing_tasks=typing_tasks,
    )
    await asyncio.sleep(0)
    assert started == []
    assert refreshed == []
    assert typing_tasks == {}

    await _send_or_edit_telegram_task_status_message(
        bot,
        status_messages,
        chat_id="123",
        group_id="grp-1",
        text=done_text,
        runtime=runtime,
        bot_token="token",
        typing_tasks=typing_tasks,
    )
    assert ("123", "grp-1") not in typing_tasks
    assert cancelled == []


def test_telegram_suppresses_planner_ack_only_after_confirmed_status_stream(tmp_path) -> None:
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import (
        DecisionCard,
        _should_skip_telegram_primary_reply_delivery,
        _should_suppress_planner_status_ack,
    )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")

    assert not _should_suppress_planner_status_ack(
        runtime,
        chat_id="123",
        reply="Arbitrary completion text",
        status_stream_delivered=True,
    )

    ack = "Planner status acknowledgment from structured tool results"
    runtime.store.add_conversation_event({
        "conversation_id": "telegram:123",
        "event_type": "conversation.chat_turn",
        "assistant_reply": ack,
        "tool_results": [
            {"tool_name": "Planner", "status": "completed", "output": {"summary": "Sequential Mission"}},
            {
                "tool_name": "Mini-Agents",
                "status": "running",
                "output": {
                    "summary": "Dispatched 2 helper task(s).",
                    "status_delivered": True,
                },
            },
        ],
    })

    assert not _should_suppress_planner_status_ack(
        runtime,
        chat_id="123",
        reply=ack,
        status_stream_delivered=False,
    )
    runtime.store.add_conversation_event({
        "conversation_id": "telegram:123",
        "event_type": "conversation.chat_turn",
        "assistant_reply": f"{ack} confirmed",
        "tool_results": [
            {"tool_name": "Planner", "status": "completed", "output": {"summary": "Sequential Mission"}},
            {
                "tool_name": "Mini-Agents",
                "status": "running",
                "output": {
                    "summary": "Dispatched 2 helper task(s).",
                    "status_delivered": True,
                    "status_delivery_confirmed": True,
                },
            },
        ],
    })
    assert _should_suppress_planner_status_ack(
        runtime,
        chat_id="123",
        reply=f"{ack} confirmed",
        status_stream_delivered=False,
    )
    assert not _should_suppress_planner_status_ack(
        runtime,
        chat_id="123",
        reply="Planner: Sequential Mission • 2 tasks\n→ Working on 2 tasks:\n  ☐ Find\n  ☐ Import",
        status_stream_delivered=False,
    )
    assert _should_skip_telegram_primary_reply_delivery(
        suppress_primary_reply_delivery=True,
        decision_card=None,
    )
    assert not _should_skip_telegram_primary_reply_delivery(
        suppress_primary_reply_delivery=True,
        decision_card=DecisionCard(text="Approval", reply_markup=object()),
    )


def test_telegram_planner_ack_suppression_does_not_parse_reply_text() -> None:
    source = Path("src/nullion/telegram_app.py").read_text(encoding="utf-8")

    assert "_is_planner_status_ack" not in source
    assert 'startswith("Planner:")' not in source
    assert "status_stream_delivered" in source


@pytest.mark.asyncio
async def test_telegram_planner_card_delivery_reports_actual_send_result(tmp_path, monkeypatch) -> None:
    from nullion.runtime import bootstrap_persistent_runtime
    import nullion.telegram_app as telegram_app

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")

    async def fake_send_status(*args, **kwargs):  # noqa: ANN002, ANN003
        return False

    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", fake_send_status)

    delivered = await telegram_app._deliver_telegram_planner_summary_card(
        runtime,
        bot_token="token",
        chat_id="123",
        group_id="grp-card",
        reply="Planner: Sequential Mission • 2 tasks\nFor: Make a file\n→ Working on 2 tasks:\n  ☐ Gather\n  ☐ Create",
        force=True,
    )

    assert delivered is False


@pytest.mark.asyncio
async def test_telegram_final_delivery_is_confirmed_from_running_loop() -> None:
    from nullion.telegram_app import _run_confirmed_telegram_delivery

    delivered: list[str] = []

    async def deliver() -> None:
        await asyncio.sleep(0)
        delivered.append("terminal-result")

    assert _run_confirmed_telegram_delivery(deliver)
    assert delivered == ["terminal-result"]


@pytest.mark.asyncio
async def test_telegram_task_status_delivery_retries_transient_timeout(tmp_path, monkeypatch) -> None:
    from nullion.runtime import bootstrap_persistent_runtime
    import nullion.telegram_app as telegram_app

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    calls: list[str] = []
    sleeps: list[float] = []

    async def flaky_status_send(*args, **kwargs):
        del args, kwargs
        calls.append("send")
        if len(calls) == 1:
            raise RuntimeError("Timed out")

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_status_message", flaky_status_send)
    monkeypatch.setattr(telegram_app.asyncio, "sleep", fake_sleep)

    await telegram_app._send_or_edit_telegram_task_status_message(
        object(),
        {},
        chat_id="123",
        group_id="grp-1",
        text="✓ Finalizing results\n  ☑ Find data",
        runtime=runtime,
        bot_token="token",
    )

    assert calls == ["send", "send"]
    assert sleeps == [0.75]


def test_platform_tool_activity_compaction_dedupes_repeated_details() -> None:
    from nullion.platform_activity import compact_tool_activity_text, render_task_card_text

    detail = compact_tool_activity_text(
        "Tools\n"
        "✓ connector_request — api.maton.ai\n"
        "✓ connector_request — api.maton.ai\n"
        "✓ file_write — deny-pass.txt\n"
        "✓ file_write — deny-pass.txt\n"
        "✓ run_cron — delivery saved",
        label="Tools",
    )

    assert detail == (
        "Tools\n"
        "  ✓ connector_request\n"
        "  ✓ file_write\n"
        "  ✓ run_cron"
    )
    assert render_task_card_text(
        "Planner: Sequential Mission * 4 tasks\nFor: Premarket report\n→ Working on 4 tasks",
        (detail,),
    ) == (
        "🎯 **PLANNER  SEQUENTIAL MISSION**\n\n"
        "For: Premarket report\n"
        "→ Working on 4 tasks\n\n"
        "Activity\n"
        "✓ Tools\n"
        "✓ connector_request\n"
        "✓ file_write\n"
        "✓ run_cron"
    )


def test_platform_planner_label_strips_duplicate_planner_prefix() -> None:
    from nullion.platform_activity import render_task_card_text

    assert render_task_card_text(
        "Planner: Planner Mission * Planning execution\nFor: Daily Pre-Market Stock Research\n→ Working on 1 task",
        (),
    ) == (
        "🎯 **PLANNER  MISSION * PLANNING EXECUTION**\n\n"
        "For: Daily Pre-Market Stock Research\n"
        "→ Working on 1 task"
    )


def test_platform_tool_activity_compaction_keeps_latest_tool_status() -> None:
    from nullion.platform_activity import compact_tool_activity_text

    detail = compact_tool_activity_text(
        "Tools\n"
        "✓ browser_navigate\n"
        "✓ browser_extract_text\n"
        "⊗ browser_run_js — failed\n"
        "✓ browser_run_js\n",
        label="Tools",
    )

    assert detail == (
        "Tools\n"
        "  ✓ browser_navigate\n"
        "  ✓ browser_extract_text\n"
        "  ✓ browser_run_js"
    )


def test_platform_activity_capabilities_cover_live_editable_surfaces(monkeypatch) -> None:
    import nullion.platform_activity as platform_activity
    from nullion.platform_activity import (
        PlatformTaskCardStore,
        platform_activity_capabilities,
        prepare_platform_task_status_update,
        should_deliver_task_status,
    )

    expected = {
        "web": (True, True),
        "telegram": (False, True),
        "slack": (False, True),
        "discord": (False, True),
    }
    for platform, (supports_live_stream, supports_message_edit) in expected.items():
        capabilities = platform_activity_capabilities(platform)
        assert capabilities.supports_task_card is True
        assert capabilities.supports_live_stream is supports_live_stream
        assert capabilities.supports_message_edit is supports_message_edit

        clock = iter([10.0, 12.0, 14.0, 16.0])
        monkeypatch.setattr(platform_activity.time, "monotonic", lambda: next(clock))
        store = PlatformTaskCardStore(capabilities)
        summary = store.update(
            target_id=f"{platform}:target",
            group_id="grp-live-card",
            status_kind="task_summary",
            text="Planner: Parallel Mission * 2 tasks\nFor: Boston day trip from NYC by train\n✓ Running 2 tasks",
        )
        assert summary == "🎯 **PLANNER  PARALLEL MISSION**\n\nFor: Boston day trip from NYC by train\n✓ Running 2 tasks"
        assert store.update(
            target_id=f"{platform}:target",
            group_id="grp-live-card",
            status_kind="tool_activity",
            text="Tools\n✓ browser_extract_text — Boston travel page",
            activity_id="quiet-tools",
            activity_label="Tools",
            include_activity=False,
        ) is None
        activity = store.update(
            target_id=f"{platform}:target",
            group_id="grp-live-card",
            status_kind="tool_activity",
            text="Tools\n✓ list_crons — 9 scheduled tasks\n✓ list_crons — 9 scheduled tasks",
            activity_id="cron-tools",
            activity_label="Tools",
            include_activity=True,
        )
        assert activity == (
            "🎯 **PLANNER  PARALLEL MISSION**\n\n"
            "For: Boston day trip from NYC by train\n"
            "✓ Running 2 tasks\n\n"
            "Activity\n"
            "✓ Tools\n"
            "✓ list_crons"
        )
        task_activity = store.update(
            target_id=f"{platform}:target",
            group_id="grp-live-card",
            status_kind="tool_activity",
            text="Research Boston day trip\n✓ browser_extract_text",
            activity_id="research-tools",
            activity_label="Research Boston day trip tools",
            include_activity=True,
        )
        assert task_activity is not None
        assert task_activity.count("Research Boston day trip") == 1
        assert "browser_extract_text" in task_activity
        assert store.update(
            target_id=f"{platform}:target",
            group_id="",
            status_kind="task_summary",
            text="missing group",
        ) is None
        prepared = prepare_platform_task_status_update(
            target_id=f"{platform}:target",
            group_id="grp-prepared",
            status_kind="task_summary",
            text="Planner: Parallel Mission * 2 tasks\n✓ Prepared status",
            activity_id="",
            activity_label="",
            task_card_store=store,
            planner_feed_enabled=True,
            include_activity=False,
        )
        assert prepared is not None
        assert prepared.key == (f"{platform}:target", "grp-prepared")
        assert prepared.rendered_text == "🎯 **PLANNER  PARALLEL MISSION**\n\n✓ Prepared status"

    assert should_deliver_task_status(
        status_kind="task_summary",
        planner_feed_enabled=True,
        include_activity=False,
    ) is True
    assert should_deliver_task_status(
        status_kind="tool_activity",
        planner_feed_enabled=True,
        include_activity=True,
    ) is True
    assert should_deliver_task_status(
        status_kind="tool_activity",
        planner_feed_enabled=True,
        include_activity=False,
    ) is False
    assert should_deliver_task_status(
        status_kind="task_summary",
        planner_feed_enabled=False,
        include_activity=True,
    ) is True


def test_task_status_subject_is_compact_and_language_neutral() -> None:
    subject = "東京から京都へ新幹線で行く旅行計画を作って、観光地、費用、所要時間、夜の予定も含めてください"

    assert compact_task_status_subject(subject, limit=30) == "東京から京都へ新幹線で行く旅行計画を作って、観光地、費用、..."


def test_cron_execution_registry_uses_structured_plan_tool_scope() -> None:
    from nullion.cron_execution_tools import CronExecutionToolRegistry
    from nullion.tools import (
        ToolInvocation,
        ToolRegistry,
        ToolResult,
        ToolRiskLevel,
        ToolSideEffectClass,
        ToolSpec,
    )

    registry = ToolRegistry()

    def _complete(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {})

    empty_schema = {"type": "object", "properties": {}}
    for spec in (
        ToolSpec("terminal_exec", "Run shell", ToolRiskLevel.HIGH, ToolSideEffectClass.DANGEROUS_EXEC, False, 10, input_schema=empty_schema),
        ToolSpec("connector_request", "Connector request", ToolRiskLevel.HIGH, ToolSideEffectClass.ACCOUNT_WRITE, False, 10, input_schema=empty_schema),
        ToolSpec("run_cron", "Run cron", ToolRiskLevel.MEDIUM, ToolSideEffectClass.WRITE, False, 10, input_schema=empty_schema, capability_tags=("scheduler",)),
    ):
        registry.register(spec, _complete)

    scoped = CronExecutionToolRegistry(registry, allowed_tool_names=("terminal_exec",))

    assert [definition["name"] for definition in scoped.list_tool_definitions()] == ["terminal_exec"]
    assert scoped.invoke(ToolInvocation("inv-ok", "terminal_exec", "web:1", {})).status == "completed"
    denied_unplanned = scoped.invoke(ToolInvocation("inv-connector", "connector_request", "web:1", {}))
    assert denied_unplanned.status == "denied"
    assert denied_unplanned.output["reason"] == "cron_execution_tool_not_in_structured_plan"
    assert denied_unplanned.output["suppress_activity"] is True
    denied_scheduler = scoped.invoke(ToolInvocation("inv-cron", "run_cron", "web:1", {}))
    assert denied_scheduler.status == "denied"
    assert denied_scheduler.output["reason"] == "cron_execution_capability_denied"
    assert denied_scheduler.output["suppress_activity"] is True


@pytest.mark.asyncio
async def test_telegram_planner_status_does_not_duplicate_on_noop_edit() -> None:
    from nullion.telegram_app import _send_or_edit_telegram_status_message

    class Bot:
        def __init__(self) -> None:
            self.sent: list[tuple[str, str, dict]] = []
            self.edit_count = 0

        def send_message(self, chat_id, text, **kwargs):
            self.sent.append((chat_id, text, kwargs))
            return SimpleNamespace(message_id=42)

        def edit_message_text(self, **kwargs):
            self.edit_count += 1
            raise RuntimeError("Message is not modified")

    bot = Bot()
    status_messages: dict[tuple[str, str], int] = {}
    status_texts: dict[tuple[str, str], str] = {}
    text = "→ Working on 1 task:\n  ☐ Find data"

    await _send_or_edit_telegram_status_message(
        bot,
        status_messages,
        chat_id="123",
        group_id="grp-1",
        text=text,
        status_texts=status_texts,
    )
    await _send_or_edit_telegram_status_message(
        bot,
        status_messages,
        chat_id="123",
        group_id="grp-1",
        text=text,
        status_texts=status_texts,
    )
    status_texts.clear()
    await _send_or_edit_telegram_status_message(
        bot,
        status_messages,
        chat_id="123",
        group_id="grp-1",
        text=text,
        status_texts=status_texts,
    )

    assert len(bot.sent) == 1
    assert bot.edit_count == 1
    assert status_messages == {("123", "grp-1"): 42}


@pytest.mark.asyncio
async def test_telegram_planner_status_serializes_concurrent_first_updates() -> None:
    from nullion.telegram_app import _send_or_edit_telegram_status_message

    class Bot:
        def __init__(self) -> None:
            self.sent: list[tuple[str, str, dict]] = []
            self.edited: list[dict] = []

        async def send_message(self, chat_id, text, **kwargs):
            await asyncio.sleep(0.01)
            self.sent.append((chat_id, text, kwargs))
            return SimpleNamespace(message_id=42)

        async def edit_message_text(self, **kwargs):
            self.edited.append(kwargs)
            return SimpleNamespace(message_id=kwargs["message_id"])

    bot = Bot()
    status_messages: dict[tuple[str, str], int] = {}
    status_texts: dict[tuple[str, str], str] = {}
    status_locks: dict[tuple[str, str], asyncio.Lock] = {}

    await asyncio.gather(
        _send_or_edit_telegram_status_message(
            bot,
            status_messages,
            chat_id="123",
            group_id="grp-1",
            text="→ Working on 2 tasks:\n  ◐ Find data\n  ☐ Summarize data",
            status_texts=status_texts,
            status_locks=status_locks,
        ),
        _send_or_edit_telegram_status_message(
            bot,
            status_messages,
            chat_id="123",
            group_id="grp-1",
            text="→ Working on 2 tasks:\n  ◐ Find data\n  ◐ Summarize data",
            status_texts=status_texts,
            status_locks=status_locks,
        ),
        _send_or_edit_telegram_status_message(
            bot,
            status_messages,
            chat_id="123",
            group_id="grp-1",
            text="→ Working on 2 tasks:\n  ☑ Find data\n  ◐ Summarize data",
            status_texts=status_texts,
            status_locks=status_locks,
        ),
    )

    assert len(bot.sent) == 1
    assert len(bot.edited) == 2
    assert status_messages == {("123", "grp-1"): 42}
    assert status_texts[("123", "grp-1")].startswith("→ Working on 2 tasks:")


def test_verbose_mode_documentation_and_website_content_are_current() -> None:
    docs = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            Path("README.md"),
            Path("docs/README.md"),
            Path("docs/support.md"),
            Path("website/index.html"),
            Path("website/docs/index.html"),
        ]
        if path.exists()
    )

    assert "/verbose [on|off|status]" in docs
    assert "/verbose [off|planner|full|status]" not in docs
    assert "/planner-feed" not in docs


def test_skill_auth_policy_documentation_and_website_content_are_current() -> None:
    docs = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            Path("README.md"),
            Path("docs/README.md"),
            Path("docs/plugins.md"),
            Path("docs/skill-packs.md"),
            Path("docs/support.md"),
            Path("website/index.html"),
            Path("website/docs/index.html"),
        ]
        if path.exists()
    )

    assert "auth-required skills" in docs
    assert "instruction-only" in docs
    assert "Settings -> Users -> Connections" in docs
    assert "admin-shared" in docs
    assert "MATON_API_KEY" not in docs
    assert "Maton, Composio, Nango" not in docs


def test_activity_phase_and_env_toggles(monkeypatch) -> None:
    assert classify_run_activity_phase(reply="Tool approval requested: ap-1\nApproval required before Nullion can continue.") is RunActivityPhase.WAITING_APPROVAL
    assert classify_run_activity_phase(reply="Approval required before Nullion can continue.") is RunActivityPhase.WAITING_APPROVAL
    assert classify_run_activity_phase(reply="This task mentioned approval required in ordinary prose.") is RunActivityPhase.ACTIVE
    assert classify_run_activity_phase(reply="plain reply") is RunActivityPhase.ACTIVE

    monkeypatch.delenv("NULLION_ACTIVITY_TRACE_ENABLED", raising=False)
    assert activity_trace_enabled(default=False) is False
    set_activity_trace_enabled(True)
    assert activity_trace_enabled() is True
    assert activity_trace_status_text() == "on"
    set_activity_trace_enabled(False)
    assert activity_trace_status_text() == "off"


def test_task_planner_feed_modes(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_MODE", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_ENABLED", raising=False)
    assert task_planner_feed_mode(default="bad") == "task"
    assert task_planner_feed_enabled(default=False) is True

    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "tasks")
    assert task_planner_feed_mode() == "task"
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "all")
    assert task_planner_feed_status_text() == "task"
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_ENABLED", "no")
    assert task_planner_feed_mode() == "task"

    set_task_planner_feed_enabled(True)
    assert task_planner_feed_mode() == "task"
    set_task_planner_feed_mode("off")
    assert task_planner_feed_enabled() is True
    with pytest.raises(ValueError):
        set_task_planner_feed_mode("maybe")


def test_verbose_mode_maps_activity_and_planner_settings(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_ACTIVITY_TRACE_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_MODE", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_ENABLED", raising=False)

    assert verbose_mode() == "off"
    assert task_planner_feed_mode() == "task"
    set_verbose_mode("off")
    assert activity_trace_enabled() is False
    assert task_planner_feed_mode() == "task"
    assert verbose_mode_status_text() == "off"

    set_verbose_mode("on")
    assert activity_trace_enabled() is True
    assert task_planner_feed_mode() == "task"
    assert verbose_mode() == "on"

    with pytest.raises(ValueError):
        set_verbose_mode("activity")
    with pytest.raises(ValueError):
        set_verbose_mode("planner")
    with pytest.raises(ValueError):
        set_verbose_mode("full")


def test_activity_line_formatters_and_inventory() -> None:
    assert format_activity_sublist_line(" hello ") == "  hello"
    assert format_activity_sublist_line("") == ""
    assert format_activity_detail_lines(["a", "", "b"]) == "  a\n  b"

    class ToolDefinition:
        name = "object_tool"

    registry = SimpleNamespace(list_tool_definitions=lambda: ["zeta", {"function": {"name": "alpha"}}, ToolDefinition(), {"name": ""}])
    assert format_tool_inventory_activity_detail(registry, max_tools=2) == "  Tools: alpha, object_tool, +1 more"
    assert format_tool_inventory_activity_detail(tool_definitions=[], max_tools=2) == "  No registered tools"
    assert format_tool_inventory_activity_detail(SimpleNamespace(list_tool_definitions=lambda: (_ for _ in ()).throw(RuntimeError()))) == "  No registered tools"


def test_tool_activity_details_summarize_status_output_and_safe_untrusted_metadata() -> None:
    results = [
        ToolResult("1", "file_write", "completed", {"path": "/tmp/report.txt"}),
        ToolResult("2", "web_fetch", "denied", {"reason": "approval_required"}),
        ToolResult("3", "shell", "failed", {}, error="command failed"),
        ToolResult("4", "custom", "running", "long text detail"),
        ToolResult("5", "run_cron", "completed", {"cron_delivery_status": "deferred", "task_group_id": "grp-cron"}),
        {"tool_name": "web_search", "status": "completed", "output": {"url": "https://example.com/path", "title": "Example"}},
    ]

    detail = format_tool_activity_detail(results)

    assert "✓ file_write — report.txt" in detail
    assert "⊘ web_fetch — denied" in detail
    assert "⊗ shell — command failed" in detail
    assert "→ custom — long text detail" in detail
    assert "✓ run_cron — background run started" in detail
    assert "web_search" in detail
    assert format_tool_activity_line({"tool_name": "mystery", "status": "weird", "output": {}}) == "  • mystery — weird"


def test_skill_usage_and_mini_agent_details_preserve_structured_titles() -> None:
    assert format_skill_usage_activity_line({"title": "Docs"}) == "  ⧁ Docs"
    assert format_skill_usage_activity_line({}) is None
    assert format_skill_usage_activity_detail(
        ["Docs", {"skill_title": "docs"}, SimpleNamespace(name="Tests"), {"title": ""}],
        limit=None,
    ) == "  ⧁ Docs\n  ⧁ Tests"
    assert format_skill_usage_activity_detail(["A", "B"], limit=0) == ""

    assert format_mini_agent_activity_detail(["and then", "Build tests"]) == "  ☐ and then\n  ☐ Build tests"
    assert format_mini_agent_activity_detail([], task_count=2) == "  ☐ 2 delegated tasks"
    assert format_mini_agent_activity_detail([], task_count=1) == "  ☐ 1 delegated task"
    assert format_mini_agent_activity_detail([], fallback="Recovered title") == "  ☐ Recovered title"
    assert format_mini_agent_activity_detail([]) == ""


def test_run_activity_trace_and_append_reply_cover_tools_skills_mini_agents_and_approval() -> None:
    trace = format_run_activity_trace(
        tool_results=[
            ToolResult("1", "file_read", "completed", {"path": "/tmp/a.txt"}),
            ToolResult("2", "Mini-Agents", "completed", {"tasks": [{"title": "Explore"}, {"title": "and skip"}]}),
        ],
        skill_uses=[{"title": "Coverage"}],
    )

    assert trace.startswith("Activity\n✓ Preparing request")
    assert "✓ Using learned skill" in trace
    assert "✓ Running model and tools" in trace
    assert "✓ Mini-Agents" in trace
    assert "☐ Explore" in trace
    assert "☐ and skip" in trace
    assert "✓ Writing response" in trace

    approval_trace = format_run_activity_trace(suspended_for_approval=True)
    assert "⊘ Running model and tools — approval required" in approval_trace
    assert "→ Waiting for approval" in approval_trace

    assert append_activity_trace_to_reply("Reply", enabled=False, tool_results=[ToolResult("1", "x", "completed", {})]) == "Reply"
    assert append_activity_trace_to_reply("Reply", enabled=True, skill_uses=["Skill"]).startswith("Reply\n\nActivity")
    assert append_activity_trace_to_reply("", enabled=True, suspended_for_approval=True).startswith("Activity")
