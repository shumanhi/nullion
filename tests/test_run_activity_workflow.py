from __future__ import annotations

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
from nullion.tools import ToolResult


def test_operator_verbose_command_uses_three_modes(monkeypatch) -> None:
    from nullion.operator_commands import handle_operator_command, operator_command_suggestions

    monkeypatch.delenv("NULLION_ACTIVITY_TRACE_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_MODE", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_ENABLED", raising=False)

    assert ("/verbose [off|planner|full|status]", "Choose activity and planner visibility") in operator_command_suggestions()
    assert handle_operator_command(None, "/verbose status") == "Verbose mode is full."
    assert handle_operator_command(None, "/verbose off") == "Verbose mode is off."
    assert handle_operator_command(None, "/verbose planner") == "Verbose mode is planner."
    assert handle_operator_command(None, "/verbose full") == "Verbose mode is full."
    assert handle_operator_command(None, "/verbose activity") == "Usage: /verbose [off|planner|full|status]"
    assert handle_operator_command(None, "/verbose on") == "Usage: /verbose [off|planner|full|status]"
    assert handle_operator_command(None, "/planner-feed status").startswith("Unknown command.")


def test_chat_verbose_command_uses_three_modes_per_session(tmp_path, monkeypatch) -> None:
    from nullion.chat_operator import _handle_verbose_command_for_chat, verbose_mode_status_text_for_chat
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.delenv("NULLION_ACTIVITY_TRACE_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_MODE", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_ENABLED", raising=False)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")

    assert _handle_verbose_command_for_chat(runtime, "/verbose status", chat_id="chat-1") == "Verbose mode is full."
    assert _handle_verbose_command_for_chat(runtime, "/verbose off", chat_id="chat-1") == "Verbose mode is off."
    assert verbose_mode_status_text_for_chat(runtime, chat_id="chat-1") == "off"
    assert _handle_verbose_command_for_chat(runtime, "/verbose planner", chat_id="chat-1") == "Verbose mode is planner."
    assert _handle_verbose_command_for_chat(runtime, "/verbose full", chat_id="chat-1") == "Verbose mode is full."
    assert _handle_verbose_command_for_chat(runtime, "/verbose activity", chat_id="chat-1") == "Usage: /verbose [off|planner|full|status]"
    assert _handle_verbose_command_for_chat(runtime, "/verbose on", chat_id="chat-1") == "Usage: /verbose [off|planner|full|status]"
    assert _handle_verbose_command_for_chat(runtime, "/verbos off", chat_id="chat-1") is None


def test_telegram_verbose_card_uses_three_modes() -> None:
    source = Path("src/nullion/telegram_app.py").read_text(encoding="utf-8")

    assert 'record_id="verbose_off"' in source
    assert 'record_id="verbose_planner"' in source
    assert 'record_id="verbose_full"' in source
    assert "Verbose mode is {status}." in source
    assert "activity_on" not in source
    assert "activity_off" not in source
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
    assert _telegram_allows_status_streaming(runtime, chat_id="chat-1") is False
    set_chat_streaming_enabled_for_chat(runtime, chat_id="chat-1", enabled=True)
    assert _telegram_allows_status_streaming(runtime, chat_id="chat-1") is True


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


def test_verbose_mode_documentation_and_website_content_are_current() -> None:
    docs = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in [
            "README.md",
            "docs/README.md",
            "docs/support.md",
            "website/index.html",
            "website/docs/index.html",
        ]
    )

    assert "/verbose [off|planner|full|status]" in docs
    assert "Verbose modes" in docs
    assert "/verbose [on|off|status]" not in docs
    assert "/planner-feed" not in docs
    assert "verbose on" not in docs.lower()


def test_skill_auth_policy_documentation_and_website_content_are_current() -> None:
    docs = "\n".join(
        Path(path).read_text(encoding="utf-8")
        for path in [
            "README.md",
            "docs/README.md",
            "docs/plugins.md",
            "docs/skill-packs.md",
            "docs/support.md",
            "website/index.html",
            "website/docs/index.html",
        ]
    )

    assert "auth-required skills" in docs
    assert "instruction-only" in docs
    assert "Settings -> Users -> Connections" in docs
    assert "admin-shared" in docs
    assert "skill_pack_connector_example_skills" in docs
    assert "MATON_API_KEY" not in docs
    assert "Maton, Composio, Nango" not in docs


def test_activity_phase_and_env_toggles(monkeypatch) -> None:
    assert classify_run_activity_phase(reply="Approval required before Nullion can continue.") is RunActivityPhase.WAITING_APPROVAL
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
    assert task_planner_feed_enabled(default=False) is False

    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "tasks")
    assert task_planner_feed_mode() == "task"
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "all")
    assert task_planner_feed_status_text() == "all"
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_ENABLED", "no")
    assert task_planner_feed_mode() == "off"

    set_task_planner_feed_enabled(True)
    assert task_planner_feed_mode() == "task"
    set_task_planner_feed_mode("off")
    assert task_planner_feed_enabled() is False
    with pytest.raises(ValueError):
        set_task_planner_feed_mode("maybe")


def test_verbose_mode_maps_activity_and_planner_settings(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_ACTIVITY_TRACE_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_MODE", raising=False)
    monkeypatch.delenv("NULLION_TASK_PLANNER_FEED_ENABLED", raising=False)

    assert verbose_mode() == "full"
    set_verbose_mode("off")
    assert activity_trace_enabled() is False
    assert task_planner_feed_mode() == "off"
    assert verbose_mode_status_text() == "off"

    set_verbose_mode("planner")
    assert activity_trace_enabled() is False
    assert task_planner_feed_mode() == "task"
    assert verbose_mode() == "planner"

    set_verbose_mode("full")
    assert activity_trace_enabled() is True
    assert task_planner_feed_mode() == "task"
    assert verbose_mode() == "full"

    with pytest.raises(ValueError):
        set_verbose_mode("activity")
    with pytest.raises(ValueError):
        set_verbose_mode("on")


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
        {"tool_name": "web_search", "status": "completed", "output": {"url": "https://example.com/path", "title": "Example"}},
    ]

    detail = format_tool_activity_detail(results)

    assert "✓ file_write — report.txt" in detail
    assert "⊘ web_fetch — denied" in detail
    assert "⊗ shell — command failed" in detail
    assert "→ custom — long text detail" in detail
    assert "web_search" in detail
    assert format_tool_activity_line({"tool_name": "mystery", "status": "weird", "output": {}}) == "  • mystery — weird"


def test_skill_usage_and_mini_agent_details_are_deduped_and_filtered() -> None:
    assert format_skill_usage_activity_line({"title": "Docs"}) == "  ⧁ Docs"
    assert format_skill_usage_activity_line({}) is None
    assert format_skill_usage_activity_detail(
        ["Docs", {"skill_title": "docs"}, SimpleNamespace(name="Tests"), {"title": ""}],
        limit=None,
    ) == "  ⧁ Docs\n  ⧁ Tests"
    assert format_skill_usage_activity_detail(["A", "B"], limit=0) == ""

    assert format_mini_agent_activity_detail(["and then", "Build tests"]) == "  ☐ Build tests"
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
    assert "✓ Writing response" in trace

    approval_trace = format_run_activity_trace(suspended_for_approval=True)
    assert "⊘ Running model and tools — approval required" in approval_trace
    assert "→ Waiting for approval" in approval_trace

    assert append_activity_trace_to_reply("Reply", enabled=False, tool_results=[ToolResult("1", "x", "completed", {})]) == "Reply"
    assert append_activity_trace_to_reply("Reply", enabled=True, skill_uses=["Skill"]).startswith("Reply\n\nActivity")
    assert append_activity_trace_to_reply("", enabled=True, suspended_for_approval=True).startswith("Activity")
