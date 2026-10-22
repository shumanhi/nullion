from __future__ import annotations

from nullion.approval_display import (
    approval_display_from_request,
    approval_display_from_tool_result,
    approval_turn_display_from_result,
    format_approval_detail_markdown,
)
from nullion.approval_markers import (
    is_tool_approval_marker,
    split_tool_approval_marker,
    strip_tool_approval_marker,
)
from nullion.approvals import TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND, create_approval_request
from nullion.tools import ToolResult


def test_agent_turn_limit_extension_approval_is_doctor_card() -> None:
    from nullion.agent_turn_limits import (
        AGENT_TURN_LIMIT_EXTENSION_ACTION,
        AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
        build_agent_turn_limit_extension_context,
    )
    from nullion.telegram_app import _approval_card_actions

    approval = create_approval_request(
        requested_by="telegram:123",
        action=AGENT_TURN_LIMIT_EXTENSION_ACTION,
        resource="telegram:123",
        request_kind=AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
        context=build_agent_turn_limit_extension_context(
            current_max_iterations=12,
            tool_result_count=41,
            conversation_id="telegram:123",
            requested_extensions=("xlsx",),
        ),
    )

    display = approval_display_from_request(approval)
    actions = _approval_card_actions(approval)

    assert display.title == "🩺 Doctor recommends more tool budget"
    assert display.is_web_request is False
    assert "Source: Telegram" in display.detail
    assert "Tool result records so far (including recovery): 41" in display.detail
    assert [action for _label, action in actions] == [
        "extend_limit_2x",
        "extend_limit_5x",
        "extend_limit_10x",
        "deny",
    ]


def test_web_agent_turn_limit_extension_approval_shows_source() -> None:
    from nullion.agent_turn_limits import (
        AGENT_TURN_LIMIT_EXTENSION_ACTION,
        AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
        build_agent_turn_limit_extension_context,
    )

    approval = create_approval_request(
        requested_by="web:probe",
        action=AGENT_TURN_LIMIT_EXTENSION_ACTION,
        resource="web:probe",
        request_kind=AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
        context=build_agent_turn_limit_extension_context(
            current_max_iterations=24,
            tool_result_count=30,
            conversation_id="web:probe",
        ),
    )

    display = approval_display_from_request(approval)

    assert display.title == "🩺 Doctor recommends more tool budget"
    assert display.is_web_request is True
    assert "Source: Web" in display.detail
    assert "Current turn budget: 24" in display.detail


def test_web_boundary_approval_display_uses_web_copy_and_target() -> None:
    approval = create_approval_request(
        requested_by="telegram:123",
        action="allow_boundary",
        resource="https://www.bing.com/*",
        request_kind="boundary_policy",
        context={
            "tool_name": "web_search",
            "boundary_kind": "outbound_network",
            "target": "https://www.bing.com/*",
        },
    )

    display = approval_display_from_request(approval)

    assert display.title == "🛡️ Allow web access?"
    assert display.is_web_request is True
    assert display.detail == "Web request · URL: https://www.bing.com/*"
    assert "external sites" in display.copy


def test_browser_run_js_tool_approval_is_not_web_domain_scope() -> None:
    approval = create_approval_request(
        requested_by="web:operator",
        action="use_tool",
        resource="browser_run_js",
        context={
            "tool_name": "browser_run_js",
            "tool_description": "Execute JavaScript on the current page and return the result.",
            "tool_risk_level": "high",
            "tool_side_effect_class": "write",
        },
    )

    display = approval_display_from_request(approval)

    assert display.title == "⚠️ Approve write action?"
    assert display.is_web_request is False
    assert "external sites" not in display.copy
    assert "Execute JavaScript" in display.detail


def test_browser_run_js_tool_result_is_not_web_domain_scope() -> None:
    result = ToolResult(
        invocation_id="inv-js",
        tool_name="browser_run_js",
        status="needs_approval",
        output={"reason": "approval_required"},
    )

    display = approval_display_from_tool_result(result, approval_id="approval-123")

    assert display.title == "⚠️ Approve write action?"
    assert display.is_web_request is False
    assert "external sites" not in display.copy


def test_telegram_web_approval_actions_name_all_web_domains() -> None:
    from nullion.telegram_app import _approval_card_actions

    approval = create_approval_request(
        requested_by="telegram:123",
        action="allow_boundary",
        resource="https://www.bing.com/*",
        request_kind="boundary_policy",
        context={
            "tool_name": "web_fetch",
            "boundary_kind": "outbound_network",
            "target": "https://www.bing.com/*",
        },
    )

    labels = [label for label, _action in _approval_card_actions(approval)]

    assert labels[0] == "Allow all web domains"
    assert "Allow globally" not in labels


def test_calendar_create_approval_display_uses_event_arguments() -> None:
    from nullion.telegram_app import _approval_card_actions, _approval_card_text

    approval = create_approval_request(
        requested_by="telegram:123",
        action="use_tool",
        resource="calendar_create",
        context={
            "tool_name": "calendar_create",
            "tool_description": "Create a Google Calendar event through an active write-capable Google Calendar connector.",
            "tool_risk_level": "high",
            "tool_side_effect_class": "account_write",
            "tool_arguments": {
                "calendar_id": "primary",
                "description": "Horse Meat Disco NYC Pride. Suggested start time based on prior details: 10:00 PM.",
                "end": "2026-06-27T00:00:00",
                "location": "The Ruins at Knockdown Center, Maspeth, Queens",
                "provider_id": "custom_connector_provider",
                "send_updates": "none",
                "start": "2026-06-26T22:00:00",
                "summary": "Horse Meat Disco NYC Pride",
                "time_zone": "America/New_York",
            },
        },
    )

    display = approval_display_from_request(approval)
    card = _approval_card_text(approval)

    assert display.title == "📅 Add this calendar event?"
    assert display.detail.startswith("Calendar event:")
    assert "> Title: Horse Meat Disco NYC Pride" in card
    assert "> When: Jun 26, 2026, 10:00 PM to Jun 27, 2026, 12:00 AM (America/New_York)" in card
    assert "> Where: The Ruins at Knockdown Center, Maspeth, Queens" in card
    assert "> Calendar: primary" in card
    assert "write-capable" not in card
    assert "calendar_list" not in card
    assert "account_write" not in card
    assert [label for label, _action in _approval_card_actions(approval)] == ["Add event", "Deny"]


def test_filesystem_and_account_approval_display_choose_specific_titles() -> None:
    file_approval = create_approval_request(
        requested_by="web:operator",
        action="allow_boundary",
        resource="/tmp/report.txt",
        request_kind="boundary_policy",
        context={"tool_name": "file_read", "boundary_kind": "filesystem_access", "operation": "read", "path": "/tmp/report.txt"},
    )
    account_approval = create_approval_request(
        requested_by="web:operator",
        action="allow_boundary",
        resource="acme",
        request_kind="boundary_policy",
        context={"tool_name": "connector_request", "boundary_kind": "account_access", "target": "acme"},
    )

    assert approval_display_from_request(file_approval).title == "📄 Allow file access?"
    assert approval_display_from_request(account_approval).title == "🔐 Allow account access?"


def test_tool_approval_marker_round_trip_keeps_remainder_without_false_positives() -> None:
    text = "Tool approval requested: approval-123\nContinue after approval."

    marker = split_tool_approval_marker(text)

    assert marker is not None
    assert marker.approval_id == "approval-123"
    assert marker.remainder == "Continue after approval."
    assert strip_tool_approval_marker(text) == "Continue after approval."
    assert is_tool_approval_marker(text)
    assert not is_tool_approval_marker("User asked for tool approval requested docs")


def test_tool_result_display_identifies_web_target() -> None:
    result = ToolResult(
        invocation_id="inv-1",
        tool_name="web_fetch",
        status="needs_approval",
        output={"url": "https://example.com"},
    )

    display = approval_display_from_tool_result(result, approval_id="approval-123")

    assert display.title == "🛡️ Allow web access?"
    assert display.is_web_request is True
    assert display.detail == "https://example.com"


def test_approval_turn_display_prefers_stored_approval_context() -> None:
    approval = create_approval_request(
        requested_by="web:operator",
        action="allow_boundary",
        resource="https://example.com/*",
        request_kind="boundary_policy",
        context={
            "tool_name": "web_fetch",
            "boundary_kind": "outbound_network",
            "target": "https://example.com/*",
        },
    )
    result = type("Result", (), {"approval_id": approval.approval_id, "tool_results": []})()
    runtime = type("Runtime", (), {"store": type("Store", (), {"get_approval_request": lambda self, approval_id: approval})()})()

    display = approval_turn_display_from_result(runtime, result, trigger_label_for_approval=lambda req: "Manual approval")

    assert display.label == "fetch a web page"
    assert display.detail == "Web request · URL: https://example.com/*"
    assert display.trigger_flow_label == "Manual approval"
    assert display.is_web_request is True


def test_approval_turn_display_falls_back_to_tool_result() -> None:
    result = type(
        "Result",
        (),
        {
            "approval_id": "approval-123",
            "tool_results": [
                ToolResult(
                    invocation_id="inv-web",
                    tool_name="web_fetch",
                    status="denied",
                    output={"url": "https://example.com"},
                )
            ],
        },
    )()
    runtime = type("Runtime", (), {"store": type("Store", (), {"get_approval_request": lambda self, approval_id: None})()})()

    display = approval_turn_display_from_result(runtime, result, trigger_label_for_approval=lambda req: "ignored")

    assert display.label == "fetch a web page"
    assert display.detail == "https://example.com"
    assert display.trigger_flow_label is None
    assert display.is_web_request is True


def test_tool_result_display_uses_boundary_target_when_request_row_is_missing() -> None:
    result = ToolResult(
        invocation_id="inv-1",
        tool_name="web_fetch",
        status="denied",
        output={
            "reason": "approval_required",
            "approval_id": "approval-123",
            "boundary_kind": "outbound_network",
            "target": "https://www.google.com/search?q=nullion",
        },
    )

    display = approval_display_from_tool_result(result, approval_id="approval-123")

    assert display.title == "🛡️ Allow web access?"
    assert display.detail == "https://www.google.com/search?q=nullion"
    assert display.is_web_request is True


def test_terminal_approval_display_shows_command_not_internal_placeholder() -> None:
    result = ToolResult(
        invocation_id="inv-1",
        tool_name="terminal_exec",
        status="needs_approval",
        output={"reason": "approval_required", "arguments": {"command": "git status --short"}},
    )

    display = approval_display_from_tool_result(result, approval_id="approval-123")

    assert display.title == "💻 Run this command?"
    assert display.detail == "Command: git status --short"
    assert "approval_required" not in display.detail


def test_terminal_approval_display_calls_out_missing_command_detail() -> None:
    result = ToolResult(
        invocation_id="inv-1",
        tool_name="terminal_exec",
        status="needs_approval",
        output={"reason": "approval_required"},
    )

    display = approval_display_from_tool_result(result, approval_id="approval-123")

    assert display.detail == "Command details were not provided by the runtime."


def test_terminal_destructive_approval_display_lists_exact_delete_targets() -> None:
    from nullion.telegram_app import _approval_card_actions

    approval = create_approval_request(
        requested_by="telegram:123",
        action="use_tool",
        resource="terminal_exec",
        request_kind=TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND,
        context={
            "tool_name": "terminal_exec",
            "command": "rm -f /workspace/runtime.db.corrupt-20260610",
            "destructive_target_count": 1,
            "destructive_targets": [{"target": "/workspace/runtime.db.corrupt-20260610"}],
        },
    )

    display = approval_display_from_request(approval)
    actions = _approval_card_actions(approval)

    assert display.title == "⚠️ Confirm delete?"
    assert display.label == "delete files with terminal_exec"
    assert "Review exactly what will be deleted" in display.copy
    assert "Command:\n`rm -f /workspace/runtime.db.corrupt-20260610`" in format_approval_detail_markdown(display.detail)
    assert "About to delete 1 path" in display.detail
    assert "- /workspace/runtime.db.corrupt-20260610" in display.detail
    assert actions == (("Delete", "allow_once"), ("Deny", "deny"))


def test_approval_detail_markdown_codes_expected_targets() -> None:
    assert format_approval_detail_markdown("Web request · URL: https://example.com") == (
        "Web request · URL:\n`https://example.com`"
    )
    assert format_approval_detail_markdown("Read file request · Path: /tmp/report.txt") == (
        "Read file request · Path:\n`/tmp/report.txt`"
    )
    assert format_approval_detail_markdown("Command: git status --short") == "Command:\n`git status --short`"
