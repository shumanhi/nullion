from __future__ import annotations

from nullion.approval_display import (
    approval_display_from_request,
    approval_display_from_tool_result,
    format_approval_detail_markdown,
)
from nullion.approval_markers import (
    is_tool_approval_marker,
    split_tool_approval_marker,
    strip_tool_approval_marker,
)
from nullion.approvals import create_approval_request
from nullion.tools import ToolResult


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


def test_approval_detail_markdown_codes_expected_targets() -> None:
    assert format_approval_detail_markdown("Web request · URL: https://example.com") == (
        "Web request · URL:\n`https://example.com`"
    )
    assert format_approval_detail_markdown("Read file request · Path: /tmp/report.txt") == (
        "Read file request · Path:\n`/tmp/report.txt`"
    )
    assert format_approval_detail_markdown("Command: git status --short") == "Command:\n`git status --short`"
