from __future__ import annotations

from types import SimpleNamespace

from nullion.messaging_delivery_contract import (
    deferred_cron_dispatch_from_event,
    deferred_cron_dispatch_from_tool_results,
    deferred_cron_status_owned_by_background_from_event,
    foreground_deferred_cron_planner_status_from_event,
    foreground_reply_should_be_suppressed,
    task_status_has_active_work,
)


def test_deferred_cron_receipts_emit_running_status_dispatch() -> None:
    tool_result = SimpleNamespace(
        tool_name="run_cron",
        output={
            "delivery_status": "deferred",
            "task_group_id": "manual-cron-daily-report",
            "progress_status_text": (
                "⏰ **SCHEDULED TASK**\n"
                "For: Daily report\n"
                "◐ Running: Manual scheduled task run started.\n"
                "  Result will be delivered to this chat when ready."
            ),
            "result": {
                "text": "Manual scheduled task run started. The result will be delivered when ready.",
            },
        },
    )

    dispatch = deferred_cron_dispatch_from_tool_results([tool_result])

    assert dispatch is not None
    assert dispatch.task_group_id == "manual-cron-daily-report"
    assert "◐ Running" in dispatch.planner_status_text
    assert foreground_deferred_cron_planner_status_from_event({
        "tool_results": [
            {
                "tool_name": "run_cron",
                "output": tool_result.output,
            }
        ]
    }) == ("manual-cron-daily-report", tool_result.output["progress_status_text"])


def test_legacy_background_owned_deferred_cron_without_status_text_still_suppresses_duplicate_foreground() -> None:
    event = {
        "tool_results": [
            {
                "tool_name": "run_cron",
                "output": {
                    "delivery_status": "deferred",
                    "mini_agent_dispatch": True,
                    "task_group_id": "cron-background-owned",
                    "planner_status_owned_by_background": True,
                    "status_delivered": True,
                    "planner_status_text": "⏰ **SCHEDULED TASK**\nFor: Daily report\n◐ Running: Run scheduled task",
                    "result": {
                        "text": "Manual scheduled task run started. The result will be delivered when ready.",
                    },
                },
            }
        ]
    }

    dispatch = deferred_cron_dispatch_from_event(event)

    assert dispatch is not None
    assert deferred_cron_status_owned_by_background_from_event(event) is True
    assert foreground_deferred_cron_planner_status_from_event(event) == (
        "cron-background-owned",
        "⏰ **SCHEDULED TASK**\nFor: Daily report\n◐ Running: Run scheduled task",
    )
    assert not foreground_reply_should_be_suppressed(event["tool_results"])


def test_delivered_deferred_cron_status_suppresses_duplicate_foreground_without_legacy_flag() -> None:
    event = {
        "tool_results": [
            {
                "tool_name": "run_cron",
                "output": {
                    "delivery_status": "deferred",
                    "task_group_id": "cron-status-delivered",
                    "status_delivered": True,
                    "progress_status_text": (
                        "⏰ **SCHEDULED TASK**\n"
                        "For: Daily report\n"
                        "◐ Running: Manual scheduled task run started."
                    ),
                    "result": {
                        "text": "Manual scheduled task run started. The result will be delivered when ready.",
                    },
                },
            }
        ]
    }

    assert deferred_cron_status_owned_by_background_from_event(event) is True


def test_background_owned_deferred_cron_without_status_text_suppresses_foreground_ack() -> None:
    event = {
        "tool_results": [
            {
                "tool_name": "run_cron",
                "output": {
                    "delivery_status": "deferred",
                    "mini_agent_dispatch": True,
                    "task_group_id": "cron-background-not-yet-visible",
                    "planner_status_owned_by_background": True,
                    "status_delivered": False,
                    "result": {
                        "text": "Manual scheduled task run started. The result will be delivered when ready.",
                    },
                },
            }
        ]
    }

    dispatch = deferred_cron_dispatch_from_event(event)

    assert dispatch is not None
    assert deferred_cron_status_owned_by_background_from_event(event) is True
    assert dispatch.planner_status_text == ""
    assert not foreground_reply_should_be_suppressed(event["tool_results"])


def test_foreground_owned_deferred_cron_uses_shared_payload_shape() -> None:
    tool_results = [
        {
            "tool_name": "run_cron",
            "output": {
                "delivery_status": "deferred",
                "mini_agent_dispatch": True,
                "task_group_id": "cron-foreground-owned",
                "progress_status_text": "⏰ **SCHEDULED TASK**\nFor: Daily report\n◐ Running: Manual scheduled task run started.",
                "name": "Daily report",
                "result": {
                    "text": "Manual scheduled task run started. The result will be delivered when ready.",
                },
            },
        }
    ]

    dispatch = deferred_cron_dispatch_from_tool_results(tool_results)
    assert dispatch is not None
    assert dispatch.task_group_id == "cron-foreground-owned"
    assert not foreground_reply_should_be_suppressed(tool_results)


def test_browser_tool_flag_cannot_suppress_foreground_reply() -> None:
    tool_results = [
        {
            "tool_name": "browser_extract_text",
            "output": {
                "foreground_reply_suppressed": True,
                "text": "Visible browser evidence that still needs a user-facing answer.",
            },
        },
        {
            "tool_name": "browser_click_element",
            "output": {
                "foreground_reply_suppressed": True,
                "message": "Clicked the selected result.",
            },
        },
    ]

    assert not foreground_reply_should_be_suppressed(tool_results)


def test_run_cron_flag_can_suppress_duplicate_foreground_reply() -> None:
    tool_results = [
        {
            "tool_name": "run_cron",
            "output": {
                "delivery_status": "sent",
                "foreground_reply_suppressed": True,
                "message": "Delivery sent.",
            },
        }
    ]

    assert foreground_reply_should_be_suppressed(tool_results)


def test_terminal_activity_rows_do_not_keep_platform_typing_alive() -> None:
    active_text = "→ Working on 1 task:\n  ◐ Search mail"
    terminal_with_activity_text = (
        "Planner: Sequential mission\n"
        "→ Working on 2 tasks:\n"
        "  ✕ Search mail\n"
        "  ✕ Evaluate whether to report\n\n"
        "Activity\n"
        "✓ Search mail tools\n"
        "  ✓ file_read"
    )

    assert task_status_has_active_work(active_text)
    assert task_status_has_active_work("⏰ **SCHEDULED TASK**\nFor: Daily report\n  ◑ Running: Daily report")
    assert not task_status_has_active_work(terminal_with_activity_text)
