"""Shared delivery/status rules for chat platform adapters.

Platform adapters decide how to send, edit, or suppress messages. This module
owns the cross-platform delivery rules that must stay identical for Web,
Telegram, Slack, Discord, and future chat surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


ACTIVE_TASK_STATUS_PREFIXES = ("☐", "◑", "◒", "◐", "◓", "▤")


@dataclass(frozen=True, slots=True)
class DeferredCronDispatch:
    task_group_id: str
    planner_status_text: str
    should_suppress_foreground_reply: bool = True


def foreground_reply_should_be_suppressed(tool_results: Iterable[object]) -> bool:
    """Return the shared foreground-reply suppression decision for chat surfaces."""
    for tool_result in tool_results or ():
        output = _tool_output(tool_result)
        if isinstance(output, dict) and _first_bool((output,), ("foreground_reply_suppressed",)):
            return True
    return False


def apply_deferred_cron_dispatch_to_payload(
    payload: dict[str, object],
    dispatch: object | None,
) -> dict[str, object]:
    """Attach a deferred cron status card to a platform payload when available."""
    if isinstance(dispatch, DeferredCronDispatch):
        payload["mini_agent_dispatch"] = True
        payload["task_group_id"] = dispatch.task_group_id
        payload["planner_status_text"] = dispatch.planner_status_text
        payload["progress_status_text"] = dispatch.planner_status_text
        payload["planner_status_owned_by_background"] = True
    return payload


def mark_background_owned_deferred_cron_output(output: dict[str, object]) -> dict[str, object]:
    """Compatibility hook for old callers; new cron runs should not use it."""
    output["foreground_reply_suppressed"] = True
    return output


def task_status_has_active_work(text: str) -> bool:
    """Return true only for active checklist rows in the planner section."""
    in_activity_section = False
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.upper().startswith("ACTIVITY"):
            in_activity_section = True
            continue
        # Activity rows are historical tool evidence. They must not keep a
        # platform typing indicator alive after the planner checklist is done.
        if in_activity_section:
            continue
        if line.startswith(ACTIVE_TASK_STATUS_PREFIXES):
            return True
    return False


def deferred_cron_dispatch_from_event(event: dict[str, object] | None) -> DeferredCronDispatch | None:
    """Return the foreground-owned status dispatch for a deferred cron turn."""
    status = foreground_deferred_cron_planner_status_from_event(event)
    if status is None:
        return None
    group_id, text = status
    return DeferredCronDispatch(task_group_id=group_id, planner_status_text=text)


def deferred_cron_status_owned_by_background_from_event(event: dict[str, object] | None) -> bool:
    """Return true when a background cron runner already owns the status card."""
    if not isinstance(event, dict):
        return False
    tool_results = event.get("tool_results")
    if not isinstance(tool_results, list):
        return False
    for result in tool_results:
        if not isinstance(result, dict) or result.get("tool_name") != "run_cron":
            continue
        output = result.get("output")
        if isinstance(output, dict) and _first_bool((output,), ("planner_status_owned_by_background",)):
            return True
    return False


def foreground_deferred_cron_planner_status_from_event(
    event: dict[str, object] | None,
) -> tuple[str, str] | None:
    """Return a visible running-status card for a deferred cron tool result."""
    if not isinstance(event, dict):
        return None
    tool_results = event.get("tool_results")
    if not isinstance(tool_results, list):
        return None
    for result in tool_results:
        if not isinstance(result, dict) or result.get("tool_name") != "run_cron":
            continue
        output = result.get("output")
        status = _deferred_cron_status_from_output(output)
        if status is not None:
            return status
    return None


def deferred_cron_dispatch_from_tool_results(tool_results: Iterable[object]) -> DeferredCronDispatch | None:
    """Return a deferred cron status dispatch from tool results."""
    event = {
        "tool_results": [
            {
                "tool_name": getattr(tool_result, "tool_name", None)
                if not isinstance(tool_result, dict)
                else tool_result.get("tool_name"),
                "output": _tool_output(tool_result),
            }
            for tool_result in (tool_results or ())
        ]
    }
    return deferred_cron_dispatch_from_event(event)


def _deferred_cron_status_from_output(output: object) -> tuple[str, str] | None:
    if not isinstance(output, dict):
        return None
    delivery_status = str(output.get("delivery_status") or output.get("cron_delivery_status") or "").strip()
    if delivery_status != "deferred":
        return None
    group_id = str(output.get("task_group_id") or "").strip()
    status_text = str(output.get("progress_status_text") or output.get("planner_status_text") or "").strip()
    if group_id and status_text:
        return group_id, status_text
    if group_id and output.get("planner_status_owned_by_background") is True:
        return group_id, ""
    deferred_results = [
        item
        for item in (output.get("results") or ())
        if isinstance(item, dict)
        and str(item.get("delivery_status") or item.get("cron_delivery_status") or "").strip() == "deferred"
    ]
    if not deferred_results:
        return None
    group_id = str(output.get("task_group_id") or "manual-cron-batch").strip()
    rows = []
    for item in deferred_results[:12]:
        label = str(item.get("name") or item.get("id") or "scheduled task").strip()
        if label:
            rows.append(f"◐ Running: {label}")
    if not rows:
        return None
    status_text = "\n".join(
        [
            "❖ SCHEDULED TASKS",
            f"For: {len(deferred_results)} manual scheduled task run(s)",
            *rows,
            "  Results will be delivered to the configured destinations when ready.",
        ]
    )
    return group_id, status_text


def _tool_output(tool_result: object) -> object:
    if isinstance(tool_result, dict):
        return tool_result.get("output")
    return getattr(tool_result, "output", None)


def _first_bool(sources: tuple[object, ...], keys: tuple[str, ...]) -> bool:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in keys:
            value = source.get(key)
            if value is True or value == 1:
                return True
            if isinstance(value, str) and value.strip().lower() == "true":
                return True
    return False
