"""Shared Doctor playbook command handling.

This module handles structured Doctor action commands from any surface:
Telegram inline buttons, Web UI buttons, and text-only messaging commands.
It deliberately operates on persisted Doctor action records and command IDs,
not free-form user text.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from functools import lru_cache
from typing import Any, Callable, TypedDict

from langgraph.graph import END, START, StateGraph


_TERMINAL_ACTION_STATUSES = {"completed", "cancelled", "failed", "dismissed", "resolved"}


@dataclass(frozen=True, slots=True)
class DoctorPlaybookResult:
    acknowledgement: str
    message: str
    action: dict[str, str | None]


def _runtime_store(runtime: Any) -> Any:
    return getattr(runtime, "store", runtime)


def _get_doctor_action(runtime: Any, action_id: str) -> dict[str, Any]:
    store = _runtime_store(runtime)
    getter = getattr(store, "get_doctor_action", None)
    if getter is None:
        raise KeyError(action_id)
    action = getter(action_id)
    if action is None:
        raise KeyError(action_id)
    return action


def _reason_fields(action: dict[str, Any]) -> dict[str, str]:
    raw = str(action.get("reason") or action.get("source_reason") or "")
    fields: dict[str, str] = {}
    for part in raw.split(";"):
        key, separator, value = part.partition("=")
        if separator and key.strip():
            fields[key.strip()] = value.strip()
    return fields


def _cancel_related_runtime_work(runtime: Any, action: dict[str, Any], *, source: str) -> list[str]:
    """Best-effort cancellation for structured work referenced by a Doctor action."""
    fields = _reason_fields(action)
    store = _runtime_store(runtime)
    cancelled: list[str] = []

    conversation_id = fields.get("conversation_id")
    if conversation_id and hasattr(store, "get_active_task_frame_id") and hasattr(store, "get_task_frame"):
        frame_id = store.get_active_task_frame_id(conversation_id)
        frame = store.get_task_frame(frame_id) if isinstance(frame_id, str) and frame_id else None
        if frame is not None:
            try:
                from nullion.task_frames import TaskFrameStatus

                store.add_task_frame(
                    replace(
                        frame,
                        status=TaskFrameStatus.CANCELLED,
                        updated_at=_now_utc(),
                    )
                )
                store.set_active_task_frame_id(conversation_id, None)
                cancelled.append(f"task frame {frame.frame_id}")
            except Exception:
                pass

    run_id = fields.get("run_id") or fields.get("mini_agent_run_id") or fields.get("task_id")
    if run_id and hasattr(runtime, "fail_mini_agent_run"):
        try:
            runtime.fail_mini_agent_run(run_id, result_summary=f"Cancelled from {source} via Doctor")
            cancelled.append(f"Mini-Agent run {run_id}")
        except Exception:
            pass

    if cancelled and hasattr(runtime, "checkpoint"):
        try:
            runtime.checkpoint()
        except Exception:
            pass
    return cancelled


def _now_utc() -> Any:
    from datetime import UTC, datetime

    return datetime.now(UTC)


def _start_if_open(runtime: Any, action_id: str) -> dict[str, str | None]:
    action = _get_doctor_action(runtime, action_id)
    status = str(action.get("status") or "").lower()
    if status == "pending":
        return runtime.start_doctor_action(action_id)
    if status in _TERMINAL_ACTION_STATUSES:
        return action
    return action


def _cancel_if_open(runtime: Any, action_id: str, *, reason: str) -> dict[str, str | None]:
    action = _get_doctor_action(runtime, action_id)
    status = str(action.get("status") or "").lower()
    if status in {"pending", "in_progress"}:
        return runtime.cancel_doctor_action(action_id, reason=reason)
    return action


def _format_recent_run(run: Any) -> str:
    run_id = str(getattr(run, "run_id", "") or "")
    status = str(getattr(getattr(run, "status", None), "value", getattr(run, "status", "")) or "")
    agent_type = str(getattr(run, "mini_agent_type", "") or "agent")
    capsule_id = str(getattr(run, "capsule_id", "") or "")
    created_at = getattr(run, "created_at", None)
    created = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or "")
    summary = str(getattr(run, "result_summary", "") or "").strip()
    head = f"- {run_id or '(unknown run)'} [{status or 'unknown'}] {agent_type}"
    details = []
    if capsule_id:
        details.append(f"capsule={capsule_id}")
    if created:
        details.append(f"created={created}")
    if summary:
        details.append(f"summary={summary}")
    return f"{head} ({'; '.join(details)})" if details else head


def _recent_runs(runtime: Any, *, limit: int = 3) -> list[Any]:
    list_runs = getattr(_runtime_store(runtime), "list_mini_agent_runs", None)
    if list_runs is None:
        return []
    try:
        runs = list(list_runs())
    except Exception:
        return []
    return sorted(
        runs,
        key=lambda run: str(getattr(run, "created_at", "") or ""),
        reverse=True,
    )[:limit]


def _doctor_action_command_lines(action_id: str) -> list[str]:
    if not action_id:
        return []
    return [
        f"Inspect again: /doctor run {action_id} doctor:inspect_run",
        f"Cancel if stuck: /doctor run {action_id} doctor:cancel_run",
        f"Retry after cleanup: /doctor run {action_id} doctor:retry_workflow",
    ]


def _doctor_worktree_command_line(action_id: str) -> str | None:
    if not action_id:
        return None
    suffix = action_id.removeprefix("act-")[:12] or "doctor-run"
    return (
        "If this needs code changes, isolate the fix first: "
        f"git worktree add .worktrees/doctor-{suffix} -b codex/doctor-{suffix}"
    )


def _numbered_lines(items: list[str]) -> list[str]:
    return [f"{index}. {item}" for index, item in enumerate(items, start=1)]


def format_doctor_action_inspection(runtime: Any, action: dict[str, Any]) -> str:
    """Render concrete Doctor evidence for chat surfaces."""
    fields = _reason_fields(action)
    action_id = str(action.get("action_id") or "")
    lines = [
        "Doctor run inspection",
        f"ID: {action_id}",
        f"Status: {str(action.get('status') or 'unknown').replace('_', ' ')}",
        f"Severity: {action.get('severity') or 'unknown'}",
        f"Summary: {action.get('summary') or 'Health action'}",
    ]

    evidence_pairs = [
        ("Source", fields.get("source")),
        ("Issue", fields.get("issue_type")),
        ("Conversation", fields.get("conversation_id")),
        ("Principal", fields.get("principal_id")),
        ("Tool count", fields.get("tool_count")),
        ("Threshold", fields.get("soft_threshold")),
        ("Last tool", fields.get("last_tool")),
        ("Last status", fields.get("last_status")),
        ("Stage", fields.get("stage")),
        ("Detail", fields.get("detail") or fields.get("backend_detail") or fields.get("error") or action.get("error")),
        ("Message preview", fields.get("message_preview")),
    ]
    evidence = [(label, str(value).strip()) for label, value in evidence_pairs if str(value or "").strip()]
    if evidence:
        lines.extend(["", "Evidence"])
        lines.extend(f"- {label}: {value}" for label, value in evidence)

    recent_runs = _recent_runs(runtime)
    if recent_runs:
        lines.extend(["", "Recent Mini-Agent runs"])
        lines.extend(_format_recent_run(run) for run in recent_runs)

    next_steps = [
        "If the run is still making progress, leave it running and watch recent activity with /status.",
        *_doctor_action_command_lines(action_id),
    ]
    worktree_line = _doctor_worktree_command_line(action_id)
    if worktree_line is not None:
        next_steps.append(worktree_line)
    lines.extend(["", "Next steps"])
    lines.extend(_numbered_lines(next_steps))
    return "\n".join(lines)


def execute_doctor_playbook_command(
    runtime: Any,
    *,
    action_id: str,
    command: str,
    source_label: str,
    restart_chat_services: Callable[[], str] | None = None,
    signal_current_process_restart: Callable[[], None] | None = None,
) -> DoctorPlaybookResult:
    """Execute a structured Doctor playbook command for any chat/web surface."""
    final_state = _compiled_doctor_playbook_graph().invoke(
        {
            "runtime": runtime,
            "action_id": action_id,
            "command": command,
            "source": source_label.strip() or "operator",
            "restart_chat_services": restart_chat_services,
            "signal_current_process_restart": signal_current_process_restart,
        }
    )
    result = final_state.get("result")
    if isinstance(result, DoctorPlaybookResult):
        return result
    raise RuntimeError("Doctor playbook graph finished without a result")


class _DoctorPlaybookState(TypedDict, total=False):
    runtime: Any
    action_id: str
    command: str
    source: str
    restart_chat_services: Callable[[], str] | None
    signal_current_process_restart: Callable[[], None] | None
    result: DoctorPlaybookResult


def _doctor_route_command(state: _DoctorPlaybookState) -> str:
    command = state["command"]
    if command == "doctor:inspect_run":
        return "inspect"
    if command == "doctor:cancel_run":
        return "cancel_run"
    if command == "doctor:retry_workflow":
        return "retry_workflow"
    if command in {"doctor:restart_bot", "doctor:reconnect_telegram"}:
        return "telegram"
    if command == "doctor:retry_model_api":
        return "retry_model_api"
    if command == "doctor:pause_chat":
        return "pause_chat"
    if command == "doctor:switch_fallback_model":
        return "switch_model"
    if command == "doctor:retry_later":
        return "retry_later"
    if command in {"doctor:reconnect_slack", "doctor:restart_slack_adapter"}:
        return "slack"
    if command in {"doctor:reconnect_discord", "doctor:restart_discord_adapter"}:
        return "discord"
    if command == "doctor:open_schedule":
        return "open_schedule"
    if command == "doctor:disable_task":
        return "disable_task"
    if command == "doctor:review_approvals":
        return "review_approvals"
    if command == "doctor:clear_stale_approvals":
        return "clear_stale_approvals"
    if command == "doctor:run_diagnosis":
        return "run_diagnosis"
    if command == "doctor:create_backup":
        return "create_backup"
    if command == "doctor:repair_checkpoint":
        return "repair_checkpoint"
    if command in {"doctor:restart_plugin", "doctor:disable_plugin"}:
        return "plugin"
    return "unknown"


def _doctor_inspect_node(state: _DoctorPlaybookState) -> dict[str, object]:
    updated = _start_if_open(state["runtime"], state["action_id"])
    return {
        "result": DoctorPlaybookResult(
            "Inspect run",
            format_doctor_action_inspection(state["runtime"], updated),
            updated,
        )
    }


def _doctor_cancel_run_node(state: _DoctorPlaybookState) -> dict[str, object]:
    current = _get_doctor_action(state["runtime"], state["action_id"])
    cancelled_work = _cancel_related_runtime_work(state["runtime"], current, source=state["source"])
    updated = _cancel_if_open(
        state["runtime"],
        state["action_id"],
        reason=f"Run cancellation requested from {state['source']}",
    )
    detail = (
        f" Cleared {', '.join(cancelled_work)}."
        if cancelled_work
        else " No active runtime work was found for this Doctor item."
    )
    return {"result": DoctorPlaybookResult("Cancel noted", f"Marked the Doctor item cancelled.{detail}", updated)}


def _doctor_retry_workflow_node(state: _DoctorPlaybookState) -> dict[str, object]:
    updated = _start_if_open(state["runtime"], state["action_id"])
    return {
        "result": DoctorPlaybookResult(
            "Retry workflow",
            "Retry the workflow only after confirming the previous run is stopped or no longer making changes.",
            updated,
        )
    }


def _doctor_telegram_node(state: _DoctorPlaybookState) -> dict[str, object]:
    runtime = state["runtime"]
    action_id = state["action_id"]
    command = state["command"]
    source = state["source"]
    if state.get("restart_chat_services") is not None:
        message = state["restart_chat_services"]()
        updated = _cancel_if_open(runtime, action_id, reason=f"{command} triggered from {source}")
        acknowledgement = "Restarting" if command == "doctor:restart_bot" else "Reconnecting"
        return {"result": DoctorPlaybookResult(acknowledgement, message, updated)}
    if state.get("signal_current_process_restart") is not None:
        state["signal_current_process_restart"]()
        updated = _cancel_if_open(runtime, action_id, reason=f"{command} triggered from {source}")
        message = "Restarting the bot now. It will be back online shortly."
        if command == "doctor:reconnect_telegram":
            message = "Reconnecting to Telegram now."
        acknowledgement = "Restarting" if command == "doctor:restart_bot" else "Reconnecting"
        return {"result": DoctorPlaybookResult(acknowledgement, message, updated)}
    updated = _start_if_open(runtime, action_id)
    return {
        "result": DoctorPlaybookResult(
            "Telegram action needed",
            "Restart or reconnect the Telegram adapter from the host service manager, then send a Telegram test message.",
            updated,
        )
    }


def _doctor_retry_model_api_node(state: _DoctorPlaybookState) -> dict[str, object]:
    source = state["source"]
    updated = _cancel_if_open(state["runtime"], state["action_id"], reason=f"Retry requested from {source}")
    message = (
        "👍 Will retry the model API on your next message."
        if source.lower() == "telegram"
        else "Will retry the model API on your next message."
        if source.lower() == "operator command"
        else "Will retry the model API on the next request."
    )
    return {"result": DoctorPlaybookResult("Retrying", message, updated)}


def _doctor_static_node(acknowledgement: str, message: str, *, cancel_reason: str | None = None):
    def _node(state: _DoctorPlaybookState) -> dict[str, object]:
        if cancel_reason is None:
            updated = _start_if_open(state["runtime"], state["action_id"])
        else:
            updated = _cancel_if_open(state["runtime"], state["action_id"], reason=cancel_reason.format(**state))
        return {"result": DoctorPlaybookResult(acknowledgement, message, updated)}

    return _node


def _doctor_run_diagnosis_node(state: _DoctorPlaybookState) -> dict[str, object]:
    from nullion.runtime import format_doctor_diagnosis_for_operator

    report = state["runtime"].diagnose_runtime_health()
    updated = _start_if_open(state["runtime"], state["action_id"])
    return {"result": DoctorPlaybookResult("Diagnosis complete", format_doctor_diagnosis_for_operator(report), updated)}


def _doctor_plugin_node(state: _DoctorPlaybookState) -> dict[str, object]:
    updated = _cancel_if_open(
        state["runtime"],
        state["action_id"],
        reason=f"{state['command']} triggered from {state['source']}",
    )
    return {"result": DoctorPlaybookResult("Acknowledged", "Action noted.", updated)}


def _doctor_unknown_node(state: _DoctorPlaybookState) -> dict[str, object]:
    updated = _cancel_if_open(
        state["runtime"],
        state["action_id"],
        reason=f"Unknown command from {state['source']}: {state['command']}",
    )
    return {"result": DoctorPlaybookResult("Dismissed", "Action dismissed.", updated)}


@lru_cache(maxsize=1)
def _compiled_doctor_playbook_graph():
    graph = StateGraph(_DoctorPlaybookState)
    graph.add_node("inspect", _doctor_inspect_node)
    graph.add_node("cancel_run", _doctor_cancel_run_node)
    graph.add_node("retry_workflow", _doctor_retry_workflow_node)
    graph.add_node("telegram", _doctor_telegram_node)
    graph.add_node("retry_model_api", _doctor_retry_model_api_node)
    graph.add_node(
        "pause_chat",
        _doctor_static_node(
            "Paused",
            "Chat paused. Send any message to resume.",
            cancel_reason="Chat paused from {source}",
        ),
    )
    graph.add_node(
        "switch_model",
        _doctor_static_node(
            "Model switch needed",
            "Choose a configured fallback provider in Models, then retry the request.",
        ),
    )
    graph.add_node(
        "retry_later",
        _doctor_static_node(
            "Retry later",
            "Retry this request after provider quota or availability recovers.",
            cancel_reason="Retry later requested from {source}",
        ),
    )
    graph.add_node(
        "slack",
        _doctor_static_node(
            "Slack action needed",
            "Restart or reconnect the Slack adapter from the host service manager, then send a Slack test message.",
        ),
    )
    graph.add_node(
        "discord",
        _doctor_static_node(
            "Discord action needed",
            "Restart or reconnect the Discord adapter from the host service manager, then send a Discord test message.",
        ),
    )
    graph.add_node(
        "open_schedule",
        _doctor_static_node(
            "Open schedule",
            "Open the Scheduled Tasks panel and select a valid capsule for the affected task.",
        ),
    )
    graph.add_node(
        "disable_task",
        _doctor_static_node(
            "Disable task",
            "Open the Scheduled Tasks panel and disable the affected task if its capsule is gone.",
        ),
    )
    graph.add_node(
        "review_approvals",
        _doctor_static_node(
            "Review approvals",
            "Open /approvals or the web Approvals panel and resolve pending requests.",
        ),
    )
    graph.add_node(
        "clear_stale_approvals",
        _doctor_static_node(
            "Clear stale approvals",
            "Review old approvals and deny the ones that no longer correspond to active work.",
        ),
    )
    graph.add_node("run_diagnosis", _doctor_run_diagnosis_node)
    graph.add_node(
        "create_backup",
        _doctor_static_node(
            "Create backup",
            "Create a runtime backup before attempting storage or checkpoint repair.",
        ),
    )
    graph.add_node(
        "repair_checkpoint",
        _doctor_static_node(
            "Repair checkpoint",
            "Restore or repair the runtime checkpoint from the Backups panel after creating a fresh backup.",
        ),
    )
    graph.add_node("plugin", _doctor_plugin_node)
    graph.add_node("unknown", _doctor_unknown_node)
    graph.add_conditional_edges(
        START,
        _doctor_route_command,
        {
            "inspect": "inspect",
            "cancel_run": "cancel_run",
            "retry_workflow": "retry_workflow",
            "telegram": "telegram",
            "retry_model_api": "retry_model_api",
            "pause_chat": "pause_chat",
            "switch_model": "switch_model",
            "retry_later": "retry_later",
            "slack": "slack",
            "discord": "discord",
            "open_schedule": "open_schedule",
            "disable_task": "disable_task",
            "review_approvals": "review_approvals",
            "clear_stale_approvals": "clear_stale_approvals",
            "run_diagnosis": "run_diagnosis",
            "create_backup": "create_backup",
            "repair_checkpoint": "repair_checkpoint",
            "plugin": "plugin",
            "unknown": "unknown",
        },
    )
    for node in (
        "inspect",
        "cancel_run",
        "retry_workflow",
        "telegram",
        "retry_model_api",
        "pause_chat",
        "switch_model",
        "retry_later",
        "slack",
        "discord",
        "open_schedule",
        "disable_task",
        "review_approvals",
        "clear_stale_approvals",
        "run_diagnosis",
        "create_backup",
        "repair_checkpoint",
        "plugin",
        "unknown",
    ):
        graph.add_edge(node, END)
    return graph.compile()


__all__ = [
    "DoctorPlaybookResult",
    "execute_doctor_playbook_command",
    "format_doctor_action_inspection",
]
