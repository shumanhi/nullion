"""Read-model helpers for lightweight runtime status snapshots."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from nullion.approval_context import approval_trigger_flow_label
from nullion.approval_display import approval_display_from_request
from nullion.approvals import ApprovalRequest, ApprovalStatus, PermissionGrant, is_permission_grant_active
from nullion.doctor_actions import PENDING
from nullion.intent import IntentCapsule, IntentState
from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus
from nullion.mission_status import build_mission_snapshot
from nullion.missions import MissionRecord
from nullion.progress import ProgressUpdate
from nullion.scheduler import ScheduledTask
from nullion.runtime_persistence import (
    get_latest_runtime_restore_metadata,
    list_runtime_store_backups,
)
from nullion.runtime_store import RuntimeStore
from nullion.sentinel_escalations import EscalationStatus, SentinelEscalationArtifact


_ACTIVE_CAPSULE_STATES = {"running", "waiting_approval", "blocked", "pending"}
_ACTIVE_DOCTOR_ACTION_STATUSES = {"pending", "in_progress"}
_ACTIVE_MINI_AGENT_RUN_STATUSES = {"pending", "running"}
_OPEN_SENTINEL_ESCALATION_STATUSES = {"escalated", "acknowledged"}
_ACTIVE_MISSION_STATUSES = {"pending", "running", "waiting_approval", "waiting_user", "blocked", "verifying"}

_CAPSULE_STATE_PRIORITY = {
    "waiting_approval": 0,
    "blocked": 1,
    "running": 2,
    "pending": 3,
    "completed": 4,
    "failed": 5,
}
_DOCTOR_STATUS_PRIORITY = {
    "in_progress": 0,
    "pending": 1,
    "completed": 2,
    "cancelled": 3,
    "failed": 4,
}
_MINI_AGENT_STATUS_PRIORITY = {
    "running": 0,
    "pending": 1,
    "failed": 2,
    "completed": 3,
}
_SENTINEL_STATUS_PRIORITY = {
    "escalated": 0,
    "acknowledged": 1,
    "resolved": 2,
}
_MISSION_STATUS_PRIORITY = {
    "running": 0,
    "pending": 1,
    "completed": 2,
    "failed": 3,
    "cancelled": 4,
}
_SEVERITY_PRIORITY = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
    "normal": 4,
}
_RISK_PRIORITY = {
    "critical": 0,
    "high": 1,
    "normal": 2,
    "low": 3,
}
_APPROVAL_STATUS_PRIORITY = {
    "pending": 0,
    "approved": 1,
    "denied": 2,
}
_GRANT_STATE_PRIORITY = {
    "active": 0,
    "expired": 1,
    "revoked": 2,
}

_DENSE_STATUS_APPROVAL_LIMIT = 3
_DENSE_STATUS_GRANT_LIMIT = 3



def _latest_progress_by_capsule(store: RuntimeStore) -> dict[str, ProgressUpdate]:
    latest: dict[str, ProgressUpdate] = {}
    for update in store.list_progress_updates():
        latest[update.capsule_id] = update
    return latest



def _capsule_summary(
    capsule: IntentCapsule,
    *,
    latest_progress: ProgressUpdate | None,
) -> dict[str, object]:
    return {
        "capsule_id": capsule.capsule_id,
        "owner": capsule.owner,
        "goal": capsule.goal,
        "state": capsule.state.value,
        "risk_level": capsule.risk_level,
        "active_mini_agents": list(capsule.active_mini_agents),
        "pending_approval_id": capsule.pending_approval_id,
        "latest_progress": None
        if latest_progress is None
        else {
            "state": latest_progress.state.value,
            "message": latest_progress.message,
        },
    }



def _doctor_action_summary(action: dict[str, object]) -> dict[str, object]:
    return {
        "action_id": action["action_id"],
        "status": action["status"],
        "action_type": action["action_type"],
        "recommendation_code": action["recommendation_code"],
        "severity": action["severity"],
        "summary": action["summary"],
        "created_at": action.get("created_at"),
        "updated_at": action.get("updated_at"),
    }



def _mini_agent_run_summary(run: MiniAgentRun) -> dict[str, object]:
    return {
        "run_id": run.run_id,
        "capsule_id": run.capsule_id,
        "mini_agent_type": run.mini_agent_type,
        "status": run.status.value,
        "result_summary": run.result_summary,
    }



def _scheduled_task_summary(task: ScheduledTask) -> dict[str, object]:
    return {
        "task_id": task.task_id,
        "capsule_id": task.capsule_id,
        "schedule_kind": task.schedule_kind.value,
        "interval_minutes": task.interval_minutes,
        "enabled": task.enabled,
        "last_run_at": None if task.last_run_at is None else task.last_run_at.isoformat(),
        "failure_count": task.failure_count,
    }



def _scheduled_task_sort_key(task: dict[str, object]) -> tuple[object, ...]:
    return (
        0 if bool(task.get("enabled", False)) else 1,
        str(task.get("capsule_id", "")),
        int(task.get("interval_minutes", 0)),
        str(task.get("task_id", "")),
    )



def _sentinel_escalation_summary(escalation: SentinelEscalationArtifact) -> dict[str, object]:
    return {
        "escalation_id": escalation.escalation_id,
        "status": escalation.status.value,
        "severity": escalation.severity,
        "approval_id": escalation.approval_id,
        "summary": escalation.summary,
    }



def _approval_request_summary(approval: ApprovalRequest) -> dict[str, object]:
    display = approval_display_from_request(approval)
    return {
        "approval_id": approval.approval_id,
        "requested_by": approval.requested_by,
        "action": approval.action,
        "resource": approval.resource,
        "status": approval.status.value,
        "display_label": display.label,
        "display_detail": display.detail,
        "display_title": display.title,
        "trigger_flow_label": approval_trigger_flow_label(approval),
    }



def _permission_grant_state(grant: PermissionGrant, *, now: datetime | None = None) -> str:
    check_time = now or datetime.now(UTC)
    if grant.revoked_at is not None and grant.revoked_at <= check_time:
        return "revoked"
    if grant.expires_at is not None and grant.expires_at <= check_time:
        return "expired"
    return "active"



def _permission_grant_summary(grant: PermissionGrant) -> dict[str, object]:
    return {
        "grant_id": grant.grant_id,
        "approval_id": grant.approval_id,
        "principal_id": grant.principal_id,
        "permission": grant.permission,
        "granted_by": grant.granted_by,
        "is_active": is_permission_grant_active(grant),
        "grant_state": _permission_grant_state(grant),
    }



def _conversation_summary(
    store: RuntimeStore,
    *,
    conversation_id: str,
) -> dict[str, object]:
    head = store.get_conversation_head(conversation_id) or {}
    events = store.list_conversation_events(conversation_id)
    latest_event_type = events[-1]["event_type"] if events else None
    active_task_frame_id = store.get_active_task_frame_id(conversation_id)
    active_task_frame = (
        store.get_task_frame(active_task_frame_id)
        if isinstance(active_task_frame_id, str)
        else None
    )
    output_summary = None
    blocking_condition = None
    target_summary = None
    if active_task_frame is not None:
        if active_task_frame.output.artifact_kind and active_task_frame.output.delivery_mode:
            output_summary = f"{active_task_frame.output.artifact_kind} via {active_task_frame.output.delivery_mode}"
        target_summary = active_task_frame.target.normalized_value if active_task_frame.target is not None else None
        if active_task_frame.status.value == "waiting_approval":
            blocking_condition = "approval required"
        elif active_task_frame.status.value == "waiting_input":
            blocking_condition = "waiting input"
        elif active_task_frame.status.value == "failed":
            blocking_condition = "failed"
    return {
        "conversation_id": conversation_id,
        "active_branch_id": head.get("active_branch_id"),
        "active_turn_id": head.get("active_turn_id"),
        "recent_event_count": len(events),
        "latest_event_type": latest_event_type,
        "active_task_frame_id": active_task_frame.frame_id if active_task_frame is not None else None,
        "active_task_frame_status": active_task_frame.status.value if active_task_frame is not None else None,
        "active_task_frame_summary": active_task_frame.summary if active_task_frame is not None else None,
        "active_task_frame_target": target_summary,
        "active_task_frame_output": output_summary,
        "active_task_frame_blocking_condition": blocking_condition,
    }



def _label_for_count(count: int, singular: str, plural: str | None = None) -> str:
    if count == 1:
        return singular
    return plural or f"{singular}s"



def _humanize_label(value: object) -> str:
    normalized = str(value)
    if normalized == "waiting_approval":
        return "approval required"
    return normalized.replace("_", " ")



def _approval_pressure_counts(
    *,
    capsules: list[dict[str, object]],
    missions: list[dict[str, object]],
    approval_requests: list[dict[str, object]],
) -> dict[str, int]:
    approval_required_capsules = sum(1 for capsule in capsules if str(capsule.get("state")) == "waiting_approval")
    approval_required_missions = sum(1 for mission in missions if str(mission.get("status")) == "waiting_approval")
    pending_approval_requests = sum(
        1
        for approval in approval_requests
        if str(approval.get("status")) == ApprovalStatus.PENDING.value
    )
    return {
        "approval_required_capsules": approval_required_capsules,
        "approval_required_missions": approval_required_missions,
        "pending_approval_requests": pending_approval_requests,
        "total": approval_required_capsules + approval_required_missions + pending_approval_requests,
    }



def compute_approval_pressure(
    snapshot: dict[str, object],
    *,
    active_only: bool = False,
) -> dict[str, int]:
    capsules = list(snapshot.get("capsules", []))
    missions = list(snapshot.get("missions", []))
    approval_requests = list(snapshot.get("approval_requests", []))
    if active_only:
        capsules = [capsule for capsule in capsules if str(capsule.get("state")) in _ACTIVE_CAPSULE_STATES]
        missions = [mission for mission in missions if str(mission.get("status")) in _ACTIVE_MISSION_STATUSES]
        approval_requests = [
            approval
            for approval in approval_requests
            if str(approval.get("status")) == ApprovalStatus.PENDING.value
        ]
    return _approval_pressure_counts(
        capsules=capsules,
        missions=missions,
        approval_requests=approval_requests,
    )



def _mission_phase_label(mission: dict[str, object]) -> str:
    phase = str(mission.get("execution_phase") or "").strip()
    if phase:
        return phase

    status = str(mission.get("status") or "")
    if status in {"completed", "failed", "cancelled"}:
        return "terminal"
    if status in {"waiting_approval", "waiting_user", "blocked"}:
        return "waiting"
    if status == "verifying":
        return "verifying"
    if status == "running":
        return "executing"
    if status == "pending":
        return "planning"
    return "planning"



def _capsule_sort_key(capsule: dict[str, object]) -> tuple[object, ...]:
    return (
        _CAPSULE_STATE_PRIORITY.get(str(capsule["state"]), 99),
        _RISK_PRIORITY.get(str(capsule["risk_level"]), 99),
        str(capsule["goal"]),
        str(capsule["capsule_id"]),
    )



def _doctor_action_sort_key(action: dict[str, object]) -> tuple[object, ...]:
    return (
        _DOCTOR_STATUS_PRIORITY.get(str(action["status"]), 99),
        _SEVERITY_PRIORITY.get(str(action["severity"]), 99),
        str(action["summary"]),
        str(action["action_id"]),
    )



def _mini_agent_run_sort_key(run: dict[str, object]) -> tuple[object, ...]:
    return (
        _MINI_AGENT_STATUS_PRIORITY.get(str(run["status"]), 99),
        str(run["mini_agent_type"]),
        str(run["capsule_id"]),
        str(run["run_id"]),
    )


def _timestamp_text(value: object) -> str:
    return str(value or "").strip()


def _event_timestamp_text(item: dict[str, object]) -> str:
    return _timestamp_text(item.get("updated_at")) or _timestamp_text(item.get("created_at"))



def _sentinel_escalation_sort_key(escalation: dict[str, object]) -> tuple[object, ...]:
    return (
        _SEVERITY_PRIORITY.get(str(escalation["severity"]), 99),
        _SENTINEL_STATUS_PRIORITY.get(str(escalation["status"]), 99),
        str(escalation["summary"]),
        str(escalation["escalation_id"]),
    )



def _mission_sort_key(mission: dict[str, object]) -> tuple[object, ...]:
    return (
        _MISSION_STATUS_PRIORITY.get(str(mission["status"]), 99),
        str(mission["title"]),
        str(mission["mission_id"]),
    )



def _mission_checklist_items(mission: MissionRecord) -> list[dict[str, object]]:
    return [
        {
            "item_id": item.item_id,
            "label": item.label,
            "required": item.required,
            "satisfied": item.satisfied,
            "details": item.details,
        }
        for item in mission.completion_checklist
    ]



def _mission_summary_line(mission: dict[str, object]) -> str:
    line = f"- {mission['title']} [{_humanize_label(mission['status'])}]"

    details: list[str] = []
    total_steps = int(mission.get("total_steps", 0))
    completed_steps = int(mission.get("completed_steps", 0))
    checklist = dict(mission.get("checklist", {}))
    checklist_required = int(checklist.get("required", 0))
    checklist_satisfied = int(checklist.get("satisfied", 0))
    if total_steps > 0 or checklist_required > 0:
        details.append(
            f"Progress: {completed_steps}/{total_steps} steps • {checklist_satisfied}/{checklist_required} checklist"
        )

    phase = _mission_phase_label(mission)
    if phase:
        details.append(f"Phase: {phase}")

    active_step_index = mission.get("active_step_index")
    active_step = mission.get("active_step")
    execution_role = mission.get("execution_role")
    if (
        isinstance(execution_role, str)
        and execution_role
        and execution_role != "terminal"
        and (total_steps > 0 or active_step)
    ):
        details.append(f"Role: {_humanize_label(execution_role)}")

    if isinstance(active_step_index, int) and active_step and total_steps > 0:
        details.append(f"Step {active_step_index + 1}/{total_steps}: {active_step}")

    active_mini_agent_runs = int(mission.get("active_mini_agent_runs", 0))
    if active_mini_agent_runs > 0:
        details.append(f"Mini-Agents: {active_mini_agent_runs}")

    required_step_run_ids = [
        str(run_id)
        for run_id in mission.get("required_step_run_ids", [])
        if isinstance(run_id, str) and run_id.strip()
    ]
    active_step_run_ids = [
        str(run_id)
        for run_id in mission.get("active_step_run_ids", [])
        if isinstance(run_id, str) and run_id.strip()
    ]
    if required_step_run_ids and (
        len(active_step_run_ids) > len(required_step_run_ids) or active_step_run_ids != required_step_run_ids
    ):
        details.append(f"Required runs: {', '.join(required_step_run_ids)}")

    if len(active_step_run_ids) > 1:
        details.append(f"Runs: {', '.join(active_step_run_ids)}")

    checklist_items = [dict(item) for item in mission.get("checklist_items", []) if isinstance(item, dict)]
    first_unmet_required_item = next(
        (
            item
            for item in checklist_items
            if bool(item.get("required")) and not bool(item.get("satisfied"))
        ),
        None,
    )
    if first_unmet_required_item is not None:
        checklist_label = str(first_unmet_required_item.get("label") or "").strip()
        checklist_details = str(first_unmet_required_item.get("details") or "").strip()
        if checklist_label:
            if checklist_details and checklist_details != checklist_label:
                details.append(f"Checklist: {checklist_label} — {checklist_details}")
            else:
                details.append(f"Checklist: {checklist_label}")

    waiting_on = mission.get("waiting_on")
    blocked_reason = mission.get("blocked_reason")
    result_summary = mission.get("result_summary")
    last_progress_message = mission.get("last_progress_message")
    if waiting_on:
        details.append(f"Waiting on: {waiting_on}")
    elif blocked_reason:
        details.append(f"Blocked: {blocked_reason}")
    elif result_summary:
        details.append(f"Result: {result_summary}")
    elif last_progress_message:
        details.append(f"Next: {last_progress_message}")

    if not details:
        return line
    return f"{line} — {' • '.join(details)}"



def _approval_request_sort_key(approval: dict[str, object]) -> tuple[object, ...]:
    return (
        _APPROVAL_STATUS_PRIORITY.get(str(approval["status"]), 99),
        str(approval["requested_by"]),
        str(approval["action"]),
        str(approval["approval_id"]),
    )



def _permission_grant_sort_key(grant: dict[str, object]) -> tuple[object, ...]:
    state = str(grant.get("grant_state", "active" if bool(grant.get("is_active", True)) else "inactive"))
    return (
        _GRANT_STATE_PRIORITY.get(state, 99),
        str(grant["principal_id"]),
        str(grant["permission"]),
        str(grant["grant_id"]),
    )



def _checkpoint_summary(store: RuntimeStore, checkpoint_path: str | Path | None) -> dict[str, object] | None:
    if checkpoint_path is None:
        return None

    checkpoint = Path(checkpoint_path)
    available_backups = []
    if checkpoint.exists():
        available_backups = [backup["name"] for backup in list_runtime_store_backups(checkpoint)]
    latest_restore = get_latest_runtime_restore_metadata(store)

    return {
        "path": str(checkpoint),
        "available_backups": available_backups,
        "latest_restore": latest_restore,
    }



def build_runtime_status_snapshot(
    store: RuntimeStore,
    *,
    capsule_id: str | None = None,
    checkpoint_path: str | Path | None = None,
) -> dict[str, object]:
    capsules = store.list_capsules() if hasattr(store, "list_capsules") else list(store.capsules.values())
    if capsule_id is not None:
        capsules = [capsule for capsule in capsules if capsule.capsule_id == capsule_id]

    capsule_ids = {capsule.capsule_id for capsule in capsules}
    latest_progress = _latest_progress_by_capsule(store)
    capsule_summaries = [
        _capsule_summary(capsule, latest_progress=latest_progress.get(capsule.capsule_id))
        for capsule in capsules
    ]
    capsule_summaries = sorted(capsule_summaries, key=_capsule_sort_key)

    mini_agent_runs = store.list_mini_agent_runs()
    if capsule_id is not None:
        mini_agent_runs = [run for run in mini_agent_runs if run.capsule_id in capsule_ids]

    scheduled_tasks = store.list_scheduled_tasks()
    if capsule_id is not None:
        scheduled_tasks = [task for task in scheduled_tasks if task.capsule_id == capsule_id]

    doctor_actions = store.list_doctor_actions()
    sentinel_escalations = store.list_sentinel_escalations()
    approval_requests = store.list_approval_requests()
    permission_grants = store.list_permission_grants()
    missions = store.list_missions()
    if capsule_id is not None:
        missions = [
            mission
            for mission in missions
            if mission.active_capsule_id == capsule_id or mission.created_from_capsule_id == capsule_id or mission.mission_id == capsule_id
        ]

    doctor_action_summaries = sorted(
        [_doctor_action_summary(action) for action in doctor_actions],
        key=_doctor_action_sort_key,
    )
    mini_agent_run_summaries = sorted(
        [_mini_agent_run_summary(run) for run in mini_agent_runs],
        key=_mini_agent_run_sort_key,
    )
    scheduled_task_summaries = sorted(
        [_scheduled_task_summary(task) for task in scheduled_tasks],
        key=_scheduled_task_sort_key,
    )
    sentinel_escalation_summaries = sorted(
        [_sentinel_escalation_summary(escalation) for escalation in sentinel_escalations],
        key=_sentinel_escalation_sort_key,
    )
    approval_request_summaries = sorted(
        [_approval_request_summary(approval) for approval in approval_requests],
        key=_approval_request_sort_key,
    )
    permission_grant_summaries = sorted(
        [_permission_grant_summary(grant) for grant in permission_grants],
        key=_permission_grant_sort_key,
    )

    mission_summaries = sorted(
        [{**build_mission_snapshot(mission), "checklist_items": _mission_checklist_items(mission)} for mission in missions],
        key=_mission_sort_key,
    )

    conversation_ids = {
        *store.conversation_heads.keys(),
        *(branch.conversation_id for branch in store.conversation_branches.values()),
        *(turn.conversation_id for turn in store.conversation_turns.values()),
        *(str(event.get("conversation_id")) for event in store.list_conversation_events()),
    }
    conversation_summaries = [
        _conversation_summary(store, conversation_id=conversation_id)
        for conversation_id in sorted(conversation_ids)
        if conversation_id
    ]

    return {
        "counts": {
            "capsules": len(capsules),
            "running_capsules": sum(1 for capsule in capsules if capsule.state is IntentState.RUNNING),
            "waiting_approval_capsules": sum(
                1 for capsule in capsules if capsule.state is IntentState.WAITING_APPROVAL
            ),
            "active_mini_agent_runs": sum(
                1
                for run in mini_agent_runs
                if run.status in {MiniAgentRunStatus.PENDING, MiniAgentRunStatus.RUNNING}
            ),
            "pending_doctor_actions": sum(1 for action in doctor_actions if action["status"] == PENDING),
            "open_sentinel_escalations": sum(
                1
                for escalation in sentinel_escalations
                if escalation.status is not EscalationStatus.RESOLVED
            ),
        },
        "capsules": capsule_summaries,
        "doctor_actions": doctor_action_summaries,
        "mini_agent_runs": mini_agent_run_summaries,
        **({"scheduled_tasks": scheduled_task_summaries} if scheduled_task_summaries else {}),
        "sentinel_escalations": sentinel_escalation_summaries,
        "approval_requests": approval_request_summaries,
        "permission_grants": permission_grant_summaries,
        **({"missions": mission_summaries} if mission_summaries else {}),
        **({"conversations": conversation_summaries} if conversation_summaries else {}),
        "checkpoint": _checkpoint_summary(store, checkpoint_path),
    }



def format_runtime_status_for_telegram(
    snapshot: dict[str, object],
    *,
    active_only: bool = False,
) -> str:
    counts = snapshot["counts"]
    capsules = list(snapshot["capsules"])
    mini_agent_runs = list(snapshot["mini_agent_runs"])
    scheduled_tasks = list(snapshot.get("scheduled_tasks", []))
    sentinel_escalations = list(snapshot["sentinel_escalations"])
    doctor_actions = list(snapshot["doctor_actions"])
    missions = list(snapshot.get("missions", []))
    conversations = list(snapshot.get("conversations", []))
    approval_requests = list(snapshot.get("approval_requests", []))
    permission_grants = list(snapshot.get("permission_grants", []))
    checkpoint = snapshot.get("checkpoint")

    if active_only:
        capsules = [capsule for capsule in capsules if capsule["state"] in _ACTIVE_CAPSULE_STATES]
        mini_agent_runs = [
            run for run in mini_agent_runs if run["status"] in _ACTIVE_MINI_AGENT_RUN_STATUSES
        ]
        scheduled_tasks = [task for task in scheduled_tasks if bool(task.get("enabled", False))]
        missions = [mission for mission in missions if str(mission["status"]) in _ACTIVE_MISSION_STATUSES]
        sentinel_escalations = [
            escalation
            for escalation in sentinel_escalations
            if escalation["status"] in _OPEN_SENTINEL_ESCALATION_STATUSES
        ]
        doctor_actions = [
            action for action in doctor_actions if action["status"] in _ACTIVE_DOCTOR_ACTION_STATUSES
        ]
        approval_requests = [
            approval
            for approval in approval_requests
            if approval["status"] == ApprovalStatus.PENDING.value
        ]
        permission_grants = [grant for grant in permission_grants if grant.get("grant_state", "active") == "active"]

    capsules = sorted(capsules, key=_capsule_sort_key)
    mini_agent_runs = sorted(mini_agent_runs, key=_mini_agent_run_sort_key)
    scheduled_tasks = sorted(scheduled_tasks, key=_scheduled_task_sort_key)
    missions = sorted(missions, key=_mission_sort_key)
    sentinel_escalations = sorted(sentinel_escalations, key=_sentinel_escalation_sort_key)
    doctor_actions = sorted(doctor_actions, key=_doctor_action_sort_key)
    approval_requests = sorted(approval_requests, key=_approval_request_sort_key)
    permission_grants = sorted(permission_grants, key=_permission_grant_sort_key)
    conversations = sorted(conversations, key=lambda conversation: str(conversation.get("conversation_id", "")))

    blocked_capsule_count = sum(1 for capsule in capsules if capsule["state"] == "blocked")
    blocked_mission_count = sum(1 for mission in missions if str(mission.get("status")) == "blocked")
    approval_pressure = compute_approval_pressure(snapshot, active_only=active_only)
    pending_approval_request_count = approval_pressure["pending_approval_requests"]

    summary_capsule_count = counts["capsules"]
    summary_running_capsule_count = counts["running_capsules"]
    summary_approval_required_count = approval_pressure["total"]
    summary_active_mini_agent_count = counts["active_mini_agent_runs"]
    summary_open_sentinel_count = counts["open_sentinel_escalations"]
    summary_pending_doctor_action_count = counts["pending_doctor_actions"]
    if active_only:
        summary_capsule_count = len(capsules)
        summary_running_capsule_count = sum(1 for capsule in capsules if capsule["state"] == "running")
        summary_approval_required_count = approval_pressure["total"]
        summary_active_mini_agent_count = len(mini_agent_runs)
        summary_open_sentinel_count = len(sentinel_escalations)
        summary_pending_doctor_action_count = len(doctor_actions)

    lines = [
        "📌 Nullion status",
        "",
        (
            f"✅ {summary_capsule_count} {_label_for_count(summary_capsule_count, 'capsule')}"
            f" • {summary_running_capsule_count} running"
            f" • {summary_approval_required_count} approval required"
        ),
        (
            f"✅ {summary_active_mini_agent_count} active {_label_for_count(summary_active_mini_agent_count, 'Mini-Agent')}"
            f" • {summary_open_sentinel_count} open {_label_for_count(summary_open_sentinel_count, 'Sentinel escalation')}"
            f" • {summary_pending_doctor_action_count} pending {_label_for_count(summary_pending_doctor_action_count, 'Doctor action')}"
        ),
    ]
    if blocked_capsule_count > 0 or blocked_mission_count > 0:
        lines.append(
            (
                f"🚫 {blocked_capsule_count} blocked {_label_for_count(blocked_capsule_count, 'capsule')}"
                f" • {blocked_mission_count} blocked {_label_for_count(blocked_mission_count, 'mission')}"
            )
        )

    if (
        not capsules
        and not missions
        and not conversations
        and not mini_agent_runs
        and not scheduled_tasks
        and not sentinel_escalations
        and not doctor_actions
        and not approval_requests
        and not permission_grants
    ):
        lines.extend(["", "No active work right now."])
    else:
        if capsules:
            lines.extend(["", "Capsules"])
            for capsule in capsules:
                latest_progress = capsule["latest_progress"]
                progress_text = latest_progress["message"] if latest_progress is not None else "No progress yet."
                lines.append(
                    f"- {capsule['owner']}: {capsule['goal']} [{_humanize_label(capsule['state'])}] — {progress_text}"
                )

        if conversations:
            lines.extend(["", "Conversations"])
            for conversation in conversations:
                conversation_id = str(conversation.get("conversation_id", ""))
                active_branch_id = conversation.get("active_branch_id") or "none"
                active_turn_id = conversation.get("active_turn_id") or "none"
                recent_event_count = int(conversation.get("recent_event_count", 0))
                latest_event_type = conversation.get("latest_event_type") or "none"
                line = (
                    f"- {conversation_id}: branch={active_branch_id} • turn={active_turn_id} • "
                    f"events={recent_event_count} • latest={latest_event_type}"
                )
                active_task_frame_id = conversation.get("active_task_frame_id")
                if isinstance(active_task_frame_id, str) and active_task_frame_id:
                    frame_status = _humanize_label(conversation.get("active_task_frame_status") or "active")
                    line += f" • frame={active_task_frame_id} [{frame_status}]"
                    target_summary = conversation.get("active_task_frame_target")
                    if isinstance(target_summary, str) and target_summary:
                        line += f" • target={target_summary}"
                    output_summary = conversation.get("active_task_frame_output")
                    if isinstance(output_summary, str) and output_summary:
                        line += f" • output={output_summary}"
                    blocking_condition = conversation.get("active_task_frame_blocking_condition")
                    if isinstance(blocking_condition, str) and blocking_condition:
                        line += f" • blocking={blocking_condition}"
                    frame_summary = conversation.get("active_task_frame_summary")
                    if isinstance(frame_summary, str) and frame_summary:
                        line += f" • summary={frame_summary}"
                lines.append(line)

        if missions:
            lines.extend(["", "Missions"])
            for mission in missions:
                lines.append(_mission_summary_line(mission))

        if mini_agent_runs:
            lines.extend(["", "Mini-Agents"])
            for run in mini_agent_runs:
                lines.append(f"- {run['mini_agent_type']} on {run['capsule_id']} [{_humanize_label(run['status'])}]")

        if scheduled_tasks:
            lines.extend(["", "Scheduler"])
            for task in scheduled_tasks:
                enabled_label = "enabled" if bool(task.get("enabled", False)) else "disabled"
                schedule_kind = str(task.get("schedule_kind") or "unknown")
                interval_minutes = int(task.get("interval_minutes", 0))
                last_run_at = task.get("last_run_at") or "never"
                failure_count = int(task.get("failure_count", 0))
                lines.append(
                    f"- {task['capsule_id']}: {schedule_kind} every {interval_minutes}m [{enabled_label}] • "
                    f"last run: {last_run_at} • failures: {failure_count}"
                )

        if sentinel_escalations:
            lines.extend(["", "Sentinel"])
            for escalation in sentinel_escalations:
                lines.append(
                    f"- {_humanize_label(escalation['severity'])} [{_humanize_label(escalation['status'])}]: {escalation['summary']}"
                )

        if doctor_actions:
            lines.extend(["", "Doctor"])
            for action in doctor_actions:
                timestamp = _event_timestamp_text(action)
                timestamp_suffix = f" • updated: {timestamp}" if timestamp else ""
                lines.append(
                    f"- {_humanize_label(action['severity'])} {_humanize_label(action['action_type'])} "
                    f"[{_humanize_label(action['status'])}]: {action['summary']}{timestamp_suffix}"
                )

        if approval_requests:
            dense_approvals = len(approval_requests) > _DENSE_STATUS_APPROVAL_LIMIT
            pending_count = sum(1 for approval in approval_requests if approval["status"] == "pending")
            approved_count = sum(1 for approval in approval_requests if approval["status"] == "approved")
            denied_count = sum(1 for approval in approval_requests if approval["status"] == "denied")
            if dense_approvals:
                lines.extend(
                    [
                        "",
                        (
                            f"Approvals (showing {_DENSE_STATUS_APPROVAL_LIMIT}/{len(approval_requests)}"
                            f" • {pending_count} pending"
                            f" • {approved_count} approved"
                            f" • {denied_count} denied)"
                        ),
                    ]
                )
                shown_approvals = approval_requests[:_DENSE_STATUS_APPROVAL_LIMIT]
            else:
                lines.extend(["", "Approvals"])
                shown_approvals = approval_requests
            for approval in shown_approvals:
                trigger = approval.get("trigger_flow_label")
                trigger_suffix = f" · triggered by {trigger}" if trigger else ""
                lines.append(
                    f"- {approval['approval_id']}: {approval['requested_by']} requested {approval['action']} on {approval['resource']} [{_humanize_label(approval['status'])}]{trigger_suffix}"
                )
            if dense_approvals:
                remaining_approvals = len(approval_requests) - len(shown_approvals)
                label = "approval request" if remaining_approvals == 1 else "approval requests"
                lines.append(f"... and {remaining_approvals} more {label}.")

        if permission_grants:
            dense_grants = len(permission_grants) > _DENSE_STATUS_GRANT_LIMIT
            active_count = 0
            expired_count = 0
            revoked_count = 0
            for grant in permission_grants:
                state = grant.get("grant_state")
                if state is None:
                    state = "active" if bool(grant.get("is_active", True)) else "inactive"
                if state == "active":
                    active_count += 1
                elif state == "expired":
                    expired_count += 1
                elif state == "revoked":
                    revoked_count += 1
            if dense_grants:
                lines.extend(
                    [
                        "",
                        (
                            f"Grants (showing {_DENSE_STATUS_GRANT_LIMIT}/{len(permission_grants)}"
                            f" • {active_count} active"
                            f" • {expired_count} expired"
                            f" • {revoked_count} revoked)"
                        ),
                    ]
                )
                shown_grants = permission_grants[:_DENSE_STATUS_GRANT_LIMIT]
            else:
                lines.extend(["", "Grants"])
                shown_grants = permission_grants
            for grant in shown_grants:
                state = grant.get("grant_state")
                if state is None:
                    state = "active" if bool(grant.get("is_active", True)) else "inactive"
                lines.append(
                    f"- {grant['principal_id']} → {grant['permission']} (from {grant['approval_id']}) [{_humanize_label(state)}]"
                )
            if dense_grants:
                remaining_grants = len(permission_grants) - len(shown_grants)
                label = "permission grant" if remaining_grants == 1 else "permission grants"
                lines.append(f"... and {remaining_grants} more {label}.")

    if checkpoint is not None:
        checkpoint_name = Path(str(checkpoint["path"])).name
        backup_names = ", ".join(checkpoint["available_backups"]) if checkpoint["available_backups"] else "none"
        lines.extend(["", "Recovery", f"- Checkpoint: {checkpoint_name}", f"- Backups: {backup_names}"])
        latest_restore = checkpoint.get("latest_restore")
        if latest_restore is not None:
            lines.append(
                f"- Last restore: {latest_restore['source']} (generation {latest_restore['generation']})"
            )

    return "\n".join(lines)



def render_runtime_status_for_telegram(
    store: RuntimeStore,
    *,
    capsule_id: str | None = None,
    active_only: bool = False,
    checkpoint_path: str | Path | None = None,
) -> str:
    snapshot = build_runtime_status_snapshot(store, capsule_id=capsule_id, checkpoint_path=checkpoint_path)
    return format_runtime_status_for_telegram(snapshot, active_only=active_only)


__all__ = [
    "build_runtime_status_snapshot",
    "compute_approval_pressure",
    "format_runtime_status_for_telegram",
    "render_runtime_status_for_telegram",
]
