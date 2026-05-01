"""Read-model helpers for Mission OS snapshots and Telegram rendering."""

from __future__ import annotations

from nullion.missions import MissionContinuationPolicy, MissionRecord, MissionStatus, MissionStep



def _unique_step_run_ids(step: MissionStep) -> list[str]:
    run_ids = [
        run_id
        for run_id in (step.mini_agent_run_id, *step.mini_agent_run_ids)
        if isinstance(run_id, str) and run_id.strip()
    ]
    unique_run_ids: list[str] = []
    for run_id in run_ids:
        if run_id not in unique_run_ids:
            unique_run_ids.append(run_id)
    return unique_run_ids



def _required_step_run_ids(step: MissionStep) -> list[str]:
    run_ids = [
        run_id
        for run_id in (step.mini_agent_run_id, *step.required_mini_agent_run_ids)
        if isinstance(run_id, str) and run_id.strip()
    ]
    unique_run_ids: list[str] = []
    for run_id in run_ids:
        if run_id not in unique_run_ids:
            unique_run_ids.append(run_id)
    return unique_run_ids



def classify_mission_execution_phase(mission: MissionRecord) -> str:
    if mission.status in {MissionStatus.COMPLETED, MissionStatus.FAILED, MissionStatus.CANCELLED}:
        return "terminal"
    if mission.status in {MissionStatus.WAITING_APPROVAL, MissionStatus.WAITING_USER, MissionStatus.BLOCKED}:
        return "waiting"
    if mission.status is MissionStatus.VERIFYING:
        return "verifying"
    if mission.status is MissionStatus.RUNNING:
        return "executing"
    if mission.status is MissionStatus.PENDING:
        return "planning"
    if mission.continuation_policy is MissionContinuationPolicy.AUTO_FINISH:
        return "executing"
    return "planning"



def classify_mission_execution_role(mission: MissionRecord) -> str | None:
    active_step_kind = None
    for step in mission.steps:
        if step.step_id == mission.active_step_id:
            active_step_kind = step.kind
            break

    phase = classify_mission_execution_phase(mission)
    if phase == "terminal":
        return "terminal"
    if active_step_kind == "plan":
        return "planner"
    if active_step_kind == "verify":
        return "verifier"
    if active_step_kind in {"tool", "mini_agent"}:
        return "executor"
    if active_step_kind in {"approval", "user_input", "external_wait"} or phase == "waiting":
        return "waiting"
    if phase == "verifying":
        return "verifier"
    if phase == "executing":
        return "executor"
    if phase == "planning":
        return "planner"
    return None



def build_mission_snapshot(mission: MissionRecord) -> dict[str, object]:

    active_step_index = None
    active_step = None
    active_step_kind = None
    active_step_run_ids: list[str] = []
    required_step_run_ids: list[str] = []
    active_mini_agent_runs = 0
    completed_steps = 0
    for index, step in enumerate(mission.steps):
        if step.status == "completed":
            completed_steps += 1
        if step.status == "running":
            linked_run_ids = _unique_step_run_ids(step)
            active_mini_agent_runs += len(linked_run_ids)
        if mission.active_step_id == step.step_id:
            active_step_index = index
            active_step = step.title
            active_step_kind = step.kind
            active_step_run_ids = _unique_step_run_ids(step)
            required_step_run_ids = _required_step_run_ids(step)

    required_count = sum(1 for item in mission.completion_checklist if item.required)
    satisfied_count = sum(1 for item in mission.completion_checklist if item.required and item.satisfied)

    return {
        "mission_id": mission.mission_id,
        "owner": mission.owner,
        "title": mission.title,
        "goal": mission.goal,
        "status": mission.status.value,
        "continuation_policy": mission.continuation_policy.value,
        "active_step_index": active_step_index,
        "active_step": active_step,
        "active_step_kind": active_step_kind,
        "active_step_run_ids": active_step_run_ids,
        "required_step_run_ids": required_step_run_ids,
        "execution_phase": classify_mission_execution_phase(mission),
        "execution_role": classify_mission_execution_role(mission),
        "completed_steps": completed_steps,
        "total_steps": len(mission.steps),
        "active_mini_agent_runs": active_mini_agent_runs,
        "waiting_on": mission.waiting_on,
        "blocked_reason": mission.blocked_reason,
        "result_summary": mission.result_summary,
        "terminal_reason": None if mission.terminal_reason is None else mission.terminal_reason.value,
        "last_progress_message": mission.last_progress_message,
        "checklist": {
            "total": len(mission.completion_checklist),
            "required": required_count,
            "satisfied": satisfied_count,
        },
    }


def format_mission_for_telegram(snapshot: dict[str, object]) -> str:
    lines = [
        "📌 Mission",
        str(snapshot["title"]),
        f"Status: {str(snapshot['status']).replace('_', ' ')}",
    ]

    total_steps = int(snapshot.get("total_steps", 0))
    completed_steps = int(snapshot.get("completed_steps", 0))
    checklist = dict(snapshot.get("checklist", {}))
    checklist_satisfied = int(checklist.get("satisfied", 0))
    checklist_required = int(checklist.get("required", 0))
    active_step_index = snapshot.get("active_step_index")
    active_step = snapshot.get("active_step")
    execution_role = snapshot.get("execution_role")
    if isinstance(execution_role, str) and execution_role and execution_role != "terminal" and (total_steps > 0 or active_step):
        lines.append(f"Role: {execution_role.replace('_', ' ')}")
    lines.append(f"Progress: {completed_steps}/{total_steps} steps • {checklist_satisfied}/{checklist_required} checklist")

    if isinstance(active_step_index, int) and active_step and total_steps > 0:
        lines.append(f"Step {active_step_index + 1}/{total_steps}: {active_step}")

    active_mini_agent_runs = int(snapshot.get("active_mini_agent_runs", 0))
    if active_mini_agent_runs > 0:
        label = "Mini-Agent" if active_mini_agent_runs == 1 else "Mini-Agents"
        lines.append(f"{label}: {active_mini_agent_runs} running")

    required_step_run_ids = [
        str(run_id)
        for run_id in snapshot.get("required_step_run_ids", [])
        if isinstance(run_id, str) and run_id.strip()
    ]
    active_step_run_ids = [
        str(run_id)
        for run_id in snapshot.get("active_step_run_ids", [])
        if isinstance(run_id, str) and run_id.strip()
    ]
    if required_step_run_ids and (
        len(active_step_run_ids) > len(required_step_run_ids) or active_step_run_ids != required_step_run_ids
    ):
        lines.append(f"Required runs: {', '.join(required_step_run_ids)}")

    if len(active_step_run_ids) > 1:
        lines.append(f"Runs: {', '.join(active_step_run_ids)}")

    waiting_on = snapshot.get("waiting_on")
    blocked_reason = snapshot.get("blocked_reason")
    result_summary = snapshot.get("result_summary")
    last_progress_message = snapshot.get("last_progress_message")

    if waiting_on:
        lines.append(f"Waiting on: {waiting_on}")
    elif blocked_reason:
        lines.append(f"Blocked: {blocked_reason}")
    elif result_summary:
        lines.append(f"Result: {result_summary}")
    elif last_progress_message:
        lines.append(f"Next: {last_progress_message}")

    return "\n".join(lines)


def render_mission_for_telegram(mission: MissionRecord) -> str:
    return format_mission_for_telegram(build_mission_snapshot(mission))


__all__ = [
    "build_mission_snapshot",
    "classify_mission_execution_phase",
    "classify_mission_execution_role",
    "format_mission_for_telegram",
    "render_mission_for_telegram",
]
