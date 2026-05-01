"""Safe skill-guided execution planning scaffolding."""

from __future__ import annotations

from dataclasses import dataclass, replace

from nullion.mini_agent_runs import MiniAgentRunStatus
from nullion.progress import ProgressState
from nullion.skills import SkillRecord


SAFE_SKILL_PLAN_MODE = "suggest_only"
SAFE_SKILL_EXECUTION_INTENT = "guided_suggestion"
SKILL_STEP_PENDING = "pending"
SKILL_STEP_IN_PROGRESS = "in_progress"
SKILL_STEP_COMPLETED = "completed"


@dataclass(slots=True, frozen=True)
class SkillExecutionPlan:
    plan_id: str
    skill_id: str
    title: str
    steps: tuple[str, ...]
    safety_mode: str = SAFE_SKILL_PLAN_MODE
    step_states: tuple[str, ...] = ()
    active_step_index: int | None = None


def _default_step_states(steps: tuple[str, ...]) -> tuple[str, ...]:
    if not steps:
        return ()
    return (SKILL_STEP_IN_PROGRESS, *([SKILL_STEP_PENDING] * (len(steps) - 1)))


def _plan_progress_snapshot(plan: SkillExecutionPlan) -> dict[str, object]:
    active_step = None
    if plan.active_step_index is not None and 0 <= plan.active_step_index < len(plan.steps):
        active_step = plan.steps[plan.active_step_index]
    return {
        "active_step_index": plan.active_step_index,
        "active_step": active_step,
        "completed_steps": sum(1 for state in plan.step_states if state == SKILL_STEP_COMPLETED),
        "total_steps": len(plan.steps),
    }


def build_skill_execution_plan(skill: SkillRecord) -> SkillExecutionPlan:
    steps = tuple(skill.steps)
    return SkillExecutionPlan(
        plan_id=f"skill-plan-{skill.skill_id}",
        skill_id=skill.skill_id,
        title=skill.title,
        steps=steps,
        safety_mode=SAFE_SKILL_PLAN_MODE,
        step_states=_default_step_states(steps),
        active_step_index=0 if steps else None,
    )


def build_skill_execution_plan_snapshot(plan: SkillExecutionPlan) -> dict[str, object]:
    return {
        "plan_id": plan.plan_id,
        "skill_id": plan.skill_id,
        "title": plan.title,
        "steps": list(plan.steps),
        "safety_mode": plan.safety_mode,
        "step_states": _normalized_step_states(plan),
        **_plan_progress_snapshot(plan),
    }


def build_skill_execution_intent_snapshot(plan: SkillExecutionPlan) -> dict[str, object]:
    return {
        "plan_id": plan.plan_id,
        "skill_id": plan.skill_id,
        "intent": SAFE_SKILL_EXECUTION_INTENT,
        "safety_mode": plan.safety_mode,
        "side_effects_allowed": False,
        "execution_progress": _plan_progress_snapshot(plan),
    }


def format_skill_execution_plan_for_telegram(snapshot: dict[str, object]) -> str:
    title = str(snapshot.get("title", "Skill execution plan"))
    safety_mode = str(snapshot.get("safety_mode", "")).replace("_", " ").strip()
    if not safety_mode:
        safety_mode = SAFE_SKILL_PLAN_MODE.replace("_", " ")

    total_steps = int(snapshot.get("total_steps", 0))
    completed_steps = int(snapshot.get("completed_steps", 0))
    active_step = snapshot.get("active_step")

    lines = ["🧭 Skill execution plan", title, f"Mode: {safety_mode}"]
    progress_line = f"Progress: {completed_steps}/{total_steps} complete"
    if isinstance(active_step, str) and active_step.strip():
        progress_line += f" • Active: {active_step.strip()}"
    elif total_steps > 0 and completed_steps == total_steps:
        progress_line += " • Complete"
    lines.append(progress_line)

    steps = snapshot.get("steps")
    if isinstance(steps, list) and steps:
        lines.append(f"Plan: {' → '.join(str(step).strip() for step in steps)}")
    return "\n".join(lines)


def render_skill_execution_plan_for_telegram(plan: SkillExecutionPlan) -> str:
    return format_skill_execution_plan_for_telegram(build_skill_execution_plan_snapshot(plan))


def _normalized_step_states(plan: SkillExecutionPlan) -> list[str]:
    step_states = list(plan.step_states[: len(plan.steps)])
    if len(step_states) < len(plan.steps):
        step_states.extend([SKILL_STEP_PENDING] * (len(plan.steps) - len(step_states)))
    return step_states


def _advance_skill_execution_plan(plan: SkillExecutionPlan) -> SkillExecutionPlan:
    if not plan.steps or plan.active_step_index is None:
        return plan

    step_states = _normalized_step_states(plan)
    active_step_index = plan.active_step_index
    if not (0 <= active_step_index < len(step_states)):
        return plan

    step_states[active_step_index] = SKILL_STEP_COMPLETED
    next_step_index = active_step_index + 1
    if next_step_index >= len(step_states):
        return replace(plan, step_states=tuple(step_states), active_step_index=None)

    if step_states[next_step_index] == SKILL_STEP_PENDING:
        step_states[next_step_index] = SKILL_STEP_IN_PROGRESS
    return replace(plan, step_states=tuple(step_states), active_step_index=next_step_index)


def _complete_skill_execution_plan(plan: SkillExecutionPlan) -> SkillExecutionPlan:
    if not plan.steps:
        return replace(plan, step_states=(), active_step_index=None)
    return replace(
        plan,
        step_states=tuple(SKILL_STEP_COMPLETED for _ in plan.steps),
        active_step_index=None,
    )


def transition_skill_execution_plan_for_mini_agent_status(
    plan: SkillExecutionPlan,
    mini_agent_status: MiniAgentRunStatus,
) -> SkillExecutionPlan:
    if mini_agent_status is MiniAgentRunStatus.COMPLETED:
        return _advance_skill_execution_plan(plan)
    return plan


def transition_skill_execution_plan_for_step_completion(plan: SkillExecutionPlan) -> SkillExecutionPlan:
    return _advance_skill_execution_plan(plan)


def transition_skill_execution_plan_for_progress(
    plan: SkillExecutionPlan,
    progress_state: ProgressState,
) -> SkillExecutionPlan:
    if progress_state is ProgressState.COMPLETED:
        return _complete_skill_execution_plan(plan)
    return plan


__all__ = [
    "SAFE_SKILL_EXECUTION_INTENT",
    "SAFE_SKILL_PLAN_MODE",
    "SKILL_STEP_COMPLETED",
    "SKILL_STEP_IN_PROGRESS",
    "SKILL_STEP_PENDING",
    "SkillExecutionPlan",
    "build_skill_execution_intent_snapshot",
    "build_skill_execution_plan",
    "build_skill_execution_plan_snapshot",
    "format_skill_execution_plan_for_telegram",
    "render_skill_execution_plan_for_telegram",
    "transition_skill_execution_plan_for_mini_agent_status",
    "transition_skill_execution_plan_for_step_completion",
    "transition_skill_execution_plan_for_progress",
]
