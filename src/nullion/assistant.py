"""Nullion Assistant runtime spine composed from core Project Nullion primitives."""

from dataclasses import dataclass
from typing import Iterable

from nullion.intent import IntentCapsule, IntentState, create_intent_capsule, with_state
from nullion.mini_agents import (
    MiniAgentLaunchDecision,
    MiniAgentLaunchPlan,
    decide_mini_agent_launch,
)
from nullion.orchestration import (
    OrchestrationOutcome,
    OrchestrationResult,
    orchestrate_request,
)
from nullion.tools import ToolResult
from nullion.policy import PolicyDecision
from nullion.progress import ProgressUpdate, progress_update_for_intent
from nullion.run_activity import format_skill_usage_activity_line
from nullion.signals import SignalRoute, route_policy_decision


@dataclass(slots=True)
class AssistantTurn:
    capsule: IntentCapsule
    orchestration_result: OrchestrationResult
    progress_update: ProgressUpdate
    mini_agent_launch_plan: MiniAgentLaunchPlan
    user_message: str


def _policy_decision_from_outcome(outcome: OrchestrationOutcome) -> PolicyDecision:
    return PolicyDecision(outcome.value)


def _capsule_with_orchestration_state(
    capsule: IntentCapsule,
    orchestration_result: OrchestrationResult,
) -> IntentCapsule:
    if orchestration_result.outcome is OrchestrationOutcome.ALLOW:
        return with_state(capsule, IntentState.RUNNING)

    if orchestration_result.outcome is OrchestrationOutcome.REQUIRE_APPROVAL:
        approval_id = None
        if orchestration_result.approval_request is not None:
            approval_id = orchestration_result.approval_request.approval_id
        return with_state(
            capsule,
            IntentState.WAITING_APPROVAL,
            pending_approval_id=approval_id,
        )

    return with_state(capsule, IntentState.BLOCKED)


def _user_message_for_turn(
    outcome: OrchestrationOutcome,
    launch_decision: MiniAgentLaunchDecision,
) -> str:
    if outcome is OrchestrationOutcome.REQUIRE_APPROVAL:
        return "Approval required before Nullion Assistant can continue."

    if outcome is OrchestrationOutcome.DENY:
        return "Request denied — blocked by Sentinel policy."

    if launch_decision is MiniAgentLaunchDecision.LAUNCH:
        return "Got it — Nullion is on it."

    return "Got it — accepted and queued."


def handle_request(
    owner: str,
    goal: str,
    action: str,
    resource: str,
    risk_level: str = "normal",
    success_criteria: list[str] | None = None,
    allowed_actions: Iterable[str] | None = None,
    approval_actions: Iterable[str] | None = None,
) -> AssistantTurn:
    capsule = create_intent_capsule(
        owner=owner,
        goal=goal,
        risk_level=risk_level,
        success_criteria=success_criteria,
    )
    orchestration_result = orchestrate_request(
        actor=owner,
        action=action,
        resource=resource,
        allowed_actions=allowed_actions,
        approval_actions=approval_actions,
    )

    updated_capsule = _capsule_with_orchestration_state(capsule, orchestration_result)
    progress_update = progress_update_for_intent(updated_capsule)

    policy_decision = _policy_decision_from_outcome(orchestration_result.outcome)
    policy_route: SignalRoute = route_policy_decision(
        decision=policy_decision,
        action=action,
        resource=resource,
    )
    mini_agent_launch_plan = decide_mini_agent_launch(
        updated_capsule,
        policy_route=policy_route,
    )

    user_message = _user_message_for_turn(
        outcome=orchestration_result.outcome,
        launch_decision=mini_agent_launch_plan.decision,
    )

    return AssistantTurn(
        capsule=updated_capsule,
        orchestration_result=orchestration_result,
        progress_update=progress_update,
        mini_agent_launch_plan=mini_agent_launch_plan,
        user_message=user_message,
    )
def _humanize_state(value: str) -> str:
    return value.replace("_", " ")


def build_assistant_turn_snapshot(turn: AssistantTurn) -> dict[str, object]:
    mini_agent_decision = turn.mini_agent_launch_plan.decision.value
    if turn.mini_agent_launch_plan.decision is MiniAgentLaunchDecision.LAUNCH:
        mini_agent_summary = f"launch {turn.mini_agent_launch_plan.mini_agent_type or 'general'}"
    elif turn.mini_agent_launch_plan.decision is MiniAgentLaunchDecision.HOLD:
        mini_agent_summary = "holding"
    else:
        mini_agent_summary = "not launching"

    if turn.capsule.state is IntentState.RUNNING:
        next_step = "I’ll update you when results are ready."
    elif turn.capsule.state is IntentState.WAITING_APPROVAL:
        next_step = "I’ll update you when approval lands."
    elif turn.capsule.state is IntentState.BLOCKED:
        next_step = "Nothing is running yet."
    else:
        next_step = "I’ll update you when the task starts moving."

    return {
        "owner": turn.capsule.owner,
        "goal": turn.capsule.goal,
        "user_message": turn.user_message,
        "capsule_state": _humanize_state(turn.capsule.state.value),
        "progress_state": _humanize_state(turn.progress_update.state.value),
        "progress_message": turn.progress_update.message,
        "mini_agent_decision": mini_agent_decision,
        "mini_agent_summary": mini_agent_summary,
        "next_step": next_step,
    }


def format_assistant_turn_for_telegram(snapshot: dict[str, object]) -> str:
    lines = [
        "📌 Nullion Assistant",
        f"{snapshot['owner']} • {snapshot['goal']}",
        "",
        str(snapshot["user_message"]),
        (
            f"State: {snapshot['capsule_state']} • Progress: {snapshot['progress_state']}"
            f" • Mini-Agent: {snapshot['mini_agent_summary']}"
        ),
    ]

    applied_skill = snapshot.get("applied_skill")
    skill_execution_plan = snapshot.get("skill_execution_plan")
    skill_execution_intent = snapshot.get("skill_execution_intent")

    if isinstance(skill_execution_plan, dict):
        plan_title = skill_execution_plan.get("title")
        if plan_title:
            lines.append("Using learned skill")
            lines.append(format_skill_usage_activity_line({"title": str(plan_title)}) or f"  ⧁ {plan_title}")
        plan_steps = skill_execution_plan.get("steps")
        if isinstance(plan_steps, list) and plan_steps:
            lines.append(f"Plan: {' → '.join(str(step) for step in plan_steps[:3])}")
    elif isinstance(applied_skill, dict):
        applied_title = applied_skill.get("title")
        if applied_title:
            lines.append("Using learned skill")
            lines.append(format_skill_usage_activity_line({"title": str(applied_title)}) or f"  ⧁ {applied_title}")
        applied_steps = applied_skill.get("steps")
        if isinstance(applied_steps, list) and applied_steps:
            lines.append(f"Plan: {' → '.join(str(step) for step in applied_steps[:3])}")

    if isinstance(skill_execution_plan, dict):
        completed_steps = skill_execution_plan.get("completed_steps")
        total_steps = skill_execution_plan.get("total_steps")
        active_step = skill_execution_plan.get("active_step")
        if isinstance(completed_steps, int) and isinstance(total_steps, int):
            if active_step:
                lines.append(f"Skill progress: {completed_steps}/{total_steps} complete • Active: {active_step}")
            else:
                lines.append(f"Skill progress: {completed_steps}/{total_steps} complete")

    if isinstance(skill_execution_intent, dict):
        safety_mode = skill_execution_intent.get("safety_mode")
        if safety_mode and skill_execution_intent.get("side_effects_allowed") is False:
            lines.append(f"Execution mode: {safety_mode} (no autonomous side effects)")

    lines.append(f"Next: {snapshot['next_step']}")
    return "\n".join(lines)


def _humanize_tool_name(tool_name: str) -> str:
    return tool_name.replace("_", " ").capitalize()


def build_tool_result_snapshot(result: ToolResult) -> dict[str, object]:
    title = _humanize_tool_name(result.tool_name)
    summary_lines: list[str] = []

    if result.tool_name == "file_read" and isinstance(result.output, dict):
        path = result.output.get("path")
        content = result.output.get("content")
        if path is not None:
            summary_lines.append(f"Path: {path}")
        if content is not None:
            summary_lines.append(str(content).rstrip("\n"))
    elif result.error:
        summary_lines = [result.error]
    elif isinstance(result.output, dict):
        summary_lines = [str(value) for value in result.output.values() if value is not None]

    return {
        "tool_name": result.tool_name,
        "status": result.status,
        "title": title,
        "summary_lines": summary_lines,
        "error": result.error,
    }


def format_tool_result_for_telegram(snapshot: dict[str, object]) -> str:
    lines = ["📌 Nullion Assistant", str(snapshot["title"])]
    summary_lines = [str(line) for line in snapshot.get("summary_lines", [])]
    if summary_lines:
        lines.append("")
        lines.extend(summary_lines)
    elif snapshot.get("error"):
        lines.append("")
        lines.append(str(snapshot["error"]))
    return "\n".join(lines)


def render_tool_result_for_telegram(result: ToolResult) -> str:
    return format_tool_result_for_telegram(build_tool_result_snapshot(result))


def render_assistant_turn_for_telegram(turn: AssistantTurn) -> str:
    return format_assistant_turn_for_telegram(build_assistant_turn_snapshot(turn))


__all__ = [
    "AssistantTurn",
    "build_assistant_turn_snapshot",
    "build_tool_result_snapshot",
    "format_assistant_turn_for_telegram",
    "format_tool_result_for_telegram",
    "handle_request",
    "render_assistant_turn_for_telegram",
    "render_tool_result_for_telegram",
]
