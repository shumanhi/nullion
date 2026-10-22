from __future__ import annotations

from types import SimpleNamespace

from nullion.assistant import (
    AssistantTurn,
    build_assistant_turn_snapshot,
    build_tool_result_snapshot,
    format_assistant_turn_for_telegram,
    handle_request,
    render_tool_result_for_telegram,
)
from nullion.intent import IntentCapsule, IntentState
from nullion.mini_agents import MiniAgentLaunchDecision, MiniAgentLaunchPlan
from nullion.orchestration import OrchestrationOutcome
from nullion.progress import ProgressState, ProgressUpdate
from nullion.tools import ToolResult


def _turn(state: IntentState, decision: MiniAgentLaunchDecision, *, outcome: OrchestrationOutcome) -> AssistantTurn:
    capsule = IntentCapsule(
        capsule_id="cap-1",
        owner="operator",
        goal="ship",
        state=state,
        risk_level="normal",
        active_mini_agents=(),
        pending_approval_id=None,
        success_criteria=(),
    )
    return AssistantTurn(
        capsule=capsule,
        orchestration_result=SimpleNamespace(outcome=outcome),
        progress_update=ProgressUpdate(ProgressState.WORKING, "working", "cap-1"),
        mini_agent_launch_plan=MiniAgentLaunchPlan(decision=decision, reason="test", mini_agent_type="researcher"),
        user_message="ok",
    )


def test_handle_request_maps_orchestration_outcomes_to_user_visible_state() -> None:
    allowed = handle_request("operator", "ship", "read", "docs", allowed_actions=["read"])
    assert allowed.capsule.state is IntentState.RUNNING
    assert allowed.user_message == "Got it — Nullion is on it."

    approval = handle_request("operator", "deploy", "deploy", "prod", approval_actions=["deploy"])
    assert approval.capsule.state is IntentState.WAITING_APPROVAL
    assert approval.capsule.pending_approval_id is not None
    assert approval.user_message == "Approval required before Nullion Assistant can continue."

    denied = handle_request("operator", "delete", "delete", "prod")
    assert denied.capsule.state is IntentState.BLOCKED
    assert denied.user_message == "Request denied — blocked by Sentinel policy."


def test_assistant_turn_snapshot_and_formatter_cover_decision_variants() -> None:
    running = build_assistant_turn_snapshot(_turn(IntentState.RUNNING, MiniAgentLaunchDecision.LAUNCH, outcome=OrchestrationOutcome.ALLOW))
    assert running["mini_agent_summary"] == "launch researcher"
    assert running["next_step"] == "I’ll update you when results are ready."

    waiting = build_assistant_turn_snapshot(_turn(IntentState.WAITING_APPROVAL, MiniAgentLaunchDecision.HOLD, outcome=OrchestrationOutcome.REQUIRE_APPROVAL))
    assert waiting["mini_agent_summary"] == "holding"
    assert waiting["next_step"] == "I’ll update you when approval lands."

    blocked = build_assistant_turn_snapshot(_turn(IntentState.BLOCKED, MiniAgentLaunchDecision.DENY, outcome=OrchestrationOutcome.DENY))
    assert blocked["mini_agent_summary"] == "not launching"
    assert blocked["next_step"] == "Nothing is running yet."

    blocked["applied_skill"] = {"title": "Review deploy", "steps": ["inspect", "fix", "verify", "ship"]}
    formatted = format_assistant_turn_for_telegram(blocked)
    assert "Using learned skill" in formatted
    assert "Plan: inspect → fix → verify" in formatted

    blocked["skill_execution_plan"] = {"title": "Repair", "steps": ["a", "b"], "completed_steps": 1, "total_steps": 3, "active_step": "b"}
    blocked["skill_execution_intent"] = {"safety_mode": "dry-run", "side_effects_allowed": False}
    formatted_plan = format_assistant_turn_for_telegram(blocked)
    assert "Skill progress: 1/3 complete • Active: b" in formatted_plan
    assert "Execution mode: dry-run" in formatted_plan


def test_tool_result_snapshots_render_file_output_errors_and_dict_values() -> None:
    file_result = ToolResult("inv-1", "file_read", "completed", {"path": "/tmp/a.txt", "content": "hello\n"})
    snapshot = build_tool_result_snapshot(file_result)
    assert snapshot["title"] == "File read"
    assert snapshot["summary_lines"] == ["Path: /tmp/a.txt", "hello"]
    assert "hello" in render_tool_result_for_telegram(file_result)

    error_snapshot = build_tool_result_snapshot(ToolResult("inv-2", "shell", "failed", {}, error="nope"))
    assert error_snapshot["summary_lines"] == ["nope"]

    generic_snapshot = build_tool_result_snapshot(ToolResult("inv-3", "web_search", "completed", {"answer": "42", "none": None}))
    assert generic_snapshot["summary_lines"] == ["42"]
