from __future__ import annotations

import pytest

from nullion.builder import (
    BuilderDecisionType,
    BuilderInputPacket,
    build_builder_proposal,
    evaluate_builder_decision,
)


@pytest.mark.parametrize(
    ("packet", "decision_type", "reason", "approval_mode"),
    [
        (
            BuilderInputPacket(successful_task=True, repeated_failures=4, explicit_user_request=True),
            BuilderDecisionType.NOOP,
            "successful_task",
            "none",
        ),
        (
            BuilderInputPacket(task_completed=True, repeated_failures=4, explicit_user_request=True),
            BuilderDecisionType.NOOP,
            "task_completed",
            "none",
        ),
        (
            BuilderInputPacket(repeated_failures=2),
            BuilderDecisionType.MEMORY_PROPOSAL,
            "repeated_failures",
            "memory",
        ),
        (
            BuilderInputPacket(missing_plugins_for_request=("browser",), core_fallback_available=True),
            BuilderDecisionType.SKILL_PROPOSAL,
            "core_fallback_workflow",
            "skill",
        ),
        (
            BuilderInputPacket(explicit_user_request=True),
            BuilderDecisionType.SKILL_PROPOSAL,
            "explicit_user_request",
            "skill",
        ),
        (
            BuilderInputPacket(file_count_touched=4, tool_call_count=7),
            BuilderDecisionType.SKILL_PROPOSAL,
            "workflow_pattern",
            "skill",
        ),
        (
            BuilderInputPacket(tool_call_count=5),
            BuilderDecisionType.TOOL_PROPOSAL,
            "high_tool_call_count",
            "tool",
        ),
        (
            BuilderInputPacket(tool_call_count=4),
            BuilderDecisionType.NOOP,
            "no_trigger",
            "none",
        ),
    ],
)
def test_builder_graph_decision_priority_and_proposals(
    packet: BuilderInputPacket,
    decision_type: BuilderDecisionType,
    reason: str,
    approval_mode: str,
) -> None:
    decision = evaluate_builder_decision(packet)
    proposal = build_builder_proposal(decision)

    assert decision.decision_type is decision_type
    assert decision.reason == reason
    assert decision.should_propose is (decision_type is not BuilderDecisionType.NOOP)
    assert proposal.decision_type is decision_type
    assert proposal.approval_mode == approval_mode


def test_builder_core_fallback_proposal_captures_reusable_steps() -> None:
    decision = evaluate_builder_decision(
        BuilderInputPacket(missing_plugins_for_request=("browser",), core_fallback_available=True)
    )
    proposal = build_builder_proposal(decision)

    assert proposal.title == "Capture a core fallback workflow"
    assert proposal.suggested_skill_title == "Core fallback workflow"
    assert "core-fallback" in proposal.suggested_tags
    assert len(proposal.suggested_steps) == 4
