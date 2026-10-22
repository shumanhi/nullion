from __future__ import annotations

import pytest

from nullion.builder import (
    BuilderDecision,
    BuilderDecisionType,
    BuilderInputPacket,
    BuilderProposal,
    builder_proposal_acceptance_benefit,
    build_builder_proposal,
    evaluate_builder_decision,
)
from nullion.skills import SKILL_WRITE_DELETE_CONSENT_STEP


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


def test_runtime_auto_accepts_builder_skill_proposals_by_default(monkeypatch) -> None:
    from nullion.runtime import RuntimeStore, run_request
    import nullion.runtime as runtime_module

    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Capture workflow",
        summary="Save the reusable path.",
        confidence=0.9,
        approval_mode="skill",
        suggested_skill_title="Reusable workflow",
        suggested_trigger="Use when this workflow repeats.",
        suggested_steps=("Inspect state", "Run the workflow", "Verify the result"),
        suggested_tags=("builder",),
    )
    monkeypatch.delenv("NULLION_BUILDER_AUTO_APPROVE_SKILLS", raising=False)
    monkeypatch.setattr(
        runtime_module,
        "evaluate_builder_decision_read_model",
        lambda packet: BuilderDecision(BuilderDecisionType.SKILL_PROPOSAL, True, "workflow_pattern"),
    )
    monkeypatch.setattr(runtime_module, "build_builder_proposal_read_model", lambda decision: proposal)

    store = RuntimeStore()
    run_request(store, owner="operator", goal="do reusable work", action="run", resource="workflow")

    records = store.builder_proposals.values()
    assert [record.status for record in records] == ["accepted"]
    assert store.skills
    skill = next(iter(store.skills.values()))
    assert SKILL_WRITE_DELETE_CONSENT_STEP in skill.steps


def test_builder_acceptance_copy_requires_chat_confirmation_for_write_or_delete() -> None:
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Capture deletion workflow",
        summary="Save the reusable path.",
        confidence=0.9,
        approval_mode="skill",
        suggested_skill_title="Delete Drive document",
        suggested_trigger="Use for Drive cleanup.",
        suggested_steps=("Find the document", "Delete the document"),
        suggested_tags=("builder",),
    )

    benefit = builder_proposal_acceptance_benefit(proposal)

    assert "does not grant permission to modify or delete data" in benefit
    assert "explicit chat confirmation" in benefit


def test_runtime_can_disable_builder_skill_auto_accept(monkeypatch) -> None:
    from nullion.runtime import RuntimeStore, run_request
    import nullion.runtime as runtime_module

    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Capture workflow",
        summary="Save the reusable path.",
        confidence=0.9,
        approval_mode="skill",
        suggested_skill_title="Reusable workflow",
        suggested_trigger="Use when this workflow repeats.",
        suggested_steps=("Inspect state", "Run the workflow", "Verify the result"),
        suggested_tags=("builder",),
    )
    monkeypatch.setenv("NULLION_BUILDER_AUTO_APPROVE_SKILLS", "false")
    monkeypatch.setattr(
        runtime_module,
        "evaluate_builder_decision_read_model",
        lambda packet: BuilderDecision(BuilderDecisionType.SKILL_PROPOSAL, True, "workflow_pattern"),
    )
    monkeypatch.setattr(runtime_module, "build_builder_proposal_read_model", lambda decision: proposal)

    store = RuntimeStore()
    run_request(store, owner="operator", goal="do reusable work", action="run", resource="workflow")

    assert [record.status for record in store.builder_proposals.values()] == ["pending"]
    assert not store.skills
