"""Builder v0 decision scaffold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from functools import lru_cache
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.skills import SKILL_WRITE_DELETE_CONSENT_STEP


class BuilderDecisionType(str, Enum):
    NOOP = "noop"
    MEMORY_PROPOSAL = "memory_proposal"
    SKILL_PROPOSAL = "skill_proposal"
    TOOL_PROPOSAL = "tool_proposal"
    DEPENDENCY_PROPOSAL = "dependency_proposal"


@dataclass(slots=True)
class BuilderInputPacket:
    explicit_user_request: bool = False
    tool_call_count: int = 0
    repeated_failures: int = 0
    successful_task: bool = False
    task_completed: bool = False
    recent_doctor_signals: tuple[str, ...] = ()
    recent_sentinel_signals: tuple[str, ...] = ()
    file_count_touched: int = 0
    core_tool_names: tuple[str, ...] = ()
    missing_plugins_for_request: tuple[str, ...] = ()
    core_fallback_available: bool = False


@dataclass(slots=True)
class BuilderDecision:
    decision_type: BuilderDecisionType
    should_propose: bool
    reason: str


@dataclass(slots=True)
class BuilderProposal:
    decision_type: BuilderDecisionType
    title: str
    summary: str
    confidence: float
    approval_mode: str
    suggested_skill_title: str | None = None
    suggested_trigger: str | None = None
    suggested_steps: tuple[str, ...] = ()
    suggested_tags: tuple[str, ...] = ()
    dependency_id: str | None = None
    dependency_package: str | None = None
    dependency_import_name: str | None = None
    dependency_requirement: str | None = None
    dependency_install_command: tuple[str, ...] = ()
    dependency_docs_url: str | None = None
    dependency_github_url: str | None = None
    dependency_license: str | None = None
    dependency_usage_note: str | None = None


@dataclass(slots=True)
class BuilderProposalRecord:
    proposal_id: str
    proposal: BuilderProposal
    status: str
    created_at: datetime
    accepted_skill_id: str | None = None
    resolved_at: datetime | None = None
    context_key: str | None = None
    result: dict[str, object] | None = None


@dataclass(slots=True)
class SkillRefinementProposal:
    skill_id: str
    skill_title: str
    current_revision: int
    workflow_signal_count: int
    dominant_sources: tuple[str, ...]
    summary: str
    confidence: float


_CONNECTOR_APP_TAG_PREFIX = "connector-app:"


def builder_proposal_connector_app_id(proposal: BuilderProposal) -> str | None:
    for tag in getattr(proposal, "suggested_tags", ()) or ():
        tag_text = str(tag or "").strip()
        if tag_text.startswith(_CONNECTOR_APP_TAG_PREFIX):
            app_id = tag_text.removeprefix(_CONNECTOR_APP_TAG_PREFIX).strip()
            return app_id or None
    return None


def builder_proposal_connector_app_label(proposal: BuilderProposal) -> str | None:
    app_id = builder_proposal_connector_app_id(proposal)
    if app_id is None:
        return None
    label = app_id.replace("-", " ").replace("_", " ").strip()
    return label.title() if label else None


def builder_proposal_acceptance_benefit(proposal: BuilderProposal) -> str:
    """Shared, platform-neutral copy explaining why accepting a proposal helps."""
    approval_mode = str(getattr(proposal, "approval_mode", "") or "").strip().lower()
    if approval_mode == "dependency":
        return (
            "Why accept: Builder installs the approved package so Nullion can use that capability "
            "locally, create or update files/artifacts that need it, and finish similar requests "
            "with less setup."
        )
    if approval_mode == "memory":
        return (
            "Why accept: Builder saves the lesson as durable memory so future work can reuse the "
            "right context instead of asking you to repeat it."
        )
    connector_label = builder_proposal_connector_app_label(proposal)
    if approval_mode == "skill" and connector_label:
        return (
            f"Why accept: Builder saves this {connector_label} connector workflow so future requests "
            "can start with the right connection steps. It may request read access through that "
            "connector, but it does not grant write access by itself; write actions still require a "
            "write-capable connector, explicit chat confirmation, and normal approval. It does not connect "
            "the account or rerun the previous request by itself."
        )
    return (
        "Why accept: Builder saves this as a reusable skill so future similar work can start with "
        "the right steps, create or update files/artifacts when needed, and finish requested work "
        "with less setup. It does not grant permission to modify or delete data; write/delete actions "
        "still require explicit chat confirmation."
    )


def format_builder_proposal_notification(record: BuilderProposalRecord) -> str:
    """Short user-facing copy for a newly pending Builder proposal."""
    proposal = record.proposal
    approval_mode = str(getattr(proposal, "approval_mode", "") or "").strip().lower()
    if approval_mode == "dependency":
        action_label = "install a package"
        accept_label = "install it"
    elif approval_mode == "memory":
        action_label = "save a memory"
        accept_label = "save it"
    else:
        action_label = "save a reusable skill"
        accept_label = "save it"
    summary = str(getattr(proposal, "summary", "") or "").strip()
    summary_text = summary or action_label
    if summary_text and summary_text[-1] not in ".!?":
        summary_text += "."
    lines = [
        "🛠️ **Builder suggestion**",
        "",
        "I opened a Builder suggestion you can review and approve.",
        f"Optional improvement: {proposal.title}",
        f"What it would do: {summary_text}",
        builder_proposal_acceptance_benefit(proposal),
        f"Consent rule: {SKILL_WRITE_DELETE_CONSENT_STEP}",
        "",
        "Actions",
        "- Review: /proposal latest",
        f"- Approve: /accept-proposal latest ({accept_label})",
        "- Dismiss: /reject-proposal latest",
    ]
    return "\n".join(lines)


def _primary_trigger_for_packet(packet: BuilderInputPacket) -> str:
    if packet.successful_task:
        return "successful_task"
    if packet.task_completed:
        return "task_completed"
    if packet.repeated_failures >= 2:
        return "repeated_failures"
    if packet.missing_plugins_for_request and packet.core_fallback_available:
        return "core_fallback_workflow"
    if packet.explicit_user_request:
        return "explicit_user_request"
    if packet.file_count_touched >= 4 and packet.tool_call_count >= 7:
        return "workflow_pattern"
    if packet.tool_call_count >= 5:
        return "high_tool_call_count"
    return "no_trigger"


def build_builder_input_snapshot(packet: BuilderInputPacket) -> dict[str, object]:
    return {
        "explicit_user_request": packet.explicit_user_request,
        "tool_call_count": packet.tool_call_count,
        "repeated_failures": packet.repeated_failures,
        "successful_task": packet.successful_task,
        "task_completed": packet.task_completed,
        "recent_doctor_signals": list(packet.recent_doctor_signals),
        "recent_sentinel_signals": list(packet.recent_sentinel_signals),
        "file_count_touched": packet.file_count_touched,
        "core_tool_names": list(packet.core_tool_names),
        "core_tool_count": len(packet.core_tool_names),
        "missing_plugins_for_request": list(packet.missing_plugins_for_request),
        "core_fallback_available": packet.core_fallback_available,
        "doctor_signal_count": len(packet.recent_doctor_signals),
        "sentinel_signal_count": len(packet.recent_sentinel_signals),
        "primary_trigger": _primary_trigger_for_packet(packet),
    }


def evaluate_builder_decision(packet: BuilderInputPacket) -> BuilderDecision:
    final_state = _compiled_builder_decision_graph().invoke({"packet": packet})
    decision = final_state.get("decision")
    if isinstance(decision, BuilderDecision):
        return decision
    raise RuntimeError("Builder decision graph finished without a decision")


def build_builder_proposal(decision: BuilderDecision) -> BuilderProposal:
    final_state = _compiled_builder_proposal_graph().invoke({"decision": decision})
    proposal = final_state.get("proposal")
    if isinstance(proposal, BuilderProposal):
        return proposal
    raise RuntimeError("Builder proposal graph finished without a proposal")


class _BuilderDecisionState(TypedDict, total=False):
    packet: BuilderInputPacket
    decision: BuilderDecision


def _builder_decision_success_node(state: _BuilderDecisionState) -> dict[str, object]:
    packet = state["packet"]
    if packet.successful_task:
        return {
            "decision": BuilderDecision(BuilderDecisionType.NOOP, False, "successful_task")
        }
    if packet.task_completed:
        return {"decision": BuilderDecision(BuilderDecisionType.NOOP, False, "task_completed")}
    return {}


def _builder_decision_failures_node(state: _BuilderDecisionState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    if state["packet"].repeated_failures >= 2:
        return {
            "decision": BuilderDecision(
                BuilderDecisionType.MEMORY_PROPOSAL,
                True,
                "repeated_failures",
            )
        }
    return {}


def _builder_decision_skill_node(state: _BuilderDecisionState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    packet = state["packet"]
    if packet.missing_plugins_for_request and packet.core_fallback_available:
        return {
            "decision": BuilderDecision(
                BuilderDecisionType.SKILL_PROPOSAL,
                True,
                "core_fallback_workflow",
            )
        }
    if packet.explicit_user_request:
        return {
            "decision": BuilderDecision(
                BuilderDecisionType.SKILL_PROPOSAL,
                True,
                "explicit_user_request",
            )
        }
    if packet.file_count_touched >= 4 and packet.tool_call_count >= 7:
        return {
            "decision": BuilderDecision(
                BuilderDecisionType.SKILL_PROPOSAL,
                True,
                "workflow_pattern",
            )
        }
    return {}


def _builder_decision_tool_node(state: _BuilderDecisionState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    if state["packet"].tool_call_count >= 5:
        return {
            "decision": BuilderDecision(
                BuilderDecisionType.TOOL_PROPOSAL,
                True,
                "high_tool_call_count",
            )
        }
    return {"decision": BuilderDecision(BuilderDecisionType.NOOP, False, "no_trigger")}


@lru_cache(maxsize=1)
def _compiled_builder_decision_graph():
    graph = StateGraph(_BuilderDecisionState)
    graph.add_node("success", _builder_decision_success_node)
    graph.add_node("failures", _builder_decision_failures_node)
    graph.add_node("skill", _builder_decision_skill_node)
    graph.add_node("tool", _builder_decision_tool_node)
    graph.add_edge(START, "success")
    graph.add_edge("success", "failures")
    graph.add_edge("failures", "skill")
    graph.add_edge("skill", "tool")
    graph.add_edge("tool", END)
    return graph.compile()


class _BuilderProposalState(TypedDict, total=False):
    decision: BuilderDecision
    proposal: BuilderProposal


def _builder_proposal_memory_node(state: _BuilderProposalState) -> dict[str, object]:
    decision = state["decision"]
    if decision.decision_type is BuilderDecisionType.MEMORY_PROPOSAL:
        return {
            "proposal": BuilderProposal(
                decision_type=decision.decision_type,
                title="Propose a memory",
                summary="Repeated failures suggest preserving a durable lesson.",
                confidence=0.8,
                approval_mode="memory",
            )
        }
    return {}


def _builder_proposal_skill_node(state: _BuilderProposalState) -> dict[str, object]:
    if state.get("proposal") is not None:
        return {}
    decision = state["decision"]
    if decision.decision_type is not BuilderDecisionType.SKILL_PROPOSAL:
        return {}
    if decision.reason == "core_fallback_workflow":
        return {
            "proposal": BuilderProposal(
                decision_type=decision.decision_type,
                title="Capture a core fallback workflow",
                summary="A plugin is missing, but Builder can still orchestrate a slower core-tools fallback path.",
                confidence=0.8,
                approval_mode="skill",
                suggested_skill_title="Core fallback workflow",
                suggested_trigger="Use when a preferred plugin is missing but the task remains doable through core tools.",
                suggested_steps=(
                    "Inspect the installed plugins and available core tools",
                    "Plan a slower core-tool fallback path",
                    "Execute the fallback steps with approvals when needed",
                    "Verify the requested outcome and capture reusable lessons",
                ),
                suggested_tags=("core-fallback", "builder", "workflow"),
            )
        }
    if decision.reason == "workflow_pattern":
        return {
            "proposal": BuilderProposal(
                decision_type=decision.decision_type,
                title="Capture a reusable skill",
                summary="This looks like a repeated multi-step workflow worth saving as a skill.",
                confidence=0.8,
                approval_mode="skill",
                suggested_skill_title="Reusable workflow",
                suggested_trigger="Use when this workflow pattern repeats.",
                suggested_steps=(
                    "Inspect the repo state",
                    "Run focused tests",
                    "Apply the minimal fix",
                    "Run final verification",
                ),
                suggested_tags=("workflow", "automation"),
            )
        }
    return {
        "proposal": BuilderProposal(
            decision_type=decision.decision_type,
            title="Propose a skill",
            summary="The user explicitly asked for reusable behavior.",
            confidence=0.75,
            approval_mode="skill",
        )
    }


def _builder_proposal_tool_node(state: _BuilderProposalState) -> dict[str, object]:
    if state.get("proposal") is not None:
        return {}
    decision = state["decision"]
    if decision.decision_type is BuilderDecisionType.TOOL_PROPOSAL:
        return {
            "proposal": BuilderProposal(
                decision_type=decision.decision_type,
                title="Propose a tool",
                summary="Tool usage is high enough to justify a helper.",
                confidence=0.7,
                approval_mode="tool",
            )
        }
    return {
        "proposal": BuilderProposal(
            decision_type=BuilderDecisionType.NOOP,
            title="No Builder action",
            summary="No trigger matched.",
            confidence=0.0,
            approval_mode="none",
        )
    }


@lru_cache(maxsize=1)
def _compiled_builder_proposal_graph():
    graph = StateGraph(_BuilderProposalState)
    graph.add_node("memory", _builder_proposal_memory_node)
    graph.add_node("skill", _builder_proposal_skill_node)
    graph.add_node("tool", _builder_proposal_tool_node)
    graph.add_edge(START, "memory")
    graph.add_edge("memory", "skill")
    graph.add_edge("skill", "tool")
    graph.add_edge("tool", END)
    return graph.compile()


def build_builder_proposal_snapshot(proposal: BuilderProposal) -> dict[str, object]:
    return {
        "decision_type": proposal.decision_type.value,
        "title": proposal.title,
        "summary": proposal.summary,
        "acceptance_benefit": builder_proposal_acceptance_benefit(proposal),
        "confidence": proposal.confidence,
        "approval_mode": proposal.approval_mode,
        "confidence_percent": int(round(proposal.confidence * 100)),
    }


def format_builder_proposal_for_telegram(snapshot: dict[str, object]) -> str:
    benefit = str(snapshot.get("acceptance_benefit") or "")
    benefit_line = f"{benefit}\n" if benefit else ""
    return (
        "🛠️ **Builder suggestion**\n"
        "\n"
        f"{snapshot['title']}\n"
        "\n"
        f"{snapshot['summary']}\n"
        f"{benefit_line}"
        f"Confidence: {snapshot['confidence_percent']}% • Approval mode: {snapshot['approval_mode']}"
    )


def render_builder_proposal_for_telegram(proposal: BuilderProposal) -> str:
    return format_builder_proposal_for_telegram(build_builder_proposal_snapshot(proposal))


def build_skill_refinement_proposal_snapshot(proposal: SkillRefinementProposal) -> dict[str, object]:
    return {
        "proposal_type": "skill_refinement",
        "skill_id": proposal.skill_id,
        "skill_title": proposal.skill_title,
        "current_revision": proposal.current_revision,
        "workflow_signal_count": proposal.workflow_signal_count,
        "dominant_sources": list(proposal.dominant_sources),
        "summary": proposal.summary,
        "confidence": proposal.confidence,
        "confidence_percent": int(round(proposal.confidence * 100)),
    }


def format_skill_refinement_proposal_for_telegram(snapshot: dict[str, object]) -> str:
    sources = ", ".join(str(source) for source in snapshot["dominant_sources"])
    return (
        "🛠 Skill refinement\n"
        f"{snapshot['skill_title']} (/skill {snapshot['skill_id']})\n"
        "\n"
        f"{snapshot['summary']}\n"
        f"Signals: {snapshot['workflow_signal_count']} • Revision: {snapshot['current_revision']}"
        f" • Sources: {sources} • Confidence: {snapshot['confidence_percent']}%"
    )


def render_skill_refinement_proposal_for_telegram(proposal: SkillRefinementProposal) -> str:
    return format_skill_refinement_proposal_for_telegram(build_skill_refinement_proposal_snapshot(proposal))


__all__ = [
    "BuilderDecision",
    "BuilderDecisionType",
    "BuilderInputPacket",
    "BuilderProposal",
    "BuilderProposalRecord",
    "SkillRefinementProposal",
    "build_builder_input_snapshot",
    "build_builder_proposal",
    "build_builder_proposal_snapshot",
    "builder_proposal_acceptance_benefit",
    "builder_proposal_connector_app_id",
    "builder_proposal_connector_app_label",
    "build_skill_refinement_proposal_snapshot",
    "evaluate_builder_decision",
    "format_builder_proposal_notification",
    "format_builder_proposal_for_telegram",
    "format_skill_refinement_proposal_for_telegram",
    "render_builder_proposal_for_telegram",
    "render_skill_refinement_proposal_for_telegram",
]
