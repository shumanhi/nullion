"""Runtime service for handling assistant turns and persistence."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from fnmatch import fnmatch
from functools import lru_cache
import logging
import os
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping, TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.approvals import (
    ApprovalRequest,
    ApprovalStatus,
    BoundaryPermit,
    PermissionGrant,
    approve,
    consume_boundary_permit as consume_boundary_permit_record,
    create_approval_request,
    create_boundary_permit,
    create_permission_grant,
    deny,
    is_boundary_permit_active,
    is_permission_grant_active,
    revoke_permission_grant as revoke_permission_grant_record,
)
from nullion.artifacts import media_candidate_paths_from_text
from nullion.doctor_actions import (
    IN_PROGRESS,
    PENDING,
    DoctorAction,
    cancel_action,
    complete_action,
    create_doctor_action,
    fail_action,
    start_action,
)

from nullion.assistant import (
    AssistantTurn,
    build_assistant_turn_snapshot as build_assistant_turn_snapshot_read_model,
    build_tool_result_snapshot as build_tool_result_snapshot_read_model,
    format_assistant_turn_for_telegram as format_assistant_turn_for_telegram_read_model,
    handle_request,
    render_assistant_turn_for_telegram as render_assistant_turn_snapshot_for_telegram,
    render_tool_result_for_telegram as render_tool_result_for_telegram_read_model,
)
from nullion.audit import make_audit_record
from nullion.builder import (
    BuilderDecision,
    BuilderDecisionType,
    BuilderInputPacket,
    BuilderProposal,
    BuilderProposalRecord,
    SkillRefinementProposal,
    build_builder_input_snapshot as build_builder_input_snapshot_read_model,
    build_builder_proposal as build_builder_proposal_read_model,
    build_builder_proposal_snapshot as build_builder_proposal_snapshot_read_model,
    build_skill_refinement_proposal_snapshot as build_skill_refinement_snapshot_read_model,
    evaluate_builder_decision as evaluate_builder_decision_read_model,
    format_builder_proposal_for_telegram as format_builder_proposal_for_telegram_read_model,
    render_builder_proposal_for_telegram as render_builder_proposal_for_telegram_read_model,
    render_skill_refinement_proposal_for_telegram as render_skill_refinement_for_telegram_read_model,
)
from nullion.progress import (
    ProgressState,
    ProgressUpdate,
    build_progress_update_snapshot as build_progress_update_snapshot_read_model,
    format_progress_update_for_telegram as format_progress_update_for_telegram_read_model,
    progress_update_for_intent,
    render_progress_update_for_telegram as render_progress_update_for_telegram_read_model,
    should_emit_nudge,
)
from nullion.events import make_event
from nullion.health import HealthIssueType, make_health_alert
from nullion.intent import add_mini_agent, remove_mini_agent
from nullion.live_information import classify_live_information_route, route_required_plugins
from nullion.mini_agent_runs import (
    MiniAgentRun,
    MiniAgentRunStatus,
    create_mini_agent_run,
    transition_mini_agent_run_status,
)
from nullion.mini_agent_config import mini_agent_stale_after
from nullion.missions import (
    MissionChecklistItem,
    MissionContinuationPolicy,
    MissionRecord,
    MissionStatus,
    MissionStep,
    MissionTerminalReason,
)
from nullion.mission_status import render_mission_for_telegram as render_mission_for_telegram_read_model
from nullion.mini_agents import MiniAgentLaunchDecision
from nullion.policy import (
    BoundaryFact,
    BoundaryKind,
    BoundaryPolicyRequest,
    BoundaryPolicyRule,
    GLOBAL_PERMISSION_PRINCIPAL,
    OPERATOR_PERMISSION_PRINCIPAL,
    PolicyDecision,
    PolicyMode,
    evaluate_boundary_request,
    normalize_outbound_network_selector,
    permission_grant_principal,
    permission_scope_principal,
)
from nullion.codebase_summary import (
    build_codebase_summary as build_codebase_summary_read_model,
    format_codebase_summary as format_codebase_summary_read_model,
)
from nullion.conversation_runtime import (
    ConversationBranch,
    ConversationBranchStatus,
    ConversationEnvelope,
    ConversationTurn,
    ConversationTurnDisposition,
    WorkerResultEnvelope,
    WorkerResultKind,
)
from nullion.intent_router import (
    TurnDispositionAmbiguityClassifier,
    TurnDispositionAmbiguityFallback,
    classify_turn_disposition,
    classify_turn_disposition_with_reason,
)
from nullion.runtime_store import RuntimeStore
from nullion.runtime_persistence import (
    load_runtime_store,
    render_runtime_store_payload_json,
    restore_runtime_store_backup,
    save_runtime_store,
)
from nullion.task_frames import (
    TaskFrame,
    TaskFrameContinuationDecision,
    TaskFrameStatus,
    resolve_task_frame_continuation,
)
from nullion.task_planner import TaskPlanner
from nullion.runtime_status import (
    build_runtime_status_snapshot as build_runtime_status_snapshot_read_model,
    format_runtime_status_for_telegram as format_runtime_status_for_telegram_read_model,
    render_runtime_status_for_telegram as render_runtime_status_snapshot_for_telegram,
)
from nullion.reminders import ReminderRecord, normalize_reminder_due_at
from nullion.scheduler import (
    ScheduleKind,
    ScheduledTask,
    create_recurring_task,
    disable_task,
    mark_task_failed,
    mark_task_ran,
    should_run,
)
from nullion.sentinel_escalations import (
    SentinelEscalationArtifact,
    acknowledge_escalation,
    create_escalation_from_signal_route,
    link_escalation_to_approval,
    resolve_escalation,
)
from nullion.signals import SignalRoute, SignalTarget, route_health_alert, route_policy_decision
from nullion.skill_planner import (
    SkillExecutionPlan,
    build_skill_execution_intent_snapshot,
    build_skill_execution_plan,
    build_skill_execution_plan_snapshot,
    format_skill_execution_plan_for_telegram as format_skill_execution_plan_for_telegram_read_model,
    transition_skill_execution_plan_for_mini_agent_status,
    transition_skill_execution_plan_for_progress,
    transition_skill_execution_plan_for_step_completion as transition_skill_execution_plan_for_step_completion_read_model,
)
from nullion.skills import SkillRecord, SkillRevision, SkillWorkflowSignal
from nullion.system_context import (
    CORE_FALLBACK_TOOL_NAMES,
    build_system_context_snapshot as build_system_context_snapshot_read_model,
    format_system_context_for_prompt as format_system_context_for_prompt_read_model,
)
from nullion.tools import (
    ToolExecutor,
    ToolInvocation,
    ToolRegistry,
    ToolResult,
    create_core_tool_registry as create_core_tool_registry_from_tools,
    create_default_tool_registry as create_default_tool_registry_from_tools,
    create_plugin_tool_registry as create_plugin_tool_registry_from_tools,
    create_extension_tool_registry as create_extension_tool_registry_from_tools,
    normalize_tool_status,
)
from nullion.tool_boundaries import extract_boundary_facts


logger = logging.getLogger(__name__)

BUILDER_PROPOSAL_COOLDOWN = timedelta(hours=1)
TRUSTED_MUTATION_ACTORS = frozenset({"runtime", "operator", "builder_reflector", "builder_auto"})
SKILL_RECOMMENDATION_SCORE = 4
STRONG_SKILL_APPLICATION_SCORE = 6
MISSION_STEP_STATUSES = frozenset({"pending", "running", "completed", "blocked", "failed", "skipped"})
MISSION_STEP_KINDS = frozenset({"plan", "tool", "mini_agent", "verify", "user_input", "approval", "external_wait"})
_UNSET = object()


def _require_trusted_mutation_actor(*, actor: str, action: str) -> str:
    if not isinstance(actor, str) or not actor.strip():
        raise PermissionError(f"untrusted mutation actor: {actor}")
    normalized_actor = actor.strip().lower()
    if normalized_actor not in TRUSTED_MUTATION_ACTORS:
        raise PermissionError(f"untrusted mutation actor: {actor}")
    return normalized_actor


@dataclass(slots=True)
class RuntimeResult:
    turn: AssistantTurn
    store: RuntimeStore


@dataclass(slots=True)
class ConversationMessageResult:
    envelope: ConversationEnvelope
    branch: ConversationBranch
    turn: ConversationTurn
    superseded_branch_id: str | None = None
    disposition_reason: str | None = None
    active_task_frame_id: str | None = None
    task_frame_continuation: TaskFrameContinuationDecision | None = None


@dataclass(slots=True)
class WorkerResultCommitResult:
    conversation_id: str
    branch_id: str
    turn_id: str
    idempotency_key: str
    committed: bool
    reason: str | None = None


@dataclass(slots=True, weakref_slot=True, eq=False)
class PersistentRuntime:
    store: RuntimeStore
    checkpoint_path: Path
    started_at: datetime
    last_checkpoint_fingerprint: str | None = None
    live_tool_registry: ToolRegistry | None = None
    model_client: Any | None = None

    @property
    def active_tool_registry(self) -> ToolRegistry | None:
        return self.live_tool_registry

    @property
    def chat_history(self) -> dict[str, list[dict[str, str]]]:
        history: dict[str, list[dict[str, str]]] = {}
        seen_conversation_ids: set[str] = set()
        for event in self.store.list_conversation_events():
            conversation_id = event.get("conversation_id")
            if not isinstance(conversation_id, str) or not conversation_id or conversation_id in seen_conversation_ids:
                continue
            seen_conversation_ids.add(conversation_id)
            chat_turns = self.list_conversation_chat_turns(conversation_id)
            if chat_turns:
                history[conversation_id] = chat_turns
        return history

    @active_tool_registry.setter
    def active_tool_registry(self, value: ToolRegistry | None) -> None:
        self.live_tool_registry = value

    def checkpoint(self) -> Path:
        fingerprint = render_runtime_store_payload_json(self.store)
        if fingerprint == self.last_checkpoint_fingerprint and self.checkpoint_path.exists():
            return self.checkpoint_path
        saved_path = checkpoint_runtime_store(self.store, self.checkpoint_path)
        self.last_checkpoint_fingerprint = render_runtime_store_payload_json(self.store)
        return saved_path

    def list_backups(self) -> list[dict[str, object]]:
        return list_runtime_store_backups(self.checkpoint_path)

    def latest_restore_metadata(self) -> dict[str, object] | None:
        return get_latest_runtime_restore_metadata(self.store)

    def list_conversation_chat_turns(self, conversation_id: str) -> list[dict[str, str]]:
        return list_conversation_chat_turns(self.store, conversation_id)

    def render_status_for_telegram(
        self,
        *,
        capsule_id: str | None = None,
        active_only: bool = False,
    ) -> str:
        return render_runtime_status(
            self.store,
            capsule_id=capsule_id,
            active_only=active_only,
            checkpoint_path=self.checkpoint_path,
        )

    def render_status(
        self,
        *,
        capsule_id: str | None = None,
        active_only: bool = False,
    ) -> str:
        return render_runtime_status(
            self.store,
            capsule_id=capsule_id,
            active_only=active_only,
            checkpoint_path=self.checkpoint_path,
        )

    def restore_from_backup(self, *, generation: int = 0) -> RuntimeStore:
        self.store = restore_runtime_store_checkpoint(self.checkpoint_path, generation=generation)
        self.checkpoint()
        return self.store

    def run_request(self, **kwargs) -> RuntimeResult:
        result = run_request(self.store, tool_registry=self.active_tool_registry, **kwargs)
        self.checkpoint()
        return result

    def process_conversation_message(self, **kwargs) -> ConversationMessageResult:
        result = process_conversation_message(self.store, **kwargs)
        self.checkpoint()
        return result

    def create_worker_result_envelope(self, **kwargs) -> WorkerResultEnvelope:
        return create_worker_result_envelope(self.store, **kwargs)

    def create_mini_agent_worker_result_envelope(self, **kwargs) -> WorkerResultEnvelope:
        return create_mini_agent_worker_result_envelope(self.store, **kwargs)

    def commit_worker_result(self, **kwargs) -> WorkerResultCommitResult:
        result = commit_worker_result(self.store, **kwargs)
        self.checkpoint()
        return result

    def schedule_heartbeat(self, *, capsule_id: str, interval_minutes: int):
        scheduled_task = schedule_heartbeat(self.store, capsule_id=capsule_id, interval_minutes=interval_minutes)
        self.checkpoint()
        return scheduled_task

    def schedule_reminder(self, *, chat_id: str, text: str, due_at: datetime):
        scheduled_task = schedule_reminder(self.store, chat_id=chat_id, text=text, due_at=due_at)
        self.checkpoint()
        return scheduled_task

    def report_health_issue(
        self,
        *,
        issue_type: HealthIssueType,
        source: str,
        message: str,
        details: dict[str, object] | None = None,
    ) -> SignalRoute:
        route = report_health_issue(
            self.store,
            issue_type=issue_type,
            source=source,
            message=message,
            details=details,
        )
        self.checkpoint()
        return route

    def record_policy_signal(
        self,
        *,
        decision: PolicyDecision,
        action: str,
        resource: str,
    ) -> SignalRoute:
        route = record_policy_signal(
            self.store,
            decision=decision,
            action=action,
            resource=resource,
        )
        self.checkpoint()
        return route

    def start_doctor_action(self, action_id: str) -> dict[str, str | None]:
        action = start_doctor_action(self.store, action_id)
        self.checkpoint()
        return action

    def complete_doctor_action(self, action_id: str) -> dict[str, str | None]:
        action = complete_doctor_action(self.store, action_id)
        self.checkpoint()
        return action

    def cancel_doctor_action(self, action_id: str, *, reason: str) -> dict[str, str | None]:
        action = cancel_doctor_action(self.store, action_id, reason=reason)
        self.checkpoint()
        return action

    def fail_doctor_action(self, action_id: str, *, error: str) -> dict[str, str | None]:
        action = fail_doctor_action(self.store, action_id, error=error)
        self.checkpoint()
        return action

    def acknowledge_sentinel_escalation(self, escalation_id: str) -> SentinelEscalationArtifact:
        escalation = acknowledge_sentinel_escalation(self.store, escalation_id)
        self.checkpoint()
        return escalation

    def acknowledge_sentinel_escalation_for_approval(self, approval_id: str) -> SentinelEscalationArtifact:
        escalation = acknowledge_sentinel_escalation_for_approval(self.store, approval_id)
        self.checkpoint()
        return escalation

    def resolve_sentinel_escalation(self, escalation_id: str) -> SentinelEscalationArtifact:
        escalation = resolve_sentinel_escalation(self.store, escalation_id)
        self.checkpoint()
        return escalation

    def resolve_sentinel_escalation_for_approval(self, approval_id: str) -> SentinelEscalationArtifact:
        escalation = resolve_sentinel_escalation_for_approval(self.store, approval_id)
        self.checkpoint()
        return escalation

    def approve_approval_request(
        self,
        approval_id: str,
        *,
        principal_id: str,
        permissions: Iterable[str],
        actor: str = "operator",
        boundary_allow_once_selector: str | None = None,
        boundary_allow_once_uses: int | None = None,
        boundary_always_allow_selector: str | None = None,
        boundary_kind: BoundaryKind | None = None,
        expires_at: datetime | None = None,
        reason: str | None = None,
    ) -> ApprovalRequest:
        approval = approve_approval_request(
            self.store,
            approval_id,
            principal_id=principal_id,
            permissions=permissions,
            actor=actor,
            boundary_allow_once_selector=boundary_allow_once_selector,
            boundary_allow_once_uses=boundary_allow_once_uses,
            boundary_always_allow_selector=boundary_always_allow_selector,
            boundary_kind=boundary_kind,
            expires_at=expires_at,
            reason=reason,
        )
        self.checkpoint()
        return approval

    def deny_approval_request(
        self,
        approval_id: str,
        *,
        actor: str = "operator",
        reason: str | None = None,
    ) -> ApprovalRequest:
        approval = deny_approval_request(
            self.store,
            approval_id,
            actor=actor,
            reason=reason,
        )
        self.checkpoint()
        return approval

    def reconcile_effectively_approved_pending_approvals(
        self,
        *,
        actor: str = "runtime",
    ) -> list[ApprovalRequest]:
        approvals = reconcile_effectively_approved_pending_approvals(self.store, actor=actor)
        if approvals:
            self.checkpoint()
        return approvals

    def revoke_permission_grant(
        self,
        grant_id: str,
        *,
        actor: str = "operator",
        reason: str | None = None,
    ) -> PermissionGrant:
        grant = revoke_permission_grant(
            self.store,
            grant_id,
            actor=actor,
            reason=reason,
        )
        self.checkpoint()
        return grant

    def create_skill(
        self,
        *,
        title: str,
        summary: str,
        trigger: str,
        steps: list[str],
        tags: list[str] | None = None,
        skill_id: str | None = None,
        actor: str = "runtime",
    ) -> SkillRecord:
        skill = create_skill(
            self.store,
            title=title,
            summary=summary,
            trigger=trigger,
            steps=steps,
            tags=tags,
            skill_id=skill_id,
            actor=actor,
        )
        self.checkpoint()
        return skill

    def create_mission(
        self,
        *,
        owner: str,
        title: str,
        goal: str,
        continuation_policy: MissionContinuationPolicy = MissionContinuationPolicy.MANUAL,
        mission_id: str | None = None,
        created_from_capsule_id: str | None = None,
        active_capsule_id: str | None = None,
        active_step_id: str | None = None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = create_mission(
            self.store,
            owner=owner,
            title=title,
            goal=goal,
            continuation_policy=continuation_policy,
            mission_id=mission_id,
            created_from_capsule_id=created_from_capsule_id,
            active_capsule_id=active_capsule_id,
            active_step_id=active_step_id,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def get_mission(self, mission_id: str) -> MissionRecord | None:
        return get_mission(self.store, mission_id)

    def list_missions(self) -> list[MissionRecord]:
        return list_missions(self.store)

    def set_mission_plan(
        self,
        mission_id: str,
        *,
        steps: Iterable[MissionStep],
        completion_checklist: Iterable[MissionChecklistItem] | None = None,
        active_step_id: str | None = None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = set_mission_plan(
            self.store,
            mission_id,
            steps=steps,
            completion_checklist=completion_checklist,
            active_step_id=active_step_id,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def plan_mission(
        self,
        *,
        owner: str,
        user_message: str,
        active_task_frame: TaskFrame | None = None,
        mission_id: str | None = None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = plan_mission(
            self.store,
            owner=owner,
            user_message=user_message,
            active_task_frame=active_task_frame,
            mission_id=mission_id,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def mark_mission_step_running(
        self,
        mission_id: str,
        *,
        step_id: str,
        notes: str | None = None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = mark_mission_step_running(
            self.store,
            mission_id,
            step_id=step_id,
            notes=notes,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def mark_mission_step_completed(
        self,
        mission_id: str,
        *,
        step_id: str,
        notes: str | None = None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = mark_mission_step_completed(
            self.store,
            mission_id,
            step_id=step_id,
            notes=notes,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def mark_mission_step_failed(
        self,
        mission_id: str,
        *,
        step_id: str,
        notes: str | None = None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = mark_mission_step_failed(
            self.store,
            mission_id,
            step_id=step_id,
            notes=notes,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def mark_mission_step_blocked(
        self,
        mission_id: str,
        *,
        step_id: str,
        notes: str | None = None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = mark_mission_step_blocked(
            self.store,
            mission_id,
            step_id=step_id,
            notes=notes,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def link_mini_agent_run_to_mission_step(
        self,
        mission_id: str,
        *,
        step_id: str,
        mini_agent_run_id: str,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = link_mini_agent_run_to_mission_step(
            self.store,
            mission_id,
            step_id=step_id,
            mini_agent_run_id=mini_agent_run_id,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def set_mission_checklist_item_satisfied(
        self,
        mission_id: str,
        *,
        item_id: str,
        satisfied: bool,
        details: str | None = None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = set_mission_checklist_item_satisfied(
            self.store,
            mission_id,
            item_id=item_id,
            satisfied=satisfied,
            details=details,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def update_mission_step_notes(
        self,
        mission_id: str,
        *,
        step_id: str,
        notes: str | None,
        actor: str = "runtime",
    ) -> MissionRecord:
        mission = update_mission_step_notes(
            self.store,
            mission_id,
            step_id=step_id,
            notes=notes,
            actor=actor,
        )
        self.checkpoint()
        return mission

    def advance_mission(self, mission_id: str, *, actor: str = "runtime") -> MissionRecord:
        mission = advance_mission(self.store, mission_id, actor=actor)
        self.checkpoint()
        return mission

    def update_skill(
        self,
        skill_id: str,
        *,
        title: str | None = None,
        summary: str | None = None,
        trigger: str | None = None,
        steps: list[str] | None = None,
        tags: list[str] | None = None,
        actor: str = "runtime",
    ) -> SkillRecord:
        skill = update_skill(
            self.store,
            skill_id,
            title=title,
            summary=summary,
            trigger=trigger,
            steps=steps,
            tags=tags,
            actor=actor,
        )
        self.checkpoint()
        return skill

    def promote_skill_replacement(
        self,
        skill_id: str,
        *,
        replacement_summary: str,
        replacement_trigger: str,
        replacement_steps: list[str],
        failed_workflow_summary: str,
        successful_workflow_summary: str,
        replacement_title: str | None = None,
        replacement_tags: list[str] | None = None,
        actor: str = "builder_auto",
    ) -> SkillRecord:
        skill = promote_skill_replacement(
            self.store,
            skill_id,
            replacement_summary=replacement_summary,
            replacement_trigger=replacement_trigger,
            replacement_steps=replacement_steps,
            failed_workflow_summary=failed_workflow_summary,
            successful_workflow_summary=successful_workflow_summary,
            replacement_title=replacement_title,
            replacement_tags=replacement_tags,
            actor=actor,
        )
        self.checkpoint()
        return skill

    def accept_builder_skill_proposal(
        self,
        proposal: BuilderProposal,
        *,
        trigger: str | None = None,
        steps: list[str] | None = None,
        tags: list[str] | None = None,
        title: str | None = None,
        skill_id: str | None = None,
        actor: str = "runtime",
    ) -> SkillRecord:
        skill = accept_builder_skill_proposal(
            self.store,
            proposal,
            trigger=trigger,
            steps=steps,
            tags=tags,
            title=title,
            skill_id=skill_id,
            actor=actor,
        )
        self.checkpoint()
        return skill

    def store_builder_proposal(
        self,
        proposal: BuilderProposal,
        *,
        proposal_id: str | None = None,
        actor: str = "runtime",
    ) -> BuilderProposalRecord:
        record = store_builder_proposal(self.store, proposal, proposal_id=proposal_id, actor=actor)
        self.checkpoint()
        return record

    def get_builder_proposal(self, proposal_id: str) -> BuilderProposalRecord | None:
        return get_builder_proposal(self.store, proposal_id)

    def list_builder_proposals(self) -> list[BuilderProposalRecord]:
        return list_builder_proposals(self.store)

    def list_pending_builder_proposals(self) -> list[BuilderProposalRecord]:
        return list_pending_builder_proposals(self.store)

    def accept_stored_builder_skill_proposal(self, proposal_id: str, *, actor: str = "runtime") -> SkillRecord:
        skill = accept_stored_builder_skill_proposal(self.store, proposal_id, actor=actor)
        self.checkpoint()
        return skill

    def reject_stored_builder_proposal(self, proposal_id: str, *, actor: str = "runtime") -> BuilderProposalRecord:
        proposal = reject_stored_builder_proposal(self.store, proposal_id, actor=actor)
        self.checkpoint()
        return proposal

    def archive_stored_builder_proposal(self, proposal_id: str, *, actor: str = "runtime") -> BuilderProposalRecord:
        proposal = archive_stored_builder_proposal(self.store, proposal_id, actor=actor)
        self.checkpoint()
        return proposal

    def get_skill(self, skill_id: str) -> SkillRecord | None:
        return get_skill(self.store, skill_id)

    def list_skills(self) -> list[SkillRecord]:
        return list_skills(self.store)

    def initialize_skill_execution_plan(self, capsule_id: str, skill_id: str) -> SkillExecutionPlan:
        plan = initialize_skill_execution_plan(self.store, capsule_id=capsule_id, skill_id=skill_id)
        self.checkpoint()
        return plan

    def get_skill_execution_plan(self, capsule_id: str) -> SkillExecutionPlan | None:
        return get_skill_execution_plan(self.store, capsule_id)

    def transition_skill_execution_plan_for_mini_agent_run(
        self,
        *,
        capsule_id: str,
        mini_agent_status: MiniAgentRunStatus,
    ) -> SkillExecutionPlan | None:
        plan = transition_skill_execution_plan_for_mini_agent_run(
            self.store,
            capsule_id=capsule_id,
            mini_agent_status=mini_agent_status,
        )
        if plan is not None:
            self.checkpoint()
        return plan

    def transition_skill_execution_plan_for_progress_update(
        self,
        *,
        capsule_id: str,
        progress_state: ProgressState,
    ) -> SkillExecutionPlan | None:
        plan = transition_skill_execution_plan_for_progress_update(
            self.store,
            capsule_id=capsule_id,
            progress_state=progress_state,
        )
        if plan is not None:
            self.checkpoint()
        return plan

    def transition_skill_execution_plan_for_step_completion(self, *, capsule_id: str) -> SkillExecutionPlan | None:
        plan = transition_skill_execution_plan_for_step_completion(self.store, capsule_id=capsule_id)
        if plan is not None:
            self.checkpoint()
        return plan

    def sync_mission_from_skill_execution_plan(self, capsule_id: str) -> MissionRecord | None:
        mission = sync_mission_from_skill_execution_plan(self.store, capsule_id=capsule_id)
        if mission is not None:
            self.checkpoint()
        return mission

    def start_mini_agent_run(
        self,
        *,
        run_id: str,
        capsule_id: str,
        mini_agent_type: str,
        created_at: datetime,
        conversation_id: str | None = None,
        turn_id: str | None = None,
        branch_id: str | None = None,
    ) -> MiniAgentRun:
        run = start_mini_agent_run(
            self.store,
            run_id=run_id,
            capsule_id=capsule_id,
            mini_agent_type=mini_agent_type,
            created_at=created_at,
            conversation_id=conversation_id,
            turn_id=turn_id,
            branch_id=branch_id,
        )
        self.checkpoint()
        return run

    def mark_mini_agent_run_running(self, run_id: str) -> MiniAgentRun:
        run = mark_mini_agent_run_running(self.store, run_id)
        self.checkpoint()
        return run

    def complete_mini_agent_run(self, run_id: str, *, result_summary: str | None = None) -> MiniAgentRun:
        run = complete_mini_agent_run(self.store, run_id, result_summary=result_summary)
        self.checkpoint()
        return run

    def fail_mini_agent_run(self, run_id: str, *, result_summary: str | None = None) -> MiniAgentRun:
        run = fail_mini_agent_run(self.store, run_id, result_summary=result_summary)
        self.checkpoint()
        return run

    def reconcile_stale_mini_agent_runs(
        self,
        *,
        now: datetime | None = None,
        stale_after: timedelta | None = None,
        live_run_ids: Iterable[str] | None = None,
    ) -> list[MiniAgentRun]:
        runs = reconcile_stale_mini_agent_runs(
            self.store,
            now=now,
            stale_after=stale_after,
            live_run_ids=live_run_ids,
        )
        if runs:
            self.checkpoint()
        return runs

    def diagnose_runtime_health(
        self,
        *,
        now: datetime | None = None,
        stale_after: timedelta | None = None,
        live_mini_agent_run_ids: Iterable[str] | None = None,
        repair: bool = True,
    ) -> DoctorDiagnosisReport:
        report = diagnose_runtime_health(
            self.store,
            now=now,
            stale_after=stale_after,
            live_mini_agent_run_ids=live_mini_agent_run_ids,
            repair=repair,
        )
        self.checkpoint()
        return report

    def run_due_scheduled_tasks(self, *, now: datetime):
        due_tasks = run_due_scheduled_tasks(self.store, now=now)
        self.checkpoint()
        return due_tasks

    def record_progress_update(self, progress_update: ProgressUpdate) -> ProgressUpdate | None:
        recorded = record_progress_update(self.store, progress_update)
        if recorded is not None:
            self.checkpoint()
        return recorded

def start_mini_agent_run(
    store: RuntimeStore,
    *,
    run_id: str,
    capsule_id: str,
    mini_agent_type: str,
    created_at: datetime,
    conversation_id: str | None = None,
    turn_id: str | None = None,
    branch_id: str | None = None,
) -> MiniAgentRun:
    run = create_mini_agent_run(
        run_id=run_id,
        capsule_id=capsule_id,
        mini_agent_type=mini_agent_type,
        created_at=created_at,
    )
    store.add_mini_agent_run(run)

    capsule = store.get_capsule(capsule_id)
    if capsule is not None:
        store.add_capsule(add_mini_agent(capsule, mini_agent_type))

    payload = _mini_agent_run_payload(run)
    store.add_event(make_event(event_type="mini_agent.run_started", actor="runtime", payload=payload))
    store.add_audit_record(make_audit_record(action="mini_agent.run_started", actor="runtime", details=payload))

    _record_mini_agent_conversation_task_link(
        store,
        run=run,
        conversation_id=conversation_id,
        turn_id=turn_id,
        branch_id=branch_id,
        created_at=created_at,
    )

    return run



def transition_mini_agent_run(
    store: RuntimeStore,
    run_id: str,
    *,
    new_status: MiniAgentRunStatus,
    result_summary: str | None = None,
) -> MiniAgentRun:
    existing = store.get_mini_agent_run(run_id)
    if existing is None:
        raise KeyError(run_id)

    updated = transition_mini_agent_run_status(
        existing,
        new_status,
        result_summary=result_summary,
    )
    store.add_mini_agent_run(updated)
    if store.get_skill_execution_plan(updated.capsule_id) is not None:
        transition_skill_execution_plan_for_mini_agent_run(
            store,
            capsule_id=updated.capsule_id,
            mini_agent_status=updated.status,
        )
    _sync_missions_for_mini_agent_run(store, updated)

    if updated.status in {MiniAgentRunStatus.COMPLETED, MiniAgentRunStatus.FAILED}:
        capsule = store.get_capsule(updated.capsule_id)
        if capsule is not None:
            store.add_capsule(remove_mini_agent(capsule, updated.mini_agent_type))

    event_type = {
        MiniAgentRunStatus.RUNNING: "mini_agent.run_running",
        MiniAgentRunStatus.COMPLETED: "mini_agent.run_completed",
        MiniAgentRunStatus.FAILED: "mini_agent.run_failed",
    }.get(updated.status, f"mini_agent.run_{updated.status.value}")
    payload = _mini_agent_run_payload(updated)
    store.add_event(make_event(event_type=event_type, actor="runtime", payload=payload))
    store.add_audit_record(make_audit_record(action=event_type, actor="runtime", details=payload))

    if updated.status in {MiniAgentRunStatus.COMPLETED, MiniAgentRunStatus.FAILED}:
        _auto_commit_mini_agent_terminal_worker_result(store, run=updated)

    return updated



def mark_mini_agent_run_running(store: RuntimeStore, run_id: str) -> MiniAgentRun:
    return transition_mini_agent_run(
        store,
        run_id=run_id,
        new_status=MiniAgentRunStatus.RUNNING,
    )



def complete_mini_agent_run(
    store: RuntimeStore,
    run_id: str,
    *,
    result_summary: str | None = None,
) -> MiniAgentRun:
    existing = store.get_mini_agent_run(run_id)
    if existing is not None and existing.status is MiniAgentRunStatus.PENDING:
        transition_mini_agent_run(
            store,
            run_id=run_id,
            new_status=MiniAgentRunStatus.RUNNING,
        )
    return transition_mini_agent_run(
        store,
        run_id=run_id,
        new_status=MiniAgentRunStatus.COMPLETED,
        result_summary=result_summary,
    )



def fail_mini_agent_run(
    store: RuntimeStore,
    run_id: str,
    *,
    result_summary: str | None = None,
) -> MiniAgentRun:
    existing = store.get_mini_agent_run(run_id)
    if existing is not None and existing.status is MiniAgentRunStatus.PENDING:
        transition_mini_agent_run(
            store,
            run_id=run_id,
            new_status=MiniAgentRunStatus.RUNNING,
        )
    return transition_mini_agent_run(
        store,
        run_id=run_id,
        new_status=MiniAgentRunStatus.FAILED,
        result_summary=result_summary,
    )


def reconcile_stale_mini_agent_runs(
    store: RuntimeStore,
    *,
    now: datetime | None = None,
    stale_after: timedelta | None = None,
    live_run_ids: Iterable[str] | None = None,
) -> list[MiniAgentRun]:
    """Fail orphaned active Mini-Agent records that can no longer make progress.

    Mini-Agent execution state is split between an in-memory dispatcher and
    persisted ``mini_agent_runs`` rows. If the dispatcher loop is lost, restarted,
    or fails before writing a terminal state, the persisted row can otherwise
    remain ``pending``/``running`` forever and keep the system looking busy.
    This reconciler is deliberately conservative: live dispatcher task IDs are
    protected, fresh rows are left alone, and each row is repaired independently.
    """

    observed_at = now or datetime.now(UTC)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    live_ids = {str(run_id) for run_id in (live_run_ids or []) if str(run_id)}
    stale_after = stale_after or mini_agent_stale_after()
    repaired: list[MiniAgentRun] = []
    active_statuses = {MiniAgentRunStatus.PENDING, MiniAgentRunStatus.RUNNING}

    for run in list(store.list_mini_agent_runs()):
        if run.status not in active_statuses or run.run_id in live_ids:
            continue
        created_at = run.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        age = observed_at - created_at
        if age < stale_after:
            continue

        summary = (
            "Auto-reconciled as failed: persisted Mini-Agent run was still "
            f"{run.status.value} after {int(age.total_seconds())}s with no live dispatcher owner."
        )
        try:
            repaired.append(fail_mini_agent_run(store, run.run_id, result_summary=summary))
        except Exception as exc:
            logger.warning(
                "Unable to reconcile stale Mini-Agent run %s: %s",
                run.run_id,
                exc,
                exc_info=True,
            )
            try:
                store.add_event(make_event(
                    event_type="mini_agent.reconcile_failed",
                    actor="runtime",
                    payload={
                        "run_id": run.run_id,
                        "capsule_id": run.capsule_id,
                        "status": run.status.value,
                        "error": str(exc),
                    },
                ))
            except Exception:
                logger.debug("Unable to record Mini-Agent reconciliation failure", exc_info=True)

    return repaired


@dataclass(frozen=True, slots=True)
class DoctorDiagnosisReport:
    checked_at: datetime
    stale_mini_agent_run_ids: tuple[str, ...]
    repaired_mini_agent_run_ids: tuple[str, ...]
    live_mini_agent_run_ids: tuple[str, ...]
    pending_doctor_actions: int
    recommendations: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return (
            not self.stale_mini_agent_run_ids
            and not self.repaired_mini_agent_run_ids
            and self.pending_doctor_actions == 0
        )

    @property
    def summary(self) -> str:
        if self.repaired_mini_agent_run_ids:
            count = len(self.repaired_mini_agent_run_ids)
            noun = "run" if count == 1 else "runs"
            return f"Auto-repaired {count} stale Mini-Agent {noun}."
        if self.stale_mini_agent_run_ids:
            count = len(self.stale_mini_agent_run_ids)
            noun = "run" if count == 1 else "runs"
            return f"Found {count} stale Mini-Agent {noun}."
        if self.pending_doctor_actions:
            count = self.pending_doctor_actions
            noun = "action" if count == 1 else "actions"
            return f"{count} Doctor {noun} still need attention."
        return "No stale Mini-Agent runs or pending Doctor actions found."

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "checked_at": self.checked_at.isoformat(),
            "summary": self.summary,
            "stale_mini_agent_run_ids": list(self.stale_mini_agent_run_ids),
            "repaired_mini_agent_run_ids": list(self.repaired_mini_agent_run_ids),
            "live_mini_agent_run_ids": list(self.live_mini_agent_run_ids),
            "pending_doctor_actions": self.pending_doctor_actions,
            "recommendations": list(self.recommendations),
        }


def diagnose_runtime_health(
    store: RuntimeStore,
    *,
    now: datetime | None = None,
    stale_after: timedelta | None = None,
    live_mini_agent_run_ids: Iterable[str] | None = None,
    repair: bool = True,
) -> DoctorDiagnosisReport:
    checked_at = now or datetime.now(UTC)
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=UTC)
    stale_after = stale_after or mini_agent_stale_after()
    live_ids = tuple(sorted({str(run_id) for run_id in (live_mini_agent_run_ids or []) if str(run_id)}))
    live_id_set = set(live_ids)
    active_statuses = {MiniAgentRunStatus.PENDING, MiniAgentRunStatus.RUNNING}
    stale_runs: list[MiniAgentRun] = []

    for run in store.list_mini_agent_runs():
        if run.status not in active_statuses or run.run_id in live_id_set:
            continue
        created_at = run.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        if checked_at - created_at >= stale_after:
            stale_runs.append(run)

    repaired_runs: list[MiniAgentRun] = []
    if repair and stale_runs:
        repaired_runs = reconcile_stale_mini_agent_runs(
            store,
            now=checked_at,
            stale_after=stale_after,
            live_run_ids=live_id_set,
        )

    open_action_statuses = {"pending", "in_progress"}
    pending_doctor_actions = sum(
        1
        for action in store.list_doctor_actions()
        if str(action.get("status") or "").lower() in open_action_statuses
    )
    recommendations: list[str] = []
    if repaired_runs:
        recommendations.append("Review /status to confirm the active task list is clear, then retry the original request only if it is still needed.")
    elif stale_runs:
        recommendations.append("Run Doctor diagnose with repair enabled or restart the dispatcher before retrying the affected request.")
    if pending_doctor_actions:
        recommendations.append("Review pending Doctor actions with /doctor and resolve or dismiss items that are no longer relevant.")
    if not recommendations:
        recommendations.append("No action needed.")

    report = DoctorDiagnosisReport(
        checked_at=checked_at,
        stale_mini_agent_run_ids=tuple(run.run_id for run in stale_runs),
        repaired_mini_agent_run_ids=tuple(run.run_id for run in repaired_runs),
        live_mini_agent_run_ids=live_ids,
        pending_doctor_actions=pending_doctor_actions,
        recommendations=tuple(recommendations),
    )
    store.add_event(make_event(event_type="doctor.diagnose_completed", actor="doctor", payload=report.as_dict()))
    store.add_audit_record(make_audit_record(action="doctor.diagnose_completed", actor="doctor", details=report.as_dict()))
    return report


def format_doctor_diagnosis_for_operator(report: DoctorDiagnosisReport) -> str:
    lines = ["🩺 Doctor diagnose", "", report.summary]
    if report.repaired_mini_agent_run_ids:
        lines.extend(["", "Repaired Mini-Agent runs:"])
        lines.extend(f"• {run_id}" for run_id in report.repaired_mini_agent_run_ids[:10])
    elif report.stale_mini_agent_run_ids:
        lines.extend(["", "Stale Mini-Agent runs:"])
        lines.extend(f"• {run_id}" for run_id in report.stale_mini_agent_run_ids[:10])
    if len(report.repaired_mini_agent_run_ids) > 10 or len(report.stale_mini_agent_run_ids) > 10:
        total = max(len(report.repaired_mini_agent_run_ids), len(report.stale_mini_agent_run_ids))
        lines.append(f"Showing 10 of {total}.")
    if report.pending_doctor_actions:
        noun = "action" if report.pending_doctor_actions == 1 else "actions"
        lines.extend(["", f"Pending Doctor: {report.pending_doctor_actions} {noun}"])
    if report.recommendations:
        lines.extend(["", "Recommendation:"])
        lines.extend(f"• {item}" for item in report.recommendations)
    return "\n".join(lines)


def doctor_recommendation_for_route(target: str, reason: str, severity: str) -> dict[str, str]:
    normalized_target = str(getattr(target, "value", target)).lower()
    recommendation_code = "review_doctor_route"
    summary = "Review the routed health issue and choose the next repair step."

    if normalized_target == SignalTarget.DOCTOR.value:
        if "missing_capsule" in reason:
            recommendation_code = "repair_missing_capsule_reference"
            summary = "Repair or remove the scheduled task because its capsule reference no longer exists."
        elif "timeout" in reason:
            recommendation_code = "investigate_timeout"
            summary = "Inspect the stalled workflow and retry only after timeout cause is understood."
        elif "stalled" in reason:
            recommendation_code = "investigate_stall"
            summary = "Inspect the stalled workflow and verify whether it should resume or be retired."

    return {
        "target": normalized_target,
        "severity": severity,
        "reason": reason,
        "recommendation_code": recommendation_code,
        "summary": summary,
    }



def _doctor_action_type_for_recommendation(recommendation_code: str) -> str:
    if recommendation_code.startswith("investigate_"):
        return "investigate"
    if recommendation_code.startswith("repair_"):
        return "repair"
    if recommendation_code.startswith("monitor_"):
        return "monitor"
    return "inspect"



def _doctor_action_from_record(record: dict[str, Any]) -> DoctorAction:
    return DoctorAction(
        action_id=str(record["action_id"]),
        recommendation_code=str(record["recommendation_code"]),
        status=str(record["status"]),
        summary=str(record["summary"]),
        severity=str(record["severity"]),
        owner=str(record["owner"]),
        source_reason=record.get("source_reason"),
        reason=record.get("reason"),
        error=record.get("error"),
    )



def _doctor_action_record(action: DoctorAction, *, action_type: str) -> dict[str, str | None]:
    return {
        "action_id": action.action_id,
        "owner": action.owner,
        "status": action.status,
        "action_type": action_type,
        "recommendation_code": action.recommendation_code,
        "summary": action.summary,
        "source_reason": action.source_reason,
        "reason": action.reason,
        "error": action.error,
        "severity": action.severity,
    }



def doctor_action_for_recommendation(recommendation: dict[str, str]) -> dict[str, str | None]:
    recommendation_code = recommendation["recommendation_code"]
    action_type = _doctor_action_type_for_recommendation(recommendation_code)
    action = create_doctor_action(
        recommendation_code=recommendation_code,
        summary=recommendation["summary"],
        severity=recommendation["severity"],
        owner="doctor",
        route_target=recommendation["target"],
        route_reason=recommendation["reason"],
    )

    # Carry the raw error text through so the telegram/web card can show it.
    error_text = recommendation.get("error") or action.error

    return _doctor_action_record(
        DoctorAction(
            action_id=action.action_id,
            recommendation_code=action.recommendation_code,
            status=action.status,
            summary=action.summary,
            severity=action.severity,
            owner=action.owner,
            source_reason=recommendation["reason"],
            reason=recommendation["reason"],
            error=error_text,
        ),
        action_type=action_type,
    )


def _uniquify_doctor_action_id(
    store: RuntimeStore,
    action_record: dict[str, str | None],
) -> dict[str, str | None]:
    candidate_id = str(action_record["action_id"])
    if store.get_doctor_action(candidate_id) is None:
        return action_record

    suffix = 2
    while store.get_doctor_action(f"{candidate_id}-{suffix}") is not None:
        suffix += 1

    unique = dict(action_record)
    unique["action_id"] = f"{candidate_id}-{suffix}"
    return unique


def _find_open_matching_doctor_action(
    store: RuntimeStore,
    action_record: dict[str, str | None],
) -> dict[str, Any] | None:
    terminal_statuses = {"completed", "cancelled", "failed", "dismissed", "resolved"}
    fingerprint_fields = (
        "owner",
        "action_type",
        "recommendation_code",
        "summary",
        "severity",
        "source_reason",
        "reason",
        "error",
    )

    for existing in store.list_doctor_actions():
        if str(existing.get("status", "")).lower() in terminal_statuses:
            continue
        if all(existing.get(field) == action_record.get(field) for field in fingerprint_fields):
            return existing
    return None


def _uniquify_sentinel_escalation_id(
    store: RuntimeStore,
    escalation: SentinelEscalationArtifact,
) -> SentinelEscalationArtifact:
    candidate_id = escalation.escalation_id
    if store.get_sentinel_escalation(candidate_id) is None:
        return escalation

    suffix = 2
    while store.get_sentinel_escalation(f"{candidate_id}-{suffix}") is not None:
        suffix += 1

    return replace(escalation, escalation_id=f"{candidate_id}-{suffix}")


def _transition_doctor_action(
    store: RuntimeStore,
    action_id: str,
    transition: Any,
) -> dict[str, str | None]:
    stored = store.get_doctor_action_object(action_id)
    if stored is None:
        raise KeyError(action_id)

    action, action_type = stored
    updated_action = transition(action)
    store.update_doctor_action_object(updated_action, action_type=action_type)
    updated = _doctor_action_record(updated_action, action_type=action_type)

    event_type = {
        "in_progress": "doctor.action_started",
        "completed": "doctor.action_completed",
        "cancelled": "doctor.action_cancelled",
        "failed": "doctor.action_failed",
    }.get(str(updated["status"]), f"doctor.action_{updated['status']}")
    payload = {
        "action_id": updated["action_id"],
        "status": str(updated["status"]),
        "action_type": str(updated["action_type"]),
        "recommendation_code": str(updated["recommendation_code"]),
    }
    store.add_event(make_event(event_type=event_type, actor="doctor", payload=payload))
    store.add_audit_record(make_audit_record(action=event_type, actor="doctor", details=payload))
    return updated



def start_doctor_action(store: RuntimeStore, action_id: str) -> dict[str, str | None]:
    stored = store.get_doctor_action_object(action_id)
    if stored is None:
        raise KeyError(action_id)

    action, action_type = stored
    if action.status == IN_PROGRESS:
        return _doctor_action_record(action, action_type=action_type)

    return _transition_doctor_action(store, action_id, start_action)



def complete_doctor_action(store: RuntimeStore, action_id: str) -> dict[str, str | None]:
    stored = store.get_doctor_action_object(action_id)
    if stored is None:
        raise KeyError(action_id)

    action, _action_type = stored
    if action.status == PENDING:
        _transition_doctor_action(store, action_id, start_action)
    return _transition_doctor_action(store, action_id, complete_action)



def cancel_doctor_action(
    store: RuntimeStore,
    action_id: str,
    *,
    reason: str | None = None,
) -> dict[str, str | None]:
    return _transition_doctor_action(
        store,
        action_id,
        lambda action: cancel_action(action, reason=reason),
    )



def fail_doctor_action(
    store: RuntimeStore,
    action_id: str,
    *,
    error: str | None = None,
) -> dict[str, str | None]:
    return _transition_doctor_action(
        store,
        action_id,
        lambda action: fail_action(action, error=error),
    )


def _transition_sentinel_escalation(
    store: RuntimeStore,
    escalation_id: str,
    transition: Any,
) -> SentinelEscalationArtifact:
    escalation = store.get_sentinel_escalation(escalation_id)
    if escalation is None:
        raise KeyError(escalation_id)

    updated = transition(escalation)
    store.update_sentinel_escalation(updated)

    event_type = f"sentinel.escalation_{updated.status.value}"
    payload = {
        "escalation_id": updated.escalation_id,
        "status": updated.status.value,
        "severity": updated.severity,
        "approval_id": updated.approval_id,
    }
    store.add_event(make_event(event_type=event_type, actor="sentinel", payload=payload))
    store.add_audit_record(make_audit_record(action=event_type, actor="sentinel", details=payload))
    return updated


def _transition_sentinel_escalation_for_approval(
    store: RuntimeStore,
    approval_id: str,
    transition: Any,
) -> SentinelEscalationArtifact:
    escalation = store.get_sentinel_escalation_by_approval_id(approval_id)
    if escalation is None:
        raise KeyError(approval_id)
    return _transition_sentinel_escalation(store, escalation.escalation_id, transition)


def acknowledge_sentinel_escalation(
    store: RuntimeStore,
    escalation_id: str,
) -> SentinelEscalationArtifact:
    return _transition_sentinel_escalation(store, escalation_id, acknowledge_escalation)


def acknowledge_sentinel_escalation_for_approval(
    store: RuntimeStore,
    approval_id: str,
) -> SentinelEscalationArtifact:
    return _transition_sentinel_escalation_for_approval(store, approval_id, acknowledge_escalation)


def resolve_sentinel_escalation(
    store: RuntimeStore,
    escalation_id: str,
) -> SentinelEscalationArtifact:
    return _transition_sentinel_escalation(store, escalation_id, resolve_escalation)


def resolve_sentinel_escalation_for_approval(
    store: RuntimeStore,
    approval_id: str,
) -> SentinelEscalationArtifact:
    return _transition_sentinel_escalation_for_approval(store, approval_id, resolve_escalation)


def _approval_resource_matches_identifier(*, resource: str, identifier: str | None) -> bool:
    if not isinstance(identifier, str):
        return False
    normalized_identifier = identifier.strip().lower()
    if not normalized_identifier:
        return False
    normalized_resource = resource.strip().lower()
    if not normalized_resource:
        return False
    if normalized_identifier in normalized_resource:
        return True
    resource_tokens = {token for token in re.split(r"[^a-z0-9_.-]+", normalized_resource) if token}
    return normalized_identifier in resource_tokens


def _find_linked_mission_approval_step(
    mission: MissionRecord,
    *,
    approval: ApprovalRequest,
) -> MissionStep | None:
    open_approval_steps = [
        step
        for step in mission.steps
        if step.kind == "approval" and step.status in {"pending", "running", "blocked"}
    ]
    if not open_approval_steps:
        return None

    if mission.active_step_id is not None:
        for step in open_approval_steps:
            if step.step_id == mission.active_step_id:
                return step

    for step in open_approval_steps:
        if _approval_resource_matches_identifier(resource=approval.resource, identifier=step.capsule_id):
            return step

    return None


def _mission_is_linked_to_approval_resource(
    mission: MissionRecord,
    *,
    approval: ApprovalRequest,
) -> bool:
    return any(
        _approval_resource_matches_identifier(resource=approval.resource, identifier=identifier)
        for identifier in (mission.active_capsule_id, mission.created_from_capsule_id)
    )


def _set_mission_step_status(
    store: RuntimeStore,
    mission: MissionRecord,
    *,
    step_id: str,
    new_status: str,
    notes: str | None,
) -> MissionRecord:
    updated_steps: list[MissionStep] = []
    for step in mission.steps:
        if step.step_id != step_id:
            updated_steps.append(step)
            continue
        updated_steps.append(replace(step, status=new_status, notes=notes if notes is not None else step.notes))
    updated = replace(mission, steps=tuple(updated_steps), updated_at=datetime.now(UTC))
    store.add_mission(updated)
    return updated


def _sync_missions_for_approval_decision(store: RuntimeStore, approval: ApprovalRequest) -> None:
    for mission in store.list_missions():
        linked_step = _find_linked_mission_approval_step(mission, approval=approval)
        is_linked_by_resource = _mission_is_linked_to_approval_resource(mission, approval=approval)
        if linked_step is None or not is_linked_by_resource:
            continue

        if approval.status is ApprovalStatus.APPROVED:
            updated_mission = _set_mission_step_status(
                store,
                mission,
                step_id=linked_step.step_id,
                new_status="completed",
                notes=linked_step.notes,
            )
            advance_mission(store, updated_mission.mission_id, actor="runtime")
            continue

        if approval.status is ApprovalStatus.DENIED:
            denial_summary = approval.decision_reason or linked_step.notes or linked_step.title
            updated_mission = _set_mission_step_status(
                store,
                mission,
                step_id=linked_step.step_id,
                new_status="failed",
                notes=denial_summary,
            )
            transition_mission_status(
                store,
                updated_mission.mission_id,
                new_status=MissionStatus.FAILED,
                terminal_reason=MissionTerminalReason.APPROVAL_DENIED,
                result_summary=denial_summary,
                active_step_id=linked_step.step_id,
                actor="runtime",
            )


def _boundary_permission_principal(principal_id: str | None, boundary_kind: BoundaryKind | None) -> str:
    if boundary_kind is BoundaryKind.FILESYSTEM_ACCESS:
        return permission_scope_principal(principal_id)
    return GLOBAL_PERMISSION_PRINCIPAL


def _add_permission_grant_replacing_active_duplicates(
    store: RuntimeStore,
    grant: PermissionGrant,
    *,
    actor: str,
    revoked_at: datetime,
) -> list[PermissionGrant]:
    replaced: list[PermissionGrant] = []
    for existing in store.list_permission_grants():
        if existing.grant_id == grant.grant_id:
            continue
        if existing.principal_id != grant.principal_id or existing.permission != grant.permission:
            continue
        if not is_permission_grant_active(existing, now=revoked_at):
            continue
        revoked = revoke_permission_grant_record(
            existing,
            revoked_by=actor,
            revoked_at=revoked_at,
            reason="Replaced by newer permission grant",
        )
        store.add_permission_grant(revoked)
        replaced.append(revoked)
    store.add_permission_grant(grant)
    return replaced


def _active_boundary_permit_duplicate(
    store: RuntimeStore,
    permit: BoundaryPermit,
    *,
    now: datetime,
) -> BoundaryPermit | None:
    for existing in store.list_boundary_permits():
        if existing.permit_id == permit.permit_id:
            continue
        if existing.principal_id != permit.principal_id:
            continue
        if existing.boundary_kind is not permit.boundary_kind:
            continue
        if existing.selector != permit.selector:
            continue
        if is_boundary_permit_active(existing, now=now):
            return existing
    return None


def _active_boundary_rule_duplicate(
    store: RuntimeStore,
    rule: BoundaryPolicyRule,
    *,
    now: datetime,
) -> BoundaryPolicyRule | None:
    for existing in store.list_boundary_policy_rules():
        if existing.rule_id == rule.rule_id:
            continue
        if existing.principal_id != rule.principal_id:
            continue
        if existing.kind is not rule.kind:
            continue
        if existing.mode is not rule.mode:
            continue
        if existing.selector != rule.selector:
            continue
        if existing.revoked_at is not None and existing.revoked_at <= now:
            continue
        if existing.expires_at is not None and existing.expires_at <= now:
            continue
        return existing
    return None


def _approval_permission_candidates(approval: ApprovalRequest) -> tuple[str, ...]:
    if approval.action == "use_tool":
        tool_name = approval.resource
        context = approval.context if isinstance(approval.context, dict) else {}
        if isinstance(context.get("tool_name"), str) and context["tool_name"]:
            tool_name = str(context["tool_name"])
        return (f"tool:{tool_name}", f"tool.{tool_name}", tool_name)
    if approval.action and approval.resource:
        return (f"{approval.action}:{approval.resource}",)
    return ()


def _principal_aliases_for_permission(principal_id: str | None) -> set[str]:
    aliases = {
        str(principal_id or ""),
        permission_scope_principal(principal_id),
        OPERATOR_PERMISSION_PRINCIPAL,
        "operator",
        "workspace:workspace_admin",
    }
    return {alias for alias in aliases if alias}


def _active_boundary_rules_for_approval(
    store: RuntimeStore,
    approval: ApprovalRequest,
    *,
    boundary_kind: BoundaryKind,
    now: datetime,
) -> list[BoundaryPolicyRule]:
    principal_ids = _principal_aliases_for_permission(approval.requested_by)
    rules = [
        replace(rule, principal_id=permission_scope_principal(approval.requested_by))
        if rule.principal_id in {OPERATOR_PERMISSION_PRINCIPAL, "operator", "workspace:workspace_admin"}
        else rule
        for rule in store.list_boundary_policy_rules()
        if rule.principal_id in principal_ids
        and rule.kind is boundary_kind
        and rule.revoked_at is None
        and (rule.expires_at is None or rule.expires_at > now)
    ]
    for permit in store.list_boundary_permits():
        if permit.principal_id not in principal_ids:
            continue
        if permit.boundary_kind is not boundary_kind:
            continue
        if not is_boundary_permit_active(permit, now=now):
            continue
        rules.append(
            BoundaryPolicyRule(
                rule_id=f"permit:{permit.permit_id}",
                principal_id=permission_scope_principal(approval.requested_by),
                kind=permit.boundary_kind,
                mode=PolicyMode.ALLOW,
                selector=permit.selector,
                created_by=permit.granted_by,
                created_at=permit.granted_at,
                expires_at=permit.expires_at,
                revoked_at=permit.revoked_at,
                reason="boundary_permit",
            )
        )
    return rules


def _pending_approval_already_covered(
    store: RuntimeStore,
    approval: ApprovalRequest,
    *,
    now: datetime,
) -> bool:
    if approval.status is not ApprovalStatus.PENDING:
        return False
    permissions = set(_approval_permission_candidates(approval))
    if permissions:
        principal_ids = _principal_aliases_for_permission(approval.requested_by)
        for grant in store.list_permission_grants():
            if grant.principal_id in principal_ids and grant.permission in permissions and is_permission_grant_active(grant, now=now):
                return True
    if approval.request_kind != "boundary_policy" and approval.action != "allow_boundary":
        return False
    context = approval.context if isinstance(approval.context, dict) else {}
    try:
        boundary_kind = BoundaryKind(str(context.get("boundary_kind") or "outbound_network"))
    except ValueError:
        return False
    target = str(context.get("target") or approval.resource or "").strip()
    if not target:
        return False
    fact = BoundaryFact(
        kind=boundary_kind,
        tool_name=str(context.get("tool_name") or approval.action or "approval"),
        operation=str(context.get("operation") or approval.action or "allow_boundary"),
        target=target,
        attributes={},
    )
    request = BoundaryPolicyRequest(
        principal_id=permission_scope_principal(approval.requested_by),
        boundary=fact,
    )
    return evaluate_boundary_request(
        request,
        rules=_active_boundary_rules_for_approval(store, approval, boundary_kind=boundary_kind, now=now),
    ) is PolicyDecision.ALLOW


def reconcile_effectively_approved_pending_approvals(
    store: RuntimeStore,
    *,
    actor: str = "runtime",
    now: datetime | None = None,
) -> list[ApprovalRequest]:
    """Approve pending rows that are already covered by active grants or boundary policy."""
    decided_at = now or datetime.now(UTC)
    reconciled: list[ApprovalRequest] = []
    for approval in store.list_approval_requests():
        if not _pending_approval_already_covered(store, approval, now=decided_at):
            continue
        updated = approve(
            approval,
            decided_by=actor,
            decided_at=decided_at,
            reason="Already covered by active permission or boundary policy",
        )
        store.add_approval_request(updated)
        reconciled.append(updated)
        payload = {
            "approval_id": updated.approval_id,
            "status": updated.status.value,
            "requested_by": updated.requested_by,
            "action": updated.action,
            "resource": updated.resource,
            "decided_by": updated.decided_by,
            "decision_reason": updated.decision_reason,
            "reconciled": True,
        }
        store.add_event(make_event(event_type="approval.request_reconciled", actor=actor, payload=payload))
        store.add_audit_record(make_audit_record(action="approval.request_reconciled", actor=actor, details=payload))
    return reconciled


class _ApprovalDecisionState(TypedDict, total=False):
    store: RuntimeStore
    approval_id: str
    actor: str
    normalized_actor: str
    transition: Any
    event_type: str
    reason: str | None
    principal_id: str | None
    permissions: tuple[str, ...]
    boundary_allow_once_selector: str | None
    boundary_allow_once_uses: int | None
    boundary_always_allow_selector: str | None
    boundary_kind: BoundaryKind | None
    expires_at: datetime | None
    approval: ApprovalRequest
    updated: ApprovalRequest
    context_tool_name: str | None
    context_permission_scope: str | None
    permission_principal: str | None
    grants: list[PermissionGrant]
    boundary_permits: list[BoundaryPermit]
    boundary_rules: list[BoundaryPolicyRule]


def _approval_decision_transition_node(state: _ApprovalDecisionState) -> dict[str, object]:
    normalized_actor = _require_trusted_mutation_actor(actor=state["actor"], action=state["event_type"])
    approval = state["store"].get_approval_request(state["approval_id"])
    if approval is None:
        raise KeyError(state["approval_id"])
    updated = state["transition"](
        approval,
        decided_by=normalized_actor,
        decided_at=datetime.now(UTC),
        reason=state.get("reason"),
    )
    state["store"].add_approval_request(updated)
    ctx = updated.context if isinstance(updated.context, dict) else {}
    context_tool_name = ctx.get("tool_name") if isinstance(ctx.get("tool_name"), str) else None
    context_permission_scope = ctx.get("tool_permission_scope") if isinstance(ctx.get("tool_permission_scope"), str) else None
    principal_id = state.get("principal_id")
    boundary_kind = state.get("boundary_kind")
    permission_principal = (
        _boundary_permission_principal(principal_id, boundary_kind)
        if boundary_kind is not None
        else permission_grant_principal(
            principal_id,
            tool_name=context_tool_name,
            permission_scope=context_permission_scope,
        )
        if principal_id is not None
        else None
    )
    return {
        "normalized_actor": normalized_actor,
        "approval": approval,
        "updated": updated,
        "context_tool_name": context_tool_name,
        "context_permission_scope": context_permission_scope,
        "permission_principal": permission_principal,
        "grants": [],
        "boundary_permits": [],
        "boundary_rules": [],
    }


def _approval_decision_permission_grants_node(state: _ApprovalDecisionState) -> dict[str, object]:
    permission_principal = state.get("permission_principal")
    if permission_principal is None:
        return {"grants": []}
    store = state["store"]
    updated = state["updated"]
    grants: list[PermissionGrant] = []
    for permission in state.get("permissions") or ():
        grant_principal = permission_grant_principal(
            state.get("principal_id"),
            permission=permission,
            tool_name=state.get("context_tool_name"),
            permission_scope=state.get("context_permission_scope"),
        )
        grant = create_permission_grant(
            approval_id=updated.approval_id,
            principal_id=grant_principal,
            permission=permission,
            granted_by=state["normalized_actor"],
            granted_at=updated.decided_at,
            expires_at=state.get("expires_at"),
        )
        _add_permission_grant_replacing_active_duplicates(
            store,
            grant,
            actor=state["normalized_actor"],
            revoked_at=updated.decided_at,
        )
        grants.append(grant)
    return {"grants": grants}


def _approval_decision_allow_once_node(state: _ApprovalDecisionState) -> dict[str, object]:
    updated = state["updated"]
    permission_principal = state.get("permission_principal")
    selector = state.get("boundary_allow_once_selector")
    boundary_kind = state.get("boundary_kind")
    if (
        updated.status is not ApprovalStatus.APPROVED
        or permission_principal is None
        or not isinstance(selector, str)
        or not selector
        or boundary_kind is None
    ):
        return {"boundary_permits": []}
    permit_selector = (
        normalize_outbound_network_selector(selector)
        if boundary_kind is BoundaryKind.OUTBOUND_NETWORK and selector != "*"
        else selector
    )
    explicit_uses = state.get("boundary_allow_once_uses")
    permit = create_boundary_permit(
        approval_id=updated.approval_id,
        principal_id=permission_principal,
        boundary_kind=boundary_kind,
        selector=permit_selector,
        granted_by=state["normalized_actor"],
        granted_at=updated.decided_at,
        uses_remaining=explicit_uses if isinstance(explicit_uses, int) and explicit_uses > 0 else 1,
        expires_at=state.get("expires_at"),
    )
    if _active_boundary_permit_duplicate(state["store"], permit, now=updated.decided_at) is not None:
        return {"boundary_permits": []}
    state["store"].add_boundary_permit(permit)
    return {"boundary_permits": [permit]}


def _approval_decision_always_allow_node(state: _ApprovalDecisionState) -> dict[str, object]:
    updated = state["updated"]
    permission_principal = state.get("permission_principal")
    selector = state.get("boundary_always_allow_selector")
    boundary_kind = state.get("boundary_kind")
    if (
        updated.status is not ApprovalStatus.APPROVED
        or permission_principal is None
        or not isinstance(selector, str)
        or not selector
        or boundary_kind is None
    ):
        return {"boundary_rules": []}
    rule_selector = (
        normalize_outbound_network_selector(selector)
        if boundary_kind is BoundaryKind.OUTBOUND_NETWORK
        else selector
    )
    rule = BoundaryPolicyRule(
        rule_id=f"rule-{uuid4().hex[:12]}",
        principal_id=permission_principal,
        kind=boundary_kind,
        mode=PolicyMode.ALLOW,
        selector=rule_selector,
        created_by=state["normalized_actor"],
        created_at=updated.decided_at,
        reason=updated.decision_reason,
        expires_at=state.get("expires_at"),
    )
    if _active_boundary_rule_duplicate(state["store"], rule, now=updated.decided_at) is not None:
        return {"boundary_rules": []}
    state["store"].add_boundary_policy_rule(rule)
    return {"boundary_rules": [rule]}


def _approval_decision_sync_missions_node(state: _ApprovalDecisionState) -> dict[str, object]:
    _sync_missions_for_approval_decision(state["store"], state["updated"])
    return {}


def _approval_decision_record_event_node(state: _ApprovalDecisionState) -> dict[str, object]:
    updated = state["updated"]
    grants = list(state.get("grants") or [])
    boundary_permits = list(state.get("boundary_permits") or [])
    boundary_rules = list(state.get("boundary_rules") or [])
    payload = {
        "approval_id": updated.approval_id,
        "status": updated.status.value,
        "requested_by": updated.requested_by,
        "action": updated.action,
        "resource": updated.resource,
        "decided_by": updated.decided_by,
        "decision_reason": updated.decision_reason,
        "grant_ids": [grant.grant_id for grant in grants],
        "boundary_permit_ids": [permit.permit_id for permit in boundary_permits],
        "boundary_rule_ids": [rule.rule_id for rule in boundary_rules],
    }
    state["store"].add_event(make_event(event_type=state["event_type"], actor=state["normalized_actor"], payload=payload))
    state["store"].add_audit_record(make_audit_record(action=state["event_type"], actor=state["normalized_actor"], details=payload))
    return {}


@lru_cache(maxsize=1)
def _compiled_approval_decision_graph():
    graph = StateGraph(_ApprovalDecisionState)
    graph.add_node("transition", _approval_decision_transition_node)
    graph.add_node("permission_grants", _approval_decision_permission_grants_node)
    graph.add_node("allow_once", _approval_decision_allow_once_node)
    graph.add_node("always_allow", _approval_decision_always_allow_node)
    graph.add_node("sync_missions", _approval_decision_sync_missions_node)
    graph.add_node("record_event", _approval_decision_record_event_node)
    graph.add_edge(START, "transition")
    graph.add_edge("transition", "permission_grants")
    graph.add_edge("permission_grants", "allow_once")
    graph.add_edge("allow_once", "always_allow")
    graph.add_edge("always_allow", "sync_missions")
    graph.add_edge("sync_missions", "record_event")
    graph.add_edge("record_event", END)
    return graph.compile()


def _approve_or_deny_approval_request(
    store: RuntimeStore,
    approval_id: str,
    *,
    actor: str,
    transition: Any,
    event_type: str,
    reason: str | None = None,
    principal_id: str | None = None,
    permissions: Iterable[str] = (),
    boundary_allow_once_selector: str | None = None,
    boundary_allow_once_uses: int | None = None,
    boundary_always_allow_selector: str | None = None,
    boundary_kind: BoundaryKind | None = None,
    expires_at: datetime | None = None,
) -> ApprovalRequest:
    final_state = _compiled_approval_decision_graph().invoke(
        {
            "store": store,
            "approval_id": approval_id,
            "actor": actor,
            "transition": transition,
            "event_type": event_type,
            "reason": reason,
            "principal_id": principal_id,
            "permissions": tuple(permissions),
            "boundary_allow_once_selector": boundary_allow_once_selector,
            "boundary_allow_once_uses": boundary_allow_once_uses,
            "boundary_always_allow_selector": boundary_always_allow_selector,
            "boundary_kind": boundary_kind,
            "expires_at": expires_at,
        }
    )
    updated = final_state.get("updated")
    if isinstance(updated, ApprovalRequest):
        return updated
    raise RuntimeError("Approval decision graph finished without an updated approval")


def approve_approval_request(
    store: RuntimeStore,
    approval_id: str,
    *,
    actor: str = "operator",
    principal_id: str,
    permissions: Iterable[str],
    boundary_allow_once_selector: str | None = None,
    boundary_allow_once_uses: int | None = None,
    boundary_always_allow_selector: str | None = None,
    boundary_kind: BoundaryKind | None = None,
    expires_at: datetime | None = None,
    reason: str | None = None,
) -> ApprovalRequest:
    return _approve_or_deny_approval_request(
        store,
        approval_id,
        actor=actor,
        transition=approve,
        event_type="approval.request_approved",
        reason=reason,
        principal_id=principal_id,
        permissions=tuple(permissions),
        boundary_allow_once_selector=boundary_allow_once_selector,
        boundary_allow_once_uses=boundary_allow_once_uses,
        boundary_always_allow_selector=boundary_always_allow_selector,
        boundary_kind=boundary_kind,
        expires_at=expires_at,
    )


def deny_approval_request(
    store: RuntimeStore,
    approval_id: str,
    *,
    actor: str = "operator",
    reason: str | None = None,
) -> ApprovalRequest:
    return _approve_or_deny_approval_request(
        store,
        approval_id,
        actor=actor,
        transition=deny,
        event_type="approval.request_denied",
        reason=reason,
    )


def list_active_permission_grants(
    store: RuntimeStore,
    *,
    principal_id: str | None = None,
    permission: str | None = None,
    now: datetime | None = None,
) -> list[PermissionGrant]:
    grants = [
        grant
        for grant in store.list_permission_grants()
        if is_permission_grant_active(grant, now=now)
    ]
    if principal_id is not None:
        principal_aliases = {
            principal_id,
            permission_scope_principal(principal_id),
            OPERATOR_PERMISSION_PRINCIPAL,
        }
        if principal_id in {"operator", "workspace:workspace_admin", OPERATOR_PERMISSION_PRINCIPAL}:
            principal_aliases.update({"operator", "workspace:workspace_admin", OPERATOR_PERMISSION_PRINCIPAL})
        grants = [grant for grant in grants if grant.principal_id in principal_aliases]
    if permission is not None:
        grants = [grant for grant in grants if grant.permission == permission]
    return grants


def revoke_permission_grant(
    store: RuntimeStore,
    grant_id: str,
    *,
    actor: str = "operator",
    reason: str | None = None,
) -> PermissionGrant:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="approval.grant_revoked")
    grant = store.get_permission_grant(grant_id)
    if grant is None:
        raise KeyError(grant_id)

    revoked = revoke_permission_grant_record(
        grant,
        revoked_by=normalized_actor,
        revoked_at=datetime.now(UTC),
        reason=reason,
    )
    store.add_permission_grant(revoked)

    payload = {
        "grant_id": revoked.grant_id,
        "approval_id": revoked.approval_id,
        "principal_id": revoked.principal_id,
        "permission": revoked.permission,
        "revoked_by": revoked.revoked_by,
        "revoked_reason": revoked.revoked_reason,
    }
    store.add_event(make_event(event_type="approval.grant_revoked", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="approval.grant_revoked", actor=normalized_actor, details=payload))
    return revoked


def revoke_session_web_boundary_permits(
    store: RuntimeStore,
    *,
    actor: str = "runtime",
    reason: str = "New session started",
) -> int:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="approval.session_web_permits_revoked")
    now = datetime.now(UTC)
    revoked = 0
    for permit in store.list_boundary_permits():
        if permit.revoked_at is not None:
            continue
        if permit.boundary_kind is not BoundaryKind.OUTBOUND_NETWORK or permit.selector != "*":
            continue
        approval = store.get_approval_request(permit.approval_id)
        decision_reason = str(getattr(approval, "decision_reason", "") or "").lower()
        if "session" not in decision_reason:
            continue
        store.add_boundary_permit(
            replace(
                permit,
                revoked_by=normalized_actor,
                revoked_at=now,
                revoked_reason=reason,
            )
        )
        revoked += 1
    if revoked:
        store.add_event(
            make_event(
                event_type="approval.session_web_permits_revoked",
                actor=normalized_actor,
                payload={"count": revoked, "reason": reason},
            )
        )
    return revoked


def revoke_related_boundary_permission(
    store: RuntimeStore,
    *,
    permission_kind: str,
    permission_id: str,
    actor: str = "operator",
    reason: str = "Revoked from web UI",
) -> int:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="approval.boundary_permission_revoked")
    now = datetime.now(UTC)
    if permission_kind == "boundary-permit":
        target = store.get_boundary_permit(permission_id)
        if target is None:
            raise KeyError(permission_id)
        boundary_kind = target.boundary_kind
        selector = target.selector
        revoke_permits = True
        revoke_rules = True
        rule_mode = PolicyMode.ALLOW
    elif permission_kind == "boundary-rule":
        target = store.get_boundary_policy_rule(permission_id)
        if target is None:
            raise KeyError(permission_id)
        boundary_kind = target.kind
        selector = target.selector
        revoke_permits = target.mode is PolicyMode.ALLOW
        revoke_rules = True
        rule_mode = target.mode
    else:
        raise ValueError("unknown permission kind")

    def selector_matches(candidate: str) -> bool:
        if selector == "*" or candidate == "*":
            return selector == candidate
        if boundary_kind is BoundaryKind.OUTBOUND_NETWORK:
            return normalize_outbound_network_selector(candidate) == normalize_outbound_network_selector(selector)
        return candidate == selector

    revoked = 0
    revoked_permit_ids: list[str] = []
    revoked_rule_ids: list[str] = []
    if revoke_permits:
        for permit in store.list_boundary_permits():
            if permit.revoked_at is not None:
                continue
            if permit.boundary_kind is not boundary_kind or not selector_matches(permit.selector):
                continue
            store.add_boundary_permit(
                replace(
                    permit,
                    revoked_by=normalized_actor,
                    revoked_at=now,
                    revoked_reason=reason,
                )
            )
            revoked += 1
            revoked_permit_ids.append(permit.permit_id)
    if revoke_rules:
        for rule in store.list_boundary_policy_rules():
            if rule.revoked_at is not None:
                continue
            if rule.kind is not boundary_kind or rule.mode is not rule_mode or not selector_matches(rule.selector):
                continue
            store.add_boundary_policy_rule(replace(rule, revoked_at=now))
            revoked += 1
            revoked_rule_ids.append(rule.rule_id)
    normalized_selector = (
        normalize_outbound_network_selector(selector)
        if boundary_kind is BoundaryKind.OUTBOUND_NETWORK and selector != "*"
        else selector
    )
    store.add_event(
        make_event(
            event_type="approval.boundary_permission_revoked",
            actor=normalized_actor,
            payload={
                "permission_kind": permission_kind,
                "permission_id": permission_id,
                "boundary_kind": str(getattr(boundary_kind, "value", boundary_kind)),
                "selector": normalized_selector,
                "revoked_permit_ids": revoked_permit_ids,
                "revoked_rule_ids": revoked_rule_ids,
                "count": revoked,
                "reason": reason,
            },
        )
    )
    return revoked



def _auto_create_mission_for_turn(
    store: RuntimeStore,
    *,
    turn: AssistantTurn,
    resource: str,
    action: str,
) -> MissionRecord | None:
    capsule = turn.capsule
    if capsule is None:
        return None

    approval_request = getattr(turn.orchestration_result, "approval_request", None)
    if approval_request is not None:
        mission = create_mission(
            store,
            owner=capsule.owner,
            title=capsule.goal,
            goal=capsule.goal,
            continuation_policy=MissionContinuationPolicy.APPROVAL_GATED,
            created_from_capsule_id=capsule.capsule_id,
            active_capsule_id=capsule.capsule_id,
            actor="runtime",
        )
        return set_mission_plan(
            store,
            mission.mission_id,
            steps=(
                MissionStep(
                    step_id="approval-step-1",
                    title=f"Approve {action} {resource}",
                    status="pending",
                    kind="approval",
                    capsule_id=capsule.capsule_id,
                    notes=resource,
                ),
            ),
            active_step_id="approval-step-1",
            actor="runtime",
        )

    if turn.mini_agent_launch_plan.decision is MiniAgentLaunchDecision.LAUNCH and turn.mini_agent_launch_plan.mini_agent_type:
        pending_runs = [run for run in store.list_mini_agent_runs() if run.capsule_id == capsule.capsule_id]
        pending_run = pending_runs[-1] if pending_runs else None
        mission = create_mission(
            store,
            owner=capsule.owner,
            title=capsule.goal,
            goal=capsule.goal,
            continuation_policy=MissionContinuationPolicy.AUTO_FINISH,
            created_from_capsule_id=capsule.capsule_id,
            active_capsule_id=capsule.capsule_id,
            actor="runtime",
        )
        return set_mission_plan(
            store,
            mission.mission_id,
            steps=(
                MissionStep(
                    step_id="mini-agent-step-1",
                    title=f"Run {turn.mini_agent_launch_plan.mini_agent_type}",
                    status="pending",
                    kind="mini_agent",
                    capsule_id=capsule.capsule_id,
                    mini_agent_run_id=None if pending_run is None else pending_run.run_id,
                    notes=resource,
                ),
            ),
            actor="runtime",
        )

    return None


def run_request(
    store: RuntimeStore,
    *,
    owner: str,
    goal: str,
    action: str,
    resource: str,
    risk_level: str = "normal",
    success_criteria: list[str] | None = None,
    allowed_actions: Iterable[str] | None = None,
    approval_actions: Iterable[str] | None = None,
    tool_registry: ToolRegistry | None = None,
) -> RuntimeResult:
    turn = handle_request(
        owner=owner,
        goal=goal,
        action=action,
        resource=resource,
        risk_level=risk_level,
        success_criteria=success_criteria,
        allowed_actions=allowed_actions,
        approval_actions=approval_actions,
    )

    capsule = turn.capsule
    if capsule is not None:
        if turn.mini_agent_launch_plan.decision is MiniAgentLaunchDecision.LAUNCH and turn.mini_agent_launch_plan.mini_agent_type:
            capsule = add_mini_agent(capsule, turn.mini_agent_launch_plan.mini_agent_type)
            pending_run = create_mini_agent_run(
                run_id=f"run-{uuid4().hex[:12]}",
                capsule_id=capsule.capsule_id,
                mini_agent_type=turn.mini_agent_launch_plan.mini_agent_type,
                created_at=datetime.now(UTC),
            )
            store.add_mini_agent_run(pending_run)
        store.add_capsule(capsule)
        turn = replace(turn, capsule=capsule)

    approval_request = getattr(turn.orchestration_result, "approval_request", None)
    if approval_request is not None:
        store.add_approval_request(approval_request)

    _auto_create_mission_for_turn(
        store,
        turn=turn,
        resource=resource,
        action=action,
    )

    progress_update = getattr(turn, "progress_update", None)
    if progress_update is not None:
        record_progress_update(store, progress_update)

    event = getattr(turn.orchestration_result, "event", None)
    if event is not None:
        store.add_event(event)
    audit_record = getattr(turn.orchestration_result, "audit_record", None)
    if audit_record is not None:
        store.add_audit_record(audit_record)

    packet = _build_request_builder_packet(
        store,
        goal=goal,
        tool_registry=tool_registry,
    )
    decision = evaluate_builder_decision_read_model(packet)
    if decision.should_propose:
        proposal = build_builder_proposal_read_model(decision)
        context_key = "|".join(
            [
                decision.reason,
                str(packet.repeated_failures),
                ";".join(packet.recent_doctor_signals),
                ";".join(packet.recent_sentinel_signals),
            ]
        )
        store_builder_proposal(store, proposal, context_key=context_key, actor="runtime")

    return RuntimeResult(turn=turn, store=store)



def _next_runtime_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"


def _core_fallback_tool_names(tool_registry: ToolRegistry | None) -> tuple[str, ...]:
    if tool_registry is None:
        return ()
    spec_names = {spec.name for spec in tool_registry.list_specs()}
    return tuple(name for name in CORE_FALLBACK_TOOL_NAMES if name in spec_names)


def _build_request_builder_packet(
    store: RuntimeStore,
    *,
    goal: str,
    tool_registry: ToolRegistry | None,
) -> BuilderInputPacket:
    route = classify_live_information_route(goal)
    required_plugins = tuple(
        plugin_name
        for plugin_name in route_required_plugins(route)
        if tool_registry is None or not tool_registry.is_plugin_installed(plugin_name)
    )
    core_tool_names = _core_fallback_tool_names(tool_registry)
    return BuilderInputPacket(
        repeated_failures=len(store.list_doctor_signals()),
        recent_doctor_signals=tuple(signal.reason for signal in store.list_doctor_signals()[-5:]),
        recent_sentinel_signals=tuple(signal.reason for signal in store.list_sentinel_signals()[-5:]),
        core_tool_names=core_tool_names,
        missing_plugins_for_request=required_plugins,
        core_fallback_available=bool(required_plugins and core_tool_names),
    )


def _append_conversation_event_log(
    store: RuntimeStore,
    *,
    conversation_id: str,
    event_type: str,
    created_at: datetime,
    **fields: Any,
) -> None:
    event: dict[str, Any] = {
        "conversation_id": conversation_id,
        "event_type": event_type,
        "created_at": created_at.isoformat(),
    }
    event.update({key: value for key, value in fields.items() if value is not None})
    store.add_conversation_event(event)



def _is_active_conversation_branch(branch: ConversationBranch | None) -> bool:
    if branch is None:
        return False
    status = branch.status
    if isinstance(status, ConversationBranchStatus):
        return status is ConversationBranchStatus.ACTIVE
    return str(status) == ConversationBranchStatus.ACTIVE.value



def _resolve_active_conversation_branch(
    store: RuntimeStore,
    conversation_id: str,
) -> tuple[ConversationBranch | None, str | None]:
    head = store.get_conversation_head(conversation_id)
    if head is None:
        return None, None
    active_branch_id = head.get("active_branch_id")
    active_turn_id = head.get("active_turn_id")
    if not isinstance(active_branch_id, str) or not active_branch_id:
        return None, active_turn_id if isinstance(active_turn_id, str) else None
    branch = store.get_conversation_branch(active_branch_id)
    if not _is_active_conversation_branch(branch):
        return None, active_turn_id if isinstance(active_turn_id, str) else None
    return branch, active_turn_id if isinstance(active_turn_id, str) else None



def _record_mini_agent_conversation_task_link(
    store: RuntimeStore,
    *,
    run: MiniAgentRun,
    conversation_id: str | None,
    turn_id: str | None,
    branch_id: str | None,
    created_at: datetime,
) -> None:
    if conversation_id is None and turn_id is None and branch_id is None:
        return
    if not isinstance(conversation_id, str) or not conversation_id:
        raise ValueError("conversation_id is required when linking mini-agent run to conversation")
    if not isinstance(turn_id, str) or not turn_id:
        raise ValueError("turn_id is required when linking mini-agent run to conversation")
    if not isinstance(branch_id, str) or not branch_id:
        raise ValueError("branch_id is required when linking mini-agent run to conversation")

    turn = store.get_conversation_turn(turn_id)
    if turn is None:
        raise KeyError(turn_id)
    branch = store.get_conversation_branch(branch_id)
    if branch is None:
        raise KeyError(branch_id)

    if turn.conversation_id != conversation_id or turn.branch_id != branch_id:
        raise ValueError("turn does not belong to the provided conversation/branch")
    if branch.conversation_id != conversation_id:
        raise ValueError("branch does not belong to the provided conversation")

    head = store.get_conversation_head(conversation_id)
    if head is not None and (
        head.get("active_branch_id") != branch_id or head.get("active_turn_id") != turn_id
    ):
        raise ValueError("mini-agent conversation link must target active turn/branch")

    _append_conversation_event_log(
        store,
        conversation_id=conversation_id,
        event_type="conversation.task_spawned",
        created_at=created_at,
        turn_id=turn_id,
        branch_id=branch_id,
        task_id=run.run_id,
        capsule_id=run.capsule_id,
        mini_agent_type=run.mini_agent_type,
    )


def _branch_spawned_task_ids(
    store: RuntimeStore,
    *,
    conversation_id: str,
    branch_id: str,
) -> list[tuple[str, str | None]]:
    seen_task_ids: set[str] = set()
    linked_tasks: list[tuple[str, str | None]] = []
    for event in store.list_conversation_events(conversation_id):
        if event.get("event_type") != "conversation.task_spawned":
            continue
        if event.get("branch_id") != branch_id:
            continue
        task_id = event.get("task_id")
        if not isinstance(task_id, str) or not task_id or task_id in seen_task_ids:
            continue
        seen_task_ids.add(task_id)
        turn_id = event.get("turn_id")
        linked_tasks.append((task_id, turn_id if isinstance(turn_id, str) else None))
    return linked_tasks


def _conversation_task_was_cancelled(
    store: RuntimeStore,
    *,
    conversation_id: str,
    task_id: str,
) -> bool:
    for event in reversed(store.list_conversation_events(conversation_id)):
        if event.get("task_id") != task_id:
            continue
        if event.get("event_type") == "conversation.task_cancelled":
            return True
    return False


def _cancel_conversation_branch_tasks(
    store: RuntimeStore,
    *,
    conversation_id: str,
    branch_id: str,
    replacement_branch_id: str,
    created_at: datetime,
) -> None:
    for task_id, turn_id in _branch_spawned_task_ids(store, conversation_id=conversation_id, branch_id=branch_id):
        if _conversation_task_was_cancelled(store, conversation_id=conversation_id, task_id=task_id):
            continue
        _append_conversation_event_log(
            store,
            conversation_id=conversation_id,
            event_type="conversation.task_cancelled",
            created_at=created_at,
            task_id=task_id,
            turn_id=turn_id,
            branch_id=branch_id,
            replacement_branch_id=replacement_branch_id,
            reason="branch_superseded",
        )


def _resolve_mini_agent_conversation_task_link(
    store: RuntimeStore,
    *,
    run: MiniAgentRun,
) -> tuple[str, str, str] | None:
    for event in reversed(store.list_conversation_events()):
        if event.get("event_type") != "conversation.task_spawned":
            continue
        if event.get("task_id") != run.run_id:
            continue

        conversation_id = event.get("conversation_id")
        turn_id = event.get("turn_id")
        branch_id = event.get("branch_id")
        if not isinstance(conversation_id, str) or not conversation_id:
            continue
        if not isinstance(turn_id, str) or not turn_id:
            continue
        if not isinstance(branch_id, str) or not branch_id:
            continue
        return conversation_id, turn_id, branch_id
    return None


def _auto_commit_mini_agent_terminal_worker_result(store: RuntimeStore, *, run: MiniAgentRun) -> WorkerResultCommitResult | None:
    link = _resolve_mini_agent_conversation_task_link(store, run=run)
    if link is None:
        return None

    conversation_id, turn_id, branch_id = link
    idempotency_key = f"mini-agent:{run.run_id}:{run.status.value}"
    kind = WorkerResultKind.ARTIFACT if run.status is MiniAgentRunStatus.COMPLETED else WorkerResultKind.OBSOLETE
    payload = {
        "run_id": run.run_id,
        "capsule_id": run.capsule_id,
        "mini_agent_type": run.mini_agent_type,
        "status": run.status.value,
        "result_summary": run.result_summary,
    }

    try:
        envelope = create_mini_agent_worker_result_envelope(
            store,
            run_id=run.run_id,
            conversation_id=conversation_id,
            turn_id=turn_id,
            branch_id=branch_id,
            kind=kind,
            idempotency_key=idempotency_key,
            payload=payload,
            require_active_turn=False,
        )
    except (KeyError, ValueError, TypeError):
        return None

    return commit_worker_result(store, result_envelope=envelope)


def _freeze_worker_result_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        frozen = {str(key): _freeze_worker_result_payload(subvalue) for key, subvalue in value.items()}
        return MappingProxyType(frozen)
    if isinstance(value, list):
        return tuple(_freeze_worker_result_payload(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_worker_result_payload(item) for item in value)
    return value


def _conversation_result_freshness_reason(
    store: RuntimeStore,
    *,
    conversation_id: str,
    turn_id: str,
    branch_id: str,
) -> str | None:
    head = store.get_conversation_head(conversation_id)
    if head is None:
        return None
    if head.get("active_branch_id") != branch_id:
        return "stale_branch"
    if head.get("active_turn_id") != turn_id:
        return "stale_turn"
    return None


def create_worker_result_envelope(
    store: RuntimeStore,
    *,
    conversation_id: str,
    turn_id: str,
    branch_id: str,
    task_id: str,
    kind: WorkerResultKind,
    idempotency_key: str,
    payload: Mapping[str, Any],
    result_id: str | None = None,
    created_at: datetime | None = None,
    require_active_turn: bool = True,
) -> WorkerResultEnvelope:
    turn = store.get_conversation_turn(turn_id)
    if turn is None:
        raise KeyError(turn_id)

    branch = store.get_conversation_branch(branch_id)
    if branch is None:
        raise KeyError(branch_id)

    if turn.conversation_id != conversation_id or turn.branch_id != branch_id:
        raise ValueError("turn does not belong to the provided conversation/branch")
    if branch.conversation_id != conversation_id:
        raise ValueError("branch does not belong to the provided conversation")

    reason = _conversation_result_freshness_reason(
        store,
        conversation_id=conversation_id,
        turn_id=turn_id,
        branch_id=branch_id,
    )
    if require_active_turn and reason is not None:
        if reason == "stale_turn":
            raise ValueError("turn is not the active turn for the conversation")
        raise ValueError("branch is not the active branch for the conversation")

    if not isinstance(task_id, str) or not task_id:
        raise ValueError("task_id must be a non-empty string")
    if not isinstance(idempotency_key, str) or not idempotency_key:
        raise ValueError("idempotency_key must be a non-empty string")

    normalized_kind = kind if isinstance(kind, WorkerResultKind) else WorkerResultKind(str(kind))
    frozen_payload = _freeze_worker_result_payload(payload)
    if not isinstance(frozen_payload, Mapping):
        raise TypeError("payload must be a mapping")

    return WorkerResultEnvelope(
        result_id=result_id or _next_runtime_id("result"),
        conversation_id=conversation_id,
        branch_id=branch_id,
        turn_id=turn_id,
        task_id=task_id,
        kind=normalized_kind,
        idempotency_key=idempotency_key,
        payload=frozen_payload,
        created_at=created_at or datetime.now(UTC),
    )


def create_mini_agent_worker_result_envelope(
    store: RuntimeStore,
    *,
    run_id: str,
    conversation_id: str,
    turn_id: str,
    branch_id: str,
    kind: WorkerResultKind,
    idempotency_key: str,
    payload: Mapping[str, Any],
    result_id: str | None = None,
    created_at: datetime | None = None,
    require_active_turn: bool = True,
) -> WorkerResultEnvelope:
    run = store.get_mini_agent_run(run_id)
    if run is None:
        raise KeyError(run_id)
    return create_worker_result_envelope(
        store,
        conversation_id=conversation_id,
        turn_id=turn_id,
        branch_id=branch_id,
        task_id=run.run_id,
        kind=kind,
        idempotency_key=idempotency_key,
        payload=payload,
        result_id=result_id,
        created_at=created_at,
        require_active_turn=require_active_turn,
    )



def update_active_task_frame_from_outcomes(
    store: RuntimeStore,
    *,
    conversation_id: str,
    tool_results: list[ToolResult] | tuple[ToolResult, ...],
    rendered_reply: str,
    completion_turn_id: str | None = None,
):
    active_task_frame_id = store.get_active_task_frame_id(conversation_id)
    if not isinstance(active_task_frame_id, str) or not active_task_frame_id:
        return None
    frame = store.get_task_frame(active_task_frame_id)
    if frame is None:
        return None

    normalized_results = [
        (result, normalize_tool_status(result.status))
        for result in tool_results
    ]
    updated_frame = frame
    now = datetime.now(UTC)

    for result, normalized_status in normalized_results:
        if normalized_status != "denied":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if output.get("reason") != "approval_required":
            continue
        approval_id = output.get("approval_id")
        if not isinstance(approval_id, str) or not approval_id:
            continue
        approval = store.get_approval_request(approval_id)
        if approval is None or approval.status.value != "pending":
            continue
        updated_frame = replace(
            updated_frame,
            status=TaskFrameStatus.WAITING_APPROVAL,
            updated_at=now,
        )
        store.add_task_frame(updated_frame)
        return updated_frame

    completed_tool_names = {
        result.tool_name
        for result, normalized_status in normalized_results
        if normalized_status == "completed"
    }
    required_tool_completion = set(updated_frame.finish.required_tool_completion)
    required_tools_satisfied = not required_tool_completion or required_tool_completion.issubset(completed_tool_names)
    artifact_delivery_satisfied = (
        (not updated_frame.finish.requires_artifact_delivery)
        or (bool(completed_tool_names) and _rendered_reply_has_existing_media_attachment(rendered_reply))
    )
    if required_tools_satisfied and artifact_delivery_satisfied and completed_tool_names:
        updated_frame = replace(
            updated_frame,
            status=TaskFrameStatus.COMPLETED,
            updated_at=now,
            completion_turn_id=completion_turn_id or updated_frame.source_turn_id,
        )
        store.add_task_frame(updated_frame)
        store.set_active_task_frame_id(conversation_id, None)
        return updated_frame

    store.add_task_frame(updated_frame)
    return updated_frame


def _rendered_reply_has_existing_media_attachment(rendered_reply: str) -> bool:
    for candidate in media_candidate_paths_from_text(rendered_reply):
        try:
            path = candidate.expanduser().resolve()
        except (OSError, RuntimeError, ValueError):
            continue
        if path.is_file() and path.stat().st_size > 0:
            return True
    return False



def _apply_task_frame_continuation_head(
    store: RuntimeStore,
    *,
    conversation_id: str,
    turn: ConversationTurn,
    active_frame,
    continuation: TaskFrameContinuationDecision | None,
) -> str | None:
    if active_frame is None or continuation is None:
        return active_frame.frame_id if active_frame is not None else None
    if continuation.mode.value not in {"substitute_target", "revise"}:
        return active_frame.frame_id

    updated_frame = replace(
        active_frame,
        frame_id=_next_runtime_id("frame"),
        source_turn_id=turn.turn_id,
        parent_frame_id=active_frame.frame_id,
        target=continuation.target,
        output=continuation.output,
        execution=continuation.execution,
        finish=continuation.finish,
        updated_at=turn.created_at,
        last_activity_turn_id=turn.turn_id,
        completion_turn_id=None,
    )
    store.add_task_frame(updated_frame)
    store.set_active_task_frame_id(conversation_id, updated_frame.frame_id)
    return updated_frame.frame_id



def process_conversation_message(
    store: RuntimeStore,
    *,
    user_message: str,
    conversation_id: str | None = None,
    chat_id: str | None = None,
    message_id: str | None = None,
    request_id: str | None = None,
    turn_id: str | None = None,
    branch_id: str | None = None,
    received_at: datetime | None = None,
    ambiguity_fallback: TurnDispositionAmbiguityFallback | None = None,
    ambiguity_fallback_reason: str | None = None,
    ambiguity_classifier: TurnDispositionAmbiguityClassifier | None = None,
    ambiguity_classifier_reason: str | None = None,
    previous_assistant_message: str | None = None,
) -> ConversationMessageResult:
    conversation_id = conversation_id or _next_runtime_id("conv")
    received_at = received_at or datetime.now(UTC)
    turn_id = turn_id or _next_runtime_id("turn")

    active_branch, active_turn_id = _resolve_active_conversation_branch(store, conversation_id)
    active_task_frame_id = store.get_active_task_frame_id(conversation_id)
    active_task_frame = (
        store.get_task_frame(active_task_frame_id)
        if isinstance(active_task_frame_id, str)
        else None
    )
    disposition_decision = classify_turn_disposition_with_reason(
        text=user_message,
        active_branch_exists=active_branch is not None,
        ambiguity_fallback=ambiguity_fallback,
        previous_assistant_message=previous_assistant_message,
        ambiguity_classifier=ambiguity_classifier,
    )
    disposition = disposition_decision.disposition
    disposition_reason = disposition_decision.reason
    if disposition_reason == "ambiguity_fallback" and ambiguity_fallback_reason:
        disposition_reason = f"ambiguity_fallback:{ambiguity_fallback_reason}"
    if disposition_reason == "ambiguity_classifier" and ambiguity_classifier_reason:
        disposition_reason = f"ambiguity_classifier:{ambiguity_classifier_reason}"
    should_continue = disposition is ConversationTurnDisposition.CONTINUE and active_branch is not None
    task_frame_branch_continuous = active_branch is not None
    task_frame_continuation = (
        resolve_task_frame_continuation(
            text=user_message,
            active_frame=active_task_frame,
            branch_continuous=task_frame_branch_continuous,
        )
        if active_task_frame is not None
        else None
    )
    if (
        active_branch is not None
        and task_frame_continuation is not None
        and task_frame_continuation.mode.value != "start_new"
        and not should_continue
    ):
        should_continue = True
        disposition = ConversationTurnDisposition.CONTINUE
        if disposition_reason is None or not disposition_reason.startswith("task_frame"):
            disposition_reason = f"task_frame:{task_frame_continuation.mode.value}"

    superseded_branch_id: str | None = None
    if should_continue:
        branch = active_branch
        resolved_branch_id = active_branch.branch_id
        parent_turn_id = active_turn_id
    else:
        resolved_branch_id = branch_id or _next_runtime_id("branch")
        branch = ConversationBranch(
            branch_id=resolved_branch_id,
            conversation_id=conversation_id,
            status=ConversationBranchStatus.ACTIVE,
            created_from_turn_id=turn_id,
        )
        parent_turn_id = None

        if disposition in {ConversationTurnDisposition.INTERRUPT, ConversationTurnDisposition.REVISE} and active_branch is not None:
            superseded = replace(
                active_branch,
                status=ConversationBranchStatus.SUPERSEDED,
                superseded_by_branch_id=resolved_branch_id,
                cancelled_at=received_at,
            )
            store.add_conversation_branch(superseded)
            superseded_branch_id = active_branch.branch_id

            superseded_payload = {
                "conversation_id": conversation_id,
                "superseded_branch_id": active_branch.branch_id,
                "replacement_branch_id": resolved_branch_id,
                "disposition": disposition.value,
            }
            store.add_event(
                make_event(
                    event_type="conversation.branch_superseded",
                    actor="runtime",
                    payload=superseded_payload,
                )
            )
            store.add_audit_record(
                make_audit_record(
                    action="conversation.branch_superseded",
                    actor="runtime",
                    details=superseded_payload,
                )
            )
            _append_conversation_event_log(
                store,
                conversation_id=conversation_id,
                event_type="conversation.branch_superseded",
                created_at=received_at,
                superseded_branch_id=active_branch.branch_id,
                replacement_branch_id=resolved_branch_id,
            )
            _cancel_conversation_branch_tasks(
                store,
                conversation_id=conversation_id,
                branch_id=active_branch.branch_id,
                replacement_branch_id=resolved_branch_id,
                created_at=received_at,
            )

    store.add_conversation_branch(branch)

    envelope = ConversationEnvelope(
        conversation_id=conversation_id,
        message_id=message_id or _next_runtime_id("msg"),
        request_id=request_id or _next_runtime_id("req"),
        turn_id=turn_id,
        branch_id=resolved_branch_id,
        parent_turn_id=parent_turn_id,
        received_at=received_at,
        user_message=user_message,
        chat_id=chat_id,
    )

    message_received_payload = {
        "conversation_id": conversation_id,
        "message_id": envelope.message_id,
        "request_id": envelope.request_id,
        "turn_id": turn_id,
        "branch_id": resolved_branch_id,
        "chat_id": chat_id,
        "user_message": user_message,
        "disposition_reason": disposition_reason,
    }
    store.add_event(
        make_event(
            event_type="conversation.message_received",
            actor="runtime",
            payload=message_received_payload,
        )
    )
    store.add_audit_record(
        make_audit_record(
            action="conversation.message_received",
            actor="runtime",
            details=message_received_payload,
        )
    )
    _append_conversation_event_log(
        store,
        conversation_id=conversation_id,
        event_type="conversation.message_received",
        created_at=received_at,
        message_id=envelope.message_id,
        request_id=envelope.request_id,
        turn_id=turn_id,
        branch_id=resolved_branch_id,
        chat_id=chat_id,
        disposition_reason=disposition_reason,
    )

    if not should_continue:
        branch_created_payload = {
            "conversation_id": conversation_id,
            "branch_id": resolved_branch_id,
            "turn_id": turn_id,
            "created_from_turn_id": turn_id,
            "parent_turn_id": parent_turn_id,
            "disposition": disposition.value,
            "disposition_reason": disposition_reason,
        }
        store.add_event(
            make_event(
                event_type="conversation.branch_created",
                actor="runtime",
                payload=branch_created_payload,
            )
        )
        store.add_audit_record(
            make_audit_record(
                action="conversation.branch_created",
                actor="runtime",
                details=branch_created_payload,
            )
        )
        _append_conversation_event_log(
            store,
            conversation_id=conversation_id,
            event_type="conversation.branch_created",
            created_at=received_at,
            turn_id=turn_id,
            branch_id=resolved_branch_id,
            parent_turn_id=parent_turn_id,
            created_from_turn_id=turn_id,
            disposition=disposition.value,
            disposition_reason=disposition_reason,
        )

    turn = ConversationTurn(
        turn_id=turn_id,
        conversation_id=conversation_id,
        branch_id=resolved_branch_id,
        parent_turn_id=parent_turn_id,
        disposition=disposition,
        user_message=user_message,
        status="accepted",
        created_at=received_at,
        disposition_reason=disposition_reason,
    )
    store.add_conversation_turn(turn)
    store.set_conversation_head(
        conversation_id,
        active_branch_id=resolved_branch_id,
        active_turn_id=turn_id,
    )
    resolved_active_task_frame_id = _apply_task_frame_continuation_head(
        store,
        conversation_id=conversation_id,
        turn=turn,
        active_frame=active_task_frame,
        continuation=task_frame_continuation,
    )

    payload = {
        "conversation_id": conversation_id,
        "message_id": envelope.message_id,
        "request_id": envelope.request_id,
        "turn_id": turn_id,
        "branch_id": resolved_branch_id,
        "parent_turn_id": parent_turn_id,
        "disposition": disposition.value,
        "disposition_reason": disposition_reason,
        "chat_id": chat_id,
        "superseded_branch_id": superseded_branch_id,
    }
    store.add_event(
        make_event(
            event_type="conversation.turn_recorded",
            actor="runtime",
            payload=payload,
        )
    )
    store.add_audit_record(
        make_audit_record(
            action="conversation.turn_recorded",
            actor="runtime",
            details=payload,
        )
    )
    _append_conversation_event_log(
        store,
        conversation_id=conversation_id,
        event_type="conversation.turn_recorded",
        created_at=received_at,
        turn_id=turn_id,
        branch_id=resolved_branch_id,
        parent_turn_id=parent_turn_id,
        disposition_reason=disposition_reason,
    )

    return ConversationMessageResult(
        envelope=envelope,
        branch=branch,
        turn=turn,
        superseded_branch_id=superseded_branch_id,
        disposition_reason=disposition_reason,
        active_task_frame_id=resolved_active_task_frame_id,
        task_frame_continuation=task_frame_continuation,
    )



def list_conversation_chat_turns(store: RuntimeStore, conversation_id: str) -> list[dict[str, str]]:
    return store.list_conversation_chat_turns(conversation_id)



def commit_worker_result(
    store: RuntimeStore,
    *,
    result_envelope: WorkerResultEnvelope | None = None,
    conversation_id: str | None = None,
    turn_id: str | None = None,
    branch_id: str | None = None,
    idempotency_key: str | None = None,
) -> WorkerResultCommitResult:
    if result_envelope is not None:
        conversation_id = result_envelope.conversation_id
        turn_id = result_envelope.turn_id
        branch_id = result_envelope.branch_id
        idempotency_key = result_envelope.idempotency_key

    if (
        not isinstance(conversation_id, str)
        or not conversation_id
        or not isinstance(turn_id, str)
        or not turn_id
        or not isinstance(branch_id, str)
        or not branch_id
        or not isinstance(idempotency_key, str)
        or not idempotency_key
    ):
        raise ValueError("conversation_id, turn_id, branch_id, and idempotency_key are required")

    turn = store.get_conversation_turn(turn_id)
    if turn is None:
        raise KeyError(turn_id)

    branch = store.get_conversation_branch(branch_id)
    if branch is None:
        raise KeyError(branch_id)

    if turn.conversation_id != conversation_id or turn.branch_id != branch_id:
        raise ValueError("turn does not belong to the provided conversation/branch")
    if branch.conversation_id != conversation_id:
        raise ValueError("branch does not belong to the provided conversation")

    reason: str | None = None
    if store.has_committed_idempotency_key(conversation_id, idempotency_key):
        reason = "duplicate_idempotency_key"
    elif (
        result_envelope is not None
        and _conversation_task_was_cancelled(
            store,
            conversation_id=conversation_id,
            task_id=result_envelope.task_id,
        )
    ):
        reason = "task_cancelled"
    elif not _is_active_conversation_branch(branch):
        reason = "branch_inactive"
    else:
        head = store.get_conversation_head(conversation_id)
        if head is not None and head.get("active_branch_id") != branch_id:
            reason = "stale_branch"
        elif head is not None and head.get("active_turn_id") != turn_id:
            reason = "stale_turn"

    committed = reason is None
    if committed:
        store.add_committed_idempotency_key(conversation_id, idempotency_key)

    payload = {
        "conversation_id": conversation_id,
        "branch_id": branch_id,
        "turn_id": turn_id,
        "idempotency_key": idempotency_key,
    }
    if result_envelope is not None:
        payload.update(
            {
                "result_id": result_envelope.result_id,
                "task_id": result_envelope.task_id,
                "kind": result_envelope.kind.value,
                "created_at": result_envelope.created_at.isoformat(),
            }
        )
    if reason is not None:
        payload["reason"] = reason

    event_type = "conversation.worker_result_committed" if committed else "conversation.worker_result_ignored"
    store.add_event(make_event(event_type=event_type, actor="runtime", payload=payload))
    store.add_audit_record(make_audit_record(action=event_type, actor="runtime", details=payload))
    _append_conversation_event_log(
        store,
        conversation_id=conversation_id,
        event_type=event_type,
        created_at=result_envelope.created_at if result_envelope is not None else datetime.now(UTC),
        turn_id=turn_id,
        branch_id=branch_id,
        idempotency_key=idempotency_key,
        task_id=result_envelope.task_id if result_envelope is not None else None,
        result_id=result_envelope.result_id if result_envelope is not None else None,
        reason=reason,
    )

    return WorkerResultCommitResult(
        conversation_id=conversation_id,
        branch_id=branch_id,
        turn_id=turn_id,
        idempotency_key=idempotency_key,
        committed=committed,
        reason=reason,
    )



def schedule_heartbeat(
    store: RuntimeStore,
    *,
    capsule_id: str,
    interval_minutes: int,
):
    task = create_recurring_task(capsule_id=capsule_id, interval_minutes=interval_minutes)
    store.add_scheduled_task(task)
    return task



def schedule_reminder(
    store: RuntimeStore,
    *,
    chat_id: str,
    text: str,
    due_at: datetime,
):
    normalized_due_at = normalize_reminder_due_at(due_at)
    task = ScheduledTask(
        task_id=_next_runtime_id("task"),
        capsule_id="",
        schedule_kind=ScheduleKind.ONCE,
        interval_minutes=0,
        enabled=True,
        last_run_at=None,
        failure_count=0,
    )
    store.add_scheduled_task(task)
    store.add_reminder(
        ReminderRecord(
            task_id=task.task_id,
            chat_id=chat_id,
            text=text,
            due_at=normalized_due_at,
        )
    )
    return task



def _is_severe_sentinel_route(route: SignalRoute) -> bool:
    return route.target is SignalTarget.SENTINEL and route.severity in {"high", "critical"}



def _persist_signal_route(store: RuntimeStore, route: SignalRoute) -> SignalRoute:
    payload = {
        "target": route.target.value,
        "severity": route.severity,
        "reason": route.reason,
    }

    if route.target is SignalTarget.DOCTOR:
        store.add_doctor_signal(route)
        recommendation = doctor_recommendation_for_route(route.target.value, route.reason, route.severity)
        # If the route carries a human-readable summary (from alert.message or
        # the playbook summary), use it instead of the generic keyword-matched one.
        if route.summary:
            recommendation["summary"] = route.summary
        if route.recommendation_code:
            recommendation["recommendation_code"] = route.recommendation_code
        # If the route carries the raw error text, include it in the recommendation
        # so it flows through to the doctor action record and the card displayed to
        # the user — without having to parse the mangled reason string.
        if route.error:
            recommendation["error"] = route.error
        store.add_doctor_recommendation(recommendation)
        action = doctor_action_for_recommendation(recommendation)
        existing_action = _find_open_matching_doctor_action(store, action)
        if existing_action is not None:
            store.update_doctor_action(existing_action)
        else:
            store.add_doctor_action(_uniquify_doctor_action_id(store, action))
        event_type = "doctor.signal_received"
        audit_action = "doctor.signal_received"
    elif route.target is SignalTarget.SENTINEL:
        store.add_sentinel_signal(route)
        if _is_severe_sentinel_route(route):
            escalation = _uniquify_sentinel_escalation_id(
                store,
                create_escalation_from_signal_route(route),
            )
            approval = create_approval_request(
                requested_by="sentinel",
                action="review_escalation",
                resource=escalation.escalation_id,
            )
            store.add_approval_request(approval)
            store.add_sentinel_escalation(link_escalation_to_approval(escalation, approval))
        event_type = "sentinel.signal_received"
        audit_action = "sentinel.signal_received"
    else:
        event_type = "signal.noop"
        audit_action = "signal.noop"

    store.add_event(make_event(event_type=event_type, actor="runtime", payload=payload))
    store.add_audit_record(make_audit_record(action=audit_action, actor="runtime", details=payload))
    return route



def report_health_issue(
    store: RuntimeStore,
    issue_type: HealthIssueType,
    source: str,
    message: str,
    details: dict[str, object] | None = None,
) -> SignalRoute:
    alert = make_health_alert(
        issue_type=issue_type,
        source=source,
        message=message,
        details=details,
    )
    route = route_health_alert(alert)
    if alert.details:
        detail_suffix = ";".join(
            f"{key}={value}"
            for key, value in sorted(alert.details.items())
        )
        route = SignalRoute(
            target=route.target,
            reason=f"{route.reason};{detail_suffix}",
            severity=route.severity,
            summary=route.summary if route.recommendation_code else None,
            error=route.error,
            recommendation_code=route.recommendation_code,
        )
    if (
        route.target is SignalTarget.DOCTOR
        and os.environ.get("NULLION_DOCTOR_ENABLED", "true").strip().lower() in {"0", "false", "no", "off"}
    ):
        return route
    return _persist_signal_route(store, route)



def record_policy_signal(
    store: RuntimeStore,
    decision: PolicyDecision,
    action: str,
    resource: str,
) -> SignalRoute:
    route = route_policy_decision(
        decision=decision,
        action=action,
        resource=resource,
    )
    return _persist_signal_route(store, route)



def run_due_scheduled_tasks(store: RuntimeStore, now: datetime):
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    now = now.astimezone(UTC)
    due_tasks = []

    for task in store.list_scheduled_tasks():
        if not task.enabled:
            continue
        reminder = store.get_reminder(task.task_id)
        if reminder is not None:
            if reminder.delivered_at is not None:
                continue
            reminder_due_at = normalize_reminder_due_at(reminder.due_at)
            if reminder_due_at > now:
                continue
            due_tasks.append(task)
            continue

        if not should_run(task.last_run_at, now, task.interval_minutes):
            continue
        if store.get_capsule(task.capsule_id) is None:
            failed_task = mark_task_failed(task)
            store.add_scheduled_task(failed_task)
            report_health_issue(
                store=store,
                issue_type=HealthIssueType.ISSUE,
                source="scheduler",
                message="Scheduled task references missing capsule.",
                details={
                    "task_id": failed_task.task_id,
                    "capsule_id": failed_task.capsule_id,
                    "anomaly": "missing_capsule",
                    "failure_count": failed_task.failure_count,
                },
            )
            if failed_task.failure_count >= 3:
                disabled_task = disable_task(failed_task)
                store.add_scheduled_task(disabled_task)
                details = {
                    "task_id": disabled_task.task_id,
                    "capsule_id": disabled_task.capsule_id,
                    "failure_count": disabled_task.failure_count,
                    "reason": "missing_capsule",
                }
                store.add_event(
                    make_event(
                        event_type="scheduler.task_disabled",
                        actor="scheduler",
                        payload=details,
                    )
                )
                store.add_audit_record(
                    make_audit_record(
                        action="scheduler.disable",
                        actor="scheduler",
                        details=details,
                    )
                )
                _persist_signal_route(
                    store,
                    SignalRoute(
                        target=SignalTarget.SENTINEL,
                        reason=(
                            "scheduler_auto_disabled=true"
                            f";task_id={disabled_task.task_id}"
                            f";capsule_id={disabled_task.capsule_id}"
                            f";failure_count={disabled_task.failure_count}"
                            ";anomaly=missing_capsule"
                        ),
                        severity="high",
                    ),
                )
            continue

        updated_task = mark_task_ran(task, now)
        store.add_scheduled_task(updated_task)
        due_tasks.append(updated_task)

        details = {
            "task_id": updated_task.task_id,
            "capsule_id": updated_task.capsule_id,
            "interval_minutes": updated_task.interval_minutes,
        }
        store.add_event(
            make_event(
                event_type="scheduler.task_triggered",
                actor="scheduler",
                payload=details,
            )
        )
        store.add_audit_record(
            make_audit_record(
                action="scheduler.trigger",
                actor="scheduler",
                details=details,
            )
        )

    return due_tasks



def checkpoint_runtime_store(store: RuntimeStore, path: str | Path) -> Path:
    return save_runtime_store(store, path)



def list_runtime_store_backups(path: str | Path) -> list[dict[str, object]]:
    checkpoint_path = Path(path)
    backups: list[dict[str, object]] = []
    generation = 0
    while True:
        candidate = checkpoint_path.with_name(
            f"{checkpoint_path.name}.bak" if generation == 0 else f"{checkpoint_path.name}.bak.{generation}"
        )
        if not candidate.exists():
            break
        backups.append(
            {
                "generation": generation,
                "name": candidate.name,
                "path": str(candidate),
            }
        )
        generation += 1
    return backups



def get_latest_runtime_restore_metadata(store: RuntimeStore) -> dict[str, object] | None:
    for event in reversed(store.list_events()):
        if event.event_type != "runtime.store_restored":
            continue
        payload = dict(event.payload)
        return {
            "checkpoint_path": payload.get("checkpoint_path"),
            "source": payload.get("source"),
            "generation": payload.get("generation"),
        }
    return None



def _restore_source_name(*, generation: int) -> str:
    return "backup" if generation == 0 else f"backup.{generation}"


def restore_runtime_store_checkpoint(path: str | Path, *, generation: int = 0) -> RuntimeStore:
    checkpoint_path = Path(path)
    restore_runtime_store_backup(checkpoint_path, generation=generation)
    restored_store = load_runtime_store(checkpoint_path)
    details = {
        "checkpoint_path": str(checkpoint_path),
        "source": _restore_source_name(generation=generation),
        "generation": generation,
    }
    restored_store.add_event(
        make_event(
            event_type="runtime.store_restored",
            actor="runtime",
            payload=details,
        )
    )
    restored_store.add_audit_record(
        make_audit_record(
            action="runtime.store_restored",
            actor="runtime",
            details=details,
        )
    )
    return restored_store



def _legacy_runtime_record_identity(record: object) -> str:
    for attr in (
        "event_id",
        "audit_id",
        "update_id",
        "run_id",
        "action_id",
        "escalation_id",
        "frame_id",
        "turn_id",
        "branch_id",
        "proposal_id",
        "skill_id",
        "task_id",
        "id",
    ):
        value = getattr(record, attr, None)
        if isinstance(value, str) and value:
            return f"{attr}:{value}"
    if isinstance(record, dict):
        for key in (
            "event_id",
            "audit_id",
            "update_id",
            "run_id",
            "action_id",
            "escalation_id",
            "frame_id",
            "turn_id",
            "branch_id",
            "proposal_id",
            "skill_id",
            "task_id",
            "id",
        ):
            value = record.get(key)
            if isinstance(value, str) and value:
                return f"{key}:{value}"
    return repr(record)


def _merge_legacy_runtime_list(current: list, legacy: list) -> int:
    imported = 0
    seen = {_legacy_runtime_record_identity(item) for item in current}
    for item in legacy:
        identity = _legacy_runtime_record_identity(item)
        if identity in seen:
            continue
        current.append(deepcopy(item))
        seen.add(identity)
        imported += 1
    return imported


def _merge_legacy_runtime_dict(store: RuntimeStore, legacy_store: RuntimeStore, attr: str) -> int:
    current = getattr(store, attr)
    legacy = getattr(legacy_store, attr)
    imported = 0
    for key, value in legacy.items():
        if key not in current:
            if attr == "skills" and _find_duplicate_skill(store, title=value.title, summary=value.summary, trigger=value.trigger):
                continue
            current[key] = deepcopy(value)
            imported += 1
            continue
        if attr == "approval_requests":
            existing = current[key]
            if existing.status is ApprovalStatus.PENDING and value.status is not ApprovalStatus.PENDING:
                current[key] = deepcopy(value)
                imported += 1
            elif existing.decided_at is None and value.decided_at is not None:
                current[key] = deepcopy(value)
                imported += 1
            elif existing.decided_at is not None and value.decided_at is not None and value.decided_at > existing.decided_at:
                current[key] = deepcopy(value)
                imported += 1
    return imported


def _merge_legacy_runtime_sets(store: RuntimeStore, legacy_store: RuntimeStore, attr: str) -> int:
    current = getattr(store, attr)
    legacy = getattr(legacy_store, attr)
    imported = 0
    for key, values in legacy.items():
        current_values = current.setdefault(key, set())
        before = len(current_values)
        current_values.update(values)
        imported += len(current_values) - before
    return imported


def _import_legacy_json_records_if_needed(store: RuntimeStore, source: Path) -> bool:
    if source.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
        return False
    legacy_json = source.with_name("runtime-store.json")
    if not legacy_json.exists() or legacy_json.resolve() == source.resolve():
        return False
    try:
        legacy_store = load_runtime_store(legacy_json)
    except Exception:
        return False

    imported_by_collection: dict[str, int] = {}
    for attr in (
        "capsules",
        "scheduled_tasks",
        "reminders",
        "suspended_turns",
        "approval_requests",
        "permission_grants",
        "boundary_permits",
        "boundary_policy_rules",
        "mini_agent_runs",
        "missions",
        "builder_proposals",
        "skills",
        "user_facts",
        "preferences",
        "environment_facts",
        "skill_execution_plans",
        "task_frames",
        "active_task_frames",
        "conversation_turns",
        "conversation_branches",
        "conversation_heads",
    ):
        imported_by_collection[attr] = _merge_legacy_runtime_dict(store, legacy_store, attr)
    for attr in ("conversation_commits", "conversation_ingress_ids"):
        imported_by_collection[attr] = _merge_legacy_runtime_sets(store, legacy_store, attr)
    for attr in (
        "events",
        "audit_records",
        "progress_updates",
        "doctor_signals",
        "sentinel_signals",
        "sentinel_escalations",
        "doctor_recommendations",
        "doctor_actions",
        "conversation_events",
    ):
        imported_by_collection[attr] = _merge_legacy_runtime_list(getattr(store, attr), getattr(legacy_store, attr))

    imported_total = sum(imported_by_collection.values())
    if imported_total:
        details = {
            "checkpoint_path": str(source),
            "legacy_path": str(legacy_json),
            "records_imported": str(imported_total),
            **{
                f"{collection}_imported": str(count)
                for collection, count in imported_by_collection.items()
                if count
            },
        }
        store.add_event(make_event(event_type="runtime.legacy_records_imported", actor="runtime", payload=details))
        store.add_audit_record(make_audit_record(action="runtime.legacy_records_imported", actor="runtime", details=details))
        return True
    return False


def bootstrap_runtime_store(path: str | Path) -> RuntimeStore:
    source = Path(path)
    if source.suffix.lower() in {".db", ".sqlite", ".sqlite3"} and not source.exists():
        legacy_json = source.with_name("runtime-store.json")
        if legacy_json.exists():
            store = load_runtime_store(legacy_json)
            save_runtime_store(store, source)
            return store
    if not source.exists():
        return RuntimeStore()
    store = load_runtime_store(source)
    changed = _import_legacy_json_records_if_needed(store, source)
    changed = bool(deduplicate_skills(store, actor="runtime")) or changed
    if changed:
        save_runtime_store(store, source)
    return store



def bootstrap_persistent_runtime(path: str | Path) -> PersistentRuntime:
    checkpoint_path = Path(path)
    store = bootstrap_runtime_store(checkpoint_path)
    fingerprint = render_runtime_store_payload_json(store) if checkpoint_path.exists() else None
    return PersistentRuntime(
        store=store,
        checkpoint_path=checkpoint_path,
        started_at=datetime.now(UTC),
        last_checkpoint_fingerprint=fingerprint,
    )



def render_runtime_status_for_telegram(
    store: RuntimeStore,
    *,
    capsule_id: str | None = None,
    active_only: bool = False,
    checkpoint_path: str | Path | None = None,
) -> str:
    return render_runtime_status_snapshot_for_telegram(
        store,
        capsule_id=capsule_id,
        active_only=active_only,
        checkpoint_path=checkpoint_path,
    )


def render_runtime_status(
    store: RuntimeStore,
    *,
    capsule_id: str | None = None,
    active_only: bool = False,
    checkpoint_path: str | Path | None = None,
) -> str:
    return render_runtime_status_for_telegram(
        store,
        capsule_id=capsule_id,
        active_only=active_only,
        checkpoint_path=checkpoint_path,
    )



def format_runtime_status_for_telegram(snapshot: dict[str, object], *, active_only: bool = False) -> str:
    return format_runtime_status_for_telegram_read_model(snapshot, active_only=active_only)



def render_assistant_turn_for_telegram(
    turn: AssistantTurn,
    *,
    store: RuntimeStore | None = None,
    recommendation_text: str | None = None,
    recommendation_limit: int = 3,
) -> str:
    if store is None and recommendation_text is None and recommendation_limit == 3:
        return render_assistant_turn_snapshot_for_telegram(turn)
    snapshot = build_assistant_turn_snapshot(
        turn,
        store=store,
        recommendation_text=recommendation_text,
        recommendation_limit=recommendation_limit,
    )
    return format_assistant_turn_for_telegram_read_model(snapshot)



def render_progress_update_for_telegram(progress_update: ProgressUpdate) -> str:
    return render_progress_update_for_telegram_read_model(progress_update)



def render_mission_for_telegram(mission: MissionRecord) -> str:
    return render_mission_for_telegram_read_model(mission)



def build_progress_update_snapshot(progress_update: ProgressUpdate) -> dict[str, object]:
    return build_progress_update_snapshot_read_model(progress_update)



def record_progress_update(store: RuntimeStore, progress_update: ProgressUpdate) -> ProgressUpdate | None:
    previous_for_capsule = None
    for existing in reversed(store.list_progress_updates()):
        if existing.capsule_id == progress_update.capsule_id:
            previous_for_capsule = existing
            break

    if previous_for_capsule is None or should_emit_nudge(previous_for_capsule.state, progress_update.state):
        store.add_progress_update(progress_update)
        if store.get_skill_execution_plan(progress_update.capsule_id) is not None:
            transition_skill_execution_plan_for_progress_update(
                store,
                capsule_id=progress_update.capsule_id,
                progress_state=progress_update.state,
            )
        _sync_missions_for_progress_update(store, progress_update)
        payload = {
            "capsule_id": progress_update.capsule_id,
            "state": progress_update.state.value,
            "message": progress_update.message,
        }
        store.add_event(make_event(event_type="progress.updated", actor="runtime", payload=payload))
        store.add_audit_record(make_audit_record(action="progress.updated", actor="runtime", details=payload))
        return progress_update

    return None



def format_progress_update_for_telegram(snapshot: dict[str, object]) -> str:
    return format_progress_update_for_telegram_read_model(snapshot)



def evaluate_builder_decision(packet) -> BuilderDecision:
    return evaluate_builder_decision_read_model(packet)



def build_builder_input_snapshot(packet) -> dict[str, object]:
    return build_builder_input_snapshot_read_model(packet)



def build_codebase_summary(repo_root: str | Path, package_root: str = "src/nullion"):
    return build_codebase_summary_read_model(repo_root, package_root=package_root)



def format_codebase_summary(summary) -> str:
    return format_codebase_summary_read_model(summary)



def build_system_context_snapshot(
    *,
    project_summary: str | None = None,
    goals: Iterable[str] = (),
    initial_focus: Iterable[str] = (),
    tool_registry: ToolRegistry | None = None,
    available_tools: Iterable[object] | None = None,
    sections: Mapping[str, Iterable[str]] | Iterable[object] | None = None,
):
    return build_system_context_snapshot_read_model(
        project_summary=project_summary,
        goals=goals,
        initial_focus=initial_focus,
        tool_registry=tool_registry,
        available_tools=available_tools,
        sections=sections,
    )



def format_system_context_for_prompt(snapshot) -> str:
    return format_system_context_for_prompt_read_model(snapshot)



def build_builder_proposal(decision: BuilderDecision) -> BuilderProposal:
    return build_builder_proposal_read_model(decision)



def build_builder_proposal_snapshot(proposal: BuilderProposal) -> dict[str, object]:
    return build_builder_proposal_snapshot_read_model(proposal)



def format_builder_proposal_for_telegram(snapshot: dict[str, object]) -> str:
    return format_builder_proposal_for_telegram_read_model(snapshot)



def render_builder_proposal_for_telegram(proposal: BuilderProposal) -> str:
    return render_builder_proposal_for_telegram_read_model(proposal)


def _build_skill_recommendation_snapshot(skills: list[SkillRecord]) -> list[dict[str, str]]:
    return [
        {
            "skill_id": skill.skill_id,
            "title": skill.title,
            "trigger": skill.trigger,
        }
        for skill in skills
    ]



def _build_applied_skill_snapshot(skill: SkillRecord) -> dict[str, object]:
    return {
        "skill_id": skill.skill_id,
        "title": skill.title,
        "trigger": skill.trigger,
        "steps": list(skill.steps),
    }



def _build_planner_aware_skill_execution_plan(
    skill: SkillRecord,
    *,
    store: RuntimeStore,
    capsule_id: str,
) -> SkillExecutionPlan:
    plan: SkillExecutionPlan = build_skill_execution_plan(skill)

    for run in store.list_mini_agent_runs():
        if run.capsule_id != capsule_id:
            continue
        plan = transition_skill_execution_plan_for_mini_agent_status(plan, run.status)

    for progress_update in store.list_progress_updates():
        if progress_update.capsule_id != capsule_id:
            continue
        plan = transition_skill_execution_plan_for_progress(plan, progress_update.state)

    return plan



def build_assistant_turn_snapshot(
    turn: AssistantTurn,
    *,
    store: RuntimeStore | None = None,
    recommendation_text: str | None = None,
    recommendation_limit: int = 3,
) -> dict[str, object]:
    snapshot = build_assistant_turn_snapshot_read_model(turn)
    if store is None:
        return snapshot

    prompt = recommendation_text if recommendation_text is not None else turn.capsule.goal
    scored_recommendations = _recommend_skills_with_scores(store, prompt, limit=recommendation_limit)
    recommendations = [
        skill
        for score, skill in scored_recommendations
        if score >= SKILL_RECOMMENDATION_SCORE
    ]
    if recommendations:
        snapshot["recommended_skills"] = _build_skill_recommendation_snapshot(recommendations)
    if scored_recommendations and scored_recommendations[0][0] >= STRONG_SKILL_APPLICATION_SCORE:
        applied_skill = scored_recommendations[0][1]
        snapshot["applied_skill"] = _build_applied_skill_snapshot(applied_skill)

        planner_aware_plan = _build_planner_aware_skill_execution_plan(
            applied_skill,
            store=store,
            capsule_id=turn.capsule.capsule_id,
        )
        skill_execution_plan = build_skill_execution_plan_snapshot(planner_aware_plan)
        snapshot["skill_execution_plan"] = skill_execution_plan
        snapshot["skill_execution_intent"] = build_skill_execution_intent_snapshot(planner_aware_plan)

        active_step = skill_execution_plan.get("active_step")
        active_step_index = skill_execution_plan.get("active_step_index")
        total_steps = skill_execution_plan.get("total_steps")
        completed_steps = skill_execution_plan.get("completed_steps")
        if isinstance(active_step, str) and active_step.strip() and isinstance(active_step_index, int) and isinstance(total_steps, int):
            snapshot["next_step"] = f"Skill step {active_step_index + 1}/{total_steps}: {active_step.strip()}"
        elif (
            isinstance(completed_steps, int)
            and isinstance(total_steps, int)
            and total_steps > 0
            and completed_steps == total_steps
        ):
            snapshot["next_step"] = "Skill plan complete."
    return snapshot


def build_tool_result_snapshot(result: ToolResult) -> dict[str, object]:
    return build_tool_result_snapshot_read_model(result)


def render_tool_result_for_telegram(result: ToolResult) -> str:
    return render_tool_result_for_telegram_read_model(result)


def _normalize_skill_steps(steps: list[str]) -> list[str]:
    return [step.strip() for step in steps if step.strip()]


def _normalize_skill_tags(tags: list[str] | None) -> list[str]:
    normalized_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in tags or []:
        normalized_tag = tag.strip().lower()
        if not normalized_tag or normalized_tag in seen_tags:
            continue
        seen_tags.add(normalized_tag)
        normalized_tags.append(normalized_tag)
    return normalized_tags



def create_mission(
    store: RuntimeStore,
    *,
    owner: str,
    title: str,
    goal: str,
    continuation_policy: MissionContinuationPolicy = MissionContinuationPolicy.MANUAL,
    mission_id: str | None = None,
    created_from_capsule_id: str | None = None,
    active_capsule_id: str | None = None,
    active_step_id: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="create_mission")
    normalized_owner = owner.strip()
    normalized_title = title.strip()
    normalized_goal = goal.strip()
    if not normalized_owner:
        raise ValueError("owner is required")
    if not normalized_title:
        raise ValueError("title is required")
    if not normalized_goal:
        raise ValueError("goal is required")

    mission = MissionRecord(
        mission_id=mission_id or f"mission-{uuid4().hex[:12]}",
        owner=normalized_owner,
        title=normalized_title,
        goal=normalized_goal,
        status=MissionStatus.PENDING,
        continuation_policy=continuation_policy,
        created_from_capsule_id=created_from_capsule_id,
        active_capsule_id=active_capsule_id,
        active_step_id=active_step_id,
        steps=(),
        completion_checklist=(),
    )
    store.add_mission(mission)
    payload = {
        "mission_id": mission.mission_id,
        "owner": mission.owner,
        "title": mission.title,
        "status": mission.status.value,
        "continuation_policy": mission.continuation_policy.value,
    }
    store.add_event(make_event(event_type="mission.created", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="mission.created", actor=normalized_actor, details=payload))
    return mission



def plan_mission(
    store: RuntimeStore,
    *,
    owner: str,
    user_message: str,
    active_task_frame: TaskFrame | None = None,
    mission_id: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    planner = TaskPlanner()
    planned_mission = planner.plan(
        user_message=user_message,
        principal_id=owner,
        active_task_frame=active_task_frame,
    )
    mission = create_mission(
        store,
        owner=owner,
        title=planned_mission.title,
        goal=planned_mission.goal,
        continuation_policy=planned_mission.continuation_policy,
        mission_id=mission_id or planned_mission.mission_id,
        actor=actor,
    )
    if planned_mission.steps:
        mission = set_mission_plan(
            store,
            mission.mission_id,
            steps=planned_mission.steps,
            active_step_id=planned_mission.steps[0].step_id,
            actor=actor,
        )
        mission = mark_mission_step_running(store, mission.mission_id, step_id=mission.active_step_id, actor=actor)
    return mission



def _mission_payload(mission: MissionRecord) -> dict[str, str | None]:
    return {
        "mission_id": mission.mission_id,
        "owner": mission.owner,
        "title": mission.title,
        "goal": mission.goal,
        "status": mission.status.value,
        "active_capsule_id": mission.active_capsule_id,
        "active_step_id": mission.active_step_id,
        "waiting_on": mission.waiting_on,
        "blocked_reason": mission.blocked_reason,
        "result_summary": mission.result_summary,
        "terminal_reason": None if mission.terminal_reason is None else mission.terminal_reason.value,
    }



def _mission_event_type(status: MissionStatus) -> str:
    return f"mission.{status.value}"



def transition_mission_status(
    store: RuntimeStore,
    mission_id: str,
    *,
    new_status: MissionStatus,
    waiting_on: str | None = None,
    blocked_reason: str | None = None,
    result_summary: str | None = None,
    terminal_reason: MissionTerminalReason | None = None,
    active_capsule_id: str | None = None,
    active_step_id: str | None | object = _UNSET,
    actor: str = "runtime",
) -> MissionRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="transition_mission_status")
    existing = store.get_mission(mission_id)
    if existing is None:
        raise KeyError(mission_id)

    normalized_waiting_on = waiting_on.strip() if isinstance(waiting_on, str) and waiting_on.strip() else None
    normalized_blocked_reason = blocked_reason.strip() if isinstance(blocked_reason, str) and blocked_reason.strip() else None
    normalized_result_summary = result_summary.strip() if isinstance(result_summary, str) and result_summary.strip() else None
    active_step_id_is_set = active_step_id is not _UNSET

    if new_status in {MissionStatus.PENDING, MissionStatus.RUNNING, MissionStatus.VERIFYING}:
        normalized_waiting_on = None
        normalized_blocked_reason = None
        normalized_result_summary = None
        terminal_reason = None
    elif new_status in {MissionStatus.WAITING_APPROVAL, MissionStatus.WAITING_USER}:
        normalized_blocked_reason = None
        normalized_result_summary = None
        terminal_reason = None
    elif new_status is MissionStatus.BLOCKED:
        normalized_waiting_on = None
        normalized_result_summary = None
        terminal_reason = None
    elif new_status is MissionStatus.COMPLETED:
        terminal_reason = terminal_reason or MissionTerminalReason.COMPLETED
        normalized_waiting_on = None
        normalized_blocked_reason = None
        active_step_id = None
        active_step_id_is_set = True
    elif new_status is MissionStatus.FAILED:
        terminal_reason = terminal_reason or MissionTerminalReason.EXECUTION_FAILED
        normalized_waiting_on = None
        normalized_blocked_reason = None
    elif new_status is MissionStatus.CANCELLED:
        terminal_reason = terminal_reason or MissionTerminalReason.USER_CANCELLED
        normalized_waiting_on = None
        normalized_blocked_reason = None

    updated_steps = existing.steps
    if new_status is MissionStatus.COMPLETED:
        updated_steps = tuple(
            replace(step, status="completed")
            if step.status in {"pending", "running"}
            else step
            for step in existing.steps
        )

    updated = replace(
        existing,
        status=new_status,
        active_capsule_id=existing.active_capsule_id if active_capsule_id is None else active_capsule_id,
        active_step_id=existing.active_step_id if not active_step_id_is_set else active_step_id,
        steps=updated_steps,
        waiting_on=normalized_waiting_on,
        blocked_reason=normalized_blocked_reason,
        result_summary=normalized_result_summary,
        terminal_reason=terminal_reason,
        updated_at=datetime.now(UTC),
    )
    store.add_mission(updated)
    payload = _mission_payload(updated)
    event_type = _mission_event_type(new_status)
    store.add_event(make_event(event_type=event_type, actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action=event_type, actor=normalized_actor, details=payload))
    return updated



def mark_mission_running(store: RuntimeStore, mission_id: str, *, actor: str = "runtime") -> MissionRecord:
    return transition_mission_status(store, mission_id, new_status=MissionStatus.RUNNING, actor=actor)



def mark_mission_waiting_approval(
    store: RuntimeStore,
    mission_id: str,
    *,
    waiting_on: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return transition_mission_status(
        store,
        mission_id,
        new_status=MissionStatus.WAITING_APPROVAL,
        waiting_on=waiting_on,
        actor=actor,
    )



def mark_mission_waiting_user(
    store: RuntimeStore,
    mission_id: str,
    *,
    waiting_on: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return transition_mission_status(
        store,
        mission_id,
        new_status=MissionStatus.WAITING_USER,
        waiting_on=waiting_on,
        actor=actor,
    )



def mark_mission_blocked(
    store: RuntimeStore,
    mission_id: str,
    *,
    blocked_reason: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return transition_mission_status(
        store,
        mission_id,
        new_status=MissionStatus.BLOCKED,
        blocked_reason=blocked_reason,
        actor=actor,
    )



def mark_mission_completed(
    store: RuntimeStore,
    mission_id: str,
    *,
    result_summary: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return transition_mission_status(
        store,
        mission_id,
        new_status=MissionStatus.COMPLETED,
        result_summary=result_summary,
        actor=actor,
    )



def mark_mission_failed(
    store: RuntimeStore,
    mission_id: str,
    *,
    result_summary: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return transition_mission_status(
        store,
        mission_id,
        new_status=MissionStatus.FAILED,
        result_summary=result_summary,
        actor=actor,
    )



def get_mission(store: RuntimeStore, mission_id: str) -> MissionRecord | None:
    return store.get_mission(mission_id)


def list_missions(store: RuntimeStore) -> list[MissionRecord]:
    return store.list_missions()


def _normalize_mission_steps(steps: Iterable[MissionStep]) -> tuple[MissionStep, ...]:
    normalized_steps: list[MissionStep] = []
    seen_step_ids: set[str] = set()
    for step in steps:
        if not isinstance(step, MissionStep):
            raise TypeError("steps must contain MissionStep values")
        step_id = step.step_id.strip()
        title = step.title.strip()
        status = step.status.strip().lower()
        kind = step.kind.strip().lower()
        notes = step.notes.strip() if isinstance(step.notes, str) and step.notes.strip() else None
        capsule_id = step.capsule_id.strip() if isinstance(step.capsule_id, str) and step.capsule_id.strip() else None
        mini_agent_run_id = (
            step.mini_agent_run_id.strip()
            if isinstance(step.mini_agent_run_id, str) and step.mini_agent_run_id.strip()
            else None
        )
        mini_agent_run_ids = tuple(
            run_id.strip()
            for run_id in step.mini_agent_run_ids
            if isinstance(run_id, str) and run_id.strip()
        )
        required_mini_agent_run_ids = tuple(
            run_id.strip()
            for run_id in step.required_mini_agent_run_ids
            if isinstance(run_id, str) and run_id.strip()
        )
        if mini_agent_run_id is not None and mini_agent_run_id in mini_agent_run_ids:
            mini_agent_run_ids = tuple(run_id for run_id in mini_agent_run_ids if run_id != mini_agent_run_id)
        if mini_agent_run_id is not None and mini_agent_run_id in required_mini_agent_run_ids:
            required_mini_agent_run_ids = tuple(
                run_id for run_id in required_mini_agent_run_ids if run_id != mini_agent_run_id
            )
        if not step_id:
            raise ValueError("mission step id is required")
        if step_id in seen_step_ids:
            raise ValueError(f"duplicate mission step id: {step_id}")
        seen_step_ids.add(step_id)
        if not title:
            raise ValueError("mission step title is required")
        if not status:
            raise ValueError("mission step status is required")
        if status not in MISSION_STEP_STATUSES:
            raise ValueError(f"invalid mission step status: {status}")
        if not kind:
            raise ValueError("mission step kind is required")
        if kind not in MISSION_STEP_KINDS:
            raise ValueError(f"invalid mission step kind: {kind}")
        normalized_steps.append(
            replace(
                step,
                step_id=step_id,
                title=title,
                status=status,
                kind=kind,
                capsule_id=capsule_id,
                mini_agent_run_id=mini_agent_run_id,
                mini_agent_run_ids=mini_agent_run_ids,
                required_mini_agent_run_ids=required_mini_agent_run_ids,
                notes=notes,
            )
        )
    return tuple(normalized_steps)


def _normalize_mission_checklist(
    completion_checklist: Iterable[MissionChecklistItem] | None,
) -> tuple[MissionChecklistItem, ...]:
    if completion_checklist is None:
        return ()
    normalized_items: list[MissionChecklistItem] = []
    seen_item_ids: set[str] = set()
    for item in completion_checklist:
        if not isinstance(item, MissionChecklistItem):
            raise TypeError("completion_checklist must contain MissionChecklistItem values")
        item_id = item.item_id.strip()
        label = item.label.strip()
        details = item.details.strip() if isinstance(item.details, str) and item.details.strip() else None
        if not item_id:
            raise ValueError("mission checklist item id is required")
        if item_id in seen_item_ids:
            raise ValueError(f"duplicate mission checklist item id: {item_id}")
        seen_item_ids.add(item_id)
        if not label:
            raise ValueError("mission checklist item label is required")
        normalized_items.append(replace(item, item_id=item_id, label=label, details=details))
    return tuple(normalized_items)


def _resolve_active_step_id(
    *,
    existing: MissionRecord,
    steps: tuple[MissionStep, ...],
    active_step_id: str | None,
) -> str | None:
    valid_step_ids = {step.step_id for step in steps}
    if isinstance(active_step_id, str):
        normalized_active_step_id = active_step_id.strip()
        if not normalized_active_step_id:
            return None
        if normalized_active_step_id not in valid_step_ids:
            raise ValueError(f"active_step_id not found in mission steps: {normalized_active_step_id}")
        return normalized_active_step_id
    for step in steps:
        if step.status == "running":
            return step.step_id
    if existing.active_step_id in valid_step_ids:
        return existing.active_step_id
    return None


def set_mission_plan(
    store: RuntimeStore,
    mission_id: str,
    *,
    steps: Iterable[MissionStep],
    completion_checklist: Iterable[MissionChecklistItem] | None = None,
    active_step_id: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="set_mission_plan")
    existing = store.get_mission(mission_id)
    if existing is None:
        raise KeyError(mission_id)

    normalized_steps = _normalize_mission_steps(steps)
    normalized_checklist = (
        existing.completion_checklist
        if completion_checklist is None
        else _normalize_mission_checklist(completion_checklist)
    )
    resolved_active_step_id = _resolve_active_step_id(
        existing=existing,
        steps=normalized_steps,
        active_step_id=active_step_id,
    )
    updated = replace(
        existing,
        steps=normalized_steps,
        completion_checklist=normalized_checklist,
        active_step_id=resolved_active_step_id,
        updated_at=datetime.now(UTC),
    )
    store.add_mission(updated)
    payload = {
        "mission_id": updated.mission_id,
        "owner": updated.owner,
        "active_step_id": updated.active_step_id,
        "step_count": str(len(updated.steps)),
        "checklist_count": str(len(updated.completion_checklist)),
    }
    store.add_event(make_event(event_type="mission.plan_updated", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="mission.plan_updated", actor=normalized_actor, details=payload))
    return updated


def _normalize_optional_text(value: str | None) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _mutate_mission_step(
    store: RuntimeStore,
    mission_id: str,
    *,
    step_id: str,
    status: str | None = None,
    notes: str | None | object = _UNSET,
    actor: str = "runtime",
) -> MissionRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="mutate_mission_step")
    existing = store.get_mission(mission_id)
    if existing is None:
        raise KeyError(mission_id)
    if existing.status in {MissionStatus.COMPLETED, MissionStatus.FAILED, MissionStatus.CANCELLED}:
        raise ValueError(f"cannot mutate steps for terminal mission: {existing.status.value}")

    normalized_step_id = step_id.strip()
    if not normalized_step_id:
        raise ValueError("step_id is required")

    normalized_status = None
    if status is not None:
        normalized_status = status.strip().lower()
        if not normalized_status:
            raise ValueError("mission step status is required")
        if normalized_status not in MISSION_STEP_STATUSES:
            raise ValueError(f"invalid mission step status: {normalized_status}")

    notes_is_set = notes is not _UNSET
    normalized_notes = _normalize_optional_text(notes if notes_is_set else None)

    updated_steps: list[MissionStep] = []
    target_step: MissionStep | None = None
    for step in existing.steps:
        if step.step_id != normalized_step_id:
            updated_steps.append(step)
            continue
        target_step = step
        updated_steps.append(
            replace(
                step,
                status=step.status if normalized_status is None else normalized_status,
                notes=normalized_notes if notes_is_set else step.notes,
            )
        )

    if target_step is None:
        raise KeyError(normalized_step_id)

    mutated_step = next(step for step in updated_steps if step.step_id == normalized_step_id)
    active_step_id = existing.active_step_id
    if mutated_step.status == "running":
        active_step_id = mutated_step.step_id
    elif active_step_id == mutated_step.step_id and mutated_step.status in {"completed", "failed", "blocked", "skipped"}:
        active_step_id = None

    updated = replace(
        existing,
        steps=tuple(updated_steps),
        active_step_id=active_step_id,
        updated_at=datetime.now(UTC),
    )
    store.add_mission(updated)
    payload = {
        "mission_id": updated.mission_id,
        "step_id": mutated_step.step_id,
        "status": mutated_step.status,
        "kind": mutated_step.kind,
        "active_step_id": updated.active_step_id,
    }
    store.add_event(make_event(event_type="mission.step_updated", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="mission.step_updated", actor=normalized_actor, details=payload))
    return advance_mission(store, mission_id, actor=normalized_actor)


def mark_mission_step_running(
    store: RuntimeStore,
    mission_id: str,
    *,
    step_id: str,
    notes: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return _mutate_mission_step(
        store,
        mission_id,
        step_id=step_id,
        status="running",
        notes=notes,
        actor=actor,
    )


def mark_mission_step_completed(
    store: RuntimeStore,
    mission_id: str,
    *,
    step_id: str,
    notes: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return _mutate_mission_step(
        store,
        mission_id,
        step_id=step_id,
        status="completed",
        notes=notes,
        actor=actor,
    )


def mark_mission_step_failed(
    store: RuntimeStore,
    mission_id: str,
    *,
    step_id: str,
    notes: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return _mutate_mission_step(
        store,
        mission_id,
        step_id=step_id,
        status="failed",
        notes=notes,
        actor=actor,
    )


def mark_mission_step_blocked(
    store: RuntimeStore,
    mission_id: str,
    *,
    step_id: str,
    notes: str | None = None,
    actor: str = "runtime",
) -> MissionRecord:
    return _mutate_mission_step(
        store,
        mission_id,
        step_id=step_id,
        status="blocked",
        notes=notes,
        actor=actor,
    )


def link_mini_agent_run_to_mission_step(
    store: RuntimeStore,
    mission_id: str,
    *,
    step_id: str,
    mini_agent_run_id: str,
    required: bool = True,
    actor: str = "runtime",
) -> MissionRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="link_mini_agent_run_to_mission_step")
    existing = store.get_mission(mission_id)
    if existing is None:
        raise KeyError(mission_id)
    if existing.status in {MissionStatus.COMPLETED, MissionStatus.FAILED, MissionStatus.CANCELLED}:
        raise ValueError(f"cannot mutate steps for terminal mission: {existing.status.value}")

    normalized_step_id = step_id.strip()
    if not normalized_step_id:
        raise ValueError("step_id is required")

    normalized_run_id = mini_agent_run_id.strip()
    if not normalized_run_id:
        raise ValueError("mini_agent_run_id is required")

    normalized_required = bool(required)
    updated_steps: list[MissionStep] = []
    target_step: MissionStep | None = None
    changed = False
    for step in existing.steps:
        if step.step_id != normalized_step_id:
            updated_steps.append(step)
            continue

        target_step = step
        linked_run_ids = [
            run_id
            for run_id in (step.mini_agent_run_id, *step.mini_agent_run_ids)
            if isinstance(run_id, str) and run_id.strip()
        ]
        required_run_ids = [
            run_id
            for run_id in (step.mini_agent_run_id, *step.required_mini_agent_run_ids)
            if isinstance(run_id, str) and run_id.strip()
        ]
        if normalized_run_id in linked_run_ids:
            if normalized_required and normalized_run_id not in required_run_ids:
                if step.mini_agent_run_id == normalized_run_id:
                    updated_steps.append(step)
                else:
                    updated_steps.append(
                        replace(
                            step,
                            required_mini_agent_run_ids=(*step.required_mini_agent_run_ids, normalized_run_id),
                        )
                    )
                changed = True
            else:
                updated_steps.append(step)
            continue

        changed = True
        if step.mini_agent_run_id is None:
            updated_steps.append(replace(step, mini_agent_run_id=normalized_run_id))
        elif normalized_required:
            updated_steps.append(
                replace(
                    step,
                    mini_agent_run_ids=(*step.mini_agent_run_ids, normalized_run_id),
                    required_mini_agent_run_ids=(*step.required_mini_agent_run_ids, normalized_run_id),
                )
            )
        else:
            updated_steps.append(replace(step, mini_agent_run_ids=(*step.mini_agent_run_ids, normalized_run_id)))

    if target_step is None:
        raise KeyError(normalized_step_id)

    if changed:
        updated = replace(existing, steps=tuple(updated_steps), updated_at=datetime.now(UTC))
        store.add_mission(updated)
        payload = {
            "mission_id": updated.mission_id,
            "step_id": normalized_step_id,
            "mini_agent_run_id": normalized_run_id,
            "required": str(normalized_required).lower(),
            "linked_run_ids": [
                run_id
                for run_id in (target_step.mini_agent_run_id, *target_step.mini_agent_run_ids, normalized_run_id)
                if isinstance(run_id, str) and run_id.strip()
            ],
            "required_run_ids": [
                run_id
                for run_id in (target_step.mini_agent_run_id, *target_step.required_mini_agent_run_ids, normalized_run_id)
                if isinstance(run_id, str) and run_id.strip()
            ],
        }
        store.add_event(make_event(event_type="mission.step_mini_agent_run_linked", actor=normalized_actor, payload=payload))
        store.add_audit_record(
            make_audit_record(action="mission.step_mini_agent_run_linked", actor=normalized_actor, details=payload)
        )

    return advance_mission(store, mission_id, actor=normalized_actor)


def advance_mission_step(store: RuntimeStore, mission_id: str, *, actor: str = "runtime") -> MissionRecord:
    return advance_mission(store, mission_id, actor=actor)


def attach_mini_agent_run_to_mission_step(
    store: RuntimeStore,
    mission_id: str,
    *,
    step_id: str,
    mini_agent_run_id: str,
    actor: str = "runtime",
) -> MissionRecord:
    return link_mini_agent_run_to_mission_step(
        store,
        mission_id,
        step_id=step_id,
        mini_agent_run_id=mini_agent_run_id,
        actor=actor,
    )


def set_mission_checklist_item_satisfied(
    store: RuntimeStore,
    mission_id: str,
    *,
    item_id: str,
    satisfied: bool,
    details: str | None | object = _UNSET,
    actor: str = "runtime",
) -> MissionRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="set_mission_checklist_item_satisfied")
    existing = store.get_mission(mission_id)
    if existing is None:
        raise KeyError(mission_id)
    if existing.status in {MissionStatus.COMPLETED, MissionStatus.FAILED, MissionStatus.CANCELLED}:
        raise ValueError(f"cannot mutate checklist for terminal mission: {existing.status.value}")

    normalized_item_id = item_id.strip()
    if not normalized_item_id:
        raise ValueError("item_id is required")

    details_is_set = details is not _UNSET
    normalized_details = _normalize_optional_text(details if details_is_set else None)

    updated_items: list[MissionChecklistItem] = []
    target_item: MissionChecklistItem | None = None
    for checklist_item in existing.completion_checklist:
        if checklist_item.item_id != normalized_item_id:
            updated_items.append(checklist_item)
            continue
        target_item = checklist_item
        updated_items.append(
            replace(
                checklist_item,
                satisfied=bool(satisfied),
                details=normalized_details if details_is_set else checklist_item.details,
            )
        )

    if target_item is None:
        raise KeyError(normalized_item_id)

    updated = replace(
        existing,
        completion_checklist=tuple(updated_items),
        updated_at=datetime.now(UTC),
    )
    store.add_mission(updated)
    payload = {
        "mission_id": updated.mission_id,
        "item_id": normalized_item_id,
        "satisfied": str(bool(satisfied)).lower(),
    }
    store.add_event(make_event(event_type="mission.checklist_item_updated", actor=normalized_actor, payload=payload))
    store.add_audit_record(
        make_audit_record(action="mission.checklist_item_updated", actor=normalized_actor, details=payload)
    )
    return advance_mission(store, mission_id, actor=normalized_actor)


def update_mission_step_notes(
    store: RuntimeStore,
    mission_id: str,
    *,
    step_id: str,
    notes: str | None,
    actor: str = "runtime",
) -> MissionRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="update_mission_step_notes")
    existing = store.get_mission(mission_id)
    if existing is None:
        raise KeyError(mission_id)
    if existing.status in {MissionStatus.COMPLETED, MissionStatus.FAILED, MissionStatus.CANCELLED}:
        raise ValueError(f"cannot mutate steps for terminal mission: {existing.status.value}")

    normalized_step_id = step_id.strip()
    if not normalized_step_id:
        raise ValueError("step_id is required")

    normalized_notes = _normalize_optional_text(notes)
    updated_steps: list[MissionStep] = []
    target_step: MissionStep | None = None
    for step in existing.steps:
        if step.step_id != normalized_step_id:
            updated_steps.append(step)
            continue
        target_step = step
        updated_steps.append(replace(step, notes=normalized_notes))

    if target_step is None:
        raise KeyError(normalized_step_id)

    updated = replace(existing, steps=tuple(updated_steps), updated_at=datetime.now(UTC))
    store.add_mission(updated)
    payload = {
        "mission_id": updated.mission_id,
        "step_id": normalized_step_id,
        "notes": normalized_notes,
    }
    store.add_event(make_event(event_type="mission.step_notes_updated", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="mission.step_notes_updated", actor=normalized_actor, details=payload))
    return updated


def _required_checklist_satisfied(mission: MissionRecord) -> bool:
    return all(item.satisfied for item in mission.completion_checklist if item.required)


def _first_step_with_status(mission: MissionRecord, statuses: set[str]) -> MissionStep | None:
    for step in mission.steps:
        if step.status in statuses:
            return step
    return None


def _linked_missions_for_capsule(store: RuntimeStore, capsule_id: str) -> list[MissionRecord]:
    return [
        mission
        for mission in store.list_missions()
        if mission.active_capsule_id == capsule_id or mission.created_from_capsule_id == capsule_id
    ]


def _update_mission_last_progress_message(
    store: RuntimeStore,
    mission: MissionRecord,
    *,
    message: str,
) -> MissionRecord:
    updated = replace(
        mission,
        last_progress_message=message,
        updated_at=datetime.now(UTC),
    )
    store.add_mission(updated)
    return updated


def _update_mission_step_for_mini_agent_run(
    store: RuntimeStore,
    mission: MissionRecord,
    *,
    run: MiniAgentRun,
) -> MissionRecord:
    changed = False
    updated_steps: list[MissionStep] = []
    for step in mission.steps:
        linked_run_ids = tuple(
            run_id for run_id in (step.mini_agent_run_id, *step.mini_agent_run_ids) if run_id is not None
        )
        if run.run_id not in linked_run_ids:
            updated_steps.append(step)
            continue

        required_run_ids = tuple(
            run_id for run_id in (step.mini_agent_run_id, *step.required_mini_agent_run_ids) if run_id is not None
        )
        relevant_run_ids = required_run_ids or linked_run_ids
        relevant_runs = [store.get_mini_agent_run(run_id) for run_id in relevant_run_ids]
        relevant_runs = [linked_run for linked_run in relevant_runs if linked_run is not None]
        is_required_run = run.run_id in required_run_ids
        if not is_required_run and step.status == "completed":
            updated_steps.append(step)
            continue

        if any(linked_run.status is MiniAgentRunStatus.FAILED for linked_run in relevant_runs):
            next_status = "failed"
        elif relevant_runs and all(linked_run.status is MiniAgentRunStatus.COMPLETED for linked_run in relevant_runs):
            next_status = "completed"
        elif relevant_runs and any(linked_run.status is not MiniAgentRunStatus.PENDING for linked_run in relevant_runs):
            next_status = "running"
        else:
            next_status = "pending"

        changed = True
        next_notes = (
            run.result_summary
            if run.result_summary is not None and (is_required_run or step.notes is None or step.status != "completed")
            else step.notes
        )
        updated_steps.append(
            replace(
                step,
                status=next_status,
                notes=next_notes,
            )
        )
    if not changed:
        return mission
    updated = replace(
        mission,
        steps=tuple(updated_steps),
        updated_at=datetime.now(UTC),
    )
    store.add_mission(updated)
    return updated


def _sync_missions_for_progress_update(store: RuntimeStore, progress_update: ProgressUpdate) -> None:
    for mission in _linked_missions_for_capsule(store, progress_update.capsule_id):
        updated = _update_mission_last_progress_message(store, mission, message=progress_update.message)
        if progress_update.state is ProgressState.WAITING_APPROVAL:
            transition_mission_status(
                store,
                updated.mission_id,
                new_status=MissionStatus.WAITING_APPROVAL,
                waiting_on=progress_update.message,
                actor="runtime",
            )
        elif progress_update.state is ProgressState.BLOCKED:
            transition_mission_status(
                store,
                updated.mission_id,
                new_status=MissionStatus.BLOCKED,
                blocked_reason=progress_update.message,
                actor="runtime",
            )
        else:
            advance_mission(store, updated.mission_id, actor="runtime")


def _sync_missions_for_mini_agent_run(store: RuntimeStore, run: MiniAgentRun) -> None:
    for mission in _linked_missions_for_capsule(store, run.capsule_id):
        updated = _update_mission_step_for_mini_agent_run(store, mission, run=run)
        advance_mission(store, updated.mission_id, actor="runtime")


def advance_mission(store: RuntimeStore, mission_id: str, *, actor: str = "runtime") -> MissionRecord:
    mission = get_mission(store, mission_id)
    if mission is None:
        raise KeyError(mission_id)
    if mission.status in {MissionStatus.COMPLETED, MissionStatus.FAILED, MissionStatus.CANCELLED}:
        return mission

    running_step = _first_step_with_status(mission, {"running"})
    if running_step is not None:
        return transition_mission_status(
            store,
            mission_id,
            new_status=MissionStatus.RUNNING,
            active_step_id=running_step.step_id,
            actor=actor,
        )

    failed_step = _first_step_with_status(mission, {"failed"})
    if failed_step is not None:
        return mark_mission_failed(
            store,
            mission_id,
            result_summary=failed_step.notes or failed_step.title,
            actor=actor,
        )

    blocked_step = _first_step_with_status(mission, {"blocked"})
    if blocked_step is not None:
        return mark_mission_blocked(
            store,
            mission_id,
            blocked_reason=blocked_step.notes or blocked_step.title,
            actor=actor,
        )

    pending_steps = [step for step in mission.steps if step.status == "pending"]
    for step in pending_steps:
        detail = step.notes or step.title
        if step.kind == "approval":
            return transition_mission_status(
                store,
                mission_id,
                new_status=MissionStatus.WAITING_APPROVAL,
                waiting_on=detail,
                active_step_id=step.step_id,
                actor=actor,
            )
        if step.kind == "user_input":
            return transition_mission_status(
                store,
                mission_id,
                new_status=MissionStatus.WAITING_USER,
                waiting_on=detail,
                active_step_id=step.step_id,
                actor=actor,
            )
        if step.kind == "external_wait":
            return transition_mission_status(
                store,
                mission_id,
                new_status=MissionStatus.BLOCKED,
                blocked_reason=detail,
                active_step_id=step.step_id,
                actor=actor,
            )

    if mission.steps and all(step.status in {"completed", "skipped"} for step in mission.steps):
        if _required_checklist_satisfied(mission):
            return mark_mission_completed(
                store,
                mission_id,
                result_summary="Mission checklist satisfied.",
                actor=actor,
            )
        return transition_mission_status(
            store,
            mission_id,
            new_status=MissionStatus.VERIFYING,
            actor=actor,
        )

    if pending_steps:
        next_step = pending_steps[0]
        if mission.continuation_policy is MissionContinuationPolicy.MANUAL:
            return transition_mission_status(
                store,
                mission_id,
                new_status=MissionStatus.PENDING,
                active_step_id=next_step.step_id,
                actor=actor,
            )
        return transition_mission_status(
            store,
            mission_id,
            new_status=MissionStatus.RUNNING,
            active_step_id=next_step.step_id,
            actor=actor,
        )

    return mission


def _append_skill_workflow_signal(
    skill: SkillRecord,
    *,
    source: str,
    summary: str,
    recorded_at: datetime,
    max_signals: int = 5,
) -> None:
    normalized_source = source.strip().lower()
    normalized_summary = summary.strip()
    if not normalized_source or not normalized_summary:
        return
    signal = SkillWorkflowSignal(
        source=normalized_source,
        summary=normalized_summary,
        recorded_at=recorded_at,
    )
    skill.workflow_signals.append(signal)
    if len(skill.workflow_signals) > max_signals:
        skill.workflow_signals[:] = skill.workflow_signals[-max_signals:]


AUTO_SKILL_ACTORS = frozenset({"auto-skill", "web-auto-skill", "builder_reflector", "builder_auto"})


def _normalized_skill_text(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", value.lower()))


def _skill_duplicate_key(*, title: str, summary: str, trigger: str) -> str:
    normalized_summary = summary.strip().lower()
    if normalized_summary.startswith("captured from:"):
        captured = _normalized_skill_text(normalized_summary.removeprefix("captured from:"))
        if captured:
            return f"captured:{captured}"
    return f"title-trigger:{_normalized_skill_text(title)}::{_normalized_skill_text(trigger)}"


def _skill_record_duplicate_key(skill: SkillRecord) -> str:
    return _skill_duplicate_key(title=skill.title, summary=skill.summary, trigger=skill.trigger)


def _find_duplicate_skill(
    store: RuntimeStore,
    *,
    title: str,
    summary: str,
    trigger: str,
    exclude_skill_id: str | None = None,
) -> SkillRecord | None:
    key = _skill_duplicate_key(title=title, summary=summary, trigger=trigger)
    for skill in sorted(store.skills.values(), key=lambda s: ((s.created_at or datetime.min.replace(tzinfo=UTC)), s.skill_id)):
        if exclude_skill_id is not None and skill.skill_id == exclude_skill_id:
            continue
        if _skill_record_duplicate_key(skill) == key:
            return skill
    return None


def _merge_skill_contents(primary: SkillRecord, duplicate: SkillRecord) -> bool:
    changed = False
    for step in duplicate.steps:
        if step not in primary.steps:
            primary.steps.append(step)
            changed = True
    for tag in duplicate.tags:
        if tag not in primary.tags:
            primary.tags.append(tag)
            changed = True
    for signal in duplicate.workflow_signals:
        if signal not in primary.workflow_signals:
            primary.workflow_signals.append(signal)
            changed = True
    if duplicate.summary and duplicate.summary not in primary.summary:
        primary.summary = primary.summary or duplicate.summary
    latest = max(
        [dt for dt in (primary.updated_at, duplicate.updated_at, duplicate.created_at) if dt is not None],
        default=datetime.now(UTC),
    )
    if primary.updated_at != latest:
        primary.updated_at = latest
        changed = True
    if changed:
        _append_skill_workflow_signal(
            primary,
            source="duplicate_merge",
            summary=f"Merged duplicate skill {duplicate.skill_id}",
            recorded_at=datetime.now(UTC),
        )
    return changed


def deduplicate_skills(store: RuntimeStore, *, actor: str = "runtime") -> int:
    groups: dict[str, list[SkillRecord]] = {}
    for skill in store.skills.values():
        groups.setdefault(_skill_record_duplicate_key(skill), []).append(skill)
    removed = 0
    for records in groups.values():
        if len(records) < 2:
            continue
        records.sort(key=lambda s: ((s.created_at or datetime.min.replace(tzinfo=UTC)), s.skill_id))
        primary = records[0]
        for duplicate in records[1:]:
            _merge_skill_contents(primary, duplicate)
            store.skills.pop(duplicate.skill_id, None)
            removed += 1
    if removed:
        details = {"removed": str(removed)}
        store.add_event(make_event(event_type="skill.duplicates_merged", actor=actor, payload=details))
        store.add_audit_record(make_audit_record(action="skill.duplicates_merged", actor=actor, details=details))
    return removed


def create_skill(
    store: RuntimeStore,
    *,
    title: str,
    summary: str,
    trigger: str,
    steps: list[str],
    tags: list[str] | None = None,
    skill_id: str | None = None,
    actor: str = "runtime",
) -> SkillRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="create_skill")
    normalized_title = title.strip()
    normalized_summary = summary.strip()
    normalized_trigger = trigger.strip()
    normalized_steps = _normalize_skill_steps(steps)
    if not normalized_title:
        raise ValueError("title is required")
    if not normalized_summary:
        raise ValueError("summary is required")
    if not normalized_trigger:
        raise ValueError("trigger is required")
    if not normalized_steps:
        raise ValueError("at least one step is required")
    normalized_tags = _normalize_skill_tags(tags)
    if normalized_actor in AUTO_SKILL_ACTORS:
        duplicate = _find_duplicate_skill(
            store,
            title=normalized_title,
            summary=normalized_summary,
            trigger=normalized_trigger,
            exclude_skill_id=skill_id,
        )
        if duplicate is not None:
            _merge_skill_contents(
                duplicate,
                SkillRecord(
                    skill_id=skill_id or f"candidate-{uuid4().hex[:12]}",
                    title=normalized_title,
                    summary=normalized_summary,
                    trigger=normalized_trigger,
                    steps=normalized_steps,
                    tags=normalized_tags,
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                ),
            )
            payload = {"skill_id": duplicate.skill_id, "title": duplicate.title}
            store.add_event(make_event(event_type="skill.duplicate_merged", actor=normalized_actor, payload=payload))
            store.add_audit_record(make_audit_record(action="skill.duplicate_merged", actor=normalized_actor, details=payload))
            return duplicate
    now = datetime.now(UTC)
    skill = SkillRecord(
        skill_id=skill_id or f"skill-{uuid4().hex[:12]}",
        title=normalized_title,
        summary=normalized_summary,
        trigger=normalized_trigger,
        steps=normalized_steps,
        tags=normalized_tags,
        created_at=now,
        updated_at=now,
    )
    store.skills[skill.skill_id] = skill
    payload = {
        "skill_id": skill.skill_id,
        "title": skill.title,
    }
    store.add_event(make_event(event_type="skill.created", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="skill.created", actor=normalized_actor, details=payload))
    return skill



def accept_builder_skill_proposal(
    store: RuntimeStore,
    proposal: BuilderProposal,
    *,
    trigger: str | None = None,
    steps: list[str] | None = None,
    tags: list[str] | None = None,
    title: str | None = None,
    skill_id: str | None = None,
    actor: str = "runtime",
) -> SkillRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="accept_builder_skill_proposal")
    if proposal.decision_type is not BuilderDecisionType.SKILL_PROPOSAL:
        raise ValueError("proposal must be a skill proposal")
    resolved_title = title or proposal.suggested_skill_title or proposal.title
    resolved_trigger = trigger or proposal.suggested_trigger
    resolved_steps = steps or list(proposal.suggested_steps)
    resolved_tags = tags or list(proposal.suggested_tags)
    if not resolved_trigger or not resolved_steps:
        raise ValueError("proposal is missing draft skill details")
    skill = create_skill(
        store,
        title=resolved_title,
        summary=proposal.summary,
        trigger=resolved_trigger,
        steps=resolved_steps,
        tags=resolved_tags,
        skill_id=skill_id,
        actor=normalized_actor,
    )
    _append_skill_workflow_signal(
        skill,
        source="builder_accept",
        summary=f"Converged workflow from accepted proposal: {proposal.title}",
        recorded_at=skill.updated_at or datetime.now(UTC),
    )
    return skill



def store_builder_proposal(
    store: RuntimeStore,
    proposal: BuilderProposal,
    *,
    proposal_id: str | None = None,
    context_key: str | None = None,
    actor: str = "runtime",
) -> BuilderProposalRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="store_builder_proposal")
    now = datetime.now(UTC)
    if proposal_id is not None:
        existing_by_id = store.builder_proposals.get(proposal_id)
        if existing_by_id is not None:
            if existing_by_id.proposal == proposal:
                return existing_by_id
            raise ValueError(f"proposal_id already exists with different proposal: {proposal_id}")

    for existing in store.builder_proposals.values():
        if existing.status != "pending":
            continue
        if existing.proposal == proposal:
            return existing

    for existing in store.builder_proposals.values():
        if existing.status == "pending":
            continue
        if existing.proposal != proposal:
            continue
        resolved_at = existing.resolved_at or existing.created_at
        if now - resolved_at > BUILDER_PROPOSAL_COOLDOWN:
            continue
        if context_key and existing.context_key and context_key != existing.context_key:
            continue
        return existing

    record = BuilderProposalRecord(
        proposal_id=proposal_id or f"proposal-{uuid4().hex[:12]}",
        proposal=proposal,
        status="pending",
        created_at=now,
        context_key=context_key,
    )
    store.builder_proposals[record.proposal_id] = record
    payload = {
        "proposal_id": record.proposal_id,
        "decision_type": record.proposal.decision_type.value,
        "status": record.status,
    }
    store.add_event(make_event(event_type="builder.proposal_stored", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="builder.proposal_stored", actor=normalized_actor, details=payload))
    return record



def get_builder_proposal(store: RuntimeStore, proposal_id: str) -> BuilderProposalRecord | None:
    return store.builder_proposals.get(proposal_id)



def list_builder_proposals(store: RuntimeStore) -> list[BuilderProposalRecord]:
    return sorted(
        store.builder_proposals.values(),
        key=lambda record: record.created_at or datetime.fromtimestamp(0, tz=UTC),
    )


def list_pending_builder_proposals(store: RuntimeStore) -> list[BuilderProposalRecord]:
    return [record for record in list_builder_proposals(store) if record.status == "pending"]


def _require_pending_builder_proposal(record: BuilderProposalRecord, *, action: str) -> None:

    if record.status != "pending":
        raise ValueError(
            f"proposal {record.proposal_id} is {record.status}; only pending proposals can be {action}"
        )



def accept_stored_builder_skill_proposal(
    store: RuntimeStore,
    proposal_id: str,
    *,
    actor: str = "runtime",
) -> SkillRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="accept_stored_builder_skill_proposal")
    record = get_builder_proposal(store, proposal_id)
    if record is None:
        raise KeyError(proposal_id)
    _require_pending_builder_proposal(record, action="accepted")
    skill = accept_builder_skill_proposal(store, record.proposal, actor=normalized_actor)
    store.builder_proposals[proposal_id] = BuilderProposalRecord(
        proposal_id=record.proposal_id,
        proposal=record.proposal,
        status="accepted",
        created_at=record.created_at,
        accepted_skill_id=skill.skill_id,
        resolved_at=datetime.now(UTC),
        context_key=record.context_key,
    )
    payload = {
        "proposal_id": proposal_id,
        "decision_type": record.proposal.decision_type.value,
        "status": "accepted",
        "skill_id": skill.skill_id,
    }
    store.add_event(make_event(event_type="builder.proposal_accepted", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="builder.proposal_accepted", actor=normalized_actor, details=payload))
    return skill


def reject_stored_builder_proposal(
    store: RuntimeStore,
    proposal_id: str,
    *,
    actor: str = "runtime",
) -> BuilderProposalRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="reject_stored_builder_proposal")
    record = get_builder_proposal(store, proposal_id)
    if record is None:
        raise KeyError(proposal_id)
    _require_pending_builder_proposal(record, action="rejected")
    updated = replace(
        record,
        status="rejected",
        accepted_skill_id=None,
        resolved_at=datetime.now(UTC),
    )
    store.builder_proposals[proposal_id] = updated
    payload = {
        "proposal_id": proposal_id,
        "decision_type": record.proposal.decision_type.value,
        "status": updated.status,
    }
    store.add_event(make_event(event_type="builder.proposal_rejected", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="builder.proposal_rejected", actor=normalized_actor, details=payload))
    return updated


def archive_stored_builder_proposal(
    store: RuntimeStore,
    proposal_id: str,
    *,
    actor: str = "runtime",
) -> BuilderProposalRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="archive_stored_builder_proposal")
    record = get_builder_proposal(store, proposal_id)
    if record is None:
        raise KeyError(proposal_id)
    _require_pending_builder_proposal(record, action="archived")
    updated = replace(record, status="archived", resolved_at=datetime.now(UTC))
    store.builder_proposals[proposal_id] = updated
    payload = {
        "proposal_id": updated.proposal_id,
        "decision_type": updated.proposal.decision_type.value,
        "status": updated.status,
    }
    store.add_event(make_event(event_type="builder.proposal_archived", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="builder.proposal_archived", actor=normalized_actor, details=payload))
    return updated


def update_skill(
    store: RuntimeStore,
    skill_id: str,
    *,
    title: str | None = None,
    summary: str | None = None,
    trigger: str | None = None,
    steps: list[str] | None = None,
    tags: list[str] | None = None,
    actor: str = "runtime",
) -> SkillRecord:
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="update_skill")
    skill = store.skills.get(skill_id)
    if skill is None:
        raise KeyError(skill_id)

    next_title = skill.title if title is None else title.strip()
    next_summary = skill.summary if summary is None else summary.strip()
    next_trigger = skill.trigger if trigger is None else trigger.strip()
    next_steps = skill.steps if steps is None else _normalize_skill_steps(steps)
    next_tags = skill.tags if tags is None else _normalize_skill_tags(tags)

    if not next_title:
        raise ValueError("title is required")
    if not next_summary:
        raise ValueError("summary is required")
    if not next_trigger:
        raise ValueError("trigger is required")
    if not next_steps:
        raise ValueError("at least one step is required")

    if (
        next_title == skill.title
        and next_summary == skill.summary
        and next_trigger == skill.trigger
        and next_steps == skill.steps
        and next_tags == skill.tags
    ):
        return skill

    previous_revision = SkillRevision(
        revision=skill.revision,
        title=skill.title,
        summary=skill.summary,
        trigger=skill.trigger,
        steps=list(skill.steps),
        tags=list(skill.tags),
        updated_at=skill.updated_at,
    )
    now = datetime.now(UTC)
    skill.title = next_title
    skill.summary = next_summary
    skill.trigger = next_trigger
    skill.steps = list(next_steps)
    skill.tags = list(next_tags)
    skill.revision += 1
    skill.revision_history.append(previous_revision)
    skill.updated_at = now
    _append_skill_workflow_signal(
        skill,
        source="skill_update",
        summary=f"Converged workflow at revision {skill.revision} via skill update",
        recorded_at=now,
    )

    payload = {
        "skill_id": skill.skill_id,
        "revision": str(skill.revision),
    }
    store.add_event(make_event(event_type="skill.updated", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="skill.updated", actor=normalized_actor, details=payload))
    return skill


def promote_skill_replacement(
    store: RuntimeStore,
    skill_id: str,
    *,
    replacement_summary: str,
    replacement_trigger: str,
    replacement_steps: list[str],
    failed_workflow_summary: str,
    successful_workflow_summary: str,
    replacement_title: str | None = None,
    replacement_tags: list[str] | None = None,
    actor: str = "builder_auto",
) -> SkillRecord:
    """Make a successful replacement workflow the active skill revision.

    The previous workflow is preserved in revision_history, but it no longer
    remains in the active trigger/steps that future skill matching uses.
    """
    normalized_actor = _require_trusted_mutation_actor(actor=actor, action="promote_skill_replacement")
    failed_summary = failed_workflow_summary.strip()
    success_summary = successful_workflow_summary.strip()
    if not failed_summary:
        raise ValueError("failed_workflow_summary is required")
    if not success_summary:
        raise ValueError("successful_workflow_summary is required")

    if skill_id not in store.skills:
        raise KeyError(skill_id)

    updated = update_skill(
        store,
        skill_id,
        title=replacement_title,
        summary=replacement_summary,
        trigger=replacement_trigger,
        steps=replacement_steps,
        tags=replacement_tags,
        actor=normalized_actor,
    )
    now = updated.updated_at or datetime.now(UTC)
    _append_skill_workflow_signal(
        updated,
        source="skill_replacement",
        summary=f"Promoted replacement after failure: {success_summary}",
        recorded_at=now,
    )
    _append_skill_workflow_signal(
        updated,
        source="deprecated_workflow",
        summary=f"Deprecated previous workflow: {failed_summary}",
        recorded_at=now,
    )
    payload = {
        "skill_id": updated.skill_id,
        "revision": str(updated.revision),
    }
    store.add_event(make_event(event_type="skill.replacement_promoted", actor=normalized_actor, payload=payload))
    store.add_audit_record(make_audit_record(action="skill.replacement_promoted", actor=normalized_actor, details=payload))
    return updated


def get_skill(store: RuntimeStore, skill_id: str) -> SkillRecord | None:
    return store.skills.get(skill_id)


def list_skills(store: RuntimeStore) -> list[SkillRecord]:
    return sorted(store.skills.values(), key=lambda skill: (skill.title.lower(), skill.skill_id))



def _mission_step_status_for_skill_state(state: str) -> str:
    if state == "in_progress":
        return "running"
    if state == "completed":
        return "completed"
    return "pending"



def _build_mission_steps_for_skill_execution_plan(plan: SkillExecutionPlan) -> tuple[MissionStep, ...]:
    steps: list[MissionStep] = []
    for index, step in enumerate(plan.steps):
        state = plan.step_states[index] if index < len(plan.step_states) else "pending"
        steps.append(
            MissionStep(
                step_id=f"skill-step-{index + 1}",
                title=step,
                status=_mission_step_status_for_skill_state(state),
                kind="plan",
                notes=step,
            )
        )
    return tuple(steps)



def _sync_mission_from_skill_execution_plan(
    store: RuntimeStore,
    *,
    capsule_id: str,
    plan: SkillExecutionPlan,
) -> MissionRecord | None:
    if len(plan.steps) <= 1:
        return None

    capsule = store.get_capsule(capsule_id)
    if capsule is None:
        return None

    skill_mission = next(
        (
            mission
            for mission in store.list_missions()
            if mission.active_capsule_id == capsule_id
            and mission.continuation_policy is MissionContinuationPolicy.AUTO_FINISH
            and mission.steps
            and all(step.kind == "plan" for step in mission.steps)
        ),
        None,
    )

    if skill_mission is None:
        skill_mission = create_mission(
            store,
            owner=capsule.owner,
            title=capsule.goal,
            goal=capsule.goal,
            continuation_policy=MissionContinuationPolicy.AUTO_FINISH,
            created_from_capsule_id=capsule.capsule_id,
            active_capsule_id=capsule.capsule_id,
            actor="runtime",
        )

    existing_steps_by_id = {step.step_id: step for step in skill_mission.steps}
    merged_steps: list[MissionStep] = []
    for plan_step in _build_mission_steps_for_skill_execution_plan(plan):
        existing_step = existing_steps_by_id.get(plan_step.step_id)
        if existing_step is None:
            merged_steps.append(plan_step)
            continue

        merged_steps.append(
            replace(
                plan_step,
                capsule_id=existing_step.capsule_id if existing_step.capsule_id is not None else plan_step.capsule_id,
                mini_agent_run_id=existing_step.mini_agent_run_id,
                mini_agent_run_ids=existing_step.mini_agent_run_ids,
                required_mini_agent_run_ids=existing_step.required_mini_agent_run_ids,
                notes=existing_step.notes if existing_step.notes is not None else plan_step.notes,
            )
        )

    active_step_id = None
    if plan.active_step_index is not None and 0 <= plan.active_step_index < len(plan.steps):
        active_step_id = f"skill-step-{plan.active_step_index + 1}"

    refreshed = set_mission_plan(
        store,
        skill_mission.mission_id,
        steps=merged_steps,
        active_step_id=active_step_id,
        actor="runtime",
    )
    return advance_mission(store, refreshed.mission_id, actor="runtime")



def initialize_skill_execution_plan(store: RuntimeStore, *, capsule_id: str, skill_id: str) -> SkillExecutionPlan:
    skill = get_skill(store, skill_id)
    if skill is None:
        raise KeyError(skill_id)
    plan = build_skill_execution_plan(skill)
    store.set_skill_execution_plan(capsule_id, plan)
    _sync_mission_from_skill_execution_plan(store, capsule_id=capsule_id, plan=plan)
    return plan


def sync_mission_from_skill_execution_plan(store: RuntimeStore, *, capsule_id: str) -> MissionRecord | None:
    plan = store.get_skill_execution_plan(capsule_id)
    if plan is None:
        return None
    return _sync_mission_from_skill_execution_plan(store, capsule_id=capsule_id, plan=plan)


def get_skill_execution_plan(store: RuntimeStore, capsule_id: str) -> SkillExecutionPlan | None:
    return store.get_skill_execution_plan(capsule_id)




def _mini_agent_run_payload(run: MiniAgentRun) -> dict[str, str | None]:
    return {
        "run_id": run.run_id,
        "capsule_id": run.capsule_id,
        "mini_agent_type": run.mini_agent_type,
        "status": run.status.value,
        "result_summary": run.result_summary,
    }



def transition_skill_execution_plan_for_mini_agent_run(
    store: RuntimeStore,
    *,
    capsule_id: str,
    mini_agent_status: MiniAgentRunStatus,
) -> SkillExecutionPlan | None:
    plan = store.get_skill_execution_plan(capsule_id)
    if plan is None:
        return None
    updated = transition_skill_execution_plan_for_mini_agent_status(plan, mini_agent_status)
    store.set_skill_execution_plan(capsule_id, updated)
    _sync_mission_from_skill_execution_plan(store, capsule_id=capsule_id, plan=updated)
    return updated



def transition_skill_execution_plan_for_progress_update(
    store: RuntimeStore,
    *,
    capsule_id: str,
    progress_state: ProgressState,
) -> SkillExecutionPlan | None:
    plan = store.get_skill_execution_plan(capsule_id)
    if plan is None:
        return None
    updated = transition_skill_execution_plan_for_progress(plan, progress_state)
    store.set_skill_execution_plan(capsule_id, updated)
    _sync_mission_from_skill_execution_plan(store, capsule_id=capsule_id, plan=updated)
    return updated



def transition_skill_execution_plan_for_step_completion(
    store: RuntimeStore,
    *,
    capsule_id: str,
) -> SkillExecutionPlan | None:
    plan = store.get_skill_execution_plan(capsule_id)
    if plan is None:
        return None
    updated = transition_skill_execution_plan_for_step_completion_read_model(plan)
    store.set_skill_execution_plan(capsule_id, updated)
    _sync_mission_from_skill_execution_plan(store, capsule_id=capsule_id, plan=updated)
    return updated


def _skill_matching_tokens(value: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", value.lower()))


def _recommend_skills_with_scores(
    store: RuntimeStore,
    text: str,
    *,
    limit: int = 3,
) -> list[tuple[int, SkillRecord]]:
    if limit <= 0:
        return []
    normalized_text = text.strip().lower()
    if not normalized_text:
        return []
    query_tokens = _skill_matching_tokens(normalized_text)
    if not query_tokens:
        return []

    scored: list[tuple[int, SkillRecord]] = []
    for skill in list_skills(store):
        title_tokens = _skill_matching_tokens(skill.title)
        trigger_tokens = _skill_matching_tokens(skill.trigger)
        tag_tokens = {_tag.lower() for _tag in skill.tags}
        score = 0
        score += len(query_tokens & title_tokens) * 3
        score += len(query_tokens & trigger_tokens) * 2
        score += len(query_tokens & tag_tokens)
        if skill.title.lower() in normalized_text:
            score += 4
        if score <= 0:
            continue
        scored.append((score, skill))

    scored.sort(key=lambda item: (-item[0], item[1].title.lower(), item[1].skill_id))
    return scored[:limit]



def recommend_skills(store: RuntimeStore, text: str, *, limit: int = 3) -> list[SkillRecord]:
    return [skill for _, skill in _recommend_skills_with_scores(store, text, limit=limit)]


def render_skill_recommendations_for_telegram(skills: list[SkillRecord]) -> str:
    if not skills:
        return ""
    lines = ["💡 Relevant skills"]
    for index, skill in enumerate(skills, start=1):
        lines.append(f"{index}. {skill.title} (/skill {skill.skill_id})")
        lines.append(f"   Trigger: {skill.trigger}")
    return "\n".join(lines)


def build_skill_snapshot(skill: SkillRecord) -> dict[str, object]:
    return {
        "skill_id": skill.skill_id,
        "title": skill.title,
        "summary": skill.summary,
        "trigger": skill.trigger,
        "steps": list(skill.steps),
        "tags": list(skill.tags),
    }


def _dominant_workflow_sources(skill: SkillRecord) -> tuple[str, ...]:
    counts: dict[str, int] = {}
    for signal in skill.workflow_signals:
        source = signal.source.strip().lower()
        if not source:
            continue
        counts[source] = counts.get(source, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return tuple(source for source, _ in ranked[:3])


def propose_skill_refinement(skill: SkillRecord, *, min_signal_count: int = 2) -> SkillRefinementProposal | None:
    workflow_signal_count = len(skill.workflow_signals)
    if workflow_signal_count < min_signal_count:
        return None
    dominant_sources = _dominant_workflow_sources(skill)
    if not dominant_sources:
        return None
    confidence = round(min(0.9, 0.4 + (workflow_signal_count * 0.1)), 2)
    return SkillRefinementProposal(
        skill_id=skill.skill_id,
        skill_title=skill.title,
        current_revision=skill.revision,
        workflow_signal_count=workflow_signal_count,
        dominant_sources=dominant_sources,
        summary="Skill has repeated workflow updates; refine trigger and steps before next reuse.",
        confidence=confidence,
    )


def list_skill_refinement_proposals(store: RuntimeStore, *, min_signal_count: int = 2) -> list[SkillRefinementProposal]:
    proposals: list[SkillRefinementProposal] = []
    for skill in list_skills(store):
        proposal = propose_skill_refinement(skill, min_signal_count=min_signal_count)
        if proposal is None:
            continue
        proposals.append(proposal)
    return proposals


def build_skill_refinement_snapshot(proposal: SkillRefinementProposal) -> dict[str, object]:
    return build_skill_refinement_snapshot_read_model(proposal)


def render_skill_refinement_proposal_for_telegram(proposal: SkillRefinementProposal) -> str:
    return render_skill_refinement_for_telegram_read_model(proposal)


def render_skill_for_telegram(skill: SkillRecord) -> str:
    lines = [
        "🧠 Skill",
        skill.title,
        "",
        skill.summary,
        f"Trigger: {skill.trigger}",
    ]
    lines.extend(f"{index}. {step}" for index, step in enumerate(skill.steps, start=1))
    if skill.tags:
        lines.append(f"Tags: {', '.join(skill.tags)}")
    return "\n".join(lines)


def render_skill_execution_plan_for_telegram(plan: SkillExecutionPlan) -> str:
    return format_skill_execution_plan_for_telegram_read_model(build_skill_execution_plan_snapshot(plan))


def run_assistant_tool(
    store: RuntimeStore,
    *,
    invocation_id: str,
    tool_name: str,
    principal_id: str,
    arguments: dict[str, object],
    capsule_id: str | None = None,
    registry: ToolRegistry | None = None,
) -> str:
    result = invoke_tool(
        store,
        invocation_id=invocation_id,
        tool_name=tool_name,
        principal_id=principal_id,
        arguments=arguments,
        capsule_id=capsule_id,
        registry=registry,
    )
    return render_tool_result_for_telegram(result)


def create_core_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
) -> ToolRegistry:
    return create_core_tool_registry_from_tools(
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
    )



def create_default_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
) -> ToolRegistry:
    return create_default_tool_registry_from_tools(
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
    )



def create_plugin_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
) -> ToolRegistry:
    return create_plugin_tool_registry_from_tools(
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
    )



def create_extension_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
) -> ToolRegistry:
    return create_extension_tool_registry_from_tools(
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
    )


def execute_tool_invocation(
    store: RuntimeStore,
    invocation: ToolInvocation,
    *,
    registry: ToolRegistry | None = None,
    allowed_tool_names: Iterable[str] | None = None,
    denied_tool_names: Iterable[str] | None = None,
) -> ToolResult:
    policy_arguments = _boundary_policy_args_for_invocation(store, invocation)
    resolved_invocation = invocation
    if policy_arguments is not None:
        resolved_invocation = ToolInvocation(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            principal_id=invocation.principal_id,
            arguments={
                **invocation.arguments,
                **policy_arguments,
            },
            capsule_id=invocation.capsule_id,
            trusted_filesystem_selectors=invocation.trusted_filesystem_selectors,
            flow_context=invocation.flow_context,
        )

    executor = ToolExecutor(
        store=store,
        registry=registry or create_default_tool_registry_from_tools(),
        allowed_tool_names=allowed_tool_names,
        denied_tool_names=denied_tool_names,
    )
    return executor.invoke(resolved_invocation)


def invoke_tool(
    store: RuntimeStore,
    invocation: ToolInvocation | None = None,
    *,
    invocation_id: str | None = None,
    tool_name: str | None = None,
    principal_id: str | None = None,
    arguments: dict[str, object] | None = None,
    capsule_id: str | None = None,
    registry: ToolRegistry | None = None,
    allowed_tool_names: Iterable[str] | None = None,
    denied_tool_names: Iterable[str] | None = None,
) -> ToolResult:
    if invocation is None:
        if invocation_id is None or tool_name is None or principal_id is None or arguments is None:
            raise TypeError("invoke_tool requires either invocation or invocation_id, tool_name, principal_id, and arguments")
        invocation = ToolInvocation(
            invocation_id=invocation_id,
            tool_name=tool_name,
            principal_id=principal_id,
            arguments=dict(arguments),
            capsule_id=capsule_id,
        )
    return execute_tool_invocation(
        store,
        invocation,
        registry=registry,
        allowed_tool_names=allowed_tool_names,
        denied_tool_names=denied_tool_names,
    )


def _boundary_policy_args_for_invocation(store: RuntimeStore, invocation: ToolInvocation) -> dict[str, object] | None:
    if invocation.tool_name != "terminal_exec":
        return None

    principal_ids = {
        invocation.principal_id,
        permission_scope_principal(invocation.principal_id),
        GLOBAL_PERMISSION_PRINCIPAL,
        OPERATOR_PERMISSION_PRINCIPAL,
        "operator",
        "workspace:workspace_admin",
    }
    active_permit_selectors = []
    for permit in store.list_boundary_permits():
        if permit.principal_id not in principal_ids:
            continue
        if permit.boundary_kind is not BoundaryKind.OUTBOUND_NETWORK:
            continue
        if not is_boundary_permit_active(permit, now=datetime.now(UTC)):
            continue
        active_permit_selectors.append(permit.selector)

    boundary_rules = store.list_boundary_policy_rules()
    allow_selectors = tuple(
        dict.fromkeys(
            [
                *active_permit_selectors,
                *(
                    rule.selector
                    for rule in boundary_rules
                    if rule.principal_id in principal_ids
                    and rule.kind is BoundaryKind.OUTBOUND_NETWORK
                    and rule.mode is PolicyMode.ALLOW
                    and rule.revoked_at is None
                    and (rule.expires_at is None or rule.expires_at > datetime.now(UTC))
                ),
            ]
        )
    )
    if allow_selectors:
        return {
            "network_mode": "approved_only",
            "approved_targets": allow_selectors,
        }
    return {"network_mode": "none"}


def _matching_active_boundary_permits_for_invocation(store: RuntimeStore, invocation: ToolInvocation) -> list[BoundaryPermit]:
    principal_ids = {
        invocation.principal_id,
        permission_scope_principal(invocation.principal_id),
        GLOBAL_PERMISSION_PRINCIPAL,
        OPERATOR_PERMISSION_PRINCIPAL,
        "operator",
        "workspace:workspace_admin",
    }
    now = datetime.now(UTC)
    facts = extract_boundary_facts(invocation)
    if not facts:
        return []
    matched: dict[tuple[str, BoundaryKind, str], BoundaryPermit] = {}
    for permit in store.list_boundary_permits():
        if permit.principal_id not in principal_ids:
            continue
        if not is_boundary_permit_active(permit, now=now):
            continue
        permit_rule = BoundaryPolicyRule(
            rule_id=f"permit:{permit.permit_id}",
            principal_id=(
                permission_scope_principal(invocation.principal_id)
                if permit.boundary_kind is BoundaryKind.FILESYSTEM_ACCESS
                else GLOBAL_PERMISSION_PRINCIPAL
            ),
            kind=permit.boundary_kind,
            mode=PolicyMode.ALLOW,
            selector=permit.selector,
            created_by=permit.granted_by,
            created_at=permit.granted_at,
            expires_at=permit.expires_at,
            revoked_at=permit.revoked_at,
            reason="boundary_permit",
        )
        for fact in facts:
            request = BoundaryPolicyRequest(
                principal_id=(
                    permission_scope_principal(invocation.principal_id)
                    if permit.boundary_kind is BoundaryKind.FILESYSTEM_ACCESS
                    else GLOBAL_PERMISSION_PRINCIPAL
                ),
                boundary=fact,
            )
            if evaluate_boundary_request(request, rules=[permit_rule]) is PolicyDecision.ALLOW:
                key = (permit.principal_id, permit.boundary_kind, permit.selector)
                current = matched.get(key)
                if current is None or permit.granted_at >= current.granted_at:
                    matched[key] = permit
                break
    return list(matched.values())


def _record_wildcard_boundary_permit_accesses(
    store: RuntimeStore,
    invocation: ToolInvocation,
    permits: Iterable[BoundaryPermit],
) -> None:
    facts = extract_boundary_facts(invocation)
    if not facts:
        return
    seen: set[tuple[str, str, str]] = set()
    for permit in permits:
        if permit.selector != "*":
            continue
        for fact in facts:
            if fact.kind is not permit.boundary_kind:
                continue
            request = BoundaryPolicyRequest(
                principal_id=(
                    permission_scope_principal(invocation.principal_id)
                    if permit.boundary_kind is BoundaryKind.FILESYSTEM_ACCESS
                    else GLOBAL_PERMISSION_PRINCIPAL
                ),
                boundary=fact,
            )
            permit_rule = BoundaryPolicyRule(
                rule_id=f"permit:{permit.permit_id}",
                principal_id=request.principal_id,
                kind=permit.boundary_kind,
                mode=PolicyMode.ALLOW,
                selector=permit.selector,
                created_by=permit.granted_by,
                created_at=permit.granted_at,
                expires_at=permit.expires_at,
                revoked_at=permit.revoked_at,
                reason="boundary_permit",
            )
            if evaluate_boundary_request(request, rules=[permit_rule]) is not PolicyDecision.ALLOW:
                continue
            domain = (
                normalize_outbound_network_selector(fact.target)
                if fact.kind is BoundaryKind.OUTBOUND_NETWORK
                else fact.target
            )
            key = (permit.permit_id, domain, fact.target)
            if key in seen:
                continue
            seen.add(key)
            payload = {
                "permit_id": permit.permit_id,
                "approval_id": permit.approval_id,
                "principal_id": invocation.principal_id,
                "permit_principal_id": permit.principal_id,
                "boundary_kind": permit.boundary_kind.value,
                "selector": permit.selector,
                "domain": domain,
                "target": fact.target,
                "operation": fact.operation,
                "tool_name": invocation.tool_name,
                "invocation_id": invocation.invocation_id,
                "capsule_id": invocation.capsule_id,
                "accessed_at": datetime.now(UTC).isoformat(),
            }
            store.add_event(make_event("boundary_permit.wildcard_access", invocation.principal_id, payload))
            store.add_audit_record(
                make_audit_record("boundary_permit.wildcard_access", invocation.principal_id, payload)
            )


def invoke_tool_with_boundary_policy(
    store: RuntimeStore,
    invocation: ToolInvocation | None = None,
    *,
    invocation_id: str | None = None,
    tool_name: str | None = None,
    principal_id: str | None = None,
    arguments: dict[str, object] | None = None,
    capsule_id: str | None = None,
    registry: ToolRegistry | None = None,
    allowed_tool_names: Iterable[str] | None = None,
    denied_tool_names: Iterable[str] | None = None,
) -> ToolResult:
    if invocation is None:
        if invocation_id is None or tool_name is None or principal_id is None or arguments is None:
            raise TypeError(
                "invoke_tool_with_boundary_policy requires either invocation or invocation_id, tool_name, principal_id, and arguments"
            )
        invocation = ToolInvocation(
            invocation_id=invocation_id,
            tool_name=tool_name,
            principal_id=principal_id,
            arguments=dict(arguments),
            capsule_id=capsule_id,
        )

    active_permits = _matching_active_boundary_permits_for_invocation(store, invocation)
    policy_arguments = _boundary_policy_args_for_invocation(store, invocation)
    if policy_arguments is None or any(key in invocation.arguments for key in ("network_mode", "approved_targets")):
        result = invoke_tool(
            store,
            invocation,
            registry=registry,
            allowed_tool_names=allowed_tool_names,
            denied_tool_names=denied_tool_names,
        )
    else:
        policy_aware_invocation = ToolInvocation(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            principal_id=invocation.principal_id,
            arguments={**invocation.arguments, **policy_arguments},
            capsule_id=invocation.capsule_id,
            trusted_filesystem_selectors=invocation.trusted_filesystem_selectors,
            flow_context=invocation.flow_context,
        )
        result = invoke_tool(
            store,
            policy_aware_invocation,
            registry=registry,
            allowed_tool_names=allowed_tool_names,
            denied_tool_names=denied_tool_names,
        )

    if result.status != "denied" and invocation.tool_name == "terminal_exec":
        _record_wildcard_boundary_permit_accesses(store, invocation, active_permits)
        for permit in active_permits:
            if permit.uses_remaining <= 0:
                continue
            store.add_boundary_permit(consume_boundary_permit_record(permit))
    return result


def build_runtime_status_snapshot(
    store: RuntimeStore,
    *,
    capsule_id: str | None = None,
    checkpoint_path: str | Path | None = None,
) -> dict[str, object]:
    return build_runtime_status_snapshot_read_model(
        store,
        capsule_id=capsule_id,
        checkpoint_path=checkpoint_path,
    )


def handle_chat_operator_message(
    runtime: PersistentRuntime,
    text: str,
    *,
    chat_id: str | None = None,
    settings=None,
    request_id: str | None = None,
    message_id: str | None = None,
) -> str | None:
    from nullion.chat_operator import handle_chat_operator_message as handle_chat_operator_message_read_model

    return handle_chat_operator_message_read_model(
        runtime,
        text,
        chat_id=chat_id,
        settings=settings,
        request_id=request_id,
        message_id=message_id,
    )


__all__ = [
    "PersistentRuntime",
    "RuntimeResult",
    "ConversationMessageResult",
    "WorkerResultCommitResult",
    "acknowledge_sentinel_escalation",
    "acknowledge_sentinel_escalation_for_approval",
    "approve_approval_request",
    "accept_builder_skill_proposal",
    "accept_stored_builder_skill_proposal",
    "archive_stored_builder_proposal",
    "bootstrap_persistent_runtime",
    "bootstrap_runtime_store",
    "build_assistant_turn_snapshot",
    "build_builder_input_snapshot",
    "build_builder_proposal",
    "build_builder_proposal_snapshot",
    "build_codebase_summary",
    "build_progress_update_snapshot",
    "build_runtime_status_snapshot",
    "build_skill_refinement_snapshot",
    "build_skill_snapshot",
    "build_system_context_snapshot",
    "build_tool_result_snapshot",
    "cancel_doctor_action",
    "checkpoint_runtime_store",
    "commit_worker_result",
    "create_worker_result_envelope",
    "create_mini_agent_worker_result_envelope",
    "complete_doctor_action",
    "complete_mini_agent_run",
    "create_core_tool_registry",
    "create_default_tool_registry",
    "create_plugin_tool_registry",
    "create_extension_tool_registry",
    "create_mission",
    "create_skill",
    "deduplicate_skills",
    "deny_approval_request",
    "diagnose_runtime_health",
    "DoctorDiagnosisReport",
    "initialize_skill_execution_plan",
    "sync_mission_from_skill_execution_plan",
    "doctor_action_for_recommendation",
    "doctor_recommendation_for_route",
    "evaluate_builder_decision",
    "execute_tool_invocation",
    "fail_doctor_action",
    "fail_mini_agent_run",
    "format_builder_proposal_for_telegram",
    "format_codebase_summary",
    "format_doctor_diagnosis_for_operator",
    "format_progress_update_for_telegram",
    "format_runtime_status_for_telegram",
    "format_system_context_for_prompt",
    "get_builder_proposal",
    "get_latest_runtime_restore_metadata",
    "get_mission",
    "get_skill",
    "get_skill_execution_plan",
    "handle_chat_operator_message",
    "invoke_tool",
    "invoke_tool_with_boundary_policy",
    "list_active_permission_grants",
    "list_runtime_store_backups",
    "list_builder_proposals",
    "list_pending_builder_proposals",
    "list_missions",
    "list_skill_refinement_proposals",
    "list_skills",
    "recommend_skills",
    "mark_mission_blocked",
    "mark_mission_completed",
    "mark_mission_failed",
    "mark_mission_running",
    "mark_mission_step_running",
    "mark_mission_step_completed",
    "mark_mission_step_failed",
    "mark_mission_step_blocked",
    "link_mini_agent_run_to_mission_step",
    "attach_mini_agent_run_to_mission_step",
    "mark_mission_waiting_approval",
    "mark_mission_waiting_user",
    "set_mission_checklist_item_satisfied",
    "update_mission_step_notes",
    "update_active_task_frame_from_outcomes",
    "set_mission_plan",
    "advance_mission",
    "advance_mission_step",
    "mark_mini_agent_run_running",
    "record_policy_signal",
    "record_progress_update",
    "reconcile_effectively_approved_pending_approvals",
    "reconcile_stale_mini_agent_runs",
    "process_conversation_message",
    "promote_skill_replacement",
    "list_conversation_chat_turns",
    "plan_mission",
    "reject_stored_builder_proposal",
    "render_assistant_turn_for_telegram",
    "render_builder_proposal_for_telegram",
    "render_mission_for_telegram",
    "render_progress_update_for_telegram",
    "render_runtime_status",
    "render_runtime_status_for_telegram",
    "render_skill_for_telegram",
    "render_skill_execution_plan_for_telegram",
    "render_skill_refinement_proposal_for_telegram",
    "render_skill_recommendations_for_telegram",
    "render_tool_result_for_telegram",
    "report_health_issue",
    "revoke_permission_grant",
    "revoke_related_boundary_permission",
    "revoke_session_web_boundary_permits",
    "resolve_sentinel_escalation",
    "resolve_sentinel_escalation_for_approval",
    "restore_runtime_store_checkpoint",
    "run_assistant_tool",
    "run_due_scheduled_tasks",
    "run_request",
    "schedule_heartbeat",
    "start_doctor_action",
    "start_mini_agent_run",
    "store_builder_proposal",
    "transition_mission_status",
    "transition_mini_agent_run",
    "transition_skill_execution_plan_for_mini_agent_run",
    "transition_skill_execution_plan_for_progress_update",
    "transition_skill_execution_plan_for_step_completion",
    "update_skill",
]
