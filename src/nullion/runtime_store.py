"""In-memory runtime store for Project Nullion."""

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from nullion.approvals import ApprovalRequest, BoundaryPermit, PermissionGrant
from nullion.audit import AuditRecord
from nullion.builder import BuilderProposalRecord
from nullion.doctor_actions import DoctorAction
from nullion.events import Event
from nullion.intent import IntentCapsule
from nullion.mini_agent_runs import MiniAgentRun
from nullion.memory import UserMemoryEntry, UserMemoryKind
from nullion.missions import MissionRecord
from nullion.policy import BoundaryPolicyRule
from nullion.progress import ProgressUpdate
from nullion.reminders import ReminderRecord
from nullion.scheduler import ScheduledTask
from nullion.sentinel_escalations import SentinelEscalationArtifact
from nullion.signals import SignalRoute
from nullion.skill_planner import SkillExecutionPlan
from nullion.skills import SkillRecord
from nullion.task_frames import TaskFrame
from nullion.suspended_turns import SuspendedTurn

try:
    from nullion.conversation_runtime import ConversationBranch, ConversationTurn
except ImportError:  # pragma: no cover - removed when conversation_runtime module lands
    @dataclass(slots=True)
    class ConversationBranch:
        branch_id: str
        conversation_id: str
        status: Any
        created_from_turn_id: str
        superseded_by_branch_id: str | None = None
        cancelled_at: datetime | None = None

    @dataclass(slots=True)
    class ConversationTurn:
        turn_id: str
        conversation_id: str
        branch_id: str
        parent_turn_id: str | None
        disposition: Any
        user_message: str
        status: str
        created_at: datetime
        disposition_reason: str | None = None
        started_snapshot_summary: str | None = None
        cancellation_token: str | None = None


def _require_string_field(record: dict[str, Any], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str):
        raise TypeError(f"doctor action field '{key}' must be str")
    return value


def _require_optional_string_field(record: dict[str, Any], key: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"doctor action field '{key}' must be str or None")
    return value


def _normalize_doctor_action_record(action: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "action_id": _require_string_field(action, "action_id"),
        "owner": _require_string_field(action, "owner"),
        "status": _require_string_field(action, "status"),
        "action_type": _require_string_field(action, "action_type"),
        "recommendation_code": _require_string_field(action, "recommendation_code"),
        "summary": _require_string_field(action, "summary"),
        "severity": _require_string_field(action, "severity"),
        "reason": _require_optional_string_field(action, "reason"),
        "error": _require_optional_string_field(action, "error"),
    }
    if "source_reason" in action:
        normalized["source_reason"] = _require_optional_string_field(action, "source_reason")
    if "created_at" in action:
        normalized["created_at"] = _require_optional_string_field(action, "created_at")
    if "updated_at" in action:
        normalized["updated_at"] = _require_optional_string_field(action, "updated_at")
    return normalized



def _doctor_action_record_from_object(
    action: DoctorAction,
    *,
    action_type: str,
) -> dict[str, Any]:
    return _normalize_doctor_action_record(
        {
            "action_id": action.action_id,
            "owner": action.owner,
            "status": action.status,
            "action_type": action_type,
            "recommendation_code": action.recommendation_code,
            "summary": action.summary,
            "severity": action.severity,
            "source_reason": action.source_reason,
            "reason": action.reason,
            "error": action.error,
        }
    )



def _doctor_action_object_from_record(record: dict[str, Any]) -> tuple[DoctorAction, str]:
    normalized = _normalize_doctor_action_record(record)
    action = DoctorAction(
        action_id=normalized["action_id"],
        recommendation_code=normalized["recommendation_code"],
        status=normalized["status"],
        summary=normalized["summary"],
        severity=normalized["severity"],
        owner=normalized["owner"],
        source_reason=normalized.get("source_reason"),
        reason=normalized.get("reason"),
        error=normalized.get("error"),
    )
    return action, normalized["action_type"]


def _normalize_conversation_event_record(event: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise TypeError("conversation event must be a dict")
    conversation_id = event.get("conversation_id")
    if not isinstance(conversation_id, str):
        raise TypeError("conversation event field 'conversation_id' must be str")
    event_type = event.get("event_type")
    if not isinstance(event_type, str):
        raise TypeError("conversation event field 'event_type' must be str")
    normalized = deepcopy(event)
    normalized["conversation_id"] = conversation_id
    normalized["event_type"] = event_type
    return normalized


@dataclass(slots=True)
class RuntimeStore:
    capsules: dict[str, IntentCapsule] = field(default_factory=dict)
    events: list[Event] = field(default_factory=list)
    audit_records: list[AuditRecord] = field(default_factory=list)
    progress_updates: list[ProgressUpdate] = field(default_factory=list)
    scheduled_tasks: dict[str, ScheduledTask] = field(default_factory=dict)
    reminders: dict[str, ReminderRecord] = field(default_factory=dict)
    suspended_turns: dict[str, SuspendedTurn] = field(default_factory=dict)
    doctor_signals: list[SignalRoute] = field(default_factory=list)
    sentinel_signals: list[SignalRoute] = field(default_factory=list)
    sentinel_escalations: list[SentinelEscalationArtifact] = field(default_factory=list)
    approval_requests: dict[str, ApprovalRequest] = field(default_factory=dict)
    permission_grants: dict[str, PermissionGrant] = field(default_factory=dict)
    boundary_permits: dict[str, BoundaryPermit] = field(default_factory=dict)
    boundary_policy_rules: dict[str, BoundaryPolicyRule] = field(default_factory=dict)
    doctor_recommendations: list[dict[str, Any]] = field(default_factory=list)
    doctor_actions: list[dict[str, Any]] = field(default_factory=list)
    mini_agent_runs: dict[str, MiniAgentRun] = field(default_factory=dict)
    missions: dict[str, MissionRecord] = field(default_factory=dict)
    builder_proposals: dict[str, BuilderProposalRecord] = field(default_factory=dict)
    skills: dict[str, SkillRecord] = field(default_factory=dict)
    user_facts: dict[str, UserMemoryEntry] = field(default_factory=dict)
    preferences: dict[str, UserMemoryEntry] = field(default_factory=dict)
    environment_facts: dict[str, UserMemoryEntry] = field(default_factory=dict)
    skill_execution_plans: dict[str, SkillExecutionPlan] = field(default_factory=dict)
    cancelled_mission_ids: set[str] = field(default_factory=set)
    task_frames: dict[str, TaskFrame] = field(default_factory=dict)
    active_task_frames: dict[str, str] = field(default_factory=dict)
    conversation_turns: dict[str, ConversationTurn] = field(default_factory=dict)
    conversation_branches: dict[str, ConversationBranch] = field(default_factory=dict)
    conversation_heads: dict[str, dict[str, str | None]] = field(default_factory=dict)
    conversation_commits: dict[str, set[str]] = field(default_factory=dict)
    conversation_ingress_ids: dict[str, set[str]] = field(default_factory=dict)
    conversation_events: list[dict[str, Any]] = field(default_factory=list)

    def add_capsule(self, capsule: IntentCapsule) -> None:
        self.capsules[capsule.capsule_id] = capsule

    def get_capsule(self, capsule_id: str) -> IntentCapsule | None:
        return self.capsules.get(capsule_id)

    def add_event(self, event: Event) -> None:
        self.events.append(event)

    def list_events(self) -> list[Event]:
        return list(self.events)

    def add_audit_record(self, record: AuditRecord) -> None:
        self.audit_records.append(record)

    def list_audit_records(self) -> list[AuditRecord]:
        return list(self.audit_records)

    def add_progress_update(self, update: ProgressUpdate) -> None:
        self.progress_updates.append(update)

    def list_progress_updates(self) -> list[ProgressUpdate]:
        return list(self.progress_updates)

    def add_scheduled_task(self, task: ScheduledTask) -> None:
        self.scheduled_tasks[task.task_id] = task

    def get_scheduled_task(self, task_id: str) -> ScheduledTask | None:
        return self.scheduled_tasks.get(task_id)

    def list_scheduled_tasks(self) -> list[ScheduledTask]:
        return list(self.scheduled_tasks.values())

    def add_reminder(self, reminder: ReminderRecord) -> None:
        self.reminders[reminder.task_id] = reminder

    def get_reminder(self, task_id: str) -> ReminderRecord | None:
        return self.reminders.get(task_id)

    def list_reminders(self) -> list[ReminderRecord]:
        return list(self.reminders.values())

    def remove_reminder(self, task_id: str) -> bool:
        """Remove a reminder by task_id. Returns True if it existed."""
        return self.reminders.pop(task_id, None) is not None

    def add_suspended_turn(self, suspended_turn: SuspendedTurn) -> None:
        self.suspended_turns[suspended_turn.approval_id] = suspended_turn

    def get_suspended_turn(self, approval_id: str) -> SuspendedTurn | None:
        return self.suspended_turns.get(approval_id)

    def remove_suspended_turn(self, approval_id: str) -> None:
        self.suspended_turns.pop(approval_id, None)

    def list_suspended_turns(self) -> list[SuspendedTurn]:
        return list(self.suspended_turns.values())

    def add_doctor_signal(self, route: SignalRoute) -> None:
        self.doctor_signals.append(route)

    def list_doctor_signals(self) -> list[SignalRoute]:
        return list(self.doctor_signals)

    def add_sentinel_signal(self, route: SignalRoute) -> None:
        self.sentinel_signals.append(route)

    def list_sentinel_signals(self) -> list[SignalRoute]:
        return list(self.sentinel_signals)

    def add_sentinel_escalation(self, escalation: SentinelEscalationArtifact) -> None:
        if self.get_sentinel_escalation(escalation.escalation_id) is not None:
            raise ValueError(f"Duplicate escalation_id: {escalation.escalation_id}")
        approval_id = escalation.approval_id
        if approval_id is not None and self.get_sentinel_escalation_by_approval_id(approval_id) is not None:
            raise ValueError(f"Duplicate approval linkage for approval_id={approval_id}")
        self.sentinel_escalations.append(escalation)

    def get_sentinel_escalation(self, escalation_id: str) -> SentinelEscalationArtifact | None:
        for escalation in self.sentinel_escalations:
            if escalation.escalation_id == escalation_id:
                return escalation
        return None

    def get_sentinel_escalation_by_approval_id(
        self,
        approval_id: str,
    ) -> SentinelEscalationArtifact | None:
        matched = [
            escalation
            for escalation in self.sentinel_escalations
            if escalation.approval_id == approval_id
        ]
        if not matched:
            return None
        if len(matched) > 1:
            raise ValueError(f"Ambiguous approval linkage for approval_id={approval_id}")
        return matched[0]

    def update_sentinel_escalation(self, escalation: SentinelEscalationArtifact) -> None:
        escalation_id = escalation.escalation_id
        approval_id = escalation.approval_id
        if approval_id is not None:
            for existing in self.sentinel_escalations:
                if existing.escalation_id != escalation_id and existing.approval_id == approval_id:
                    raise ValueError(f"Duplicate approval linkage for approval_id={approval_id}")
        for index, existing in enumerate(self.sentinel_escalations):
            if existing.escalation_id == escalation_id:
                self.sentinel_escalations[index] = escalation
                return
        raise KeyError(escalation_id)

    def list_sentinel_escalations(self) -> list[SentinelEscalationArtifact]:
        return list(self.sentinel_escalations)

    def add_approval_request(self, approval: ApprovalRequest) -> None:
        self.approval_requests[approval.approval_id] = approval

    def get_approval_request(self, approval_id: str) -> ApprovalRequest | None:
        return self.approval_requests.get(approval_id)

    def list_approval_requests(self) -> list[ApprovalRequest]:
        return list(self.approval_requests.values())

    def add_permission_grant(self, grant: PermissionGrant) -> None:
        self.permission_grants[grant.grant_id] = grant

    def get_permission_grant(self, grant_id: str) -> PermissionGrant | None:
        return self.permission_grants.get(grant_id)

    def list_permission_grants(self) -> list[PermissionGrant]:
        return list(self.permission_grants.values())

    def add_boundary_permit(self, permit: BoundaryPermit) -> None:
        self.boundary_permits[permit.permit_id] = permit

    def get_boundary_permit(self, permit_id: str) -> BoundaryPermit | None:
        return self.boundary_permits.get(permit_id)

    def list_boundary_permits(self) -> list[BoundaryPermit]:
        return list(self.boundary_permits.values())

    def add_boundary_policy_rule(self, rule: BoundaryPolicyRule) -> None:
        self.boundary_policy_rules[rule.rule_id] = rule

    def get_boundary_policy_rule(self, rule_id: str) -> BoundaryPolicyRule | None:
        return self.boundary_policy_rules.get(rule_id)

    def list_boundary_policy_rules(self) -> list[BoundaryPolicyRule]:
        return list(self.boundary_policy_rules.values())

    def add_doctor_recommendation(self, recommendation: dict[str, Any]) -> None:
        self.doctor_recommendations.append(dict(recommendation))

    def list_doctor_recommendations(self) -> list[dict[str, Any]]:
        return [dict(recommendation) for recommendation in self.doctor_recommendations]

    def _user_memory_collection_for_kind(self, kind: UserMemoryKind) -> dict[str, UserMemoryEntry]:
        if kind is UserMemoryKind.FACT:
            return self.user_facts
        if kind is UserMemoryKind.PREFERENCE:
            return self.preferences
        if kind is UserMemoryKind.ENVIRONMENT_FACT:
            return self.environment_facts
        raise ValueError(f"Unsupported user memory kind: {kind}")

    def _drop_user_memory_entry(self, entry_id: str) -> None:
        self.user_facts.pop(entry_id, None)
        self.preferences.pop(entry_id, None)
        self.environment_facts.pop(entry_id, None)

    def remove_user_memory_entry(self, entry_id: str) -> bool:
        existing = self.get_user_memory_entry(entry_id)
        if existing is None:
            return False
        self._drop_user_memory_entry(entry_id)
        return True

    def add_user_memory_entry(self, entry: UserMemoryEntry) -> None:
        self._drop_user_memory_entry(entry.entry_id)
        self._user_memory_collection_for_kind(entry.kind)[entry.entry_id] = entry

    def update_user_memory_entry(self, entry: UserMemoryEntry) -> None:
        self.add_user_memory_entry(entry)

    def get_user_memory_entry(self, entry_id: str) -> UserMemoryEntry | None:
        entry = self.user_facts.get(entry_id)
        if entry is not None:
            return entry
        entry = self.preferences.get(entry_id)
        if entry is not None:
            return entry
        return self.environment_facts.get(entry_id)

    def list_user_memory_entries(self, *, kind: UserMemoryKind | None = None) -> list[UserMemoryEntry]:
        if kind is not None:
            return sorted(
                self._user_memory_collection_for_kind(kind).values(),
                key=lambda entry: (entry.key, entry.entry_id),
            )
        entries = [*self.user_facts.values(), *self.preferences.values(), *self.environment_facts.values()]
        kind_order = {
            UserMemoryKind.FACT: 0,
            UserMemoryKind.PREFERENCE: 1,
            UserMemoryKind.ENVIRONMENT_FACT: 2,
        }
        return sorted(entries, key=lambda entry: (kind_order[entry.kind], entry.key, entry.entry_id))

    def add_conversation_turn(self, turn: ConversationTurn) -> None:
        self.conversation_turns[turn.turn_id] = turn

    def get_conversation_turn(self, turn_id: str) -> ConversationTurn | None:
        return self.conversation_turns.get(turn_id)

    def list_conversation_turns(self, conversation_id: str) -> list[ConversationTurn]:
        return [
            turn
            for turn in self.conversation_turns.values()
            if turn.conversation_id == conversation_id
        ]

    def add_conversation_branch(self, branch: ConversationBranch) -> None:
        self.conversation_branches[branch.branch_id] = branch

    def get_conversation_branch(self, branch_id: str) -> ConversationBranch | None:
        return self.conversation_branches.get(branch_id)

    def list_conversation_branches(self, conversation_id: str) -> list[ConversationBranch]:
        return [
            branch
            for branch in self.conversation_branches.values()
            if branch.conversation_id == conversation_id
        ]

    def set_conversation_head(
        self,
        conversation_id: str,
        *,
        active_branch_id: str | None,
        active_turn_id: str | None,
    ) -> None:
        self.conversation_heads[conversation_id] = {
            "active_branch_id": active_branch_id,
            "active_turn_id": active_turn_id,
        }

    def get_conversation_head(self, conversation_id: str) -> dict[str, str | None] | None:
        head = self.conversation_heads.get(conversation_id)
        if head is None:
            return None
        return dict(head)

    def add_committed_idempotency_key(self, conversation_id: str, idempotency_key: str) -> None:
        keys = self.conversation_commits.setdefault(conversation_id, set())
        keys.add(idempotency_key)

    def has_committed_idempotency_key(self, conversation_id: str, idempotency_key: str) -> bool:
        return idempotency_key in self.conversation_commits.get(conversation_id, set())

    def list_committed_idempotency_keys(self, conversation_id: str) -> set[str]:
        return set(self.conversation_commits.get(conversation_id, set()))

    def add_conversation_ingress_id(self, conversation_id: str, ingress_id: str) -> None:
        ingress_ids = self.conversation_ingress_ids.setdefault(conversation_id, set())
        ingress_ids.add(ingress_id)

    def has_conversation_ingress_id(self, conversation_id: str, ingress_id: str) -> bool:
        return ingress_id in self.conversation_ingress_ids.get(conversation_id, set())

    def list_conversation_ingress_ids(self, conversation_id: str) -> set[str]:
        return set(self.conversation_ingress_ids.get(conversation_id, set()))

    def add_conversation_event(self, event: dict[str, Any]) -> None:
        self.conversation_events.append(_normalize_conversation_event_record(event))

    def list_conversation_events(self, conversation_id: str | None = None) -> list[dict[str, Any]]:
        if conversation_id is None:
            events = self.conversation_events
        else:
            events = [
                event
                for event in self.conversation_events
                if event.get("conversation_id") == conversation_id
            ]
        return [deepcopy(event) for event in events]

    def list_conversation_chat_turns(self, conversation_id: str) -> list[dict[str, str]]:
        chat_turns: list[dict[str, str]] = []
        for event in self.list_conversation_events(conversation_id):
            if event.get("event_type") == "conversation.session_reset":
                chat_turns = []
                continue
            if event.get("event_type") != "conversation.chat_turn":
                continue
            user_message = event.get("user_message")
            assistant_reply = event.get("assistant_reply")
            if not isinstance(user_message, str) or not isinstance(assistant_reply, str):
                continue
            chat_turns.append({"user": user_message, "assistant": assistant_reply})
        return chat_turns

    def add_doctor_action(self, action: dict[str, Any]) -> None:
        normalized = _normalize_doctor_action_record(action)
        action_id = normalized["action_id"]
        if self.get_doctor_action(action_id) is not None:
            raise ValueError(f"Duplicate doctor action_id: {action_id}")
        now = datetime.now(UTC).isoformat()
        normalized.setdefault("created_at", now)
        normalized.setdefault("updated_at", normalized["created_at"])
        self.doctor_actions.append(normalized)

    def get_doctor_action(self, action_id: str) -> dict[str, Any] | None:
        for action in self.doctor_actions:
            if action["action_id"] == action_id:
                return dict(action)
        return None

    def update_doctor_action(self, action: dict[str, Any]) -> None:
        normalized = _normalize_doctor_action_record(action)
        action_id = normalized["action_id"]
        for index, existing in enumerate(self.doctor_actions):
            if existing["action_id"] == action_id:
                normalized.setdefault("created_at", existing.get("created_at") or datetime.now(UTC).isoformat())
                normalized["updated_at"] = datetime.now(UTC).isoformat()
                self.doctor_actions[index] = normalized
                return
        raise KeyError(action_id)

    def list_doctor_actions(self) -> list[dict[str, Any]]:
        return [dict(action) for action in self.doctor_actions]

    def add_doctor_action_object(self, action: DoctorAction, *, action_type: str) -> None:
        self.add_doctor_action(_doctor_action_record_from_object(action, action_type=action_type))

    def get_doctor_action_object(self, action_id: str) -> tuple[DoctorAction, str] | None:
        record = self.get_doctor_action(action_id)
        if record is None:
            return None
        action, action_type = _doctor_action_object_from_record(record)
        return action, action_type

    def update_doctor_action_object(self, action: DoctorAction, *, action_type: str) -> None:
        self.update_doctor_action(_doctor_action_record_from_object(action, action_type=action_type))

    def list_doctor_action_objects(self) -> list[tuple[DoctorAction, str]]:
        return [_doctor_action_object_from_record(action) for action in self.doctor_actions]

    def add_mini_agent_run(self, run: MiniAgentRun) -> None:
        self.mini_agent_runs[run.run_id] = run

    def get_mini_agent_run(self, run_id: str) -> MiniAgentRun | None:
        return self.mini_agent_runs.get(run_id)

    def list_mini_agent_runs(self) -> list[MiniAgentRun]:
        return list(self.mini_agent_runs.values())

    def add_mission(self, mission: MissionRecord) -> None:
        self.missions[mission.mission_id] = mission

    def get_mission(self, mission_id: str) -> MissionRecord | None:
        return self.missions.get(mission_id)

    def list_missions(self) -> list[MissionRecord]:
        return list(self.missions.values())

    def add_builder_proposal(self, proposal: BuilderProposalRecord) -> None:
        self.builder_proposals[proposal.proposal_id] = proposal

    def get_builder_proposal(self, proposal_id: str) -> BuilderProposalRecord | None:
        return self.builder_proposals.get(proposal_id)

    def list_builder_proposals(self) -> list[BuilderProposalRecord]:
        return list(self.builder_proposals.values())

    def set_skill_execution_plan(self, capsule_id: str, plan: SkillExecutionPlan) -> None:
        self.skill_execution_plans[capsule_id] = plan

    def get_skill_execution_plan(self, capsule_id: str) -> SkillExecutionPlan | None:
        return self.skill_execution_plans.get(capsule_id)

    def list_skill_execution_plans(self) -> dict[str, SkillExecutionPlan]:
        return dict(self.skill_execution_plans)

    def cancel_mission(self, mission_id: str) -> None:
        """Mark a mission as cancelled so run_mission exits cleanly after the current step."""
        self.cancelled_mission_ids.add(mission_id)

    def is_mission_cancelled(self, mission_id: str) -> bool:
        return mission_id in self.cancelled_mission_ids

    def clear_mission_cancel(self, mission_id: str) -> None:
        self.cancelled_mission_ids.discard(mission_id)

    def add_task_frame(self, frame: TaskFrame) -> None:
        self.task_frames[frame.frame_id] = frame

    def get_task_frame(self, frame_id: str) -> TaskFrame | None:
        return self.task_frames.get(frame_id)

    def list_task_frames(self, conversation_id: str) -> list[TaskFrame]:
        return [
            frame
            for frame in self.task_frames.values()
            if frame.conversation_id == conversation_id
        ]

    def set_active_task_frame_id(self, conversation_id: str, frame_id: str | None) -> None:
        if frame_id is None:
            self.active_task_frames.pop(conversation_id, None)
            return
        self.active_task_frames[conversation_id] = frame_id

    def get_active_task_frame_id(self, conversation_id: str) -> str | None:
        return self.active_task_frames.get(conversation_id)
