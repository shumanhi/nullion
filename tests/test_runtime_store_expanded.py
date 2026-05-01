from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from nullion.approvals import ApprovalRequest, ApprovalStatus, BoundaryPermit, PermissionGrant
from nullion.audit import AuditRecord
from nullion.builder import BuilderDecisionType, BuilderProposal, BuilderProposalRecord
from nullion.doctor_actions import DoctorAction
from nullion.events import Event
from nullion.intent import IntentCapsule, IntentState
from nullion.memory import UserMemoryEntry, UserMemoryKind
from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus
from nullion.missions import MissionContinuationPolicy, MissionRecord, MissionStatus
from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode
from nullion.progress import ProgressState, ProgressUpdate
from nullion.reminders import ReminderRecord
from nullion.runtime_store import RuntimeStore
from nullion.scheduler import ScheduleKind, ScheduledTask
from nullion.sentinel_escalations import EscalationStatus, SentinelEscalationArtifact
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
)


NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_runtime_store_basic_collections_round_trip() -> None:
    store = RuntimeStore()
    capsule = IntentCapsule("cap", "owner", "goal", IntentState.RUNNING, "normal", [], None, [])
    event = Event(event_id="evt", event_type="type", actor="p", payload={"x": 1}, created_at=NOW)
    audit = AuditRecord(record_id="audit", actor="p", action="act", details={"res": "ok"}, created_at=NOW)
    progress = ProgressUpdate(ProgressState.WORKING, "Working", "cap")
    task = ScheduledTask("task", "cap", ScheduleKind.RECURRING, 5, True, None, 0)
    reminder = ReminderRecord("rem", "chat", "text", NOW)
    suspended = SimpleNamespace(approval_id="ap-s")

    store.add_capsule(capsule)
    store.add_event(event)
    store.add_audit_record(audit)
    store.add_progress_update(progress)
    store.add_scheduled_task(task)
    store.add_reminder(reminder)
    store.add_suspended_turn(suspended)

    assert store.get_capsule("cap") is capsule
    assert store.list_events() == [event]
    assert store.list_audit_records() == [audit]
    assert store.list_progress_updates() == [progress]
    assert store.get_scheduled_task("task") is task
    assert store.list_scheduled_tasks() == [task]
    assert store.get_reminder("rem") is reminder
    assert store.remove_reminder("rem") is True
    assert store.remove_reminder("rem") is False
    assert store.get_suspended_turn("ap-s") is suspended
    assert store.list_suspended_turns() == [suspended]
    store.remove_suspended_turn("ap-s")
    assert store.list_suspended_turns() == []


def test_runtime_store_sentinel_approval_grant_boundary_and_doctor_actions() -> None:
    store = RuntimeStore()
    escalation = SentinelEscalationArtifact("esc", "reason", "high", EscalationStatus.ESCALATED, NOW, "summary", "ap")
    store.add_sentinel_escalation(escalation)
    assert store.get_sentinel_escalation("esc") is escalation
    assert store.get_sentinel_escalation_by_approval_id("ap") is escalation
    with pytest.raises(ValueError):
        store.add_sentinel_escalation(escalation)
    with pytest.raises(ValueError):
        store.add_sentinel_escalation(SentinelEscalationArtifact("esc-2", "reason", "high", EscalationStatus.ESCALATED, NOW, "summary", "ap"))
    updated = SentinelEscalationArtifact("esc", "reason", "high", EscalationStatus.ACKNOWLEDGED, NOW, "summary", "ap")
    store.update_sentinel_escalation(updated)
    assert store.get_sentinel_escalation("esc").status is EscalationStatus.ACKNOWLEDGED
    with pytest.raises(KeyError):
        store.update_sentinel_escalation(SentinelEscalationArtifact("missing", "reason", "low", EscalationStatus.ESCALATED, NOW, "summary"))

    approval = ApprovalRequest("ap", "owner", "act", "res", ApprovalStatus.PENDING, NOW)
    grant = PermissionGrant("grant", "ap", "owner", "perm", "admin", NOW)
    permit = BoundaryPermit("permit", "ap", "owner", BoundaryKind.FILESYSTEM_ACCESS, "/tmp", "admin", NOW)
    rule = BoundaryPolicyRule("rule", "owner", BoundaryKind.FILESYSTEM_ACCESS, PolicyMode.ALLOW, "/tmp", "admin", NOW)
    store.add_approval_request(approval)
    store.add_permission_grant(grant)
    store.add_boundary_permit(permit)
    store.add_boundary_policy_rule(rule)
    assert store.get_approval_request("ap") is approval
    assert store.list_approval_requests() == [approval]
    assert store.get_permission_grant("grant") is grant
    assert store.get_boundary_permit("permit") is permit
    assert store.get_boundary_policy_rule("rule") is rule

    action = {
        "action_id": "doc",
        "owner": "owner",
        "status": "pending",
        "action_type": "repair",
        "recommendation_code": "fix",
        "summary": "Fix it",
        "severity": "high",
        "reason": None,
        "error": None,
    }
    store.add_doctor_action(action)
    assert store.get_doctor_action("doc")["summary"] == "Fix it"
    with pytest.raises(ValueError):
        store.add_doctor_action(action)
    updated_action = {**action, "status": "completed"}
    store.update_doctor_action(updated_action)
    assert store.get_doctor_action("doc")["status"] == "completed"
    with pytest.raises(KeyError):
        store.update_doctor_action({**action, "action_id": "missing"})
    with pytest.raises(TypeError):
        store.add_doctor_action({**action, "summary": 42})

    doctor_object = DoctorAction("obj", "rec", "pending", "Summary", "low", "owner")
    store.add_doctor_action_object(doctor_object, action_type="diagnose")
    loaded, action_type = store.get_doctor_action_object("obj")
    assert loaded.action_id == "obj"
    assert action_type == "diagnose"
    assert len(store.list_doctor_action_objects()) == 2


def test_runtime_store_user_memory_conversations_missions_and_frames() -> None:
    store = RuntimeStore()
    fact = UserMemoryEntry("m1", "owner", UserMemoryKind.FACT, "alpha", "A")
    preference = UserMemoryEntry("m2", "owner", UserMemoryKind.PREFERENCE, "beta", "B")
    env = UserMemoryEntry("m3", "owner", UserMemoryKind.ENVIRONMENT_FACT, "gamma", "C")
    for entry in (preference, env, fact):
        store.add_user_memory_entry(entry)
    assert store.list_user_memory_entries() == [fact, preference, env]
    assert store.list_user_memory_entries(kind=UserMemoryKind.PREFERENCE) == [preference]
    assert store.get_user_memory_entry("m3") is env
    moved = UserMemoryEntry("m1", "owner", UserMemoryKind.PREFERENCE, "alpha", "A")
    store.update_user_memory_entry(moved)
    assert store.get_user_memory_entry("m1").kind is UserMemoryKind.PREFERENCE
    assert store.remove_user_memory_entry("m1") is True
    assert store.remove_user_memory_entry("m1") is False

    turn = SimpleNamespace(turn_id="turn", conversation_id="conv", branch_id="branch")
    branch = SimpleNamespace(branch_id="branch", conversation_id="conv")
    store.add_conversation_turn(turn)
    store.add_conversation_branch(branch)
    store.set_conversation_head("conv", active_branch_id="branch", active_turn_id="turn")
    assert store.get_conversation_turn("turn") is turn
    assert store.list_conversation_turns("conv") == [turn]
    assert store.get_conversation_branch("branch") is branch
    assert store.list_conversation_branches("conv") == [branch]
    assert store.get_conversation_head("conv") == {"active_branch_id": "branch", "active_turn_id": "turn"}
    store.add_committed_idempotency_key("conv", "key")
    store.add_conversation_ingress_id("conv", "ingress")
    assert store.has_committed_idempotency_key("conv", "key") is True
    assert store.list_committed_idempotency_keys("conv") == {"key"}
    assert store.has_conversation_ingress_id("conv", "ingress") is True
    assert store.list_conversation_ingress_ids("conv") == {"ingress"}

    store.add_conversation_event({"conversation_id": "conv", "event_type": "conversation.chat_turn", "user_message": "u1", "assistant_reply": "a1"})
    store.add_conversation_event({"conversation_id": "conv", "event_type": "conversation.session_reset"})
    store.add_conversation_event({"conversation_id": "conv", "event_type": "conversation.chat_turn", "user_message": "u2", "assistant_reply": "a2"})
    assert store.list_conversation_chat_turns("conv") == [{"user": "u2", "assistant": "a2"}]
    with pytest.raises(TypeError):
        store.add_conversation_event({"conversation_id": "conv"})

    run = MiniAgentRun("run", "cap", "type", MiniAgentRunStatus.PENDING, NOW)
    mission = MissionRecord("mission", "owner", "Title", "Goal", MissionStatus.RUNNING, MissionContinuationPolicy.MANUAL)
    proposal = BuilderProposalRecord(
        "prop",
        BuilderProposal(BuilderDecisionType.SKILL_PROPOSAL, "title", "summary", 0.9, "manual"),
        "pending",
        NOW,
    )
    frame = TaskFrame("frame", "conv", "branch", "turn", None, TaskFrameStatus.ACTIVE, TaskFrameOperation.UNKNOWN, None, TaskFrameExecutionContract(), TaskFrameOutputContract(), TaskFrameFinishCriteria(), "summary", NOW, NOW)
    store.add_mini_agent_run(run)
    store.add_mission(mission)
    store.add_builder_proposal(proposal)
    store.add_task_frame(frame)
    store.set_active_task_frame_id("conv", "frame")
    store.cancel_mission("mission")
    assert store.get_mini_agent_run("run") is run
    assert store.get_mission("mission") is mission
    assert store.get_builder_proposal("prop") is proposal
    assert store.get_task_frame("frame") is frame
    assert store.list_task_frames("conv") == [frame]
    assert store.get_active_task_frame_id("conv") == "frame"
    assert store.is_mission_cancelled("mission") is True
    store.clear_mission_cancel("mission")
    assert store.is_mission_cancelled("mission") is False
    store.set_active_task_frame_id("conv", None)
    assert store.get_active_task_frame_id("conv") is None
