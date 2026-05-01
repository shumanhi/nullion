from __future__ import annotations

from datetime import UTC, datetime, timedelta

from nullion.approvals import approve, create_approval_request, create_boundary_permit, create_permission_grant
from nullion.audit import make_audit_record
from nullion.builder import BuilderDecisionType, BuilderProposal, BuilderProposalRecord
from nullion.conversation_runtime import ConversationBranchStatus, ConversationTurnDisposition
from nullion.events import make_event
from nullion.intent import create_intent_capsule
from nullion.memory import UserMemoryEntry, UserMemoryKind
from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus
from nullion.missions import MissionChecklistItem, MissionContinuationPolicy, MissionRecord, MissionStatus, MissionStep
from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode
from nullion.progress import ProgressState, ProgressUpdate
from nullion.reminders import ReminderRecord
from nullion.runtime import bootstrap_runtime_store
from nullion.runtime_persistence import (
    get_latest_runtime_restore_metadata,
    list_runtime_store_backups,
    load_runtime_store,
    migrate_runtime_store_payload,
    restore_runtime_store_backup,
    save_runtime_store,
)
from nullion.runtime_store import ConversationBranch, ConversationTurn, RuntimeStore
from nullion.scheduler import ScheduleKind, ScheduledTask
from nullion.sentinel_escalations import EscalationStatus, SentinelEscalationArtifact
from nullion.signals import SignalRoute, SignalTarget
from nullion.skill_planner import SkillExecutionPlan
from nullion.skills import SkillRecord, SkillRevision, SkillWorkflowSignal
from nullion.suspended_turns import SuspendedTurn
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
)


def test_runtime_store_round_trips_approvals_permissions_boundaries_events_and_audit(tmp_path) -> None:
    store = RuntimeStore()
    approval = create_approval_request(
        requested_by="telegram:123",
        action="allow_boundary",
        resource="https://example.com/*",
        request_kind="boundary_policy",
        context={"boundary_kind": "outbound_network", "target": "https://example.com/*"},
    )
    grant = create_permission_grant(
        approval_id=approval.approval_id,
        principal_id="global:operator",
        permission="tool:web_fetch",
        granted_by="operator",
    )
    permit = create_boundary_permit(
        approval_id=approval.approval_id,
        principal_id="global:operator",
        boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector="example.com",
        granted_by="operator",
        uses_remaining=5,
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    rule = BoundaryPolicyRule(
        rule_id="rule-1",
        principal_id="global:operator",
        kind=BoundaryKind.OUTBOUND_NETWORK,
        mode=PolicyMode.ALLOW,
        selector="example.com",
        created_by="operator",
        created_at=datetime.now(UTC),
        reason="test",
    )
    event = make_event("boundary_permit.wildcard_access", {"domain": "example.com", "permit_id": permit.permit_id})
    audit = make_audit_record("approval.created", "operator", {"approval_id": approval.approval_id})
    store.add_approval_request(approval)
    store.add_permission_grant(grant)
    store.add_boundary_permit(permit)
    store.add_boundary_policy_rule(rule)
    store.add_event(event)
    store.add_audit_record(audit)

    checkpoint = tmp_path / "runtime-store.json"
    save_runtime_store(store, checkpoint)
    loaded = load_runtime_store(checkpoint)

    assert loaded.get_approval_request(approval.approval_id).resource == "https://example.com/*"
    assert loaded.get_permission_grant(grant.grant_id).permission == "tool:web_fetch"
    assert loaded.get_boundary_permit(permit.permit_id).selector == "example.com"
    assert loaded.get_boundary_policy_rule("rule-1").mode is PolicyMode.ALLOW
    assert loaded.list_events()[0].event_type == "boundary_permit.wildcard_access"
    assert loaded.list_audit_records()[0].action == "approval.created"


def test_runtime_store_save_creates_backup_on_overwrite(tmp_path) -> None:
    checkpoint = tmp_path / "runtime-store.json"
    first = RuntimeStore()
    first.add_audit_record(make_audit_record("first", "operator"))
    second = RuntimeStore()
    second.add_audit_record(make_audit_record("second", "operator"))

    save_runtime_store(first, checkpoint)
    save_runtime_store(second, checkpoint)

    assert checkpoint.exists()
    assert checkpoint.with_name("runtime-store.json.bak").exists()
    loaded_actions = [record.action for record in load_runtime_store(checkpoint).list_audit_records()]
    assert "second" in loaded_actions


def test_sqlite_save_merges_approval_rows_from_other_runtime_process(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    first = RuntimeStore()
    second = RuntimeStore()
    save_runtime_store(first, checkpoint)
    save_runtime_store(second, checkpoint)

    approval = create_approval_request(
        requested_by="telegram_chat",
        action="allow_boundary",
        resource="https://www.bing.com/*",
        request_kind="boundary_policy",
        context={"workspace_id": "workspace_admin"},
    )
    first.add_approval_request(approval)
    save_runtime_store(first, checkpoint)

    second.add_audit_record(make_audit_record("web.status.refresh", "web"))
    save_runtime_store(second, checkpoint)

    loaded = load_runtime_store(checkpoint)
    assert loaded.get_approval_request(approval.approval_id) is not None
    assert any(record.action == "web.status.refresh" for record in loaded.list_audit_records())


def test_existing_sqlite_bootstrap_imports_legacy_json_boundaries(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    legacy_json = tmp_path / "runtime-store.json"
    save_runtime_store(RuntimeStore(), checkpoint)

    legacy = RuntimeStore()
    approval = approve(
        create_approval_request(
            requested_by="telegram:123",
            action="allow_boundary",
            resource="*",
            request_kind="boundary_policy",
            context={"boundary_kind": "outbound_network", "target": "*"},
        ),
        decided_by="operator",
    )
    permit = create_boundary_permit(
        approval_id=approval.approval_id,
        principal_id="global:operator",
        boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector="*",
        granted_by="operator",
        uses_remaining=80,
    )
    legacy.add_approval_request(approval)
    legacy.add_boundary_permit(permit)
    legacy.add_event(make_event("boundary_permit.wildcard_access", "runtime", {"permit_id": permit.permit_id}))
    save_runtime_store(legacy, legacy_json)

    imported = bootstrap_runtime_store(checkpoint)

    assert imported.get_approval_request(approval.approval_id).status.name == "APPROVED"
    assert imported.get_boundary_permit(permit.permit_id).selector == "*"
    assert any(event.event_type == "runtime.legacy_records_imported" for event in imported.list_events())
    reloaded = load_runtime_store(checkpoint)
    assert reloaded.get_boundary_permit(permit.permit_id).uses_remaining == 80


def test_sqlite_merge_does_not_restore_suspended_turn_for_decided_approval(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    approval = create_approval_request(
        requested_by="telegram_chat",
        action="allow_boundary",
        resource="https://www.bing.com/*",
        request_kind="boundary_policy",
    )
    pending = RuntimeStore()
    pending.add_approval_request(approval)
    pending.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="telegram:514132807",
            chat_id="514132807",
            message="/chat weather",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
        )
    )
    save_runtime_store(pending, checkpoint)

    decided = RuntimeStore()
    decided.add_approval_request(approve(approval, decided_by="operator"))
    save_runtime_store(decided, checkpoint)

    loaded = load_runtime_store(checkpoint)
    assert loaded.get_approval_request(approval.approval_id).decided_by == "operator"
    assert loaded.get_suspended_turn(approval.approval_id) is None


def test_runtime_store_round_trips_full_workflow_surface_json_and_sqlite(tmp_path) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store = RuntimeStore()
    capsule = create_intent_capsule(owner="operator", goal="ship", risk_level="normal", success_criteria=["done"])
    store.add_capsule(capsule)
    store.add_progress_update(ProgressUpdate(ProgressState.WORKING, "working", capsule.capsule_id))
    store.add_scheduled_task(ScheduledTask("sched", capsule.capsule_id, ScheduleKind.RECURRING, 10, True, now, 1))
    store.add_reminder(ReminderRecord("rem", "telegram:1", "standup", now, delivered_at=now))
    store.add_suspended_turn(
        SuspendedTurn(
            approval_id="ap",
            conversation_id="conv",
            chat_id="telegram:1",
            message="pending",
            request_id="req",
            message_id="msg",
            created_at=now,
            mission_id="mission",
            pending_step_idx=1,
            messages_snapshot=[{"role": "user"}],
            pending_tool_calls=[{"id": "tool"}],
            task_id="task-1",
            group_id="group-1",
            agent_id="agent-1",
            resume_token={"backend": "deepagents", "thread_id": "thread-1"},
        )
    )
    store.add_doctor_signal(
        SignalRoute(
            SignalTarget.DOCTOR,
            "doctor",
            "high",
            summary="Probe failed",
            error="connection refused",
            recommendation_code="restart_service",
        )
    )
    store.add_sentinel_signal(SignalRoute(SignalTarget.SENTINEL, "sentinel", "critical", summary="Boundary blocked"))
    store.add_sentinel_escalation(
        SentinelEscalationArtifact("esc", "unsafe", "critical", EscalationStatus.ESCALATED, now, "Escalated", approval_id="ap")
    )
    store.add_doctor_recommendation({"id": "rec", "summary": "recommend"})
    store.add_doctor_action(
        {
            "action_id": "act",
            "owner": "operator",
            "status": "pending",
            "action_type": "repair",
            "recommendation_code": "fix",
            "summary": "Fix",
            "severity": "high",
            "reason": None,
            "error": None,
        }
    )
    store.add_mini_agent_run(MiniAgentRun("run", capsule.capsule_id, "researcher", MiniAgentRunStatus.RUNNING, now, "working"))
    store.add_mission(
        MissionRecord(
            "mission",
            "operator",
            "Mission",
            "Goal",
            MissionStatus.RUNNING,
            MissionContinuationPolicy.AUTO_FINISH,
            created_from_capsule_id=capsule.capsule_id,
            active_capsule_id=capsule.capsule_id,
            active_step_id="step",
            steps=(MissionStep("step", "Step", "running", "work", capsule_id=capsule.capsule_id),),
            completion_checklist=(MissionChecklistItem("check", "Check", required=True, satisfied=False),),
            created_at=now,
            updated_at=now,
        )
    )
    store.builder_proposals["prop"] = BuilderProposalRecord(
        "prop",
        BuilderProposal(BuilderDecisionType.SKILL_PROPOSAL, "Skill", "Summary", 0.8, "manual", "Skill", "when asked", ("do",), ("tag",)),
        "pending",
        now,
        context_key="ctx",
    )
    store.skills["skill"] = SkillRecord(
        "skill",
        "Skill",
        "Summary",
        "trigger",
        ["step"],
        ["tag"],
        now,
        now,
        revision=2,
        revision_history=[SkillRevision(1, "Old", "Old summary", "old", ["old"], ["tag"], now)],
        workflow_signals=[SkillWorkflowSignal("source", "summary", now)],
    )
    for kind in (UserMemoryKind.FACT, UserMemoryKind.PREFERENCE, UserMemoryKind.ENVIRONMENT_FACT):
        store.add_user_memory_entry(UserMemoryEntry(f"entry-{kind.value}", "owner", kind, kind.value, "value", "test", now, now))
    store.set_skill_execution_plan("cap", SkillExecutionPlan("plan", "skill", "Skill", ("one",), step_states=("in_progress",), active_step_index=0))
    frame = TaskFrame(
        "frame",
        "conv",
        "branch",
        "turn",
        None,
        TaskFrameStatus.ACTIVE,
        TaskFrameOperation.GENERATE_ARTIFACT,
        TaskFrameTarget("file", "report", "report"),
        TaskFrameExecutionContract("workspace", "fallback", True, "filesystem"),
        TaskFrameOutputContract("txt", "attachment", "text"),
        TaskFrameFinishCriteria(True, True, "txt", ("file_write",)),
        "summary",
        now,
        now,
        metadata={"k": "v"},
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("conv", "frame")
    store.add_conversation_turn(ConversationTurn("turn", "conv", "branch", None, ConversationTurnDisposition.INDEPENDENT, "hi", "open", now))
    store.add_conversation_branch(ConversationBranch("branch", "conv", ConversationBranchStatus.ACTIVE, "turn"))
    store.set_conversation_head("conv", active_branch_id="branch", active_turn_id="turn")
    store.add_committed_idempotency_key("conv", "idem")
    store.add_conversation_ingress_id("conv", "ingress")
    store.add_conversation_event({"conversation_id": "conv", "event_type": "conversation.chat_turn", "created_at": now.isoformat()})

    json_path = tmp_path / "runtime.json"
    save_runtime_store(store, json_path)
    loaded = load_runtime_store(json_path)

    assert loaded.get_capsule(capsule.capsule_id).goal == "ship"
    assert loaded.get_reminder("rem").delivered_at == now
    assert loaded.get_suspended_turn("ap").pending_step_idx == 1
    assert loaded.get_suspended_turn("ap").resume_token["backend"] == "deepagents"
    assert loaded.get_suspended_turn("ap").task_id == "task-1"
    assert loaded.list_missions()[0].steps[0].step_id == "step"
    assert loaded.get_task_frame("frame").metadata == {"k": "v"}
    assert loaded.get_conversation_head("conv")["active_turn_id"] == "turn"
    assert loaded.list_conversation_events("conv")[0]["event_type"] == "conversation.chat_turn"
    loaded_doctor_signal = loaded.list_doctor_signals()[0]
    assert loaded_doctor_signal.summary == "Probe failed"
    assert loaded_doctor_signal.error == "connection refused"
    assert loaded_doctor_signal.recommendation_code == "restart_service"
    assert loaded.list_sentinel_signals()[0].summary == "Boundary blocked"

    sqlite_path = tmp_path / "runtime.db"
    save_runtime_store(store, sqlite_path)
    sqlite_loaded = load_runtime_store(sqlite_path)
    assert sqlite_loaded.get_task_frame("frame").summary == "summary"
    assert sqlite_loaded.get_suspended_turn("ap").group_id == "group-1"
    assert sqlite_loaded.list_skill_execution_plans()["cap"].plan_id == "plan"
    sqlite_doctor_signal = sqlite_loaded.list_doctor_signals()[0]
    assert sqlite_doctor_signal.summary == "Probe failed"
    assert sqlite_doctor_signal.error == "connection refused"
    assert sqlite_doctor_signal.recommendation_code == "restart_service"
    assert sqlite_loaded.list_sentinel_signals()[0].summary == "Boundary blocked"

    save_runtime_store(RuntimeStore(), json_path)
    assert list_runtime_store_backups(json_path)[0]["generation"] == 0
    restore_runtime_store_backup(json_path)
    assert load_runtime_store(json_path).get_capsule(capsule.capsule_id).goal == "ship"


def test_runtime_store_migration_and_restore_metadata() -> None:
    payload = migrate_runtime_store_payload({"builder_proposals": [{"proposal_id": "p", "resolved_at": "2026-01-01T00:00:00+00:00"}]})
    assert payload["format_version"] == 1
    assert payload["builder_proposals"][0]["created_at"] == "2026-01-01T00:00:00+00:00"
    assert payload["capsules"] == []

    store = RuntimeStore()
    store.add_event(make_event("runtime.store_restored", "operator", {"checkpoint_path": "runtime.json", "source": "backup", "generation": 0}))
    assert get_latest_runtime_restore_metadata(store) == {
        "checkpoint_path": "runtime.json",
        "source": "backup",
        "generation": 0,
    }
    assert get_latest_runtime_restore_metadata(RuntimeStore()) is None
