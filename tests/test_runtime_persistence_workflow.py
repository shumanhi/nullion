from __future__ import annotations

import logging
import sqlite3
import json
from contextlib import contextmanager
from dataclasses import replace
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
from nullion.runtime import bootstrap_persistent_runtime, bootstrap_runtime_store
import nullion.runtime_persistence as runtime_persistence
from nullion.runtime_persistence import (
    get_latest_runtime_restore_metadata,
    list_runtime_store_backups,
    load_runtime_store,
    migrate_sqlite_runtime_memory_payloads,
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


def test_sqlite_save_and_restore_use_runtime_file_lock(monkeypatch, tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    lock_targets = []
    original_lock = runtime_persistence._runtime_store_file_lock

    @contextmanager
    def fake_lock(target):
        if target == checkpoint:
            lock_targets.append(target)
            yield
        else:
            # Other runtime instances may checkpoint concurrently. Keep their
            # actual locks and observe only this test's save/restore operations.
            with original_lock(target):
                yield

    monkeypatch.setattr(runtime_persistence, "_runtime_store_file_lock", fake_lock)

    first = RuntimeStore()
    first.add_audit_record(make_audit_record("first", "operator"))
    second = RuntimeStore()
    second.add_audit_record(make_audit_record("second", "operator"))

    save_runtime_store(first, checkpoint)
    save_runtime_store(second, checkpoint)
    restore_runtime_store_backup(checkpoint)

    assert lock_targets == [checkpoint, checkpoint, checkpoint]


def test_runtime_backup_listing_includes_recovered_sqlite_snapshots(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    first = RuntimeStore()
    first.add_audit_record(make_audit_record("first", "operator"))
    second = RuntimeStore()
    second.add_audit_record(make_audit_record("second", "operator"))

    save_runtime_store(first, checkpoint)
    save_runtime_store(second, checkpoint)
    recovered = checkpoint.with_name("runtime.db.corrupt-123")
    recovered.write_bytes(checkpoint.read_bytes())

    backups = list_runtime_store_backups(checkpoint)

    assert backups[0]["restore_id"] == "0"
    recovered_record = next(backup for backup in backups if backup["restore_id"] == "corrupt-123")
    assert recovered_record["kind"] == "recovered"
    assert recovered_record["size_bytes"] > 0
    assert recovered_record["integrity"] == "ok"
    assert recovered_record["restorable"] is True

    restore_runtime_store_backup(checkpoint, generation="corrupt-123")
    restored_actions = [record.action for record in load_runtime_store(checkpoint).list_audit_records()]
    assert "second" in restored_actions


def test_sqlite_save_moves_corrupt_checkpoint_aside(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    checkpoint.write_text("not a sqlite database", encoding="utf-8")
    store = RuntimeStore()
    store.add_audit_record(make_audit_record("recovered", "operator"))

    save_runtime_store(store, checkpoint)

    recovered = list(tmp_path.glob("runtime.db.corrupt-*"))
    assert len(recovered) == 1
    assert recovered[0].read_text(encoding="utf-8") == "not a sqlite database"
    loaded_actions = [record.action for record in load_runtime_store(checkpoint).list_audit_records()]
    assert loaded_actions == ["recovered"]


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


def test_sqlite_save_merges_builder_proposals_and_skills_from_other_runtime_process(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    first = RuntimeStore()
    second = RuntimeStore()
    save_runtime_store(first, checkpoint)
    save_runtime_store(second, checkpoint)

    now = datetime(2026, 6, 9, tzinfo=UTC)
    proposal = BuilderProposalRecord(
        "proposal-drive",
        BuilderProposal(
            BuilderDecisionType.SKILL_PROPOSAL,
            "Build Google Drive connector skill",
            "Create a reusable Builder workflow for Google Drive.",
            0.86,
            "skill",
            "Google Drive connector workflow",
            "Use when Google Drive connector access is missing.",
            ("Check connector metadata.",),
            ("builder", "connector"),
        ),
        "pending",
        now,
        context_key="connector-skill:google-drive",
    )
    first.builder_proposals[proposal.proposal_id] = proposal
    first.skills["skill-drive"] = SkillRecord(
        "skill-drive",
        "Google Drive connector workflow",
        "Use the connector route for Google Drive requests.",
        "drive request",
        ["Check connector metadata."],
        ["builder", "connector"],
        now,
        now,
    )
    save_runtime_store(first, checkpoint)

    second.add_audit_record(make_audit_record("web.status.refresh", "web"))
    save_runtime_store(second, checkpoint)

    loaded = load_runtime_store(checkpoint)
    loaded_proposal = loaded.get_builder_proposal("proposal-drive")
    assert loaded_proposal is not None
    assert loaded_proposal.context_key == "connector-skill:google-drive"
    assert loaded.skills.get("skill-drive") is not None
    assert any(record.action == "web.status.refresh" for record in loaded.list_audit_records())


def test_sqlite_save_does_not_rewrite_unchanged_rows_from_other_runtime_process(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    first = RuntimeStore()
    first_record = make_audit_record("telegram.turn_recorded", "telegram")
    first.add_audit_record(first_record)
    save_runtime_store(first, checkpoint)

    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "UPDATE runtime_audit_records SET updated_at = ? WHERE collection = ? AND item_key = ?",
            ("sentinel-unchanged", "audit_records", first_record.record_id),
        )

    second = RuntimeStore()
    second_record = make_audit_record("web.status.refresh", "web")
    second.add_audit_record(second_record)
    save_runtime_store(second, checkpoint)

    loaded = load_runtime_store(checkpoint)
    loaded_actions = [record.action for record in loaded.list_audit_records()]
    with sqlite3.connect(checkpoint) as conn:
        first_updated_at = conn.execute(
            "SELECT updated_at FROM runtime_audit_records WHERE collection = ? AND item_key = ?",
            ("audit_records", first_record.record_id),
        ).fetchone()[0]

    assert first_updated_at == "sentinel-unchanged"
    assert "telegram.turn_recorded" in loaded_actions
    assert "web.status.refresh" in loaded_actions


def test_persistent_runtime_checkpoint_skips_sqlite_merge_when_file_unchanged(tmp_path, monkeypatch) -> None:
    checkpoint = tmp_path / "runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)
    runtime.store.add_audit_record(make_audit_record("initial.turn", "telegram"))
    runtime.checkpoint(force=True)

    load_calls = {"count": 0}
    original_load = runtime_persistence._load_runtime_store_sqlite_collections

    def count_sqlite_load(path, collections):  # noqa: ANN001
        load_calls["count"] += 1
        return original_load(path, collections)

    monkeypatch.setattr(runtime_persistence, "_load_runtime_store_sqlite_collections", count_sqlite_load)

    runtime.store.add_audit_record(make_audit_record("followup.turn", "telegram"))
    runtime.checkpoint(force=True)

    assert load_calls["count"] == 0
    assert [record.action for record in load_runtime_store(checkpoint).list_audit_records()] == [
        "initial.turn",
        "followup.turn",
    ]


def test_persistent_runtime_checkpoint_merges_when_file_changed_by_another_process(tmp_path, monkeypatch) -> None:
    checkpoint = tmp_path / "runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)
    runtime.store.add_audit_record(make_audit_record("initial.turn", "telegram"))
    runtime.checkpoint(force=True)

    other_runtime = bootstrap_persistent_runtime(checkpoint)
    other_runtime.store.add_audit_record(make_audit_record("external.web.turn", "web"))
    other_runtime.checkpoint(force=True)

    load_calls = {"count": 0}
    original_load = runtime_persistence._load_runtime_store_sqlite_collections

    def count_sqlite_load(path, collections):  # noqa: ANN001
        load_calls["count"] += 1
        return original_load(path, collections)

    monkeypatch.setattr(runtime_persistence, "_load_runtime_store_sqlite_collections", count_sqlite_load)

    runtime.store.add_audit_record(make_audit_record("telegram.followup", "telegram"))
    runtime.checkpoint(force=True)

    assert load_calls["count"] == 1
    loaded_actions = [record.action for record in load_runtime_store(checkpoint).list_audit_records()]
    assert "initial.turn" in loaded_actions
    assert "external.web.turn" in loaded_actions
    assert "telegram.followup" in loaded_actions


def test_sqlite_load_quarantines_semantically_invalid_rows(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    store = RuntimeStore()
    store.add_audit_record(make_audit_record("valid.record", "test"))
    save_runtime_store(store, checkpoint)

    bad_approval = {
        "approval_id": "approval-bad",
        "requested_by": "telegram_chat",
        "action": "allow_boundary",
        "resource": "https://example.test/*",
        "status": "not-a-real-status",
        "created_at": datetime.now(UTC).isoformat(),
        "request_kind": "boundary_policy",
        "context": {},
    }
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO approvals (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("approval_requests", "approval-bad", json.dumps(bad_approval), datetime.now(UTC).isoformat()),
        )

    loaded = load_runtime_store(checkpoint)

    assert [record.action for record in loaded.list_audit_records()] == ["valid.record"]
    assert loaded.get_approval_request("approval-bad") is None
    with sqlite3.connect(checkpoint) as conn:
        rows = conn.execute(
            "SELECT collection, original_key, reason FROM memory_migration_failures"
        ).fetchall()
        remaining_bad_rows = conn.execute(
            "SELECT COUNT(*) FROM approvals WHERE collection = 'approval_requests' AND item_key = 'approval-bad'"
        ).fetchone()[0]
    assert rows == [("approval_requests", "approval-bad", "'not-a-real-status' is not a valid ApprovalStatus")]
    assert remaining_bad_rows == 0


def test_sqlite_runtime_indexes_and_timing_logs(tmp_path, monkeypatch, caplog) -> None:
    checkpoint = tmp_path / "runtime.db"
    store = RuntimeStore()
    store.add_event(
        make_event(
            "tool.invocation.completed",
            "runtime",
            {
                "conversation_id": "web:indexes",
                "tool_name": "web_fetch",
                "task_id": "task-1",
            },
        )
    )

    monkeypatch.setattr(runtime_persistence, "RUNTIME_SQLITE_MEASURE_ENABLED", True)
    monkeypatch.setattr(runtime_persistence, "RUNTIME_SQLITE_SLOW_MS", 0)
    with caplog.at_level(logging.WARNING, logger="nullion.runtime_persistence"):
        save_runtime_store(store, checkpoint)
        loaded = load_runtime_store(checkpoint)

    assert loaded.list_events()[0].event_type == "tool.invocation.completed"
    assert "runtime SQLite save took" in caplog.text
    assert "runtime SQLite load took" in caplog.text

    with sqlite3.connect(checkpoint) as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            )
        }

    assert "idx_runtime_events_type_created" in indexes
    assert "idx_runtime_events_conversation_created" in indexes
    assert "idx_runtime_events_tool_created" in indexes
    assert "idx_runtime_events_item_key" in indexes
    assert "idx_runtime_events_collection_updated" in indexes
    assert "idx_doctor_actions_item_key" in indexes
    assert "idx_conversation_events_item_key" in indexes
    assert "idx_conversation_events_conversation_created" in indexes


def test_sqlite_runtime_checkpoint_preserves_cron_jobs_in_shared_table(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)
    cron_payload = {
        "id": "cron-keep",
        "name": "Keep me",
        "schedule": "0 9 * * *",
        "task": "Send the summary",
        "workspace_id": "workspace_admin",
        "delivery_channel": "web",
        "delivery_target": "web:conv",
        "enabled": True,
        "created_at": datetime.now(UTC).isoformat(),
        "last_run": None,
        "last_result": None,
        "next_run": datetime.now(UTC).isoformat(),
    }
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO reminders_crons (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("cron_jobs", "cron-keep", json.dumps(cron_payload), datetime.now(UTC).isoformat()),
        )

    store = RuntimeStore()
    store.add_reminder(ReminderRecord("rem", "web:conv", "standup", datetime.now(UTC)))
    save_runtime_store(store, checkpoint)

    with sqlite3.connect(checkpoint) as conn:
        cron_rows = conn.execute(
            "SELECT item_key, payload FROM reminders_crons WHERE collection = 'cron_jobs'"
        ).fetchall()
        reminder_rows = conn.execute(
            "SELECT item_key FROM reminders_crons WHERE collection = 'reminders'"
        ).fetchall()

    assert len(cron_rows) == 1
    assert cron_rows[0][0] == "cron-keep"
    assert json.loads(cron_rows[0][1])["task"] == "Send the summary"
    assert reminder_rows == [("rem",)]


def test_sqlite_corrupt_checkpoint_rebuild_restores_auxiliary_cron_jobs(
    tmp_path,
    monkeypatch,
) -> None:
    checkpoint = tmp_path / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)
    cron_payload = {
        "id": "cron-recovered",
        "name": "Recover me",
        "schedule": "0 9 * * *",
        "task": "Run the scheduled account check",
        "workspace_id": "workspace_admin",
        "delivery_channel": "telegram",
        "delivery_target": "100001",
        "enabled": True,
        "created_at": datetime.now(UTC).isoformat(),
        "last_run": None,
        "last_result": None,
        "next_run": datetime.now(UTC).isoformat(),
    }
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO reminders_crons (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("cron_jobs", "cron-recovered", json.dumps(cron_payload), datetime.now(UTC).isoformat()),
        )

    original_save_sqlite = runtime_persistence._save_runtime_store_sqlite
    calls = 0

    def fail_once(store, path, *, merge_existing=True, payload=None):  # noqa: ANN001
        nonlocal calls
        if calls == 0:
            calls += 1
            raise sqlite3.DatabaseError("database disk image is malformed")
        return original_save_sqlite(store, path, merge_existing=merge_existing, payload=payload)

    store = RuntimeStore()
    store.add_audit_record(make_audit_record("after.rebuild", "unit"))
    monkeypatch.setattr(runtime_persistence, "_save_runtime_store_sqlite", fail_once)

    save_runtime_store(store, checkpoint)

    with sqlite3.connect(checkpoint) as conn:
        rows = conn.execute(
            "SELECT item_key, payload FROM reminders_crons WHERE collection = 'cron_jobs'"
        ).fetchall()
        corrupt_backups = sorted(checkpoint.parent.glob("runtime.db.corrupt-*"))

    assert calls == 1
    assert len(corrupt_backups) == 1
    assert rows and rows[0][0] == "cron-recovered"
    assert json.loads(rows[0][1])["delivery_target"] == "100001"
    assert [record.action for record in load_runtime_store(checkpoint).list_audit_records()] == ["after.rebuild"]


def test_runtime_backup_rotation_ignores_concurrent_missing_backup(tmp_path, monkeypatch) -> None:
    target = tmp_path / "runtime.db"
    target.write_text("current", encoding="utf-8")
    backup = runtime_persistence._backup_path_for(target, 0)
    backup.write_text("backup", encoding="utf-8")
    original_replace = type(backup).replace

    def disappearing_replace(self, target_path):  # noqa: ANN001
        if self == backup:
            self.unlink()
            raise FileNotFoundError(str(self))
        return original_replace(self, target_path)

    monkeypatch.setattr(type(backup), "replace", disappearing_replace)

    runtime_persistence._rotate_runtime_store_backups(target)

    assert not backup.exists()


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
            conversation_id="telegram:100001",
            chat_id="100001",
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


def test_sqlite_runtime_store_encrypts_user_memory_payloads(tmp_path) -> None:
    store = RuntimeStore()
    now = datetime.now(UTC)
    store.add_user_memory_entry(
        UserMemoryEntry(
            "workspace:ops:favorite_color",
            "workspace:ops",
            UserMemoryKind.PREFERENCE,
            "favorite_color",
            "Favorite color is ultraviolet.",
            "builder",
            now,
            now,
            use_count=3,
            use_score=7.0,
            last_used_at=now,
        )
    )

    checkpoint = tmp_path / "runtime.db"
    save_runtime_store(store, checkpoint)

    raw_db = checkpoint.read_bytes()
    assert b"Favorite color is ultraviolet" not in raw_db
    assert b"favorite_color" not in raw_db
    assert b"workspace:ops" not in raw_db

    with sqlite3.connect(checkpoint) as conn:
        rows = conn.execute("SELECT collection, item_key, payload FROM memory").fetchall()

    assert len(rows) == 1
    collection, item_key, payload = rows[0]
    assert collection == "preferences"
    assert item_key == "000000000000"
    assert payload.startswith("fernet:v1:")
    assert "Favorite color is ultraviolet" not in payload
    assert "favorite_color" not in payload

    loaded = load_runtime_store(checkpoint)
    [memory] = loaded.list_user_memory_entries(kind=UserMemoryKind.PREFERENCE)
    assert memory.owner == "workspace:ops"
    assert memory.key == "favorite_color"
    assert memory.value == "Favorite color is ultraviolet."
    assert memory.use_count == 3
    assert memory.use_score == 7.0


def test_sqlite_checkpoint_merges_existing_conversation_session_settings(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"

    telegram_store = RuntimeStore()
    telegram_store.add_conversation_event(
        {
            "conversation_id": "telegram:100001",
            "event_type": "conversation.session_settings",
            "created_at": "2026-05-24T23:39:40+00:00",
            "chat_id": "100001",
            "activity_trace_enabled": False,
        }
    )
    save_runtime_store(telegram_store, checkpoint)

    stale_web_store = RuntimeStore()
    stale_web_store.add_conversation_event(
        {
            "conversation_id": "web:settings",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-05-24T23:40:00+00:00",
            "turn_id": "turn-after-setting",
            "user_message": "still running",
            "assistant_reply": "ok",
        }
    )
    save_runtime_store(stale_web_store, checkpoint)

    loaded = load_runtime_store(checkpoint)
    telegram_events = loaded.list_conversation_events("telegram:100001")
    assert any(
        event.get("event_type") == "conversation.session_settings"
        and event.get("activity_trace_enabled") is False
        for event in telegram_events
    )
    assert any(
        event.get("turn_id") == "turn-after-setting"
        for event in loaded.list_conversation_events("web:settings")
    )


def test_sqlite_conversation_events_load_in_created_at_order_for_recent_context(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    store = RuntimeStore()
    store.add_conversation_event(
        {
            "conversation_id": "telegram:1",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-05-25T18:35:00+00:00",
            "turn_id": "turn-latest",
            "user_message": "latest request",
            "assistant_reply": "latest answer",
        }
    )
    store.add_conversation_event(
        {
            "conversation_id": "telegram:1",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-05-25T18:31:00+00:00",
            "turn_id": "turn-earlier",
            "user_message": "earlier request",
            "assistant_reply": "earlier answer",
        }
    )
    save_runtime_store(store, checkpoint)

    loaded = load_runtime_store(checkpoint)

    assert [
        event["turn_id"]
        for event in loaded.list_conversation_events("telegram:1")
        if event.get("event_type") == "conversation.chat_turn"
    ] == ["turn-earlier", "turn-latest"]
    assert [
        event["turn_id"]
        for event in loaded.list_recent_conversation_events(
            "telegram:1",
            event_type="conversation.chat_turn",
            limit=1,
        )
    ] == ["turn-latest"]


def test_sqlite_save_preserves_existing_conversation_events_when_merge_fails(tmp_path, monkeypatch) -> None:
    checkpoint = tmp_path / "runtime.db"
    current_store = RuntimeStore()
    current_store.add_conversation_event(
        {
            "conversation_id": "telegram:1",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-05-25T18:31:00+00:00",
            "turn_id": "turn-current",
            "user_message": "current request",
            "assistant_reply": "current answer",
        }
    )
    save_runtime_store(current_store, checkpoint)

    stale_store = RuntimeStore()
    stale_store.add_conversation_event(
        {
            "conversation_id": "telegram:1",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-05-25T18:05:00+00:00",
            "turn_id": "turn-stale",
            "user_message": "stale request",
            "assistant_reply": "stale answer",
        }
    )
    monkeypatch.setattr(
        runtime_persistence,
        "_load_runtime_store_sqlite_collections",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")),
    )

    save_runtime_store(stale_store, checkpoint)
    loaded = load_runtime_store(checkpoint)

    assert [
        event["turn_id"]
        for event in loaded.list_conversation_events("telegram:1")
        if event.get("event_type") == "conversation.chat_turn"
    ] == ["turn-stale", "turn-current"]


def test_sqlite_memory_migration_encrypts_plaintext_rows_without_plaintext_backup(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)
    now = datetime.now(UTC).isoformat()
    plaintext = json.dumps(
        {
            "entry_id": "workspace:ops:watcher",
            "owner": "workspace:ops",
            "kind": "preference",
            "key": "watcher",
            "value": "Watcher memory should be encrypted.",
            "source": "legacy",
            "created_at": now,
            "updated_at": now,
        },
        sort_keys=True,
    )
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO memory (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("preferences", "workspace:ops:watcher", plaintext, now),
        )

    result = migrate_sqlite_runtime_memory_payloads(checkpoint)

    assert result["encrypted_rows"] == 1
    assert result["quarantined_rows"] == 0
    assert not checkpoint.with_name("runtime.db.pre-memory-encryption.bak").exists()
    raw_db = checkpoint.read_bytes()
    assert b"Watcher memory should be encrypted" not in raw_db
    assert b"workspace:ops:watcher" not in raw_db
    [memory] = load_runtime_store(checkpoint).list_user_memory_entries(kind=UserMemoryKind.PREFERENCE)
    assert memory.value == "Watcher memory should be encrypted."


def test_sqlite_runtime_load_quarantines_unreadable_memory_rows(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    now = datetime.now(UTC)
    store = RuntimeStore()
    store.add_user_memory_entry(
        UserMemoryEntry(
            "workspace:ops:kept",
            "workspace:ops",
            UserMemoryKind.PREFERENCE,
            "kept",
            "Keep this memory.",
            "builder",
            now,
            now,
        )
    )
    save_runtime_store(store, checkpoint)

    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO memory (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("preferences", "bad", "fernet:v1:not-a-valid-token", now.isoformat()),
        )

    loaded = load_runtime_store(checkpoint)

    memories = loaded.list_user_memory_entries(kind=UserMemoryKind.PREFERENCE)
    assert [memory.key for memory in memories] == ["kept"]
    with sqlite3.connect(checkpoint) as conn:
        active_bad_rows = conn.execute(
            "SELECT COUNT(*) FROM memory WHERE collection = ? AND item_key = ?",
            ("preferences", "bad"),
        ).fetchone()[0]
        quarantined = conn.execute(
            "SELECT collection, original_key, payload, reason FROM memory_migration_failures "
            "WHERE collection = ? AND original_key = ?",
            ("preferences", "bad"),
        ).fetchone()

    assert active_bad_rows == 0
    assert quarantined is not None
    assert quarantined[0] == "preferences"
    assert quarantined[2] == "fernet:v1:not-a-valid-token"
    assert quarantined[3]


def test_sqlite_runtime_load_quarantines_unreadable_non_memory_rows(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    now = datetime.now(UTC)
    approval = create_approval_request(
        requested_by="web:local",
        action="open_workspace",
        resource="workspace_admin",
        request_kind="tool",
        context={"workspace": "workspace_admin"},
    )
    store = RuntimeStore()
    store.add_approval_request(approval)
    save_runtime_store(store, checkpoint)

    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO approvals (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("approval_requests", "bad", "", now.isoformat()),
        )

    loaded = load_runtime_store(checkpoint)

    assert loaded.get_approval_request(approval.approval_id) is not None
    with sqlite3.connect(checkpoint) as conn:
        active_bad_rows = conn.execute(
            "SELECT COUNT(*) FROM approvals WHERE collection = ? AND item_key = ?",
            ("approval_requests", "bad"),
        ).fetchone()[0]
        quarantined = conn.execute(
            "SELECT collection, original_key, payload, reason FROM memory_migration_failures "
            "WHERE collection = ? AND original_key = ?",
            ("approval_requests", "bad"),
        ).fetchone()

    assert active_bad_rows == 0
    assert quarantined is not None
    assert quarantined[0] == "approval_requests"
    assert str(quarantined[2]).startswith("fernet:v1:")
    assert quarantined[3]


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


def test_runtime_store_migration_backfills_missing_workflow_collections_without_clobbering_legacy_rows() -> None:
    payload = migrate_runtime_store_payload(
        {
            "approvals": [
                {
                    "approval_id": "approval-legacy",
                    "requested_by": "web:operator",
                    "action": "use_tool",
                    "resource": "web_fetch",
                    "status": "pending",
                    "created_at": "2026-01-01T00:00:00+00:00",
                }
            ],
            "reminders": [
                {
                    "reminder_id": "reminder-legacy",
                    "chat_id": "web:operator",
                    "text": "ship check",
                    "due_at": "2026-01-01T00:01:00+00:00",
                }
            ],
        }
    )

    assert payload["format_version"] == 1
    assert payload["approvals"][0]["approval_id"] == "approval-legacy"
    assert payload["reminders"][0]["reminder_id"] == "reminder-legacy"
    for collection in (
        "builder_proposals",
        "conversation_branches",
        "conversation_events",
        "mini_agent_runs",
        "task_frames",
        "user_facts",
        "preferences",
        "environment_facts",
    ):
        assert payload[collection] == []


def test_scheduled_background_mini_agent_reconciliation_fails_orphaned_run(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    scheduled_run = runtime.start_mini_agent_run(
        run_id="run-scheduled-background",
        capsule_id="scheduled-group",
        mini_agent_type="Deliver report",
        created_at=datetime.now(UTC) - timedelta(hours=2),
        metadata={"scheduled_task_run": True, "deep_agent_profiles": ["scheduled_job"]},
    )
    runtime.store.add_mini_agent_run(replace(scheduled_run, status=MiniAgentRunStatus.RUNNING))
    ordinary_run = runtime.start_mini_agent_run(
        run_id="run-ordinary-stale",
        capsule_id="ordinary-group",
        mini_agent_type="Ordinary task",
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    runtime.store.add_mini_agent_run(replace(ordinary_run, status=MiniAgentRunStatus.RUNNING))

    repaired = runtime.reconcile_stale_mini_agent_runs(
        now=datetime.now(UTC),
        stale_after=timedelta(minutes=1),
        live_run_ids=set(),
        live_group_ids=set(),
    )

    assert [item.run_id for item in repaired] == ["run-scheduled-background", "run-ordinary-stale"]
    assert runtime.store.get_mini_agent_run("run-ordinary-stale").status is MiniAgentRunStatus.FAILED
    assert runtime.store.get_mini_agent_run("run-scheduled-background").status is MiniAgentRunStatus.FAILED
    assert "scheduled background Mini-Agent" in (runtime.store.get_mini_agent_run("run-scheduled-background").result_summary or "")


def test_stale_mini_agent_reconciliation_protects_live_group_and_repairs_orphan(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    live_group_run = runtime.start_mini_agent_run(
        run_id="run-live-group",
        capsule_id="live-group",
        mini_agent_type="Live group task",
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    orphaned_run = runtime.start_mini_agent_run(
        run_id="run-orphaned-group",
        capsule_id="orphaned-group",
        mini_agent_type="Orphaned group task",
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    runtime.store.add_mini_agent_run(replace(live_group_run, status=MiniAgentRunStatus.RUNNING))
    runtime.store.add_mini_agent_run(replace(orphaned_run, status=MiniAgentRunStatus.RUNNING))

    repaired = runtime.reconcile_stale_mini_agent_runs(
        now=datetime.now(UTC),
        stale_after=timedelta(minutes=1),
        live_run_ids=set(),
        live_group_ids={"live-group"},
    )

    assert [item.run_id for item in repaired] == ["run-orphaned-group"]
    assert runtime.store.get_mini_agent_run("run-live-group").status is MiniAgentRunStatus.RUNNING
    assert runtime.store.get_mini_agent_run("run-orphaned-group").status is MiniAgentRunStatus.FAILED


def test_stale_mini_agent_reconciliation_can_scope_to_owned_conversation_prefix(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    web_run = runtime.start_mini_agent_run(
        run_id="run-web-live",
        capsule_id="web-group",
        mini_agent_type="Web task",
        created_at=datetime.now(UTC) - timedelta(hours=2),
        metadata={"conversation_id": "web:status"},
    )
    telegram_run = runtime.start_mini_agent_run(
        run_id="run-telegram-active",
        capsule_id="telegram-group",
        mini_agent_type="Telegram task",
        created_at=datetime.now(UTC) - timedelta(hours=2),
        metadata={"conversation_id": "telegram:123"},
    )
    runtime.store.add_mini_agent_run(replace(web_run, status=MiniAgentRunStatus.RUNNING))
    runtime.store.add_mini_agent_run(replace(telegram_run, status=MiniAgentRunStatus.RUNNING))

    repaired = runtime.reconcile_stale_mini_agent_runs(
        now=datetime.now(UTC),
        stale_after=timedelta(minutes=1),
        live_run_ids={"run-web-live"},
        live_group_ids={"web-group"},
        owned_conversation_prefixes=("web:",),
    )

    assert repaired == []
    assert runtime.store.get_mini_agent_run("run-web-live").status is MiniAgentRunStatus.RUNNING
    assert runtime.store.get_mini_agent_run("run-telegram-active").status is MiniAgentRunStatus.RUNNING
