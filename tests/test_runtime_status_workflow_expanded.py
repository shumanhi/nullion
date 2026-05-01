from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from nullion.approvals import ApprovalRequest, ApprovalStatus, PermissionGrant
from nullion.intent import IntentCapsule, IntentState
from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus
from nullion.missions import MissionChecklistItem, MissionContinuationPolicy, MissionRecord, MissionStatus, MissionStep
from nullion.progress import ProgressState, ProgressUpdate
from nullion.runtime_status import (
    build_runtime_status_snapshot,
    compute_approval_pressure,
    format_runtime_status_for_telegram,
    render_runtime_status_for_telegram,
)
from nullion.scheduler import ScheduleKind, ScheduledTask
from nullion.sentinel_escalations import EscalationStatus, SentinelEscalationArtifact
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
)


NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class StatusStore:
    def __init__(self) -> None:
        self.capsules = {
            "cap-wait": IntentCapsule(
                capsule_id="cap-wait",
                owner="workspace:one",
                goal="Approve deployment",
                state=IntentState.WAITING_APPROVAL,
                risk_level="high",
                active_mini_agents=["runner"],
                pending_approval_id="ap-1",
                success_criteria=["approved"],
            ),
            "cap-run": IntentCapsule(
                capsule_id="cap-run",
                owner="workspace:two",
                goal="Run checks",
                state=IntentState.RUNNING,
                risk_level="normal",
                active_mini_agents=[],
                pending_approval_id=None,
                success_criteria=[],
            ),
        }
        self.conversation_heads = {"conv-1": {"active_branch_id": "branch-1", "active_turn_id": "turn-2"}}
        self.conversation_branches = {"branch-1": SimpleNamespace(conversation_id="conv-1")}
        self.conversation_turns = {"turn-2": SimpleNamespace(conversation_id="conv-1")}
        self.frame = TaskFrame(
            frame_id="frame-1",
            conversation_id="conv-1",
            branch_id="branch-1",
            source_turn_id="turn-1",
            parent_frame_id=None,
            status=TaskFrameStatus.WAITING_APPROVAL,
            operation=TaskFrameOperation.GENERATE_ARTIFACT,
            target=TaskFrameTarget(kind="url", value="https://example.com", normalized_value="https://example.com/"),
            execution=TaskFrameExecutionContract(),
            output=TaskFrameOutputContract(artifact_kind="html", delivery_mode="attachment"),
            finish=TaskFrameFinishCriteria(requires_artifact_delivery=True),
            summary="fetch page",
            created_at=NOW,
            updated_at=NOW,
        )

    def list_capsules(self):
        return list(self.capsules.values())

    def list_progress_updates(self):
        return [
            ProgressUpdate(ProgressState.ACKNOWLEDGED, "older", "cap-wait"),
            ProgressUpdate(ProgressState.WAITING_APPROVAL, "Need approval", "cap-wait"),
        ]

    def list_mini_agent_runs(self):
        return [
            MiniAgentRun("run-1", "cap-wait", "researcher", MiniAgentRunStatus.RUNNING, NOW, "working"),
            MiniAgentRun("run-2", "cap-run", "verifier", MiniAgentRunStatus.COMPLETED, NOW, "done"),
        ]

    def list_scheduled_tasks(self):
        return [
            ScheduledTask("sched-1", "cap-wait", ScheduleKind.RECURRING, 15, True, NOW, 1),
            ScheduledTask("sched-2", "cap-run", ScheduleKind.ONCE, 0, False, None, 0),
        ]

    def list_doctor_actions(self):
        return [
            {
                "action_id": "doc-1",
                "status": "pending",
                "action_type": "repair",
                "recommendation_code": "fix_env",
                "severity": "high",
                "summary": "Fix env",
                "updated_at": NOW.isoformat(),
            }
        ]

    def list_sentinel_escalations(self):
        return [
            SentinelEscalationArtifact(
                escalation_id="esc-1",
                source_signal_reason="unsafe",
                severity="critical",
                status=EscalationStatus.ESCALATED,
                created_at=NOW,
                summary="Unsafe action",
                approval_id="ap-1",
            )
        ]

    def list_approval_requests(self):
        return [
            ApprovalRequest("ap-1", "cap-wait", "execute", "deploy", ApprovalStatus.PENDING, NOW),
            ApprovalRequest("ap-2", "cap-run", "read", "file", ApprovalStatus.APPROVED, NOW),
            ApprovalRequest("ap-3", "cap-run", "write", "file", ApprovalStatus.DENIED, NOW),
            ApprovalRequest("ap-4", "cap-run", "fetch", "url", ApprovalStatus.PENDING, NOW),
        ]

    def list_permission_grants(self):
        return [
            PermissionGrant("grant-1", "ap-2", "cap-run", "file_read", "admin", NOW),
            PermissionGrant("grant-2", "ap-old", "cap-run", "web", "admin", NOW, expires_at=NOW - timedelta(seconds=1)),
            PermissionGrant("grant-3", "ap-old", "cap-run", "terminal", "admin", NOW, revoked_at=NOW),
            PermissionGrant("grant-4", "ap-2", "cap-run", "extra", "admin", NOW),
        ]

    def list_missions(self):
        return [
            MissionRecord(
                mission_id="mission-1",
                owner="workspace:one",
                title="Deploy",
                goal="Ship safely",
                status=MissionStatus.WAITING_APPROVAL,
                continuation_policy=MissionContinuationPolicy.MANUAL,
                active_capsule_id="cap-wait",
                active_step_id="step-2",
                steps=(
                    MissionStep("step-1", "Plan", "completed", "plan"),
                    MissionStep("step-2", "Approve", "running", "approval", mini_agent_run_ids=("run-1", "run-2"), required_mini_agent_run_ids=("run-2",)),
                ),
                completion_checklist=(
                    MissionChecklistItem("item-1", "Approval", required=True, satisfied=False, details="Need human approval"),
                ),
                waiting_on="operator",
            )
        ]

    def list_conversation_events(self, conversation_id=None):
        events = [
            {"conversation_id": "conv-1", "event_type": "conversation.chat_turn", "created_at": NOW.isoformat()},
            {"conversation_id": "conv-1", "event_type": "tool.result", "updated_at": NOW.isoformat()},
        ]
        if conversation_id is None:
            return events
        return [event for event in events if event["conversation_id"] == conversation_id]

    def get_conversation_head(self, conversation_id):
        return self.conversation_heads.get(conversation_id)

    def get_active_task_frame_id(self, conversation_id):
        return "frame-1" if conversation_id == "conv-1" else None

    def get_task_frame(self, frame_id):
        return self.frame if frame_id == "frame-1" else None


def test_runtime_status_snapshot_summarizes_store_and_filters_capsule() -> None:
    store = StatusStore()
    snapshot = build_runtime_status_snapshot(store)

    assert snapshot["counts"]["capsules"] == 2
    assert snapshot["counts"]["running_capsules"] == 1
    assert snapshot["counts"]["waiting_approval_capsules"] == 1
    assert snapshot["counts"]["active_mini_agent_runs"] == 1
    assert snapshot["counts"]["pending_doctor_actions"] == 1
    assert snapshot["counts"]["open_sentinel_escalations"] == 1
    assert snapshot["capsules"][0]["capsule_id"] == "cap-wait"
    assert snapshot["capsules"][0]["latest_progress"]["message"] == "Need approval"
    assert snapshot["conversations"][0]["active_task_frame_blocking_condition"] == "approval required"
    assert snapshot["missions"][0]["checklist_items"][0]["details"] == "Need human approval"

    filtered = build_runtime_status_snapshot(store, capsule_id="cap-run")
    assert [capsule["capsule_id"] for capsule in filtered["capsules"]] == ["cap-run"]
    assert [run["capsule_id"] for run in filtered["mini_agent_runs"]] == ["cap-run"]
    assert [task["capsule_id"] for task in filtered["scheduled_tasks"]] == ["cap-run"]
    assert filtered.get("missions") is None


def test_runtime_status_format_full_and_active_only_dense_sections() -> None:
    snapshot = build_runtime_status_snapshot(StatusStore())
    rendered = format_runtime_status_for_telegram(snapshot)

    assert "📌 Nullion status" in rendered
    assert "2 capsules • 1 running • 4 approval required" in rendered
    assert "Capsules" in rendered
    assert "Conversations" in rendered
    assert "frame=frame-1 [approval required]" in rendered
    assert "Missions" in rendered
    assert "Checklist: Approval — Need human approval" in rendered
    assert "Scheduler" in rendered
    assert "Sentinel" in rendered
    assert "Doctor" in rendered
    assert "Approvals (showing 3/4" in rendered
    assert "... and 1 more approval request." in rendered
    assert "Grants (showing 3/4" in rendered
    assert "... and 1 more permission grant." in rendered

    active = format_runtime_status_for_telegram(snapshot, active_only=True)
    assert "cap-run: Run checks" not in active
    assert "grant-2" not in active
    assert "fetch on url" in active


def test_runtime_status_empty_snapshot_and_render_wrapper() -> None:
    empty = {
        "counts": {
            "capsules": 0,
            "running_capsules": 0,
            "waiting_approval_capsules": 0,
            "active_mini_agent_runs": 0,
            "open_sentinel_escalations": 0,
            "pending_doctor_actions": 0,
        },
        "capsules": [],
        "mini_agent_runs": [],
        "sentinel_escalations": [],
        "doctor_actions": [],
        "approval_requests": [],
        "permission_grants": [],
        "checkpoint": None,
    }
    assert "No active work right now." in format_runtime_status_for_telegram(empty)
    assert render_runtime_status_for_telegram(StatusStore(), capsule_id="cap-wait", active_only=True).startswith("📌 Nullion status")


def test_approval_pressure_counts_active_and_total() -> None:
    snapshot = build_runtime_status_snapshot(StatusStore())
    assert compute_approval_pressure(snapshot) == {
        "approval_required_capsules": 1,
        "approval_required_missions": 1,
        "pending_approval_requests": 2,
        "total": 4,
    }
    assert compute_approval_pressure(snapshot, active_only=True) == {
        "approval_required_capsules": 1,
        "approval_required_missions": 1,
        "pending_approval_requests": 2,
        "total": 4,
    }
