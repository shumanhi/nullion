from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
)


NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _action(**overrides):
    data = {
        "action_id": "act-1234567890abcdef",
        "status": "pending",
        "severity": "high",
        "summary": "Run appears stuck",
        "reason": (
            "source=runtime;issue_type=stalled;conversation_id=conv-1;"
            "run_id=run-1;principal_id=operator;tool_count=3;last_tool=shell;"
            "last_status=running;detail=No progress"
        ),
        "recommendation_code": "stuck_run",
    }
    data.update(overrides)
    return data


def _frame() -> TaskFrame:
    return TaskFrame(
        frame_id="frame-1",
        conversation_id="conv-1",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.RUNNING,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=TaskFrameTarget(kind="chat", value="question"),
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="answer",
        created_at=NOW,
        updated_at=NOW,
    )


class StoreDouble:
    def __init__(self, action=None):
        self.action = action or _action()
        self.frame = _frame()
        self.active_frame_id = "frame-1"
        self.saved_frames = []
        self.runs = [
            MiniAgentRun("run-new", "cap", "researcher", MiniAgentRunStatus.RUNNING, NOW + timedelta(minutes=1), "new"),
            MiniAgentRun("run-old", "cap", "verifier", MiniAgentRunStatus.FAILED, NOW, "old"),
        ]

    def get_doctor_action(self, action_id):
        return self.action if action_id == self.action["action_id"] else None

    def get_active_task_frame_id(self, conversation_id):
        return self.active_frame_id if conversation_id == "conv-1" else None

    def get_task_frame(self, frame_id):
        return self.frame if frame_id == "frame-1" else None

    def add_task_frame(self, frame):
        self.saved_frames.append(frame)
        self.frame = frame

    def set_active_task_frame_id(self, conversation_id, frame_id):
        self.active_frame_id = frame_id

    def list_mini_agent_runs(self):
        return list(self.runs)


class RuntimeDouble:
    def __init__(self, action=None):
        self.store = StoreDouble(action)
        self.started = []
        self.cancelled = []
        self.failed_runs = []
        self.checkpoints = 0

    def start_doctor_action(self, action_id):
        self.started.append(action_id)
        self.store.action = {**self.store.action, "status": "in_progress"}
        return self.store.action

    def cancel_doctor_action(self, action_id, *, reason):
        self.cancelled.append((action_id, reason))
        self.store.action = {**self.store.action, "status": "cancelled", "reason": reason}
        return self.store.action

    def fail_mini_agent_run(self, run_id, *, result_summary):
        self.failed_runs.append((run_id, result_summary))

    def checkpoint(self):
        self.checkpoints += 1

    def diagnose_runtime_health(self):
        return {"summary": "healthy"}


def test_format_doctor_action_inspection_includes_evidence_runs_and_commands() -> None:
    from nullion.doctor_playbooks import format_doctor_action_inspection

    text = format_doctor_action_inspection(RuntimeDouble(), _action())

    assert "Doctor run inspection" in text
    assert "Issue: stalled" in text
    assert "Recent Mini-Agent runs" in text
    assert text.index("run-new") < text.index("run-old")
    assert "/doctor run act-1234567890abcdef doctor:inspect_run" in text
    assert "git worktree add .worktrees/doctor-1234567890ab" in text


def test_doctor_inspect_cancel_and_retry_workflow_commands() -> None:
    from nullion.doctor_playbooks import execute_doctor_playbook_command

    runtime = RuntimeDouble()
    inspected = execute_doctor_playbook_command(
        runtime,
        action_id="act-1234567890abcdef",
        command="doctor:inspect_run",
        source_label="telegram",
    )
    assert inspected.acknowledgement == "Inspect run"
    assert runtime.started == ["act-1234567890abcdef"]

    runtime = RuntimeDouble()
    cancelled = execute_doctor_playbook_command(
        runtime,
        action_id="act-1234567890abcdef",
        command="doctor:cancel_run",
        source_label="web",
    )
    assert cancelled.acknowledgement == "Cancel noted"
    assert "Cleared task frame frame-1, Mini-Agent run run-1" in cancelled.message
    assert runtime.store.frame.status is TaskFrameStatus.CANCELLED
    assert runtime.store.active_frame_id is None
    assert runtime.failed_runs == [("run-1", "Cancelled from web via Doctor")]
    assert runtime.checkpoints == 1

    retried = execute_doctor_playbook_command(
        RuntimeDouble(),
        action_id="act-1234567890abcdef",
        command="doctor:retry_workflow",
        source_label="operator command",
    )
    assert retried.acknowledgement == "Retry workflow"
    assert "previous run is stopped" in retried.message


def test_doctor_restart_and_reconnect_commands_use_available_callbacks() -> None:
    from nullion.doctor_playbooks import execute_doctor_playbook_command

    runtime = RuntimeDouble()
    restarted = execute_doctor_playbook_command(
        runtime,
        action_id="act-1234567890abcdef",
        command="doctor:restart_bot",
        source_label="telegram",
        restart_chat_services=lambda: "services restarting",
    )
    assert restarted.acknowledgement == "Restarting"
    assert restarted.message == "services restarting"
    assert runtime.cancelled

    signalled = []
    reconnect = execute_doctor_playbook_command(
        RuntimeDouble(),
        action_id="act-1234567890abcdef",
        command="doctor:reconnect_telegram",
        source_label="telegram",
        signal_current_process_restart=lambda: signalled.append("restart"),
    )
    assert reconnect.acknowledgement == "Reconnecting"
    assert reconnect.message == "Reconnecting to Telegram now."
    assert signalled == ["restart"]

    manual = execute_doctor_playbook_command(
        RuntimeDouble(),
        action_id="act-1234567890abcdef",
        command="doctor:reconnect_telegram",
        source_label="telegram",
    )
    assert manual.acknowledgement == "Telegram action needed"


def test_doctor_common_remediation_commands_update_expected_statuses(monkeypatch) -> None:
    from nullion import doctor_playbooks

    cases = {
        "doctor:retry_model_api": ("Retrying", "next message"),
        "doctor:pause_chat": ("Paused", "resume"),
        "doctor:switch_fallback_model": ("Model switch needed", "fallback provider"),
        "doctor:retry_later": ("Retry later", "quota"),
        "doctor:reconnect_slack": ("Slack action needed", "Slack test message"),
        "doctor:restart_discord_adapter": ("Discord action needed", "Discord test message"),
        "doctor:open_schedule": ("Open schedule", "Scheduled Tasks"),
        "doctor:disable_task": ("Disable task", "disable"),
        "doctor:review_approvals": ("Review approvals", "Approvals"),
        "doctor:clear_stale_approvals": ("Clear stale approvals", "deny"),
        "doctor:create_backup": ("Create backup", "backup"),
        "doctor:repair_checkpoint": ("Repair checkpoint", "checkpoint"),
        "doctor:restart_plugin": ("Acknowledged", "Action noted"),
    }
    for command, (ack, expected_text) in cases.items():
        result = doctor_playbooks.execute_doctor_playbook_command(
            RuntimeDouble(),
            action_id="act-1234567890abcdef",
            command=command,
            source_label="operator command",
        )
        assert result.acknowledgement == ack
        assert expected_text in result.message

    monkeypatch.setattr(
        "nullion.runtime.format_doctor_diagnosis_for_operator",
        lambda report: f"diagnosis: {report['summary']}",
    )
    diagnosed = doctor_playbooks.execute_doctor_playbook_command(
        RuntimeDouble(),
        action_id="act-1234567890abcdef",
        command="doctor:run_diagnosis",
        source_label="web",
    )
    assert diagnosed.acknowledgement == "Diagnosis complete"
    assert diagnosed.message == "diagnosis: healthy"


def test_doctor_terminal_and_unknown_actions_are_left_or_dismissed() -> None:
    from nullion.doctor_playbooks import execute_doctor_playbook_command

    terminal = RuntimeDouble(_action(status="completed"))
    result = execute_doctor_playbook_command(
        terminal,
        action_id="act-1234567890abcdef",
        command="doctor:inspect_run",
        source_label="web",
    )
    assert result.action["status"] == "completed"
    assert terminal.started == []

    unknown = execute_doctor_playbook_command(
        RuntimeDouble(),
        action_id="act-1234567890abcdef",
        command="doctor:made_up",
        source_label="web",
    )
    assert unknown.acknowledgement == "Dismissed"
    assert unknown.message == "Action dismissed."
