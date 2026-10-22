from __future__ import annotations

import base64
import asyncio
import inspect
import importlib.util
import json
import sqlite3
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from runtime_sqlite_compat_fixture import (
    LEGACY_APPROVAL_ID,
    LEGACY_BRANCH_ID,
    LEGACY_CONVERSATION_ID,
    LEGACY_FRAME_ID,
    LEGACY_GRANT_ID,
    LEGACY_MISSION_ID,
    LEGACY_TURN_ID,
    create_legacy_runtime_sqlite_db,
)
from nullion import crons, providers, telegram_entrypoint, updater, webview_app
from nullion.approvals import create_approval_request
from nullion.audit import make_audit_record
from nullion.credential_store import load_encrypted_credentials
from nullion.memory import UserMemoryKind
from nullion.reminders import reminder_chat_context
from nullion.runtime_persistence import load_runtime_store, save_runtime_store
from nullion.runtime_store import RuntimeStore
from nullion.tools import (
    ToolInvocation,
    _build_create_cron_handler,
    _build_delete_cron_handler,
    _build_list_crons_handler,
    _build_run_cron_handler,
    _build_toggle_cron_handler,
    _build_update_cron_handler,
)


@pytest.fixture
def manual_cron_workers(monkeypatch):
    workers = []
    original_start = threading.Thread.start

    def start(thread, *args, **kwargs):
        if thread.name.startswith("nullion-telegram-manual-cron-"):
            workers.append(thread)
        return original_start(thread, *args, **kwargs)

    def finish():
        # These checks prove final delivery, rather than racing the deferred
        # receipt. Keep transport and datastore patches alive through cleanup.
        for thread in workers:
            thread.join(timeout=10)
            assert not thread.is_alive(), "Manual cron worker did not finish"

    monkeypatch.setattr(threading.Thread, "start", start)
    yield finish
    finish()


def invoke(tool_name: str, arguments: dict[str, object]) -> ToolInvocation:
    return ToolInvocation(f"inv-{tool_name}", tool_name, "user:admin", arguments)


@pytest.fixture
def registered_cron_operator(tmp_path,monkeypatch):
    from nullion import users
    monkeypatch.setattr(users,'_USERS_PATH',tmp_path/'users.json')
    users.save_user_registry(users.UserRegistry(True,[users.NullionUser('1','Fixture operator',role='admin')]))


def test_cron_final_repair_evidence_preserves_account_result_head_and_tail() -> None:
    from nullion.cron_delivery import _cron_final_repair_tool_evidence
    from nullion.tools import ToolResult

    result = ToolResult(
        "email-search-1",
        "email_search",
        "completed",
        {
            "messages": [
                {
                    "subject": f"Rosemarie thread {index}",
                    "from": f"person-{index}@example.test",
                    "date": f"2026-06-{index + 1:02d}",
                    "snippet": f"Useful escalation clue {index}",
                }
                for index in range(14)
            ]
        },
    )

    evidence = json.loads(_cron_final_repair_tool_evidence([result]))

    record = evidence[0]
    assert record["tool_name"] == "email_search"
    assert record["items_truncated"] == {"shown": 8, "head": 5, "tail": 3, "total": 14}
    assert record["items"][0]["title"] == "Rosemarie thread 0"
    assert record["items"][-1]["title"] == "Rosemarie thread 13"
    evidence_text = json.dumps(evidence)
    assert "Rosemarie thread 7" not in evidence_text


def test_cron_block_reason_rejects_raw_terminal_stdout_delivery() -> None:
    from nullion.cron_delivery import cron_delivery_block_reason

    stdout = (
        'traffic_views {\n'
        '  "count": 688,\n'
        '  "uniques": 38,\n'
        '  "views": [{"timestamp": "2026-05-28T00:00:00Z", "count": 12}]\n'
        "}"
    )
    result = {
        "text": f"STDOUT\n\n{stdout}",
        "tool_results": [
            {
                "tool_name": "terminal_exec",
                "status": "completed",
                "output": {"stdout": stdout, "stderr": "", "exit_code": 0},
            }
        ],
        "artifacts": [],
    }

    assert (
        cron_delivery_block_reason(result, str(result["text"]), result["artifacts"])
        == "cron_run_internal_tool_output_leaked"
    )


def test_cron_tool_handlers_cover_create_list_delete_toggle_and_run(tmp_path, monkeypatch,registered_cron_operator) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: crons.timezone.utc)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace-a")
    monkeypatch.setattr("nullion.artifacts.is_safe_artifact_path", lambda path: True)

    create = _build_create_cron_handler(default_delivery_channel="web", default_delivery_target="web:operator")
    assert create(invoke("create_cron", {})).error == "name, schedule and task are all required"
    created = create(
        invoke(
            "create_cron",
            {
                "name": "Morning Brief",
                "schedule": "*/5 * * * *",
                "task": "Summarize",
                "workspace_id": "workspace-a",
            },
        )
    )
    assert created.status == "completed"
    cron_id = created.output["id"]

    listed = _build_list_crons_handler()(invoke("list_crons", {}))
    assert listed.output["crons"][0]["name"] == "Morning Brief"
    assert listed.output["crons"][0]["selection_index"] == 1
    assert listed.output["crons"][0]["schedule_description"] == "Every 5 minutes (UTC)"
    assert "schedule" not in listed.output["crons"][0]
    assert "next_run" not in listed.output["crons"][0]
    assert "cron expressions" in listed.output["crons"][0]["presentation_hint"]
    assert "exactly one numbered options list" in listed.output["crons"][0]["presentation_hint"]
    assert "Schedule: Every 5 minutes (UTC)" in listed.output["message"]
    assert listed.output["message"].startswith("Here are your 1 crons:")
    assert "\n\n1. Morning Brief\n   Status: enabled\n   Schedule: Every 5 minutes (UTC)" in listed.output["message"]
    assert "Schedule: */5 * * * *" not in listed.output["message"]
    assert listed.output["crons"][0]["task"] == "Summarize"
    assert listed.output["crons"][0]["has_task"] is True
    assert "last_result" not in listed.output["crons"][0]
    assert listed.output["crons"][0]["has_last_result"] is False
    assert "task:" not in listed.output["message"]
    assert "Summarize" not in listed.output["message"]

    toggle = _build_toggle_cron_handler()
    assert toggle(invoke("toggle_cron", {})).error == "id, ids, or all_current_workspace is required"
    assert toggle(invoke("toggle_cron", {"id": "missing"})).error == "No cron found with id='missing'"
    disabled = toggle(invoke("toggle_cron", {"id": cron_id, "enabled": False}))
    assert disabled.output["enabled"] is False

    update = _build_update_cron_handler()
    assert update(invoke("update_cron", {})).error == "id is required"
    assert update(invoke("update_cron", {"id": "missing", "name": "Nope"})).error == "No cron found with id='missing'"
    renamed = update(
        invoke(
            "update_cron",
            {
                "id": cron_id,
                "name": "Morning Brief v2",
                "schedule": "*/15 * * * *",
                "task": "Summarize with changed instructions",
                "enabled": True,
                "delivery_channel": "web",
                "delivery_target": "web:operator",
            },
        )
    )
    assert renamed.status == "completed"
    assert renamed.output["name"] == "Morning Brief v2"
    assert renamed.output["schedule"] == "*/15 * * * *"
    assert renamed.output["enabled"] is True
    assert renamed.output["has_task"] is True
    assert "task" not in renamed.output

    artifact = tmp_path / "artifact.json"
    artifact.write_text("{}", encoding="utf-8")
    run = _build_run_cron_handler(lambda job: {"text": f"finished\nMEDIA:{artifact}", "artifact_paths": [{"path": str(artifact)}]})
    assert run(invoke("run_cron", {})).error == "id or name is required"
    by_name = run(invoke("run_cron", {"name": "morning brief"}))
    assert by_name.status == "completed"
    assert "task" not in by_name.output
    assert by_name.output["has_task"] is True
    assert by_name.output["result_text"] == "Cron ran successfully; no output was produced."
    assert by_name.output["foreground_auto_attach_created_artifacts"] is False
    assert by_name.output["foreground_media_directive_count"] == 1
    assert "artifact_paths" not in by_name.output
    assert "text" not in by_name.output["result"]
    assert "artifact_paths" not in by_name.output["result"]

    structured_result = _build_run_cron_handler(
        lambda job: {
            "text": f"prelude\n\nMEDIA:{artifact}\n\n後続の要約",
            "message": f"prelude\n\nMEDIA:{artifact}\n\n後続の要約",
            "artifact_paths": [{"path": str(artifact)}],
        }
    )(invoke("run_cron", {"id": cron_id}))
    assert structured_result.status == "completed"
    assert structured_result.output["result_text"] == "Cron ran successfully; no output was produced."
    assert structured_result.output["foreground_media_directive_count"] == 1
    assert "artifact_paths" not in structured_result.output
    assert "text" not in structured_result.output["result"]
    assert "message" not in structured_result.output["result"]
    assert "artifact_paths" not in structured_result.output["result"]

    state_artifact = tmp_path / "payment_reminder_tracker.json"
    state_artifact.write_text("{}", encoding="utf-8")
    activity_only = _build_run_cron_handler(
        lambda job: {
            "final_text": (
                f"state saved\n\nMEDIA:{state_artifact}\n\n"
                "Activity\n"
                "✓ Preparing request\n"
                "✓ Running model and tools\n"
                "✓ file_read — payment_reminder_tracker.json\n"
                "⊗ connector_request — URL can't contain control characters\n"
                "✓ file_write — payment_reminder_tracker.json\n"
                "✓ Writing response\n"
                "✨ Learned a new skill: Track payment reminders"
            ),
            "artifact_paths": [{"path": str(state_artifact)}],
        }
    )(invoke("run_cron", {"id": cron_id}))
    assert activity_only.status == "completed"
    assert activity_only.output["result_text"] == "Cron ran successfully; no output was produced."
    assert activity_only.output["foreground_media_directive_count"] == 1
    assert "artifact_paths" not in activity_only.output
    serialized_activity_only = json.dumps(activity_only.output, ensure_ascii=False)
    assert "MEDIA:" not in serialized_activity_only
    assert "payment_reminder_tracker" not in serialized_activity_only
    assert "state saved" not in serialized_activity_only
    assert "Activity" not in serialized_activity_only
    assert "Preparing request" not in serialized_activity_only
    assert "Writing response" not in serialized_activity_only
    assert "Learned a new skill" not in serialized_activity_only
    assert "URL can't contain" not in serialized_activity_only

    delivered = _build_run_cron_handler(
        lambda job: {
            "cron_delivery_status": "sent",
            "final_text": f"artifact caption\n\nMEDIA:{artifact}\n\ninternal activity",
            "artifacts": [{"path": str(artifact)}],
        }
    )(invoke("run_cron", {"id": cron_id}))
    assert delivered.status == "completed"
    assert "result_text" not in delivered.output
    assert delivered.output["foreground_auto_attach_created_artifacts"] is False
    assert delivered.output["foreground_reply_suppressed"] is True
    assert delivered.output["delivery_status"] == "sent"
    assert delivered.output["result"] == {"cron_delivery_status": "sent"}
    assert "foreground_media_directive_count" not in delivered.output

    background_owned = _build_run_cron_handler(
        lambda job: {
            "cron_delivery_status": "deferred",
            "text": "Manual scheduled task run started. The result will be delivered when ready.",
        }
    )(invoke("run_cron", {"id": cron_id}))
    assert background_owned.status == "completed"
    assert background_owned.output["delivery_status"] == "deferred"
    assert "mini_agent_dispatch" not in background_owned.output
    assert "planner_status_owned_by_background" not in background_owned.output
    assert str(background_owned.output["task_group_id"]).startswith("manual-cron-")
    assert "⏰ **MANUAL SCHEDULED TASK RUN" in str(background_owned.output["progress_status_text"])
    assert background_owned.output.get("foreground_reply_suppressed") is not True

    failed_run = _build_run_cron_handler(lambda job: {"reached_iteration_limit": True, "text": "stopped"})
    assert failed_run(invoke("run_cron", {"id": cron_id})).status == "failed"

    raw_payload_failed = _build_run_cron_handler(
        lambda job: {
            "cron_delivery_failed": True,
            "cron_run_failed": True,
            "raw_tool_payload_blocked": True,
            "reason": "cron_run_raw_tool_payload",
            "tool_results": [
                {
                    "tool_name": "connector_request",
                    "status": "completed",
                    "output": {"messages": [{"id": "msg-1", "subject": "Deadline"}]},
                }
            ],
            "text": "I checked connector data and received 1 structured item.",
        }
    )(invoke("run_cron", {"id": cron_id}))
    assert raw_payload_failed.status == "failed"
    assert raw_payload_failed.output["reason"] == "cron_run_raw_tool_payload"
    assert raw_payload_failed.output["result_text"] == (
        "Cron run was blocked because it produced raw structured tool output "
        "instead of a deliverable report."
    )
    assert "I checked connector data" not in raw_payload_failed.output["result_text"]
    assert "I checked connector data" not in str(raw_payload_failed.output["result"])
    assert raw_payload_failed.error == "Cron run produced raw structured tool output instead of a deliverable report."
    assert "tool_results" not in raw_payload_failed.output["result"]

    approval_paused = _build_run_cron_handler(
        lambda job: {
            "suspended_for_approval": True,
            "approval_id": "approval-cron-1",
            "cron_delivery_status": "paused_for_approval",
        }
    )(invoke("run_cron", {"id": cron_id}))
    assert approval_paused.status == "denied"
    assert approval_paused.output["reason"] == "approval_required"
    assert approval_paused.output["requires_approval"] is True
    assert approval_paused.output["approval_id"] == "approval-cron-1"
    assert approval_paused.output["result"]["approval_id"] == "approval-cron-1"
    assert approval_paused.error == "Approval required before the cron can continue."

    no_runner = _build_run_cron_handler(None)(invoke("run_cron", {"id": cron_id}))
    assert no_runner.error == "This runtime can list crons but cannot run them on demand."
    assert "task" not in no_runner.output

    second = create(invoke("create_cron", {"name": "Morning Brief Copy", "schedule": "*/10 * * * *", "task": "Summarize", "workspace_id": "workspace-a"}))
    bulk = run(invoke("run_cron", {"ids": [cron_id, second.output["id"]]}))
    assert bulk.status == "completed"
    assert bulk.output["started_count"] == 2
    assert bulk.output["failed_count"] == 0
    assert bulk.output["ids"] == [cron_id, second.output["id"]]
    assert [item["status"] for item in bulk.output["results"]] == ["completed", "completed"]
    assert [item["name"] for item in bulk.output["results"]] == ["Morning Brief v2", "Morning Brief Copy"]
    assert bulk.output["message"] == "Started 2 cron runs."

    enabled_bulk = run(invoke("run_cron", {"all_enabled": True}))
    assert enabled_bulk.status == "completed"
    assert enabled_bulk.output["all_enabled"] is True
    assert enabled_bulk.output["started_count"] == 2
    assert enabled_bulk.output["failed_count"] == 0
    assert enabled_bulk.output["ids"] == [cron_id, second.output["id"]]

    mixed_selector = run(invoke("run_cron", {"all_enabled": True, "name": "Morning Brief v2"}))
    assert mixed_selector.status == "failed"
    assert mixed_selector.error == "all_enabled cannot be combined with id, ids, or name"

    partial_bulk = run(invoke("run_cron", {"ids": [cron_id, "missing"]}))
    assert partial_bulk.status == "completed"
    assert partial_bulk.output["started_count"] == 1
    assert partial_bulk.output["failed_count"] == 1
    assert partial_bulk.output["results"][1]["error"] == "No cron found for 'missing'"

    ambiguous = run(invoke("run_cron", {"name": "morning"}))
    assert ambiguous.status == "failed"
    assert "Multiple crons matched" in ambiguous.error
    assert ambiguous.output["matches"][0]["selection_index"] == 1
    assert ambiguous.output["matches"][0]["reply_with"] == "1"
    assert "1. Morning Brief" in ambiguous.output["message"]

    delete = _build_delete_cron_handler()
    assert delete(invoke("delete_cron", {})).error == "id is required"
    assert delete(invoke("delete_cron", {"id": second.output["id"]})).status == "completed"
    assert delete(invoke("delete_cron", {"id": "missing"})).error == "No cron found with id='missing'"


def test_create_cron_updates_existing_same_delivery_name_instead_of_duplicating(tmp_path, monkeypatch,registered_cron_operator) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: crons.timezone.utc)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace-a")

    create = _build_create_cron_handler(default_delivery_channel="telegram", default_delivery_target="100001")
    first = create(
        invoke(
            "create_cron",
            {
                "name": "Delivery tracker",
                "schedule": "*/30 * * * *",
                "task": "Check delivery status.",
                "workspace_id": "workspace-a",
            },
        )
    )
    second = create(
        invoke(
            "create_cron",
            {
                "name": "Delivery tracker",
                "schedule": "0 */6 * * *",
                "task": "Check delivery status every six hours.",
                "workspace_id": "workspace-a",
            },
        )
    )

    jobs = crons.list_crons(workspace_id="workspace-a")
    assert first.status == "completed"
    assert second.status == "completed"
    assert second.output["reason"] == "duplicate_cron_create_resolved_as_update"
    assert second.output["created_new"] is False
    assert second.output["id"] == first.output["id"]
    assert second.output["schedule"] == "0 */6 * * *"
    assert second.output["changed_fields"] == ["schedule", "task"]
    assert len(jobs) == 1
    assert jobs[0].id == first.output["id"]
    assert jobs[0].schedule == "0 */6 * * *"


def test_create_cron_blocks_second_creation_in_same_capsule(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: crons.timezone.utc)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace-a")

    create = _build_create_cron_handler(default_delivery_channel="telegram", default_delivery_target="100001")
    first = create(
        ToolInvocation(
            "inv-create-1",
            "create_cron",
            "telegram_chat",
            {
                "name": "Delivery tracker",
                "schedule": "*/30 * * * *",
                "task": "Check delivery status.",
                "workspace_id": "workspace-a",
            },
            capsule_id="turn-create-once",
        )
    )
    second = create(
        ToolInvocation(
            "inv-create-2",
            "create_cron",
            "telegram_chat",
            {
                "name": "noop",
                "schedule": "0 0 31 2 *",
                "task": "noop",
                "workspace_id": "workspace-a",
            },
            capsule_id="turn-create-once",
        )
    )

    jobs = crons.list_crons(workspace_id="workspace-a")
    assert first.status == "completed"
    assert second.status == "failed"
    assert second.output["reason"] == "multiple_scheduler_creation_tools_in_turn"
    assert [job.name for job in jobs] == ["Delivery tracker"]


def test_run_cron_matches_diacritic_user_reference_against_enabled_job_task(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: crons.timezone.utc)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace-a")
    job = crons.add_cron(
        "Nullion social and GitHub traffic monitor",
        "0 12 * * *",
        "Track Nullion public awareness and audience metrics.",
        workspace_id="workspace-a",
    )
    ran: list[str] = []

    def runner(cron_job):
        ran.append(cron_job.id)
        return {"cron_delivery_status": "deferred"}

    result = _build_run_cron_handler(runner)(
        invoke("run_cron", {"name": "Trigger my Nulliøn tracker"})
    )

    assert result.status == "completed"
    assert ran == [job.id]
    assert "mini_agent_dispatch" not in result.output


def test_run_cron_resolves_unique_descriptive_reference_without_exact_name(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: crons.timezone.utc)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace-a")
    target = crons.add_cron(
        "Nullion social + GitHub traffic monitor   no empty graphs",
        "0 12,18,0 * * *",
        "Track Nullion public awareness and audience metrics.",
        workspace_id="workspace-a",
        enabled=True,
    )
    crons.add_cron(
        "Monitor Nullion PR readiness",
        "*/15 * * * *",
        "Check release pull request status.",
        workspace_id="workspace-a",
        enabled=False,
    )
    ran: list[str] = []

    def runner(cron_job):
        ran.append(cron_job.id)
        return {"cron_delivery_status": "deferred"}

    result = _build_run_cron_handler(runner)(
        invoke("run_cron", {"name": "Trigger my nullion cron"})
    )

    assert result.status == "completed"
    assert ran == [target.id]


def test_run_cron_resolves_small_name_typo_against_unique_cron(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: crons.timezone.utc)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace-a")
    target = crons.add_cron(
        "Provincetown Airbnb Monitor",
        "0 9 * * *",
        "Find Provincetown lodging and deliver a report.",
        workspace_id="workspace-a",
        enabled=True,
    )
    crons.add_cron(
        "Daily Post-Market Stock Report",
        "0 17 * * 1-5",
        "Create the post-market report.",
        workspace_id="workspace-a",
        enabled=True,
    )
    ran: list[str] = []

    def runner(cron_job):
        ran.append(cron_job.id)
        return {"cron_delivery_status": "deferred"}

    result = _build_run_cron_handler(runner)(
        invoke("run_cron", {"name": "trigger my provinctown cron"})
    )

    assert result.status == "completed"
    assert ran == [target.id]


def test_run_cron_returns_numbered_choices_for_ambiguous_descriptive_reference(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: crons.timezone.utc)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace-a")
    for name in (
        "Daily Pre-Market Stock Research",
        "Daily Post-Market Stock Report",
        "Mid-Market Stock Alert",
    ):
        crons.add_cron(
            name,
            "0 12 * * 1-5",
            "Review stock market updates.",
            workspace_id="workspace-a",
            enabled=True,
        )

    result = _build_run_cron_handler(lambda job: "ran")(invoke("run_cron", {"name": "stock cron"}))

    assert result.status == "failed"
    assert "choose by number" in result.error
    assert [match["selection_index"] for match in result.output["matches"]] == [1, 2, 3]
    assert "1. Daily Pre-Market Stock Research" in result.output["message"]
    assert result.output["matches"][0]["reply_with"] == "1"


def test_admin_cron_handlers_can_repair_cross_workspace_crons(tmp_path, monkeypatch,registered_cron_operator) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")

    misplaced = crons.add_cron(
        "Misplaced Nathan Cron",
        "0 8 * * *",
        "Check the misplaced cron",
        workspace_id="workspace_nathan",
        enabled=False,
    )
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")

    updated = _build_update_cron_handler()(
        invoke(
            "update_cron",
            {
                "id": misplaced.id,
                "workspace_id": "workspace_nate",
                "enabled": True,
            },
        )
    )

    assert updated.status == "completed"
    assert updated.output["workspace_id"] == "workspace_nate"
    assert updated.output["enabled"] is True
    assert updated.output["admin_cross_workspace"] is True
    assert crons.get_cron(misplaced.id).workspace_id == "workspace_nate"

    to_toggle = crons.add_cron(
        "Toggle Nathan Cron",
        "0 9 * * *",
        "Toggle misplaced cron",
        workspace_id="workspace_nathan",
        enabled=True,
    )
    toggled = _build_toggle_cron_handler()(invoke("toggle_cron", {"id": to_toggle.id, "enabled": False}))
    assert toggled.status == "completed"
    assert toggled.output["admin_cross_workspace"] is True
    assert crons.get_cron(to_toggle.id).enabled is False

    to_delete = crons.add_cron(
        "Delete Nathan Cron",
        "0 10 * * *",
        "Delete misplaced cron",
        workspace_id="workspace_nathan",
    )
    deleted = _build_delete_cron_handler()(invoke("delete_cron", {"id": to_delete.id}))
    assert deleted.status == "completed"
    assert deleted.output["admin_cross_workspace"] is True
    assert crons.get_cron(to_delete.id) is None


def test_member_cron_handlers_stay_workspace_scoped(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    cron = crons.add_cron(
        "Other Workspace Cron",
        "0 8 * * *",
        "Keep this scoped",
        workspace_id="workspace_nathan",
    )
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_nate")

    updated = _build_update_cron_handler()(invoke("update_cron", {"id": cron.id, "workspace_id": "workspace_nate"}))
    toggled = _build_toggle_cron_handler()(invoke("toggle_cron", {"id": cron.id, "enabled": False}))
    deleted = _build_delete_cron_handler()(invoke("delete_cron", {"id": cron.id}))

    assert updated.status == "failed"
    assert toggled.status == "failed"
    assert deleted.status == "failed"
    assert {updated.error, toggled.error, deleted.error} == {f"Cron {cron.id!r} belongs to workspace workspace_nathan."}
    assert crons.get_cron(cron.id).workspace_id == "workspace_nathan"


def test_create_cron_uses_structured_messaging_context_for_non_telegram_platforms(tmp_path, monkeypatch,registered_cron_operator) -> None:
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    create = _build_create_cron_handler(default_delivery_channel="telegram", default_delivery_target="100001")

    with reminder_chat_context("slack:C123"):
        created = create(invoke("create_cron", {
            "name": "Slack cron",
            "schedule": "0 8 * * *",
            "task": "Produce the structured report.",
        }))

    assert created.status == "completed"
    assert created.output["delivery_channel"] == "slack"
    assert created.output["delivery_target"] == "C123"
    saved = crons.get_cron(created.output["id"])
    assert saved is not None
    assert saved.delivery_channel == "slack"
    assert saved.delivery_target == "C123"


def test_telegram_manual_cron_runner_uses_structured_delivery_workflow() -> None:
    from nullion.cron_delivery import cron_delivery_block_reason, cron_structured_result_block_reason

    source = inspect.getsource(telegram_entrypoint._build_runtime_service_from_settings)
    block_source = inspect.getsource(cron_delivery_block_reason)
    structured_block_source = inspect.getsource(cron_structured_result_block_reason)

    assert "run_cron_delivery_workflow" in source
    assert "CronRunDeliveryCallbacks" in source
    assert "scheduled_task_delivery_text" in source
    assert "cron_delivery_block_reason" in source
    assert "cron_structured_result_block_reason" in block_source
    assert "raw_tool_payload_blocked" in structured_block_source
    assert "service.handle_text_message(\n            text=f\"[Manual scheduled task run:" not in source
    assert "message = f\"Scheduled task:" not in source


def test_web_runtime_builder_uses_config_settings_for_model_client() -> None:
    from nullion import web_app

    source = inspect.getsource(web_app._build_runtime)
    assert "app_settings = load_app_settings()" in source
    assert "build_model_client_from_settings(app_settings)" in source


@pytest.mark.parametrize(
    ("channel", "target", "env_lines"),
    [
        (
            "telegram",
            "100001",
            [
                "NULLION_TELEGRAM_BOT_TOKEN=123456:abcdefghijklmnopqrstuvwxyzABCDEFGHI",
                "NULLION_TELEGRAM_OPERATOR_CHAT_ID=100001",
                "NULLION_TELEGRAM_CHAT_ENABLED=true",
            ],
        ),
        (
            "slack",
            "C123",
            [
                "NULLION_SLACK_BOT_TOKEN=xoxb-test-token",
                "NULLION_SLACK_OPERATOR_USER_ID=U123",
            ],
        ),
        (
            "discord",
            "987654321",
            [
                "NULLION_DISCORD_BOT_TOKEN=discord-test-token",
                "NULLION_DISCORD_OPERATOR_CHANNEL_ID=987654321",
            ],
        ),
    ],
)
def test_messaging_runtime_manual_cron_blocks_raw_connector_payload_before_platform_send(
    tmp_path,
    monkeypatch,
    manual_cron_workers,
    channel: str,
    target: str,
    env_lines: list[str],
) -> None:
    checkpoint = tmp_path / "runtime.db"
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "NULLION_HOME=" + str(tmp_path),
                "NULLION_MODEL_PROVIDER=openai",
                "NULLION_OPENAI_API_KEY=test-key",
                *env_lines,
            ]
        ),
        encoding="utf-8",
    )
    env_path.chmod(0o600)
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", checkpoint)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    sent: list[str] = []
    async def telegram_send(*args, **kwargs):
        sent.append(str(args[2]))
        return True

    async def platform_send(**kwargs):
        sent.append(str(kwargs.get("text")))
        return True

    monkeypatch.setattr(telegram_entrypoint, "_send_operator_telegram_delivery", telegram_send)
    monkeypatch.setattr("nullion.slack_app.send_slack_platform_delivery", platform_send)
    monkeypatch.setattr("nullion.discord_app.send_discord_platform_delivery", platform_send)

    class FakeOrchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(
                final_text="I checked connector data and received 1 structured item.",
                tool_results=[
                    SimpleNamespace(
                        tool_name="connector_request",
                        status="completed",
                        output={"messages": [{"id": "msg-1", "subject": "Deadline"}]},
                        error=None,
                    )
                ],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=True,
            )

    def fake_service_factory(runtime, *, settings):  # noqa: ANN001
        return SimpleNamespace(runtime=runtime, settings=settings, agent_orchestrator=FakeOrchestrator())

    service = telegram_entrypoint._build_runtime_service_from_settings(
        checkpoint_path=checkpoint,
        env_path=env_path,
        service_factory=fake_service_factory,
    )
    job = crons.add_cron(
        "Email deadline triage -- Maton Gmail",
        "0 8,13,18 * * *",
        "Use connector_request and produce a concise triage report.",
        workspace_id="workspace_admin",
        delivery_channel=channel,
        delivery_target=target,
    )

    result = service.runtime.active_tool_registry.invoke(
        ToolInvocation("inv-run-cron", "run_cron", "telegram_chat", {"id": job.id})
    )

    assert result.status == "completed"
    assert result.output["delivery_status"] == "deferred"
    manual_cron_workers()
    assert len(sent) == 1
    assert "source checks did not produce a complete report" in sent[0]
    assert "msg-1" not in sent[0] and "Deadline" not in sent[0]
    assert "structured item" not in sent[0]


@pytest.mark.parametrize(
    ("channel", "target", "env_lines"),
    [
        ("slack", "C123", ["NULLION_SLACK_BOT_TOKEN=xoxb-test-token"]),
        ("discord", "987654321", ["NULLION_DISCORD_BOT_TOKEN=discord-test-token"]),
    ],
)
def test_messaging_runtime_manual_cron_can_send_non_telegram_platform_delivery(
    tmp_path,
    monkeypatch,
    manual_cron_workers,
    channel: str,
    target: str,
    env_lines: list[str],
) -> None:
    checkpoint = tmp_path / "runtime.db"
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "NULLION_HOME=" + str(tmp_path),
                "NULLION_MODEL_PROVIDER=openai",
                "NULLION_OPENAI_API_KEY=test-key",
                *env_lines,
            ]
        ),
        encoding="utf-8",
    )
    env_path.chmod(0o600)
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", checkpoint)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    sent: list[tuple[str, str]] = []

    async def fake_slack_delivery(**kwargs):  # noqa: ANN003
        sent.append(("slack", str(kwargs.get("text"))))
        return True

    async def fake_discord_delivery(**kwargs):  # noqa: ANN003
        sent.append(("discord", str(kwargs.get("text"))))
        return True

    monkeypatch.setattr("nullion.slack_app.send_slack_platform_delivery", fake_slack_delivery)
    monkeypatch.setattr("nullion.discord_app.send_discord_platform_delivery", fake_discord_delivery)

    class FakeOrchestrator:
        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(
                final_text="Here is the concise cron report.",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    def fake_service_factory(runtime, *, settings):  # noqa: ANN001
        return SimpleNamespace(runtime=runtime, settings=settings, agent_orchestrator=FakeOrchestrator())

    service = telegram_entrypoint._build_runtime_service_from_settings(
        checkpoint_path=checkpoint,
        env_path=env_path,
        service_factory=fake_service_factory,
    )
    job = crons.add_cron(
        "Platform delivery cron",
        "0 8 * * *",
        "Produce a concise report.",
        workspace_id="workspace_admin",
        delivery_channel=channel,
        delivery_target=target,
    )

    result = service.runtime.active_tool_registry.invoke(
        ToolInvocation("inv-run-cron", "run_cron", f"{channel}_chat", {"id": job.id})
    )

    assert result.status == "completed"
    assert result.output["delivery_status"] == "deferred"
    manual_cron_workers()
    assert len(sent) == 1
    assert sent[0][0] == channel
    assert "⏰ **MANUAL SCHEDULED TASK RUN: Platform delivery cron**" in sent[0][1]
    assert sent[0][1].count("Platform delivery cron") == 1
    assert "Here is the concise cron report." in sent[0][1]


def test_telegram_entrypoint_exits_without_polling_when_chat_disabled(tmp_path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "NULLION_TELEGRAM_CHAT_ENABLED=false",
                "NULLION_TELEGRAM_BOT_TOKEN=123456:abcdefghijklmnopqrstuvwxyzABCDEFGHI",
                "NULLION_TELEGRAM_OPERATOR_CHAT_ID=123",
            ]
        ),
        encoding="utf-8",
    )
    env_path.chmod(0o600)

    called = False

    def fail_if_called(**kwargs):  # noqa: ANN003
        nonlocal called
        called = True
        raise AssertionError("disabled Telegram should not build or poll a service")

    assert (
        telegram_entrypoint.main(
            checkpoint_path=tmp_path / "runtime.db",
            env_path=env_path,
            service_builder=fail_if_called,
        )
        is None
    )
    assert called is False
    assert not (tmp_path / "runtime.db").exists()


def test_telegram_browser_backend_prefers_saved_setting_over_stale_env(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "true")
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "auto")
    monkeypatch.setattr("nullion.auth.load_stored_credentials", lambda: {"browser_backend": "playwright"})

    assert telegram_entrypoint._resolve_browser_backend() == "playwright"


def test_telegram_browser_backend_uses_preferred_enabled_browser_plugin(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "true")
    monkeypatch.delenv("NULLION_BROWSER_BACKEND", raising=False)
    monkeypatch.delenv("NULLION_PLUGINS", raising=False)
    monkeypatch.setenv("NULLION_ENABLED_PLUGINS", "search_plugin,browser_plugin,workspace_plugin")
    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "brave")
    monkeypatch.setattr("nullion.auth.load_stored_credentials", lambda: {})

    assert telegram_entrypoint._resolve_browser_backend() == "auto"


def test_telegram_recovery_does_not_quarantine_valid_sqlite_checkpoint(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    store = RuntimeStore()
    store.add_audit_record(make_audit_record("existing.runtime.state", "test"))
    save_runtime_store(store, checkpoint)

    with pytest.raises(ValueError, match="Expecting value"):
        telegram_entrypoint._recover_runtime_from_corrupt_checkpoint(
            checkpoint,
            ValueError("Expecting value: line 1 column 1 (char 0)"),
        )

    assert checkpoint.exists()
    assert not list(tmp_path.glob("runtime.db.corrupt-*"))
    loaded = load_runtime_store(checkpoint)
    assert [record.action for record in loaded.list_audit_records()] == ["existing.runtime.state"]


def test_telegram_recovery_restores_good_backup_instead_of_empty_runtime(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    store = RuntimeStore()
    store.add_audit_record(make_audit_record("good.backup.state", "test"))
    save_runtime_store(store, checkpoint)
    checkpoint.with_name("runtime.db.bak").write_bytes(checkpoint.read_bytes())
    checkpoint.write_text("not sqlite", encoding="utf-8")

    runtime = telegram_entrypoint._recover_runtime_from_corrupt_checkpoint(
        checkpoint,
        ValueError("Invalid runtime store JSON"),
    )

    assert list(tmp_path.glob("runtime.db.corrupt-*"))
    assert [record.action for record in runtime.store.list_audit_records()] == ["good.backup.state"]
    loaded = load_runtime_store(checkpoint)
    assert [record.action for record in loaded.list_audit_records()] == ["good.backup.state"]


def test_telegram_recovery_without_good_backup_starts_empty_runtime(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    checkpoint.write_text("not sqlite", encoding="utf-8")

    runtime = telegram_entrypoint._recover_runtime_from_corrupt_checkpoint(
        checkpoint,
        ValueError("Invalid runtime store JSON"),
    )

    assert list(tmp_path.glob("runtime.db.corrupt-*"))
    assert checkpoint.exists()
    assert runtime.store.list_audit_records() == []


def test_web_cron_delivery_uses_shared_platform_header() -> None:
    from nullion import web_app

    source = inspect.getsource(web_app.create_app)

    assert "scheduled_task_delivery_text" in source
    assert "message = f\"Scheduled task:" not in source


def test_telegram_cron_delivery_uses_shared_single_agent_runner() -> None:
    source = inspect.getsource(telegram_entrypoint._build_runtime_service_from_settings)

    assert "run_single_agent_cron_turn" in source
    assert "start_manual_cron_background_delivery" in source
    assert "dispatch_request_sync" not in source
    assert "cron_deep_agent_dispatch_plan" not in source
    assert "NULLION_CRON_SINGLE_TASK_FAST_PATH_ENABLED" not in source


def test_web_cron_delivery_uses_shared_manual_background_runner() -> None:
    from nullion import web_app

    source = inspect.getsource(web_app.create_app)

    assert "run_single_agent_cron_turn" in source
    assert "start_manual_cron_background_delivery" in source
    assert "cron_deep_agent_dispatch_plan" not in source
    assert "NULLION_CRON_SINGLE_TASK_FAST_PATH_ENABLED" not in source


def test_provider_helpers_search_media_and_custom_api(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("NULLION_OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("NULLION_MEDIA_OPENAI_API_KEY", "media-key")
    assert providers._provider_key_for_media("openai") == "media-key"
    monkeypatch.delenv("NULLION_MEDIA_OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "provider-key")
    assert providers._provider_key_for_media("openai") == "provider-key"
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_ENABLED", "off")
    assert providers._media_model_selection("NULLION_AUDIO_TRANSCRIBE_PROVIDER", "NULLION_AUDIO_TRANSCRIBE_MODEL", "NULLION_AUDIO_TRANSCRIBE_ENABLED") is None
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_ENABLED", "true")
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_PROVIDER", "openai")
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_MODEL", "whisper")
    assert providers._media_model_selection("NULLION_AUDIO_TRANSCRIBE_PROVIDER", "NULLION_AUDIO_TRANSCRIBE_MODEL", "NULLION_AUDIO_TRANSCRIBE_ENABLED") == ("openai", "whisper")

    monkeypatch.setattr(providers.shutil, "which", lambda name: f"/usr/bin/{name}" if name == "tool" else None)
    assert providers._which_local_tool("tool") == "tool"
    assert providers._first_env("MISSING", "OPENAI_API_KEY") == "provider-key"
    with pytest.raises(RuntimeError):
        providers._require_env("MISSING", provider_name="demo")
    assert providers._clamped_limit(99, max_limit=3) == 3
    assert providers._result(" Title ", " https://example.com ", " Snip ") == {"title": "Title", "url": "https://example.com", "snippet": "Snip"}
    assert providers._result("", "url") is None

    payloads: list[tuple[str, dict | None]] = []

    def fake_request_json(url, **kwargs):
        payloads.append((url, kwargs.get("payload")))
        if "brave" in url:
            return {"web": {"results": [{"title": "One", "url": "https://one", "description": "desc"}, "bad"]}}
        if "customsearch" in url:
            return {"items": [{"title": "Two", "link": "https://two", "snippet": "desc"}]}
        if "perplexity" in url:
            return {"results": [{"title": "Three", "url": "https://three", "snippet": "desc", "date": "2026-01-01"}]}
        if "duckduckgo" in url:
            return {
                "Heading": "Duck",
                "AbstractURL": "https://duck",
                "AbstractText": "summary",
                "RelatedTopics": [{"Topics": [{"Text": "Nested", "FirstURL": "https://nested"}]}],
            }
        return {"results": [{"id": "m1"}], "message": {"id": "m1"}}

    monkeypatch.setattr(providers, "_request_json", fake_request_json)
    monkeypatch.setenv("NULLION_BRAVE_SEARCH_API_KEY", "brave")
    monkeypatch.setenv("NULLION_GOOGLE_SEARCH_API_KEY", "google")
    monkeypatch.setenv("NULLION_GOOGLE_SEARCH_CX", "cx")
    monkeypatch.setenv("NULLION_PERPLEXITY_API_KEY", "perp")
    assert providers._brave_web_search("q", 2)[0]["title"] == "One"
    assert providers._google_custom_search("q", 2)[0]["title"] == "Two"
    assert providers._perplexity_search("q", 2)[0]["date"] == "2026-01-01"
    assert [r["title"] for r in providers._duckduckgo_instant_answer_search("q", 3)] == ["Duck", "Nested"]

    monkeypatch.setattr(providers.subprocess, "run", lambda argv, **kwargs: SimpleNamespace(returncode=0, stdout="generated text\n", stderr=""))
    monkeypatch.setattr(providers.shutil, "which", lambda name: name)
    assert providers._run_media_command_template("echo {input}", substitutions={"input": "hello"}).stdout == "generated text\n"
    with pytest.raises(RuntimeError, match="missing placeholder"):
        providers._run_media_command_template("echo {missing}", substitutions={})
    with pytest.raises(RuntimeError, match="empty"):
        providers._run_media_command_template("   ", substitutions={})
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_COMMAND", "echo {input}")
    assert providers._local_audio_transcribe(str(tmp_path / "audio.wav"), "en")["text"] == "generated text"
    monkeypatch.setenv("NULLION_IMAGE_OCR_COMMAND", "echo {input}")
    assert providers._local_image_extract_text(str(tmp_path / "image.png"))["text"] == "generated text"

    out = tmp_path / "image.png"
    monkeypatch.setattr(providers, "messaging_media_scratch_root", lambda: tmp_path)
    monkeypatch.setattr(providers, "_run_media_command_template", lambda *args, **kwargs: out.write_bytes(b"png") or SimpleNamespace(stdout=""))
    assert providers._command_image_generate("make {output}", "draw", str(out), "1024x1024")["path"] == str(out)
    providers._write_image_url_to_path("data:image/png;base64," + base64.b64encode(b"img").decode(), tmp_path / "data.png")
    assert (tmp_path / "data.png").read_bytes() == b"img"
    assert providers._openrouter_image_config("1200x800") == {"aspect_ratio": "3:2"}
    assert providers._gemini_model_name("models/gemini") == "gemini"

    monkeypatch.setenv("NULLION_CUSTOM_API_TOKEN", "token")
    assert providers._custom_api_base_url(SimpleNamespace(provider_profile="https://api.example.com/root")) == "https://api.example.com/root"
    assert providers._custom_api_headers(None)["Authorization"] == "Bearer token"
    monkeypatch.setattr(providers, "_custom_api_connection", lambda principal_id: SimpleNamespace(provider_profile="https://api.example.com", credential_ref="NULLION_CUSTOM_API_TOKEN"))
    assert providers._custom_api_email_search("hi", 2)[0]["id"] == "m1"
    assert providers._custom_api_email_read("m1")["id"] == "m1"

    assert "web_searcher" in providers.resolve_plugin_provider_kwargs(plugin_name="search_plugin", provider_name="duckduckgo_search_provider")
    assert "image_generator" in providers.resolve_plugin_provider_kwargs(plugin_name="media_plugin", provider_name="local_media_provider")
    with pytest.raises(ValueError):
        providers.resolve_plugin_provider_kwargs(plugin_name="missing", provider_name="x")


def test_updater_helpers_rollback_and_health_checks(tmp_path, monkeypatch) -> None:
    install = tmp_path / "install"
    src = tmp_path / "src"
    src.mkdir()
    (src / ".git").mkdir()
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    monkeypatch.setenv("NULLION_SRC_DIR", str(src))
    assert updater._install_dir() == install
    assert updater._src_dir() == src
    assert updater._backup_dir().exists()
    assert updater._version_label("bad", cwd=src) == "bad"

    git_calls: list[tuple[str, ...]] = []

    def fake_git(*args, cwd=None):
        git_calls.append(args)
        if args[:2] == ("rev-parse", "--verify"):
            return 0, "a" * 40
        if args[:2] == ("reset", "--hard"):
            return 0, "reset"
        if args[0] == "merge-base":
            return 1, ""
        if args[0] == "describe":
            return 1, ""
        if args[0] == "rev-parse":
            return 0, "abc1234"
        return 0, ""

    monkeypatch.setattr(updater, "_git", fake_git)
    assert updater._commit_relation("a", "a") == "current"
    assert updater._commit_relation("a", "b") == "diverged"
    assert updater._validate_commit_hash("abc1234") == "a" * 40
    with pytest.raises(ValueError):
        updater._validate_commit_hash("not a hash")
    updater._validate_requirements_content("requests==1\n--index-url https://example.com\n")
    with pytest.raises(ValueError):
        updater._validate_requirements_content("; rm -rf /\n")
    assert updater._requirements_without_editable_self(
        "-e git+file:///tmp/nullion-work/nullion-src@abc1234#egg=nullion\nrequests==1\n"
    ) == "requests==1\n"
    assert updater._requirements_without_editable_self(
        "-e /tmp/source-checkout#egg=nullion\nrequests==1\n"
    ) == "requests==1\n"
    assert updater._requirements_without_editable_self(
        "nullion @ file:///tmp/nullion-work/nullion-src\n"
        "nullion @ git+file:///tmp/nullion-work/nullion-src@abc1234\n"
        "nullion==0.0.1\n"
        "not-nullion==1\n"
        "requests==1\n"
    ) == "not-nullion==1\nrequests==1\n"

    restore_snap = tmp_path / "restore-snap"
    restore_snap.mkdir()
    restore_req = restore_snap / "requirements.txt"
    restore_req.write_text(
        "-e git+file:///tmp/nullion-work/nullion-src@abc1234#egg=nullion\n"
        "requests==1\n",
        encoding="utf-8",
    )
    restore_calls: list[list[str]] = []

    def capture_restore_install(cmd, **kwargs):
        restore_calls.append(list(cmd))
        req_index = list(cmd).index("-r") + 1
        assert Path(cmd[req_index]).read_text(encoding="utf-8") == "requests==1\n"
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(updater.subprocess, "run", capture_restore_install)
    assert updater._restore_snapshot_requirements(restore_req).returncode == 0
    assert restore_calls

    snap = tmp_path / "snap"
    snap.mkdir()
    (snap / "git_commit.txt").write_text("abc1234", encoding="utf-8")
    (snap / "requirements.txt").write_text("requests==1\n", encoding="utf-8")
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout='{"status":"ok"}', stderr=""))
    assert updater.rollback(snap) is True

    locked_snap = tmp_path / "locked-snap"
    locked_snap.mkdir()
    (locked_snap / "git_commit.txt").write_text("abc1234", encoding="utf-8")
    (locked_snap / "requirements.txt").write_text(
        f"-e {src}#egg=nullion\n",
        encoding="utf-8",
    )
    locked_error = (
        "ERROR: Could not install packages due to an OSError: [WinError 32] "
        "The process cannot access the file because it is being used by another process: "
        "'C:\\Users\\ContainerAdministrator\\.nullion\\venv\\Scripts\\nullion.exe' -> "
        "'C:\\Users\\ContainerAdministrator\\.nullion\\venv\\Scripts\\nullion.exe.deleteme'"
    )
    monkeypatch.setattr(
        updater.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr=locked_error),
    )
    monkeypatch.setattr(updater, "_is_windows_locked_entrypoint_restore_error", lambda stderr: stderr == locked_error)
    monkeypatch.setattr(updater, "_editable_source_imports", lambda restored_src: restored_src == src)
    assert updater.rollback(locked_snap) is True

    dependency_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    (src / "pyproject.toml").write_text(
        '[project]\ndependencies = ["requests>=2", "httpx>=0.27"]\n',
        encoding="utf-8",
    )
    (src / "src" / "nullion").mkdir(parents=True, exist_ok=True)

    def capture_dependency_install(*args, **kwargs):
        if "-c" in args[0]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        dependency_calls.append((args, kwargs))
        req_index = tuple(args[0]).index("-r") + 1
        assert Path(args[0][req_index]).read_text(encoding="utf-8") == "requests>=2\nhttpx>=0.27\n"
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(updater, "_is_windows", lambda: True)
    monkeypatch.setattr(updater, "_pip_command", lambda: ["python", "-m", "pip"])
    monkeypatch.setattr(updater.subprocess, "run", capture_dependency_install)
    assert updater._install_updated_dependencies(src).returncode == 0
    install_cmd = tuple(dependency_calls[-1][0][0])
    assert install_cmd[:5] == (
        "python",
        "-m",
        "pip",
        "install",
        "--quiet",
    )
    assert "-r" in install_cmd
    assert "--only-deps" not in install_cmd
    assert "-e" not in install_cmd

    site_packages = tmp_path / "site-packages"
    import_checks: list[list[object]] = []

    def capture_dependency_install_and_import_check(cmd, **kwargs):
        if "-c" in cmd:
            import_checks.append(list(cmd))
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        dependency_calls.append((tuple(cmd), kwargs))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(updater, "_site_packages_dir", lambda: site_packages)
    monkeypatch.setattr(updater.subprocess, "run", capture_dependency_install_and_import_check)
    assert updater._install_updated_dependencies(src).returncode == 0
    assert (site_packages / "nullion-editable-src.pth").read_text(encoding="utf-8") == str((src / "src").resolve()) + "\n"
    assert import_checks and "import nullion" in import_checks[-1][2]

    locked_install_error = (
        "WARNING: Ignoring invalid distribution ~ullion\n"
        "ERROR: Could not install packages due to an OSError: [WinError 32] "
        "The process cannot access the file because it is being used by another process: "
        "'C:\\Users\\shash\\.nullion\\venv\\Scripts\\nullion.exe'"
    )

    def locked_dependency_install_then_import_check(cmd, **kwargs):
        if "-c" in cmd:
            import_checks.append(list(cmd))
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr=locked_install_error)

    monkeypatch.setattr(updater, "_is_windows_locked_entrypoint_restore_error", lambda stderr: "WinError 32" in stderr)
    monkeypatch.setattr(updater.subprocess, "run", locked_dependency_install_then_import_check)
    repaired_locked_install = updater._install_updated_dependencies(src)
    assert repaired_locked_install.returncode == 0
    assert repaired_locked_install.stderr == locked_install_error

    def fail_import_check(cmd, **kwargs):
        if "-c" in cmd:
            return SimpleNamespace(returncode=1, stdout="", stderr="ModuleNotFoundError: No module named 'nullion'")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(updater.subprocess, "run", fail_import_check)
    failed_repair = updater._install_updated_dependencies(src)
    assert failed_repair.returncode == 1
    assert "Nullion import check failed after editable path repair" in failed_repair.stderr

    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr="bad"))
    assert updater._probe_messaging_platform_bootstrap(env_path=None, checkpoint_path=tmp_path / "runtime.json") is False
    warnings: list[str] = []
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="PROVIDER_WARNING: missing\nok\n", stderr=""))
    monkeypatch.setattr(updater, "_probe_web_status", lambda **kwargs: True)
    monkeypatch.setattr(updater, "_probe_messaging_platform_bootstrap", lambda **kwargs: True)
    assert updater.health_check(_warnings_out=warnings) is True
    assert warnings == ["missing"]
    monkeypatch.setattr(updater, "_probe_web_status", lambda **kwargs: False)
    assert updater.health_check(require_messaging=False) is False

    fresh_warnings: list[str] = []
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="PROVIDER_WARNING: fresh\n", stderr=""))
    assert updater.fresh_health_check(_warnings_out=fresh_warnings) is True
    assert fresh_warnings == ["fresh"]
    monkeypatch.setattr(updater.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="no", stderr=""))
    assert updater.fresh_health_check() is False


def test_post_update_migrations_import_credentials_and_runtime_json(tmp_path, monkeypatch) -> None:
    install = tmp_path / "install"
    install.mkdir()
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    env_path = install / ".env"
    env_path.write_text('NULLION_KEY_STORAGE="local"\n', encoding="utf-8")
    env_path.chmod(0o600)
    credentials_json = install / "credentials.json"
    credentials_json.write_text(json.dumps({"provider": "openai", "api_key": "sk-secret"}), encoding="utf-8")

    checkpoint = install / "runtime.db"
    legacy = RuntimeStore()
    approval = create_approval_request(
        requested_by="telegram_chat",
        action="allow_boundary",
        resource="https://www.cnn.com/*",
        request_kind="boundary_policy",
    )
    legacy.add_approval_request(approval)
    save_runtime_store(legacy, install / "runtime-store.json")

    result = updater.run_post_update_migrations(env_path=env_path, checkpoint_path=checkpoint, install_dir=install)

    assert result["credentials_imported"] is True
    assert result["credentials_json_deleted"] is True
    assert result["runtime_imported"] is True
    assert load_encrypted_credentials(db_path=checkpoint)["api_key"] == "sk-secret"
    assert load_runtime_store(checkpoint).get_approval_request(approval.approval_id) is not None


def test_post_update_migrations_import_env_credentials_when_db_is_empty(tmp_path, monkeypatch) -> None:
    install = tmp_path / "install"
    install.mkdir()
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    env_path = install / ".env"
    env_path.write_text(
        "\n".join(
            [
                'NULLION_KEY_STORAGE="local"',
                'NULLION_MODEL_PROVIDER="openrouter"',
                'NULLION_MODEL="openai/gpt-4o"',
                'OPENROUTER_API_KEY="sk-or-secret"',
            ]
        ),
        encoding="utf-8",
    )
    env_path.chmod(0o600)

    checkpoint = install / "runtime.db"
    result = updater.run_post_update_migrations(env_path=env_path, checkpoint_path=checkpoint, install_dir=install)

    assert result["env_credentials_imported"] is True
    stored = load_encrypted_credentials(db_path=checkpoint)
    assert stored["provider"] == "openrouter"
    assert stored["api_key"] == "sk-or-secret"
    assert stored["model"] == "openai/gpt-4o"


def test_post_update_migrations_encrypts_plaintext_memory_and_warns_on_bad_rows(tmp_path, monkeypatch) -> None:
    install = tmp_path / "install"
    install.mkdir()
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    env_path = install / ".env"
    env_path.write_text('NULLION_KEY_STORAGE="local"\n', encoding="utf-8")
    env_path.chmod(0o600)

    checkpoint = install / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)
    now = "2026-01-01T00:00:00+00:00"
    valid_memory = json.dumps(
        {
            "entry_id": "workspace:ops:watcher_scope",
            "owner": "workspace:ops",
            "kind": "fact",
            "key": "watcher_scope",
            "value": "Legacy watcher scope must become encrypted.",
            "source": "legacy",
            "created_at": now,
            "updated_at": now,
        },
        sort_keys=True,
    )
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO memory (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("user_facts", "workspace:ops:watcher_scope", valid_memory, now),
        )
        conn.execute(
            "INSERT INTO memory (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("preferences", "workspace:ops:broken_memory", "{bad json", now),
        )

    result = updater.run_post_update_migrations(env_path=env_path, checkpoint_path=checkpoint, install_dir=install)

    assert result["memory_migrated"] is True
    assert result["memory_rows_encrypted"] == 1
    assert result["memory_rows_quarantined"] == 1
    assert any("old memory row" in warning and "deleting old memories" in warning for warning in result["warnings"])
    raw_db = checkpoint.read_bytes()
    assert b"Legacy watcher scope must become encrypted" not in raw_db
    assert b"workspace:ops:watcher_scope" not in raw_db
    assert b"workspace:ops:broken_memory" not in raw_db
    [memory] = load_runtime_store(checkpoint).list_user_memory_entries(kind=UserMemoryKind.FACT)
    assert memory.value == "Legacy watcher scope must become encrypted."
    with sqlite3.connect(checkpoint) as conn:
        quarantined = conn.execute("SELECT original_key, payload FROM memory_migration_failures").fetchone()
    assert quarantined[0].startswith("row:")
    assert str(quarantined[1]).startswith("fernet:v1:")


def test_post_update_migrations_preserve_legacy_sqlite_runtime_data_models(tmp_path, monkeypatch) -> None:
    install = tmp_path / "install"
    install.mkdir()
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    env_path = install / ".env"
    env_path.write_text('NULLION_KEY_STORAGE="local"\n', encoding="utf-8")
    checkpoint = create_legacy_runtime_sqlite_db(install / "runtime.db")

    result = updater.run_post_update_migrations(env_path=env_path, checkpoint_path=checkpoint, install_dir=install)

    assert result["memory_migrated"] is True
    assert result["memory_rows_encrypted"] == 3
    assert result["memory_rows_quarantined"] == 0
    assert result["runtime_imported"] is False
    assert result["warnings"] == []

    loaded = load_runtime_store(checkpoint)
    assert loaded.get_approval_request(LEGACY_APPROVAL_ID).resource == "https://api.example.test/*"
    assert loaded.get_permission_grant(LEGACY_GRANT_ID).permission == "tool:web_fetch"
    assert loaded.get_conversation_head(LEGACY_CONVERSATION_ID) == {
        "active_branch_id": LEGACY_BRANCH_ID,
        "active_turn_id": LEGACY_TURN_ID,
    }
    assert loaded.get_task_frame(LEGACY_FRAME_ID).metadata["fixture"] == "legacy-sqlite"
    assert loaded.get_mission(LEGACY_MISSION_ID).active_step_id == "step-compat"
    assert loaded.user_facts["mem-fact"].value == "beta"
    assert loaded.preferences["mem-pref"].value == "dark"
    assert loaded.environment_facts["mem-env"].value == "linux"

    loaded.add_conversation_event({"conversation_id": LEGACY_CONVERSATION_ID, "event_type": "compat.after_update"})
    save_runtime_store(loaded, checkpoint)
    reloaded = load_runtime_store(checkpoint)
    assert reloaded.get_approval_request(LEGACY_APPROVAL_ID).status.value == "pending"
    assert [event["event_type"] for event in reloaded.list_conversation_events()].count("compat.after_update") == 1

    with sqlite3.connect(checkpoint) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"runtime_meta", "tasks_missions", "memory", "conversation_events"} <= tables
        encrypted_memory_rows = conn.execute(
            "SELECT COUNT(*) FROM memory WHERE collection IN ('user_facts', 'preferences', 'environment_facts')"
        ).fetchone()[0]
        assert encrypted_memory_rows == 3


def test_run_update_workflow_handles_no_update_success_and_rollback(tmp_path, monkeypatch) -> None:
    emitted: list[updater.UpdateProgress] = []
    target = updater.UpdateTarget(channel="release", ref="v2", commit="new1234", label="v2")

    monkeypatch.setattr(updater, "_current_commit", lambda: "old1234")
    monkeypatch.setattr(updater, "_update_target", lambda channel: target)
    monkeypatch.setattr(updater, "_version_label", lambda ref="HEAD": "v1" if ref == "HEAD" else ref)
    monkeypatch.setattr(updater, "_commit_relation", lambda local, remote: "current")

    no_update = asyncio.run(updater.run_update(emit=emitted.append))

    assert no_update.success is True
    assert no_update.from_version == "v1"
    assert no_update.to_version == "v1"
    assert [step.step for step in emitted] == ["fetch", "fetch"]

    emitted.clear()
    snapshot_path = tmp_path / "snapshot"
    snapshot_path.mkdir()
    git_calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(updater, "_commit_relation", lambda local, remote: "behind")
    monkeypatch.setattr(updater, "_probe_web_status", lambda **kwargs: True)
    monkeypatch.setattr(updater, "snapshot", lambda **kwargs: snapshot_path)
    monkeypatch.setattr(updater, "_git", lambda *args, **kwargs: git_calls.append(args) or (0, "ok"))
    monkeypatch.setattr(updater, "_src_dir", lambda: tmp_path)
    subprocess_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_subprocess_run(*args, **kwargs):
        subprocess_calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(updater.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(updater, "fresh_post_update_migrations", lambda **kwargs: {"warnings": []})
    monkeypatch.setattr(updater, "fresh_health_check", lambda **kwargs: True)

    updated = asyncio.run(updater.run_update(emit=emitted.append))

    assert updated.success is True
    assert updated.from_version == "v1"
    assert updated.to_version == "v2"
    assert updated.snapshot_path == str(snapshot_path)
    assert ("reset", "--hard", "v2") in git_calls
    assert ("clean", "-fd") in git_calls
    assert any(
        tuple(call_args[0]) == (sys.executable, "-m", "pip", "install", "--quiet", "-e", str(tmp_path))
        and call_kwargs.get("cwd") == tmp_path
        for call_args, call_kwargs in subprocess_calls
    )
    emitted_steps = [step.step for step in emitted]
    assert "migrate" in emitted_steps
    assert emitted_steps[-1] == "done"

    emitted.clear()
    rolled_back: list[Path | None] = []
    monkeypatch.setattr(updater, "fresh_health_check", lambda **kwargs: False)
    monkeypatch.setattr(updater, "rollback", lambda snap: rolled_back.append(snap) or True)

    failed = asyncio.run(updater.run_update(emit=emitted.append))

    assert failed.success is False
    assert failed.rolled_back is True
    assert failed.error == "Health check failed after update."
    assert rolled_back == [snapshot_path]
    assert [step.step for step in emitted][-2:] == ["health", "rollback"]


def test_webview_helpers_single_instance_sizing_and_cli(tmp_path, monkeypatch) -> None:
    pid_file = tmp_path / "webview.pid"
    monkeypatch.setattr(webview_app, "_WEBVIEW_PID_FILE", pid_file)
    monkeypatch.setattr(webview_app, "_NULLION_HOME", tmp_path)
    assert webview_app._pid_is_running(-1) is False
    monkeypatch.setattr(webview_app.os, "kill", lambda pid, sig: None)
    assert webview_app._pid_is_running(123) is True
    monkeypatch.setattr(webview_app, "_focus_process", lambda pid: True)
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text("123", encoding="utf-8")
    assert webview_app._claim_single_instance() is False
    monkeypatch.setattr(webview_app, "_focus_process", lambda pid: False)
    assert webview_app._claim_single_instance() is True

    webview = SimpleNamespace(screens=[SimpleNamespace(width=3000, height=1600)])
    assert webview_app._resolve_window_size(webview) == (1920, 1200)
    monkeypatch.setenv("NULLION_WEBVIEW_WIDTH", "1000")
    monkeypatch.setenv("NULLION_WEBVIEW_HEIGHT", "700")
    assert webview_app._resolve_window_size(SimpleNamespace(screens=[])) == (1000, 700)
    monkeypatch.setenv("NULLION_WEBVIEW_WIDTH", "bad")
    assert webview_app._resolve_window_size(SimpleNamespace(screens=[])) == (1440, 900)

    icon_path = tmp_path / "icon.png"
    monkeypatch.setattr(webview_app, "_claim_single_instance", lambda: True)
    monkeypatch.setattr(webview_app, "_webview_icon_path", lambda: str(icon_path))
    monkeypatch.setattr(webview_app, "_set_macos_app_icon", lambda path: None)
    events: list[tuple] = []
    fake_webview = SimpleNamespace(
        screens=[SimpleNamespace(width=1400, height=900)],
        create_window=lambda *args, **kwargs: events.append(("window", args, kwargs)),
        start=lambda **kwargs: events.append(("start", kwargs)),
    )
    webview_app._run_webview(SimpleNamespace(width=800, height=600, debug=True), fake_webview, "http://localhost")
    assert events[0][2]["width"] == 800
    assert "js_api" in events[0][2]
    loading_html = events[0][2]["html"]
    assert "Restart services" in loading_html
    assert "fetch(healthUrl" in loading_html
    assert 'id="version-tag"' in loading_html
    assert "Version ${version}" in loading_html
    assert "Local app is not responding" in loading_html
    assert "Startup appears stuck." in loading_html
    assert "Restart failed" in loading_html
    assert "window.pywebview.api.restart_services" in loading_html
    assert "api.health" in loading_html
    assert "window.location.replace(target)" in loading_html
    assert "Connecting to the local app" not in loading_html
    assert "position: fixed; right: 28px; bottom: 24px" in loading_html
    assert events[-1][1]["debug"] is True

    opened: list[str] = []
    monkeypatch.setattr(webview_app, "_load_env", lambda env_file: None)
    monkeypatch.setattr(webview_app.webbrowser, "open", lambda url: opened.append(url))
    monkeypatch.setattr(sys, "argv", ["nullion-webview", "--browser-fallback", "--path", "approvals"])
    monkeypatch.setitem(sys.modules, "webview", None)
    webview_app._cli_impl()
    assert opened[-1].endswith("/approvals")


def test_webview_pid_file_uses_configured_nullion_home(tmp_path, monkeypatch) -> None:
    lane_home = tmp_path / "stage-home"
    monkeypatch.setenv("NULLION_HOME", str(lane_home))
    spec = importlib.util.spec_from_file_location("webview_app_lane_home", webview_app.__file__)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    assert module._NULLION_HOME == lane_home
    assert module._WEBVIEW_PID_FILE == lane_home / "webview.pid"


def test_webview_loading_restart_api_restarts_all_managed_services(monkeypatch) -> None:
    from nullion import service_control

    calls: list[dict[str, object]] = []

    def fake_restart_managed_services(**kwargs):
        calls.append(kwargs)
        return [
            SimpleNamespace(service="web", ok=True, message="restarted web"),
            SimpleNamespace(service="tray", ok=True, message="restarted tray"),
            SimpleNamespace(service="telegram", ok=False, message="not installed"),
        ]

    monkeypatch.setattr(service_control, "restart_managed_services", fake_restart_managed_services)

    payload = webview_app._LoadingScreenApi().restart_services()

    assert calls == [{"continue_on_error": True}]
    assert payload["ok"] is True
    assert "2 installed services" in payload["message"]
    assert payload["results"][0]["service"] == "web"


def test_webview_loading_health_api_reports_failure(monkeypatch) -> None:
    requested_urls: list[str] = []

    def fake_urlopen(*args, **kwargs):
        requested_urls.append(args[0].full_url)
        raise webview_app.URLError("connection refused")

    monkeypatch.setattr(webview_app, "urlopen", fake_urlopen)

    payload = webview_app._LoadingScreenApi("http://127.0.0.1:8753/approvals").health()

    assert requested_urls == ["http://127.0.0.1:8753/api/health"]
    assert payload["ok"] is False
    assert payload["status"] == 0
    assert "Cannot reach the local app" in payload["message"]
