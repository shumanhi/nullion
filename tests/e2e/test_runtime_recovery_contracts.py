from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run runtime recovery e2e tests",
)


def _audit_actions(checkpoint: Path) -> list[str]:
    from nullion.runtime_persistence import load_runtime_store

    return [record.action for record in load_runtime_store(checkpoint).list_audit_records()]


def test_runtime_recovery_preserves_valid_data_quarantines_bad_rows_and_keeps_crons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import crons
    from nullion.audit import make_audit_record
    from nullion.crons import add_cron, load_crons
    from nullion.runtime_persistence import load_runtime_store, save_runtime_store
    from nullion.runtime_store import RuntimeStore

    checkpoint = tmp_path / "runtime.db"
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", checkpoint)

    store = RuntimeStore()
    store.add_audit_record(make_audit_record("valid.runtime.record", "e2e"))
    save_runtime_store(store, checkpoint)

    cron = add_cron(
        "Recovered DB Cron",
        "0 9 * * *",
        "Run the recovery contract",
        workspace_id="workspace_recovery",
        delivery_channel="telegram",
        delivery_target="100001",
    )

    bad_approval = {
        "approval_id": "approval-bad",
        "requested_by": "operator",
        "action": "allow_boundary",
        "resource": "https://example.com/*",
        "request_kind": "boundary_policy",
        "status": "not-a-real-status",
        "context": {},
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO approvals (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ("approval_requests", "approval-bad", json.dumps(bad_approval), datetime.now(UTC).isoformat()),
        )

    loaded = load_runtime_store(checkpoint)
    loaded_crons = load_crons()

    assert [record.action for record in loaded.list_audit_records()] == ["valid.runtime.record"]
    assert [job.id for job in loaded_crons] == [cron.id]
    assert loaded_crons[0].delivery_target == "100001"
    with sqlite3.connect(checkpoint) as conn:
        quarantine = conn.execute(
            "SELECT collection, original_key, reason FROM memory_migration_failures"
        ).fetchone()
        active_bad = conn.execute(
            "SELECT COUNT(*) FROM approvals WHERE collection = ? AND item_key = ?",
            ("approval_requests", "approval-bad"),
        ).fetchone()[0]
    assert quarantine[0] == "approval_requests"
    assert quarantine[1] == "approval-bad"
    assert "not-a-real-status" in quarantine[2]
    assert active_bad == 0


def test_runtime_corrupt_checkpoint_recovery_restores_cron_jobs_from_recovered_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import crons, telegram_entrypoint
    from nullion.crons import add_cron, load_crons
    from nullion.runtime_persistence import save_runtime_store
    from nullion.runtime_store import RuntimeStore

    checkpoint = tmp_path / "runtime.db"
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", checkpoint)

    save_runtime_store(RuntimeStore(), checkpoint)
    cron = add_cron(
        "Recovered account cron",
        "0 9 * * *",
        "Use the configured account tools for this scheduled run",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="100001",
    )

    monkeypatch.setattr(telegram_entrypoint, "_checkpoint_is_valid_sqlite", lambda path: False)

    runtime = telegram_entrypoint._recover_runtime_from_corrupt_checkpoint(
        checkpoint,
        sqlite3.DatabaseError("database disk image is malformed"),
    )

    recovered_crons = load_crons()
    corrupt_backups = sorted(checkpoint.parent.glob("runtime.db.corrupt-*"))

    assert runtime is not None
    assert len(corrupt_backups) == 1
    assert [job.id for job in recovered_crons] == [cron.id]
    assert recovered_crons[0].delivery_target == "100001"


def test_operator_backup_restore_lists_named_recovered_dbs_with_size_and_restores_them(tmp_path: Path) -> None:
    from nullion.audit import make_audit_record
    from nullion.operator_commands import handle_operator_command
    from nullion.runtime import bootstrap_persistent_runtime

    checkpoint = tmp_path / "runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)
    runtime.store.add_audit_record(make_audit_record("before.failure", "e2e"))
    runtime.checkpoint()

    recovered = checkpoint.with_name("runtime.db.corrupt-123")
    recovered.write_bytes(checkpoint.read_bytes())

    runtime.store.add_audit_record(make_audit_record("after.failure", "e2e"))
    runtime.checkpoint()

    backups_text = handle_operator_command(runtime, "/backups")
    restore_text = handle_operator_command(runtime, "/restore corrupt-123")

    assert "corrupt-123" in backups_text
    assert "recovered" in backups_text
    assert "B, ok" in backups_text or "KB, ok" in backups_text or "MB, ok" in backups_text
    assert "/restore corrupt-123" in backups_text
    assert restore_text.startswith("Restored corrupt-123.")
    assert _audit_actions(checkpoint) == ["before.failure", "runtime.store_restored"]


def test_operator_restore_rejects_non_sqlite_recovered_backup_without_replacing_runtime(tmp_path: Path) -> None:
    from nullion.audit import make_audit_record
    from nullion.operator_commands import handle_operator_command
    from nullion.runtime import bootstrap_persistent_runtime

    checkpoint = tmp_path / "runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)
    runtime.store.add_audit_record(make_audit_record("safe.current", "e2e"))
    runtime.checkpoint()

    bad_recovered = checkpoint.with_name("runtime.db.corrupt-bad")
    bad_recovered.write_text("not sqlite", encoding="utf-8")

    backups_text = handle_operator_command(runtime, "/backups")
    restore_text = handle_operator_command(runtime, "/restore corrupt-bad")

    assert "corrupt-bad" in backups_text
    assert "not restorable" in backups_text
    assert "is not restorable" in restore_text
    assert _audit_actions(checkpoint) == ["safe.current"]
