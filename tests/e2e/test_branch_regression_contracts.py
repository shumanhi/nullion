from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run branch regression e2e tests",
)


@dataclass
class _CronJob:
    id: str = "cron-e2e"
    name: str = "Repo watcher"
    task: str = "check structured runtime facts"
    delivery_channel: str = "telegram"
    delivery_target: str = "42"


class _MemoryStore:
    def __init__(self, entries):
        self.entries = {entry.entry_id: entry for entry in entries}
        self.removed: list[str] = []
        self.written: list[object] = []

    def list_user_memory_entries(self):
        return list(self.entries.values())

    def add_user_memory_entry(self, entry) -> None:
        self.written.append(entry)
        self.entries[entry.entry_id] = entry

    def remove_user_memory_entry(self, entry_id: str) -> bool:
        if entry_id not in self.entries:
            return False
        self.entries.pop(entry_id)
        self.removed.append(entry_id)
        return True


class _Runtime:
    def __init__(self, store: _MemoryStore) -> None:
        self.store = store
        self.checkpoints = 0

    def checkpoint(self) -> None:
        self.checkpoints += 1


class _StructuredMemoryModel:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return {"content": [{"type": "text", "text": self.text}]}


def _memory_entry(entry_id: str, owner: str, key: str, value: str):
    from nullion.memory import UserMemoryEntry, UserMemoryKind

    now = datetime(2026, 5, 3, tzinfo=UTC)
    return UserMemoryEntry(
        entry_id=entry_id,
        owner=owner,
        kind=UserMemoryKind.FACT,
        key=key,
        value=value,
        created_at=now,
        updated_at=now,
    )


def _seed_legacy_memory_row(checkpoint: Path, *, collection: str, item_key: str, payload: str) -> None:
    now = datetime(2026, 5, 3, tzinfo=UTC).isoformat()
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO memory (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            (collection, item_key, payload, now),
        )


def _legacy_memory_payload(*, entry_id: str, owner: str, kind: str, key: str, value: str) -> str:
    now = datetime(2026, 5, 3, tzinfo=UTC).isoformat()
    return json.dumps(
        {
            "entry_id": entry_id,
            "owner": owner,
            "kind": kind,
            "key": key,
            "value": value,
            "source": "legacy",
            "created_at": now,
            "updated_at": now,
        },
        sort_keys=True,
    )


def test_builder_manages_concise_workspace_memory_from_structured_model_output() -> None:
    from nullion import builder_memory

    store = _MemoryStore(
        [
            _memory_entry("workspace:admin:old", "workspace:admin", "old", "stale detail"),
            _memory_entry("workspace:other:keep", "workspace:other", "keep", "other workspace"),
        ]
    )
    model = _StructuredMemoryModel(
        """
        {
          "entries": [
            {
              "key": "reply_style",
              "value": "Prefers concise updates",
              "kind": "preference",
              "confidence": 0.92
            },
            {
              "key": "uncertain",
              "value": "drop low confidence memory",
              "kind": "fact",
              "confidence": 0.4
            }
          ]
        }
        """
    )

    result = builder_memory.manage_turn_memory(
        _Runtime(store),
        model,
        owner="workspace:admin",
        user_message="mantener la memoria limpia y breve",
        assistant_reply="Listo.",
        tool_results=[{"tool_name": "file_write", "status": "completed", "metadata": {"path": "/workspace/notes.txt"}}],
    )

    assert result.written == 1
    assert result.removed == 1
    assert "workspace:admin:old" in store.removed
    assert "workspace:other:keep" in store.entries
    assert store.entries["workspace:admin:reply_style"].value == "Prefers concise updates"
    prompt_payload = model.calls[0]["messages"][1]["content"][0]["text"]
    assert "mantener la memoria limpia" in prompt_payload
    assert len(prompt_payload) < 5000


def test_memory_bucket_settings_round_trip_through_config(monkeypatch, tmp_path: Path) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    from nullion.tools import ToolRegistry

    monkeypatch.setattr(web_app, "_find_env_path", lambda: tmp_path / ".env")
    monkeypatch.delenv("NULLION_MEMORY_LONG_TERM_LIMIT", raising=False)
    monkeypatch.delenv("NULLION_MEMORY_MID_TERM_LIMIT", raising=False)
    monkeypatch.delenv("NULLION_MEMORY_SHORT_TERM_LIMIT", raising=False)

    client = TestClient(web_app.create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry()))
    initial = client.get("/api/config").json()
    assert initial["memory_long_term_limit"] == 25
    assert initial["memory_mid_term_limit"] == 15
    assert initial["memory_short_term_limit"] == 10

    response = client.post(
        "/api/config",
        json={
            "memory_long_term_limit": 8,
            "memory_mid_term_limit": 11,
            "memory_short_term_limit": 4,
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    updated = client.get("/api/config").json()
    assert updated["memory_long_term_limit"] == 8
    assert updated["memory_mid_term_limit"] == 11
    assert updated["memory_short_term_limit"] == 4


def test_recalled_memory_scores_protect_short_term_context() -> None:
    from nullion import builder_memory
    from nullion.memory import UserMemoryEntry, UserMemoryKind

    owner = "workspace:admin"
    base = datetime(2026, 5, 3, tzinfo=UTC)
    stale = UserMemoryEntry(
        entry_id=f"{owner}:stale",
        owner=owner,
        kind=UserMemoryKind.FACT,
        key="stale",
        value="rarely useful short term detail",
        created_at=base,
        updated_at=base,
    )
    recalled = UserMemoryEntry(
        entry_id=f"{owner}:recalled",
        owner=owner,
        kind=UserMemoryKind.FACT,
        key="recalled",
        value="used repeatedly",
        created_at=base,
        updated_at=base,
        use_count=4,
        use_score=9.0,
        last_used_at=base,
    )
    store = _MemoryStore([stale, recalled])

    selected = builder_memory.select_memory_entries_for_prompt(
        [stale, recalled],
        policy=builder_memory.MemoryPolicy(long_term_limit=0, mid_term_limit=0, short_term_limit=1),
    )
    assert selected == [recalled]
    assert builder_memory.reinforce_memory_entries(store, selected) == 1
    assert store.entries[recalled.entry_id].use_score == 10.0
    assert store.entries[stale.entry_id].use_score == 0.0


def test_cron_messaging_delivery_never_promotes_attachment_text_over_final_reply(tmp_path: Path) -> None:
    from nullion.cron_delivery import CronRunDeliveryCallbacks, cron_agent_prompt, run_cron_delivery_workflow

    artifact = tmp_path / "account_attachment_ocr.txt"
    artifact.write_text(
        "NOTICE DATE\n07/03/2026\nAPPLICATION/PETITION/REQUEST NUMBER\nIOE0000000000",
        encoding="utf-8",
    )
    sent: list[str] = []
    events: list[str] = []
    job = _CronJob()

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda _job: "telegram",
        delivery_target=lambda _job, _channel: "42",
        run_agent_turn=lambda _job, _conv_id: {
            "text": "One dated notice was reviewed; no deadline was clear enough to schedule.",
            "artifacts": [{"path": str(artifact)}],
        },
        record_event=lambda event_type, *args, **kwargs: events.append(event_type),
        block_reason=lambda _result, _text, _artifacts: None,
        save_web_delivery=lambda _job, _conv_id, _text, _artifacts, _result: False,
        send_platform_delivery=lambda _job, _channel, text: sent.append(text) or True,
    )

    result = run_cron_delivery_workflow(job, label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == [
        "⏰ **SCHEDULED TASK: Repo watcher**\n\n"
        "One dated notice was reviewed; no deadline was clear enough to schedule."
    ]
    assert str(artifact) not in sent[0]
    assert "APPLICATION/PETITION/REQUEST NUMBER" not in sent[0]
    assert "IOE0000000000" not in sent[0]
    assert "MEDIA:" not in sent[0]


def test_sqlite_platform_delivery_uses_exact_completed_turn_during_overlap(monkeypatch, tmp_path: Path) -> None:
    from nullion import chat_operator, messaging_turn_graph
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    request_text = "List every scheduled task."
    full_reply = "Here are all scheduled tasks:\n\n" + "\n\n".join(
        (
            f"{index}. Scheduled task {index}\n"
            "   Status: enabled\n"
            f"   Schedule: At {index}:00 PM every weekday\n"
            f"   Next run: Jul 14, 2026 at {index}:00 PM"
        )
        for index in range(1, 9)
    )
    partial_preview = full_reply[:600]
    runtime.store.add_conversation_event(
        {
            "conversation_id": "telegram:42",
            "workspace_id": "workspace_admin",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-07-13T13:05:00.879318+00:00",
            "chat_id": "42",
            "turn_id": "turn-inventory",
            "user_message": request_text,
            "assistant_reply": full_reply,
            "tool_results": [],
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": "telegram:42",
            "workspace_id": "workspace_admin",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-07-13T13:05:00.900000+00:00",
            "chat_id": "42",
            "turn_id": "turn-catalog",
            "user_message": "Find a ranked product.",
            "assistant_reply": "A later sibling response.",
            "tool_results": [],
        }
    )
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: partial_preview,
    )

    response = messaging_turn_graph.run_platform_chat_request(
        runtime,
        messaging_turn_graph.PlatformChatRequest(
            platform="telegram",
            text=request_text,
            chat_id="42",
            turn_id="turn-inventory",
        ),
    )

    assert response.text is not None
    assert "8. Scheduled task 8" in response.text
    assert "Next run: Jul 14, 2026 at 8:00 PM" in response.text
    assert len(response.text) > len(partial_preview)


def test_cron_messaging_delivery_blocks_raw_terminal_stdout_dump() -> None:
    from nullion.cron_delivery import CronRunDeliveryCallbacks, cron_delivery_block_reason, run_cron_delivery_workflow

    sent: list[str] = []
    events: list[tuple[str, str]] = []
    terminal_stdout = (
        'traffic_views {\n'
        '  "count": 688,\n'
        '  "uniques": 38,\n'
        '  "views": [\n'
        '    {"timestamp": "2026-05-28T00:00:00Z", "count": 12, "uniques": 4}\n'
        "  ]\n"
        "}\n"
    )
    job = _CronJob(name="Nullion social + GitHub traffic monitor no empty graphs")

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda _job: "telegram",
        delivery_target=lambda _job, _channel: "42",
        run_agent_turn=lambda _job, _conv_id: {
            "text": f"STDOUT\n\n{terminal_stdout}",
            "tool_results": [
                {
                    "tool_name": "terminal_exec",
                    "status": "completed",
                    "output": {"stdout": terminal_stdout, "stderr": "", "exit_code": 0},
                }
            ],
            "artifacts": [],
        },
        record_event=lambda event_type, *args, **kwargs: events.append((event_type, str(kwargs.get("reason") or ""))),
        block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda _job, _conv_id, _text, _artifacts, _result: False,
        send_platform_delivery=lambda _job, _channel, text: sent.append(text) or True,
    )

    result = run_cron_delivery_workflow(job, label="Scheduled task", callbacks=callbacks)

    assert sent == []
    assert result["cron_delivery_status"] == "failed"
    assert result["reason"] == "cron_run_internal_tool_output_leaked"
    assert ("cron.delivery.failed", "cron_run_internal_tool_output_leaked") in events


def test_update_migration_encrypts_legacy_memory_rows_without_warning(tmp_path: Path, monkeypatch) -> None:
    from nullion import updater
    from nullion.memory import UserMemoryKind
    from nullion.runtime_persistence import load_runtime_store, save_runtime_store
    from nullion.runtime_store import RuntimeStore

    install = tmp_path / "install"
    install.mkdir()
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    env_path = install / ".env"
    env_path.write_text('NULLION_KEY_STORAGE="local"\n', encoding="utf-8")
    env_path.chmod(0o600)
    checkpoint = install / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)
    _seed_legacy_memory_row(
        checkpoint,
        collection="preferences",
        item_key="workspace:ops:alert_style",
        payload=_legacy_memory_payload(
            entry_id="workspace:ops:alert_style",
            owner="workspace:ops",
            kind="preference",
            key="alert_style",
            value="User wants concise watcher alerts.",
        ),
    )

    result = updater.run_post_update_migrations(env_path=env_path, checkpoint_path=checkpoint, install_dir=install)

    assert result["memory_migrated"] is True
    assert result["memory_rows_encrypted"] == 1
    assert result["memory_rows_quarantined"] == 0
    assert result["warnings"] == []
    raw_db = checkpoint.read_bytes()
    assert b"User wants concise watcher alerts" not in raw_db
    assert b"workspace:ops:alert_style" not in raw_db
    [memory] = load_runtime_store(checkpoint).list_user_memory_entries(kind=UserMemoryKind.PREFERENCE)
    assert memory.owner == "workspace:ops"
    assert memory.value == "User wants concise watcher alerts."
    with sqlite3.connect(checkpoint) as conn:
        row = conn.execute("SELECT item_key, payload FROM memory WHERE collection = ?", ("preferences",)).fetchone()
    assert row[0].startswith("encrypted:")
    assert str(row[1]).startswith("fernet:v1:")


def test_update_migration_quarantines_bad_memory_rows_and_keeps_runtime_loading(tmp_path: Path, monkeypatch) -> None:
    from nullion import updater
    from nullion.memory import UserMemoryKind
    from nullion.runtime_persistence import load_runtime_store, save_runtime_store
    from nullion.runtime_store import RuntimeStore

    install = tmp_path / "install"
    install.mkdir()
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    env_path = install / ".env"
    env_path.write_text('NULLION_KEY_STORAGE="local"\n', encoding="utf-8")
    env_path.chmod(0o600)
    checkpoint = install / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)
    _seed_legacy_memory_row(
        checkpoint,
        collection="user_facts",
        item_key="workspace:ops:watcher_scope",
        payload=_legacy_memory_payload(
            entry_id="workspace:ops:watcher_scope",
            owner="workspace:ops",
            kind="fact",
            key="watcher_scope",
            value="Legacy watcher scope should survive migration.",
        ),
    )
    _seed_legacy_memory_row(
        checkpoint,
        collection="preferences",
        item_key="workspace:ops:broken_memory",
        payload="{bad json",
    )

    result = updater.run_post_update_migrations(env_path=env_path, checkpoint_path=checkpoint, install_dir=install)

    assert result["memory_migrated"] is True
    assert result["memory_rows_encrypted"] == 1
    assert result["memory_rows_quarantined"] == 1
    assert any("old memory row" in warning and "deleting old memories" in warning for warning in result["warnings"])
    raw_db = checkpoint.read_bytes()
    assert b"Legacy watcher scope should survive migration" not in raw_db
    assert b"workspace:ops:broken_memory" not in raw_db
    [memory] = load_runtime_store(checkpoint).list_user_memory_entries(kind=UserMemoryKind.FACT)
    assert memory.value == "Legacy watcher scope should survive migration."
    with sqlite3.connect(checkpoint) as conn:
        quarantined = conn.execute("SELECT original_key, payload FROM memory_migration_failures").fetchone()
    assert quarantined[0].startswith("row:")
    assert str(quarantined[1]).startswith("fernet:v1:")


def test_web_startup_quarantines_bad_runtime_rows_and_keeps_loading(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from nullion.approvals import create_approval_request
    from nullion.runtime_persistence import save_runtime_store
    from nullion.runtime_store import RuntimeStore
    from nullion import web_app

    install = tmp_path / "install"
    install.mkdir()
    checkpoint = install / "runtime.db"
    memory_key = install / "memory.key"
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    monkeypatch.setenv("NULLION_CHECKPOINT_PATH", str(checkpoint))
    monkeypatch.setenv("NULLION_MEMORY_KEY_PATH", str(memory_key))
    monkeypatch.setenv("NULLION_KEY_STORAGE", "local")
    monkeypatch.setenv("NULLION_WORKSPACE_ROOT", str(tmp_path / "workspace_admin"))
    monkeypatch.setenv("NULLION_ENABLED_PLUGINS", "")
    monkeypatch.setattr(web_app, "_STARTUP_WARNINGS", [])

    approval = create_approval_request(
        requested_by="web:operator",
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
            ("approval_requests", "bad-json-row", "", datetime.now(UTC).isoformat()),
        )

    runtime, orchestrator, registry = web_app._build_runtime()
    captured = capsys.readouterr()

    assert runtime.store.get_approval_request(approval.approval_id) is not None
    assert orchestrator is not None
    assert registry is not None
    assert "Traceback" not in captured.err
    assert "Expecting value" not in captured.err
    with sqlite3.connect(checkpoint) as conn:
        active_bad_rows = conn.execute(
            "SELECT COUNT(*) FROM approvals WHERE collection = ? AND item_key = ?",
            ("approval_requests", "bad-json-row"),
        ).fetchone()[0]
        quarantined = conn.execute(
            "SELECT collection, original_key, payload, reason FROM memory_migration_failures "
            "WHERE collection = ? AND original_key = ?",
            ("approval_requests", "bad-json-row"),
        ).fetchone()
    assert active_bad_rows == 0
    assert quarantined is not None
    assert str(quarantined[2]).startswith("fernet:v1:")
    assert quarantined[3]


def test_update_workflow_surfaces_memory_migration_warnings(monkeypatch) -> None:
    from nullion import updater

    emitted: list[updater.UpdateProgress] = []
    warning = "1 old memory row(s) could not be imported and were moved aside. You can continue by deleting old memories."
    monkeypatch.setattr(
        updater,
        "fresh_post_update_migrations",
        lambda **_kwargs: {
            "memory_migrated": True,
            "memory_rows_encrypted": 1,
            "memory_rows_quarantined": 1,
            "warnings": [warning],
        },
    )

    result = asyncio.run(
        updater._update_migrate_node(
            {
                "steps": [],
                "emit": emitted.append,
                "checkpoint_path": None,
                "env_path": None,
                "ignore_check_failures": False,
            }
        )
    )

    messages = [step.message for step in result["steps"]]
    assert "Migration complete (memory imported)." in messages
    assert f"Warning: {warning}" in messages
    assert any(step.ok is False and step.message == f"Warning: {warning}" for step in emitted)
