from __future__ import annotations

from datetime import UTC, datetime
import json
import sqlite3

from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_store import RuntimeStore
from nullion.runtime_persistence import save_runtime_store
from nullion.startup_maintenance import STARTUP_MAINTENANCE_MARKER, run_startup_maintenance
from nullion.tools import ToolInvocation, ToolResult, _build_workspace_summary_handler


def _insert_runtime_row(conn: sqlite3.Connection, table: str, collection: str, key: str, payload: dict) -> None:
    now = datetime.now(UTC).isoformat()
    conn.execute(
        f"INSERT OR REPLACE INTO {table} (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
        (collection, key, json.dumps(payload), now),
    )


def test_startup_maintenance_compacts_bloated_tool_history_and_prunes_malformed_doctor_rows(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)
    huge_sample = [f"/Users/example/file-{index}.txt" for index in range(1000)]
    huge_body = "x" * 20_000
    with sqlite3.connect(checkpoint) as conn:
        _insert_runtime_row(
            conn,
            "conversation_events",
            "conversation_events",
            "turn-big",
            {
                "conversation_id": "telegram:1",
                "event_type": "conversation.chat_turn",
                "created_at": datetime.now(UTC).isoformat(),
                "user_message": "what is in my workspace",
                "assistant_reply": "summary",
                "tool_results": [
                    {
                        "tool_name": "workspace_summary",
                        "status": "completed",
                        "output": {"file_count": 1000, "sample_files": huge_sample, "body": huge_body},
                    }
                ],
            },
        )
        _insert_runtime_row(
            conn,
            "doctor_actions",
            "doctor_actions",
            "legacy-bad",
            {"target": "doctor", "reason": "legacy health alert"},
        )
        _insert_runtime_row(
            conn,
            "doctor_actions",
            "doctor_actions",
            "act-good",
            {
                "action_id": "act-good",
                "owner": "doctor",
                "status": "pending",
                "action_type": "inspect",
                "recommendation_code": "review_doctor_route",
                "summary": "Review",
                "severity": "low",
            },
        )

    result = run_startup_maintenance(checkpoint)

    assert result["skipped"] is False
    assert result["conversation_rows_compacted"] == 1
    assert result["doctor_rows_removed"] == 1
    assert result["backup_path"]
    assert (tmp_path / str(result["backup_path"]).split("/")[-1]).exists()
    with sqlite3.connect(checkpoint) as conn:
        marker = conn.execute("SELECT value FROM runtime_meta WHERE key = ?", (STARTUP_MAINTENANCE_MARKER,)).fetchone()
        assert marker is not None
        payload = json.loads(
            conn.execute("SELECT payload FROM conversation_events WHERE item_key = 'turn-big'").fetchone()[0]
        )
        output = payload["tool_results"][0]["output"]
        assert output["truncated"] is True
        assert output["reason"] == "startup_maintenance_tool_output_retention"
        assert len(output["preview"]) == 12_000
        assert conn.execute("SELECT COUNT(*) FROM doctor_actions WHERE item_key = 'legacy-bad'").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM doctor_actions WHERE item_key = 'act-good'").fetchone()[0] == 1


def test_startup_maintenance_is_idempotent_after_marker(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)

    first = run_startup_maintenance(checkpoint)
    second = run_startup_maintenance(checkpoint)

    assert first["skipped"] is False
    assert second["skipped"] is True
    assert second["reason"] == "already_ran"


def test_startup_maintenance_clears_doctor_actions_as_last_resort(monkeypatch, tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"
    save_runtime_store(RuntimeStore(), checkpoint)
    with sqlite3.connect(checkpoint) as conn:
        _insert_runtime_row(
            conn,
            "doctor_actions",
            "doctor_actions",
            "act-good",
            {
                "action_id": "act-good",
                "owner": "doctor",
                "status": "pending",
                "action_type": "inspect",
                "recommendation_code": "review_doctor_route",
                "summary": "Review",
                "severity": "low",
            },
        )

    import nullion.startup_maintenance as startup_maintenance

    def fail_targeted_cleanup(conn, result):  # noqa: ANN001
        raise RuntimeError("doctor scan failed")

    monkeypatch.setattr(startup_maintenance, "_malformed_doctor_action_keys", fail_targeted_cleanup)

    result = run_startup_maintenance(checkpoint)

    assert result["skipped"] is False
    assert result["doctor_last_resort_cleared"] is True
    assert result["doctor_rows_removed"] == 1
    assert result["backup_path"]
    with sqlite3.connect(checkpoint) as conn:
        assert conn.execute("SELECT COUNT(*) FROM doctor_actions").fetchone()[0] == 0


def test_bootstrap_persistent_runtime_continues_when_startup_maintenance_fails(monkeypatch, tmp_path) -> None:
    checkpoint = tmp_path / "runtime.db"

    def fail_startup_maintenance(path):  # noqa: ANN001
        raise RuntimeError("maintenance boom")

    import nullion.startup_maintenance as startup_maintenance

    monkeypatch.setattr(startup_maintenance, "run_startup_maintenance", fail_startup_maintenance)

    runtime = bootstrap_persistent_runtime(checkpoint)

    assert runtime.checkpoint_path == checkpoint


def test_workspace_summary_output_is_bounded(monkeypatch, tmp_path) -> None:
    for index in range(10):
        (tmp_path / f"file-{index}.txt").write_text("hello", encoding="utf-8")
    monkeypatch.setenv("NULLION_WORKSPACE_SUMMARY_MAX_ENTRIES", "5")
    monkeypatch.setenv("NULLION_WORKSPACE_SUMMARY_SAMPLE_FILES", "2")
    handler = _build_workspace_summary_handler(
        workspace_root=tmp_path,
        allowed_roots=None,
        include_principal_workspace=False,
    )

    result = handler(ToolInvocation("inv", "workspace_summary", "web:operator", {}))

    assert result.status == "completed"
    assert result.output["truncated"] is True
    assert result.output["scanned_entries"] == 6
    assert len(result.output["sample_files"]) <= 2


def test_stored_chat_tool_results_are_compacted_before_persistence() -> None:
    from nullion.chat_operator import _compact_tool_results_for_context
    from nullion.web_app import _compact_web_tool_results_for_context

    payload = {
        "file_count": 1000,
        "sample_files": [f"/tmp/file-{index}.txt" for index in range(1000)],
        "body": "x" * 20_000,
    }
    tool_result = ToolResult("inv", "workspace_summary", "completed", payload)

    telegram_compact = _compact_tool_results_for_context([tool_result])[0]["output"]
    web_compact = _compact_web_tool_results_for_context([tool_result])[0]["output"]

    assert telegram_compact["truncated"] is True
    assert web_compact["truncated"] is True
    assert telegram_compact["original_chars"] > 12_000
    assert web_compact["original_chars"] > 12_000


def test_scheduler_context_compaction_preserves_matched_cron_for_followup() -> None:
    from nullion.chat_operator import _compact_tool_results_for_context
    from nullion.tools import ToolResult
    from nullion.web_app import _compact_web_tool_results_for_context

    crons = [
        {
            "selection_index": index,
            "id": f"cron-{index}",
            "name": f"Unrelated cron {index}",
            "enabled": True,
            "has_task": True,
            "run_by_name": f'run_cron name="Unrelated cron {index}"',
        }
        for index in range(1, 40)
    ]
    crons.append(
        {
            "selection_index": 40,
            "id": "target-cron",
            "name": "Stage live scheduler short followup target",
            "display_name": "Stage live scheduler short followup target",
            "enabled": True,
            "has_task": True,
            "run_by_name": 'run_cron name="Stage live scheduler short followup target"',
        }
    )
    result = ToolResult(
        "list-1",
        "list_crons",
        "completed",
        {"crons": crons, "message": "large cron inventory " * 500},
    )
    user_message = (
        "Find the enabled scheduled task named 'Stage live scheduler short followup target' "
        "in my current cron list."
    )

    telegram_output = _compact_tool_results_for_context([result], user_message=user_message)[0]["output"]
    web_output = _compact_web_tool_results_for_context([result], user_message=user_message)[0]["output"]

    for output in (telegram_output, web_output):
        assert output["cron_count"] == 40
        assert output["matched_cron_count"] == 1
        assert output["matched_crons"][0]["id"] == "target-cron"
        assert output["crons"][0]["id"] == "target-cron"
        assert output["resolved_target"]["id"] == "target-cron"
        assert output["continuation_tools"] == ["run_cron"]
        assert output["omitted_cron_count"] > 0
        assert len(output["crons"]) <= 12
        assert "message" not in output
        assert output["message_truncated"] is True
        assert output["message_character_count"] > 600


def test_agent_model_context_never_exposes_partial_cron_message() -> None:
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context

    long_message = "Here are your scheduled tasks: " + "task details " * 100
    output = _compact_tool_output_for_model_context(
        "list_crons",
        {
            "crons": [
                {
                    "selection_index": 1,
                    "id": "cron-1",
                    "name": "Daily report",
                    "enabled": True,
                    "schedule_description": "At 8:00 AM",
                    "next_run_description": "Tomorrow at 8:00 AM",
                }
            ],
            "message": long_message,
        },
    )

    assert isinstance(output, dict)
    assert "message" not in output
    assert output["message_truncated"] is True
    assert output["message_character_count"] == len(" ".join(long_message.split()))
    assert output["crons"][0]["name"] == "Daily report"


def test_agent_model_context_preserves_deep_browser_product_evidence() -> None:
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context

    product_evidence = (
        "Dermaharmony 5% Sulfur and 2% Salicylic Acid Liquid Castile Soap. "
        "ACTIVE INGREDIENTS: 5% Sulfur and 2% Salicylic Acid. Price $9.99."
    )
    rendered_page = "Amazon navigation and recommendations\n" + ("unrelated listing\n" * 1800) + product_evidence

    output = _compact_tool_output_for_model_context(
        "browser_extract_text",
        {
            "url": "https://example.test/results",
            "title": "Search results",
            "text": rendered_page,
            "length": len(rendered_page),
        },
    )

    assert isinstance(output, dict)
    assert product_evidence in output["text"]
    assert output["text_compaction"]["strategy"] == "distributed_page_sections"
    assert len(output["text"]) <= 24_000
