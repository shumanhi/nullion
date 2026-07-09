"""Bounded runtime repairs that run before Nullion starts serving traffic."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import shutil
import sqlite3
from typing import Any


STARTUP_MAINTENANCE_MARKER = "startup_maintenance_tool_output_retention_v2"
_SQLITE_SUFFIXES = {".db", ".sqlite", ".sqlite3"}
_MAX_STORED_TOOL_OUTPUT_CHARS = 12_000
_MAX_WORKSPACE_SUMMARY_SAMPLE_FILES = 50
_MAX_PAYLOAD_CHARS_BEFORE_REPAIR = 250_000


@dataclass(slots=True)
class StartupMaintenanceResult:
    checkpoint_path: str
    marker: str = STARTUP_MAINTENANCE_MARKER
    skipped: bool = False
    reason: str | None = None
    backup_path: str | None = None
    conversation_rows_examined: int = 0
    conversation_rows_compacted: int = 0
    doctor_rows_examined: int = 0
    doctor_rows_removed: int = 0
    doctor_last_resort_cleared: bool = False
    vacuumed: bool = False
    warnings: list[str] | None = None

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["warnings"] = list(self.warnings or [])
        return payload


def run_startup_maintenance(checkpoint_path: str | Path) -> dict[str, object]:
    """Run idempotent startup repairs against an installed runtime DB."""
    checkpoint = Path(checkpoint_path).expanduser()
    result = StartupMaintenanceResult(checkpoint_path=str(checkpoint), warnings=[])
    if checkpoint.suffix.lower() not in _SQLITE_SUFFIXES:
        result.skipped = True
        result.reason = "not_sqlite_runtime"
        return result.as_dict()
    if not checkpoint.exists():
        result.skipped = True
        result.reason = "checkpoint_missing"
        return result.as_dict()
    if os.environ.get("NULLION_STARTUP_MAINTENANCE", "").strip().lower() in {"0", "false", "no", "off"}:
        result.skipped = True
        result.reason = "disabled"
        return result.as_dict()

    timeout = _env_float("NULLION_STARTUP_MAINTENANCE_SQLITE_TIMEOUT_S", 5.0)
    try:
        try:
            with sqlite3.connect(str(checkpoint), timeout=timeout) as conn:
                conn.row_factory = sqlite3.Row
                if not _table_exists(conn, "runtime_meta"):
                    result.skipped = True
                    result.reason = "runtime_meta_missing"
                    return result.as_dict()
                if _marker_exists(conn):
                    result.skipped = True
                    result.reason = "already_ran"
                    return result.as_dict()

                updates: list[tuple[str, str]] = []
                try:
                    updates = _conversation_event_repairs(conn, result)
                except Exception as exc:
                    result.warnings = [
                        *(result.warnings or []),
                        f"conversation event compaction skipped: {exc}",
                    ]
                doctor_deletes: list[str] = []
                try:
                    doctor_deletes = _malformed_doctor_action_keys(conn, result)
                except Exception as exc:
                    result.warnings = [
                        *(result.warnings or []),
                        f"targeted Doctor cleanup failed; clearing Doctor actions as last resort: {exc}",
                    ]
                    doctor_deletes = _all_doctor_action_keys(conn, result)
                    result.doctor_last_resort_cleared = bool(doctor_deletes)
                if updates or doctor_deletes:
                    result.backup_path = str(_backup_runtime_db(checkpoint))
                now = datetime.now(UTC).isoformat()
                for item_key, payload in updates:
                    conn.execute(
                        "UPDATE conversation_events SET payload = ?, updated_at = ? WHERE item_key = ?",
                        (payload, now, item_key),
                    )
                for item_key in doctor_deletes:
                    conn.execute("DELETE FROM doctor_actions WHERE item_key = ?", (item_key,))
                conn.execute(
                    "INSERT OR REPLACE INTO runtime_meta (key, value) VALUES (?, ?)",
                    (STARTUP_MAINTENANCE_MARKER, now),
                )
        except sqlite3.Error as exc:
            result.skipped = True
            result.reason = "sqlite_error"
            result.warnings = [str(exc)]
            return result.as_dict()
    except Exception as exc:
        result.skipped = True
        result.reason = "startup_maintenance_error"
        result.warnings = [str(exc)]
        return result.as_dict()

    if result.backup_path and _env_flag("NULLION_STARTUP_MAINTENANCE_VACUUM", default=True):
        try:
            with sqlite3.connect(str(checkpoint), timeout=timeout) as conn:
                conn.execute("VACUUM")
            result.vacuumed = True
        except sqlite3.Error as exc:
            result.warnings = [*(result.warnings or []), f"vacuum skipped: {exc}"]
    return result.as_dict()


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(0.1, float(raw))
    except ValueError:
        return default


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _marker_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT value FROM runtime_meta WHERE key = ?",
        (STARTUP_MAINTENANCE_MARKER,),
    ).fetchone()
    return row is not None


def _backup_runtime_db(checkpoint: Path) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    backup = checkpoint.with_name(f"{checkpoint.name}.pre-startup-maintenance-{stamp}.bak")
    suffix = 1
    while backup.exists():
        backup = checkpoint.with_name(f"{checkpoint.name}.pre-startup-maintenance-{stamp}-{suffix}.bak")
        suffix += 1
    shutil.copy2(checkpoint, backup)
    return backup


def _conversation_event_repairs(
    conn: sqlite3.Connection,
    result: StartupMaintenanceResult,
) -> list[tuple[str, str]]:
    if not _table_exists(conn, "conversation_events"):
        return []
    rows = conn.execute(
        "SELECT item_key, payload FROM conversation_events "
        "WHERE length(payload) >= ? OR payload LIKE '%\"tool_results\"%'",
        (_MAX_PAYLOAD_CHARS_BEFORE_REPAIR,),
    ).fetchall()
    repairs: list[tuple[str, str]] = []
    for row in rows:
        result.conversation_rows_examined += 1
        item_key = str(row["item_key"])
        payload_text = str(row["payload"] or "")
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict) or payload.get("event_type") != "conversation.chat_turn":
            continue
        tool_results = payload.get("tool_results")
        if not isinstance(tool_results, list) or not tool_results:
            continue
        compacted = [_compact_stored_tool_result(item) for item in tool_results]
        if compacted == tool_results:
            continue
        repaired = {**payload, "tool_results": compacted}
        repairs.append((item_key, json.dumps(repaired, ensure_ascii=False, sort_keys=True)))
        result.conversation_rows_compacted += 1
    return repairs


def _compact_stored_tool_result(item: object) -> object:
    if not isinstance(item, dict):
        return item
    tool_name = str(item.get("tool_name") or item.get("name") or "")
    output = item.get("output")
    compact_output = _compact_tool_output(tool_name, output)
    if compact_output is output:
        return item
    return {**item, "output": compact_output}


def _compact_tool_output(tool_name: str, output: object) -> object:
    candidate = output
    if tool_name == "list_crons" and isinstance(candidate, dict):
        crons = candidate.get("crons")
        compact_crons: list[dict[str, object]] = []
        if isinstance(crons, list):
            for item in crons:
                if not isinstance(item, dict):
                    continue
                compact_crons.append(
                    {
                        key: item.get(key)
                        for key in (
                            "selection_index",
                            "id",
                            "name",
                            "display_name",
                            "enabled",
                            "schedule_description",
                            "next_run_description",
                            "run_by_name",
                            "has_task",
                            "has_last_result",
                        )
                        if item.get(key) is not None
                    }
                )
        candidate = {
            "cron_count": len(crons) if isinstance(crons, list) else 0,
            "crons": compact_crons,
        }
        message = output.get("message")
        if isinstance(message, str) and message.strip():
            candidate["message"] = message[:8_000]
    if tool_name == "workspace_summary" and isinstance(candidate, dict):
        sample_files = candidate.get("sample_files")
        if isinstance(sample_files, list) and len(sample_files) > _MAX_WORKSPACE_SUMMARY_SAMPLE_FILES:
            candidate = {
                **candidate,
                "sample_files": sample_files[:_MAX_WORKSPACE_SUMMARY_SAMPLE_FILES],
                "sample_files_truncated": {
                    "shown": _MAX_WORKSPACE_SUMMARY_SAMPLE_FILES,
                    "total": len(sample_files),
                },
            }
    encoded = _json_size_text(candidate)
    if len(encoded) <= _MAX_STORED_TOOL_OUTPUT_CHARS:
        return candidate
    return {
        "truncated": True,
        "original_chars": len(encoded),
        "reason": "startup_maintenance_tool_output_retention",
        "preview": encoded[:_MAX_STORED_TOOL_OUTPUT_CHARS],
    }


def _json_size_text(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except TypeError:
        return str(value)


def _malformed_doctor_action_keys(
    conn: sqlite3.Connection,
    result: StartupMaintenanceResult,
) -> list[str]:
    if not _table_exists(conn, "doctor_actions"):
        return []
    rows = conn.execute("SELECT item_key, payload FROM doctor_actions").fetchall()
    malformed: list[str] = []
    for row in rows:
        result.doctor_rows_examined += 1
        item_key = str(row["item_key"])
        try:
            payload = json.loads(str(row["payload"] or ""))
        except json.JSONDecodeError:
            malformed.append(item_key)
            continue
        if not isinstance(payload, dict) or _is_malformed_doctor_action(payload):
            malformed.append(item_key)
    result.doctor_rows_removed = len(malformed)
    return malformed


def _all_doctor_action_keys(
    conn: sqlite3.Connection,
    result: StartupMaintenanceResult,
) -> list[str]:
    if not _table_exists(conn, "doctor_actions"):
        return []
    rows = conn.execute("SELECT item_key FROM doctor_actions").fetchall()
    keys = [str(row["item_key"]) for row in rows]
    result.doctor_rows_examined = max(result.doctor_rows_examined, len(keys))
    result.doctor_rows_removed = len(keys)
    return keys


def _is_malformed_doctor_action(action: dict[str, Any]) -> bool:
    required_fields = ("action_id", "owner", "status", "action_type", "recommendation_code", "summary", "severity")
    return any(not str(action.get(field) or "").strip() for field in required_fields)


__all__ = ["STARTUP_MAINTENANCE_MARKER", "run_startup_maintenance"]
