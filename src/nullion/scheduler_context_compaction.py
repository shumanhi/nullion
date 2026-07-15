"""Shared compact scheduler tool context for follow-up turns."""

from __future__ import annotations

import re
from typing import Mapping


_CRON_CONTEXT_KEYS = (
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


def _normalized_scheduler_reference(value: object) -> str:
    return " ".join(str(value or "").casefold().split())


def _cron_row_identity(row: Mapping[str, object]) -> tuple[str, str]:
    cron_id = str(row.get("id") or "").strip()
    name = str(row.get("name") or row.get("display_name") or "").strip()
    return cron_id, name


def _compact_cron_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        key: row.get(key)
        for key in _CRON_CONTEXT_KEYS
        if row.get(key) is not None
    }


def _cron_row_matches_reference(row: Mapping[str, object], reference_text: str) -> bool:
    normalized_reference = _normalized_scheduler_reference(reference_text)
    if not normalized_reference:
        return False
    cron_id, _name = _cron_row_identity(row)
    if cron_id and cron_id.casefold() in normalized_reference:
        return True
    for key in ("name", "display_name"):
        normalized_value = _normalized_scheduler_reference(row.get(key))
        if normalized_value and normalized_value in normalized_reference:
            return True
    return False


def _dedupe_cron_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        cron_id, name = _cron_row_identity(row)
        key = (cron_id.casefold(), name.casefold())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def compact_list_crons_output_for_context(
    output: Mapping[str, object],
    *,
    user_message: str | None = None,
    message_limit: int = 600,
    max_rows: int = 12,
) -> dict[str, object]:
    """Keep scheduler context compact while preserving resolved follow-up targets."""

    raw_crons = output.get("crons")
    crons = [item for item in raw_crons if isinstance(item, Mapping)] if isinstance(raw_crons, list) else []
    compact_rows = [_compact_cron_row(item) for item in crons]
    matched_rows = [
        _compact_cron_row(item)
        for item in crons
        if _cron_row_matches_reference(item, user_message or "")
    ]
    matched_rows = _dedupe_cron_rows(matched_rows)
    prioritized_rows = _dedupe_cron_rows([*matched_rows, *compact_rows])
    row_limit = max(1, int(max_rows or 1))
    visible_rows = prioritized_rows[:row_limit]
    compact: dict[str, object] = {
        "cron_count": len(crons),
        "crons": visible_rows,
    }
    omitted_count = max(0, len(prioritized_rows) - len(visible_rows))
    if omitted_count:
        compact["omitted_cron_count"] = omitted_count
    if matched_rows:
        compact["matched_crons"] = matched_rows[:3]
        compact["matched_cron_count"] = len(matched_rows)
    if len(matched_rows) == 1:
        matched = matched_rows[0]
        compact["resolved_target"] = {
            key: matched.get(key)
            for key in ("id", "name", "display_name", "selection_index")
            if matched.get(key) is not None
        }
        if matched.get("enabled") is not False and matched.get("has_task") is not False:
            compact["continuation_tools"] = ["run_cron"]
    message = output.get("message")
    if isinstance(message, str) and message.strip():
        limit = max(0, int(message_limit or 0))
        if limit:
            normalized_message = re.sub(r"\s+", " ", message).strip()
            if len(normalized_message) <= limit:
                compact["message"] = normalized_message
            else:
                # The structured rows above are the authoritative compact
                # representation.  A blind prefix can end mid-item and later
                # become a user-visible partial answer, so expose only typed
                # truncation metadata when the full message does not fit.
                compact["message_truncated"] = True
                compact["message_character_count"] = len(normalized_message)
    return compact


__all__ = ["compact_list_crons_output_for_context"]
