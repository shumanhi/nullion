"""Shared compact scheduler tool context for follow-up turns."""

from __future__ import annotations

import re
import json
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
    "last_run",
    "tracker_status",
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
        "cron_count": output.get("cron_count", len(crons)),
        "crons": visible_rows,
    }
    # Context sampling may omit descriptive prose, never the tool's typed
    # failure/recovery contract. Otherwise a stale cursor becomes a permanent
    # unavailable capability and later scope selection retires its reader.
    for key in ("reason", "error_code", "retryable", "recovery", "required_source_tool",
                "executed", "terminal_user_action_required", "connection_state", "failure_scope", "source_urls"):
        if key in output:
            compact[key] = output[key]
    for key in ("offset", "limit", "matched_count", "inventory_version", "has_more", "next_offset", "inventory_scope"):
        if key in output:
            compact[key] = output[key]
    if isinstance(output.get("query"), str):
        compact["query"] = output["query"]
    if isinstance(output.get("total_count"), int):
        compact["total_cron_count"] = output["total_count"]
    omitted_count = max(0, len(prioritized_rows) - len(visible_rows))
    if omitted_count:
        # Follow-up/repair passes need the identities and display timing of
        # every observed row even when verbose health/task details are sampled.
        # Keep source pagination separate from this detail sample: the index
        # observes this page only and never certifies unseen inventory pages.
        index = []
        index_chars = 0
        for row in compact_rows:
            entry = {key: row[key] for key in ("selection_index", "id", "name", "enabled",
                "schedule_description", "next_run_description", "has_task") if key in row}
            size = len(json.dumps(entry, ensure_ascii=False))
            if index_chars + size > 24000:
                break
            index.append(entry)
            index_chars += size
        compact["inventory_index"] = index
        compact["inventory_index_complete"] = len(index) == len(compact_rows)
        compact["source_page_has_more"] = output.get("has_more")
        compact["inventory_index_guidance"] = (
            "The inventory_index preserves exact task identities, status and display timing from this source page. "
            "When naming an entry, copy its full name verbatim, including distinguishing suffixes; do not shorten it. "
            "Use it for an inventory answer or wording correction; crons is only the detailed sample. "
            "A complete index does not include unseen source pages or full stored task instructions. "
            "Inspect retained detail only when those omitted fields are needed."
        )
        compact["omitted_cron_count"] = omitted_count
        # This history view may prioritize a referenced row out of order. Its
        # sample cannot advance the source cursor past unseen records. Re-read
        # that exact page to enumerate it; retain the original boundary too.
        if "offset" in output:
            compact["source_page_next_offset"] = output.get("next_offset")
            compact["has_more"] = True
            compact["next_offset"] = output["offset"]
            compact["context_sample_only"] = True
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
