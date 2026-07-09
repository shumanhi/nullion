"""Guards for user-visible assistant replies.

The model sometimes mirrors the previous tool result instead of converting it
into a human answer. That is noisy at best and can leak local paths or other
diagnostic details at worst, so this module blocks bare structured payloads
unless the user explicitly asked for raw output.
"""

from __future__ import annotations

import ast
import html
import json
import math
import re
import unicodedata
from collections.abc import Mapping
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from nullion.tips import IMAGE_GENERATION_SETUP_TIP, format_setup_tip
from nullion.tools import ToolResult


_SENSITIVE_STRUCTURED_KEYS = {
    "arguments",
    "audit_entries",
    "body",
    "content",
    "directory_count",
    "extensions",
    "file_count",
    "html",
    "image_base64",
    "match_details",
    "output",
    "raw_body",
    "roots",
    "sample_files",
    "stderr",
    "stdout",
    "tool_results",
}

_LOCAL_POSIX_PATH_RE = re.compile(
    r"(?P<path>/(?:Users|home)/[^/\s`\"'<>:)]+/[^\s`\"'<>)]*)"
)
_LOCAL_WINDOWS_PATH_RE = re.compile(
    r"(?P<path>[A-Za-z]:\\Users\\[^\\\s`\"'<>:)]+\\[^\s`\"'<>)]*)"
)
_PRESERVE_DIRECTIVE_PREFIXES = ("MEDIA:", "ARTIFACT:")
_SEMANTIC_STATUS_SYMBOLS = {"☐", "☑", "☒"}
_RAW_FUNCTION_CALL_RE = re.compile(
    r"<\s*(?:function|tool_request|tool_call|tool_use|request_tool_scope)\b[\s\S]*?</\s*(?:function|tool_request|tool_call|tool_use|request_tool_scope)\s*>"
    r"|#\{\s*<\s*(?:function|tool_request|tool_call|tool_use|request_tool_scope)\b[\s\S]*?</\s*(?:function|tool_request|tool_call|tool_use|request_tool_scope)\s*>\s*\}",
    flags=re.IGNORECASE,
)
_SCHEDULER_ACTION_TOOLS = frozenset(
    {
        "create_cron",
        "run_cron",
        "update_cron",
        "delete_cron",
        "toggle_cron",
        "set_reminder",
        "create_reminder",
        "update_reminder",
        "delete_reminder",
    }
)
_SCHEDULER_READ_TOOLS = frozenset({"list_crons", "list_reminders"})
_ACTION_RECEIPT_REPLY_EXCLUDED_TOOLS = frozenset({"terminal_exec"})
_READ_TOOL_SOURCE_TOOLS = {
    "email_read": ("email_search",),
    "email_attachment_read": ("email_read",),
}
_BROWSER_FORM_ACTION_TOOLS = frozenset(
    {
        "browser_click_element",
        "browser_click_id",
        "browser_type_field",
        "browser_type_id",
        "browser_select_combobox",
    }
)


def _tool_result_completed(result: ToolResult) -> bool:
    status = str(getattr(result, "status", "") or "").strip().lower()
    if status:
        return status == "completed"
    output = getattr(result, "output", None)
    return isinstance(output, dict) and bool(output)


def user_requested_raw_output(user_message: str | None) -> bool:
    """Free-form user text does not bypass raw payload protection."""

    return False


def sanitize_user_visible_reply(
    *,
    user_message: str | None,
    reply: str | None,
    tool_results: Iterable[ToolResult | Mapping[str, Any] | object] | None = None,
    requested_sections: Iterable[object] | None = None,
    source: str = "agent",
) -> str | None:
    """Replace bare raw tool payloads with a safe user-visible explanation."""

    if reply is None or user_requested_raw_output(user_message):
        return reply
    raw = _strip_invisible_tracking_text(str(reply))
    results = _coerce_tool_results(tool_results)
    if not results:
        if _looks_like_raw_function_markup(raw):
            return safe_raw_tool_payload_replacement(tool_results=(), source=source)
        raw = _repair_collapsed_numbered_list_reply(raw)
        raw = _renumber_visible_numbered_list_reply(raw)
        return _sanitize_reply_style(_sanitize_local_paths(raw))
    if _has_completed_tool_result(results, "email_send"):
        raw = _sanitize_email_send_transport_details(raw)
    raw = _sanitize_account_tool_transport_details(raw, results, user_message=user_message)
    raw = _sanitize_internal_tool_state_phrasing(raw, results, user_message=user_message)
    raw = _sanitize_account_tool_body_leak(raw, results)
    raw = _sanitize_account_tool_family_drift(raw, results, user_message=user_message)
    if account_detour_reply := _account_read_reply_over_invalid_attachment_detour(raw, results):
        return _sanitize_reply_style(
            _sanitize_local_paths(account_detour_reply),
            account_tool_family=_primary_account_tool_family(results),
        )
    if account_browser_detour_reply := _account_read_reply_over_browser_detour(raw, results):
        return _sanitize_reply_style(
            _sanitize_local_paths(account_browser_detour_reply),
            account_tool_family=_primary_account_tool_family(results),
        )
    raw = _account_tool_reply_over_ungrounded_result(raw, results, user_message=user_message)
    raw = _calendar_relevance_reply_over_raw_listing(user_message, raw, results)
    if image_generation_unavailable_reply := _image_generation_scope_unavailable_reply(results):
        return _sanitize_reply_style(_sanitize_local_paths(image_generation_unavailable_reply))
    raw = _repair_collapsed_numbered_list_reply(raw)
    raw = _renumber_visible_numbered_list_reply(raw)
    raw = _repair_missing_requested_section_replies(requested_sections, raw, results)
    raw = _normalize_requested_section_reply_format(requested_sections, raw, results)
    raw = _prefix_account_tool_reply(raw, results)
    raw = _strip_leading_tool_status_paragraph(raw, results)
    if _browser_grounded_reply_should_pass_through(raw, results):
        return _sanitize_local_paths(raw)
    if browser_incomplete_reply := _browser_incomplete_reply_over_generic_verified_state(
        raw,
        results,
        user_message=user_message,
    ):
        return _sanitize_local_paths(browser_incomplete_reply)
    if browser_extract_reply := _browser_extract_evidence_reply_over_blocker(
        raw,
        results,
        user_message=user_message,
    ):
        return _sanitize_local_paths(browser_extract_reply)
    if browser_prior_catalog_reply := _browser_catalog_reply_over_prior_extract_dump(raw, results):
        return _sanitize_local_paths(browser_prior_catalog_reply)
    if browser_catalog_reply := _browser_catalog_reply_over_raw_extract_dump(raw, results):
        return _sanitize_local_paths(browser_catalog_reply)
    if browser_extract_dump_reply := _browser_extract_dump_reply_over_raw_extract(
        raw,
        results,
        user_message=user_message,
    ):
        return _sanitize_local_paths(browser_extract_dump_reply)
    if browser_empty_reply := _browser_empty_reply_over_missing_verified_records(raw, results, user_message=user_message):
        return _sanitize_local_paths(browser_empty_reply)
    if browser_withheld_extract_reply := _browser_withheld_raw_page_reply_over_extract(
        raw,
        results,
        user_message=user_message,
    ):
        return _sanitize_local_paths(browser_withheld_extract_reply)
    if browser_verified_state_reply := _browser_verified_state_reply_over_tool_status(raw, results):
        return _sanitize_local_paths(browser_verified_state_reply)
    if browser_prior_catalog_reply := _browser_catalog_reply_over_prior_extract_dump(raw, results):
        return _sanitize_local_paths(browser_prior_catalog_reply)
    if browser_catalog_reply := _browser_catalog_reply_over_raw_extract_dump(raw, results):
        return _sanitize_local_paths(browser_catalog_reply)
    if browser_extract_dump_reply := _browser_extract_dump_reply_over_raw_extract(
        raw,
        results,
        user_message=user_message,
    ):
        return _sanitize_local_paths(browser_extract_dump_reply)
    if browser_better_extract_reply := _browser_reply_over_wrong_browser_extract(
        raw,
        results,
        user_message=user_message,
    ):
        return _sanitize_local_paths(browser_better_extract_reply)
    if browser_extract_reply := _browser_extract_text_reply_over_assertion_drift(
        raw,
        results,
        user_message=user_message,
    ):
        return _sanitize_local_paths(browser_extract_reply)
    if browser_low_quality_reply := _browser_low_quality_items_reply_over_top_matches(raw, results):
        return _sanitize_local_paths(browser_low_quality_reply)
    if action_receipt_reply := _action_receipt_reply_over_drift(raw, results):
        return _sanitize_local_paths(action_receipt_reply)
    if scheduler_action_reply := _scheduler_action_reply_over_read_drift(raw, results):
        return _sanitize_local_paths(scheduler_action_reply)
    if structured_evidence_reply := _structured_tool_evidence_reply_over_ignored_results(
        raw,
        results,
        user_message=user_message,
    ):
        return _sanitize_local_paths(structured_evidence_reply)
    if web_search_reply := _web_search_reply_over_ignored_results(raw, results, user_message=user_message):
        return _sanitize_local_paths(web_search_reply)
    if artifact_completion_reply := _artifact_completion_reply_over_drift(raw, results):
        return _sanitize_local_paths(artifact_completion_reply)
    if empty_reminder_cancel_reply := _empty_reminder_cancel_reply(results):
        return empty_reminder_cancel_reply
    if scheduler_read_reply := _scheduler_read_reply_over_model_reformat(raw, results):
        return _sanitize_local_paths(scheduler_read_reply)
    if numbered_reply := _structured_numbered_choice_reply(results):
        return numbered_reply
    if cron_list_reply := _cron_list_reply_over_empty_reminder_drift(raw, results):
        return cron_list_reply
    if _looks_like_raw_function_markup(raw):
        return safe_raw_tool_payload_replacement(tool_results=results, source=source)
    parsed = _parse_bare_structured_payload(raw)
    if parsed is None:
        return _sanitize_reply_style(
            _sanitize_local_paths(raw),
            account_tool_family=_primary_account_tool_family(results),
        )
    if _has_completed_tool_result(results, "connector_request"):
        return _sanitize_reply_style(
            safe_raw_tool_payload_replacement(tool_results=results, source=source, parsed_payload=parsed),
            account_tool_family=_primary_account_tool_family(results),
        )
    if not _looks_like_raw_tool_payload(parsed, results):
        return _sanitize_reply_style(
            _sanitize_local_paths(raw),
            account_tool_family=_primary_account_tool_family(results),
        )
    return _sanitize_reply_style(
        safe_raw_tool_payload_replacement(tool_results=results, source=source, parsed_payload=parsed),
        account_tool_family=_primary_account_tool_family(results),
    )


def _sanitize_reply_style(text: str | None, *, account_tool_family: str | None = None) -> str | None:
    if text is None:
        return None
    try:
        from nullion.preferences import load_preferences

        preferences = load_preferences()
    except Exception:
        preferences = None
    if str(getattr(preferences, "emoji_level", "") or "").strip().lower() != "none":
        return text
    cleaned = _strip_emoji_characters(text)
    return _restore_account_tool_prefix(cleaned, original=text, account_tool_family=account_tool_family)


def _restore_account_tool_prefix(text: str, *, original: str, account_tool_family: str | None) -> str:
    if account_tool_family == "email":
        prefix = "✉️"
    elif account_tool_family == "calendar":
        prefix = "📅"
    else:
        prefix = ""
    if not prefix:
        return text
    original_stripped = str(original or "").lstrip()
    cleaned_stripped = str(text or "").lstrip()
    if not original_stripped.startswith(prefix) or cleaned_stripped.startswith(prefix):
        return text
    leading = str(text or "")[: len(str(text or "")) - len(cleaned_stripped)]
    return f"{leading}{prefix} {cleaned_stripped}".strip()


def _strip_emoji_characters(text: str) -> str:
    value = str(text or "")
    chars: list[str] = []
    line_start = True
    preserve_line_heading_emoji = False
    for index, char in enumerate(value):
        if line_start:
            preserve_line_heading_emoji = _line_has_leading_emoji_heading(value, index)
        if char in {"\n", "\r"}:
            line_start = True
            preserve_line_heading_emoji = False
            chars.append(char)
            continue
        if line_start and char in {" ", "\t"}:
            chars.append(char)
            continue
        category = unicodedata.category(char)
        if char == "°":
            chars.append(char)
            line_start = False
            continue
        if char in _SEMANTIC_STATUS_SYMBOLS:
            chars.append(char)
            line_start = False
            continue
        if preserve_line_heading_emoji and (category == "So" or char in {"\ufe0f", "\u200d"}):
            chars.append(char)
            continue
        if category == "So" or char in {"\ufe0f", "\u200d"}:
            continue
        chars.append(char)
        line_start = False
    cleaned = "".join(chars)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"(?m)[ \t]+$", "", cleaned)
    return cleaned.strip()


def _line_has_leading_emoji_heading(text: str, index: int) -> bool:
    line = str(text or "")[index:].splitlines()[0] if index < len(str(text or "")) else ""
    stripped = line.lstrip()
    if not stripped:
        return False
    first = stripped[0]
    if unicodedata.category(first) != "So":
        return False
    rest = stripped[1:].lstrip("\ufe0f\u200d").lstrip()
    return rest.startswith(("**", "__", "*", "_"))


def _strip_invisible_tracking_text(text: str) -> str:
    chars: list[str] = []
    for char in str(text or ""):
        category = unicodedata.category(char)
        if category == "Cf" or char in {"\u034f", "\ufeff"}:
            continue
        if category.startswith("C") and char not in {"\n", "\r", "\t"}:
            continue
        chars.append(char)
    cleaned = "".join(chars)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"(?m)[ \t]+$", "", cleaned)
    cleaned = re.sub(r"\n{4,}", "\n\n\n", cleaned)
    return cleaned.strip()


def _repair_collapsed_numbered_list_reply(text: str) -> str:
    """Make model-collapsed numbered lists readable without changing content."""

    value = str(text or "")
    if "\n" in value and not _has_inline_numbered_markers(value):
        return value
    paragraphs = re.split(r"(\n{2,})", value)
    repaired: list[str] = []
    for paragraph in paragraphs:
        if paragraph.startswith("\n"):
            repaired.append(paragraph)
            continue
        repaired.append(_repair_collapsed_numbered_list_paragraph(paragraph))
    return "".join(repaired)


def _has_inline_numbered_markers(text: str) -> bool:
    for line in str(text or "").splitlines():
        if len(_inline_numbered_marker_matches(line)) >= 2:
            return True
    return False


def _inline_numbered_marker_matches(text: str) -> list[re.Match[str]]:
    return list(
        re.finditer(
            r"(?<![\w.])(?P<marker>(?:\d{1,2}\.|\(\d{1,2}\))\s+)(?=[A-Z0-9`*_])",
            str(text or ""),
        )
    )


def _repair_collapsed_numbered_list_paragraph(paragraph: str) -> str:
    matches = _inline_numbered_marker_matches(paragraph)
    if len(matches) < 2:
        return paragraph
    pieces: list[str] = []
    cursor = 0
    for match in matches:
        start = match.start("marker")
        if start > cursor:
            pieces.append(paragraph[cursor:start].rstrip())
        if pieces:
            pieces.append("\n")
        pieces.append(paragraph[start:match.end("marker")])
        cursor = match.end("marker")
    pieces.append(paragraph[cursor:].strip())
    return "".join(pieces).strip()


def _renumber_visible_numbered_list_reply(text: str) -> str:
    """Keep visible ordered lists from starting at an impossible number."""

    value = str(text or "")
    matches = list(
        re.finditer(
            r"(?m)^(?P<indent>\s*)(?P<marker>(?P<paren>\((?P<pnum>\d{1,2})\))|(?P<dnum>\d{1,2})\.)(?P<space>\s+)",
            value,
        )
    )
    if not matches:
        return value
    first_number = int(matches[0].group("pnum") or matches[0].group("dnum") or "1")
    if first_number == 1:
        return value
    pieces: list[str] = []
    cursor = 0
    expected = 1
    for match in matches:
        pieces.append(value[cursor:match.start("marker")])
        replacement = f"({expected})" if match.group("paren") else f"{expected}."
        pieces.append(replacement)
        cursor = match.end("marker")
        expected += 1
    pieces.append(value[cursor:])
    return "".join(pieces)


def _repair_missing_requested_section_replies(
    requested_sections: Iterable[object] | None,
    reply: str,
    results: list[ToolResult],
) -> str:
    sections_request = _requested_visible_sections(requested_sections)
    if not sections_request:
        return reply
    sections = _extract_visible_sections(reply, sections_request)
    if not sections:
        return reply
    repaired: list[str] = []
    for section_id, label in sections_request:
        content = sections.get(section_id)
        if content is None:
            content = _section_repair_content(section_id, results)
        if content is None:
            return reply
        display = _section_display_content(section_id, content, results)
        repaired.append(f"{label}: {_clean_section_content_for_display(display)}")
    return "\n\n".join(repaired)


def _normalize_requested_section_reply_format(
    requested_sections: Iterable[object] | None,
    reply: str,
    results: list[ToolResult],
) -> str:
    sections_request = _requested_visible_sections(requested_sections)
    if not sections_request:
        return reply
    sections = _extract_visible_sections(reply, sections_request)
    if not sections:
        return reply
    normalized: list[str] = []
    for section_id, label in sections_request:
        content = sections.get(section_id)
        if content is None:
            return reply
        display = _section_display_content(section_id, content, results)
        normalized.append(f"{label}: {_clean_section_content_for_display(display)}")
    return "\n\n".join(normalized)


_DEFAULT_SECTION_LABELS = {
    "outcome": "Outcome",
    "tools": "Tools exercised",
    "artifacts": "Artifacts",
    "fallbacks": "Fallbacks",
    "next": "Next",
}
def _requested_visible_sections(requested_sections: Iterable[object] | None) -> list[tuple[str, str]]:
    if requested_sections is None:
        return []
    sections: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in requested_sections:
        section_id: str | None
        label: str | None = None
        if isinstance(item, Mapping):
            section_id = _normalize_requested_section_id(
                item.get("id") or item.get("key") or item.get("section_id") or item.get("section")
            )
            raw_label = item.get("label") or item.get("title")
            if raw_label is not None:
                label = str(raw_label).strip()
        else:
            section_id = _normalize_requested_section_id(item)
        if not section_id or section_id in seen:
            continue
        seen.add(section_id)
        sections.append((section_id, label or _DEFAULT_SECTION_LABELS[section_id]))
    return sections


def _normalize_requested_section_id(value: object) -> str | None:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().casefold()).strip("_")
    return normalized if normalized in _DEFAULT_SECTION_LABELS else None


def _section_display_content(section_id: str, content: str, results: list[ToolResult]) -> str:
    if section_id == "artifacts":
        artifact_summary = _artifact_results_section_summary(results)
        if artifact_summary and _section_content_is_filename_heavy(content):
            return artifact_summary
    return content


def _section_content_is_filename_heavy(content: str) -> bool:
    text = str(content or "")
    filename_matches = re.findall(r"[\w.-]+\.(?:pdf|xlsx|docx|pptx|png|jpe?g|csv|txt|html?)\b", text, flags=re.IGNORECASE)
    return len(filename_matches) >= 2 or any(len(match) > 32 for match in filename_matches)


def _artifact_results_section_summary(results: list[ToolResult]) -> str | None:
    suffixes: list[str] = []
    for result in results:
        if str(getattr(result, "status", "") or "").strip().casefold() != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        values: list[object] = []
        for key in ("artifact_path", "path", "output_path"):
            value = output.get(key)
            if value:
                values.append(value)
        artifact_paths = output.get("artifact_paths")
        if isinstance(artifact_paths, (list, tuple)):
            values.extend(artifact_paths)
        for value in values:
            suffix = Path(str(value or "")).suffix.lower().lstrip(".")
            if suffix:
                suffixes.append(suffix)
    suffixes = list(dict.fromkeys(suffixes))
    if not suffixes:
        return None
    labels = [_artifact_suffix_label(suffix) for suffix in suffixes]
    return f"{_join_human_labels(labels)} attached."


def _artifact_suffix_label(suffix: str) -> str:
    return {
        "pdf": "PDF",
        "xlsx": "spreadsheet",
        "xls": "spreadsheet",
        "csv": "CSV",
        "docx": "document",
        "pptx": "presentation",
        "png": "image",
        "jpg": "image",
        "jpeg": "image",
        "html": "HTML file",
        "htm": "HTML file",
        "txt": "text file",
    }.get(suffix.lower(), f".{suffix.lower()} file")


def _join_human_labels(labels: list[str]) -> str:
    unique = list(dict.fromkeys(label for label in labels if label))
    if not unique:
        return "File"
    if len(unique) == 1:
        return unique[0]
    if len(unique) == 2:
        return f"{unique[0]} and {unique[1]}"
    return f"{', '.join(unique[:-1])}, and {unique[-1]}"


def _clean_section_content_for_display(content: str) -> str:
    value = str(content or "").strip()
    value = re.sub(r"`([^`\n]{1,160})`", r"\1", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value

def _normalized_section_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(label or "").casefold()).strip()


def _section_heading_pattern(labels: list[str]) -> re.Pattern[str] | None:
    escaped = [re.escape(label.strip()) for label in labels if label.strip()]
    if not escaped:
        return None
    return re.compile(rf"(?im)^(?P<label>{'|'.join(escaped)})(?:\s*:\s*|\s*$)")


def _extract_visible_sections(reply: str, sections_request: list[tuple[str, str]]) -> dict[str, str]:
    labels = [label for _section_id, label in sections_request]
    label_to_id = {_normalized_section_label(label): section_id for section_id, label in sections_request}
    pattern = _section_heading_pattern(labels)
    if pattern is None:
        return {}
    text = str(reply or "")
    matches = list(pattern.finditer(text))
    sections: dict[str, str] = {}
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        content = text[start:end].strip()
        if content:
            section_id = label_to_id.get(_normalized_section_label(match.group("label")))
            if section_id:
                sections[section_id] = content
    return sections


def _section_reply_outer_text(reply: str, labels: list[str]) -> tuple[str, str]:
    pattern = _section_heading_pattern(labels)
    if pattern is None:
        return "", ""
    text = str(reply or "")
    matches = list(pattern.finditer(text))
    if not matches:
        return text, ""
    return text[: matches[0].start()], ""


def _section_repair_content(section_id: str, results: list[ToolResult]) -> str | None:
    if section_id != "tools":
        return None
    return _tool_results_section_summary(results)


def _tool_results_section_summary(results: list[ToolResult]) -> str | None:
    completed: list[str] = []
    failed: list[str] = []
    failed_seen: set[str] = set()
    skipped = {"request_tool_scope"}
    for result in results:
        name = str(getattr(result, "tool_name", "") or "").strip()
        if not name or name in skipped:
            continue
        status = str(getattr(result, "status", "") or "").strip().casefold()
        if status == "completed":
            completed.append(name)
        elif status in {"failed", "denied", "timeout", "timed_out"}:
            failed.append(name)
            failed_seen.add(name)
    completed = list(dict.fromkeys(completed))
    completed_set = set(completed)
    failed = [name for name in dict.fromkeys(failed) if name not in completed_set]
    parts: list[str] = []
    if completed:
        parts.append(", ".join(f"{name} (retried)" if name in failed_seen else name for name in completed))
    if failed:
        parts.append("failed: " + ", ".join(failed))
    return "; ".join(parts) if parts else None


def _has_completed_tool_result(results: Iterable[ToolResult], tool_name: str) -> bool:
    for result in results:
        if getattr(result, "tool_name", None) == tool_name and str(getattr(result, "status", None)) == "completed":
            return True
    return False


def _tool_result_failed(result: ToolResult) -> bool:
    return str(getattr(result, "status", "") or "").strip().lower() == "failed"


def _tool_result_reason(result: ToolResult) -> str:
    output = getattr(result, "output", None)
    if not isinstance(output, Mapping):
        return ""
    return str(output.get("reason") or "").strip().lower()


def _browser_recovery_should_preserve_model_reply(text: str, results: list[ToolResult]) -> bool:
    if not _normalized_reply_text(text):
        return False
    if _reply_is_grounded_in_completed_browser_evidence(text, results):
        return True
    unknown_browser_tool_recovered = any(
        str(getattr(result, "tool_name", "") or "").startswith("browser_")
        and _tool_result_failed(result)
        and _tool_result_reason(result) == "unknown_tool"
        for result in results
    ) and _has_completed_tool_result(results, "browser_extract_text")
    if unknown_browser_tool_recovered:
        return True
    connector_recovered_to_browser = (
        any(
            getattr(result, "tool_name", None) == "connector_request"
            and _tool_result_failed(result)
            for result in results
        )
        and _has_completed_tool_result(results, "browser_navigate")
        and not _browser_form_action_failed(results)
        and not _latest_unverified_browser_assert_page_state(results)
    )
    return connector_recovered_to_browser


def _reply_is_grounded_in_completed_browser_evidence(text: str, results: list[ToolResult]) -> bool:
    normalized = _normalized_reply_text(text).casefold()
    if not normalized:
        return False
    if "live-search blocker" in normalized or "verified result records" in normalized:
        return False
    if _browser_reply_claims_stale_result_list(normalized):
        return False
    for result in results:
        if not _tool_result_completed(result):
            continue
        if result.tool_name == "browser_extract_text":
            output = result.output if isinstance(result.output, Mapping) else {}
            extracted = _browser_text_value_from_output(output, max_chars=20000)
            catalog_allowed = _browser_raw_extract_catalog_records_allowed(results, extracted_text=extracted)
            if _browser_completed_interaction_count(results) and not catalog_allowed:
                if (
                    not _browser_raw_extract_has_substantive_result_evidence(extracted, allow_catalog_pairs=False)
                    or _browser_catalog_signal_score(str(extracted or "")) >= 3
                ):
                    continue
            if extracted and (
                _reply_mentions_browser_extract_evidence(text, extracted)
                or _reply_has_token_overlap_with_browser_text(text, extracted)
            ):
                return True
            continue
        if result.tool_name == "browser_extract_items":
            assertion = _latest_unverified_browser_assert_page_state(results)
            if assertion is not None:
                missing = _browser_assertion_missing_required_labels(assertion)
                forbidden = _browser_assertion_forbidden_found_labels(assertion)
                if forbidden or (missing and not _browser_items_satisfy_labels(results, missing)):
                    continue
            output = result.output if isinstance(result.output, Mapping) else {}
            if _reply_has_token_overlap_with_browser_text(text, _browser_visible_evidence_text_from_mapping(output)):
                return True
            continue
        if result.tool_name in {"browser_run_js", "browser_assert_page_state"}:
            output = result.output if isinstance(result.output, Mapping) else {}
            if _reply_has_token_overlap_with_browser_text(text, _browser_visible_evidence_text_from_mapping(output)):
                return True
    return False


def _reply_has_token_overlap_with_browser_text(reply: object, evidence: object) -> bool:
    reply_tokens = _browser_evidence_tokens(reply)
    evidence_tokens = _browser_evidence_tokens(evidence)
    if not reply_tokens or not evidence_tokens:
        return False
    matched = reply_tokens & evidence_tokens
    reply_amounts = set(_CURRENCY_AMOUNT_RE.findall(str(reply or "")))
    evidence_amounts = set(_CURRENCY_AMOUNT_RE.findall(str(evidence or "")))
    if matched and reply_amounts & evidence_amounts:
        return True
    if len(matched) >= 3:
        return True
    distinctive = {token for token in matched if len(token) >= 5}
    if len(distinctive) >= 2:
        return True
    if len(matched) >= 2 and any(token in {"open", "opened", "available"} for token in matched):
        return True
    return False


_BROWSER_EVIDENCE_TOKEN_STOPWORDS = frozenset(
    {
        "about",
        "after",
        "again",
        "available",
        "browser",
        "could",
        "current",
        "found",
        "from",
        "have",
        "page",
        "ready",
        "record",
        "records",
        "request",
        "requested",
        "result",
        "results",
        "search",
        "source",
        "state",
        "still",
        "that",
        "this",
        "with",
    }
)


def _browser_evidence_tokens(value: object) -> set[str]:
    normalized = _normalized_reply_text(value).casefold()
    tokens = {
        token
        for token in re.findall(r"[\w.-]+", normalized, flags=re.UNICODE)
        if len(token) >= 3 and token not in _BROWSER_EVIDENCE_TOKEN_STOPWORDS
    }
    return tokens


def _browser_reply_claims_stale_result_list(normalized_text: str) -> bool:
    return bool(
        normalized_text.startswith("search leads i found")
        or normalized_text.startswith("top matches i found")
        or normalized_text.startswith("results i found")
    )


def _browser_reply_is_nonverifying_explanation(text: object) -> bool:
    normalized = _normalized_reply_text(text).casefold()
    if not normalized:
        return False
    if _browser_reply_claims_stale_result_list(normalized):
        return False
    if "source-backed points" in normalized:
        return False
    return bool(
        re.search(r"\b(?:could not|couldn't|cannot|can't)\s+verify\b", normalized)
        or "not have verified" in normalized
        or "do not have verified" in normalized
    )


def _browser_grounded_reply_should_pass_through(text: object, results: list[ToolResult]) -> bool:
    if not _browser_tools_attempted(results):
        return False
    if _browser_form_action_failed(results):
        return False
    if _browser_reply_is_nonverifying_explanation(text):
        return False
    normalized = _normalized_reply_text(text).casefold()
    if not normalized or "live-search blocker" in normalized:
        return False
    for extracted in _completed_browser_extract_texts(results, max_chars=2000):
        normalized_extract = _normalized_reply_text(extracted).casefold()
        if (
            len(normalized_extract) >= 4
            and normalized_extract in normalized
            and normalized_extract != normalized
            and len(normalized) > len(normalized_extract) + 12
        ):
            return True
    return False


def _browser_visible_evidence_text_from_mapping(value: Mapping[str, Any]) -> str:
    parts: list[str] = []

    def collect(candidate: object, *, depth: int = 0) -> None:
        if depth > 4:
            return
        if isinstance(candidate, str):
            text = _compact_browser_extract_line(candidate)
            if text:
                parts.append(text)
            return
        if isinstance(candidate, Mapping):
            for key in (
                "title",
                "name",
                "compact_text",
                "summary",
                "text",
                "state",
                "result",
                "records",
                "items",
                "results",
                "fields",
                "price",
                "price_text",
                "url",
            ):
                if key in candidate:
                    collect(candidate.get(key), depth=depth + 1)
            return
        if isinstance(candidate, list):
            for item in candidate[:8]:
                collect(item, depth=depth + 1)

    collect(value)
    return "\n".join(parts)


def _cron_list_reply_over_empty_reminder_drift(text: str, results: list[ToolResult]) -> str | None:
    if any(
        _tool_result_completed(result) and result.tool_name in _SCHEDULER_ACTION_TOOLS
        for result in results
    ):
        return None
    cron_result: ToolResult | None = None
    reminder_result_seen_after_crons = False
    for result in results:
        if not _tool_result_completed(result) or not isinstance(result.output, dict):
            continue
        if result.tool_name == "list_crons" and isinstance(result.output.get("crons"), list):
            cron_result = result
            reminder_result_seen_after_crons = False
            continue
        if cron_result is not None and result.tool_name == "list_reminders":
            reminders = result.output.get("reminders")
            count = result.output.get("count")
            if reminders == [] or count == 0:
                reminder_result_seen_after_crons = True
    if cron_result is None or not reminder_result_seen_after_crons:
        return None
    cron_output = cron_result.output if isinstance(cron_result.output, dict) else {}
    crons = cron_output.get("crons")
    if not isinstance(crons, list) or not crons:
        return None
    normalized_reply = _normalized_reply_text(text)
    for cron in crons:
        if not isinstance(cron, dict):
            continue
        for key in ("name", "display_name"):
            value = cron.get(key)
            if isinstance(value, str) and value.strip() and _normalized_reply_text(value) in normalized_reply:
                return None
    message = cron_output.get("message")
    if isinstance(message, str) and message.strip():
        return message.strip()
    return None


def _empty_reminder_cancel_reply(results: list[ToolResult]) -> str | None:
    delete_requested = False
    reminders_empty = False
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if result.tool_name == "request_tool_scope":
            required = result.output.get("required_tool_names")
            available = result.output.get("available_tools")
            if (
                isinstance(required, list)
                and "delete_reminder" in {str(name) for name in required}
            ) or (
                isinstance(available, list)
                and "delete_reminder" in {str(name) for name in available}
                and "set_reminder" not in {str(name) for name in available}
            ):
                delete_requested = True
            continue
        if result.tool_name == "delete_reminder":
            return None
        if result.tool_name == "list_reminders":
            reminders = result.output.get("reminders")
            count = result.output.get("count")
            if reminders == [] or count == 0:
                reminders_empty = True
    if delete_requested and reminders_empty:
        return "No pending reminders to cancel."
    return None


def _scheduler_read_reply_over_model_reformat(text: str, results: list[ToolResult]) -> str | None:
    if any(
        result.tool_name in _SCHEDULER_ACTION_TOOLS and result.status == "completed"
        for result in results
    ):
        return None
    completed_cron_lists = [
        result
        for result in results
        if result.tool_name == "list_crons"
        and result.status == "completed"
        and isinstance(result.output, dict)
        and isinstance(result.output.get("crons"), list)
    ]
    if not completed_cron_lists:
        return None
    cron_result = completed_cron_lists[-1]
    message = cron_result.output.get("message")
    if not isinstance(message, str) or not message.strip():
        return None
    crons = cron_result.output.get("crons")
    if isinstance(crons, list) and crons and not _cron_reply_mentions_scheduler_list(text, crons):
        return None
    return _compact_cron_list_reply(cron_result)


def _cron_reply_mentions_scheduler_list(text: str, crons: list[object]) -> bool:
    normalized_reply = _normalized_reply_text(text)
    if not normalized_reply:
        return False
    mentioned = 0
    comparable = 0
    for cron in crons:
        if not isinstance(cron, dict):
            continue
        names = []
        for key in ("name", "display_name"):
            value = cron.get(key)
            if isinstance(value, str) and value.strip():
                names.append(value)
        if not names:
            continue
        comparable += 1
        if any(_normalized_reply_text(name) in normalized_reply for name in names):
            mentioned += 1
    if comparable <= 0:
        return False
    if comparable <= 3:
        return mentioned == comparable
    threshold = max(3, math.ceil(comparable * 0.8))
    return mentioned >= threshold


def _compact_cron_list_reply(result: ToolResult) -> str | None:
    output = result.output if isinstance(result.output, dict) else {}
    crons = output.get("crons")
    if not isinstance(crons, list):
        return None
    message = output.get("message")
    if isinstance(message, str) and message.strip():
        return message.strip()
    return "No crons scheduled." if not crons else None


def _scheduler_action_reply_over_read_drift(text: str, results: list[ToolResult]) -> str | None:
    action_message = ""
    for result in reversed(results):
        if result.status != "completed" or result.tool_name not in _SCHEDULER_ACTION_TOOLS:
            continue
        output = result.output if isinstance(result.output, dict) else {}
        message = output.get("message")
        if isinstance(message, str) and message.strip():
            action_message = message.strip()
            break
    if not action_message:
        return None
    normalized_reply = _normalized_reply_text(text)
    normalized_action = _normalized_reply_text(action_message)
    if normalized_action and normalized_action in normalized_reply:
        return None
    for result in results:
        if result.status != "completed" or result.tool_name not in _SCHEDULER_READ_TOOLS:
            continue
        output = result.output if isinstance(result.output, dict) else {}
        message = output.get("message")
        if not isinstance(message, str) or not message.strip():
            continue
        if _normalized_reply_text(message) == normalized_reply:
            return action_message
    return None


def _format_action_receipt(output: dict[str, object]) -> str | None:
    receipt = output.get("action_receipt")
    if not isinstance(receipt, dict):
        return None
    summary = str(receipt.get("summary") or output.get("message") or "").strip()
    if not summary:
        return None
    lines = [summary]
    details = receipt.get("details")
    if isinstance(details, list):
        clean_details = [str(detail).strip() for detail in details if str(detail or "").strip()]
        if clean_details:
            lines.extend(["", "Details:"])
            lines.extend(f"- {detail}" for detail in clean_details)
    return "\n".join(lines).strip()


def _result_foreground_reply_suppressed(result: ToolResult) -> bool:
    output = result.output if isinstance(result.output, dict) else {}
    return output.get("foreground_reply_suppressed") is True


def _result_is_deferred_background_receipt(result: ToolResult) -> bool:
    output = result.output if isinstance(result.output, dict) else {}
    return str(output.get("delivery_status") or output.get("cron_delivery_status") or "").strip() == "deferred"


def _reply_contains_action_receipt(text: str, receipt_text: str) -> bool:
    normalized_reply = _normalized_reply_text(text)
    normalized_receipt = _normalized_reply_text(receipt_text)
    if normalized_receipt and normalized_receipt in normalized_reply:
        return True
    first_line = receipt_text.splitlines()[0] if receipt_text else ""
    normalized_first = _normalized_reply_text(first_line)
    if normalized_first and normalized_first in normalized_reply:
        detail_lines = [
            line.removeprefix("-").strip()
            for line in receipt_text.splitlines()
            if line.strip().startswith("-")
        ]
        if not detail_lines:
            return True
        return any(_normalized_reply_text(detail) in normalized_reply for detail in detail_lines)
    return False


def _action_receipt_reply_over_drift(text: str, results: list[ToolResult]) -> str | None:
    scheduler_receipts: list[str] = []
    for result in results:
        if result.status != "completed" or result.tool_name not in _SCHEDULER_ACTION_TOOLS:
            continue
        if _result_foreground_reply_suppressed(result) or _result_is_deferred_background_receipt(result):
            continue
        if not isinstance(result.output, dict):
            continue
        formatted = _format_action_receipt(result.output)
        if formatted:
            scheduler_receipts.append(formatted)
    if scheduler_receipts:
        missing_receipts = [
            receipt for receipt in scheduler_receipts
            if not _reply_contains_action_receipt(text, receipt)
        ]
        if missing_receipts:
            return "\n\n".join(scheduler_receipts)

    receipt_text = ""
    receipt_tool_name = ""
    for result in reversed(results):
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if _result_foreground_reply_suppressed(result) or _result_is_deferred_background_receipt(result):
            continue
        if result.tool_name in _ACTION_RECEIPT_REPLY_EXCLUDED_TOOLS:
            continue
        formatted = _format_action_receipt(result.output)
        if formatted:
            receipt_text = formatted
            receipt_tool_name = result.tool_name
            break
    if not receipt_text or _reply_contains_action_receipt(text, receipt_text):
        return None

    normalized_reply = _normalized_reply_text(text)
    if receipt_tool_name not in _SCHEDULER_ACTION_TOOLS:
        return None
    for result in results:
        if result.status != "completed" or result.tool_name not in _SCHEDULER_READ_TOOLS:
            continue
        output = result.output if isinstance(result.output, dict) else {}
        message = output.get("message")
        if isinstance(message, str) and message.strip() and _normalized_reply_text(message) == normalized_reply:
            return receipt_text
    if not normalized_reply:
        return receipt_text
    return None


def _artifact_completion_reply_over_drift(text: str, results: list[ToolResult]) -> str | None:
    requested_extensions = _requested_artifact_extensions_from_results(results)
    if not requested_extensions:
        return None
    paths = _completed_artifact_paths_for_extensions(results, requested_extensions)
    if not paths:
        return None
    path_extensions = {Path(path).suffix.lower() for path in paths}
    if any(extension not in path_extensions for extension in requested_extensions):
        return None
    normalized_reply = _normalized_reply_text(text)
    if any(_normalized_reply_text(Path(path).name) in normalized_reply for path in paths):
        return None
    labels = [_artifact_suffix_label(Path(path).suffix.lower().lstrip(".")) for path in paths]
    joined = _join_human_labels(labels)
    lines = [f"Created the requested {joined}:"]
    for path in paths[:3]:
        lines.append(f"- `{Path(path).name}`")
    return "\n".join(lines)


def _browser_extract_text_reply_over_assertion_drift(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    extracted_text = _latest_completed_browser_extract_text(results)
    if not extracted_text:
        return None
    assertion = _latest_unverified_browser_assert_page_state(results)
    if assertion is None:
        return None
    if _reply_mentions_browser_extract_evidence(text, extracted_text):
        stripped = _strip_browser_assertion_meta_drift(text, assertion)
        if stripped:
            return stripped
        if not _reply_echoes_tool_failure(text, assertion):
            return None
    if not _reply_echoes_tool_failure(text, assertion):
        if assertion.tool_name != "browser_wait_for":
            return None
    return _browser_extract_reply_from_raw_text(
        extracted_text,
        results,
        user_message=user_message,
    )


_CURRENCY_AMOUNT_RE = re.compile(
    r"(?<![\w])(?:[$€£₹]\s?\d[\d,]*(?:\.\d{1,2})?|\d[\d,]*(?:\.\d{1,2})?\s?(?:USD|EUR|GBP))(?![\w])",
    flags=re.IGNORECASE,
)
_QUOTE_PAGE_TITLE_RE = re.compile(r"^(?P<name>.+?)\s+\((?P<symbol>[A-Z][A-Z0-9.]{1,7})\)\s*$")
_QUOTE_PAGE_SYMBOL_EXCHANGE_RE = re.compile(r"^(?P<symbol>[A-Z][A-Z0-9.]{1,7})\s*:\s*[A-Z][A-Z0-9.]{1,10}$")
_QUOTE_PAGE_PRICE_RE = re.compile(r"^\d{1,4}(?:,\d{3})*(?:\.\d{1,4})?$")


def _browser_text_value_from_output(output: Mapping[str, Any], *, max_chars: int) -> str | None:
    for key in ("text", "content", "result", "preview"):
        value = output.get(key)
        text = _browser_text_value_from_any(value)
        if text:
            return text[:max_chars]
    return None


def _browser_text_value_from_any(value: object, *, _depth: int = 0) -> str | None:
    if _depth > 3:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        parsed = _browser_text_value_from_json_string(text, _depth=_depth + 1)
        return parsed or text
    if isinstance(value, Mapping):
        for key in ("text", "content", "result", "body", "preview"):
            nested = _browser_text_value_from_any(value.get(key), _depth=_depth + 1)
            if nested:
                return nested
        return None
    if isinstance(value, list):
        parts = [
            nested
            for item in value[:20]
            if (nested := _browser_text_value_from_any(item, _depth=_depth + 1))
        ]
        return "\n".join(parts) if parts else None
    return None


def _browser_text_value_from_json_string(value: str, *, _depth: int) -> str | None:
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        parsed = json.loads(stripped)
    except Exception:
        return None
    return _browser_text_value_from_any(parsed, _depth=_depth)


def _completed_browser_extract_texts(results: list[ToolResult], *, max_chars: int = 8000) -> list[str]:
    extracts: list[str] = []
    for result in results:
        if result.tool_name != "browser_extract_text" or not _tool_result_completed(result):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if value := _browser_text_value_from_output(output, max_chars=max_chars):
            extracts.append(value.strip())
    return extracts


def _completed_browser_extract_item_records(results: list[ToolResult]) -> list[Mapping[str, Any]]:
    records: list[Mapping[str, Any]] = []
    for result in results:
        if result.tool_name != "browser_extract_items" or not _tool_result_completed(result):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        for key in ("items", "results", "records", "entries"):
            values = output.get(key)
            if not isinstance(values, list):
                continue
            for item in values:
                if isinstance(item, Mapping):
                    records.append(item)
            if records:
                break
    return records


def _completed_web_search_result_records(results: list[ToolResult]) -> list[Mapping[str, Any]]:
    records: list[Mapping[str, Any]] = []
    for result in results:
        if result.tool_name != "web_search" or not _tool_result_completed(result):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        query = _compact_account_text(output.get("query"))
        values = output.get("results")
        if not isinstance(values, list):
            continue
        for item in values:
            if isinstance(item, Mapping):
                if query:
                    copied = dict(item)
                    copied.setdefault("_query", query)
                    records.append(copied)
                else:
                    records.append(item)
    return records


_CONCRETE_RECORD_FIELD_KEYS = (
    "record_type",
    "entity_type",
    "kind",
    "address",
    "location",
    "rating",
    "price",
    "distance",
    "phone",
    "date",
    "start",
    "end",
    "price_text",
    "artifact_path",
    "path",
    "filename",
    "image_url",
    "image",
    "thumbnail",
)

_ACTION_URL_FIELD_KEYS = (
    "action_url",
    "target_url",
    "source_url",
    "website_url",
    "url",
    "href",
    "link",
)


def _rank_structured_output_items(
    items: list[Mapping[str, Any]],
    *,
    kind: str,
) -> list[Mapping[str, Any]]:
    scored = [
        (_structured_output_item_quality_score(item, kind=kind), index, item)
        for index, item in enumerate(items)
    ]
    if not any(score > 0 for score, _index, _item in scored):
        return []
    concrete = [(score, index, item) for score, index, item in scored if score > 0]
    concrete.sort(key=lambda value: (-value[0], value[1]))
    return [item for _score, _index, item in concrete]


def _browser_extract_item_query_text(item: Mapping[str, Any]) -> str:
    return " ".join(
        _compact_account_text(item.get(key))
        for key in ("compact_text", "summary", "title", "name", "link_text", "text")
        if _compact_account_text(item.get(key))
    )


def _browser_extract_items_ranked_for_user_message(
    records: list[Mapping[str, Any]],
    *,
    user_message: str | None,
) -> list[Mapping[str, Any]]:
    ranked = _rank_structured_output_items(records, kind="browser_item")
    if not ranked:
        return []
    query_terms = _browser_query_terms(user_message)
    if not query_terms:
        return ranked
    scored_ranked: list[tuple[int, int, Mapping[str, Any]]] = []
    for index, item in enumerate(ranked):
        normalized_text = _normalized_reply_text(_browser_extract_item_query_text(item)).casefold()
        matched_terms = {term for term in query_terms if term and term in normalized_text}
        scored_ranked.append((len(matched_terms), index, item))
    if not any(score > 0 for score, _index, _item in scored_ranked):
        return ranked
    minimum_matches = _minimum_browser_item_query_matches(query_terms)
    eligible = [(score, index, item) for score, index, item in scored_ranked if score >= minimum_matches]
    if not eligible:
        return []
    eligible.sort(key=lambda value: (-value[0], value[1]))
    return [item for _score, _index, item in eligible]


def _structured_output_item_quality_score(item: Mapping[str, Any], *, kind: str) -> int:
    if kind == "browser_item" and not _browser_extract_item_has_substantive_evidence(item):
        return 0
    score = 0
    concrete_score = 0
    for key in _CONCRETE_RECORD_FIELD_KEYS:
        value = item.get(key)
        if key in {"image_url", "image", "thumbnail"} and _same_normalized_url(value, item.get("url")):
            continue
        if value not in (None, "", [], {}):
            concrete_score += 2
    fields = item.get("fields")
    if isinstance(fields, Mapping):
        concrete_score += 2 * sum(1 for value in fields.values() if value not in (None, "", [], {}))
    elif isinstance(fields, list):
        concrete_score += 2 * sum(1 for value in fields if value not in (None, "", [], {}))
    score += concrete_score
    if concrete_score > 0 and _structured_output_item_action_links(item):
        score += 3
    if concrete_score > 0 and _structured_output_item_url_path_depth(item) >= 2:
        score += 2
    return score


def _browser_extract_item_has_substantive_evidence(item: Mapping[str, Any]) -> bool:
    for key in ("price", "price_text", "rating", "distance", "address", "location", "phone", "status", "hours"):
        if item.get(key) not in (None, "", [], {}):
            return True
    for key in ("fields", "price_candidates"):
        value = item.get(key)
        if isinstance(value, Mapping) and any(field not in (None, "", [], {}) for field in value.values()):
            return True
        if isinstance(value, list) and any(field not in (None, "", [], {}) for field in value):
            return True
    return False


def _structured_tool_evidence_reply_over_ignored_results(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    if _reply_contains_completed_scheduler_action_message(text, results):
        return None
    if _has_completed_deliverable_artifact_result(results):
        return None
    if _browser_recovery_should_preserve_model_reply(text, results):
        return None
    if _parse_bare_structured_payload(text) is not None:
        return None
    if missing_state_reply := _browser_missing_required_state_reply_over_structured_evidence(
        results,
        user_message=user_message,
    ):
        return missing_state_reply
    sections = _structured_tool_evidence_sections(
        results,
        user_message=user_message,
        prefer_browser_source_heading=_browser_structured_items_should_use_source_heading(text, results),
    )
    if not sections:
        return None
    excluded_tokens = _structured_evidence_excluded_tokens(user_message, results)
    if any(
        _structured_tool_evidence_section_covered(
            text,
            lines,
            items,
            excluded_tokens=excluded_tokens,
        )
        for _heading, lines, items in sections
    ):
        return None
    if _primary_account_tool_family(results) is not None and not _account_structured_evidence_repair_needed(text, results):
        return None
    rendered: list[str] = []
    for heading, lines, _items in sections[:3]:
        if not lines:
            continue
        rendered.append(f"{heading}\n\n" + "\n\n".join(lines[:4]))
    if not rendered:
        return None
    return "\n\n".join(rendered)


def _reply_contains_completed_scheduler_action_message(text: str, results: list[ToolResult]) -> bool:
    normalized_reply = _normalized_reply_text(text)
    if not normalized_reply:
        return False
    for result in reversed(results):
        if result.status != "completed" or result.tool_name not in _SCHEDULER_ACTION_TOOLS:
            continue
        output = result.output if isinstance(result.output, dict) else {}
        message = output.get("message")
        if isinstance(message, str) and message.strip():
            normalized_message = _normalized_reply_text(message)
            if normalized_message and normalized_message in normalized_reply:
                return True
        receipt_text = _format_action_receipt(output)
        if receipt_text and _reply_contains_action_receipt(text, receipt_text):
            return True
    return False


_ACCOUNT_STRUCTURED_REPAIR_PHRASE_RE = re.compile(
    r"\b(?:message\s+id|if\s+you\s+want|can\s+check|do\s+not\s+(?:currently\s+)?have|don't\s+(?:currently\s+)?have|not\s+available|cannot\s+send|can't\s+send)\b",
    flags=re.IGNORECASE,
)


def _account_structured_evidence_repair_needed(text: str, results: list[ToolResult]) -> bool:
    if not _normalized_reply_text(text):
        return True
    if _ACCOUNT_STRUCTURED_REPAIR_PHRASE_RE.search(str(text or "")):
        return True
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if _account_tool_family(result.tool_name) is None:
            continue
        metadata = result.output.get("reply_metadata")
        if isinstance(metadata, Mapping) and metadata.get("reply_contains_internal_tool_state") is True:
            return True
    return False


def _structured_tool_evidence_sections(
    results: list[ToolResult],
    *,
    user_message: str | None,
    prefer_browser_source_heading: bool = False,
) -> list[tuple[str, list[str], list[Mapping[str, Any]]]]:
    sections: list[tuple[str, list[str], list[Mapping[str, Any]]]] = []
    browser_item_records = _completed_browser_extract_item_records(results)
    browser_items = _browser_extract_items_ranked_for_user_message(
        browser_item_records,
        user_message=user_message,
    )
    if browser_items:
        browser_lines = _structured_output_lines(browser_items, kind="browser_item")
        if browser_lines:
            heading = "Source-backed points" if prefer_browser_source_heading else "Top matches I found"
            sections.append((heading, browser_lines, browser_items))
    elif not browser_item_records:
        web_items = _rank_structured_output_items(
            _completed_web_search_result_records(results),
            kind="web_search",
        )
        if web_items:
            web_lines = _structured_output_lines(web_items, kind="web_search")
            if web_lines:
                sections.append(("Top matches I found", web_lines, web_items))

    for result in reversed(results):
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if result.tool_name in {
            "connector_request",
            "web_search",
            "browser_extract_items",
            "browser_extract_text",
            "email_send",
        }:
            continue
        if result.tool_name == "email_read":
            summary = _email_read_summary(result.output)
            items = _structured_evidence_items_from_mapping(result.output, preferred_key="message")
            lines = _summary_to_bullet_lines(summary)
            if lines:
                sections.append(("Relevant email", lines, items or [result.output]))
            continue
        if result.tool_name == "email_attachment_read":
            summary = _email_attachment_read_summary(result.output)
            lines = _summary_to_bullet_lines(summary)
            if lines:
                sections.append(("Email attachment", lines, [result.output]))
            continue
        if result.tool_name == "email_search":
            summary = _email_search_summary(result.output)
            items = _structured_evidence_items_from_mapping(result.output)
            lines = _structured_output_lines(items, kind="email")
            if not lines:
                lines = _summary_to_bullet_lines(summary)
            if lines:
                sections.append(("Relevant email results", lines, items or [result.output]))
            continue
        if result.tool_name == "calendar_list":
            items = _structured_evidence_items_from_mapping(result.output)
            lines = _structured_output_lines(items, kind="calendar")
            if not lines:
                summary = _calendar_list_summary(result.output, user_message=user_message)
                lines = _summary_to_bullet_lines(summary)
            if lines:
                sections.append(("Relevant calendar results", lines, items or [result.output]))
            continue
        if result.tool_name == "file_search":
            summary = _file_search_payload_summary([result])
            items = _structured_evidence_items_from_mapping(result.output)
            lines = _summary_to_bullet_lines(summary)
            if lines:
                sections.append(("Files I found", lines, items or [result.output]))
            continue
        generic_items = _structured_evidence_items_from_mapping(result.output)
        if not generic_items:
            continue
        generic_kind = "generic"
        heading = "Results I found"
        if result.tool_name.startswith("browser_"):
            generic_items = _rank_structured_output_items(generic_items, kind="browser_item")
            generic_kind = "browser_item"
            heading = "Source-backed points"
        generic_lines = _structured_output_lines(generic_items, kind=generic_kind)
        if generic_lines:
            sections.append((heading, generic_lines, generic_items))
    return sections


def _browser_structured_items_should_use_source_heading(text: str, results: list[ToolResult]) -> bool:
    records = _completed_browser_extract_item_records(results)
    if not records:
        return False
    if _latest_unverified_browser_assert_page_state(results) is not None:
        return True
    normalized = _normalized_reply_text(text).casefold()
    if "live-search blocker" in normalized or "verified result records" in normalized:
        return True
    if _browser_form_action_failed(results):
        return True
    has_completed_search = any(result.tool_name == "web_search" and _tool_result_completed(result) for result in results)
    has_action_links = any(_browser_item_has_explicit_action_link(item) for item in records)
    if not has_completed_search and not has_action_links:
        return True
    return False


def _browser_item_has_explicit_action_link(item: Mapping[str, Any]) -> bool:
    if _compact_account_text(item.get("link_text")) and any(
        _compact_account_text(item.get(key))
        for key in _ACTION_URL_FIELD_KEYS
    ):
        return True
    for key in ("links", "actions"):
        values = item.get(key)
        if not isinstance(values, list):
            continue
        if any(isinstance(value, Mapping) and _structured_output_item_action_links(value) for value in values):
            return True
    return False


def _structured_evidence_items_from_mapping(
    output: Mapping[str, Any],
    *,
    preferred_key: str | None = None,
) -> list[Mapping[str, Any]]:
    if preferred_key:
        value = output.get(preferred_key)
        if isinstance(value, Mapping):
            return [value]
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
    preview = output.get("preview")
    if isinstance(preview, str) and preview.strip():
        try:
            parsed_preview = json.loads(preview)
        except Exception:
            parsed_preview = None
        if isinstance(parsed_preview, Mapping):
            preview_items = _structured_evidence_items_from_mapping(parsed_preview, preferred_key=preferred_key)
            if preview_items:
                return preview_items
    items: list[Mapping[str, Any]] = []
    for key in (
        "selected_results",
        "selected_records",
        "selected_items",
        "results",
        "items",
        "records",
        "entries",
        "result",
        "messages",
        "events",
        "files",
        "matches",
    ):
        value = output.get(key)
        if not isinstance(value, list):
            continue
        items.extend(item for item in value if isinstance(item, Mapping))
        if items:
            break
    return items


def _summary_to_bullet_lines(summary: object) -> list[str]:
    lines: list[str] = []
    for raw_line in str(summary or "").splitlines():
        line = _clip_browser_extract_item_text(raw_line.strip().lstrip("-• "), max_chars=220)
        if line:
            lines.append(f"- {line}")
    return _deduped_evidence_lines(lines)


def _deduped_evidence_lines(values: Iterable[str]) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for value in values:
        line = str(value or "").strip()
        if not line:
            continue
        key = _normalized_reply_text(line[:180])
        if not key or key in seen:
            continue
        seen.add(key)
        lines.append(line)
        if len(lines) >= 5:
            break
    return lines


def _generic_structured_evidence_line(item: Mapping[str, Any]) -> str:
    return _structured_output_item_line(1, item, kind="generic").removeprefix("1. ").strip()


def _structured_output_lines(
    items: Iterable[Mapping[str, Any]],
    *,
    kind: str,
    limit: int = 4,
) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for item in items:
        line = _structured_output_item_line(len(lines) + 1, item, kind=kind)
        if not line:
            continue
        key = _normalized_reply_text(line)
        if not key or key in seen:
            continue
        seen.add(key)
        lines.append(line)
        if len(lines) >= limit:
            break
    return lines


def _structured_output_item_line(index: int, item: Mapping[str, Any], *, kind: str) -> str:
    title = _structured_output_item_title(item, kind=kind)
    if not title:
        return ""
    details = _structured_output_item_details(item, kind=kind)
    actions = _structured_output_item_action_links(item)
    source = _displayable_evidence_source_label(_compact_account_text(item.get("url")))
    lines = [f"{index}. {title}"]
    if details:
        lines.append(f"   {details}")
    for action in actions[:2]:
        lines.append(f"   {action}")
    if source:
        lines.append(f"   Source: {source}")
    return "\n".join(lines)


def _structured_output_item_title(item: Mapping[str, Any], *, kind: str) -> str:
    if kind == "browser_item":
        keys = ("compact_text", "summary", "title", "name", "link_text", "text")
        max_chars = 128
    elif kind == "calendar":
        keys = ("summary", "title", "name")
        max_chars = 76
    elif kind == "email":
        keys = ("subject", "title", "summary", "from", "sender")
        max_chars = 76
    else:
        keys = ("title", "name", "heading", "subject", "summary", "label", "path", "filename")
        max_chars = 76
    for key in keys:
        title = _compact_evidence_title(item.get(key), max_chars=max_chars)
        if title:
            return title
    return ""


def _structured_output_item_details(item: Mapping[str, Any], *, kind: str) -> str:
    if kind == "calendar":
        details = []
        when = " - ".join(
            part
            for part in (
                _calendar_time_label(item.get("start")),
                _calendar_time_label(item.get("end")),
            )
            if part
        )
        if when:
            details.append(when)
        location = _clip_browser_extract_item_text(item.get("location"), max_chars=72)
        if location:
            details.append(location)
        return "; ".join(details[:2])
    if kind == "browser_item":
        values: list[str] = []
        for key in ("price_text", "price", "rating", "distance", "address", "location", "phone", "status", "hours"):
            value = _clip_browser_extract_item_text(item.get(key), max_chars=64)
            if value:
                values.append(value)
        fields = item.get("fields")
        if isinstance(fields, Mapping):
            for field_value in fields.values():
                value = _clip_browser_extract_item_text(field_value, max_chars=64)
                if value:
                    values.append(value)
        price_candidates = item.get("price_candidates")
        if isinstance(price_candidates, list):
            for candidate in price_candidates[:3]:
                value = _clip_browser_extract_item_text(candidate, max_chars=64)
                if value:
                    values.append(value)
        return "; ".join(_dedupe_preserving_order(values)[:4])
    if kind == "email":
        values = []
        for key in ("from", "sender", "date", "snippet", "summary"):
            value = _clip_browser_extract_item_text(item.get(key), max_chars=82)
            if value:
                values.append(value)
        return "; ".join(_dedupe_preserving_order(values)[:2])
    if kind in {"web_search", "generic"}:
        values = _structured_output_item_concrete_details(item)
        if values:
            return "; ".join(values[:3])
    values = []
    detail_keys = ("snippet", "description", "summary", "date", "start", "end", "price", "rating", "distance")
    for key in detail_keys:
        value = _clip_browser_extract_item_text(item.get(key), max_chars=92)
        if value and _normalized_reply_text(value) != _normalized_reply_text(_structured_output_item_title(item, kind=kind)):
            values.append(value)
    return "; ".join(_dedupe_preserving_order(values)[:2])


def _structured_output_item_concrete_details(item: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("rating", "distance", "price", "price_text", "address", "location", "phone", "status", "hours", "date", "start", "end"):
        value = _clip_browser_extract_item_text(item.get(key), max_chars=72)
        if value:
            values.append(value)
    fields = item.get("fields")
    if isinstance(fields, Mapping):
        for value in fields.values():
            clipped = _clip_browser_extract_item_text(value, max_chars=72)
            if clipped:
                values.append(clipped)
    return _dedupe_preserving_order(values)


def _structured_output_item_action_links(item: Mapping[str, Any]) -> list[str]:
    actions: list[str] = []
    for key in _ACTION_URL_FIELD_KEYS:
        url = _displayable_browser_extract_item_url(_compact_account_text(item.get(key)))
        if not url:
            continue
        label = _structured_output_item_action_label(item, fallback_key=key)
        actions.append(f"{label}: {url}" if label else f"Open: {url}")
    for key in ("links", "actions"):
        values = item.get(key)
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, Mapping):
                continue
            url = ""
            for url_key in _ACTION_URL_FIELD_KEYS:
                url = _displayable_browser_extract_item_url(_compact_account_text(value.get(url_key)))
                if url:
                    break
            if not url:
                continue
            label = _structured_output_item_action_label(value, fallback_key=key)
            actions.append(f"{label}: {url}" if label else f"Open: {url}")
    return _dedupe_preserving_order(actions)


def _structured_output_item_action_label(item: Mapping[str, Any], *, fallback_key: str) -> str:
    for key in ("action_label", "label", "text", "link_text", "kind", "type", "role"):
        value = _clip_browser_extract_item_text(item.get(key), max_chars=36)
        if value and not _looks_like_url(value):
            return value
    if fallback_key in {"website_url", "source_url"}:
        return "Source"
    if fallback_key in {"href", "link", "url"}:
        return "Open"
    return "Open"


def _structured_output_item_url_path_depth(item: Mapping[str, Any]) -> int:
    raw_depth = item.get("url_path_depth")
    try:
        return int(raw_depth)
    except (TypeError, ValueError):
        pass
    url = _compact_account_text(item.get("url") or item.get("href") or item.get("link"))
    if not url:
        return 0
    try:
        return len([part for part in urlparse(url).path.split("/") if part])
    except Exception:
        return 0


def _looks_like_url(value: object) -> bool:
    return bool(re.match(r"https?://", str(value or "").strip(), flags=re.IGNORECASE))


def _same_normalized_url(left: object, right: object) -> bool:
    left_text = _compact_account_text(left)
    right_text = _compact_account_text(right)
    if not left_text or not right_text:
        return False
    try:
        left_parsed = urlparse(left_text)
        right_parsed = urlparse(right_text)
    except Exception:
        return left_text == right_text
    return (
        left_parsed.scheme.casefold(),
        left_parsed.netloc.casefold(),
        left_parsed.path.rstrip("/"),
    ) == (
        right_parsed.scheme.casefold(),
        right_parsed.netloc.casefold(),
        right_parsed.path.rstrip("/"),
    )


def _dedupe_preserving_order(values: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = _normalized_reply_text(text)
        if not text or not key or key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def _structured_evidence_excluded_tokens(user_message: str | None, results: list[ToolResult]) -> set[str]:
    excluded = _structured_evidence_tokens(user_message)
    for result in results:
        output = result.output if isinstance(result.output, dict) else {}
        excluded.update(_structured_evidence_tokens(output.get("query")))
    return excluded


def _structured_evidence_tokens(value: object) -> set[str]:
    return {
        token[:10]
        for token in re.findall(r"[a-z0-9]{4,}", _normalized_reply_text(value).casefold())
    }


def _structured_tool_evidence_section_covered(
    text: str,
    lines: list[str],
    items: list[Mapping[str, Any]],
    *,
    excluded_tokens: set[str],
) -> bool:
    normalized = _normalized_reply_text(text)
    if not normalized:
        return False
    for line in lines:
        evidence_line = _normalized_reply_text(str(line or "").lstrip("-• ").strip())
        if evidence_line and (evidence_line in normalized or normalized in evidence_line):
            return True
    return _structured_tool_evidence_reply_covered(text, items, excluded_tokens=excluded_tokens)


def _structured_tool_evidence_reply_covered(
    text: str,
    items: list[Mapping[str, Any]],
    *,
    excluded_tokens: set[str],
) -> bool:
    normalized = _normalized_reply_text(text)
    if not normalized:
        return False
    comparable = 0
    matched = 0
    for item in items[:8]:
        candidates = [
            _compact_account_text(item.get(key))
            for key in (
                "title",
                "name",
                "heading",
                "subject",
                "summary",
                "compact_text",
                "link_text",
                "path",
                "filename",
                "from",
                "sender",
                "snippet",
                "description",
                "text",
            )
            if _compact_account_text(item.get(key))
        ]
        if not candidates:
            continue
        comparable += 1
        if any(
            _normalized_reply_text(candidate[:80]) in normalized
            or _structured_evidence_token_overlap(candidate, normalized, excluded_tokens=excluded_tokens)
            for candidate in candidates
        ):
            matched += 1
    if comparable <= 0:
        return False
    return matched >= min(comparable, 2)


def _structured_evidence_token_overlap(
    candidate: object,
    normalized_reply: str,
    *,
    excluded_tokens: set[str],
) -> bool:
    reply_tokens = _structured_evidence_tokens(normalized_reply) - excluded_tokens
    candidate_tokens = [
        token[:10]
        for token in re.findall(r"[a-z0-9]{4,}", _normalized_reply_text(candidate).casefold())
        if token[:10] not in excluded_tokens
    ]
    tokens = _dedupe_preserving_order(candidate_tokens)
    if not tokens:
        return False
    matched = [
        token
        for token in tokens
        if token in reply_tokens or _structured_evidence_token_has_close_match(token, reply_tokens)
    ]
    return len(matched) >= min(len(tokens), 2) or any(len(token) >= 7 for token in matched)


def _structured_evidence_token_has_close_match(token: str, reply_tokens: set[str]) -> bool:
    if len(token) < 7:
        return False
    return any(
        len(reply_token) >= 7 and SequenceMatcher(None, token, reply_token).ratio() >= 0.86
        for reply_token in reply_tokens
    )


def _web_search_result_evidence_text(item: Mapping[str, Any]) -> str:
    for key in ("title", "name", "heading"):
        value = _compact_account_text(item.get(key))
        if value:
            return value
    for key in ("snippet", "summary", "description", "text"):
        value = _compact_account_text(item.get(key))
        if value:
            return value
    return ""


def _web_search_result_line(index: int, item: Mapping[str, Any]) -> str:
    title = _structured_output_item_title(item, kind="web_search")
    if not title:
        return ""
    snippet = _clip_browser_extract_item_text(
        item.get("snippet") or item.get("summary") or item.get("description"),
        max_chars=118,
    )
    url = _displayable_browser_extract_item_url(_compact_account_text(item.get("url")))
    source = _displayable_evidence_source_label(url)
    lines = [f"{index}. {title}"]
    if snippet and _normalized_reply_text(snippet) != _normalized_reply_text(title):
        lines.append(f"   {snippet}")
    if url:
        label = source or "open result"
        lines.append(f"   Link: [{label}]({url})")
    return "\n".join(lines)


def _web_search_result_query(item: Mapping[str, Any]) -> str:
    return _compact_account_text(item.get("_query"))


def _web_search_result_currency_values(item: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("price", "price_text", "snippet", "summary", "description", "title", "name", "heading"):
        text = html.unescape(_compact_account_text(item.get(key)))
        if not text:
            continue
        for match in _CURRENCY_AMOUNT_RE.finditer(text):
            value = re.sub(r"\s+", " ", match.group(0)).strip()
            if value and not _browser_catalog_amount_is_zero(value):
                values.append(value)
    return _dedupe_preserving_order(values)


def _web_search_result_label(item: Mapping[str, Any]) -> str:
    title = _structured_output_item_title(item, kind="web_search")
    title_symbol = re.search(r"\(([A-Z][A-Z0-9.]{1,7})\)", title)
    if title_symbol:
        return title_symbol.group(1)
    query = _web_search_result_query(item)
    if query:
        tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9._-]{0,12}", query)
        if tokens:
            title_tokens = {token.casefold() for token in _structured_evidence_tokens(title)}
            for token in tokens:
                if token.casefold() in title_tokens:
                    return token.upper() if len(token) <= 8 else token
            if len(tokens) == 1:
                return tokens[0].upper() if len(tokens[0]) <= 8 else tokens[0]
            return tokens[0].upper() if len(tokens[0]) <= 8 else tokens[0]
        return _clip_browser_extract_item_text(query, max_chars=42)
    return _clip_browser_extract_item_text(title, max_chars=42)


def _web_search_compact_value_reply(items: list[Mapping[str, Any]]) -> str | None:
    by_label: dict[str, list[str]] = {}
    for item in items:
        label = _web_search_result_label(item)
        if not label:
            continue
        amounts = _web_search_result_currency_values(item)
        if not amounts:
            continue
        by_label.setdefault(label, [])
        by_label[label].extend(amounts)
    lines: list[str] = []
    for label, amounts in by_label.items():
        unique_amounts = _dedupe_preserving_order(amounts)
        if not unique_amounts:
            continue
        lines.append(f"- {label}: {', '.join(unique_amounts[:2])}")
        if len(lines) >= 8:
            break
    if not lines:
        return None
    return "Unverified values from structured search snippets\n\n" + "\n".join(lines)


def _web_search_query_tokens(results: list[ToolResult], *, user_message: str | None) -> set[str]:
    values: list[object] = [user_message]
    for result in results:
        if result.tool_name != "web_search" or result.status != "completed" or not isinstance(result.output, dict):
            continue
        values.append(result.output.get("query"))
    tokens: set[str] = set()
    for value in values:
        tokens.update(token for token in _structured_evidence_tokens(value) if len(token) >= 4)
    return tokens


def _web_search_short_query_tokens(results: list[ToolResult], *, user_message: str | None) -> set[str]:
    values: list[object] = [user_message]
    for result in results:
        if result.tool_name != "web_search" or result.status != "completed" or not isinstance(result.output, dict):
            continue
        values.append(result.output.get("query"))
    tokens: set[str] = set()
    for value in values:
        normalized = _normalized_reply_text(value).casefold()
        tokens.update(token for token in re.findall(r"[a-z0-9]{2}", normalized) if not token.isdigit())
    return tokens


def _web_search_item_tokens(item: Mapping[str, Any]) -> set[str]:
    values: list[object] = [
        item.get("title"),
        item.get("name"),
        item.get("heading"),
        item.get("snippet"),
        item.get("summary"),
        item.get("description"),
        item.get("url"),
    ]
    return set().union(*(_structured_evidence_tokens(value) for value in values))


def _web_search_item_url_tokens(item: Mapping[str, Any]) -> set[str]:
    url = _compact_account_text(item.get("url"))
    if not url:
        return set()
    try:
        parsed = urlparse(url)
    except Exception:
        parsed = None
    value = " ".join(
        part
        for part in (
            parsed.netloc if parsed else "",
            parsed.path if parsed else url,
        )
        if part
    )
    return set(token for token in re.findall(r"[a-z0-9]{2,}", value.casefold()) if not token.isdigit())


def _web_search_item_short_text_tokens(item: Mapping[str, Any]) -> set[str]:
    values = [item.get("title"), item.get("name"), item.get("heading")]
    tokens: set[str] = set()
    for value in values:
        normalized = _normalized_reply_text(value).casefold()
        tokens.update(token for token in re.findall(r"[a-z0-9]{2}", normalized) if not token.isdigit())
    return tokens


def _web_search_token_matches(query_token: str, item_token: str) -> bool:
    if query_token == item_token:
        return True
    if len(query_token) >= 4 and len(item_token) >= 4 and (
        query_token.startswith(item_token) or item_token.startswith(query_token)
    ):
        return True
    return _structured_evidence_token_has_close_match(query_token, {item_token})


def _web_search_item_relevance_score(
    item: Mapping[str, Any],
    query_tokens: set[str],
    *,
    short_query_tokens: set[str],
) -> int:
    item_tokens = _web_search_item_tokens(item)
    if not item_tokens:
        return 0
    matched = {
        query_token
        for query_token in query_tokens
        if any(_web_search_token_matches(query_token, item_token) for item_token in item_tokens)
    }
    short_url_matches = short_query_tokens & _web_search_item_url_tokens(item)
    short_title_matches = short_query_tokens & _web_search_item_short_text_tokens(item)
    return len(matched) + (len(short_url_matches) * 3) + (len(short_title_matches) * 2)


def _rank_web_search_results_for_reply(
    items: list[Mapping[str, Any]],
    *,
    query_tokens: set[str],
    short_query_tokens: set[str],
) -> list[Mapping[str, Any]]:
    if not query_tokens and not short_query_tokens:
        return items
    evidence_short_tokens = {
        token
        for token in short_query_tokens
        if any(token in _web_search_item_url_tokens(item) for item in items)
    }
    scored = [
        (
            _web_search_item_relevance_score(
                item,
                query_tokens,
                short_query_tokens=evidence_short_tokens,
            ),
            index,
            item,
        )
        for index, item in enumerate(items)
    ]
    if not any(score > 0 for score, _index, _item in scored):
        return items
    scored.sort(key=lambda value: (-value[0], value[1]))
    best_score = scored[0][0]
    floor = 2 if best_score >= 2 else 1
    return [item for score, _index, item in scored if score >= floor]


def _web_search_results_reply_covered(text: str, items: list[Mapping[str, Any]]) -> bool:
    normalized = _normalized_reply_text(text)
    if not normalized:
        return False
    comparable = 0
    matched = 0
    for item in items[:5]:
        candidates = [
            _compact_account_text(item.get(key))
            for key in ("title", "name", "heading", "snippet", "summary", "description")
            if _compact_account_text(item.get(key))
        ]
        source_label = _displayable_evidence_source_label(_compact_account_text(item.get("url")))
        if source_label:
            candidates.append(source_label)
        if not candidates:
            continue
        comparable += 1
        if any(_normalized_reply_text(candidate[:80]) in normalized for candidate in candidates):
            matched += 1
    if comparable <= 0:
        return False
    return matched >= min(comparable, 2)


def _web_search_reply_over_ignored_results(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    if _has_completed_deliverable_artifact_result(results):
        return None
    if _browser_recovery_should_preserve_model_reply(text, results):
        return None
    if _browser_tools_attempted(results):
        if browser_extract_reply := _browser_extract_evidence_reply(results, user_message=user_message):
            if not _reply_matches_browser_extract_text(text, browser_extract_reply):
                return browser_extract_reply
            return None
        if _browser_reply_is_nonverifying_explanation(text):
            return None
        return _browser_unverified_search_result_blocker(results, user_message=user_message)
    browser_item_records = _completed_browser_extract_item_records(results)
    if browser_item_records and not _rank_structured_output_items(browser_item_records, kind="browser_item"):
        return None
    items = _completed_web_search_result_records(results)
    if not items:
        return None
    if compact_values := _web_search_compact_value_reply(
        _rank_web_search_results_for_reply(
            items,
            query_tokens=_web_search_query_tokens(results, user_message=user_message),
            short_query_tokens=_web_search_short_query_tokens(results, user_message=user_message),
        )
    ):
        if _web_search_results_reply_covered(text, items):
            return None
        return compact_values
    ranked_items = _rank_web_search_results_for_reply(
        items,
        query_tokens=_web_search_query_tokens(results, user_message=user_message),
        short_query_tokens=_web_search_short_query_tokens(results, user_message=user_message),
    )
    lines: list[str] = []
    seen: set[str] = set()
    for item in ranked_items:
        line = _web_search_result_line(len(lines) + 1, item)
        if not line:
            continue
        key = _normalized_reply_text(line[:180])
        if key in seen:
            continue
        seen.add(key)
        lines.append(line)
        if len(lines) >= 3:
            break
    if not lines:
        return None
    if _web_search_results_reply_covered(text, items):
        return None
    return "I found source snippets, but they were not enough to produce a verified final answer from this run."


def _browser_tools_attempted(results: list[ToolResult]) -> bool:
    return any(result.tool_name.startswith("browser_") for result in results)


def _browser_empty_reply_over_missing_verified_records(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    if _normalized_reply_text(text):
        return None
    if _has_completed_deliverable_artifact_result(results):
        return None
    if not _browser_tools_attempted(results):
        return None
    if _rank_structured_output_items(_completed_browser_extract_item_records(results), kind="browser_item"):
        return None
    if browser_extract_reply := _browser_extract_evidence_reply(results, user_message=user_message):
        return browser_extract_reply
    return _browser_unverified_search_result_blocker(results, user_message=user_message)


def _browser_unverified_search_result_blocker(
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str:
    lines = [
        "Live-search blocker: the browser/search tools did not produce verified result records "
        "from this run, so I do not have a reliable final answer for this request yet."
    ]
    request_context = _compact_browser_request_context(user_message)
    if request_context:
        lines.extend(["", f"Requested task: {request_context}"])
    return "\n".join(lines)


def _browser_extract_item_evidence_text(item: Mapping[str, Any]) -> str:
    for key in ("compact_text", "summary", "text", "title", "name", "link_text"):
        value = _compact_account_text(item.get(key))
        if value:
            return value
    return ""


def _browser_low_quality_items_reply_over_top_matches(text: str, results: list[ToolResult]) -> str | None:
    records = _completed_browser_extract_item_records(results)
    if not records or _rank_structured_output_items(records, kind="browser_item"):
        return None
    normalized = _normalized_reply_text(text).casefold()
    if "top matches i found" not in normalized and "search leads i found" not in normalized:
        return None
    if not any(result.tool_name == "web_search" and _tool_result_completed(result) for result in results):
        return "I could not verify live result records from this page; the browser extraction only returned navigation/media rows."
    return _browser_unverified_search_result_blocker(results)


def _browser_extract_item_line(item: Mapping[str, Any]) -> str:
    return _structured_output_item_line(1, item, kind="browser_item").removeprefix("1. ").strip()


def _clip_browser_extract_item_text(value: object, *, max_chars: int) -> str:
    text = _compact_account_text(value)
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[*_`]{1,3}", "", text)
    text = re.sub(r"https?://\\S+", "", text).strip(" -•,")
    text = re.sub(r"\\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip(" -•,") + "…"


def _displayable_browser_extract_item_url(url: str) -> str:
    if not url or len(url) > 120:
        return ""
    lowered = url.lower()
    if "/maps/dir" in lowered or "/search?" in lowered or "google.com/maps" in lowered:
        return ""
    return url


def _displayable_evidence_source_label(url: str) -> str:
    if not url:
        return ""
    match = re.match(r"https?://([^/?#]+)", url.strip(), flags=re.IGNORECASE)
    host = match.group(1) if match else url.split("/", 1)[0]
    host = host.lower().removeprefix("www.")
    if not host or len(host) > 42:
        return ""
    return host


def _compact_evidence_title(value: object, *, max_chars: int) -> str:
    text = _clip_browser_extract_item_text(value, max_chars=max(max_chars * 2, max_chars + 20))
    if not text:
        return ""
    parts = [part.strip() for part in re.split(r"\s+(?:[-–—|])\s+", text) if part.strip()]
    if len(parts) > 1:
        candidate = parts[0]
        for part in parts[1:3]:
            joined = f"{candidate} - {part}"
            if len(joined) <= max_chars:
                candidate = joined
        text = candidate
    return _clip_browser_extract_item_text(text, max_chars=max_chars)


def _browser_extract_items_reply_covered(text: str, items: list[Mapping[str, Any]]) -> bool:
    normalized = _normalized_reply_text(text)
    if not normalized:
        return False
    comparable = 0
    matched = 0
    for item in items[:5]:
        candidates = [
            _compact_account_text(item.get(key))
            for key in ("title", "name", "compact_text", "summary", "link_text")
            if _compact_account_text(item.get(key))
        ]
        if not candidates:
            continue
        comparable += 1
        if any(_normalized_reply_text(candidate[:80]) in normalized for candidate in candidates):
            matched += 1
    if comparable <= 0:
        return False
    return matched >= min(comparable, 2)


def _browser_extract_items_reply_over_ignored_rows(text: str, results: list[ToolResult]) -> str | None:
    if _has_completed_deliverable_artifact_result(results):
        return None
    items = _rank_structured_output_items(
        _completed_browser_extract_item_records(results),
        kind="browser_item",
    )
    if not items:
        return None
    lines: list[str] = []
    seen: set[str] = set()
    for item in items:
        line = _browser_extract_item_line(item)
        if not line:
            continue
        key = _normalized_reply_text(line[:160])
        if key in seen:
            continue
        seen.add(key)
        lines.append(line)
        if len(lines) >= 5:
            break
    if not lines:
        return None
    if _browser_extract_items_reply_covered(text, items):
        return None
    return "**Top matches I found**\n\n" + "\n".join(lines)


def _minimum_browser_item_query_matches(query_terms: tuple[str, ...]) -> int:
    if len(query_terms) <= 2:
        return len(query_terms)
    if len(query_terms) <= 5:
        return 2
    return 3


def _browser_extract_items_reply_over_blocker(
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    records = _completed_browser_extract_item_records(results)
    if not records:
        return None
    query_terms = _browser_query_terms(user_message)
    ranked = _rank_structured_output_items(records, kind="browser_item")
    if ranked and query_terms:
        scored_ranked: list[tuple[int, int, Mapping[str, Any]]] = []
        for index, item in enumerate(ranked):
            compact_text = " ".join(
                _compact_account_text(item.get(key))
                for key in ("compact_text", "summary", "title", "name", "link_text", "text")
                if _compact_account_text(item.get(key))
            )
            normalized_text = _normalized_reply_text(compact_text).casefold()
            matched_terms = {
                term
                for term in query_terms
                if term and term in normalized_text
            }
            scored_ranked.append((len(matched_terms), index, item))
        if any(score > 0 for score, _index, _item in scored_ranked):
            minimum_matches = _minimum_browser_item_query_matches(query_terms)
            eligible = [
                (score, index, item)
                for score, index, item in scored_ranked
                if score >= minimum_matches
            ]
            if not eligible:
                return None
            eligible.sort(key=lambda value: (-value[0], value[1]))
            ranked = [item for _score, _index, item in eligible]
    if not ranked:
        scored: list[tuple[int, int, Mapping[str, Any]]] = []
        for index, item in enumerate(records):
            compact_text = " ".join(
                _compact_account_text(item.get(key))
                for key in ("compact_text", "summary", "title", "name", "link_text", "text")
                if _compact_account_text(item.get(key))
            )
            normalized_text = _normalized_reply_text(compact_text).casefold()
            if len(normalized_text) < 40:
                continue
            source_depth = _structured_output_item_url_path_depth(item)
            if source_depth < 2:
                continue
            matched_terms = {
                term
                for term in query_terms
                if term and term in normalized_text
            }
            if query_terms and len(matched_terms) < min(2, len(query_terms)):
                continue
            scored.append((len(matched_terms), index, item))
        scored.sort(key=lambda value: (-value[0], value[1]))
        ranked = [item for _score, _index, item in scored]
    if not ranked:
        return None
    lines: list[str] = []
    seen: set[str] = set()
    for item in ranked:
        line = _browser_extract_item_line(item)
        if not line:
            continue
        key = _normalized_reply_text(line[:180])
        if not key or key in seen:
            continue
        seen.add(key)
        lines.append(line)
        if len(lines) >= 4:
            break
    if not lines:
        return None
    rendered = ["- " + line.replace("\n   ", "\n  ") for line in lines]
    return "Source-backed points\n\n" + "\n\n".join(rendered)


def _reply_matches_browser_extract_text(text: str, extracted_text: str) -> bool:
    normalize = lambda value: re.sub(r"\s+", " ", str(value or "")).strip()
    reply_text = normalize(text)
    extracted = normalize(extracted_text)
    if not reply_text or not extracted:
        return False
    return extracted.startswith(reply_text[: min(len(reply_text), 500)]) or reply_text.startswith(
        extracted[: min(len(extracted), 500)]
    )


def _browser_catalog_signal_score(text: str) -> int:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    price_hits = sum(1 for line in lines if _CURRENCY_AMOUNT_RE.search(line))
    paired_hits = len(_browser_catalog_line_pairs(text, limit=12))
    return price_hits + (paired_hits * 2)


_SPLIT_CURRENCY_SYMBOLS = frozenset({"$", "€", "£", "₹"})
_SPLIT_CURRENCY_DECIMAL_RE = re.compile(r"^\.\d{1,2}$")
_SPLIT_CURRENCY_WHOLE_RE = re.compile(r"^\d[\d,]*$")


def _browser_catalog_price_at(lines: list[str], index: int) -> tuple[str, int, bool] | None:
    line = lines[index].strip()
    regular = _CURRENCY_AMOUNT_RE.search(line)
    if regular is not None:
        amount = regular.group(0)
        if _browser_catalog_amount_is_zero(amount):
            return None
        price_text = line if len(line) <= 80 else amount
        return price_text, index, False
    if line not in _SPLIT_CURRENCY_SYMBOLS or index + 1 >= len(lines):
        return None
    whole = lines[index + 1].strip()
    if not _SPLIT_CURRENCY_WHOLE_RE.match(whole):
        return None
    end_index = index + 1
    decimal = ""
    if index + 2 < len(lines) and _SPLIT_CURRENCY_DECIMAL_RE.match(lines[index + 2].strip()):
        decimal = lines[index + 2].strip()
        end_index = index + 2
    amount = f"{line}{whole}{decimal}"
    if _browser_catalog_amount_is_zero(amount):
        return None
    secondary = False
    if index + 3 < len(lines):
        unit = _browser_catalog_price_unit_label(lines[index + 3])
        if unit:
            amount = f"{amount} {unit}"
            end_index = index + 3
            secondary = _browser_catalog_unit_is_secondary(unit)
    return amount, end_index, secondary


def _browser_catalog_amount_is_zero(amount: object) -> bool:
    digits = re.sub(r"[^\d.]+", "", str(amount or ""))
    if not digits:
        return False
    try:
        return float(digits) == 0
    except ValueError:
        return False


_BROWSER_CATALOG_UNIT_STOPWORDS = frozenset({"select"})


def _browser_catalog_price_unit_label(value: object) -> str:
    label = _compact_browser_extract_line(value)
    if not label or len(label) > 32:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", " ", label.casefold()).strip()
    if normalized in _BROWSER_CATALOG_UNIT_STOPWORDS:
        return ""
    if _CURRENCY_AMOUNT_RE.search(label) or label in _SPLIT_CURRENCY_SYMBOLS:
        return ""
    if _SPLIT_CURRENCY_WHOLE_RE.match(label) or _SPLIT_CURRENCY_DECIMAL_RE.match(label):
        return ""
    if not any(char.isalpha() for char in label):
        return ""
    return label


def _browser_catalog_unit_is_secondary(value: object) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold()).strip()
    return normalized in {"total"}


def _browser_catalog_line_pairs(text: str, *, limit: int = 8) -> list[tuple[str, str]]:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    pairs: list[tuple[str, str]] = []
    seen_label_indexes: dict[str, int] = {}
    index = 0
    while index < len(lines):
        price = _browser_catalog_price_at(lines, index)
        if price is None:
            index += 1
            continue
        price_text, end_index, secondary = price
        label = _previous_catalog_label(
            lines,
            index,
            include_previous_price_block=secondary,
        )
        if not label:
            index = end_index + 1
            continue
        normalized_label = _normalized_reply_text(label).casefold()
        if normalized_label in seen_label_indexes:
            pair_index = seen_label_indexes[normalized_label]
            old_label, old_price = pairs[pair_index]
            if secondary and price_text not in old_price:
                pairs[pair_index] = (old_label, f"{old_price}; {price_text}")
            index = end_index + 1
            continue
        seen_label_indexes[normalized_label] = len(pairs)
        pairs.append((label, price_text))
        if len(pairs) >= limit:
            break
        index = end_index + 1
    return pairs


def _previous_catalog_label(
    lines: list[str],
    index: int,
    *,
    include_previous_price_block: bool = False,
) -> str | None:
    lower_bound = max(-1, index - 14)
    if not include_previous_price_block:
        for previous_index in range(index - 1, lower_bound, -1):
            if _browser_catalog_price_at(lines, previous_index) is not None:
                lower_bound = previous_index
                break
    candidates: list[tuple[int, int, str]] = []
    for candidate_index in range(index - 1, lower_bound, -1):
        candidate = str(lines[candidate_index] or "").strip()
        if not candidate:
            continue
        if _CURRENCY_AMOUNT_RE.search(candidate):
            continue
        if candidate in _SPLIT_CURRENCY_SYMBOLS:
            continue
        score = _browser_catalog_label_score(candidate)
        if score <= 0:
            continue
        candidates.append((score, candidate_index, candidate))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][2]


_BROWSER_CATALOG_LABEL_STOPWORDS = frozenset(
    {
        "features price details",
        "pay later",
        "pay now",
        "pay in points",
        "pay in",
        "per day",
        "per hour",
        "per month",
        "per rental",
        "per week",
        "total",
        "select",
        "sorted by featured",
        "sort filter",
        "automatic",
    }
)


def _browser_catalog_label_score(candidate: object) -> int:
    text = _compact_browser_extract_line(candidate)
    if not text or len(text) > 140:
        return 0
    normalized = re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()
    if not normalized or normalized in _BROWSER_CATALOG_LABEL_STOPWORDS:
        return 0
    if _CURRENCY_AMOUNT_RE.search(text):
        return 0
    if text in _SPLIT_CURRENCY_SYMBOLS:
        return 0
    if _SPLIT_CURRENCY_WHOLE_RE.match(text) or _SPLIT_CURRENCY_DECIMAL_RE.match(text):
        return 0
    if not any(char.isalpha() for char in text):
        return 0
    alnum_count = sum(1 for char in text if char.isalnum())
    if alnum_count < 3:
        return 0
    words = re.findall(r"[\w.-]+", text, flags=re.UNICODE)
    score = 1
    if 2 <= len(words) <= 8:
        score += 3
    if any(char.islower() for char in text) and any(char.isupper() for char in text):
        score += 3
    if not any(char.isdigit() for char in text):
        score += 2
    if text.isupper() and len(text) <= 24:
        score -= 4
    if len(words) == 1 and len(text) <= 12:
        score -= 1
    return max(score, 0)
    return None


def _browser_catalog_reply_over_raw_extract_dump(text: str, results: list[ToolResult]) -> str | None:
    if not _browser_raw_extract_catalog_records_allowed(results):
        return None
    extracts = _completed_browser_extract_texts(results)
    if not extracts:
        return None
    if not any(_reply_matches_browser_extract_text(text, extracted) for extracted in extracts):
        return None
    scored_extracts = sorted(
        (( _browser_catalog_signal_score(extracted), index, extracted) for index, extracted in enumerate(extracts)),
        key=lambda item: (item[0], item[1]),
        reverse=True,
    )
    best_score, _index, best_text = scored_extracts[0]
    if best_score < 3:
        return None
    pairs = _browser_catalog_line_pairs(best_text)
    if not pairs:
        return None
    return _browser_catalog_reply_from_extract_text(best_text)


def _browser_catalog_reply_over_prior_extract_dump(text: str, results: list[ToolResult]) -> str | None:
    if not _browser_raw_extract_catalog_records_allowed(results):
        return None
    extracts = _completed_browser_extract_texts(results)
    if not extracts:
        return None
    matching_indexes = {
        index
        for index, extracted in enumerate(extracts)
        if _reply_matches_browser_extract_text(text, extracted)
    }
    if not matching_indexes:
        return None
    scored_extracts = sorted(
        (
            (_browser_catalog_signal_score(extracted), index, extracted)
            for index, extracted in enumerate(extracts)
            if index not in matching_indexes
        ),
        key=lambda item: (item[0], item[1]),
        reverse=True,
    )
    if not scored_extracts:
        return None
    best_score, _index, best_text = scored_extracts[0]
    matching_best_score = max((_browser_catalog_signal_score(extracts[index]) for index in matching_indexes), default=0)
    if best_score < 3 or best_score <= matching_best_score:
        return None
    pairs = _browser_catalog_line_pairs(best_text)
    if not pairs:
        return None
    return _browser_catalog_reply_from_extract_text(best_text)


def _browser_extract_dump_reply_over_raw_extract(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    extracts = _completed_browser_extract_texts(results, max_chars=12000)
    matching_extracts = [
        extracted
        for extracted in extracts
        if _reply_matches_browser_extract_text(text, extracted)
    ]
    if not matching_extracts:
        return None
    normalized_text = _normalized_reply_text(text)
    if _browser_recovery_should_preserve_model_reply(text, results) and not any(
        normalized_text == _normalized_reply_text(extracted)
        for extracted in matching_extracts
    ):
        return None
    normalized_text_casefold = _normalized_reply_text(text).casefold()
    if not _browser_reply_is_nonverifying_explanation(text):
        for extracted in matching_extracts:
            normalized_extract = _normalized_reply_text(extracted).casefold()
            if (
                len(normalized_extract) >= 4
                and normalized_extract in normalized_text_casefold
                and normalized_extract != normalized_text_casefold
                and len(normalized_text_casefold) > len(normalized_extract) + 12
            ):
                return None
    if (
        len(normalized_text) <= 1000
        and _latest_verified_browser_assert_page_state(results) is None
        and _browser_exact_extract_reply_can_pass_through(text)
        and any(
            normalized_text == _normalized_reply_text(extracted)
            for extracted in matching_extracts
        )
    ):
        return None
    if (
        _browser_raw_extract_catalog_records_allowed(results, extracted_text=text)
        and _browser_catalog_signal_score(text) >= 3
        and _browser_catalog_line_pairs(text)
    ):
        return _browser_catalog_reply_from_extract_text(text)
    selected_extract = (
        _best_browser_extract_text_for_reply(extracts, user_message=user_message)
        or matching_extracts[-1]
    )
    return _browser_extract_reply_from_raw_text(
        selected_extract,
        results,
        user_message=user_message,
    )


def _browser_reply_over_wrong_browser_extract(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    extracts = _completed_browser_extract_texts(results, max_chars=12000)
    if len(extracts) < 2:
        return None
    ranked = _ranked_browser_extract_texts_for_reply(
        extracts,
        user_message=user_message,
        results=results,
    )
    if len(ranked) < 2:
        return None
    best_extract = ranked[0]
    best_reply_score = _browser_reply_extract_grounding_score(
        text,
        best_extract,
        results=results,
        user_message=user_message,
    )
    other_reply_score = max(
        (
            _browser_reply_extract_grounding_score(
                text,
                extracted_text,
                results=results,
                user_message=user_message,
            )
            for extracted_text in ranked[1:]
        ),
        default=0,
    )
    if other_reply_score <= 0 or best_reply_score >= other_reply_score:
        return None
    best_evidence_score = _browser_extract_request_evidence_score(
        best_extract,
        results=results,
        user_message=user_message,
    )
    if best_evidence_score <= 0:
        return None
    return _browser_source_backed_reply_from_extract_text(
        best_extract,
        results,
        user_message=user_message,
    ) or _browser_extract_reply_from_raw_text(
        best_extract,
        results,
        user_message=user_message,
    )


def _browser_reply_extract_grounding_score(
    reply: object,
    extracted_text: str,
    *,
    results: list[ToolResult],
    user_message: str | None,
) -> int:
    normalized_reply = _normalized_reply_text(reply).casefold()
    if not normalized_reply:
        return 0
    score = 0
    source_label = _browser_source_label_for_extract_text(results, extracted_text)
    if source_label:
        normalized_source = _normalized_reply_text(source_label).casefold()
        if normalized_source and normalized_source in normalized_reply:
            score += 100
    for line in _browser_extract_request_evidence_lines(extracted_text, user_message=user_message):
        key = _normalized_reply_text(line).casefold()
        if len(key) >= 18 and key in normalized_reply:
            score += 10
    return score


def _browser_extract_request_evidence_score(
    extracted_text: str,
    *,
    results: list[ToolResult],
    user_message: str | None,
) -> int:
    query_terms = _browser_query_terms(user_message)
    highlights = _browser_extract_highlight_lines(
        extracted_text,
        query_terms=query_terms,
        limit=8,
    )
    substantive = _browser_extract_substantive_evidence_lines(extracted_text, limit=4)
    score = len(substantive) * 20
    if highlights:
        matched = {
            term
            for line in highlights
            for term in query_terms
            if term and term in line.casefold()
        }
        score += len(matched) * 10 + len(highlights)
    if results and _browser_raw_extract_catalog_records_allowed(results, extracted_text=extracted_text):
        score += _browser_catalog_signal_score(extracted_text) * 30
    return score


def _browser_extract_request_evidence_lines(
    extracted_text: str,
    *,
    user_message: str | None,
) -> list[str]:
    highlights = _browser_extract_highlight_lines(
        extracted_text,
        query_terms=_browser_query_terms(user_message),
        limit=8,
    )
    substantive = _browser_extract_substantive_evidence_lines(extracted_text, limit=4)
    return _merge_browser_extract_highlights(highlights, substantive)


def _browser_source_backed_reply_from_extract_text(
    extracted_text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    lines = _browser_extract_request_evidence_lines(
        extracted_text,
        user_message=user_message,
    )
    if not lines:
        return None
    source_label = _browser_source_label_for_extract_text(results, extracted_text) or _browser_source_label(results)
    reply_lines = ["Source-backed points"]
    if source_label:
        reply_lines.extend(["", f"Source: {source_label}"])
    reply_lines.append("")
    reply_lines.extend(f"- {line}" for line in lines)
    return "\n".join(reply_lines)


def _browser_exact_extract_reply_can_pass_through(text: object) -> bool:
    lines = [
        _compact_browser_extract_line(line)
        for line in str(text or "").splitlines()
        if _compact_browser_extract_line(line)
    ]
    if not lines or len(lines) > 2:
        return False
    joined = " ".join(lines)
    if len(joined) > 500:
        return False
    if joined.startswith(("http://", "https://")):
        return False
    words = re.findall(r"[\w.-]+", joined, flags=re.UNICODE)
    if len(words) < 4:
        return False
    return sum(1 for char in joined if char.isalpha()) >= 12


def _browser_extract_evidence_reply(
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    if item_reply := _browser_extract_items_reply_over_blocker(results, user_message=user_message):
        return item_reply
    if _browser_form_action_failed(results):
        return None
    extracts = _completed_browser_extract_texts(results)
    if not extracts:
        return None
    if quote_reply := _browser_quote_reply_from_extracts(results, user_message=user_message, extracts=extracts):
        return quote_reply
    fallback_reply: str | None = None
    for extracted_text in _ranked_browser_extract_texts_for_reply(
        extracts,
        user_message=user_message,
        results=results,
    ):
        reply = _browser_extract_reply_from_raw_text(
            extracted_text,
            results,
            user_message=user_message,
        )
        if not _browser_evidence_reply_is_blocker(reply):
            return reply
        fallback_reply = fallback_reply or reply
    return fallback_reply


def _browser_extract_evidence_reply_over_blocker(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    normalized = _normalized_reply_text(text).casefold()
    if "live-search blocker" not in normalized and "verified result records" not in normalized:
        return None
    return _browser_extract_evidence_reply(results, user_message=user_message)


def _best_browser_extract_text_for_reply(extracts: list[str], *, user_message: str | None) -> str | None:
    ranked = _ranked_browser_extract_texts_for_reply(extracts, user_message=user_message)
    return ranked[0] if ranked else None


def _ranked_browser_extract_texts_for_reply(
    extracts: list[str],
    *,
    user_message: str | None,
    results: list[ToolResult] | None = None,
) -> list[str]:
    if not extracts:
        return []
    query_terms = _browser_query_terms(user_message)
    if not query_terms:
        return [extracts[-1], *list(reversed(extracts[:-1]))]
    scored: list[tuple[int, int, int, str]] = []
    for index, extracted_text in enumerate(extracts):
        score = _browser_extract_request_evidence_score(
            extracted_text,
            results=results or [],
            user_message=user_message,
        )
        scored.append((score, len(str(extracted_text or "")), index, extracted_text))
    scored.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    best_score, _length, _index, _best_text = scored[0]
    if best_score <= 0:
        return [extracts[-1], *list(reversed(extracts[:-1]))]
    return [extracted_text for _score, _length, _index, extracted_text in scored]


def _browser_evidence_reply_is_blocker(reply: object) -> bool:
    normalized = _normalized_reply_text(reply).casefold()
    if not normalized:
        return False
    return "live-search blocker" in normalized or "verified result records" in normalized


def _browser_quote_reply_from_extracts(
    results: list[ToolResult],
    *,
    user_message: str | None,
    extracts: list[str],
) -> str | None:
    requested_symbols = _requested_quote_symbols(results, user_message=user_message)
    records: list[tuple[str, str, str, str | None]] = []
    seen: set[str] = set()
    for extracted_text in extracts:
        for name, symbol, close_price, secondary_price in _browser_quote_records_from_extract_text(
            extracted_text,
            requested_symbols=requested_symbols,
        ):
            if symbol in seen:
                continue
            seen.add(symbol)
            records.append((name, symbol, close_price, secondary_price))
    if not records:
        return None
    if requested_symbols:
        matched_symbols = {symbol for _name, symbol, _close_price, _secondary_price in records}
        if not (matched_symbols & requested_symbols):
            return None
    elif len(records) < 2:
        return None
    lines = ["Market prices from live quote text", ""]
    for name, symbol, close_price, secondary_price in records[:8]:
        line = f"- {symbol} ({name}): {close_price}"
        if secondary_price:
            line += f"; {secondary_price} pre-market"
        lines.append(line)
    return "\n".join(lines)


def _requested_quote_symbols(results: list[ToolResult], *, user_message: str | None) -> set[str]:
    values: list[object] = [user_message]
    for result in results:
        if result.tool_name != "web_search" or result.status != "completed" or not isinstance(result.output, dict):
            continue
        values.append(result.output.get("query"))
    symbols: set[str] = set()
    for value in values:
        for token in re.findall(r"\b[A-Z][A-Z0-9.]{1,7}\b", str(value or "")):
            if any(char.isdigit() for char in token) and not any(char.isalpha() for char in token):
                continue
            symbols.add(token)
    return symbols


def _browser_quote_records_from_extract_text(
    extracted_text: str,
    *,
    requested_symbols: set[str],
) -> list[tuple[str, str, str, str | None]]:
    lines = [
        _compact_browser_extract_line(line)
        for line in str(extracted_text or "").splitlines()
        if _compact_browser_extract_line(line)
    ]
    records: list[tuple[str, str, str, str | None]] = []
    for index, line in enumerate(lines):
        match = _QUOTE_PAGE_TITLE_RE.match(line)
        if match:
            name = match.group("name").strip()
            symbol = match.group("symbol").strip()
            price_start_index = index
        else:
            symbol_match = _QUOTE_PAGE_SYMBOL_EXCHANGE_RE.match(line)
            if not symbol_match:
                continue
            symbol = symbol_match.group("symbol").strip()
            name_info = _browser_quote_company_after(lines, index)
            if name_info is None:
                continue
            name, price_start_index = name_info
        if requested_symbols and symbol not in requested_symbols:
            continue
        prices = _browser_quote_prices_after(lines, price_start_index)
        if not prices:
            continue
        close_price = prices[0]
        secondary_price = prices[1] if len(prices) > 1 else None
        records.append((name, symbol, close_price, secondary_price))
    return records


_QUOTE_PAGE_NAME_SKIP_LINES = frozenset(
    {
        "|",
        "add",
        "add to list",
        "home",
        "finance",
        "beta",
        "check_indeterminate_small",
        "check indeterminate small",
        "arrow_back",
    }
)


def _browser_quote_company_after(lines: list[str], symbol_index: int) -> tuple[str, int] | None:
    for offset, line in enumerate(lines[symbol_index + 1 : symbol_index + 8], start=1):
        text = _compact_browser_extract_line(line)
        normalized = re.sub(r"[^a-z0-9|]+", " ", text.casefold()).strip()
        if not text or normalized in _QUOTE_PAGE_NAME_SKIP_LINES:
            continue
        if _browser_quote_price_value(text):
            continue
        if "%" in text or "arrow_" in normalized:
            continue
        if not any(char.isalpha() for char in text):
            continue
        return text, symbol_index + offset
    return None


def _browser_quote_prices_after(lines: list[str], title_index: int) -> list[str]:
    prices: list[str] = []
    for line in lines[title_index + 1 : title_index + 18]:
        value = _browser_quote_price_value(line)
        if not value:
            continue
        if value not in prices:
            prices.append(value)
        if len(prices) >= 2:
            break
    return prices


def _browser_quote_price_value(line: object) -> str | None:
    text = _compact_browser_extract_line(line).replace(",", "")
    for symbol in _SPLIT_CURRENCY_SYMBOLS:
        if text.startswith(symbol):
            text = text.removeprefix(symbol).strip()
            break
    if not _QUOTE_PAGE_PRICE_RE.match(text):
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if number <= 0 or number >= 100000:
        return None
    return f"${number:,.2f}"


def _browser_extract_reply_from_raw_text(
    extracted_text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str:
    if _browser_extracted_text_looks_like_site_blocker(extracted_text):
        return _browser_unverified_search_result_blocker(results, user_message=user_message)
    catalog_records_allowed = _browser_raw_extract_catalog_records_allowed(
        results,
        extracted_text=extracted_text,
    )
    if _browser_form_action_failed(results) and not _browser_raw_extract_has_substantive_result_evidence(
        extracted_text,
        allow_catalog_pairs=catalog_records_allowed,
    ):
        return _browser_incomplete_interaction_reply(results, user_message=user_message)
    if (
        _browser_completed_interaction_count(results)
        and not catalog_records_allowed
        and not _rank_structured_output_items(_completed_browser_extract_item_records(results), kind="browser_item")
        and not _browser_raw_extract_has_substantive_result_evidence(
            extracted_text,
            allow_catalog_pairs=False,
        )
    ):
        return _browser_incomplete_interaction_reply(results, user_message=user_message)
    catalog_reply = (
        _browser_catalog_reply_from_extract_text(extracted_text)
        if catalog_records_allowed
        else None
    )
    if catalog_reply and _browser_catalog_signal_score(extracted_text) >= 3:
        return catalog_reply
    highlights = _browser_extract_highlight_lines(
        extracted_text,
        query_terms=_browser_query_terms(user_message),
    )
    substantive = _browser_extract_substantive_evidence_lines(extracted_text)
    if substantive:
        highlights = _merge_browser_extract_highlights(substantive, highlights)
    if catalog_reply and not highlights:
        return catalog_reply
    if not catalog_records_allowed and _browser_catalog_signal_score(extracted_text) >= 3:
        return _browser_unverified_catalog_text_reply(results, user_message=user_message)
    if not highlights:
        return _browser_raw_extract_dump_replacement(
            results,
            extracted_text=extracted_text,
            user_message=user_message,
        )
    if catalog_reply and _browser_catalog_signal_score(extracted_text) >= 6:
        return catalog_reply
    source_label = _browser_source_label_for_extract_text(results, extracted_text) or _browser_source_label(results)
    lines = ["Source-backed points"]
    if source_label:
        lines.extend(["", f"Source: {source_label}"])
    lines.append("")
    lines.extend(f"- {line}" for line in highlights)
    return "\n".join(lines)


def _browser_raw_extract_dump_replacement(
    results: list[ToolResult],
    *,
    extracted_text: str | None = None,
    user_message: str | None = None,
) -> str:
    catalog_records_allowed = _browser_raw_extract_catalog_records_allowed(
        results,
        extracted_text=extracted_text,
    )
    if _browser_form_action_failed(results) and not _browser_raw_extract_has_substantive_result_evidence(
        extracted_text,
        allow_catalog_pairs=catalog_records_allowed,
    ):
        return _browser_incomplete_interaction_reply(results, user_message=user_message)
    catalog_reply = (
        _browser_catalog_reply_from_extract_text(extracted_text)
        if catalog_records_allowed
        else None
    )
    if catalog_reply:
        return catalog_reply
    if not catalog_records_allowed and _browser_catalog_signal_score(str(extracted_text or "")) >= 3:
        return _browser_unverified_catalog_text_reply(results, user_message=user_message)
    verified_state_reply = _browser_verified_state_reply(results)
    if verified_state_reply:
        return verified_state_reply
    if _browser_extracted_text_looks_like_site_blocker(extracted_text):
        return _browser_unverified_search_result_blocker(results, user_message=user_message)
    summary_lines = _browser_general_summary_lines(extracted_text)
    if summary_lines:
        lines = ["The page is open.", "", "I found:"]
        lines.extend(f"- {line}" for line in summary_lines)
        return "\n".join(lines)
    return "The page is open, but it only exposed navigation-style text. I do not have a reliable page summary from this run."


def _browser_form_action_failed(results: list[ToolResult]) -> bool:
    return any(
        result.tool_name in _BROWSER_FORM_ACTION_TOOLS and result.status != "completed"
        for result in results
    )


def _browser_raw_extract_has_substantive_result_evidence(
    extracted_text: object,
    *,
    allow_catalog_pairs: bool = False,
) -> bool:
    text = str(extracted_text or "")
    if not text.strip():
        return False
    if _browser_extract_substantive_evidence_lines(text, limit=1):
        return True
    if not allow_catalog_pairs:
        return False
    if _browser_catalog_signal_score(text) >= 3:
        return True
    return bool(_browser_catalog_line_pairs(text, limit=1))


def _browser_unverified_catalog_text_reply(
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str:
    return _browser_unverified_search_result_blocker(results, user_message=user_message)


def _browser_raw_extract_catalog_records_allowed(
    results: list[ToolResult],
    *,
    extracted_text: object | None = None,
) -> bool:
    expected = _normalized_reply_text(extracted_text) if extracted_text is not None else ""
    selected_index: int | None = None
    for result in results:
        if result.tool_name != "browser_extract_text" or not _tool_result_completed(result):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if expected:
            extracted_value = _browser_text_value_from_output(output, max_chars=20000)
            if _normalized_reply_text(extracted_value) != expected:
                continue
        selected_index = _tool_result_identity_index(results, result)
        if _mapping_flag_is_true(output, "records_verified"):
            return True
        if _mapping_flag_is_true(output, "structured_records"):
            return True
        metadata = output.get("metadata")
        if isinstance(metadata, Mapping):
            if _mapping_flag_is_true(metadata, "records_verified"):
                return True
            if _mapping_flag_is_true(metadata, "structured_records"):
                return True
    if selected_index is None:
        return False
    if _browser_catalog_allowed_by_completed_click(results, selected_index=selected_index):
        return True
    return _browser_catalog_allowed_by_superseded_assertion(
        results,
        selected_index=selected_index,
        extracted_text=extracted_text,
    )


def _browser_catalog_allowed_by_completed_click(results: list[ToolResult], *, selected_index: int) -> bool:
    return any(
        result.tool_name in {"browser_click_element", "browser_click_id"}
        and _tool_result_completed(result)
        for result in results[: selected_index + 1]
    )


def _browser_catalog_allowed_by_superseded_assertion(
    results: list[ToolResult],
    *,
    selected_index: int,
    extracted_text: object | None,
) -> bool:
    text = str(extracted_text or "")
    if not text.strip():
        return False
    for result in reversed(results[:selected_index]):
        if result.tool_name == "browser_wait_for":
            break
        if result.tool_name != "browser_assert_page_state":
            continue
        if _tool_result_completed(result):
            output = result.output if isinstance(result.output, dict) else {}
            state = output.get("result")
            state_payload = state if isinstance(state, Mapping) else {}
            if output.get("verified") is True or state_payload.get("ok") is True:
                return False
        missing = _browser_assertion_missing_required_labels(result)
        if missing and all(_browser_text_satisfies_probe(text, label) for label in missing):
            return True
        return False
    return False


def _mapping_flag_is_true(value: Mapping[str, Any], key: str) -> bool:
    return value.get(key) is True


def _browser_completed_interaction_count(results: list[ToolResult]) -> int:
    return sum(
        1
        for result in results
        if result.tool_name in _BROWSER_FORM_ACTION_TOOLS and _tool_result_completed(result)
    )


def _browser_catalog_reply_from_extract_text(extracted_text: object) -> str | None:
    pairs = _browser_catalog_line_pairs(str(extracted_text or ""), limit=5)
    if not pairs:
        return None
    return "Results I found\n\n" + "\n".join(f"- {label}: {price}" for label, price in pairs)


def _compact_browser_request_context(user_message: str | None) -> str:
    text = _compact_account_text(user_message)
    if not text:
        return ""
    if len(text) > 220:
        text = text[:217].rstrip() + "..."
    return text


def _browser_incomplete_interaction_reply(
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str:
    lines = [
        "Live-search blocker: the browser interaction needed to finish the request did not complete, so I do not have verified result records from this run."
    ]
    request_context = _compact_browser_request_context(user_message)
    if request_context:
        lines.extend(["", f"Requested task: {request_context}"])
    verified_state_reply = (
        _browser_verified_state_title_reply(results)
        if _browser_form_action_failed(results)
        else _browser_verified_state_reply(results)
    )
    if verified_state_reply:
        lines.extend(["", verified_state_reply])
    return "\n".join(lines)


def _browser_incomplete_reply_over_generic_verified_state(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str | None:
    if not _browser_form_action_failed(results):
        return None
    extracted_text = _latest_completed_browser_extract_text(results)
    if _browser_raw_extract_has_substantive_result_evidence(extracted_text):
        return None
    verified_state_reply = _browser_verified_state_reply(results)
    normalized_text = _normalized_reply_text(text).casefold()
    if _browser_reply_is_nonverifying_explanation(text):
        normalized_verified = _normalized_reply_text(verified_state_reply).casefold() if verified_state_reply else ""
        if normalized_verified and normalized_verified in normalized_text:
            if stripped := _strip_generic_browser_found_tail(text):
                return stripped
        return _browser_incomplete_interaction_reply(results, user_message=user_message)
    if not verified_state_reply:
        return None
    normalized_verified = _normalized_reply_text(verified_state_reply).casefold()
    if normalized_verified and normalized_verified in normalized_text:
        if stripped := _strip_generic_browser_found_tail(text):
            return stripped
        return _browser_incomplete_interaction_reply(results, user_message=user_message)
    return None


def _strip_generic_browser_found_tail(text: str) -> str | None:
    paragraphs = [paragraph.strip() for paragraph in re.split(r"\n\s*\n", str(text or "").strip()) if paragraph.strip()]
    if len(paragraphs) <= 2:
        return None
    last = _normalized_reply_text(paragraphs[-1]).casefold()
    if not last.startswith("i found"):
        return None
    kept = "\n\n".join(paragraphs[:-1]).strip()
    return kept or None


def _browser_withheld_raw_page_reply_over_extract(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> str | None:
    normalized = _normalized_reply_text(text).casefold()
    if "raw page text" not in normalized or "withheld" not in normalized:
        return None
    if "browser page" not in normalized and "opened" not in normalized:
        return None
    extracted_text = _latest_completed_browser_extract_text(results)
    if not extracted_text:
        return None
    return _browser_extract_reply_from_raw_text(
        extracted_text,
        results,
        user_message=user_message,
    )


def _browser_general_summary_lines(extracted_text: object, *, limit: int = 4) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for raw_line in str(extracted_text or "").splitlines():
        line = _clip_browser_extract_highlight(_compact_browser_extract_line(raw_line), max_chars=220)
        if not _browser_general_summary_line_allowed(line):
            continue
        key = _normalized_reply_text(line).casefold()
        if not key or key in seen:
            continue
        if any(key in existing or existing in key for existing in seen):
            continue
        seen.add(key)
        lines.append(line)
        if len(lines) >= limit:
            break
    return lines


def _browser_general_summary_line_allowed(line: str) -> bool:
    text = _compact_browser_extract_line(line)
    if len(text) < 24:
        return False
    if _browser_site_blocker_line(text):
        return False
    words = re.findall(r"[\w.-]+", text, flags=re.UNICODE)
    if len(words) < 5:
        return False
    alpha_count = sum(1 for char in text if char.isalpha())
    if alpha_count < 12:
        return False
    if text.startswith(("http://", "https://")):
        return False
    if len(text) < 40 and not re.search(r"[.!?]", text):
        return False
    return True


def _browser_extracted_text_looks_like_site_blocker(extracted_text: object) -> bool:
    lines = [
        _compact_browser_extract_line(line)
        for line in str(extracted_text or "").splitlines()
        if _compact_browser_extract_line(line)
    ]
    if not lines:
        return False
    if _browser_extract_substantive_evidence_lines(extracted_text, limit=1):
        return False
    blocker_count = sum(1 for line in lines[:20] if _browser_site_blocker_line(line))
    if blocker_count >= 2:
        return True
    return blocker_count == 1 and len(lines) <= 5


def _browser_site_blocker_line(line: object) -> bool:
    text = _normalized_reply_text(line).casefold()
    if not text:
        return False
    blocker_fragments = (
        "access denied",
        "are you a robot",
        "captcha",
        "checking your browser",
        "contact the site owner",
        "error code",
        "forbidden",
        "if the problem continues",
        "not authorized",
        "request blocked",
        "service unavailable",
        "temporarily unavailable",
        "verify you are human",
    )
    if any(fragment in text for fragment in blocker_fragments):
        return True
    return bool(re.search(r"\b(?:40[1349]|429|50[0234])\b", text) and re.search(r"\b(?:error|denied|blocked|unavailable|forbidden)\b", text))


def _browser_verified_state_reply_over_tool_status(text: str, results: list[ToolResult]) -> str | None:
    if _latest_verified_browser_assert_page_state(results) is None:
        return None
    normalized = _normalized_reply_text(text).casefold()
    if not normalized:
        return None
    leaked_status = (
        "verified the page state" in normalized
        or "page title:" in normalized
        or "confirmed:" in normalized
        or ("opened the browser page" in normalized and "verified" in normalized)
    )
    if not leaked_status:
        return None
    return _browser_verified_state_reply(results)


def _browser_verified_state_reply(results: list[ToolResult]) -> str | None:
    assertion = _latest_verified_browser_assert_page_state(results)
    if assertion is None:
        return None
    output = assertion.output if isinstance(assertion.output, dict) else {}
    state = output.get("result")
    state_payload = state if isinstance(state, dict) else {}
    title = _browser_assertion_title_label(state_payload)
    lines = [f"{title} is visible." if title else "The requested browser page is visible."]
    title_key = _normalized_reply_text(title).casefold() if title else ""
    source_key = _normalized_reply_text(_browser_assertion_source_label(state_payload) or "").casefold()
    confirmed = [
        label
        for label in _browser_assertion_confirmed_labels(state_payload)
        if _normalized_reply_text(label).casefold() not in {title_key, source_key}
    ]
    if confirmed:
        lines.extend(["", "I found:"])
        lines.extend(f"- {label}" for label in confirmed[:6])
    return "\n".join(lines)


def _browser_verified_state_title_reply(results: list[ToolResult]) -> str | None:
    assertion = _latest_verified_browser_assert_page_state(results)
    if assertion is None:
        return None
    output = assertion.output if isinstance(assertion.output, dict) else {}
    state = output.get("result")
    state_payload = state if isinstance(state, dict) else {}
    title = _browser_assertion_title_label(state_payload)
    return f"{title} is visible." if title else "The requested browser page is visible."


def _browser_assertion_title_label(state_payload: Mapping[str, Any]) -> str | None:
    title = state_payload.get("title")
    if isinstance(title, str):
        clean = re.sub(r"\s+", " ", title).strip()
        if clean:
            return clean
    return None


def _browser_assertion_source_label(state_payload: Mapping[str, Any]) -> str | None:
    for key in ("url", "page_url"):
        value = state_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in ("title", "page_title"):
        value = state_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _browser_assertion_confirmed_labels(state_payload: Mapping[str, Any]) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()
    required = state_payload.get("required")
    if not isinstance(required, list):
        return labels
    for item in required:
        if not isinstance(item, Mapping):
            continue
        value = item.get("expected")
        if not isinstance(value, str):
            continue
        label = re.sub(r"\s+", " ", value).strip()
        if len(label) < 3:
            continue
        normalized = label.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        labels.append(label)
    return labels


_DOMAIN_LIKE_RE = re.compile(r"\b(?:https?://)?(?:www\.)?([a-z0-9-]+(?:\.[a-z0-9-]+)+)\b", re.IGNORECASE)


def _browser_domain_query_terms(user_message: str | None) -> frozenset[str]:
    domain_terms: set[str] = set()
    for match in _DOMAIN_LIKE_RE.finditer(str(user_message or "")):
        domain = match.group(1).casefold()
        labels = [label for label in domain.split(".") if label]
        for label in labels[:-1]:
            term = re.sub(r"[_\W]+", "", label, flags=re.UNICODE).strip()
            if len(term) >= 4 and term != "www":
                domain_terms.add(term)
    return frozenset(domain_terms)


def _browser_query_terms(user_message: str | None) -> tuple[str, ...]:
    terms: list[str] = []
    domain_terms = _browser_domain_query_terms(user_message)
    for raw in re.findall(r"[\w-]+", str(user_message or "").casefold()):
        for token in raw.split("-"):
            term = re.sub(r"[_\W]+", "", token, flags=re.UNICODE).strip()
            if len(term) >= 4 and term not in domain_terms and term not in terms:
                terms.append(term)
    return tuple(terms[:12])


def _browser_extract_highlight_lines(
    extracted_text: str,
    *,
    query_terms: tuple[str, ...],
    limit: int = 5,
) -> list[str]:
    if not query_terms:
        return []
    lines = [
        _compact_browser_extract_line(line)
        for line in str(extracted_text or "").splitlines()
    ]
    lines = [line for line in lines if len(line) >= 8]
    if not lines:
        return []
    scored: list[tuple[int, int, int, int, str]] = []
    for index, line in enumerate(lines):
        normalized = line.casefold()
        matched_terms = tuple(term for term in query_terms if term and term in normalized)
        if matched_terms:
            long_term_hits = sum(1 for term in matched_terms if len(term) >= 6)
            unique_hits = len(matched_terms)
            occurrence_score = sum(normalized.count(term) for term in matched_terms)
            scored.append((long_term_hits, unique_hits, occurrence_score, index, line))
    if not scored:
        return []
    ranked = [
        line
        for _long_hits, _unique_hits, _occurrences, _index, line in sorted(
            scored,
            key=lambda item: (-item[0], -item[1], -item[2], item[3]),
        )
    ]
    highlights: list[str] = []
    seen: set[str] = set()
    for line in ranked:
        clipped = _clip_browser_extract_highlight(line)
        key = _normalized_reply_text(clipped).casefold()
        if not key or key in seen:
            continue
        if any(key in existing or existing in key for existing in seen):
            continue
        highlights.append(clipped)
        seen.add(key)
        if len(highlights) >= limit:
            break
    return highlights


def _browser_extract_substantive_evidence_lines(extracted_text: object, *, limit: int = 4) -> list[str]:
    raw_lines = [
        _compact_browser_extract_line(line)
        for line in str(extracted_text or "").splitlines()
        if _compact_browser_extract_line(line)
    ]
    candidates: list[str] = []
    numeric_status_candidates: list[str] = []
    for line in raw_lines:
        if _CURRENCY_AMOUNT_RE.search(line):
            candidates.append(line)
        if re.match(
            r"^\d{1,9}\s+(?!(?:h|m|min|mins|hr|hrs|hour|hours)\b)[^\W\d_][\w -]{1,80}$",
            line,
            re.IGNORECASE,
        ):
            numeric_status_candidates.append(line)
        tokens = [token.strip() for token in re.split(r"\s+\|\s+|\|", line) if token.strip()]
        for index, token in enumerate(tokens):
            if token not in _SPLIT_CURRENCY_SYMBOLS or index + 1 >= len(tokens):
                continue
            whole = tokens[index + 1].strip()
            if not _SPLIT_CURRENCY_WHOLE_RE.match(whole):
                continue
            end_index = index + 1
            decimal = ""
            if index + 2 < len(tokens) and _SPLIT_CURRENCY_DECIMAL_RE.match(tokens[index + 2].strip()):
                decimal = tokens[index + 2].strip()
                end_index = index + 2
            amount = f"{token}{whole}{decimal}"
            before = _browser_extract_context_tokens(tokens[max(0, index - 4) : index])
            after = _browser_extract_context_tokens(tokens[end_index + 1 : end_index + 10])
            window = [*before, amount, *after]
            if window:
                candidates.append(" | ".join(window))
    if not candidates:
        candidates.extend(numeric_status_candidates)
    return _dedupe_browser_extract_highlights(candidates, limit=limit)


def _browser_extract_context_tokens(tokens: Iterable[object]) -> list[str]:
    context: list[str] = []
    for raw_token in tokens:
        token = _compact_browser_extract_line(raw_token)
        if not token:
            continue
        if token in _SPLIT_CURRENCY_SYMBOLS:
            continue
        if _SPLIT_CURRENCY_WHOLE_RE.match(token) or _SPLIT_CURRENCY_DECIMAL_RE.match(token):
            continue
        context.append(token)
    return context


def _merge_browser_extract_highlights(*groups: Iterable[str]) -> list[str]:
    merged: list[str] = []
    for group in groups:
        merged.extend(group)
    return _dedupe_browser_extract_highlights(merged, limit=5)


def _dedupe_browser_extract_highlights(candidates: Iterable[object], *, limit: int) -> list[str]:
    highlights: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        clipped = _clip_browser_extract_highlight(_compact_browser_extract_line(candidate))
        key = _normalized_reply_text(clipped).casefold()
        if not key or key in seen:
            continue
        if any(key in existing or existing in key for existing in seen):
            continue
        highlights.append(clipped)
        seen.add(key)
        if len(highlights) >= limit:
            break
    return highlights


def _compact_browser_extract_line(line: object) -> str:
    text = str(line or "").replace("\t", " | ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s+\|\s+", " | ", text)
    return text


def _clip_browser_extract_highlight(line: str, *, max_chars: int = 260) -> str:
    text = _compact_browser_extract_line(line)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip(" ,;:-") + "..."


def _browser_source_label_from_result(result: ToolResult) -> str | None:
    output = result.output if isinstance(result.output, dict) else {}
    for key in ("page_title", "title"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in ("page_url", "url"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    value = output.get("result")
    if isinstance(value, str):
        match = re.search(r"https?://[^\s)]+", value)
        if match:
            return match.group(0).rstrip(".,")
    return None


def _browser_source_label_for_extract_text(results: list[ToolResult], extracted_text: str) -> str | None:
    selected_index: int | None = None
    selected_key = _normalized_reply_text(extracted_text).casefold()
    if not selected_key:
        return None
    for index, result in enumerate(results):
        if result.tool_name != "browser_extract_text" or not _tool_result_completed(result):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        value = _browser_text_value_from_output(output, max_chars=20000)
        if _normalized_reply_text(value).casefold() == selected_key:
            selected_index = index
            break
    if selected_index is None:
        return None
    for result in reversed(results[: selected_index + 1]):
        if result.tool_name not in {"browser_extract_text", "browser_navigate", "browser_open", "web_fetch"}:
            continue
        if label := _browser_source_label_from_result(result):
            return label
    return None


def _browser_source_label(results: list[ToolResult]) -> str | None:
    for result in reversed(results):
        if label := _browser_source_label_from_result(result):
            return label
    return None


def _strip_leading_tool_status_paragraph(text: str, results: list[ToolResult]) -> str:
    paragraphs = re.split(r"\n\s*\n", str(text or "").strip())
    if len(paragraphs) <= 1:
        return text
    first = _normalized_reply_text(paragraphs[0]).casefold()
    if not first:
        return text
    tool_markers: set[str] = set()
    for result in results:
        name = str(getattr(result, "tool_name", "") or "").strip()
        if not name:
            continue
        tool_markers.add(name.casefold())
        tool_markers.add(name.replace("_", " ").casefold())
    if not any(marker and marker in first for marker in tool_markers):
        return text
    evidence = {
        _normalized_reply_text(value).casefold()
        for result in results
        for value in _tool_result_visible_evidence_strings(result)
    }
    if any(value and len(value) >= 8 and value in first for value in evidence):
        return text
    remaining = "\n\n".join(paragraph.strip() for paragraph in paragraphs[1:] if paragraph.strip()).strip()
    return remaining or text


def _tool_result_visible_evidence_strings(result: ToolResult) -> list[str]:
    values: list[str] = []
    output = result.output if isinstance(result.output, dict) else {}
    if getattr(result, "error", None):
        values.append(str(result.error))
    for key in ("message", "summary", "text", "title", "url", "page_url", "page_title", "error", "reason"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value)
    for key in ("items", "results"):
        records = output.get(key)
        if not isinstance(records, list):
            continue
        for item in records[:5]:
            if isinstance(item, Mapping):
                for value in item.values():
                    if isinstance(value, str) and value.strip():
                        values.append(value)
    return values


def _strip_browser_assertion_meta_drift(text: str, assertion: ToolResult) -> str | None:
    paragraphs = re.split(r"\n\s*\n", text.strip())
    if len(paragraphs) <= 1:
        return None
    first = paragraphs[0]
    if not _reply_echoes_tool_failure(first, assertion):
        return None
    remaining = "\n\n".join(paragraph.strip() for paragraph in paragraphs[1:] if paragraph.strip()).strip()
    return remaining or None


def _latest_completed_browser_extract_text(results: list[ToolResult], *, max_chars: int = 5000) -> str | None:
    for result in reversed(results):
        if result.tool_name != "browser_extract_text" or not _tool_result_completed(result):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if value := _browser_text_value_from_output(output, max_chars=max_chars):
            return value.strip()
    return None


def _latest_unverified_browser_assert_page_state(results: list[ToolResult]) -> ToolResult | None:
    for result in reversed(results):
        if result.tool_name == "browser_wait_for" and not _tool_result_completed(result):
            return result
        if result.tool_name != "browser_assert_page_state":
            continue
        if not _tool_result_completed(result):
            return result
        output = result.output if isinstance(result.output, dict) else {}
        if output.get("verified") is False:
            return result
    return None


def _browser_missing_required_state_reply_over_structured_evidence(
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str | None:
    assertion = _latest_unverified_browser_assert_page_state(results)
    if assertion is None:
        return None
    missing = _browser_assertion_missing_required_labels(assertion)
    forbidden = _browser_assertion_forbidden_found_labels(assertion)
    if _browser_extract_items_reply_over_blocker(results, user_message=user_message):
        if not missing and not forbidden:
            return None
        if missing and not forbidden and _browser_items_satisfy_labels(results, missing):
            return None
    if missing and _browser_missing_required_state_superseded_by_later_extract(
        results,
        assertion=assertion,
        missing=missing,
    ):
        return None
    lines = [
        "Live-search blocker: I could not verify the requested page state, so I do not have verified result records from this run."
    ]
    request_context = _compact_browser_request_context(user_message)
    if request_context:
        lines.extend(["", f"Requested task: {request_context}"])
    if missing:
        lines.extend(["", "Still missing:"])
        lines.extend(f"- {label}" for label in missing[:6])
    if forbidden:
        lines.extend(["", "Unexpected page state found:"])
        lines.extend(f"- {label}" for label in forbidden[:6])
    return "\n".join(lines)


def _browser_items_satisfy_labels(results: list[ToolResult], labels: Iterable[str]) -> bool:
    records = _completed_browser_extract_item_records(results)
    if not records:
        return False
    text = "\n".join(_browser_extract_item_query_text(item) for item in records)
    return all(_browser_text_satisfies_probe(text, label) for label in labels)


def _browser_missing_required_state_superseded_by_later_extract(
    results: list[ToolResult],
    *,
    assertion: ToolResult,
    missing: list[str],
) -> bool:
    if not missing:
        return False
    assertion_index = _tool_result_identity_index(results, assertion)
    if assertion_index is None:
        return False
    later_texts: list[str] = []
    for result in results[assertion_index + 1 :]:
        if result.tool_name != "browser_extract_text" or not _tool_result_completed(result):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if value := _browser_text_value_from_output(output, max_chars=20000):
            later_texts.append(value)
    if not later_texts:
        return False
    combined = "\n".join(later_texts)
    if not all(_browser_text_satisfies_probe(combined, label) for label in missing):
        return False
    return True


def _tool_result_identity_index(results: list[ToolResult], target: ToolResult) -> int | None:
    for index, result in enumerate(results):
        if result is target:
            return index
    return None


def _browser_text_satisfies_probe(text: object, probe: object) -> bool:
    normalized_text = _browser_probe_normalized_text(text)
    normalized_probe = _browser_probe_normalized_text(probe)
    if not normalized_text or not normalized_probe:
        return False
    if normalized_probe in normalized_text:
        return True
    probe_tokens = normalized_probe.split()
    if not probe_tokens:
        return False
    return all(token in normalized_text.split() for token in probe_tokens)


def _browser_probe_normalized_text(value: object) -> str:
    tokens = re.findall(r"[a-z0-9]+", str(value or "").casefold())
    normalized_tokens = [
        str(int(token)) if token.isdecimal() else token
        for token in tokens
    ]
    return " ".join(normalized_tokens)


def _latest_browser_assert_page_state_with_missing_required(results: list[ToolResult]) -> ToolResult | None:
    for result in reversed(results):
        if result.tool_name != "browser_assert_page_state":
            continue
        if not _tool_result_completed(result):
            return result
        output = result.output if isinstance(result.output, dict) else {}
        state = output.get("result")
        state_payload = state if isinstance(state, Mapping) else {}
        if output.get("verified") is True or state_payload.get("ok") is True:
            return None
        if _browser_assertion_missing_required_labels(result):
            return result
    return None


def _browser_assertion_missing_required_labels(assertion: ToolResult) -> list[str]:
    output = assertion.output if isinstance(assertion.output, dict) else {}
    labels: list[str] = []
    for value in output.get("missing_required") or ():
        label = _clip_browser_extract_item_text(value, max_chars=80)
        if label:
            labels.append(label)
    state = output.get("result")
    state_payload = state if isinstance(state, Mapping) else {}
    missing = state_payload.get("missing")
    if isinstance(missing, list):
        for item in missing:
            if isinstance(item, Mapping):
                label = _clip_browser_extract_item_text(item.get("expected"), max_chars=80)
            else:
                label = _clip_browser_extract_item_text(item, max_chars=80)
            if label:
                labels.append(label)
    return _dedupe_preserving_order(labels)


def _browser_assertion_forbidden_found_labels(assertion: ToolResult) -> list[str]:
    output = assertion.output if isinstance(assertion.output, dict) else {}
    labels: list[str] = []
    for value in output.get("forbidden_found") or ():
        if isinstance(value, Mapping):
            label = _clip_browser_extract_item_text(value.get("expected") or value.get("match"), max_chars=80)
        else:
            label = _clip_browser_extract_item_text(value, max_chars=80)
        if label:
            labels.append(label)
    state = output.get("result")
    state_payload = state if isinstance(state, Mapping) else {}
    forbidden = state_payload.get("forbidden_found")
    if isinstance(forbidden, list):
        for item in forbidden:
            if isinstance(item, Mapping):
                label = _clip_browser_extract_item_text(item.get("expected") or item.get("match"), max_chars=80)
            else:
                label = _clip_browser_extract_item_text(item, max_chars=80)
            if label:
                labels.append(label)
    return _dedupe_preserving_order(labels)


def _latest_verified_browser_assert_page_state(results: list[ToolResult]) -> ToolResult | None:
    for result in reversed(results):
        if result.tool_name != "browser_assert_page_state" or not _tool_result_completed(result):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        state = output.get("result")
        state_payload = state if isinstance(state, dict) else {}
        if output.get("verified") is True or state_payload.get("ok") is True:
            return result
    return None


def _reply_mentions_browser_extract_evidence(text: object, extracted_text: object) -> bool:
    normalized_reply = _normalized_reply_text(text).casefold()
    if not normalized_reply:
        return False
    lines = [
        _normalized_reply_text(line).casefold()
        for line in str(extracted_text or "").splitlines()
    ]
    evidence_lines = [
        line
        for line in lines
        if len(line) >= 24
        and not line.isdecimal()
    ]
    for line in evidence_lines[:40]:
        if line in normalized_reply:
            return True
    return False


def _reply_echoes_tool_failure(text: str, result: ToolResult) -> bool:
    normalized_reply = _normalized_reply_text(text)
    if not normalized_reply:
        return False
    probes: list[str] = []
    if result.error:
        probes.append(str(result.error))
    output = result.output if isinstance(result.output, dict) else {}
    for key in ("message", "error", "reason", "summary"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            probes.append(value)
    for value in output.values():
        if isinstance(value, str) and value.strip():
            probes.append(value)
    result_payload = output.get("result")
    if isinstance(result_payload, dict):
        missing = result_payload.get("missing")
        if isinstance(missing, list):
            for item in missing:
                if isinstance(item, dict):
                    value = item.get("expected")
                    if isinstance(value, str) and value.strip():
                        probes.append(value)
        forbidden_found = result_payload.get("forbidden_found")
        if isinstance(forbidden_found, list):
            for item in forbidden_found:
                if isinstance(item, dict):
                    for key in ("expected", "match"):
                        value = item.get(key)
                        if isinstance(value, str) and value.strip():
                            probes.append(value)
    missing_required = output.get("missing_required")
    if isinstance(missing_required, list):
        probes.extend(str(value).strip() for value in missing_required if str(value).strip())
    forbidden_found = output.get("forbidden_found")
    if isinstance(forbidden_found, list):
        probes.extend(str(value).strip() for value in forbidden_found if str(value).strip())
    for probe in probes:
        normalized_probe = _normalized_reply_text(probe)
        if normalized_probe and (
            normalized_probe in normalized_reply
            or normalized_reply in normalized_probe
        ):
            return True
    return False


def _requested_artifact_extensions_from_results(results: list[ToolResult]) -> set[str]:
    extensions: set[str] = set()
    for result in results:
        if result.tool_name != "request_tool_scope" or not isinstance(result.output, dict):
            continue
        for key in ("artifact_extensions", "requested_artifact_extensions"):
            raw = result.output.get(key)
            if not isinstance(raw, (list, tuple)):
                continue
            for value in raw:
                extension = str(value or "").strip().lower()
                if extension and not extension.startswith("."):
                    extension = f".{extension}"
                if extension:
                    extensions.add(extension)
    return extensions


def _completed_artifact_paths_for_extensions(results: list[ToolResult], extensions: set[str]) -> list[str]:
    paths: list[str] = []
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        values: list[object] = []
        for key in ("artifact_path", "path", "output_path"):
            value = result.output.get(key)
            if value:
                values.append(value)
        artifact_paths = result.output.get("artifact_paths")
        if isinstance(artifact_paths, (list, tuple)):
            values.extend(artifact_paths)
        for value in values:
            path = str(value or "").strip()
            if path and Path(path).suffix.lower() in extensions and path not in paths:
                paths.append(path)
    return paths


_EMAIL_TRANSPORT_PAREN_RE = re.compile(
    r"\s*\((?:\s*(?:status\s+\d{3}|message\s+id\s+[A-Za-z0-9_.:-]+)\s*,?)+\)",
    flags=re.IGNORECASE,
)
_EMAIL_TRANSPORT_LINE_RE = re.compile(
    r"^\s*(?:[-*]\s*)?(?:\*\*)?(?:message\s+id|status)\s*(?::|\*\*:|[:#])?\s*(?:\*\*)?\s*`?[A-Za-z0-9_.:-]+`?\s*\.?\s*$",
    flags=re.IGNORECASE,
)


def _sanitize_email_send_transport_details(text: str) -> str:
    cleaned = _EMAIL_TRANSPORT_PAREN_RE.sub("", text)
    lines = [
        line
        for line in cleaned.splitlines()
        if not _EMAIL_TRANSPORT_LINE_RE.match(line)
    ]
    return "\n".join(lines).strip()


_ACCOUNT_TRANSPORT_LINE_RE = re.compile(
    r"^\s*(?:[-*]\s*)?(?:\*\*)?(?:message\s+id|response\s+code|status\s+code)\s*(?::|\*\*:|[:#])?\s*(?:\*\*)?\s*`?[A-Za-z0-9_.:-]+`?\s*\.?\s*$",
    flags=re.IGNORECASE,
)
_ACCOUNT_CONNECTOR_DIAGNOSTIC_SENTENCE_RE = re.compile(
    r"(?:(?:^|(?<=[.!?])\s+))"
    r"[^.!?]*(?:lower-level\s+connector\s+route|connector\s+URL\s+policy|connector\s+request|api\.maton\.ai|HTTP\s+Error\s+\d{3})"
    r"[^.!?]*[.!?]?",
    flags=re.IGNORECASE,
)
_INTERNAL_TOOL_NAME_RE = re.compile(
    r"\b(?:"
    r"calendar_list|connector_request|email_attachment_read|email_read|email_search|email_send|skill_pack_read|"
    r"browser_extract_items|browser_extract_text|browser_navigate|browser_open|browser_run_js|browser_screenshot|"
    r"terminal_exec|file_read|file_write"
    r")\b"
)
_INTERNAL_TOOL_NAME_SENTENCE_RE = re.compile(
    r"(?:(?:^|(?<=[.!?])\s+))"
    r"[^.!?\n]*"
    r"(?:calendar_list|connector_request|email_attachment_read|email_read|email_search|email_send|skill_pack_read|"
    r"browser_extract_items|browser_extract_text|browser_navigate|browser_open|browser_run_js|browser_screenshot|"
    r"terminal_exec|file_read|file_write)"
    r"[^.!?\n]*[.!?]?",
    flags=re.IGNORECASE,
)
_BROWSER_TOOL_PHRASE_RE = re.compile(
    r"\b(?:with|using|through)\s+the\s+(?:available\s+)?browser\s+tool\b",
    flags=re.IGNORECASE,
)
_BROWSER_TOOL_NOUN_RE = re.compile(r"\b(?:available\s+)?browser\s+tool\b", flags=re.IGNORECASE)


def _sanitize_account_tool_transport_details(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str:
    if not any(result.tool_name in {"email_search", "email_read", "email_attachment_read", "calendar_list", "connector_request"} for result in results):
        return text
    lines = [
        line
        for line in str(text).splitlines()
        if not _ACCOUNT_TRANSPORT_LINE_RE.match(line)
    ]
    cleaned = "\n".join(lines).strip()
    cleaned = _ACCOUNT_CONNECTOR_DIAGNOSTIC_SENTENCE_RE.sub("", cleaned).strip()
    primary_family = _primary_account_tool_family(results)
    if not cleaned:
        return _account_tool_summary(results, user_message=user_message) or cleaned
    return cleaned


def _sanitize_account_tool_family_drift(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str:
    primary_family = _primary_account_tool_family(results)
    if primary_family is None:
        return text
    families = {
        family
        for result in results
        if (family := _account_tool_family(result.tool_name)) is not None
        and result.status == "completed"
        and isinstance(result.output, dict)
    }
    if len(families) < 2:
        return text
    if _reply_mentions_account_family_evidence(text, results, primary_family):
        return text
    for family in families:
        if family == primary_family:
            continue
        if _reply_mentions_account_family_evidence(text, results, family):
            return _account_tool_summary(results, user_message=user_message) or text
    return text


def _account_tool_reply_over_ungrounded_result(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str:
    summary = _account_tool_summary(results, user_message=user_message)
    if not summary:
        return text
    primary_family = _primary_account_tool_family(results)
    if primary_family is None:
        return text
    if _reply_mentions_account_family_evidence(text, results, primary_family):
        return text
    if not _normalized_reply_text(text):
        return summary
    if _has_uncompleted_required_account_tool(results, primary_family):
        return summary
    if _primary_account_tool_family_has_empty_result(results, primary_family):
        return summary
    return text


def _primary_account_tool_family_has_empty_result(results: list[ToolResult], family: str) -> bool:
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if _account_tool_family(result.tool_name) != family:
            continue
        output = result.output
        if result.tool_name in {"email_search", "calendar_list"}:
            records = output.get("results")
            count = output.get("count", output.get("result_count", output.get("resultSizeEstimate")))
            if records == [] or count == 0:
                return True
    return False


def _sanitize_account_tool_body_leak(text: str, results: list[ToolResult]) -> str:
    """Replace copied account message bodies with the structured account summary.

    This branches on completed tool-result payload shape, not on the user's
    wording. If a model copies a long email body into the visible answer, the
    safe fallback should be the compact account summary instead.
    """

    visible = _compact_account_text(text)
    if len(visible) < 180:
        return text
    for result in results:
        if result.status != "completed" or result.tool_name != "email_read" or not isinstance(result.output, dict):
            continue
        message = result.output.get("message")
        if not isinstance(message, Mapping):
            continue
        body = _compact_account_text(message.get("body"))
        if len(body) < 180:
            continue
        probe = body[: min(len(body), 320)]
        if body in visible or (len(probe) >= 180 and probe in visible):
            return _email_read_summary(result.output)
    return text


def _account_read_reply_over_invalid_attachment_detour(text: str, results: list[ToolResult]) -> str | None:
    """Prefer completed email body evidence over an invalid attachment detour."""

    completed_email_read: ToolResult | None = None
    completed_email_read_index = -1
    for index, result in enumerate(results):
        if result.status == "completed" and result.tool_name == "email_read" and isinstance(result.output, dict):
            completed_email_read = result
            completed_email_read_index = index
    if completed_email_read is None:
        return None
    message = completed_email_read.output.get("message")
    if not isinstance(message, Mapping):
        return None
    subject = str(message.get("subject") or "").strip()
    if subject and subject.casefold() in str(text or "").casefold():
        return None
    for result in results[completed_email_read_index + 1 :]:
        if result.status == "completed" or result.tool_name != "email_attachment_read":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        attachment_id = str(output.get("attachment_id") or "").strip().casefold()
        message_id = str(output.get("message_id") or "").strip().casefold()
        if attachment_id in {"", "unknown"} or message_id in {"", "unknown"}:
            return _email_read_summary(completed_email_read.output)
    return None


def _account_read_reply_over_browser_detour(text: str, results: list[ToolResult]) -> str | None:
    """Prefer completed email body evidence over a later browser page detour."""

    completed_email_read: ToolResult | None = None
    completed_email_read_index = -1
    for index, result in enumerate(results):
        if result.status == "completed" and result.tool_name == "email_read" and isinstance(result.output, dict):
            completed_email_read = result
            completed_email_read_index = index
    if completed_email_read is None:
        return None
    browser_detour = False
    for result in results[completed_email_read_index + 1 :]:
        if result.status != "completed" or not result.tool_name.startswith("browser_"):
            continue
        if result.tool_name != "browser_extract_text" or not isinstance(result.output, dict):
            browser_detour = True
            continue
        extracted_text = result.output.get("text") or result.output.get("content") or ""
        if _reply_mentions_browser_extract_evidence(text, extracted_text):
            browser_detour = True
            break
    if not browser_detour:
        return None
    message = completed_email_read.output.get("message")
    if not isinstance(message, Mapping):
        return None
    normalized = str(text or "").casefold()
    subject = str(message.get("subject") or "").strip()
    sender = str(message.get("from") or "").strip()
    if subject and subject.casefold() in normalized:
        return None
    if sender and sender.casefold() in normalized:
        return None
    return _email_read_summary(completed_email_read.output)


def _compact_account_text(value: object) -> str:
    return re.sub(r"\s+", " ", _strip_invisible_tracking_text(str(value or ""))).strip()


def _image_generation_scope_unavailable_reply(results: list[ToolResult]) -> str | None:
    if _has_completed_deliverable_artifact_result(results):
        return None
    for result in results:
        if result.tool_name != "request_tool_scope" or result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        capabilities = {
            str(value or "").strip().lower()
            for value in output.get("capabilities", [])
            if str(value or "").strip()
        } if isinstance(output.get("capabilities"), list) else set()
        if "image_generation" not in capabilities:
            continue
        available_tools = {
            str(value or "").strip()
            for value in output.get("available_tools", [])
            if str(value or "").strip()
        } if isinstance(output.get("available_tools"), list) else set()
        if "image_generate" in available_tools:
            continue
        return f"Image generation is not configured.\n\n{format_setup_tip(IMAGE_GENERATION_SETUP_TIP)}"
    return None


def _has_completed_deliverable_artifact_result(results: list[ToolResult]) -> bool:
    for result in results:
        if result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        for key in ("artifact_path", "path"):
            value = output.get(key)
            if isinstance(value, str) and Path(value).suffix:
                return True
        for key in ("artifact_paths", "artifacts"):
            values = output.get(key)
            if not isinstance(values, list):
                continue
            if any(isinstance(value, str) and Path(value).suffix for value in values):
                return True
    return False


def _account_tool_family_has_structured_records(results: list[ToolResult], family: str) -> bool:
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if _account_tool_family(result.tool_name) != family:
            continue
        output = result.output
        if result.tool_name in {"calendar_list", "email_search"}:
            records = output.get("results")
            if isinstance(records, list):
                if not records:
                    return True
                if any(isinstance(item, Mapping) for item in records):
                    return True
        if result.tool_name == "email_read" and isinstance(output.get("message"), Mapping):
            return True
        if result.tool_name == "email_attachment_read" and (
            output.get("artifact_path") or output.get("filename") or output.get("text_preview")
        ):
            return True
    return False


def _primary_account_tool_family(results: list[ToolResult]) -> str | None:
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        family = _account_tool_family(result.tool_name)
        if family is not None:
            return family
    return None


def _account_tool_family(tool_name: str) -> str | None:
    if tool_name in {"email_search", "email_read", "email_attachment_read", "email_send"}:
        return "email"
    if tool_name == "calendar_list":
        return "calendar"
    return None


def _has_uncompleted_required_account_tool(results: list[ToolResult], family: str) -> bool:
    completed = {
        result.tool_name
        for result in results
        if result.status == "completed"
    }
    for result in results:
        if result.tool_name != "request_tool_scope" or result.status != "completed" or not isinstance(result.output, dict):
            continue
        required = result.output.get("required_tool_names")
        if not isinstance(required, list):
            continue
        for tool_name in required:
            normalized = str(tool_name or "").strip()
            if normalized and _account_tool_family(normalized) == family and normalized not in completed:
                return True
    return False


def _prefix_account_tool_reply(text: str, results: list[ToolResult]) -> str:
    family = _primary_account_tool_family(results)
    if family is None:
        return text
    stripped = str(text or "").lstrip()
    if not stripped:
        return text
    summary = _account_tool_summary(results)
    normalized_stripped = _normalized_reply_text(stripped)
    normalized_summary = _normalized_reply_text(summary or "")
    if not (
        _reply_mentions_account_family_evidence(stripped, results, family)
        or (normalized_summary and normalized_stripped == normalized_summary)
    ):
        return text
    prefix = "✉️" if family == "email" else "📅" if family == "calendar" else ""
    if not prefix or stripped.startswith(prefix):
        return text
    leading = str(text or "")[: len(str(text or "")) - len(stripped)]
    return f"{leading}{prefix} {stripped}"


def _reply_mentions_account_family_evidence(text: str, results: list[ToolResult], family: str) -> bool:
    normalized_text = str(text or "").casefold()
    if not normalized_text:
        return False
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if _account_tool_family(result.tool_name) != family:
            continue
        for evidence in _account_tool_evidence_strings(result):
            if evidence.casefold() in normalized_text:
                return True
    return False


def _account_tool_evidence_strings(result: ToolResult) -> list[str]:
    output = result.output if isinstance(result.output, dict) else {}
    values: list[str] = []
    if result.tool_name == "calendar_list":
        records = output.get("results")
        for item in records if isinstance(records, list) else []:
            if isinstance(item, Mapping):
                values.extend([
                    str(item.get("summary") or ""),
                    str(item.get("description") or ""),
                    str(item.get("location") or ""),
                ])
    elif result.tool_name == "email_search":
        records = output.get("results")
        for item in records if isinstance(records, list) else []:
            if isinstance(item, Mapping):
                values.extend([
                    str(item.get("subject") or ""),
                    str(item.get("from") or ""),
                    str(item.get("snippet") or ""),
                ])
    elif result.tool_name == "email_read":
        message = output.get("message")
        if isinstance(message, Mapping):
            values.extend([
                str(message.get("subject") or ""),
                str(message.get("from") or ""),
                str(message.get("snippet") or ""),
            ])
    elif result.tool_name == "email_attachment_read":
        values.extend([
            str(output.get("filename") or ""),
            str(output.get("mime_type") or ""),
            str(output.get("artifact_path") or ""),
        ])
    elif result.tool_name == "email_send":
        values.extend([
            str(output.get("subject") or ""),
            str(output.get("id") or ""),
            str(output.get("message_id") or ""),
        ])
        recipients = output.get("to")
        if isinstance(recipients, str):
            values.append(recipients)
        elif isinstance(recipients, (list, tuple)):
            values.extend(str(item or "") for item in recipients)
    evidence: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        stripped = value.strip()
        if len(stripped) >= 3:
            evidence.append(stripped)
        for fragment in re.split(r"\s+(?:[-–—]|/)\s+|[:;]\s+", stripped):
            fragment = fragment.strip()
            if len(fragment) >= 8:
                evidence.append(fragment)
    return list(dict.fromkeys(evidence))


def _sanitize_internal_tool_state_phrasing(
    text: str,
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str:
    raw = str(text or "")
    if not raw.strip():
        return raw
    raw = _sanitize_browser_tool_user_phrasing(raw, results)
    source_failure_summary = _source_required_failure_account_summary(results, user_message=user_message)
    if source_failure_summary is not None:
        return source_failure_summary
    structured_reply = _structured_internal_tool_state_reply(results, user_message=user_message)
    if structured_reply is not None:
        return structured_reply
    if _INTERNAL_TOOL_NAME_RE.search(raw):
        cleaned = _INTERNAL_TOOL_NAME_SENTENCE_RE.sub("", raw).strip()
        if cleaned:
            raw = cleaned
        else:
            summary = _account_tool_summary(results, user_message=user_message)
            return summary or raw
    if not raw:
        summary = _account_tool_summary(results, user_message=user_message)
        return summary or raw
    return raw


def _source_required_failure_account_summary(
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str | None:
    for result in results:
        if result.status not in {"failed", "error"} or not isinstance(result.output, dict):
            continue
        reason = str(result.output.get("reason") or "").strip()
        if not reason.startswith("invalid_"):
            continue
        for required_source in _completed_source_tool_names_for_failed_result(results, result):
            if result.tool_name == "email_read" and required_source == "email_search":
                email_summary = _email_search_source_failure_summary(results)
                if email_summary:
                    return email_summary
            if result.tool_name == "email_attachment_read" and required_source == "email_read":
                for source_result in reversed(results):
                    if source_result.status == "completed" and source_result.tool_name == "email_read" and isinstance(source_result.output, dict):
                        return _email_read_summary(source_result.output)
            return (
                _source_required_failure_structured_summary(
                    results,
                    failed_result=result,
                    required_source_tool=required_source,
                )
                or _account_tool_summary(results, user_message=user_message)
            )
    return None


def _completed_source_tool_names_for_failed_result(
    results: list[ToolResult],
    failed_result: ToolResult,
) -> tuple[str, ...]:
    completed_outputs = {
        result.tool_name: result.output
        for result in results
        if result.status == "completed" and isinstance(result.output, dict)
    }
    names: list[str] = []
    output = failed_result.output if isinstance(failed_result.output, dict) else {}
    required_source = str(output.get("required_source_tool") or "").strip()
    if required_source in completed_outputs:
        names.append(required_source)
    failed_tool = str(getattr(failed_result, "tool_name", "") or "").strip()
    for source_tool in _READ_TOOL_SOURCE_TOOLS.get(failed_tool, ()):
        if source_tool in names:
            continue
        source_output = completed_outputs.get(source_tool)
        if not isinstance(source_output, Mapping):
            continue
        records = _structured_records(source_output)
        if not records:
            continue
        next_tool = str(source_output.get("next_tool_for_body") or source_output.get("next_tool") or "").strip()
        body_requires_tool = source_output.get("body_requires_tool")
        body_required_name = str(body_requires_tool or "").strip()
        if next_tool == failed_tool or body_required_name == failed_tool or body_requires_tool is True:
            names.append(source_tool)
            continue
        if source_tool in _READ_TOOL_SOURCE_TOOLS.get(failed_tool, ()):
            names.append(source_tool)
    return tuple(dict.fromkeys(names))


def _source_required_failure_structured_summary(
    results: list[ToolResult],
    *,
    failed_result: ToolResult,
    required_source_tool: str,
) -> str | None:
    source_output: Mapping[str, Any] | None = None
    for result in reversed(results):
        if result.tool_name != required_source_tool or result.status != "completed" or not isinstance(result.output, dict):
            continue
        source_output = result.output
        break
    if source_output is None:
        return None
    records = _structured_records(source_output)
    failed_tool = str(getattr(failed_result, "tool_name", "") or "").strip()
    lines = [
        "I found structured source evidence, but I could not open the selected detail record.",
        "",
        "What is proven from the source step:",
    ]
    if records:
        noun = "record" if len(records) == 1 else "records"
        lines.append(f"- {len(records)} source {noun} returned.")
        for record in records[:3]:
            summary = _structured_record_summary(record)
            if summary:
                lines.append(f"- {summary}")
    else:
        lines.append("- The source step completed, but it did not expose detailed records.")
    next_tool = (
        str(source_output.get("next_tool_for_body") or "").strip()
        or str(source_output.get("next_tool") or "").strip()
        or failed_tool
    )
    if next_tool:
        lines.extend(
            [
                "",
                "What is not proven yet:",
                f"- I still need to open a matching detail record with `{next_tool}` before treating the source list as final.",
            ]
        )
    lines.extend(
        [
            "",
            "I will not treat the source list as the final answer by itself.",
        ]
    )
    return "\n".join(lines)


def _email_search_source_failure_summary(results: list[ToolResult]) -> str | None:
    search_output: Mapping[str, Any] | None = None
    for result in reversed(results):
        if result.tool_name != "email_search" or result.status != "completed" or not isinstance(result.output, dict):
            continue
        output_candidates: list[Mapping[str, Any]] = [result.output]
        preview = result.output.get("preview")
        if isinstance(preview, str) and preview.strip():
            try:
                parsed_preview = json.loads(preview)
            except Exception:
                parsed_preview = None
            if isinstance(parsed_preview, Mapping):
                output_candidates.append(parsed_preview)
        for output_candidate in output_candidates:
            if (
                output_candidate.get("body_requires_tool") is True
                or str(output_candidate.get("next_tool_for_body") or "") == "email_read"
                or str(output_candidate.get("body_requires_tool") or "") == "email_read"
            ):
                search_output = output_candidate
                break
        if search_output is not None:
            break
    if search_output is None:
        return None
    items = [item for item in _structured_records(search_output) if isinstance(item, Mapping)]
    if not items:
        return "✉️ I found the email source, but I could not open the full message body."

    def _labels(item: Mapping[str, Any]) -> set[str]:
        raw = item.get("labelIds") or item.get("labels") or ()
        if isinstance(raw, str):
            return {raw.upper()}
        if isinstance(raw, Iterable):
            return {str(value or "").strip().upper() for value in raw if str(value or "").strip()}
        return set()

    def _compact_item(item: Mapping[str, Any], *, include_sender: bool = True) -> str:
        subject = _compact_account_text(item.get("subject") or "Email")
        sender = _compact_account_text(item.get("from"))
        date = _compact_account_text(item.get("date"))
        snippet = _compact_account_text(item.get("snippet"))
        parts: list[str] = []
        if subject:
            parts.append(subject)
        if date:
            parts.append(date)
        if include_sender and sender:
            parts.append(f"from {sender}")
        line = " - ".join(parts)
        if snippet:
            line = f"{line}: {snippet[:180].rstrip()}" if line else snippet[:180].rstrip()
        return line or "Email"

    latest = items[0]
    latest_sent_index_item = next(
        (
            (index, item)
            for index, item in enumerate(items)
            if "SENT" in _labels(item) and "DRAFT" not in _labels(item)
        ),
        None,
    )
    latest_sent = latest_sent_index_item[1] if latest_sent_index_item is not None else None
    latest_inbound = next(
        (
            item
            for item in items
            if "SENT" not in _labels(item)
            and "DRAFT" not in _labels(item)
            and not _labels(item).intersection({"TRASH", "SPAM"})
        ),
        None,
    )
    latest_inbound_index = next(
        (
            index
            for index, item in enumerate(items)
            if "SENT" not in _labels(item)
            and "DRAFT" not in _labels(item)
            and not _labels(item).intersection({"TRASH", "SPAM"})
        ),
        None,
    )
    lines = [
        "✉️ I found the matching email thread, but I could not open the full message body.",
        "",
        "What the structured search evidence shows:",
        f"- Latest matching item: {_compact_item(latest)}",
    ]
    if latest_sent is not None:
        lines.append(f"- Latest sent follow-up: {_compact_item(latest_sent, include_sender=False)}")
    if latest_inbound is not None:
        lines.append(f"- Latest inbound reply: {_compact_item(latest_inbound)}")
    if (
        latest_sent_index_item is not None
        and latest_inbound_index is not None
        and latest_sent_index_item[0] < latest_inbound_index
    ):
        recommendation = (
            "Recommended next step: send a concise follow-up that asks for confirmation and a specific ETA. "
            "If this is time-sensitive, use a second channel or escalate to the office/contact method you have on file."
        )
    else:
        recommendation = (
            "I could not verify the full message body in this run, so I will not treat the search rows as the final answer."
        )
    lines.extend(["", recommendation])
    return "\n".join(lines)


def _sanitize_browser_tool_user_phrasing(text: str, results: list[ToolResult]) -> str:
    if not any(str(getattr(result, "tool_name", "") or "").startswith("browser_") for result in results):
        return text
    cleaned = _BROWSER_TOOL_PHRASE_RE.sub("in the browser", text)
    cleaned = _BROWSER_TOOL_NOUN_RE.sub("browser", cleaned)
    return re.sub(r"\s+([,.;:!?])", r"\1", cleaned)


def _structured_internal_tool_state_reply(
    results: list[ToolResult],
    *,
    user_message: str | None = None,
) -> str | None:
    flagged = False
    for result in reversed(results):
        output = result.output if isinstance(result.output, dict) else {}
        metadata_candidates: list[Mapping[str, object]] = [output]
        for key in ("reply_metadata", "sanitizer", "response_contract"):
            value = output.get(key)
            if isinstance(value, Mapping):
                metadata_candidates.append(value)
        for metadata in metadata_candidates:
            for reply_key in ("sanitized_reply", "user_visible_summary", "visible_reply"):
                reply = metadata.get(reply_key)
                if isinstance(reply, str) and reply.strip():
                    return reply.strip()
            if any(
                bool(metadata.get(flag))
                for flag in (
                    "reply_contains_internal_tool_state",
                    "internal_tool_state",
                    "internal_tool_state_leak",
                    "required_tool_completed",
                )
            ):
                flagged = True
    if not flagged:
        return None
    return _account_tool_summary(results, user_message=user_message)


def _account_tool_summary(results: list[ToolResult], *, user_message: str | None = None) -> str | None:
    primary_family: str | None = None
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if result.tool_name in {"email_read", "email_search", "email_attachment_read"}:
            primary_family = "email"
            break
        if result.tool_name == "calendar_list":
            primary_family = "calendar"
            break
        if result.tool_name == "market_quote":
            primary_family = "market_quote"
            break
    if primary_family == "email":
        for result in reversed(results):
            if result.status == "completed" and isinstance(result.output, dict) and result.tool_name == "email_attachment_read":
                return _email_attachment_read_summary(result.output)
        for result in reversed(results):
            if result.status == "completed" and isinstance(result.output, dict) and result.tool_name == "email_read":
                return _email_read_summary(result.output)
        for result in reversed(results):
            if result.status == "completed" and isinstance(result.output, dict) and result.tool_name == "email_search":
                return _email_search_summary(result.output)
    if primary_family == "calendar":
        for result in reversed(results):
            if result.status == "completed" and isinstance(result.output, dict) and result.tool_name == "calendar_list":
                return _calendar_list_summary(result.output, user_message=user_message)
    if primary_family == "market_quote":
        for result in reversed(results):
            if result.status == "completed" and isinstance(result.output, dict) and result.tool_name == "market_quote":
                return _market_quote_summary(result.output)
    for result in reversed(results):
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if result.tool_name == "calendar_list":
            return _calendar_list_summary(result.output, user_message=user_message)
        if result.tool_name == "market_quote":
            return _market_quote_summary(result.output)
        if result.tool_name == "email_read":
            return _email_read_summary(result.output)
        if result.tool_name == "email_attachment_read":
            return _email_attachment_read_summary(result.output)
        if result.tool_name == "email_search":
            return _email_search_summary(result.output)
    return None


def _market_quote_price_label(price: object, currency: object) -> str | None:
    try:
        number = float(price)
    except (TypeError, ValueError):
        return None
    currency_text = str(currency or "").strip().upper()
    if currency_text == "USD" or not currency_text:
        return f"${number:,.2f}"
    return f"{number:,.2f} {currency_text}"


def _market_quote_summary(output: Mapping[str, Any]) -> str | None:
    quotes = output.get("quotes")
    rows = [item for item in quotes if isinstance(item, Mapping)] if isinstance(quotes, list) else []
    if not rows:
        return None
    lines = ["Market prices from live quote data:"]
    for row in rows[:8]:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        name = str(row.get("name") or "").strip()
        price = _market_quote_price_label(row.get("price"), row.get("currency"))
        if not price:
            continue
        exchange = str(row.get("exchange") or "").strip()
        label = symbol
        if name and name.upper() != symbol:
            label = f"{symbol} ({name})"
        suffix = f" - {exchange}" if exchange else ""
        lines.append(f"- {label}: {price}{suffix}")
    return "\n".join(lines) if len(lines) > 1 else None


def _calendar_list_summary(output: Mapping[str, Any], *, user_message: str | None = None) -> str:
    records = output.get("results")
    items = records if isinstance(records, list) else []
    if not items:
        return "I checked your calendar. No events found for that window."
    selected = _calendar_selected_items_from_output(output)
    if selected:
        noun = "event" if len(selected) == 1 else "events"
        lines = [f"📅 I found {len(selected)} matching calendar {noun}:"]
        for item in selected:
            lines.append(f"- {_calendar_event_display_line(item)}")
        return "\n".join(lines)
    noun = "event" if len(items) == 1 else "events"
    lines = [f"I checked your calendar. Found {len(items)} {noun}:"]
    for item in items[:6]:
        if not isinstance(item, Mapping):
            continue
        title = str(item.get("summary") or "Untitled event").strip()
        start = _calendar_time_label(item.get("start"))
        end = _calendar_time_label(item.get("end"))
        when = f"{start} - {end}" if start and end else start or end
        location = str(item.get("location") or "").strip()
        detail = title
        if when:
            detail = f"{when}: {detail}"
        if location:
            detail = f"{detail} ({location})"
        lines.append(f"- {detail}")
    return "\n".join(lines)


def _calendar_relevance_reply_over_raw_listing(
    user_message: str | None,
    text: str,
    results: list[ToolResult],
) -> str:
    if not _reply_mentions_calendar_result_listing(text, results):
        return text
    summary = _calendar_selected_record_summary(results, user_message=user_message)
    return summary or text


def _reply_mentions_calendar_result_listing(text: object, results: list[ToolResult]) -> bool:
    normalized = _normalized_reply_text(text)
    if not normalized:
        return False
    mentioned = 0
    comparable = 0
    for result in results:
        if result.status != "completed" or result.tool_name != "calendar_list" or not isinstance(result.output, dict):
            continue
        records = result.output.get("results")
        for item in records if isinstance(records, list) else []:
            if not isinstance(item, Mapping):
                continue
            evidence = [
                str(item.get(key) or "").strip()
                for key in ("title", "summary", "name", "id")
                if str(item.get(key) or "").strip()
            ]
            if not evidence:
                continue
            comparable += 1
            if any(_normalized_reply_text(value) in normalized for value in evidence):
                mentioned += 1
    if comparable <= 0:
        return False
    return mentioned >= min(comparable, 3)


def _calendar_selected_record_summary(results: list[ToolResult], *, user_message: str | None = None) -> str | None:
    matches: list[Mapping[str, Any]] = []
    for result in results:
        if result.status != "completed" or result.tool_name != "calendar_list" or not isinstance(result.output, dict):
            continue
        matches.extend(_calendar_selected_items_from_output(result.output))
    if not matches:
        return None
    noun = "event" if len(matches) == 1 else "events"
    lines = [f"📅 I found {len(matches)} matching calendar {noun}:"]
    for item in matches[:3]:
        lines.append(f"- {_calendar_event_display_line(item)}")
    return "\n".join(lines)


def _calendar_selected_items_from_output(output: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("matched_results", "selected_results", "relevant_results"):
        records = output.get(key)
        if not isinstance(records, list):
            continue
        items: list[Mapping[str, Any]] = []
        for item in records:
            if isinstance(item, Mapping):
                items.append(item)
        if items:
            return items[:3]
    selected_ids = output.get("selected_result_ids")
    if not isinstance(selected_ids, list):
        return []
    wanted = {str(value) for value in selected_ids if str(value or "").strip()}
    if not wanted:
        return []
    matches: list[Mapping[str, Any]] = []
    records = output.get("results")
    for item in records if isinstance(records, list) else []:
        if not isinstance(item, Mapping):
            continue
        candidate_ids = {
            str(item.get("id") or ""),
            str(item.get("event_id") or ""),
            str(item.get("uid") or ""),
        }
        if wanted.intersection(candidate_ids):
            matches.append(item)
    return matches[:3]


def _calendar_event_display_line(item: Mapping[str, Any]) -> str:
    title = str(item.get("summary") or "Untitled event").strip()
    start = _calendar_time_label(item.get("start"))
    end = _calendar_time_label(item.get("end"))
    when = f"{start} - {end}" if start and end else start or end
    location = str(item.get("location") or "").strip()
    detail = f"**{title}**"
    if when:
        detail = f"{detail} — {when}"
    if location:
        detail = f"{detail} ({location})"
    return detail


def _calendar_time_label(value: object) -> str:
    if not isinstance(value, Mapping):
        return ""
    raw = str(value.get("dateTime") or value.get("date") or "").strip()
    if not raw:
        return ""
    return raw.replace("T", " ")[:16]


def _email_search_summary(output: Mapping[str, Any]) -> str:
    explicit_records = output.get("results")
    explicit_count = output.get("count", output.get("result_count", output.get("resultSizeEstimate")))
    if explicit_records == [] or explicit_count == 0:
        return "I checked your email. No matching messages found."
    items = [item for item in _structured_records(output) if isinstance(item, Mapping)]
    if not items:
        return "I checked your email. No matching messages found."
    noun = "message" if len(items) == 1 else "messages"
    lines = [f"I checked your email. Found {len(items)} matching {noun}:"]
    for item in items[:5]:
        if not isinstance(item, Mapping):
            continue
        lines.append(f"- {_email_message_line(item)}")
    return "\n".join(lines)


def _email_read_summary(output: Mapping[str, Any]) -> str:
    message = output.get("message")
    if not isinstance(message, Mapping):
        return "I checked your email."
    subject = str(message.get("subject") or "Email").strip()
    sender = str(message.get("from") or "").strip()
    date = str(message.get("date") or "").strip()
    snippet = _strip_invisible_tracking_text(str(message.get("snippet") or "")).strip()
    if not snippet:
        snippet = _strip_invisible_tracking_text(str(message.get("body") or "")).strip()
    snippet = re.sub(r"\s+", " ", snippet).strip()
    lines = [f"I checked your email.\n\n**{subject}**"]
    if sender:
        lines.append(f"From: {sender}")
    if date:
        lines.append(f"Date: {date}")
    if snippet:
        lines.append("")
        lines.append(f"Preview: {snippet[:360].rstrip()}")
    return "\n".join(lines).strip()


def _email_attachment_read_summary(output: Mapping[str, Any]) -> str:
    filename = str(output.get("filename") or "attachment").strip() or "attachment"
    mime_type = str(output.get("mime_type") or "").strip()
    size = output.get("size_bytes")
    artifact_path = str(output.get("artifact_path") or "").strip()
    lines = [f"I found the email attachment: **{filename}**"]
    details: list[str] = []
    if mime_type:
        details.append(mime_type)
    if isinstance(size, int) and size >= 0:
        details.append(f"{size} bytes")
    if details:
        lines.append(", ".join(details))
    if artifact_path:
        lines.append(f"Saved file: {artifact_path}")
    preview = str(output.get("text_preview") or "").strip()
    if preview:
        lines.append("")
        lines.append(f"Preview: {preview[:360].rstrip()}")
    return "\n".join(lines).strip()


def _email_message_line(message: Mapping[str, Any]) -> str:
    subject = str(message.get("subject") or "Email").strip()
    sender = str(message.get("from") or "").strip()
    date = str(message.get("date") or "").strip()
    parts = [subject]
    if sender:
        parts.append(f"from {sender}")
    if date:
        parts.append(date)
    snippet = _strip_invisible_tracking_text(str(message.get("snippet") or "")).strip()
    snippet = re.sub(r"\s+", " ", snippet).strip()
    if snippet:
        parts.append(snippet[:180].rstrip())
    return " - ".join(parts)


def is_raw_tool_payload_reply(
    *,
    reply: str | None,
    tool_results: Iterable[ToolResult] | None = None,
) -> bool:
    """Return true when a draft reply is a bare structured tool payload."""

    if reply is None:
        return False
    if _looks_like_raw_function_markup(str(reply)):
        return True
    parsed = _parse_bare_structured_payload(str(reply))
    if parsed is None:
        return False
    return _looks_like_raw_tool_payload(parsed, list(tool_results or ()))


def is_safe_raw_tool_payload_replacement_reply(
    *,
    reply: str | None,
    tool_results: Iterable[ToolResult | Mapping[str, Any] | object] | None = None,
) -> bool:
    """Return true when a reply is the deterministic raw-payload safe fallback.

    This checks against fallback text generated from structured tool-result
    evidence. It is intentionally not a phrase detector over user prompts.
    """

    if reply is None:
        return False
    results = _coerce_tool_results(tool_results)
    if not results:
        return False
    actual = _normalized_reply_text(reply)
    if not actual:
        return False
    candidates: list[str] = []
    for source in ("agent", "tool", "deep-agent"):
        candidates.append(safe_raw_tool_payload_replacement(tool_results=results, source=source))
        for result in results:
            candidates.append(safe_raw_tool_payload_replacement(tool_results=[result], source=source))
    return any(actual == _normalized_reply_text(candidate) for candidate in candidates)


def is_structured_tool_evidence_replacement_reply(
    *,
    reply: str | None,
    tool_results: Iterable[ToolResult | Mapping[str, Any] | object] | None = None,
    user_message: str | None = None,
) -> bool:
    """Return true when a reply is the deterministic evidence-fallback text."""

    if reply is None:
        return False
    results = _coerce_tool_results(tool_results)
    if not results:
        return False
    actual = _normalized_reply_text(reply)
    if not actual:
        return False
    candidates = _structured_tool_evidence_replacement_candidates(
        results,
        user_message=user_message,
    )
    return any(actual == _normalized_reply_text(candidate) for candidate in candidates)


def _structured_tool_evidence_replacement_candidates(
    results: list[ToolResult],
    *,
    user_message: str | None,
) -> list[str]:
    candidates: list[str] = []
    for message in (user_message, None):
        fallback = _structured_tool_evidence_reply_over_ignored_results(
            "",
            results,
            user_message=message,
        )
        if fallback:
            candidates.append(fallback)
        web_fallback = _web_search_reply_over_ignored_results(
            "",
            results,
            user_message=message,
        )
        if web_fallback:
            candidates.append(web_fallback)
    return list(dict.fromkeys(candidates))


def is_safe_connector_payload_summary_reply(
    *,
    reply: str | None,
    tool_results: Iterable[ToolResult | Mapping[str, Any] | object] | None = None,
) -> bool:
    """Return true when a reply is a deterministic connector summary."""

    if reply is None:
        return False
    results = _coerce_tool_results(tool_results)
    if not results:
        return False
    actual = _normalized_reply_text(reply)
    if not actual:
        return False
    return any(
        actual == _normalized_reply_text(summary)
        for summary in (
            _connector_request_payload_summary(results, parsed_payload=None),
            *(
                _connector_request_payload_summary([result], parsed_payload=None)
                for result in results
            ),
        )
        if summary
    )


def _sanitize_local_paths(text: str) -> str:
    """Hide machine-local absolute paths in text that will be shown to users."""

    redacted_lines: list[str] = []
    changed = False
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith(_PRESERVE_DIRECTIVE_PREFIXES):
            redacted_lines.append(line)
            continue
        redacted = _LOCAL_POSIX_PATH_RE.sub(_local_posix_path_display, line)
        redacted = _LOCAL_WINDOWS_PATH_RE.sub(_local_windows_path_display, redacted)
        changed = changed or redacted != line
        redacted_lines.append(redacted)
    return "".join(redacted_lines) if changed else text


def _normalized_reply_text(value: object) -> str:
    return "\n".join(line.rstrip() for line in str(value or "").strip().splitlines()).strip()


def _coerce_tool_results(
    values: Iterable[ToolResult | Mapping[str, Any] | object] | None,
) -> list[ToolResult]:
    results: list[ToolResult] = []
    for value in values or ():
        if isinstance(value, ToolResult):
            results.append(value)
            continue
        if isinstance(value, Mapping):
            tool_name = str(value.get("tool_name") or value.get("name") or "").strip()
            if not tool_name:
                continue
            output = value.get("output")
            results.append(
                ToolResult(
                    invocation_id=str(value.get("invocation_id") or ""),
                    tool_name=tool_name,
                    status=str(value.get("status") or ""),
                    output=output if isinstance(output, dict) else {},
                    error=str(value.get("error")) if value.get("error") is not None else None,
                )
            )
            continue
        tool_name = str(getattr(value, "tool_name", "") or getattr(value, "name", "") or "").strip()
        if not tool_name:
            continue
        output = getattr(value, "output", {})
        results.append(
            ToolResult(
                invocation_id=str(getattr(value, "invocation_id", "") or ""),
                tool_name=tool_name,
                status=str(getattr(value, "status", "") or ""),
                output=output if isinstance(output, dict) else {},
                error=str(getattr(value, "error", "")) if getattr(value, "error", None) is not None else None,
            )
        )
    return results


def _local_posix_path_display(match: re.Match[str]) -> str:
    path = match.group("path").rstrip(".,;:")
    suffix = match.group("path")[len(path) :]
    workspace_match = re.search(
        r"/\.nullion/workspaces/[^/]+/(?P<kind>artifacts|files|media)/(?P<name>[^/\s`\"'<>)]*)$",
        path,
    )
    if workspace_match:
        return f"{workspace_match.group('kind')}/{workspace_match.group('name')}{suffix}"
    return f"{Path(path).name or '[local path]'}{suffix}"


def _local_windows_path_display(match: re.Match[str]) -> str:
    path = match.group("path").rstrip(".,;:")
    suffix = match.group("path")[len(path) :]
    name = path.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
    return f"{name or '[local path]'}{suffix}"


def safe_raw_tool_payload_replacement(
    *,
    tool_results: Iterable[ToolResult] | None = None,
    source: str = "agent",
    parsed_payload: Any | None = None,
) -> str:
    results = list(tool_results or ())
    if file_search_summary := _file_search_payload_summary(results):
        return file_search_summary
    if connector_summary := _connector_request_payload_summary(results, parsed_payload=parsed_payload):
        return connector_summary
    tool_names = _completed_tool_names(results)
    if tool_names:
        tool_text = ", ".join(f"`{name}`" for name in tool_names[:4])
        if len(tool_names) > 4:
            tool_text += f", and {len(tool_names) - 4} more"
        tool_sentence = f" The tool(s) involved were {tool_text}."
    else:
        tool_sentence = ""
    if any(result.tool_name == "workspace_summary" for result in results):
        detail = (
            " It looked like a workspace inventory, which can include local paths and file names."
        )
    else:
        detail = ""
    return (
        f"I blocked a raw structured payload from {source} output before sending it here."
        f"{tool_sentence}{detail} Please ask me to summarize the result or rerun a focused check, "
        "and I’ll return a human-readable answer instead of the raw data."
    )


def _structured_numbered_choice_reply(results: list[ToolResult]) -> str | None:
    for result in reversed(results):
        if result.status != "failed" or not isinstance(result.output, dict):
            continue
        matches = result.output.get("matches")
        if not isinstance(matches, list) or len(matches) < 2:
            continue
        rows: list[tuple[int, str]] = []
        for item in matches:
            if not isinstance(item, dict):
                continue
            try:
                index = int(item.get("selection_index"))
            except (TypeError, ValueError):
                continue
            label = str(item.get("name") or item.get("title") or item.get("label") or "").strip()
            if not label:
                continue
            rows.append((index, label))
        if len(rows) < 2:
            continue
        rows = sorted(rows, key=lambda row: row[0])
        item_label = "matching options"
        if result.tool_name == "run_cron":
            item_label = "matching cron jobs"
        choices = "\n".join(f"{index}. {label}" for index, label in rows)
        return (
            f"I found multiple {item_label}. Which one should I use?\n\n"
            f"{choices}\n\n"
            "Reply with the number."
        )
    return None


def _connector_request_payload_summary(
    results: list[ToolResult],
    *,
    parsed_payload: Any | None = None,
) -> str | None:
    connector_results = [
        result
        for result in results
        if result.tool_name == "connector_request" and result.status == "completed"
    ]
    if not connector_results:
        return None
    candidates: list[Any] = []
    if parsed_payload is not None:
        candidates.append(parsed_payload)
    for result in reversed(connector_results):
        output = result.output if isinstance(result.output, dict) else {}
        for key in ("json", "result", "data", "items", "messages", "text"):
            value = output.get(key)
            if value is not None:
                candidates.append(value)
        if output:
            candidates.append(output)
    for candidate in candidates:
        summary = _structured_payload_highlights(candidate)
        if summary:
            return (
                "Connector result summary:\n"
                f"{summary}\n\n"
                "These records are evidence; they do not by themselves prove the final answer."
            )
    tool_count = len(connector_results)
    noun = "call" if tool_count == 1 else "calls"
    return (
        f"Connector result summary:\nCompleted {tool_count} connector {noun}.\n\n"
        "The connector completed, but it did not return a direct user-visible answer."
    )


def _structured_payload_highlights(payload: Any) -> str | None:
    records = _structured_records(payload)
    if not records:
        if isinstance(payload, dict) and payload:
            keys = ", ".join(str(key) for key in list(payload.keys())[:5])
            return f"structured data with fields: {keys}"
        return None
    record_count = len(records)
    noun = "item" if record_count == 1 else "items"
    highlights: list[str] = []
    for record in records[:3]:
        summary = _structured_record_summary(record)
        if summary:
            highlights.append(summary)
    if not highlights:
        return f"{record_count} structured {noun}"
    if record_count == 1:
        return f"- {highlights[0]}"
    return "\n".join(f"- {highlight}" for highlight in highlights)


def _structured_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    preview = payload.get("preview")
    if isinstance(preview, str) and preview.strip():
        try:
            parsed_preview = json.loads(preview)
        except Exception:
            parsed_preview = None
        preview_records = _structured_records(parsed_preview)
        if preview_records:
            return preview_records
    for key in ("messages", "items", "value", "results", "data", "events"):
        value = payload.get(key)
        if isinstance(value, list):
            records = [item for item in value if isinstance(item, dict)]
            if records:
                return records
    for value in payload.values():
        if isinstance(value, list):
            records = [item for item in value if isinstance(item, dict)]
            if records:
                return records
    return [payload]


def _structured_record_summary(record: dict[str, Any]) -> str:
    parts: list[str] = []
    header_values = _mail_header_values(record)
    for label, value in header_values:
        if value:
            parts.append(f"{label}: {_safe_scalar_text(value)}")
    for key in (
        "subject",
        "title",
        "summary",
        "name",
        "sender",
        "from",
        "snippet",
        "date",
        "due",
        "due_date",
        "start",
        "end",
        "id",
        "threadId",
    ):
        if any(part.lower().startswith(f"{key.lower()}:") for part in parts):
            continue
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, dict):
            value = value.get("dateTime") or value.get("date") or value.get("value")
        text = _safe_scalar_text(value)
        if text:
            parts.append(f"{key}: {text}")
        if len(parts) >= 4:
            break
    if not parts:
        return ""
    return ", ".join(parts[:4])


def _mail_header_values(record: dict[str, Any]) -> list[tuple[str, str]]:
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return []
    headers = payload.get("headers")
    if not isinstance(headers, list):
        return []
    wanted = {"subject": "subject", "from": "from", "date": "date"}
    found: list[tuple[str, str]] = []
    for header in headers:
        if not isinstance(header, dict):
            continue
        name = str(header.get("name") or "").strip().lower()
        label = wanted.get(name)
        value = str(header.get("value") or "").strip()
        if label and value:
            found.append((label, value))
    return found


def _safe_scalar_text(value: Any) -> str:
    if isinstance(value, (dict, list, tuple, set)):
        return ""
    text = str(value or "").strip()
    if not text:
        return ""
    text = " ".join(text.split())
    return _sanitize_local_paths(text)[:220]


_FILE_SEARCH_PREVIEW_PATH_RE = re.compile(r'"path"\s*:\s*"((?:\\.|[^"\\])*)"')


def _file_search_match_paths_from_output(output: Mapping[str, Any]) -> list[str]:
    paths: list[str] = []

    def collect_from_payload(payload: Any) -> None:
        if not isinstance(payload, Mapping):
            return
        matches = payload.get("matches")
        if isinstance(matches, list):
            for match in matches:
                if isinstance(match, str) and match.strip():
                    paths.append(match)
                elif isinstance(match, Mapping):
                    candidate = match.get("path")
                    if isinstance(candidate, str) and candidate.strip():
                        paths.append(candidate)
        match_details = payload.get("match_details")
        if isinstance(match_details, list):
            for match in match_details:
                if not isinstance(match, Mapping):
                    continue
                candidate = match.get("path")
                if isinstance(candidate, str) and candidate.strip():
                    paths.append(candidate)

    collect_from_payload(output)
    preview = output.get("preview")
    if isinstance(preview, str) and preview.strip():
        try:
            parsed_preview = json.loads(preview)
        except Exception:
            parsed_preview = None
        collect_from_payload(parsed_preview)
        for match in _FILE_SEARCH_PREVIEW_PATH_RE.finditer(preview):
            encoded = match.group(1)
            try:
                decoded = json.loads(f'"{encoded}"')
            except Exception:
                decoded = encoded
            if isinstance(decoded, str) and decoded.strip():
                paths.append(decoded)
    return list(dict.fromkeys(paths))


def _file_search_payload_summary(results: list[ToolResult]) -> str | None:
    for result in reversed(results):
        if result.tool_name != "file_search" or result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        safe_names: list[str] = []
        for match in _file_search_match_paths_from_output(output):
            name = Path(match).name
            if name and name not in safe_names:
                safe_names.append(name)
        if not safe_names:
            return "I searched the available files but did not find a matching file, so I could not prove the answer from files."
        shown = ", ".join(f"`{name}`" for name in safe_names[:5])
        extra = len(safe_names) - 5
        if extra > 0:
            shown += f", and {extra} more"
        count = len(safe_names)
        noun = "file" if count == 1 else "files"
        return f"I found {count} matching {noun}: {shown}. I have not read the file contents yet, so this is evidence, not the final answer."
    return None


def _completed_tool_names(results: list[ToolResult]) -> list[str]:
    names: list[str] = []
    for result in results:
        if result.status != "completed":
            continue
        if result.tool_name not in names:
            names.append(result.tool_name)
    return names


def _parse_bare_structured_payload(text: str) -> Any | None:
    stripped = text.strip()
    if not stripped:
        return None
    fenced = re.fullmatch(r"```(?:json|python)?\s*([\s\S]*?)\s*```", stripped, flags=re.IGNORECASE)
    if fenced:
        stripped = fenced.group(1).strip()
    if not stripped.startswith(("{", "[")):
        return None
    try:
        return json.loads(stripped)
    except Exception:
        pass
    try:
        parsed = ast.literal_eval(stripped)
    except Exception:
        return None
    return parsed if isinstance(parsed, (dict, list)) else None


def _looks_like_raw_function_markup(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    fenced = re.fullmatch(r"```(?:xml|html|text)?\s*([\s\S]*?)\s*```", stripped, flags=re.IGNORECASE)
    if fenced:
        stripped = fenced.group(1).strip()
    return _RAW_FUNCTION_CALL_RE.search(stripped) is not None


def _looks_like_raw_tool_payload(parsed: Any, results: list[ToolResult]) -> bool:
    if results and _matches_tool_output(parsed, results):
        return True
    if isinstance(parsed, dict):
        keys = set(parsed)
        if keys & _SENSITIVE_STRUCTURED_KEYS:
            return True
        if {"status", "output"} <= keys:
            return True
        return len(keys) >= 4 and any(isinstance(value, (dict, list)) for value in parsed.values())
    if isinstance(parsed, list):
        if not parsed:
            return False
        if len(parsed) >= 3:
            return True
        return any(isinstance(item, (dict, list)) for item in parsed)
    return False


def _matches_tool_output(parsed: Any, results: list[ToolResult]) -> bool:
    parsed_canonical = _canonical(parsed)
    for result in results:
        output = result.output
        candidates = [
            output,
            {"status": result.status, "output": output},
        ]
        if result.error:
            candidates.append({"status": result.status, "output": output, "error": result.error})
        for candidate in candidates:
            if parsed_canonical == _canonical(candidate):
                return True
    return False


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return repr(value)
