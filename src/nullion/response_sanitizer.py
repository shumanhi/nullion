"""Guards for user-visible assistant replies.

The model sometimes mirrors the previous tool result instead of converting it
into a human answer. That is noisy at best and can leak local paths or other
diagnostic details at worst, so this module blocks bare structured payloads
unless the user explicitly asked for raw output.
"""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterable

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
_RAW_FUNCTION_CALL_RE = re.compile(
    r"<\s*function\b[\s\S]*?</\s*function\s*>"
    r"|#\{\s*<\s*function\b[\s\S]*?</\s*function\s*>\s*\}"
    r"|(?:^|\n)\s*/[A-Za-z0-9_./-]+(?:\?[^\n]*)?\s*\n\s*\[[a-z][a-z0-9_]{2,}\]\s*=\s*\\?\{[\s\S]*?/function\b",
    flags=re.IGNORECASE,
)
_INTERNAL_SLASH_COMMAND_RE = re.compile(r"^/([A-Za-z_][A-Za-z0-9_]*)\b[^\n]*$")
_INTERNAL_TOOL_CALL_TEXT_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(?:\(|\s+)[^\n]*$")
_INTERNAL_ARGUMENT_FRAGMENT_RE = re.compile(
    r"^(?:[A-Za-z0-9_.:@/-]{2,80}\s*;\s*)?"
    r"[A-Za-z_][A-Za-z0-9_.-]{0,40}\s*=\s*[^;\n]+"
    r"(?:\s*;\s*[A-Za-z_][A-Za-z0-9_.-]{0,40}\s*=\s*[^;\n]+)*\s*;?$"
)
_COMMON_INTERNAL_TOOL_COMMAND_HEADS = frozenset(
    {
        "list_crons",
        "run_cron",
        "create_cron",
        "update_cron",
        "delete_cron",
        "email_search",
        "email_read",
        "email_send",
        "calendar_list",
        "file_search",
        "file_read",
        "file_write",
        "terminal_exec",
        "connector_request",
        "skill_pack_read",
        "web_search",
        "web_fetch",
        "weather_forecast",
        "image_generate",
    }
)


def user_requested_raw_output(user_message: str | None) -> bool:
    """Free-form user text does not bypass raw payload protection."""

    return False


def sanitize_user_visible_reply(
    *,
    user_message: str | None,
    reply: str | None,
    tool_results: Iterable[ToolResult] | None = None,
    source: str = "agent",
) -> str | None:
    """Replace bare raw tool payloads with a safe user-visible explanation."""

    if reply is None or user_requested_raw_output(user_message):
        return reply
    raw = str(reply)
    results = list(tool_results or ())
    if _has_completed_tool_result(results, "email_send"):
        raw = _sanitize_email_send_transport_details(raw)
    raw = _sanitize_account_tool_transport_details(raw, results)
    raw = _sanitize_internal_tool_state_phrasing(raw, results)
    raw = _sanitize_account_tool_family_drift(raw, results)
    raw = _sanitize_read_only_email_result_drift(raw, results)
    raw = _prefix_account_tool_reply(raw, results)
    if numbered_reply := _structured_numbered_choice_reply(results):
        return numbered_reply
    if cron_list_reply := _cron_list_reply_over_empty_reminder_drift(raw, results):
        return cron_list_reply
    if _looks_like_raw_function_markup(raw):
        return safe_raw_tool_payload_replacement(tool_results=results, source=source)
    if is_internal_tool_command_fragment_reply(reply=raw):
        return safe_raw_tool_payload_replacement(tool_results=results, source=source)
    parsed = _parse_bare_structured_payload(raw)
    if parsed is None:
        return _sanitize_local_paths(raw)
    if not _looks_like_raw_tool_payload(parsed, results):
        return _sanitize_local_paths(raw)
    return safe_raw_tool_payload_replacement(tool_results=results, source=source, parsed_payload=parsed)


def _has_completed_tool_result(results: Iterable[ToolResult], tool_name: str) -> bool:
    for result in results:
        if getattr(result, "tool_name", None) == tool_name and str(getattr(result, "status", None)) == "completed":
            return True
    return False


def is_internal_tool_command_fragment_reply(
    *,
    reply: str | None,
    registered_tool_names: Iterable[object] | None = None,
) -> bool:
    """Detect internal tool-command fragments that are not user-visible answers.

    This looks only at the model's proposed final answer and optional registered
    tool names. It is not a user-intent router; it is a delivery boundary that
    prevents pseudo tool calls like `/list_crons name=...` or `query=...;
    count=10` from being sent to users instead of a real tool result.
    """

    if reply is None:
        return False
    text = str(reply or "").strip()
    if not text or len(text) > 300:
        return False
    if "\n" in text:
        return False
    registered = {
        str(name or "").strip()
        for name in (registered_tool_names or ())
        if str(name or "").strip()
    }
    if slash := _INTERNAL_SLASH_COMMAND_RE.fullmatch(text):
        head = slash.group(1)
        if _is_public_operator_command(text):
            return False
        return head in registered or head in _COMMON_INTERNAL_TOOL_COMMAND_HEADS or "_" in head
    if call := _INTERNAL_TOOL_CALL_TEXT_RE.fullmatch(text):
        head = call.group(1)
        if head in registered or head in _COMMON_INTERNAL_TOOL_COMMAND_HEADS:
            return True
    if _INTERNAL_ARGUMENT_FRAGMENT_RE.fullmatch(text):
        return True
    return False


def _is_public_operator_command(text: str) -> bool:
    try:
        from nullion.operator_commands import is_operator_command_text

        return is_operator_command_text(text)
    except Exception:
        return False


def _cron_list_reply_over_empty_reminder_drift(text: str, results: list[ToolResult]) -> str | None:
    cron_result: ToolResult | None = None
    reminder_result_seen_after_crons = False
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
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
    r"[^.!?]*(?:lower-level\s+connector\s+route|connector\s+URL\s+policy|connector\s+request|connector\s+path|"
    r"connector\s+endpoint|endpoint\s+I\s+tried|api\.maton\.ai|HTTP\s+Error\s+\d{3}|\b\d{3}\s+Not\s+Found\b)"
    r"[^.!?]*[.!?]?",
    flags=re.IGNORECASE,
)
_LEADING_CONTRAST_TRANSITION_RE = re.compile(r"^\s*(?:however|but),\s+", flags=re.IGNORECASE)
_INTERNAL_TOOL_STATE_START_RE = re.compile(
    r"^\s*(?:"
    r"Completed\s+the\s+required\s+tool\s+step\.\s*|"
    r"I\s+completed\s+the\s+required[^.\n]*tool[^.\n]*\.\s*|"
    r"I\s+(?:have\s+)?already\s+completed\s+the\s+required[^.\n]*tool[^.\n]*\.\s*|"
    r"I\s+(?:have\s+)?already\s+completed\s+the\s+[^.\n]*(?:check|lookup|request)[^.\n]*\.\s*|"
    r"I\s+(?:have\s+)?already\s+(?:run|ran)\s+the\s+required[^.\n]*tool[^.\n]*\.\s*|"
    r"I\s+already\s+completed\s+the\s+required[^.\n]*tool[^.\n]*\.\s*|"
    r"I\s+ran\s+the\s+required\s+follow-up\s+tool\.\s*|"
    r"Your\s+[^.\n]*(?:calendar|email)[^.\n]*has\s+been\s+checked[^.\n]*\.\s*|"
    r"The\s+[^.\n]*(?:check|lookup|request)\s+is\s+already\s+complete[^.\n]*tool[^.\n]*\.\s*"
    r"(?:No\s+additional\s+[^.\n]*tool[^.\n]*\.\s*)?|"
    r"The\s+[^.\n]*check\s+is\s+already\s+complete,\s+and\s+no\s+additional\s+registered\s+tool\s+is\s+required[^.\n]*\.\s*"
    r")",
    flags=re.IGNORECASE,
)
_INTERNAL_TOOL_NAME_RE = re.compile(
    r"\b(?:"
    r"calendar_list|connector_request|email_read|email_search|email_send|skill_pack_read|"
    r"browser_extract_text|browser_navigate|browser_screenshot|terminal_exec|file_read|file_write"
    r")\b"
)
_INTERNAL_TOOL_NAME_SENTENCE_RE = re.compile(
    r"(?:(?:^|(?<=[.!?])\s+))"
    r"[^.!?\n]*"
    r"(?:calendar_list|connector_request|email_read|email_search|email_send|skill_pack_read|"
    r"browser_extract_text|browser_navigate|browser_screenshot|terminal_exec|file_read|file_write)"
    r"[^.!?\n]*[.!?]?",
    flags=re.IGNORECASE,
)
_EMAIL_FOLLOWUP_PREFIX_RE = re.compile(
    r"^\s*I\s+checked\s+one\s+of\s+the\s+unread\s+emails\s+as\s+a\s+follow-up\.",
    flags=re.IGNORECASE,
)
_INTERNAL_TOOL_AND_PREFIX_RE = re.compile(
    r"^\s*I\s+ran\s+the\s+required\s+follow-up\s+tool\s+and\s+",
    flags=re.IGNORECASE,
)
_INTERNAL_TOOL_COMPLETION_PREFIX_RE = re.compile(
    r"^\s*I\s+ran\s+the\s+required\s+tool\s+completion\s+step\.\s*",
    flags=re.IGNORECASE,
)
_INTERNAL_REQUIRED_TOOL_SENTENCE_RE = re.compile(
    r"^\s*(?:Done\s+[—-]\s+)?I\s+ran\s+the\s+required[^.\n]*\.\s*",
    flags=re.IGNORECASE,
)
_INTERNAL_COMPLETION_CLAUSE_RE = re.compile(
    r"^\s*The\s+calendar\s+check\s+is\s+complete,\s+and\s+",
    flags=re.IGNORECASE,
)


def _sanitize_account_tool_transport_details(text: str, results: list[ToolResult]) -> str:
    if not any(result.tool_name in {"email_search", "email_read", "calendar_list", "connector_request"} for result in results):
        return text
    lines = [
        line
        for line in str(text).splitlines()
        if not _ACCOUNT_TRANSPORT_LINE_RE.match(line)
    ]
    cleaned = "\n".join(lines).strip()
    cleaned = _ACCOUNT_CONNECTOR_DIAGNOSTIC_SENTENCE_RE.sub("", cleaned).strip()
    cleaned = _LEADING_CONTRAST_TRANSITION_RE.sub("", cleaned).strip()
    if not cleaned or cleaned.lower().startswith("if you want"):
        return _account_tool_summary(results) or cleaned
    return cleaned


def _sanitize_account_tool_family_drift(text: str, results: list[ToolResult]) -> str:
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
            return _account_tool_summary(results) or text
    return text


def _sanitize_read_only_email_result_drift(text: str, results: list[ToolResult]) -> str:
    if _primary_account_tool_family(results) != "email":
        return text
    read_results = [
        result
        for result in results
        if result.tool_name in {"email_search", "email_read"}
        and result.status == "completed"
        and isinstance(result.output, dict)
    ]
    if not read_results or _has_completed_tool_result(results, "email_send"):
        return text
    # A successful read-only account tool is the contract for the final answer.
    # If the model ignores every returned sender/subject/snippet, use the
    # deterministic tool-result summary instead of letting unrelated email
    # write/send guidance leak into the visible reply.
    if _reply_mentions_account_family_evidence(text, read_results, "email"):
        return text
    summary = _account_tool_summary(read_results)
    return summary or text


def _primary_account_tool_family(results: list[ToolResult]) -> str | None:
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        family = _account_tool_family(result.tool_name)
        if family is not None:
            return family
    return None


def _account_tool_family(tool_name: str) -> str | None:
    if tool_name in {"email_search", "email_read", "email_send"}:
        return "email"
    if tool_name == "calendar_list":
        return "calendar"
    return None


def _prefix_account_tool_reply(text: str, results: list[ToolResult]) -> str:
    family = _primary_account_tool_family(results)
    if family is None:
        return text
    stripped = str(text or "").lstrip()
    if not stripped:
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
    return [
        value.strip()
        for value in values
        if isinstance(value, str) and len(value.strip()) >= 6
    ]


def _sanitize_internal_tool_state_phrasing(text: str, results: list[ToolResult]) -> str:
    raw = str(text or "")
    if not raw.strip():
        return raw
    raw = _EMAIL_FOLLOWUP_PREFIX_RE.sub("I checked one of the unread emails.", raw, count=1).strip()
    raw = _INTERNAL_TOOL_AND_PREFIX_RE.sub("I checked and ", raw, count=1).strip()
    raw = _INTERNAL_TOOL_COMPLETION_PREFIX_RE.sub("", raw, count=1).strip()
    raw = _INTERNAL_REQUIRED_TOOL_SENTENCE_RE.sub("", raw, count=1).strip()
    raw = _INTERNAL_COMPLETION_CLAUSE_RE.sub("", raw, count=1).strip()
    if _INTERNAL_TOOL_NAME_RE.search(raw):
        cleaned = _INTERNAL_TOOL_NAME_SENTENCE_RE.sub("", raw).strip()
        if cleaned:
            if cleaned.lower().startswith("if you want"):
                summary = _account_tool_summary(results)
                return summary or cleaned
            raw = cleaned
        else:
            summary = _account_tool_summary(results)
            return summary or raw
    if not raw:
        summary = _account_tool_summary(results)
        return summary or raw
    if not _INTERNAL_TOOL_STATE_START_RE.search(raw):
        return raw
    cleaned = _INTERNAL_TOOL_STATE_START_RE.sub("", raw, count=1).strip()
    summary = _account_tool_summary(results)
    if summary:
        return summary
    if cleaned and not cleaned.lower().startswith("if you want") and not _INTERNAL_TOOL_NAME_RE.search(cleaned):
        return cleaned
    return summary or cleaned or raw


def _account_tool_summary(results: list[ToolResult]) -> str | None:
    primary_family: str | None = None
    for result in results:
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if result.tool_name in {"email_read", "email_search"}:
            primary_family = "email"
            break
        if result.tool_name == "calendar_list":
            primary_family = "calendar"
            break
    if primary_family == "email":
        for result in reversed(results):
            if result.status == "completed" and isinstance(result.output, dict) and result.tool_name == "email_read":
                return _email_read_summary(result.output)
        for result in reversed(results):
            if result.status == "completed" and isinstance(result.output, dict) and result.tool_name == "email_search":
                return _email_search_summary(result.output)
    if primary_family == "calendar":
        for result in reversed(results):
            if result.status == "completed" and isinstance(result.output, dict) and result.tool_name == "calendar_list":
                return _calendar_list_summary(result.output)
    for result in reversed(results):
        if result.status != "completed" or not isinstance(result.output, dict):
            continue
        if result.tool_name == "calendar_list":
            return _calendar_list_summary(result.output)
        if result.tool_name == "email_read":
            return _email_read_summary(result.output)
        if result.tool_name == "email_search":
            return _email_search_summary(result.output)
    return None


def _calendar_list_summary(output: Mapping[str, Any]) -> str:
    records = output.get("results")
    items = records if isinstance(records, list) else []
    if not items:
        return "I checked your calendar. No events found for that window."
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


def _calendar_time_label(value: object) -> str:
    if not isinstance(value, Mapping):
        return ""
    raw = str(value.get("dateTime") or value.get("date") or "").strip()
    if not raw:
        return ""
    return raw.replace("T", " ")[:16]


def _email_search_summary(output: Mapping[str, Any]) -> str:
    records = output.get("results")
    items = records if isinstance(records, list) else []
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
    snippet = str(message.get("body") or message.get("snippet") or "").strip()
    lines = [f"I checked your email.\n\n**{subject}**"]
    if sender:
        lines.append(f"From: {sender}")
    if date:
        lines.append(f"Date: {date}")
    if snippet:
        lines.append("")
        lines.append(snippet[:1000])
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
    # If the model falls back to raw/internal payload text, prefer a stable
    # summary built from typed tool results. This keeps connector/file details
    # useful without leaking transport dumps or internal state files.
    if directory_summary := _file_read_directory_payload_summary(results):
        return directory_summary
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
        "I could not produce a verified user-facing result from the available tool evidence."
        f"{tool_sentence}{detail} No raw internal payload was sent."
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
            return f"Connector result summary:\n{summary}"
    tool_count = len(connector_results)
    noun = "call" if tool_count == 1 else "calls"
    return f"Connector result summary:\nCompleted {tool_count} connector {noun}."


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


def _file_search_payload_summary(results: list[ToolResult]) -> str | None:
    for result in reversed(results):
        if result.tool_name != "file_search" or result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        matches = output.get("matches")
        if not isinstance(matches, list):
            continue
        safe_names: list[str] = []
        for match in matches:
            if not isinstance(match, str) or not match.strip():
                continue
            name = Path(match).name
            if name and name not in safe_names:
                safe_names.append(name)
        if not safe_names:
            return "I searched the available files but did not find a matching file."
        shown = ", ".join(f"`{name}`" for name in safe_names[:5])
        extra = len(safe_names) - 5
        if extra > 0:
            shown += f", and {extra} more"
        count = len(safe_names)
        noun = "file" if count == 1 else "files"
        return f"I found {count} matching {noun}: {shown}."
    return None


def _file_read_directory_payload_summary(results: list[ToolResult]) -> str | None:
    for result in reversed(results):
        if result.tool_name != "file_read" or result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if output.get("is_directory") is not True:
            continue
        entries = output.get("entries")
        names = [str(item).strip() for item in entries if isinstance(item, str) and str(item).strip()] if isinstance(entries, list) else []
        if not names:
            return "That path is a folder, but I could not list any files inside it."
        shown = ", ".join(f"`{Path(name).name}`" for name in names[:10])
        entry_count = output.get("entry_count")
        try:
            total = int(entry_count)
        except (TypeError, ValueError):
            total = len(names)
        extra = max(0, total - min(total, 10))
        if extra:
            shown += f", and {extra} more"
        noun = "entry" if total == 1 else "entries"
        return f"That path is a folder. I found {total} {noun}: {shown}."
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
