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
    r"<\s*function\b[\s\S]*?</\s*function\s*>|#\{\s*<\s*function\b[\s\S]*?</\s*function\s*>\s*\}",
    flags=re.IGNORECASE,
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
    if _looks_like_raw_function_markup(raw):
        return safe_raw_tool_payload_replacement(tool_results=list(tool_results or ()), source=source)
    parsed = _parse_bare_structured_payload(raw)
    if parsed is None:
        return _sanitize_local_paths(raw)
    results = list(tool_results or ())
    if not _looks_like_raw_tool_payload(parsed, results):
        return _sanitize_local_paths(raw)
    return safe_raw_tool_payload_replacement(tool_results=results, source=source, parsed_payload=parsed)


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
    for source in ("agent", "tool"):
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
