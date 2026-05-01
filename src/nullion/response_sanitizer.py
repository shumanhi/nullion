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
    parsed = _parse_bare_structured_payload(raw)
    if parsed is None:
        return reply
    results = list(tool_results or ())
    if not _looks_like_raw_tool_payload(parsed, results):
        return reply
    return safe_raw_tool_payload_replacement(tool_results=results, source=source)


def safe_raw_tool_payload_replacement(
    *,
    tool_results: Iterable[ToolResult] | None = None,
    source: str = "agent",
) -> str:
    results = list(tool_results or ())
    if file_search_summary := _file_search_payload_summary(results):
        return file_search_summary
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
