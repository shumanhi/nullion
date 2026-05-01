"""Platform-agnostic run activity phases for live conversation execution."""

from __future__ import annotations

from enum import Enum
import os
from pathlib import PurePosixPath
from typing import Any, Iterable
from urllib.parse import urlparse

from nullion.chat_response_contract import text_mentions_approval_claim
from nullion.prompt_injection import is_untrusted_tool_name, safe_untrusted_tool_metadata

SKILL_USAGE_GLYPH = "⧁"
ACTIVITY_GROUP_ICON = "→"
ACTIVITY_SUBLIST_PREFIX = ""
DEFAULT_SKILL_USAGE_ACTIVITY_LIMIT = 3
VERBOSE_MODE_VALUES = {"off", "planner", "full"}


class RunActivityPhase(str, Enum):
    ACTIVE = "active"
    WAITING_APPROVAL = "waiting_approval"



def classify_run_activity_phase(*, reply: str | None) -> RunActivityPhase:
    if text_mentions_approval_claim(reply):
        return RunActivityPhase.WAITING_APPROVAL
    return RunActivityPhase.ACTIVE


def activity_trace_enabled(*, default: bool = True) -> bool:
    raw = os.environ.get("NULLION_ACTIVITY_TRACE_ENABLED")
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def set_activity_trace_enabled(enabled: bool) -> None:
    os.environ["NULLION_ACTIVITY_TRACE_ENABLED"] = "true" if enabled else "false"


def activity_trace_status_text() -> str:
    return "on" if activity_trace_enabled() else "off"


def task_planner_feed_enabled(*, default: bool = True) -> bool:
    mode = task_planner_feed_mode(default="task" if default else "off")
    return mode != "off"


def task_planner_feed_mode(*, default: str = "task") -> str:
    raw_mode = os.environ.get("NULLION_TASK_PLANNER_FEED_MODE")
    if raw_mode is not None and raw_mode.strip():
        normalized = raw_mode.strip().lower().replace("_", "-")
        if normalized in {"all", "task", "tasks", "off"}:
            return "task" if normalized == "tasks" else normalized
    raw = os.environ.get("NULLION_TASK_PLANNER_FEED_ENABLED")
    if raw is None or raw.strip() == "":
        return default if default in {"all", "task", "off"} else "task"
    return "off" if raw.strip().lower() in {"0", "false", "no", "off"} else "task"


def set_task_planner_feed_enabled(enabled: bool) -> None:
    set_task_planner_feed_mode("task" if enabled else "off")


def set_task_planner_feed_mode(mode: str) -> None:
    normalized = str(mode or "").strip().lower().replace("_", "-")
    if normalized == "tasks":
        normalized = "task"
    if normalized not in {"all", "task", "off"}:
        raise ValueError("task planner feed mode must be all, task, or off")
    os.environ["NULLION_TASK_PLANNER_FEED_MODE"] = normalized
    os.environ["NULLION_TASK_PLANNER_FEED_ENABLED"] = "false" if normalized == "off" else "true"


def task_planner_feed_status_text() -> str:
    mode = task_planner_feed_mode()
    if mode == "all":
        return "all"
    if mode == "task":
        return "task"
    return "off"


def normalize_verbose_mode(mode: str) -> str:
    normalized = str(mode or "").strip().lower().replace("_", "-")
    if normalized not in VERBOSE_MODE_VALUES:
        raise ValueError("verbose mode must be off, planner, or full")
    return normalized


def verbose_mode(*, default_activity_trace: bool = True, default_planner_feed: str = "task") -> str:
    activity_enabled = activity_trace_enabled(default=default_activity_trace)
    planner_enabled = task_planner_feed_mode(default=default_planner_feed) != "off"
    if activity_enabled:
        return "full"
    if planner_enabled:
        return "planner"
    return "off"


def set_verbose_mode(mode: str) -> None:
    normalized = normalize_verbose_mode(mode)
    set_activity_trace_enabled(normalized == "full")
    set_task_planner_feed_mode("task" if normalized in {"planner", "full"} else "off")


def verbose_mode_status_text() -> str:
    return verbose_mode()


def _tool_field(tool_result: Any, name: str, default: Any = None) -> Any:
    if isinstance(tool_result, dict):
        return tool_result.get(name, default)
    return getattr(tool_result, name, default)


def _tool_status_icon(status: str) -> str:
    normalized = status.lower()
    if normalized in {"completed", "approved", "ok", "success"}:
        return "✓"
    if normalized in {"denied", "approval_required", "blocked", "suspended"}:
        return "⊘"
    if normalized in {"failed", "failure", "error"}:
        return "⊗"
    if normalized in {"running", "pending"}:
        return "→"
    return "•"


def format_activity_sublist_line(text: str) -> str:
    stripped = str(text or "").strip()
    prefix = f"{ACTIVITY_SUBLIST_PREFIX} " if ACTIVITY_SUBLIST_PREFIX else ""
    return f"  {prefix}{stripped}" if stripped else f"  {ACTIVITY_SUBLIST_PREFIX}".rstrip()


def format_activity_detail_lines(lines: Iterable[str]) -> str:
    return "\n".join(format_activity_sublist_line(line) for line in lines if str(line or "").strip())


def _tool_definition_name(tool_definition: Any) -> str | None:
    if isinstance(tool_definition, str):
        name = tool_definition
    elif isinstance(tool_definition, dict):
        name = tool_definition.get("name")
        if not name and isinstance(tool_definition.get("function"), dict):
            name = tool_definition["function"].get("name")
    else:
        name = getattr(tool_definition, "name", None)
        if name is None:
            function = getattr(tool_definition, "function", None)
            if isinstance(function, dict):
                name = function.get("name")
            else:
                name = getattr(function, "name", None)
    if not isinstance(name, str):
        return None
    stripped = name.strip()
    return stripped or None


def _tool_definitions_from_registry(tool_registry: Any) -> list[Any]:
    if tool_registry is None:
        return []
    list_tool_definitions = getattr(tool_registry, "list_tool_definitions", None)
    if not callable(list_tool_definitions):
        return []
    try:
        return list(list_tool_definitions() or [])
    except Exception:
        return []


def format_tool_inventory_activity_detail(
    tool_registry: Any = None,
    *,
    tool_definitions: Iterable[Any] | None = None,
    max_tools: int = 12,
) -> str:
    definitions = list(tool_definitions or _tool_definitions_from_registry(tool_registry))
    names = sorted({name for definition in definitions if (name := _tool_definition_name(definition))})
    if not names:
        return format_activity_sublist_line("No registered tools")
    visible = names[:max(1, max_tools)]
    suffix = f", +{len(names) - len(visible)} more" if len(names) > len(visible) else ""
    return format_activity_sublist_line(f"Tools: {', '.join(visible)}{suffix}")


def format_tool_results_activity_detail(tool_results: Iterable[Any]) -> str:
    lines: list[str] = []
    for result in tool_results or ():
        status = str(_tool_field(result, "status", "unknown") or "unknown")
        tool_name = str(_tool_field(result, "tool_name", "tool") or "tool")
        detail = _short_tool_detail(result)
        suffix = f" — {detail}" if detail else ""
        normalized = status.lower()
        status_suffix = "" if normalized in {"completed", "approved", "ok", "success"} else f" — {status}"
        lines.append(f"{_tool_status_icon(status)} {tool_name}{suffix or status_suffix}")
    return format_activity_detail_lines(lines)


def _short_tool_detail(tool_result: Any) -> str:
    error = _tool_field(tool_result, "error")
    if error:
        return str(error).strip()[:140]
    output = _tool_field(tool_result, "output")
    tool_name = str(_tool_field(tool_result, "tool_name", "") or "")
    if is_untrusted_tool_name(tool_name):
        metadata = safe_untrusted_tool_metadata(tool_name, output)
        for key in ("url", "title", "path", "query", "status_code", "content_type"):
            value = metadata.get(key)
            if value:
                return _compact_tool_detail_value(key, value)
        return ""
    if isinstance(output, dict):
        for key in ("reason", "summary", "message", "path", "url"):
            value = output.get(key)
            if value:
                return _compact_tool_detail_value(key, value)
    if isinstance(output, str) and output.strip():
        return _compact_tool_detail_value("", output)
    return ""


def _compact_tool_detail_value(key: str, value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if key == "url" or text.startswith(("http://", "https://")):
        parsed = urlparse(text)
        if parsed.netloc:
            return parsed.netloc[:140]
    if key == "path" or text.startswith(("/", "~/")):
        name = PurePosixPath(text).name
        if name:
            return name[:140]
    return text[:140]


def _skill_usage_title(skill_use: Any) -> str | None:
    if isinstance(skill_use, str):
        title = skill_use
    elif isinstance(skill_use, dict):
        title = skill_use.get("title") or skill_use.get("skill_title") or skill_use.get("name")
    else:
        title = (
            getattr(skill_use, "title", None)
            or getattr(skill_use, "skill_title", None)
            or getattr(skill_use, "name", None)
        )
    if not isinstance(title, str):
        return None
    stripped = title.strip()
    return stripped or None


def format_skill_usage_activity_line(skill_use: Any) -> str | None:
    title = _skill_usage_title(skill_use)
    if title is None:
        return None
    return format_activity_sublist_line(f"{SKILL_USAGE_GLYPH} {title}")


def format_skill_usage_activity_detail(
    skill_uses: Iterable[Any],
    *,
    limit: int | None = DEFAULT_SKILL_USAGE_ACTIVITY_LIMIT,
) -> str:
    lines: list[str] = []
    seen_titles: set[str] = set()
    for skill_use in skill_uses or ():
        title = _skill_usage_title(skill_use)
        if title is None:
            continue
        normalized = title.casefold()
        if normalized in seen_titles:
            continue
        seen_titles.add(normalized)
        lines.append(format_activity_sublist_line(f"{SKILL_USAGE_GLYPH} {title}"))
    if limit is not None:
        if limit <= 0:
            return ""
        lines = lines[:limit]
    return "\n".join(lines)


def format_tool_activity_line(tool_result: Any) -> str:
    status = str(_tool_field(tool_result, "status", "unknown") or "unknown")
    tool_name = str(_tool_field(tool_result, "tool_name", "tool") or "tool")
    detail = _short_tool_detail(tool_result)
    suffix = f" — {detail}" if detail else ""
    normalized = status.lower()
    status_suffix = "" if normalized in {"completed", "approved", "ok", "success"} else f" — {status}"
    return format_activity_sublist_line(f"{_tool_status_icon(status)} {tool_name}{suffix or status_suffix}")


def format_tool_activity_detail(tool_results: Iterable[Any]) -> str:
    return "\n".join(format_tool_activity_line(result) for result in (tool_results or ()))


def format_tool_results_activity_detail(tool_results: Iterable[Any]) -> str:
    return format_tool_activity_detail(tool_results)


def _mini_agent_task_titles_from_result(tool_result: Any) -> list[str]:
    output = _tool_field(tool_result, "output")
    if not isinstance(output, dict):
        return []
    raw_tasks = output.get("tasks") or output.get("task_titles") or ()
    titles: list[str] = []
    for raw_task in raw_tasks if isinstance(raw_tasks, (list, tuple)) else ():
        title = None
        if isinstance(raw_task, str):
            title = raw_task
        elif isinstance(raw_task, dict):
            title = raw_task.get("title") or raw_task.get("name")
        else:
            title = getattr(raw_task, "title", None) or getattr(raw_task, "name", None)
        if isinstance(title, str) and _is_useful_mini_agent_task_title(title):
            titles.append(title.strip())
    return titles


def _is_useful_mini_agent_task_title(title: str) -> bool:
    stripped = title.strip()
    if not stripped:
        return False
    lowered = stripped.casefold()
    if lowered in {"and", "or", "then", "also"}:
        return False
    if lowered.startswith(("and ", "or ", "then ", "also ", "as ")):
        return False
    return True


def format_mini_agent_activity_detail(
    task_titles: Iterable[Any] = (),
    *,
    task_count: int | None = None,
    fallback: str | None = None,
) -> str:
    lines: list[str] = []
    for raw_title in task_titles or ():
        title = raw_title.strip() if isinstance(raw_title, str) else str(raw_title or "").strip()
        if _is_useful_mini_agent_task_title(title):
            lines.append(format_activity_sublist_line(f"☐ {title}"))
    if lines:
        return "\n".join(lines)
    if task_count:
        return format_activity_sublist_line(f"☐ {task_count} delegated task{'s' if task_count != 1 else ''}")
    if fallback:
        stripped = fallback.strip()
        if stripped:
            return format_activity_sublist_line(f"☐ {stripped}")
    return ""


def format_run_activity_trace(
    *,
    tool_results: Iterable[Any] = (),
    skill_uses: Iterable[Any] = (),
    suspended_for_approval: bool = False,
    label: str = "Activity",
) -> str:
    results = list(tool_results or ())
    skill_detail = format_skill_usage_activity_detail(skill_uses or ())
    mini_agent_results = [
        result
        for result in results
        if str(_tool_field(result, "tool_name", "") or "").strip().lower() in {"mini-agents", "mini agents"}
    ]
    regular_results = [result for result in results if result not in mini_agent_results]
    lines = [label, "✓ Preparing request"]
    if skill_detail:
        lines.append("✓ Using learned skill")
        lines.extend(skill_detail.splitlines())
    if regular_results:
        lines.append("✓ Running model and tools")
        lines.extend(format_tool_activity_detail(regular_results).splitlines())
    elif suspended_for_approval:
        lines.append("⊘ Running model and tools — approval required")
    else:
        lines.append("✓ Running model and tools")
    for result in mini_agent_results:
        lines.append("✓ Mini-Agents")
        detail = format_mini_agent_activity_detail(
            _mini_agent_task_titles_from_result(result),
            fallback=_short_tool_detail(result),
        )
        if detail:
            lines.extend(detail.splitlines())
    if suspended_for_approval:
        lines.append("→ Waiting for approval")
    else:
        lines.append("✓ Writing response")
    return "\n".join(lines)


def append_activity_trace_to_reply(
    reply: str,
    *,
    tool_results: Iterable[Any] = (),
    skill_uses: Iterable[Any] = (),
    suspended_for_approval: bool = False,
    enabled: bool | None = None,
) -> str:
    should_append = activity_trace_enabled() if enabled is None else enabled
    results = list(tool_results or ())
    skills = list(skill_uses or ())
    if not should_append or (not results and not skills and not suspended_for_approval):
        return reply
    trace = format_run_activity_trace(
        tool_results=results,
        skill_uses=skills,
        suspended_for_approval=suspended_for_approval,
    )
    return f"{reply.rstrip()}\n\n{trace}" if reply else trace


__all__ = [
    "RunActivityPhase",
    "ACTIVITY_SUBLIST_PREFIX",
    "DEFAULT_SKILL_USAGE_ACTIVITY_LIMIT",
    "activity_trace_enabled",
    "activity_trace_status_text",
    "append_activity_trace_to_reply",
    "classify_run_activity_phase",
    "format_activity_detail_lines",
    "format_activity_sublist_line",
    "format_skill_usage_activity_line",
    "format_skill_usage_activity_detail",
    "format_tool_activity_detail",
    "format_tool_activity_line",
    "format_mini_agent_activity_detail",
    "format_run_activity_trace",
    "ACTIVITY_GROUP_ICON",
    "format_tool_inventory_activity_detail",
    "format_tool_results_activity_detail",
    "normalize_verbose_mode",
    "SKILL_USAGE_GLYPH",
    "set_activity_trace_enabled",
    "set_task_planner_feed_enabled",
    "set_task_planner_feed_mode",
    "set_verbose_mode",
    "task_planner_feed_enabled",
    "task_planner_feed_mode",
    "task_planner_feed_status_text",
    "verbose_mode",
    "verbose_mode_status_text",
]
