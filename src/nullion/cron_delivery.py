"""Shared cron delivery routing helpers.

Cron jobs can be created by web chat, Telegram, Slack, Discord, or direct REST.
Keep routing decisions here so adapters do not each infer delivery semantics.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import mimetypes
import inspect
from pathlib import Path
import re
from typing import Any, Callable, TypedDict
from urllib.parse import unquote, urlsplit

from langgraph.graph import END, START, StateGraph

SUPPORTED_CRON_DELIVERY_CHANNELS = frozenset({"web", "telegram", "slack", "discord"})
MESSAGING_CRON_DELIVERY_CHANNELS = frozenset({"telegram", "slack", "discord"})
MAX_CRON_TEXT_ARTIFACT_CHARS = 12000
MAX_CRON_ATTACHMENT_FALLBACK_CHARS = 420
DEFAULT_CRON_NO_OUTPUT_MESSAGE = "Cron ran successfully; no output was produced."
CRON_DELIVERY_REPLY_PREFIX = "⏰ "
SCHEDULED_TASK_DELIVERY_PREFIX = "⏰ Scheduled task:"
CRON_INTERNAL_CAPABILITY_TAGS = frozenset({"scheduler"})
CRON_INTERNAL_REFERENCE_TOOLS = frozenset({"skill_pack_read"})
CRON_DELIVERABLE_ARTIFACT_TOOLS = frozenset(
    {
        "file_write",
        "pdf_create",
        "pdf_edit",
        "image_generate",
    }
)
_CRON_DISPATCH_TOOL_SHAPE_CACHE_SIZE = 64
_CRON_DISPATCH_PLAN_CACHE_NAMESPACE = "cron.dispatch_plan"
_CRON_DISPATCH_PLAN_CACHE_VERSION = "v1"
_CRON_DISPATCH_PLAN_CACHE_MAX_ENTRIES = 128
_HTML_LOCAL_IMAGE_SRC_RE = re.compile(
    r"(?P<prefix><img\b[^>]*?\bsrc\s*=\s*)(?P<quote>[\"'])(?P<src>[^\"']+)(?P=quote)",
    re.IGNORECASE,
)
_CRON_ARTIFACT_PATH_RE = re.compile(r"(?<![\w./-])(?:[~\w./:-]*/)?artifacts/[^\s`\"'<>]+")
_HTML_INLINE_IMAGE_EXTENSIONS = frozenset({".apng", ".avif", ".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"})

# Cron delivery contract for future agents:
# - Cron can deliver text, file attachments, both, or no message.
# - Explicit MEDIA lines are user-facing file delivery and must be preserved.
# - Completed tool results with structured artifact fields, or verified
#   workspace-artifact file reads/writes, are user-facing file delivery evidence
#   and should be converted to MEDIA lines after state-file filtering.
# - Raw artifact paths/objects are internal evidence unless the agent makes them
#   explicit with MEDIA or they came from completed structured tool evidence.
# - Activity/status summaries should show that tools ran, but tool outputs that
#   contain internal task text, paths, state files, artifacts, or connector
#   payloads are not deliverables.
# - Alert-only/no-change runs may be silent.
# - Unspecified no-output runs should use DEFAULT_CRON_NO_OUTPUT_MESSAGE.
# If you are asked to change this contract, confirm the intended behavior first
# and update the cron delivery E2E matrix in nullion-test with the change.


def normalize_cron_delivery_channel(channel: object) -> str:
    normalized = str(channel or "").strip().lower()
    return normalized if normalized in SUPPORTED_CRON_DELIVERY_CHANNELS else ""


def cron_agent_prompt(job: object, *, label: str) -> str:
    """Build the synthetic user message for a scheduled task turn."""
    name = str(getattr(job, "name", "") or "Scheduled task").strip()
    task = str(getattr(job, "task", "") or "").strip()
    return (
        f"[{label}: {name}] {task}\n\n"
        "Scheduled task execution context:\n"
        "- This is an existing scheduled task run. Schedule text is runtime metadata, not a request to create another schedule.\n"
        "- Do not create, update, delete, toggle, or run scheduled tasks from this execution context.\n\n"
        "Scheduled task delivery contract:\n"
        "- Cron may deliver text, file attachments, both, or no message, depending on the task.\n"
        "- If a file/report/export is expected, create it and attach it with a MEDIA line.\n"
        "- Generated artifacts must be internally consistent and valid for their file type before delivery: "
        "JSON must parse, text must be non-empty, HTML reports must not duplicate or contradict their own "
        "visible counts, and binary formats must be real files.\n"
        "- For HTML dashboards/reports, derive visible metrics, charts, and tables from the same source rows "
        "instead of manually repeating totals.\n"
        "- Keep scratch/checkpoint/state files in the workspace unless they are requested deliverables.\n"
        "- If the task says to alert only on new data or meaningful changes, return no output when nothing changed.\n"
        "- If a preferred connector, gateway, provider, or account API fails during this run, treat that as a "
        "runtime failure of the primary path, not as proof the whole scheduled task is impossible. Try other "
        "available typed tools for the same account family, then browser/web/manual setup fallbacks when safe. "
        "Do not keep retrying the same failing connector route or deliver a final report that only says the "
        "preferred connector failed.\n"
        f"- If no output behavior is specified and there is nothing specific to report, send: {DEFAULT_CRON_NO_OUTPUT_MESSAGE}"
    )


def _cron_tool_definition_shape(tool_definitions: object) -> tuple[tuple[str, tuple[str, ...]], ...]:
    shape: list[tuple[str, tuple[str, ...]]] = []
    for definition in tool_definitions or ():
        if not isinstance(definition, dict):
            continue
        name = str(definition.get("name") or "").strip()
        if not name:
            continue
        tags = tuple(
            sorted(
                {
                    str(tag).strip().lower()
                    for tag in (definition.get("capability_tags") or ())
                    if str(tag).strip()
                }
            )
        )
        shape.append((name, tags))
    return tuple(sorted(shape))


@lru_cache(maxsize=_CRON_DISPATCH_TOOL_SHAPE_CACHE_SIZE)
def _cached_cron_dispatch_tool_names(
    shape: tuple[tuple[str, tuple[str, ...]], ...],
) -> tuple[str, ...]:
    return tuple(name for name, _tags in shape)


def clear_cron_execution_metadata_caches() -> None:
    """Clear compact cron execution metadata caches after tool-registry changes."""

    _cached_cron_dispatch_tool_names.cache_clear()
    try:
        from nullion import runtime_cache

        runtime_cache.invalidate_namespace(_CRON_DISPATCH_PLAN_CACHE_NAMESPACE)
    except Exception:
        pass


def cron_deep_agent_dispatch_plan(
    job: object,
    *,
    label: str,
    tool_registry: object,
    planner_preview: object | None = None,
):
    """Build a scheduled-job DAG without an execution-time planner/model call."""

    from nullion.task_decomposer import DagPlan, DecomposedTask
    from nullion.task_queue import TaskPriority

    try:
        tool_definitions = tool_registry.list_tool_definitions()
    except Exception:
        tool_definitions = []
    allowed_tools = list(_cached_cron_dispatch_tool_names(_cron_tool_definition_shape(tool_definitions)))
    title = str(getattr(job, "name", "") or "Scheduled task").strip() or "Scheduled task"
    description = cron_agent_prompt(job, label=label)
    cache_key = _cron_dispatch_plan_cache_key(job, tool_definitions)
    preview_plan = _cron_dispatch_plan_from_preview(
        planner_preview,
        description=description,
        fallback_tools=allowed_tools,
    )
    if preview_plan is not None:
        _cron_dispatch_plan_cache_set(cache_key, preview_plan)
        return preview_plan
    cached_plan = _cron_dispatch_plan_cache_get(
        cache_key,
        description=description,
        fallback_tools=allowed_tools,
    )
    if cached_plan is not None:
        return cached_plan
    return DagPlan(
        disposition="single_turn",
        tasks=[
            DecomposedTask(
                title=f"Run {title}"[:50],
                description=description,
                tool_scope=allowed_tools,
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out=None,
                required_inputs=[],
                can_start=True,
                metadata={
                    "scheduled_task_run": True,
                    "no_user_input_requests": True,
                    "authoritative_scheduled_task_context": True,
                    "deep_agent_profiles": ["scheduled_job"],
                    "skip_tool_profile_inference": True,
                    "tool_capability_tags": ("scheduler", "cron"),
                },
            )
        ],
        routing_evidence=["scheduled_task_runtime"],
    )


def _cron_dispatch_plan_cache_key(job: object, tool_definitions: object) -> list[object]:
    return [
        str(getattr(job, "id", "") or "").strip(),
        _short_hash(str(getattr(job, "name", "") or "")),
        _short_hash(str(getattr(job, "task", "") or "")),
        [
            [name, list(tags)]
            for name, tags in _cron_tool_definition_shape(tool_definitions)
        ],
    ]


def _short_hash(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()[:16]


def _cron_dispatch_plan_cache_set(cache_key: list[object], plan: object) -> None:
    payload = _cron_dispatch_plan_to_cache_payload(plan)
    if payload is None:
        return
    try:
        from nullion import runtime_cache

        runtime_cache.set_json(
            _CRON_DISPATCH_PLAN_CACHE_NAMESPACE,
            cache_key,
            payload,
            version=_CRON_DISPATCH_PLAN_CACHE_VERSION,
            persistent=True,
            max_entries=_CRON_DISPATCH_PLAN_CACHE_MAX_ENTRIES,
        )
    except Exception:
        pass


def _cron_dispatch_plan_cache_get(
    cache_key: list[object],
    *,
    description: str,
    fallback_tools: list[str],
):
    try:
        from nullion import runtime_cache

        cached = runtime_cache.get_json(
            _CRON_DISPATCH_PLAN_CACHE_NAMESPACE,
            cache_key,
            version=_CRON_DISPATCH_PLAN_CACHE_VERSION,
            persistent=True,
        )
    except Exception:
        return None
    if not cached.hit:
        return None
    return _cron_dispatch_plan_from_cache_payload(
        cached.value,
        description=description,
        fallback_tools=fallback_tools,
    )


def _cron_dispatch_plan_to_cache_payload(plan: object) -> dict[str, object] | None:
    tasks = []
    for task in getattr(plan, "tasks", []) or []:
        title = str(getattr(task, "title", "") or "").strip()
        if not title:
            return None
        tasks.append(
            {
                "title": title,
                "tool_scope": [
                    str(tool).strip()
                    for tool in (getattr(task, "tool_scope", None) or [])
                    if str(tool).strip()
                ],
                "dep_indices": [
                    int(index)
                    for index in (getattr(task, "dep_indices", None) or [])
                    if isinstance(index, int)
                ],
                "context_key_in": getattr(task, "context_key_in", None),
                "context_key_out": getattr(task, "context_key_out", None),
            }
        )
    if not tasks:
        return None
    return {
        "disposition": str(getattr(plan, "disposition", "") or "sequential_mission"),
        "tasks": tasks,
    }


def _cron_dispatch_plan_from_cache_payload(
    payload: object,
    *,
    description: str,
    fallback_tools: list[str],
):
    if not isinstance(payload, dict):
        return None
    raw_tasks = payload.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        return None
    from nullion.task_decomposer import DagPlan, DecomposedTask
    from nullion.task_queue import TaskPriority

    planned_tasks: list[DecomposedTask] = []
    for item in raw_tasks:
        if not isinstance(item, dict):
            return None
        title = str(item.get("title") or "").strip()
        if not title:
            return None
        cached_tools = [
            str(tool).strip()
            for tool in (item.get("tool_scope") or [])
            if str(tool).strip()
        ]
        scoped_tools = list(dict.fromkeys([*cached_tools, *fallback_tools]))
        dep_indices = [
            int(index)
            for index in (item.get("dep_indices") or [])
            if isinstance(index, int)
        ]
        planned_tasks.append(
            DecomposedTask(
                title=title[:50],
                description=_scheduled_task_subtask_description(
                    description,
                    title=title,
                    details=title,
                ),
                tool_scope=scoped_tools,
                priority=TaskPriority.NORMAL,
                dep_indices=dep_indices,
                context_key_in=item.get("context_key_in") if isinstance(item.get("context_key_in"), str) else None,
                context_key_out=item.get("context_key_out") if isinstance(item.get("context_key_out"), str) else None,
                required_inputs=[],
                can_start=True,
                metadata={
                    "scheduled_task_run": True,
                    "no_user_input_requests": True,
                    "authoritative_scheduled_task_context": True,
                    "deep_agent_profiles": ["scheduled_job"],
                    "skip_tool_profile_inference": True,
                    "tool_capability_tags": ("scheduler", "cron"),
                    "cached_cron_dispatch_plan": True,
                },
            )
        )
    disposition = str(payload.get("disposition") or "").strip()
    if disposition not in {"single_turn", "sequential_mission", "parallel_mission"}:
        disposition = "single_turn" if len(planned_tasks) == 1 else "sequential_mission"
    return DagPlan(
        disposition=disposition,
        tasks=planned_tasks,
        routing_evidence=["cached_cron_dispatch_plan", "scheduled_task_runtime"],
    )


def _cron_dispatch_plan_from_preview(
    planner_preview: object | None,
    *,
    description: str,
    fallback_tools: list[str],
):
    from nullion.task_decomposer import DagPlan, DecomposedTask
    from nullion.task_queue import TaskPriority

    group = getattr(planner_preview, "group", None)
    tasks = list(getattr(group, "tasks", None) or [])
    metadata = getattr(group, "planner_metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    planner_kind = str(metadata.get("planner") or "").strip()
    if not tasks:
        return None
    if planner_kind == "cron_metadata_fallback":
        return None
    if planner_kind and planner_kind != "model_dag":
        return None
    if not planner_kind and len(tasks) <= 1:
        return None
    task_index_by_id = {str(getattr(task, "task_id", "")): index for index, task in enumerate(tasks)}
    planned_tasks: list[DecomposedTask] = []
    for task in tasks:
        task_title = str(getattr(task, "title", "") or "Run scheduled task").strip() or "Run scheduled task"
        task_description = str(getattr(task, "description", "") or "").strip()
        task_tools = [str(tool).strip() for tool in (getattr(task, "allowed_tools", None) or ()) if str(tool).strip()]
        scoped_tools = list(dict.fromkeys([*task_tools, *fallback_tools]))
        dep_indices = [
            task_index_by_id[dep_id]
            for dep_id in (str(dep).strip() for dep in (getattr(task, "dependencies", None) or ()))
            if dep_id in task_index_by_id
        ]
        planned_tasks.append(
            DecomposedTask(
                title=task_title[:50],
                description=_scheduled_task_subtask_description(
                    description,
                    title=task_title,
                    details=task_description,
                ),
                tool_scope=scoped_tools,
                priority=getattr(task, "priority", TaskPriority.NORMAL) or TaskPriority.NORMAL,
                dep_indices=dep_indices,
                context_key_in=getattr(task, "context_key_in", None),
                context_key_out=getattr(task, "context_key_out", None),
                required_inputs=[],
                can_start=True,
                metadata={
                    **dict(getattr(task, "metadata", None) or {}),
                    "scheduled_task_run": True,
                    "no_user_input_requests": True,
                    "authoritative_scheduled_task_context": True,
                    "deep_agent_profiles": ["scheduled_job"],
                    "skip_tool_profile_inference": True,
                    "tool_capability_tags": ("scheduler", "cron"),
                },
            )
        )
    disposition = str(metadata.get("disposition") or "").strip()
    if not disposition:
        disposition = _cron_preview_disposition(planner_preview)
    if disposition not in {"single_turn", "sequential_mission", "parallel_mission"}:
        disposition = "single_turn" if len(planned_tasks) == 1 else "sequential_mission"
    return DagPlan(
        disposition=disposition,
        tasks=planned_tasks,
        routing_evidence=["model_structured_plan", "scheduled_task_runtime", "cron_planner_preview"],
    )


def _scheduled_task_subtask_description(authoritative_context: str, *, title: str, details: str) -> str:
    """Attach the stored cron context to every planner subtask before execution."""

    lines = [
        "Scheduled job authoritative context:",
        str(authoritative_context or "").strip(),
        "",
        "Assigned planner subtask:",
        f"Title: {str(title or 'Run scheduled task').strip()}",
    ]
    if str(details or "").strip():
        lines.append(f"Details: {str(details).strip()}")
    lines.extend(
        [
            "",
            "Complete this subtask using the scheduled job context above. Do not ask the user to clarify "
            "fields already contained in the scheduled job context. If the subtask is blocked, return a "
            "concise failure or evidence summary instead of requesting user input.",
            "Provider and connector preferences in the stored job identify the primary path. If runtime tool "
            "results show that primary path is failing, recover through other available typed account tools "
            "for the same account family, then safe browser/web/manual setup fallbacks. Do not keep retrying "
            "the same failed connector route or treat a stale provider preference as a prohibition on recovery.",
        ]
    )
    return "\n".join(lines)


def _cron_preview_disposition(planner_preview: object | None) -> str:
    summary = str(getattr(planner_preview, "planner_summary", "") or "").strip().lower()
    if "parallel mission" in summary:
        return "parallel_mission"
    if "sequential mission" in summary:
        return "sequential_mission"
    if "single turn" in summary:
        return "single_turn"
    return ""


def _cron_title_timestamp_suffix(title: str) -> str:
    timestamp_match = re.search(
        r"(?:\s+[—-]\s+)?((?:\d{4}-\d{2}-\d{2}|[A-Z][a-z]{2,8}\s+\d{1,2},\s+\d{4}).*?\b\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?)\s*$",
        str(title or ""),
    )
    return timestamp_match.group(1).strip() if timestamp_match else ""


def _cron_title_is_redundant_with_job(title: str, job_name: str) -> bool:
    title_tokens = {token.casefold() for token in re.findall(r"[A-Za-z0-9]+", str(title or ""))}
    if not title_tokens:
        return False
    job_tokens = {token.casefold() for token in re.findall(r"[A-Za-z0-9]+", str(job_name or ""))}
    return title_tokens.issubset(job_tokens)


def _strip_leading_cron_report_heading(body: str, job_name: str) -> tuple[str, str]:
    body = str(body or "").strip()
    if not body.startswith(CRON_DELIVERY_REPLY_PREFIX):
        return "", body
    first_line, _, rest = body.partition("\n")
    title = first_line[len(CRON_DELIVERY_REPLY_PREFIX):].strip()
    timestamp_suffix = _cron_title_timestamp_suffix(title)
    if timestamp_suffix:
        return timestamp_suffix, rest.strip()
    rest_body = rest.strip()
    if not rest_body.startswith(CRON_DELIVERY_REPLY_PREFIX):
        return "", rest_body if _cron_title_is_redundant_with_job(title, job_name) else body
    second_line, _, second_rest = rest_body.partition("\n")
    second_title = second_line[len(CRON_DELIVERY_REPLY_PREFIX):].strip()
    timestamp_suffix = _cron_title_timestamp_suffix(second_title)
    if not timestamp_suffix:
        return "", body
    # Some cron report bodies start with a short account/source line before the
    # actual dated report heading. When that line is already in the cron name,
    # collapse both generated headings into the scheduled-task header.
    if _cron_title_is_redundant_with_job(title, job_name):
        return timestamp_suffix, second_rest.strip()
    prefix_body = "\n\n".join(part for part in (title, second_rest.strip()) if part)
    return timestamp_suffix, prefix_body


def scheduled_task_delivery_text(job: object, text: str, *, run_label: str | None = None) -> str:
    """Format a user-visible scheduled task delivery header."""
    name = str(getattr(job, "name", "") or "Scheduled task").strip() or "Scheduled task"
    label = str(run_label or "Scheduled task").strip() or "Scheduled task"
    body = str(text or "").strip()
    timestamp_suffix, normalized_body = _strip_leading_cron_report_heading(body, name)
    if timestamp_suffix or normalized_body != body:
        body = normalized_body
    header = f"⏰ {label}: {name}"
    if timestamp_suffix:
        header = f"{header} — {timestamp_suffix}"
    return f"{header}\n\n{body}" if body else header


def configured_delivery_target(channel: str, settings: object | None = None, env: dict[str, str] | None = None) -> str:
    """Return the configured operator target for a supported delivery channel."""
    import os

    env_map = env if env is not None else os.environ
    channel = normalize_cron_delivery_channel(channel)
    if channel == "web":
        return "web:operator"
    if channel == "telegram":
        configured = getattr(getattr(settings, "telegram", None), "operator_chat_id", None)
        if isinstance(configured, str) and configured.strip():
            return configured.strip()
        return str(env_map.get("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "") or "").strip()
    if channel == "slack":
        configured = getattr(getattr(settings, "slack", None), "operator_user_id", None)
        if isinstance(configured, str) and configured.strip():
            return configured.strip()
        return str(env_map.get("NULLION_SLACK_OPERATOR_USER_ID", "") or "").strip()
    if channel == "discord":
        return str(env_map.get("NULLION_DISCORD_OPERATOR_CHANNEL_ID", "") or "").strip()
    return ""


def effective_cron_delivery_channel(
    job: object,
    *,
    settings: object | None = None,
    env: dict[str, str] | None = None,
    fallback_channel: str = "web",
) -> str:
    """Resolve a cron delivery channel from structured metadata.

    Blank legacy jobs prefer Telegram when an operator target is configured,
    otherwise they fall back to web. Explicit supported channels are preserved.
    """
    explicit = normalize_cron_delivery_channel(getattr(job, "delivery_channel", ""))
    if explicit:
        return explicit
    if configured_delivery_target("telegram", settings=settings, env=env):
        return "telegram"
    fallback = normalize_cron_delivery_channel(fallback_channel)
    return fallback or "web"


def cron_delivery_target(
    job: object,
    channel: str,
    *,
    settings: object | None = None,
    env: dict[str, str] | None = None,
) -> str:
    channel = normalize_cron_delivery_channel(channel)
    explicit_target = str(getattr(job, "delivery_target", "") or "").strip()
    if explicit_target and not (channel in MESSAGING_CRON_DELIVERY_CHANNELS and explicit_target.startswith("web:")):
        return explicit_target
    workspace_id = str(getattr(job, "workspace_id", "") or "").strip()
    if channel in MESSAGING_CRON_DELIVERY_CHANNELS and workspace_id and workspace_id != "workspace_admin":
        try:
            from nullion.users import messaging_delivery_targets_for_workspace

            for candidate in messaging_delivery_targets_for_workspace(workspace_id, settings=settings):
                candidate_channel = str(getattr(candidate, "channel", "") or "").strip().lower()
                candidate_target = str(getattr(candidate, "target_id", "") or "").strip()
                if candidate_channel == channel and candidate_target:
                    return candidate_target
        except Exception:
            pass
    return configured_delivery_target(channel, settings=settings, env=env)


def cron_conversation_id(job: object, channel: str, target: str) -> str:
    if channel in MESSAGING_CRON_DELIVERY_CHANNELS and target:
        return f"{channel}:{target}"
    if channel == "web":
        return target or "web:operator"
    job_id = str(getattr(job, "id", "") or "").strip() or "unknown"
    return f"cron:{job_id}"


def _artifact_path_from_value(value: Any) -> str:
    if isinstance(value, dict):
        candidate = value.get("path")
    else:
        candidate = getattr(value, "path", value)
    if isinstance(candidate, Path):
        return str(candidate)
    if isinstance(candidate, str):
        return candidate.strip()
    return ""


def _artifact_values(artifacts: object) -> tuple[object, ...]:
    if isinstance(artifacts, dict) and "path" in artifacts:
        return (artifacts,)
    if isinstance(artifacts, dict):
        return tuple(artifacts.values())
    if isinstance(artifacts, (list, tuple, set, frozenset)):
        return tuple(artifacts)
    return (artifacts,)


def _cron_text_artifact_content(artifacts: object) -> str:
    for artifact in _artifact_values(artifacts):
        path_text = _artifact_path_from_value(artifact)
        if not path_text:
            continue
        path = Path(path_text).expanduser()
        if path.suffix.lower() != ".txt" or not path.is_file():
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            continue
        if content:
            return content[:MAX_CRON_TEXT_ARTIFACT_CHARS].rstrip()
    return ""


def cron_delivery_text(text: str, artifacts: object = None) -> str:
    """Return the cron's user-visible text without inventing attachments.

    Cron can deliver text, explicit MEDIA attachments, both, or nothing. Artifact
    paths are not automatically appended because scheduled tasks often write
    internal state/checkpoints; agents must make requested deliverables explicit.
    """
    return _cron_text_artifact_content(artifacts) or str(text or "")


def cron_delivery_reply_text(text: str) -> str:
    """Mark a final delivered cron reply without changing normal chat output."""

    raw = str(text or "")
    stripped = raw.lstrip()
    if not stripped:
        return ""
    if stripped.startswith(CRON_DELIVERY_REPLY_PREFIX.strip()):
        return raw
    leading = raw[: len(raw) - len(stripped)]
    return f"{leading}{CRON_DELIVERY_REPLY_PREFIX}{stripped}"


def _path_parts(path_text: str) -> tuple[str, ...]:
    try:
        return Path(path_text).expanduser().parts
    except (OSError, RuntimeError, ValueError):
        return ()


def _tool_result_output(result: object) -> dict[str, object]:
    if isinstance(result, dict):
        output = result.get("output")
        return output if isinstance(output, dict) else {}
    output = getattr(result, "output", None)
    return output if isinstance(output, dict) else {}


def _tool_result_name(result: object) -> str:
    if isinstance(result, dict):
        return str(result.get("tool_name") or "")
    return str(getattr(result, "tool_name", "") or "")


def _tool_result_status(result: object) -> str:
    if isinstance(result, dict):
        return str(result.get("status") or "")
    return str(getattr(result, "status", "") or "")


def _normalized_tool_result_status(result: object) -> str:
    try:
        from nullion.tools import normalize_tool_status

        return normalize_tool_status(_tool_result_status(result))
    except Exception:
        return _tool_result_status(result).strip().lower()


def _tool_result_capability_tags(result: object) -> frozenset[str]:
    output = _tool_result_output(result)
    raw_tags = output.get("tool_capability_tags")
    if raw_tags is None:
        raw_tags = output.get("denied_capability_tags")
    if not isinstance(raw_tags, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(
        str(tag).strip().lower() for tag in raw_tags if str(tag).strip()
    )


def _cron_result_has_internal_capability_denial(result: dict[str, object]) -> bool:
    for tool_result in result.get("tool_results") or ():
        if _normalized_tool_result_status(tool_result) != "denied":
            continue
        if (
            _tool_result_output(tool_result).get("reason")
            != "cron_execution_capability_denied"
        ):
            continue
        if _tool_result_capability_tags(tool_result).intersection(CRON_INTERNAL_CAPABILITY_TAGS):
            return True
    return False


def _cron_result_has_completed_tool_evidence(result: dict[str, object]) -> bool:
    for tool_result in result.get("tool_results") or ():
        if _normalized_tool_result_status(tool_result) != "completed":
            continue
        if _tool_result_capability_tags(tool_result).intersection(CRON_INTERNAL_CAPABILITY_TAGS):
            continue
        if _tool_result_name(tool_result) in CRON_INTERNAL_REFERENCE_TOOLS:
            continue
        return True
    return False


def _cron_result_leaked_internal_tool_output(result: dict[str, object], text: str | None) -> bool:
    visible_text = str(text or "").strip()
    if not visible_text:
        return False
    for tool_result in result.get("tool_results") or ():
        if _normalized_tool_result_status(tool_result) != "completed":
            continue
        if _tool_result_name(tool_result) not in CRON_INTERNAL_REFERENCE_TOOLS:
            continue
        output_text = str(_tool_result_output(tool_result).get("text") or "").strip()
        if output_text and (visible_text == output_text or output_text in visible_text):
            return True
    return False


def cron_structured_result_block_reason(
    result: dict[str, object],
    artifacts: object,
    *,
    text: str | None = None,
) -> str | None:
    """Return a delivery block reason from typed cron execution facts."""
    from nullion.response_sanitizer import is_safe_raw_tool_payload_replacement_reply

    if result.get("raw_tool_payload_blocked"):
        return "cron_run_raw_tool_payload"
    if result.get("response_fulfilled") is False:
        return "cron_run_unfulfilled_delivery_contract"
    if is_safe_raw_tool_payload_replacement_reply(reply=text, tool_results=result.get("tool_results") or ()):
        return "cron_run_raw_tool_payload"
    if _cron_result_leaked_internal_tool_output(result, text):
        return "cron_run_internal_tool_output_leaked"
    if _cron_result_has_internal_capability_denial(result):
        return "cron_run_denied_internal_capability"
    if (
        result.get("tool_results")
        and not artifacts
        and not _cron_result_has_completed_tool_evidence(result)
    ):
        return "cron_run_without_completed_tool_evidence"
    return None


def _artifact_paths_from_value(value: object) -> tuple[str, ...]:
    paths: list[str] = []
    for item in _artifact_values(value):
        path = _artifact_path_from_value(item)
        if path:
            paths.append(path)
    return tuple(dict.fromkeys(paths))


def _is_inline_or_remote_asset_src(src: str) -> bool:
    raw = str(src or "").strip()
    if not raw or raw.startswith("#"):
        return True
    parsed = urlsplit(raw)
    if parsed.scheme in {"http", "https", "data", "blob", "cid", "mailto", "tel"}:
        return True
    return bool(parsed.netloc)


def _resolve_html_local_image_asset(html_path: Path, src: str) -> Path | None:
    if _is_inline_or_remote_asset_src(src):
        return None
    parsed = urlsplit(str(src or "").strip())
    raw_path = unquote(parsed.path).strip()
    if not raw_path:
        return None
    asset_path = Path(raw_path).expanduser()
    if asset_path.suffix.lower() not in _HTML_INLINE_IMAGE_EXTENSIONS:
        return None
    if not asset_path.is_absolute():
        asset_path = html_path.parent / asset_path
    try:
        resolved = asset_path.resolve()
        if not resolved.is_file() or resolved.stat().st_size <= 0:
            return None
        return resolved
    except OSError:
        return None


def _inline_html_local_image_assets(html_path_text: str) -> set[str]:
    html_path = Path(str(html_path_text or "")).expanduser()
    if html_path.suffix.lower() not in {".html", ".htm"}:
        return set()
    try:
        original = html_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return set()
    support_assets: set[str] = set()
    changed = False

    def replace(match: re.Match[str]) -> str:
        nonlocal changed
        src = match.group("src")
        asset = _resolve_html_local_image_asset(html_path, src)
        if asset is None:
            return match.group(0)
        try:
            raw = asset.read_bytes()
        except OSError:
            return match.group(0)
        mime_type = mimetypes.guess_type(str(asset))[0] or "application/octet-stream"
        encoded = base64.b64encode(raw).decode("ascii")
        support_assets.add(str(asset))
        changed = True
        return f"{match.group('prefix')}{match.group('quote')}data:{mime_type};base64,{encoded}{match.group('quote')}"

    updated = _HTML_LOCAL_IMAGE_SRC_RE.sub(replace, original)
    if changed and updated != original:
        try:
            html_path.write_text(updated, encoding="utf-8")
        except OSError:
            return set()
    return support_assets


def _prepare_cron_deliverable_paths_for_delivery(paths: tuple[str, ...]) -> tuple[tuple[str, ...], set[str]]:
    """Make HTML artifacts self-contained and suppress their local support files."""
    unique_paths = tuple(dict.fromkeys(path for path in paths if str(path or "").strip()))
    support_assets: set[str] = set()
    for path in unique_paths:
        support_assets.update(_inline_html_local_image_assets(path))
    if not support_assets:
        return unique_paths, support_assets
    filtered: list[str] = []
    for path in unique_paths:
        try:
            resolved = str(Path(path).expanduser().resolve())
        except OSError:
            resolved = str(Path(path).expanduser())
        if resolved in support_assets:
            continue
        filtered.append(path)
    return tuple(filtered), support_assets


def _filter_html_support_media_from_text(text: str, support_assets: set[str]) -> str:
    if not support_assets:
        return text
    from nullion.artifacts import parse_media_directive_line

    kept: list[str] = []
    for raw_line in str(text or "").splitlines():
        directive = parse_media_directive_line(raw_line)
        if directive is None:
            kept.append(raw_line)
            continue
        try:
            resolved = str(Path(str(directive.path)).expanduser().resolve())
        except OSError:
            resolved = str(Path(str(directive.path)).expanduser())
        if resolved in support_assets:
            continue
        kept.append(raw_line)
    return "\n".join(kept).strip()


def _workspace_state_filenames(result: dict[str, object]) -> set[str]:
    state_names: set[str] = set()
    for tool_result in result.get("tool_results") or ():
        if _tool_result_name(tool_result) not in {"file_read", "file_write"}:
            continue
        path_text = str(_tool_result_output(tool_result).get("path") or "").strip()
        if not path_text:
            continue
        parts = _path_parts(path_text)
        if "files" in parts and "artifacts" not in parts:
            state_names.add(Path(path_text).name)
    return state_names


def _is_state_artifact_media(path_text: str, state_filenames: set[str]) -> bool:
    if not state_filenames:
        return False
    parts = _path_parts(path_text)
    return "artifacts" in parts and Path(path_text).name in state_filenames


def _file_write_deliverable_artifact_path(path_text: object, state_filenames: set[str]) -> str:
    path = _artifact_path_from_value(path_text)
    if not path or _is_state_artifact_media(path, state_filenames):
        return ""
    parts = _path_parts(path)
    if "artifacts" not in parts:
        return ""
    try:
        candidate = Path(path).expanduser()
        if not candidate.is_file() or candidate.stat().st_size <= 0:
            return ""
        return str(candidate)
    except OSError:
        return ""


def _structured_tool_artifact_paths(result: dict[str, object], state_filenames: set[str]) -> tuple[str, ...]:
    paths: list[str] = []
    for tool_result in result.get("tool_results") or ():
        if _normalized_tool_result_status(tool_result) != "completed":
            continue
        tool_name = _tool_result_name(tool_result)
        output = _tool_result_output(tool_result)
        tool_name = _tool_result_name(tool_result)
        # Only structured outputs from producing tools become outbound cron
        # attachments. Read/verification tools can point at existing files, but
        # those paths are evidence, not a delivery decision.
        if tool_name in CRON_DELIVERABLE_ARTIFACT_TOOLS:
            for key in ("artifact_path", "artifact_paths", "artifacts"):
                for path in _artifact_paths_from_value(output.get(key)):
                    if _is_state_artifact_media(path, state_filenames):
                        continue
                    paths.append(path)
        if tool_name == "file_write":
            path = _file_write_deliverable_artifact_path(output.get("path"), state_filenames)
            if path:
                paths.append(path)
    return tuple(dict.fromkeys(paths))


def _filter_state_media_from_text(text: str, state_filenames: set[str]) -> str:
    if not state_filenames:
        return text
    from nullion.artifacts import parse_media_directive_line

    blocks: list[dict[str, object]] = []
    current_lines: list[str] = []
    current_state_media = False

    def flush_block() -> None:
        nonlocal current_state_media
        if not current_lines and not current_state_media:
            return
        blocks.append({"text": "\n".join(current_lines).strip(), "state_media": current_state_media})
        current_lines.clear()
        current_state_media = False

    for raw_line in str(text or "").splitlines():
        if not raw_line.strip():
            flush_block()
            continue
        directive = parse_media_directive_line(raw_line)
        if directive is not None and _is_state_artifact_media(str(directive.path), state_filenames):
            current_state_media = True
            continue
        current_lines.append(raw_line)
    flush_block()

    if not any(block.get("state_media") for block in blocks):
        return text

    state_media_indexes = [index for index, block in enumerate(blocks) if block.get("state_media")]
    caption_indexes = {
        max((index for index in range(media_index) if str(blocks[index].get("text") or "").strip()), default=-1)
        for media_index in state_media_indexes
    }
    caption_indexes.discard(-1)
    kept: list[str] = []
    for index, block in enumerate(blocks):
        block_text = str(block.get("text") or "").strip()
        if not block_text or block.get("state_media") or index in caption_indexes:
            continue
        kept.append(block_text)
    return "\n\n".join(kept).strip()


def _strip_split_artifact_directives(text: str, deliverable_paths: tuple[str, ...]) -> str:
    if not deliverable_paths:
        return text
    deliverable = {str(Path(path).expanduser()) for path in deliverable_paths}
    lines = str(text or "").splitlines()
    kept: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index].strip()
        following = lines[index + 1].strip().strip("`'\"<>") if index + 1 < len(lines) else ""
        if current in {"MEDIA", "ARTIFACT"} and following and str(Path(following).expanduser()) in deliverable:
            index += 2
            continue
        if current in {"MEDIA", "ARTIFACT"}:
            index += 1
            continue
        kept.append(lines[index])
        index += 1
    return "\n".join(kept).strip()


def _normalize_split_artifact_directives(text: str) -> str:
    from nullion.artifacts import parse_media_directive_line

    lines = str(text or "").splitlines()
    normalized: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index].strip()
        following = lines[index + 1].strip().strip("`'\"<>") if index + 1 < len(lines) else ""
        if current in {"MEDIA", "ARTIFACT"} and following:
            normalized.append(f"{current}:{following}")
            index += 2
            continue
        directive = parse_media_directive_line(lines[index])
        if directive is not None and ":" not in current.split(maxsplit=1)[0]:
            prefix = f"{directive.prefix}\n" if directive.prefix else ""
            normalized.append(f"{prefix}MEDIA:{directive.path}")
            index += 1
            continue
        normalized.append(lines[index])
        index += 1
    return "\n".join(normalized).strip()


def _resolve_cron_media_path(path: Path, *, principal_id: str | None) -> Path | None:
    if path.is_absolute():
        return path if path.is_file() else None
    if not path.parts:
        return None
    try:
        from nullion.workspace_storage import workspace_storage_roots_for_principal

        roots = workspace_storage_roots_for_principal(principal_id)
    except Exception:
        return None
    root_by_name = {
        "artifacts": roots.artifacts,
        "files": roots.files,
        "media": roots.media,
    }
    if path.parts[0] in root_by_name:
        candidate = root_by_name[path.parts[0]].joinpath(*path.parts[1:]) if len(path.parts) > 1 else root_by_name[path.parts[0]]
        return candidate if candidate.is_file() else None
    if len(path.parts) == 1 and path.name and path.suffix:
        all_roots = [roots.artifacts, roots.files, roots.media]
        try:
            from nullion.workspace_storage import workspace_storage_base

            for workspace_root in workspace_storage_base().glob("*"):
                if workspace_root.is_dir():
                    all_roots.extend(
                        [
                            workspace_root / "artifacts",
                            workspace_root / "files",
                            workspace_root / "media",
                        ]
                    )
        except Exception:
            pass
        candidates = [
            root / path.name
            for root in tuple(dict.fromkeys(candidate_root.resolve() for candidate_root in all_roots))
            if (root / path.name).is_file()
        ]
        unique = tuple(dict.fromkeys(candidate.resolve() for candidate in candidates))
        return unique[0] if len(unique) == 1 else None
    return None


def _resolve_relative_media_directives(text: str, *, principal_id: str | None) -> str:
    if not principal_id:
        return text
    from nullion.artifacts import parse_media_directive_line

    lines: list[str] = []
    changed = False
    for raw_line in str(text or "").splitlines():
        directive = parse_media_directive_line(raw_line)
        if directive is None or directive.path.is_absolute():
            lines.append(raw_line)
            continue
        resolved = _resolve_cron_media_path(directive.path, principal_id=principal_id)
        if resolved is None:
            lines.append(raw_line)
            continue
        if directive.prefix:
            lines.append(directive.prefix)
        lines.append(f"MEDIA:{resolved}")
        changed = True
    return "\n".join(lines).strip() if changed else text


def _append_media_directives(text: str, deliverable_paths: tuple[str, ...]) -> str:
    if not deliverable_paths:
        return text
    from nullion.artifacts import parse_media_directive_line

    existing = {
        str(Path(str(directive.path)).expanduser())
        for raw_line in str(text or "").splitlines()
        if (directive := parse_media_directive_line(raw_line)) is not None
    }
    media_lines = [f"MEDIA:{path}" for path in deliverable_paths if str(Path(path).expanduser()) not in existing]
    if not media_lines:
        return text
    parts = [part for part in (str(text or "").strip(), "\n".join(media_lines)) if part]
    return "\n\n".join(parts)


def _cron_internal_state_path(path_text: object) -> bool:
    path = Path(str(path_text or "").strip().strip("`'\"<>.,)")).expanduser()
    parts = _path_parts(str(path))
    if "artifacts" not in parts:
        return False
    suffix = path.suffix.lower()
    if suffix in {".json", ".jsonl", ".db", ".sqlite", ".sqlite3"}:
        return True
    name = path.name.casefold()
    return any(token in name for token in ("checkpoint", "state", "cache", "snapshot"))


def _filter_internal_state_paths_from_text(text: str, deliverable_paths: tuple[str, ...]) -> str:
    from nullion.artifacts import media_candidate_paths_from_text

    deliverable = {str(Path(path).expanduser()) for path in deliverable_paths}
    kept: list[str] = []
    for raw_line in str(text or "").splitlines():
        candidate_paths = tuple(
            dict.fromkeys(
                [
                    *(str(path) for path in media_candidate_paths_from_text(raw_line)),
                    *(
                        match.group(0).rstrip(".,;:)]}")
                        for match in _CRON_ARTIFACT_PATH_RE.finditer(raw_line)
                    ),
                ]
            )
        )
        if candidate_paths and all(
            str(Path(str(path)).expanduser()) not in deliverable and _cron_internal_state_path(path)
            for path in candidate_paths
        ):
            continue
        kept.append(raw_line)
    return "\n".join(kept).strip()


def _cron_text_artifact_path_candidates(text: str) -> tuple[str, ...]:
    candidates: list[str] = []
    for raw_line in str(text or "").splitlines():
        for match in _CRON_ARTIFACT_PATH_RE.finditer(raw_line):
            path_text = match.group(0).rstrip(".,;:)]}")
            if path_text:
                candidates.append(path_text)
    return tuple(dict.fromkeys(candidates))


def _text_referenced_deliverable_paths(
    text: str,
    *,
    principal_id: str | None,
    state_filenames: set[str],
) -> tuple[str, ...]:
    if not principal_id:
        return ()
    paths: list[str] = []
    for path_text in _cron_text_artifact_path_candidates(text):
        if _cron_internal_state_path(path_text) or _is_state_artifact_media(path_text, state_filenames):
            continue
        resolved = _resolve_cron_media_path(Path(path_text), principal_id=principal_id)
        if resolved is None:
            continue
        if _cron_internal_state_path(str(resolved)) or _is_state_artifact_media(str(resolved), state_filenames):
            continue
        paths.append(str(resolved))
    return tuple(dict.fromkeys(paths))


def _strip_deliverable_artifact_paths_from_text(text: str, deliverable_paths: tuple[str, ...]) -> str:
    if not deliverable_paths:
        return text
    deliverable_names = {Path(path).name for path in deliverable_paths}
    kept: list[str] = []
    for raw_line in str(text or "").splitlines():
        candidates = _cron_text_artifact_path_candidates(raw_line)
        if not candidates:
            kept.append(raw_line)
            continue
        remaining = raw_line
        line_has_deliverable = False
        for candidate in candidates:
            if Path(candidate).name not in deliverable_names:
                continue
            remaining = remaining.replace(candidate, "")
            line_has_deliverable = True
        if not line_has_deliverable:
            kept.append(raw_line)
            continue
        if remaining.strip(" \t:-_*•>") and len(remaining.strip()) > 24:
            kept.append(remaining.rstrip())
    return "\n".join(kept).strip()


def cron_delivery_text_from_result(result: dict[str, object], *, principal_id: str | None = None) -> str:
    """Return deliverable cron text after filtering state-file-only media.

    The filter uses runtime facts, not prompt wording: a MEDIA path in
    workspace artifacts is suppressed when it mirrors a file accessed through
    the workspace files area during the same cron run. That preserves explicit
    report/export attachments and completed tool artifact fields while
    preventing tracker/checkpoint files from becoming user-facing attachments.
    """
    from nullion.response_fulfillment_contract import user_visible_text_from_output

    text = cron_delivery_text(user_visible_text_from_output(result), result.get("artifacts"))
    text = _normalize_split_artifact_directives(text)
    text = _resolve_relative_media_directives(text, principal_id=principal_id)
    state_filenames = _workspace_state_filenames(result)
    deliverable_paths = tuple(
        dict.fromkeys(
            [
                *_structured_tool_artifact_paths(result, state_filenames),
                *_text_referenced_deliverable_paths(
                    text,
                    principal_id=principal_id,
                    state_filenames=state_filenames,
                ),
            ]
        )
    )
    deliverable_paths, support_assets = _prepare_cron_deliverable_paths_for_delivery(deliverable_paths)
    text = _filter_state_media_from_text(text, state_filenames)
    text = _filter_html_support_media_from_text(text, support_assets)
    text = _strip_split_artifact_directives(text, deliverable_paths)
    text = _filter_internal_state_paths_from_text(text, deliverable_paths)
    text = _strip_deliverable_artifact_paths_from_text(text, deliverable_paths)
    return _append_media_directives(text, deliverable_paths)


def cron_delivery_artifact_paths_from_result(
    result: dict[str, object],
    text: str | None = None,
    *,
    principal_id: str | None = None,
) -> tuple[str, ...]:
    """Return concrete artifact paths that this cron delivery is about to expose."""
    from nullion.artifacts import parse_media_directive_line

    state_filenames = _workspace_state_filenames(result)
    paths = list(_structured_tool_artifact_paths(result, state_filenames))
    paths.extend(
        _text_referenced_deliverable_paths(
            text or "",
            principal_id=principal_id,
            state_filenames=state_filenames,
        )
    )
    for raw_line in str(text or "").splitlines():
        directive = parse_media_directive_line(raw_line)
        if directive is None:
            continue
        resolved = _resolve_cron_media_path(directive.path, principal_id=principal_id) if principal_id else None
        path_text = str(resolved or directive.path)
        if _cron_internal_state_path(path_text):
            continue
        paths.append(path_text)
    deliverable_paths, _support_assets = _prepare_cron_deliverable_paths_for_delivery(
        tuple(dict.fromkeys(path for path in paths if path))
    )
    return deliverable_paths


def _cron_delivery_text_without_media_directives(text: str) -> str:
    from nullion.artifacts import parse_media_directive_line

    lines = str(text or "").splitlines()
    kept: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index].strip().lstrip("-*•> ").strip()
        following = lines[index + 1].strip().strip("`'\"<>") if index + 1 < len(lines) else ""
        if current in {"MEDIA", "ARTIFACT"}:
            index += 2 if following else 1
            continue
        directive = parse_media_directive_line(lines[index])
        if directive is not None:
            if directive.prefix:
                kept.append(directive.prefix)
            index += 1
            continue
        kept.append(lines[index])
        index += 1
    return "\n".join(kept).strip()


def _cron_attachment_fallback_text(text: str) -> str:
    """Return a compact fallback when artifact upload fails.

    The original delivery attempt may already have emitted a long caption or
    status body before the upload failed. Keep the fallback deliberately short
    so Telegram does not receive a second full report dump.
    """
    fallback_body = _cron_delivery_text_without_media_directives(text)
    fallback_body = " ".join(fallback_body.split())
    if len(fallback_body) > MAX_CRON_ATTACHMENT_FALLBACK_CHARS:
        fallback_body = f"{fallback_body[:MAX_CRON_ATTACHMENT_FALLBACK_CHARS].rstrip()}..."
    if fallback_body:
        return (
            "The scheduled task ran, but I could not attach the report file. "
            "I am not resending the full report here to avoid duplicate long messages.\n\n"
            f"Preview: {fallback_body}"
        )
    return (
        "The scheduled task ran, but I could not attach the report file. "
        "I am not resending the full report here to avoid duplicate long messages."
    )


def cron_artifact_validation_block_reason(
    result: dict[str, object],
    text: str | None = None,
    *,
    principal_id: str | None = None,
) -> str | None:
    """Validate deliverable cron artifacts before marking a run delivered."""
    from nullion.artifact_validation import validate_artifact_paths

    paths = cron_delivery_artifact_paths_from_result(result, text, principal_id=principal_id)
    if not paths:
        result.pop("cron_artifact_validation_errors", None)
        return None
    validation = validate_artifact_paths(paths)
    if validation.ok:
        result.pop("cron_artifact_validation_errors", None)
        return None
    result["cron_artifact_validation_errors"] = [
        {"path": issue.path, "code": issue.code, "message": issue.message}
        for issue in validation.issues
    ]
    return "cron_artifact_validation_failed"


def legacy_cron_delivery_text_with_media(text: str, artifacts: object) -> str:
    """Append MEDIA directives from path-like artifacts without assuming a type."""
    media_lines: list[str] = []
    for artifact in _artifact_values(artifacts):
        path = _artifact_path_from_value(artifact)
        if path:
            media_lines.append(f"MEDIA:{path}")
    if not media_lines:
        return text
    return "\n\n".join([text, "\n".join(dict.fromkeys(media_lines))])


@dataclass(frozen=True)
class CronRunDeliveryCallbacks:
    effective_channel: Callable[[object], str]
    delivery_target: Callable[[object, str], str]
    run_agent_turn: Callable[[object, str], dict[str, object]]
    record_event: Callable[..., None]
    block_reason: Callable[[dict[str, object], str, object], str | None]
    save_web_delivery: Callable[[object, str, str, object, dict[str, object]], bool]
    send_platform_delivery: Callable[[object, str, str, str], bool]
    start_background_delivery: Callable[[str, object], None] | None = None
    clear_background_delivery: Callable[[str], None] | None = None


class _CronRunDeliveryState(TypedDict, total=False):
    job: object
    label: str
    callbacks: CronRunDeliveryCallbacks
    delivery_channel: str
    delivery_target: str
    conversation_id: str
    result: dict[str, object]
    text: str
    artifacts: object
    block_reason: str | None
    send_attempts: int


def _cron_run_resolve_route_node(state: _CronRunDeliveryState) -> dict[str, object]:
    job = state["job"]
    callbacks = state["callbacks"]
    delivery_channel = callbacks.effective_channel(job)
    delivery_target = callbacks.delivery_target(job, delivery_channel)
    conversation_id = cron_conversation_id(job, delivery_channel, delivery_target)
    callbacks.record_event("cron.delivery.started", job, delivery_channel, delivery_target, conversation_id)
    if delivery_channel in MESSAGING_CRON_DELIVERY_CHANNELS and callbacks.start_background_delivery is not None:
        callbacks.start_background_delivery(conversation_id, job)
    return {
        "delivery_channel": delivery_channel,
        "delivery_target": delivery_target,
        "conversation_id": conversation_id,
    }


def _cron_run_agent_node(state: _CronRunDeliveryState) -> dict[str, object]:
    result = state["callbacks"].run_agent_turn(state["job"], state["conversation_id"])
    return {"result": dict(result or {})}


def _cron_run_route_after_agent(state: _CronRunDeliveryState) -> str:
    return "paused" if state.get("result", {}).get("suspended_for_approval") else "prepare"


def _cron_run_paused_node(state: _CronRunDeliveryState) -> dict[str, object]:
    state["callbacks"].record_event(
        "cron.delivery.paused",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
        reason="waiting_for_approval",
    )
    result = dict(state.get("result") or {})
    result["cron_delivery_status"] = "paused_for_approval"
    return {"result": result}


def _cron_run_prepare_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    result = dict(state.get("result") or {})
    artifacts = result.get("artifacts")
    text = cron_delivery_text_from_result(result, principal_id=state.get("conversation_id"))
    block_reason = state["callbacks"].block_reason(result, str(text), artifacts)
    if block_reason is None:
        block_reason = cron_artifact_validation_block_reason(
            result,
            str(text),
            principal_id=state.get("conversation_id"),
        )
    return {"result": result, "text": str(text), "artifacts": artifacts, "block_reason": block_reason}


def _cron_run_route_prepared(state: _CronRunDeliveryState) -> str:
    result = state.get("result") or {}
    if result.get("mini_agent_dispatch"):
        return "deferred"
    if state.get("block_reason"):
        return "blocked"
    if not str(state.get("text") or "").strip():
        return "silent"
    return "web" if state.get("delivery_channel") == "web" else "messaging"


def _send_platform_delivery(
    callback: Callable[..., bool],
    job: object,
    channel: str,
    target: str,
    text: str,
) -> bool:
    """Call platform delivery callbacks across the old and current contracts."""
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return bool(callback(job, channel, target, text))
    positional_count = sum(
        1
        for parameter in signature.parameters.values()
        if parameter.kind
        in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }
    )
    has_varargs = any(
        parameter.kind is inspect.Parameter.VAR_POSITIONAL
        for parameter in signature.parameters.values()
    )
    if has_varargs or positional_count >= 4:
        return bool(callback(job, channel, target, text))
    return bool(callback(job, channel, text))


def _cron_run_blocked_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    if callbacks.clear_background_delivery is not None:
        callbacks.clear_background_delivery(state["conversation_id"])
    result = dict(state.get("result") or {})
    reason = state.get("block_reason") or "cron_delivery_blocked"
    result["cron_delivery_status"] = "failed"
    result["cron_delivery_failed"] = True
    result["cron_run_failed"] = True
    result["reason"] = reason
    callbacks.record_event(
        "cron.delivery.failed",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
        reason=reason,
    )
    return {"result": result}


def _cron_run_silent_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    if callbacks.clear_background_delivery is not None:
        callbacks.clear_background_delivery(state["conversation_id"])
    result = dict(state.get("result") or {})
    result["cron_delivery_status"] = "silent"
    callbacks.record_event(
        "cron.delivery.silent",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
    )
    return {"result": result}


def _cron_run_deferred_node(state: _CronRunDeliveryState) -> dict[str, object]:
    """Leave final delivery to the dispatched mini-agent result callback."""
    callbacks = state["callbacks"]
    result = dict(state.get("result") or {})
    result["cron_delivery_status"] = "deferred"
    callbacks.record_event(
        "cron.delivery.deferred",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
        reason="mini_agent_dispatch",
    )
    return {"result": result}


def _cron_run_web_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    result = dict(state.get("result") or {})
    callbacks.save_web_delivery(
        state["job"],
        state["conversation_id"],
        cron_delivery_reply_text(str(state.get("text") or "")),
        state.get("artifacts"),
        result,
    )
    callbacks.record_event(
        "cron.delivery.saved",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
    )
    result["cron_delivery_status"] = "saved"
    return {"result": result}


def _cron_run_messaging_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    from nullion.artifacts import media_candidate_paths_from_text

    callbacks = state["callbacks"]
    result = dict(state.get("result") or {})
    attempts = int(state.get("send_attempts") or 0) + 1
    text = cron_delivery_reply_text(cron_delivery_text(str(state.get("text") or ""), state.get("artifacts")))
    if _send_platform_delivery(
        callbacks.send_platform_delivery,
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        text,
    ):
        if callbacks.clear_background_delivery is not None:
            callbacks.clear_background_delivery(state["conversation_id"])
        callbacks.record_event(
            "cron.delivery.sent",
            state["job"],
            state["delivery_channel"],
            state["delivery_target"],
            state["conversation_id"],
        )
        result["cron_delivery_status"] = "sent"
        return {"result": result, "send_attempts": attempts}
    if media_candidate_paths_from_text(text):
        fallback_text = _cron_attachment_fallback_text(text)
        if fallback_text and _send_platform_delivery(
            callbacks.send_platform_delivery,
            state["job"],
            state["delivery_channel"],
            state["delivery_target"],
            fallback_text,
        ):
            if callbacks.clear_background_delivery is not None:
                callbacks.clear_background_delivery(state["conversation_id"])
            callbacks.record_event(
                "cron.delivery.partial_success",
                state["job"],
                state["delivery_channel"],
                state["delivery_target"],
                state["conversation_id"],
                reason="attachment delivery failed after text fallback",
            )
            result["cron_delivery_status"] = "partial_success"
            result["cron_delivery_partial_success"] = True
            result["cron_delivery_attachment_failed"] = True
            result["reason"] = "attachment delivery failed"
            return {"result": result, "send_attempts": attempts}
        callbacks.record_event(
            "cron.delivery.failed",
            state["job"],
            state["delivery_channel"],
            state["delivery_target"],
            state["conversation_id"],
            reason="attachment delivery failed",
        )
        result["cron_delivery_status"] = "failed"
        result["cron_delivery_failed"] = True
        return {"result": result, "send_attempts": attempts}
    if attempts < 2:
        callbacks.record_event(
            "cron.delivery.retry",
            state["job"],
            state["delivery_channel"],
            state["delivery_target"],
            state["conversation_id"],
            reason="platform delivery failed",
        )
        return {"result": result, "send_attempts": attempts}
    callbacks.record_event(
        "cron.delivery.failed",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
        reason="missing bot token or target",
    )
    result["cron_delivery_status"] = "failed"
    result["cron_delivery_failed"] = True
    return {"result": result, "send_attempts": attempts}


def _cron_run_route_after_messaging(state: _CronRunDeliveryState) -> str:
    result = state.get("result") or {}
    if result.get("cron_delivery_status") in {"sent", "failed", "partial_success"}:
        return END
    if int(state.get("send_attempts") or 0) < 2:
        return "retry"
    return END


@lru_cache(maxsize=1)
def _compiled_cron_run_delivery_graph():
    graph = StateGraph(_CronRunDeliveryState)
    graph.add_node("resolve_route", _cron_run_resolve_route_node)
    graph.add_node("run_agent", _cron_run_agent_node)
    graph.add_node("paused", _cron_run_paused_node)
    graph.add_node("prepare", _cron_run_prepare_delivery_node)
    graph.add_node("blocked", _cron_run_blocked_node)
    graph.add_node("silent", _cron_run_silent_node)
    graph.add_node("deferred", _cron_run_deferred_node)
    graph.add_node("web", _cron_run_web_delivery_node)
    graph.add_node("messaging", _cron_run_messaging_delivery_node)
    graph.add_edge(START, "resolve_route")
    graph.add_edge("resolve_route", "run_agent")
    graph.add_conditional_edges("run_agent", _cron_run_route_after_agent, {"paused": "paused", "prepare": "prepare"})
    graph.add_conditional_edges(
        "prepare",
        _cron_run_route_prepared,
        {"blocked": "blocked", "silent": "silent", "deferred": "deferred", "web": "web", "messaging": "messaging"},
    )
    graph.add_conditional_edges("messaging", _cron_run_route_after_messaging, {"retry": "messaging", END: END})
    for node in ("paused", "blocked", "silent", "deferred", "web"):
        graph.add_edge(node, END)
    return graph.compile()


def run_cron_delivery_workflow(
    job: object,
    *,
    label: str,
    callbacks: CronRunDeliveryCallbacks,
) -> dict[str, object]:
    final_state = _compiled_cron_run_delivery_graph().invoke(
        {"job": job, "label": label, "callbacks": callbacks, "result": {}}
    )
    result = final_state.get("result")
    return dict(result or {})


__all__ = [
    "CronRunDeliveryCallbacks",
    "MESSAGING_CRON_DELIVERY_CHANNELS",
    "SUPPORTED_CRON_DELIVERY_CHANNELS",
    "configured_delivery_target",
    "clear_cron_execution_metadata_caches",
    "cron_agent_prompt",
    "cron_conversation_id",
    "cron_artifact_validation_block_reason",
    "cron_delivery_artifact_paths_from_result",
    "cron_delivery_reply_text",
    "cron_delivery_target",
    "cron_delivery_text",
    "cron_delivery_text_from_result",
    "cron_deep_agent_dispatch_plan",
    "cron_structured_result_block_reason",
    "effective_cron_delivery_channel",
    "normalize_cron_delivery_channel",
    "run_cron_delivery_workflow",
    "scheduled_task_delivery_text",
]
