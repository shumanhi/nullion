"""Planner-card previews for cron runs.

Cron execution still needs one reliable agent turn to produce the deliverable.
This module builds the optional task-card surface from the structured planner
without dispatching background mini-agents.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any

from nullion import runtime_cache
from nullion.task_decomposer import TaskDecomposer
from nullion.task_queue import TaskGroup, TaskPriority, TaskRecord, TaskStatus, make_group_id, make_task_id
from nullion.task_status_format import format_task_status_line, format_task_status_summary


_PREVIEW_CACHE_MAX_ENTRIES = 64
_PREVIEW_CACHE_TTL_SECONDS_ENV = "NULLION_CRON_PLANNER_PREVIEW_CACHE_TTL_SECONDS"
_DEFAULT_PREVIEW_CACHE_TTL_SECONDS = 0
_PREVIEW_MODEL_TIMEOUT_SECONDS_ENV = "NULLION_CRON_PLANNER_PREVIEW_MODEL_TIMEOUT_SECONDS"
_DEFAULT_PREVIEW_MODEL_TIMEOUT_SECONDS = 30.0
_PREVIEW_MAX_TOKENS_ENV = "NULLION_CRON_PLANNER_PREVIEW_MAX_TOKENS"
_DEFAULT_PREVIEW_MAX_TOKENS = 384
_PREVIEW_REASONING_EFFORT_ENV = "NULLION_CRON_PLANNER_PREVIEW_REASONING_EFFORT"
_DEFAULT_PREVIEW_REASONING_EFFORT = "low"
_PREVIEW_MAX_TOOLS_ENV = "NULLION_CRON_PLANNER_PREVIEW_MAX_TOOLS"
_DEFAULT_PREVIEW_MAX_TOOLS = 20
_PREVIEW_PERSIST_PATH_ENV = "NULLION_CRON_PLANNER_PREVIEW_CACHE_PATH"
_DEFAULT_PREVIEW_PERSIST_FILE = "cron_planner_preview_cache.json"
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _CachedTaskTemplate:
    title: str
    allowed_tools: tuple[str, ...]
    dependency_indexes: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _CachedPreviewTemplate:
    planner_summary: str
    subject: str
    tasks: tuple[_CachedTaskTemplate, ...]


@dataclass(slots=True)
class _CachedPreviewEntry:
    created_at: datetime
    template: _CachedPreviewTemplate


_PREVIEW_CACHE: dict[tuple[str, tuple[str, ...]], _CachedPreviewEntry] = {}
_PERSIST_CACHE_LOADED = False
_PREVIEW_CACHE_NAMESPACE = "cron.planner_preview"
_PREVIEW_CACHE_MESSAGE_NAMESPACE = "cron.planner_preview.message"
_PREVIEW_CACHE_VERSION = "v3"


@dataclass(frozen=True, slots=True)
class CronPlannerStatusPreview:
    group: TaskGroup
    planner_summary: str
    subject: str = ""

    @property
    def group_id(self) -> str:
        return self.group.group_id

    @property
    def allowed_tools(self) -> tuple[str, ...]:
        names: set[str] = set()
        for task in self.group.tasks:
            for tool_name in task.allowed_tools or ():
                normalized = str(tool_name or "").strip()
                if normalized:
                    names.add(normalized)
        return tuple(sorted(names))

    def initial_text(self) -> str:
        status_lines = {
            task.task_id: format_task_status_line(
                task,
                status=TaskStatus.RUNNING if not task.dependencies else TaskStatus.PENDING,
            )
            for task in self.group.tasks
        }
        body = format_task_status_summary(
            self.group.tasks,
            planner_summary=self.planner_summary,
            subject=self.subject,
            status_lines=status_lines,
            default_status=TaskStatus.PENDING,
        )
        return _with_scheduled_task_preview_header(body, self.subject)

    def terminal_text(self, *, success: bool) -> str:
        terminal_status = TaskStatus.COMPLETE if success else TaskStatus.FAILED
        terminal_tasks = [replace(task, status=terminal_status) for task in self.group.tasks]
        status_lines = {
            task.task_id: format_task_status_line(task, status=terminal_status)
            for task in terminal_tasks
        }
        body = format_task_status_summary(
            terminal_tasks,
            planner_summary=self.planner_summary,
            subject=self.subject,
            status_lines=status_lines,
            default_status=terminal_status,
        )
        return _with_scheduled_task_preview_header(body, self.subject)

    def with_group_id(self, group_id: str) -> CronPlannerStatusPreview:
        target_group_id = str(group_id or "").strip()
        if not target_group_id or target_group_id == self.group.group_id:
            return self
        tasks = [
            TaskRecord(
                task_id=task.task_id,
                group_id=target_group_id,
                conversation_id=task.conversation_id,
                principal_id=task.principal_id,
                title=task.title,
                description=task.description,
                status=task.status,
                priority=task.priority,
                allowed_tools=list(task.allowed_tools or []),
                dependencies=list(task.dependencies or []),
                context_key_in=task.context_key_in,
                context_key_out=task.context_key_out,
                created_at=task.created_at,
                started_at=task.started_at,
                completed_at=task.completed_at,
                agent_id=task.agent_id,
                result=task.result,
                retry_count=task.retry_count,
                max_retries=task.max_retries,
                timeout_s=task.timeout_s,
                deep_agent_skills=list(task.deep_agent_skills or []),
                deep_agent_subagents=list(task.deep_agent_subagents or []),
            )
            for task in self.group.tasks
        ]
        group = TaskGroup(
            group_id=target_group_id,
            conversation_id=self.group.conversation_id,
            original_message=self.group.original_message,
            tasks=tasks,
            planner_metadata=dict(self.group.planner_metadata or {}),
            created_at=self.group.created_at,
        )
        return CronPlannerStatusPreview(
            group=group,
            planner_summary=self.planner_summary,
            subject=self.subject,
        )


def _with_scheduled_task_preview_header(text: str, subject: str = "") -> str:
    subject_text = " ".join(str(subject or "").split())
    header = f"⏰ SCHEDULED TASK: {subject_text}" if subject_text else "⏰ SCHEDULED TASK"
    body = str(text or "").strip()
    return f"{header}\n\n{body}" if body else header


def build_cron_planner_status_preview(
    *,
    model_client: Any,
    user_message: str,
    conversation_id: str,
    principal_id: str,
    tool_registry: Any,
    subject: str = "",
    cache_only: bool = False,
) -> CronPlannerStatusPreview | None:
    """Return a display-only planner card from a validated structured DAG."""
    if model_client is None:
        return None
    if not _cron_planner_model_preview_enabled():
        return build_cron_planner_status_fallback(
            user_message=user_message,
            conversation_id=conversation_id,
            principal_id=principal_id,
            subject=subject,
        )
    if cache_only:
        cached = _preview_cache_get_by_message(str(user_message or ""), subject=subject)
        if cached is not None:
            logger.info("cron planner preview cache hit conversation_id=%s cache_only=true", conversation_id)
            return _materialize_cached_preview(
                cached,
                conversation_id=conversation_id,
                principal_id=principal_id,
                subject=subject,
            )
        logger.info("cron planner preview cache miss conversation_id=%s cache_only=true", conversation_id)
        return None
    tools = [
        str(tool.get("name", ""))
        for tool in (tool_registry.list_tool_definitions() if tool_registry is not None else ())
        if isinstance(tool, dict) and tool.get("name")
    ]
    tools = _preview_tools(tools)
    cache_key = (_preview_cache_message(str(user_message or ""), subject=subject), tuple(sorted(tools)))
    _load_persistent_preview_cache()
    cached = _preview_cache_get(cache_key)
    if cached is not None:
        logger.info("cron planner preview cache hit conversation_id=%s", conversation_id)
        return _materialize_cached_preview(
            cached,
            conversation_id=conversation_id,
            principal_id=principal_id,
            subject=subject,
        )
    timeout_seconds = _preview_model_timeout_seconds()
    started = time.perf_counter()
    preview_model_client = _cron_preview_model_client(model_client)
    logger.info(
        "cron planner preview profile conversation_id=%s model_client=%s model=%s prompt_chars=%d tools=%d timeout_s=%.1f max_tokens=%s reasoning_effort=%s",
        conversation_id,
        type(preview_model_client).__name__,
        getattr(preview_model_client, "model", None),
        len(str(user_message or "")),
        len(tools),
        timeout_seconds,
        getattr(preview_model_client, "max_tokens", None),
        getattr(preview_model_client, "reasoning_effort", None),
    )
    logger.info(
        "cron planner preview started conversation_id=%s timeout_s=%.1f tools=%d",
        conversation_id,
        timeout_seconds,
        len(tools),
    )
    decomposer = TaskDecomposer(
        model_client=preview_model_client,
        model_timeout_seconds=timeout_seconds,
        scheduled_preview_mode=True,
    )
    preview_request = _cron_preview_request_text(user_message, subject=subject)
    dag_plan = decomposer.plan_dag(preview_request, available_tools=tools)
    plan_ms = (time.perf_counter() - started) * 1000
    if not dag_plan.can_dispatch_when_requested:
        logger.warning(
            "cron planner preview skipped conversation_id=%s plan_ms=%.1f disposition=%s valid=%s tasks=%d errors=%s",
            conversation_id,
            plan_ms,
            dag_plan.disposition,
            dag_plan.is_valid,
            len(dag_plan.tasks),
            dag_plan.validation_errors,
        )
        return None
    group = decomposer.decompose(
        preview_request,
        group_id=make_group_id(),
        conversation_id=conversation_id,
        principal_id=principal_id,
        available_tools=tools,
        dag_plan=dag_plan,
    )
    group = _strip_runtime_delivery_only_tasks(group)
    preview = CronPlannerStatusPreview(
        group=group,
        planner_summary=_planner_summary_from_disposition(
            disposition=dag_plan.disposition,
            task_count=len(group.tasks),
        ),
        subject=subject,
    )
    _preview_cache_set(cache_key, _build_cached_template(preview))
    logger.info(
        "cron planner preview built conversation_id=%s plan_ms=%.1f total_ms=%.1f disposition=%s tasks=%d tools=%d",
        conversation_id,
        plan_ms,
        (time.perf_counter() - started) * 1000,
        dag_plan.disposition,
        len(group.tasks),
        len(tools),
    )
    return preview


def _cron_planner_model_preview_enabled() -> bool:
    raw = os.environ.get("NULLION_CRON_PLANNER_MODEL_PREVIEW_ENABLED")
    if raw is None or raw.strip() == "":
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def build_cron_planner_status_fallback(
    *,
    user_message: str,
    conversation_id: str,
    principal_id: str,
    subject: str = "",
) -> CronPlannerStatusPreview:
    """Return a non-blocking display preview from structured cron metadata.

    This is only for the status surface when the model planner is unavailable or
    returns unparsable output. It intentionally does not constrain tool scope.
    """
    task_text = _fallback_task_text(user_message)
    title = _fallback_task_title(task_text, subject=subject)
    group_id = make_group_id()
    task = TaskRecord(
        task_id=make_task_id(),
        group_id=group_id,
        conversation_id=conversation_id,
        principal_id=principal_id,
        title=title,
        description=task_text or "Building execution plan for scheduled task.",
        status=TaskStatus.PENDING,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
    )
    group = TaskGroup(
        group_id=group_id,
        conversation_id=conversation_id,
        original_message=user_message,
        tasks=[task],
        planner_metadata={"planner": "cron_metadata_fallback", "dispatchable": False},
    )
    return CronPlannerStatusPreview(
        group=group,
        planner_summary="Scheduled task",
        subject=subject,
    )


def cron_planner_run_succeeded(result: dict[str, object]) -> bool:
    if result.get("cron_delivery_failed") or result.get("cron_run_failed"):
        return False
    if result.get("reached_iteration_limit") or result.get("raw_tool_payload_blocked"):
        return False
    if result.get("suspended_for_approval"):
        return False
    status = str(result.get("cron_delivery_status") or "").strip()
    return status not in {"failed", "paused_for_approval"}


def _planner_summary_from_disposition(*, disposition: str, task_count: int) -> str:
    label = str(disposition or "").replace("_", " ").title() or "Mission"
    if task_count:
        return f"{label} * {task_count} task{'s' if task_count != 1 else ''}"
    return label


def _fallback_task_text(user_message: str) -> str:
    try:
        parsed = json.loads(str(user_message or ""))
    except json.JSONDecodeError:
        return str(user_message or "").strip()
    if isinstance(parsed, dict):
        for key in ("task", "name", "label"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return str(user_message or "").strip()


def _fallback_task_title(task_text: str, *, subject: str) -> str:
    title = str(task_text or subject or "Run scheduled task").strip().splitlines()[0]
    title = " ".join(title.split())
    if len(title) <= 50:
        return title or "Run scheduled task"
    return title[:47].rstrip() + "..."


def _cron_preview_request_text(user_message: str, *, subject: str) -> str:
    """Build a provider-agnostic structured preview request for scheduled runs.

    This is explicit task-frame metadata, not intent derived from free-form words.
    """
    task_name = str(subject or "").strip()
    task_text = str(user_message or "").strip()
    lines = [
        "scheduled_task_preview=true",
        "required_inputs_resolved=true",
        "clarification_allowed=false",
        "decompose_best_effort=true",
        "external_platform_delivery_managed_by_runtime=true",
        "omit_runtime_delivery_only_terminal_steps=true",
    ]
    if task_name:
        lines.append(f"scheduled_task_name={task_name}")
    if task_text:
        lines.append(f"scheduled_task_request={task_text}")
    return "\n".join(lines)


def _preview_cache_ttl_seconds() -> int:
    raw = os.environ.get(_PREVIEW_CACHE_TTL_SECONDS_ENV, "").strip()
    if not raw:
        return _DEFAULT_PREVIEW_CACHE_TTL_SECONDS
    try:
        parsed = int(raw)
    except ValueError:
        return _DEFAULT_PREVIEW_CACHE_TTL_SECONDS
    return max(0, parsed)


def clear_cron_planner_preview_caches() -> None:
    """Clear in-memory and shared cron planner-preview caches."""

    global _PERSIST_CACHE_LOADED
    _PREVIEW_CACHE.clear()
    _PERSIST_CACHE_LOADED = False
    runtime_cache.invalidate_prefix(_PREVIEW_CACHE_NAMESPACE)


def _preview_model_timeout_seconds() -> float:
    raw = os.environ.get(_PREVIEW_MODEL_TIMEOUT_SECONDS_ENV, "").strip()
    if not raw:
        return _DEFAULT_PREVIEW_MODEL_TIMEOUT_SECONDS
    try:
        parsed = float(raw)
    except ValueError:
        return _DEFAULT_PREVIEW_MODEL_TIMEOUT_SECONDS
    return max(1.0, parsed)


def _preview_max_tokens() -> int:
    raw = os.environ.get(_PREVIEW_MAX_TOKENS_ENV, "").strip()
    if not raw:
        return _DEFAULT_PREVIEW_MAX_TOKENS
    try:
        parsed = int(raw)
    except ValueError:
        return _DEFAULT_PREVIEW_MAX_TOKENS
    return max(64, parsed)


def _preview_reasoning_effort() -> str:
    value = str(os.environ.get(_PREVIEW_REASONING_EFFORT_ENV, _DEFAULT_PREVIEW_REASONING_EFFORT)).strip().lower()
    return value or _DEFAULT_PREVIEW_REASONING_EFFORT


def _preview_tools(tools: list[str]) -> list[str]:
    clean = sorted({str(name).strip() for name in (tools or []) if str(name).strip()})
    raw = os.environ.get(_PREVIEW_MAX_TOOLS_ENV, "").strip()
    limit = _DEFAULT_PREVIEW_MAX_TOOLS
    if raw:
        try:
            limit = max(1, int(raw))
        except ValueError:
            limit = _DEFAULT_PREVIEW_MAX_TOOLS
    if len(clean) <= limit:
        return clean
    return clean[:limit]


def _preview_persist_path() -> Path:
    override = str(os.environ.get(_PREVIEW_PERSIST_PATH_ENV, "")).strip()
    if override:
        return Path(override).expanduser()
    home = Path(str(os.environ.get("NULLION_HOME") or Path.home() / ".nullion"))
    return home / _DEFAULT_PREVIEW_PERSIST_FILE


def _load_persistent_preview_cache() -> None:
    global _PERSIST_CACHE_LOADED
    if _PERSIST_CACHE_LOADED:
        return
    _PERSIST_CACHE_LOADED = True
    path = _preview_persist_path()
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.debug("cron planner preview persistent cache read failed", exc_info=True)
        return
    if not isinstance(payload, list):
        return
    now = datetime.now(timezone.utc)
    ttl = _preview_cache_ttl_seconds()
    for item in payload:
        try:
            user_message = str(item.get("user_message") or "")
            tools = tuple(str(name) for name in (item.get("tools") or []))
            created_at_raw = str(item.get("created_at") or "")
            template_raw = item.get("template") or {}
            created_at = datetime.fromisoformat(created_at_raw) if created_at_raw else now
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            if ttl > 0 and created_at + timedelta(seconds=ttl) < now:
                continue
            template = _cached_template_from_payload(template_raw)
            if template is None:
                continue
            key = (user_message, tools)
            entry = _CachedPreviewEntry(
                created_at=created_at,
                template=template,
            )
            _PREVIEW_CACHE[key] = entry
            _shared_preview_cache_set(key, entry)
        except Exception:
            logger.debug("cron planner preview persistent cache item parse failed", exc_info=True)
            continue
    _trim_preview_cache()


def _persist_preview_cache() -> None:
    path = _preview_persist_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: list[dict[str, object]] = []
        for (user_message, tools), entry in _PREVIEW_CACHE.items():
            payload.append(
                {
                    "user_message": user_message,
                    "tools": list(tools),
                    "created_at": entry.created_at.isoformat(),
                    "template": _cached_template_to_payload(entry.template),
                }
            )
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        logger.debug("cron planner preview persistent cache write failed", exc_info=True)


def _preview_cache_entry_to_payload(entry: _CachedPreviewEntry) -> dict[str, object]:
    return {
        "created_at": entry.created_at.isoformat(),
        "template": _cached_template_to_payload(entry.template),
    }


def _preview_cache_entry_from_payload(payload: object) -> _CachedPreviewEntry | None:
    if not isinstance(payload, dict):
        return None
    template = _cached_template_from_payload(payload.get("template"))
    if template is None:
        return None
    created_at_raw = str(payload.get("created_at") or "")
    try:
        created_at = datetime.fromisoformat(created_at_raw) if created_at_raw else datetime.now(timezone.utc)
    except ValueError:
        created_at = datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return _CachedPreviewEntry(created_at=created_at, template=template)


def _shared_preview_cache_get(key: tuple[str, tuple[str, ...]]) -> _CachedPreviewEntry | None:
    ttl_seconds = _preview_cache_ttl_seconds()
    cached = runtime_cache.get_json(
        _PREVIEW_CACHE_NAMESPACE,
        [key[0], list(key[1])],
        version=_PREVIEW_CACHE_VERSION,
        ttl_seconds=ttl_seconds if ttl_seconds > 0 else None,
        persistent=True,
    )
    if not cached.hit:
        return None
    entry = _preview_cache_entry_from_payload(cached.value)
    if entry is None:
        return None
    return entry


def _shared_preview_cache_get_by_message(user_message: str, *, subject: str = "") -> _CachedPreviewEntry | None:
    ttl_seconds = _preview_cache_ttl_seconds()
    for candidate in _preview_cache_message_candidates(user_message, subject=subject):
        cached = runtime_cache.get_json(
            _PREVIEW_CACHE_MESSAGE_NAMESPACE,
            [candidate],
            version=_PREVIEW_CACHE_VERSION,
            ttl_seconds=ttl_seconds if ttl_seconds > 0 else None,
            persistent=True,
        )
        if not cached.hit:
            continue
        entry = _preview_cache_entry_from_payload(cached.value)
        if entry is not None:
            return entry
    return None


def _shared_preview_cache_set(key: tuple[str, tuple[str, ...]], entry: _CachedPreviewEntry) -> None:
    payload = _preview_cache_entry_to_payload(entry)
    ttl_seconds = _preview_cache_ttl_seconds()
    ttl = ttl_seconds if ttl_seconds > 0 else None
    runtime_cache.set_json(
        _PREVIEW_CACHE_NAMESPACE,
        [key[0], list(key[1])],
        payload,
        version=_PREVIEW_CACHE_VERSION,
        ttl_seconds=ttl,
        persistent=True,
        max_entries=_PREVIEW_CACHE_MAX_ENTRIES,
    )
    for candidate in _preview_cache_message_candidates(key[0], subject=entry.template.subject):
        runtime_cache.set_json(
            _PREVIEW_CACHE_MESSAGE_NAMESPACE,
            [candidate],
            payload,
            version=_PREVIEW_CACHE_VERSION,
            ttl_seconds=ttl,
            persistent=True,
            max_entries=_PREVIEW_CACHE_MAX_ENTRIES,
        )


def _cached_template_to_payload(template: _CachedPreviewTemplate) -> dict[str, object]:
    return {
        "planner_summary": template.planner_summary,
        "subject": template.subject,
        "tasks": [
            {
                "title": task.title,
                "allowed_tools": list(task.allowed_tools),
                "dependency_indexes": list(task.dependency_indexes),
            }
            for task in template.tasks
        ],
    }


def _cached_template_from_payload(payload: object) -> _CachedPreviewTemplate | None:
    if not isinstance(payload, dict):
        return None
    tasks_raw = payload.get("tasks")
    if not isinstance(tasks_raw, list):
        return None
    tasks: list[_CachedTaskTemplate] = []
    for item in tasks_raw:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "Task")
        allowed_tools = tuple(str(name) for name in (item.get("allowed_tools") or []))
        dep_indexes = tuple(int(value) for value in (item.get("dependency_indexes") or []) if isinstance(value, int))
        tasks.append(
            _CachedTaskTemplate(
                title=title,
                allowed_tools=allowed_tools,
                dependency_indexes=dep_indexes,
            )
        )
    if not tasks:
        return None
    return _CachedPreviewTemplate(
        planner_summary=str(payload.get("planner_summary") or "Mission"),
        subject=str(payload.get("subject") or ""),
        tasks=tuple(tasks),
    )


def _cron_preview_model_client(model_client: Any) -> Any:
    if model_client is None:
        return None
    updates: dict[str, object] = {}
    if hasattr(model_client, "max_tokens"):
        max_tokens = _preview_max_tokens()
        if getattr(model_client, "max_tokens", None) != max_tokens:
            updates["max_tokens"] = max_tokens
    if hasattr(model_client, "reasoning_effort"):
        effort = _preview_reasoning_effort()
        current = str(getattr(model_client, "reasoning_effort", "") or "").strip().lower()
        if current != effort:
            updates["reasoning_effort"] = effort
    if not updates:
        return model_client
    try:
        return replace(model_client, **updates)
    except Exception:
        logger.debug("cron planner preview model profile replace failed", exc_info=True)
        return model_client


def _preview_cache_get(
    key: tuple[str, tuple[str, ...]],
) -> _CachedPreviewTemplate | None:
    _load_persistent_preview_cache()
    ttl_seconds = _preview_cache_ttl_seconds()
    entry = _PREVIEW_CACHE.get(key)
    if entry is None:
        entry = _shared_preview_cache_get(key)
        if entry is not None:
            _PREVIEW_CACHE[key] = entry
    if entry is None:
        return None
    if ttl_seconds > 0 and datetime.now(timezone.utc) - entry.created_at > timedelta(seconds=ttl_seconds):
        _PREVIEW_CACHE.pop(key, None)
        _persist_preview_cache()
        return None
    return entry.template


def _preview_cache_get_by_message(user_message: str, *, subject: str = "") -> _CachedPreviewTemplate | None:
    _load_persistent_preview_cache()
    ttl_seconds = _preview_cache_ttl_seconds()
    if not user_message:
        return None
    now = datetime.now(timezone.utc)
    key_candidates = set(_preview_cache_message_candidates(user_message, subject=subject))
    for key, entry in list(_PREVIEW_CACHE.items()):
        if key[0] not in key_candidates:
            continue
        if ttl_seconds > 0 and now - entry.created_at > timedelta(seconds=ttl_seconds):
            _PREVIEW_CACHE.pop(key, None)
            _persist_preview_cache()
            continue
        return entry.template
    shared_entry = _shared_preview_cache_get_by_message(user_message, subject=subject)
    if shared_entry is not None:
        return shared_entry.template
    return None


def _preview_cache_set(
    key: tuple[str, tuple[str, ...]],
    template: _CachedPreviewTemplate,
) -> None:
    entry = _CachedPreviewEntry(created_at=datetime.now(timezone.utc), template=template)
    _PREVIEW_CACHE[key] = entry
    _trim_preview_cache()
    _shared_preview_cache_set(key, entry)
    _persist_preview_cache()


def _preview_cache_message(user_message: str, *, subject: str = "") -> str:
    candidates = _preview_cache_message_candidates(user_message, subject=subject)
    return candidates[0] if candidates else ""


def _preview_cache_message_candidates(user_message: str, *, subject: str = "") -> list[str]:
    raw = str(user_message or "").strip()
    task_text = _fallback_task_text(raw)
    subject_text = " ".join(str(subject or "").split()).strip()
    normalized_lines = []
    for line in str(task_text or "").splitlines():
        compact = " ".join(str(line).split()).strip()
        if compact:
            normalized_lines.append(compact)
    normalized_task = "\n".join(normalized_lines).strip()
    canonical = normalized_task
    if subject_text:
        task_lower = canonical.casefold()
        subject_lower = subject_text.casefold()
        if not task_lower.startswith(subject_lower):
            canonical = f"{subject_text}\n{canonical}".strip()
    candidates: list[str] = []
    for value in (canonical, normalized_task, raw):
        if value and value not in candidates:
            candidates.append(value)
    return candidates


def _trim_preview_cache() -> None:
    while len(_PREVIEW_CACHE) > _PREVIEW_CACHE_MAX_ENTRIES:
        oldest_key = min(_PREVIEW_CACHE, key=lambda item: _PREVIEW_CACHE[item].created_at)
        _PREVIEW_CACHE.pop(oldest_key, None)


def _build_cached_template(preview: CronPlannerStatusPreview) -> _CachedPreviewTemplate:
    index_by_task_id = {task.task_id: idx for idx, task in enumerate(preview.group.tasks)}
    tasks: list[_CachedTaskTemplate] = []
    for task in preview.group.tasks:
        tasks.append(
            _CachedTaskTemplate(
                title=str(task.title or "").strip() or "Task",
                allowed_tools=tuple(str(tool).strip() for tool in (task.allowed_tools or ()) if str(tool).strip()),
                dependency_indexes=tuple(
                    sorted(
                        index_by_task_id[dep]
                        for dep in (task.dependencies or ())
                        if dep in index_by_task_id
                    )
                ),
            )
        )
    return _CachedPreviewTemplate(
        planner_summary=preview.planner_summary,
        subject=preview.subject,
        tasks=tuple(tasks),
    )


def _materialize_cached_preview(
    template: _CachedPreviewTemplate,
    *,
    conversation_id: str,
    principal_id: str,
    subject: str = "",
) -> CronPlannerStatusPreview:
    group_id = make_group_id()
    task_ids = [make_task_id() for _ in template.tasks]
    tasks: list[TaskRecord] = []
    disposition = _disposition_from_planner_summary(template.planner_summary)
    for idx, task_template in enumerate(template.tasks):
        dependency_indexes = [
            dep_idx
            for dep_idx in task_template.dependency_indexes
            if 0 <= dep_idx < idx
        ]
        # Cached previews can outlive planner fixes. Preserve the user-visible
        # contract: a sequential preview must not dispatch later steps until the
        # previous step has completed successfully.
        if disposition == "sequential_mission" and idx > 0 and (idx - 1) not in dependency_indexes:
            dependency_indexes.append(idx - 1)
        dependencies = [task_ids[dep_idx] for dep_idx in sorted(set(dependency_indexes)) if dep_idx < len(task_ids)]
        tasks.append(
            TaskRecord(
                task_id=task_ids[idx],
                group_id=group_id,
                conversation_id=conversation_id,
                principal_id=principal_id,
                title=task_template.title,
                description=task_template.title,
                status=TaskStatus.PENDING,
                priority=TaskPriority.NORMAL,
                allowed_tools=list(task_template.allowed_tools),
                dependencies=dependencies,
            )
        )
    group = TaskGroup(
        group_id=group_id,
        conversation_id=conversation_id,
        original_message="",
        tasks=tasks,
        planner_metadata={
            "planner": "model_dag",
            "disposition": disposition,
            "dispatchable_when_requested": True,
            "from_cache": True,
        },
    )
    group = _strip_runtime_delivery_only_tasks(group)
    planner_summary = _planner_summary_from_disposition(
        disposition=disposition,
        task_count=len(group.tasks),
    )
    return CronPlannerStatusPreview(
        group=group,
        planner_summary=planner_summary,
        subject=subject or template.subject,
    )


def _strip_runtime_delivery_only_tasks(group: TaskGroup) -> TaskGroup:
    """Remove terminal presentation/delivery placeholders from cron previews.

    Platform delivery happens after the scheduled run. A terminal task with no
    tools, no artifact contract, and no context output has no runtime work for a
    mini-agent to perform, so keeping it in the DAG only creates false failures.
    """
    tasks = list(group.tasks or [])
    if len(tasks) <= 1:
        return group
    kept = {task.task_id for task in tasks}
    removed_any = False
    while True:
        dependents = {
            dep
            for task in tasks
            if task.task_id in kept
            for dep in (task.dependencies or [])
            if dep in kept
        }
        removable = {
            task.task_id
            for task in tasks
            if task.task_id in kept
            and task.task_id not in dependents
            and not _task_has_runtime_work(task)
        }
        if not removable or len(kept - removable) == 0:
            break
        kept -= removable
        removed_any = True
    if not removed_any:
        return group
    filtered = [
        replace(
            task,
            dependencies=[dep for dep in (task.dependencies or []) if dep in kept],
        )
        for task in tasks
        if task.task_id in kept
    ]
    return TaskGroup(
        group_id=group.group_id,
        conversation_id=group.conversation_id,
        original_message=group.original_message,
        tasks=filtered,
        planner_metadata=dict(group.planner_metadata or {}),
        created_at=group.created_at,
    )


def _task_has_runtime_work(task: TaskRecord) -> bool:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    if any(str(tool or "").strip() for tool in (getattr(task, "allowed_tools", None) or ())):
        return True
    if str(getattr(task, "context_key_out", "") or "").strip():
        return True
    if metadata.get("requires_artifact_delivery") or metadata.get("required_artifact_kind"):
        return True
    if str(metadata.get("artifact_role") or "").strip():
        return True
    return False


def _disposition_from_planner_summary(summary: str) -> str:
    normalized = str(summary or "").strip().lower()
    if "parallel mission" in normalized:
        return "parallel_mission"
    if "sequential mission" in normalized:
        return "sequential_mission"
    if "single turn" in normalized:
        return "single_turn"
    return ""


__all__ = [
    "CronPlannerStatusPreview",
    "build_cron_planner_status_preview",
    "build_cron_planner_status_fallback",
    "clear_cron_planner_preview_caches",
    "cron_planner_run_succeeded",
]
