"""Shared cron delivery routing helpers.

Cron jobs can be created by web chat, Telegram, Slack, Discord, or direct REST.
Keep routing decisions here so adapters do not each infer delivery semantics.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from contextlib import nullcontext
from datetime import UTC, datetime
from functools import lru_cache
import html
import json
import mimetypes
import inspect
import logging
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, TypedDict
from urllib.error import URLError
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

from langgraph.graph import END, START, StateGraph
from nullion.execution_outcome import build_execution_outcome

logger = logging.getLogger(__name__)

SUPPORTED_CRON_DELIVERY_CHANNELS = frozenset({"web", "telegram", "slack", "discord"})
MESSAGING_CRON_DELIVERY_CHANNELS = frozenset({"telegram", "slack", "discord"})
MAX_CRON_TEXT_ARTIFACT_CHARS = 12000
MAX_CRON_ATTACHMENT_FALLBACK_CHARS = 420
DEFAULT_CRON_NO_OUTPUT_MESSAGE = "Cron ran successfully; no output was produced."
CRON_DELIVERY_REPLY_PREFIX = "⏰ "
CRON_DELIVERY_REPLY_PREFIXES = (CRON_DELIVERY_REPLY_PREFIX, "⏱️ ", "❖ ")
SCHEDULED_TASK_STATUS_TITLE = "⏱️ SCHEDULED TASK"
SCHEDULED_TASK_DELIVERY_PREFIX = "⏱️ SCHEDULED TASK:"
CRON_INTERNAL_CAPABILITY_TAGS = frozenset({"scheduler"})
CRON_INTERNAL_REFERENCE_TOOLS = frozenset({"request_tool_scope", "skill_pack_read"})
CRON_DELIVERABLE_ARTIFACT_TOOLS = frozenset(
    {
        "document_create",
        "file_write",
        "pdf_create",
        "pdf_edit",
        "presentation_create",
        "image_generate",
        "spreadsheet_create",
    }
)
_HTML_LOCAL_IMAGE_SRC_RE = re.compile(
    r"(?P<prefix><img\b[^>]*?\bsrc\s*=\s*)(?P<quote>[\"'])(?P<src>[^\"']+)(?P=quote)",
    re.IGNORECASE,
)
_CRON_ARTIFACT_PATH_RE = re.compile(r"(?<![\w./-])(?:[~\w./:-]*/)?artifacts/[^\s`\"'<>]+")
_HTML_INLINE_IMAGE_EXTENSIONS = frozenset({".apng", ".avif", ".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"})
_HTML_INLINE_REMOTE_IMAGE_MAX_BYTES = 4 * 1024 * 1024
_HTML_INLINE_REMOTE_IMAGE_TIMEOUT_SECONDS = 8.0
_HTML_INLINE_REMOTE_IMAGE_USER_AGENT = "NullionCronDelivery/1.0"
_HTML_AUTO_INLINE_REMOTE_IMAGE_MAX_ATTEMPTS = 12
HTML_IMAGE_DELIVERY_MODE_LINKED = "linked"
HTML_IMAGE_DELIVERY_MODE_AUTO = "auto"
HTML_IMAGE_DELIVERY_MODE_SELF_CONTAINED = "self_contained"
_HTML_SELF_CONTAINED_REMOTE_SRC_RE = re.compile(
    r"<img\b[^>]*\bsrc\s*=\s*[\"']\s*https?://[^\"']+[\"']",
    flags=re.IGNORECASE,
)
_CRON_INTERNAL_PREVIEW_SCHEMA_RE = re.compile(r'"(?:original_chars|preview)"\s*:', re.IGNORECASE)
_CRON_INTERNAL_UUID_TOKEN_RE = re.compile(
    r"\$[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_CRON_SPACED_TOKEN_RE = re.compile(r"(?:\b[A-Za-z]\s+){4,}[A-Za-z]\b")
_CRON_RESTART_ACTIVE_EVENT_TYPES = frozenset(
    {
        "cron.delivery.started",
        "cron.delivery.agent_preflight",
        "cron.delivery.deferred",
        "cron.delivery.blocked",
    }
)
_CRON_RESTART_TERMINAL_EVENT_TYPES = frozenset(
    {
        "cron.delivery.sent",
        "cron.delivery.failed",
        "cron.delivery.saved",
        "cron.delivery.silent",
        "cron.delivery.cancelled",
    }
)


@dataclass(frozen=True, slots=True)
class ManualCronBackgroundRunHandle:
    conversation_id: str
    task_group_id: str
    job_id: str
    job_name: str
    cancel: Callable[[], bool]


_MANUAL_CRON_BACKGROUND_RUNS_LOCK = threading.Lock()
_MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION: dict[str, dict[str, ManualCronBackgroundRunHandle]] = {}
_MANUAL_CRON_BACKGROUND_SERIAL_LOCKS_BY_CONVERSATION: dict[str, threading.Lock] = {}


def _manual_cron_background_serial_lock(conversation_id: object) -> threading.Lock | None:
    conversation_key = str(conversation_id or "").strip()
    if not conversation_key:
        return None
    with _MANUAL_CRON_BACKGROUND_RUNS_LOCK:
        lock = _MANUAL_CRON_BACKGROUND_SERIAL_LOCKS_BY_CONVERSATION.get(conversation_key)
        if lock is None:
            lock = threading.Lock()
            _MANUAL_CRON_BACKGROUND_SERIAL_LOCKS_BY_CONVERSATION[conversation_key] = lock
        return lock


def register_manual_cron_background_run(
    conversation_id: object,
    task_group_id: object,
    job: object,
    cancel: Callable[[], bool],
) -> Callable[[], None]:
    """Track a manual cron run so session-level stop commands can cancel it."""

    conversation_key = str(conversation_id or "").strip()
    group_key = str(task_group_id or "").strip()
    if not conversation_key or not group_key:
        return lambda: None
    handle = ManualCronBackgroundRunHandle(
        conversation_id=conversation_key,
        task_group_id=group_key,
        job_id=str(getattr(job, "id", "") or "").strip(),
        job_name=str(getattr(job, "name", "") or "scheduled task").strip() or "scheduled task",
        cancel=cancel,
    )
    with _MANUAL_CRON_BACKGROUND_RUNS_LOCK:
        runs = _MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION.setdefault(conversation_key, {})
        runs[group_key] = handle

    def _unregister() -> None:
        with _MANUAL_CRON_BACKGROUND_RUNS_LOCK:
            runs = _MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION.get(conversation_key)
            if runs is None:
                return
            if runs.get(group_key) == handle:
                runs.pop(group_key, None)
            if not runs:
                _MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION.pop(conversation_key, None)

    return _unregister


def cancel_manual_cron_background_runs(conversation_id: object) -> int:
    """Cancel in-flight manual cron runs for one chat/session."""

    conversation_key = str(conversation_id or "").strip()
    if not conversation_key:
        return 0
    with _MANUAL_CRON_BACKGROUND_RUNS_LOCK:
        runs = tuple(_MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION.pop(conversation_key, {}).values())
    cancelled = 0
    for handle in runs:
        try:
            if handle.cancel():
                cancelled += 1
        except Exception:
            logger.debug("Unable to cancel manual cron background run %s", handle.task_group_id, exc_info=True)
    return cancelled


def cancel_manual_cron_background_run(
    *,
    conversation_id: object = "",
    task_group_id: object = "",
    cron_id: object = "",
) -> int:
    """Cancel matching in-flight manual cron runs.

    Dashboard task rows can identify scheduled runs by either the status-card
    group id or the cron id. Keep both paths typed here instead of making the
    UI guess how a background run was registered.
    """

    conversation_key = str(conversation_id or "").strip()
    group_key = str(task_group_id or "").strip()
    cron_key = str(cron_id or "").strip()
    if cron_key.startswith("cron-delivery:"):
        cron_key = cron_key.split(":", 1)[1].strip()
    if not group_key and not cron_key:
        return 0

    with _MANUAL_CRON_BACKGROUND_RUNS_LOCK:
        matched: list[ManualCronBackgroundRunHandle] = []
        conversations = (
            {conversation_key: _MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION.get(conversation_key, {})}
            if conversation_key
            else dict(_MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION)
        )
        for current_conversation, runs in conversations.items():
            for current_group, handle in tuple(runs.items()):
                group_matches = bool(group_key) and (current_group == group_key or handle.task_group_id == group_key)
                cron_matches = bool(cron_key) and handle.job_id == cron_key
                if not group_matches and not cron_matches:
                    continue
                matched.append(handle)
                live_runs = _MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION.get(current_conversation)
                if live_runs is not None:
                    live_runs.pop(current_group, None)
                    if not live_runs:
                        _MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION.pop(current_conversation, None)

    cancelled = 0
    for handle in matched:
        try:
            if handle.cancel():
                cancelled += 1
        except Exception:
            logger.debug("Unable to cancel manual cron background run %s", handle.task_group_id, exc_info=True)
    return cancelled


def _is_timeout_exception(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    exc_type = type(exc)
    type_name = exc_type.__name__.casefold()
    module_name = exc_type.__module__.casefold()
    return "timeout" in type_name and (
        module_name.startswith("httpx")
        or module_name.startswith("httpcore")
        or module_name.startswith("openai")
        or module_name.startswith("anthropic")
    )


def _cron_agent_exception_result(exc: BaseException) -> dict[str, object]:
    error_text = " ".join(str(exc).strip().split()) or exc.__class__.__name__
    if _is_timeout_exception(exc):
        return {
            "text": (
                "The scheduled task timed out while waiting for the model/provider response. "
                "No final result was delivered."
            ),
            "tool_results": [],
            "artifacts": [],
            "cron_run_failed": True,
            "cron_delivery_failed": True,
            "reason": "cron_run_model_timeout",
            "error": error_text,
        }
    return {
        "text": "The scheduled task could not complete. Please try again or check the task configuration.",
        "tool_results": [],
        "artifacts": [],
        "cron_run_failed": True,
        "cron_delivery_failed": True,
        "reason": "cron_run_exception",
        "error": error_text,
    }


def _coerce_utc_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def record_interrupted_cron_delivery_runs(
    store: object,
    *,
    actor: str = "cron_scheduler",
    now: datetime | None = None,
) -> int:
    """Mark non-terminal cron delivery runs interrupted by a scheduler restart."""

    list_events = getattr(store, "list_events", None)
    add_event = getattr(store, "add_event", None)
    if not callable(list_events) or not callable(add_event):
        return 0
    try:
        events = list(list_events() or [])
    except Exception:
        return 0
    if not events:
        return 0

    observed_at = now or datetime.now(UTC)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    else:
        observed_at = observed_at.astimezone(UTC)

    latest_by_cron: dict[str, tuple[str, datetime | None, dict[str, object]]] = {}
    for event in reversed(events):
        event_type = str(getattr(event, "event_type", "") or "").strip()
        if not event_type.startswith("cron.delivery."):
            continue
        payload = getattr(event, "payload", None)
        if not isinstance(payload, dict):
            continue
        cron_id = str(payload.get("cron_id") or "").strip()
        if not cron_id or cron_id in latest_by_cron:
            continue
        latest_by_cron[cron_id] = (
            event_type,
            _coerce_utc_datetime(getattr(event, "created_at", None)),
            dict(payload),
        )

    if not latest_by_cron:
        return 0

    from nullion.events import make_event

    recorded = 0
    for cron_id, (event_type, created_at, payload) in latest_by_cron.items():
        if event_type in _CRON_RESTART_TERMINAL_EVENT_TYPES:
            continue
        if event_type not in _CRON_RESTART_ACTIVE_EVENT_TYPES:
            continue
        if created_at is not None and created_at > observed_at:
            continue
        failure_payload = {
            **payload,
            "cron_id": cron_id,
            "reason": "cron_run_interrupted_by_runtime_restart",
            "error": "Scheduled task was interrupted by runtime restart before terminal delivery.",
            "interrupted_at": observed_at.isoformat(),
            "previous_event_type": event_type,
        }
        try:
            add_event(make_event("cron.delivery.failed", actor, failure_payload))
            recorded += 1
        except Exception:
            logger.debug("Could not record interrupted cron delivery for %s", cron_id, exc_info=True)
    return recorded


def _manual_cron_failure_detail(reason: str, error: str) -> str:
    if reason == "cron_run_model_timeout":
        return "  Reason: Model/provider response timed out before the scheduled task finished."
    if reason == "cron_run_reached_iteration_limit":
        return "  Reason: Scheduled task stopped before producing a deliverable result."
    if reason == "cron_run_waiting_for_approval" or reason == "approval_required":
        return "  Reason: Approval is required before the scheduled task can continue."
    if reason == "cron_run_raw_tool_payload":
        return "  Reason: Scheduled task produced raw tool output instead of a deliverable report."
    if reason == "cron_run_internal_tool_output_leaked":
        return "  Reason: Scheduled task tried to deliver internal tool reference content."
    if reason == "cron_run_malformed_delivery_text":
        return "  Reason: Scheduled task produced malformed internal text instead of a deliverable report."
    if reason == "cron_run_without_completed_tool_evidence":
        return "  Reason: Scheduled task did not complete a data-gathering tool before producing a result."
    if reason == "cron_run_account_connection_unavailable":
        return f"  Reason: {error or 'Account connection is unavailable. Reconnect or update the connection, then try again.'}."
    if error and error != reason:
        return f"  Reason: {error}."
    if reason:
        return f"  Reason: {reason}."
    return "  The run ended before a result could be delivered."
_CRON_SENSITIVE_ACCOUNT_TOOLS = frozenset(
    {
        "calendar_read",
        "calendar_search",
        "connector_request",
        "email_read",
        "email_search",
        "google_calendar_read",
        "google_calendar_search",
        "google_mail_read",
        "google_mail_search",
    }
)
_CRON_SENSITIVE_BODY_KEYS = frozenset(
    {
        "body",
        "content",
        "full_body",
        "full_text",
        "html",
        "html_body",
        "raw",
        "raw_body",
        "text",
    }
)
_CRON_SENSITIVE_METADATA_KEYS = frozenset(
    {
        "date",
        "from",
        "sender",
        "subject",
        "to",
    }
)
_CRON_ACCOUNT_SUMMARY_TITLE_KEYS = frozenset({"name", "subject", "title"})
_CRON_ACCOUNT_SUMMARY_SOURCE_KEYS = frozenset({"from", "sender", "source"})
_CRON_ACCOUNT_SUMMARY_DATE_KEYS = frozenset({"created_at", "date", "received_at", "updated_at"})
_CRON_ACCOUNT_SUMMARY_DETAIL_KEYS = frozenset({"description", "preview", "snippet", "summary"})
_CRON_ACCOUNT_SUMMARY_MAX_ITEMS = 6
_CRON_ACCOUNT_SUMMARY_FIELD_MAX_CHARS = 180
_CRON_ACCOUNT_SUMMARY_DETAIL_MAX_CHARS = 160
_CRON_SENSITIVE_BODY_MIN_CHARS = 180
_CRON_SENSITIVE_BODY_MATCH_CHARS = 220
_CRON_SENSITIVE_METADATA_MIN_CHARS = 8
_CRON_SENSITIVE_METADATA_MATCH_MIN_COUNT = 2

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


def normalize_html_image_delivery_mode(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {
        HTML_IMAGE_DELIVERY_MODE_LINKED,
        HTML_IMAGE_DELIVERY_MODE_AUTO,
        HTML_IMAGE_DELIVERY_MODE_SELF_CONTAINED,
    }:
        return normalized
    return HTML_IMAGE_DELIVERY_MODE_AUTO


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
        "- For document-like deliverables such as PDF, DOCX, PPTX, reports, itineraries, and decks, use "
        "structured title/sections/slides/text pages so the artifact tool can produce a readable report-quality "
        "layout; do not deliver raw browser screenshots, loose image attachments, or unformatted text dumps as "
        "a substitute for the requested formatted document.\n"
        "- Generated artifacts must be internally consistent and valid for their file type before delivery: "
        "JSON must parse, text must be non-empty, HTML reports must not duplicate or contradict their own "
        "visible counts, and binary formats must be real files.\n"
        "- For HTML dashboards/reports, derive visible metrics, charts, and tables from the same source rows "
        "instead of manually repeating totals.\n"
        "- HTML report primary content must be present in static markup. Do not require client-side JavaScript "
        "to populate the visible tables, cards, rows, or summary a recipient needs to read.\n"
        "- Browser evidence and screenshots must come from this scheduled-task run. If current-run browser "
        "navigation or extraction fails, use text placeholders or an explicit insufficiency note instead of "
        "embedding unrelated workspace screenshots.\n"
        "- If a browser tool returns browser_connection_notice, include that notice in the final cron message "
        "or report and do not imply that authenticated browser state was available.\n"
        "- User-facing reminders, reports, and alerts are delivered by the scheduler through this task's "
        "configured delivery channel. Do not invoke account-write tools merely to deliver the scheduled-task output.\n"
        "- Keep scratch/checkpoint/state files in the workspace unless they are requested deliverables.\n"
        "- For email, calendar, and account connector results, deliver concise extracted obligations, confirmations, "
        "dates, and actions. Do not paste full message bodies or raw connector payloads.\n"
        "- If the task says to alert only on new data or meaningful changes, return no output when nothing changed.\n"
        f"- If no output behavior is specified and there is nothing specific to report, send: {DEFAULT_CRON_NO_OUTPUT_MESSAGE}"
    )


def cron_agent_history(
    runtime: object,
    settings: object,
    *,
    model_client: object,
    principal_id: str,
    tool_registry: object,
    include_connector_context: bool,
    include_structured_connection_context: bool = False,
) -> list[dict[str, object]]:
    """Build the single cron-agent context shared by every delivery surface."""

    from nullion.artifacts import artifact_root_for_principal
    from nullion.builder_capabilities import format_installed_dependency_context
    from nullion.connections import format_workspace_connections_for_prompt
    from nullion.runtime_config import format_runtime_config_for_prompt
    from nullion.skill_pack_catalog import skill_pack_access_prompt
    from nullion.skill_pack_installer import format_cached_enabled_skill_pack_index_for_prompt
    from nullion.system_context import build_system_context_snapshot, format_compact_system_context_for_prompt
    from nullion.workspace_storage import format_workspace_storage_for_prompt

    history: list[dict[str, object]] = []
    caps_text = format_compact_system_context_for_prompt(build_system_context_snapshot(tool_registry=tool_registry))
    dependency_text = format_installed_dependency_context(runtime)
    if dependency_text:
        caps_text = (caps_text + "\n\n" + dependency_text).strip()
    if caps_text:
        history.append({
            "role": "system",
            "content": [{"type": "text", "text": (
                "You are Nullion, a security-first AI agent. Use only registered tools. "
                "Below is the live inventory of tools registered in this session.\n\n"
                + caps_text
            )}],
        })

    config_text = format_runtime_config_for_prompt(model_client=model_client)
    if config_text:
        history.append({"role": "system", "content": [{"type": "text", "text": config_text}]})

    connections_text = format_workspace_connections_for_prompt(
        principal_id=principal_id,
        include_external_connectors=include_connector_context or include_structured_connection_context,
    )
    if connections_text:
        history.append({"role": "system", "content": [{"type": "text", "text": connections_text}]})

    enabled_skill_packs = tuple(getattr(settings, "enabled_skill_packs", ()) or ())
    skill_text = format_cached_enabled_skill_pack_index_for_prompt(
        enabled_skill_packs,
        max_total_chars=900,
    )
    access_text = (
        skill_pack_access_prompt(enabled_skill_packs, principal_id=principal_id, compact=True)
        if include_connector_context
        else ""
    )
    if access_text:
        skill_text = (skill_text + "\n\n" + access_text).strip()
    if skill_text:
        history.append({"role": "system", "content": [{"type": "text", "text": skill_text}]})

    artifact_root = artifact_root_for_principal(principal_id)
    storage_text = format_workspace_storage_for_prompt(principal_id=principal_id)
    history.append({
        "role": "system",
        "content": [{"type": "text", "text": (
            "Cron delivery contract: create requested deliverable files under this artifact directory "
            f"and attach them with explicit MEDIA lines: {artifact_root}. "
            "For document-like deliverables, produce structured report-quality artifacts rather than raw screenshots, "
            "loose images, or unformatted text dumps. Keep scratch, checkpoint, and state files in the workspace "
            "unless they are requested deliverables.\n\n"
            "If a tool requires approval, pause and wait for the approval decision card. "
            "After approval, resume from the suspended step; if denied, stop the run.\n\n"
            f"{storage_text}"
        )}],
    })
    return history


def run_single_agent_cron_turn(
    job: object,
    conversation_id: str,
    *,
    label: str,
    orchestrator: object,
    runtime: object,
    tool_registry: object,
    settings: object,
    model_client: object | None = None,
    record_event: Callable[..., None] | None = None,
    tool_result_callback: Callable[[object], None] | None = None,
    turn_guard: Callable[[], object] | None = None,
    cancellation_checker: Callable[[], bool] | None = None,
) -> dict[str, object]:
    """Run a scheduled task with exactly one agent, regardless of chat platform."""

    from nullion.cron_execution_tools import (
        CronExecutionToolRegistry,
        build_cron_connector_scope_decision,
        structured_cron_passthrough_tool_names,
    )

    if orchestrator is None or not hasattr(orchestrator, "run_turn"):
        return {
            "cron_run_failed": True,
            "cron_delivery_failed": True,
            "reason": "agent_orchestrator_unavailable",
            "text": "The scheduled task could not complete because the agent runtime is unavailable.",
            "tool_results": [],
            "artifacts": [],
        }

    started_at = time.perf_counter()

    def _record_preflight(stage: str, **extra: object) -> None:
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 1)
        logger.info(
            "cron single-agent preflight conversation_id=%s stage=%s elapsed_ms=%.1f extra=%s",
            conversation_id,
            stage,
            elapsed_ms,
            extra,
        )
        if record_event is None:
            return
        try:
            record_event(
                "cron.delivery.agent_preflight",
                job,
                "",
                "",
                conversation_id,
                stage=stage,
                elapsed_ms=elapsed_ms,
                **extra,
            )
        except Exception:
            logger.debug("Could not record cron single-agent preflight event", exc_info=True)

    prompt = cron_agent_prompt(job, label=label)
    connector_scope_prompt = {
        "name": str(getattr(job, "name", "") or ""),
        "task": str(getattr(job, "task", "") or ""),
    }
    active_model_client = model_client if model_client is not None else getattr(orchestrator, "model_client", None)
    _record_preflight("started", execution_mode="single_agent")
    connector_scope = build_cron_connector_scope_decision(
        model_client=active_model_client,
        user_message=json.dumps(connector_scope_prompt, ensure_ascii=False, sort_keys=True),
        principal_id=conversation_id,
        registry=tool_registry,
        planned_tool_names=None,
    )
    structured_tool_names = structured_cron_passthrough_tool_names(tool_registry)
    _record_preflight(
        "connector_scope",
        connector_allowed=bool(connector_scope.allow_connector_tools),
        connector_providers=len(connector_scope.provider_ids),
        structured_passthrough_tools=len(structured_tool_names),
    )
    execution_registry = CronExecutionToolRegistry(
        tool_registry,
        allowed_tool_names=None,
        allowed_structured_tool_names=structured_tool_names,
        allow_connector_tools=connector_scope.allow_connector_tools,
        connector_provider_ids=connector_scope.provider_ids,
    )
    guard = turn_guard() if turn_guard is not None else nullcontext()
    browser_session_scope = f"cron:{getattr(job, 'id', '')}:{uuid4().hex}"
    try:
        with guard:
            result = orchestrator.run_turn(
                conversation_id=conversation_id,
                principal_id=conversation_id,
                user_message=prompt,
                conversation_history=cron_agent_history(
                    runtime,
                    settings,
                    model_client=active_model_client,
                    principal_id=conversation_id,
                    tool_registry=execution_registry,
                    include_connector_context=connector_scope.allow_connector_tools,
                    include_structured_connection_context=bool(structured_tool_names),
                ),
                tool_registry=execution_registry,
                policy_store=getattr(runtime, "store", None),
                approval_store=getattr(runtime, "store", None),
                tool_result_callback=tool_result_callback,
                cancellation_checker=cancellation_checker,
                tool_flow_context={
                    "scheduled_task_run": True,
                    "isolated_browser_session": True,
                    "browser_session_scope": browser_session_scope,
                    "cron_id": str(getattr(job, "id", "") or ""),
                    "cron_name": str(getattr(job, "name", "") or ""),
                },
            )
    except BaseException as exc:
        error_text = " ".join(str(exc).strip().split()) or exc.__class__.__name__
        failure_result = _cron_agent_exception_result(exc)
        logger.warning(
            "Cron single-agent turn failed before terminal delivery [cron_id=%s]: %s",
            getattr(job, "id", ""),
            error_text,
            exc_info=True,
        )
        _record_preflight(
            "single_agent_failed",
            reason=str(failure_result.get("reason") or ""),
            error=error_text,
        )
        return failure_result

    _record_preflight(
        "single_agent_done",
        tool_results=len(getattr(result, "tool_results", ()) or ()),
        artifacts=len(getattr(result, "artifacts", ()) or ()),
    )
    return {
        "text": getattr(result, "final_text", "") or "",
        "tool_results": list(getattr(result, "tool_results", ()) or ()),
        "artifacts": list(getattr(result, "artifacts", ()) or ()),
        "suspended_for_approval": bool(getattr(result, "suspended_for_approval", False)),
        "approval_id": getattr(result, "approval_id", None),
        "reached_iteration_limit": bool(getattr(result, "reached_iteration_limit", False)),
        "raw_tool_payload_blocked": bool(getattr(result, "raw_tool_payload_blocked", False)),
        "cron_execution_mode": "single_agent",
    }


def manual_cron_silent_delivery_text(
    job: object,
    label: str,
    result: dict[str, object] | None = None,
) -> str:
    """Return the visible terminal message for a manually triggered no-output cron."""
    _ = (job, label, result)
    return "There was nothing new to report."


def manual_cron_deferred_receipt() -> dict[str, object]:
    """Return the shared immediate receipt for a manually triggered cron run."""

    return {
        "cron_delivery_status": "deferred",
        "delivery_status": "deferred",
        "text": (
            "Manual scheduled task run started. The result will be delivered when ready."
        ),
        "status_delivered": False,
    }


def manual_cron_status_group_id(job: object, *, run_id: object = "") -> str:
    """Return a stable status-card id for one manual cron invocation."""

    cron_id = re.sub(r"[^A-Za-z0-9_.:-]+", "-", str(getattr(job, "id", "") or "cron")).strip("-")
    run_text = re.sub(r"[^A-Za-z0-9_.:-]+", "-", str(run_id or "")).strip("-")
    if run_text:
        return f"manual-cron-{cron_id}-{run_text}"
    return f"manual-cron-{cron_id}-{uuid4().hex[:8]}"


def manual_cron_running_status_text(job: object, *, label: str = "Manual scheduled task run") -> str:
    """Return the compact visible status card for a deferred manual cron run."""

    name = str(getattr(job, "name", "") or "scheduled task").strip() or "scheduled task"
    return "\n".join(
        [
            SCHEDULED_TASK_STATUS_TITLE,
            f"For: {name}",
            f"◐ Running: {label} started.",
            "  Result will be delivered to this chat when ready.",
        ]
    )


def manual_cron_terminal_status_text(
    job: object,
    *,
    label: str = "Manual scheduled task run",
    result: dict[str, object] | None = None,
    error: object | None = None,
) -> str:
    """Return the terminal status-card text for a manual cron run."""

    name = str(getattr(job, "name", "") or "scheduled task").strip() or "scheduled task"
    result_payload = result if isinstance(result, dict) else {}
    status = str(result_payload.get("cron_delivery_status") or result_payload.get("delivery_status") or "").strip()
    reason = str(result_payload.get("reason") or result_payload.get("error") or "").strip()
    failed = bool(
        error is not None
        or result_payload.get("cron_delivery_failed")
        or result_payload.get("cron_run_failed")
        or status == "failed"
    )
    if result_payload.get("cron_run_cancelled") or status == "cancelled":
        row = f"✕ Stopped: {label} stopped."
        detail = "  Stopped by /stop."
    elif failed:
        detail = _manual_cron_failure_detail(reason, str(result_payload.get("error") or ""))
        row = f"! Failed: {label} could not complete."
    elif status == "silent" or result_payload.get("cron_silent_result_replaced"):
        row = f"✓ Completed: {label} finished."
        detail = "  There was nothing new to report."
    elif status == "saved":
        row = f"✓ Completed: {label} finished."
        detail = "  Result saved to this chat."
    elif status == "partial_success":
        row = f"✓ Completed: {label} finished with a delivery fallback."
        detail = "  Result text was delivered; some attachments may need review."
    elif status == "paused_for_approval":
        row = f"▤ Waiting: {label} needs approval."
        approval_id = str(result_payload.get("approval_id") or "").strip()
        approval_hint = f" Approval ID: {approval_id[:8]}." if approval_id else ""
        detail = f"  Open the approval request to continue.{approval_hint}"
    else:
        row = f"✓ Completed: {label} finished."
        detail = "  Result delivered to this chat."
    return "\n".join([SCHEDULED_TASK_STATUS_TITLE, f"For: {name}", row, detail])


def start_manual_cron_background_delivery(
    job: object,
    *,
    label: str,
    callbacks: "CronRunDeliveryCallbacks",
    origin_conversation_id: object = "",
    thread_name_prefix: str = "nullion-manual-cron",
    before_run: Callable[[], Callable[[], None] | None] | None = None,
    workflow_runner: Callable[..., dict[str, object]] | None = None,
    status_update_callback: Callable[[str, str, bool], object] | None = None,
    status_update_interval_seconds: float = 1.5,
) -> dict[str, object]:
    """Start a manual cron run once and return the platform-agnostic receipt."""

    task_group_id = manual_cron_status_group_id(job)
    running_status_text = manual_cron_running_status_text(job, label=label)
    cancel_event = threading.Event()
    completed_event = threading.Event()
    status_stop = threading.Event()
    status_terminal = threading.Event()
    terminal_status_delivered = threading.Event()
    status_animation_started = threading.Event()

    def _emit_status_update(status_text: str, *, terminal: bool) -> bool:
        if status_update_callback is None:
            return False
        if terminal:
            status_terminal.set()
        elif status_terminal.is_set():
            return False
        try:
            delivered = status_update_callback(task_group_id, status_text, terminal)
            if isinstance(delivered, bool):
                return delivered
            return True
        except Exception:
            logger.debug("Could not deliver manual cron status update", exc_info=True)
            return False

    def _emit_terminal_status(status_text: str) -> None:
        if terminal_status_delivered.is_set():
            return
        terminal_status_delivered.set()
        _emit_status_update(status_text, terminal=True)

    def _cancel_background_run() -> bool:
        if completed_event.is_set() or cancel_event.is_set():
            return False
        cancel_event.set()
        status_stop.set()
        _emit_terminal_status(
            manual_cron_terminal_status_text(
                job,
                label=label,
                result={"cron_delivery_status": "cancelled", "cron_run_cancelled": True},
            )
        )
        return True

    unregister_background_run = register_manual_cron_background_run(
        origin_conversation_id,
        task_group_id,
        job,
        _cancel_background_run,
    )

    def _status_animation_loop() -> None:
        interval = max(float(status_update_interval_seconds or 0), 0.0)
        if interval <= 0:
            return
        while not status_stop.wait(interval):
            if status_terminal.is_set():
                return
            _emit_status_update(running_status_text, terminal=False)

    def _start_status_animation_loop() -> None:
        if status_update_callback is None or status_animation_started.is_set():
            return
        status_animation_started.set()
        status_thread = threading.Thread(
            target=_status_animation_loop,
            name=f"{thread_name_prefix}-status-{getattr(job, 'id', 'run')}",
            daemon=True,
        )
        status_thread.start()

    def _background_manual_cron_run() -> None:
        cleanup: Callable[[], None] | None = None
        result: dict[str, object] | None = None
        error: BaseException | None = None
        serial_lock = _manual_cron_background_serial_lock(origin_conversation_id)

        def _run_once() -> None:
            nonlocal cleanup, result, error
            if cancel_event.is_set():
                result = {"cron_delivery_status": "cancelled", "cron_run_cancelled": True}
                return
            _start_status_animation_loop()
            if before_run is not None:
                cleanup = before_run()
            runner = workflow_runner or run_cron_delivery_workflow
            runner_kwargs: dict[str, object] = {"label": label, "callbacks": callbacks}
            try:
                runner_signature = inspect.signature(runner)
            except (TypeError, ValueError):
                runner_signature = None
            if runner_signature is not None and "cancellation_checker" in runner_signature.parameters:
                runner_kwargs["cancellation_checker"] = cancel_event.is_set
            result = runner(job, **runner_kwargs)
            if isinstance(result, dict) and (result.get("cron_delivery_failed") or result.get("cron_run_failed")):
                logger.warning(
                    "Manual cron background run failed [%s]: %s",
                    getattr(job, "id", ""),
                    result.get("reason") or result,
                )

        try:
            if serial_lock is None:
                _run_once()
            else:
                with serial_lock:
                    _run_once()
        except Exception as exc:
            error = exc
            logger.warning("Manual cron background run error [%s]", getattr(job, "id", ""), exc_info=True)
        finally:
            if cancel_event.is_set() and result is None and error is None:
                result = {"cron_delivery_status": "cancelled", "cron_run_cancelled": True}
            completed_event.set()
            status_terminal.set()
            status_stop.set()
            _emit_terminal_status(
                manual_cron_terminal_status_text(job, label=label, result=result, error=error),
            )
            if cleanup is not None:
                try:
                    cleanup()
                except Exception:
                    logger.debug("Could not clean up manual cron background hooks", exc_info=True)
            unregister_background_run()

    if status_update_callback is not None:
        initial_status_delivered = _emit_status_update(running_status_text, terminal=False)
    else:
        initial_status_delivered = False

    thread = threading.Thread(
        target=_background_manual_cron_run,
        name=f"{thread_name_prefix}-{getattr(job, 'id', 'run')}",
        daemon=True,
    )
    thread.start()
    receipt = manual_cron_deferred_receipt()
    receipt["task_group_id"] = task_group_id
    receipt["progress_status_text"] = running_status_text
    receipt["status_delivered"] = bool(initial_status_delivered)
    if initial_status_delivered:
        receipt["planner_status_owned_by_background"] = True
        receipt["foreground_reply_suppressed"] = True
    return receipt


def clear_cron_execution_metadata_caches() -> None:
    """Preserved registry hook; cron execution no longer caches dispatch DAGs."""


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


def _cron_title_is_generated_delivery_header(title: str, job_name: str) -> bool:
    _label, separator, remainder = str(title or "").partition(":")
    if not separator:
        return False
    return _cron_title_is_redundant_with_job(remainder.strip(), job_name)


def _strip_leading_cron_report_heading(body: str, job_name: str) -> tuple[str, str]:
    body = str(body or "").strip()
    prefix = next((candidate for candidate in CRON_DELIVERY_REPLY_PREFIXES if body.startswith(candidate)), "")
    if not prefix:
        return "", body
    first_line, _, rest = body.partition("\n")
    title = first_line[len(prefix):].strip()
    body_without_first_clock = "\n".join(
        part for part in (title, rest.strip()) if part
    )
    timestamp_suffix = _cron_title_timestamp_suffix(title)
    if timestamp_suffix:
        return timestamp_suffix, rest.strip()
    if _cron_title_is_generated_delivery_header(title, job_name):
        return "", rest.strip()
    rest_body = rest.strip()
    second_prefix = next((candidate for candidate in CRON_DELIVERY_REPLY_PREFIXES if rest_body.startswith(candidate)), "")
    if not second_prefix:
        return "", rest_body if _cron_title_is_redundant_with_job(title, job_name) else body_without_first_clock
    second_line, _, second_rest = rest_body.partition("\n")
    second_title = second_line[len(second_prefix):].strip()
    timestamp_suffix = _cron_title_timestamp_suffix(second_title)
    if not timestamp_suffix:
        return "", body_without_first_clock
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
    header = f"⏱️ {label.upper()}: {name}"
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
    if any(stripped.startswith(prefix.strip()) for prefix in CRON_DELIVERY_REPLY_PREFIXES):
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


def _tool_result_error(result: object) -> str:
    if isinstance(result, dict):
        return str(result.get("error") or "").strip()
    return str(getattr(result, "error", "") or "").strip()


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
        tool_name = _tool_result_name(tool_result)
        output = _tool_result_output(tool_result)
        if tool_name in CRON_INTERNAL_REFERENCE_TOOLS:
            for key in ("text", "message"):
                output_text = str(output.get(key) or "").strip()
                if output_text and (visible_text == output_text or output_text in visible_text):
                    return True
        receipt = output.get("action_receipt")
        if not isinstance(receipt, dict):
            continue
        for key in ("summary", "message", "text"):
            receipt_text = str(receipt.get(key) or "").strip()
            if receipt_text and (visible_text == receipt_text or receipt_text in visible_text):
                return True
        details = receipt.get("details")
        if isinstance(details, list):
            for detail in details:
                receipt_text = str(detail or "").strip()
                if receipt_text and (visible_text == receipt_text or receipt_text in visible_text):
                    return True
    return False


def _cron_result_has_empty_scope_request(result: dict[str, object]) -> bool:
    """Return True when the only live evidence is a failed scope-widening request."""
    for tool_result in result.get("tool_results") or ():
        if _normalized_tool_result_status(tool_result) != "completed":
            continue
        if _tool_result_name(tool_result) != "request_tool_scope":
            continue
        output = _tool_result_output(tool_result)
        if output.get("scope_requested") is not True:
            continue
        raw_tools = output.get("available_tools")
        if isinstance(raw_tools, list) and not raw_tools:
            return True
    return False


def _cron_result_account_connection_failure(result: dict[str, object]) -> str | None:
    for tool_result in result.get("tool_results") or ():
        tool_name = _tool_result_name(tool_result)
        if tool_name not in _CRON_SENSITIVE_ACCOUNT_TOOLS:
            continue
        output = _tool_result_output(tool_result)
        error = _tool_result_error(tool_result)
        result_text = str(output.get("result_text") or "").strip()
        if output.get("terminal_user_action_required") is True and result_text:
            return result_text
        connection_state = str(output.get("connection_state") or "").strip()
        if connection_state in {"pending_or_failed", "missing_credential", "unavailable"}:
            connector_app = str(output.get("connector_app_id") or output.get("app") or "account").strip()
            return f"{connector_app} connection is unavailable. Reconnect or update the connection, then try again."
        if " requires " in error and ("_TOKEN" in error or "_API_KEY" in error or "_BASE_URL" in error):
            provider = tool_name
            provider_id = output.get("provider_id")
            if isinstance(provider_id, str) and provider_id.strip():
                provider = provider_id.strip()
            else:
                provider = error.split(" requires ", 1)[0].strip() or provider
            return f"{provider} is missing required connection configuration: {error}"
    return None


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
    if result.get("cron_run_failed") or result.get("cron_delivery_failed"):
        return str(result.get("reason") or "cron_run_failed")
    if result.get("response_fulfilled") is False:
        return "cron_run_unfulfilled_delivery_contract"
    if is_safe_raw_tool_payload_replacement_reply(reply=text, tool_results=result.get("tool_results") or ()):
        return "cron_run_raw_tool_payload"
    if _cron_result_leaked_internal_tool_output(result, text):
        return "cron_run_internal_tool_output_leaked"
    if _cron_delivery_text_is_malformed_or_internal(text):
        return "cron_run_malformed_delivery_text"
    if _cron_result_has_empty_scope_request(result):
        return "cron_run_tool_scope_unavailable"
    if _cron_result_has_internal_capability_denial(result):
        return "cron_run_denied_internal_capability"
    account_connection_failure = _cron_result_account_connection_failure(result)
    if account_connection_failure:
        result["error"] = account_connection_failure
        return "cron_run_account_connection_unavailable"
    if (
        result.get("tool_results")
        and not artifacts
        and not _cron_result_has_completed_tool_evidence(result)
    ):
        return "cron_run_without_completed_tool_evidence"
    return None


def _cron_delivery_text_is_malformed_or_internal(text: str | None) -> bool:
    raw = str(text or "")
    stripped = raw.strip()
    if not stripped:
        return False
    if _CRON_INTERNAL_PREVIEW_SCHEMA_RE.search(stripped):
        return True
    replacement_count = stripped.count("\ufffd")
    if replacement_count >= 2:
        return True
    if replacement_count and (
        _CRON_SPACED_TOKEN_RE.search(stripped)
        or _CRON_INTERNAL_UUID_TOKEN_RE.search(stripped)
    ):
        return True
    if _CRON_INTERNAL_UUID_TOKEN_RE.search(stripped) and _CRON_SPACED_TOKEN_RE.search(stripped):
        return True
    return False


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


def _normalize_html_image_delivery_mode(value: object) -> str:
    return normalize_html_image_delivery_mode(value)


def _cron_html_image_delivery_mode(job: object, result: dict[str, object]) -> str:
    options = result.get("artifact_delivery_options")
    if isinstance(options, dict):
        option_mode = options.get("html_image_mode")
        if option_mode is not None:
            return _normalize_html_image_delivery_mode(option_mode)
        nested = options.get("html")
        if isinstance(nested, dict) and nested.get("image_mode") is not None:
            return _normalize_html_image_delivery_mode(nested.get("image_mode"))
    job_options = getattr(job, "artifact_delivery_options", None)
    if isinstance(job_options, dict):
        option_mode = job_options.get("html_image_mode")
        if option_mode is not None:
            return _normalize_html_image_delivery_mode(option_mode)
        nested = job_options.get("html")
        if isinstance(nested, dict) and nested.get("image_mode") is not None:
            return _normalize_html_image_delivery_mode(nested.get("image_mode"))
    return _normalize_html_image_delivery_mode(getattr(job, "html_image_delivery_mode", None))


def _is_remote_http_asset_src(src: str) -> bool:
    parsed = urlsplit(str(src or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _inline_placeholder_image_data_uri() -> str:
    placeholder_svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' width='640' height='360' viewBox='0 0 640 360'>"
        "<rect width='640' height='360' fill='#111827'/>"
        "<text x='50%' y='46%' dominant-baseline='middle' text-anchor='middle' "
        "fill='#d1d5db' font-family='Arial,Helvetica,sans-serif' font-size='20'>Image unavailable</text>"
        "<text x='50%' y='56%' dominant-baseline='middle' text-anchor='middle' "
        "fill='#9ca3af' font-family='Arial,Helvetica,sans-serif' font-size='13'>Rendered as self-contained fallback</text>"
        "</svg>"
    )
    encoded = base64.b64encode(placeholder_svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


_INLINE_IMAGE_UNAVAILABLE_DATA_URI = _inline_placeholder_image_data_uri()


def _inline_html_remote_image_asset_data_uri(src: str) -> str | None:
    src_text = str(src or "").strip()
    if not _is_remote_http_asset_src(src_text):
        return None
    request = Request(
        src_text,
        headers={
            "User-Agent": _HTML_INLINE_REMOTE_IMAGE_USER_AGENT,
            "Accept": "image/*,*/*;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=_HTML_INLINE_REMOTE_IMAGE_TIMEOUT_SECONDS) as response:
            declared_size_text = str(response.headers.get("Content-Length") or "").strip()
            if declared_size_text.isdigit() and int(declared_size_text) > _HTML_INLINE_REMOTE_IMAGE_MAX_BYTES:
                return None
            content_type = str(response.headers.get_content_type() or "").strip().lower()
            if not content_type.startswith("image/"):
                guessed_type = mimetypes.guess_type(urlsplit(src_text).path)[0] or ""
                content_type = guessed_type.lower()
            if not content_type.startswith("image/"):
                return None
            raw = response.read(_HTML_INLINE_REMOTE_IMAGE_MAX_BYTES + 1)
    except (OSError, ValueError, URLError):
        return None
    if not raw or len(raw) > _HTML_INLINE_REMOTE_IMAGE_MAX_BYTES:
        return None
    encoded = base64.b64encode(raw).decode("ascii")
    return f"data:{content_type};base64,{encoded}"


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


def _inline_html_local_image_assets(
    html_path_text: str,
    *,
    html_image_delivery_mode: str,
) -> set[str]:
    html_path = Path(str(html_path_text or "")).expanduser()
    if html_path.suffix.lower() not in {".html", ".htm"}:
        return set()
    try:
        original = html_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return set()
    support_assets: set[str] = set()
    remote_image_cache: dict[str, str | None] = {}
    remote_attempt_count = 0
    changed = False
    normalized_mode = _normalize_html_image_delivery_mode(html_image_delivery_mode)
    inline_remote_images = normalized_mode in {
        HTML_IMAGE_DELIVERY_MODE_AUTO,
        HTML_IMAGE_DELIVERY_MODE_SELF_CONTAINED,
    }
    replace_remote_with_placeholder = normalized_mode == HTML_IMAGE_DELIVERY_MODE_SELF_CONTAINED

    def replace(match: re.Match[str]) -> str:
        nonlocal changed, remote_attempt_count
        src = match.group("src")
        asset = _resolve_html_local_image_asset(html_path, src)
        if asset is not None:
            try:
                raw = asset.read_bytes()
            except OSError:
                raw = b""
            if raw:
                mime_type = mimetypes.guess_type(str(asset))[0] or "application/octet-stream"
                encoded = base64.b64encode(raw).decode("ascii")
                support_assets.add(str(asset))
                changed = True
                return (
                    f"{match.group('prefix')}{match.group('quote')}"
                    f"data:{mime_type};base64,{encoded}{match.group('quote')}"
                )
        if inline_remote_images:
            # Optional policy for truly self-contained HTML delivery.
            remote_data_uri = remote_image_cache.get(src)
            if src not in remote_image_cache:
                if (
                    normalized_mode == HTML_IMAGE_DELIVERY_MODE_AUTO
                    and remote_attempt_count >= _HTML_AUTO_INLINE_REMOTE_IMAGE_MAX_ATTEMPTS
                ):
                    remote_data_uri = None
                else:
                    remote_attempt_count += 1
                    remote_data_uri = _inline_html_remote_image_asset_data_uri(src)
                remote_image_cache[src] = remote_data_uri
            if remote_data_uri:
                changed = True
                return f"{match.group('prefix')}{match.group('quote')}{remote_data_uri}{match.group('quote')}"
            if replace_remote_with_placeholder and _is_remote_http_asset_src(src):
                # Keep layout stable without external dependencies when fetching fails.
                changed = True
                return (
                    f"{match.group('prefix')}{match.group('quote')}"
                    f"{_INLINE_IMAGE_UNAVAILABLE_DATA_URI}{match.group('quote')}"
                )
        return match.group(0)

    updated = _HTML_LOCAL_IMAGE_SRC_RE.sub(replace, original)
    if changed and updated != original:
        try:
            html_path.write_text(updated, encoding="utf-8")
        except OSError:
            return set()
    return support_assets


def _prepare_cron_deliverable_paths_for_delivery(
    paths: tuple[str, ...],
    *,
    html_image_delivery_mode: str,
) -> tuple[tuple[str, ...], set[str]]:
    """Make HTML artifacts self-contained and suppress their local support files."""
    unique_paths = tuple(dict.fromkeys(path for path in paths if str(path or "").strip()))
    support_assets: set[str] = set()
    for path in unique_paths:
        support_assets.update(
            _inline_html_local_image_assets(
                path,
                html_image_delivery_mode=html_image_delivery_mode,
            )
        )
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


def _compact_match_text(value: object) -> str:
    return " ".join(str(value or "").split())


def _sensitive_account_tool_body_values(value: object) -> tuple[str, ...]:
    values: list[str] = []

    def visit(node: object, *, parent_key: str = "") -> None:
        if isinstance(node, dict):
            for raw_key, child in node.items():
                key = str(raw_key or "").strip().lower()
                if (
                    key in _CRON_SENSITIVE_BODY_KEYS
                    and isinstance(child, str)
                    and len(_compact_match_text(child)) >= _CRON_SENSITIVE_BODY_MIN_CHARS
                ):
                    values.append(child)
                    continue
                visit(child, parent_key=key)
            return
        if isinstance(node, (list, tuple, set, frozenset)):
            for child in node:
                visit(child, parent_key=parent_key)

    visit(value)
    return tuple(dict.fromkeys(values))


def _sensitive_account_tool_metadata_values(value: object) -> tuple[str, ...]:
    values: list[str] = []

    def visit(node: object) -> None:
        if isinstance(node, dict):
            for raw_key, child in node.items():
                key = str(raw_key or "").strip().lower()
                if key in _CRON_SENSITIVE_METADATA_KEYS and isinstance(child, str):
                    compact = _compact_match_text(child)
                    if len(compact) >= _CRON_SENSITIVE_METADATA_MIN_CHARS:
                        values.append(compact)
                    continue
                visit(child)
            return
        if isinstance(node, (list, tuple, set, frozenset)):
            for child in node:
                visit(child)

    visit(value)
    return tuple(dict.fromkeys(values))


def _cron_sensitive_account_body_leaked(result: dict[str, object], text: str) -> bool:
    visible_text = _compact_match_text(text)
    if len(visible_text) < _CRON_SENSITIVE_BODY_MIN_CHARS:
        return False
    for tool_result in result.get("tool_results") or ():
        if _normalized_tool_result_status(tool_result) != "completed":
            continue
        if _tool_result_name(tool_result) not in _CRON_SENSITIVE_ACCOUNT_TOOLS:
            continue
        candidates = [
            *_sensitive_account_tool_body_values(_tool_result_output(tool_result)),
        ]
        if isinstance(tool_result, dict):
            candidates.extend(_sensitive_account_tool_body_values(tool_result.get("message")))
        else:
            candidates.extend(_sensitive_account_tool_body_values(getattr(tool_result, "message", None)))
        for raw_body in candidates:
            body_text = _compact_match_text(raw_body)
            if len(body_text) < _CRON_SENSITIVE_BODY_MIN_CHARS:
                continue
            if body_text in visible_text:
                return True
            probe = body_text[: min(len(body_text), _CRON_SENSITIVE_BODY_MATCH_CHARS)]
            if len(probe) >= _CRON_SENSITIVE_BODY_MIN_CHARS and probe in visible_text:
                return True
    return False


def _cron_sensitive_account_metadata_copied(result: dict[str, object], text: str) -> bool:
    visible_text = _compact_match_text(text)
    if len(visible_text) < _CRON_SENSITIVE_BODY_MIN_CHARS:
        return False
    visible_lower = visible_text.lower()
    copied_count = 0
    for tool_result in result.get("tool_results") or ():
        if _normalized_tool_result_status(tool_result) != "completed":
            continue
        if _tool_result_name(tool_result) not in _CRON_SENSITIVE_ACCOUNT_TOOLS:
            continue
        candidates = [*_sensitive_account_tool_metadata_values(_tool_result_output(tool_result))]
        if isinstance(tool_result, dict):
            candidates.extend(_sensitive_account_tool_metadata_values(tool_result.get("message")))
        else:
            candidates.extend(_sensitive_account_tool_metadata_values(getattr(tool_result, "message", None)))
        for raw_value in dict.fromkeys(candidates):
            if raw_value.lower() in visible_lower:
                copied_count += 1
                if copied_count >= _CRON_SENSITIVE_METADATA_MATCH_MIN_COUNT:
                    return True
    return False


def _compact_safe_account_summary_value(value: object, *, max_chars: int) -> str:
    compact = html.unescape(_compact_match_text(value)).strip()
    if not compact:
        return ""
    if len(compact) <= max_chars:
        return compact
    return f"{compact[:max_chars].rstrip()}..."


def _account_summary_field_values(record: dict[object, object], keys: frozenset[str]) -> tuple[str, ...]:
    values: list[str] = []

    def visit(node: object) -> None:
        if not isinstance(node, dict):
            return
        for raw_key, child in node.items():
            key = str(raw_key or "").strip().lower()
            if key in keys and isinstance(child, str):
                values.append(child)
                continue
            if key in _CRON_SENSITIVE_BODY_KEYS:
                continue
            if isinstance(child, dict):
                visit(child)

    visit(record)
    return tuple(dict.fromkeys(value for value in values if _compact_match_text(value)))


def _account_summary_first_field(record: dict[object, object], keys: frozenset[str], *, max_chars: int) -> str:
    for value in _account_summary_field_values(record, keys):
        compact = _compact_safe_account_summary_value(value, max_chars=max_chars)
        if compact:
            return compact
    return ""


def _iter_sensitive_account_summary_records(value: object) -> tuple[dict[object, object], ...]:
    records: list[dict[object, object]] = []

    def visit(node: object) -> None:
        if isinstance(node, dict):
            has_summary_field = bool(
                _account_summary_field_values(node, _CRON_ACCOUNT_SUMMARY_TITLE_KEYS)
                or _account_summary_field_values(node, _CRON_ACCOUNT_SUMMARY_SOURCE_KEYS)
                or _account_summary_field_values(node, _CRON_ACCOUNT_SUMMARY_DATE_KEYS)
                or _account_summary_field_values(node, _CRON_ACCOUNT_SUMMARY_DETAIL_KEYS)
            )
            if has_summary_field:
                records.append(node)
            for raw_key, child in node.items():
                if str(raw_key or "").strip().lower() in _CRON_SENSITIVE_BODY_KEYS:
                    continue
                visit(child)
            return
        if isinstance(node, (list, tuple, set, frozenset)):
            for child in node:
                visit(child)

    visit(value)
    unique: list[dict[object, object]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for record in records:
        identity = (
            _account_summary_first_field(
                record,
                _CRON_ACCOUNT_SUMMARY_TITLE_KEYS,
                max_chars=_CRON_ACCOUNT_SUMMARY_FIELD_MAX_CHARS,
            ),
            _account_summary_first_field(
                record,
                _CRON_ACCOUNT_SUMMARY_SOURCE_KEYS,
                max_chars=_CRON_ACCOUNT_SUMMARY_FIELD_MAX_CHARS,
            ),
            _account_summary_first_field(
                record,
                _CRON_ACCOUNT_SUMMARY_DATE_KEYS,
                max_chars=_CRON_ACCOUNT_SUMMARY_FIELD_MAX_CHARS,
            ),
            _account_summary_first_field(
                record,
                _CRON_ACCOUNT_SUMMARY_DETAIL_KEYS,
                max_chars=_CRON_ACCOUNT_SUMMARY_DETAIL_MAX_CHARS,
            ),
        )
        if not any(identity) or identity in seen:
            continue
        seen.add(identity)
        unique.append(record)
    return tuple(unique)


def _safe_account_message_summary_from_tool_results(result: dict[str, object]) -> str:
    blocks: list[str] = []
    seen_records: set[tuple[str, str, str]] = set()
    for tool_result in result.get("tool_results") or ():
        if _normalized_tool_result_status(tool_result) != "completed":
            continue
        if _tool_result_name(tool_result) not in _CRON_SENSITIVE_ACCOUNT_TOOLS:
            continue
        for record in _iter_sensitive_account_summary_records(_tool_result_output(tool_result)):
            title = _account_summary_first_field(
                record,
                _CRON_ACCOUNT_SUMMARY_TITLE_KEYS,
                max_chars=_CRON_ACCOUNT_SUMMARY_FIELD_MAX_CHARS,
            )
            source = _account_summary_first_field(
                record,
                _CRON_ACCOUNT_SUMMARY_SOURCE_KEYS,
                max_chars=_CRON_ACCOUNT_SUMMARY_FIELD_MAX_CHARS,
            )
            date = _account_summary_first_field(
                record,
                _CRON_ACCOUNT_SUMMARY_DATE_KEYS,
                max_chars=_CRON_ACCOUNT_SUMMARY_FIELD_MAX_CHARS,
            )
            detail = _account_summary_first_field(
                record,
                _CRON_ACCOUNT_SUMMARY_DETAIL_KEYS,
                max_chars=_CRON_ACCOUNT_SUMMARY_DETAIL_MAX_CHARS,
            )
            identity = (title.casefold(), source.casefold(), date.casefold())
            if not any(identity) or identity in seen_records:
                continue
            seen_records.add(identity)
            block_lines = [f"{len(blocks) + 1}. {title or 'Account item'}"]
            if source:
                block_lines.append(f"   From: {source}")
            if date:
                block_lines.append(f"   Date: {date}")
            if detail and detail.casefold() not in " ".join(part.casefold() for part in (title, source, date)):
                block_lines.append(f"   Summary: {detail}")
            blocks.append("\n".join(block_lines))
            if len(blocks) >= _CRON_ACCOUNT_SUMMARY_MAX_ITEMS:
                break
        if len(blocks) >= _CRON_ACCOUNT_SUMMARY_MAX_ITEMS:
            break
    if not blocks:
        return ""
    return (
        "The scheduled task checked email/account data. Raw message body text was removed before delivery.\n\n"
        "Safe summary from verified tool results:\n"
        + "\n\n".join(blocks)
    )


def _safe_account_message_summary(result: dict[str, object], *, use_structured_summary: bool = False) -> str:
    if use_structured_summary:
        summary = _safe_account_message_summary_from_tool_results(result)
        if summary:
            result["cron_sensitive_tool_summary_repaired"] = True
            return summary
    result["cron_sensitive_tool_summary_withheld"] = True
    return (
        "The scheduled task checked email/account data, but the generated delivery included raw account "
        "message content, so it was withheld. No verified triage summary was produced for this run."
    )


def _replace_sensitive_account_leak(result: dict[str, object], text: str) -> str:
    if _cron_sensitive_account_body_leaked(result, text):
        result["cron_sensitive_tool_body_withheld"] = True
        return _safe_account_message_summary(result, use_structured_summary=True)
    if result.get("raw_tool_payload_blocked") and _cron_sensitive_account_metadata_copied(result, text):
        result["cron_sensitive_tool_metadata_withheld"] = True
        return _safe_account_message_summary(result)
    return text


def cron_delivery_text_from_result(
    result: dict[str, object],
    *,
    principal_id: str | None = None,
    html_image_delivery_mode: str = HTML_IMAGE_DELIVERY_MODE_AUTO,
) -> str:
    """Return deliverable cron text after filtering state-file-only media.

    The filter uses runtime facts, not prompt wording: a MEDIA path in
    workspace artifacts is suppressed when it mirrors a file accessed through
    the workspace files area during the same cron run. That preserves explicit
    report/export attachments and completed tool artifact fields while
    preventing tracker/checkpoint files from becoming user-facing attachments.
    """
    from nullion.response_fulfillment_contract import user_visible_text_from_output

    text = cron_delivery_text(user_visible_text_from_output(result), result.get("artifacts"))
    text = _replace_sensitive_account_leak(result, text)
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
    deliverable_paths, support_assets = _prepare_cron_deliverable_paths_for_delivery(
        deliverable_paths,
        html_image_delivery_mode=_normalize_html_image_delivery_mode(html_image_delivery_mode),
    )
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
    html_image_delivery_mode: str = HTML_IMAGE_DELIVERY_MODE_AUTO,
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
        tuple(dict.fromkeys(path for path in paths if path)),
        html_image_delivery_mode=_normalize_html_image_delivery_mode(html_image_delivery_mode),
    )
    return deliverable_paths


def _has_remote_html_image_dependency(path_text: str) -> bool:
    path = Path(str(path_text or "")).expanduser()
    if path.suffix.lower() not in {".html", ".htm"}:
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return _HTML_SELF_CONTAINED_REMOTE_SRC_RE.search(text) is not None


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
    html_image_delivery_mode: str = HTML_IMAGE_DELIVERY_MODE_AUTO,
) -> str | None:
    """Validate deliverable cron artifacts before marking a run delivered."""
    from nullion.artifact_validation import validate_artifact_paths

    normalized_mode = _normalize_html_image_delivery_mode(html_image_delivery_mode)
    paths = cron_delivery_artifact_paths_from_result(
        result,
        text,
        principal_id=principal_id,
        html_image_delivery_mode=normalized_mode,
    )
    if not paths:
        result.pop("cron_artifact_validation_errors", None)
        return None
    validation = validate_artifact_paths(paths)
    if validation.ok:
        issues: list[dict[str, str]] = []
    else:
        issues = [
            {"path": issue.path, "code": issue.code, "message": issue.message}
            for issue in validation.issues
        ]
    if normalized_mode == HTML_IMAGE_DELIVERY_MODE_SELF_CONTAINED:
        for path in paths:
            if _has_remote_html_image_dependency(path):
                issues.append(
                    {
                        "path": str(path),
                        "code": "html_remote_image_dependency",
                        "message": "HTML artifact still references remote image URLs under self-contained mode.",
                    }
                )
    if not issues:
        result.pop("cron_artifact_validation_errors", None)
        return None
    result["cron_artifact_validation_errors"] = issues
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
    silent_delivery_text: Callable[[object, str, dict[str, object]], str | None] | None = None
    notify_approval_required: Callable[[object, str, str, dict[str, object]], None] | None = None


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
    cancellation_checker: Callable[[], bool] | None


def _cron_run_was_cancelled(state: _CronRunDeliveryState) -> bool:
    checker = state.get("cancellation_checker")
    if checker is None:
        return False
    try:
        return bool(checker())
    except Exception:
        logger.debug("Cron run cancellation checker failed", exc_info=True)
        return False


def _cron_run_cancelled_result() -> dict[str, object]:
    return {
        "text": "Scheduled task run stopped.",
        "tool_results": [],
        "artifacts": [],
        "cron_run_cancelled": True,
        "cron_delivery_status": "cancelled",
        "reason": "stopped_by_user",
    }


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
    if _cron_run_was_cancelled(state):
        return {"result": _cron_run_cancelled_result()}
    try:
        run_agent_turn = state["callbacks"].run_agent_turn
        try:
            signature = inspect.signature(run_agent_turn)
        except (TypeError, ValueError):
            signature = None
        if signature is not None and "cancellation_checker" in signature.parameters:
            result = run_agent_turn(
                state["job"],
                state["conversation_id"],
                cancellation_checker=state.get("cancellation_checker"),
            )
        else:
            result = run_agent_turn(state["job"], state["conversation_id"])
    except BaseException as exc:
        error_text = " ".join(str(exc).strip().split()) or exc.__class__.__name__
        failure_result = _cron_agent_exception_result(exc)
        logger.warning(
            "Cron delivery agent turn failed before terminal delivery [cron_id=%s]: %s",
            getattr(state.get("job"), "id", ""),
            error_text,
            exc_info=True,
        )
        return {"result": failure_result}
    result_payload = dict(result or {})
    if _cron_run_was_cancelled(state):
        result_payload.update(_cron_run_cancelled_result())
    return {"result": result_payload}


def _cron_run_route_after_agent(state: _CronRunDeliveryState) -> str:
    if state.get("result", {}).get("cron_run_cancelled"):
        return "cancelled"
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
    if state["callbacks"].notify_approval_required is not None:
        try:
            state["callbacks"].notify_approval_required(
                state["job"],
                state["delivery_channel"],
                state["delivery_target"],
                result,
            )
        except Exception:
            logger.debug("Could not notify pending cron approval", exc_info=True)
    _attach_cron_execution_outcome(state, result, text=str(result.get("text") or ""))
    return {"result": result}


def _cron_run_cancelled_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    if callbacks.clear_background_delivery is not None:
        callbacks.clear_background_delivery(state["conversation_id"])
    result = dict(state.get("result") or {})
    result["cron_delivery_status"] = "cancelled"
    result["cron_run_cancelled"] = True
    callbacks.record_event(
        "cron.delivery.cancelled",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
        reason="stopped_by_user",
    )
    _attach_cron_execution_outcome(state, result, text=str(result.get("text") or "Scheduled task run stopped."))
    return {"result": result}


def _cron_run_prepare_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    result = dict(state.get("result") or {})
    artifacts = result.get("artifacts")
    html_image_delivery_mode = _cron_html_image_delivery_mode(state.get("job"), result)
    text = cron_delivery_text_from_result(
        result,
        principal_id=state.get("conversation_id"),
        html_image_delivery_mode=html_image_delivery_mode,
    )
    block_reason = state["callbacks"].block_reason(result, str(text), artifacts)
    if block_reason is None:
        block_reason = cron_artifact_validation_block_reason(
            result,
            str(text),
            principal_id=state.get("conversation_id"),
            html_image_delivery_mode=html_image_delivery_mode,
        )
    if not str(text or "").strip() and state["callbacks"].silent_delivery_text is not None:
        replacement = state["callbacks"].silent_delivery_text(state["job"], state.get("label", ""), result)
        if str(replacement or "").strip():
            text = str(replacement)
            result["cron_silent_result_replaced"] = True
    return {"result": result, "text": str(text), "artifacts": artifacts, "block_reason": block_reason}


def _cron_run_route_prepared(state: _CronRunDeliveryState) -> str:
    result = state.get("result") or {}
    if result.get("cron_run_failed") or result.get("cron_delivery_failed"):
        return "blocked"
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


def _cron_requested_outcome(job: object) -> str:
    name = str(getattr(job, "name", "") or "").strip()
    task = str(getattr(job, "task", "") or "").strip()
    return "\n".join(part for part in (name, task) if part).strip()


def _attach_cron_execution_outcome(
    state: _CronRunDeliveryState,
    result: dict[str, object],
    *,
    text: str | None = None,
    delivered_artifacts: Iterable[str] | None = None,
) -> dict[str, object]:
    artifacts_created = cron_delivery_artifact_paths_from_result(
        result,
        text,
        principal_id=state.get("conversation_id"),
        html_image_delivery_mode=_cron_html_image_delivery_mode(state.get("job"), result),
    )
    outcome = build_execution_outcome(
        requested_outcome=_cron_requested_outcome(state["job"]),
        user_visible_message=text or str(result.get("text") or result.get("final_text") or ""),
        tool_results=result.get("tool_results") or (),
        artifacts_created=artifacts_created,
        artifacts_delivered=delivered_artifacts or (),
        delivery_status=str(result.get("cron_delivery_status") or ""),
        fulfillment_satisfied=not bool(result.get("cron_delivery_failed") or result.get("cron_run_failed")),
        missing_requirements=(str(result.get("reason") or ""),) if result.get("cron_delivery_failed") or result.get("cron_run_failed") else (),
        suspended_for_approval=bool(result.get("suspended_for_approval")),
        reason=str(result.get("reason") or result.get("error") or ""),
    )
    result["execution_outcome"] = outcome.to_dict()
    return result


def _cron_run_blocked_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    if callbacks.clear_background_delivery is not None:
        callbacks.clear_background_delivery(state["conversation_id"])
    result = dict(state.get("result") or {})
    reason = state.get("block_reason") or str(result.get("reason") or "cron_delivery_blocked")
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
    _attach_cron_execution_outcome(state, result, text=str(state.get("text") or ""))
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
    _attach_cron_execution_outcome(state, result, text=str(state.get("text") or ""))
    return {"result": result}


def _cron_run_web_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    result = dict(state.get("result") or {})
    callbacks.save_web_delivery(
        state["job"],
        state["conversation_id"],
        scheduled_task_delivery_text(
            state["job"],
            str(state.get("text") or ""),
            run_label=state.get("label"),
        ),
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
    _attach_cron_execution_outcome(
        state,
        result,
        text=str(state.get("text") or ""),
        delivered_artifacts=cron_delivery_artifact_paths_from_result(
            result,
            str(state.get("text") or ""),
            principal_id=state.get("conversation_id"),
            html_image_delivery_mode=_cron_html_image_delivery_mode(state.get("job"), result),
        ),
    )
    return {"result": result}


def _cron_run_messaging_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    from nullion.artifacts import media_candidate_paths_from_text

    callbacks = state["callbacks"]
    result = dict(state.get("result") or {})
    attempts = int(state.get("send_attempts") or 0) + 1
    text = scheduled_task_delivery_text(
        state["job"],
        cron_delivery_text(str(state.get("text") or ""), state.get("artifacts")),
        run_label=state.get("label"),
    )
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
        _attach_cron_execution_outcome(
            state,
            result,
            text=str(state.get("text") or ""),
            delivered_artifacts=cron_delivery_artifact_paths_from_result(
                result,
                str(state.get("text") or ""),
                principal_id=state.get("conversation_id"),
                html_image_delivery_mode=_cron_html_image_delivery_mode(state.get("job"), result),
            ),
        )
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
            _attach_cron_execution_outcome(state, result, text=fallback_text)
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
        result["reason"] = "attachment delivery failed"
        _attach_cron_execution_outcome(state, result, text=str(state.get("text") or ""))
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
        reason="platform delivery failed",
    )
    result["cron_delivery_status"] = "failed"
    result["cron_delivery_failed"] = True
    result["reason"] = "platform delivery failed"
    _attach_cron_execution_outcome(state, result, text=str(state.get("text") or ""))
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
    graph.add_node("cancelled", _cron_run_cancelled_node)
    graph.add_node("prepare", _cron_run_prepare_delivery_node)
    graph.add_node("blocked", _cron_run_blocked_node)
    graph.add_node("silent", _cron_run_silent_node)
    graph.add_node("web", _cron_run_web_delivery_node)
    graph.add_node("messaging", _cron_run_messaging_delivery_node)
    graph.add_edge(START, "resolve_route")
    graph.add_edge("resolve_route", "run_agent")
    graph.add_conditional_edges(
        "run_agent",
        _cron_run_route_after_agent,
        {"paused": "paused", "cancelled": "cancelled", "prepare": "prepare"},
    )
    graph.add_conditional_edges(
        "prepare",
        _cron_run_route_prepared,
        {"blocked": "blocked", "silent": "silent", "web": "web", "messaging": "messaging"},
    )
    graph.add_conditional_edges("messaging", _cron_run_route_after_messaging, {"retry": "messaging", END: END})
    for node in ("paused", "cancelled", "blocked", "silent", "web"):
        graph.add_edge(node, END)
    return graph.compile()


def run_cron_delivery_workflow(
    job: object,
    *,
    label: str,
    callbacks: CronRunDeliveryCallbacks,
    cancellation_checker: Callable[[], bool] | None = None,
) -> dict[str, object]:
    final_state = _compiled_cron_run_delivery_graph().invoke(
        {"job": job, "label": label, "callbacks": callbacks, "result": {}, "cancellation_checker": cancellation_checker}
    )
    result = final_state.get("result")
    return dict(result or {})


__all__ = [
    "CronRunDeliveryCallbacks",
    "MESSAGING_CRON_DELIVERY_CHANNELS",
    "SUPPORTED_CRON_DELIVERY_CHANNELS",
    "configured_delivery_target",
    "clear_cron_execution_metadata_caches",
    "cron_agent_history",
    "cron_agent_prompt",
    "cron_conversation_id",
    "cron_artifact_validation_block_reason",
    "cron_delivery_artifact_paths_from_result",
    "cron_delivery_reply_text",
    "cron_delivery_target",
    "cron_delivery_text",
    "cron_delivery_text_from_result",
    "cron_structured_result_block_reason",
    "cancel_manual_cron_background_run",
    "cancel_manual_cron_background_runs",
    "effective_cron_delivery_channel",
    "manual_cron_deferred_receipt",
    "manual_cron_running_status_text",
    "manual_cron_status_group_id",
    "manual_cron_silent_delivery_text",
    "manual_cron_terminal_status_text",
    "normalize_cron_delivery_channel",
    "normalize_html_image_delivery_mode",
    "record_interrupted_cron_delivery_runs",
    "run_cron_delivery_workflow",
    "run_single_agent_cron_turn",
    "scheduled_task_delivery_text",
    "start_manual_cron_background_delivery",
    "register_manual_cron_background_run",
]
