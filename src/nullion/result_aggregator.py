"""Result aggregator — drains the progress queue and delivers updates to the user.

The aggregator runs as a single background coroutine per orchestrator instance.
It receives ProgressUpdate events from all running mini-agents, debounces rapid
progress notes, and delivers:
  - Per-task progress edits (debounced at 3s)
  - A coherent final summary when all tasks in a group reach terminal state

Usage::

    agg = ResultAggregator(deliver_fn=my_send_fn, model_client=client)
    asyncio.create_task(agg.run(progress_queue))
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_root_for_principal,
    media_candidate_paths_from_text,
)
from nullion.attachment_format_graph import is_domain_suffix_extension, plan_attachment_format
from nullion.delegated_artifact_workflow import finalize_delegated_artifacts
from nullion.mini_agent_runner import ProgressUpdate
from nullion.task_queue import TaskGroup, TaskRecord, TaskRegistry, TaskStatus
from nullion.task_status_format import format_task_status_line, format_task_status_summary

logger = logging.getLogger(__name__)

_SUMMARY_SYSTEM_PROMPT = """\
You are summarizing the results of parallel sub-tasks for the user.
Given the original request and a list of completed task outputs, write a concise, \
direct reply (1–4 sentences) that answers the original request. \
Mention any failures only when they change the user-visible outcome. Do not include file paths, \
MEDIA directives, markdown links, or quoted empty filenames. If runtime-verified deliverable \
artifacts will be attached, refer to them generically as attached files; the system will attach \
them separately. Do not mention scratch, raw, or intermediate file paths. \
Do not enumerate every step — just deliver the answer. Speak as the main agent."""

# Minimum seconds between consecutive progress-note edits for the same group.
MIN_PROGRESS_INTERVAL_S: float = 3.0
DEFAULT_FINAL_SUMMARY_TIMEOUT_S: float = 12.0
_FILENAME_TOKEN_RE = re.compile(r"(?<![\w./-])([A-Za-z0-9][\w .()@+-]{0,180}\.[A-Za-z0-9]{1,16})(?![\w./-])")
_ARTIFACT_DELIVERY_ROLES = frozenset({"deliverable", "deliver_receipt", "verify"})
_ARTIFACT_PRODUCER_TOOLS = frozenset({"file_write", "document_create", "spreadsheet_create", "presentation_create"})
_ARTIFACT_SOURCE_TOOLS = frozenset(
    {
        "archive_extract",
        "browser_extract_detail",
        "browser_extract_items",
        "browser_extract_text",
        "browser_image_collect",
        "browser_run_js",
        "file_download",
        "file_read",
        "file_search",
        "workspace_summary",
    }
)
_INTERNAL_ARTIFACT_SUFFIXES = frozenset({".json", ".jsonl", ".db", ".sqlite", ".sqlite3"})
_RAW_TOOL_PAYLOAD_KEYS = frozenset(
    {
        "artifact_path",
        "artifact_paths",
        "artifacts",
        "command",
        "exit_code",
        "network_mode",
        "provider_id",
        "shell",
        "stderr",
        "stdout",
        "timeout_seconds",
        "tool_name",
    }
)


@dataclass
class GroupState:
    group_id: str
    conversation_id: str
    original_message: str
    last_progress_time: float = field(default_factory=time.monotonic)
    status_lines: dict[str, str] = field(default_factory=dict)  # task_id → one-line status
    task_summary_visible: bool = False
    delivered_input_questions: set[str] = field(default_factory=set)


# Delivery callback type: (conversation_id, text, *, is_status) -> Awaitable[None] | None
DeliverFn = Callable[..., Any]


class ResultAggregator:
    """Drains a shared ProgressUpdate queue and delivers results to the interface."""

    def __init__(
        self,
        *,
        deliver_fn: DeliverFn,
        task_registry: TaskRegistry,
        model_client: Any | None = None,
        min_progress_interval_s: float = MIN_PROGRESS_INTERVAL_S,
    ) -> None:
        self._deliver_fn = deliver_fn
        self._registry = task_registry
        self._model_client = model_client
        self._min_interval = min_progress_interval_s
        self._group_state: dict[str, GroupState] = {}
        self._completed_groups: set[str] = set()

    async def run(self, progress_queue: asyncio.Queue) -> None:
        """Drain the queue forever. Call as a background asyncio task."""
        while True:
            try:
                update: ProgressUpdate = await progress_queue.get()
                await self._handle(update)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("ResultAggregator: error handling update: %s", exc, exc_info=True)

    # ── Handlers ───────────────────────────────────────────────────────────

    async def _handle(self, update: ProgressUpdate) -> None:
        await _compiled_result_aggregation_graph().ainvoke({"aggregator": self, "update": update})

    async def _debounced_progress(
        self, gs: GroupState, update: ProgressUpdate, task: TaskRecord | None
    ) -> None:
        now = time.monotonic()
        if now - gs.last_progress_time < self._min_interval:
            return
        gs.last_progress_time = now
        label = task.title if task else update.task_id
        await self._deliver(
            gs.conversation_id,
            f"→ [{label}] {update.message or ''}",
            is_status=True,
            group_id=gs.group_id,
            status_kind="progress_note",
        )

    async def _deliver_tool_activity(
        self, gs: GroupState, update: ProgressUpdate, task: TaskRecord | None
    ) -> None:
        label = task.title if task else update.task_id
        tool_line = str(update.message or "").strip()
        if not tool_line:
            return
        await self._deliver(
            gs.conversation_id,
            tool_line,
            is_status=True,
            group_id=gs.group_id,
            status_kind="tool_activity",
            task_id=update.task_id,
            activity_id=f"mini-agent-tools:{update.task_id}",
            activity_label=f"{label} tools",
        )

    async def _deliver_status(
        self,
        gs: GroupState,
        group: TaskGroup,
        *,
        status_line_overrides: dict[str, str] | None = None,
        terminal: bool = False,
    ) -> None:
        """Emit the current status summary for the group."""
        planner_summary = _planner_summary_from_group(group)
        overrides = status_line_overrides or {}
        persistent_overrides: dict[str, str] = {}
        for task_id, line in overrides.items():
            if task_id and str(line or "").strip():
                persistent_overrides[task_id] = str(line).strip()
                gs.status_lines[task_id] = str(line).strip()
        for task in group.tasks:
            if task.task_id in overrides:
                continue
            if task.status in {
                TaskStatus.RUNNING,
                TaskStatus.QUEUED,
                TaskStatus.WAITING_INPUT,
                TaskStatus.COMPLETE,
                TaskStatus.FAILED,
                TaskStatus.CANCELLED,
            }:
                current = gs.status_lines.get(task.task_id)
                next_line = format_task_status_line(task)
                if _should_replace_task_status_line(current, next_line):
                    gs.status_lines[task.task_id] = next_line
        display_status_lines = dict(gs.status_lines)
        display_status_lines.update(persistent_overrides)
        if not persistent_overrides:
            display_status_lines.update(artifact_status_line_overrides_for_group(group))
        delivered = await self._deliver(
            gs.conversation_id,
            format_task_status_summary(
                group.tasks,
                planner_summary=planner_summary,
                subject=group.original_message,
                status_lines=display_status_lines,
                default_status=TaskStatus.PENDING,
            ),
            is_status=True,
            group_id=group.group_id,
            status_kind="task_summary",
            terminal=terminal,
        )
        if delivered:
            gs.task_summary_visible = True

    async def _on_group_complete(self, gs: GroupState, group: TaskGroup) -> None:
        """All tasks terminal — generate and deliver the final summary."""
        if group.group_id in self._completed_groups:
            return
        self._completed_groups.add(group.group_id)
        await self._deliver_status(gs, group, terminal=True)
        recovered_artifacts = finalize_delegated_artifacts(group)
        preverified_artifacts = _artifact_paths_for_group_delivery(
            group,
            recovered_artifacts,
            summary=None,
        )
        summary = (
            _artifact_recovery_summary(group, recovered_artifacts)
            if recovered_artifacts
            else await self._generate_summary(group, deliverable_artifacts=preverified_artifacts)
        )
        requested_extension = _requested_attachment_extension_for_group(group, model_client=self._model_client)
        deliverable_artifacts = _artifact_paths_for_group_delivery(
            group,
            recovered_artifacts,
            summary=summary,
            requested_extension=requested_extension,
        )
        if requested_extension and not deliverable_artifacts:
            # Preserve real terminal failure context, then append the missing
            # requested-artifact note when it adds useful delivery clarity.
            summary = _merge_missing_requested_artifact_summary(
                group,
                summary,
                requested_extension=requested_extension,
            )
        summary = _strip_attached_artifact_references(summary, deliverable_artifacts)
        if not deliverable_artifacts:
            summary = _strip_unverified_attachment_claims(summary)
        await self._deliver(
            gs.conversation_id,
            _summary_with_original_request_context(group, summary),
            group_id=gs.group_id,
        )

        # Deliver any artifacts.
        delivered_artifacts: list[str] = []
        for artifact in deliverable_artifacts:
            delivered = await self._deliver(gs.conversation_id, artifact, is_artifact=True, group_id=gs.group_id)
            if delivered:
                delivered_artifacts.append(artifact)
        if delivered_artifacts:
            await self._deliver_status(
                gs,
                group,
                status_line_overrides=_status_lines_for_delivered_artifacts(group, delivered_artifacts),
                terminal=True,
            )

        # Clean up group state.
        self._group_state.pop(gs.group_id, None)

    async def _generate_summary(self, group: TaskGroup, *, deliverable_artifacts: list[str] | None = None) -> str:
        """One-shot LLM call to synthesize a coherent final reply."""
        if self._model_client is None:
            return self._fallback_summary(group)

        task_summaries = []
        for task in group.tasks:
            if task.result:
                status_label = "✓" if task.result.status == "success" else "✗"
                output = _safe_task_result_preview(task, max_chars=300) or "(no user-visible output)"
                task_summaries.append(f"{status_label} {task.title}: {output}")
            else:
                task_summaries.append(f"— {task.title}: (no result)")

        verified_artifacts = [
            str(path)
            for path in (deliverable_artifacts or ())
            if isinstance(path, str) and path.strip()
        ]
        artifact_context = ""
        if verified_artifacts:
            artifact_context = (
                "\n\nRuntime-verified deliverable artifacts that will be attached:\n"
                + "\n".join(f"- {path}" for path in verified_artifacts)
                + "\nDo not include these paths in the reply. Say attached file or attached files instead."
            )
        prompt = (
            f"Original request: {group.original_message}\n\n"
            + "\n".join(task_summaries)
            + artifact_context
        )
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self._model_client.create,
                    messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
                    tools=[],
                    max_tokens=512,
                    system=_SUMMARY_SYSTEM_PROMPT,
                ),
                timeout=_final_summary_timeout_seconds(),
            )
            content = response.get("content") or []
            parts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
            text = "".join(parts).strip()
            return text or self._fallback_summary(group)
        except TimeoutError:
            logger.warning(
                "ResultAggregator: summary LLM call timed out for group %s; using fallback summary",
                group.group_id,
            )
            return self._fallback_summary(group)
        except Exception as exc:
            logger.debug("ResultAggregator: summary LLM call failed: %s", exc)
            return self._fallback_summary(group)

    def _fallback_summary(self, group: TaskGroup) -> str:
        """Plain-text summary when LLM call fails or is unavailable."""
        done = sum(1 for t in group.tasks if t.status == TaskStatus.COMPLETE)
        failed = sum(1 for t in group.tasks if t.status == TaskStatus.FAILED)
        cancelled = sum(1 for t in group.tasks if t.status == TaskStatus.CANCELLED)
        total = len(group.tasks)
        parts = [f"Completed {done}/{total} task(s)."]
        if failed:
            parts.append(f"{failed} failed.")
        if cancelled:
            parts.append(f"{cancelled} cancelled.")
        failure_details = []
        for task in group.tasks:
            preview = (
                _safe_result_text_preview(task.result.output, max_chars=120)
                if task.result and task.result.output
                else ""
            )
            if preview:
                parts.append(f"• {task.title}: {preview}")
            if task.status == TaskStatus.FAILED and task.result and task.result.error:
                failure = _safe_result_text_preview(task.result.error, max_chars=180)
                if failure:
                    failure_details.append(f"{task.title}: {failure}")
        if failure_details:
            # The fallback is often what users see when a scheduled task fails;
            # keep the causal failure visible instead of reducing the result to counts.
            details = "; ".join(failure_details[:3])
            if len(failure_details) > 3:
                details = f"{details}; {len(failure_details) - 3} more failed task(s)"
            parts.append(f"Failure reasons: {details}.")
        return " ".join(parts)

    @staticmethod
    def _fallback_summary_is_bare_failure_notice(group: TaskGroup) -> bool:
        failed = sum(1 for task in group.tasks if task.status == TaskStatus.FAILED)
        if failed <= 0:
            return False
        return not any(
            task.result and (task.result.output or task.result.error or task.result.artifacts)
            for task in group.tasks
        )

    async def _deliver(self, conversation_id: str, text: str, **kwargs) -> bool:
        try:
            result = self._deliver_fn(conversation_id, text, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            delivered = result is not False
            if not delivered:
                logger.warning(
                    "ResultAggregator: deliver_fn returned false conversation_id=%s group_id=%s is_artifact=%s",
                    conversation_id,
                    kwargs.get("group_id"),
                    bool(kwargs.get("is_artifact")),
                )
            return delivered
        except Exception as exc:
            logger.warning(
                "ResultAggregator: deliver_fn failed conversation_id=%s group_id=%s is_artifact=%s: %s",
                conversation_id,
                kwargs.get("group_id"),
                bool(kwargs.get("is_artifact")),
                exc,
            )
            return False


def _safe_task_result_preview(task: TaskRecord, *, max_chars: int) -> str:
    result = getattr(task, "result", None)
    if result is None:
        return ""
    text = getattr(result, "output", None) or getattr(result, "error", None) or ""
    preview = _safe_result_text_preview(text, max_chars=max_chars)
    if preview:
        return preview
    if getattr(result, "artifacts", None):
        return "completed with a verified artifact."
    if getattr(result, "status", "") == "success":
        return "completed with verified runtime evidence."
    return ""


def _safe_result_text_preview(value: object, *, max_chars: int) -> str:
    text = _plain_result_text(value)
    if not text:
        return ""
    text = re.sub(r"\b(?:MEDIA|ARTIFACT):?\s+[^ \n\r\t]+", "", text)
    text = "\n".join(line for line in text.splitlines() if not line.strip().startswith("MEDIA:"))
    text = text.replace("\\n", " ")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    parsed = _parse_jsonish_result(text)
    if parsed is not None:
        return _safe_structured_result_preview(parsed)
    if _looks_like_raw_tool_payload(text):
        return "completed with verified runtime evidence."
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + "..."
    return text


def _plain_result_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (dict, list, tuple)):
        try:
            return json.dumps(value, ensure_ascii=False, default=str)
        except TypeError:
            return str(value)
    return str(value).strip()


def _parse_jsonish_result(text: str) -> object | None:
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        return json.loads(stripped)
    except Exception:
        return None


def _safe_structured_result_preview(value: object) -> str:
    if isinstance(value, dict):
        lower_keys = {str(key).lower() for key in value}
        if lower_keys & _RAW_TOOL_PAYLOAD_KEYS:
            status = str(value.get("status") or "").strip().lower()
            if status in {"failed", "failure", "error"}:
                return "failed with structured runtime evidence."
            return "completed with structured runtime evidence."
        for key in ("summary", "message", "answer", "result"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip() and not _looks_like_raw_tool_payload(candidate):
                return _safe_result_text_preview(candidate, max_chars=180)
        return "completed with structured runtime evidence."
    if isinstance(value, list):
        return "completed with structured runtime evidence."
    return ""


def _looks_like_raw_tool_payload(text: str) -> bool:
    lowered = text.lower()
    return any(f'"{key}"' in lowered or f"{key}:" in lowered for key in _RAW_TOOL_PAYLOAD_KEYS)


def _summary_with_original_request_context(group: TaskGroup, summary: str) -> str:
    text = str(summary or "").strip()
    if _is_scheduled_task_request(group.original_message):
        return text
    if _is_planner_group(group):
        return _planner_result_summary_text(group, text)
    request = _compact_original_request(group.original_message)
    if not text or not request:
        return text
    if text.lower().startswith("result for "):
        return text
    return f'Result for "{request}":\n{text}'


def _is_planner_group(group: TaskGroup) -> bool:
    metadata = getattr(group, "planner_metadata", None)
    if not isinstance(metadata, dict):
        return False
    disposition = str(metadata.get("disposition") or "").strip()
    return bool(disposition) and bool(metadata.get("valid", True))


def _planner_result_summary_text(group: TaskGroup, summary: str) -> str:
    text = str(summary or "").strip()
    if text.startswith("📊 **PLANNER RESULT**"):
        return text
    request = _compact_original_request(group.original_message)
    header = "📊 **PLANNER RESULT**"
    if request and text:
        return f"{header}\n\nFor: {request}\n\n{text}"
    if request:
        return f"{header}\n\nFor: {request}"
    return f"{header}\n\n{text}" if text else header


def _final_summary_timeout_seconds() -> float:
    raw_value = os.environ.get("NULLION_RESULT_SUMMARY_TIMEOUT_SECONDS", "").strip()
    if not raw_value:
        return DEFAULT_FINAL_SUMMARY_TIMEOUT_S
    try:
        return max(0.5, float(raw_value))
    except ValueError:
        return DEFAULT_FINAL_SUMMARY_TIMEOUT_S


def _strip_attached_artifact_references(summary: str, artifact_paths: list[str] | tuple[str, ...]) -> str:
    text = str(summary or "").strip()
    if not text:
        return text
    text = re.sub(r"\b(?:MEDIA|ARTIFACT):?\s+[^ \n\r\t]+", "attached file", text)
    kept_lines = [
        line
        for line in text.splitlines()
        if not line.strip().startswith("MEDIA:")
    ]
    text = "\n".join(kept_lines).strip()
    for path in artifact_paths or ():
        path_text = str(path or "").strip()
        if not path_text:
            continue
        text = text.replace(path_text, "attached file")
        text = text.replace(Path(path_text).name, "attached file")
    text = re.sub(r"`\s*`", "attached file", text)
    text = re.sub(r"\battached file(?:\s*(?:,|and)\s*attached file)+\b", "attached files", text)
    sentences = re.split(r"(?<=[.!?])\s+", text)
    normalized_sentences: list[str] = []
    for sentence in sentences:
        lowered = sentence.lower()
        if " attached" in lowered and "artifact" in lowered:
            normalized_sentences.append("The verified report is attached.")
        else:
            normalized_sentences.append(sentence)
    text = " ".join(sentence for sentence in normalized_sentences if sentence).strip()
    if len([path for path in (artifact_paths or ()) if str(path or "").strip()]) == 1:
        text = re.sub(r"\b(files|reports|artifacts)\s+are attached\b", "file is attached", text, flags=re.IGNORECASE)
        sentences = re.split(r"(?<=[.!?])\s+", text)
        normalized_sentences = []
        for sentence in sentences:
            lowered = sentence.lower()
            if " attached" in lowered and " and " in lowered:
                normalized_sentences.append("The requested file is attached.")
            else:
                normalized_sentences.append(sentence)
        text = " ".join(sentence for sentence in normalized_sentences if sentence).strip()
    return text.strip()


def _strip_unverified_attachment_claims(summary: str) -> str:
    text = str(summary or "").strip()
    if not text:
        return text
    sentences = re.split(r"(?<=[.!?])\s+", text)
    kept: list[str] = []
    for sentence in sentences:
        lowered = sentence.lower()
        mentions_attachment = any(
            marker in lowered
            for marker in (
                " attached",
                " is attached",
                " are attached",
                "attachment",
                "media:",
                "artifact:",
            )
        )
        mentions_file = any(
            marker in lowered
            for marker in (
                "file",
                "pdf",
                "html",
                "report copy",
                "artifact",
                "media:",
                "artifact:",
            )
        )
        if mentions_attachment and mentions_file:
            continue
        kept.append(sentence)
    return " ".join(sentence for sentence in kept if sentence).strip() or text


def _artifact_recovery_summary(group: TaskGroup, artifact_paths: list[str]) -> str:
    if len(artifact_paths) == 1:
        return "I created the requested file from the completed task output and attached it."
    return f"I created {len(artifact_paths)} requested files from the completed task output and attached them."


def _artifact_paths_for_group_delivery(
    group: TaskGroup,
    recovered_artifacts: list[str] | tuple[str, ...],
    *,
    summary: str | None = None,
    requested_extension: str | None = None,
) -> list[str]:
    explicit_paths: list[str] = []
    artifact_task_paths: list[str] = []
    scheduled_fallback_paths: list[str] = []
    all_text_paths: list[str] = []
    scheduled_group = _is_scheduled_task_request(group.original_message)
    leaf_task_ids = _leaf_task_ids(group)
    for task in group.tasks:
        result = task.result
        if result is None:
            continue
        if getattr(result, "status", None) != "success":
            # Failure text is explanatory evidence, not an attachment contract.
            # In scheduled/report flows it often names the expected file and the
            # wrong file that caused the failure; treating those names as media
            # candidates can attach stale artifacts after a verifier rejects them.
            continue
        task_paths = [str(path) for path in (result.artifacts or []) if isinstance(path, str) and path.strip()]
        explicit_paths.extend(task_paths)
        if _task_has_artifact_delivery_scope(task) or (task.task_id in leaf_task_ids and not scheduled_group):
            artifact_task_paths.extend(task_paths)
    explicit_paths.extend(str(path) for path in (recovered_artifacts or []) if isinstance(path, str) and path.strip())
    artifact_task_paths.extend(str(path) for path in (recovered_artifacts or []) if isinstance(path, str) and path.strip())
    roots = _artifact_roots_for_group(group)
    summary_paths = _existing_artifacts_referenced_by_text(str(summary or ""), artifact_roots=roots)
    requested_extension = _normalize_requested_extension(requested_extension)
    if _group_has_artifact_verifier(group) and not _group_has_successful_artifact_verifier(group):
        trusted_artifact_task_paths = _filter_internal_artifact_paths(artifact_task_paths)
        if scheduled_group or not trusted_artifact_task_paths:
            return []
        if requested_extension:
            matching = _filter_artifact_paths_by_extension(trusted_artifact_task_paths, requested_extension)
            if matching:
                trusted_artifact_task_paths = matching
            elif _artifact_paths_have_single_extension(trusted_artifact_task_paths):
                requested_extension = None
            else:
                return []
        artifact_task_paths = trusted_artifact_task_paths
        explicit_paths = trusted_artifact_task_paths
        summary_paths = []
        all_text_paths = []
    for task in group.tasks:
        result = task.result
        if result is None:
            continue
        if getattr(result, "status", None) != "success":
            continue
        task_produces_artifacts = bool(
            set(str(tool).lower() for tool in (getattr(task, "allowed_tools", None) or ()))
            & _ARTIFACT_PRODUCER_TOOLS
        )
        for text in _task_result_text_fragments(result):
            media_found = _existing_media_artifacts_referenced_by_text(text, artifact_roots=roots)
            if media_found and (
                not scheduled_group
                or _task_has_artifact_delivery_scope(task)
                or task_produces_artifacts
            ):
                artifact_task_paths.extend(media_found)
            elif scheduled_group and task.task_id not in leaf_task_ids:
                scheduled_fallback_paths.extend(media_found)
            found = _existing_artifacts_referenced_by_text(text, artifact_roots=roots)
            all_text_paths.extend(found)
            if (
                _task_has_artifact_delivery_scope(task)
                or task_produces_artifacts
                or (task.task_id in leaf_task_ids and not scheduled_group)
                or (task.task_id not in leaf_task_ids and found and not scheduled_group)
            ):
                artifact_task_paths.extend(found)
            elif scheduled_group and task.task_id not in leaf_task_ids:
                scheduled_fallback_paths.extend(found)
    if scheduled_group and not requested_extension and not _group_has_explicit_artifact_delivery(group):
        return []
    if requested_extension:
        candidate_groups = (
            (artifact_task_paths, summary_paths, scheduled_fallback_paths, explicit_paths, all_text_paths)
            if scheduled_group
            else (artifact_task_paths, explicit_paths, summary_paths, all_text_paths)
        )
        for candidates in candidate_groups:
            matching = _filter_artifact_paths_by_extension(candidates, requested_extension)
            if matching:
                if scheduled_group and not _group_has_explicit_artifact_delivery(group):
                    return _collapse_same_format_scheduled_artifacts(matching)
                return matching
        if scheduled_group:
            summary_fallback = _filter_internal_artifact_paths(summary_paths)
            if summary_fallback:
                return list(dict.fromkeys(summary_fallback))
        return []
    candidate_groups = (
        (artifact_task_paths, summary_paths, scheduled_fallback_paths, explicit_paths)
        if scheduled_group
        else (artifact_task_paths,)
    )
    for candidates in candidate_groups:
        candidates = _filter_internal_artifact_paths(candidates)
        if candidates:
            candidates = list(dict.fromkeys(candidates))
            if scheduled_group and not _group_has_explicit_artifact_delivery(group):
                return _collapse_same_format_scheduled_artifacts(candidates)
            return candidates
    return []


def _requested_attachment_extension_for_group(group: TaskGroup, *, model_client: Any | None) -> str | None:
    try:
        return plan_attachment_format(
            group.original_message,
            model_client=model_client,
            allow_filename_tokens=True,
        ).extension
    except Exception:
        logger.debug("Could not plan requested attachment extension for group %s", group.group_id, exc_info=True)
        return None


def _normalize_requested_extension(extension: str | None) -> str | None:
    text = str(extension or "").strip().lower()
    if not text:
        return None
    normalized = text if text.startswith(".") else f".{text}"
    return None if is_domain_suffix_extension(normalized) else normalized


def _filter_artifact_paths_by_extension(paths: list[str] | tuple[str, ...], extension: str) -> list[str]:
    normalized = _normalize_requested_extension(extension)
    if normalized is None:
        return _filter_internal_artifact_paths(paths)
    return list(
        dict.fromkeys(
            path for path in paths if Path(str(path)).suffix.lower() == normalized
        )
    )


def _filter_internal_artifact_paths(paths: list[str] | tuple[str, ...]) -> list[str]:
    return list(dict.fromkeys(path for path in paths if not _is_internal_artifact_path(path)))


def _artifact_paths_have_single_extension(paths: list[str] | tuple[str, ...]) -> bool:
    suffixes = {
        Path(str(path)).suffix.lower()
        for path in paths
        if str(path or "").strip() and Path(str(path)).suffix
    }
    return len(suffixes) == 1


def _group_has_explicit_artifact_delivery(group: TaskGroup) -> bool:
    return any(_task_has_artifact_delivery_scope(task) for task in getattr(group, "tasks", ()) or ())


def _group_has_artifact_verifier(group: TaskGroup) -> bool:
    return any(_task_artifact_role(task) == "verify" for task in getattr(group, "tasks", ()) or ())


def _group_has_successful_artifact_verifier(group: TaskGroup) -> bool:
    return any(
        _task_artifact_role(task) == "verify"
        and getattr(getattr(task, "result", None), "status", None) == "success"
        for task in getattr(group, "tasks", ()) or ()
    )


def _task_artifact_role(task: TaskRecord) -> str:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    return str(metadata.get("artifact_role") or "").strip()


def _collapse_same_format_scheduled_artifacts(paths: list[str] | tuple[str, ...]) -> list[str]:
    """For scheduled runs, keep one unmarked deliverable per file format.

    Parallel cron subtasks can independently produce equivalent report files.
    Without an explicit artifact-delivery contract, same-extension candidates
    are alternate deliverables, not separate user-requested files.
    """

    ordered = list(dict.fromkeys(str(path) for path in paths if str(path or "").strip()))
    grouped: dict[str, list[str]] = {}
    for path in ordered:
        suffix = Path(path).suffix.lower()
        grouped.setdefault(suffix or path, []).append(path)
    collapsed: list[str] = []
    for candidates in grouped.values():
        if len(candidates) == 1:
            collapsed.append(candidates[0])
            continue
        collapsed.append(max(candidates, key=_artifact_sort_key))
    return list(dict.fromkeys(collapsed))


def _artifact_sort_key(path: str) -> tuple[float, int]:
    try:
        stat = Path(path).expanduser().stat()
        return (float(stat.st_mtime), len(Path(path).name))
    except OSError:
        return (0.0, len(str(path)))


def _is_internal_artifact_path(path: object) -> bool:
    try:
        candidate = Path(str(path))
    except (TypeError, ValueError):
        return True
    return candidate.suffix.lower() in _INTERNAL_ARTIFACT_SUFFIXES


def _missing_requested_artifact_summary(group: TaskGroup, requested_extension: str) -> str:
    normalized = _normalize_requested_extension(requested_extension) or str(requested_extension)
    return (
        f"I could not attach the requested {normalized} file because no verified "
        f"{normalized} artifact was produced for this run."
    )


def _merge_missing_requested_artifact_summary(
    group: TaskGroup,
    summary: str | None,
    *,
    requested_extension: str,
) -> str:
    missing_note = _missing_requested_artifact_summary(group, requested_extension).strip()
    text = str(summary or "").strip()
    if not text:
        return missing_note
    if _summary_already_mentions_missing_artifact(text, requested_extension):
        return text
    if _group_has_failed_or_cancelled_tasks(group):
        separator = "" if text.endswith((".", "!", "?")) else "."
        return f"{text}{separator} Also, {missing_note}"
    return missing_note


def _summary_already_mentions_missing_artifact(summary: str, requested_extension: str) -> bool:
    normalized = _normalize_requested_extension(requested_extension) or str(requested_extension)
    haystack = str(summary or "").strip().lower()
    return (
        f"requested {normalized.lower()} file" in haystack
        and "artifact was produced for this run" in haystack
    )


def _group_has_failed_or_cancelled_tasks(group: TaskGroup) -> bool:
    return any(
        task.status in {TaskStatus.FAILED, TaskStatus.CANCELLED}
        for task in (group.tasks or ())
    )


def _artifact_roots_for_group(group: TaskGroup) -> tuple[Path, ...]:
    roots: list[Path] = []
    for task in group.tasks:
        principal_id = str(getattr(task, "principal_id", "") or "").strip()
        if not principal_id:
            continue
        try:
            roots.append(artifact_root_for_principal(principal_id))
        except Exception:
            logger.debug("Could not resolve artifact root for task %s", getattr(task, "task_id", None), exc_info=True)
    if not roots:
        try:
            roots.append(artifact_root_for_principal(group.conversation_id))
        except Exception:
            logger.debug("Could not resolve fallback artifact root for group %s", group.group_id, exc_info=True)
    return tuple(dict.fromkeys(root.expanduser().resolve() for root in roots))


def _task_has_artifact_delivery_scope(task: TaskRecord) -> bool:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    if bool(metadata.get("requires_artifact_delivery") or metadata.get("required_artifact_kind")):
        return True
    artifact_role = str(metadata.get("artifact_role") or "").strip()
    return artifact_role in _ARTIFACT_DELIVERY_ROLES


def _status_lines_for_active_artifact_recovery(group: TaskGroup) -> dict[str, str]:
    tasks = list(getattr(group, "tasks", ()) or ())
    if not tasks:
        return {}
    producer_indexes = [
        index
        for index, task in enumerate(tasks)
        if _task_has_artifact_producer_scope(task)
        and task.status in {TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.COMPLETE}
    ]
    if not producer_indexes:
        return {}
    active_ids = _active_artifact_workflow_task_ids(group, producer_indexes=producer_indexes)
    if not active_ids:
        return {}
    lines: dict[str, str] = {}
    for task in tasks:
        if task.task_id not in active_ids:
            continue
        if _task_has_artifact_delivery_scope(task) and task.status in {TaskStatus.FAILED, TaskStatus.CANCELLED}:
            lines[task.task_id] = format_task_status_line(task, status=TaskStatus.PENDING)
        elif _task_has_artifact_source_scope(task) and task.status in {TaskStatus.FAILED, TaskStatus.CANCELLED}:
            lines[task.task_id] = format_task_status_line(task, status=TaskStatus.COMPLETE)
    return lines


def artifact_status_line_overrides_for_group(group: TaskGroup) -> dict[str, str]:
    """Return display-only status overrides for active artifact workflows."""
    return _status_lines_for_active_artifact_recovery(group)


def _active_artifact_workflow_task_ids(group: TaskGroup, *, producer_indexes: list[int]) -> set[str]:
    tasks = list(getattr(group, "tasks", ()) or ())
    task_by_id = {task.task_id: task for task in tasks}
    active_ids: set[str] = set()
    for index in producer_indexes:
        task = tasks[index]
        active_ids.add(task.task_id)
        active_ids.update(_task_dependency_ancestors(task_by_id, task.task_id))
    for task in tasks:
        if _task_has_artifact_delivery_scope(task):
            active_ids.add(task.task_id)
            active_ids.update(_task_dependency_ancestors(task_by_id, task.task_id))
    disposition = str((getattr(group, "planner_metadata", None) or {}).get("disposition") or "").strip().lower()
    if disposition == "sequential_mission":
        last_producer_index = max(producer_indexes)
        for task in tasks[: last_producer_index + 1]:
            if _task_has_artifact_source_scope(task) or _task_has_artifact_producer_scope(task):
                active_ids.add(task.task_id)
    return active_ids


def _status_lines_for_delivered_artifacts(group: TaskGroup, delivered_artifacts: list[str] | tuple[str, ...]) -> dict[str, str]:
    if not any(str(path or "").strip() for path in delivered_artifacts or ()):
        return {}
    task_ids = _delivered_artifact_workflow_task_ids(group)
    lines: dict[str, str] = {}
    for task in getattr(group, "tasks", ()) or ():
        if task.task_id in task_ids:
            lines[task.task_id] = format_task_status_line(task, status=TaskStatus.COMPLETE)
    return lines


def _delivered_artifact_workflow_task_ids(group: TaskGroup) -> set[str]:
    tasks = list(getattr(group, "tasks", ()) or ())
    if not tasks:
        return set()
    task_by_id = {task.task_id: task for task in tasks}
    artifact_task_ids: set[str] = set()
    producer_indexes: list[int] = []
    for index, task in enumerate(tasks):
        if _task_has_artifact_delivery_scope(task):
            artifact_task_ids.add(task.task_id)
        if _task_is_successful_artifact_producer(task):
            artifact_task_ids.add(task.task_id)
            producer_indexes.append(index)
    for task_id in tuple(artifact_task_ids):
        artifact_task_ids.update(_task_dependency_ancestors(task_by_id, task_id))
    disposition = str((getattr(group, "planner_metadata", None) or {}).get("disposition") or "").strip().lower()
    if disposition == "sequential_mission" and producer_indexes:
        last_producer_index = max(producer_indexes)
        for task in tasks[: last_producer_index + 1]:
            if _task_has_artifact_source_scope(task) or task.status in {TaskStatus.COMPLETE, TaskStatus.FAILED}:
                artifact_task_ids.add(task.task_id)
    return artifact_task_ids


def _task_dependency_ancestors(task_by_id: dict[str, TaskRecord], task_id: str) -> set[str]:
    ancestors: set[str] = set()
    pending = list(getattr(task_by_id.get(task_id), "dependencies", None) or ())
    while pending:
        dep_id = str(pending.pop() or "").strip()
        if not dep_id or dep_id in ancestors:
            continue
        dep = task_by_id.get(dep_id)
        if dep is None:
            continue
        ancestors.add(dep_id)
        pending.extend(str(item or "").strip() for item in getattr(dep, "dependencies", None) or ())
    return ancestors


def _task_is_successful_artifact_producer(task: TaskRecord) -> bool:
    result = getattr(task, "result", None)
    if getattr(result, "status", None) != "success":
        return False
    if _task_allowed_tools(task) & _ARTIFACT_PRODUCER_TOOLS:
        return True
    return bool([path for path in (getattr(result, "artifacts", None) or ()) if isinstance(path, str) and path.strip()])


def _task_has_artifact_source_scope(task: TaskRecord) -> bool:
    return bool(_task_allowed_tools(task) & _ARTIFACT_SOURCE_TOOLS)


def _task_has_artifact_producer_scope(task: TaskRecord) -> bool:
    return bool(_task_allowed_tools(task) & _ARTIFACT_PRODUCER_TOOLS)


def _task_allowed_tools(task: TaskRecord) -> set[str]:
    names = set(str(tool or "").strip().lower() for tool in (getattr(task, "allowed_tools", None) or ()))
    metadata = getattr(task, "metadata", None)
    if isinstance(metadata, dict):
        names.update(str(tool or "").strip().lower() for tool in (metadata.get("allowed_tools") or ()))
    return {name for name in names if name}


def _leaf_task_ids(group: TaskGroup) -> set[str]:
    dependency_ids: set[str] = set()
    task_ids: set[str] = set()
    for task in group.tasks:
        task_id = str(getattr(task, "task_id", "") or "").strip()
        if task_id:
            task_ids.add(task_id)
        dependency_ids.update(
            str(dep).strip()
            for dep in (getattr(task, "dependencies", None) or ())
            if str(dep).strip()
        )
    return task_ids - dependency_ids


def _task_result_text_fragments(result: Any) -> tuple[str, ...]:
    fragments: list[str] = []
    for value in (
        getattr(result, "output", None),
        getattr(result, "error", None),
    ):
        text = _task_result_text(value)
        if text:
            fragments.append(text)
    return tuple(dict.fromkeys(fragments))


def _task_result_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (dict, list, tuple)):
        try:
            import json

            return json.dumps(value, ensure_ascii=False, default=str)
        except TypeError:
            return str(value)
    return str(value).strip()


def _existing_artifacts_referenced_by_text(text: str, *, artifact_roots: tuple[Path, ...]) -> list[str]:
    if not text or not artifact_roots:
        return []
    paths: list[str] = []
    for candidate in media_candidate_paths_from_text(text):
        paths.extend(_resolve_candidate_artifact_path(candidate, artifact_roots=artifact_roots))
    for name in _filename_tokens_from_text(text):
        for root in artifact_roots:
            paths.extend(_resolve_candidate_artifact_path(root / name, artifact_roots=(root,)))
    return list(dict.fromkeys(paths))


def _existing_media_artifacts_referenced_by_text(text: str, *, artifact_roots: tuple[Path, ...]) -> list[str]:
    if not text or not artifact_roots:
        return []
    paths: list[str] = []
    for candidate in media_candidate_paths_from_text(text):
        paths.extend(_resolve_candidate_artifact_path(candidate, artifact_roots=artifact_roots))
    return list(dict.fromkeys(paths))


def _filename_tokens_from_text(text: str) -> tuple[str, ...]:
    names: list[str] = []
    for match in _FILENAME_TOKEN_RE.finditer(str(text or "")):
        raw_name = match.group(1).strip().strip("`'\"<>.,;:!?)(")
        candidates = [raw_name]
        if any(char.isspace() for char in raw_name):
            candidates.append(raw_name.split()[-1])
        for candidate in candidates:
            name = Path(candidate.strip().strip("`'\"<>.,;:!?)(")).name
            if name and Path(name).suffix:
                names.append(name)
    return tuple(dict.fromkeys(names))


def _resolve_candidate_artifact_path(candidate: Path, *, artifact_roots: tuple[Path, ...]) -> list[str]:
    paths: list[str] = []
    for root in artifact_roots:
        path = candidate
        if not path.is_absolute():
            path = root / path.name
        descriptor = artifact_descriptor_for_path(path, artifact_root=root)
        if descriptor is not None:
            paths.append(descriptor.path)
    return list(dict.fromkeys(paths))


def _is_scheduled_task_request(message: str) -> bool:
    text = str(message or "").lstrip()
    return text.startswith("[Scheduled task: ") or text.startswith("[Manual scheduled task run: ")


def _compact_original_request(message: str, *, limit: int = 120) -> str:
    text = " ".join(str(message or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _task_status_rank(line: str | None) -> int:
    text = str(line or "").lstrip()
    if text.startswith("☑"):
        return 4
    if text.startswith(("✕", "⊘")):
        return 3
    if text.startswith(("◐", "▤")):
        return 2
    if text.startswith(("☐", "▣")):
        return 1
    return 0


def _should_replace_task_status_line(current: str | None, candidate: str | None) -> bool:
    if current is None:
        return bool(str(candidate or "").strip())
    current_rank = _task_status_rank(current)
    candidate_rank = _task_status_rank(candidate)
    if candidate_rank > current_rank:
        return True
    if candidate_rank < current_rank:
        return False
    return len(str(candidate or "").strip()) > len(str(current or "").strip())


class _ResultAggregationState(TypedDict, total=False):
    aggregator: ResultAggregator
    update: ProgressUpdate
    task: TaskRecord | None
    group: TaskGroup | None
    group_state: GroupState | None


def _result_aggregation_load_node(state: _ResultAggregationState) -> dict[str, object]:
    aggregator = state["aggregator"]
    update = state["update"]
    task = aggregator._registry.get_task(update.task_id)
    group = aggregator._registry.get_group(update.group_id)
    return {"task": task, "group": group}


def _result_aggregation_route_after_load(state: _ResultAggregationState) -> str:
    return "ensure_group_state" if state.get("group") is not None else END


def _result_aggregation_ensure_group_state_node(state: _ResultAggregationState) -> dict[str, object]:
    aggregator = state["aggregator"]
    update = state["update"]
    group = state["group"]
    if group is None:
        return {"group_state": None}
    if update.group_id not in aggregator._group_state:
        aggregator._group_state[update.group_id] = GroupState(
            group_id=update.group_id,
            conversation_id=group.conversation_id,
            original_message=group.original_message,
        )
    return {"group_state": aggregator._group_state[update.group_id]}


def _result_aggregation_route_kind(state: _ResultAggregationState) -> str:
    kind = state["update"].kind
    if kind in {
        "progress_note",
        "tool_activity",
        "task_started",
        "task_complete",
        "task_failed",
        "task_cancelled",
        "input_needed",
        "approval_needed",
    }:
        return kind
    return END


async def _result_aggregation_progress_node(state: _ResultAggregationState) -> dict[str, object]:
    await state["aggregator"]._debounced_progress(
        state["group_state"],
        state["update"],
        state.get("task"),
    )
    return {}


async def _result_aggregation_tool_activity_node(state: _ResultAggregationState) -> dict[str, object]:
    await state["aggregator"]._deliver_tool_activity(
        state["group_state"],
        state["update"],
        state.get("task"),
    )
    return {}


async def _result_aggregation_task_started_node(state: _ResultAggregationState) -> dict[str, object]:
    update = state["update"]
    task = state.get("task")
    group = state["group"]
    gs = state["group_state"]
    if task:
        gs.status_lines[update.task_id] = format_task_status_line(task, status=TaskStatus.RUNNING)
    await state["aggregator"]._deliver_status(gs, group)
    return {}


async def _result_aggregation_task_complete_node(state: _ResultAggregationState) -> dict[str, object]:
    update = state["update"]
    task = state.get("task")
    group = state["group"]
    gs = state["group_state"]
    if task and not task.is_terminal():
        return {}
    if task:
        gs.status_lines[update.task_id] = format_task_status_line(task, status=TaskStatus.COMPLETE)
    await state["aggregator"]._deliver_status(gs, group)
    if group.all_terminal():
        await state["aggregator"]._on_group_complete(gs, group)
    return {}


async def _result_aggregation_task_failed_node(state: _ResultAggregationState) -> dict[str, object]:
    update = state["update"]
    task = state.get("task")
    group = state["group"]
    gs = state["group_state"]
    if task and not task.is_terminal():
        return {}
    if task:
        gs.status_lines[update.task_id] = format_task_status_line(
            task,
            status=TaskStatus.FAILED,
            detail=update.message or "failed",
        )
    await state["aggregator"]._deliver_status(gs, group)
    if group.all_terminal():
        await state["aggregator"]._on_group_complete(gs, group)
    return {}


async def _result_aggregation_task_cancelled_node(state: _ResultAggregationState) -> dict[str, object]:
    task = state.get("task")
    group = state["group"]
    gs = state["group_state"]
    if task:
        gs.status_lines[state["update"].task_id] = (
            f"{format_task_status_line(task, status=TaskStatus.CANCELLED)} (cancelled)"
        )
    if group.all_terminal():
        await state["aggregator"]._on_group_complete(gs, group)
    return {}


async def _result_aggregation_input_needed_node(state: _ResultAggregationState) -> dict[str, object]:
    update = state["update"]
    gs = state["group_state"]
    message = str(update.message or "Input needed").strip()
    dedupe_key = f"{update.task_id}:{message}"
    if dedupe_key in gs.delivered_input_questions:
        return {}
    gs.delivered_input_questions.add(dedupe_key)
    await state["aggregator"]._deliver(
        gs.conversation_id,
        f"? {message}",
        is_question=True,
    )
    return {}


async def _result_aggregation_approval_needed_node(state: _ResultAggregationState) -> dict[str, object]:
    update = state["update"]
    gs = state["group_state"]
    await state["aggregator"]._deliver(
        gs.conversation_id,
        update.message or "Approval required before this task can continue.",
        is_status=True,
        group_id=gs.group_id,
        status_kind="approval_needed",
    )
    return {}


@lru_cache(maxsize=1)
def _compiled_result_aggregation_graph():
    graph = StateGraph(_ResultAggregationState)
    graph.add_node("load", _result_aggregation_load_node)
    graph.add_node("ensure_group_state", _result_aggregation_ensure_group_state_node)
    graph.add_node("progress_note", _result_aggregation_progress_node)
    graph.add_node("tool_activity", _result_aggregation_tool_activity_node)
    graph.add_node("task_started", _result_aggregation_task_started_node)
    graph.add_node("task_complete", _result_aggregation_task_complete_node)
    graph.add_node("task_failed", _result_aggregation_task_failed_node)
    graph.add_node("task_cancelled", _result_aggregation_task_cancelled_node)
    graph.add_node("input_needed", _result_aggregation_input_needed_node)
    graph.add_node("approval_needed", _result_aggregation_approval_needed_node)
    graph.add_edge(START, "load")
    graph.add_conditional_edges("load", _result_aggregation_route_after_load, {"ensure_group_state": "ensure_group_state", END: END})
    graph.add_conditional_edges(
        "ensure_group_state",
        _result_aggregation_route_kind,
        {
            "progress_note": "progress_note",
            "tool_activity": "tool_activity",
            "task_started": "task_started",
            "task_complete": "task_complete",
            "task_failed": "task_failed",
            "task_cancelled": "task_cancelled",
            "input_needed": "input_needed",
            "approval_needed": "approval_needed",
            END: END,
        },
    )
    for node in (
        "progress_note",
        "tool_activity",
        "task_started",
        "task_complete",
        "task_failed",
        "task_cancelled",
        "input_needed",
        "approval_needed",
    ):
        graph.add_edge(node, END)
    return graph.compile()


def _planner_summary_from_group(group: TaskGroup) -> str:
    metadata = getattr(group, "planner_metadata", None)
    if not isinstance(metadata, dict):
        return ""
    disposition = str(metadata.get("disposition") or "").strip()
    if not disposition:
        return ""
    if bool(metadata.get("needs_clarification")):
        return "Needs clarification"
    if not bool(metadata.get("valid", True)):
        return "Fallback to normal turn"
    label = disposition.replace("_", " ").title()
    tasks = metadata.get("tasks")
    task_count = len(tasks) if isinstance(tasks, list) else len(getattr(group, "tasks", ()) or ())
    if task_count:
        return f"{label} • {task_count} task{'s' if task_count != 1 else ''}"
    return label


__all__ = ["ResultAggregator", "GroupState", "DeliverFn", "artifact_status_line_overrides_for_group"]
