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
import logging
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
from nullion.attachment_format_graph import plan_attachment_format
from nullion.delegated_artifact_workflow import finalize_delegated_artifacts
from nullion.mini_agent_runner import ProgressUpdate
from nullion.task_queue import TaskGroup, TaskRecord, TaskRegistry, TaskStatus
from nullion.task_status_format import format_task_status_line, format_task_status_summary

logger = logging.getLogger(__name__)

_SUMMARY_SYSTEM_PROMPT = """\
You are summarizing the results of parallel sub-tasks for the user.
Given the original request and a list of completed task outputs, write a concise, \
direct reply (1–4 sentences) that answers the original request. \
Mention any failures. Include file paths for any artifacts produced. \
Do not enumerate every step — just deliver the answer. Speak as the main agent."""

# Minimum seconds between consecutive progress-note edits for the same group.
MIN_PROGRESS_INTERVAL_S: float = 3.0
_FILENAME_TOKEN_RE = re.compile(r"(?<![\w./-])([A-Za-z0-9][\w .()@+-]{0,180}\.[A-Za-z0-9]{1,16})(?![\w./-])")
_ARTIFACT_DELIVERY_TOOLS = frozenset(
    {
        "browser_screenshot",
        "file_write",
        "image_generate",
        "pdf_create",
        "pdf_edit",
        "render",
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

    async def _deliver_status(self, gs: GroupState, group: TaskGroup) -> None:
        """Emit the current status summary for the group."""
        planner_summary = _planner_summary_from_group(group)
        for task in group.tasks:
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
                if current is None or _task_status_rank(next_line) >= _task_status_rank(current):
                    gs.status_lines[task.task_id] = next_line
        delivered = await self._deliver(
            gs.conversation_id,
            format_task_status_summary(
                group.tasks,
                planner_summary=planner_summary,
                subject=group.original_message,
                status_lines=gs.status_lines,
                default_status=TaskStatus.PENDING,
            ),
            is_status=True,
            group_id=group.group_id,
            status_kind="task_summary",
        )
        if delivered:
            gs.task_summary_visible = True

    async def _on_group_complete(self, gs: GroupState, group: TaskGroup) -> None:
        """All tasks terminal — generate and deliver the final summary."""
        if group.group_id in self._completed_groups:
            return
        self._completed_groups.add(group.group_id)
        recovered_artifacts = finalize_delegated_artifacts(group)
        summary = (
            _artifact_recovery_summary(group, recovered_artifacts)
            if recovered_artifacts
            else await self._generate_summary(group)
        )
        requested_extension = _requested_attachment_extension_for_group(group, model_client=self._model_client)
        deliverable_artifacts = _artifact_paths_for_group_delivery(
            group,
            recovered_artifacts,
            summary=summary,
            requested_extension=requested_extension,
        )
        if requested_extension and not deliverable_artifacts:
            summary = _missing_requested_artifact_summary(group, requested_extension)
        await self._deliver(
            gs.conversation_id,
            _summary_with_original_request_context(group, summary),
            group_id=gs.group_id,
        )

        # Deliver any artifacts.
        for artifact in deliverable_artifacts:
            await self._deliver(gs.conversation_id, artifact, is_artifact=True, group_id=gs.group_id)

        # Clean up group state.
        self._group_state.pop(gs.group_id, None)

    async def _generate_summary(self, group: TaskGroup) -> str:
        """One-shot LLM call to synthesize a coherent final reply."""
        if self._model_client is None:
            return self._fallback_summary(group)

        task_summaries = []
        for task in group.tasks:
            if task.result:
                status_label = "✓" if task.result.status == "success" else "✗"
                output = (task.result.output or task.result.error or "(no output)")[:300]
                task_summaries.append(f"{status_label} {task.title}: {output}")
            else:
                task_summaries.append(f"— {task.title}: (no result)")

        prompt = (
            f"Original request: {group.original_message}\n\n"
            + "\n".join(task_summaries)
        )
        try:
            response = self._model_client.create(
                messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
                tools=[],
                max_tokens=512,
                system=_SUMMARY_SYSTEM_PROMPT,
            )
            content = response.get("content") or []
            parts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
            text = "".join(parts).strip()
            return text or self._fallback_summary(group)
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
        for task in group.tasks:
            if task.result and task.result.output:
                parts.append(f"• {task.title}: {task.result.output[:120]}")
        return " ".join(parts)

    @staticmethod
    def _fallback_summary_is_bare_failure_notice(group: TaskGroup) -> bool:
        failed = sum(1 for task in group.tasks if task.status == TaskStatus.FAILED)
        if failed <= 0:
            return False
        return not any(
            task.result and (task.result.output or task.result.artifacts)
            for task in group.tasks
        )

    async def _deliver(self, conversation_id: str, text: str, **kwargs) -> bool:
        try:
            result = self._deliver_fn(conversation_id, text, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result is not False
        except Exception as exc:
            logger.debug("ResultAggregator: deliver_fn failed: %s", exc)
            return False


def _summary_with_original_request_context(group: TaskGroup, summary: str) -> str:
    text = str(summary or "").strip()
    if _is_scheduled_task_request(group.original_message):
        return text
    request = _compact_original_request(group.original_message)
    if not text or not request:
        return text
    if text.lower().startswith("result for "):
        return text
    return f'Result for "{request}":\n{text}'


def _artifact_recovery_summary(group: TaskGroup, artifact_paths: list[str]) -> str:
    if len(artifact_paths) == 1:
        return f"I created the requested file from the completed task output and attached it: {artifact_paths[0]}"
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
    all_text_paths: list[str] = []
    for task in group.tasks:
        result = task.result
        if result is None:
            continue
        task_paths = [str(path) for path in (result.artifacts or []) if isinstance(path, str) and path.strip()]
        explicit_paths.extend(task_paths)
        if _task_has_artifact_delivery_scope(task):
            artifact_task_paths.extend(task_paths)
    explicit_paths.extend(str(path) for path in (recovered_artifacts or []) if isinstance(path, str) and path.strip())
    artifact_task_paths.extend(str(path) for path in (recovered_artifacts or []) if isinstance(path, str) and path.strip())
    roots = _artifact_roots_for_group(group)
    summary_paths = _existing_artifacts_referenced_by_text(str(summary or ""), artifact_roots=roots)
    for task in group.tasks:
        result = task.result
        if result is None:
            continue
        for text in _task_result_text_fragments(result):
            found = _existing_artifacts_referenced_by_text(text, artifact_roots=roots)
            all_text_paths.extend(found)
            if _task_has_artifact_delivery_scope(task):
                artifact_task_paths.extend(found)
    requested_extension = _normalize_requested_extension(requested_extension)
    candidate_groups = (
        summary_paths,
        artifact_task_paths,
        explicit_paths,
        all_text_paths,
    )
    if requested_extension:
        for candidates in candidate_groups:
            matching = _filter_artifact_paths_by_extension(candidates, requested_extension)
            if matching:
                return matching
        return []
    for candidates in candidate_groups:
        if candidates:
            return list(dict.fromkeys(candidates))
    return []


def _requested_attachment_extension_for_group(group: TaskGroup, *, model_client: Any | None) -> str | None:
    del model_client
    try:
        return plan_attachment_format(group.original_message, model_client=None).extension
    except Exception:
        logger.debug("Could not plan requested attachment extension for group %s", group.group_id, exc_info=True)
        return None


def _normalize_requested_extension(extension: str | None) -> str | None:
    text = str(extension or "").strip().lower()
    if not text:
        return None
    return text if text.startswith(".") else f".{text}"


def _filter_artifact_paths_by_extension(paths: list[str] | tuple[str, ...], extension: str) -> list[str]:
    normalized = _normalize_requested_extension(extension)
    if normalized is None:
        return list(dict.fromkeys(paths))
    return list(
        dict.fromkeys(
            path for path in paths if Path(str(path)).suffix.lower() == normalized
        )
    )


def _missing_requested_artifact_summary(group: TaskGroup, requested_extension: str) -> str:
    normalized = _normalize_requested_extension(requested_extension) or str(requested_extension)
    return (
        f"I could not attach the requested {normalized} file because no verified "
        f"{normalized} artifact was produced for this run."
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
    allowed_tools = {str(tool) for tool in (getattr(task, "allowed_tools", None) or [])}
    return bool(allowed_tools.intersection(_ARTIFACT_DELIVERY_TOOLS))


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
    if text.startswith(("☑", "✕", "⊘")):
        return 3
    if text.startswith(("◐", "▤")):
        return 2
    if text.startswith(("☐", "▣")):
        return 1
    return 0


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
    await state["aggregator"]._deliver(
        gs.conversation_id,
        f"? {update.message or 'Input needed'}",
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


__all__ = ["ResultAggregator", "GroupState", "DeliverFn"]
