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
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Callable, TypedDict

from langgraph.graph import END, START, StateGraph

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
        summary = await self._generate_summary(group)
        fallback = self._fallback_summary(group)
        if not (
            gs.task_summary_visible
            and summary == fallback
            and self._fallback_summary_is_bare_failure_notice(group)
        ):
            await self._deliver(gs.conversation_id, summary)

        # Deliver any artifacts.
        for task in group.tasks:
            if task.result and task.result.artifacts:
                for artifact in task.result.artifacts:
                    await self._deliver(gs.conversation_id, artifact, is_artifact=True)

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
        total = len(group.tasks)
        parts = [f"Completed {done}/{total} task(s)."]
        if failed:
            parts.append(f"{failed} failed.")
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


def _task_status_rank(line: str | None) -> int:
    text = str(line or "").lstrip()
    if text.startswith(("☑", "✕", "⊘")):
        return 3
    if text.startswith(("◐", "▣", "▤")):
        return 2
    if text.startswith("☐"):
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
    if kind in {"progress_note", "task_started", "task_complete", "task_failed", "task_cancelled", "input_needed", "approval_needed"}:
        return kind
    return END


async def _result_aggregation_progress_node(state: _ResultAggregationState) -> dict[str, object]:
    await state["aggregator"]._debounced_progress(
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
            "task_started": "task_started",
            "task_complete": "task_complete",
            "task_failed": "task_failed",
            "task_cancelled": "task_cancelled",
            "input_needed": "input_needed",
            "approval_needed": "approval_needed",
            END: END,
        },
    )
    for node in ("progress_note", "task_started", "task_complete", "task_failed", "task_cancelled", "input_needed", "approval_needed"):
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
