"""Task decomposer — breaks a user message into parallel TaskRecords via one LLM call.

The decomposer sends a single structured prompt to the model and parses the
JSON response into a list of TaskRecord objects with dependency edges set up
as a DAG. A single-task response is the fast path — no async overhead is added
for simple queries.

Usage::

    decomposer = TaskDecomposer(model_client=client)
    tasks = decomposer.decompose(
        user_message="fetch example.com, summarize it, then email me",
        group_id="grp-abc",
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        available_tools=["web_fetch", "email_send"],
    )
"""
from __future__ import annotations

import inspect
import json
import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.deep_agent_profiles import deep_agent_skills_for_task, deep_agent_subagents_for_task
from nullion.task_planner import strip_composer_mode_instruction
from nullion.task_queue import (
    TaskGroup,
    TaskPriority,
    TaskRecord,
    TaskStatus,
    make_group_id,
    make_task_id,
)

logger = logging.getLogger(__name__)

_DECOMPOSE_SYSTEM_PROMPT = """You are a task decomposer for the Nullion agent system.
Given a user request and a list of available tools, break the request into discrete, parallelizable tasks.

Output ONLY one JSON object (no markdown fences, no commentary):
{
  "disposition": "single_turn" | "clarification" | "sequential_mission" | "parallel_mission",
  "needs_clarification": boolean,
  "clarification_question": string or null,
  "tasks": [
    {
      "title": string, ≤ 50 chars, human-readable task name,
      "description": string, full goal with enough context to act independently,
      "tool_scope": array of strings, subset of available tools needed (empty = reasoning only),
      "priority": "urgent" | "high" | "normal" | "low",
      "dependencies": array of 0-based indices of tasks that MUST complete before this one,
      "context_key_in": string or null — context bus key to read from,
      "context_key_out": string or null — context bus key to write result to,
      "required_inputs": array of missing input names,
      "can_start": boolean
    }
  ]
}

Rules:
- Tasks that can run in parallel must have no dependency between them.
- If task B needs task A's output, set dependencies=[A_index] and matching context keys.
- Discovery must precede interpretation: if one task finds/locates/lists config,
  files, records, IDs, or data and another task audits/summarizes/changes that
  found material, the second task depends on the discovery task.
- For a single-step request, output one task with disposition="single_turn".
- Merge steps that use the same tools and have no natural split point.
- Do not split clarification from execution. If the request is one action but lacks
  information needed to complete it, output one task whose description asks the
  main turn to resolve the missing information.
- Conversational setup, corrections, or resets attached to one concrete action
  are context for that action, not separate tasks.
- If any required input is missing, set disposition="clarification",
  needs_clarification=true, write one clarification_question, and do not split
  future execution into a runnable parallel task.
- Only set disposition="parallel_mission" when at least two tasks can run
  independently now.
- context_key_in of a task must equal context_key_out of one of its dependencies.
- Do not fabricate tools — only use names from the available_tools list.

Example output for "fetch example.com, summarize it, email me":
{
  "disposition": "sequential_mission",
  "needs_clarification": false,
  "clarification_question": null,
  "tasks": [
    {"title": "Fetch example.com", "description": "Retrieve https://example.com/ HTML.", "tool_scope": ["web_fetch"], "priority": "normal", "dependencies": [], "context_key_in": null, "context_key_out": "page_html", "required_inputs": [], "can_start": true},
    {"title": "Summarize content", "description": "Read page_html from context. Extract 3-5 key points.", "tool_scope": [], "priority": "normal", "dependencies": [0], "context_key_in": "page_html", "context_key_out": "summary", "required_inputs": [], "can_start": true},
    {"title": "Email summary", "description": "Send the summary from context to the user's email.", "tool_scope": ["email_send"], "priority": "normal", "dependencies": [1], "context_key_in": "summary", "context_key_out": null, "required_inputs": [], "can_start": true}
  ]
}"""


@dataclass
class DecomposedTask:
    """Intermediate representation before TaskRecord IDs are assigned."""
    title: str
    description: str
    tool_scope: list[str]
    priority: TaskPriority
    dep_indices: list[int]             # indices into the raw decomposed list
    context_key_in: str | None
    context_key_out: str | None
    required_inputs: list[str] | None = None
    can_start: bool = True


@dataclass
class DagPlan:
    disposition: str
    tasks: list[DecomposedTask]
    needs_clarification: bool = False
    clarification_question: str | None = None
    validation_errors: list[str] | None = None

    @property
    def is_valid(self) -> bool:
        return not self.validation_errors

    @property
    def can_dispatch(self) -> bool:
        return (
            self.is_valid
            and not self.needs_clarification
            and self.disposition in {"sequential_mission", "parallel_mission"}
            and len(self.tasks) > 1
            and all(task.can_start and not task.required_inputs for task in self.tasks)
        )


class _DagPlanningState(TypedDict, total=False):
    model_client: Any
    user_message: str
    available_tools: list[str]
    raw_text: str | None
    parsed_plan: DagPlan | None
    dag_plan: DagPlan


class TaskDecomposer:
    """Decomposes a user message into a list of TaskRecords via one LLM call."""

    def __init__(self, model_client: Any) -> None:
        self._model_client = model_client

    def decompose(
        self,
        user_message: str,
        *,
        group_id: str | None = None,
        conversation_id: str,
        principal_id: str,
        available_tools: list[str],
    ) -> TaskGroup:
        """Decompose *user_message* into a TaskGroup.

        Returns a TaskGroup with status=PENDING/BLOCKED on each task.
        For single-task results, the one task has status=QUEUED (fast path).
        """
        gid = group_id or make_group_id()
        normalized_message = strip_composer_mode_instruction(user_message)
        dag_plan = self.plan_dag(normalized_message, available_tools=available_tools)
        raw_tasks = dag_plan.tasks if dag_plan.can_dispatch or (dag_plan.is_valid and len(dag_plan.tasks) == 1) else []

        if not raw_tasks:
            # Fallback: treat the whole message as one task
            raw_tasks = [DecomposedTask(
                title=normalized_message[:50],
                description=normalized_message,
                tool_scope=list(available_tools),
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out=None,
                required_inputs=[],
                can_start=True,
            )]

        # Assign IDs in order so dependency indices can be resolved.
        task_ids = [make_task_id() for _ in raw_tasks]

        records: list[TaskRecord] = []
        for i, dt in enumerate(raw_tasks):
            dep_ids = [task_ids[j] for j in dt.dep_indices if 0 <= j < len(task_ids)]
            if len(raw_tasks) == 1:
                initial_status = TaskStatus.QUEUED   # single-task fast path
            elif dep_ids:
                initial_status = TaskStatus.BLOCKED
            else:
                initial_status = TaskStatus.QUEUED

            record = TaskRecord(
                task_id=task_ids[i],
                group_id=gid,
                conversation_id=conversation_id,
                principal_id=principal_id,
                title=dt.title[:50],
                description=dt.description,
                status=initial_status,
                priority=dt.priority,
                allowed_tools=dt.tool_scope,
                dependencies=dep_ids,
                context_key_in=dt.context_key_in,
                context_key_out=dt.context_key_out,
            )
            record.deep_agent_skills = deep_agent_skills_for_task(record)
            record.deep_agent_subagents = deep_agent_subagents_for_task(record)
            records.append(record)

        group = TaskGroup(
            group_id=gid,
            conversation_id=conversation_id,
            original_message=normalized_message,
            tasks=records,
            planner_metadata=_planner_metadata(dag_plan),
        )
        logger.info(
            "TaskDecomposer: group=%s disposition=%s valid=%s dispatchable=%s tasks=%d",
            gid,
            dag_plan.disposition,
            dag_plan.is_valid,
            dag_plan.can_dispatch,
            len(records),
        )
        return group

    def plan_dag(self, user_message: str, *, available_tools: list[str]) -> DagPlan:
        """Return a validated model-generated DAG plan.

        Invalid, ambiguous, or clarification-seeking plans are intentionally
        non-dispatchable; callers can fall back to a normal single model turn.
        """
        final_state = _compiled_dag_planning_graph().invoke(
            {
                "model_client": self._model_client,
                "user_message": user_message,
                "available_tools": list(available_tools),
            },
            config={"configurable": {"thread_id": "task-decomposer-dag-planning"}},
        )
        dag_plan = final_state.get("dag_plan")
        if isinstance(dag_plan, DagPlan):
            return dag_plan
        return DagPlan(
            disposition="single_turn",
            tasks=[],
            validation_errors=["planner graph returned no plan"],
        )

    # ── Private ────────────────────────────────────────────────────────────

    def _call_model(
        self, user_message: str, *, available_tools: list[str]
    ) -> DagPlan | None:
        raw_text = _call_decomposer_model_text(self._model_client, user_message, available_tools=available_tools)
        if raw_text is None:
            return None
        return _parse_dag_plan(raw_text)


def _call_decomposer_model_text(model_client: Any, user_message: str, *, available_tools: list[str]) -> str | None:
    tools_str = ", ".join(available_tools) if available_tools else "(none)"
    prompt = (
        f"Available tools: {tools_str}\n\n"
        f"User request: {user_message}"
    )
    messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
    create_kwargs: dict[str, Any] = {
        "messages": messages,
        "tools": [],
    }
    try:
        params = inspect.signature(model_client.create).parameters
    except (TypeError, ValueError):
        params = {}
    accepts_extra = any(param.kind is inspect.Parameter.VAR_KEYWORD for param in params.values())
    if accepts_extra or "max_tokens" in params:
        create_kwargs["max_tokens"] = 1024
    if accepts_extra or "system" in params:
        create_kwargs["system"] = _DECOMPOSE_SYSTEM_PROMPT
    else:
        create_kwargs["messages"] = [
            {"role": "system", "content": _DECOMPOSE_SYSTEM_PROMPT},
            *messages,
        ]
    try:
        response = model_client.create(**create_kwargs)
    except Exception as exc:
        logger.warning("TaskDecomposer: model call failed: %s", exc)
        return None
    return _response_text(response)


def _dag_model_call_node(state: _DagPlanningState) -> dict[str, object]:
    return {
        "raw_text": _call_decomposer_model_text(
            state["model_client"],
            state["user_message"],
            available_tools=state.get("available_tools") or [],
        )
    }


def _dag_parse_node(state: _DagPlanningState) -> dict[str, object]:
    raw_text = state.get("raw_text")
    if not isinstance(raw_text, str) or not raw_text.strip():
        return {"parsed_plan": None}
    return {"parsed_plan": _parse_dag_plan(raw_text)}


def _dag_validate_node(state: _DagPlanningState) -> dict[str, object]:
    parsed_plan = state.get("parsed_plan")
    if parsed_plan is None:
        return {"dag_plan": DagPlan(
            disposition="single_turn",
            tasks=[],
            validation_errors=["planner returned no parseable plan"],
        )}
    return {
        "dag_plan": _validate_dag_plan(
            parsed_plan,
            available_tools=state.get("available_tools") or [],
        )
    }


@lru_cache(maxsize=1)
def _compiled_dag_planning_graph():
    graph = StateGraph(_DagPlanningState)
    graph.add_node("model_call", _dag_model_call_node)
    graph.add_node("parse", _dag_parse_node)
    graph.add_node("validate", _dag_validate_node)
    graph.add_edge(START, "model_call")
    graph.add_edge("model_call", "parse")
    graph.add_edge("parse", "validate")
    graph.add_edge("validate", END)
    return graph.compile()


# ── Parser ─────────────────────────────────────────────────────────────────────

def _response_text(response: Any) -> str:
    """Extract text from provider-normalized and provider-native response shapes."""
    if not isinstance(response, dict):
        return ""
    content = response.get("content") or []
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    raw_text = ""
    for block in content:
        if isinstance(block, str):
            raw_text += block
            continue
        if isinstance(block, dict):
            block_type = block.get("type")
            if block_type in {"text", "output_text"} or (
                block_type is None and isinstance(block.get("text"), str)
            ):
                raw_text += str(block.get("text", ""))
            continue
        text = getattr(block, "text", None)
        if isinstance(text, str):
            raw_text += text
    return raw_text


def _parse_dag_plan(raw: str) -> DagPlan | None:
    """Parse model output into a DagPlan.

    Legacy array output is still accepted so older tests and provider fixtures
    continue to exercise the same execution path.
    """
    parsed = _parse_json_payload(raw)
    if isinstance(parsed, list):
        tasks = _parse_decomposed_task_items(parsed)
        disposition = "single_turn" if len(tasks) <= 1 else _infer_legacy_disposition(tasks)
        return DagPlan(disposition=disposition, tasks=tasks)
    if not isinstance(parsed, dict):
        logger.debug("TaskDecomposer: no JSON object/array in response")
        return None
    tasks = _parse_decomposed_task_items(parsed.get("tasks") or [])
    disposition = str(parsed.get("disposition") or ("single_turn" if len(tasks) <= 1 else _infer_legacy_disposition(tasks)))
    clarification_question = parsed.get("clarification_question")
    return DagPlan(
        disposition=disposition,
        tasks=tasks,
        needs_clarification=bool(parsed.get("needs_clarification")),
        clarification_question=str(clarification_question) if clarification_question else None,
    )


def _parse_decomposed_tasks(raw: str) -> list[DecomposedTask]:
    """Parse the LLM's JSON response into DecomposedTask objects."""
    plan = _parse_dag_plan(raw)
    return list(plan.tasks) if plan is not None else []


def _parse_json_payload(raw: str) -> object | None:
    # Strip markdown fences if present
    text = raw.strip()
    fence_match = re.search(r"```(?:json)?\s*([\s\S]+?)```", text)
    if fence_match:
        text = fence_match.group(1).strip()
    start = _first_json_start(text)
    if start is None:
        return None
    try:
        payload, _ = json.JSONDecoder().raw_decode(text[start:])
        return payload
    except json.JSONDecodeError as exc:
        logger.debug("TaskDecomposer: JSON parse failed: %s", exc)
        return None


def _first_json_start(text: str) -> int | None:
    object_start = text.find("{")
    array_start = text.find("[")
    starts = [index for index in (object_start, array_start) if index >= 0]
    if not starts:
        return None
    return min(starts)


def _parse_decomposed_task_items(items: object) -> list[DecomposedTask]:
    if not isinstance(items, list):
        return []

    tasks: list[DecomposedTask] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "Task")[:50]
        description = str(item.get("description") or title)
        tool_scope = [str(t) for t in (item.get("tool_scope") or []) if isinstance(t, str)]
        priority_raw = item.get("priority", "normal")
        try:
            priority = TaskPriority(priority_raw)
        except ValueError:
            priority = TaskPriority.NORMAL
        dep_indices = [int(d) for d in (item.get("dependencies") or []) if isinstance(d, (int, float))]
        ctx_in = item.get("context_key_in")
        ctx_out = item.get("context_key_out")
        required_inputs = [
            str(value)
            for value in (item.get("required_inputs") or [])
            if isinstance(value, str) and value.strip()
        ]
        can_start = item.get("can_start", True)
        tasks.append(DecomposedTask(
            title=title,
            description=description,
            tool_scope=tool_scope,
            priority=priority,
            dep_indices=dep_indices,
            context_key_in=str(ctx_in) if ctx_in else None,
            context_key_out=str(ctx_out) if ctx_out else None,
            required_inputs=required_inputs,
            can_start=bool(can_start),
        ))
    return tasks


def _infer_legacy_disposition(tasks: list[DecomposedTask]) -> str:
    if len(tasks) <= 1:
        return "single_turn"
    if any(task.dep_indices for task in tasks):
        return "sequential_mission"
    return "parallel_mission"


def _validate_dag_plan(plan: DagPlan, *, available_tools: list[str], max_tasks: int = 20) -> DagPlan:
    errors: list[str] = []
    if plan.disposition not in {"single_turn", "clarification", "sequential_mission", "parallel_mission"}:
        errors.append(f"invalid disposition: {plan.disposition}")
    if not plan.tasks:
        errors.append("plan has no tasks")
    if len(plan.tasks) > max_tasks:
        errors.append(f"plan has too many tasks: {len(plan.tasks)}")
    available = set(available_tools)
    context_outputs = {
        task.context_key_out
        for task in plan.tasks
        if isinstance(task.context_key_out, str) and task.context_key_out
    }
    for index, task in enumerate(plan.tasks):
        unknown_tools = [tool for tool in task.tool_scope if tool not in available]
        if unknown_tools:
            errors.append(f"task {index} uses unknown tools: {', '.join(unknown_tools)}")
        for dep in task.dep_indices:
            if dep < 0 or dep >= len(plan.tasks):
                errors.append(f"task {index} has invalid dependency {dep}")
            if dep == index:
                errors.append(f"task {index} depends on itself")
        if task.context_key_in and task.context_key_in not in context_outputs:
            errors.append(f"task {index} reads missing context key: {task.context_key_in}")
        if task.context_key_in and not any(
            0 <= dep < len(plan.tasks) and plan.tasks[dep].context_key_out == task.context_key_in
            for dep in task.dep_indices
        ):
            errors.append(f"task {index} context_key_in is not produced by a dependency")
    if _has_cycle([task.dep_indices for task in plan.tasks]):
        errors.append("plan has a dependency cycle")
    if plan.disposition == "parallel_mission":
        independent = [task for task in plan.tasks if not task.dep_indices and task.can_start and not task.required_inputs]
        if len(independent) < 2:
            errors.append("parallel_mission needs at least two independently runnable tasks")
    if plan.needs_clarification and not plan.clarification_question:
        errors.append("clarification plan lacks clarification_question")
    return DagPlan(
        disposition=plan.disposition,
        tasks=plan.tasks,
        needs_clarification=plan.needs_clarification or plan.disposition == "clarification",
        clarification_question=plan.clarification_question,
        validation_errors=errors,
    )


def _planner_metadata(plan: DagPlan) -> dict[str, object]:
    return {
        "planner": "model_dag",
        "disposition": plan.disposition,
        "valid": plan.is_valid,
        "dispatchable": plan.can_dispatch,
        "needs_clarification": plan.needs_clarification,
        "clarification_question": plan.clarification_question,
        "validation_errors": list(plan.validation_errors or []),
        "tasks": [
            {
                "index": index,
                "title": task.title,
                "dependencies": list(task.dep_indices),
                "tool_scope": list(task.tool_scope),
                "context_key_in": task.context_key_in,
                "context_key_out": task.context_key_out,
                "required_inputs": list(task.required_inputs or []),
                "can_start": task.can_start,
            }
            for index, task in enumerate(plan.tasks)
        ],
    }


def _has_cycle(dependencies: list[list[int]]) -> bool:
    visiting: set[int] = set()
    visited: set[int] = set()

    def visit(index: int) -> bool:
        if index in visited:
            return False
        if index in visiting:
            return True
        visiting.add(index)
        for dep in dependencies[index]:
            if 0 <= dep < len(dependencies) and visit(dep):
                return True
        visiting.remove(index)
        visited.add(index)
        return False

    return any(visit(index) for index in range(len(dependencies)))


__all__ = ["DagPlan", "TaskDecomposer", "DecomposedTask"]
