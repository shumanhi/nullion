"""Mini-agent runner — async agentic loop for a single scoped task.

Each mini-agent executes one TaskRecord in isolation. It has a restricted
tool scope, inherits the parent's principal_id (never escalates), and runs a
standard tool-use loop against the shared model client.

Two meta-tools are always available regardless of `allowed_tools`:
  - report_progress  — posts a short update visible to the user
  - request_user_input — pauses and asks the user a question

Usage (called by the orchestrator, not directly)::

    runner = MiniAgentRunner()
    result = await runner.run(config, anthropic_client=client,
                              tool_registry=registry, ...)
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Callable, TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.context_bus import ContextBus
from nullion.mini_agent_config import (
    mini_agent_max_continuations,
    mini_agent_max_iterations,
    mini_agent_timeout_seconds,
)
from nullion.response_sanitizer import sanitize_user_visible_reply
from nullion.task_queue import TaskRecord, TaskResult
from nullion.tools import ToolInvocation, ToolResult

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass
class MiniAgentConfig:
    agent_id: str
    task: TaskRecord
    context_in: Any | None = None          # pre-loaded from context bus
    max_iterations: int = field(default_factory=mini_agent_max_iterations)
    max_continuations: int = field(default_factory=mini_agent_max_continuations)
    timeout_s: float = field(default_factory=mini_agent_timeout_seconds)
    can_request_user_input: bool = True
    depth: int = 0                         # nesting depth (hard cap: 3)


# ── Meta-tool schemas (always injected, not in allowed_tools scope check) ──────

_REPORT_PROGRESS_SCHEMA = {
    "name": "report_progress",
    "description": "Report a short progress update to the user (≤ 100 chars).",
    "input_schema": {
        "type": "object",
        "properties": {
            "message": {"type": "string", "description": "Progress note, ≤ 100 chars."}
        },
        "required": ["message"],
    },
}

_REQUEST_USER_INPUT_SCHEMA = {
    "name": "request_user_input",
    "description": "Ask the user a question and wait for their answer. Use sparingly.",
    "input_schema": {
        "type": "object",
        "properties": {
            "question": {"type": "string"},
            "options": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional list of choices to show the user.",
            },
        },
        "required": ["question"],
    },
}

_META_TOOL_NAMES = {"report_progress", "request_user_input"}

_MINI_AGENT_SYSTEM_PROMPT = """\
You are a focused sub-agent inside the Nullion system. Your sole job is to complete \
the task described below. Work independently — you cannot ask the orchestrator for help. \
Use the tools available to you. Call report_progress after each significant step. \
When the task is complete, stop and return your final answer as plain text.

You must not attempt to use tools not in your allowed list. You must not escalate \
permissions or impersonate other agents."""


class SubAgentDepthError(RuntimeError):
    """Raised when a mini-agent would exceed the maximum nesting depth."""


# ── Runner ─────────────────────────────────────────────────────────────────────

class MiniAgentRunner:
    """Runs one task asynchronously in a scoped execution context."""

    async def run(
        self,
        config: MiniAgentConfig,
        *,
        anthropic_client: Any,                    # anthropic.AsyncAnthropic or adapter
        tool_registry: Any,                       # nullion.tools.ToolRegistry
        policy_store: Any,
        approval_store: Any,
        context_bus: ContextBus,
        progress_queue: asyncio.Queue,            # Queue[ProgressUpdate]
    ) -> TaskResult:
        """Execute the task. Returns a TaskResult (never raises)."""
        if config.depth >= 3:
            raise SubAgentDepthError(
                f"Agent {config.agent_id} reached max nesting depth {config.depth}"
            )

        task = config.task
        agent_id = config.agent_id
        task_id = task.task_id
        group_id = task.group_id

        # Emit task_started
        await _emit(progress_queue, ProgressUpdate(
            agent_id=agent_id, task_id=task_id, group_id=group_id,
            kind="task_started", message=task.title,
        ))

        try:
            result = await asyncio.wait_for(
                self._inner_loop(config, anthropic_client=anthropic_client,
                                 tool_registry=tool_registry,
                                 policy_store=policy_store,
                                 approval_store=approval_store,
                                 context_bus=context_bus,
                                 progress_queue=progress_queue),
                timeout=config.timeout_s,
            )
        except asyncio.TimeoutError:
            logger.warning("MiniAgent %s timed out after %.0fs", agent_id, config.timeout_s)
            result = TaskResult(
                task_id=task_id, status="failure",
                error=f"Timed out after {config.timeout_s:.0f}s",
            )
        except Exception as exc:
            logger.warning("MiniAgent %s failed: %s", agent_id, exc, exc_info=True)
            result = TaskResult(task_id=task_id, status="failure", error=str(exc))
        finally:
            await _run_tool_registry_cleanup(tool_registry, scope_id=task_id)

        # Publish context output if the task succeeded and has a context key.
        if result.status == "success" and task.context_key_out and result.context_out is not None:
            context_bus.publish(
                task.context_key_out,
                result.context_out,
                group_id=group_id,
                agent_id=agent_id,
                task_id=task_id,
            )

        kind = "task_complete" if result.status == "success" else "task_failed"
        await _emit(progress_queue, ProgressUpdate(
            agent_id=agent_id, task_id=task_id, group_id=group_id,
            kind=kind, message=result.output or result.error,
        ))
        return result

    async def _inner_loop(
        self,
        config: MiniAgentConfig,
        *,
        anthropic_client: Any,
        tool_registry: Any,
        policy_store: Any,
        approval_store: Any,
        context_bus: ContextBus,
        progress_queue: asyncio.Queue,
    ) -> TaskResult:
        task = config.task
        group_id = task.group_id

        # Fetch context_in from bus if needed.
        context_in = config.context_in
        if context_in is None and task.context_key_in:
            context_in = context_bus.get(task.context_key_in, group_id=group_id)

        # Build system prompt.
        system = _MINI_AGENT_SYSTEM_PROMPT + f"\n\nTask: {task.description}"
        if context_in is not None:
            system += f"\n\nContext input ({task.context_key_in}):\n{context_in}"

        # Build tool list: scoped allowed tools + meta-tools.
        all_tools = _build_tool_list(tool_registry, allowed=task.allowed_tools)

        messages: list[dict] = [
            {"role": "user", "content": [{"type": "text", "text": task.description}]}
        ]
        final_state = await _compiled_mini_agent_graph().ainvoke(
            {
                "config": config,
                "anthropic_client": anthropic_client,
                "tool_registry": tool_registry,
                "policy_store": policy_store,
                "approval_store": approval_store,
                "progress_queue": progress_queue,
                "system": system,
                "all_tools": all_tools,
                "messages": messages,
                "artifacts": [],
                "context_out": None,
                "tool_results": [],
                "iteration": 0,
                "total_iteration_budget": config.max_iterations * (max(0, config.max_continuations) + 1),
                "response": None,
                "content": [],
                "stop_reason": None,
            },
            config=_mini_agent_graph_config(config),
        )
        result = final_state.get("result")
        if isinstance(result, TaskResult):
            return result
        raise RuntimeError("Mini-agent graph finished without a TaskResult")


class _MiniAgentGraphState(TypedDict, total=False):
    config: MiniAgentConfig
    anthropic_client: Any
    tool_registry: Any
    policy_store: Any
    approval_store: Any
    progress_queue: asyncio.Queue
    system: str
    all_tools: list[dict]
    messages: list[dict]
    artifacts: list[str]
    context_out: Any
    tool_results: list[ToolResult]
    iteration: int
    total_iteration_budget: int
    response: dict[str, Any] | None
    content: list[dict[str, Any]]
    stop_reason: str | None
    result: TaskResult


def _mini_agent_iteration_limit_result(config: MiniAgentConfig) -> TaskResult:
    if config.max_continuations > 0:
        output = (
            f"Reached max iterations ({config.max_iterations}) plus "
            f"{config.max_continuations} continuation tranche"
            f"{'s' if config.max_continuations != 1 else ''} without completing."
        )
    else:
        output = f"Reached max iterations ({config.max_iterations}) without completing."
    return TaskResult(task_id=config.task.task_id, status="partial", output=output)


async def _mini_agent_model_node(state: _MiniAgentGraphState) -> dict[str, object]:
    config = state["config"]
    task = config.task
    iteration = int(state.get("iteration") or 0)
    total_iteration_budget = int(state.get("total_iteration_budget") or 0)
    if iteration >= total_iteration_budget:
        return {"result": _mini_agent_iteration_limit_result(config)}
    if config.max_iterations > 0 and iteration > 0 and iteration % config.max_iterations == 0:
        continuation_number = iteration // config.max_iterations
        logger.info(
            "MiniAgent %s continuing after %d iterations (%d/%d continuation tranches)",
            config.agent_id,
            iteration,
            continuation_number,
            config.max_continuations,
        )
        await _emit(
            state["progress_queue"],
            ProgressUpdate(
                agent_id=config.agent_id,
                task_id=task.task_id,
                group_id=task.group_id,
                kind="progress_note",
                message=f"Continuing after {iteration} tool steps.",
            ),
        )
    response = await _model_create(
        state["anthropic_client"],
        messages=list(state.get("messages") or []),
        tools=list(state.get("all_tools") or []),
        max_tokens=2048,
        system=state.get("system"),
    )
    content = response.get("content") or []
    return {
        "iteration": iteration + 1,
        "response": response,
        "stop_reason": response.get("stop_reason"),
        "content": list(content) if isinstance(content, list) else [],
    }


def _mini_agent_finalize_node(state: _MiniAgentGraphState) -> dict[str, object]:
    config = state["config"]
    task = config.task
    content = list(state.get("content") or [])
    artifacts = list(state.get("artifacts") or [])
    context_out = state.get("context_out")
    tool_results = list(state.get("tool_results") or [])
    final_parts = [
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    final_text = "".join(final_parts).strip()
    output_text = final_text
    if not output_text and context_out is not None:
        output_text = str(context_out or "").strip()
    if not output_text and artifacts:
        output_text = f"Produced artifact{'s' if len(artifacts) != 1 else ''}: {', '.join(artifacts)}"
    if not output_text:
        return {
            "result": TaskResult(
                task_id=task.task_id,
                status="failure",
                error="Agent finished without a final answer.",
                artifacts=artifacts,
                context_out=context_out,
            )
        }
    output_text = sanitize_user_visible_reply(
        user_message=task.description,
        reply=output_text,
        tool_results=tool_results,
        source="mini-agent",
    ) or output_text
    return {
        "result": TaskResult(
            task_id=task.task_id,
            status="success",
            output=output_text,
            artifacts=artifacts,
            context_out=context_out if context_out is not None else output_text,
        )
    }


async def _mini_agent_tools_node(state: _MiniAgentGraphState) -> dict[str, object]:
    config = state["config"]
    task = config.task
    content = list(state.get("content") or [])
    messages = list(state.get("messages") or [])
    artifacts = list(state.get("artifacts") or [])
    context_out = state.get("context_out")
    tool_results = list(state.get("tool_results") or [])
    tool_result_blocks: list[dict] = []
    messages.append({"role": "assistant", "content": content})

    for block in content:
        if not isinstance(block, dict) or block.get("type") != "tool_use":
            continue
        tool_name = block.get("name")
        tool_input = block.get("input") or {}
        tool_use_id = block.get("id", f"tu-{uuid4().hex[:8]}")

        if not isinstance(tool_name, str):
            continue
        if not isinstance(tool_input, dict):
            tool_input = {}

        if tool_name == "report_progress":
            msg = str(tool_input.get("message", ""))[:100]
            await _emit(
                state["progress_queue"],
                ProgressUpdate(
                    agent_id=config.agent_id,
                    task_id=task.task_id,
                    group_id=task.group_id,
                    kind="progress_note",
                    message=msg,
                ),
            )
            tool_result_blocks.append(_tool_result(tool_use_id, "Progress noted."))
            continue

        if tool_name == "request_user_input":
            if config.can_request_user_input:
                question = str(tool_input.get("question", ""))
                options = tool_input.get("options") or []
                await _emit(
                    state["progress_queue"],
                    ProgressUpdate(
                        agent_id=config.agent_id,
                        task_id=task.task_id,
                        group_id=task.group_id,
                        kind="input_needed",
                        message=question,
                        data={"options": options},
                    ),
                )
                return {
                    "result": TaskResult(
                        task_id=task.task_id,
                        status="partial",
                        output=f"Waiting for user input: {question}",
                    )
                }
            tool_result_blocks.append(_tool_result(tool_use_id, "User input not available in this context."))
            continue

        if task.allowed_tools and tool_name not in task.allowed_tools:
            tool_result_blocks.append(
                _tool_result(
                    tool_use_id,
                    f"Tool {tool_name!r} is not in this agent's allowed tool scope.",
                    is_error=True,
                )
            )
            continue

        tool_result = await _invoke_tool_async(
            tool_name=tool_name,
            tool_input=tool_input,
            principal_id=task.principal_id,
            cleanup_scope=task.task_id,
            tool_registry=state["tool_registry"],
            policy_store=state.get("policy_store"),
            approval_store=state.get("approval_store"),
        )
        tool_results.append(tool_result)
        output_str = str(tool_result.output) if tool_result.output is not None else str(tool_result.error or "")

        if tool_name in {"file_write", "write_file"} and isinstance(tool_input.get("path"), str):
            artifacts.append(tool_input["path"])
        if task.context_key_out:
            context_out = output_str
        tool_result_blocks.append(_tool_result(tool_use_id, output_str))

    if not tool_result_blocks:
        logger.warning(
            "MiniAgent %s: iteration %d produced no tool results — breaking loop to avoid runaway spin",
            config.agent_id,
            int(state.get("iteration") or 0) - 1,
        )
        return {
            "result": TaskResult(
                task_id=task.task_id,
                status="failure",
                error="Agent produced no actionable tool results; check tool scope or prompt.",
            )
        }

    messages.append({"role": "user", "content": tool_result_blocks})
    return {
        "messages": messages,
        "artifacts": artifacts,
        "context_out": context_out,
        "tool_results": tool_results,
    }


def _mini_agent_route_after_model(state: _MiniAgentGraphState) -> str:
    if state.get("result") is not None:
        return END
    return "tools" if state.get("stop_reason") == "tool_use" else "finalize"


def _mini_agent_route_after_tools(state: _MiniAgentGraphState) -> str:
    return END if state.get("result") is not None else "model"


@lru_cache(maxsize=1)
def _compiled_mini_agent_graph():
    graph = StateGraph(_MiniAgentGraphState)
    graph.add_node("model", _mini_agent_model_node)
    graph.add_node("tools", _mini_agent_tools_node)
    graph.add_node("finalize", _mini_agent_finalize_node)
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", _mini_agent_route_after_model, {"tools": "tools", "finalize": "finalize", END: END})
    graph.add_conditional_edges("tools", _mini_agent_route_after_tools, {"model": "model", END: END})
    graph.add_edge("finalize", END)
    return graph.compile()


def _mini_agent_graph_config(config: MiniAgentConfig) -> dict[str, int]:
    budget = config.max_iterations * (max(0, config.max_continuations) + 1)
    return {"recursion_limit": max(25, budget * 3 + 8)}


# ── Progress event ─────────────────────────────────────────────────────────────

@dataclass
class ProgressUpdate:
    agent_id: str
    task_id: str
    group_id: str
    kind: str           # task_started | tool_called | progress_note | input_needed |
                        # task_complete | task_failed | task_cancelled
    message: str | None = None
    data: dict | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ── Internal helpers ───────────────────────────────────────────────────────────

def _build_tool_list(tool_registry: Any, *, allowed: list[str]) -> list[dict]:
    """Return tool definitions scoped to *allowed* plus the two meta-tools."""
    try:
        if allowed:
            defs = tool_registry.list_tool_definitions(allowed=allowed)
        else:
            defs = []
    except TypeError:
        # Older ToolRegistry without `allowed` kwarg — filter manually.
        try:
            all_defs = tool_registry.list_tool_definitions()
            if allowed:
                defs = [d for d in all_defs if d.get("name") in allowed]
            else:
                defs = []
        except Exception:
            defs = []
    except Exception:
        defs = []

    return defs + [_REPORT_PROGRESS_SCHEMA, _REQUEST_USER_INPUT_SCHEMA]


async def _model_create(client: Any, **kwargs) -> dict:
    """Call the model client, supporting both async and sync clients."""
    if hasattr(client, "messages") and hasattr(client.messages, "create"):
        # anthropic.AsyncAnthropic
        try:
            resp = await client.messages.create(**{k: v for k, v in kwargs.items() if v is not None and not (k == "tools" and not v)})
            return {
                "stop_reason": resp.stop_reason,
                "content": [
                    {"type": b.type, "text": b.text} if b.type == "text"
                    else {"type": b.type, "id": b.id, "name": b.name, "input": b.input}
                    for b in resp.content
                ],
            }
        except Exception as exc:
            raise RuntimeError(f"Async model call failed: {exc}") from exc
    else:
        # Synchronous adapter (e.g. the _Adapter classes in web_app.py / cli.py)
        create = getattr(client, "create")
        if inspect.iscoroutinefunction(create):
            return await create(**kwargs)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: create(**kwargs))


async def _invoke_tool_async(
    *,
    tool_name: str,
    tool_input: dict,
    principal_id: str,
    cleanup_scope: str,
    tool_registry: Any,
    policy_store: Any,
    approval_store: Any,
) -> ToolResult:
    """Execute a tool call through the firewall (async wrapper around sync invoke)."""
    invocation = ToolInvocation(
        invocation_id=f"mini-{uuid4().hex[:12]}",
        tool_name=tool_name,
        principal_id=principal_id,
        arguments=dict(tool_input),
        capsule_id=cleanup_scope,
    )
    loop = asyncio.get_event_loop()
    try:
        from nullion.runtime import invoke_tool
        result = await loop.run_in_executor(
            None,
            lambda: invoke_tool(policy_store, invocation, registry=tool_registry),
        )
    except Exception:
        result = await loop.run_in_executor(
            None,
            lambda: tool_registry.invoke(invocation),
        )
    return result


async def _run_tool_registry_cleanup(tool_registry: Any, *, scope_id: str) -> None:
    cleanup = getattr(tool_registry, "run_cleanup_hooks", None)
    if cleanup is None:
        return
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, lambda: cleanup(scope_id=scope_id))
    except Exception:
        logger.debug("Tool cleanup failed for scope %s", scope_id, exc_info=True)


def _tool_result(tool_use_id: str, content: str, *, is_error: bool = False) -> dict:
    return {
        "type": "tool_result",
        "tool_use_id": tool_use_id,
        "content": [{"type": "text", "text": content}],
        **({"is_error": True} if is_error else {}),
    }


async def _emit(queue: asyncio.Queue, update: ProgressUpdate) -> None:
    try:
        queue.put_nowait(update)
    except asyncio.QueueFull:
        logger.debug("MiniAgent progress queue full, dropping update")


__all__ = [
    "MiniAgentConfig",
    "MiniAgentRunner",
    "ProgressUpdate",
    "SubAgentDepthError",
]
