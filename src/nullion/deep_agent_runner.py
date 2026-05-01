"""Deep Agents backend for scoped Nullion mini-agent tasks."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from typing import Any

from nullion.deep_agent_profiles import (
    deep_agent_skill_files_for_task,
    deep_agent_skills_for_task,
    deep_agent_subagents_for_task,
)
from nullion.langchain_adapters import nullion_client_as_langchain_chat_model, nullion_tools_as_langchain_tools
from nullion.response_sanitizer import sanitize_user_visible_reply
from nullion.response_fulfillment_contract import artifact_paths_from_tool_results
from nullion.task_queue import TaskResult

logger = logging.getLogger(__name__)


class DeepAgentUserInputRequested(RuntimeError):
    def __init__(self, question: str, options: list[str]) -> None:
        super().__init__(question)
        self.question = question
        self.options = options


class DeepAgentMiniAgentRunner:
    """Run a Nullion mini-agent task through the optional deepagents harness."""

    async def run(
        self,
        config,
        *,
        anthropic_client: Any,
        tool_registry: Any,
        policy_store: Any,
        approval_store: Any,
        context_bus: Any,
        progress_queue: asyncio.Queue,
    ) -> TaskResult:
        del approval_store
        task = config.task
        await _emit_progress(
            progress_queue,
            config=config,
            kind="task_started",
            message=task.title,
        )
        try:
            result = await asyncio.wait_for(
                self._run_inner(
                    config,
                    anthropic_client=anthropic_client,
                    tool_registry=tool_registry,
                    policy_store=policy_store,
                    context_bus=context_bus,
                    progress_queue=progress_queue,
                ),
                timeout=config.timeout_s,
            )
        except DeepAgentUserInputRequested as exc:
            await _emit_progress(
                progress_queue,
                config=config,
                kind="input_needed",
                message=exc.question,
                data={"options": exc.options},
            )
            result = TaskResult(
                task_id=task.task_id,
                status="partial",
                output=f"Waiting for user input: {exc.question}",
                resume_token=_resume_token_for_pause(
                    config,
                    reason="user_input",
                    payload={"question": exc.question, "options": exc.options},
                ),
            )
        except asyncio.TimeoutError:
            logger.warning("DeepAgent mini-agent %s timed out after %.0fs", config.agent_id, config.timeout_s)
            result = TaskResult(
                task_id=task.task_id,
                status="failure",
                error=f"Timed out after {config.timeout_s:.0f}s",
            )
        except Exception as exc:
            logger.warning("DeepAgent mini-agent %s failed: %s", config.agent_id, exc, exc_info=True)
            result = TaskResult(task_id=task.task_id, status="failure", error=str(exc))
        finally:
            await _run_tool_registry_cleanup(tool_registry, scope_id=task.task_id)

        if result.status == "success" and task.context_key_out and result.context_out is not None:
            context_bus.publish(
                task.context_key_out,
                result.context_out,
                group_id=task.group_id,
                agent_id=config.agent_id,
                task_id=task.task_id,
            )

        await _emit_progress(
            progress_queue,
            config=config,
            kind=_completion_progress_kind(result),
            message=result.output or result.error,
        )
        return result

    async def _run_inner(
        self,
        config,
        *,
        anthropic_client: Any,
        tool_registry: Any,
        policy_store: Any,
        context_bus: Any,
        progress_queue: asyncio.Queue,
    ) -> TaskResult:
        try:
            from deepagents import create_deep_agent
        except Exception as exc:  # pragma: no cover - depends on optional package
            raise RuntimeError("Deep Agents backend requires the deepagents package") from exc

        model = os.environ.get("NULLION_DEEP_AGENTS_MODEL", "").strip()
        if not model:
            model = nullion_client_as_langchain_chat_model(anthropic_client)

        task = config.task
        context_in = config.context_in
        if context_in is None and task.context_key_in:
            context_in = context_bus.get(task.context_key_in, group_id=task.group_id)

        system_prompt = _system_prompt_for_task(config, context_in=context_in)
        tool_results: list[Any] = []
        tools = [
            *nullion_tools_as_langchain_tools(
                tool_registry,
                allowed_tools=list(task.allowed_tools),
                principal_id=task.principal_id,
                cleanup_scope=task.task_id,
                policy_store=policy_store,
                tool_result_callback=tool_results.append,
            ),
            *_deep_agent_meta_tools(config, progress_queue=progress_queue),
        ]
        agent = create_deep_agent(
            model=model,
            tools=tools,
            system_prompt=system_prompt,
            skills=_deep_agent_skills_for_task(config),
            subagents=_deep_agent_subagents_for_task(config),
        )
        payload = {"messages": [{"role": "user", "content": task.description}]}
        skill_files = _deep_agent_skill_files_for_task(config)
        if skill_files:
            payload["files"] = skill_files
        response = await _invoke_agent(
            agent,
            payload,
            config=_deep_agent_graph_config(config),
            progress_queue=progress_queue,
            mini_agent_config=config,
        )
        output_text = _extract_response_text(response)
        if not output_text:
            return TaskResult(
                task_id=task.task_id,
                status="failure",
                error="Deep Agent finished without a final answer.",
            )
        output_text = sanitize_user_visible_reply(
            user_message=task.description,
            reply=output_text,
            tool_results=tool_results,
            source="deep-agent",
        ) or output_text
        artifacts = artifact_paths_from_tool_results(tool_results)
        pending_approval = _pending_approval_from_tool_results(tool_results)
        if pending_approval is not None:
            await _emit_progress(
                progress_queue,
                config=config,
                kind="approval_needed",
                message=pending_approval["message"],
                data={
                    "approval_id": pending_approval.get("approval_id"),
                    "resume_supported": True,
                },
            )
            return TaskResult(
                task_id=task.task_id,
                status="partial",
                output=pending_approval["message"],
                artifacts=artifacts,
                context_out=output_text,
                resume_token=_resume_token_for_pause(
                    config,
                    reason="approval_required",
                    payload={"approval_id": pending_approval.get("approval_id")},
                ),
            )
        return TaskResult(
            task_id=task.task_id,
            status="success",
            output=output_text,
            artifacts=artifacts,
            context_out=output_text,
        )


def _system_prompt_for_task(config, *, context_in: Any) -> str:
    task = config.task
    prompt = (
        "You are a scoped Deep Agent running inside Nullion. Complete only the assigned task. "
        "Use the provided Nullion tools for side effects so Sentinel policy remains authoritative. "
        "Do not claim a file, message, approval, or external change succeeded unless a tool result confirms it. "
        "Return a concise final answer for the user.\n\n"
        f"Task: {task.description}"
    )
    if context_in is not None:
        prompt += f"\n\nContext input ({task.context_key_in}):\n{context_in}"
    return prompt


def _completion_progress_kind(result: TaskResult) -> str:
    if result.status == "success":
        return "task_complete"
    if result.status == "partial":
        return "input_needed" if (result.output or "").startswith("Waiting for user input:") else "progress_note"
    return "task_failed"


def _deep_agent_skills_for_task(config) -> list[str] | None:
    task = config.task
    raw = getattr(task, "deep_agent_skills", None)
    if raw is None:
        raw = os.environ.get("NULLION_DEEP_AGENTS_SKILLS", "")
    inferred = deep_agent_skills_for_task(task)
    raw_values = raw if isinstance(raw, (list, tuple)) else str(raw).split(",")
    skills = [str(skill).strip() for skill in [*raw_values, *inferred]]
    skills = [skill for skill in skills if skill]
    return list(dict.fromkeys(skills)) or None


def _deep_agent_skill_files_for_task(config) -> dict[str, dict[str, str]]:
    files: dict[str, dict[str, str]] = {}
    raw = getattr(config.task, "deep_agent_skill_files", None)
    if isinstance(raw, dict):
        files.update({str(path): _skill_file_payload(content) for path, content in raw.items()})
    files.update(deep_agent_skill_files_for_task(config.task))
    return files


def _skill_file_payload(content: Any) -> dict[str, str]:
    if isinstance(content, dict):
        text = str(content.get("content") or "")
        encoding = str(content.get("encoding") or "utf-8")
        return {"content": text, "encoding": encoding}
    return {"content": str(content), "encoding": "utf-8"}


def _deep_agent_subagents_for_task(config) -> list[dict[str, Any]] | None:
    raw = getattr(config.task, "deep_agent_subagents", None)
    if isinstance(raw, dict):
        raw_items = [raw]
    elif isinstance(raw, (list, tuple)):
        raw_items = list(raw)
    else:
        raw_items = []
    subagents: list[dict[str, Any]] = []
    for item in [*raw_items, *deep_agent_subagents_for_task(config.task)]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        description = str(item.get("description") or "").strip()
        system_prompt = str(item.get("system_prompt") or "").strip()
        if not name or not description or not system_prompt:
            continue
        subagents.append(
            {
                "name": name,
                "description": description,
                "system_prompt": system_prompt,
            }
        )
    return subagents or None


def _deep_agent_meta_tools(config, *, progress_queue: asyncio.Queue) -> list[Any]:
    try:
        from langchain_core.tools import StructuredTool
    except Exception as exc:  # pragma: no cover - depends on optional package
        raise RuntimeError("Deep Agents meta-tools require langchain-core") from exc

    async def report_progress(message: str) -> str:
        await _emit_progress(
            progress_queue,
            config=config,
            kind="progress_note",
            message=str(message)[:100],
        )
        return "Progress noted."

    async def request_user_input(question: str, options: list[str] | None = None) -> str:
        if config.can_request_user_input:
            raise DeepAgentUserInputRequested(str(question), [str(option) for option in (options or [])])
        return "User input is not available in this context."

    return [
        StructuredTool.from_function(
            coroutine=report_progress,
            name="report_progress",
            description="Report a short progress update to the user.",
        ),
        StructuredTool.from_function(
            coroutine=request_user_input,
            name="request_user_input",
            description="Ask the user a question and pause the task.",
        ),
    ]


def _deep_agent_graph_config(config) -> dict[str, Any]:
    budget = int(config.max_iterations) * (max(0, int(config.max_continuations)) + 1)
    return {
        "recursion_limit": max(25, budget * 3 + 8),
        "configurable": {"thread_id": _deep_agent_thread_id(config)},
        "metadata": {"nullion_mini_agent_id": config.agent_id, "nullion_task_id": config.task.task_id},
    }


def _deep_agent_thread_id(config) -> str:
    return f"nullion:{config.task.group_id}:{config.task.task_id}:{config.agent_id}"


def _resume_token_for_pause(config, *, reason: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "backend": "deepagents",
        "reason": reason,
        "thread_id": _deep_agent_thread_id(config),
        "agent_id": config.agent_id,
        "task_id": config.task.task_id,
        "group_id": config.task.group_id,
        **{key: value for key, value in payload.items() if value is not None},
    }


async def _invoke_agent(
    agent: Any,
    payload: dict[str, Any],
    *,
    config: dict[str, Any] | None = None,
    progress_queue: asyncio.Queue | None = None,
    mini_agent_config: Any = None,
) -> Any:
    if hasattr(agent, "astream_events") and progress_queue is not None and mini_agent_config is not None:
        return await _invoke_agent_with_events(
            agent,
            payload,
            config=config,
            progress_queue=progress_queue,
            mini_agent_config=mini_agent_config,
        )
    if hasattr(agent, "ainvoke"):
        return await agent.ainvoke(payload, config=config)
    invoke = getattr(agent, "invoke")
    if inspect.iscoroutinefunction(invoke):
        return await invoke(payload, config=config)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: invoke(payload, config=config))


async def _invoke_agent_with_events(
    agent: Any,
    payload: dict[str, Any],
    *,
    config: dict[str, Any] | None,
    progress_queue: asyncio.Queue,
    mini_agent_config: Any,
) -> Any:
    final_output: Any = None
    emitted: set[tuple[str, str]] = set()
    async for event in agent.astream_events(payload, config=config, version="v2"):
        if event.get("event") == "on_chain_end" and event.get("name") == "LangGraph":
            final_output = (event.get("data") or {}).get("output")
        progress_update = _progress_update_from_deep_agent_event(event)
        if progress_update is None:
            continue
        kind, message, data = progress_update
        signature = (str(event.get("run_id") or ""), kind, message)
        if signature in emitted:
            continue
        emitted.add(signature)
        await _emit_progress(
            progress_queue,
            config=mini_agent_config,
            kind=kind,
            message=message,
            data=data,
        )
    if final_output is not None:
        return final_output
    return await agent.ainvoke(payload, config=config)


def _progress_message_from_deep_agent_event(event: dict[str, Any]) -> str | None:
    progress_update = _progress_update_from_deep_agent_event(event)
    if progress_update is None:
        return None
    return progress_update[1]


def _progress_update_from_deep_agent_event(event: dict[str, Any]) -> tuple[str, str, dict[str, Any] | None] | None:
    event_name = str(event.get("event") or "")
    name = str(event.get("name") or "")
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    node = str(metadata.get("langgraph_node") or name)
    if event_name == "on_chain_start" and node == "model":
        return "progress_note", "Planning next step.", {"phase": "model_start"}
    if event_name == "on_chain_start" and _looks_like_subagent_event(name, metadata):
        return "progress_note", f"Starting {_human_label(name)}.", {"phase": "subagent_start", "subagent": name}
    if event_name == "on_chain_end" and _looks_like_subagent_event(name, metadata):
        return "progress_note", f"{_human_label(name)} completed.", {"phase": "subagent_end", "subagent": name}
    if event_name == "on_chat_model_end":
        tool_names = _tool_names_from_chat_model_output((event.get("data") or {}).get("output"))
        if tool_names:
            return "progress_note", f"Planning tool use: {', '.join(tool_names[:3])}.", {"phase": "tool_plan", "tools": tool_names}
        return "progress_note", "Model step completed.", {"phase": "model_end"}
    if event_name == "on_tool_start":
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        return "progress_note", f"Calling {name}.", {
            "phase": "tool_start",
            "tool": name,
            "args_preview": _redacted_preview(data.get("input")),
        }
    if event_name == "on_tool_end":
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        output = data.get("output")
        if _tool_output_requires_approval(output):
            return "approval_needed", f"{name} needs approval before continuing.", {"phase": "approval_needed", "tool": name}
        if _tool_output_failed(output):
            return "progress_note", f"{name} reported a recoverable failure.", {"phase": "tool_failed", "tool": name}
        return "progress_note", f"{name} completed.", {"phase": "tool_end", "tool": name}
    if event_name == "on_retry":
        return "progress_note", f"Retrying {_human_label(name)}.", {"phase": "retry", "name": name}
    return None


def _looks_like_subagent_event(name: str, metadata: dict[str, Any]) -> bool:
    label = " ".join(str(value or "") for value in (name, metadata.get("subagent"), metadata.get("agent_name"))).lower().strip()
    return "subagent" in label or label.endswith("_agent")


def _human_label(name: str) -> str:
    return str(name or "agent").replace("_", " ").replace("-", " ").strip() or "agent"


def _redacted_preview(value: Any, *, limit: int = 160) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            key_text = str(key)
            if any(secret in key_text.lower() for secret in ("token", "secret", "password", "api_key", "key")):
                redacted[key_text] = "[redacted]"
            else:
                redacted[key_text] = item
        value = redacted
    text = str(value)
    return text if len(text) <= limit else text[: limit - 3].rstrip() + "..."


def _tool_output_failed(output: Any) -> bool:
    status = getattr(output, "status", None)
    if status is None and isinstance(output, dict):
        status = output.get("status")
    error = getattr(output, "error", None)
    if error is None and isinstance(output, dict):
        error = output.get("error")
    return str(status or "").lower() in {"failed", "failure", "error", "denied"} or bool(error)


def _tool_output_requires_approval(output: Any) -> bool:
    payload = getattr(output, "output", output)
    if not isinstance(payload, dict):
        return False
    return payload.get("reason") == "approval_required" or bool(payload.get("requires_approval"))


def _tool_names_from_chat_model_output(output: Any) -> list[str]:
    names: list[str] = []
    for tool_call in getattr(output, "tool_calls", []) or []:
        if isinstance(tool_call, dict) and tool_call.get("name"):
            names.append(str(tool_call["name"]))
    return names


def _extract_response_text(response: Any) -> str:
    if isinstance(response, str):
        return response.strip()
    if isinstance(response, dict):
        for key in ("final", "final_text", "output", "content"):
            value = response.get(key)
            text = _extract_response_text(value)
            if text:
                return text
        messages = response.get("messages")
        if isinstance(messages, list):
            for message in reversed(messages):
                role = _message_role(message)
                if role and role not in {"ai", "assistant"}:
                    continue
                text = _extract_response_text(message)
                if text:
                    return text
    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and isinstance(block.get("text"), str):
                parts.append(block["text"])
            elif hasattr(block, "text"):
                parts.append(str(block.text))
        return "".join(parts).strip()
    if hasattr(response, "messages"):
        return _extract_response_text({"messages": list(response.messages)})
    return ""


def _message_role(message: Any) -> str | None:
    if isinstance(message, dict):
        role = message.get("role") or message.get("type")
        return str(role).lower() if role else None
    role = getattr(message, "type", None) or getattr(message, "role", None)
    return str(role).lower() if role else None


def _pending_approval_from_tool_results(tool_results: list[Any]) -> dict[str, str | None] | None:
    for result in tool_results:
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        if output.get("reason") != "approval_required" and not output.get("requires_approval"):
            continue
        approval_id = output.get("approval_id")
        if approval_id:
            return {
                "message": f"Approval required before this delegated task can continue. Approval ID: {approval_id}",
                "approval_id": str(approval_id),
            }
        return {"message": "Approval required before this delegated task can continue.", "approval_id": None}
    return None


async def _emit_progress(
    queue: asyncio.Queue,
    *,
    config,
    kind: str,
    message: str | None,
    data: dict | None = None,
) -> None:
    from nullion.mini_agent_runner import ProgressUpdate

    try:
        queue.put_nowait(
            ProgressUpdate(
                agent_id=config.agent_id,
                task_id=config.task.task_id,
                group_id=config.task.group_id,
                kind=kind,
                message=message,
                data=data,
            )
        )
    except asyncio.QueueFull:
        logger.debug("DeepAgent progress queue full, dropping update")


async def _run_tool_registry_cleanup(tool_registry: Any, *, scope_id: str) -> None:
    cleanup = getattr(tool_registry, "run_cleanup_hooks", None)
    if cleanup is None:
        return
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, lambda: cleanup(scope_id=scope_id))
    except Exception:
        logger.debug("Tool cleanup failed for scope %s", scope_id, exc_info=True)


__all__ = ["DeepAgentMiniAgentRunner"]
