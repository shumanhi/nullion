"""Deep Agents backend for scoped Nullion mini-agent tasks."""

from __future__ import annotations

import asyncio
from collections import deque
import inspect
import json
import logging
import os
from pathlib import Path
import shutil
from typing import Any

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_path_for_generated_workspace_file,
    artifact_root_for_principal,
)
from nullion.deep_agent_profiles import (
    deep_agent_skill_files_for_task,
    deep_agent_skills_for_task,
    deep_agent_subagents_for_task,
)
from nullion.langchain_adapters import nullion_client_as_langchain_chat_model, nullion_tools_as_langchain_tools
from nullion.response_sanitizer import sanitize_user_visible_reply
from nullion.response_fulfillment_contract import artifact_paths_from_tool_results
from nullion.run_activity import format_tool_activity_line, should_suppress_tool_activity
from nullion.task_queue import TaskResult

logger = logging.getLogger(__name__)


class DeepAgentUserInputRequested(RuntimeError):
    def __init__(self, question: str, options: list[str]) -> None:
        super().__init__(question)
        self.question = question
        self.options = options


class DeepAgentStalledLoopError(RuntimeError):
    """Raised when Deep Agent tool execution loops without progress."""


class DeepAgentEvidenceFallbackUnavailable(RuntimeError):
    """Raised when tool evidence is insufficient to recover a final answer."""


_DEEP_AGENT_EVIDENCE_FALLBACK_TIMEOUT_SECONDS_ENV = "NULLION_DEEP_AGENT_EVIDENCE_FALLBACK_TIMEOUT_SECONDS"
_DEFAULT_DEEP_AGENT_EVIDENCE_FALLBACK_TIMEOUT_SECONDS = 5.0


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

        def record_tool_result(result: Any) -> None:
            tool_results.append(result)
            _emit_tool_progress(progress_queue, config=config, result=result)

        tools = [
            *nullion_tools_as_langchain_tools(
                tool_registry,
                allowed_tools=list(task.allowed_tools),
                principal_id=task.principal_id,
                cleanup_scope=task.task_id,
                policy_store=policy_store,
                tool_result_callback=record_tool_result,
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
        try:
            response = await _invoke_agent(
                agent,
                payload,
                config=_deep_agent_graph_config(config),
                progress_queue=progress_queue,
                mini_agent_config=config,
            )
        except Exception as exc:
            if not _is_graph_recursion_limit(exc):
                raise
            await _emit_progress(
                progress_queue,
                config=config,
                kind="progress_note",
                message="Recovering final answer from verified tool evidence.",
                data={"phase": "graph_limit_recovery"},
            )
            fallback_text = await _fallback_answer_from_tool_evidence(
                config,
                tool_results=tool_results,
                model_client=anthropic_client,
            )
            response = {"content": [{"type": "text", "text": fallback_text}]}
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
        artifacts = _relocate_external_artifact_paths_for_task(config, artifacts)
        deliverable_artifacts = _deliverable_artifact_paths_for_task(config, artifacts)
        if deliverable_artifacts and _task_requires_user_file_delivery(task):
            output_text = _artifact_delivery_success_output_text(deliverable_artifacts)
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
                artifacts=deliverable_artifacts,
                context_out=output_text,
                resume_token=_resume_token_for_pause(
                    config,
                    reason="approval_required",
                    payload={"approval_id": pending_approval.get("approval_id")},
                ),
            )
        artifact_failure = _artifact_delivery_failure_for_task(config, artifacts, deliverable_artifacts)
        if artifact_failure:
            return TaskResult(
                task_id=task.task_id,
                status="failure",
                error=artifact_failure,
                context_out=output_text,
            )
        return TaskResult(
            task_id=task.task_id,
            status="success",
            output=output_text,
            artifacts=deliverable_artifacts,
            context_out=output_text,
        )


def _system_prompt_for_task(config, *, context_in: Any) -> str:
    task = config.task
    artifact_root = _artifact_root_for_prompt(task)
    prompt = (
        "You are a scoped Deep Agent running inside Nullion. Complete only the assigned task. "
        "Use the provided Nullion tools for side effects so Sentinel policy remains authoritative. "
        "Do not claim a file, message, approval, or external change succeeded unless a tool result confirms it. "
        "Return a concise final answer for the user.\n\n"
        "File delivery rules:\n"
        f"- Save final user-facing files under the workspace artifact directory: {artifact_root}\n"
        "- Do not use /tmp, /var/tmp, or arbitrary absolute paths for final files the user asked to receive.\n"
        "- Temporary scratch files must also stay inside the workspace storage area unless a tool explicitly returns "
        "a workspace-safe generated path.\n"
        "- For typed .xlsx artifact requirements, use spreadsheet_create with structured rows, links, and existing "
        "image artifact paths instead of terminal_exec.\n"
        "- Mention a saved or attached file only after file_write, pdf_create, or another file-producing tool "
        "returns a path in the workspace artifact directory.\n\n"
        f"Task: {task.description}"
    )
    if context_in is not None:
        prompt += f"\n\nContext input ({task.context_key_in}):\n{context_in}"
    return prompt


def _artifact_root_for_prompt(task) -> str:
    try:
        return str(artifact_root_for_principal(task.principal_id))
    except Exception:
        logger.debug("Could not resolve artifact root for deep-agent prompt", exc_info=True)
        return "the current workspace artifacts directory"


def _deliverable_artifact_paths_for_task(config, artifact_paths: list[str]) -> list[str]:
    if not artifact_paths:
        return []
    task = config.task
    try:
        artifact_root = artifact_root_for_principal(task.principal_id)
    except Exception:
        logger.debug("Could not resolve artifact root for mini-agent artifact validation", exc_info=True)
        return list(dict.fromkeys(artifact_paths))
    deliverable: list[str] = []
    for raw_path in artifact_paths:
        try:
            descriptor = artifact_descriptor_for_path(Path(raw_path), artifact_root=artifact_root)
        except Exception:
            descriptor = None
        if descriptor is not None:
            deliverable.append(descriptor.path)
    return list(dict.fromkeys(deliverable))


def _relocate_external_artifact_paths_for_task(config, artifact_paths: list[str]) -> list[str]:
    if not artifact_paths:
        return []
    task = config.task
    try:
        artifact_root = artifact_root_for_principal(task.principal_id)
    except Exception:
        logger.debug("Could not resolve artifact root for external artifact relocation", exc_info=True)
        return list(dict.fromkeys(artifact_paths))

    relocated: list[str] = []
    for raw_path in artifact_paths:
        if not isinstance(raw_path, str) or not raw_path:
            continue
        source_path = Path(raw_path).expanduser()
        try:
            descriptor = artifact_descriptor_for_path(source_path, artifact_root=artifact_root)
        except Exception:
            descriptor = None
        if descriptor is not None:
            relocated.append(descriptor.path)
            continue

        try:
            resolved_source = source_path.resolve()
        except Exception:
            continue
        if not resolved_source.is_file():
            continue
        suffix = resolved_source.suffix or ".bin"
        target_path = artifact_path_for_generated_workspace_file(
            principal_id=task.principal_id,
            suffix=suffix,
            stem=resolved_source.stem or "artifact",
        )
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(resolved_source, target_path)
            target_descriptor = artifact_descriptor_for_path(target_path, artifact_root=artifact_root)
        except Exception:
            logger.debug("Could not relocate external artifact %s", resolved_source, exc_info=True)
            continue
        if target_descriptor is not None:
            relocated.append(target_descriptor.path)

    return list(dict.fromkeys(relocated or artifact_paths))


def _artifact_delivery_failure_for_task(config, artifact_paths: list[str], deliverable_artifacts: list[str]) -> str | None:
    task = config.task
    if deliverable_artifacts:
        return None
    if not artifact_paths and not _task_requires_user_file_delivery(task):
        return None
    try:
        artifact_root = artifact_root_for_principal(task.principal_id)
    except Exception:
        artifact_root = None
    root_text = str(artifact_root) if artifact_root is not None else "the workspace artifacts directory"
    if artifact_paths:
        return (
            "The mini-agent created a file outside the downloadable workspace, so it was not delivered. "
            f"Final user-facing files must be written under {root_text}."
        )
    return (
        "The mini-agent did not create a downloadable file for this request. "
        f"Final user-facing files must be written under {root_text}."
    )


def _artifact_delivery_success_output_text(deliverable_artifacts: list[str]) -> str:
    names = [Path(path).name for path in deliverable_artifacts if isinstance(path, str) and path]
    if len(names) == 1:
        return f"Created and verified downloadable artifact: {names[0]}"
    if names:
        return "Created and verified downloadable artifacts: " + ", ".join(names)
    return "Created and verified the requested downloadable artifact."


def _task_requires_user_file_delivery(task) -> bool:
    finish = getattr(task, "finish", None)
    if bool(getattr(finish, "requires_artifact_delivery", False)):
        return True
    output = getattr(task, "output", None)
    if getattr(output, "artifact_kind", None):
        return True
    metadata = getattr(task, "metadata", None)
    if isinstance(metadata, dict):
        return bool(metadata.get("requires_artifact_delivery") or metadata.get("required_artifact_kind"))
    return False


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


def _is_graph_recursion_limit(exc: BaseException) -> bool:
    try:
        from langgraph.errors import GraphRecursionError
    except Exception:  # pragma: no cover - optional dependency shape
        GraphRecursionError = None  # type: ignore[assignment]
    if GraphRecursionError is not None and isinstance(exc, GraphRecursionError):
        return True
    return type(exc).__name__ == "GraphRecursionError"


def _float_env(name: str, default: float, *, minimum: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _evidence_fallback_timeout_seconds() -> float:
    return _float_env(
        _DEEP_AGENT_EVIDENCE_FALLBACK_TIMEOUT_SECONDS_ENV,
        _DEFAULT_DEEP_AGENT_EVIDENCE_FALLBACK_TIMEOUT_SECONDS,
        minimum=0.1,
    )


async def _fallback_answer_from_tool_evidence(
    config,
    *,
    tool_results: list[Any],
    model_client: Any,
) -> str:
    completed_results = [
        result
        for result in tool_results
        if str(getattr(result, "status", "") or "").lower() in {"completed", "success", "succeeded"}
    ]
    if not completed_results:
        raise DeepAgentEvidenceFallbackUnavailable("Verified tool evidence was unavailable for recovery.")
    create = getattr(model_client, "create", None)
    if create is None:
        raise DeepAgentEvidenceFallbackUnavailable("Verified tool evidence could not be summarized.")
    evidence = _tool_evidence_payload(completed_results)
    system_prompt = (
        "You are completing a delegated Nullion task after the stateful agent graph hit a runtime step limit. "
        "Use only the verified tool-result evidence provided by the runtime. "
        "Output only JSON with this schema: "
        '{"status":"success"|"failure","answer":"string"}. '
        "Set status=failure when the evidence is insufficient. "
        "Do not mention internal graph limits, retries, tools, or recovery mechanics to the user."
    )
    user_payload = {
        "task": str(getattr(config.task, "description", "") or getattr(config.task, "title", "") or ""),
        "tool_evidence": evidence,
    }
    create_kwargs = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": json.dumps(user_payload, ensure_ascii=False, default=str)}],
            }
        ],
        "tools": [],
        "max_tokens": 900,
        "system": system_prompt,
    }
    try:
        response = await asyncio.wait_for(
            _call_model_create(create, create_kwargs),
            timeout=_evidence_fallback_timeout_seconds(),
        )
    except TimeoutError as exc:
        raise DeepAgentEvidenceFallbackUnavailable(
            "Verified tool evidence recovery timed out."
        ) from exc
    decision = _parse_evidence_fallback_decision(_extract_response_text(response))
    if decision is None:
        raise DeepAgentEvidenceFallbackUnavailable("Verified tool evidence recovery returned no valid structured decision.")
    if decision.get("status") != "success":
        reason = str(decision.get("answer") or "Verified tool evidence was insufficient to finish the task.").strip()
        raise DeepAgentEvidenceFallbackUnavailable(reason)
    answer = str(decision.get("answer") or "").strip()
    if not answer:
        raise DeepAgentEvidenceFallbackUnavailable("Verified tool evidence recovery returned an empty answer.")
    return answer


def _tool_evidence_payload(tool_results: list[Any], *, max_results: int = 12, max_chars: int = 1200) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for result in tool_results[-max_results:]:
        output = getattr(result, "output", None)
        try:
            output_text = json.dumps(output, ensure_ascii=False, default=str)
        except TypeError:
            output_text = str(output)
        if len(output_text) > max_chars:
            output_text = output_text[:max_chars] + "...[truncated]"
        evidence.append(
            {
                "tool_name": str(getattr(result, "tool_name", "") or ""),
                "status": str(getattr(result, "status", "") or ""),
                "output": output_text,
                "error": str(getattr(result, "error", "") or ""),
            }
        )
    return evidence


async def _call_model_create(create: Any, create_kwargs: dict[str, Any]) -> Any:
    if inspect.iscoroutinefunction(create):
        return await create(**create_kwargs)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: create(**create_kwargs))


def _parse_evidence_fallback_decision(raw_text: str) -> dict[str, str] | None:
    raw_text = str(raw_text or "").strip()
    if not raw_text:
        return None
    candidates = [raw_text]
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start >= 0 and end > start:
        candidates.append(raw_text[start : end + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        status = str(parsed.get("status") or "").strip().lower()
        answer = str(parsed.get("answer") or "").strip()
        if status in {"success", "failure"}:
            return {"status": status, "answer": answer}
    return None


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
    loop_guard = _DeepAgentToolLoopGuard()
    async for event in agent.astream_events(payload, config=config, version="v2"):
        stall_reason = loop_guard.observe(event)
        if stall_reason is not None:
            raise DeepAgentStalledLoopError(stall_reason)
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


def _int_env(name: str, default: int, *, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _stable_event_signature(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, (str, int, float, bool)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    if isinstance(value, dict):
        normalized = {str(key): _stable_event_signature(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
        return json.dumps(normalized, ensure_ascii=True, sort_keys=True)
    if isinstance(value, (list, tuple)):
        normalized_list = [_stable_event_signature(item) for item in value]
        return json.dumps(normalized_list, ensure_ascii=True, sort_keys=True)
    return f"<{type(value).__name__}>"


def _tool_call_signature(event: dict[str, Any]) -> str | None:
    if str(event.get("event") or "") != "on_tool_start":
        return None
    tool_name = str(event.get("name") or "").strip()
    if not tool_name:
        return None
    data = event.get("data")
    tool_input = data.get("input") if isinstance(data, dict) else None
    return f"{tool_name}:{_stable_event_signature(tool_input)}"


def _repeated_cycle_length(signatures: list[str], *, min_repeats: int, max_cycle_len: int) -> int | None:
    if len(signatures) < min_repeats:
        return None
    max_len = min(max_cycle_len, len(signatures) // min_repeats)
    for cycle_len in range(1, max_len + 1):
        window_len = cycle_len * min_repeats
        window = signatures[-window_len:]
        cycle = window[:cycle_len]
        if cycle * min_repeats == window:
            return cycle_len
    return None


class _DeepAgentToolLoopGuard:
    def __init__(self) -> None:
        self._min_repeats = _int_env("NULLION_DEEP_AGENT_STALL_MIN_REPEATS", 3, minimum=2)
        self._max_cycle_len = _int_env("NULLION_DEEP_AGENT_STALL_MAX_CYCLE_LEN", 6, minimum=1)
        min_events_default = self._min_repeats * 2
        self._min_events = _int_env("NULLION_DEEP_AGENT_STALL_MIN_TOOL_EVENTS", min_events_default, minimum=self._min_repeats)
        history_cap_default = max(32, self._max_cycle_len * self._min_repeats * 2)
        self._history: deque[str] = deque(maxlen=_int_env("NULLION_DEEP_AGENT_STALL_HISTORY_CAP", history_cap_default, minimum=8))

    def observe(self, event: dict[str, Any]) -> str | None:
        signature = _tool_call_signature(event)
        if signature is None:
            return None
        self._history.append(signature)
        if len(self._history) < self._min_events:
            return None
        signatures = list(self._history)
        cycle_len = _repeated_cycle_length(
            signatures,
            min_repeats=self._min_repeats,
            max_cycle_len=self._max_cycle_len,
        )
        if cycle_len is None:
            return None
        repeated_cycle = signatures[-cycle_len:]
        cycle_preview = ", ".join(signature.split(":", 1)[0] for signature in repeated_cycle[:3])
        return (
            "stalled_tool_loop_no_progress: repeated tool cycle detected "
            f"({self._min_repeats} repeats, cycle_len={cycle_len}). "
            f"Recent tools: {cycle_preview}"
        )


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
    if isinstance(response, list):
        parts = [_extract_response_text(item) for item in response]
        return "".join(part for part in parts if part).strip()
    if isinstance(response, dict):
        text_value = response.get("text")
        if isinstance(text_value, str) and text_value.strip():
            return text_value.strip()
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


def _emit_tool_progress(queue: asyncio.Queue, *, config, result: Any) -> None:
    if queue is None or should_suppress_tool_activity(result):
        return
    from nullion.mini_agent_runner import ProgressUpdate

    try:
        output = getattr(result, "output", None)
        data: dict[str, object] = {
            "tool_name": str(getattr(result, "tool_name", "") or "tool"),
            "tool_status": str(getattr(result, "status", "") or "unknown"),
        }
        if isinstance(output, dict):
            for key in ("path", "url", "title", "status_code", "content_type"):
                value = output.get(key)
                if value is not None:
                    data[key] = str(value)[:300]
        queue.put_nowait(
            ProgressUpdate(
                agent_id=config.agent_id,
                task_id=config.task.task_id,
                group_id=config.task.group_id,
                kind="tool_activity",
                message=format_tool_activity_line(result),
                data=data,
            )
        )
    except asyncio.QueueFull:
        logger.debug("DeepAgent tool progress queue full, dropping update")


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
