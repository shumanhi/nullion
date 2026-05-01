from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Callable, TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.missions import MissionContinuationPolicy, MissionRecord, MissionStep
from nullion.mini_agent_runs import MiniAgentRunStatus, create_mini_agent_run, transition_mini_agent_run_status
from nullion.prompt_injection import (
    UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,
    UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,
    is_untrusted_tool_name,
    model_security_envelope,
    safe_untrusted_tool_metadata,
)
from nullion.response_sanitizer import sanitize_user_visible_reply, safe_raw_tool_payload_replacement
from nullion.response_fulfillment_contract import evaluate_response_fulfillment
from nullion.runtime import (
    mark_mission_completed,
    mark_mission_failed,
    mark_mission_running,
    mark_mission_waiting_approval,
)
from nullion.suspended_turns import SuspendedTurn
from nullion.thinking_display import extract_thinking_text
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult

logger = logging.getLogger(__name__)


def _resolve_runtime_store(*, policy_store, approval_store):
    if policy_store is not None and approval_store is not None and policy_store is not approval_store:
        return None
    return policy_store if policy_store is not None else approval_store


def _run_tool_cleanup_hooks(tool_registry: ToolRegistry, scope_id: str) -> None:
    cleanup = getattr(tool_registry, "run_cleanup_hooks", None)
    if cleanup is None:
        return
    try:
        cleanup(scope_id=scope_id)
    except Exception:
        logger.debug("Tool cleanup failed for scope %s", scope_id, exc_info=True)


def _artifact_paths_from_tool_result(result: ToolResult, *, runtime_store=None) -> list[str]:
    if result.status != "completed":
        return []
    output = result.output if isinstance(result.output, dict) else {}
    forwarded_paths: list[str] = []
    for key in ("artifact_paths", "artifacts"):
        value = output.get(key)
        if isinstance(value, list):
            forwarded_paths.extend(path for path in value if isinstance(path, str) and path)
        elif isinstance(value, str) and value:
            forwarded_paths.append(value)
    if forwarded_paths:
        return list(dict.fromkeys(forwarded_paths))
    if result.tool_name == "file_write":
        path = output.get("path")
        return [path] if isinstance(path, str) and path else []
    if result.tool_name == "image_generate":
        paths = [
            path
            for path in (output.get("path"), output.get("output_path"))
            if isinstance(path, str) and path
        ]
        return list(dict.fromkeys(paths))
    if result.tool_name == "browser_screenshot" and runtime_store is not None:
        image_base64 = output.get("image_base64")
        if not isinstance(image_base64, str) or not image_base64:
            return []
        try:
            from nullion.artifacts import artifact_path_for_generated_file

            image_bytes = base64.b64decode(image_base64)
            artifact_path = artifact_path_for_generated_file(runtime_store, suffix=".png")
            artifact_path.write_bytes(image_bytes)
            output["path"] = str(artifact_path)
            output.pop("image_base64", None)
            return [str(artifact_path)]
        except Exception:
            logger.warning("Failed to materialize browser screenshot artifact", exc_info=True)
    return []


def _artifact_root_snapshot(runtime_store) -> dict[str, tuple[int, int]]:
    if runtime_store is None:
        return {}
    try:
        from nullion.artifacts import artifact_descriptor_for_path, artifact_root_for_runtime

        root = artifact_root_for_runtime(runtime_store)
        if not root.exists():
            return {}
        snapshot: dict[str, tuple[int, int]] = {}
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            descriptor = artifact_descriptor_for_path(path, artifact_root=root)
            if descriptor is None:
                continue
            stat = path.stat()
            snapshot[str(path.resolve())] = (stat.st_mtime_ns, stat.st_size)
        return snapshot
    except Exception:
        logger.debug("Failed to snapshot artifact root", exc_info=True)
        return {}


def _new_artifact_paths_since(
    before: dict[str, tuple[int, int]],
    *,
    runtime_store,
) -> list[str]:
    after = _artifact_root_snapshot(runtime_store)
    if not after:
        return []
    changed = [
        path
        for path, fingerprint in after.items()
        if before.get(path) != fingerprint
    ]
    return sorted(changed, key=lambda path: after[path][0])


def _tool_result_message_payload(result: ToolResult) -> str:
    payload: dict[str, Any] = {
        "status": result.status,
        "output": result.output,
    }
    security = model_security_envelope(result.tool_name, result.output)
    if security is not None:
        payload["security"] = security
        payload["untrusted_output_boundary"] = {
            "start": UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,
            "end": UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,
        }
    if result.error:
        payload["error"] = result.error
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _malformed_tool_call_result(*, principal_id: str, reason: str, block: object) -> ToolResult:
    tool_name = "malformed_tool_call"
    if isinstance(block, dict) and isinstance(block.get("name"), str) and block.get("name"):
        tool_name = str(block["name"])
    return ToolResult(
        invocation_id=f"orchestrator-malformed-{uuid4().hex}",
        tool_name=tool_name,
        status="failed",
        output={"reason": "malformed_tool_call", "principal_id": principal_id},
        error=reason,
    )


def _last_useful_tool_message(tool_results: list[ToolResult]) -> str:
    if not tool_results:
        return (
            "I got stuck before I could finish the request. Please try again with a more specific "
            "target, or use a direct command if this was a scheduled task."
        )
    last = tool_results[-1]
    output = last.output if isinstance(last.output, dict) else {}
    if is_untrusted_tool_name(last.tool_name):
        return _untrusted_tool_result_safe_fallback_text(last)
    message = output.get("message")
    if isinstance(message, str) and message.strip():
        return (
            "I could not complete the request before the tool loop limit, but the last tool result was:\n\n"
            f"{message.strip()}"
        )
    if last.status == "failed":
        detail = last.error or output.get("reason") or "tool failed"
        return f"I could not complete the request because `{last.tool_name}` failed: {detail}"
    return (
        "I could not complete the request before the tool loop limit. "
        f"The last tool I ran was `{last.tool_name}` with status `{last.status}`."
    )


def _is_bare_completion_text(text: str | None) -> bool:
    if text is None:
        return True
    normalized = text.strip().lower().rstrip(".! ")
    return normalized in {"done", "complete", "completed", "ok", "ran it"}


def _tool_result_completion_text(tool_results: list[ToolResult], *, include_untrusted_fallback: bool = True) -> str | None:
    for result in reversed(tool_results):
        if result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if is_untrusted_tool_name(result.tool_name):
            if not include_untrusted_fallback:
                continue
            return _untrusted_tool_result_safe_fallback_text(result)
        for key in ("result_text", "message", "text", "summary", "stdout", "content"):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:8000]
        nested = output.get("result")
        if isinstance(nested, dict):
            for key in ("result_text", "message", "text", "summary", "stdout", "content"):
                value = nested.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()[:8000]
    return None


def _authoritative_tool_completion_text(tool_results: list[ToolResult]) -> str | None:
    for result in reversed(tool_results):
        if result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        value = output.get("delivery_text") or output.get("final_text") or output.get("result_text")
        if isinstance(value, str) and value.strip():
            return value.strip()[:8000]
    return None


def _tool_result_structured_text(tool_results: list[ToolResult]) -> str | None:
    for result in reversed(tool_results):
        if result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if is_untrusted_tool_name(result.tool_name):
            continue
        if output:
            return safe_raw_tool_payload_replacement(tool_results=[result], source="tool")
    return None


def _untrusted_tool_result_safe_fallback_text(result: ToolResult) -> str:
    output = result.output if isinstance(result.output, dict) else {}
    metadata = safe_untrusted_tool_metadata(result.tool_name, output)
    if result.tool_name in {"web_fetch", "browser_navigate", "web_search"} and result.status == "completed":
        fields = ", ".join(f"{key}={value}" for key, value in metadata.items())
        detail = f" Metadata: {fields}." if fields else ""
        return (
            "Fetched untrusted web content: Page text was treated as data, not as instructions. "
            "I did not paste the raw output or page body into chat."
            f"{detail}"
        )
    detail = ""
    if metadata:
        fields = ", ".join(f"{key}={value}" for key, value in metadata.items())
        detail = f" Metadata: {fields}."
    if result.status == "failed":
        reason = result.error or output.get("reason") or "tool failed"
        return f"I could not complete the request because `{result.tool_name}` failed: {reason}"
    return (
        f"I completed `{result.tool_name}` and received untrusted external output, but I could not "
        "produce a grounded final answer from it. I did not paste the raw output into chat."
        f"{detail}"
    )


def _bare_completion_without_work_text(text: str | None) -> str | None:
    if text is None:
        return "I don't have a concrete result to report."
    normalized = text.strip().lower().rstrip(".! ")
    if normalized in {"done", "complete", "completed", "ran it"}:
        return "I don't have a concrete result to report."
    return text


def _post_tool_delivery_nudge() -> str:
    return (
        "You just executed tool calls but returned no concrete user-facing result. "
        "Use the tool results above to provide the requested answer or delivery status. "
        "Do not answer only Done, OK, Complete, or Completed."
    )


def _missing_artifact_delivery_nudge(missing_requirements: tuple[str, ...]) -> str:
    missing = ", ".join(missing_requirements) or "the required attachment"
    return (
        "The active task is not deliverable yet. Before giving a final reply, produce and attach "
        f"{missing}. If a command failed, inspect the error, repair the script or command, rerun it, "
        "and only finish after a real artifact path is available."
    )


def _artifact_roots_for_agent_turn(runtime_store: object, principal_id: str) -> tuple[Any, ...]:
    roots: list[Any] = []
    try:
        from nullion.artifacts import artifact_root_for_principal

        roots.append(artifact_root_for_principal(principal_id))
    except Exception:
        logger.debug("Could not resolve principal artifact root", exc_info=True)
    try:
        from nullion.artifacts import artifact_root_for_runtime

        roots.append(artifact_root_for_runtime(runtime_store))
    except Exception:
        logger.debug("Could not resolve runtime artifact root", exc_info=True)
    return tuple(roots)


def _conversation_visible_content(content: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        block
        for block in content
        if isinstance(block, dict) and block.get("type") not in {"thinking", "reasoning", "reasoning_summary"}
    ]


def _tool_loop_doctor_threshold() -> int:
    raw_value = os.environ.get("NULLION_TOOL_LOOP_DOCTOR_THRESHOLD", "12").strip()
    try:
        threshold = int(raw_value)
    except ValueError:
        return 20
    return max(1, threshold)


def _repeated_tool_failure_limit() -> int:
    raw_value = os.environ.get("NULLION_REPEATED_TOOL_FAILURE_LIMIT", "2").strip()
    try:
        limit = int(raw_value)
    except ValueError:
        return 2
    return max(1, limit)


def _tool_invocation_signature(*, tool_name: str, tool_input: dict[str, Any]) -> str:
    return json.dumps(
        {"tool_name": tool_name, "arguments": tool_input},
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )


def _tool_failure_fingerprint(*, result: ToolResult, invocation_signature: str) -> str | None:
    if result.status == "completed":
        return None
    output = result.output if isinstance(result.output, dict) else {}
    failure_shape: dict[str, Any] = {
        "invocation": invocation_signature,
        "status": result.status,
    }
    for key in ("reason", "network_mode", "requires_approval"):
        value = output.get(key)
        if value is not None:
            failure_shape[key] = value
    if result.error:
        failure_shape["error"] = result.error
    return json.dumps(failure_shape, ensure_ascii=False, sort_keys=True, default=str)


def _repeated_tool_failure_message(
    *,
    result: ToolResult,
    repeated_count: int,
) -> str:
    output = result.output if isinstance(result.output, dict) else {}
    detail = result.error or output.get("reason") or result.status
    return (
        f"I stopped after `{result.tool_name}` returned the same non-completing result "
        f"{repeated_count} time(s). Last detail: {detail}"
    )


def _report_long_running_tool_loop(
    runtime_store,
    *,
    conversation_id: str,
    principal_id: str,
    user_message: str,
    tool_results: list[ToolResult],
    threshold: int,
) -> None:
    if runtime_store is None or not tool_results:
        return
    last = tool_results[-1]
    try:
        from nullion.health import HealthIssueType
        from nullion.runtime import report_health_issue

        report_health_issue(
            runtime_store,
            issue_type=HealthIssueType.STALLED,
            source="agent_orchestrator",
            message=(
                "Long-running request is still active. Doctor should inspect whether it is making progress "
                "and surface continue or stop guidance to the user."
            ),
            details={
                "conversation_id": conversation_id,
                "principal_id": principal_id,
                "tool_count": len(tool_results),
                "soft_threshold": threshold,
                "last_tool": last.tool_name,
                "last_status": last.status,
                "message_preview": user_message[:160],
            },
        )
    except Exception:
        logger.debug("Could not report long-running tool loop to Doctor", exc_info=True)


def _notify_long_running_tool_loop(
    deliver_fn: Any,
    *,
    conversation_id: str,
    tool_results: list[ToolResult],
) -> None:
    if deliver_fn is None or not tool_results:
        return
    last = tool_results[-1]
    try:
        deliver_fn(
            conversation_id,
            (
                "Doctor is watching this longer request. "
                f"It has run {len(tool_results)} tool step(s) and is still active; "
                f"latest tool: {last.tool_name} ({last.status})."
            ),
            kind="doctor_progress",
            tool_count=len(tool_results),
            last_tool=last.tool_name,
            last_status=last.status,
        )
    except Exception:
        logger.debug("Could not deliver long-running tool-loop notice", exc_info=True)


@dataclass(slots=True)
class TurnResult:
    turn_id: str
    final_text: str | None
    tool_results: list[ToolResult] = field(default_factory=list)
    suspended_for_approval: bool = False
    approval_id: str | None = None
    artifacts: list[str] = field(default_factory=list)
    thinking_text: str | None = None
    reached_iteration_limit: bool = False


@dataclass(slots=True)
class MissionResult:
    mission_id: str
    status: str
    completed_steps: int
    total_steps: int
    final_summary: str | None
    artifacts: list[str] = field(default_factory=list)
    tool_results: list[ToolResult] = field(default_factory=list)
    suspended_approval_id: str | None = None
    interrupt_handled: object | None = None


class _AgentTurnGraphState(TypedDict, total=False):
    orchestrator: Any
    conversation_id: str
    principal_id: str
    user_message: str
    messages: list[dict[str, Any]]
    tool_registry: ToolRegistry
    runtime_store: Any
    max_iterations: int | None
    tool_result_callback: Callable[[ToolResult], None] | None
    cleanup_scope: str
    cleanup_done: bool
    tool_results: list[ToolResult]
    artifacts: list[str]
    iterations: int
    doctor_threshold: int
    next_doctor_notice_at: int
    post_tool_delivery_nudged: bool
    repeated_failure_limit: int
    failure_fingerprints: dict[str, int]
    thinking_parts: list[str]
    initial_tool_content: list[dict[str, Any]] | None
    enable_repeated_failure_guard: bool
    enable_doctor_notifications: bool
    use_authoritative_completion_text: bool
    response: dict[str, Any]
    content: list[dict[str, Any]]
    stop_reason: str | None
    result: TurnResult


def _agent_turn_thinking_text(state: _AgentTurnGraphState) -> str | None:
    return "\n\n".join(state.get("thinking_parts") or []) or None


def _complete_agent_turn(
    state: _AgentTurnGraphState,
    *,
    final_text: str | None,
    suspended_for_approval: bool = False,
    approval_id: str | None = None,
    reached_iteration_limit: bool = False,
) -> dict[str, object]:
    cleanup_done = bool(state.get("cleanup_done"))
    tool_registry = state.get("tool_registry")
    cleanup_scope = state.get("cleanup_scope") or f"turn-{uuid4().hex}"
    if not cleanup_done and tool_registry is not None:
        _run_tool_cleanup_hooks(tool_registry, cleanup_scope)
        cleanup_done = True
    return {
        "cleanup_done": cleanup_done,
        "result": TurnResult(
            turn_id=f"turn-{uuid4().hex}",
            final_text=final_text,
            tool_results=list(state.get("tool_results") or []),
            suspended_for_approval=suspended_for_approval,
            approval_id=approval_id,
            artifacts=list(dict.fromkeys(state.get("artifacts") or [])),
            thinking_text=_agent_turn_thinking_text(state),
            reached_iteration_limit=reached_iteration_limit,
        ),
    }


def _execute_agent_turn_tool_uses(
    state: _AgentTurnGraphState,
    content: list[dict[str, Any]],
) -> dict[str, object]:
    principal_id = state["principal_id"]
    conversation_id = state["conversation_id"]
    user_message = state["user_message"]
    tool_registry = state["tool_registry"]
    runtime_store = state.get("runtime_store")
    cleanup_scope = state["cleanup_scope"]
    tool_result_callback = state.get("tool_result_callback")
    messages = list(state.get("messages") or [])
    tool_results = list(state.get("tool_results") or [])
    artifacts = list(state.get("artifacts") or [])
    failure_fingerprints = dict(state.get("failure_fingerprints") or {})
    tool_result_blocks: list[dict[str, object]] = []

    for block in content:
        if not isinstance(block, dict) or block.get("type") != "tool_use":
            continue
        tool_name = block.get("name")
        tool_input = block.get("input")
        tool_use_id = block.get("id")
        if not isinstance(tool_use_id, str) or not tool_use_id.strip():
            result = _malformed_tool_call_result(
                principal_id=principal_id,
                reason="Model returned a tool call without a valid tool call id.",
                block=block,
            )
            tool_results.append(result)
            continue
        if not isinstance(tool_name, str) or not tool_name.strip():
            result = _malformed_tool_call_result(
                principal_id=principal_id,
                reason="Model returned a tool call without a valid tool name.",
                block=block,
            )
            tool_results.append(result)
            continue
        if not isinstance(tool_input, dict):
            result = _malformed_tool_call_result(
                principal_id=principal_id,
                reason=f"Model returned invalid arguments for `{tool_name}`.",
                block=block,
            )
            tool_results.append(result)
            tool_result_blocks.append(
                {
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": [{"type": "text", "text": _tool_result_message_payload(result)}],
                }
            )
            continue

        invocation = ToolInvocation(
            invocation_id=f"orchestrator-{uuid4().hex}",
            tool_name=tool_name,
            principal_id=principal_id,
            arguments=dict(tool_input),
            capsule_id=cleanup_scope,
        )
        invocation_signature = _tool_invocation_signature(
            tool_name=tool_name,
            tool_input=dict(tool_input),
        )
        artifact_snapshot = (
            _artifact_root_snapshot(runtime_store)
            if tool_name == "terminal_exec" and runtime_store is not None
            else None
        )
        if runtime_store is not None:
            from nullion.runtime import invoke_tool_with_boundary_policy

            result = invoke_tool_with_boundary_policy(runtime_store, invocation, registry=tool_registry)
        else:
            result = tool_registry.invoke(invocation)
        tool_results.append(result)
        if tool_result_callback is not None:
            try:
                tool_result_callback(result)
            except Exception:
                logger.debug("Tool result callback failed", exc_info=True)
        artifacts.extend(_artifact_paths_from_tool_result(result, runtime_store=runtime_store))
        if result.status == "completed" and artifact_snapshot is not None:
            artifacts.extend(_new_artifact_paths_since(artifact_snapshot, runtime_store=runtime_store))
            artifacts = list(dict.fromkeys(artifacts))

        output = result.output if isinstance(result.output, dict) else {}
        approval_id = output.get("approval_id") if isinstance(output.get("approval_id"), str) else None
        if result.status == "denied" and output.get("reason") == "approval_required" and approval_id is not None:
            if runtime_store is not None:
                try:
                    runtime_store.add_suspended_turn(
                        SuspendedTurn(
                            approval_id=approval_id,
                            conversation_id=conversation_id,
                            chat_id=_messaging_target_from_conversation_id(conversation_id),
                            message=f"/chat {user_message}",
                            request_id=None,
                            message_id=None,
                            created_at=datetime.now(UTC),
                            mission_id=None,
                            pending_step_idx=None,
                            messages_snapshot=list(messages),
                            pending_tool_calls=_serialize_pending_tool_calls(tool_results),
                        )
                    )
                except Exception:
                    logger.debug("Could not persist suspended turn", exc_info=True)
            updated_state = dict(state)
            updated_state.update({"tool_results": tool_results, "artifacts": artifacts})
            return {
                "tool_results": tool_results,
                "artifacts": artifacts,
                **_complete_agent_turn(
                    updated_state,
                    final_text=None,
                    suspended_for_approval=True,
                    approval_id=approval_id,
                ),
            }

        if state.get("enable_repeated_failure_guard", False):
            failure_fingerprint = _tool_failure_fingerprint(
                result=result,
                invocation_signature=invocation_signature,
            )
            if failure_fingerprint is not None:
                failure_fingerprints[failure_fingerprint] = failure_fingerprints.get(failure_fingerprint, 0) + 1
                repeated_count = failure_fingerprints[failure_fingerprint]
                if repeated_count >= int(state.get("repeated_failure_limit") or 1):
                    updated_state = dict(state)
                    updated_state.update(
                        {
                            "tool_results": tool_results,
                            "artifacts": artifacts,
                            "failure_fingerprints": failure_fingerprints,
                        }
                    )
                    return {
                        "tool_results": tool_results,
                        "artifacts": artifacts,
                        "failure_fingerprints": failure_fingerprints,
                        **_complete_agent_turn(
                            updated_state,
                            final_text=_repeated_tool_failure_message(
                                result=result,
                                repeated_count=repeated_count,
                            ),
                        ),
                    }

        tool_result_blocks.append(
            {
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": [{"type": "text", "text": _tool_result_message_payload(result)}],
            }
        )

    if not tool_result_blocks:
        updated_state = dict(state)
        updated_state.update({"tool_results": tool_results, "artifacts": artifacts})
        return {
            "tool_results": tool_results,
            "artifacts": artifacts,
            **_complete_agent_turn(updated_state, final_text=_last_useful_tool_message(tool_results)),
        }

    messages.append({"role": "user", "content": tool_result_blocks})
    return {
        "messages": messages,
        "tool_results": tool_results,
        "artifacts": list(dict.fromkeys(artifacts)),
        "failure_fingerprints": failure_fingerprints,
    }


def _agent_turn_initial_tools_node(state: _AgentTurnGraphState) -> dict[str, object]:
    content = state.get("initial_tool_content")
    if not content:
        return {"initial_tool_content": None}
    update = _execute_agent_turn_tool_uses(state, list(content))
    update["initial_tool_content"] = None
    return update


def _agent_turn_model_node(state: _AgentTurnGraphState) -> dict[str, object]:
    iterations = int(state.get("iterations") or 0)
    max_iterations = state.get("max_iterations")
    if max_iterations is not None and iterations >= max_iterations:
        logger.warning(
            "Agent orchestrator reached max_iterations (conversation_id=%s, tool_results=%s)",
            state.get("conversation_id"),
            len(state.get("tool_results") or []),
        )
        return _complete_agent_turn(
            state,
            final_text=_last_useful_tool_message(list(state.get("tool_results") or [])),
            reached_iteration_limit=True,
        )
    iterations += 1
    response = state["orchestrator"].model_client.create(
        messages=list(state.get("messages") or []),
        tools=state["tool_registry"].list_tool_definitions(),
    )
    content = response.get("content") or []
    content_list = list(content) if isinstance(content, list) else []
    thinking_parts = list(state.get("thinking_parts") or [])
    thinking_text = extract_thinking_text(content_list)
    if thinking_text:
        thinking_parts.append(thinking_text)
    return {
        "iterations": iterations,
        "response": response,
        "stop_reason": response.get("stop_reason"),
        "content": content_list,
        "thinking_parts": thinking_parts,
    }


def _agent_turn_tools_node(state: _AgentTurnGraphState) -> dict[str, object]:
    messages = list(state.get("messages") or [])
    content = list(state.get("content") or [])
    messages.append({"role": "assistant", "content": _conversation_visible_content(content)})
    updated_state = dict(state)
    updated_state["messages"] = messages
    update = _execute_agent_turn_tool_uses(updated_state, content)
    tool_results = list(update.get("tool_results") or state.get("tool_results") or [])
    if state.get("enable_doctor_notifications", False) and "result" not in update:
        doctor_threshold = int(state.get("doctor_threshold") or 1)
        next_notice = int(state.get("next_doctor_notice_at") or doctor_threshold)
        if len(tool_results) >= next_notice:
            _report_long_running_tool_loop(
                state.get("runtime_store"),
                conversation_id=state["conversation_id"],
                principal_id=state["principal_id"],
                user_message=state["user_message"],
                tool_results=tool_results,
                threshold=doctor_threshold,
            )
            _notify_long_running_tool_loop(
                getattr(state["orchestrator"], "_deliver_fn", None),
                conversation_id=state["conversation_id"],
                tool_results=tool_results,
            )
            update["next_doctor_notice_at"] = next_notice + doctor_threshold
    return update


def _agent_turn_finalize_node(state: _AgentTurnGraphState) -> dict[str, object]:
    content = list(state.get("content") or [])
    tool_results = list(state.get("tool_results") or [])
    artifacts = list(state.get("artifacts") or [])
    messages = list(state.get("messages") or [])
    final_parts = [
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    final_text = "".join(part for part in final_parts if isinstance(part, str)).strip() or None
    if state.get("use_authoritative_completion_text", False):
        authoritative_text = _authoritative_tool_completion_text(tool_results)
        if authoritative_text is not None:
            final_text = authoritative_text
    if _is_bare_completion_text(final_text):
        if tool_results and tool_results[-1].status == "failed":
            final_text = _last_useful_tool_message(tool_results)
        elif tool_results:
            tool_completion_text = _tool_result_completion_text(tool_results, include_untrusted_fallback=False)
            if tool_completion_text is not None:
                final_text = tool_completion_text
            elif artifacts:
                final_text = final_text
            elif (structured_text := _tool_result_structured_text(tool_results)) is not None:
                final_text = structured_text
            elif not state.get("post_tool_delivery_nudged", False):
                messages.append(
                    {
                        "role": "assistant",
                        "content": _conversation_visible_content(content) or [{"type": "text", "text": "(empty)"}],
                    }
                )
                messages.append({"role": "user", "content": [{"type": "text", "text": _post_tool_delivery_nudge()}]})
                return {"messages": messages, "post_tool_delivery_nudged": True}
            else:
                final_text = _last_useful_tool_message(tool_results)
        else:
            final_text = _bare_completion_without_work_text(final_text)
    if (
        tool_results
        and not state.get("post_tool_delivery_nudged", False)
        and state.get("runtime_store") is not None
    ):
        decision = evaluate_response_fulfillment(
            store=state["runtime_store"],
            conversation_id=state["conversation_id"],
            user_message=state["user_message"],
            reply=final_text or "",
            tool_results=tool_results,
            artifact_paths=artifacts,
            artifact_roots=_artifact_roots_for_agent_turn(
                state["runtime_store"],
                state["principal_id"],
            ),
        )
        if not decision.satisfied and any(
            "attachment" in requirement for requirement in decision.missing_requirements
        ):
            messages.append(
                {
                    "role": "assistant",
                    "content": _conversation_visible_content(content) or [{"type": "text", "text": final_text or ""}],
                }
            )
            messages.append(
                {
                    "role": "user",
                    "content": [{"type": "text", "text": _missing_artifact_delivery_nudge(decision.missing_requirements)}],
                }
            )
            return {"messages": messages, "post_tool_delivery_nudged": True}
    final_text = sanitize_user_visible_reply(
        user_message=state["user_message"],
        reply=final_text,
        tool_results=tool_results,
        source="agent",
    )
    return _complete_agent_turn(state, final_text=final_text)


def _agent_turn_route_after_initial(state: _AgentTurnGraphState) -> str:
    return END if state.get("result") is not None else "model"


def _agent_turn_route_after_model(state: _AgentTurnGraphState) -> str:
    if state.get("result") is not None:
        return END
    return "tools" if state.get("stop_reason") == "tool_use" else "finalize"


def _agent_turn_route_after_step(state: _AgentTurnGraphState) -> str:
    return END if state.get("result") is not None else "model"


@lru_cache(maxsize=1)
def _compiled_agent_turn_graph():
    graph = StateGraph(_AgentTurnGraphState)
    graph.add_node("initial_tools", _agent_turn_initial_tools_node)
    graph.add_node("model", _agent_turn_model_node)
    graph.add_node("tools", _agent_turn_tools_node)
    graph.add_node("finalize", _agent_turn_finalize_node)
    graph.add_edge(START, "initial_tools")
    graph.add_conditional_edges("initial_tools", _agent_turn_route_after_initial, {"model": "model", END: END})
    graph.add_conditional_edges("model", _agent_turn_route_after_model, {"tools": "tools", "finalize": "finalize", END: END})
    graph.add_conditional_edges("tools", _agent_turn_route_after_step, {"model": "model", END: END})
    graph.add_conditional_edges("finalize", _agent_turn_route_after_step, {"model": "model", END: END})
    return graph.compile()


def _agent_turn_graph_config(max_iterations: int | None) -> dict[str, int]:
    budget = max_iterations if max_iterations is not None else 60
    return {"recursion_limit": max(25, budget * 3 + 8)}


class AgentOrchestrator:
    def __init__(self, *, model_client) -> None:
        self._model_client = model_client

    @property
    def model_client(self):
        return self._model_client

    def run_mission(
        self,
        *,
        mission: MissionRecord,
        conversation_id: str,
        principal_id: str,
        conversation_history: list[dict],
        tool_registry: ToolRegistry,
        policy_store,
        approval_store,
        runtime_store,
        resume_from_step: int = 0,
        resume_messages: list[dict] | None = None,
        max_iterations: int = 20,
        progress_callback: Callable[[str, int, int], None] | None = None,
    ) -> MissionResult:
        """Execute a mission sequentially across its steps.

        Args:
            progress_callback: Optional callable(message, completed, total) called after
                each step completes when continuation_policy is APPROVAL_GATED.  Use this
                to send step-level progress updates to the user without blocking.
        """
        del max_iterations
        runtime_store.add_mission(mission)
        mark_mission_running(runtime_store, mission.mission_id)
        # Clear any stale cancel flag from a previous run
        runtime_store.clear_mission_cancel(mission.mission_id)
        messages = list(resume_messages) if resume_messages is not None else list(conversation_history)
        if resume_messages is None:
            messages.append({"role": "user", "content": [{"type": "text", "text": mission.goal}]})
        artifacts: list[str] = []
        tool_results: list[ToolResult] = []
        completed_steps = resume_from_step
        is_approval_gated = mission.continuation_policy is MissionContinuationPolicy.APPROVAL_GATED

        for step_index in range(resume_from_step, len(mission.steps)):
            # Graceful cancel: check before starting each step
            if runtime_store.is_mission_cancelled(mission.mission_id):
                runtime_store.clear_mission_cancel(mission.mission_id)
                mark_mission_failed(
                    runtime_store,
                    mission.mission_id,
                    result_summary="Mission cancelled by user",
                )
                return MissionResult(
                    mission_id=mission.mission_id,
                    status="cancelled",
                    completed_steps=completed_steps,
                    total_steps=len(mission.steps),
                    final_summary="Mission cancelled by user",
                    artifacts=artifacts,
                    tool_results=tool_results,
                    interrupt_handled="cancel",
                )

            step = mission.steps[step_index]

            # Per-step delay (configurable via MissionStep.delay_seconds)
            if step.delay_seconds > 0:
                time.sleep(step.delay_seconds)

            mission.active_step_id = step.step_id
            runtime_store.add_mission(mission)
            step_message = _step_user_message(step)
            try:
                result = self.run_turn(
                    conversation_id=mission.mission_id,
                    principal_id=principal_id,
                    user_message=step_message,
                    conversation_history=messages,
                    tool_registry=tool_registry,
                    policy_store=policy_store,
                    approval_store=approval_store,
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                mark_mission_failed(runtime_store, mission.mission_id, result_summary=str(exc))
                return MissionResult(
                    mission_id=mission.mission_id,
                    status="failed",
                    completed_steps=completed_steps,
                    total_steps=len(mission.steps),
                    final_summary=str(exc),
                    artifacts=artifacts,
                    tool_results=tool_results,
                )

            artifacts.extend(result.artifacts)
            tool_results.extend(result.tool_results)
            if result.suspended_for_approval:
                approval_id = result.approval_id
                messages_snapshot = list(messages)
                if approval_id is not None:
                    runtime_store.add_suspended_turn(
                        SuspendedTurn(
                            approval_id=approval_id,
                            conversation_id=conversation_id,
                            chat_id=_messaging_target_from_conversation_id(conversation_id),
                            message=mission.goal,
                            request_id=None,
                            message_id=None,
                            created_at=datetime.now(UTC),
                            mission_id=mission.mission_id,
                            pending_step_idx=step_index,
                            messages_snapshot=messages_snapshot,
                            pending_tool_calls=_serialize_pending_tool_calls(result.tool_results),
                        )
                    )
                    mark_mission_waiting_approval(runtime_store, mission.mission_id, waiting_on=approval_id)
                return MissionResult(
                    mission_id=mission.mission_id,
                    status="suspended",
                    completed_steps=completed_steps,
                    total_steps=len(mission.steps),
                    final_summary=None,
                    artifacts=artifacts,
                    tool_results=tool_results,
                    suspended_approval_id=approval_id,
                )

            summary_text = result.final_text
            if summary_text is None and result.tool_results:
                summary_text = str(result.tool_results[-1].output)
            messages.append({"role": "assistant", "content": [{"type": "text", "text": summary_text or ""}]})
            completed_steps = step_index + 1

            # APPROVAL_GATED: emit a step-level progress update after each completed step
            if is_approval_gated and progress_callback is not None:
                try:
                    progress_msg = f"✓ Step {completed_steps}/{len(mission.steps)}: {step.title}"
                    if summary_text:
                        progress_msg += f"\n{summary_text}"
                    progress_callback(progress_msg, completed_steps, len(mission.steps))
                except Exception:  # pragma: no cover - callback errors must not kill the mission
                    pass

        final_summary = messages[-1]["content"][0]["text"] if messages else None
        mark_mission_completed(runtime_store, mission.mission_id, result_summary=final_summary)
        return MissionResult(
            mission_id=mission.mission_id,
            status="completed",
            completed_steps=len(mission.steps),
            total_steps=len(mission.steps),
            final_summary=final_summary,
            artifacts=artifacts,
            tool_results=tool_results,
        )

    def resume_mission(
        self,
        *,
        mission_id: str,
        conversation_id: str,
        principal_id: str,
        tool_registry: ToolRegistry,
        policy_store,
        approval_store,
        runtime_store,
        max_iterations: int = 20,
        progress_callback: Callable[[str, int, int], None] | None = None,
    ) -> MissionResult:
        del max_iterations
        mission = runtime_store.get_mission(mission_id)
        if mission is None:
            raise KeyError(mission_id)
        suspended_turn = next((turn for turn in reversed(runtime_store.list_suspended_turns()) if turn.mission_id == mission_id), None)
        if suspended_turn is None:
            raise KeyError(mission_id)
        runtime_store.remove_suspended_turn(suspended_turn.approval_id)
        return self.run_mission(
            mission=mission,
            conversation_id=conversation_id,
            principal_id=principal_id,
            conversation_history=[],
            tool_registry=tool_registry,
            policy_store=policy_store,
            approval_store=approval_store,
            runtime_store=runtime_store,
            resume_from_step=suspended_turn.pending_step_idx or 0,
            resume_messages=list(suspended_turn.messages_snapshot or []),
            progress_callback=progress_callback,
        )

    def run_turn(
        self,
        *,
        conversation_id: str,
        principal_id: str,
        user_message: str,
        user_content_blocks: list[dict[str, Any]] | None = None,
        conversation_history: list[dict],
        tool_registry: ToolRegistry,
        policy_store,
        approval_store,
        max_iterations: int | None = None,
        tool_result_callback: Callable[[ToolResult], None] | None = None,
    ) -> TurnResult:
        runtime_store = _resolve_runtime_store(policy_store=policy_store, approval_store=approval_store)
        messages = list(conversation_history)
        messages.append({"role": "user", "content": user_content_blocks or [{"type": "text", "text": user_message}]})
        doctor_threshold = _tool_loop_doctor_threshold()
        final_state = _compiled_agent_turn_graph().invoke(
            {
                "orchestrator": self,
                "conversation_id": conversation_id,
                "principal_id": principal_id,
                "user_message": user_message,
                "messages": messages,
                "tool_registry": tool_registry,
                "runtime_store": runtime_store,
                "max_iterations": max_iterations,
                "tool_result_callback": tool_result_callback,
                "cleanup_scope": f"turn-{uuid4().hex}",
                "cleanup_done": False,
                "tool_results": [],
                "artifacts": [],
                "iterations": 0,
                "doctor_threshold": doctor_threshold,
                "next_doctor_notice_at": doctor_threshold,
                "post_tool_delivery_nudged": False,
                "repeated_failure_limit": _repeated_tool_failure_limit(),
                "failure_fingerprints": {},
                "thinking_parts": [],
                "initial_tool_content": None,
                "enable_repeated_failure_guard": True,
                "enable_doctor_notifications": True,
                "use_authoritative_completion_text": True,
            },
            config=_agent_turn_graph_config(max_iterations),
        )
        result = final_state.get("result")
        if isinstance(result, TurnResult):
            return result
        raise RuntimeError("Agent turn graph finished without a TurnResult")

    def resume_turn(
        self,
        *,
        conversation_id: str,
        principal_id: str,
        user_message: str,
        messages_snapshot: list[dict[str, Any]],
        tool_registry: ToolRegistry,
        policy_store,
        approval_store,
        max_iterations: int | None = None,
        tool_result_callback: Callable[[ToolResult], None] | None = None,
    ) -> TurnResult:
        """Continue a suspended turn from its stored assistant tool call."""
        if not messages_snapshot:
            return self.run_turn(
                conversation_id=conversation_id,
                principal_id=principal_id,
                user_message=user_message,
                conversation_history=[],
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
                max_iterations=max_iterations,
                tool_result_callback=tool_result_callback,
            )

        runtime_store = _resolve_runtime_store(policy_store=policy_store, approval_store=approval_store)
        messages = list(messages_snapshot)
        initial_tool_content: list[dict[str, Any]] | None = None
        last_message = messages[-1] if messages else {}
        if isinstance(last_message, dict) and last_message.get("role") == "assistant":
            content = last_message.get("content") or []
            if isinstance(content, list) and any(
                isinstance(block, dict) and block.get("type") == "tool_use" for block in content
            ):
                initial_tool_content = list(content)

        final_state = _compiled_agent_turn_graph().invoke(
            {
                "orchestrator": self,
                "conversation_id": conversation_id,
                "principal_id": principal_id,
                "user_message": user_message,
                "messages": messages,
                "tool_registry": tool_registry,
                "runtime_store": runtime_store,
                "max_iterations": max_iterations,
                "tool_result_callback": tool_result_callback,
                "cleanup_scope": f"turn-{uuid4().hex}",
                "cleanup_done": False,
                "tool_results": [],
                "artifacts": [],
                "iterations": 0,
                "doctor_threshold": _tool_loop_doctor_threshold(),
                "next_doctor_notice_at": _tool_loop_doctor_threshold(),
                "post_tool_delivery_nudged": False,
                "repeated_failure_limit": _repeated_tool_failure_limit(),
                "failure_fingerprints": {},
                "thinking_parts": [],
                "initial_tool_content": initial_tool_content,
                "enable_repeated_failure_guard": False,
                "enable_doctor_notifications": False,
                "use_authoritative_completion_text": False,
            },
            config=_agent_turn_graph_config(max_iterations),
        )
        result = final_state.get("result")
        if isinstance(result, TurnResult):
            return result
        raise RuntimeError("Agent turn graph finished without a TurnResult")

    # ── Phase 5 dispatcher state (lazily populated) ────────────────────────

    # These are instance attributes set in __init__ or lazily on first use.
    # Declared here as class defaults so type checkers see them.
    _pool: Any = None
    _task_registry: Any = None
    _context_bus: Any = None
    _result_aggregator: Any = None
    _progress_queue: Any = None
    _aggregator_task: Any = None
    _deliver_fn: Any = None
    _supervisor_tasks: set[asyncio.Task] | None = None
    _dispatcher_loop: Any = None
    _dispatcher_thread: threading.Thread | None = None

    def set_deliver_fn(self, fn: Any) -> None:
        """Set the callback used by the result aggregator to deliver text."""
        self._deliver_fn = fn

    def _ensure_dispatcher_loop(self) -> asyncio.AbstractEventLoop:
        """Return the persistent loop used by sync chat adapters for background dispatch."""
        loop = self._dispatcher_loop
        if loop is not None and loop.is_running():
            return loop

        ready = threading.Event()
        state: dict[str, Any] = {}

        def _run() -> None:
            dispatcher_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(dispatcher_loop)
            state["loop"] = dispatcher_loop
            ready.set()
            dispatcher_loop.run_forever()
            pending = [task for task in asyncio.all_tasks(dispatcher_loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                dispatcher_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            dispatcher_loop.close()

        thread = threading.Thread(target=_run, name="nullion-mini-agent-dispatcher", daemon=True)
        thread.start()
        ready.wait(timeout=5)
        loop = state.get("loop")
        if loop is None:
            raise RuntimeError("Mini-agent dispatcher loop did not start.")
        self._dispatcher_loop = loop
        self._dispatcher_thread = thread
        return loop

    def dispatch_request_sync(self, *, timeout_s: float = 30.0, **kwargs: Any) -> "DispatchResult":
        """Submit a dispatch request from synchronous adapters without killing background tasks."""
        loop = self._ensure_dispatcher_loop()
        future = asyncio.run_coroutine_threadsafe(self.dispatch_request(**kwargs), loop)
        return future.result(timeout=timeout_s)

    def _record_dispatch_task_run_pending(self, store: Any, task: Any) -> None:
        if store is None or not hasattr(store, "add_mini_agent_run"):
            return
        try:
            if hasattr(store, "get_mini_agent_run") and store.get_mini_agent_run(task.task_id) is not None:
                return
            store.add_mini_agent_run(
                create_mini_agent_run(
                    run_id=task.task_id,
                    capsule_id=task.group_id,
                    mini_agent_type=task.title or "general",
                    created_at=getattr(task, "created_at", None) or datetime.now(UTC),
                )
            )
        except Exception:
            logger.debug("Could not record pending mini-agent run", exc_info=True)

    def _transition_dispatch_task_run(
        self,
        store: Any,
        task: Any,
        status: MiniAgentRunStatus,
        *,
        result_summary: str | None = None,
    ) -> None:
        if store is None or not hasattr(store, "get_mini_agent_run") or not hasattr(store, "add_mini_agent_run"):
            return
        try:
            existing = store.get_mini_agent_run(task.task_id)
            if existing is None:
                self._record_dispatch_task_run_pending(store, task)
                existing = store.get_mini_agent_run(task.task_id)
            if existing is None or existing.status is status:
                return
            if existing.status is MiniAgentRunStatus.PENDING and status in {
                MiniAgentRunStatus.COMPLETED,
                MiniAgentRunStatus.FAILED,
            }:
                existing = transition_mini_agent_run_status(existing, MiniAgentRunStatus.RUNNING)
                store.add_mini_agent_run(existing)
            store.add_mini_agent_run(
                transition_mini_agent_run_status(existing, status, result_summary=result_summary)
            )
        except Exception:
            logger.debug("Could not transition mini-agent run status", exc_info=True)

    def _supervision_interval_seconds(self) -> float:
        raw_value = os.environ.get("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "10").strip()
        try:
            return max(0.1, float(raw_value))
        except ValueError:
            return 10.0

    def _supervision_timeout_grace_seconds(self) -> float:
        raw_value = os.environ.get("NULLION_MINI_AGENT_SUPERVISION_GRACE_SECONDS", "5").strip()
        try:
            return max(0.0, float(raw_value))
        except ValueError:
            return 5.0

    async def _emit_supervised_status(self, conversation_id: str, text: str, **kwargs: Any) -> None:
        deliver_fn = self._deliver_fn
        if deliver_fn is None:
            return
        try:
            result = deliver_fn(conversation_id, text, **kwargs)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.debug("Could not deliver Mini-Agent supervision status", exc_info=True)

    async def _supervise_dispatch_group(
        self,
        group_id: str,
        *,
        policy_store: Any,
    ) -> None:
        from nullion.mini_agent_runner import ProgressUpdate
        from nullion.task_queue import TaskResult, TaskStatus

        interval = self._supervision_interval_seconds()
        grace = self._supervision_timeout_grace_seconds()
        last_status_at = 0.0
        try:
            while True:
                await asyncio.sleep(interval)
                if self._task_registry is None:
                    return
                group = self._task_registry.get_group(group_id)
                if group is None:
                    return
                if group.all_terminal():
                    return

                now = datetime.now(UTC)
                tasks_by_id = {task.task_id: task for task in group.tasks}
                failed_deps = {
                    task.task_id
                    for task in group.tasks
                    if task.status in {TaskStatus.FAILED, TaskStatus.CANCELLED}
                }
                failures: list[tuple[Any, str]] = []
                for task in group.tasks:
                    if task.is_terminal():
                        continue
                    failed_dependency_ids = [dep_id for dep_id in task.dependencies if dep_id in failed_deps]
                    if failed_dependency_ids:
                        failures.append((task, f"Dependency failed: {', '.join(failed_dependency_ids)}"))
                        continue
                    started_at = task.started_at or task.created_at
                    if started_at.tzinfo is None:
                        started_at = started_at.replace(tzinfo=UTC)
                    allowed_seconds = float(getattr(task, "timeout_s", 180.0) or 180.0) + grace
                    age_seconds = (now - started_at).total_seconds()
                    if age_seconds >= allowed_seconds:
                        failures.append((
                            task,
                            f"Timed out after {int(age_seconds)}s without reaching a terminal state.",
                        ))

                for task, reason in failures:
                    result = TaskResult(task_id=task.task_id, status="failure", error=reason)
                    await self._task_registry.update_task(
                        task.task_id,
                        status=TaskStatus.FAILED,
                        completed_at=now,
                        result=result,
                    )
                    self._transition_dispatch_task_run(
                        policy_store,
                        task,
                        MiniAgentRunStatus.FAILED,
                        result_summary=reason,
                    )
                    if self._progress_queue is not None:
                        await self._progress_queue.put(
                            ProgressUpdate(
                                agent_id=task.agent_id or "supervisor",
                                task_id=task.task_id,
                                group_id=task.group_id,
                                kind="task_failed",
                                message=reason,
                            )
                        )

                group = self._task_registry.get_group(group_id)
                if group is None or group.all_terminal():
                    return
                monotonic_now = time.monotonic()
                if monotonic_now - last_status_at >= interval:
                    last_status_at = monotonic_now
                    from nullion.task_status_format import format_task_status_summary

                    await self._emit_supervised_status(
                        group.conversation_id,
                        format_task_status_summary(
                            group.tasks,
                            planner_summary=_planner_summary_from_group(group),
                        ),
                        is_status=True,
                        group_id=group.group_id,
                        status_kind="task_summary",
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("Mini-Agent supervision failed for group %s", group_id, exc_info=True)
        finally:
            if self._supervisor_tasks is not None:
                current = asyncio.current_task()
                if current is not None:
                    self._supervisor_tasks.discard(current)

    def shutdown_dispatcher_sync(self, *, timeout_s: float = 5.0) -> None:
        """Stop the background dispatcher loop created for synchronous adapters."""
        loop = self._dispatcher_loop
        if loop is None or not loop.is_running():
            return
        future = asyncio.run_coroutine_threadsafe(self.shutdown_dispatcher(), loop)
        future.result(timeout=timeout_s)
        loop.call_soon_threadsafe(loop.stop)
        if self._dispatcher_thread is not None:
            self._dispatcher_thread.join(timeout=timeout_s)
        self._dispatcher_loop = None
        self._dispatcher_thread = None

    async def dispatch_request(
        self,
        *,
        conversation_id: str,
        principal_id: str,
        user_message: str,
        tool_registry: ToolRegistry,
        policy_store: Any,
        approval_store: Any,
        available_tools: list[str] | None = None,
        single_task_fast_path: bool = True,
    ) -> "DispatchResult":
        """Decompose *user_message* and dispatch tasks to mini-agents.

        Returns immediately with an acknowledgment. Task execution continues in
        background asyncio tasks. Single-task requests use run_turn() directly.
        """
        from nullion.context_bus import ContextBus
        from nullion.mini_agent_runner import MiniAgentRunner
        from nullion.result_aggregator import ResultAggregator
        from nullion.task_decomposer import TaskDecomposer
        from nullion.task_queue import TaskGroup, TaskRegistry, TaskStatus
        from nullion.task_status_format import (
            format_task_status_activity_detail,
            format_task_status_line,
            format_task_status_summary,
        )
        from nullion.warm_pool import WarmAgentPool

        tools = available_tools or [
            t.get("name", "") for t in tool_registry.list_tool_definitions()
        ]

        # Lazy init.
        if self._task_registry is None:
            self._task_registry = TaskRegistry()
        if self._context_bus is None:
            self._context_bus = ContextBus()
        if self._progress_queue is None:
            self._progress_queue = asyncio.Queue(maxsize=500)
        if self._deliver_fn is None:
            self._deliver_fn = lambda conv_id, text, **kw: None
        if self._result_aggregator is None:
            self._result_aggregator = ResultAggregator(
                deliver_fn=self._deliver_fn,
                task_registry=self._task_registry,
                model_client=self._model_client,
            )
        if self._aggregator_task is None or self._aggregator_task.done():
            self._aggregator_task = asyncio.create_task(
                self._result_aggregator.run(self._progress_queue),
                name="result-aggregator",
            )
        if self._supervisor_tasks is None:
            self._supervisor_tasks = set()
        if self._pool is None:
            self._pool = WarmAgentPool(min_size=3, max_size=20, shared_client=self._model_client)
            await self._pool.start()

        # Decompose.
        decomposer = TaskDecomposer(model_client=self._model_client)
        group: TaskGroup = decomposer.decompose(
            user_message,
            conversation_id=conversation_id,
            principal_id=principal_id,
            available_tools=tools,
        )
        await self._task_registry.add_group(group)

        # Single-task fast path — no async overhead.
        if len(group.tasks) == 1:
            task = group.tasks[0]
            if not single_task_fast_path:
                return DispatchResult(
                    group_id=group.group_id,
                    acknowledgment="",
                    task_count=1,
                    is_single_task=True,
                    dispatched=False,
                )
            turn_result = self.run_turn(
                conversation_id=conversation_id,
                principal_id=principal_id,
                user_message=task.description,
                conversation_history=[],
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
            )
            return DispatchResult(
                group_id=group.group_id,
                acknowledgment=turn_result.final_text or "(no reply)",
                task_count=1,
                is_single_task=True,
            )

        # Multi-task — build acknowledgment and spawn tasks.
        planner_summary = _planner_summary_from_group(group)
        acknowledgment = format_task_status_summary(
            group.tasks,
            planner_summary=planner_summary,
            default_status=TaskStatus.PENDING,
        )
        task_status_detail = format_task_status_activity_detail(
            group.tasks,
            status_lines={
                task.task_id: format_task_status_line(task, status=TaskStatus.PENDING)
                for task in group.tasks
            },
        )

        for task in group.tasks:
            self._record_dispatch_task_run_pending(policy_store, task)

        runner = MiniAgentRunner()
        for task in group.tasks:
            if task.status == TaskStatus.QUEUED:
                asyncio.create_task(
                    self._run_task(task, runner=runner, group=group,
                                   tool_registry=tool_registry,
                                   policy_store=policy_store,
                                   approval_store=approval_store),
                    name=f"task-{task.task_id}",
                )
        supervisor_task = asyncio.create_task(
            self._supervise_dispatch_group(group.group_id, policy_store=policy_store),
            name=f"supervise-{group.group_id}",
        )
        self._supervisor_tasks.add(supervisor_task)

        return DispatchResult(
            group_id=group.group_id,
            acknowledgment=acknowledgment,
            task_count=len(group.tasks),
            planner_summary=planner_summary,
            planner_metadata=dict(getattr(group, "planner_metadata", {}) or {}),
            task_titles=[task.title for task in group.tasks],
            task_status_detail=task_status_detail,
        )

    async def _run_task(
        self,
        task: Any,
        *,
        runner: Any,
        group: Any,
        tool_registry: ToolRegistry,
        policy_store: Any,
        approval_store: Any,
    ) -> None:
        from nullion.mini_agent_runner import MiniAgentConfig, ProgressUpdate
        from nullion.task_queue import TaskResult
        from nullion.task_queue import TaskStatus
        from nullion.warm_pool import get_agent_client

        agent = None
        result: TaskResult | None = None
        try:
            agent = await self._pool.acquire(preferred_tools=task.allowed_tools, task_id=task.task_id)
            config = MiniAgentConfig(
                agent_id=agent.agent_id,
                task=task,
                context_in=self._context_bus.get(task.context_key_in, group_id=task.group_id)
                           if task.context_key_in else None,
                timeout_s=float(getattr(task, "timeout_s", 180.0) or 180.0),
            )
            await self._task_registry.update_task(
                task.task_id, status=TaskStatus.RUNNING,
                started_at=datetime.now(UTC), agent_id=agent.agent_id,
            )
            self._transition_dispatch_task_run(policy_store, task, MiniAgentRunStatus.RUNNING)
            if self._progress_queue is not None:
                await self._progress_queue.put(
                    ProgressUpdate(
                        agent_id=agent.agent_id,
                        task_id=task.task_id,
                        group_id=task.group_id,
                        kind="task_started",
                    )
                )
            result = await runner.run(
                config,
                anthropic_client=get_agent_client(agent),
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
                context_bus=self._context_bus,
                progress_queue=self._progress_queue,
            )
        except Exception as exc:
            logger.warning("Mini-agent task %s failed before completion: %s", task.task_id, exc, exc_info=True)
            result = TaskResult(task_id=task.task_id, status="failure", error=str(exc) or exc.__class__.__name__)
        finally:
            if agent is not None:
                self._pool.release(agent)

        final_status = TaskStatus.COMPLETE if result.status == "success" else TaskStatus.FAILED
        self._transition_dispatch_task_run(
            policy_store,
            task,
            MiniAgentRunStatus.COMPLETED if result.status == "success" else MiniAgentRunStatus.FAILED,
            result_summary=result.output or result.error,
        )
        await self._task_registry.update_task(
            task.task_id, status=final_status,
            completed_at=datetime.now(UTC), result=result,
        )
        if self._progress_queue is not None:
            await self._progress_queue.put(
                ProgressUpdate(
                    agent_id=agent.agent_id,
                    task_id=task.task_id,
                    group_id=task.group_id,
                    kind="task_complete" if result.status == "success" else "task_failed",
                    message=result.output or result.error,
                )
            )

        # Unblock dependents.
        for dep_task in self._task_registry.ready_tasks_for_group(task.group_id):
            await self._task_registry.update_task(dep_task.task_id, status=TaskStatus.QUEUED)
            asyncio.create_task(
                self._run_task(dep_task, runner=runner, group=group,
                               tool_registry=tool_registry, policy_store=policy_store,
                               approval_store=approval_store),
                name=f"task-{dep_task.task_id}",
            )

        grp = self._task_registry.get_group(task.group_id)
        if grp is not None and grp.all_terminal():
            self._context_bus.clear_group(task.group_id)

    def get_status(
        self,
        *,
        conversation_id: str | None = None,
        group_id: str | None = None,
    ) -> list[Any]:
        if self._task_registry is None:
            return []
        if group_id:
            return self._task_registry.list_by_group(group_id)
        if conversation_id:
            return self._task_registry.list_by_conversation(conversation_id)
        return []

    async def cancel_task(self, task_id: str) -> bool:
        if self._task_registry is None:
            return False
        return await self._task_registry.cancel_task(task_id)

    async def cancel_group(self, group_id: str) -> int:
        if self._task_registry is None:
            return 0
        count = await self._task_registry.cancel_group(group_id)
        if self._context_bus is not None:
            self._context_bus.clear_group(group_id)
        return count

    async def shutdown_dispatcher(self) -> None:
        if self._supervisor_tasks:
            for task in list(self._supervisor_tasks):
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._supervisor_tasks, return_exceptions=True)
            self._supervisor_tasks.clear()
        if self._aggregator_task and not self._aggregator_task.done():
            self._aggregator_task.cancel()
            try:
                await self._aggregator_task
            except asyncio.CancelledError:
                pass
        if self._pool is not None:
            await self._pool.stop()


def _step_user_message(step: MissionStep) -> str:
    source_clause = step.metadata.get("source_clause") if isinstance(step.metadata, dict) else None
    if isinstance(source_clause, str) and source_clause.strip():
        return source_clause.strip()
    return step.title


def _messaging_target_from_conversation_id(conversation_id: str) -> str | None:
    if ":" in conversation_id:
        return conversation_id.split(":", 1)[1] or None
    return None


def _planner_summary_from_group(group: Any) -> str:
    metadata = getattr(group, "planner_metadata", None)
    if not isinstance(metadata, dict):
        return ""
    disposition = str(metadata.get("disposition") or "").strip()
    if not disposition:
        return ""
    label = disposition.replace("_", " ").title()
    tasks = metadata.get("tasks")
    task_count = len(tasks) if isinstance(tasks, list) else len(getattr(group, "tasks", ()) or ())
    if bool(metadata.get("needs_clarification")):
        return "Needs clarification"
    if not bool(metadata.get("valid", True)):
        return "Fallback to normal turn"
    if task_count:
        return f"{label} • {task_count} task{'s' if task_count != 1 else ''}"
    return label


def _serialize_pending_tool_calls(tool_results: list[ToolResult]) -> list[dict[str, object]]:
    return [
        {
            "invocation_id": result.invocation_id,
            "tool_name": result.tool_name,
            "status": result.status,
            "output": result.output,
            "error": result.error,
        }
        for result in tool_results
    ]


@dataclass
class DispatchResult:
    """Returned immediately by dispatch_request() before any task completes."""
    group_id: str
    acknowledgment: str           # "Working on N tasks: ..." or final reply (single-task)
    task_count: int
    is_single_task: bool = False  # True when the fast path (run_turn) was used
    dispatched: bool = True       # False when caller requested fallback for a single-task group
    planner_summary: str = ""
    planner_metadata: dict[str, object] | None = None
    task_titles: list[str] | None = None
    task_status_detail: str = ""


__all__ = ["AgentOrchestrator", "DispatchResult", "MissionResult", "TurnResult"]
