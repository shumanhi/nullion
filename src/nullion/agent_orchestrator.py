from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, TypedDict
from urllib.parse import urlparse
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.missions import MissionContinuationPolicy, MissionRecord, MissionStep
from nullion.mini_agent_runs import MiniAgentRunStatus, create_mini_agent_run, transition_mini_agent_run_status
from nullion.messaging_delivery_contract import foreground_reply_should_be_suppressed
from nullion.prompt_injection import (
    UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,
    UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,
    is_untrusted_tool_name,
    model_security_envelope,
    safe_untrusted_tool_metadata,
)
from nullion.response_sanitizer import (
    is_raw_tool_payload_reply,
    is_safe_raw_tool_payload_replacement_reply,
    safe_raw_tool_payload_replacement,
    sanitize_user_visible_reply,
)
from nullion.response_fulfillment_contract import (
    evaluate_response_fulfillment,
    artifact_media_plain_replacement_guard_result,
    normalize_artifact_media_required_extensions,
)
from nullion.runtime import (
    mark_mission_completed,
    mark_mission_failed,
    mark_mission_running,
    mark_mission_waiting_approval,
)
from nullion.suspended_turns import SuspendedTurn
from nullion.thinking_display import extract_thinking_text
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, normalize_tool_status

logger = logging.getLogger(__name__)

_DEFAULT_MODEL_TOOL_RESULT_MAX_CHARS = 87_420
_ALWAYS_COMPACT_MODEL_TOOL_OUTPUTS = frozenset({"terminal_exec", "workspace_summary"})


_ARTIFACT_RECOVERY_TOOLS = frozenset(
    {
        "file_write",
        "pdf_create",
        "pdf_edit",
        "render",
        "image_generate",
        "browser_screenshot",
    }
)


def _planner_task_timeout_seconds() -> float:
    from nullion.mini_agent_config import planner_mini_agent_timeout_seconds

    return planner_mini_agent_timeout_seconds()


def _planner_dependency_recovery_attempts() -> int:
    from nullion.mini_agent_config import planner_dependency_recovery_attempts

    return planner_dependency_recovery_attempts()


def _mini_agent_runner_concurrency_limit() -> int:
    try:
        value = int(os.environ.get("NULLION_MINI_AGENT_RUNNER_CONCURRENCY", "3"))
    except ValueError:
        value = 3
    return max(1, min(value, 8))


def _group_uses_planner_timeout(group: Any, *, single_task_fast_path: bool) -> bool:
    return len(getattr(group, "tasks", ()) or ()) > 1 or not single_task_fast_path


def _apply_planner_timeout_policy(group: Any, *, single_task_fast_path: bool) -> Any:
    if not _group_uses_planner_timeout(group, single_task_fast_path=single_task_fast_path):
        return group
    timeout_s = _planner_task_timeout_seconds()
    try:
        group.tasks = [
            replace(task, timeout_s=max(float(getattr(task, "timeout_s", 0.0) or 0.0), timeout_s))
            for task in group.tasks
        ]
    except Exception:
        logger.debug("Could not apply planner mini-agent timeout policy", exc_info=True)
    return group


def _task_has_artifact_delivery_scope(task: Any) -> bool:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    if bool(metadata.get("requires_artifact_delivery") or metadata.get("required_artifact_kind")):
        return True
    artifact_role = str(metadata.get("artifact_role") or "").strip()
    if artifact_role in {"deliverable", "deliver_receipt", "verify"}:
        return True
    allowed_tools = {str(tool) for tool in (getattr(task, "allowed_tools", None) or [])}
    return bool(allowed_tools.intersection(_ARTIFACT_RECOVERY_TOOLS))


def _task_has_explicit_artifact_delivery_contract(task: Any) -> bool:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    if bool(metadata.get("requires_artifact_delivery") or metadata.get("required_artifact_kind")):
        return True
    artifact_role = str(metadata.get("artifact_role") or "").strip()
    return artifact_role in {"deliver_receipt", "verify"}


def _task_is_scheduled_background_run(task: Any) -> bool:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    return bool(metadata.get("scheduled_task_run"))


def _mini_agent_run_metadata_for_task(task: Any) -> dict[str, object]:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    compact: dict[str, object] = {}
    for key in (
        "scheduled_task_run",
        "no_user_input_requests",
        "authoritative_scheduled_task_context",
    ):
        if metadata.get(key):
            compact[key] = True
    profiles = metadata.get("deep_agent_profiles")
    if isinstance(profiles, (list, tuple, set)):
        compact_profiles = tuple(
            str(profile).strip()
            for profile in profiles
            if str(profile).strip()
        )
        if compact_profiles:
            compact["deep_agent_profiles"] = compact_profiles
    return compact


def _task_dependency_recovery_description(
    group: Any,
    task: Any,
    *,
    failed_dependency_ids: list[str],
    tasks_by_id: dict[str, Any],
) -> str:
    lines = [
        str(getattr(task, "description", "") or getattr(task, "title", "") or "").strip(),
        "",
        "Dependency recovery context:",
        f"Original request: {getattr(group, 'original_message', '')}",
        "One or more prerequisite tasks reached a terminal failure before producing usable context:",
    ]
    for dependency_id in failed_dependency_ids:
        dependency = tasks_by_id.get(dependency_id)
        title = str(getattr(dependency, "title", dependency_id) or dependency_id)
        result = getattr(dependency, "result", None)
        detail = str(getattr(result, "error", None) or getattr(result, "output", None) or "failed").strip()
        lines.append(f"- {title}: {detail[:240]}")
    lines.extend(
        [
            "",
            "Recover the deliverable from the original request and verified runtime/tool evidence. "
            "Use the minimum additional tool work needed, create the requested artifact, and verify it before finishing.",
        ]
    )
    return "\n".join(line for line in lines if line is not None).strip()


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


def _artifact_paths_from_tool_result(
    result: ToolResult,
    *,
    runtime_store=None,
    include_file_write_path: bool = True,
) -> list[str]:
    if result.status != "completed":
        return []
    output = result.output if isinstance(result.output, dict) else {}
    forwarded_paths: list[str] = []
    for key in ("artifact_path", "artifact_paths", "artifacts"):
        value = output.get(key)
        if isinstance(value, list):
            forwarded_paths.extend(path for path in value if isinstance(path, str) and path)
        elif isinstance(value, str) and value:
            forwarded_paths.append(value)
    if forwarded_paths:
        return list(dict.fromkeys(forwarded_paths))
    if result.tool_name == "file_write" and include_file_write_path:
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
            path = str(artifact_path)
            output["path"] = path
            output["artifact_path"] = path
            output["artifact_paths"] = [path]
            output.pop("image_base64", None)
            return [path]
        except Exception:
            logger.warning("Failed to materialize browser screenshot artifact", exc_info=True)
    return []


def _turn_has_artifact_delivery_contract(state: Mapping[str, Any]) -> bool:
    tool_registry = state.get("tool_registry")
    if _required_attachment_extensions_from_turn_scope(tool_registry):
        return True
    evidence = getattr(tool_registry, "_evidence", None)
    if tuple(getattr(evidence, "requested_extensions", ()) or ()):
        return True
    if _required_embedded_media_extensions_from_turn_state(state):
        return True
    flow_context = state.get("tool_flow_context")
    return isinstance(flow_context, dict) and bool(
        flow_context.get("requires_artifact_delivery")
        or flow_context.get("artifact_extensions")
        or flow_context.get("required_artifact_extensions")
    )


def _artifact_root_snapshot(runtime_store, *, principal_id: str | None = None) -> dict[str, tuple[int, int]]:
    if runtime_store is None and not principal_id:
        return {}
    try:
        from nullion.artifacts import artifact_descriptor_for_path

        roots = _artifact_roots_for_agent_turn(runtime_store, principal_id or "") if principal_id else ()
        if not roots:
            from nullion.artifacts import artifact_root_for_runtime

            roots = (artifact_root_for_runtime(runtime_store),)
        snapshot: dict[str, tuple[int, int]] = {}
        for root in roots:
            root = Path(root).expanduser().resolve()
            if not root.exists():
                continue
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
    principal_id: str | None = None,
) -> list[str]:
    after = _artifact_root_snapshot(runtime_store, principal_id=principal_id)
    if not after:
        return []
    changed = [
        path
        for path, fingerprint in after.items()
        if before.get(path) != fingerprint
    ]
    return sorted(changed, key=lambda path: after[path][0])


def _model_tool_result_max_chars() -> int:
    raw = os.environ.get("NULLION_MODEL_TOOL_RESULT_MAX_CHARS", "")
    if raw.strip():
        try:
            return max(int(raw), 10_000)
        except ValueError:
            pass
    return _DEFAULT_MODEL_TOOL_RESULT_MAX_CHARS


def _latency_threshold_ms(env_name: str, default: float) -> float:
    raw = os.environ.get(env_name, "")
    if raw.strip():
        try:
            return max(float(raw), 0.0)
        except ValueError:
            pass
    return default


def _json_safe_tool_value(value: object) -> object:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        return {
            "content_kind": "binary",
            "byte_count": len(value),
            "body_omitted": True,
        }
    if isinstance(value, tuple):
        return [_json_safe_tool_value(item) for item in value]
    if isinstance(value, list):
        return [_json_safe_tool_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_tool_value(item) for key, item in value.items()}
    return str(value)


def _compact_tool_output_for_model_context(tool_name: str, output: object) -> object:
    safe_output = _json_safe_tool_value(output)
    if not isinstance(safe_output, dict):
        return _truncate_text(str(safe_output or ""), 12_000)
    if tool_name == "workspace_summary":
        extensions = safe_output.get("extensions")
        compact_extensions = extensions
        if isinstance(extensions, list):
            compact_extensions = sorted(
                extensions,
                key=lambda item: int(item.get("count") or 0) if isinstance(item, dict) else 0,
                reverse=True,
            )[:40]
        sample_files = safe_output.get("sample_files")
        compact_sample_files = sample_files
        if isinstance(sample_files, list):
            compact_sample_files = sample_files[:40]
        compact = {
            key: safe_output.get(key)
            for key in ("roots", "file_count", "directory_count", "scanned_entries", "truncated")
            if safe_output.get(key) is not None
        }
        if compact_extensions is not None:
            compact["extensions"] = compact_extensions
            if isinstance(extensions, list) and len(extensions) > 40:
                compact["extensions_truncated"] = {"shown": 40, "total": len(extensions)}
        if compact_sample_files is not None:
            compact["sample_files"] = compact_sample_files
            if isinstance(sample_files, list) and len(sample_files) > 40:
                compact["sample_files_truncated"] = {"shown": 40, "total": len(sample_files)}
        return compact
    if tool_name == "web_fetch":
        compact = {
            key: safe_output.get(key)
            for key in (
                "url",
                "status_code",
                "content_type",
                "content_kind",
                "title",
                "body_size",
                "body_truncated",
                "suggested_extension",
            )
            if safe_output.get(key) is not None
        }
        text = safe_output.get("text")
        if isinstance(text, str) and text.strip():
            compact["text"] = _truncate_text(text, 24_000)
        return compact
    if tool_name == "terminal_exec":
        compact = {
            key: safe_output.get(key)
            for key in ("exit_code", "shell", "timeout_seconds", "network_mode", "artifact_paths")
            if safe_output.get(key) is not None
        }
        for key in ("stdout", "stderr"):
            value = safe_output.get(key)
            if isinstance(value, str) and value.strip():
                compact[key] = _truncate_text(value, 4_000)
        return compact
    return _compact_tool_output_for_repair(tool_name, safe_output)


def _tool_result_message_payload(result: ToolResult) -> str:
    payload: dict[str, Any] = {
        "status": result.status,
        "output": (
            _compact_tool_output_for_model_context(result.tool_name, result.output)
            if result.tool_name in _ALWAYS_COMPACT_MODEL_TOOL_OUTPUTS
            else _json_safe_tool_value(result.output)
        ),
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
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    max_chars = _model_tool_result_max_chars()
    if len(text) <= max_chars:
        return text
    original_chars = len(text)
    payload["output"] = _compact_tool_output_for_model_context(result.tool_name, result.output)
    payload["model_context_compaction"] = {
        "original_json_chars": original_chars,
        "max_json_chars": max_chars,
    }
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if len(text) <= max_chars:
        return text
    payload["output"] = _truncate_text(json.dumps(payload["output"], ensure_ascii=False, sort_keys=True), max_chars // 2)
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


def _terminal_tool_failure_text(result: ToolResult) -> str | None:
    output = result.output if isinstance(result.output, dict) else {}
    reason = output.get("reason")
    if result.tool_name == "run_cron" and result.status == "failed":
        matches = output.get("matches")
        if isinstance(matches, list) and matches:
            lines = []
            for item in matches:
                if not isinstance(item, dict):
                    continue
                index = str(item.get("selection_index") or item.get("reply_with") or "").strip()
                name = str(item.get("name") or "").strip()
                if index and name:
                    lines.append(f"{index}. {name}")
            if lines:
                return "I found multiple matching cron jobs. Which one should I use?\n\n" + "\n".join(
                    lines
                ) + "\n\nReply with the number."
        if result.error and str(result.error).startswith("No cron found"):
            lookup = str(output.get("name") or output.get("id") or "").strip()
            target = f" for `{lookup}`" if lookup else ""
            return f"I couldn't find a scheduled cron job{target}."
    if (
        result.tool_name == "run_cron"
        and result.status == "failed"
        and reason == "cron_run_raw_tool_payload"
    ):
        cron_name = output.get("name")
        label = str(cron_name).strip() if isinstance(cron_name, str) and cron_name.strip() else "the scheduled task"
        return (
            f"I triggered {label}, but delivery was blocked because the run produced raw structured tool "
            "output instead of a readable report."
        )
    return None


def _foreground_suppressed_tool_completion_text(result: ToolResult) -> str | None:
    if result.status != "completed":
        return None
    output = result.output if isinstance(result.output, dict) else {}
    if not foreground_reply_should_be_suppressed([result]):
        return None
    message = str(output.get("message") or "").strip()
    delivery_status = str(output.get("delivery_status") or "").strip()
    if result.tool_name == "run_cron":
        if delivery_status == "sent":
            delivery_text = "Delivery was sent to the configured channel."
        elif delivery_status == "saved":
            delivery_text = "Delivery was saved to the configured destination."
        else:
            delivery_text = "The configured delivery completed."
        return " ".join(part for part in (message, delivery_text) if part).strip()
    return message or "Done."


def _deferred_background_tool_completion_text(result: ToolResult) -> str | None:
    if result.status != "completed":
        return None
    output = result.output if isinstance(result.output, dict) else {}
    if output.get("mini_agent_dispatch") is not True:
        return None
    if str(output.get("delivery_status") or output.get("cron_delivery_status") or "").strip() != "deferred":
        return None
    nested = output.get("result") if isinstance(output.get("result"), dict) else {}
    for key in ("result_text", "final_text", "text", "message"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in ("result_text", "final_text", "text", "message"):
        value = nested.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "Started. The result will be delivered when ready."


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
        if foreground_reply_should_be_suppressed([result]):
            continue
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
        if foreground_reply_should_be_suppressed([result]):
            continue
        value = output.get("delivery_text") or output.get("final_text") or output.get("result_text")
        if isinstance(value, str) and value.strip():
            return value.strip()[:8000]
    return None


def _tool_result_structured_text(tool_results: list[ToolResult]) -> str | None:
    for result in reversed(tool_results):
        if result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if foreground_reply_should_be_suppressed([result]):
            continue
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


def _raw_tool_payload_delivery_nudge() -> str:
    return (
        "Your draft final response was a raw structured tool payload. Convert the completed tool results "
        "into a concise human-readable answer for the user. Do not paste JSON, connector payloads, "
        "internal paths, or full raw tool output."
    )


def _raw_tool_payload_repair_system_prompt() -> str:
    return (
        "You are the final response repair step for a tool-using agent. "
        "Use only the verified tool evidence provided by the runtime and the original request. "
        "Write the concise user-facing answer that should have been delivered. "
        "Do not mention JSON, raw payloads, internal tool output, or repair. "
        "If the evidence is insufficient, say what was found and what could not be verified."
    )


def _compact_tool_evidence_for_repair(tool_results: list[ToolResult], *, limit: int = 7000) -> str:
    records: list[dict[str, object]] = []
    remaining = limit
    for result in tool_results[-8:]:
        output = result.output if isinstance(result.output, dict) else result.output
        record = {
            "tool_name": result.tool_name,
            "status": result.status,
            "error": result.error,
            "output": _compact_tool_output_for_repair(result.tool_name, output),
        }
        text = json.dumps(record, ensure_ascii=False, sort_keys=True)
        if len(text) > remaining:
            record["output"] = _truncate_text(str(record["output"]), max(200, remaining))
            text = json.dumps(record, ensure_ascii=False, sort_keys=True)
        if len(text) > remaining and records:
            break
        records.append(record)
        remaining -= min(len(text), remaining)
        if remaining <= 0:
            break
    return json.dumps(records, ensure_ascii=False, sort_keys=True)


def _compact_tool_output_for_repair(tool_name: str, output: object) -> object:
    if not isinstance(output, dict):
        return _truncate_text(str(output or ""), 1200)
    if tool_name == "connector_request":
        compact: dict[str, object] = {
            key: output.get(key)
            for key in ("provider_id", "method", "url", "status_code", "content_type")
            if output.get(key) is not None
        }
        data = output.get("json")
        if data is not None:
            compact["json"] = _compact_structured_value_for_repair(data)
        text = output.get("text")
        if isinstance(text, str) and text.strip():
            compact["text"] = _truncate_text(text, 1200)
        return compact
    compact_output: dict[str, object] = {}
    for key in ("result_text", "delivery_text", "final_text", "message", "summary", "content", "stdout", "result"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            compact_output[key] = _truncate_text(value, 1600)
    text = output.get("text")
    if isinstance(text, str) and text.strip():
        compact_output["text"] = _truncate_text(text, 2200)
    for key in ("path", "artifact_path", "artifact_paths", "artifacts", "url", "title", "length"):
        value = output.get(key)
        if value is not None:
            compact_output[key] = _compact_structured_value_for_repair(value)
    return compact_output or _compact_structured_value_for_repair(output)


def _compact_structured_value_for_repair(value: object, *, depth: int = 0) -> object:
    if depth >= 4:
        return _truncate_text(str(value), 300)
    if isinstance(value, str):
        return _truncate_text(value, 1200 if depth == 0 else 500)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_compact_structured_value_for_repair(item, depth=depth + 1) for item in value[:8]]
    if isinstance(value, dict):
        email_summary = _compact_email_message_for_repair(value)
        if email_summary is not None:
            return email_summary
        compact: dict[str, object] = {}
        preferred_keys = (
            "id",
            "threadId",
            "resultSizeEstimate",
            "messages",
            "items",
            "snippet",
            "subject",
            "from",
            "date",
            "name",
            "title",
            "summary",
            "text",
        )
        keys = [key for key in preferred_keys if key in value]
        keys.extend(key for key in value.keys() if key not in keys)
        for key in keys[:12]:
            compact[str(key)] = _compact_structured_value_for_repair(value.get(key), depth=depth + 1)
        return compact
    return _truncate_text(str(value), 500)


def _compact_email_message_for_repair(value: dict[str, object]) -> dict[str, object] | None:
    payload = value.get("payload")
    if not isinstance(payload, dict):
        return None
    headers = payload.get("headers")
    if not isinstance(headers, list):
        return None
    wanted = {"from", "to", "subject", "date"}
    compact_headers: dict[str, str] = {}
    for header in headers:
        if not isinstance(header, dict):
            continue
        name = str(header.get("name") or "").strip().lower()
        if name not in wanted:
            continue
        header_value = str(header.get("value") or "").strip()
        if header_value:
            compact_headers[name] = _truncate_text(header_value, 300)
    if not compact_headers:
        return None
    result: dict[str, object] = {
        "id": value.get("id"),
        "threadId": value.get("threadId"),
        "headers": compact_headers,
    }
    snippet = value.get("snippet")
    if isinstance(snippet, str) and snippet.strip():
        result["snippet"] = _truncate_text(snippet, 700)
    label_ids = value.get("labelIds")
    if isinstance(label_ids, list):
        result["labelIds"] = [str(item) for item in label_ids[:8]]
    return result


def _truncate_text(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _model_response_text(response: object) -> str:
    if not isinstance(response, dict):
        return ""
    content = response.get("content") or []
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and block.get("type") in {"text", "output_text"}:
            parts.append(str(block.get("text") or ""))
    return "".join(parts).strip()


def _repair_raw_tool_payload_final_text(state: "_AgentTurnGraphState", final_text: str | None) -> str | None:
    tool_results = list(state.get("tool_results") or [])
    if not tool_results:
        return None
    orchestrator = state.get("orchestrator")
    model_client = getattr(orchestrator, "model_client", None)
    if model_client is None:
        return None
    evidence = _compact_tool_evidence_for_repair(tool_results)
    if not evidence:
        return None
    prompt = (
        f"Original request:\n{state.get('user_message') or ''}\n\n"
        f"Rejected draft final response:\n{final_text or ''}\n\n"
        f"Verified compact tool evidence:\n{evidence}"
    )
    try:
        response = model_client.create(
            messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            tools=[],
            max_tokens=900,
            system=_raw_tool_payload_repair_system_prompt(),
        )
    except Exception:
        logger.debug("Raw tool payload final repair failed", exc_info=True)
        return None
    repaired = _model_response_text(response)
    if not repaired:
        return None
    if is_raw_tool_payload_reply(reply=repaired, tool_results=tool_results):
        return None
    if is_safe_raw_tool_payload_replacement_reply(reply=repaired, tool_results=tool_results):
        return None
    return repaired


def _missing_artifact_delivery_nudge(missing_requirements: tuple[str, ...]) -> str:
    missing = ", ".join(missing_requirements) or "the required attachment"
    return (
        "The active task is not deliverable yet. Before giving a final reply, produce and attach "
        f"{missing}. If a command failed, inspect the error, repair the script or command, rerun it, "
        "and only finish after a real artifact path is available."
    )


def _missing_required_tool_nudge(missing_requirements: tuple[str, ...]) -> str:
    missing = ", ".join(missing_requirements) or "the required tool completion"
    return (
        "The active task is not complete yet. Before giving a final reply, run the registered tool needed for "
        f"{missing}. If that tool requires approval, invoke it so the approval prompt is created."
    )


def _required_tool_names_from_turn_scope(tool_registry: object | None) -> tuple[str, ...]:
    decision = getattr(tool_registry, "turn_tool_scope_decision", None)
    if decision is None:
        return ()
    return tuple(
        dict.fromkeys(
            str(tool_name or "").strip()
            for tool_name in (getattr(decision, "required_tool_names", ()) or ())
            if str(tool_name or "").strip()
        )
    )


def _required_attachment_extensions_from_turn_scope(tool_registry: object | None) -> tuple[str, ...]:
    decision = getattr(tool_registry, "turn_tool_scope_decision", None)
    if decision is None:
        return ()
    return tuple(
        dict.fromkeys(
            str(extension or "").strip().lower()
            for extension in (getattr(decision, "requested_artifact_extensions", ()) or ())
            if str(extension or "").strip().startswith(".")
        )
    )


def _required_embedded_media_extensions_from_turn_scope(tool_registry: object | None) -> tuple[str, ...]:
    decision = getattr(tool_registry, "turn_tool_scope_decision", None)
    if decision is None:
        return ()
    return normalize_artifact_media_required_extensions(
        getattr(decision, "required_embedded_media_extensions", ()) or ()
    )


def _required_embedded_media_extensions_from_turn_state(state: Mapping[str, object]) -> tuple[str, ...]:
    extensions: list[str] = []
    flow_context = state.get("tool_flow_context")
    if isinstance(flow_context, dict):
        for key in (
            "required_embedded_media_extensions",
            "embedded_media_artifact_extensions",
            "media_required_artifact_extensions",
        ):
            for extension in normalize_artifact_media_required_extensions(flow_context.get(key)):
                if extension not in extensions:
                    extensions.append(extension)
    for extension in _required_embedded_media_extensions_from_turn_scope(state.get("tool_registry")):
        if extension not in extensions:
            extensions.append(extension)
    return tuple(extensions)


_SCHEDULER_RUN_ACTION_TOOLS = frozenset({"run_cron"})
_SCHEDULER_MUTATE_ACTION_TOOLS = frozenset(
    {
        "create_cron",
        "delete_cron",
        "set_reminder",
        "toggle_cron",
        "update_cron",
    }
)
_SCHEDULER_READ_ACTION_TOOLS = frozenset({"list_crons", "list_reminders"})


def _completed_tool_names(tool_results: Iterable[ToolResult] | None) -> set[str]:
    return {
        str(getattr(result, "tool_name", "") or "")
        for result in (tool_results or ())
        if normalize_tool_status(getattr(result, "status", None)) == "completed"
    }


def _scope_requested_capabilities(tool_results: Iterable[ToolResult] | None) -> set[str]:
    capabilities: set[str] = set()
    for result in tool_results or ():
        if str(getattr(result, "tool_name", "") or "") != "request_tool_scope":
            continue
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        raw_capabilities = output.get("capabilities")
        if isinstance(raw_capabilities, list):
            capabilities.update(
                str(capability or "").strip().lower()
                for capability in raw_capabilities
                if str(capability or "").strip()
            )
    return capabilities


def _scheduler_action_contract(tool_registry: object | None, tool_results: Iterable[ToolResult] | None) -> str:
    decision = getattr(tool_registry, "turn_tool_scope_decision", None)
    scheduler_action = str(getattr(decision, "scheduler_action", "") or "").strip().lower()
    capabilities = _scope_requested_capabilities(tool_results)
    if "scheduler_mutate" in capabilities:
        scheduler_action = "mutate"
    elif "scheduler_run" in capabilities and scheduler_action != "mutate":
        scheduler_action = "run"
    return scheduler_action if scheduler_action in {"run", "mutate"} else ""


def _scheduler_action_contract_missing(
    *,
    tool_registry: object | None,
    tool_results: Iterable[ToolResult] | None,
) -> str:
    action = _scheduler_action_contract(tool_registry, tool_results)
    if not action:
        return ""
    completed = _completed_tool_names(tool_results)
    if action == "mutate" and not completed.intersection(_SCHEDULER_MUTATE_ACTION_TOOLS):
        return "scheduler mutation"
    if action == "run" and not completed.intersection(_SCHEDULER_RUN_ACTION_TOOLS):
        return "scheduler run"
    return ""


def _missing_scope_action_nudge(missing: str) -> str:
    if missing == "scheduler mutation":
        return (
            "The current tool scope is for changing a scheduled task or reminder, but only read tools "
            "have completed. Continue the same user request by running the appropriate registered "
            "scheduler mutation tool, or ask the user for the missing schedule/detail. Do not finish by "
            "listing scheduled tasks."
        )
    if missing == "scheduler run":
        return (
            "The current tool scope is for starting a scheduled task run, but no run tool has completed. "
            "Continue the same user request by running the selected scheduled task, or ask the user which "
            "scheduled task to run. Do not finish by listing scheduled tasks."
        )
    return (
        "The requested action has not completed yet. Continue the same user request using the registered "
        "tool required for the action, or ask the user for the missing detail."
    )


def _missing_scope_action_final_reply(missing: str) -> str:
    if missing == "scheduler mutation":
        return (
            "I did not create or update a scheduled task yet. I need the missing schedule or task detail "
            "before I can complete it."
        )
    if missing == "scheduler run":
        return (
            "I did not start a scheduled task run yet. I need the specific scheduled task selection before "
            "I can run it."
        )
    return "I did not complete the requested action yet. I need one more detail before I can finish it."


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


DEFAULT_TOOL_LOOP_DOCTOR_THRESHOLD = 60


def _tool_loop_doctor_threshold() -> int:
    raw_value = os.environ.get("NULLION_TOOL_LOOP_DOCTOR_THRESHOLD", str(DEFAULT_TOOL_LOOP_DOCTOR_THRESHOLD)).strip()
    try:
        threshold = int(raw_value)
    except ValueError:
        return DEFAULT_TOOL_LOOP_DOCTOR_THRESHOLD
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
    if result.tool_name == "connector_request":
        # Connector retries often vary URL/operation while failing for the same
        # capability/provider reason. Count those together so recovery can move
        # to another available tool family before the agent loops.
        failure_shape = {
            "tool_name": result.tool_name,
            "status": result.status,
            "provider_id": output.get("provider_id"),
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
    tool_label = str(result.tool_name or "tool").replace("_", " ").strip() or "tool"
    if result.tool_name == "connector_request":
        output = result.output if isinstance(result.output, dict) else {}
        provider_id = str(output.get("provider_id") or "").strip()
        provider_text = f" for `{provider_id}`" if provider_id else ""
        error_text = str(result.error or "").strip()
        if len(error_text) > 220:
            error_text = error_text[:217].rstrip() + "..."
        detail = f" Last error: {error_text}" if error_text else ""
        return (
            f"The connector request{provider_text} failed {repeated_count} times, so I stopped retrying it. "
            f"{detail}".rstrip()
            + "\n\nI can still help with this. Options:\n"
            "1. Try another available tool path, such as browser or web tools.\n"
            "2. Use a configured connector for this account.\n"
            "3. Install or enable the matching skill/connector, then retry."
        )
    return (
        f"I stopped because the same {tool_label} step failed {repeated_count} times in a row. "
        "I did not keep retrying the same action."
    )


def _connector_failure_has_public_url_evidence(tool_results: list[ToolResult]) -> bool:
    for result in tool_results:
        if result.tool_name != "connector_request" or result.status == "completed":
            continue
        error_text = str(result.error or "")
        if "Blocked URL for connector_request:" not in error_text:
            continue
        if "http://" in error_text or "https://" in error_text:
            return True
    return False


def _failed_tool_result_count(tool_results: list[ToolResult], *, tool_name: str) -> int:
    return sum(1 for result in tool_results if result.tool_name == tool_name and result.status != "completed")


def _tool_registry_names(tool_registry: object) -> set[str]:
    try:
        return {str(definition.get("name") or "") for definition in tool_registry.list_tool_definitions()}
    except Exception:
        pass
    try:
        return {str(getattr(spec, "name", "") or "") for spec in tool_registry.list_specs()}
    except Exception:
        return set()


class _BlockedToolRegistry:
    def __init__(self, delegate: object, blocked_tool_names: set[str]) -> None:
        self._delegate = delegate
        self._blocked_tool_names = {str(name) for name in blocked_tool_names if str(name)}
        self.turn_tool_scope_decision = getattr(delegate, "turn_tool_scope_decision", None)

    def _is_blocked(self, tool_name: object) -> bool:
        return str(tool_name or "") in self._blocked_tool_names

    def get_spec(self, name: str):
        if self._is_blocked(name):
            raise KeyError(name)
        return self._delegate.get_spec(name)

    def list_specs(self):
        return [spec for spec in self._delegate.list_specs() if not self._is_blocked(getattr(spec, "name", ""))]

    def list_tool_definitions(self, *args, **kwargs):
        return [
            definition
            for definition in self._delegate.list_tool_definitions(*args, **kwargs)
            if not self._is_blocked(definition.get("name"))
        ]

    def filesystem_allowed_roots(self):
        roots = getattr(self._delegate, "filesystem_allowed_roots", None)
        if callable(roots):
            return roots()
        return ()

    def set_filesystem_allowed_roots(self, roots) -> None:
        setter = getattr(self._delegate, "set_filesystem_allowed_roots", None)
        if callable(setter):
            setter(roots)
            return
        setattr(self._delegate, "_filesystem_allowed_roots", tuple(Path(root).resolve() for root in roots))

    def invoke(self, invocation: ToolInvocation) -> ToolResult:
        if self._is_blocked(invocation.tool_name):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"reason": "tool_recovery_blocked"},
                error=f"{invocation.tool_name} was skipped after repeated failures; use the available fallback tools.",
            )
        return self._delegate.invoke(invocation)

    def apply_scope_request(self, invocation: ToolInvocation):
        apply_scope_request = getattr(self._delegate, "apply_scope_request", None)
        if not callable(apply_scope_request):
            raise AttributeError("delegate does not support apply_scope_request")
        result, widened = apply_scope_request(invocation)
        return result, _BlockedToolRegistry(widened, self._blocked_tool_names)


def _block_tools_for_recovery(tool_registry: object, blocked_tool_names: set[str]):
    if not blocked_tool_names:
        return tool_registry
    existing = getattr(tool_registry, "_blocked_tool_names", None)
    if isinstance(existing, set):
        blocked_tool_names = set(blocked_tool_names) | existing
        delegate = getattr(tool_registry, "_delegate", tool_registry)
        return _BlockedToolRegistry(delegate, blocked_tool_names)
    return _BlockedToolRegistry(tool_registry, blocked_tool_names)


def _synthetic_recovery_scope_result(
    state: _AgentTurnGraphState,
    *,
    tool_registry: object,
    skipped_scopes: set[str],
) -> tuple[ToolRegistry, ToolResult, str] | None:
    names = _tool_registry_names(tool_registry)
    recovery_candidates = (
        (
            "web",
            [
                "web_search",
                "web_fetch",
                "browser_open",
                "browser_navigate",
                "browser_extract_text",
                "browser_find",
                "browser_scroll",
                "browser_wait_for",
            ],
        ),
        ("local_shell", ["terminal_exec"]),
    )
    for recovery_scope, candidate_names in recovery_candidates:
        if recovery_scope in skipped_scopes:
            continue
        available_tools = [name for name in candidate_names if name in names]
        if not available_tools:
            continue
        return (
            tool_registry,
            ToolResult(
                invocation_id=f"orchestrator-{uuid4().hex}",
                tool_name="request_tool_scope",
                status="completed",
                output={
                    "scope_requested": True,
                    "capabilities": [recovery_scope],
                    "available_tools": available_tools,
                    "message": "Recovery tools are already available. Continue the same user request using them.",
                    "suppress_activity": True,
                },
            ),
            recovery_scope,
        )
    return None


def _maybe_widen_scope_after_repeated_tool_failure(
    state: _AgentTurnGraphState,
    *,
    result: ToolResult,
    tool_registry: ToolRegistry,
    tool_results: list[ToolResult],
    tool_recovery_scopes_attempted: list[str],
) -> tuple[ToolRegistry, ToolResult, str] | None:
    if result.tool_name != "connector_request":
        return None
    skipped_scopes = set(tool_recovery_scopes_attempted)
    if {"web", "local_shell"}.issubset(skipped_scopes):
        return None
    failure_limit = _connector_recovery_failure_limit(state)
    connector_failure_count = _failed_tool_result_count(tool_results, tool_name="connector_request")
    if connector_failure_count < failure_limit and not _connector_failure_has_public_url_evidence(tool_results):
        return None
    apply_scope_request = getattr(tool_registry, "apply_scope_request", None)
    if callable(apply_scope_request):
        for recovery_scope in ("web", "local_shell"):
            if recovery_scope in skipped_scopes:
                continue
            invocation = ToolInvocation(
                invocation_id=f"orchestrator-{uuid4().hex}",
                tool_name="request_tool_scope",
                principal_id=state["principal_id"],
                arguments={"capabilities": [recovery_scope]},
                capsule_id=state["cleanup_scope"],
            )
            try:
                scope_result, widened_registry = apply_scope_request(invocation)
            except Exception:
                logger.debug("Could not widen tool scope after repeated connector failure", exc_info=True)
                continue
            if scope_result.status != "completed":
                continue
            available_tools = []
            output = scope_result.output if isinstance(scope_result.output, dict) else {}
            raw_tools = output.get("available_tools")
            if isinstance(raw_tools, list):
                available_tools = [str(tool).strip() for tool in raw_tools if str(tool).strip()]
            if not available_tools:
                continue
            return widened_registry, scope_result, recovery_scope
    return _synthetic_recovery_scope_result(state, tool_registry=tool_registry, skipped_scopes=skipped_scopes)


def _scope_recovery_capabilities_for_tool_name(tool_name: str) -> tuple[str, ...]:
    normalized = str(tool_name or "").strip().lower()
    if not normalized:
        return ()
    if normalized.startswith("browser_") or normalized.startswith("web_"):
        return ("web",)
    if normalized in {"terminal_exec"}:
        return ("local_shell",)
    if normalized in {"file_read", "file_write", "file_search", "file_patch", "workspace_summary"}:
        return ("local_files", "local_shell")
    if normalized == "weather_forecast":
        return ("weather",)
    if normalized == "image_generate":
        return ("image_generation",)
    if normalized in {
        "list_crons",
        "list_reminders",
        "run_cron",
        "create_cron",
        "update_cron",
        "delete_cron",
        "toggle_cron",
        "set_reminder",
    }:
        if normalized in {"run_cron"}:
            return ("scheduler_run", "scheduler_read")
        if normalized in {"list_crons", "list_reminders"}:
            return ("scheduler_read",)
        return ("scheduler_mutate", "scheduler_read")
    if normalized.startswith("connector_") or normalized.startswith("email_") or normalized.startswith("calendar_") or normalized.startswith("contacts_"):
        return ("connector", "skill_pack")
    return ()


def _connector_recovery_failure_limit(state: Mapping[str, Any]) -> int:
    configured = int(state.get("repeated_failure_limit") or _repeated_tool_failure_limit())
    # Connector failures are often remote/provider-specific. Two failures are
    # enough evidence to try another available tool family without waiting for
    # a broader loop guard budget.
    return min(max(1, configured), 2)


def _maybe_widen_scope_after_scope_denial(
    state: _AgentTurnGraphState,
    *,
    result: ToolResult,
    tool_registry: ToolRegistry,
    tool_results: list[ToolResult] | None = None,
    tool_recovery_scopes_attempted: list[str],
) -> tuple[ToolRegistry, ToolResult, str] | None:
    if result.status not in {"denied", "failed"}:
        return None
    output = result.output if isinstance(result.output, dict) else {}
    reason = str(output.get("reason") or "").strip().lower()
    connector_failure_count = _failed_tool_result_count(tool_results or (), tool_name="connector_request")
    repeated_connector_failure = (
        result.tool_name == "connector_request"
        and result.status == "failed"
        and connector_failure_count >= _connector_recovery_failure_limit(state)
    )
    if reason not in {"tool_requires_structured_turn_scope", "unknown_tool"} and not repeated_connector_failure:
        return None
    capabilities = ("web", "local_shell") if repeated_connector_failure else _scope_recovery_capabilities_for_tool_name(result.tool_name)
    if not capabilities:
        return None
    skipped_scopes = set(tool_recovery_scopes_attempted)
    apply_scope_request = getattr(tool_registry, "apply_scope_request", None)
    if callable(apply_scope_request):
        for capability in capabilities:
            if capability in skipped_scopes:
                continue
            invocation = ToolInvocation(
                invocation_id=f"orchestrator-{uuid4().hex}",
                tool_name="request_tool_scope",
                principal_id=state["principal_id"],
                arguments={"capabilities": [capability]},
                capsule_id=state["cleanup_scope"],
            )
            try:
                scope_result, widened_registry = apply_scope_request(invocation)
            except Exception:
                logger.debug("Could not widen tool scope after scope denial", exc_info=True)
                continue
            if scope_result.status != "completed":
                continue
            scope_output = scope_result.output if isinstance(scope_result.output, dict) else {}
            available_tools = scope_output.get("available_tools")
            if not isinstance(available_tools, list) or not any(str(tool).strip() for tool in available_tools):
                continue
            return widened_registry, scope_result, capability
    if {"web", "local_shell"} & set(capabilities):
        return _synthetic_recovery_scope_result(
            state,
            tool_registry=tool_registry,
            skipped_scopes=skipped_scopes,
        )
    return None


def _append_tool_scope_recovery_result(
    *,
    tool_result_blocks: list[dict[str, object]],
    tool_use_id: str,
    failed_result: ToolResult,
    scope_result: ToolResult,
) -> None:
    tool_result_blocks.append(
        {
            "type": "tool_result",
            "tool_use_id": tool_use_id,
            "content": [
                {
                    "type": "text",
                    "text": (
                        _tool_result_message_payload(failed_result)
                        + "\n\n"
                        + _tool_result_message_payload(scope_result)
                    ),
                }
            ],
        }
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
    raw_tool_payload_blocked: bool = False


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
    text_delta_callback: Callable[[str], None] | None
    cancellation_checker: Callable[[], bool] | None
    tool_flow_context: dict[str, object] | None
    cleanup_scope: str
    cleanup_done: bool
    tool_results: list[ToolResult]
    artifacts: list[str]
    iterations: int
    doctor_threshold: int
    next_doctor_notice_at: int
    post_tool_delivery_nudged: bool
    raw_tool_payload_nudge_count: int
    repeated_failure_limit: int
    failure_fingerprints: dict[str, int]
    tool_recovery_scopes_attempted: list[str]
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


def _agent_turn_was_cancelled(state: _AgentTurnGraphState) -> bool:
    checker = state.get("cancellation_checker")
    if checker is None:
        return False
    try:
        return bool(checker())
    except Exception:
        logger.debug("Agent turn cancellation checker failed", exc_info=True)
        return False


def _tool_output_shape(output: object) -> dict[str, object]:
    if isinstance(output, dict):
        return {
            "type": "dict",
            "keys": sorted(str(key) for key in output.keys())[:40],
            "key_count": len(output),
        }
    if isinstance(output, list):
        return {"type": "list", "count": len(output)}
    if isinstance(output, str):
        return {"type": "str", "chars": len(output)}
    if output is None:
        return {"type": "none"}
    return {"type": type(output).__name__}


def _tool_source_domain(output: object) -> str | None:
    if not isinstance(output, dict):
        return None
    for key in ("source_url", "url", "geocoding_source_url", "forecast_source_url"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            parsed = urlparse(value)
            if parsed.netloc:
                return parsed.netloc
    source = output.get("source")
    if isinstance(source, dict):
        return _tool_source_domain(source)
    return None


def _message_payload_shape(messages: list[dict[str, Any]]) -> dict[str, int]:
    text_chars = 0
    block_count = 0
    for message in messages:
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, str):
            text_chars += len(content)
            block_count += 1
            continue
        if not isinstance(content, list):
            continue
        block_count += len(content)
        for block in content:
            if isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str):
                    text_chars += len(text)
                nested = block.get("content")
                if isinstance(nested, list):
                    for nested_block in nested:
                        if isinstance(nested_block, dict) and isinstance(nested_block.get("text"), str):
                            text_chars += len(nested_block["text"])
            elif isinstance(block, str):
                text_chars += len(block)
    return {"message_count": len(messages), "content_block_count": block_count, "text_chars": text_chars}


def _safe_prompt_section_label(role: str, text: str, index: int) -> str:
    if role != "system":
        return role or f"message_{index}"
    first_line = str(text or "").strip().splitlines()[0][:160] if str(text or "").strip() else ""
    known_prefixes = (
        ("You are Nullion", "capability_inventory"),
        ("Runtime configuration", "runtime_config"),
        ("Configured workspace connections", "workspace_connections"),
        ("Enabled skill packs", "skill_packs"),
        ("Skill access policy", "skill_access_policy"),
        ("Web delivery contract", "delivery_contract"),
        ("Chat delivery contract", "delivery_contract"),
        ("Known user memory", "memory_context"),
        ("Builder route hints", "builder_route_hints"),
        ("Recent tool context", "recent_tool_context"),
        ("Workspace", "workspace_context"),
    )
    for prefix, label in known_prefixes:
        if first_line.startswith(prefix):
            return label
    return f"system_{index}"


def _message_payload_breakdown(messages: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, message in enumerate(messages):
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "")
        content = message.get("content")
        chars = 0
        blocks = 0
        label_text = ""
        if isinstance(content, str):
            chars = len(content)
            blocks = 1
            label_text = content
        elif isinstance(content, list):
            blocks = len(content)
            label_parts: list[str] = []
            for block in content:
                if isinstance(block, dict):
                    text = block.get("text")
                    if isinstance(text, str):
                        chars += len(text)
                        if not label_parts:
                            label_parts.append(text)
                    nested = block.get("content")
                    if isinstance(nested, list):
                        for nested_block in nested:
                            if isinstance(nested_block, dict) and isinstance(nested_block.get("text"), str):
                                nested_text = nested_block["text"]
                                chars += len(nested_text)
                                if not label_parts:
                                    label_parts.append(nested_text)
                elif isinstance(block, str):
                    chars += len(block)
                    if not label_parts:
                        label_parts.append(block)
            label_text = "\n".join(label_parts)
        if chars <= 0:
            continue
        rows.append({
            "index": index,
            "role": role,
            "label": _safe_prompt_section_label(role, label_text, index),
            "text_chars": chars,
            "content_block_count": blocks,
        })
    rows.sort(key=lambda row: int(row.get("text_chars") or 0), reverse=True)
    return rows[: max(0, int(limit))]


def _record_agent_tool_timing(
    runtime_store: object,
    *,
    conversation_id: str,
    iteration: int,
    invocation: ToolInvocation,
    result: ToolResult,
    duration_ms: float,
    artifact_count: int,
) -> None:
    add_conversation_event = getattr(runtime_store, "add_conversation_event", None)
    if not callable(add_conversation_event):
        return
    output = getattr(result, "output", None)
    error_text = getattr(result, "error", None)
    try:
        add_conversation_event(
            {
                "event_id": f"tool-timing:{conversation_id}:{iteration}:{invocation.tool_name}:{uuid4().hex}",
                "conversation_id": conversation_id,
                "event_type": "conversation.tool_timing",
                "created_at": datetime.now(UTC).isoformat(),
                "iteration": iteration,
                "invocation_id": invocation.invocation_id,
                "tool_name": invocation.tool_name,
                "status": result.status,
                "duration_ms": round(duration_ms, 1),
                "argument_keys": sorted(str(key) for key in invocation.arguments.keys())[:40],
                "output_shape": _tool_output_shape(output),
                "source_domain": _tool_source_domain(output),
                "artifact_count": artifact_count,
                "error": str(error_text)[:240] if error_text else None,
            }
        )
    except Exception:
        logger.debug("Tool timing event recording failed", exc_info=True)


def _cancelled_agent_turn_update(state: _AgentTurnGraphState) -> dict[str, object]:
    return _complete_agent_turn(state, final_text="Stopped by /stop.")


def _complete_agent_turn(
    state: _AgentTurnGraphState,
    *,
    final_text: str | None,
    suspended_for_approval: bool = False,
    approval_id: str | None = None,
    reached_iteration_limit: bool = False,
    raw_tool_payload_blocked: bool = False,
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
            raw_tool_payload_blocked=raw_tool_payload_blocked,
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
    tool_recovery_scopes_attempted = list(state.get("tool_recovery_scopes_attempted") or [])
    tool_result_blocks: list[dict[str, object]] = []

    def _emit_tool_activity(result: ToolResult) -> None:
        if tool_result_callback is None:
            return
        try:
            tool_result_callback(result)
        except Exception:
            logger.debug("Tool result callback failed", exc_info=True)

    for block in content:
        if _agent_turn_was_cancelled(state):
            return _cancelled_agent_turn_update(state)
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
            flow_context=dict(state.get("tool_flow_context") or {}) or None,
        )
        if tool_name == "request_tool_scope":
            apply_scope_request = getattr(tool_registry, "apply_scope_request", None)
            if callable(apply_scope_request):
                _emit_tool_activity(
                    ToolResult(
                        invocation_id=invocation.invocation_id,
                        tool_name=tool_name,
                        status="running",
                        output={"suppress_activity": True},
                    )
                )
                tool_started_at = time.perf_counter()
                result, widened_registry = apply_scope_request(invocation)
                tool_duration_ms = (time.perf_counter() - tool_started_at) * 1000
                tool_registry = widened_registry
                state["tool_registry"] = widened_registry
                tool_results.append(result)
                _emit_tool_activity(result)
                if runtime_store is not None:
                    _record_agent_tool_timing(
                        runtime_store,
                        conversation_id=conversation_id,
                        iteration=int(state.get("iterations") or 0),
                        invocation=invocation,
                        result=result,
                        duration_ms=tool_duration_ms,
                        artifact_count=0,
                    )
                tool_result_blocks.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": [{"type": "text", "text": _tool_result_message_payload(result)}],
                    }
                )
                continue
        invocation_signature = _tool_invocation_signature(
            tool_name=tool_name,
            tool_input=dict(tool_input),
        )
        artifact_snapshot = (
            _artifact_root_snapshot(runtime_store, principal_id=principal_id)
            if tool_name == "terminal_exec" and runtime_store is not None
            else None
        )
        tool_started_at = time.perf_counter()
        _emit_tool_activity(
            ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=tool_name,
                status="running",
                output={},
            )
        )
        guarded_result = artifact_media_plain_replacement_guard_result(
            invocation,
            tool_results,
            required_embedded_media_extensions=_required_embedded_media_extensions_from_turn_state(state),
        )
        if guarded_result is not None:
            result = guarded_result
        else:
            try:
                if runtime_store is not None:
                    from nullion.runtime import invoke_tool_with_boundary_policy

                    result = invoke_tool_with_boundary_policy(runtime_store, invocation, registry=tool_registry)
                else:
                    result = tool_registry.invoke(invocation)
            except KeyError as exc:
                result = ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="failed",
                    output={
                        "reason": "unknown_tool",
                        "requested_tool_name": invocation.tool_name,
                        "suppress_activity": True,
                    },
                    error=str(exc),
                )
        tool_duration_ms = (time.perf_counter() - tool_started_at) * 1000
        tool_results.append(result)
        _emit_tool_activity(result)
        has_artifact_delivery_contract = _turn_has_artifact_delivery_contract(state)
        new_artifacts = _artifact_paths_from_tool_result(
            result,
            runtime_store=runtime_store,
            include_file_write_path=has_artifact_delivery_contract,
        )
        artifacts.extend(new_artifacts)
        if runtime_store is not None:
            _record_agent_tool_timing(
                runtime_store,
                conversation_id=conversation_id,
                iteration=int(state.get("iterations") or 0),
                invocation=invocation,
                result=result,
                duration_ms=tool_duration_ms,
                artifact_count=len(new_artifacts),
            )
        logger.info(
            "agent tool timing conversation_id=%s iteration=%s tool=%s status=%s duration_ms=%.1f artifacts=%s",
            conversation_id,
            int(state.get("iterations") or 0),
            tool_name,
            result.status,
            tool_duration_ms,
            len(new_artifacts),
        )
        if tool_duration_ms >= _latency_threshold_ms("NULLION_SLOW_TOOL_LOG_MS", 2000.0):
            logger.warning(
                "agent slow tool conversation_id=%s iteration=%s tool=%s status=%s duration_ms=%.1f source_domain=%s output_shape=%s",
                conversation_id,
                int(state.get("iterations") or 0),
                tool_name,
                result.status,
                tool_duration_ms,
                _tool_source_domain(getattr(result, "output", None)),
                _tool_output_shape(getattr(result, "output", None)),
            )
        if _agent_turn_was_cancelled(state):
            updated_state = dict(state)
            updated_state.update({"tool_results": tool_results, "artifacts": artifacts})
            return _cancelled_agent_turn_update(updated_state)
        if result.status == "completed" and artifact_snapshot is not None and has_artifact_delivery_contract:
            artifacts.extend(
                _new_artifact_paths_since(
                    artifact_snapshot,
                    runtime_store=runtime_store,
                    principal_id=principal_id,
                )
            )
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

        scope_recovery_update = _maybe_widen_scope_after_scope_denial(
            state,
            result=result,
            tool_registry=tool_registry,
            tool_results=tool_results,
            tool_recovery_scopes_attempted=tool_recovery_scopes_attempted,
        )
        if scope_recovery_update is not None:
            widened_registry, scope_result, recovery_scope = scope_recovery_update
            if result.tool_name == "connector_request" and recovery_scope in {"web", "local_shell"}:
                widened_registry = _block_tools_for_recovery(widened_registry, {result.tool_name})
            tool_registry = widened_registry
            state["tool_registry"] = widened_registry
            tool_results.append(scope_result)
            tool_recovery_scopes_attempted.append(recovery_scope)
            _append_tool_scope_recovery_result(
                tool_result_blocks=tool_result_blocks,
                tool_use_id=tool_use_id,
                failed_result=result,
                scope_result=scope_result,
            )
            continue

        terminal_failure_text = _terminal_tool_failure_text(result)
        if terminal_failure_text is not None:
            updated_state = dict(state)
            updated_state.update({"tool_results": tool_results, "artifacts": artifacts})
            return {
                "tool_results": tool_results,
                "artifacts": artifacts,
                **_complete_agent_turn(updated_state, final_text=terminal_failure_text),
            }

        foreground_suppressed_text = _foreground_suppressed_tool_completion_text(result)
        if foreground_suppressed_text is not None:
            updated_state = dict(state)
            updated_state.update({"tool_results": tool_results, "artifacts": artifacts})
            return {
                "tool_results": tool_results,
                "artifacts": artifacts,
                **_complete_agent_turn(updated_state, final_text=foreground_suppressed_text),
            }

        deferred_background_text = _deferred_background_tool_completion_text(result)
        if deferred_background_text is not None:
            updated_state = dict(state)
            updated_state.update({"tool_results": tool_results, "artifacts": artifacts})
            return {
                "tool_results": tool_results,
                "artifacts": artifacts,
                **_complete_agent_turn(updated_state, final_text=deferred_background_text),
            }

        if state.get("enable_repeated_failure_guard", False):
            failure_fingerprint = _tool_failure_fingerprint(
                result=result,
                invocation_signature=invocation_signature,
            )
            if failure_fingerprint is not None:
                failure_fingerprints[failure_fingerprint] = failure_fingerprints.get(failure_fingerprint, 0) + 1
                repeated_count = failure_fingerprints[failure_fingerprint]
                recovery_update = _maybe_widen_scope_after_repeated_tool_failure(
                    state,
                    result=result,
                    tool_registry=tool_registry,
                    tool_results=tool_results,
                    tool_recovery_scopes_attempted=tool_recovery_scopes_attempted,
                )
                if recovery_update is not None:
                    widened_registry, scope_result, recovery_scope = recovery_update
                    widened_registry = _block_tools_for_recovery(widened_registry, {result.tool_name})
                    tool_registry = widened_registry
                    state["tool_registry"] = widened_registry
                    tool_results.append(scope_result)
                    tool_recovery_scopes_attempted.append(recovery_scope)
                    _append_tool_scope_recovery_result(
                        tool_result_blocks=tool_result_blocks,
                        tool_use_id=tool_use_id,
                        failed_result=result,
                        scope_result=scope_result,
                    )
                    failure_fingerprints = {}
                    continue
                if repeated_count >= int(state.get("repeated_failure_limit") or 1):
                    updated_state = dict(state)
                    updated_state.update(
                        {
                            "tool_results": tool_results,
                            "artifacts": artifacts,
                            "failure_fingerprints": failure_fingerprints,
                            "tool_recovery_scopes_attempted": tool_recovery_scopes_attempted,
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
        "tool_registry": tool_registry,
        "tool_results": tool_results,
        "artifacts": list(dict.fromkeys(artifacts)),
        "failure_fingerprints": failure_fingerprints,
        "tool_recovery_scopes_attempted": tool_recovery_scopes_attempted,
    }


def _agent_turn_initial_tools_node(state: _AgentTurnGraphState) -> dict[str, object]:
    content = state.get("initial_tool_content")
    if not content:
        return {"initial_tool_content": None}
    update = _execute_agent_turn_tool_uses(state, list(content))
    update["initial_tool_content"] = None
    return update


def _agent_turn_model_node(state: _AgentTurnGraphState) -> dict[str, object]:
    if _agent_turn_was_cancelled(state):
        return _cancelled_agent_turn_update(state)
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
    tool_registry = state["tool_registry"]
    tool_results = list(state.get("tool_results") or [])
    tool_recovery_scopes_attempted = list(state.get("tool_recovery_scopes_attempted") or [])
    if _failed_tool_result_count(tool_results, tool_name="connector_request") >= _connector_recovery_failure_limit(state):
        last_connector_failure = next(
            (
                result
                for result in reversed(tool_results)
                if result.tool_name == "connector_request" and result.status != "completed"
            ),
            None,
        )
        if last_connector_failure is not None:
            recovery_update = _maybe_widen_scope_after_scope_denial(
                state,
                result=last_connector_failure,
                tool_registry=tool_registry,
                tool_results=tool_results,
                tool_recovery_scopes_attempted=tool_recovery_scopes_attempted,
            )
            if recovery_update is not None:
                widened_registry, scope_result, recovery_scope = recovery_update
                tool_registry = _block_tools_for_recovery(widened_registry, {"connector_request"})
                tool_results.append(scope_result)
                tool_recovery_scopes_attempted.append(recovery_scope)
                state = dict(state)
                state.update(
                    {
                        "messages": [
                            *list(state.get("messages") or []),
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": _tool_result_message_payload(scope_result),
                                    }
                                ],
                            },
                        ],
                        "tool_registry": tool_registry,
                        "tool_results": tool_results,
                        "tool_recovery_scopes_attempted": tool_recovery_scopes_attempted,
                    }
                )
    create_kwargs: dict[str, Any] = {
        "messages": list(state.get("messages") or []),
        "tools": tool_registry.list_tool_definitions(),
    }
    text_delta_callback = state.get("text_delta_callback")
    if text_delta_callback is not None:
        create_kwargs["text_delta_callback"] = text_delta_callback
    model_started_at = time.perf_counter()
    response = state["orchestrator"].model_client.create(**create_kwargs)
    model_duration_ms = (time.perf_counter() - model_started_at) * 1000
    logger.info(
        "agent model timing conversation_id=%s iteration=%s tools=%s duration_ms=%.1f stop_reason=%s",
        state.get("conversation_id"),
        iterations,
        len(create_kwargs.get("tools") or []),
        model_duration_ms,
        response.get("stop_reason"),
    )
    message_shape = _message_payload_shape(list(create_kwargs.get("messages") or []))
    context_breakdown = _message_payload_breakdown(list(create_kwargs.get("messages") or []))
    if (
        model_duration_ms >= _latency_threshold_ms("NULLION_SLOW_MODEL_LOG_MS", 5000.0)
        or message_shape["text_chars"] >= _latency_threshold_ms("NULLION_LARGE_CONTEXT_LOG_CHARS", 20_000.0)
    ):
        logger.warning(
            "agent slow model conversation_id=%s iteration=%s tools=%s duration_ms=%.1f stop_reason=%s messages=%s blocks=%s text_chars=%s streaming=%s",
            state.get("conversation_id"),
            iterations,
            len(create_kwargs.get("tools") or []),
            model_duration_ms,
            response.get("stop_reason"),
            message_shape["message_count"],
            message_shape["content_block_count"],
            message_shape["text_chars"],
            text_delta_callback is not None,
        )
    runtime_store = state.get("runtime_store")
    add_conversation_event = getattr(runtime_store, "add_conversation_event", None)
    if callable(add_conversation_event):
        try:
            add_conversation_event(
                {
                    "event_id": f"model-timing:{state.get('conversation_id') or ''}:{iterations}:{uuid4().hex}",
                    "conversation_id": str(state.get("conversation_id") or ""),
                    "event_type": "conversation.model_timing",
                    "created_at": datetime.now(UTC).isoformat(),
                    "iteration": iterations,
                    "tool_count": len(create_kwargs.get("tools") or []),
                    "message_count": message_shape["message_count"],
                    "content_block_count": message_shape["content_block_count"],
                    "text_chars": message_shape["text_chars"],
                    "context_breakdown": context_breakdown,
                    "streaming_enabled": text_delta_callback is not None,
                    "duration_ms": round(model_duration_ms, 1),
                    "stop_reason": response.get("stop_reason"),
                }
            )
        except Exception:
            logger.debug("Model timing event recording failed", exc_info=True)
    content = response.get("content") or []
    content_list = list(content) if isinstance(content, list) else []
    thinking_parts = list(state.get("thinking_parts") or [])
    thinking_text = extract_thinking_text(content_list)
    if thinking_text:
        thinking_parts.append(thinking_text)
    return {
        "iterations": iterations,
        "messages": list(state.get("messages") or []),
        "tool_registry": tool_registry,
        "tool_results": tool_results,
        "tool_recovery_scopes_attempted": tool_recovery_scopes_attempted,
        "response": response,
        "stop_reason": response.get("stop_reason"),
        "content": content_list,
        "thinking_parts": thinking_parts,
    }


def _agent_turn_tools_node(state: _AgentTurnGraphState) -> dict[str, object]:
    if _agent_turn_was_cancelled(state):
        return _cancelled_agent_turn_update(state)
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
    missing_scope_action = _scheduler_action_contract_missing(
        tool_registry=state.get("tool_registry"),
        tool_results=tool_results,
    )
    if missing_scope_action:
        if not state.get("post_tool_delivery_nudged", False):
            messages.append(
                {
                    "role": "assistant",
                    "content": _conversation_visible_content(content) or [{"type": "text", "text": final_text or ""}],
                }
            )
            messages.append(
                {
                    "role": "user",
                    "content": [{"type": "text", "text": _missing_scope_action_nudge(missing_scope_action)}],
                }
            )
            return {"messages": messages, "post_tool_delivery_nudged": True}
        final_text = _missing_scope_action_final_reply(missing_scope_action)
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
            required_attachment_extensions=_required_attachment_extensions_from_turn_scope(
                state.get("tool_registry")
            ),
            required_embedded_media_extensions=_required_embedded_media_extensions_from_turn_state(state),
            required_tool_names=_required_tool_names_from_turn_scope(state.get("tool_registry")),
        )
        if not decision.satisfied:
            missing_attachment = any(
                "attachment" in requirement for requirement in decision.missing_requirements
            )
            missing_required_tool = any(
                "required tool completion" in requirement
                for requirement in decision.missing_requirements
            )
        else:
            missing_attachment = False
            missing_required_tool = False
        if not decision.satisfied and (missing_attachment or missing_required_tool):
            messages.append(
                {
                    "role": "assistant",
                    "content": _conversation_visible_content(content) or [{"type": "text", "text": final_text or ""}],
                }
            )
            nudge_text = (
                _missing_artifact_delivery_nudge(decision.missing_requirements)
                if missing_attachment
                else _missing_required_tool_nudge(decision.missing_requirements)
            )
            messages.append(
                {
                    "role": "user",
                    "content": [{"type": "text", "text": nudge_text}],
                }
            )
            return {"messages": messages, "post_tool_delivery_nudged": True}
    if (
        tool_results
        and int(state.get("raw_tool_payload_nudge_count") or 0) < 1
        and (
            is_raw_tool_payload_reply(reply=final_text, tool_results=tool_results)
            or is_safe_raw_tool_payload_replacement_reply(reply=final_text, tool_results=tool_results)
        )
    ):
        nudge_count = int(state.get("raw_tool_payload_nudge_count") or 0) + 1
        messages.append(
            {
                "role": "assistant",
                "content": _conversation_visible_content(content) or [{"type": "text", "text": final_text or ""}],
            }
        )
        messages.append(
            {
                "role": "user",
                "content": [{"type": "text", "text": _raw_tool_payload_delivery_nudge()}],
            }
        )
        return {"messages": messages, "raw_tool_payload_nudge_count": nudge_count}
    raw_payload_like = bool(
        tool_results
        and (
            is_raw_tool_payload_reply(reply=final_text, tool_results=tool_results)
            or is_safe_raw_tool_payload_replacement_reply(reply=final_text, tool_results=tool_results)
        )
    )
    if raw_payload_like:
        repaired_final_text = _repair_raw_tool_payload_final_text(state, final_text)
        if repaired_final_text is not None:
            final_text = repaired_final_text
            raw_payload_like = bool(
                is_raw_tool_payload_reply(reply=final_text, tool_results=tool_results)
                or is_safe_raw_tool_payload_replacement_reply(reply=final_text, tool_results=tool_results)
            )
    final_text = sanitize_user_visible_reply(
        user_message=state["user_message"],
        reply=final_text,
        tool_results=tool_results,
        source="agent",
    )
    return _complete_agent_turn(
        state,
        final_text=final_text,
        raw_tool_payload_blocked=raw_payload_like,
    )


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
        text_delta_callback: Callable[[str], None] | None = None,
        cancellation_checker: Callable[[], bool] | None = None,
        tool_flow_context: dict[str, object] | None = None,
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
                "text_delta_callback": text_delta_callback,
                "cancellation_checker": cancellation_checker,
                "tool_flow_context": dict(tool_flow_context or {}) or None,
                "cleanup_scope": f"turn-{uuid4().hex}",
                "cleanup_done": False,
                "tool_results": [],
                "artifacts": [],
                "iterations": 0,
                "doctor_threshold": doctor_threshold,
                "next_doctor_notice_at": doctor_threshold,
                "post_tool_delivery_nudged": False,
                "raw_tool_payload_nudge_count": 0,
                "repeated_failure_limit": _repeated_tool_failure_limit(),
                "failure_fingerprints": {},
                "tool_recovery_scopes_attempted": [],
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
        tool_flow_context: dict[str, object] | None = None,
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
                tool_flow_context=tool_flow_context,
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
                "tool_flow_context": dict(tool_flow_context or {}) or None,
                "cleanup_scope": f"turn-{uuid4().hex}",
                "cleanup_done": False,
                "tool_results": [],
                "artifacts": [],
                "iterations": 0,
                "doctor_threshold": _tool_loop_doctor_threshold(),
                "next_doctor_notice_at": _tool_loop_doctor_threshold(),
                "post_tool_delivery_nudged": False,
                "raw_tool_payload_nudge_count": 0,
                "repeated_failure_limit": _repeated_tool_failure_limit(),
                "failure_fingerprints": {},
                "tool_recovery_scopes_attempted": [],
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
    _checkpoint_fn: Any = None
    _supervisor_tasks: set[asyncio.Task] | None = None
    _runner_tasks_by_group: dict[str, set[asyncio.Task]] | None = None
    _runner_semaphore: asyncio.Semaphore | None = None
    _dispatch_policy_store: Any = None
    _dispatcher_loop: Any = None
    _dispatcher_thread: threading.Thread | None = None

    def set_deliver_fn(self, fn: Any) -> None:
        """Set the callback used by the result aggregator to deliver text."""
        self._deliver_fn = fn
        if self._result_aggregator is not None:
            self._result_aggregator._deliver_fn = fn

    def set_checkpoint_fn(self, fn: Any) -> None:
        """Set the callback used to persist delegated-task state transitions."""
        self._checkpoint_fn = fn

    def _track_runner_task(self, group_id: str, task: asyncio.Task) -> None:
        if self._runner_tasks_by_group is None:
            self._runner_tasks_by_group = {}
        group_tasks = self._runner_tasks_by_group.setdefault(group_id, set())
        group_tasks.add(task)

        def _forget_runner_task(done_task: asyncio.Task, *, task_group_id: str = group_id) -> None:
            if self._runner_tasks_by_group is None:
                return
            tracked = self._runner_tasks_by_group.get(task_group_id)
            if tracked is None:
                return
            tracked.discard(done_task)
            if not tracked:
                self._runner_tasks_by_group.pop(task_group_id, None)

        task.add_done_callback(_forget_runner_task)

    def _spawn_runner_task(
        self,
        task: Any,
        *,
        runner: Any,
        group: Any,
        tool_registry: ToolRegistry,
        policy_store: Any,
        approval_store: Any,
        name: str,
    ) -> asyncio.Task:
        runner_task = asyncio.create_task(
            self._run_task(
                task,
                runner=runner,
                group=group,
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
            ),
            name=name,
        )
        self._track_runner_task(str(group.group_id), runner_task)
        return runner_task

    def _checkpoint_dispatch_state(self) -> None:
        fn = self._checkpoint_fn
        if fn is None:
            return
        try:
            result = fn()
            if asyncio.iscoroutine(result):
                logger.debug("Ignoring asynchronous mini-agent checkpoint callback")
        except Exception:
            logger.debug("Could not checkpoint mini-agent dispatch state", exc_info=True)

    async def _finalize_terminal_dispatch_group(self, group_id: str) -> None:
        if self._task_registry is None or self._result_aggregator is None:
            return
        group = self._task_registry.get_group(group_id)
        if group is None or not group.all_terminal():
            return
        from nullion.result_aggregator import GroupState

        group_state = self._result_aggregator._group_state.setdefault(
            group_id,
            GroupState(
                group_id=group_id,
                conversation_id=group.conversation_id,
                original_message=group.original_message,
            ),
        )
        await self._result_aggregator._on_group_complete(group_state, group)

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
                    metadata=_mini_agent_run_metadata_for_task(task),
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
            if existing is None:
                return
            existing_status = existing.status
            if not isinstance(existing_status, MiniAgentRunStatus):
                existing_status = MiniAgentRunStatus(str(existing_status))
                existing = replace(existing, status=existing_status)
            if existing_status == status:
                if result_summary is not None and result_summary != existing.result_summary:
                    store.add_mini_agent_run(replace(existing, result_summary=result_summary))
                return
            if existing_status == MiniAgentRunStatus.PENDING and status in {
                MiniAgentRunStatus.COMPLETED,
                MiniAgentRunStatus.FAILED,
            }:
                existing = transition_mini_agent_run_status(existing, MiniAgentRunStatus.RUNNING)
                store.add_mini_agent_run(existing)
            store.add_mini_agent_run(
                transition_mini_agent_run_status(existing, status, result_summary=result_summary)
            )
            persisted = store.get_mini_agent_run(task.task_id)
            persisted_status = getattr(persisted, "status", None)
            if persisted is not None and (
                persisted_status != status
                or (result_summary is not None and persisted.result_summary != result_summary)
            ):
                store.add_mini_agent_run(replace(persisted, status=status, result_summary=result_summary))
        except Exception:
            try:
                existing = store.get_mini_agent_run(task.task_id)
                if existing is not None:
                    existing_status = existing.status
                    if not isinstance(existing_status, MiniAgentRunStatus):
                        existing_status = MiniAgentRunStatus(str(existing_status))
                        existing = replace(existing, status=existing_status)
                    store.add_mini_agent_run(replace(existing, status=status, result_summary=result_summary))
                    return
            except Exception:
                pass
            logger.warning("Could not transition mini-agent run status", exc_info=True)

    def _force_persist_dispatch_task_run(
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
            if existing is None:
                return
            existing_status = existing.status
            if not isinstance(existing_status, MiniAgentRunStatus):
                existing_status = MiniAgentRunStatus(str(existing_status))
                existing = replace(existing, status=existing_status)
            if existing_status != status or (
                result_summary is not None and existing.result_summary != result_summary
            ):
                store.add_mini_agent_run(replace(existing, status=status, result_summary=result_summary))
        except Exception:
            logger.warning("Could not force-persist mini-agent run status", exc_info=True)

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

    async def _emit_supervised_status(self, conversation_id: str, text: str, **kwargs: Any) -> bool:
        deliver_fn = self._deliver_fn
        if deliver_fn is None:
            return False
        try:
            result = deliver_fn(conversation_id, text, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result is not False
        except Exception:
            logger.debug("Could not deliver Mini-Agent supervision status", exc_info=True)
            return False

    def _can_recover_blocked_artifact_task(self, task: Any, failed_dependency_ids: list[str]) -> bool:
        if not failed_dependency_ids:
            return False
        if _task_is_scheduled_background_run(task) and _task_has_explicit_artifact_delivery_contract(task):
            # A scheduled report verifier/deliverer must not invent missing
            # dependency context after an upstream failure. The terminal summary
            # can still report partial findings from the failed step.
            return False
        if not _task_has_artifact_delivery_scope(task):
            return False
        retry_count = int(getattr(task, "retry_count", 0) or 0)
        return retry_count < _planner_dependency_recovery_attempts()

    async def _recover_blocked_artifact_task(
        self,
        task: Any,
        *,
        group: Any,
        failed_dependency_ids: list[str],
        tasks_by_id: dict[str, Any],
    ) -> Any | None:
        if self._task_registry is None:
            return None
        from nullion.task_queue import TaskStatus

        dependency_tools: list[str] = []
        for dependency_id in failed_dependency_ids:
            dependency = tasks_by_id.get(dependency_id)
            dependency_tools.extend(str(tool) for tool in (getattr(dependency, "allowed_tools", None) or []))
        allowed_tools = list(dict.fromkeys([
            *(str(tool) for tool in (getattr(task, "allowed_tools", None) or [])),
            *dependency_tools,
        ]))
        description = _task_dependency_recovery_description(
            group,
            task,
            failed_dependency_ids=failed_dependency_ids,
            tasks_by_id=tasks_by_id,
        )
        timeout_s = max(
            float(getattr(task, "timeout_s", 0.0) or 0.0),
            _planner_task_timeout_seconds(),
        )
        recovered = await self._task_registry.update_task(
            task.task_id,
            status=TaskStatus.QUEUED,
            dependencies=[],
            retry_count=int(getattr(task, "retry_count", 0) or 0) + 1,
            allowed_tools=allowed_tools,
            description=description,
            timeout_s=timeout_s,
            started_at=None,
            completed_at=None,
            result=None,
            agent_id=None,
        )
        self._checkpoint_dispatch_state()
        return recovered

    async def _supervise_dispatch_group(
        self,
        group_id: str,
        *,
        policy_store: Any,
        runner: Any | None = None,
        tool_registry: ToolRegistry | None = None,
        approval_store: Any | None = None,
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
                    await self._finalize_terminal_dispatch_group(group_id)
                    return
                now = datetime.now(UTC)
                tasks_by_id = {task.task_id: task for task in group.tasks}
                failed_deps = {
                    task.task_id
                    for task in group.tasks
                    if task.status in {TaskStatus.FAILED, TaskStatus.CANCELLED}
                }
                failures: list[tuple[Any, str]] = []
                recoveries: list[tuple[Any, list[str]]] = []
                for task in group.tasks:
                    if task.is_terminal() or task.status == TaskStatus.WAITING_INPUT:
                        continue
                    failed_dependency_ids = _failed_dependency_ids(task, failed_deps, tasks_by_id)
                    if failed_dependency_ids:
                        if self._can_recover_blocked_artifact_task(task, failed_dependency_ids):
                            recoveries.append((task, failed_dependency_ids))
                            continue
                        failures.append((task, f"Dependency failed: {', '.join(failed_dependency_ids)}"))
                        continue
                    started_at = task.started_at or task.created_at
                    if started_at.tzinfo is None:
                        started_at = started_at.replace(tzinfo=UTC)
                    allowed_seconds = float(getattr(task, "timeout_s", 180.0) or 180.0) + grace
                    age_seconds = (now - started_at).total_seconds()
                    if age_seconds >= allowed_seconds:
                        if _task_is_scheduled_background_run(task):
                            continue
                        failures.append((
                            task,
                            f"Timed out after {int(age_seconds)}s without reaching a terminal state.",
                        ))

                for task, failed_dependency_ids in recoveries:
                    if runner is None or tool_registry is None:
                        failures.append((task, f"Dependency failed: {', '.join(failed_dependency_ids)}"))
                        continue
                    recovered_task = await self._recover_blocked_artifact_task(
                        task,
                        group=group,
                        failed_dependency_ids=failed_dependency_ids,
                        tasks_by_id=tasks_by_id,
                    )
                    if recovered_task is None:
                        failures.append((task, f"Dependency failed: {', '.join(failed_dependency_ids)}"))
                        continue
                    if self._progress_queue is not None:
                        await self._progress_queue.put(
                            ProgressUpdate(
                                agent_id=recovered_task.agent_id or "supervisor",
                                task_id=recovered_task.task_id,
                                group_id=recovered_task.group_id,
                                kind="progress_note",
                                message="Recovering deliverable after dependency failure.",
                            )
                        )
                    self._spawn_runner_task(
                        recovered_task,
                        runner=runner,
                        group=group,
                        tool_registry=tool_registry,
                        policy_store=policy_store,
                        approval_store=approval_store,
                        name=f"task-recovery-{recovered_task.task_id}",
                    )

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
                if group is not None and group.all_terminal():
                    # Supervisor-created terminal transitions do not pass
                    # through a runner task, so finalize them here before
                    # treating the group as quiet.
                    await self._finalize_terminal_dispatch_group(group_id)
                    return
                if group is None or _group_all_quiescent(group):
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
                            subject=group.original_message,
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
        dag_plan: Any | None = None,
        preferred_group_id: str | None = None,
        requires_artifact_delivery: bool = False,
        required_artifact_kind: str | None = None,
    ) -> "DispatchResult":
        """Decompose *user_message* and dispatch tasks to mini-agents.

        Returns immediately with an acknowledgment. Task execution continues in
        background asyncio tasks. Single-task requests use run_turn() directly.
        """
        from nullion.context_bus import ContextBus
        from nullion.deep_agent_profiles import deep_agent_tool_profile_metadata
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

        tool_definitions = tool_registry.list_tool_definitions()
        tools = available_tools or [t.get("name", "") for t in tool_definitions]
        tool_profile_metadata = deep_agent_tool_profile_metadata(tool_definitions)
        self._dispatch_policy_store = policy_store

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
        if self._runner_tasks_by_group is None:
            self._runner_tasks_by_group = {}
        runner_limit = _mini_agent_runner_concurrency_limit()
        if self._runner_semaphore is None:
            self._runner_semaphore = asyncio.Semaphore(runner_limit)
        if self._pool is None:
            self._pool = WarmAgentPool(min_size=min(2, runner_limit), max_size=max(2, runner_limit), shared_client=self._model_client)

        # Decompose.
        decomposer = TaskDecomposer(model_client=self._model_client)
        group: TaskGroup = decomposer.decompose(
            user_message,
            conversation_id=conversation_id,
            principal_id=principal_id,
            available_tools=tools,
            dag_plan=dag_plan,
            requires_artifact_delivery=requires_artifact_delivery,
            required_artifact_kind=required_artifact_kind,
            tool_profile_metadata=tool_profile_metadata,
        )
        preferred_group_id = str(preferred_group_id or "").strip()
        if preferred_group_id and preferred_group_id != group.group_id:
            group = replace(
                group,
                group_id=preferred_group_id,
                tasks=[replace(task, group_id=preferred_group_id) for task in group.tasks],
            )
        group = _apply_planner_timeout_policy(group, single_task_fast_path=single_task_fast_path)
        await self._task_registry.add_group(group)
        self._checkpoint_dispatch_state()

        # Single-task fast path — no async overhead unless the caller explicitly
        # requested planner/mini-agent status delivery.
        if len(group.tasks) == 1:
            task = group.tasks[0]
            if single_task_fast_path:
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

        # Planner dispatch path — build acknowledgment and spawn task runner(s).
        planner_summary = _planner_summary_from_group(group)
        acknowledgment = format_task_status_summary(
            group.tasks,
            planner_summary=planner_summary,
            subject=user_message,
            default_status=TaskStatus.PENDING,
            include_next_request_hint=True,
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
        self._checkpoint_dispatch_state()

        runner = MiniAgentRunner()
        status_delivered = await self._emit_supervised_status(
            group.conversation_id,
            acknowledgment,
            is_status=True,
            group_id=group.group_id,
            status_kind="task_summary",
        )
        await self._pool.start()
        for task in group.tasks:
            if task.status == TaskStatus.QUEUED:
                self._spawn_runner_task(
                    task,
                    runner=runner,
                    group=group,
                    tool_registry=tool_registry,
                    policy_store=policy_store,
                    approval_store=approval_store,
                    name=f"task-{task.task_id}",
                )
        supervisor_task = asyncio.create_task(
            self._supervise_dispatch_group(
                group.group_id,
                policy_store=policy_store,
                runner=runner,
                tool_registry=tool_registry,
                approval_store=approval_store,
            ),
            name=f"supervise-{group.group_id}",
        )
        self._supervisor_tasks.add(supervisor_task)

        return DispatchResult(
            group_id=group.group_id,
            acknowledgment=acknowledgment,
            task_count=len(group.tasks),
            is_single_task=len(group.tasks) == 1,
            planner_summary=planner_summary,
            planner_metadata=dict(getattr(group, "planner_metadata", {}) or {}),
            task_titles=[task.title for task in group.tasks],
            task_status_detail=task_status_detail,
            status_delivered=status_delivered,
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
        semaphore = self._runner_semaphore
        if semaphore is None:
            await self._run_task_inner(
                task,
                runner=runner,
                group=group,
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
            )
            return
        async with semaphore:
            await self._run_task_inner(
                task,
                runner=runner,
                group=group,
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
            )

    async def _run_task_inner(
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
        agent_id = task.agent_id or "mini-agent"
        result: TaskResult | None = None
        cancelled = False
        try:
            current_task_record = self._task_registry.get_task(task.task_id) if self._task_registry is not None else None
            if current_task_record is not None and current_task_record.status == TaskStatus.CANCELLED:
                return
            agent = await self._pool.acquire(preferred_tools=task.allowed_tools, task_id=task.task_id)
            agent_id = agent.agent_id
            task_metadata = getattr(task, "metadata", None)
            task_metadata = task_metadata if isinstance(task_metadata, dict) else {}
            can_request_user_input = not bool(
                task_metadata.get("no_user_input_requests")
                or task_metadata.get("scheduled_task_run")
            )
            config = MiniAgentConfig(
                agent_id=agent.agent_id,
                task=task,
                context_in=self._context_bus.get(task.context_key_in, group_id=task.group_id)
                           if task.context_key_in else None,
                timeout_s=float(getattr(task, "timeout_s", 180.0) or 180.0),
                can_request_user_input=can_request_user_input,
            )
            await self._task_registry.update_task(
                task.task_id, status=TaskStatus.RUNNING,
                started_at=datetime.now(UTC), agent_id=agent.agent_id,
            )
            self._transition_dispatch_task_run(policy_store, task, MiniAgentRunStatus.RUNNING)
            self._checkpoint_dispatch_state()
            if self._progress_queue is not None:
                await self._progress_queue.put(
                    ProgressUpdate(
                        agent_id=agent.agent_id,
                        task_id=task.task_id,
                        group_id=task.group_id,
                        kind="task_started",
                    )
                )
            run_coro = runner.run(
                config,
                anthropic_client=get_agent_client(agent),
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
                context_bus=self._context_bus,
                progress_queue=self._progress_queue,
            )
            if _task_is_scheduled_background_run(task):
                result = await run_coro
            else:
                timeout_seconds = max(0.1, float(config.timeout_s or 180.0))
                try:
                    result = await asyncio.wait_for(run_coro, timeout=timeout_seconds)
                except asyncio.TimeoutError:
                    result = TaskResult(
                        task_id=task.task_id,
                        status="failure",
                        error=f"Timed out after {timeout_seconds:g}s without reaching a terminal state.",
                    )
        except asyncio.CancelledError:
            cancelled = True
            result = TaskResult(task_id=task.task_id, status="cancelled", error="Cancelled by user.")
        except Exception as exc:
            logger.warning("Mini-agent task %s failed before completion: %s", task.task_id, exc, exc_info=True)
            result = TaskResult(task_id=task.task_id, status="failure", error=str(exc) or exc.__class__.__name__)
        finally:
            if agent is not None:
                self._pool.release(agent)

        final_status = TaskStatus.CANCELLED if cancelled else _task_status_for_task_result(result)
        _store_delegated_pause_suspended_turn(policy_store, approval_store, task=task, result=result, agent_id=agent_id)
        self._transition_dispatch_task_run(
            policy_store,
            task,
            _mini_agent_run_status_for_task_result(result),
            result_summary=result.output or result.error,
        )
        self._force_persist_dispatch_task_run(
            policy_store,
            task,
            _mini_agent_run_status_for_task_result(result),
            result_summary=result.output or result.error,
        )
        await self._task_registry.update_task(
            task.task_id, status=final_status,
            completed_at=datetime.now(UTC), result=result,
        )
        self._checkpoint_dispatch_state()
        if self._progress_queue is not None:
            await self._progress_queue.put(
                ProgressUpdate(
                    agent_id=agent_id,
                    task_id=task.task_id,
                    group_id=task.group_id,
                    kind="task_cancelled" if final_status == TaskStatus.CANCELLED else _progress_kind_for_task_result(result),
                    message=result.output or result.error,
                )
            )

        # Unblock dependents.
        for dep_task in self._task_registry.ready_tasks_for_group(task.group_id):
            await self._task_registry.update_task(dep_task.task_id, status=TaskStatus.QUEUED)
            self._checkpoint_dispatch_state()
            self._spawn_runner_task(
                dep_task,
                runner=runner,
                group=group,
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
                name=f"task-{dep_task.task_id}",
            )

        grp = self._task_registry.get_group(task.group_id)
        if grp is not None and grp.all_terminal():
            await self._finalize_terminal_dispatch_group(task.group_id)
            self._context_bus.clear_group(task.group_id)

    async def resume_paused_task(
        self,
        *,
        task_id: str,
        tool_registry: ToolRegistry,
        policy_store: Any,
        approval_store: Any,
        user_response: str | None = None,
    ) -> Any | None:
        """Resume a delegated task that paused for approval or user input.

        Resume re-runs the scoped delegated task with the approval now granted
        or the user's response appended to the task prompt. Dispatch state is
        checkpointed before and after each resume transition when the host
        runtime provides a checkpoint callback.
        """
        from nullion.mini_agent_runner import MiniAgentRunner
        from nullion.task_queue import TaskStatus

        if self._task_registry is None:
            return None
        task = self._task_registry.get_task(task_id)
        if task is None or task.status is not TaskStatus.WAITING_INPUT:
            return None
        group = self._task_registry.get_group(task.group_id)
        if group is None:
            return None
        description = task.description
        response = str(user_response or "").strip()
        if response:
            description = f"{description}\n\nUser response for paused task: {response}"
        task = await self._task_registry.update_task(
            task.task_id,
            status=TaskStatus.QUEUED,
            completed_at=None,
            result=None,
            description=description,
        )
        if task is None:
            return None
        self._checkpoint_dispatch_state()
        await self._run_task(
            task,
            runner=MiniAgentRunner(),
            group=group,
            tool_registry=tool_registry,
            policy_store=policy_store,
            approval_store=approval_store,
        )
        updated = self._task_registry.get_task(task_id)
        return getattr(updated, "result", None)

    def resume_paused_task_sync(
        self,
        *,
        task_id: str,
        tool_registry: ToolRegistry,
        policy_store: Any,
        approval_store: Any,
        user_response: str | None = None,
        timeout_s: float = 30.0,
    ) -> Any | None:
        loop = self._ensure_dispatcher_loop()
        future = asyncio.run_coroutine_threadsafe(
            self.resume_paused_task(
                task_id=task_id,
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
                user_response=user_response,
            ),
            loop,
        )
        return future.result(timeout=timeout_s)

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
        return list(getattr(self._task_registry, "_tasks", {}).values())

    def live_dispatch_group_ids(self) -> set[str]:
        """Return groups with an actual in-process runner task still alive."""
        if not self._runner_tasks_by_group:
            return set()
        live: set[str] = set()
        for group_id, tasks in self._runner_tasks_by_group.items():
            if any(not task.done() for task in tasks):
                live.add(str(group_id))
        return live

    async def cancel_task(self, task_id: str) -> bool:
        if self._task_registry is None:
            return False
        return await self._task_registry.cancel_task(task_id)

    async def cancel_group(self, group_id: str) -> int:
        if self._task_registry is None:
            return 0
        group = self._task_registry.get_group(group_id)
        cancellable_tasks = [
            task
            for task in (group.tasks if group is not None else self._task_registry.list_by_group(group_id))
            if not task.is_terminal()
        ]
        count = await self._task_registry.cancel_group(group_id)
        policy_store = self._dispatch_policy_store
        for task in cancellable_tasks:
            self._transition_dispatch_task_run(
                policy_store,
                task,
                MiniAgentRunStatus.CANCELLED,
                result_summary="Cancelled by user.",
            )
        if self._runner_tasks_by_group is not None:
            for runner_task in list(self._runner_tasks_by_group.get(group_id, ())):
                if not runner_task.done():
                    runner_task.cancel()
        if self._supervisor_tasks is not None:
            for supervisor_task in list(self._supervisor_tasks):
                if not supervisor_task.done() and supervisor_task.get_name() == f"supervise-{group_id}":
                    supervisor_task.cancel()
        if self._context_bus is not None:
            self._context_bus.clear_group(group_id)
        self._checkpoint_dispatch_state()
        if self._progress_queue is not None:
            from nullion.mini_agent_runner import ProgressUpdate

            for task in cancellable_tasks[:1]:
                await self._progress_queue.put(
                    ProgressUpdate(
                        agent_id=task.agent_id or "supervisor",
                        task_id=task.task_id,
                        group_id=task.group_id,
                        kind="task_cancelled",
                        message="Cancelled by user.",
                    )
                )
        return count

    async def cancel_conversation(self, conversation_id: str) -> int:
        if self._task_registry is None:
            return 0
        group_ids = {
            task.group_id
            for task in self._task_registry.list_by_conversation(conversation_id)
            if not task.is_terminal()
        }
        cancelled = 0
        for group_id in sorted(group_ids):
            cancelled += await self.cancel_group(group_id)
        return cancelled

    def cancel_conversation_sync(self, conversation_id: str, *, timeout_s: float = 3.0) -> int:
        loop = self._dispatcher_loop
        if loop is not None and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self.cancel_conversation(conversation_id), loop)
            return int(future.result(timeout=timeout_s) or 0)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return int(asyncio.run(self.cancel_conversation(conversation_id)) or 0)
        return 0

    async def shutdown_dispatcher(self) -> None:
        if self._runner_tasks_by_group:
            runner_tasks = [
                task
                for tasks in self._runner_tasks_by_group.values()
                for task in tasks
                if not task.done()
            ]
            for task in runner_tasks:
                task.cancel()
            if runner_tasks:
                await asyncio.gather(*runner_tasks, return_exceptions=True)
            self._runner_tasks_by_group.clear()
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


def _delegated_pause_store(policy_store: Any, approval_store: Any) -> Any | None:
    for store in (policy_store, approval_store):
        if store is not None and hasattr(store, "add_suspended_turn"):
            return store
    return None


def _store_delegated_pause_suspended_turn(
    policy_store: Any,
    approval_store: Any,
    *,
    task: Any,
    result: Any,
    agent_id: str | None,
) -> None:
    if getattr(result, "status", None) != "partial":
        return
    resume_token = getattr(result, "resume_token", None)
    if not isinstance(resume_token, dict) or not resume_token:
        return
    store = _delegated_pause_store(policy_store, approval_store)
    if store is None:
        return
    approval_id = resume_token.get("approval_id")
    pause_id = str(approval_id or f"task:{task.task_id}")
    try:
        store.add_suspended_turn(
            SuspendedTurn(
                approval_id=pause_id,
                conversation_id=str(task.conversation_id),
                chat_id=_messaging_target_from_conversation_id(str(task.conversation_id)),
                message=str(task.description),
                request_id=None,
                message_id=None,
                created_at=datetime.now(UTC),
                task_id=str(task.task_id),
                group_id=str(task.group_id),
                agent_id=str(agent_id or task.agent_id or ""),
                resume_token=dict(resume_token),
            )
        )
    except Exception:
        logger.debug("Could not persist delegated task pause for %s", task.task_id, exc_info=True)


def _group_all_quiescent(group: Any) -> bool:
    from nullion.task_queue import TaskStatus

    return all(
        task.is_terminal() or task.status is TaskStatus.WAITING_INPUT
        for task in getattr(group, "tasks", ()) or ()
    )


def _failed_dependency_ids(task: Any, failed_deps: set[str], tasks_by_id: dict[str, Any]) -> list[str]:
    """Return failed dependency roots, including failures hidden behind blocked intermediates."""
    seen: set[str] = set()
    failures: list[str] = []
    stack = [str(dep_id) for dep_id in getattr(task, "dependencies", ()) or () if str(dep_id)]
    while stack:
        dep_id = stack.pop(0)
        if dep_id in seen:
            continue
        seen.add(dep_id)
        if dep_id in failed_deps:
            failures.append(dep_id)
            continue
        parent = tasks_by_id.get(dep_id)
        if parent is not None:
            stack.extend(str(parent_dep) for parent_dep in getattr(parent, "dependencies", ()) or () if str(parent_dep))
    return failures


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


def _task_status_for_task_result(result: Any) -> TaskStatus:
    from nullion.task_queue import TaskStatus

    if getattr(result, "status", None) == "cancelled":
        return TaskStatus.CANCELLED
    if getattr(result, "status", None) == "success":
        return TaskStatus.COMPLETE
    if getattr(result, "status", None) == "partial":
        return TaskStatus.WAITING_INPUT
    return TaskStatus.FAILED


def _mini_agent_run_status_for_task_result(result: Any) -> MiniAgentRunStatus:
    if getattr(result, "status", None) == "cancelled":
        return MiniAgentRunStatus.CANCELLED
    if getattr(result, "status", None) == "success":
        return MiniAgentRunStatus.COMPLETED
    if getattr(result, "status", None) == "partial":
        return MiniAgentRunStatus.WAITING_INPUT
    return MiniAgentRunStatus.FAILED


def _progress_kind_for_task_result(result: Any) -> str:
    if getattr(result, "status", None) == "cancelled":
        return "task_cancelled"
    if getattr(result, "status", None) == "success":
        return "task_complete"
    if getattr(result, "status", None) == "partial":
        output = str(getattr(result, "output", "") or "")
        return "input_needed" if output.startswith("Waiting for user input:") else "approval_needed"
    return "task_failed"


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
    status_delivered: bool = False


__all__ = ["AgentOrchestrator", "DispatchResult", "MissionResult", "TurnResult"]
