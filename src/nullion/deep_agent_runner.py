"""Deep Agents backend for scoped Nullion mini-agent tasks."""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
import csv
from dataclasses import replace
import inspect
import json
import logging
import os
from pathlib import Path
import re
import shutil
from typing import Any, Iterable
from urllib.parse import urlparse
from uuid import uuid4
import zipfile
import xml.etree.ElementTree as ET

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_path_for_generated_workspace_file,
    artifact_root_for_principal,
    media_candidate_paths_from_text,
)
from nullion.deep_agent_profiles import (
    deep_agent_skill_files_for_task,
    deep_agent_subagents_for_task,
)
from nullion.langchain_adapters import nullion_client_as_langchain_chat_model, nullion_tools_as_langchain_tools
from nullion.response_sanitizer import is_safe_raw_tool_payload_replacement_reply, sanitize_user_visible_reply
from nullion.response_fulfillment_contract import (
    artifact_media_embedding_obligation_outstanding,
    artifact_paths_from_tool_results,
)
from nullion.run_activity import format_tool_activity_line, should_suppress_tool_activity
from nullion.tools import ToolInvocation
from nullion.task_queue import TaskResult
from nullion.workspace_storage import workspace_storage_roots_for_principal

logger = logging.getLogger(__name__)


class DeepAgentUserInputRequested(RuntimeError):
    def __init__(self, question: str, options: list[str]) -> None:
        super().__init__(question)
        self.question = question
        self.options = options


class DeepAgentStalledLoopError(RuntimeError):
    """Raised when Deep Agent tool execution loops without progress."""


class DeepAgentInnerTimeoutError(RuntimeError):
    """Raised when an operation inside a mini-agent times out before the outer budget."""


class DeepAgentEvidenceFallbackUnavailable(RuntimeError):
    """Raised when tool evidence is insufficient to recover a final answer."""


async def _preserve_inner_timeout(awaitable: Any) -> Any:
    try:
        return await awaitable
    except asyncio.TimeoutError as exc:
        message = str(exc).strip() or "A mini-agent operation timed out before the outer task budget was reached."
        raise DeepAgentInnerTimeoutError(message) from exc


_DEEP_AGENT_EVIDENCE_FALLBACK_TIMEOUT_SECONDS_ENV = "NULLION_DEEP_AGENT_EVIDENCE_FALLBACK_TIMEOUT_SECONDS"
_DEFAULT_DEEP_AGENT_EVIDENCE_FALLBACK_TIMEOUT_SECONDS = 20.0
_DEEP_AGENT_COMPLETION_DECISION_TIMEOUT_SECONDS_ENV = "NULLION_DEEP_AGENT_COMPLETION_DECISION_TIMEOUT_SECONDS"
_DEFAULT_DEEP_AGENT_COMPLETION_DECISION_TIMEOUT_SECONDS = 15.0
_HTML_DYNAMIC_CONTENT_ASSIGNMENT_RE = re.compile(
    r"""
    (?:document\s*\.\s*getElementById\(\s*["'](?P<id>[^"']+)["']\s*\)
      |document\s*\.\s*querySelector\(\s*["']\#(?P<selector_id>[^"']+)["']\s*\))
    \s*\.\s*(?:innerHTML|outerHTML)\s*=\s*(?P<rhs>[^;]{0,1200})
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)
_HTML_ID_ELEMENT_RE = re.compile(
    r"""<(?P<tag>[A-Za-z][\w:-]*)\b(?P<attrs>[^>]*)\bid\s*=\s*["'](?P<id>[^"']+)["'][^>]*>(?P<body>.*?)</(?P=tag)>""",
    re.IGNORECASE | re.DOTALL,
)
_HTML_SCRIPT_OR_STYLE_RE = re.compile(r"<(?:script|style)\b[^>]*>.*?</(?:script|style)>", re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_DYNAMIC_PRIMARY_MARKERS = ("map(", "join(", "<article", "<li", "<tr", "<section", "<div", "<table")
_STRUCTURED_HANDOFF_MAX_FILE_BYTES = 25 * 1024 * 1024
_STRUCTURED_HANDOFF_SCHEMA = "nullion.context_handoff.structured_records.v1"
_LEGACY_STRUCTURED_HANDOFF_SCHEMAS = frozenset(
    {
        _STRUCTURED_HANDOFF_SCHEMA,
        "nullion.context_handoff.browser_items.v1",
    }
)
_URL_IN_TEXT_RE = re.compile(r"https?://[^\s<>)\\\]]+")
_MARKDOWN_TABLE_SEPARATOR_RE = re.compile(r"^:?-{3,}:?$")
_BROWSER_ITEM_IMAGE_COLLECT_CHUNK_SIZE = 8


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
        tool_results: list[Any] = []
        try:
            config = _config_with_resolved_context_in(config, context_bus)
            artifact_preflight = await _artifact_task_fallback_from_tool_evidence(
                config,
                tool_registry=tool_registry,
                tool_results=tool_results,
                progress_queue=progress_queue,
            )
            if artifact_preflight is not None:
                result = artifact_preflight
            else:
                run_coro = self._run_inner(
                    config,
                    anthropic_client=anthropic_client,
                    tool_registry=tool_registry,
                    policy_store=policy_store,
                    context_bus=context_bus,
                    progress_queue=progress_queue,
                    tool_results=tool_results,
                )
                guarded_run_coro = _preserve_inner_timeout(run_coro)
                if _is_scheduled_background_config(config):
                    result = await guarded_run_coro
                else:
                    result = await asyncio.wait_for(guarded_run_coro, timeout=config.timeout_s)
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
        except DeepAgentInnerTimeoutError as exc:
            logger.warning("DeepAgent mini-agent %s inner operation timed out: %s", config.agent_id, exc)
            artifact_fallback = await _artifact_task_fallback_from_tool_evidence(
                config,
                tool_registry=tool_registry,
                tool_results=tool_results,
                progress_queue=progress_queue,
            )
            if artifact_fallback is not None:
                result = artifact_fallback
            else:
                context_fallback = await _context_task_fallback_from_tool_evidence(
                    config,
                    tool_registry=tool_registry,
                    tool_results=tool_results,
                    progress_queue=progress_queue,
                )
                if context_fallback is not None:
                    result = context_fallback
                else:
                    result = TaskResult(
                        task_id=task.task_id,
                        status="failure",
                        error=str(exc) or "A mini-agent operation timed out before the outer task budget was reached.",
                    )
        except asyncio.TimeoutError:
            logger.warning("DeepAgent mini-agent %s timed out after %.0fs", config.agent_id, config.timeout_s)
            artifact_fallback = await _artifact_task_fallback_from_tool_evidence(
                config,
                tool_registry=tool_registry,
                tool_results=tool_results,
                progress_queue=progress_queue,
            )
            if artifact_fallback is not None:
                result = artifact_fallback
            else:
                context_fallback = await _context_task_fallback_from_tool_evidence(
                    config,
                    tool_registry=tool_registry,
                    tool_results=tool_results,
                    progress_queue=progress_queue,
                )
                if context_fallback is not None:
                    result = context_fallback
                else:
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

        contract_failure = _artifact_contract_failure_for_task_result(task, result)
        if contract_failure is not None:
            result = TaskResult(
                task_id=task.task_id,
                status="failure",
                error=contract_failure,
                context_out=getattr(result, "context_out", None),
            )

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
        tool_results: list[Any] | None = None,
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

        system_prompt = _system_prompt_for_task(config, context_in=context_in, tool_registry=tool_registry)
        if tool_results is None:
            tool_results = []

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
            skills=_deep_agent_external_skills_for_task(config),
            subagents=_deep_agent_subagents_for_task(config),
        )
        payload = {"messages": [{"role": "user", "content": task.description}]}
        skill_files = _deep_agent_skill_files_for_task(config)
        if skill_files:
            payload["files"] = skill_files
        try:
            response = await _invoke_agent_with_heartbeat(
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
            artifact_fallback = await _artifact_task_fallback_from_tool_evidence(
                config,
                tool_registry=tool_registry,
                tool_results=tool_results,
                progress_queue=progress_queue,
            )
            if artifact_fallback is not None:
                return artifact_fallback
            context_fallback = await _context_task_fallback_from_tool_evidence(
                config,
                tool_registry=tool_registry,
                tool_results=tool_results,
                progress_queue=progress_queue,
            )
            if context_fallback is not None:
                return context_fallback
            fallback_text = await _fallback_answer_from_tool_evidence(
                config,
                tool_results=tool_results,
                model_client=anthropic_client,
            )
            response = {"content": [{"type": "text", "text": fallback_text}]}
        output_text = _extract_response_text(response)
        if not output_text:
            try:
                output_text = await _fallback_answer_from_tool_evidence(
                    config,
                    tool_results=tool_results,
                    model_client=anthropic_client,
                )
            except DeepAgentEvidenceFallbackUnavailable:
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
        if is_safe_raw_tool_payload_replacement_reply(reply=output_text, tool_results=tool_results):
            try:
                output_text = await _fallback_answer_from_tool_evidence(
                    config,
                    tool_results=tool_results,
                    model_client=anthropic_client,
                )
            except DeepAgentEvidenceFallbackUnavailable as exc:
                return TaskResult(
                    task_id=task.task_id,
                    status="failure",
                    error=str(exc) or output_text,
                    context_out=output_text,
                )
        await _persist_context_handoff_from_tool_evidence(
            config,
            tool_registry=tool_registry,
            tool_results=tool_results,
            progress_queue=progress_queue,
        )
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
        artifact_failure = (
            _artifact_delivery_failure_for_task(
                config,
                artifacts,
                deliverable_artifacts,
                tool_results=tool_results,
                context_in=context_in,
            )
            if _task_requires_user_file_delivery(task)
            else None
        )
        if artifact_failure:
            repaired_artifact = await _retry_spreadsheet_after_remote_image_failure(
                config,
                tool_registry=tool_registry,
                tool_results=tool_results,
                progress_queue=progress_queue,
            )
            if repaired_artifact is not None:
                return repaired_artifact
            artifact_fallback = await _artifact_task_fallback_from_tool_evidence(
                config,
                tool_registry=tool_registry,
                tool_results=tool_results,
                progress_queue=progress_queue,
            )
            if artifact_fallback is not None:
                return artifact_fallback
            return TaskResult(
                task_id=task.task_id,
                status="failure",
                error=artifact_failure,
                context_out=output_text,
            )
        if deliverable_artifacts and _html_static_primary_content_required(task):
            static_delivery_failure = _html_static_delivery_failure_for_paths(deliverable_artifacts)
            if static_delivery_failure is not None:
                return TaskResult(
                    task_id=task.task_id,
                    status="failure",
                    error=static_delivery_failure,
                    context_out=output_text,
                )
        artifact_verification_failure = _artifact_verification_failure_for_task(
            config,
            context_in=context_in,
            output_text=output_text,
            tool_results=tool_results,
            deliverable_artifacts=deliverable_artifacts,
        )
        if artifact_verification_failure is not None:
            return TaskResult(
                task_id=task.task_id,
                status="failure",
                error=artifact_verification_failure,
                context_out=output_text,
            )
        verifier_success_output = _artifact_verification_success_output_for_task(
            config,
            context_in=context_in,
        )
        if verifier_success_output:
            output_text = verifier_success_output
        context_out = _context_out_for_task(
            config,
            output_text=output_text,
            deliverable_artifacts=deliverable_artifacts,
            tool_results=tool_results,
            context_in=context_in,
        )
        completion_decision = await _scheduled_task_completion_decision(
            config,
            output_text=output_text,
            tool_results=tool_results,
            model_client=anthropic_client,
        )
        if completion_decision is not None and completion_decision.get("status") != "success":
            reason = str(
                completion_decision.get("answer")
                or "The scheduled task could not verify enough evidence to complete this step."
            ).strip()
            if _should_emit_best_effort_scheduled_result(config, tool_results):
                # Product invariant: scheduled runs should deliver best-effort
                # output whenever verified tool work exists.
                try:
                    best_effort_answer = await _fallback_answer_from_tool_evidence(
                        config,
                        tool_results=tool_results,
                        model_client=anthropic_client,
                    )
                except DeepAgentEvidenceFallbackUnavailable:
                    best_effort_answer = reason
                best_effort_answer = str(best_effort_answer or reason).strip() or reason
                return TaskResult(
                    task_id=task.task_id,
                    status="success",
                    output=best_effort_answer,
                    context_out=best_effort_answer,
                )
            return TaskResult(
                task_id=task.task_id,
                status="failure",
                error=reason,
            )
        if completion_decision is not None:
            answer = str(completion_decision.get("answer") or "").strip()
            if answer:
                output_text = answer
        return TaskResult(
            task_id=task.task_id,
            status="success",
            output=output_text,
            artifacts=deliverable_artifacts,
            context_out=context_out,
            )


def _config_with_resolved_context_in(config: Any, context_bus: Any) -> Any:
    if getattr(config, "context_in", None) is not None:
        return config
    task = getattr(config, "task", None)
    context_key = str(getattr(task, "context_key_in", "") or "").strip()
    if not context_key or context_bus is None:
        return config
    try:
        context_in = context_bus.get(context_key, group_id=getattr(task, "group_id", None))
    except Exception:
        logger.debug("Could not resolve mini-agent context key %s", context_key, exc_info=True)
        return config
    if context_in is None:
        return config
    try:
        return replace(config, context_in=context_in)
    except Exception:
        logger.debug("Could not attach resolved mini-agent context key %s", context_key, exc_info=True)
        return config


def _is_scheduled_background_config(config: Any) -> bool:
    task = getattr(config, "task", None)
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    return bool(metadata.get("scheduled_task_run"))


def _completed_tool_results(tool_results: list[Any]) -> list[Any]:
    return [
        result
        for result in (tool_results or [])
        if str(getattr(result, "status", "") or "").lower() in {"completed", "success", "succeeded"}
    ]


def _should_emit_best_effort_scheduled_result(config: Any, tool_results: list[Any]) -> bool:
    if not _is_scheduled_background_config(config):
        return False
    task = getattr(config, "task", None)
    if getattr(task, "context_key_out", None):
        # Context-producing steps are dependency contracts. If validation says
        # evidence is insufficient, do not publish best-effort text as verified
        # context for downstream report/artifact steps.
        return False
    return bool(_completed_tool_results(tool_results))


async def _persist_context_handoff_from_tool_evidence(
    config: Any,
    *,
    tool_registry: Any,
    tool_results: list[Any],
    progress_queue: asyncio.Queue,
) -> None:
    task = getattr(config, "task", None)
    if not getattr(task, "context_key_out", None):
        return
    if "file_write" not in _task_allowed_tool_names(task):
        return
    if _tool_results_include_durable_data_handoff(tool_results):
        return
    records = _browser_item_handoff_records_from_tool_results(tool_results)
    if not records:
        return
    path = _context_handoff_path_for_task(task)
    payload = {
        "schema": _STRUCTURED_HANDOFF_SCHEMA,
        "task_id": getattr(task, "task_id", None),
        "group_id": getattr(task, "group_id", None),
        "source": "browser_tool_results",
        "record_count": len(records),
        "records": records,
    }
    await _emit_progress(
        progress_queue,
        config=config,
        kind="progress_note",
        message="Saving structured handoff data for dependent task.",
        data={"phase": "context_handoff", "record_count": len(records)},
    )
    result = await _invoke_tool_registry(
        tool_registry,
        ToolInvocation(
            str(uuid4()),
            "file_write",
            task.principal_id,
            {
                "path": str(path),
                "content": json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            },
            capsule_id=task.task_id,
        ),
    )
    tool_results.append(result)
    _emit_tool_progress(progress_queue, config=config, result=result)
    if str(getattr(result, "status", "") or "").strip().lower() not in {"completed", "success", "succeeded"}:
        logger.warning(
            "DeepAgent context handoff file_write failed agent_id=%s task_id=%s error=%s",
            getattr(config, "agent_id", ""),
            str(getattr(task, "task_id", "") or ""),
            str(getattr(result, "error", "") or ""),
        )


def _tool_results_include_durable_data_handoff(tool_results: list[Any]) -> bool:
    for item in _context_workspace_files_from_tool_results(tool_results):
        if _workspace_file_has_parseable_handoff_records(item.get("path")):
            return True
    return False


def _workspace_file_has_parseable_handoff_records(value: Any) -> bool:
    path = _existing_file_path(value)
    if path is None:
        return False

    def add_from_value(candidate: Any) -> bool:
        for item in _browser_item_candidates_from_value(candidate):
            if _normalized_browser_item_record(item) is not None:
                return True
        return False

    return bool(_append_browser_item_records_from_file(path, add_from_value=add_from_value))


def _browser_item_handoff_records_from_tool_results(
    tool_results: list[Any],
    *,
    limit: int = 500,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for result in tool_results or []:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status not in {"completed", "success", "succeeded"}:
            continue
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        if tool_name not in {
            "browser_extract_detail",
            "browser_extract_items",
            "browser_extract_text",
            "browser_run_js",
        }:
            continue
        for item in _browser_item_candidates_from_value(getattr(result, "output", None)):
            normalized = _normalized_browser_item_record(item)
            if normalized is None:
                continue
            signature = _browser_item_record_signature(normalized)
            if signature in seen:
                continue
            seen.add(signature)
            record = {"index": len(records) + 1, **normalized}
            record["source_fields"] = item
            record["source_tool"] = tool_name
            records.append(record)
            if len(records) >= limit:
                return records
    return records


def _context_handoff_path_for_task(task: Any) -> Path:
    roots = workspace_storage_roots_for_principal(getattr(task, "principal_id", None))
    stem = _artifact_fallback_file_stem(task) or "context_handoff"
    return roots.files / f"{stem}_context_handoff_{uuid4().hex[:12]}.json"


async def _artifact_task_fallback_from_tool_evidence(
    config: Any,
    *,
    tool_registry: Any,
    tool_results: list[Any],
    progress_queue: asyncio.Queue,
) -> TaskResult | None:
    if not _task_requires_user_file_delivery(config.task):
        return None
    if not _task_accepts_spreadsheet_artifact(config.task):
        return None
    if "spreadsheet_create" not in _task_allowed_tool_names(config.task):
        return None
    item_records = _browser_item_records_from_tool_results(tool_results)
    if not item_records:
        item_records = _browser_item_records_from_context(getattr(config, "context_in", None))
    if not item_records:
        return None
    required_image_urls = _browser_item_media_urls(item_records)
    image_paths_by_url: dict[str, str] = {}
    if required_image_urls and "browser_image_collect" not in _task_allowed_tool_names(config.task):
        return _artifact_fallback_failure_result(
            config,
            "Collected records include media URLs, but no scoped media collection tool is available for this artifact task.",
            tool_results=tool_results,
            context_in=getattr(config, "context_in", None),
        )
    if "browser_image_collect" in _task_allowed_tool_names(config.task):
        image_paths_by_url = await _collect_browser_item_images(
            config,
            tool_registry=tool_registry,
            tool_results=tool_results,
            item_records=item_records,
            progress_queue=progress_queue,
        )
    missing_image_urls = [url for url in required_image_urls if not image_paths_by_url.get(url)]
    collected_image_urls = [url for url in required_image_urls if image_paths_by_url.get(url)]
    if missing_image_urls and not collected_image_urls:
        return _artifact_fallback_failure_result(
            config,
            (
                "Could not materialize local media artifacts for "
                f"{len(missing_image_urls)} of {len(required_image_urls)} structured media URL(s)."
            ),
            tool_results=tool_results,
            context_in=getattr(config, "context_in", None),
        )
    if missing_image_urls:
        await _emit_progress(
            progress_queue,
            config=config,
            kind="progress_note",
            message=(
                "Some structured media could not be collected; continuing with the "
                "available local media artifacts."
            ),
            data={
                "phase": "artifact_evidence_fallback",
                "collected_media_count": len(collected_image_urls),
                "missing_media_count": len(missing_image_urls),
            },
        )
    rows = _spreadsheet_rows_from_browser_item_records(item_records, image_paths_by_url=image_paths_by_url)
    if not rows:
        return None
    columns = _spreadsheet_columns_from_browser_item_rows(rows)
    output_path = artifact_path_for_generated_workspace_file(
        principal_id=config.task.principal_id,
        suffix=".xlsx",
        stem=_artifact_fallback_file_stem(config.task),
    )
    await _emit_progress(
        progress_queue,
        config=config,
        kind="progress_note",
        message="Creating workbook from verified collected data.",
        data={"phase": "artifact_evidence_fallback"},
    )
    result = await _invoke_tool_registry(
        tool_registry,
        ToolInvocation(
            str(uuid4()),
            "spreadsheet_create",
            config.task.principal_id,
                {
                    "title": str(getattr(config.task, "title", "") or "Collected items"),
                    "sheet_name": "Items",
                    "columns": columns,
                    "rows": rows,
                    "expected_rows": len(rows),
                    "output_path": str(output_path),
                },
                capsule_id=config.task.task_id,
            ),
    )
    tool_results.append(result)
    _emit_tool_progress(progress_queue, config=config, result=result)
    if str(getattr(result, "status", "") or "").lower() not in {"completed", "success", "succeeded"}:
        return None
    artifacts = artifact_paths_from_tool_results([result])
    artifacts = _relocate_external_artifact_paths_for_task(config, artifacts)
    deliverable_artifacts = _deliverable_artifact_paths_for_task(config, artifacts)
    if not deliverable_artifacts:
        return None
    media_failure = _artifact_fallback_media_delivery_failure(deliverable_artifacts, collected_image_urls)
    if media_failure is not None:
        return _artifact_fallback_failure_result(
            config,
            media_failure,
            tool_results=tool_results,
            context_in=getattr(config, "context_in", None),
        )
    content_obligations = {
        "item_count": len(rows),
        "spreadsheet_row_floor": len(rows),
        "spreadsheet_row_contract": True,
        "image_count": len(collected_image_urls),
        "requires_media": bool(required_image_urls and collected_image_urls),
    }
    if _artifact_content_delivery_failure_for_obligations(deliverable_artifacts, content_obligations) is not None:
        return None
    output_text = _artifact_delivery_success_output_text(deliverable_artifacts)
    context_out = _context_out_for_task(
        config,
        output_text=output_text,
        deliverable_artifacts=deliverable_artifacts,
        tool_results=tool_results,
        context_in=getattr(config, "context_in", None),
    )
    return TaskResult(
        task_id=config.task.task_id,
        status="success",
        output=output_text,
        artifacts=deliverable_artifacts,
        context_out=context_out,
    )


def _artifact_fallback_failure_result(
    config: Any,
    error: str,
    *,
    tool_results: list[Any],
    context_in: Any,
) -> TaskResult:
    context_out = _context_out_for_task(
        config,
        output_text=error,
        deliverable_artifacts=[],
        tool_results=tool_results,
        context_in=context_in,
    )
    return TaskResult(
        task_id=config.task.task_id,
        status="failure",
        error=error,
        context_out=context_out,
    )


async def _retry_spreadsheet_after_remote_image_failure(
    config: Any,
    *,
    tool_registry: Any,
    tool_results: list[Any],
    progress_queue: asyncio.Queue,
) -> TaskResult | None:
    if not _task_requires_user_file_delivery(config.task):
        return None
    if not _task_accepts_spreadsheet_artifact(config.task):
        return None
    if not {"spreadsheet_create", "browser_image_collect"}.issubset(_task_allowed_tool_names(config.task)):
        return None
    failed_result = _latest_remote_image_spreadsheet_failure(tool_results)
    if failed_result is None:
        return None
    paths_by_url = await _collect_remote_spreadsheet_images(
        config,
        failed_result=failed_result,
        tool_registry=tool_registry,
        tool_results=tool_results,
        progress_queue=progress_queue,
    )
    retry_arguments = _spreadsheet_retry_arguments_with_local_images(failed_result, paths_by_url)
    if retry_arguments is None:
        return None
    await _emit_progress(
        progress_queue,
        config=config,
        kind="progress_note",
        message="Retrying workbook with saved local image artifacts.",
        data={"phase": "remote_image_materialization"},
    )
    retry_result = await _invoke_tool_registry(
        tool_registry,
        ToolInvocation(
            str(uuid4()),
            "spreadsheet_create",
            config.task.principal_id,
            retry_arguments,
            capsule_id=config.task.task_id,
        ),
    )
    tool_results.append(retry_result)
    _emit_tool_progress(progress_queue, config=config, result=retry_result)
    if str(getattr(retry_result, "status", "") or "").lower() not in {"completed", "success", "succeeded"}:
        return None
    artifacts = artifact_paths_from_tool_results([retry_result])
    artifacts = _relocate_external_artifact_paths_for_task(config, artifacts)
    deliverable_artifacts = _deliverable_artifact_paths_for_task(config, artifacts)
    if not deliverable_artifacts:
        return None
    output_text = _artifact_delivery_success_output_text(deliverable_artifacts)
    context_out = _context_out_for_task(
        config,
        output_text=output_text,
        deliverable_artifacts=deliverable_artifacts,
        tool_results=tool_results,
        context_in=getattr(config, "context_in", None),
    )
    return TaskResult(
        task_id=config.task.task_id,
        status="success",
        output=output_text,
        artifacts=deliverable_artifacts,
        context_out=context_out,
    )


def _latest_remote_image_spreadsheet_failure(tool_results: list[Any]) -> Any | None:
    for result in reversed(tool_results or []):
        if str(getattr(result, "tool_name", "") or "").strip() != "spreadsheet_create":
            continue
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status in {"completed", "success", "succeeded"}:
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        if str(output.get("reason") or "").strip() == "remote_image_paths_not_supported":
            return result
        remote_urls = output.get("remote_image_urls")
        if isinstance(remote_urls, (list, tuple)) and any(str(url or "").strip() for url in remote_urls):
            return result
    return None


async def _collect_remote_spreadsheet_images(
    config: Any,
    *,
    failed_result: Any,
    tool_registry: Any,
    tool_results: list[Any],
    progress_queue: asyncio.Queue,
) -> dict[str, str]:
    urls = _remote_spreadsheet_image_urls(failed_result)
    if not urls:
        return {}
    paths_by_url: dict[str, str] = {}
    for chunk_index, start in enumerate(range(0, len(urls), 20), start=1):
        chunk = urls[start : start + 20]
        result = await _invoke_tool_registry(
            tool_registry,
            ToolInvocation(
                str(uuid4()),
                "browser_image_collect",
                config.task.principal_id,
                {
                    "image_urls": chunk,
                    "max_images": len(chunk),
                    "output_stem": f"{_artifact_fallback_file_stem(config.task)}_retry_image_{chunk_index}",
                    "quality_profile": "content",
                    "timeout_seconds": 30,
                },
                capsule_id=config.task.task_id,
            ),
        )
        tool_results.append(result)
        _emit_tool_progress(progress_queue, config=config, result=result)
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        for image in output.get("images") or ():
            if not isinstance(image, dict):
                continue
            local_path = str(image.get("local_path") or image.get("artifact_path") or "").strip()
            if not local_path:
                continue
            for key in ("source_url", "final_url", "url"):
                url = str(image.get(key) or "").strip()
                if url:
                    paths_by_url[url] = local_path
        image_paths = output.get("image_paths")
        if isinstance(image_paths, (list, tuple)):
            for url, local_path in zip(chunk, image_paths, strict=False):
                if isinstance(local_path, str) and local_path.strip():
                    paths_by_url.setdefault(str(url), local_path.strip())
    return paths_by_url


def _remote_spreadsheet_image_urls(failed_result: Any) -> list[str]:
    output = getattr(failed_result, "output", None)
    if not isinstance(output, dict):
        return []
    urls: list[str] = []
    remote_urls = output.get("remote_image_urls")
    if isinstance(remote_urls, (list, tuple)):
        urls.extend(str(url).strip() for url in remote_urls if isinstance(url, str) and url.strip())
    invocation_args = output.get("invocation_arguments")
    if isinstance(invocation_args, dict):
        urls.extend(_remote_image_urls_from_spreadsheet_arguments(invocation_args))
    return list(dict.fromkeys(url for url in urls if url.lower().startswith(("http://", "https://"))))


def _spreadsheet_retry_arguments_with_local_images(failed_result: Any, paths_by_url: dict[str, str]) -> dict[str, Any] | None:
    if not paths_by_url:
        return None
    output = getattr(failed_result, "output", None)
    output = output if isinstance(output, dict) else {}
    source_args = output.get("invocation_arguments")
    if not isinstance(source_args, dict):
        return None
    arguments = json.loads(json.dumps(source_args, default=str))
    changed = _replace_remote_image_urls_in_spreadsheet_arguments(arguments, paths_by_url)
    return arguments if changed else None


def _remote_image_urls_from_spreadsheet_arguments(arguments: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    raw_image_paths = arguments.get("image_paths")
    if isinstance(raw_image_paths, (list, tuple)):
        urls.extend(str(value).strip() for value in raw_image_paths if _is_remote_url_text(value))
    rows = arguments.get("rows")
    if isinstance(rows, (list, tuple)):
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in ("image_path", "image_paths", "image"):
                value = row.get(key)
                if _is_remote_url_text(value):
                    urls.append(str(value).strip())
                elif isinstance(value, (list, tuple)):
                    urls.extend(str(item).strip() for item in value if _is_remote_url_text(item))
    return urls


def _replace_remote_image_urls_in_spreadsheet_arguments(arguments: dict[str, Any], paths_by_url: dict[str, str]) -> bool:
    changed = False
    raw_image_paths = arguments.get("image_paths")
    if isinstance(raw_image_paths, list):
        for index, value in enumerate(raw_image_paths):
            replacement = paths_by_url.get(str(value or "").strip())
            if replacement:
                raw_image_paths[index] = replacement
                changed = True
    rows = arguments.get("rows")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in ("image_path", "image"):
                replacement = paths_by_url.get(str(row.get(key) or "").strip())
                if replacement:
                    row[key] = replacement
                    changed = True
            value = row.get("image_paths")
            if isinstance(value, list):
                for index, item in enumerate(value):
                    replacement = paths_by_url.get(str(item or "").strip())
                    if replacement:
                        value[index] = replacement
                        changed = True
            else:
                replacement = paths_by_url.get(str(value or "").strip())
                if replacement:
                    row["image_paths"] = [replacement]
                    changed = True
    return changed


def _is_remote_url_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip().lower().startswith(("http://", "https://"))


async def _context_task_fallback_from_tool_evidence(
    config: Any,
    *,
    tool_registry: Any,
    tool_results: list[Any],
    progress_queue: asyncio.Queue,
) -> TaskResult | None:
    task = getattr(config, "task", None)
    if _task_requires_user_file_delivery(task):
        return None
    if not getattr(task, "context_key_out", None):
        return None
    if not _completed_tool_results(tool_results):
        return None
    await _persist_context_handoff_from_tool_evidence(
        config,
        tool_registry=tool_registry,
        tool_results=tool_results,
        progress_queue=progress_queue,
    )
    workspace_files = _context_workspace_files_from_tool_results(tool_results)
    if not workspace_files:
        return None
    output_text = _generic_tool_evidence_answer(config, _completed_tool_results(tool_results))
    context_out = _context_out_for_task(
        config,
        output_text=output_text,
        deliverable_artifacts=[],
        tool_results=tool_results,
        context_in=getattr(config, "context_in", None),
    )
    return TaskResult(
        task_id=task.task_id,
        status="success",
        output=output_text,
        context_out=context_out,
    )


def _task_accepts_spreadsheet_artifact(task: Any) -> bool:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    kind = str(metadata.get("required_artifact_kind") or "").strip().lower().removeprefix(".")
    output = getattr(task, "output", None)
    output_kind = str(getattr(output, "artifact_kind", "") or "").strip().lower().removeprefix(".")
    return kind in {"xlsx", "xls", "csv", "tsv", "spreadsheet"} or output_kind in {
        "xlsx",
        "xls",
        "csv",
        "tsv",
        "spreadsheet",
    }


def _task_allowed_tool_names(task: Any) -> set[str]:
    names = {str(tool or "").strip() for tool in (getattr(task, "allowed_tools", ()) or ()) if str(tool or "").strip()}
    metadata = getattr(task, "metadata", None)
    if isinstance(metadata, dict):
        names.update(str(tool or "").strip() for tool in (metadata.get("allowed_tools") or ()) if str(tool or "").strip())
    return names


def _browser_item_records_from_tool_results(tool_results: list[Any], *, limit: int = 250) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for result in tool_results or []:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status not in {"completed", "success", "succeeded"}:
            continue
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        if tool_name not in {
            "browser_extract_detail",
            "browser_extract_items",
            "browser_extract_text",
            "browser_run_js",
        }:
            continue
        for item in _browser_item_candidates_from_value(getattr(result, "output", None)):
            record = _normalized_browser_item_record(item)
            if record is None:
                continue
            signature = _browser_item_record_signature(record)
            if signature in seen:
                continue
            seen.add(signature)
            records.append(record)
            if len(records) >= limit:
                return records
    return records


def _browser_item_records_from_context(context_in: Any, *, limit: int = 250) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    seen_urls: set[str] = set()

    def add_item(item: dict[str, Any]) -> bool:
        record = _normalized_browser_item_record(item)
        if record is None:
            return False
        url = str(record.get("url") or "").strip()
        if url and url in seen_urls:
            return False
        signature = _browser_item_record_signature(record)
        if signature in seen:
            return False
        seen.add(signature)
        if url:
            seen_urls.add(url)
        records.append(record)
        return len(records) >= limit

    def add_from_value(value: Any) -> bool:
        for item in _browser_item_candidates_from_value(value):
            if add_item(item):
                return True
        return False

    add_from_value(context_in)
    direct_records = list(records)

    file_record_sets: list[list[dict[str, Any]]] = []
    for item in _context_workspace_files_from_context(context_in):
        path = _existing_file_path(item.get("path"))
        if path is None:
            continue
        file_records = _browser_item_records_from_file(path, limit=limit)
        if file_records:
            file_record_sets.append(file_records)
    if file_record_sets:
        best_file_records = _select_best_browser_item_record_set(file_record_sets)
        if not direct_records or len(_exportable_browser_item_records(best_file_records)) > len(
            _exportable_browser_item_records(direct_records)
        ):
            records = best_file_records
        else:
            records = direct_records
    else:
        records = direct_records
    if records:
        _enrich_browser_item_records_from_context_workspace_files(records, context_in)
    return records


def _enrich_browser_item_records_from_context_workspace_files(
    records: list[dict[str, Any]],
    context_in: Any,
) -> None:
    if not records:
        return
    records_by_url = {
        str(record.get("url") or "").strip(): record
        for record in records
        if str(record.get("url") or "").strip()
    }
    if not records_by_url:
        return
    for source_record in _browser_item_records_from_context_workspace_files(context_in):
        url = str(source_record.get("url") or "").strip()
        target = records_by_url.get(url)
        if target is None:
            continue
        _merge_browser_item_record_fields(target, source_record)


def _browser_item_records_from_context_workspace_files(context_in: Any, *, limit: int = 500) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add_item(item: dict[str, Any]) -> bool:
        record = _normalized_browser_item_record(item)
        if record is None:
            return False
        signature = _browser_item_record_signature(record)
        if signature in seen:
            return False
        seen.add(signature)
        records.append(record)
        return len(records) >= limit

    def add_from_value(value: Any) -> bool:
        for item in _browser_item_candidates_from_value(value):
            if add_item(item):
                return True
        return False

    for item in _context_workspace_files_from_context(context_in):
        path = _existing_file_path(item.get("path"))
        if path is None:
            continue
        if _append_browser_item_records_from_file(path, add_from_value=add_from_value):
            break
    return records


def _browser_item_records_from_file(path: Path, *, limit: int = 250) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add_item(item: dict[str, Any]) -> bool:
        record = _normalized_browser_item_record(item)
        if record is None:
            return False
        signature = _browser_item_record_signature(record)
        if signature in seen:
            return False
        seen.add(signature)
        records.append(record)
        return len(records) >= limit

    def add_from_value(value: Any) -> bool:
        for item in _browser_item_candidates_from_value(value):
            if add_item(item):
                return True
        return False

    _append_browser_item_records_from_file(path, add_from_value=add_from_value)
    return records


def _select_best_browser_item_record_set(record_sets: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    if not record_sets:
        return []

    def score(records: list[dict[str, Any]]) -> tuple[int, int, int, int]:
        exportable = _exportable_browser_item_records(records)
        with_url = sum(1 for record in exportable if str(record.get("url") or "").strip())
        with_image = sum(1 for record in exportable if str(record.get("image_url") or "").strip())
        return (len(exportable), with_url, with_image, len(records))

    return max(record_sets, key=score)


def _merge_browser_item_record_fields(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key in ("image_url", "source_text"):
        if not str(target.get(key) or "").strip() and str(source.get(key) or "").strip():
            target[key] = source[key]
    target_extras = target.get("extra_fields")
    if not isinstance(target_extras, dict):
        target_extras = {}
        target["extra_fields"] = target_extras
    source_extras = source.get("extra_fields")
    if isinstance(source_extras, dict):
        for key, value in source_extras.items():
            if key not in target_extras and str(value or "").strip():
                target_extras[key] = value


def _append_browser_item_records_from_file(path: Path, *, add_from_value) -> bool:  # noqa: ANN001
    suffix = path.suffix.lower()
    try:
        size = path.stat().st_size
    except OSError:
        return False
    if suffix in {".json", ".txt"}:
        if size > _STRUCTURED_HANDOFF_MAX_FILE_BYTES:
            return False
        try:
            text = path.read_text(encoding="utf-8")
            if suffix == ".json":
                return bool(add_from_value(json.loads(text)))
            return bool(add_from_value(text))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            logger.debug("Could not read structured text handoff file %s", path, exc_info=True)
            return False
    if suffix not in {".csv", ".tsv"}:
        return False
    delimiter = "\t" if suffix == ".tsv" else ","
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter)
            for row in reader:
                if add_from_value(row):
                    return True
    except (OSError, UnicodeDecodeError, csv.Error):
        logger.debug("Could not read structured delimited handoff file %s", path, exc_info=True)
    return False


def _browser_item_candidates_from_value(value: Any, *, depth: int = 0) -> list[dict[str, Any]]:
    if depth > 5:
        return []
    candidates: list[dict[str, Any]] = []
    if isinstance(value, dict):
        lowered = {str(key).strip().lower(): item for key, item in value.items()}
        is_item_mapping = _looks_like_browser_item_mapping(lowered)
        if is_item_mapping:
            candidates.append(dict(value))
        for key in (
            "items",
            "record",
            "records",
            "rows",
            "data",
            "output",
            "result",
            "summary",
            "result_summary",
            "payload",
            "source_fields",
            "sourcefields",
            "fields",
            "text",
            "content",
            "body",
        ):
            if is_item_mapping and key in {"source_fields", "sourcefields", "fields", "text", "content", "body"}:
                continue
            nested = lowered.get(key)
            if nested is not None and nested is not value:
                candidates.extend(_browser_item_candidates_from_value(nested, depth=depth + 1))
    elif isinstance(value, (list, tuple)):
        for item in value:
            candidates.extend(_browser_item_candidates_from_value(item, depth=depth + 1))
    elif isinstance(value, str):
        parsed = _json_like_browser_value_from_text(value)
        if parsed is not None:
            candidates.extend(_browser_item_candidates_from_value(parsed, depth=depth + 1))
        else:
            candidates.extend(_markdown_table_records_from_text(value))
    return candidates


def _json_like_browser_value_from_text(value: str) -> Any | None:
    text = value.strip()
    if not text or len(text.encode("utf-8", errors="ignore")) > _STRUCTURED_HANDOFF_MAX_FILE_BYTES:
        return None
    if text.lower().startswith("pretty-print"):
        text = "\n".join(text.splitlines()[1:]).strip()
    starts = [index for index in (text.find("{"), text.find("[")) if index >= 0]
    if starts:
        text = text[min(starts) :].strip()
    if not text.startswith(("{", "[")):
        return None
    decoder = json.JSONDecoder()
    try:
        parsed, _end = decoder.raw_decode(text)
    except json.JSONDecodeError:
        return None
    return parsed


def _markdown_table_records_from_text(value: str, *, limit: int = 500) -> list[dict[str, Any]]:
    lines = [line.strip() for line in str(value or "").splitlines()]
    records: list[dict[str, Any]] = []
    index = 0
    while index + 2 < len(lines):
        header_line = lines[index]
        separator_line = lines[index + 1]
        if not _looks_like_markdown_table_separator(separator_line):
            index += 1
            continue
        headers = _markdown_table_cells(header_line)
        separators = _markdown_table_cells(separator_line)
        if not headers or len(headers) < 2 or len(separators) < len(headers):
            index += 1
            continue
        row_index = index + 2
        while row_index < len(lines):
            row_line = lines[row_index]
            if not row_line or "|" not in row_line:
                break
            cells = _markdown_table_cells(row_line)
            if not cells:
                break
            row: dict[str, Any] = {}
            for column_index, header in enumerate(headers):
                cell = cells[column_index] if column_index < len(cells) else ""
                if header:
                    row[header] = cell
            if row:
                records.append(row)
                if len(records) >= limit:
                    return records
            row_index += 1
        index = max(row_index, index + 1)
    return records


def _looks_like_markdown_table_separator(line: str) -> bool:
    cells = _markdown_table_cells(line)
    return len(cells) >= 2 and all(_MARKDOWN_TABLE_SEPARATOR_RE.match(cell.strip()) for cell in cells if cell.strip())


def _markdown_table_cells(line: str) -> list[str]:
    text = str(line or "").strip()
    if "|" not in text:
        return []
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    return [_clean_markdown_table_cell(cell) for cell in text.split("|")]


def _clean_markdown_table_cell(cell: str) -> str:
    text = str(cell or "").strip()
    link_match = re.fullmatch(r"\[([^\]]+)\]\((https?://[^)]+)\)", text)
    if link_match:
        return f"{link_match.group(1).strip()} {link_match.group(2).strip()}".strip()
    return " ".join(text.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ").split())


def _looks_like_browser_item_mapping(mapping: dict[str, Any]) -> bool:
    markers = {
        "title",
        "name",
        "url",
        "href",
        "link",
        "image",
        "image_url",
        "imageurl",
        "src",
        "thumbnail",
        "thumbnail_url",
        "text",
        "content",
        "body",
    }
    if len(set(mapping).intersection(markers)) >= 2:
        return True
    if _looks_like_record_collection_envelope(mapping):
        return False
    return _mapping_has_url_value(mapping) and _mapping_has_non_url_scalar_value(mapping)


def _looks_like_record_collection_envelope(mapping: dict[str, Any]) -> bool:
    container_keys = {
        "items",
        "records",
        "rows",
        "data",
        "output",
        "result",
        "payload",
    }
    for key, value in mapping.items():
        normalized_key = str(key or "").strip().lower()
        if normalized_key not in container_keys:
            continue
        if isinstance(value, (list, tuple, dict)):
            return True
    return False


def _mapping_has_url_value(mapping: dict[str, Any]) -> bool:
    return any(_direct_url_from_structured_field_value(value) for value in mapping.values())


def _mapping_has_non_url_scalar_value(mapping: dict[str, Any]) -> bool:
    for value in mapping.values():
        text = _direct_text_from_structured_field_value(value)
        if not text:
            continue
        if _URL_IN_TEXT_RE.fullmatch(text.strip()):
            continue
        return True
    return False


def _direct_url_from_structured_field_value(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip()
        if text.lower().startswith(("http://", "https://")):
            return text
        return ""
    if isinstance(value, dict):
        for nested_value in value.values():
            if isinstance(nested_value, str):
                text = nested_value.strip()
                if text.lower().startswith(("http://", "https://")):
                    return text
    return ""


def _direct_text_from_structured_field_value(value: Any) -> str:
    if isinstance(value, (str, int, float, bool)):
        return " ".join(str(value).split())[:1200]
    if isinstance(value, dict):
        for nested_value in value.values():
            if isinstance(nested_value, (str, int, float, bool)):
                text = " ".join(str(nested_value).split())[:1200]
                if text:
                    return text
    return ""


def _browser_item_record_signature(record: dict[str, Any]) -> tuple[str, str, str]:
    url = str(record.get("url") or "").strip()
    title_key = _normalized_record_signature_text(record.get("title"))
    if url:
        url_key = _normalized_url_signature_key(url)
        if title_key:
            tail_key = _normalized_url_tail_signature_key(url)
            if tail_key:
                return ("url-tail-title", tail_key, title_key)
        return ("url", url_key or url, "")
    return (
        "record",
        title_key,
        str(record.get("image_url") or "").strip(),
    )


def _normalized_record_signature_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def _normalized_url_signature_key(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return str(url or "").strip()
    path = re.sub(r"/+", "/", parsed.path or "/").rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{(parsed.netloc or '').lower()}{path}"


def _normalized_url_tail_signature_key(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if not parsed.netloc:
        return ""
    segments = [segment for segment in re.split(r"/+", parsed.path or "") if segment]
    if not segments:
        return ""
    tail = segments[-1].strip().casefold()
    if not tail:
        return ""
    return f"{(parsed.netloc or '').lower()}/{tail}"


def _first_url_from_structured_values(mappings: list[dict[str, Any]], *, image_only: bool) -> str:
    fallback = ""
    for mapping in mappings:
        for value in mapping.values():
            url = _first_url_from_value(value, image_only=image_only)
            if not url:
                continue
            if image_only or not _looks_like_image_url(url):
                return url
            if not fallback:
                fallback = url
    return fallback if image_only else ""


def _first_url_from_value(value: Any, *, image_only: bool) -> str:
    if isinstance(value, str):
        match = _URL_IN_TEXT_RE.search(value.strip())
        if not match:
            return ""
        url = match.group(0).rstrip(".,;:")
        if image_only and not _looks_like_image_url(url):
            return ""
        return url
    if isinstance(value, dict):
        for nested_value in value.values():
            url = _first_url_from_value(nested_value, image_only=image_only)
            if url:
                return url
    if isinstance(value, (list, tuple)):
        for item in value:
            url = _first_url_from_value(item, image_only=image_only)
            if url:
                return url
    return ""


def _first_non_url_text_from_structured_values(mappings: list[dict[str, Any]]) -> str:
    fallback = ""
    for mapping in mappings:
        for value in mapping.values():
            text = _extra_text_for_browser_item_field(value)
            if not text or _URL_IN_TEXT_RE.fullmatch(text.strip()):
                continue
            if not fallback:
                fallback = text
            if any(character.isalpha() for character in text):
                return text
    return fallback


def _normalized_browser_item_record(item: dict[str, Any]) -> dict[str, Any] | None:
    source_mappings = _structured_source_field_mappings(item)
    title = _first_text_field_from_mappings(
        [*source_mappings, item],
        ("title", "name", "label", "heading"),
    )
    url = _first_url_field_from_mappings(
        [item, *source_mappings],
        (
            "url",
            "href",
            "link",
            "canonical_url",
            "canonicalUrl",
        ),
    )
    if not url:
        url = _first_url_from_structured_values([item, *source_mappings], image_only=False)
    image_url = _first_url_field(
        item,
        ("image_url", "imageUrl", "image", "src", "thumbnail", "thumbnail_url", "thumbnailUrl", "featured_image"),
        image_only=True,
    )
    if not image_url:
        image_url = _first_url_field_from_mappings(
            source_mappings,
            ("image_url", "imageUrl", "image", "src", "thumbnail", "thumbnail_url", "thumbnailUrl", "featured_image"),
            image_only=True,
        )
    if not image_url:
        image_url = _first_url_from_structured_values([item, *source_mappings], image_only=True)
    source_text = _first_text_field_from_mappings(
        [item, *source_mappings],
        ("text", "content", "body", "summary", "aria_label", "ariaLabel"),
    )
    extra_fields = _extra_browser_item_fields(item, source_mappings=source_mappings)
    if not title:
        title = _first_non_url_text_from_structured_values([item, *source_mappings])
    if not any((title, url, image_url, source_text, extra_fields)):
        return None
    if not title:
        title = url or source_text or "Item"
    record = {
        "title": title,
        "url": url,
        "image_url": image_url,
        "source_text": source_text,
    }
    if extra_fields:
        record["extra_fields"] = extra_fields
    return record


def _structured_source_field_mappings(item: dict[str, Any]) -> list[dict[str, Any]]:
    mappings: list[dict[str, Any]] = []
    for key in ("source_fields", "sourceFields", "fields"):
        value = _case_insensitive_mapping_value(item, key)
        if isinstance(value, dict):
            mappings.append(dict(value))
    return mappings


def _first_text_field_from_mappings(mappings: list[dict[str, Any]], keys: tuple[str, ...]) -> str:
    for mapping in mappings:
        text = _first_text_field(mapping, keys)
        if text:
            return text
    return ""


def _first_url_field_from_mappings(
    mappings: list[dict[str, Any]],
    keys: tuple[str, ...],
    *,
    image_only: bool = False,
) -> str:
    for mapping in mappings:
        url = _first_url_field(mapping, keys, image_only=image_only)
        if url:
            return url
    return ""


_BROWSER_ITEM_EXPORT_FIELD_KEYS = {
    "index",
    "title",
    "name",
    "label",
    "heading",
    "url",
    "href",
    "link",
    "canonical_url",
    "canonicalurl",
    "image",
    "image_url",
    "imageurl",
    "src",
    "thumbnail",
    "thumbnail_url",
    "thumbnailurl",
    "featured_image",
    "body",
    "content",
    "text",
    "aria_label",
    "arialabel",
    "source_fields",
    "sourcefields",
    "fields",
    "source_tool",
    "links",
    "scripts",
    "html",
    "htmllen",
}


def _extra_browser_item_fields(item: dict[str, Any], *, source_mappings: list[dict[str, Any]]) -> dict[str, str]:
    extras: dict[str, str] = {}
    for mapping in [*source_mappings, item]:
        for raw_key, value in mapping.items():
            key = str(raw_key or "").strip()
            if not key or key.startswith("_"):
                continue
            normalized_key = re.sub(r"[^a-z0-9]+", "", key.lower())
            if normalized_key in _BROWSER_ITEM_EXPORT_FIELD_KEYS:
                continue
            text = _extra_text_for_browser_item_field(value)
            if not text:
                continue
            label = _spreadsheet_label_for_structured_field(key)
            if label and label not in extras:
                extras[label] = text
    return extras


def _extra_text_for_browser_item_field(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return " ".join(str(value).split())[:1200]
    if isinstance(value, dict):
        for key in ("text", "label", "value", "name", "title", "content"):
            text = _extra_text_for_browser_item_field(_case_insensitive_mapping_value(value, key))
            if text:
                return text
        return ""
    if isinstance(value, (list, tuple)):
        parts = [_extra_text_for_browser_item_field(item) for item in value[:4]]
        return " ".join(part for part in parts if part)[:1200]
    return ""


def _spreadsheet_label_for_structured_field(key: str) -> str:
    words = [word for word in re.split(r"[^A-Za-z0-9]+", key) if word]
    if not words:
        return ""
    return " ".join(word[:1].upper() + word[1:] for word in words)[:80]


def _first_text_field(mapping: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = _case_insensitive_mapping_value(mapping, key)
        text = _text_for_browser_item_field(value)
        if text:
            return text
    return ""


def _first_url_field(mapping: dict[str, Any], keys: tuple[str, ...], *, image_only: bool = False) -> str:
    for key in keys:
        value = _case_insensitive_mapping_value(mapping, key)
        url = _url_for_browser_item_field(value, image_only=image_only, structured_image_field=image_only)
        if url:
            return url
    return ""


def _case_insensitive_mapping_value(mapping: dict[str, Any], key: str) -> Any:
    if key in mapping:
        return mapping[key]
    key_lower = key.lower()
    for existing_key, value in mapping.items():
        if str(existing_key).strip().lower() == key_lower:
            return value
    return None


def _text_for_browser_item_field(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return " ".join(str(value).split())[:1200]
    if isinstance(value, dict):
        for key in ("text", "label", "value", "name", "title", "content"):
            text = _text_for_browser_item_field(_case_insensitive_mapping_value(value, key))
            if text:
                return text
    if isinstance(value, (list, tuple)):
        parts = [_text_for_browser_item_field(item) for item in value[:4]]
        return " ".join(part for part in parts if part)[:1200]
    return ""


def _url_for_browser_item_field(value: Any, *, image_only: bool, structured_image_field: bool = False) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if not text.lower().startswith(("http://", "https://")):
            return ""
        if image_only and not structured_image_field and not _looks_like_image_url(text):
            return ""
        return text
    if isinstance(value, dict):
        for key in ("url", "href", "src", "source_url", "final_url"):
            url = _url_for_browser_item_field(
                _case_insensitive_mapping_value(value, key),
                image_only=image_only,
                structured_image_field=True if image_only else structured_image_field,
            )
            if url:
                return url
    if isinstance(value, (list, tuple)):
        for item in value:
            url = _url_for_browser_item_field(
                item,
                image_only=image_only,
                structured_image_field=structured_image_field,
            )
            if url:
                return url
    return ""


def _looks_like_image_url(url: str) -> bool:
    lower = url.split("?", 1)[0].split("#", 1)[0].lower()
    return lower.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".avif")) or "/image" in lower or "/img" in lower


async def _collect_browser_item_images(
    config: Any,
    *,
    tool_registry: Any,
    tool_results: list[Any],
    item_records: list[dict[str, Any]],
    progress_queue: asyncio.Queue,
) -> dict[str, str]:
    image_urls = list(
        dict.fromkeys(
            str(record.get("image_url") or "").strip()
            for record in item_records
            if str(record.get("image_url") or "").strip()
        )
    )
    if not image_urls:
        return {}
    paths_by_url: dict[str, str] = {}
    for chunk_index, start in enumerate(range(0, len(image_urls), _BROWSER_ITEM_IMAGE_COLLECT_CHUNK_SIZE), start=1):
        chunk = image_urls[start : start + _BROWSER_ITEM_IMAGE_COLLECT_CHUNK_SIZE]
        result = await _invoke_tool_registry(
            tool_registry,
            ToolInvocation(
                str(uuid4()),
                "browser_image_collect",
                config.task.principal_id,
                {
                    "image_urls": chunk,
                    "max_images": len(chunk),
                    "output_stem": f"{_artifact_fallback_file_stem(config.task)}_image_{chunk_index}",
                    "quality_profile": "content",
                    "timeout_seconds": 30,
                },
                capsule_id=config.task.task_id,
            ),
        )
        tool_results.append(result)
        _emit_tool_progress(progress_queue, config=config, result=result)
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        for image in output.get("images") or ():
            if not isinstance(image, dict):
                continue
            local_path = str(image.get("local_path") or image.get("artifact_path") or "").strip()
            if not local_path:
                continue
            for key in ("source_url", "final_url"):
                url = str(image.get(key) or "").strip()
                if url:
                    paths_by_url[url] = local_path
        image_paths = [
            str(path or "").strip()
            for path in (output.get("image_paths") or output.get("artifact_paths") or ())
            if str(path or "").strip()
        ]
        for url, local_path in zip(chunk, image_paths, strict=False):
            paths_by_url.setdefault(url, local_path)
    return paths_by_url


def _browser_item_media_urls(item_records: list[dict[str, Any]]) -> list[str]:
    return list(
        dict.fromkeys(
            str(record.get("image_url") or "").strip()
            for record in _exportable_browser_item_records(item_records)
            if str(record.get("image_url") or "").strip()
        )
    )


def _artifact_fallback_media_delivery_failure(paths: list[str], required_image_urls: list[str]) -> str | None:
    if not required_image_urls:
        return None
    required_count = len(required_image_urls)
    for raw_path in paths:
        path = Path(str(raw_path or ""))
        if path.suffix.lower() not in {".xlsx", ".xlsm"}:
            continue
        summary = _xlsx_artifact_content_summary(path)
        if summary is None:
            return f"The spreadsheet artifact could not be inspected as a valid workbook: {path.name}."
        media_count = int(summary.get("nontrivial_media") or summary.get("media") or 0)
        if media_count < required_count:
            return (
                "The spreadsheet artifact is missing embedded media from structured source rows. "
                f"Expected {required_count} image item(s); found {media_count}."
            )
    return None


def _spreadsheet_rows_from_browser_item_records(
    item_records: list[dict[str, Any]],
    *,
    image_paths_by_url: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    export_records = _exportable_browser_item_records(item_records)
    for index, record in enumerate(export_records, start=1):
        title = str(record.get("title") or "").strip() or f"Item {index}"
        url = str(record.get("url") or "").strip()
        row: dict[str, Any] = {
            "Index": index,
            "Title": title,
            "URL": {"label": title, "url": url} if url else "",
            "Source Text": str(record.get("source_text") or "").strip(),
        }
        image_url = str(record.get("image_url") or "").strip()
        image_path = image_paths_by_url.get(image_url)
        if image_path:
            row["image_path"] = image_path
        extra_fields = record.get("extra_fields")
        if isinstance(extra_fields, dict):
            for key, value in extra_fields.items():
                label = _spreadsheet_label_for_structured_field(str(key))
                if label and label not in row:
                    row[label] = str(value or "").strip()
        rows.append(row)
    return rows


def _exportable_browser_item_records(item_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = [record for record in item_records if isinstance(record, dict)]
    rich_records = [record for record in records if _browser_item_record_has_rich_export_value(record)]
    return rich_records or records


def _browser_item_record_has_rich_export_value(record: dict[str, Any]) -> bool:
    if any(
        str(record.get(key) or "").strip()
        for key in ("image_url", "source_text")
    ):
        return True
    extra_fields = record.get("extra_fields")
    return isinstance(extra_fields, dict) and any(str(value or "").strip() for value in extra_fields.values())


def _spreadsheet_columns_from_browser_item_rows(rows: list[dict[str, Any]]) -> list[str]:
    columns = [
        "Index",
        "Title",
        "URL",
        "Source Text",
    ]
    seen = set(columns)
    for row in rows:
        for key in row:
            if key == "image_path" or key in seen:
                continue
            seen.add(key)
            columns.append(key)
    return columns


async def _invoke_tool_registry(tool_registry: Any, invocation: ToolInvocation) -> Any:
    invoke = getattr(tool_registry, "invoke")
    if inspect.iscoroutinefunction(invoke):
        return await invoke(invocation)
    return await asyncio.to_thread(invoke, invocation)


def _artifact_fallback_file_stem(task: Any) -> str:
    source = str(getattr(task, "title", "") or getattr(task, "description", "") or "collected_items")
    source = re.sub(r"[^A-Za-z0-9]+", "_", source).strip("_").lower()
    return source[:64] or "collected_items"


def _system_prompt_for_task(config, *, context_in: Any, tool_registry: Any = None) -> str:
    task = config.task
    artifact_root = _artifact_root_for_prompt(task)
    tool_inventory = _scoped_tool_inventory_for_prompt(task, tool_registry=tool_registry)
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    artifact_kind = str(metadata.get("required_artifact_kind") or "").strip().lower().removeprefix(".")
    delivery_contract = _delivery_contract_metadata_for_task(task)
    scheduled_task_guidance = ""
    if metadata.get("scheduled_task_run"):
        scheduled_task_guidance = (
            "Scheduled task rules:\n"
            "- This is a scheduled/background task execution, not an interactive chat turn.\n"
            "- The task description contains the authoritative stored scheduled-job context.\n"
            "- Do not ask the user for missing details or clarification during this run.\n"
            "- If a step cannot be completed from the stored context and scoped tools, return concise "
            "blocked evidence instead of pausing for input.\n\n"
        )
    html_artifact_guidance = ""
    if (artifact_kind == "html" and _task_requires_user_file_delivery(task)) or _task_requires_static_html_delivery(task):
        if delivery_contract:
            contract_lines = [
                "HTML delivery/render contract:",
                f"- platform: {delivery_contract.get('platform') or 'unknown'}",
                f"- delivery_mode: {delivery_contract.get('delivery_mode') or 'attachment'}",
                f"- supports_javascript: {str(bool(delivery_contract.get('supports_javascript'))).lower()}",
                "- requires_static_primary_content: "
                f"{str(bool(delivery_contract.get('requires_static_primary_content'))).lower()}",
            ]
        else:
            platform = _delivery_platform_for_task(task) or "unknown"
            requires_static_primary_content = (
                artifact_kind == "html" and _task_requires_user_file_delivery(task)
            ) or _task_requires_static_html_delivery(task)
            supports_javascript = not requires_static_primary_content
            contract_lines = [
                "HTML delivery/render contract:",
                f"- platform: {platform}",
                "- delivery_mode: attachment",
                f"- supports_javascript: {str(supports_javascript).lower()}",
                f"- requires_static_primary_content: {str(requires_static_primary_content).lower()}",
            ]
        html_artifact_guidance = (
            "\n".join(contract_lines)
            + "\n- If requires_static_primary_content is true, build primary user-visible rows, cards, tables, "
            "lists, and key summary facts in static HTML markup.\n"
            "- JavaScript may enhance the page only when supports_javascript is true, and it must not be required "
            "when requires_static_primary_content is true.\n"
            "- Do not leave primary result containers empty for JavaScript to fill later unless the delivery "
            "contract explicitly allows JavaScript-only rendering.\n\n"
        )
    prompt = (
        "You are a scoped Deep Agent running inside Nullion. Complete only the assigned task. "
        "Use the provided Nullion tools for side effects so Sentinel policy remains authoritative. "
        "Do not claim a file, message, approval, or external change succeeded unless a tool result confirms it. "
        "Return a concise final answer for the user.\n\n"
        f"{scheduled_task_guidance}"
        f"{tool_inventory}"
        f"{html_artifact_guidance}"
        "File delivery rules:\n"
        f"- Save final user-facing files under the workspace artifact directory: {artifact_root}\n"
        "- Do not use /tmp, /var/tmp, or arbitrary absolute paths for final files the user asked to receive.\n"
        "- Temporary scratch files must also stay inside the workspace storage area unless a tool explicitly returns "
        "a workspace-safe generated path.\n"
        "- For CSV or TSV artifacts, use file_write or spreadsheet_create with a .csv/.tsv output path. For linked/non-self-contained HTML, use file_write with inline_local_html_images=false and disallow_html_data_images=true so images remain sibling file references instead of data URIs.\n"
        "- For typed .docx artifact requirements, use document_create with structured paragraphs, sections, "
        "and existing image artifact paths instead of terminal_exec.\n"
        "- For typed .xlsx artifact requirements, use spreadsheet_create with structured rows, links, and existing "
        "image artifact paths instead of terminal_exec. When formulas are requested, set formulas_required=true and include real Excel formula "
        "strings beginning with '=' in row values; static calculated numbers or prose about formula assumptions "
        "do not satisfy a formula request. When charts or conditional formatting are requested, pass "
        "structured charts or conditional_formats specs.\n"
        "- For document-like deliverables such as PDF, DOCX, PPTX, reports, itineraries, and decks, provide "
        "structured title/sections/slides/text pages so the artifact tool can produce a readable report-quality "
        "layout; do not deliver raw browser screenshots, loose image attachments, or unformatted text dumps as "
        "a substitute for the requested formatted document.\n"
        "- Mention a saved or attached file only after file_write, document_create, pdf_create, or another file-producing tool "
        "returns a path in the workspace artifact directory.\n\n"
        "Downstream handoff rules:\n"
        "- When this task produces structured data or source material for a dependent task, make the handoff durable. "
        "If the output is large or likely to be truncated, use a scoped file-producing or artifact-producing tool "
        "available to this task and include the returned workspace path in the final answer. Do not rely on oversized "
        "prose, hidden scratch state, or truncated tool transcripts as the only handoff to downstream tasks.\n"
        "- Durable structured handoffs must include columns or keys for every attribute the assigned task says it "
        "collected. Do not claim an attribute was collected unless it is present in the saved handoff or artifact. "
        "If an attribute cannot be extracted, include an explicit empty/unknown value with available source evidence "
        "instead of claiming complete data in prose only.\n\n"
        f"Task: {task.description}"
    )
    if context_in is not None:
        context_label = task.context_key_in or "dependency_context"
        prompt += f"\n\nContext input ({context_label}):\n{context_in}"
    expected_artifacts = _expected_artifact_paths_from_context(config, context_in=context_in)
    if expected_artifacts and _is_artifact_verifier_task(task):
        prompt += (
            "\n\nArtifact verification contract:\n"
            "- The dependency context contains current-run artifact descriptors.\n"
            "- Verify exactly these current-run artifact paths; do not substitute a similar or newer-looking "
            "workspace file found by search:\n"
            + "\n".join(f"  - {path}" for path in expected_artifacts)
            + "\n- If those paths cannot be read and validated, return a failure for this verifier step."
            "\n- For HTML artifacts, validate the delivered file with scripts disabled: primary user-visible "
            "rows, cards, tables, lists, and key summary facts must exist in static markup, not only in "
            "JavaScript-populated containers."
            "\n- Compare artifact claims against dependency context and tool evidence; fail if the artifact "
            "contains unsupported options, counts, or claims from inconclusive upstream evidence."
        )
    return prompt


def _scoped_tool_inventory_for_prompt(task: Any, *, tool_registry: Any) -> str:
    allowed_tools = [
        str(tool_name).strip()
        for tool_name in (getattr(task, "allowed_tools", ()) or ())
        if str(tool_name).strip()
    ]
    if not allowed_tools:
        return "Scoped tools available to this task: none.\n\n"
    definitions: list[dict[str, Any]] = []
    if tool_registry is not None:
        try:
            definitions = list(tool_registry.list_tool_definitions())
        except Exception:
            definitions = []
    definitions_by_name = {
        str(definition.get("name") or ""): definition
        for definition in definitions
        if isinstance(definition, dict)
    }
    lines = ["Scoped tools available to this task:"]
    for tool_name in allowed_tools:
        definition = definitions_by_name.get(tool_name, {})
        description = str(definition.get("description") or "").strip()
        if description:
            lines.append(f"- {tool_name}: {description}")
        else:
            lines.append(f"- {tool_name}")
    if "web_fetch" not in allowed_tools and "browser_navigate" in allowed_tools:
        extract_text = "browser_extract_text" in allowed_tools
        lines.append(
            "- Direct web_fetch is not scoped for this task; use browser_navigate"
            + (" and browser_extract_text" if extract_text else "")
            + " for public web evidence when a URL or domain is part of the task."
        )
    return "\n".join(lines) + "\n\n"


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
    if getattr(task, "context_key_out", None) and not _task_requires_user_file_delivery(task):
        return []
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


def _required_artifact_extension_for_task(task: Any) -> str | None:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    raw_value = str(metadata.get("required_artifact_kind") or "").strip().lower()
    if not raw_value:
        output = getattr(task, "output", None)
        raw_value = str(getattr(output, "artifact_kind", "") or "").strip().lower()
    if not raw_value:
        return None
    raw_value = raw_value.removeprefix(".")
    if raw_value == "spreadsheet":
        return None
    return f".{raw_value}"


def _artifact_paths_matching_required_extension(paths: Iterable[str], task: Any) -> list[str]:
    required_extension = _required_artifact_extension_for_task(task)
    cleaned = [str(path) for path in paths if str(path or "").strip()]
    if required_extension is None:
        return list(dict.fromkeys(cleaned))
    return list(
        dict.fromkeys(
            path
            for path in cleaned
            if Path(path).suffix.lower() == required_extension
        )
    )


def _artifact_contract_failure_for_task_result(task: Any, result: TaskResult) -> str | None:
    if getattr(result, "status", None) != "success":
        return None
    if not _task_requires_user_file_delivery(task):
        return None
    required_extension = _required_artifact_extension_for_task(task)
    if required_extension is None:
        return None
    candidate_paths: list[str] = []
    candidate_paths.extend(str(path) for path in (getattr(result, "artifacts", None) or ()) if str(path or "").strip())
    candidate_paths.extend(_artifact_path_candidates_from_value(getattr(result, "context_out", None)))
    candidate_paths.extend(_artifact_path_candidates_from_value(getattr(result, "output", None)))
    try:
        artifact_root = artifact_root_for_principal(task.principal_id)
    except Exception:
        artifact_root = None
    if artifact_root is not None:
        candidate_paths = _normalize_artifact_path_candidates(candidate_paths, artifact_root=artifact_root)
    else:
        candidate_paths = list(dict.fromkeys(candidate_paths))
    if _artifact_paths_matching_required_extension(candidate_paths, task):
        return None
    observed = ", ".join(Path(path).name for path in candidate_paths[:4]) or "no downloadable artifact"
    return f"The task did not produce the required {required_extension} artifact. Observed {observed}."


def _context_out_for_task(
    config,
    *,
    output_text: str,
    deliverable_artifacts: list[str],
    tool_results: list[Any] | None = None,
    context_in: Any = None,
) -> object:
    """Publish artifact identity as typed context so dependent tasks stay bound.

    Free-form summaries are not enough for multi-step report workflows: a
    verifier can otherwise search the workspace and accidentally validate an
    older sibling file.  Artifact descriptors are runtime evidence, so dependent
    tasks can verify the exact current-run path without parsing prose.
    """

    workspace_files = _dedupe_context_workspace_files(
        [
            *_context_workspace_files_from_context(context_in),
            *_context_workspace_files_from_tool_results(tool_results),
        ]
    )
    if not deliverable_artifacts:
        if workspace_files:
            return {
                "output": output_text,
                "workspace_files": workspace_files,
                "workspace_file_paths": [str(item.get("path")) for item in workspace_files if item.get("path")],
                "source_task_id": getattr(config.task, "task_id", None),
                "source_group_id": getattr(config.task, "group_id", None),
            }
        return output_text
    task = config.task
    try:
        artifact_root = artifact_root_for_principal(task.principal_id)
    except Exception:
        logger.debug("Could not resolve artifact root for context descriptor", exc_info=True)
        artifact_root = None
    descriptors: list[dict[str, object]] = []
    for raw_path in deliverable_artifacts:
        descriptor = None
        if artifact_root is not None:
            try:
                descriptor = artifact_descriptor_for_path(Path(raw_path), artifact_root=artifact_root)
            except Exception:
                descriptor = None
        if descriptor is not None:
            descriptors.append(descriptor.to_dict())
        else:
            descriptors.append({"path": str(raw_path), "name": Path(str(raw_path)).name})
    return {
        "output": output_text,
        "artifact_paths": [str(item.get("path")) for item in descriptors if item.get("path")],
        "artifacts": descriptors,
        "workspace_files": workspace_files,
        "workspace_file_paths": [str(item.get("path")) for item in workspace_files if item.get("path")],
        "source_task_id": getattr(task, "task_id", None),
        "source_group_id": getattr(task, "group_id", None),
    }


def _context_workspace_files_from_context(context_in: Any) -> list[dict[str, object]]:
    if not isinstance(context_in, dict):
        return []
    files: list[dict[str, object]] = []
    raw_files = context_in.get("workspace_files")
    if isinstance(raw_files, list):
        for item in raw_files:
            if isinstance(item, dict):
                path = _existing_file_path(item.get("path"))
                if path is not None:
                    files.append({
                        "path": str(path),
                        "name": str(item.get("name") or path.name),
                        "media_type": str(item.get("media_type") or ""),
                        "bytes": item.get("bytes"),
                        "source_tool": str(item.get("source_tool") or ""),
                    })
    raw_paths = context_in.get("workspace_file_paths")
    if isinstance(raw_paths, list):
        for raw_path in raw_paths:
            path = _existing_file_path(raw_path)
            if path is not None:
                files.append({"path": str(path), "name": path.name, "media_type": "", "bytes": path.stat().st_size})
    return files


def _context_workspace_files_from_tool_results(tool_results: list[Any] | None) -> list[dict[str, object]]:
    files: list[dict[str, object]] = []
    for result in tool_results or []:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status not in {"completed", "success", "succeeded"}:
            continue
        tool_name = str(getattr(result, "tool_name", "") or "")
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        entries = output.get("entries")
        if isinstance(entries, list):
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                path = _existing_file_path(entry.get("path"))
                if path is None:
                    continue
                files.append({
                    "path": str(path),
                    "name": str(entry.get("name") or path.name),
                    "media_type": str(entry.get("media_type") or ""),
                    "bytes": entry.get("bytes", path.stat().st_size),
                    "source_tool": tool_name,
                })
        for key in (
            "path",
            "artifact_path",
            "manifest_csv_path",
            "manifest_json_path",
            "manifest_xlsx_path",
        ):
            path = _existing_file_path(output.get(key))
            if path is not None:
                files.append({
                    "path": str(path),
                    "name": path.name,
                    "media_type": "",
                    "bytes": path.stat().st_size,
                    "source_tool": tool_name,
                })
        for key in ("artifact_paths", "manifest_artifact_paths"):
            values = output.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                path = _existing_file_path(value)
                if path is not None:
                    files.append({
                        "path": str(path),
                        "name": path.name,
                        "media_type": "",
                        "bytes": path.stat().st_size,
                        "source_tool": tool_name,
                    })
    return files


def _existing_file_path(value: Any) -> Path | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        path = Path(value).expanduser().resolve()
    except (OSError, RuntimeError):
        return None
    try:
        if path.is_file():
            return path
    except OSError:
        return None
    return None


def _dedupe_context_workspace_files(files: list[dict[str, object]], *, limit: int = 200) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in files:
        path = str(item.get("path") or "").strip()
        if not path or path in seen:
            continue
        seen.add(path)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


def _is_artifact_verifier_task(task: Any) -> bool:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    return str(metadata.get("artifact_role") or "").strip() == "verify"


def _delivery_contract_metadata_for_task(task: Any) -> dict[str, object]:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    contract = metadata.get("delivery_contract")
    return dict(contract) if isinstance(contract, dict) else {}


def _delivery_platform_for_task(task: Any) -> str:
    contract = _delivery_contract_metadata_for_task(task)
    platform = str(contract.get("platform") or "").strip().lower()
    if platform:
        return platform
    for value in (getattr(task, "principal_id", None), getattr(task, "conversation_id", None)):
        raw = str(value or "").strip().lower()
        if ":" not in raw:
            continue
        prefix = raw.split(":", 1)[0]
        if prefix:
            return prefix
    return ""


def _task_requires_static_html_delivery(task: Any) -> bool:
    contract = _delivery_contract_metadata_for_task(task)
    if "requires_static_primary_content" in contract:
        return bool(contract.get("requires_static_primary_content"))
    platform = _delivery_platform_for_task(task)
    return platform in {"telegram", "slack", "discord", "unknown"}


def _html_static_primary_content_required(task: Any) -> bool:
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    contract = _delivery_contract_metadata_for_task(task)
    if "requires_static_primary_content" in contract:
        return bool(contract.get("requires_static_primary_content"))
    if _task_requires_static_html_delivery(task):
        return True
    artifact_kind = str(metadata.get("required_artifact_kind") or "").strip().lower().removeprefix(".")
    if artifact_kind != "html":
        return False
    finish = getattr(task, "finish", None)
    output = getattr(task, "output", None)
    return bool(
        getattr(finish, "requires_artifact_delivery", False)
        or getattr(output, "artifact_kind", None)
        or metadata.get("requires_artifact_delivery")
        or metadata.get("required_artifact_kind")
    )


def _artifact_verification_failure_for_task(
    config,
    *,
    context_in: Any,
    output_text: str,
    tool_results: list[Any],
    deliverable_artifacts: list[str],
) -> str | None:
    task = config.task
    if not _is_artifact_verifier_task(task):
        return None
    expected_paths = _expected_artifact_paths_from_context(config, context_in=context_in)
    if not expected_paths:
        return None
    observed_paths = _observed_artifact_paths_for_verification(
        config,
        output_text=output_text,
        tool_results=tool_results,
        deliverable_artifacts=deliverable_artifacts,
    )
    required_extension = _required_artifact_extension_for_task(task)
    if required_extension is not None:
        expected_matching_paths = _artifact_paths_matching_required_extension(expected_paths, task)
        if not expected_matching_paths:
            observed_expected = ", ".join(Path(path).name for path in expected_paths[:4]) or "no current-run artifact path"
            return (
                f"The upstream artifact context did not contain the required {required_extension} artifact. "
                f"Observed {observed_expected}."
            )
        expected_paths = expected_matching_paths
        observed_paths = _artifact_paths_matching_required_extension(observed_paths, task)
    if not observed_paths and not _artifact_verifier_has_observation_tools(task):
        if _html_static_primary_content_required(task):
            static_delivery_failure = _html_static_delivery_failure_for_paths(expected_paths)
            if static_delivery_failure is not None:
                return static_delivery_failure
        content_failure = _artifact_content_delivery_failure_for_context(expected_paths, context_in)
        if content_failure is not None:
            return content_failure
        return None
    matched_expected_paths = [path for path in expected_paths if path in observed_paths]
    if matched_expected_paths:
        if _html_static_primary_content_required(task):
            static_delivery_failure = _html_static_delivery_failure_for_paths(matched_expected_paths)
            if static_delivery_failure is not None:
                return static_delivery_failure
        content_failure = _artifact_content_delivery_failure_for_context(matched_expected_paths, context_in)
        if content_failure is not None:
            return content_failure
        return None
    observed_text = ", ".join(Path(path).name for path in observed_paths[:4]) or "no current-run artifact path"
    expected_text = ", ".join(Path(path).name for path in expected_paths[:4])
    return (
        "The verifier did not validate the current-run artifact from dependency context. "
        f"Expected {expected_text}; observed {observed_text}."
    )


def _artifact_content_delivery_failure_for_context(paths: list[str], context_in: Any) -> str | None:
    obligations = _artifact_content_obligations_from_context(context_in)
    if obligations is None:
        return None
    return _artifact_content_delivery_failure_for_obligations(paths, obligations)


def _artifact_content_delivery_failure_for_obligations(
    paths: list[str],
    obligations: dict[str, int | bool],
) -> str | None:
    for raw_path in paths:
        if not isinstance(raw_path, str):
            continue
        path = Path(raw_path)
        if path.suffix.lower() not in {".docx", ".htm", ".html", ".pdf", ".pptx", ".xlsx", ".xlsm"}:
            continue
        summary = _artifact_content_summary_for_path(path)
        if summary is None:
            return f"The artifact could not be inspected as a valid {path.suffix.lower()} file: {path.name}."
        failure = _artifact_content_failure_for_summary(path, summary, obligations)
        if failure is not None:
            return failure
    return None


def _artifact_content_obligations_from_context(context_in: Any) -> dict[str, int | bool] | None:
    item_records = _exportable_browser_item_records(_browser_item_records_from_context(context_in))
    if not item_records:
        return None
    image_count = sum(1 for record in item_records if str(record.get("image_url") or "").strip())
    row_count_contract = _structured_row_count_contract_from_context(context_in)
    return {
        "item_count": len(item_records),
        "spreadsheet_row_floor": row_count_contract or 1,
        "spreadsheet_row_contract": bool(row_count_contract),
        "image_count": image_count,
        "requires_media": image_count > 0,
    }


def _structured_row_count_contract_from_context(context_in: Any) -> int | None:
    if not isinstance(context_in, dict):
        return None
    for key in (
        "expected_rows",
        "expected_row_count",
        "required_rows",
        "required_row_count",
        "artifact_row_count",
    ):
        value = context_in.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int) and value > 0:
            return value
    artifacts = context_in.get("artifacts")
    if isinstance(artifacts, list):
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            for key in ("expected_rows", "expected_row_count", "row_count", "data_rows"):
                value = artifact.get(key)
                if isinstance(value, bool):
                    continue
                if isinstance(value, int) and value > 0:
                    return value
    for key in ("output", "result", "summary", "result_summary", "content", "body", "text"):
        value = context_in.get(key)
        if not isinstance(value, str):
            continue
        count = len(_markdown_table_records_from_text(value))
        if count > 0:
            return count
    return None


def _artifact_content_failure_for_summary(
    path: Path,
    summary: dict[str, int | bool | str],
    obligations: dict[str, int | bool],
) -> str | None:
    kind = str(summary.get("kind") or path.suffix.lower().lstrip("."))
    if kind == "spreadsheet":
        expected_rows_floor = int(obligations.get("spreadsheet_row_floor") or 1)
        if int(summary.get("data_rows") or 0) < expected_rows_floor:
            noun = "structured row-count contract" if obligations.get("spreadsheet_row_contract") else "structured records"
            return (
                f"The spreadsheet artifact does not contain the {noun} from the collected handoff. "
                f"Expected at least {expected_rows_floor} populated data row(s); found {summary.get('data_rows') or 0}."
            )
    elif kind in {"document", "presentation", "html"}:
        if int(summary.get("text_units") or 0) <= 0 and int(summary.get("text_chars") or 0) <= 0:
            return f"The {kind} artifact does not contain visible content from the collected handoff."
    elif kind == "pdf":
        if not bool(summary.get("valid_pdf")):
            return "The PDF artifact is not a valid PDF file."
        if int(summary.get("bytes") or 0) < 1024:
            return "The PDF artifact is too small to contain the requested content."
    if bool(obligations.get("requires_media")):
        media_count = int(summary.get("nontrivial_media") or summary.get("media_refs") or 0)
        required_images = max(1, min(int(obligations.get("image_count") or 0), 10))
        if media_count < required_images:
            return (
                f"The {kind} artifact is missing embedded or referenced image media from the collected handoff. "
                f"Expected at least {required_images} image item(s); found {media_count}."
            )
    return None


def _artifact_content_summary_for_path(path: Path) -> dict[str, int | bool | str] | None:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        return _xlsx_artifact_content_summary(path)
    if suffix == ".docx":
        return _docx_artifact_content_summary(path)
    if suffix == ".pptx":
        return _pptx_artifact_content_summary(path)
    if suffix in {".htm", ".html"}:
        return _html_artifact_content_summary(path)
    if suffix == ".pdf":
        return _pdf_artifact_content_summary(path)
    return None


def _xlsx_artifact_content_summary(path: Path) -> dict[str, int | str] | None:
    try:
        with zipfile.ZipFile(path) as workbook:
            names = workbook.namelist()
            media_sizes = [
                info.file_size
                for info in workbook.infolist()
                if info.filename.startswith("xl/media/") and not info.is_dir()
            ]
            worksheet_names = [
                name
                for name in names
                if name.startswith("xl/worksheets/") and name.endswith(".xml")
            ]
            data_rows = 0
            for worksheet_name in worksheet_names:
                try:
                    root = ET.fromstring(workbook.read(worksheet_name))
                except ET.ParseError:
                    continue
                for row in root.iter():
                    if _xml_local_name(row.tag) != "row":
                        continue
                    row_index = _positive_int(row.attrib.get("r"))
                    if row_index == 1:
                        continue
                    if _xlsx_row_has_value(row):
                        data_rows += 1
            return {
                "kind": "spreadsheet",
                "data_rows": data_rows,
                "text_units": data_rows,
                "media": len(media_sizes),
                "nontrivial_media": sum(1 for size in media_sizes if size >= 512),
            }
    except (OSError, zipfile.BadZipFile):
        return None


def _docx_artifact_content_summary(path: Path) -> dict[str, int | str] | None:
    try:
        with zipfile.ZipFile(path) as package:
            text_units, text_chars = _openxml_text_summary(
                package,
                [name for name in package.namelist() if name.startswith("word/") and name.endswith(".xml")],
            )
            media_sizes = [
                info.file_size
                for info in package.infolist()
                if info.filename.startswith("word/media/") and not info.is_dir()
            ]
            return {
                "kind": "document",
                "text_units": text_units,
                "text_chars": text_chars,
                "media": len(media_sizes),
                "nontrivial_media": sum(1 for size in media_sizes if size >= 512),
            }
    except (OSError, zipfile.BadZipFile):
        return None


def _pptx_artifact_content_summary(path: Path) -> dict[str, int | str] | None:
    try:
        with zipfile.ZipFile(path) as package:
            slide_names = [
                name
                for name in package.namelist()
                if name.startswith("ppt/slides/") and name.endswith(".xml")
            ]
            text_units, text_chars = _openxml_text_summary(package, slide_names)
            media_sizes = [
                info.file_size
                for info in package.infolist()
                if info.filename.startswith("ppt/media/") and not info.is_dir()
            ]
            return {
                "kind": "presentation",
                "slides": len(slide_names),
                "text_units": text_units,
                "text_chars": text_chars,
                "media": len(media_sizes),
                "nontrivial_media": sum(1 for size in media_sizes if size >= 512),
            }
    except (OSError, zipfile.BadZipFile):
        return None


def _openxml_text_summary(package: zipfile.ZipFile, xml_names: list[str]) -> tuple[int, int]:
    text_units = 0
    text_chars = 0
    for xml_name in xml_names:
        try:
            root = ET.fromstring(package.read(xml_name))
        except (KeyError, ET.ParseError):
            continue
        for node in root.iter():
            if _xml_local_name(node.tag) != "t":
                continue
            text = (node.text or "").strip()
            if not text:
                continue
            text_units += 1
            text_chars += len(text)
    return text_units, text_chars


def _html_artifact_content_summary(path: Path) -> dict[str, int | str] | None:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None
    body = _HTML_SCRIPT_OR_STYLE_RE.sub(" ", text)
    body = _HTML_TAG_RE.sub(" ", body)
    visible_text = " ".join(body.split())
    image_refs = len(re.findall(r"<img\b", text, flags=re.IGNORECASE))
    return {
        "kind": "html",
        "text_units": 1 if visible_text else 0,
        "text_chars": len(visible_text),
        "media_refs": image_refs,
        "nontrivial_media": image_refs,
    }


def _pdf_artifact_content_summary(path: Path) -> dict[str, int | bool | str] | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    media_refs = data.count(b"/Subtype /Image") + data.count(b"/Image")
    return {
        "kind": "pdf",
        "valid_pdf": data.startswith(b"%PDF"),
        "bytes": len(data),
        "media_refs": media_refs,
        "nontrivial_media": media_refs,
    }


def _xlsx_row_has_value(row: ET.Element) -> bool:
    for cell in row.iter():
        tag = _xml_local_name(cell.tag)
        if tag == "v" and (cell.text or "").strip():
            return True
        if tag == "t" and (cell.text or "").strip():
            return True
    return False


def _xml_local_name(tag: str) -> str:
    return str(tag).rsplit("}", 1)[-1]


def _positive_int(value: Any) -> int | None:
    try:
        number = int(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _artifact_verifier_has_observation_tools(task: Any) -> bool:
    tools = {
        str(tool or "").strip()
        for tool in (getattr(task, "allowed_tools", ()) or ())
        if str(tool or "").strip()
    }
    return bool(tools.intersection({"file_read", "file_search", "workspace_summary"}))


def _artifact_verification_success_output_for_task(config, *, context_in: Any) -> str | None:
    task = getattr(config, "task", None)
    if not _is_artifact_verifier_task(task) or _artifact_verifier_has_observation_tools(task):
        return None
    expected_paths = _expected_artifact_paths_from_context(config, context_in=context_in)
    if not expected_paths:
        return None
    names = [Path(path).name for path in expected_paths if str(path or "").strip()]
    if len(names) == 1:
        return f"Verified current-run artifact: {names[0]}"
    if names:
        return "Verified current-run artifacts: " + ", ".join(names)
    return "Verified current-run artifact."


def _html_static_delivery_failure_for_paths(paths: list[str]) -> str | None:
    """Reject delivered HTML whose primary report content only appears after JS runs."""

    for raw_path in paths:
        path = Path(raw_path)
        if path.suffix.lower() not in {".html", ".htm"}:
            continue
        try:
            html = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        empty_targets = _html_dynamic_primary_targets_without_static_content(html)
        if not empty_targets:
            continue
        target_text = ", ".join(empty_targets[:4])
        return (
            "The HTML artifact is not statically deliverable: primary content for empty container(s) "
            f"{target_text} is populated only by JavaScript. Include the delivered rows, cards, tables, "
            "or lists in static HTML so Telegram, mobile previews, and no-script viewers can render them."
        )
    return None


def _html_dynamic_primary_targets_without_static_content(html: str) -> list[str]:
    target_ids: list[str] = []
    for match in _HTML_DYNAMIC_CONTENT_ASSIGNMENT_RE.finditer(html):
        rhs = str(match.group("rhs") or "").lower()
        if not any(marker in rhs for marker in _HTML_DYNAMIC_PRIMARY_MARKERS):
            continue
        target_id = str(match.group("id") or match.group("selector_id") or "").strip()
        if target_id:
            target_ids.append(target_id)
    if not target_ids:
        return []

    static_bodies = _static_html_element_bodies_by_id(html)
    empty_targets: list[str] = []
    for target_id in dict.fromkeys(target_ids):
        body = static_bodies.get(target_id)
        if body is None:
            continue
        if not _html_fragment_has_static_user_content(body):
            empty_targets.append(target_id)
    return empty_targets


def _static_html_element_bodies_by_id(html: str) -> dict[str, str]:
    html_without_scripts = _HTML_SCRIPT_OR_STYLE_RE.sub("", html)
    return {
        str(match.group("id") or "").strip(): str(match.group("body") or "")
        for match in _HTML_ID_ELEMENT_RE.finditer(html_without_scripts)
        if str(match.group("id") or "").strip()
    }


def _html_fragment_has_static_user_content(fragment: str) -> bool:
    if re.search(r"<\s*(?:a|article|figure|img|li|p|section|table|td|th|tr)\b", fragment, re.IGNORECASE):
        return True
    text = _HTML_TAG_RE.sub(" ", fragment)
    return len(" ".join(text.split())) >= 24


def _expected_artifact_paths_from_context(config, *, context_in: Any) -> list[str]:
    task = getattr(config, "task", None)
    if context_in is None or task is None:
        return []
    try:
        artifact_root = artifact_root_for_principal(task.principal_id)
    except Exception:
        logger.debug("Could not resolve artifact root for expected artifact extraction", exc_info=True)
        return []
    return _normalize_artifact_path_candidates(
        _artifact_path_candidates_from_value(context_in),
        artifact_root=artifact_root,
    )


def _observed_artifact_paths_for_verification(
    config,
    *,
    output_text: str,
    tool_results: list[Any],
    deliverable_artifacts: list[str],
) -> list[str]:
    task = config.task
    try:
        artifact_root = artifact_root_for_principal(task.principal_id)
    except Exception:
        logger.debug("Could not resolve artifact root for observed artifact extraction", exc_info=True)
        return []
    candidates: list[str] = []
    candidates.extend(deliverable_artifacts)
    candidates.extend(_artifact_path_candidates_from_value(output_text))
    for result in tool_results or ():
        output = getattr(result, "output", None)
        candidates.extend(_artifact_path_candidates_from_value(output))
    return _normalize_artifact_path_candidates(candidates, artifact_root=artifact_root)


def _artifact_path_candidates_from_value(value: Any) -> list[str]:
    candidates: list[str] = []
    if value is None:
        return candidates
    if isinstance(value, str):
        candidates.extend(str(path) for path in media_candidate_paths_from_text(value))
        return candidates
    if isinstance(value, dict):
        for key in ("path", "artifact_path"):
            raw_path = value.get(key)
            if isinstance(raw_path, str) and raw_path.strip():
                candidates.append(raw_path)
        for key in ("artifact_paths", "artifacts", "files"):
            raw_values = value.get(key)
            if isinstance(raw_values, (list, tuple)):
                for item in raw_values:
                    if isinstance(item, str) and key == "artifact_paths" and item.strip():
                        candidates.append(item)
                    else:
                        candidates.extend(_artifact_path_candidates_from_value(item))
            elif isinstance(raw_values, dict):
                candidates.extend(_artifact_path_candidates_from_value(raw_values))
        for key in ("output", "summary", "message", "text", "content"):
            nested = value.get(key)
            if nested is not value:
                candidates.extend(_artifact_path_candidates_from_value(nested))
        return candidates
    if isinstance(value, (list, tuple, set)):
        for item in value:
            candidates.extend(_artifact_path_candidates_from_value(item))
    return candidates


def _normalize_artifact_path_candidates(candidates: list[str], *, artifact_root: Path) -> list[str]:
    paths: list[str] = []
    for raw_path in candidates:
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        candidate = Path(raw_path).expanduser()
        if not candidate.is_absolute():
            candidate = artifact_root / candidate.name
        try:
            descriptor = artifact_descriptor_for_path(candidate, artifact_root=artifact_root)
        except Exception:
            descriptor = None
        if descriptor is not None:
            paths.append(descriptor.path)
    return list(dict.fromkeys(paths))


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


def _artifact_delivery_failure_for_task(
    config,
    artifact_paths: list[str],
    deliverable_artifacts: list[str],
    *,
    tool_results: list[Any] | None = None,
    context_in: Any = None,
) -> str | None:
    task = config.task
    if deliverable_artifacts and artifact_media_embedding_obligation_outstanding(tool_results):
        return (
            "The mini-agent created a downloadable file, but the current tool evidence still requires "
            "embedded image/screenshot media inside the artifact."
        )
    if deliverable_artifacts:
        required_extension = _required_artifact_extension_for_task(task)
        if required_extension is not None and not _artifact_paths_matching_required_extension(deliverable_artifacts, task):
            observed = ", ".join(Path(path).name for path in deliverable_artifacts[:4]) or "no downloadable artifact"
            return (
                f"The mini-agent created downloadable artifact(s), but none matched the required "
                f"{required_extension} artifact kind. Observed {observed}."
            )
        content_failure = _artifact_content_delivery_failure_for_context(deliverable_artifacts, context_in)
        if content_failure is not None:
            return content_failure
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
        # Verifier and receipt tasks consume an artifact contract produced by an
        # upstream step. They should validate or summarize that contract, not be
        # failed for not creating another downloadable file themselves.
        if str(metadata.get("artifact_role") or "").strip() in {"verify", "deliver_receipt"}:
            return False
        return bool(metadata.get("requires_artifact_delivery") or metadata.get("required_artifact_kind"))
    return False


def _completion_progress_kind(result: TaskResult) -> str:
    if result.status == "success":
        return "task_complete"
    if result.status == "partial":
        return "input_needed" if (result.output or "").startswith("Waiting for user input:") else "progress_note"
    return "task_failed"


def _deep_agent_external_skills_for_task(config) -> list[str] | None:
    task = config.task
    raw = getattr(task, "deep_agent_skills", None)
    if raw is None:
        raw = os.environ.get("NULLION_DEEP_AGENTS_SKILLS", "")
    raw_values = raw if isinstance(raw, (list, tuple)) else str(raw).split(",")
    skills = [
        str(skill).strip()
        for skill in raw_values
        if str(skill).strip() and not str(skill).strip().startswith("/skills/nullion/")
    ]
    env_skills = [
        str(skill).strip()
        for skill in str(os.environ.get("NULLION_DEEP_AGENTS_SKILLS", "") or "").split(",")
        if str(skill).strip()
    ]
    skills.extend(env_skills)
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

    tools = [
        StructuredTool.from_function(
            coroutine=report_progress,
            name="report_progress",
            description="Report a short progress update to the user.",
        ),
    ]
    if config.can_request_user_input:
        tools.append(
            StructuredTool.from_function(
                coroutine=request_user_input,
                name="request_user_input",
                description="Ask the user a question and pause the task.",
            )
        )
    return tools


def _deep_agent_graph_config(config) -> dict[str, Any]:
    budget = int(config.max_iterations) * (max(0, int(config.max_continuations)) + 1)
    task = getattr(config, "task", None)
    allowed_tools = _task_allowed_tool_names(task)
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    needs_long_graph = bool(
        getattr(task, "context_key_out", None)
        or metadata.get("requires_artifact_delivery")
        or {
            "browser_extract_detail",
            "browser_extract_items",
            "browser_extract_text",
            "browser_run_js",
            "browser_image_collect",
        }.intersection(
            allowed_tools
        )
    )
    recursion_limit = max(25, budget * 3 + 8)
    if needs_long_graph:
        recursion_limit = max(recursion_limit, budget * 8 + 32, 128)
    return {
        "recursion_limit": recursion_limit,
        "configurable": {"thread_id": _deep_agent_thread_id(config)},
        "metadata": {"nullion_mini_agent_id": config.agent_id, "nullion_task_id": config.task.task_id},
    }


def _deep_agent_thread_id(config) -> str:
    task = getattr(config, "task", None)
    metadata = getattr(task, "metadata", None)
    if isinstance(metadata, dict):
        thread_id = str(metadata.get("deep_agent_thread_id") or "").strip()
        if thread_id:
            return thread_id
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


def _completion_decision_timeout_seconds() -> float:
    return _float_env(
        _DEEP_AGENT_COMPLETION_DECISION_TIMEOUT_SECONDS_ENV,
        _DEFAULT_DEEP_AGENT_COMPLETION_DECISION_TIMEOUT_SECONDS,
        minimum=0.1,
    )


async def _scheduled_task_completion_decision(
    config,
    *,
    output_text: str,
    tool_results: list[Any],
    model_client: Any,
) -> dict[str, str] | None:
    """Validate scheduled mini-agent completion before publishing dependency context."""
    if not _is_scheduled_background_config(config):
        return None
    create = getattr(model_client, "create", None)
    if create is None or not _model_client_can_make_evidence_fallback_decision(model_client):
        return None
    task = getattr(config, "task", None)
    evidence = _tool_evidence_payload(tool_results)
    system_prompt = (
        "You validate one completed Nullion scheduled-task step. "
        "Use only the assigned task, dependency context, final answer, and verified tool-result evidence. "
        "Output only JSON with this schema: "
        '{"status":"success"|"failure","answer":"string"}. '
        "Set status=failure when required evidence, data, side effects, or verification are insufficient. "
        "For a reasoning/synthesis-only step with no side-effect tool to perform, do not set failure solely "
        "because some candidate items are unverified; return success when the answer can report verified "
        "partial findings with clear caveats and avoid unsafe downstream claims. "
        "External platform delivery is performed by Nullion after this step; do not require proof that a "
        "message, report, or artifact was already sent unless the assigned task had an available side-effect "
        "tool and the final answer claims that tool-side effect already happened. "
        "For success, keep answer as the concise user-facing task result. "
        "Do not mention internal validation, tools, or orchestration."
    )
    user_payload = {
        "task": str(getattr(task, "description", "") or getattr(task, "title", "") or ""),
        "allowed_tools": list(getattr(task, "allowed_tools", ()) or ()),
        "dependency_context": getattr(config, "context_in", None),
        "final_answer": str(output_text or ""),
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
        "max_tokens": 700,
        "system": system_prompt,
    }
    try:
        response = await asyncio.wait_for(
            _call_model_create(create, create_kwargs),
            timeout=_completion_decision_timeout_seconds(),
        )
    except Exception:
        logger.warning(
            "DeepAgent scheduled completion validation failed; preserving original result "
            "agent_id=%s task_id=%s",
            getattr(config, "agent_id", ""),
            str(getattr(task, "task_id", "") or ""),
            exc_info=True,
        )
        return None
    return _parse_evidence_fallback_decision(_extract_response_text(response))


async def _fallback_answer_from_tool_evidence(
    config,
    *,
    tool_results: list[Any],
    model_client: Any,
) -> str:
    completed_results = _completed_tool_results(tool_results)
    if not completed_results:
        raise DeepAgentEvidenceFallbackUnavailable("Verified tool evidence was unavailable for recovery.")
    deterministic_answer = _deterministic_scheduled_tool_evidence_answer(config, completed_results)
    create = getattr(model_client, "create", None)
    if create is None:
        if deterministic_answer:
            return deterministic_answer
        raise DeepAgentEvidenceFallbackUnavailable("Verified tool evidence could not be summarized.")
    if not _model_client_can_make_evidence_fallback_decision(model_client):
        if deterministic_answer:
            return deterministic_answer
        return _generic_tool_evidence_answer(config, completed_results)
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
        if deterministic_answer:
            logger.warning(
                "DeepAgent scheduled evidence fallback model timed out; using deterministic tool evidence summary "
                "agent_id=%s task_id=%s completed_tools=%d",
                getattr(config, "agent_id", ""),
                getattr(getattr(config, "task", None), "task_id", ""),
                len(completed_results),
            )
            return deterministic_answer
        raise DeepAgentEvidenceFallbackUnavailable(
            "Verified tool evidence recovery timed out."
        ) from exc
    decision = _parse_evidence_fallback_decision(_extract_response_text(response))
    if decision is None:
        if deterministic_answer:
            return deterministic_answer
        return _generic_tool_evidence_answer(config, completed_results)
    if decision.get("status") != "success":
        reason = str(decision.get("answer") or "Verified tool evidence was insufficient to finish the task.").strip()
        metadata = getattr(config.task, "metadata", None)
        metadata = metadata if isinstance(metadata, dict) else {}
        if metadata.get("scheduled_task_run") and reason:
            return reason
        raise DeepAgentEvidenceFallbackUnavailable(reason)
    answer = str(decision.get("answer") or "").strip()
    if not answer:
        if deterministic_answer:
            return deterministic_answer
        return _generic_tool_evidence_answer(config, completed_results)
    return answer


def _model_client_can_make_evidence_fallback_decision(model_client: Any) -> bool:
    if callable(getattr(model_client, "create", None)):
        return True
    provider = str(getattr(model_client, "provider", "") or "").strip()
    model = str(getattr(model_client, "model", "") or "").strip()
    if provider or model:
        return True
    module = str(getattr(type(model_client), "__module__", "") or "")
    return module.startswith("nullion.")


def _generic_tool_evidence_answer(config, tool_results: list[Any]) -> str:
    task = getattr(config, "task", None)
    title = str(getattr(task, "title", "") or getattr(task, "description", "") or "Task").strip()
    names = [
        str(getattr(result, "tool_name", "") or "").strip()
        for result in tool_results
        if str(getattr(result, "tool_name", "") or "").strip()
    ]
    tool_summary = ", ".join(dict.fromkeys(names))
    if tool_summary:
        return f"{title} completed using verified runtime evidence from {tool_summary}."
    return f"{title} completed using verified runtime evidence."


def _deterministic_scheduled_tool_evidence_answer(config, tool_results: list[Any]) -> str:
    task = getattr(config, "task", None)
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    if not metadata.get("scheduled_task_run"):
        return ""
    completed_results = _completed_tool_results(tool_results)
    if not completed_results:
        return ""
    title = str(getattr(task, "title", "") or "Scheduled subtask").strip()
    tool_names = [
        str(getattr(result, "tool_name", "") or "").strip()
        for result in completed_results
        if str(getattr(result, "tool_name", "") or "").strip()
    ]
    tool_summary = ", ".join(dict.fromkeys(tool_names[:6]))
    if tool_summary:
        return f"{title} completed with verified runtime evidence from {tool_summary}."
    return f"{title} completed with verified runtime evidence."


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


async def _invoke_agent_with_heartbeat(
    agent: Any,
    payload: dict[str, Any],
    *,
    config: dict[str, Any],
    progress_queue: asyncio.Queue,
    mini_agent_config: Any,
) -> dict[str, Any]:
    invoke_task = asyncio.create_task(
        _invoke_agent(
            agent,
            payload,
            config=config,
            progress_queue=progress_queue,
            mini_agent_config=mini_agent_config,
        )
    )
    heartbeat_task = asyncio.create_task(
        _emit_deep_agent_heartbeat(
            progress_queue,
            mini_agent_config=mini_agent_config,
            invoke_task=invoke_task,
        )
    )
    try:
        return await invoke_task
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task


async def _emit_deep_agent_heartbeat(
    progress_queue: asyncio.Queue,
    *,
    mini_agent_config: Any,
    invoke_task: asyncio.Task,
) -> None:
    interval = _deep_agent_heartbeat_seconds()
    while not invoke_task.done():
        await asyncio.sleep(interval)
        if invoke_task.done():
            return
        await _emit_progress(
            progress_queue,
            config=mini_agent_config,
            kind="progress_note",
            message="Still working on this planner step.",
            data={"phase": "deep_agent_heartbeat"},
        )


def _deep_agent_heartbeat_seconds() -> float:
    raw = os.environ.get("NULLION_DEEP_AGENT_HEARTBEAT_SECONDS", "45")
    try:
        return max(5.0, float(raw))
    except ValueError:
        return 45.0


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
