"""Deep Agents backend for scoped Nullion mini-agent tasks."""

from __future__ import annotations

import asyncio
from collections import deque
import inspect
import json
import logging
import os
from pathlib import Path
import re
import shutil
from typing import Any

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
            run_coro = self._run_inner(
                config,
                anthropic_client=anthropic_client,
                tool_registry=tool_registry,
                policy_store=policy_store,
                context_bus=context_bus,
                progress_queue=progress_queue,
            )
            if _is_scheduled_background_config(config):
                result = await run_coro
            else:
                result = await asyncio.wait_for(run_coro, timeout=config.timeout_s)
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

        system_prompt = _system_prompt_for_task(config, context_in=context_in, tool_registry=tool_registry)
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
            skills=_deep_agent_external_skills_for_task(config),
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
            _artifact_delivery_failure_for_task(config, artifacts, deliverable_artifacts)
            if _task_requires_user_file_delivery(task)
            else None
        )
        if artifact_failure:
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
        context_out = _context_out_for_task(
            config,
            output_text=output_text,
            deliverable_artifacts=deliverable_artifacts,
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
        "- For typed .docx artifact requirements, use document_create with structured paragraphs, sections, "
        "and existing image artifact paths instead of terminal_exec.\n"
        "- For typed .xlsx artifact requirements, use spreadsheet_create with structured rows, links, and existing "
        "image artifact paths instead of terminal_exec.\n"
        "- Mention a saved or attached file only after file_write, document_create, pdf_create, or another file-producing tool "
        "returns a path in the workspace artifact directory.\n\n"
        f"Task: {task.description}"
    )
    if context_in is not None:
        prompt += f"\n\nContext input ({task.context_key_in}):\n{context_in}"
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


def _context_out_for_task(config, *, output_text: str, deliverable_artifacts: list[str]) -> object:
    """Publish artifact identity as typed context so dependent tasks stay bound.

    Free-form summaries are not enough for multi-step report workflows: a
    verifier can otherwise search the workspace and accidentally validate an
    older sibling file.  Artifact descriptors are runtime evidence, so dependent
    tasks can verify the exact current-run path without parsing prose.
    """

    if not deliverable_artifacts:
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
        "source_task_id": getattr(task, "task_id", None),
        "source_group_id": getattr(task, "group_id", None),
    }


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
    matched_expected_paths = [path for path in expected_paths if path in observed_paths]
    if matched_expected_paths:
        if _html_static_primary_content_required(task):
            static_delivery_failure = _html_static_delivery_failure_for_paths(matched_expected_paths)
            if static_delivery_failure is not None:
                return static_delivery_failure
        return None
    observed_text = ", ".join(Path(path).name for path in observed_paths[:4]) or "no current-run artifact path"
    expected_text = ", ".join(Path(path).name for path in expected_paths[:4])
    return (
        "The verifier did not validate the current-run artifact from dependency context. "
        f"Expected {expected_text}; observed {observed_text}."
    )


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
