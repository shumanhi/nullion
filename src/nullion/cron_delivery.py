"""Shared cron delivery routing helpers.

Cron jobs can be created by web chat, Telegram, Slack, Discord, or direct REST.
Keep routing decisions here so adapters do not each infer delivery semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, TypedDict

from langgraph.graph import END, START, StateGraph

SUPPORTED_CRON_DELIVERY_CHANNELS = frozenset({"web", "telegram", "slack", "discord"})
MESSAGING_CRON_DELIVERY_CHANNELS = frozenset({"telegram", "slack", "discord"})
MAX_CRON_TEXT_ARTIFACT_CHARS = 12000
DEFAULT_CRON_NO_OUTPUT_MESSAGE = "Cron ran successfully; no output was produced."
SCHEDULED_TASK_DELIVERY_PREFIX = "⏰ Scheduled task:"

# Cron delivery contract for future agents:
# - Cron can deliver text, file attachments, both, or no message.
# - Explicit MEDIA lines are user-facing file delivery and must be preserved.
# - Completed tool results with structured artifact fields are user-facing file
#   delivery evidence and should be converted to MEDIA lines after state-file
#   filtering.
# - Raw artifact paths/objects are internal evidence unless the agent makes them
#   explicit with MEDIA or they came from a completed tool artifact field.
# - Activity/status summaries should show that tools ran, but tool outputs that
#   contain internal task text, paths, state files, artifacts, or connector
#   payloads are not deliverables.
# - Alert-only/no-change runs may be silent.
# - Unspecified no-output runs should use DEFAULT_CRON_NO_OUTPUT_MESSAGE.
# If you are asked to change this contract, confirm the intended behavior first
# and update the cron delivery E2E matrix in nullion-test with the change.


def normalize_cron_delivery_channel(channel: object) -> str:
    normalized = str(channel or "").strip().lower()
    return normalized if normalized in SUPPORTED_CRON_DELIVERY_CHANNELS else ""


def cron_agent_prompt(job: object, *, label: str) -> str:
    """Build the synthetic user message for a scheduled task turn."""
    name = str(getattr(job, "name", "") or "Scheduled task").strip()
    task = str(getattr(job, "task", "") or "").strip()
    return (
        f"[{label}: {name}] {task}\n\n"
        "Scheduled task execution context:\n"
        "- This is an existing scheduled task run. Schedule text is runtime metadata, not a request to create another schedule.\n"
        "- Do not create, update, delete, toggle, or run scheduled tasks from this execution context.\n\n"
        "Scheduled task delivery contract:\n"
        "- Cron may deliver text, file attachments, both, or no message, depending on the task.\n"
        "- If a file/report/export is expected, create it and attach it with a MEDIA line.\n"
        "- Keep scratch/checkpoint/state files in the workspace unless they are requested deliverables.\n"
        "- If the task says to alert only on new data or meaningful changes, return no output when nothing changed.\n"
        f"- If no output behavior is specified and there is nothing specific to report, send: {DEFAULT_CRON_NO_OUTPUT_MESSAGE}"
    )


def scheduled_task_delivery_text(job: object, text: str, *, run_label: str | None = None) -> str:
    """Format a user-visible scheduled task delivery header."""
    name = str(getattr(job, "name", "") or "Scheduled task").strip() or "Scheduled task"
    label = str(run_label or "Scheduled task").strip() or "Scheduled task"
    body = str(text or "").strip()
    header = f"⏰ {label}: {name}"
    return f"{header}\n\n{body}" if body else header


def configured_delivery_target(channel: str, settings: object | None = None, env: dict[str, str] | None = None) -> str:
    """Return the configured operator target for a supported delivery channel."""
    import os

    env_map = env if env is not None else os.environ
    channel = normalize_cron_delivery_channel(channel)
    if channel == "web":
        return "web:operator"
    if channel == "telegram":
        configured = getattr(getattr(settings, "telegram", None), "operator_chat_id", None)
        if isinstance(configured, str) and configured.strip():
            return configured.strip()
        return str(env_map.get("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "") or "").strip()
    if channel == "slack":
        configured = getattr(getattr(settings, "slack", None), "operator_user_id", None)
        if isinstance(configured, str) and configured.strip():
            return configured.strip()
        return str(env_map.get("NULLION_SLACK_OPERATOR_USER_ID", "") or "").strip()
    if channel == "discord":
        return str(env_map.get("NULLION_DISCORD_OPERATOR_CHANNEL_ID", "") or "").strip()
    return ""


def effective_cron_delivery_channel(
    job: object,
    *,
    settings: object | None = None,
    env: dict[str, str] | None = None,
    fallback_channel: str = "web",
) -> str:
    """Resolve a cron delivery channel from structured metadata.

    Blank legacy jobs prefer Telegram when an operator target is configured,
    otherwise they fall back to web. Explicit supported channels are preserved.
    """
    explicit = normalize_cron_delivery_channel(getattr(job, "delivery_channel", ""))
    if explicit:
        return explicit
    if configured_delivery_target("telegram", settings=settings, env=env):
        return "telegram"
    fallback = normalize_cron_delivery_channel(fallback_channel)
    return fallback or "web"


def cron_delivery_target(
    job: object,
    channel: str,
    *,
    settings: object | None = None,
    env: dict[str, str] | None = None,
) -> str:
    channel = normalize_cron_delivery_channel(channel)
    explicit_target = str(getattr(job, "delivery_target", "") or "").strip()
    if explicit_target and not (channel in MESSAGING_CRON_DELIVERY_CHANNELS and explicit_target.startswith("web:")):
        return explicit_target
    return configured_delivery_target(channel, settings=settings, env=env)


def cron_conversation_id(job: object, channel: str, target: str) -> str:
    if channel in MESSAGING_CRON_DELIVERY_CHANNELS and target:
        return f"{channel}:{target}"
    if channel == "web":
        return target or "web:operator"
    job_id = str(getattr(job, "id", "") or "").strip() or "unknown"
    return f"cron:{job_id}"


def _artifact_path_from_value(value: Any) -> str:
    if isinstance(value, dict):
        candidate = value.get("path")
    else:
        candidate = getattr(value, "path", value)
    if isinstance(candidate, Path):
        return str(candidate)
    if isinstance(candidate, str):
        return candidate.strip()
    return ""


def _artifact_values(artifacts: object) -> tuple[object, ...]:
    if isinstance(artifacts, dict) and "path" in artifacts:
        return (artifacts,)
    if isinstance(artifacts, dict):
        return tuple(artifacts.values())
    if isinstance(artifacts, (list, tuple, set, frozenset)):
        return tuple(artifacts)
    return (artifacts,)


def _cron_text_artifact_content(artifacts: object) -> str:
    for artifact in _artifact_values(artifacts):
        path_text = _artifact_path_from_value(artifact)
        if not path_text:
            continue
        path = Path(path_text).expanduser()
        if path.suffix.lower() != ".txt" or not path.is_file():
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            continue
        if content:
            return content[:MAX_CRON_TEXT_ARTIFACT_CHARS].rstrip()
    return ""


def cron_delivery_text(text: str, artifacts: object = None) -> str:
    """Return the cron's user-visible text without inventing attachments.

    Cron can deliver text, explicit MEDIA attachments, both, or nothing. Artifact
    paths are not automatically appended because scheduled tasks often write
    internal state/checkpoints; agents must make requested deliverables explicit.
    """
    return _cron_text_artifact_content(artifacts) or str(text or "")


def _path_parts(path_text: str) -> tuple[str, ...]:
    try:
        return Path(path_text).expanduser().parts
    except (OSError, RuntimeError, ValueError):
        return ()


def _tool_result_output(result: object) -> dict[str, object]:
    if isinstance(result, dict):
        output = result.get("output")
        return output if isinstance(output, dict) else {}
    output = getattr(result, "output", None)
    return output if isinstance(output, dict) else {}


def _tool_result_name(result: object) -> str:
    if isinstance(result, dict):
        return str(result.get("tool_name") or "")
    return str(getattr(result, "tool_name", "") or "")


def _tool_result_status(result: object) -> str:
    if isinstance(result, dict):
        return str(result.get("status") or "")
    return str(getattr(result, "status", "") or "")


def _artifact_paths_from_value(value: object) -> tuple[str, ...]:
    paths: list[str] = []
    for item in _artifact_values(value):
        path = _artifact_path_from_value(item)
        if path:
            paths.append(path)
    return tuple(dict.fromkeys(paths))


def _workspace_state_filenames(result: dict[str, object]) -> set[str]:
    state_names: set[str] = set()
    for tool_result in result.get("tool_results") or ():
        if _tool_result_name(tool_result) not in {"file_read", "file_write"}:
            continue
        path_text = str(_tool_result_output(tool_result).get("path") or "").strip()
        if not path_text:
            continue
        parts = _path_parts(path_text)
        if "files" in parts and "artifacts" not in parts:
            state_names.add(Path(path_text).name)
    return state_names


def _is_state_artifact_media(path_text: str, state_filenames: set[str]) -> bool:
    if not state_filenames:
        return False
    parts = _path_parts(path_text)
    return "artifacts" in parts and Path(path_text).name in state_filenames


def _structured_tool_artifact_paths(result: dict[str, object], state_filenames: set[str]) -> tuple[str, ...]:
    paths: list[str] = []
    for tool_result in result.get("tool_results") or ():
        if _tool_result_status(tool_result).strip().lower() != "completed":
            continue
        output = _tool_result_output(tool_result)
        # These fields are the typed artifact channel exposed by file-producing
        # tools. Plain `path` fields are intentionally ignored here because many
        # state/checkpoint tools use them for internal workspace files.
        for key in ("artifact_path", "artifact_paths", "artifacts"):
            for path in _artifact_paths_from_value(output.get(key)):
                if _is_state_artifact_media(path, state_filenames):
                    continue
                paths.append(path)
    return tuple(dict.fromkeys(paths))


def _filter_state_media_from_text(text: str, state_filenames: set[str]) -> str:
    if not state_filenames:
        return text
    from nullion.artifacts import parse_media_directive_line

    blocks: list[dict[str, object]] = []
    current_lines: list[str] = []
    current_state_media = False

    def flush_block() -> None:
        nonlocal current_state_media
        if not current_lines and not current_state_media:
            return
        blocks.append({"text": "\n".join(current_lines).strip(), "state_media": current_state_media})
        current_lines.clear()
        current_state_media = False

    for raw_line in str(text or "").splitlines():
        if not raw_line.strip():
            flush_block()
            continue
        directive = parse_media_directive_line(raw_line)
        if directive is not None and _is_state_artifact_media(str(directive.path), state_filenames):
            current_state_media = True
            continue
        current_lines.append(raw_line)
    flush_block()

    if not any(block.get("state_media") for block in blocks):
        return text

    state_media_indexes = [index for index, block in enumerate(blocks) if block.get("state_media")]
    caption_indexes = {
        max((index for index in range(media_index) if str(blocks[index].get("text") or "").strip()), default=-1)
        for media_index in state_media_indexes
    }
    caption_indexes.discard(-1)
    kept: list[str] = []
    for index, block in enumerate(blocks):
        block_text = str(block.get("text") or "").strip()
        if not block_text or block.get("state_media") or index in caption_indexes:
            continue
        kept.append(block_text)
    return "\n\n".join(kept).strip()


def _strip_split_artifact_directives(text: str, deliverable_paths: tuple[str, ...]) -> str:
    if not deliverable_paths:
        return text
    deliverable = {str(Path(path).expanduser()) for path in deliverable_paths}
    lines = str(text or "").splitlines()
    kept: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index].strip()
        following = lines[index + 1].strip().strip("`'\"<>") if index + 1 < len(lines) else ""
        if current in {"MEDIA", "ARTIFACT"} and following and str(Path(following).expanduser()) in deliverable:
            index += 2
            continue
        if current in {"MEDIA", "ARTIFACT"}:
            index += 1
            continue
        kept.append(lines[index])
        index += 1
    return "\n".join(kept).strip()


def _normalize_split_artifact_directives(text: str) -> str:
    from nullion.artifacts import parse_media_directive_line

    lines = str(text or "").splitlines()
    normalized: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index].strip()
        following = lines[index + 1].strip().strip("`'\"<>") if index + 1 < len(lines) else ""
        if current in {"MEDIA", "ARTIFACT"} and following:
            normalized.append(f"{current}:{following}")
            index += 2
            continue
        directive = parse_media_directive_line(lines[index])
        if directive is not None and ":" not in current.split(maxsplit=1)[0]:
            prefix = f"{directive.prefix}\n" if directive.prefix else ""
            normalized.append(f"{prefix}MEDIA:{directive.path}")
            index += 1
            continue
        normalized.append(lines[index])
        index += 1
    return "\n".join(normalized).strip()


def _append_media_directives(text: str, deliverable_paths: tuple[str, ...]) -> str:
    if not deliverable_paths:
        return text
    from nullion.artifacts import parse_media_directive_line

    existing = {
        str(Path(str(directive.path)).expanduser())
        for raw_line in str(text or "").splitlines()
        if (directive := parse_media_directive_line(raw_line)) is not None
    }
    media_lines = [f"MEDIA:{path}" for path in deliverable_paths if str(Path(path).expanduser()) not in existing]
    if not media_lines:
        return text
    parts = [part for part in (str(text or "").strip(), "\n".join(media_lines)) if part]
    return "\n\n".join(parts)


def cron_delivery_text_from_result(result: dict[str, object]) -> str:
    """Return deliverable cron text after filtering state-file-only media.

    The filter uses runtime facts, not prompt wording: a MEDIA path in
    workspace artifacts is suppressed when it mirrors a file accessed through
    the workspace files area during the same cron run. That preserves explicit
    report/export attachments and completed tool artifact fields while
    preventing tracker/checkpoint files from becoming user-facing attachments.
    """
    from nullion.response_fulfillment_contract import user_visible_text_from_output

    text = cron_delivery_text(user_visible_text_from_output(result), result.get("artifacts"))
    text = _normalize_split_artifact_directives(text)
    state_filenames = _workspace_state_filenames(result)
    deliverable_paths = _structured_tool_artifact_paths(result, state_filenames)
    text = _filter_state_media_from_text(text, state_filenames)
    text = _strip_split_artifact_directives(text, deliverable_paths)
    return _append_media_directives(text, deliverable_paths)


def legacy_cron_delivery_text_with_media(text: str, artifacts: object) -> str:
    """Append MEDIA directives from path-like artifacts without assuming a type."""
    media_lines: list[str] = []
    for artifact in _artifact_values(artifacts):
        path = _artifact_path_from_value(artifact)
        if path:
            media_lines.append(f"MEDIA:{path}")
    if not media_lines:
        return text
    return "\n\n".join([text, "\n".join(dict.fromkeys(media_lines))])


@dataclass(frozen=True)
class CronRunDeliveryCallbacks:
    effective_channel: Callable[[object], str]
    delivery_target: Callable[[object, str], str]
    run_agent_turn: Callable[[object, str], dict[str, object]]
    record_event: Callable[..., None]
    block_reason: Callable[[dict[str, object], str, object], str | None]
    save_web_delivery: Callable[[object, str, str, object, dict[str, object]], bool]
    send_platform_delivery: Callable[[object, str, str], bool]
    start_background_delivery: Callable[[str, object], None] | None = None
    clear_background_delivery: Callable[[str], None] | None = None


class _CronRunDeliveryState(TypedDict, total=False):
    job: object
    label: str
    callbacks: CronRunDeliveryCallbacks
    delivery_channel: str
    delivery_target: str
    conversation_id: str
    result: dict[str, object]
    text: str
    artifacts: object
    block_reason: str | None
    send_attempts: int


def _cron_run_resolve_route_node(state: _CronRunDeliveryState) -> dict[str, object]:
    job = state["job"]
    callbacks = state["callbacks"]
    delivery_channel = callbacks.effective_channel(job)
    delivery_target = callbacks.delivery_target(job, delivery_channel)
    conversation_id = cron_conversation_id(job, delivery_channel, delivery_target)
    callbacks.record_event("cron.delivery.started", job, delivery_channel, delivery_target, conversation_id)
    if delivery_channel in MESSAGING_CRON_DELIVERY_CHANNELS and callbacks.start_background_delivery is not None:
        callbacks.start_background_delivery(conversation_id, job)
    return {
        "delivery_channel": delivery_channel,
        "delivery_target": delivery_target,
        "conversation_id": conversation_id,
    }


def _cron_run_agent_node(state: _CronRunDeliveryState) -> dict[str, object]:
    result = state["callbacks"].run_agent_turn(state["job"], state["conversation_id"])
    return {"result": dict(result or {})}


def _cron_run_route_after_agent(state: _CronRunDeliveryState) -> str:
    return "paused" if state.get("result", {}).get("suspended_for_approval") else "prepare"


def _cron_run_paused_node(state: _CronRunDeliveryState) -> dict[str, object]:
    state["callbacks"].record_event(
        "cron.delivery.paused",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
        reason="waiting_for_approval",
    )
    result = dict(state.get("result") or {})
    result["cron_delivery_status"] = "paused_for_approval"
    return {"result": result}


def _cron_run_prepare_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    result = dict(state.get("result") or {})
    artifacts = result.get("artifacts")
    text = cron_delivery_text_from_result(result)
    block_reason = state["callbacks"].block_reason(result, str(text), artifacts)
    return {"result": result, "text": str(text), "artifacts": artifacts, "block_reason": block_reason}


def _cron_run_route_prepared(state: _CronRunDeliveryState) -> str:
    if state.get("block_reason"):
        return "blocked"
    if not str(state.get("text") or "").strip():
        return "silent"
    return "web" if state.get("delivery_channel") == "web" else "messaging"


def _cron_run_blocked_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    if callbacks.clear_background_delivery is not None:
        callbacks.clear_background_delivery(state["conversation_id"])
    result = dict(state.get("result") or {})
    reason = state.get("block_reason") or "cron_delivery_blocked"
    result["cron_delivery_status"] = "failed"
    result["cron_delivery_failed"] = True
    result["cron_run_failed"] = True
    result["reason"] = reason
    callbacks.record_event(
        "cron.delivery.failed",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
        reason=reason,
    )
    return {"result": result}


def _cron_run_silent_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    if callbacks.clear_background_delivery is not None:
        callbacks.clear_background_delivery(state["conversation_id"])
    result = dict(state.get("result") or {})
    result["cron_delivery_status"] = "silent"
    callbacks.record_event(
        "cron.delivery.silent",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
    )
    return {"result": result}


def _cron_run_web_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    callbacks = state["callbacks"]
    result = dict(state.get("result") or {})
    callbacks.save_web_delivery(
        state["job"],
        state["conversation_id"],
        state.get("text") or "",
        state.get("artifacts"),
        result,
    )
    callbacks.record_event(
        "cron.delivery.saved",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
    )
    result["cron_delivery_status"] = "saved"
    return {"result": result}


def _cron_run_messaging_delivery_node(state: _CronRunDeliveryState) -> dict[str, object]:
    from nullion.artifacts import media_candidate_paths_from_text

    callbacks = state["callbacks"]
    result = dict(state.get("result") or {})
    attempts = int(state.get("send_attempts") or 0) + 1
    text = cron_delivery_text(str(state.get("text") or ""), state.get("artifacts"))
    if callbacks.send_platform_delivery(state["job"], state["delivery_channel"], text):
        if callbacks.clear_background_delivery is not None:
            callbacks.clear_background_delivery(state["conversation_id"])
        callbacks.record_event(
            "cron.delivery.sent",
            state["job"],
            state["delivery_channel"],
            state["delivery_target"],
            state["conversation_id"],
        )
        result["cron_delivery_status"] = "sent"
        return {"result": result, "send_attempts": attempts}
    if media_candidate_paths_from_text(text):
        callbacks.record_event(
            "cron.delivery.failed",
            state["job"],
            state["delivery_channel"],
            state["delivery_target"],
            state["conversation_id"],
            reason="attachment delivery failed",
        )
        result["cron_delivery_status"] = "failed"
        result["cron_delivery_failed"] = True
        return {"result": result, "send_attempts": attempts}
    if attempts < 2:
        callbacks.record_event(
            "cron.delivery.retry",
            state["job"],
            state["delivery_channel"],
            state["delivery_target"],
            state["conversation_id"],
            reason="platform delivery failed",
        )
        return {"result": result, "send_attempts": attempts}
    callbacks.record_event(
        "cron.delivery.failed",
        state["job"],
        state["delivery_channel"],
        state["delivery_target"],
        state["conversation_id"],
        reason="missing bot token or target",
    )
    result["cron_delivery_status"] = "failed"
    result["cron_delivery_failed"] = True
    return {"result": result, "send_attempts": attempts}


def _cron_run_route_after_messaging(state: _CronRunDeliveryState) -> str:
    result = state.get("result") or {}
    if result.get("cron_delivery_status") in {"sent", "failed"}:
        return END
    if int(state.get("send_attempts") or 0) < 2:
        return "retry"
    return END


@lru_cache(maxsize=1)
def _compiled_cron_run_delivery_graph():
    graph = StateGraph(_CronRunDeliveryState)
    graph.add_node("resolve_route", _cron_run_resolve_route_node)
    graph.add_node("run_agent", _cron_run_agent_node)
    graph.add_node("paused", _cron_run_paused_node)
    graph.add_node("prepare", _cron_run_prepare_delivery_node)
    graph.add_node("blocked", _cron_run_blocked_node)
    graph.add_node("silent", _cron_run_silent_node)
    graph.add_node("web", _cron_run_web_delivery_node)
    graph.add_node("messaging", _cron_run_messaging_delivery_node)
    graph.add_edge(START, "resolve_route")
    graph.add_edge("resolve_route", "run_agent")
    graph.add_conditional_edges("run_agent", _cron_run_route_after_agent, {"paused": "paused", "prepare": "prepare"})
    graph.add_conditional_edges(
        "prepare",
        _cron_run_route_prepared,
        {"blocked": "blocked", "silent": "silent", "web": "web", "messaging": "messaging"},
    )
    graph.add_conditional_edges("messaging", _cron_run_route_after_messaging, {"retry": "messaging", END: END})
    for node in ("paused", "blocked", "silent", "web"):
        graph.add_edge(node, END)
    return graph.compile()


def run_cron_delivery_workflow(
    job: object,
    *,
    label: str,
    callbacks: CronRunDeliveryCallbacks,
) -> dict[str, object]:
    final_state = _compiled_cron_run_delivery_graph().invoke(
        {"job": job, "label": label, "callbacks": callbacks, "result": {}}
    )
    result = final_state.get("result")
    return dict(result or {})


__all__ = [
    "CronRunDeliveryCallbacks",
    "MESSAGING_CRON_DELIVERY_CHANNELS",
    "SUPPORTED_CRON_DELIVERY_CHANNELS",
    "configured_delivery_target",
    "cron_agent_prompt",
    "cron_conversation_id",
    "cron_delivery_target",
    "cron_delivery_text",
    "cron_delivery_text_from_result",
    "effective_cron_delivery_channel",
    "normalize_cron_delivery_channel",
    "run_cron_delivery_workflow",
    "scheduled_task_delivery_text",
]
