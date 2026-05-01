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


def normalize_cron_delivery_channel(channel: object) -> str:
    normalized = str(channel or "").strip().lower()
    return normalized if normalized in SUPPORTED_CRON_DELIVERY_CHANNELS else ""


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


def cron_delivery_text(text: str, artifacts: object) -> str:
    """Append MEDIA directives from path-like artifacts without assuming a type."""
    media_lines: list[str] = []
    if isinstance(artifacts, dict) and "path" in artifacts:
        artifact_values = (artifacts,)
    elif isinstance(artifacts, dict):
        artifact_values = artifacts.values()
    elif isinstance(artifacts, (list, tuple, set, frozenset)):
        artifact_values = artifacts
    else:
        artifact_values = (artifacts,)
    for artifact in artifact_values:
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
    from nullion.response_fulfillment_contract import guaranteed_user_visible_text

    result = dict(state.get("result") or {})
    text = guaranteed_user_visible_text(subject=state["job"], output=result, kind="cron")
    artifacts = result.get("artifacts")
    block_reason = state["callbacks"].block_reason(result, str(text), artifacts)
    return {"result": result, "text": str(text), "artifacts": artifacts, "block_reason": block_reason}


def _cron_run_route_prepared(state: _CronRunDeliveryState) -> str:
    if state.get("block_reason"):
        return "blocked"
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
    graph.add_node("web", _cron_run_web_delivery_node)
    graph.add_node("messaging", _cron_run_messaging_delivery_node)
    graph.add_edge(START, "resolve_route")
    graph.add_edge("resolve_route", "run_agent")
    graph.add_conditional_edges("run_agent", _cron_run_route_after_agent, {"paused": "paused", "prepare": "prepare"})
    graph.add_conditional_edges("prepare", _cron_run_route_prepared, {"blocked": "blocked", "web": "web", "messaging": "messaging"})
    graph.add_conditional_edges("messaging", _cron_run_route_after_messaging, {"retry": "messaging", END: END})
    for node in ("paused", "blocked", "web"):
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
    "cron_conversation_id",
    "cron_delivery_target",
    "cron_delivery_text",
    "effective_cron_delivery_channel",
    "normalize_cron_delivery_channel",
    "run_cron_delivery_workflow",
]
