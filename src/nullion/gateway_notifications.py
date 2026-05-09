"""Channel-agnostic gateway lifecycle notifications."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import lru_cache
import json
import logging
import os
from pathlib import Path
import threading
import urllib.parse
import urllib.request
import uuid
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.config import NullionSettings, _read_env_file, load_settings
from nullion.users import load_user_registry

logger = logging.getLogger(__name__)

_STATE_DIR = Path.home() / ".nullion"
_EVENTS_PATH: Path | None = None
_RESTART_MARKER_PATH: Path | None = None
_MAX_EVENTS = 50


@dataclass(slots=True, frozen=True)
class GatewayLifecycleEvent:
    event_id: str
    kind: str
    text: str
    created_at: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _resolve_state_dir() -> Path:
    nullion_home = os.environ.get("NULLION_HOME")
    if nullion_home:
        return Path(nullion_home).expanduser()
    env_file = os.environ.get("NULLION_ENV_FILE")
    if env_file:
        return Path(env_file).expanduser().parent
    return _STATE_DIR


def _events_path() -> Path:
    return _EVENTS_PATH or (_resolve_state_dir() / "gateway-events.json")


def _restart_marker_path() -> Path:
    return _RESTART_MARKER_PATH or (_resolve_state_dir() / "gateway-restart.json")


def record_gateway_lifecycle_event(kind: str, text: str) -> GatewayLifecycleEvent:
    state_dir = _resolve_state_dir()
    state_dir.mkdir(parents=True, exist_ok=True)
    events_path = _events_path()
    event = GatewayLifecycleEvent(
        event_id=uuid.uuid4().hex,
        kind=kind,
        text=text,
        created_at=_now(),
    )
    events = _read_json_list(events_path)
    events.append(event.to_dict())
    events_path.write_text(
        json.dumps(events[-_MAX_EVENTS:], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return event


def list_gateway_lifecycle_events(*, since_id: str | None = None) -> list[GatewayLifecycleEvent]:
    events = [
        GatewayLifecycleEvent(
            event_id=str(item.get("event_id") or ""),
            kind=str(item.get("kind") or ""),
            text=str(item.get("text") or ""),
            created_at=str(item.get("created_at") or ""),
        )
        for item in _read_json_list(_events_path())
        if isinstance(item, dict)
    ]
    if not since_id:
        return events
    for index, event in enumerate(events):
        if event.event_id == since_id:
            return events[index + 1 :]
    return events[-5:]


def _telegram_chat_ids(settings: NullionSettings) -> tuple[str, ...]:
    ids: list[str] = []
    if settings.telegram.operator_chat_id:
        ids.append(str(settings.telegram.operator_chat_id).strip())
    try:
        registry = load_user_registry(settings=settings)
        for user in registry.users:
            if user.active and user.telegram_chat_id:
                ids.append(str(user.telegram_chat_id).strip())
    except Exception:
        logger.debug("Could not load user registry for gateway notifications", exc_info=True)
    return tuple(dict.fromkeys(chat_id for chat_id in ids if chat_id))


def _send_telegram_message(bot_token: str, chat_id: str, text: str) -> None:
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data=data,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        response.read()


def _load_notification_settings() -> NullionSettings:
    env_file = os.environ.get("NULLION_ENV_FILE")
    env_path = Path(env_file).expanduser() if env_file else (_STATE_DIR / ".env")
    if env_path.exists():
        return load_settings(env=_read_env_file(env_path))
    return load_settings(env=os.environ)


def _send_gateway_telegram_messages(bot_token: str, chat_ids: tuple[str, ...], text: str) -> None:
    for chat_id in chat_ids:
        try:
            _send_telegram_message(bot_token, chat_id, text)
        except Exception:
            logger.warning("Failed to deliver gateway notification to Telegram chat %s", chat_id, exc_info=True)


class _GatewayNotificationState(TypedDict, total=False):
    text: str
    settings: NullionSettings | None
    async_delivery: bool
    bot_token: str
    chat_ids: tuple[str, ...]
    skip_reason: str


def _gateway_notification_settings_node(state: _GatewayNotificationState) -> dict[str, object]:
    settings = state.get("settings")
    if settings is not None:
        return {"settings": settings, "skip_reason": ""}
    try:
        return {"settings": _load_notification_settings(), "skip_reason": ""}
    except Exception:
        logger.warning("Could not load settings for gateway notification", exc_info=True)
        return {"skip_reason": "settings_unavailable"}


def _gateway_notification_route_settings(state: _GatewayNotificationState) -> str:
    return END if state.get("skip_reason") else "targets"


def _gateway_notification_targets_node(state: _GatewayNotificationState) -> dict[str, object]:
    settings = state.get("settings")
    if settings is None:
        return {"skip_reason": "settings_unavailable"}
    bot_token = settings.telegram.bot_token
    if not bot_token or not settings.telegram.chat_enabled:
        return {"skip_reason": "telegram_disabled"}
    chat_ids = _telegram_chat_ids(settings)
    if not chat_ids:
        return {"skip_reason": "no_telegram_targets"}
    return {"bot_token": bot_token, "chat_ids": chat_ids, "skip_reason": ""}


def _gateway_notification_route_targets(state: _GatewayNotificationState) -> str:
    return END if state.get("skip_reason") else "deliver"


def _gateway_notification_deliver_node(state: _GatewayNotificationState) -> dict[str, object]:
    bot_token = str(state.get("bot_token") or "")
    chat_ids = tuple(state.get("chat_ids") or ())
    text = str(state.get("text") or "")
    if not bot_token or not chat_ids:
        return {}
    if state.get("async_delivery", True):
        threading.Thread(target=_send_gateway_telegram_messages, args=(bot_token, chat_ids, text), daemon=True).start()
    else:
        _send_gateway_telegram_messages(bot_token, chat_ids, text)
    return {}


@lru_cache(maxsize=1)
def _compiled_gateway_notification_graph():
    graph = StateGraph(_GatewayNotificationState)
    graph.add_node("settings", _gateway_notification_settings_node)
    graph.add_node("targets", _gateway_notification_targets_node)
    graph.add_node("deliver", _gateway_notification_deliver_node)
    graph.add_edge(START, "settings")
    graph.add_conditional_edges("settings", _gateway_notification_route_settings, {"targets": "targets", END: END})
    graph.add_conditional_edges("targets", _gateway_notification_route_targets, {"deliver": "deliver", END: END})
    graph.add_edge("deliver", END)
    return graph.compile()


def notify_telegram_gateway_event(
    text: str,
    *,
    settings: NullionSettings | None = None,
    async_delivery: bool = True,
) -> None:
    _compiled_gateway_notification_graph().invoke(
        {"text": text, "settings": settings, "async_delivery": async_delivery},
        config={"configurable": {"thread_id": "gateway-notification"}},
    )


def begin_gateway_restart(
    *,
    settings: NullionSettings | None = None,
    async_delivery: bool = True,
) -> GatewayLifecycleEvent:
    text = "🟡 Nulliøn gateway is restarting. Chat may pause for a moment."
    event = record_gateway_lifecycle_event("restarting", text)
    state_dir = _resolve_state_dir()
    state_dir.mkdir(parents=True, exist_ok=True)
    _restart_marker_path().write_text(
        json.dumps({"event_id": event.event_id, "created_at": event.created_at}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    notify_telegram_gateway_event(text, settings=settings, async_delivery=async_delivery)
    return event


def complete_gateway_restart_if_needed(*, settings: NullionSettings | None = None) -> GatewayLifecycleEvent | None:
    marker_path = _restart_marker_path()
    if not marker_path.exists():
        return None
    try:
        marker_path.unlink()
    except Exception:
        logger.debug("Could not clear gateway restart marker", exc_info=True)
    text = "🟢 Nulliøn gateway is back online."
    event = record_gateway_lifecycle_event("online", text)
    notify_telegram_gateway_event(text, settings=settings)
    return event
