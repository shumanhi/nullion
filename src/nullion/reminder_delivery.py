from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from functools import lru_cache
import logging
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.config import NullionSettings, load_settings
from nullion.connections import workspace_id_for_principal
from nullion.scheduler import disable_task, mark_task_ran
from nullion.users import MessagingDeliveryTarget, messaging_delivery_targets_for_workspace, resolve_telegram_user


logger = logging.getLogger(__name__)
ReminderSend = Callable[[str, str], Awaitable[bool]]


def _settings_for_reminder_delivery(settings: NullionSettings | None) -> NullionSettings | None:
    if settings is not None:
        return settings
    try:
        return load_settings()
    except Exception:
        logger.debug("Could not load settings for workspace reminder fanout", exc_info=True)
        return None


def _target_key_for_chat_id(chat_id: object) -> str:
    text = str(chat_id or "").strip()
    if not text:
        return ""
    if ":" in text:
        channel, target_id = text.split(":", 1)
        return f"{channel.strip().lower()}:{target_id.strip()}"
    return f"telegram:{text}"


def _target_key(target: MessagingDeliveryTarget) -> str:
    return f"{target.channel}:{target.target_id}"


def _workspace_id_for_reminder_chat_id(chat_id: object, settings: NullionSettings | None) -> str:
    text = str(chat_id or "").strip()
    if not text or ":" in text:
        return workspace_id_for_principal(text)
    try:
        user = resolve_telegram_user(text, settings)
        return user.workspace_id or "workspace_admin"
    except Exception:
        logger.debug("Could not resolve Telegram reminder workspace for chat_id=%s", text, exc_info=True)
        return workspace_id_for_principal(text)


def _target_has_delivery_credentials(target: MessagingDeliveryTarget, settings: NullionSettings | None) -> bool:
    if settings is None:
        return False
    if target.channel == "telegram":
        return bool(getattr(getattr(settings, "telegram", None), "bot_token", None))
    if target.channel == "slack":
        return bool(getattr(getattr(settings, "slack", None), "bot_token", None))
    if target.channel == "discord":
        return bool(getattr(getattr(settings, "discord", None), "bot_token", None))
    return False


async def _send_workspace_reminder_target(
    target: MessagingDeliveryTarget,
    text: str,
    *,
    settings: NullionSettings | None,
) -> bool:
    if settings is None:
        return False
    if target.channel == "telegram":
        bot_token = getattr(getattr(settings, "telegram", None), "bot_token", None)
        if not bot_token:
            return False
        from nullion.telegram_entrypoint import _send_operator_telegram_delivery

        return await _send_operator_telegram_delivery(
            bot_token,
            target.target_id,
            text,
            principal_id=target.principal_id,
            suppress_link_preview=True,
        )
    if target.channel == "slack":
        bot_token = getattr(getattr(settings, "slack", None), "bot_token", None)
        if not bot_token:
            return False
        from nullion.slack_app import send_slack_platform_delivery

        return await send_slack_platform_delivery(
            bot_token=bot_token,
            channel=target.target_id,
            text=text,
            principal_id=target.principal_id,
        )
    if target.channel == "discord":
        bot_token = getattr(getattr(settings, "discord", None), "bot_token", None)
        if not bot_token:
            return False
        from nullion.discord_app import send_discord_platform_delivery

        return await send_discord_platform_delivery(
            bot_token=bot_token,
            channel_id=target.target_id,
            text=text,
            principal_id=target.principal_id,
        )
    return False


async def _deliver_reminder(
    reminder,
    text: str,
    *,
    send: ReminderSend,
    settings: NullionSettings | None,
) -> bool:
    resolved_settings = _settings_for_reminder_delivery(settings)
    workspace_id = _workspace_id_for_reminder_chat_id(getattr(reminder, "chat_id", None), resolved_settings)
    targets = tuple(
        target
        for target in messaging_delivery_targets_for_workspace(workspace_id, settings=resolved_settings)
        if _target_has_delivery_credentials(target, resolved_settings)
    )
    delivered_targets: set[str] = set()
    target_keys = {_target_key(target) for target in targets}

    for target in targets:
        try:
            if await _send_workspace_reminder_target(target, text, settings=resolved_settings):
                delivered_targets.add(_target_key(target))
        except Exception:
            logger.warning("Workspace reminder delivery failed for %s", target.channel, exc_info=True)

    origin_key = _target_key_for_chat_id(getattr(reminder, "chat_id", None))
    if origin_key and origin_key not in delivered_targets:
        try:
            if await send(getattr(reminder, "chat_id", ""), text):
                delivered_targets.add(origin_key)
        except Exception:
            logger.warning(
                "Reminder delivery failed (chat_id=%s, task_id=%s)",
                getattr(reminder, "chat_id", None),
                getattr(reminder, "task_id", None),
                exc_info=True,
            )

    if target_keys:
        return target_keys.issubset(delivered_targets)
    return origin_key in delivered_targets


async def deliver_due_reminders_once(
    runtime,
    *,
    send: ReminderSend,
    now: datetime | None = None,
    settings: NullionSettings | None = None,
) -> int:
    final_state = await _compiled_due_reminder_delivery_graph().ainvoke(
        {
            "runtime": runtime,
            "send": send,
            "settings": settings,
            "now": now,
            "due_time": None,
            "due_tasks": [],
            "task_index": 0,
            "delivered": 0,
            "task": None,
            "reminder": None,
            "sent": False,
        },
        config={"recursion_limit": 10000},
    )
    return int(final_state.get("delivered") or 0)


class _DueReminderDeliveryState(TypedDict, total=False):
    runtime: Any
    send: ReminderSend
    settings: NullionSettings | None
    now: datetime | None
    due_time: datetime | None
    due_tasks: list[Any]
    task_index: int
    delivered: int
    task: Any | None
    reminder: Any | None
    sent: bool


def _normalize_due_time(value: datetime | None) -> datetime:
    due_time = value or datetime.now(UTC)
    if due_time.tzinfo is None:
        due_time = due_time.replace(tzinfo=UTC)
    return due_time.astimezone(UTC)


async def _due_reminder_load_tasks_node(state: _DueReminderDeliveryState) -> dict[str, object]:
    due_time = _normalize_due_time(state.get("now"))
    due_tasks = await asyncio.to_thread(state["runtime"].run_due_scheduled_tasks, now=due_time)
    return {"due_time": due_time, "due_tasks": list(due_tasks or ()), "task_index": 0, "delivered": 0}


def _due_reminder_select_task_node(state: _DueReminderDeliveryState) -> dict[str, object]:
    due_tasks = list(state.get("due_tasks") or [])
    task_index = int(state.get("task_index") or 0)
    if task_index >= len(due_tasks):
        return {"task": None, "reminder": None, "sent": False}
    task = due_tasks[task_index]
    reminder = state["runtime"].store.get_reminder(task.task_id)
    if reminder is None or reminder.delivered_at is not None:
        return {"task": task, "reminder": None, "sent": False}
    return {"task": task, "reminder": reminder, "sent": False}


def _due_reminder_route_selected(state: _DueReminderDeliveryState) -> str:
    if state.get("task") is None:
        return END
    if state.get("reminder") is None:
        return "advance"
    return "deliver"


async def _due_reminder_deliver_node(state: _DueReminderDeliveryState) -> dict[str, object]:
    reminder = state.get("reminder")
    if reminder is None:
        return {"sent": False}
    sent = await _deliver_reminder(
        reminder,
        f"⏰ Reminder: {reminder.text}",
        send=state["send"],
        settings=state.get("settings"),
    )
    return {"sent": bool(sent)}


def _due_reminder_mark_delivered_node(state: _DueReminderDeliveryState) -> dict[str, object]:
    if not state.get("sent"):
        return {}
    task = state.get("task")
    reminder = state.get("reminder")
    due_time = state.get("due_time")
    if task is None or reminder is None or due_time is None:
        return {}
    runtime = state["runtime"]
    runtime.store.add_reminder(replace(reminder, delivered_at=due_time))
    runtime.store.add_scheduled_task(disable_task(mark_task_ran(task, due_time)))
    runtime.checkpoint()
    return {"delivered": int(state.get("delivered") or 0) + 1}


def _due_reminder_advance_node(state: _DueReminderDeliveryState) -> dict[str, object]:
    return {"task_index": int(state.get("task_index") or 0) + 1, "task": None, "reminder": None, "sent": False}


@lru_cache(maxsize=1)
def _compiled_due_reminder_delivery_graph():
    graph = StateGraph(_DueReminderDeliveryState)
    graph.add_node("load_tasks", _due_reminder_load_tasks_node)
    graph.add_node("select_task", _due_reminder_select_task_node)
    graph.add_node("deliver", _due_reminder_deliver_node)
    graph.add_node("mark_delivered", _due_reminder_mark_delivered_node)
    graph.add_node("advance", _due_reminder_advance_node)
    graph.add_edge(START, "load_tasks")
    graph.add_edge("load_tasks", "select_task")
    graph.add_conditional_edges("select_task", _due_reminder_route_selected, {"deliver": "deliver", "advance": "advance", END: END})
    graph.add_edge("deliver", "mark_delivered")
    graph.add_edge("mark_delivered", "advance")
    graph.add_edge("advance", "select_task")
    return graph.compile()


async def run_reminder_delivery_loop(
    runtime,
    *,
    send: ReminderSend,
    interval_seconds: float = 15.0,
    settings: NullionSettings | None = None,
) -> None:
    while True:
        try:
            await deliver_due_reminders_once(runtime, send=send, settings=settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Reminder delivery loop failed; will retry.", exc_info=True)
        await asyncio.sleep(interval_seconds)


__all__ = ["deliver_due_reminders_once", "run_reminder_delivery_loop"]
