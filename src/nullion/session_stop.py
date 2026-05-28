"""Shared cancellation helpers for per-session chat work."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import logging
from typing import Any

from nullion.task_frames import TaskFrameStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SessionStopResult:
    cancelled_turn_ids: tuple[str, ...] = ()
    cancelled_task_count: int = 0
    cancelled_task_frame: bool = False
    cancelled_activity_count: int = 0
    cancelled_background_count: int = 0

    @property
    def cancelled_count(self) -> int:
        explicit_count = len(self.cancelled_turn_ids) + self.cancelled_task_count + self.cancelled_background_count
        if explicit_count:
            return explicit_count
        if self.cancelled_activity_count:
            return self.cancelled_activity_count
        return 1 if self.cancelled_task_frame else 0


def stop_session_reply(result: SessionStopResult | int) -> str:
    cancelled_count = result if isinstance(result, int) else result.cancelled_count
    if cancelled_count <= 0:
        return "No active tasks are running in this session."
    return f"Stopped {cancelled_count} active task{'s' if cancelled_count != 1 else ''} in this session."


def cancel_active_task_frame(runtime: object, conversation_id: str | None) -> bool:
    conversation_key = str(conversation_id or "").strip()
    store = getattr(runtime, "store", None)
    if store is None or not conversation_key:
        return False
    frame_id = getattr(store, "get_active_task_frame_id", lambda _conversation_id: None)(conversation_key)
    if not isinstance(frame_id, str) or not frame_id:
        return False
    frame = getattr(store, "get_task_frame", lambda _frame_id: None)(frame_id)
    if frame is None:
        return False
    status = getattr(frame, "status", None)
    if status in {
        TaskFrameStatus.COMPLETED,
        TaskFrameStatus.FAILED,
        TaskFrameStatus.CANCELLED,
        TaskFrameStatus.SUPERSEDED,
    }:
        return False
    try:
        updated = replace(frame, status=TaskFrameStatus.CANCELLED, updated_at=datetime.now(UTC))
        store.add_task_frame(updated)
        store.set_active_task_frame_id(conversation_key, None)
        checkpoint = getattr(runtime, "checkpoint", None)
        if callable(checkpoint):
            checkpoint()
        return True
    except Exception:
        logger.debug("Unable to mark active task frame cancelled", exc_info=True)
        return False


async def cancel_orchestrator_conversation(agent_orchestrator: object | None, conversation_id: str | None) -> int:
    conversation_key = str(conversation_id or "").strip()
    if agent_orchestrator is None or not conversation_key:
        return 0
    cancel = getattr(agent_orchestrator, "cancel_conversation", None)
    if not callable(cancel):
        return 0
    try:
        result = cancel(conversation_key)
        if asyncio.iscoroutine(result):
            result = await result
        return max(0, int(result or 0))
    except Exception:
        logger.debug("Unable to cancel orchestrator conversation tasks", exc_info=True)
        return 0


def cancel_orchestrator_conversation_sync(
    agent_orchestrator: object | None,
    conversation_id: str | None,
    *,
    timeout_s: float = 3.0,
) -> int:
    conversation_key = str(conversation_id or "").strip()
    if agent_orchestrator is None or not conversation_key:
        return 0
    cancel_sync = getattr(agent_orchestrator, "cancel_conversation_sync", None)
    if callable(cancel_sync):
        try:
            return max(0, int(cancel_sync(conversation_key, timeout_s=timeout_s) or 0))
        except Exception:
            logger.debug("Unable to cancel orchestrator conversation tasks synchronously", exc_info=True)
            return 0
    cancel = getattr(agent_orchestrator, "cancel_conversation", None)
    if not callable(cancel):
        return 0
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        try:
            return max(0, int(asyncio.run(cancel_orchestrator_conversation(agent_orchestrator, conversation_key)) or 0))
        except Exception:
            logger.debug("Unable to cancel orchestrator conversation tasks", exc_info=True)
    return 0


async def stop_session_async(
    *,
    conversation_id: str,
    runtime: object | None = None,
    agent_orchestrator: object | None = None,
    turn_tracker: object | None = None,
) -> SessionStopResult:
    cancelled_turn_ids: tuple[str, ...] = ()
    cancel_conversation = getattr(turn_tracker, "cancel_conversation", None)
    if callable(cancel_conversation):
        try:
            result = cancel_conversation(conversation_id)
            if asyncio.iscoroutine(result):
                result = await result
            cancelled_turn_ids = tuple(str(turn_id) for turn_id in (result or ()) if str(turn_id).strip())
        except Exception:
            logger.debug("Unable to cancel active turn tracker tasks", exc_info=True)
    cancelled_task_count = await cancel_orchestrator_conversation(agent_orchestrator, conversation_id)
    cancelled_background_count = cancel_manual_cron_background_runs_for_conversation(conversation_id)
    cancelled_task_frame = cancel_active_task_frame(runtime, conversation_id)
    return SessionStopResult(
        cancelled_turn_ids=cancelled_turn_ids,
        cancelled_task_count=cancelled_task_count,
        cancelled_background_count=cancelled_background_count,
        cancelled_task_frame=cancelled_task_frame,
    )


def cancel_manual_cron_background_runs_for_conversation(conversation_id: str | None) -> int:
    try:
        from nullion.cron_delivery import cancel_manual_cron_background_runs
    except Exception:
        logger.debug("Unable to import manual cron background cancellation helper", exc_info=True)
        return 0
    try:
        return max(0, int(cancel_manual_cron_background_runs(conversation_id) or 0))
    except Exception:
        logger.debug("Unable to cancel manual cron background runs", exc_info=True)
        return 0


__all__ = [
    "SessionStopResult",
    "cancel_active_task_frame",
    "cancel_manual_cron_background_runs_for_conversation",
    "cancel_orchestrator_conversation",
    "cancel_orchestrator_conversation_sync",
    "stop_session_async",
    "stop_session_reply",
]
