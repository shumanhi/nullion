from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class SuspendedTurn:
    approval_id: str
    conversation_id: str
    chat_id: str | None
    message: str
    request_id: str | None
    message_id: str | None
    created_at: datetime
    mission_id: str | None = None
    pending_step_idx: int | None = None
    messages_snapshot: list[dict[str, Any]] | None = None
    pending_tool_calls: list[dict[str, Any]] | None = None
    task_id: str | None = None
    group_id: str | None = None
    agent_id: str | None = None
    resume_token: dict[str, Any] | None = None


__all__ = ["SuspendedTurn"]
