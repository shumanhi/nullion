"""Typed kernel scaffold for conversation runtime state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Mapping


class ConversationTurnDisposition(str, Enum):
    CHATTER = "chatter"
    INDEPENDENT = "independent"
    CONTINUE = "continue"
    REVISE = "revise"
    INTERRUPT = "interrupt"
    BACKGROUND_FOLLOW_UP = "background_follow_up"


class ConversationBranchStatus(str, Enum):
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


class WorkerResultKind(str, Enum):
    REPLY_CANDIDATE = "reply_candidate"
    SUPPORTING_EVIDENCE = "supporting_evidence"
    ARTIFACT = "artifact"
    OBSOLETE = "obsolete"


@dataclass(slots=True)
class ConversationEnvelope:
    conversation_id: str
    message_id: str
    request_id: str
    turn_id: str
    branch_id: str
    parent_turn_id: str | None
    received_at: datetime
    user_message: str
    chat_id: str | None = None


@dataclass(slots=True)
class ConversationBranch:
    branch_id: str
    conversation_id: str
    status: ConversationBranchStatus
    created_from_turn_id: str
    superseded_by_branch_id: str | None = None
    cancelled_at: datetime | None = None


@dataclass(slots=True)
class ConversationTurn:
    turn_id: str
    conversation_id: str
    branch_id: str
    parent_turn_id: str | None
    disposition: ConversationTurnDisposition
    user_message: str
    status: str
    created_at: datetime
    disposition_reason: str | None = None
    started_snapshot_summary: str | None = None
    cancellation_token: str | None = None


@dataclass(slots=True, frozen=True)
class WorkerResultEnvelope:
    result_id: str
    conversation_id: str
    branch_id: str
    turn_id: str
    task_id: str
    kind: WorkerResultKind
    idempotency_key: str
    payload: Mapping[str, Any]
    created_at: datetime


__all__ = [
    "ConversationTurnDisposition",
    "ConversationBranchStatus",
    "WorkerResultKind",
    "ConversationEnvelope",
    "ConversationBranch",
    "ConversationTurn",
    "WorkerResultEnvelope",
]
