from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, tzinfo


@dataclass(slots=True)
class ReminderRecord:
    task_id: str
    chat_id: str
    text: str
    due_at: datetime
    delivered_at: datetime | None = None


_CURRENT_REMINDER_CHAT_ID: ContextVar[str | None] = ContextVar("current_reminder_chat_id", default=None)


@contextmanager
def reminder_chat_context(chat_id: str | None):
    token = _CURRENT_REMINDER_CHAT_ID.set(str(chat_id).strip() if chat_id else None)
    try:
        yield
    finally:
        _CURRENT_REMINDER_CHAT_ID.reset(token)


def current_reminder_chat_id() -> str | None:
    return _CURRENT_REMINDER_CHAT_ID.get()


def reminder_timezone(timezone_name: str | None = None) -> tzinfo:
    """Return the timezone reminders should use for user-facing times."""
    try:
        from nullion.preferences import load_preferences, resolve_timezone

        return resolve_timezone(timezone_name or load_preferences().timezone)
    except Exception:
        return UTC


def normalize_reminder_due_at(due_at: datetime, *, timezone_name: str | None = None) -> datetime:
    """Normalize reminder due times to UTC, treating naive values as user-local."""
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=reminder_timezone(timezone_name))
    return due_at.astimezone(UTC)


def due_at_from_relative_seconds(seconds: int | float, *, now: datetime | None = None) -> datetime:
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return (current.astimezone(UTC) + timedelta(seconds=float(seconds))).astimezone(UTC)


def format_reminder_due_at(due_at: datetime | None, *, timezone_name: str | None = None) -> str:
    """Format a reminder due time in the user's configured timezone."""
    if due_at is None:
        return "scheduled"
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=UTC)
    local_due_at = due_at.astimezone(reminder_timezone(timezone_name))
    hour = local_due_at.strftime("%I").lstrip("0") or "12"
    return f"{local_due_at.day} {local_due_at.strftime('%b')} at {hour}:{local_due_at:%M} {local_due_at:%p}"


def reminder_due_at_output(due_at: datetime, *, timezone_name: str | None = None) -> dict[str, str]:
    normalized = normalize_reminder_due_at(due_at, timezone_name=timezone_name)
    local_due_at = normalized.astimezone(reminder_timezone(timezone_name))
    return {
        "due_at": normalized.isoformat(),
        "due_at_local": local_due_at.isoformat(),
        "timezone": getattr(local_due_at.tzinfo, "key", str(local_due_at.tzinfo)),
        "due_at_display": format_reminder_due_at(normalized, timezone_name=timezone_name),
    }


__all__ = [
    "ReminderRecord",
    "current_reminder_chat_id",
    "due_at_from_relative_seconds",
    "format_reminder_due_at",
    "normalize_reminder_due_at",
    "reminder_chat_context",
    "reminder_due_at_output",
    "reminder_timezone",
]
