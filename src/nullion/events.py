"""Event primitives for Project Nullion."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from nullion.redaction import redact_value


@dataclass(slots=True)
class Event:
    event_id: str
    event_type: str
    actor: str
    created_at: datetime
    payload: dict[str, object] = field(default_factory=dict)


def make_event(
    event_type: str,
    actor: str,
    payload: dict[str, object] | None = None,
) -> Event:
    return Event(
        event_id=uuid4().hex,
        event_type=event_type,
        actor=actor,
        created_at=datetime.now(timezone.utc),
        payload={} if payload is None else redact_value(dict(payload)),
    )
