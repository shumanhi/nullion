"""Health alert primitives for Project Nullion."""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4


class HealthIssueType(str, Enum):
    ERROR = "error"
    ISSUE = "issue"
    TIMEOUT = "timeout"
    STALLED = "stalled"
    DEGRADED = "degraded"


@dataclass(slots=True)
class HealthAlert:
    alert_id: str
    issue_type: HealthIssueType
    source: str
    message: str
    created_at: datetime
    details: dict[str, Any]


def make_health_alert(
    issue_type: HealthIssueType,
    source: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> HealthAlert:
    return HealthAlert(
        alert_id=uuid4().hex,
        issue_type=issue_type,
        source=source,
        message=message,
        created_at=datetime.now(timezone.utc),
        details=dict(details) if details is not None else {},
    )


def doctor_should_receive(alert: HealthAlert) -> bool:
    del alert
    return True
