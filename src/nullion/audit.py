"""Audit log foundation for Project Nullion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from nullion.redaction import redact_value


@dataclass(slots=True)
class AuditRecord:
    record_id: str
    action: str
    actor: str
    created_at: datetime
    details: dict


class AuditLog:
    def __init__(self) -> None:
        self._records: list[AuditRecord] = []

    def append(self, record: AuditRecord) -> None:
        self._records.append(record)

    def list_records(self) -> list[AuditRecord]:
        return list(self._records)

    def filter_by_actor(self, actor: str) -> list[AuditRecord]:
        return [record for record in self._records if record.actor == actor]


def make_audit_record(action: str, actor: str, details: dict | None = None) -> AuditRecord:
    return AuditRecord(
        record_id=uuid4().hex,
        action=action,
        actor=actor,
        created_at=datetime.now(UTC),
        details={} if details is None else redact_value(dict(details)),
    )
