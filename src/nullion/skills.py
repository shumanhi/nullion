"""Structured reusable procedures/playbooks for Project Nullion."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


SKILL_WRITE_DELETE_CONSENT_STEP = (
    "Before modifying, deleting, sending, or writing external account data or existing local data, "
    "ask the user for explicit confirmation in chat and wait for that confirmation; accepting or "
    "reusing this skill is not consent."
)


def ensure_skill_write_delete_consent_step(steps: list[str]) -> list[str]:
    normalized_guard = SKILL_WRITE_DELETE_CONSENT_STEP.casefold()
    cleaned = [str(step).strip() for step in steps if str(step).strip()]
    if any(step.casefold() == normalized_guard for step in cleaned):
        return cleaned
    return [*cleaned, SKILL_WRITE_DELETE_CONSENT_STEP]


@dataclass(slots=True)
class SkillRevision:
    revision: int
    title: str
    summary: str
    trigger: str
    steps: list[str]
    tags: list[str] = field(default_factory=list)
    updated_at: datetime | None = None


@dataclass(slots=True)
class SkillWorkflowSignal:
    source: str
    summary: str
    recorded_at: datetime | None = None


@dataclass(slots=True)
class SkillRecord:
    skill_id: str
    title: str
    summary: str
    trigger: str
    steps: list[str]
    tags: list[str] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revision: int = 1
    revision_history: list[SkillRevision] = field(default_factory=list)
    workflow_signals: list[SkillWorkflowSignal] = field(default_factory=list)


__all__ = [
    "SKILL_WRITE_DELETE_CONSENT_STEP",
    "SkillRecord",
    "SkillRevision",
    "SkillWorkflowSignal",
    "ensure_skill_write_delete_consent_step",
]
