"""Structured reusable procedures/playbooks for Project Nullion."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


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


__all__ = ["SkillRecord", "SkillRevision", "SkillWorkflowSignal"]
