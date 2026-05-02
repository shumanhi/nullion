"""Builder observer — extracts structured signals from completed turns.

Every time a turn completes, the observer runs in the background, extracts
a TurnSignal from what just happened, and appends it to a rolling window.
The Pattern Detector then reads this window to find repeating workflows.

This is the data collection layer for Builder's self-improvement loop:

    Turn → Observer → TurnSignal → Pattern Detector → Skill Inducer
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Sequence


class TurnOutcome(str, Enum):
    SUCCESS = "success"          # LLM replied, no errors, no suspension
    SUSPENDED = "suspended"      # Stopped for approval
    FAILED = "failed"            # Exception / no reply
    PARTIAL = "partial"          # Some tools failed but reply produced


class TaskCategory(str, Enum):
    FETCH = "fetch"              # web fetch, HTTP requests
    SEARCH = "search"            # web search, information lookup
    FILE = "file"                # file read/write/create
    TERMINAL = "terminal"        # shell/exec commands
    EMAIL = "email"              # email operations
    CALENDAR = "calendar"        # calendar operations
    MEMORY = "memory"            # memory read/write
    CHITCHAT = "chitchat"        # conversational, no tools
    MIXED = "mixed"              # multiple categories
    UNKNOWN = "unknown"


@dataclass(slots=True)
class TurnSignal:
    """A compact structured observation of one completed turn."""
    signal_id: str
    recorded_at: datetime
    user_message: str
    tool_names: tuple[str, ...]       # ordered list of tools invoked
    tool_count: int
    outcome: TurnOutcome
    category: TaskCategory
    assistant_reply_length: int       # proxy for response complexity
    had_approval_request: bool
    had_tool_errors: bool
    conversation_id: str


@dataclass(slots=True)
class PatternSignal:
    """A detected pattern — N turns that share the same tool sequence."""
    pattern_id: str
    tool_sequence: tuple[str, ...]    # canonical tool names in order
    category: TaskCategory
    occurrence_count: int             # how many times seen
    first_seen_at: datetime
    last_seen_at: datetime
    example_user_messages: tuple[str, ...]   # up to 3 representative prompts
    confidence: float                 # 0-1, grows with occurrences


_TOOL_CATEGORY_MAP: dict[str, TaskCategory] = {
    "web_fetch": TaskCategory.FETCH,
    "fetch_url": TaskCategory.FETCH,
    "web_search": TaskCategory.SEARCH,
    "search_web": TaskCategory.SEARCH,
    "terminal_exec": TaskCategory.TERMINAL,
    "exec": TaskCategory.TERMINAL,
    "read_file": TaskCategory.FILE,
    "write_file": TaskCategory.FILE,
    "list_files": TaskCategory.FILE,
    "send_email": TaskCategory.EMAIL,
    "read_email": TaskCategory.EMAIL,
    "search_email": TaskCategory.EMAIL,
    "list_calendar": TaskCategory.CALENDAR,
    "read_memory": TaskCategory.MEMORY,
    "write_memory": TaskCategory.MEMORY,
    "store_memory": TaskCategory.MEMORY,
}


def _category_for_tools(tool_names: Sequence[str]) -> TaskCategory:
    if not tool_names:
        return TaskCategory.CHITCHAT
    categories = {_TOOL_CATEGORY_MAP.get(name, TaskCategory.UNKNOWN) for name in tool_names}
    categories.discard(TaskCategory.UNKNOWN)
    if not categories:
        return TaskCategory.UNKNOWN
    if len(categories) == 1:
        return next(iter(categories))
    return TaskCategory.MIXED


def _canonical_tool_sequence(tool_names: Sequence[str]) -> tuple[str, ...]:
    """Deduplicate consecutive identical tools (e.g. fetch, fetch → fetch) for pattern matching."""
    seen: list[str] = []
    for name in tool_names:
        if not seen or seen[-1] != name:
            seen.append(name)
    return tuple(seen)


def extract_turn_signal(
    *,
    signal_id: str,
    user_message: str,
    assistant_reply: str | None,
    tool_names: Sequence[str],
    tool_error_count: int,
    outcome: TurnOutcome,
    conversation_id: str,
    had_approval_request: bool = False,
    recorded_at: datetime | None = None,
) -> TurnSignal:
    return TurnSignal(
        signal_id=signal_id,
        recorded_at=recorded_at or datetime.now(UTC),
        user_message=user_message.strip()[:200],   # truncate for storage
        tool_names=tuple(tool_names),
        tool_count=len(tool_names),
        outcome=outcome,
        category=_category_for_tools(tool_names),
        assistant_reply_length=len(assistant_reply or ""),
        had_approval_request=had_approval_request,
        had_tool_errors=tool_error_count > 0,
        conversation_id=conversation_id,
    )


def detect_patterns(
    signals: Sequence[TurnSignal],
    *,
    min_occurrences: int = 2,
    window: int = 50,
) -> list[PatternSignal]:
    """Scan the most recent `window` signals and find repeated tool sequences."""
    recent = list(signals)[-window:]
    tool_turns = [s for s in recent if s.tool_count > 0 and s.outcome is TurnOutcome.SUCCESS]

    # Group by canonical tool sequence
    groups: dict[tuple[str, ...], list[TurnSignal]] = {}
    for signal in tool_turns:
        key = _canonical_tool_sequence(signal.tool_names)
        if not key or len(set(key)) < 2:
            continue
        groups.setdefault(key, []).append(signal)

    patterns: list[PatternSignal] = []
    for tool_seq, group in groups.items():
        if len(group) < min_occurrences:
            continue
        sorted_group = sorted(group, key=lambda s: s.recorded_at)
        example_messages = tuple(
            s.user_message for s in sorted_group[-3:]
        )
        # Confidence: starts at 0.5 at 2 occurrences, approaches 1.0 asymptotically
        raw_confidence = 1.0 - (1.0 / (len(group) * 0.8))
        confidence = min(0.95, max(0.5, raw_confidence))
        patterns.append(PatternSignal(
            pattern_id=f"pattern-{'_'.join(tool_seq[:3])}",
            tool_sequence=tool_seq,
            category=_category_for_tools(tool_seq),
            occurrence_count=len(group),
            first_seen_at=sorted_group[0].recorded_at,
            last_seen_at=sorted_group[-1].recorded_at,
            example_user_messages=example_messages,
            confidence=confidence,
        ))

    return sorted(patterns, key=lambda p: p.confidence, reverse=True)


__all__ = [
    "PatternSignal",
    "TaskCategory",
    "TurnOutcome",
    "TurnSignal",
    "detect_patterns",
    "extract_turn_signal",
]
