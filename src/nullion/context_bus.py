"""Context Bus — scoped async data channel between mini-agents in a task group.

Mini-agents in the same group use the bus to pass data without going through
the user or the orchestrator's conversation history. A producer publishes a
value under a key; consumers await that key and receive the value once it
arrives. Keys are always scoped to a group_id so parallel user requests cannot
interfere with each other.

Usage::

    bus = ContextBus()
    # Producer (mini-agent 1):
    bus.publish("page_content", html, group_id="g1", agent_id="a1", task_id="t1")
    # Consumer (mini-agent 2, in a separate asyncio task):
    html = await bus.wait_for("page_content", group_id="g1")
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Entries larger than this are truncated before storage.
CONTEXT_BUS_MAX_BYTES: int = 32 * 1024  # 32 KB


@dataclass
class ContextEntry:
    key: str
    value: Any                  # serializable: str, dict, list, bytes
    group_id: str
    produced_by: str            # agent_id
    task_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ContextBus:
    """Async publish/subscribe data channel scoped to task groups."""

    def __init__(self) -> None:
        # _entries[group_id][key] = ContextEntry
        self._entries: dict[str, dict[str, ContextEntry]] = {}
        # _events[group_id][key] = asyncio.Event — set when key is published
        self._events: dict[str, dict[str, asyncio.Event]] = {}
        self._lock = asyncio.Lock()

    # ── Write ──────────────────────────────────────────────────────────────

    def publish(
        self,
        key: str,
        value: Any,
        *,
        group_id: str,
        agent_id: str,
        task_id: str,
    ) -> None:
        """Publish a value under *key* for the given group.

        If the value is a string larger than CONTEXT_BUS_MAX_BYTES, it is
        truncated and annotated. Consumers waiting on the key are unblocked.
        """
        value = _maybe_truncate(value)
        entry = ContextEntry(
            key=key,
            value=value,
            group_id=group_id,
            produced_by=agent_id,
            task_id=task_id,
        )
        group_entries = self._entries.setdefault(group_id, {})
        group_entries[key] = entry

        # Unblock any waiters for this (group_id, key) pair.
        event = self._events.get(group_id, {}).get(key)
        if event is not None:
            event.set()
        logger.debug("ContextBus: published %s/%s (%d bytes)", group_id, key, _value_size(value))

    # ── Read ───────────────────────────────────────────────────────────────

    def get(self, key: str, *, group_id: str) -> Any | None:
        """Return the value for *key* in *group_id*, or None if not yet published."""
        entry = self._entries.get(group_id, {}).get(key)
        return entry.value if entry is not None else None

    async def wait_for(
        self,
        key: str,
        *,
        group_id: str,
        timeout_s: float = 60.0,
    ) -> Any:
        """Await until *key* is published in *group_id*.

        Returns the value immediately if already published.
        Raises ContextTimeoutError if *timeout_s* elapses without a publish.
        """
        # Fast path — already published.
        existing = self.get(key, group_id=group_id)
        if existing is not None:
            return existing

        # Create/reuse an event for this (group_id, key).
        group_events = self._events.setdefault(group_id, {})
        if key not in group_events:
            group_events[key] = asyncio.Event()
        event = group_events[key]

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_s)
        except asyncio.TimeoutError:
            raise ContextTimeoutError(
                f"Context key {key!r} in group {group_id!r} not published within {timeout_s}s"
            )

        value = self.get(key, group_id=group_id)
        if value is None:
            raise ContextTimeoutError(f"Context key {key!r} set but value is None")
        return value

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def clear_group(self, group_id: str) -> None:
        """Release all entries and events for *group_id*.

        Called once all tasks in the group reach a terminal state.
        """
        self._entries.pop(group_id, None)
        events = self._events.pop(group_id, {})
        # Set any remaining waiters so they don't block forever.
        for event in events.values():
            event.set()
        logger.debug("ContextBus: cleared group %s", group_id)

    def group_keys(self, group_id: str) -> list[str]:
        """Return all published keys for *group_id*."""
        return list(self._entries.get(group_id, {}).keys())


# ── Helpers ────────────────────────────────────────────────────────────────────

class ContextTimeoutError(TimeoutError):
    """Raised when wait_for() exceeds its timeout."""


def _value_size(value: Any) -> int:
    if isinstance(value, (bytes, bytearray)):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8", errors="replace"))
    return 0


def _maybe_truncate(value: Any) -> Any:
    """Truncate oversized string/bytes values and annotate them."""
    if isinstance(value, str):
        encoded = value.encode("utf-8", errors="replace")
        if len(encoded) > CONTEXT_BUS_MAX_BYTES:
            total = len(encoded)
            truncated = encoded[:CONTEXT_BUS_MAX_BYTES].decode("utf-8", errors="replace")
            return truncated + f"\n\n[truncated: {total} bytes total, showing first {CONTEXT_BUS_MAX_BYTES}]"
    elif isinstance(value, (bytes, bytearray)):
        if len(value) > CONTEXT_BUS_MAX_BYTES:
            total = len(value)
            return value[:CONTEXT_BUS_MAX_BYTES] + (
                f"\n[truncated: {total} bytes total]".encode()
            )
    return value


__all__ = [
    "ContextBus",
    "ContextEntry",
    "ContextTimeoutError",
    "CONTEXT_BUS_MAX_BYTES",
]
