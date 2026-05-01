from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from nullion import builder_memory
from nullion.context_bus import CONTEXT_BUS_MAX_BYTES, ContextBus, ContextTimeoutError
from nullion.memory import UserMemoryEntry, UserMemoryKind


@pytest.mark.asyncio
async def test_context_bus_publish_get_wait_and_clear() -> None:
    bus = ContextBus()

    waiter = asyncio.create_task(bus.wait_for("page", group_id="g1", timeout_s=1))
    await asyncio.sleep(0)
    bus.publish("page", {"html": "<h1>Hi</h1>"}, group_id="g1", agent_id="a1", task_id="t1")

    assert await waiter == {"html": "<h1>Hi</h1>"}
    assert bus.get("page", group_id="g1") == {"html": "<h1>Hi</h1>"}
    assert bus.group_keys("g1") == ["page"]
    assert bus.get("page", group_id="other") is None

    bus.clear_group("g1")
    assert bus.group_keys("g1") == []


@pytest.mark.asyncio
async def test_context_bus_timeout_and_cleared_waiter_errors() -> None:
    bus = ContextBus()

    with pytest.raises(ContextTimeoutError, match="not published"):
        await bus.wait_for("missing", group_id="g1", timeout_s=0.001)

    waiter = asyncio.create_task(bus.wait_for("soon-cleared", group_id="g2", timeout_s=1))
    await asyncio.sleep(0)
    bus.clear_group("g2")
    with pytest.raises(ContextTimeoutError, match="set but value is None"):
        await waiter


def test_context_bus_truncates_large_text_and_bytes() -> None:
    bus = ContextBus()
    bus.publish("text", "x" * (CONTEXT_BUS_MAX_BYTES + 10), group_id="g", agent_id="a", task_id="t")
    bus.publish("bytes", b"x" * (CONTEXT_BUS_MAX_BYTES + 10), group_id="g", agent_id="a", task_id="t")

    assert bus.get("text", group_id="g").endswith(f"showing first {CONTEXT_BUS_MAX_BYTES}]")
    assert bus.get("bytes", group_id="g").endswith(b"bytes total]")


class MemoryStore:
    def __init__(self, entries: list[UserMemoryEntry]) -> None:
        self.entries = entries
        self.user_facts = {entry.entry_id: entry for entry in entries if entry.kind is UserMemoryKind.FACT}
        self.preferences = {entry.entry_id: entry for entry in entries if entry.kind is UserMemoryKind.PREFERENCE}
        self.environment_facts = {
            entry.entry_id: entry for entry in entries if entry.kind is UserMemoryKind.ENVIRONMENT_FACT
        }
        self.written: list[UserMemoryEntry] = []

    def list_user_memory_entries(self) -> list[UserMemoryEntry]:
        return list(self.entries)

    def add_user_memory_entry(self, entry: UserMemoryEntry) -> None:
        self.written.append(entry)
        if entry.kind is UserMemoryKind.PREFERENCE:
            self.preferences[entry.entry_id] = entry
        elif entry.kind is UserMemoryKind.ENVIRONMENT_FACT:
            self.environment_facts[entry.entry_id] = entry
        else:
            self.user_facts[entry.entry_id] = entry


class Runtime:
    def __init__(self, store: MemoryStore) -> None:
        self.store = store


class ModelClient:
    def __init__(self, text: str, *, reject_system_kwarg: bool = False) -> None:
        self.text = text
        self.reject_system_kwarg = reject_system_kwarg
        self.calls: list[dict] = []

    def create(self, **kwargs):
        if self.reject_system_kwarg and "system" in kwargs:
            raise TypeError("system unsupported")
        self.calls.append(kwargs)
        return {"content": [{"type": "text", "text": self.text}]}


def _entry(index: int, owner: str = "workspace:one") -> UserMemoryEntry:
    when = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=index)
    kind = [UserMemoryKind.FACT, UserMemoryKind.PREFERENCE, UserMemoryKind.ENVIRONMENT_FACT][index % 3]
    return UserMemoryEntry(
        entry_id=f"{owner}:e{index}",
        owner=owner,
        kind=kind,
        key=f"key_{index}",
        value=f"value {index}",
        created_at=when,
        updated_at=when,
    )


def test_compact_memory_skips_owners_below_threshold() -> None:
    store = MemoryStore([_entry(index) for index in range(builder_memory.COMPACTION_THRESHOLD - 1)])
    client = ModelClient('[{"key":"merged","value":"value","kind":"fact"}]')

    assert builder_memory.compact_memory(Runtime(store), client) == 0
    assert client.calls == []
    assert store.written == []


def test_compact_memory_writes_replacements_and_removes_candidates() -> None:
    entries = [_entry(index) for index in range(builder_memory.COMPACTION_THRESHOLD + 2)]
    store = MemoryStore(entries)
    client = ModelClient(
        """```json
        [
          {"key": "editor", "value": "likes compact tests", "kind": "preference"},
          {"key": "bad_kind", "value": "falls back", "kind": "surprise"}
        ]
        ```"""
    )

    written = builder_memory.compact_memory(Runtime(store), client)

    assert written == 2
    assert [entry.key for entry in store.written] == ["editor", "bad_kind"]
    assert store.written[0].kind is UserMemoryKind.PREFERENCE
    assert store.written[1].kind is UserMemoryKind.FACT
    newest_ids = {entry.entry_id for entry in sorted(entries, key=lambda item: item.updated_at, reverse=True)[:5]}
    remaining_original_ids = set(store.user_facts) | set(store.preferences) | set(store.environment_facts)
    assert newest_ids.issubset(remaining_original_ids)
    assert entries[0].entry_id not in remaining_original_ids


def test_compact_memory_supports_model_clients_without_system_kwarg() -> None:
    entries = [_entry(index) for index in range(builder_memory.COMPACTION_THRESHOLD)]
    store = MemoryStore(entries)
    client = ModelClient('[{"key":"merged","value":"value","kind":"fact"}]', reject_system_kwarg=True)

    assert builder_memory.compact_memory(Runtime(store), client) == 1
    assert client.calls[0]["messages"][0]["role"] == "system"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", []),
        ("not json", []),
        ('{"key":"not-list"}', []),
        ('prefix [{"key":"inside","value":"ok","kind":"fact"}] suffix', [{"key": "inside", "value": "ok", "kind": "fact"}]),
        ('[{"key":"ok"}, "drop"]', [{"key": "ok"}]),
    ],
)
def test_parse_compacted_entries_tolerates_model_output_shapes(raw: str, expected: list[dict]) -> None:
    assert builder_memory._parse_compacted_entries(raw) == expected


def test_compact_memory_fails_silently_when_store_raises() -> None:
    class BrokenStore:
        def list_user_memory_entries(self):
            raise RuntimeError("boom")

    assert builder_memory.compact_memory(Runtime(BrokenStore()), ModelClient("[]")) == 0
