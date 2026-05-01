"""Structured durable user memory for Project Nullion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from functools import lru_cache
import re
from typing import Any, TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.config import NullionSettings


class UserMemoryKind(str, Enum):
    FACT = "fact"
    PREFERENCE = "preference"
    ENVIRONMENT_FACT = "environment_fact"


@dataclass(slots=True)
class UserMemoryEntry:
    entry_id: str
    owner: str
    kind: UserMemoryKind
    key: str
    value: str
    source: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class MemoryCandidate:
    zip_code: str | None = None
    remember_fact: str | None = None
    field_key: str | None = None
    field_value: str | None = None
    is_remember_request: bool = False
    zip_context: bool = False
    weather_context: bool = False


class _MemoryCaptureState(TypedDict, total=False):
    store: Any
    owner: str
    text: str
    source: str
    stripped: str
    written: list[UserMemoryEntry]
    candidate: MemoryCandidate


def _slug_key(value: str, *, default: str = "memory") -> str:
    key = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return key[:60] or default


def memory_owner_for_workspace(workspace_id: str | None) -> str:
    normalized = str(workspace_id or "workspace_admin").strip() or "workspace_admin"
    return f"workspace:{normalized}"


def memory_owner_for_messaging(
    channel: str,
    identity: str | int | None,
    settings: NullionSettings | None,
) -> str:
    from nullion.users import resolve_messaging_user

    user = resolve_messaging_user(channel, identity, settings)
    return memory_owner_for_workspace(user.workspace_id)


def memory_owner_for_web_admin() -> str:
    return memory_owner_for_workspace("workspace_admin")


def remember_home_zip(store, *, owner: str, zip_code: str, source: str) -> UserMemoryEntry:
    now = datetime.now(UTC)
    entry_id = f"{owner}:home_zip"
    existing = store.get_user_memory_entry(entry_id)
    entry = UserMemoryEntry(
        entry_id=entry_id,
        owner=owner,
        kind=UserMemoryKind.PREFERENCE,
        key="home_zip",
        value=zip_code,
        source=source,
        created_at=now if existing is None else existing.created_at,
        updated_at=now,
    )
    store.add_user_memory_entry(entry)
    return entry


def remember_text_fact(
    store,
    *,
    owner: str,
    key: str,
    value: str,
    source: str,
    kind: UserMemoryKind = UserMemoryKind.FACT,
) -> UserMemoryEntry:
    now = datetime.now(UTC)
    normalized_key = _slug_key(key)
    entry_id = f"{owner}:{normalized_key}"
    existing = store.get_user_memory_entry(entry_id)
    entry = UserMemoryEntry(
        entry_id=entry_id,
        owner=owner,
        kind=kind,
        key=normalized_key,
        value=value.strip(),
        source=source,
        created_at=now if existing is None else existing.created_at,
        updated_at=now,
    )
    store.add_user_memory_entry(entry)
    return entry


def _memory_normalize_node(state: _MemoryCaptureState) -> dict[str, object]:
    if not state.get("text") or not state.get("owner"):
        return {"stripped": "", "written": []}
    stripped = " ".join(str(state.get("text") or "").strip().split())
    mode_match = re.match(
        r"^Mode:\s*Remember\.\s*Extract durable preferences or project context if appropriate\.\s*(?P<body>.+)$",
        stripped,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if mode_match is not None:
        stripped = mode_match.group("body").strip()
    return {"stripped": stripped, "written": []}


def _memory_detect_candidates_node(state: _MemoryCaptureState) -> dict[str, object]:
    stripped = state.get("stripped") or ""
    if not stripped:
        return {"candidate": MemoryCandidate()}
    return {"candidate": MemoryCandidate()}


def _memory_zip_node(state: _MemoryCaptureState) -> dict[str, object]:
    written = list(state.get("written") or [])
    candidate = state.get("candidate") or MemoryCandidate()
    zip_code = candidate.zip_code
    if zip_code and (candidate.is_remember_request or candidate.zip_context or candidate.weather_context):
        written.append(remember_home_zip(state["store"], owner=state["owner"], zip_code=zip_code, source=state["source"]))
    return {"written": written}


def _memory_direct_field_node(state: _MemoryCaptureState) -> dict[str, object]:
    candidate = state.get("candidate") or MemoryCandidate()
    if candidate.remember_fact is not None:
        return {}
    written = list(state.get("written") or [])
    key = candidate.field_key
    value = candidate.field_value
    if key and value and _slug_key(key) not in {"zip", "zip_code"}:
        written.append(
            remember_text_fact(
                state["store"],
                owner=state["owner"],
                key=key,
                value=value,
                source=state["source"],
                kind=UserMemoryKind.PREFERENCE,
            )
        )
    return {"written": written}


def _memory_remember_field_node(state: _MemoryCaptureState) -> dict[str, object]:
    candidate = state.get("candidate") or MemoryCandidate()
    fact = candidate.remember_fact
    if not fact:
        return {}
    written = list(state.get("written") or [])
    return {"written": written}


def _memory_freeform_fact_node(state: _MemoryCaptureState) -> dict[str, object]:
    candidate = state.get("candidate") or MemoryCandidate()
    fact = candidate.remember_fact
    if not fact or candidate.zip_code:
        return {}
    written = list(state.get("written") or [])
    entry = UserMemoryEntry(
        entry_id=f"{state['owner']}:memory:{uuid4().hex[:8]}",
        owner=state["owner"],
        kind=UserMemoryKind.FACT,
        key="memory",
        value=fact,
        source=state["source"],
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    state["store"].add_user_memory_entry(entry)
    written.append(entry)
    return {"written": written}


@lru_cache(maxsize=1)
def _compiled_memory_capture_graph():
    graph = StateGraph(_MemoryCaptureState)
    graph.add_node("normalize", _memory_normalize_node)
    graph.add_node("detect_candidates", _memory_detect_candidates_node)
    graph.add_node("zip", _memory_zip_node)
    graph.add_node("direct_field", _memory_direct_field_node)
    graph.add_node("remember_field", _memory_remember_field_node)
    graph.add_node("freeform_fact", _memory_freeform_fact_node)
    graph.add_edge(START, "normalize")
    graph.add_edge("normalize", "detect_candidates")
    graph.add_edge("detect_candidates", "zip")
    graph.add_edge("zip", "direct_field")
    graph.add_edge("direct_field", "remember_field")
    graph.add_edge("remember_field", "freeform_fact")
    graph.add_edge("freeform_fact", END)
    return graph.compile()


def capture_explicit_user_memory(store, *, owner: str, text: str, source: str) -> list[UserMemoryEntry]:
    """Persist explicit user memories from direct "remember..." style requests."""
    final_state = _compiled_memory_capture_graph().invoke(
        {
            "store": store,
            "owner": owner,
            "text": text,
            "source": source,
            "written": [],
        },
        config={"configurable": {"thread_id": f"memory-capture:{owner or 'unknown'}"}},
    )
    return list(final_state.get("written") or [])


def memory_entries_for_owner(store, owner: str) -> list[UserMemoryEntry]:
    return [entry for entry in store.list_user_memory_entries() if entry.owner == owner]


def format_memory_context(entries: list[UserMemoryEntry]) -> str | None:
    if not entries:
        return None
    labels = {
        "home_zip": "Home ZIP",
    }
    lines = []
    for entry in entries:
        label = labels.get(entry.key, entry.key.replace("_", " ").title())
        lines.append(f"{label}: {entry.value}")
    return "\n".join(lines)


__all__ = [
    "UserMemoryEntry",
    "UserMemoryKind",
    "MemoryCandidate",
    "capture_explicit_user_memory",
    "format_memory_context",
    "memory_entries_for_owner",
    "memory_owner_for_messaging",
    "memory_owner_for_workspace",
    "memory_owner_for_web_admin",
    "remember_home_zip",
    "remember_text_fact",
]
