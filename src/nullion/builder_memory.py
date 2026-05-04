"""Builder memory management — distils turns into tight workspace facts.

The builder observes each turn and learns skills. It also owns durable memory:
stable user preferences, project facts, and environment details. Memory is
scoped per workspace and rewritten into a small concise set so the prompt does
not grow without bound.

This module provides two best-effort passes:

1.  Manage one completed turn: ask the model for a structured memory rewrite
    for that workspace and replace the workspace entries with the bounded result.
2.  Compact older stores: keep compatibility with existing explicit/raw memory
    entries by distilling oversized owner buckets.

Both flows use structured JSON and validated runtime state. They do not route
from user-message words or phrase lists.

Design rules
------------
- Fails silently.  Compaction is a best-effort background pass.
- Never fires when entry count is below COMPACTION_THRESHOLD.
- Preserves entries created after RECENT_WINDOW_ENTRIES (they are still
  "hot" and should not be rewritten yet).
- Idempotent: running compaction twice should not shrink the store below a
  reasonable floor.
"""
from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Mapping, TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.memory import UserMemoryEntry, UserMemoryKind

logger = logging.getLogger(__name__)

COMPACTION_THRESHOLD = 20
RECENT_WINDOW_ENTRIES = 5
MAX_MEMORY_ENTRIES_PER_OWNER = 50
DEFAULT_MEMORY_LONG_TERM_LIMIT = 25
DEFAULT_MEMORY_MID_TERM_LIMIT = 15
DEFAULT_MEMORY_SHORT_TERM_LIMIT = 10
MAX_CONFIGURED_MEMORY_LIMIT = 50
MEMORY_RECALL_SCORE_INCREMENT = 1.0
MAX_MEMORY_USE_SCORE = 100.0
MAX_MEMORY_CONTEXT_CHARS = 1400
MAX_TURN_TEXT_CHARS = 900
MAX_MEMORY_VALUE_CHARS = 160
MIN_MEMORY_CONFIDENCE = 0.62
PROMOTE_TO_MID_TERM_SCORE = 3.0
PROMOTE_TO_LONG_TERM_SCORE = 7.0
MAX_VOLATILE_DIGIT_RUN = 9


@dataclass(slots=True)
class BuilderMemoryResult:
    written: int = 0
    removed: int = 0
    skipped: str | None = None


@dataclass(frozen=True, slots=True)
class MemoryPolicy:
    long_term_limit: int = DEFAULT_MEMORY_LONG_TERM_LIMIT
    mid_term_limit: int = DEFAULT_MEMORY_MID_TERM_LIMIT
    short_term_limit: int = DEFAULT_MEMORY_SHORT_TERM_LIMIT

    @property
    def total_limit(self) -> int:
        return self.long_term_limit + self.mid_term_limit + self.short_term_limit

    def limit_for_kind(self, kind: UserMemoryKind) -> int:
        if kind is UserMemoryKind.PREFERENCE:
            return self.long_term_limit
        if kind is UserMemoryKind.ENVIRONMENT_FACT:
            return self.mid_term_limit
        return self.short_term_limit


_MEMORY_BUCKET_LABELS: dict[UserMemoryKind, str] = {
    UserMemoryKind.PREFERENCE: "Long-term",
    UserMemoryKind.ENVIRONMENT_FACT: "Mid-term",
    UserMemoryKind.FACT: "Short-term",
}


class _BuilderMemoryState(TypedDict, total=False):
    runtime: Any
    model_client: Any
    owner: str
    user_message: str
    assistant_reply: str | None
    tool_results: list[Any]
    existing: list[UserMemoryEntry]
    should_call_model: bool
    raw_json: str
    result: BuilderMemoryResult

_COMPACTION_SYSTEM_PROMPT = """\
You are a memory compaction assistant. You will receive a list of memory entries
for an AI assistant. Your job is to distil them into a smaller set of tight,
non-redundant facts that preserve all important information.

Rules:
- Merge duplicates and near-duplicates into a single entry.
- Remove entries that are superseded by newer ones.
- Keep entries that represent stable user preferences, facts, and environment info.
- Use concise language — each entry value should be ≤ 120 characters.
- Return ONLY a JSON array of objects with keys: "key", "value", "kind".
  "kind" must be one of: "fact", "preference", "environment_fact".
- Do not add new facts that were not in the original list.
- Return at least 1 entry.
"""

_TURN_MEMORY_SYSTEM_PROMPT = """\
You are the Builder memory manager for Nullion.

Manage durable memory for exactly one workspace. You will receive:
- the current workspace memory entries
- one completed user/assistant turn
- compact structured tool-result metadata

Return ONLY valid JSON:
{
  "entries": [
    {
      "key": "stable_snake_case_key",
      "value": "concise fact, preference, or environment detail",
      "kind": "fact",
      "confidence": 0.84
    }
  ]
}

Rules:
- Keep only stable, reusable facts that should help future turns in this workspace.
- Preserve important existing memory unless it is superseded, redundant, or too specific.
- Prefer fewer entries. Follow the supplied per-category limits.
- Use kind "preference" for long-term memory, "environment_fact" for mid-term memory, and "fact" for short-term memory.
- Existing memory includes use_count, use_score, and last_used_at; preserve repeatedly recalled memory unless it is superseded.
- Each value must be 160 characters or less.
- Use kind "preference", "fact", or "environment_fact".
- Do not store transient task status, one-off command outputs, secrets, tokens, or raw conversation history.
- Do not add facts not supported by the provided turn or existing memory.
- Do not rely on English trigger words; judge memory only from the full turn evidence.
- If nothing should be remembered, return {"entries": []}.
"""

_EXPLICIT_TURN_MEMORY_SYSTEM_PROMPT = """\
You extract explicit durable memory from exactly one completed user/assistant turn.

Return ONLY valid JSON:
{
  "entries": [
    {
      "key": "stable_snake_case_key",
      "value": "concise durable fact or preference",
      "kind": "preference",
      "confidence": 0.91
    }
  ]
}

Rules:
- Use only the current turn evidence. Do not preserve or rewrite old memory here.
- Include durable user facts, preferences, and workspace context that should help later turns.
- Use kind "preference" for long-term user preferences, "environment_fact" for mid-term workspace/environment context, and "fact" for short-term context.
- Exclude transient task status, generated IDs, one-off command outputs, raw logs, secrets, tokens, and conversation history.
- Do not rely on English trigger words. Judge the turn from its meaning and the assistant acknowledgment.
- If no durable memory was explicitly stated or confirmed, return {"entries": []}.
"""


def manage_turn_memory(
    runtime,
    model_client,
    *,
    owner: str,
    user_message: str,
    assistant_reply: str | None,
    tool_results: list[Any] | tuple[Any, ...] | None = None,
) -> BuilderMemoryResult:
    """Let Builder rewrite one workspace's memory after a completed turn."""
    try:
        final_state = _compiled_builder_memory_graph().invoke(
            {
                "runtime": runtime,
                "model_client": model_client,
                "owner": owner,
                "user_message": user_message,
                "assistant_reply": assistant_reply,
                "tool_results": list(tool_results or []),
            },
            config={"configurable": {"thread_id": f"builder-memory:{owner or 'unknown'}"}},
        )
        result = final_state.get("result")
        return result if isinstance(result, BuilderMemoryResult) else BuilderMemoryResult(skipped="no_result")
    except Exception as exc:
        logger.debug("builder_memory: turn memory management failed: %s", exc)
        return BuilderMemoryResult(skipped="failed")


def capture_turn_memory_claims(
    runtime,
    model_client,
    *,
    owner: str,
    user_message: str,
    assistant_reply: str | None,
) -> BuilderMemoryResult:
    """Capture explicit memory claims from the current turn only."""
    try:
        store = getattr(runtime, "store", None)
        if store is None or model_client is None or not str(owner or "").strip():
            return BuilderMemoryResult(skipped="missing_context")
        user_content = json.dumps(
            {
                "workspace_owner": owner,
                "turn": {
                    "user_message": _trim_text(user_message or "", MAX_TURN_TEXT_CHARS),
                    "assistant_reply": _trim_text(assistant_reply or "", MAX_TURN_TEXT_CHARS),
                },
                "limits": {
                    "max_entries": 4,
                    "max_value_chars": MAX_MEMORY_VALUE_CHARS,
                },
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        response = model_client.create(
            messages=[
                {"role": "system", "content": [{"type": "text", "text": _EXPLICIT_TURN_MEMORY_SYSTEM_PROMPT}]},
                {"role": "user", "content": [{"type": "text", "text": user_content}]},
            ],
            tools=[],
            max_tokens=500,
        )
        policy = MemoryPolicy(long_term_limit=4, mid_term_limit=4, short_term_limit=4)
        parsed_entries = _parse_turn_memory_entries(_extract_text(response), policy=policy)[:4]
        if not parsed_entries:
            return BuilderMemoryResult(skipped="empty")
        existing = _entries_for_owner(store, owner)
        now = datetime.now(UTC)
        for item in parsed_entries:
            store.add_user_memory_entry(
                _replacement_memory_entry(owner=owner, item=item, existing=existing, now=now)
            )
        _checkpoint_runtime(runtime)
        return BuilderMemoryResult(written=len(parsed_entries))
    except Exception as exc:
        logger.debug("builder_memory: explicit turn memory capture failed: %s", exc)
        return BuilderMemoryResult(skipped="failed")


def _memory_precheck_node(state: _BuilderMemoryState) -> dict[str, object]:
    runtime = state.get("runtime")
    store = getattr(runtime, "store", None)
    owner = str(state.get("owner") or "").strip()
    model_client = state.get("model_client")
    if store is None or model_client is None or not owner:
        return {"should_call_model": False, "result": BuilderMemoryResult(skipped="missing_context")}
    existing = _entries_for_owner(store, owner)
    return {"existing": existing, "should_call_model": True}


def _memory_route_after_precheck(state: _BuilderMemoryState) -> str:
    return "model" if state.get("should_call_model") else END


def _memory_model_node(state: _BuilderMemoryState) -> dict[str, object]:
    existing = list(state.get("existing") or [])
    policy = memory_policy_from_env()
    user_content = json.dumps(
        {
            "workspace_owner": state.get("owner") or "",
            "limits": {
                "max_entries": policy.total_limit,
                "long_term_memory": policy.long_term_limit,
                "mid_term_memory": policy.mid_term_limit,
                "short_term_memory": policy.short_term_limit,
                "max_value_chars": MAX_MEMORY_VALUE_CHARS,
            },
            "existing_memory": _memory_payload(existing, policy=policy),
            "turn": {
                "user_message": _trim_text(state.get("user_message") or "", MAX_TURN_TEXT_CHARS),
                "assistant_reply": _trim_text(state.get("assistant_reply") or "", MAX_TURN_TEXT_CHARS),
                "tool_results": _tool_result_payload(state.get("tool_results") or []),
            },
        },
        ensure_ascii=True,
        sort_keys=True,
    )
    try:
        response = state["model_client"].create(
            messages=[
                {"role": "system", "content": [{"type": "text", "text": _TURN_MEMORY_SYSTEM_PROMPT}]},
                {"role": "user", "content": [{"type": "text", "text": user_content}]},
            ],
            tools=[],
            max_tokens=900,
        )
    except Exception as exc:
        logger.debug("builder_memory: model call failed: %s", exc)
        return {"result": BuilderMemoryResult(skipped="model_failed")}
    return {"raw_json": _extract_text(response)}


def _memory_write_node(state: _BuilderMemoryState) -> dict[str, object]:
    runtime = state["runtime"]
    store = runtime.store
    owner = str(state.get("owner") or "").strip()
    existing = list(state.get("existing") or [])
    policy = memory_policy_from_env()
    parsed_entries = _parse_turn_memory_entries(state.get("raw_json") or "", policy=policy)
    if existing and not parsed_entries:
        if smart_cleanup_enabled():
            result = smart_cleanup_owner_memory(store, owner, policy=policy)
            if result.removed:
                _checkpoint_runtime(runtime)
                return {"result": result}
        return {"result": BuilderMemoryResult(skipped="empty_rewrite")}
    now = datetime.now(UTC)
    replacement_entries = [
        _replacement_memory_entry(owner=owner, item=item, existing=existing, now=now)
        for item in parsed_entries[:policy.total_limit]
    ]
    removed = _replace_owner_memory(store, owner, replacement_entries)
    for entry in replacement_entries:
        store.add_user_memory_entry(entry)
    cleanup_removed = 0
    if smart_cleanup_enabled():
        cleanup_removed = smart_cleanup_owner_memory(store, owner, policy=policy).removed
    _checkpoint_runtime(runtime)
    return {"result": BuilderMemoryResult(written=len(replacement_entries), removed=removed + cleanup_removed)}


@lru_cache(maxsize=1)
def _compiled_builder_memory_graph():
    graph = StateGraph(_BuilderMemoryState)
    graph.add_node("precheck", _memory_precheck_node)
    graph.add_node("model", _memory_model_node)
    graph.add_node("write", _memory_write_node)
    graph.add_edge(START, "precheck")
    graph.add_conditional_edges("precheck", _memory_route_after_precheck, {"model": "model", END: END})
    graph.add_edge("model", "write")
    graph.add_edge("write", END)
    return graph.compile()


def compact_memory(runtime, model_client) -> int:
    """Compact user memory entries via LLM distillation.

    Args:
        runtime:      A PersistentRuntime whose store has a list_user_memory_entries method.
        model_client: A model client with a .create(messages, tools, max_tokens) method.

    Returns:
        Number of entries written back (0 if compaction was skipped or failed).
    """
    try:
        return _compact_memory_inner(runtime, model_client)
    except Exception as exc:
        logger.warning("builder_memory: compaction failed silently: %s", exc)
        return 0


def _compact_memory_inner(runtime, model_client) -> int:
    store = runtime.store
    all_entries = store.list_user_memory_entries()

    entries_by_owner: dict[str, list[UserMemoryEntry]] = defaultdict(list)
    for entry in all_entries:
        entries_by_owner[entry.owner].append(entry)

    written = 0
    for owner, owner_entries in entries_by_owner.items():
        if len(owner_entries) < COMPACTION_THRESHOLD:
            logger.debug(
                "builder_memory: skipping compaction for %s — %d entries < threshold %d",
                owner,
                len(owner_entries),
                COMPACTION_THRESHOLD,
            )
            continue
        written += _compact_memory_entries(store, owner, owner_entries, model_client)
    return written


def _compact_memory_entries(store, owner: str, all_entries: list[UserMemoryEntry], model_client) -> int:

    # Protect repeatedly recalled memories first, then fall back to recency.
    sorted_entries = sorted(all_entries, key=_memory_survival_sort_key, reverse=True)
    recent_entries = sorted_entries[:RECENT_WINDOW_ENTRIES]
    candidate_entries = sorted_entries[RECENT_WINDOW_ENTRIES:]

    if not candidate_entries:
        return 0

    # Build the compaction payload
    payload = [
        {"key": e.key, "value": e.value, "kind": e.kind.value}
        for e in candidate_entries
    ]
    user_content = (
        "Please compact these memory entries:\n\n"
        + json.dumps(payload, indent=2)
    )

    try:
        response = model_client.create(
            messages=[
                {"role": "user", "content": [{"type": "text", "text": user_content}]},
            ],
            tools=[],
            max_tokens=1024,
            system=_COMPACTION_SYSTEM_PROMPT,
        )
    except TypeError:
        # Fallback: some model clients don't accept a system kwarg
        response = model_client.create(
            messages=[
                {"role": "system", "content": [{"type": "text", "text": _COMPACTION_SYSTEM_PROMPT}]},
                {"role": "user", "content": [{"type": "text", "text": user_content}]},
            ],
            tools=[],
            max_tokens=1024,
        )

    raw_text = _extract_text(response)
    compacted = _parse_compacted_entries(raw_text)
    if not compacted:
        logger.warning("builder_memory: LLM returned no compacted entries — skipping write-back")
        return 0

    # Write compacted entries FIRST, then remove old ones.
    # This ordering is crash-safe: if the process dies mid-compaction we end up
    # with duplicates (recoverable) rather than data loss.
    now = datetime.now(UTC)
    written = 0
    for item in compacted:
        try:
            kind = UserMemoryKind(item["kind"])
        except (ValueError, KeyError):
            kind = UserMemoryKind.FACT
        entry = UserMemoryEntry(
            entry_id=f"compacted-{uuid4().hex[:8]}",
            owner=owner,
            kind=kind,
            key=str(item.get("key", "memory")),
            value=str(item.get("value", "")),
            created_at=now,
            updated_at=now,
        )
        try:
            store.add_user_memory_entry(entry)
            written += 1
        except Exception as exc:
            logger.warning("builder_memory: failed to write compacted entry %r: %s", item.get("key"), exc)

    # Only remove originals after all compacted replacements are safely written.
    for entry in candidate_entries:
        try:
            store.user_facts.pop(entry.entry_id, None)
            store.preferences.pop(entry.entry_id, None)
            store.environment_facts.pop(entry.entry_id, None)
        except Exception as exc:
            logger.warning("builder_memory: failed to remove old entry %s: %s", entry.entry_id, exc)

    logger.info(
        "builder_memory: compacted %d entries → %d (kept %d recent raw)",
        len(candidate_entries),
        written,
        len(recent_entries),
    )
    return written


def _extract_text(response: dict) -> str:
    content = response.get("content") or []
    parts = [
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    return "".join(parts).strip()


def _parse_compacted_entries(raw: str) -> list[dict]:
    """Extract a JSON array from the LLM response, tolerating markdown fences."""
    if not raw:
        return []
    # Strip optional markdown code fences
    text = raw
    if "```" in text:
        lines = text.splitlines()
        inner = []
        inside = False
        for line in lines:
            if line.strip().startswith("```"):
                inside = not inside
                continue
            if inside:
                inner.append(line)
        text = "\n".join(inner).strip()

    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        # Try to extract a [...] block
        start = text.find("[")
        end = text.rfind("]")
        if start == -1 or end == -1:
            return []
        try:
            parsed = json.loads(text[start : end + 1])
        except (json.JSONDecodeError, ValueError):
            return []

    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _entries_for_owner(store, owner: str) -> list[UserMemoryEntry]:
    entries = [
        entry for entry in store.list_user_memory_entries()
        if getattr(entry, "owner", None) == owner
    ]
    return sorted(entries, key=lambda entry: (entry.kind.value, entry.key, entry.entry_id))


def smart_cleanup_enabled(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    value = str(env.get("NULLION_MEMORY_SMART_CLEANUP_ENABLED", "false")).strip().lower()
    return value not in {"", "0", "false", "no", "off"}


def memory_pressure_for_owner(
    store,
    owner: str,
    *,
    policy: MemoryPolicy | None = None,
) -> dict[str, object]:
    policy = policy or memory_policy_from_env()
    entries = _entries_for_owner(store, owner)
    buckets: list[dict[str, object]] = []
    full_buckets: list[dict[str, object]] = []
    for kind in (UserMemoryKind.PREFERENCE, UserMemoryKind.ENVIRONMENT_FACT, UserMemoryKind.FACT):
        count = sum(1 for entry in entries if entry.kind is kind)
        limit = policy.limit_for_kind(kind)
        bucket = {
            "kind": kind.value,
            "label": _MEMORY_BUCKET_LABELS[kind],
            "count": count,
            "limit": limit,
            "full": count > 0 and count >= limit,
            "can_increase": limit < MAX_CONFIGURED_MEMORY_LIMIT,
        }
        buckets.append(bucket)
        if bucket["full"]:
            full_buckets.append(bucket)
    return {
        "owner": owner,
        "full": bool(full_buckets),
        "buckets": buckets,
        "full_buckets": full_buckets,
        "can_increase": any(bool(bucket["can_increase"]) for bucket in full_buckets),
        "smart_cleanup_enabled": smart_cleanup_enabled(),
    }


def smart_cleanup_owner_memory(
    store,
    owner: str,
    *,
    policy: MemoryPolicy | None = None,
    reserve_ratio: float = 0.8,
) -> BuilderMemoryResult:
    policy = policy or memory_policy_from_env()
    entries = _entries_for_owner(store, owner)
    removed = 0
    for kind in (UserMemoryKind.PREFERENCE, UserMemoryKind.ENVIRONMENT_FACT, UserMemoryKind.FACT):
        candidates = [entry for entry in entries if entry.kind is kind]
        limit = policy.limit_for_kind(kind)
        if not candidates or len(candidates) < limit:
            continue
        if limit <= 0:
            target = 0
        else:
            target = max(0, int(limit * reserve_ratio))
            if target >= limit:
                target = max(0, limit - 1)
        keep_ids = {
            entry.entry_id
            for entry in sorted(candidates, key=_memory_survival_sort_key, reverse=True)[:target]
        }
        for entry in candidates:
            if entry.entry_id in keep_ids:
                continue
            if _remove_memory_entry(store, entry.entry_id):
                removed += 1
    return BuilderMemoryResult(removed=removed, skipped=None if removed else "nothing_to_clean")


def memory_policy_from_env(environ: Mapping[str, str] | None = None) -> MemoryPolicy:
    env = os.environ if environ is None else environ
    return MemoryPolicy(
        long_term_limit=_memory_limit_from_env(
            env, "NULLION_MEMORY_LONG_TERM_LIMIT", DEFAULT_MEMORY_LONG_TERM_LIMIT
        ),
        mid_term_limit=_memory_limit_from_env(
            env,
            "NULLION_MEMORY_MID_TERM_LIMIT",
            DEFAULT_MEMORY_MID_TERM_LIMIT,
            fallback_name="NULLION_MEMORY_PROJECT_LIMIT",
        ),
        short_term_limit=_memory_limit_from_env(
            env,
            "NULLION_MEMORY_SHORT_TERM_LIMIT",
            DEFAULT_MEMORY_SHORT_TERM_LIMIT,
            fallback_name="NULLION_MEMORY_WORKING_LIMIT",
        ),
    )


def _memory_limit_from_env(
    env: Mapping[str, str],
    name: str,
    default: int,
    *,
    fallback_name: str | None = None,
) -> int:
    raw_value = env.get(name)
    if raw_value is None and fallback_name:
        raw_value = env.get(fallback_name)
    try:
        value = int(float(str(default if raw_value is None else raw_value).strip()))
    except (TypeError, ValueError):
        value = default
    return max(0, min(value, MAX_CONFIGURED_MEMORY_LIMIT))


def _memory_payload(entries: list[UserMemoryEntry], *, policy: MemoryPolicy | None = None) -> list[dict[str, str]]:
    policy = policy or memory_policy_from_env()
    payload = [
        {
            "key": entry.key,
            "value": _trim_text(entry.value, MAX_MEMORY_VALUE_CHARS),
            "kind": entry.kind.value,
            "use_count": str(int(getattr(entry, "use_count", 0) or 0)),
            "use_score": f"{float(getattr(entry, 'use_score', 0.0) or 0.0):.2f}",
            "last_used_at": _dt_for_payload(getattr(entry, "last_used_at", None)),
        }
        for entry in select_memory_entries_for_prompt(entries, policy=policy, limit_multiplier=2)
    ]
    text = json.dumps(payload, ensure_ascii=True, sort_keys=True)
    if len(text) <= MAX_MEMORY_CONTEXT_CHARS:
        return payload
    trimmed: list[dict[str, str]] = []
    budget = MAX_MEMORY_CONTEXT_CHARS
    for item in payload:
        approx = len(json.dumps(item, ensure_ascii=True, sort_keys=True))
        if trimmed and budget - approx <= 0:
            break
        trimmed.append(item)
        budget -= approx
    return trimmed


def select_memory_entries_for_prompt(
    entries: list[UserMemoryEntry],
    *,
    policy: MemoryPolicy | None = None,
    limit_multiplier: int = 1,
) -> list[UserMemoryEntry]:
    policy = policy or memory_policy_from_env()
    selected: list[UserMemoryEntry] = []
    for kind in (UserMemoryKind.PREFERENCE, UserMemoryKind.ENVIRONMENT_FACT, UserMemoryKind.FACT):
        limit = max(0, policy.limit_for_kind(kind) * max(1, limit_multiplier))
        if limit <= 0:
            continue
        candidates = [entry for entry in entries if entry.kind is kind and is_durable_memory_entry(entry)]
        selected.extend(sorted(candidates, key=_memory_survival_sort_key, reverse=True)[:limit])
    return selected


def is_durable_memory_entry(entry: UserMemoryEntry) -> bool:
    return _is_structurally_durable_memory(
        str(getattr(entry, "key", "") or ""),
        str(getattr(entry, "value", "") or ""),
    )


def reinforce_memory_entries(store, entries: list[UserMemoryEntry]) -> int:
    if not entries:
        return 0
    now = datetime.now(UTC)
    written = 0
    for entry in entries:
        next_score = min(
            MAX_MEMORY_USE_SCORE,
            float(getattr(entry, "use_score", 0.0) or 0.0) + MEMORY_RECALL_SCORE_INCREMENT,
        )
        reinforced = UserMemoryEntry(
            entry_id=entry.entry_id,
            owner=entry.owner,
            kind=_promoted_memory_kind(entry.kind, next_score),
            key=entry.key,
            value=entry.value,
            source=entry.source,
            created_at=entry.created_at,
            updated_at=entry.updated_at,
            use_count=int(getattr(entry, "use_count", 0) or 0) + 1,
            use_score=next_score,
            last_used_at=now,
        )
        try:
            store.add_user_memory_entry(reinforced)
            written += 1
        except Exception:
            logger.debug("builder_memory: failed to reinforce memory %s", entry.entry_id, exc_info=True)
    return written


def _memory_survival_sort_key(entry: UserMemoryEntry) -> tuple[float, datetime, datetime, str]:
    score = float(getattr(entry, "use_score", 0.0) or 0.0)
    last_used = getattr(entry, "last_used_at", None) or datetime.min.replace(tzinfo=UTC)
    updated = entry.updated_at or entry.created_at or datetime.min.replace(tzinfo=UTC)
    return (score, last_used, updated, entry.entry_id)


def _promoted_memory_kind(kind: UserMemoryKind, score: float) -> UserMemoryKind:
    if kind is UserMemoryKind.FACT and score >= PROMOTE_TO_MID_TERM_SCORE:
        return UserMemoryKind.ENVIRONMENT_FACT
    if kind is UserMemoryKind.ENVIRONMENT_FACT and score >= PROMOTE_TO_LONG_TERM_SCORE:
        return UserMemoryKind.PREFERENCE
    return kind


def _tool_result_payload(tool_results: list[Any]) -> list[dict[str, str]]:
    payload: list[dict[str, str]] = []
    for result in tool_results[:8]:
        output = getattr(result, "output", None)
        detail = ""
        if isinstance(output, dict):
            for key in ("summary", "message", "path", "url", "status"):
                value = output.get(key)
                if value:
                    detail = str(value)
                    break
        elif isinstance(output, str):
            detail = output
        payload.append(
            {
                "tool_name": str(getattr(result, "tool_name", "") or "")[:80],
                "status": str(getattr(result, "status", "") or "")[:40],
                "detail": _trim_text(detail, 180),
            }
        )
    return payload


def _parse_turn_memory_entries(raw: str, *, policy: MemoryPolicy | None = None) -> list[dict[str, object]]:
    policy = policy or memory_policy_from_env()
    data = _parse_json_object(raw)
    entries = data.get("entries") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        return []
    normalized: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    per_kind_seen: dict[UserMemoryKind, int] = defaultdict(int)
    for item in entries:
        if not isinstance(item, dict):
            continue
        key = _normalize_memory_key(str(item.get("key") or "memory"))
        value = _trim_text(str(item.get("value") or "").strip(), MAX_MEMORY_VALUE_CHARS)
        if not value or key in seen_keys:
            continue
        if not _is_structurally_durable_memory(key, value):
            continue
        try:
            confidence = float(item.get("confidence", 1.0))
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < MIN_MEMORY_CONFIDENCE:
            continue
        try:
            kind = UserMemoryKind(str(item.get("kind") or "fact"))
        except ValueError:
            kind = UserMemoryKind.FACT
        if per_kind_seen[kind] >= policy.limit_for_kind(kind):
            continue
        normalized.append({"key": key, "value": value, "kind": kind})
        seen_keys.add(key)
        per_kind_seen[kind] += 1
        if len(normalized) >= policy.total_limit:
            break
    return normalized


def _is_structurally_durable_memory(key: str, value: str) -> bool:
    """Reject model-proposed memory that is shaped like runtime state.

    This is a post-model structured-output validator. It does not route or
    decompose user intent from prose; it checks the generated memory key/value
    for machine identifiers that should remain operational state.
    """
    key_parts = [part for part in _normalize_memory_key(key).split("_") if part]
    if key_parts and key_parts[-1] == "id":
        return False
    if _has_long_digit_run(value):
        return False
    return True


def _has_long_digit_run(value: str) -> bool:
    run = 0
    for char in value:
        if char.isdigit():
            run += 1
            if run >= MAX_VOLATILE_DIGIT_RUN:
                return True
        else:
            run = 0
    return False


def _parse_json_object(raw: str) -> dict[str, object]:
    text = raw.strip()
    if not text:
        return {}
    if "```" in text:
        lines = text.splitlines()
        inside = False
        kept: list[str] = []
        for line in lines:
            if line.strip().startswith("```"):
                inside = not inside
                continue
            if inside:
                kept.append(line)
        if kept:
            text = "\n".join(kept).strip()
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1:
            return {}
        try:
            parsed = json.loads(text[start : end + 1])
        except (json.JSONDecodeError, ValueError):
            return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_memory_key(value: str) -> str:
    key = "".join(ch if ch.isalnum() else "_" for ch in value.lower()).strip("_")
    while "__" in key:
        key = key.replace("__", "_")
    return (key or "memory")[:60]


def _dt_for_payload(value: datetime | None) -> str:
    return value.isoformat() if value is not None else ""


def _trim_text(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _created_at_for_key(entries: list[UserMemoryEntry], key: str, fallback: datetime) -> datetime:
    for entry in entries:
        if entry.key == key and entry.created_at is not None:
            return entry.created_at
    return fallback


def _entry_for_key(entries: list[UserMemoryEntry], key: str) -> UserMemoryEntry | None:
    for entry in entries:
        if entry.key == key:
            return entry
    return None


def _replacement_memory_entry(
    *,
    owner: str,
    item: dict[str, object],
    existing: list[UserMemoryEntry],
    now: datetime,
) -> UserMemoryEntry:
    key = _normalize_memory_key(str(item["key"]))
    previous = _entry_for_key(existing, key)
    return UserMemoryEntry(
        entry_id=f"{owner}:{key}",
        owner=owner,
        kind=item["kind"],
        key=key,
        value=str(item["value"]),
        source="builder_memory",
        created_at=previous.created_at if previous and previous.created_at is not None else now,
        updated_at=now,
        use_count=int(getattr(previous, "use_count", 0) or 0) if previous else 0,
        use_score=float(getattr(previous, "use_score", 0.0) or 0.0) if previous else 0.0,
        last_used_at=getattr(previous, "last_used_at", None) if previous else None,
    )


def _replace_owner_memory(store, owner: str, replacements: list[UserMemoryEntry]) -> int:
    replacement_ids = {entry.entry_id for entry in replacements}
    existing = _entries_for_owner(store, owner)
    removed = 0
    for entry in existing:
        if entry.entry_id in replacement_ids:
            continue
        if _remove_memory_entry(store, entry.entry_id):
            removed += 1
    return removed


def _remove_memory_entry(store, entry_id: str) -> bool:
    remover = getattr(store, "remove_user_memory_entry", None)
    if callable(remover):
        return bool(remover(entry_id))
    for collection_name in ("user_facts", "preferences", "environment_facts"):
        collection = getattr(store, collection_name, None)
        if isinstance(collection, dict) and entry_id in collection:
            collection.pop(entry_id, None)
            return True
    return False


def _checkpoint_runtime(runtime) -> None:
    checkpoint = getattr(runtime, "checkpoint", None)
    if callable(checkpoint):
        try:
            checkpoint()
        except Exception:
            logger.debug("builder_memory: checkpoint failed", exc_info=True)


__all__ = [
    "BuilderMemoryResult",
    "COMPACTION_THRESHOLD",
    "DEFAULT_MEMORY_LONG_TERM_LIMIT",
    "DEFAULT_MEMORY_MID_TERM_LIMIT",
    "DEFAULT_MEMORY_SHORT_TERM_LIMIT",
    "MemoryPolicy",
    "MAX_MEMORY_ENTRIES_PER_OWNER",
    "RECENT_WINDOW_ENTRIES",
    "compact_memory",
    "manage_turn_memory",
    "memory_pressure_for_owner",
    "memory_policy_from_env",
    "reinforce_memory_entries",
    "select_memory_entries_for_prompt",
    "smart_cleanup_enabled",
    "smart_cleanup_owner_memory",
]
