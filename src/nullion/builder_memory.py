"""Builder memory compaction — distils verbose memory entries into tight facts.

The builder observes each turn and learns skills.  Over time, the user memory
store accumulates many raw entries that become redundant, contradictory, or
simply verbose.  This module provides a background pass that:

1.  Reads all memory entries from the runtime store.
2.  Sends them to the LLM as a single compaction prompt.
3.  Receives a distilled replacement list (fewer, tighter entries).
4.  Writes the replacements back, keeping the most recent raw entries intact.

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
from collections import defaultdict
from datetime import UTC, datetime
from uuid import uuid4

from nullion.memory import UserMemoryEntry, UserMemoryKind

logger = logging.getLogger(__name__)

COMPACTION_THRESHOLD = 20      # Only compact when this many entries exist
RECENT_WINDOW_ENTRIES = 5      # Keep the N most-recently-updated entries raw

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

    # Sort by updated_at descending; keep the newest N entries untouched
    def _sort_key(e: UserMemoryEntry):
        ts = e.updated_at or e.created_at
        return ts or datetime.min.replace(tzinfo=UTC)

    sorted_entries = sorted(all_entries, key=_sort_key, reverse=True)
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


__all__ = ["compact_memory", "COMPACTION_THRESHOLD", "RECENT_WINDOW_ENTRIES"]
