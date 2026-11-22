"""Durable, at-most-once ownership of an approved continuation.

A repeated tap, another surface, or a restarted adapter must not replay a
mutation. A crash leaves the claim in place: a new request is needed rather
than silently executing an action whose outcome is unknown.
"""
from __future__ import annotations

from nullion.sqlite_connections import sqlite_connection

from datetime import UTC, datetime
from pathlib import Path
import threading

_LOCK = threading.Lock()


def claim_approval_resume(runtime, approval_id: str) -> bool:
    return _claim_approved_execution(runtime.store, getattr(runtime, "checkpoint_path", None), approval_id)


def claim_approved_tool_use(store, approval_id: str) -> bool:
    """Consume exact action authority separately from continuation ownership."""
    return _claim_approved_execution(store, getattr(store, "_memory_checkpoint_path", None), "tool-use:" + approval_id)


def _claim_database(checkpoint_path) -> Path | None:
    if not checkpoint_path:
        return None
    path = Path(checkpoint_path)
    if path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
        return path
    # JSON checkpoints are replaced atomically. Keep claims in a separate
    # database so an older in-flight snapshot cannot erase ownership.
    return path.with_name(path.name + ".approval-claims.sqlite3")


def _claim_approved_execution(store, checkpoint_path, approval_id: str) -> bool:
    path = _claim_database(checkpoint_path)
    with _LOCK:
        claimed = getattr(store, "approval_resume_claims", None)
        if claimed is None:
            claimed = set()
            store.approval_resume_claims = claimed
        if approval_id in claimed:
            return False
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            # This independent table is not rewritten by store checkpoints.
            # INSERT's unique key also arbitrates separate adapter processes.
            with sqlite_connection(str(path), timeout=10) as conn:
                conn.execute("""CREATE TABLE IF NOT EXISTS approval_resume_claims (
                    approval_id TEXT PRIMARY KEY, claimed_at TEXT NOT NULL)""")
                result = conn.execute(
                    "INSERT OR IGNORE INTO approval_resume_claims VALUES (?, ?)",
                    (approval_id, datetime.now(UTC).isoformat()),
                )
                if result.rowcount != 1:
                    claimed.add(approval_id)
                    return False
        claimed.add(approval_id)
        return True


def approval_resume_claimed(runtime, approval_id: str) -> bool:
    if approval_id in getattr(runtime.store, "approval_resume_claims", ()):
        return True
    path = _claim_database(getattr(runtime, "checkpoint_path", None))
    if path is None or not path.is_file():
        return False
    with sqlite_connection(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='approval_resume_claims'").fetchone():
            return False
        return conn.execute("SELECT 1 FROM approval_resume_claims WHERE approval_id=?", (approval_id,)).fetchone() is not None
