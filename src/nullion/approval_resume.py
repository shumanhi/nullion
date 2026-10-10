"""Durable, at-most-once ownership of an approved continuation.

A repeated tap, another surface, or a restarted adapter must not replay a
mutation. A crash leaves the claim in place: a new request is needed rather
than silently executing an action whose outcome is unknown.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import sqlite3
import threading

_LOCK = threading.Lock()


def claim_approval_resume(runtime, approval_id: str) -> bool:
    path = Path(str(getattr(runtime, "checkpoint_path", "") or ""))
    with _LOCK:
        claimed = getattr(runtime.store, "approval_resume_claims", None)
        if claimed is None:
            claimed = set()
            runtime.store.approval_resume_claims = claimed
        if approval_id in claimed:
            return False
        if path.is_file() and path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
            # This independent table is not rewritten by store checkpoints.
            # INSERT's unique key also arbitrates separate adapter processes.
            with sqlite3.connect(str(path), timeout=10) as conn:
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
    path = Path(str(getattr(runtime, "checkpoint_path", "") or ""))
    if not path.is_file() or path.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
        return False
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='approval_resume_claims'").fetchone():
            return False
        return conn.execute("SELECT 1 FROM approval_resume_claims WHERE approval_id=?", (approval_id,)).fetchone() is not None
