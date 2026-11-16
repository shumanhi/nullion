"""Bounded foreground writes and coalesced retries for secondary runtime receipts.

Primary actions and approvals still commit before reporting success. This queue
only saves already-produced history/audit evidence; it never re-executes work.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
from pathlib import Path
import sqlite3
import threading
import time

from nullion.runtime_persistence import checkpoint_runtime_conversation_events, checkpoint_runtime_events

logger = logging.getLogger(__name__)
_lock = threading.Lock()
_pending: dict[str, dict[tuple[int, str | None], "_ReceiptBatch"]] = {}
_workers: set[str] = set()


def _busy(exc: sqlite3.Error) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    return (code is not None and code & 255 in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}) or (
        code is None and str(exc).lower() in {"database is locked", "database table is locked"})


@dataclass
class _ReceiptBatch:
    store: object
    path: Path
    conversation_id: str | None = None
    events: dict = field(default_factory=dict)
    audits: dict = field(default_factory=dict)
    turns: dict = field(default_factory=dict)

    def merge(self, other):
        self.events.update(other.events)
        self.audits.update(other.audits)
        self.turns.update(other.turns)

    def save(self, timeout):
        if self.events or self.audits:
            checkpoint_runtime_events(self.store, self.path, tuple(self.events.values()),
                audit_records=tuple(self.audits.values()), timeout_seconds=timeout)
        if self.turns or self.conversation_id:
            checkpoint_runtime_conversation_events(self.store, self.path,
                conversation_events=tuple(self.turns.values()), conversation_id=self.conversation_id,
                timeout_seconds=timeout)


def _enqueue(batch):
    path = str(batch.path.resolve())
    key = (id(batch.store), batch.conversation_id)
    with _lock:
        jobs = _pending.setdefault(path, {})
        if key in jobs:
            jobs[key].merge(batch)
        else:
            jobs[key] = batch
        if path in _workers:
            return
        _workers.add(path)
        threading.Thread(target=_drain, args=(path,), name="runtime-receipts", daemon=True).start()


def _drain(path):
    delay = .05
    while True:
        with _lock:
            jobs = _pending.pop(path, {})
            if not jobs:
                _workers.remove(path)
                return
        busy = []
        for batch in jobs.values():
            try:
                batch.save(.1)
            except sqlite3.Error as exc:
                if not _busy(exc):
                    logger.exception("Secondary runtime receipt persistence failed")
                else:
                    busy.append(batch)
            except Exception:
                logger.exception("Secondary runtime receipt persistence failed")
        if busy:
            with _lock:
                pending = _pending.setdefault(path, {})
                for batch in busy:
                    key = (id(batch.store), batch.conversation_id)
                    if key in pending:
                        # Preserve newer versions of an event when a request
                        # arrived while the old batch was being attempted.
                        batch.merge(pending[key])
                    pending[key] = batch
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
        else:
            delay = .05


def checkpoint_runtime_receipts(runtime, *, store=None, events=(), audit_records=(),
                                conversation_events=(), conversation_id=None) -> bool:
    """Return promptly on SQLITE_BUSY and retain exact rows until the lock clears."""
    path = getattr(runtime, "checkpoint_path", None)
    if path is None:
        runtime.checkpoint()
        return True
    source = store if store is not None else runtime.store
    turns = tuple(conversation_events)
    if conversation_id:
        turns += tuple(source.list_recent_conversation_events(conversation_id, limit=128))
    from nullion.runtime_persistence import _sqlite_collection_item_key
    batch = _ReceiptBatch(source, Path(path), conversation_id,
        {row.event_id: row for row in events},
        {row.record_id: row for row in audit_records},
        {_sqlite_collection_item_key("conversation_events", i, row): dict(row) for i, row in enumerate(turns)})
    with _lock:
        queued = str(batch.path.resolve()) in _workers
    if queued:
        # A later foreground snapshot cannot overtake an older queued write.
        _enqueue(batch)
        return False
    try:
        batch.save(.1)
        return True
    except sqlite3.Error as exc:
        if not _busy(exc):
            raise
        _enqueue(batch)
        return False
