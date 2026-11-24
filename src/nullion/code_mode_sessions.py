"""Trusted request-owned Monty checkpoints, separate from model/tool payloads.

SQLite claims survive adapter restarts and are never reset by runtime checkpoints.
A claimed/unknown outcome is deliberately not replayable. Only host-created bytes
from this private journal are loaded; tool arguments cannot supply a snapshot.
"""
from __future__ import annotations

import json
import hashlib
import threading
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

_LOCK = threading.RLock()


class CodeModeSessionMissing(ValueError):
    """The journal has no record for a saved session reference."""


class SessionJournal:
    def __init__(self, state):
        self.owner = [state.get('principal_id', ''), state.get('conversation_id', ''), hashlib.sha256(state.get('user_message', '').encode()).hexdigest()]
        store = state.get('runtime_store')
        self.store = store
        if store is not None:
            if not hasattr(store, '_code_mode_sessions'):
                store._code_mode_sessions = {}
            self.memory = store._code_mode_sessions
        else:
            self.memory = state.setdefault('completion_requirement_contract_cache', {}).setdefault('_code_mode_records', {})

    @property
    def path(self):
        # A native call may attach a formerly in-memory store to its first
        # checkpoint. The session must follow that transition between feeds.
        path = getattr(self.store, '_memory_checkpoint_path', None)
        return Path(path) if path and Path(path).suffix.lower() in {'.db', '.sqlite', '.sqlite3'} else None

    @contextmanager
    def _connection(self):
        from nullion.sqlite_connections import sqlite_connection
        migrated = list(self.memory.items())
        with sqlite_connection(str(self.path), timeout=10) as conn:
            self._table(conn)
            for identity, record in migrated:
                # Existing durable claims win, including another worker's
                # running/blocked state. Never overwrite a claimed revision.
                conn.execute('INSERT OR IGNORE INTO code_mode_sessions VALUES (?, ?, ?)',
                    (identity, record['revision'], json.dumps(record)))
            yield conn
        # Clear only after the transaction commits successfully.
        for identity, record in migrated:
            if self.memory.get(identity) == record:
                self.memory.pop(identity, None)

    @staticmethod
    def _table(conn):
        conn.execute('CREATE TABLE IF NOT EXISTS code_mode_sessions (id TEXT PRIMARY KEY, revision INTEGER NOT NULL, payload TEXT NOT NULL)')

    def read(self, identity):
        with _LOCK:
            if self.path:
                with self._connection() as conn:
                    self._table(conn)
                    row = conn.execute('SELECT payload FROM code_mode_sessions WHERE id=?', (identity,)).fetchone()
                    record = json.loads(row[0]) if row else None
            else:
                record = self.memory.get(identity)
                record = json.loads(json.dumps(record)) if record else None
            if record is None:
                raise CodeModeSessionMissing('code_mode_session_missing')
            if record['owner'] != self.owner:
                raise ValueError('code_mode_session_owner_mismatch')
            return record

    def create(self):
        record = {'id': 'cms-'+uuid4().hex, 'revision': 0, 'owner': self.owner,
                  'phase': 'idle', 'snapshot': None, 'consumed': []}
        with _LOCK:
            if self.path:
                with self._connection() as conn:
                    self._table(conn)
                    conn.execute('INSERT INTO code_mode_sessions VALUES (?, ?, ?)',
                                 (record['id'], 0, json.dumps(record)))
            else:
                self.memory[record['id']] = json.loads(json.dumps(record))
        return record

    def save(self, record, **changes):
        """Compare-and-swap makes a second worker fail before any native effect."""
        updated = {**record, **changes, 'revision': record['revision']+1}
        encoded = json.dumps(updated)
        with _LOCK:
            if self.path:
                with self._connection() as conn:
                    self._table(conn)
                    changed = conn.execute('UPDATE code_mode_sessions SET revision=?,payload=? WHERE id=? AND revision=?',
                        (updated['revision'], encoded, updated['id'], record['revision'])).rowcount
                    if changed != 1:
                        raise RuntimeError('code_mode_session_already_claimed')
            else:
                prior = self.memory[record['id']]
                if prior['revision'] != record['revision']:
                    raise RuntimeError('code_mode_session_already_claimed')
                self.memory[record['id']] = json.loads(encoded)
        record.clear()
        record.update(updated)

    def close(self, identity):
        record = self.read(identity)
        # A wrapper can lose its suspended result while the durable owner is
        # still waiting on an exact approval. Terminal cleanup must leave that
        # program available for the existing approval resume and claim checks.
        if record['phase'] == 'closed':
            return
        if record['phase'] == 'approval':
            from nullion.approval_context import FLOW_TRIGGER_CONTEXT_KEY
            from nullion.approvals import ApprovalStatus

            approval_id = record.get('approval_id')
            read_approval = getattr(self.store, 'get_approval_request', None)
            approval = read_approval(approval_id) if callable(read_approval) and approval_id else None
            if (approval is not None and approval.requested_by == self.owner[0]
                    and approval.status in {ApprovalStatus.PENDING, ApprovalStatus.APPROVED}):
                context = approval.context or {}
                trigger = context.get(FLOW_TRIGGER_CONTEXT_KEY)
                conversations = (context.get('conversation_id'),
                    trigger.get('conversation_id') if isinstance(trigger, dict) else None)
                if all(conversation in (None, '', self.owner[1]) for conversation in conversations):
                    return
        # Keep the claim/tombstone, not source data or executable bytes.
        self.save(record, phase='closed', snapshot=None, pending=None, selected=None, owner=self.owner)


def checkpoint_reference(rows):
    return next((row.get('_code_mode_session_id') for row in rows or ()
                 if isinstance(row, dict) and isinstance(row.get('_code_mode_session_id'), str)), None)
