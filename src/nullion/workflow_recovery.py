"""Request-scoped execution evidence and bounded, language-neutral recovery.

This is shared control state, not an intent classifier. It consumes admitted
model tool calls and tool-owned contracts; it never derives authority from prose.
"""
from __future__ import annotations

from nullion.sqlite_connections import sqlite_connection

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import logging
from functools import lru_cache
from pathlib import Path
import sqlite3
from typing import Mapping

logger = logging.getLogger(__name__)
MAX_RECOVERY_ROUNDS = 2
MAX_CHECKPOINT_BYTES = 64_000


def fingerprint(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


def result_id(result):
    return result.get('invocation_id', '') if isinstance(result, Mapping) else getattr(result, 'invocation_id', '')


def result_value(result, name, default=None):
    return result.get(name, default) if isinstance(result, Mapping) else getattr(result, name, default)


def recovered_failure_ids(results) -> set[str]:
    """Only runtime-owned top-level receipts, never tool output prose/JSON."""
    results = list(results or ())
    completed = {result_id(r) for r in results if result_value(r, 'status') == 'completed'}
    recovered = set()
    for result in results:
        receipts = result_value(result, 'recovered_by', ()) or ()
        if (result_value(result, 'status') in {'failed', 'denied'}
                and isinstance(receipts, (tuple, list)) and receipts
                and all(isinstance(ident, str) and ident for ident in receipts) and set(receipts) <= completed):
            recovered.add(result_id(result))
    return recovered


@dataclass
class WorkflowCheckpoint:
    workflow_id: str
    conversation_id: str
    goal_digest: str
    phase: str = 'executing'
    observations: dict = field(default_factory=dict)
    issues: dict = field(default_factory=dict)
    recovery_rounds: int = 0
    last_recovery_digest: str = ''
    revision: int = 0
    discovered_sources: dict = field(default_factory=dict)

    def observe(self, results, metadata, registry):
        """A successful unrelated call never erases a failed prerequisite."""
        from nullion.tool_repair_contract import tool_repair_kind
        results = list(results)
        from nullion.search_coverage import discovery_ledger
        self.discovered_sources = discovery_ledger(results, previous=self.discovered_sources) or {}
        known = set(self.observations)
        for result in results:
            ident = result.invocation_id
            status = str(result.status)
            previous = self.observations.get(ident)
            if ident in known and not (previous and previous['status'] in {'failed', 'denied'} and status == 'completed'):
                continue
            known.add(ident)
            if status not in {'completed', 'failed', 'denied'}:
                continue
            info = metadata.get(ident, {})
            signature = fingerprint([result.tool_name, info.get('arguments')]) if 'arguments' in info else ''
            output = result.output if isinstance(result.output, Mapping) else {}
            useful = status == 'completed' and output.get('goal_status') != 'blocked' and output.get('terminal_user_action_required') is not True
            self.observations[ident] = {'tool_name': result.tool_name, 'status': status,
                                       'signature': signature, 'useful': useful}
            if useful and signature:
                for failure_id, issue in self.issues.items():
                    if issue['state'] == 'open' and issue['signature'] == signature:
                        issue.update(state='recovered', evidence_ids=[ident])
            if status in {'failed', 'denied'}:
                recovery = output.get('recovery')
                recovery = recovery if isinstance(recovery, Mapping) else {}
                # Tool-owned required reads and registered continuations are
                # recovery opportunities, not permission to execute them.
                kind = tool_repair_kind(result)
                declared = recovery.get('required_tool_names') if kind else ()
                names = list(declared) if isinstance(declared, (list, tuple)) else []
                try:
                    names.extend(registry.get_spec(result.tool_name).continuation_tools)
                except (KeyError, AttributeError):
                    pass
                repairable = status == 'failed' and output.get('terminal_user_action_required') is not True and bool(names or kind or output.get('retryable') is True)
                self.issues[ident] = {'tool_name': result.tool_name, 'signature': signature,
                    'state': 'open', 'reason': str(output.get('failure_reason') or output.get('reason') or 'tool_failed')[:120],
                    'recovery_tools': list(dict.fromkeys(name for name in names if isinstance(name, str)))[:12],
                    'repairable': repairable, 'evidence_ids': []}
            self.revision += 1
        if self.issues:
            # Runtime-owned receipts also reconcile corrected preflight
            # proposals whose arguments necessarily differ from the rejection.
            recovered = recovered_failure_ids(results)
            for result in results:
                if result.invocation_id in recovered and result.invocation_id in self.issues:
                    self.issues[result.invocation_id].update(state='recovered', evidence_ids=list(result.recovered_by))
            self.phase = 'recovering' if self.open_issues else 'executing'
        # Re-apply persisted resolutions to live result objects after a resume.
        for result in results:
            issue = self.issues.get(result.invocation_id)
            if issue and issue['state'] == 'recovered':
                result.recovered_by = tuple(issue['evidence_ids'])

    @property
    def open_issues(self):
        return {key: issue for key, issue in self.issues.items() if issue['state'] == 'open'}

    def recovery_context(self, registry):
        if self.recovery_rounds >= MAX_RECOVERY_ROUNDS:
            return None
        allowed = {spec.name for spec in registry.list_specs()}
        issues = {key: issue for key, issue in self.open_issues.items()
                  if issue['repairable'] and (set(issue['recovery_tools']) & allowed or issue['tool_name'] in allowed)}
        if not issues:
            return None
        digest = fingerprint(issues)
        if digest == self.last_recovery_digest:
            return None
        self.last_recovery_digest = digest
        self.recovery_rounds += 1
        self.phase = 'recovering'
        return {'kind': 'workflow_recovery', 'workflow_id': self.workflow_id,
                'remaining_recovery_rounds': MAX_RECOVERY_ROUNDS - self.recovery_rounds,
                'open_issues': issues,
                'completed_evidence_ids': [key for key, row in self.observations.items() if row['useful']][-48:]}

    def finish(self, results, *, verified=False, suspended=False, failed=False, delivered=False):
        if verified:
            evidence = [r.invocation_id for r in results if r.status == 'completed' and self.observations.get(r.invocation_id, {}).get('useful')]
            for ident, issue in self.open_issues.items():
                if evidence and issue['repairable']:
                    issue.update(state='recovered', evidence_ids=evidence[-48:])
            for result in results:
                issue = self.issues.get(result.invocation_id)
                if issue and issue['state'] == 'recovered':
                    result.recovered_by = tuple(issue['evidence_ids'])
        self.phase = ('awaiting_approval' if suspended else 'blocked' if failed else
                      'delivered' if delivered else 'verified' if verified else 'blocked' if self.open_issues else 'completed')
        self.revision += 1

    def to_dict(self):
        return {'version': 1, 'workflow_id': self.workflow_id, 'conversation_id': self.conversation_id,
                'goal_digest': self.goal_digest, 'phase': self.phase, 'observations': self.observations,
                'issues': self.issues, 'recovery_rounds': self.recovery_rounds,
                'last_recovery_digest': self.last_recovery_digest, 'revision': self.revision,
                **({'discovered_sources': self.discovered_sources} if self.discovered_sources else {})}

    @classmethod
    def restore(cls, value, *, workflow_id, conversation_id, goal_digest):
        if not isinstance(value, Mapping) or any(value.get(key) != expected for key, expected in (
                ('workflow_id', workflow_id), ('conversation_id', conversation_id), ('goal_digest', goal_digest))):
            return cls(workflow_id, conversation_id, goal_digest)
        try:
            if value.get('version') != 1:
                raise ValueError('Unsupported checkpoint')
            observations = value.get('observations')
            issues = value.get('issues')
            if not isinstance(observations, dict) or not isinstance(issues, dict):
                raise ValueError('Invalid evidence')
            if any(not isinstance(row, dict) or row.get('status') not in {'completed', 'failed', 'denied'}
                   for row in observations.values()):
                raise ValueError('Invalid observations')
            if any(not isinstance(row, dict) or row.get('state') not in {'open', 'recovered'}
                   or not isinstance(row.get('evidence_ids'), list)
                   or not isinstance(row.get('recovery_tools'), list)
                   or not isinstance(row.get('signature'), str)
                   for row in issues.values()):
                raise ValueError('Invalid issues')
            return cls(workflow_id, conversation_id, goal_digest, phase=str(value.get('phase', 'executing')),
                       observations=dict(observations), issues={k: dict(v) for k, v in issues.items()},
                       recovery_rounds=min(MAX_RECOVERY_ROUNDS, max(0, int(value.get('recovery_rounds') or 0))),
                       last_recovery_digest=str(value.get('last_recovery_digest') or ''),
                       revision=max(0, int(value.get('revision') or 0)),
                       discovered_sources=dict(value.get('discovered_sources') or {})
                           if isinstance(value.get('discovered_sources'), Mapping) else {})
        except (TypeError, ValueError):
            return cls(workflow_id, conversation_id, goal_digest)



class WorkflowCheckpointStore:
    """Small independent row writes; no whole-runtime snapshot on a chat turn."""
    def __init__(self, path):
        self.path = Path(path)
        with self.connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS workflow_checkpoints (
                workflow_id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL,
                goal_digest TEXT NOT NULL, payload TEXT NOT NULL,
                updated_at TEXT NOT NULL)""")

    def connect(self):
        return sqlite_connection(str(self.path), timeout=.2)

    def load(self, workflow_id, conversation_id, goal_digest):
        with self.connect() as db:
            row = db.execute('SELECT payload FROM workflow_checkpoints WHERE workflow_id=? AND conversation_id=? AND goal_digest=?',
                             (workflow_id, conversation_id, goal_digest)).fetchone()
        return json.loads(row[0]) if row else None

    def save(self, checkpoint):
        value = checkpoint.to_dict()
        payload = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
        if len(payload.encode()) > MAX_CHECKPOINT_BYTES:
            # Keep all unresolved facts and the recovery budget. Completed
            # evidence receipts already remain in normal turn persistence.
            value['observations'] = dict(list(value['observations'].items())[-48:])
            value['issues'] = {key: row for key, row in value['issues'].items() if row['state'] == 'open'}
            payload = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
        if len(payload.encode()) > MAX_CHECKPOINT_BYTES:
            raise ValueError('Workflow checkpoint exceeded its bounded state budget')
        with self.connect() as db:
            db.execute("""INSERT INTO workflow_checkpoints VALUES (?,?,?,?,?)
                ON CONFLICT(workflow_id) DO UPDATE SET
                    payload=excluded.payload, updated_at=excluded.updated_at
                WHERE conversation_id=excluded.conversation_id
                    AND goal_digest=excluded.goal_digest
                    AND CAST(json_extract(excluded.payload, '$.revision') AS INTEGER)
                        > CAST(json_extract(workflow_checkpoints.payload, '$.revision') AS INTEGER)""",
                       (checkpoint.workflow_id, checkpoint.conversation_id, checkpoint.goal_digest, payload,
                        datetime.now(timezone.utc).isoformat()))


@lru_cache(maxsize=8)
def _checkpoint_store(path):
    return WorkflowCheckpointStore(path)


def checkpoint_for_state(state):
    """Allocate lazily for tool work; plain turns do no checkpoint I/O."""
    checkpoint = state.get('workflow_checkpoint')
    if isinstance(checkpoint, WorkflowCheckpoint):
        return checkpoint
    if (not state.get('tool_results') or not state.get('cleanup_scope')
            or not all(key in state for key in ('principal_id', 'conversation_id', 'user_message'))
            or not callable(getattr(state.get('tool_registry'), 'get_spec', None))):
        return None
    saved = state.get('workflow_checkpoint_snapshot') or {}
    goal_digest = fingerprint([state['principal_id'], state['user_message']])
    workflow_id = (saved.get('workflow_id') if isinstance(saved, Mapping)
        and saved.get('conversation_id') == state['conversation_id'] and saved.get('goal_digest') == goal_digest else None)
    checkpoint = WorkflowCheckpoint.restore(saved, workflow_id=workflow_id or state['cleanup_scope'],
        conversation_id=state['conversation_id'], goal_digest=goal_digest)
    state['workflow_checkpoint'] = checkpoint
    return checkpoint


def observe_state(state):
    checkpoint = checkpoint_for_state(state)
    if checkpoint is not None:
        checkpoint.observe(state.get('tool_results') or (), state.get('invocation_metadata') or {}, state['tool_registry'])
    return checkpoint


def persist_state(state):
    checkpoint = state.get('workflow_checkpoint')
    path = getattr(state.get('runtime_store'), '_memory_checkpoint_path', None)
    if checkpoint is None or path is None:
        return
    try:
        _checkpoint_store(str(path)).save(checkpoint)
    except (sqlite3.Error, OSError, ValueError):
        # Progress remains in graph/resume state. A busy diagnostics row must
        # never abort an otherwise completed action or restart that action.
        logger.warning('Could not persist workflow progress', exc_info=True)
