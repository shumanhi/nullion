"""Defer scheduled-alert workspace writes until a verified delivery succeeds."""
from __future__ import annotations

import base64
import json
import os
from uuid import uuid4
from pathlib import Path


class CronCheckpointJournal:
    def __init__(self, principal_id: str):
        from nullion.workspace_storage import workspace_storage_roots_for_principal
        self.root = workspace_storage_roots_for_principal(principal_id, create=False).root.resolve()
        self.principal_id = principal_id
        self.entries = {}
        self.directory = self.root / 'scratch' / '.cron-checkpoint-journals'
        self._recover_interrupted_writes()
        self.journal_path = self.directory / (uuid4().hex + '.json')

    def _recover_interrupted_writes(self):
        for file in self.directory.glob('*.json'):
            try:
                data = json.loads(file.read_text())
                try:
                    os.kill(int(data['pid']), 0)
                    continue
                except ProcessLookupError:
                    pass
                entries = data['entries']
                for row in entries:
                    path = Path(row['path'])
                    if not path.is_relative_to(self.root) or path.resolve() != path:
                        continue
                    after = base64.b64decode(row['after'])
                    if not path.is_file() or path.read_bytes() != after:
                        continue
                    before = base64.b64decode(row['before']) if row['before'] is not None else None
                    if before is None:
                        path.unlink()
                    else:
                        path.write_bytes(before)
                file.unlink()
            except (OSError, ValueError, KeyError, TypeError):
                # Preserve unreadable or concurrent state for inspection.
                continue

    def _persist(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        rows = [{'path':str(path),
                 'before':base64.b64encode(entry['before']).decode() if entry['before'] is not None else None,
                 'after':base64.b64encode(entry['after']).decode()}
                for path, entry in self.entries.items() if entry['after'] is not None]
        temporary = self.journal_path.with_suffix('.tmp')
        with temporary.open('w') as stream:
            json.dump({'pid':os.getpid(), 'entries':rows}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.journal_path)

    def before(self, invocation):
        if invocation.tool_name != 'file_write':
            return None
        from nullion.workspace_storage import resolve_virtual_workspace_path_for_principal
        raw = invocation.arguments.get('path')
        if not isinstance(raw, str):
            return None
        path = resolve_virtual_workspace_path_for_principal(raw, self.principal_id, create=False).resolve()
        if not path.is_relative_to(self.root) or path.is_symlink():
            return None
        if path not in self.entries:
            self.entries[path] = {'before': path.read_bytes() if path.is_file() else None, 'after': None}
        content = invocation.arguments.get('content')
        if isinstance(content, str):
            self.entries[path]['after'] = content.encode('utf-8')
            self._persist()
        return path

    def after(self, path, result):
        if path is not None and path.is_file():
            self.entries[path]['after'] = path.read_bytes()
            self._persist()

    def defer(self, deliverables=()):
        deliverables = {Path(p).resolve() for p in deliverables}
        updates = []
        for path, entry in self.entries.items():
            before, after = entry['before'], entry['after']
            if path in deliverables or after is None or before == after:
                continue
            # Never roll back a concurrent writer's newer content.
            if path.resolve() != path or not path.is_file() or path.read_bytes() != after:
                continue
            if before is None:
                path.unlink()
            else:
                path.write_bytes(before)
            updates.append({'path': str(path),
                            'before': base64.b64encode(before).decode() if before is not None else None,
                            'after': base64.b64encode(after).decode()})
        self.journal_path.unlink(missing_ok=True)
        return updates


def commit_delivered_checkpoints(result):
    """Only acknowledge checkpoints after the report's successful receipt."""
    updates = result.pop('cron_pending_file_updates', [])
    if result.get('cron_report_failed') or result.get('cron_run_failed') or result.get('response_fulfilled') is False:
        return
    conflicts = 0
    for update in updates:
        path = Path(update['path'])
        if path.resolve() != path:
            conflicts += 1
            continue
        try:
            before = base64.b64decode(update['before']) if update['before'] is not None else None
            current = path.read_bytes() if path.is_file() else None
            if current != before:
                conflicts += 1
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(update['after']))
        except (OSError, ValueError, KeyError, TypeError):
            # The report already reached the user. A local checkpoint failure
            # must not turn that receipt into a failed delivery or resend it.
            conflicts += 1
    result['cron_checkpoint_commit_conflicts'] = conflicts
