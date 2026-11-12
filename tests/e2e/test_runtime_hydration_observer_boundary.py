"""Real checkpoints must not replay history into a live diagnostic sink."""
import json
import os
import statistics
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker persistence boundary')


def test_reload_merge_and_validation_preserve_run_ownership_without_replay(tmp_path, monkeypatch):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_store import conversation_event_observer
    from nullion.runtime_persistence import checkpoint_runtime_conversation_events
    from nullion.sqlite_connections import sqlite_connection
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    path = tmp_path / 'runtime.db'
    runtime = bootstrap_persistent_runtime(path)
    originals = []
    for i in range(600):
        row = {'event_id': f'history-{i}', 'event_type': 'conversation.tool_timing',
            'conversation_id': 'chat:選択', 'created_at': '2026-01-01T00:00:00+00:00',
            'tool_name': 'lookup', 'cron_id': 'previous-job', 'cron_run_id': 'previous-run'}
        originals.append(row)
        runtime.store.add_conversation_event(row)
    runtime.checkpoint(force=True)
    seen, samples = [], []
    def persist_live(event):
        seen.append(event['event_id'])
        metadata = {'cron_id': 'current-job', 'cron_run_id': 'current-run'}
        checkpoint_runtime_conversation_events(runtime.store, path,
            conversation_events=[{**event, **metadata}])
        return metadata
    for i in range(3):
        started = time.monotonic()
        with conversation_event_observer(persist_live):
            loaded = bootstrap_persistent_runtime(path)
            assert seen == [f'live-{n}' for n in range(i)]
            loaded.store.add_conversation_event({'event_id': f'live-{i}',
                'conversation_id': 'chat:選択', 'event_type': 'conversation.model_timing'})
            loaded.checkpoint(force=True)
        samples.append(time.monotonic() - started)
    assert seen == ['live-0', 'live-1', 'live-2']
    with sqlite_connection(path.resolve().as_uri() + '?mode=ro', uri=True) as conn:
        rows = {json.loads(raw)['event_id']: json.loads(raw) for (raw,) in conn.execute(
            "SELECT payload FROM conversation_events WHERE collection='conversation_events'")}
    assert all(rows[row['event_id']] == row for row in originals)
    assert all(rows[f'live-{i}']['cron_run_id'] == 'current-run' for i in range(3))
    assert statistics.mean(samples) < 2 and max(samples) < 3, samples
