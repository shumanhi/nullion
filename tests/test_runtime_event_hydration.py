"""Restored diagnostics are history, never newly executed observations."""
import pytest

from nullion.runtime_store import RuntimeStore, conversation_event_observer
from nullion.runtime_persistence import load_runtime_store, save_runtime_store, _validate_sqlite_runtime_row


def historical_event():
    return {'event_id': 'historical', 'conversation_id': 'chat:選択',
        'event_type': 'conversation.tool_timing', 'tool_name': 'lookup',
        'cron_id': 'previous-job', 'cron_run_id': 'previous-run'}


@pytest.mark.parametrize('suffix', ['.db', '.json'])
def test_reload_does_not_publish_history_to_active_execution(tmp_path, suffix):
    path = tmp_path / ('runtime' + suffix)
    store = RuntimeStore()
    store.add_conversation_event(historical_event())
    save_runtime_store(store, path)
    seen = []
    def observe(event):
        seen.append(event)
        return {'cron_run_id': 'current-run'}
    with conversation_event_observer(observe):
        restored = load_runtime_store(path)
        assert seen == []
        restored.add_conversation_event({'event_id': 'current', 'conversation_id': 'chat:選択',
            'event_type': 'conversation.model_timing'})
    assert [event['event_id'] for event in seen] == ['current']
    assert restored.list_conversation_events()[0] == historical_event()
    assert restored.list_conversation_events()[1]['cron_run_id'] == 'current-run'


def test_full_checkpoint_merge_does_not_reobserve_persisted_rows(tmp_path):
    path = tmp_path / 'runtime.db'
    store = RuntimeStore()
    store.add_conversation_event(historical_event())
    save_runtime_store(store, path)
    seen = []
    with conversation_event_observer(seen.append):
        save_runtime_store(store, path)
    assert seen == []


def test_row_validation_does_not_publish_a_new_observation():
    seen = []
    with conversation_event_observer(seen.append):
        _validate_sqlite_runtime_row('conversation_events', historical_event())
    assert seen == []


def test_invalid_history_restores_observer_context():
    from nullion.runtime_persistence import _runtime_store_from_payload
    seen = []
    with conversation_event_observer(seen.append):
        with pytest.raises(TypeError):
            _runtime_store_from_payload({'format_version': 1, 'conversation_events': [
                {'event_type': 'invalid-without-conversation'}]})
        RuntimeStore().add_conversation_event(historical_event())
    assert seen == [historical_event()]


def test_cron_observer_cannot_reassign_another_runs_diagnostic(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from nullion import cron_delivery
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import checkpoint_runtime_conversation_events
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    original = historical_event()
    runtime.store.add_conversation_event(original)
    checkpoint_runtime_conversation_events(runtime.store, runtime.checkpoint_path,
        conversation_events=[original])
    def execute(*args, **kwargs):
        # A delayed receipt retains its original ownership even when it is
        # explicitly observed while a different scheduled execution is active.
        runtime.store.add_conversation_event(original)
        runtime.store.add_conversation_event({'event_id': 'current',
            'conversation_id': 'chat:選択', 'event_type': 'conversation.model_timing'})
        return {'text': '報告', 'tool_results': []}
    monkeypatch.setattr(cron_delivery, '_run_single_agent_cron_turn', execute)
    result = cron_delivery.run_single_agent_cron_turn(SimpleNamespace(id='current-job'),
        'chat:選択', label='scheduled', orchestrator=None, runtime=runtime,
        tool_registry=None, settings=None)
    rows = {r['event_id']: r for r in load_runtime_store(runtime.checkpoint_path).list_conversation_events()}
    assert rows['historical'] == original
    assert rows['current']['cron_run_id'] == result['cron_run_id']
