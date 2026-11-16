"""Probe observation stops at real terminal SQLite evidence, including failure."""
import json
import os
import sqlite3
import time
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')

@pytest.mark.parametrize('message', ['現在の状態', 'Estado actual', 'État actuel'])
def test_wrong_tool_in_durable_turn_fails_without_full_poll_window(tmp_path, message):
    from scripts import live_stage_chat_contract_probe as probe
    database = tmp_path / 'runtime.db'
    payload = {'conversation_id': 'web:probe-owned', 'event_type': 'conversation.chat_turn',
        'created_at': '2026-11-16T00:00:00+00:00', 'turn_id': 'turn-final',
        'user_message': message, 'tool_results': [{'tool_name': 'run_cron', 'status': 'completed', 'output': {}}]}
    with sqlite3.connect(database) as connection:
        connection.execute('CREATE TABLE conversation_events (payload TEXT, updated_at TEXT)')
        connection.execute('INSERT INTO conversation_events VALUES (?, ?)', (json.dumps(payload), payload['created_at']))
    started = time.perf_counter()
    names = probe._tool_names_for_user_message_with_poll(database, 'web:probe-owned', message,
        required_tool_any=('list_crons',), timeout_seconds=180)
    assert names == {'run_cron'}
    assert time.perf_counter() - started < 1
    with pytest.raises(AssertionError):
        probe._assert_contract(probe.Scenario('typed-status', message, None, 1000,
            required_tool_any=('list_crons',), forbidden_tool_any=('run_cron',)),
            response={'text': 'Unexpected action'}, elapsed_ms=1, tool_names=names,
            runtime_db_available=True)
