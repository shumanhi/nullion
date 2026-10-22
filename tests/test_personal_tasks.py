from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from nullion.conversation_history_tools import with_conversation_history_tool, _ranked_history_events
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import ToolInvocation, ToolRegistry


def invoke(registry, name, **args):
    return registry.invoke(ToolInvocation('call', name, 'owner', args))


def tools(runtime, workspace='workspace_alpha'):
    return with_conversation_history_tool(ToolRegistry(), runtime=runtime, conversation_id='telegram:user', workspace_id=workspace)


def test_task_records_survive_processes_restart_reset_and_memory_replacement(tmp_path):
    path = tmp_path / 'runtime.db'
    runtime = bootstrap_persistent_runtime(path)
    first = tools(runtime)
    saved = invoke(first, 'personal_task_save', title='Return parcel', priority='high', due_date='2026-10-15')
    assert saved.status == 'completed' and saved.output['persisted'] is True
    # Another already-running surface reads the persisted database rather than its stale store.
    other = bootstrap_persistent_runtime(path)
    runtime.store.preferences.clear()
    runtime.store.user_facts.clear()
    runtime.store.add_conversation_event({'conversation_id': 'telegram:user', 'event_type': 'conversation.session_reset'})
    runtime.checkpoint(force=True)
    assert invoke(tools(other), 'personal_tasks_list').output['items'][0]['title'] == 'Return parcel'
    restarted = bootstrap_persistent_runtime(path)
    record = invoke(tools(restarted), 'personal_tasks_list').output['items'][0]
    assert record['priority'] == 'high' and record['due_date'] == '2026-10-15'
    assert invoke(tools(restarted, 'workspace_beta'), 'personal_tasks_list').output['items'] == []
    assert invoke(tools(restarted, 'workspace_beta'), 'personal_task_save', task_id=record['task_id'], expected_revision=1, status='completed').status == 'failed'
    done = invoke(tools(other), 'personal_task_save', task_id=record['task_id'], expected_revision=1, status='completed')
    assert done.output['task']['revision'] == 2
    assert invoke(first, 'personal_task_save', task_id=record['task_id'], expected_revision=1, status='archived').output['reason'] == 'task_revision_conflict'
    assert invoke(first, 'personal_tasks_list').output['items'] == []
    assert len(invoke(first, 'personal_tasks_list', status='completed').output['items']) == 1
    archived = invoke(first, 'personal_task_save', task_id=record['task_id'], expected_revision=2, status='archived')
    assert archived.output['persisted']
    restored = invoke(first, 'personal_task_save', task_id=record['task_id'], expected_revision=3, status='pending')
    assert restored.output['task']['revision'] == 4
    assert len(invoke(first, 'personal_tasks_list').output['items']) == 1


def test_saved_history_recovers_plain_task_fact_amid_unrelated_link_records(tmp_path):
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    now = datetime.now(UTC)
    for i in range(12):
        runtime.store.add_conversation_event({'conversation_id': 'telegram:user', 'workspace_id': 'workspace_alpha',
            'event_type': 'conversation.chat_turn', 'turn_id': str(i), 'created_at': (now+timedelta(seconds=i)).isoformat(),
            'user_message': f'Keep parcel {i} on lista', 'assistant_reply': f'Parcel {i} pending on lista.'})
    runtime.store.add_conversation_event({'conversation_id': 'telegram:user', 'workspace_id': 'workspace_alpha',
        'event_type': 'conversation.chat_turn', 'turn_id': 'unrelated-link', 'created_at': now.isoformat(),
        'user_message': 'Open unrelated lista catalog https://example.test/catalog', 'assistant_reply': 'Opened lista catalog.'})
    runtime.checkpoint(force=True)
    result = invoke(tools(runtime), 'chat_history_search', query='lista', limit=5)
    assert any(row['turn_id'] != 'unrelated-link' for row in result.output['matches'])
    direct = [row for row in result.output['matches'] if not row.get('context')]
    assert len(direct) == 5  # Neighbours must not crowd out older direct matches.
    saved = invoke(tools(runtime), 'personal_task_save', title='Parcel', source_turn_id='1')
    assert saved.output['persisted']
    duplicate = invoke(tools(runtime), 'personal_task_save', title='Parcel', source_turn_id='1')
    assert duplicate.output['task']['task_id'] == saved.output['task']['task_id']
    assert len(invoke(tools(runtime), 'personal_tasks_list').output['items']) == 1
    assert invoke(tools(runtime, 'workspace_beta'), 'personal_task_save', title='Parcel', source_turn_id='1').status == 'failed'


@pytest.mark.parametrize('args', [{}, {'title': ''}, {'title': 'A', 'status': 'lost'}, {'title': 'A', 'due_date': 'tomorrow'}, {'title': 'A', 'source_turn_id': 'unknown'}])
def test_invalid_task_write_never_reports_saved(tmp_path, args):
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    result = invoke(tools(runtime), 'personal_task_save', **args)
    assert result.status == 'failed' and result.output['persisted'] is False
    assert invoke(tools(runtime), 'personal_tasks_list').output['items'] == []


def test_task_read_scope_does_not_expose_writes_and_revisions_ignore_row_reordering(tmp_path):
    import json
    import sqlite3
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    registry = tools(runtime)
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence())
    result, read = scoped.apply_scope_request(ToolInvocation('scope', 'request_tool_scope', 'owner', {'capabilities': ['personal_tasks']}))
    assert result.output['available_tools'] == ['personal_tasks_list']
    assert not read.can_invoke_tool('personal_task_save')
    _, write = scoped.apply_scope_request(ToolInvocation('write-scope', 'request_tool_scope', 'owner', {'capabilities': ['personal_tasks'], 'tool_names': ['personal_task_save']}))
    assert write.turn_tool_scope_decision.required_tool_names == ('personal_task_save',)
    saved = invoke(registry, 'personal_task_save', title='Parcel')
    task_id = saved.output['task']['task_id']
    assert invoke(registry, 'personal_task_save', task_id=task_id, expected_revision=True, status='completed').status == 'failed'
    completed = invoke(registry, 'personal_task_save', task_id=task_id, expected_revision=1, status='completed')
    assert completed.output['persisted']
    with sqlite3.connect(runtime.checkpoint_path) as connection:
        row = connection.execute("SELECT collection, item_key, payload, updated_at FROM conversation_events WHERE json_extract(payload,'$.task.revision')=1").fetchone()
        connection.execute('INSERT OR REPLACE INTO conversation_events (collection,item_key,payload,updated_at) VALUES (?,?,?,?)', row)
    assert invoke(registry, 'personal_tasks_list').output['items'] == []
    assert invoke(registry, 'personal_tasks_list', status='completed').output['items'][0]['revision'] == 2


@pytest.mark.parametrize('auxiliary_db',[False,True])
def test_fresh_task_storage_reads_empty_and_initializes_on_first_save(tmp_path,auxiliary_db):
    import sqlite3
    path=tmp_path/'runtime.db'
    runtime=bootstrap_persistent_runtime(path)
    if auxiliary_db:
        with sqlite3.connect(path) as connection:
            connection.execute('CREATE TABLE auxiliary_cache(value TEXT)')
    result=invoke(tools(runtime),'personal_tasks_list')
    assert result.status=='completed' and result.output['items']==[]
    saved=invoke(tools(runtime),'personal_task_save',title='Parcel return')
    assert saved.status=='completed' and saved.output['persisted'],saved
    restarted=bootstrap_persistent_runtime(path)
    assert invoke(tools(restarted),'personal_tasks_list').output['items'][0]['title']=='Parcel return'


def test_fresh_empty_task_read_still_validates_status(tmp_path):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    result=invoke(tools(runtime),'personal_tasks_list',status='invalid')
    assert result.status=='failed' and result.output['persisted'] is False
