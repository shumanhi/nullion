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
    assert invoke(tools(restarted, 'workspace_beta'), 'personal_task_save', task_id=record['task_id'], target_title='Return parcel', expected_revision=1, status='completed').status == 'failed'
    done = invoke(tools(other), 'personal_task_save', task_id=record['task_id'], target_title='Return parcel', expected_revision=1, status='completed')
    assert done.output['task']['revision'] == 2
    assert invoke(first, 'personal_task_save', task_id=record['task_id'], target_title='Return parcel', expected_revision=1, status='archived').output['reason'] == 'task_revision_conflict'
    assert invoke(first, 'personal_tasks_list').output['items'] == []
    assert len(invoke(first, 'personal_tasks_list', status='completed').output['items']) == 1
    archived = invoke(first, 'personal_task_save', task_id=record['task_id'], target_title='Return parcel', expected_revision=2, status='archived')
    assert archived.output['persisted']
    restored = invoke(first, 'personal_task_save', task_id=record['task_id'], target_title='Return parcel', expected_revision=3, status='pending')
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
    assert invoke(registry, 'personal_task_save', task_id=task_id, target_title='Parcel', expected_revision=True, status='completed').status == 'failed'
    completed = invoke(registry, 'personal_task_save', task_id=task_id, target_title='Parcel', expected_revision=1, status='completed')
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


@pytest.mark.parametrize('title', ['Parcel return', '荷物の返品', 'Devolver paquete'])
@pytest.mark.parametrize('operation', [None, 'update'])
def test_completing_without_an_existing_target_never_invents_a_task(tmp_path, title, operation):
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry = tools(runtime)
    result = invoke(registry, 'personal_task_save', title=title, status='completed',
        **({'operation':operation} if operation else {}))
    assert result.status == 'failed' and result.output['persisted'] is False
    assert result.output['result_kind'] == 'user_choice_required'
    assert result.output['requires_user_choice'] is True
    assert [row['number'] for row in result.output['recovery_options']] == [1,2,3]
    assert invoke(registry, 'personal_tasks_list', status='all').output['items'] == []


def test_receipts_identify_create_update_and_a_correction_reuses_the_same_record(tmp_path):
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry = tools(runtime)
    created = invoke(registry, 'personal_task_save', operation='create', title='Parcel reference', status='completed')
    task = created.output['task']
    receipt = created.output['action_receipt']
    assert receipt['action'] == 'created' and receipt['object_id'] == task['task_id']
    assert receipt['object_name'] == 'Parcel reference' and 'Created personal task' in receipt['summary']
    updated = invoke(registry, 'personal_task_save', operation='update', task_id=task['task_id'], target_title=task['title'],
        expected_revision=1, title='Parcel return', status='completed')
    assert updated.output['operation'] == 'updated'
    assert updated.output['action_receipt']['object_id'] == task['task_id']
    assert updated.output['action_receipt']['object_name'] == 'Parcel return'
    items = invoke(registry, 'personal_tasks_list', status='all').output['items']
    assert len(items) == 1 and items[0]['revision'] == 2
    assert invoke(registry, 'personal_task_save', operation='create', task_id=task['task_id'], title='Duplicate').status == 'failed'


def test_first_personal_task_write_initializes_schema_without_snapshotting_runtime(tmp_path, monkeypatch):
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    monkeypatch.setattr(type(runtime), 'checkpoint', lambda *a, **k: pytest.fail('Task initialization must not snapshot history'))
    assert invoke(tools(runtime), 'personal_task_save', title='New task').output['persisted']
    assert invoke(tools(bootstrap_persistent_runtime(runtime.checkpoint_path)), 'personal_tasks_list').output['item_count'] == 1


def test_unsupported_storage_is_not_replaced_with_sqlite(tmp_path):
    from types import SimpleNamespace
    path = tmp_path/'state.json'; path.write_text('{"original":true}')
    runtime = SimpleNamespace(checkpoint_path=path)
    from nullion.personal_tasks import PersonalTaskToolRegistry
    registry = PersonalTaskToolRegistry(ToolRegistry(), runtime=runtime, workspace_id='workspace_alpha')
    result = invoke(registry, 'personal_task_save', title='No mutation')
    assert result.status == 'failed' and path.read_text() == '{"original":true}'


@pytest.mark.parametrize('target_title', [None, 'Observatory calibration', '別の作業'])
def test_update_does_not_substitute_an_unrelated_task(tmp_path, target_title):
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry = tools(runtime)
    created = invoke(registry, 'personal_task_save', title='Return parcel').output['task']
    args = dict(task_id=created['task_id'], expected_revision=1, status='completed')
    if target_title is not None: args['target_title'] = target_title
    output = invoke(registry, 'personal_task_save', **args).output
    assert output['requires_user_choice'] and not output['persisted']
    records = invoke(registry, 'personal_tasks_list').output['items']
    assert records[0]['status']=='pending' and records[0]['revision']==1


@pytest.mark.parametrize('reference',['observatory calibration','観測所の校正','calibración del observatorio'])
def test_existing_task_id_and_revision_cannot_substitute_unrequested_target(tmp_path,reference):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools(runtime)
    saved=invoke(registry,'personal_task_save',title='Unrelated native task').output['task']
    from nullion.tools import ToolInvocation
    result=registry.invoke(ToolInvocation('wrong','personal_task_save','operator',{
        'task_id':saved['task_id'],'target_title':saved['title'],'expected_revision':saved['revision'],
        'status':'completed'},flow_context={'current_user_request':reference}))
    assert result.status=='failed' and result.output['persisted'] is False
    assert invoke(registry,'personal_tasks_list').output['items'][0]['revision']==saved['revision']


def test_partial_named_reference_requires_owned_user_quote_and_unique_native_match(tmp_path):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools(runtime)
    saved=invoke(registry,'personal_task_save',title='Return parcel').output['task']
    result=registry.invoke(ToolInvocation('right','personal_task_save','operator',{
        'task_id':saved['task_id'],'target_title':saved['title'],'target_reference':'parcel',
        'expected_revision':saved['revision'],'status':'completed'},
        flow_context={'current_user_request':'The parcel is completed.'}))
    assert result.status=='completed' and result.output['persisted'] is True


@pytest.mark.parametrize('source',['ambiguous','assistant_only','background_only','selected_user'])
def test_task_reference_rejects_ambiguous_or_unowned_history(tmp_path,source):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools(runtime)
    task=invoke(registry,'personal_task_save',title='Return parcel').output['task']
    if source=='ambiguous':invoke(registry,'personal_task_save',title='Track parcel')
    context={'current_user_request':'Mark parcel completed.' if source=='ambiguous' else '1',
        'selected_request_context':{'parent_turn_id':'owned','turns':[{'user':'Return parcel' if source in {'background_only','selected_user'} else 'Other request',
            'assistant':'Return parcel','context_only':source=='background_only'}]}}
    result=registry.invoke(ToolInvocation('write','personal_task_save','operator',{
        'task_id':task['task_id'],'target_title':task['title'],'target_reference':'parcel',
        'expected_revision':task['revision'],'status':'completed'},flow_context=context))
    assert result.status==('completed' if source=='selected_user' else 'failed')
