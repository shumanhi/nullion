from concurrent.futures import ThreadPoolExecutor
import pytest

from nullion.runtime_store import RuntimeStore, conversation_event_observer


def test_cron_runner_preserves_explicit_cancellation_interface():
    import inspect
    from nullion.cron_delivery import run_single_agent_cron_turn
    assert 'cancellation_checker' in inspect.signature(run_single_agent_cron_turn).parameters


def test_execution_context_is_bound_to_scheduled_runs_only():
    from nullion.agent_orchestrator import _completion_review_request_context
    descriptor = {'current_datetime': '2026-01-01T00:00:00+00:00', 'schedule': '5 * * * *',
        'enabled': True, 'schedule_changes_permitted': False}
    assert _completion_review_request_context({'tool_flow_context': {
        'scheduled_execution_context': descriptor}}) == {}
    assert _completion_review_request_context({'tool_flow_context': {'scheduled_task_run': True,
        'scheduled_execution_context': descriptor}})['scheduled_execution_context'] == descriptor


def test_event_observer_is_execution_scoped_and_cannot_mutate_stored_event():
    store = RuntimeStore()
    outer, inner = [], []
    def observe(event):
        outer.append(event['event_type'])
        event['event_type'] = 'changed-copy'
    with conversation_event_observer(observe):
        store.add_conversation_event({'conversation_id': 'one', 'event_type': 'first'})
        with conversation_event_observer(inner.append):
            store.add_conversation_event({'conversation_id': 'one', 'event_type': 'second'})
        # An unrelated worker must not inherit another execution's sink.
        with ThreadPoolExecutor() as pool:
            pool.submit(store.add_conversation_event,
                {'conversation_id': 'other', 'event_type': 'unrelated'}).result()
        store.add_conversation_event({'conversation_id': 'one', 'event_type': 'third'})
    store.add_conversation_event({'conversation_id': 'one', 'event_type': 'outside'})
    assert outer == ['first', 'third']
    assert [row['event_type'] for row in inner] == ['second']
    assert [row['event_type'] for row in store.list_conversation_events()] == [
        'first', 'second', 'unrelated', 'third', 'outside']


def test_failed_diagnostic_sink_does_not_break_the_execution(caplog):
    store = RuntimeStore()
    def unavailable(event):
        raise OSError('unavailable test store')
    with conversation_event_observer(unavailable):
        store.add_conversation_event({'conversation_id': 'one', 'event_type': 'stored'})
    assert len(store.list_conversation_events()) == 1
    assert 'Conversation diagnostic persistence failed' in caplog.text


def test_observer_metadata_survives_later_runtime_checkpoint(tmp_path):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import checkpoint_runtime_conversation_events
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    def persist(event):
        metadata = {'cron_run_id': 'owned-run', 'event_type': 'cannot-replace'}
        checkpoint_runtime_conversation_events(runtime.store, runtime.checkpoint_path,
            conversation_events=[{**metadata, **event, 'cron_run_id': 'owned-run'}])
        return metadata
    with conversation_event_observer(persist):
        runtime.store.add_conversation_event({'event_id': 'one', 'conversation_id': 'one', 'event_type': 'timing'})
    runtime.checkpoint()
    rows = bootstrap_persistent_runtime(runtime.checkpoint_path).store.list_conversation_events('one')
    assert len(rows) == 1 and rows[0]['cron_run_id'] == 'owned-run'
    assert rows[0]['event_type'] == 'timing'




def test_cron_repair_budget_retains_late_check_scope_and_identity():
    import json
    from nullion.cron_delivery import _cron_final_repair_tool_evidence
    from nullion.tools import ToolResult
    results = [ToolResult(str(i), 'email_read', 'completed', {'message': {
        'id': 'message-' + str(i), 'subject': 'Obligation ' + str(i), 'date': '2026-10-26',
        'body': 'A clearly dated obligation. ' + ('Additional context. ' * 300)}}) for i in range(24)]
    results.append(ToolResult('calendar', 'calendar_list', 'completed', {
        'start': '2026-11-13', 'end': '2026-11-15', 'query': 'selected obligation', 'result_count': 1,
        'results': [{'id': 'existing-appointment', 'summary': 'Selected obligation',
            'start': {'dateTime': '2026-11-14T09:00:00-05:00'}, 'end': {'dateTime': '2026-11-14T09:30:00-05:00'}}]}))
    evidence = json.loads(_cron_final_repair_tool_evidence(results))
    calendar = next(row for row in evidence if row['tool_name'] == 'calendar_list')
    assert calendar['source_scope']['result_count'] == 1
    assert calendar['source_scope']['start'] == '2026-11-13'
    assert calendar['items'][0]['source_record_id'] == 'existing-appointment'
    assert calendar['items'][0]['start']['dateTime'] == '2026-11-14T09:00:00-05:00'
    assert len([row for row in evidence if row['tool_name'] == 'email_read']) == 24
    assert len(json.dumps(evidence)) < 32000


@pytest.mark.parametrize('next_limit', [1, 10])
def test_connector_email_search_exposes_actual_page_coverage(monkeypatch, next_limit):
    from types import SimpleNamespace
    from nullion import tools
    calls = []
    monkeypatch.setattr(tools, '_default_email_connector_provider_id', lambda *_: 'selected')
    monkeypatch.setattr(tools, '_connector_connection_for_invocation', lambda *_:
        SimpleNamespace(provider_profile='https://api.example.test', permission_mode='read'))
    monkeypatch.setattr(tools, '_email_messages_endpoint_for_provider', lambda *_: 'https://api.example.test/messages')
    def network(invocation, *, provider_id, url, params=None, **kwargs):
        calls.append(params)
        if url.endswith('/messages'):
            return {'messages': [{'id': '二'}] if params.get('pageToken') else [{'id': '一'}],
                **({} if params.get('pageToken') else {'nextPageToken': 'opaque-next'}), 'resultSizeEstimate': 2}
        return {'id': __import__('urllib.parse', fromlist=['unquote']).unquote(url.rsplit('/', 1)[-1]), 'payload': {'headers': [{'name': 'Subject', 'value': '期限'}]}}
    monkeypatch.setattr(tools, '_connector_json_request', network)
    handler = tools._build_connector_email_search_handler()
    first = handler(tools.ToolInvocation('a', 'email_search', 'web:選択', {'query': '期限', 'limit': 10}))
    assert first.output['has_more'] is True and first.output['result_count'] == 1
    continuation=first.output['next_page_request']
    assert continuation=={'tool_name':'email_search','arguments':{
        'query':'期限','limit':10,'provider_id':'selected','page_token':'opaque-next'}}
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context, _compact_tool_output_for_repair
    for project in (_compact_tool_output_for_model_context, _compact_tool_output_for_repair):
        assert project('email_search',first.output)['next_page_request']==continuation
    second = handler(tools.ToolInvocation('b', 'email_search', 'web:選択', {'query': '期限', 'limit': next_limit,
        'page_token': first.output['next_page_token']}))
    assert second.output['has_more'] is False and second.output['next_page_token'] is None
    assert second.output['next_page_request'] is None
    assert first.output['source_pagination']['cursor'] is None
    assert second.output['source_pagination']['cursor'] == 'opaque-next'
    from nullion.workflow_reasoning import source_pagination_summary
    coverage = source_pagination_summary([first, second])[0]
    assert coverage['complete'] is True and coverage['linked_result_count'] == 2
    assert second.output['results'][0]['id'] == '二'
    assert any(row.get('pageToken') == 'opaque-next' for row in calls)
    assert 'page_token' in tools._email_search_input_schema(supports_pagination=True)['properties']
    assert 'page_token' not in tools._email_search_input_schema()['properties']


def test_structured_checks_preserve_configured_model_and_profile(monkeypatch):
    from dataclasses import dataclass
    from nullion.workflow_reasoning import structured_check_model_client
    @dataclass(frozen=True)
    class Adapter:
        model: str = 'configured-model'
        reasoning_effort: str = 'high'
        credential: str = 'opaque-credential'
    original = Adapter()
    checked = structured_check_model_client(original)
    assert checked is not original and checked.reasoning_effort == 'low'
    assert checked.model == original.model and checked.credential == original.credential
    assert original.reasoning_effort == 'high'
    monkeypatch.setenv('NULLION_WORKFLOW_CHECK_REASONING_EFFORT', 'inherit')
    assert structured_check_model_client(original) is original
    monkeypatch.setenv('NULLION_WORKFLOW_CHECK_REASONING_EFFORT', 'medium')
    assert structured_check_model_client(original).reasoning_effort == 'medium'
    assert structured_check_model_client(object()) is not None


def test_large_scheduler_inventory_stays_structured_and_pageable(monkeypatch):
    import json
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.tools import ToolResult
    monkeypatch.setenv('NULLION_MODEL_TOOL_RESULT_MAX_CHARS', '10000')
    rows = [{'id': str(i), 'name': '項目' + str(i), 'enabled': True, 'task': 'Internal task details. ' * 1000,
        'selection_index': i + 1, 'schedule_description': 'Hourly', 'tracker_status': {'details': 'Health context ' * 1000}} for i in range(100)]
    payload = json.loads(_tool_result_message_payload(ToolResult('r', 'list_crons', 'completed',
        {'crons': rows, 'count': 100, 'offset': 0, 'limit': 100, 'matched_count': 120,
            'has_more': True, 'next_offset': 100, 'inventory_version': 'stable'})))
    output = payload['output']
    assert isinstance(output, dict) and output['crons'][0]['id'] == '0'
    assert output['next_offset'] == len(output['crons']) and output['has_more'] is True
    assert output['inventory_version'] == 'stable'
    assert 'Internal task details' not in json.dumps(output)


def test_review_projection_keeps_nested_record_with_text_receipt_and_source_scope():
    from nullion.agent_orchestrator import _compact_tool_output_for_repair
    output = {'id':'source', 'provider_id':'configured', 'message':{'id':'source','subject':'記録',
        'body':'支払い期限は2026-11-14。金額は41。'},
        'summary':'Read complete', 'evidence_scope':{'message_sent_at':'2026-10-26T10:00:00Z'}}
    for name in ('email_read', 'opaque_adapter_read'):
        projected = _compact_tool_output_for_repair(name, output)
        assert projected['message'] == output['message']
        assert projected['evidence_scope'] == output['evidence_scope']


def test_review_budget_keeps_typed_collections_and_marks_omitted_content():
    import json
    from nullion.agent_orchestrator import _compact_tool_evidence_for_repair
    from nullion.tools import ToolResult
    result = ToolResult('record', 'opaque_adapter_read', 'completed', {'result_count':1,
        'results':[{'id':'entry','summary':'記録'*1000,'start':{'dateTime':'2026-11-14T09:00:00Z'}}]})
    evidence = json.loads(_compact_tool_evidence_for_repair([result], limit=600))[0]
    assert isinstance(evidence['output'], dict)
    assert evidence['output']['result_count'] == 1
    assert isinstance(evidence['output']['results'], list)
    assert evidence['output_omitted_paths']
    assert evidence['output']['results'][0]['start']['dateTime'] == '2026-11-14T09:00:00Z'


def test_scheduler_history_sample_does_not_skip_unseen_page_rows():
    from nullion.scheduler_context_compaction import compact_list_crons_output_for_context
    output = {'crons':[{'id':str(i),'name':'記録'+str(i)} for i in range(40)],
        'offset':40, 'limit':40,'matched_count':120,'inventory_version':'snapshot', 'has_more':True,'next_offset':80}
    context = compact_list_crons_output_for_context(output, max_rows=12)
    assert len(context['crons']) == 12
    assert context['context_sample_only'] is True and context['next_offset'] == 40
    assert context['source_page_next_offset'] == 80 and context['inventory_version'] == 'snapshot'


def test_review_projection_preserves_validated_source_consent():
    from nullion.agent_orchestrator import _compact_tool_output_for_repair, _completion_review_scope_contract
    from nullion.tools import ToolResult
    output={'message':'Scope updated', 'source_evidence_verified':True, 'connector_source_user_requested':True,
        'source_selection_required':False, 'source_resolution_required':False, 'available_tools':['email_read'],
        'connector_source_evidence':'選択したメール', 'requested_required_tool_names':['email_read']}
    compact=_compact_tool_output_for_repair('request_tool_scope', output)
    assert compact['source_evidence_verified'] is True and compact['source_selection_required'] is False
    result=ToolResult('scope','request_tool_scope','completed',output)
    contract=_completion_review_scope_contract(None,[result])
    assert contract['source_evidence_verified'] is True and contract['connector_source_user_requested'] is True
    output['source_evidence_verified']='true'
    assert _completion_review_scope_contract(None,[result])['source_evidence_verified'] is False


def test_explicit_schedule_timezone_is_validated_without_language_routing():
    import pytest
    from nullion.crons import _validated_schedule_timezone
    assert _validated_schedule_timezone(' UTC ')=='UTC'
    assert _validated_schedule_timezone('Asia/Tokyo')=='Asia/Tokyo'
    for value in ('',None,17,'Missing/Invalid'):
        with pytest.raises(ValueError):_validated_schedule_timezone(value)


def test_bounded_evidence_never_changes_query_or_date_scope():
    from nullion.workflow_reasoning import compact_evidence_value
    query='after:2026/10/12 before:2026/10/27 {'+' OR '.join('value'+str(i) for i in range(20))+'}'
    value={'query':query,'start':'2026-10-12T00:00:00Z','end':'2026-10-27T00:00:00Z',
        'result_count':0,'results':[],'text':'Long observation. '*1000}
    compact,omitted=compact_evidence_value(value,max_chars=600)
    assert compact['query']==query and compact['start']==value['start'] and compact['end']==value['end']
    assert ['text'] in omitted
    too_small,_=compact_evidence_value(value,max_chars=20)
    assert too_small is None


def test_review_inventory_retains_every_observed_identity():
    import json
    from nullion.agent_orchestrator import _compact_tool_evidence_for_repair
    from nullion.tools import ToolResult
    rows=[{'id':str(i),'name':'記録'+str(i),'selection_index':i+1,'enabled':False,
        'task':'Saved internal instructions. '*200,'schedule_description':'Daily'} for i in range(62)]
    result=ToolResult('inventory','list_crons','completed',{'crons':rows,'count':62,'matched_count':62,'has_more':False})
    evidence=json.loads(_compact_tool_evidence_for_repair([result],limit=16000))[0]['output']
    assert [row['id'] for row in evidence['crons']]==[str(i) for i in range(62)]
    assert all('task' not in row for row in evidence['crons'])
    assert evidence['has_more'] is False


def test_evidence_inspection_is_bound_to_visible_retained_results():
    import pytest
    from nullion.workflow_reasoning import inspect_review_evidence
    from nullion.tools import ToolResult
    result=ToolResult('actual','record_read','completed',{'message':{'body':'Selected full record'},'values':['first','second']})
    kwargs={'results':[result],'references':{'e0001':'actual'},'visible_ids':{'e0001'}}
    inspected=inspect_review_evidence([{'invocation_id':'e0001','json_pointer':'/message/body','offset':9,'length':4}],**kwargs)
    assert inspected==[{'invocation_id':'e0001','json_pointer':'/message/body','offset':9,'total_chars':20,
        'shown_chars':4,'complete':False,'encoding':'text','content':'full'}]
    assert inspect_review_evidence([{'invocation_id':'e0001','json_pointer':'/values/1'}],**kwargs)[0]['content']=='second'
    for request in ({'invocation_id':'other-turn','json_pointer':''},
            {'invocation_id':'e0001','json_pointer':'/missing'},
            {'invocation_id':'e0001','json_pointer':'/message/body','offset':999},
            {'invocation_id':'e0001','json_pointer':'/values/-1'}):
        with pytest.raises(ValueError):inspect_review_evidence([request],**kwargs)


def test_inspection_batches_small_records_within_one_shared_character_budget():
    from nullion.workflow_reasoning import inspect_review_evidence
    from nullion.tools import ToolResult
    rows=[ToolResult(str(i),'record_read','completed',{'body':'record-'+str(i)}) for i in range(128)]
    requests=[{'invocation_id':str(i),'json_pointer':'/body'} for i in range(128)]
    result=inspect_review_evidence(requests,results=rows,references={},visible_ids={r.invocation_id for r in rows})
    assert len(result)==128 and all(row['complete'] for row in result)
    import pytest
    from jsonschema.exceptions import ValidationError
    with pytest.raises(ValidationError):
        inspect_review_evidence([*requests,requests[0]],results=rows,references={},visible_ids={r.invocation_id for r in rows})
    huge=[ToolResult(str(i),'record_read','completed',{'body':'x'*13000}) for i in range(3)]
    capped=inspect_review_evidence(requests[:3],results=huge,references={},visible_ids={'0','1','2'})
    assert sum(row['shown_chars'] for row in capped)==24000
    assert capped[-1]['inspection_status']=='budget_exhausted' and capped[-1]['complete'] is False


def test_large_review_deadline_accounts_for_actual_input_without_slowing_small_turns(monkeypatch):
    from nullion.agent_orchestrator import _completion_review_request_timeout
    monkeypatch.delenv('NULLION_COMPLETION_REVIEW_TIMEOUT_SECONDS',raising=False)
    assert _completion_review_request_timeout(1100,request_chars=7000)==20
    assert _completion_review_request_timeout(1100,request_chars=106000)==58
    assert _completion_review_request_timeout(1100,request_chars=200000)==60
    monkeypatch.setenv('NULLION_COMPLETION_REVIEW_TIMEOUT_SECONDS','9')
    assert _completion_review_request_timeout(1100,request_chars=106000)==9


def test_monitor_history_reads_exact_persisted_job_beyond_unrelated_recent_turns(tmp_path):
    from types import SimpleNamespace
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.crons import CronJob
    from nullion.cron_delivery import _cron_prior_delivery_context, checkpoint_cron_delivery_records
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    job=CronJob(id='selected',name='観測',schedule='0 * * * *',task='新しい観測を通知。',
        delivery_channel='web',delivery_target='web:history',artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'})
    receipts=[{'event_id':'selected-old','event_type':'conversation.chat_turn','conversation_id':'web:history',
        'created_at':'2026-01-01T00:00:00Z','cron_id':job.id,'assistant_reply':'Earlier delivered observation: 6.'}]
    receipts += [{'event_id':'noise'+str(i),'event_type':'conversation.chat_turn','conversation_id':'web:history',
        'created_at':'2026-02-01T00:00:00Z','cron_id':'other','assistant_reply':'Unrelated private task.'} for i in range(60)]
    receipts.append({'event_id':'foreign','event_type':'conversation.chat_turn','conversation_id':'web:other',
        'created_at':'2026-02-01T00:00:00Z','cron_id':job.id,'assistant_reply':'Other conversation.'})
    checkpoint_cron_delivery_records(runtime,conversation_events=receipts)
    # Deliberately stale in-memory state: history must come from persisted truth.
    assert not runtime.store.list_conversation_events('web:history')
    context=_cron_prior_delivery_context(job,runtime,'web:history')
    assert context['history_read_kind']=='persisted_task_filtered'
    assert context['receipt_count']==1 and context['receipts'][0]['report']=='Earlier delivered observation: 6.'
    assert context['no_prior_delivered_messages'] is False
    runtime.store.add_conversation_event({'event_id':'selected-fresh','event_type':'conversation.chat_turn',
        'conversation_id':'web:history','created_at':'2026-03-01T00:00:00Z','cron_id':job.id,
        'assistant_reply':'Just delivered observation: 7.'})
    fresh=_cron_prior_delivery_context(job,runtime,'web:history')
    assert [row['report'] for row in fresh['receipts']]==[
        'Earlier delivered observation: 6.','Just delivered observation: 7.']
    empty=_cron_prior_delivery_context(CronJob(id='new',name='新規',schedule='0 * * * *',task='確認',
        delivery_channel='web',delivery_target='web:history',artifact_delivery_options=job.artifact_delivery_options),runtime,'web:history')
    assert empty['history_available'] is True and empty['no_prior_delivered_messages'] is True
    unavailable=_cron_prior_delivery_context(job,SimpleNamespace(checkpoint_path=tmp_path/'missing.db',store=None),'web:history')
    assert unavailable['history_available'] is False and unavailable['no_prior_delivered_messages'] is False
    assert not (tmp_path/'missing.db').exists()


def test_scheduler_detail_sample_preserves_complete_identity_timing_index():
    from nullion.scheduler_context_compaction import compact_list_crons_output_for_context
    rows=[{'id':str(i), 'name':'記録 '+str(i), 'selection_index':i+1, 'enabled':False,
        'schedule_description':'At midnight on day 1 in January', 'task':'private instructions'*1000,
        'tracker_status':{'last_issue':'private diagnostic'*100}} for i in range(69)]
    output={'crons':rows,'offset':0,'limit':100,'matched_count':69,'has_more':False,'next_offset':None}
    compact=compact_list_crons_output_for_context(output)
    assert len(compact['crons'])==12
    assert compact['inventory_index_complete'] is True and compact['source_page_has_more'] is False
    assert [r['id'] for r in compact['inventory_index']]==[r['id'] for r in rows]
    assert compact['inventory_index'][-1]['name']=='記録 68'
    assert all(r['schedule_description']==rows[0]['schedule_description'] for r in compact['inventory_index'])
    assert all('task' not in r and 'tracker_status' not in r for r in compact['inventory_index'])


def test_scheduler_identity_index_budget_never_claims_complete_when_truncated():
    from nullion.scheduler_context_compaction import compact_list_crons_output_for_context
    rows=[{'id':str(i),'name':'名'*1000,'enabled':False} for i in range(100)]
    compact=compact_list_crons_output_for_context({'crons':rows,'offset':100,'has_more':True,'next_offset':200})
    assert 0<len(compact['inventory_index'])<100
    assert compact['inventory_index_complete'] is False and compact['source_page_has_more'] is True
    assert compact['next_offset']==100
