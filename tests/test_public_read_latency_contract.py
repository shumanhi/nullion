import json
import threading
import time

import pytest


@pytest.mark.parametrize('queries', [['甲','乙','丙'], ['uno','dos','tres'], ['أ','ب','ج']])
def test_search_batch_is_bounded_preserves_all_results_and_partial_failures(queries):
    from nullion.tools import ToolRegistry, ToolInvocation, register_search_plugin
    active, peak = 0, 0
    lock = threading.Lock()
    def search(query, limit):
        nonlocal active, peak
        with lock: active += 1; peak = max(peak, active)
        try:
            time.sleep(.06)
            if query == queries[1]: raise ValueError('Source unavailable')
            return [{'title':query, 'url':'https://example.test/'+query, 'snippet':'Current displayed value: 17'}]
        finally:
            with lock: active -= 1
    registry = ToolRegistry(); register_search_plugin(registry, web_searcher=search)
    progress = []
    result = registry.invoke(ToolInvocation('batch','web_search','member',
        {'queries':queries,'independent_queries':True}, progress_callback=progress.append))
    assert result.status == 'completed' and result.output['completed_queries'] == 2
    assert peak == 2 and active == 0
    assert [row['query_index'] for row in result.output['results']] == [0,2]
    assert [row['status'] for row in result.output['query_outcomes']] == ['completed','failed','completed']
    assert progress[-1] == {'pages_checked':3,'findings_found':2,
        'source_progress': {'unit':'searches','total':3,'completed':2,'failed':1,'records':2}}


@pytest.mark.parametrize('arguments', [
    {'queries':['a','b']}, {'queries':['a',1],'independent_queries':True},
    {'query':'a','queries':['b'],'independent_queries':True},
    {'queries':['a']*7,'independent_queries':True},
    {'queries':['a'],'independent_queries':'true'},
])
def test_invalid_batch_never_calls_provider(arguments):
    from nullion.tools import _build_web_search_handler, ToolInvocation
    def never(*args):raise AssertionError('Invalid arguments cannot start provider work')
    result = _build_web_search_handler(never)(ToolInvocation('bad','web_search','member',arguments))
    assert result.status == 'failed' and result.output['reason'] == 'invalid_tool_arguments'


@pytest.mark.parametrize('cancelled', [False, True])
def test_batch_interruption_keeps_completed_evidence_and_stops_new_work(monkeypatch, cancelled):
    from nullion import tools
    from nullion.public_search_batch import search_batch
    first_completed, release = threading.Event(), threading.Event()
    calls = []
    progress = []
    monkeypatch.setattr(tools, 'tool_execution_remaining_seconds', lambda: 1.08)
    monkeypatch.setattr(tools, 'tool_execution_cancelled',
        lambda: cancelled and first_completed.is_set())
    def read(invocation):
        calls.append(invocation.arguments['query'])
        if invocation.arguments['query'] == '甲':
            first_completed.set()
            return tools.ToolResult(invocation.invocation_id, 'web_search', 'completed',
                {'results': [{'title': '甲', 'url': 'https://example.test/a'}]})
        assert release.wait(2)
        return tools.ToolResult(invocation.invocation_id, 'web_search', 'completed', {'results': []})
    try:
        started = time.monotonic()
        result = search_batch(tools.ToolInvocation('read', 'web_search', 'member',
            {'queries': ['甲', '乙', '丙', '丁'], 'independent_queries': True}, progress_callback=progress.append), read)
        assert time.monotonic() - started < .5
        assert result.status == 'completed' and result.output['completed_queries'] == 1
        assert result.output['results'][0]['url'] == 'https://example.test/a'
        assert len(calls) <= 3
        assert all(row['reason'] == ('request_cancelled' if cancelled else 'source_timeout')
            for row in result.output['query_outcomes'][1:])
        if not cancelled:
            assert progress[-1]['source_progress']['failed'] == 3
    finally:
        release.set()


@pytest.mark.parametrize('saved_history_available', [False, True])
def test_plain_public_reads_are_visible_without_scope_model_connector_inventory_or_io(saved_history_available):
    from nullion.tools import ToolRegistry, ToolSpec, ToolSideEffectClass, ToolRiskLevel, ToolResult
    from nullion.turn_context_policy import build_turn_tool_evidence, turn_tool_registry_for_evidence
    registry = ToolRegistry()
    def never(invocation):raise AssertionError('Listing tools must not execute them')
    for name, effect in [('web_search','read'),('browser_read_pages','read'),('browser_run_task','write'),('email_read','read')]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass(effect),False,1),never)
    class Model:
        def create(self, **kwargs):raise AssertionError('Plain turns must not invoke a scope classifier')
    for prompt in ('こんにちは','Hola','مرحبا'):
        evidence = build_turn_tool_evidence(user_message=prompt,conversation_result=None,
            saved_history_available=saved_history_available)
        scoped = turn_tool_registry_for_evidence(registry,evidence=evidence,model_client=Model(),
            user_message=prompt,skip_tool_scope_decision=True)
        names = {row['name'] for row in scoped.list_tool_definitions()}
        assert {'web_search','browser_read_pages'} <= names
        assert not {'browser_run_task','email_read'} & names
        # Visibility must not masquerade as an already selected mission and
        # prevent later planner/mini-agent materialization of the real request.
        assert not {'web_search', 'browser_read_pages'} & set(scoped.turn_tool_scope_decision.requested_tool_names)
        from nullion.turn_context_policy import _tool_scope_decision_has_substantive_scope
        assert not _tool_scope_decision_has_substantive_scope(scoped.turn_tool_scope_decision)


def test_browser_projection_reduces_exact_duplicates_without_losing_records_or_links():
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    url = 'https://example.test/item?' + 'q=' + 'x'*800
    original = {'result_kind':'browser_page_collection', 'items':[{
        'source_index':i,'title':f'項目{i}','link_text':f'項目{i}', 'url':url+str(i),
        'canonical_url':url+str(i),'price':'$17','unused':None} for i in range(13)],
        'documents':[{'url':'https://example.test/','text':'Current page text',
            'links':[{'url':url,'text':'Exact listing'}]*20, 'link_coverage':{'observed_count':20,'truncated':False}}]}
    before = json.dumps(original, ensure_ascii=False)
    projected = _compact_tool_output_for_model_context('browser_read_pages', original)
    assert json.dumps(original, ensure_ascii=False) == before
    assert len(projected['items']) == 13 and all(row['price']=='$17' for row in projected['items'])
    assert projected['documents'][0]['links'] == [{'url':url,'text':'Exact listing'}]
    assert projected['documents'][0]['link_coverage']['observed_count'] == 20
    assert len(json.dumps(projected,ensure_ascii=False)) < len(before)*.6


def test_search_projection_keeps_every_source_and_exact_long_link():
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    records = [{'title':f'項目{i}','url':'https://example.test/'+str(i)+'?q='+'x'*700,
        'snippet':'Displayed price $17','query_index':i//5} for i in range(13)]
    compact = _compact_tool_output_for_model_context('web_search',{'result_kind':'public_search_results','results':records})
    assert [row['url'] for row in compact['results']] == [row['url'] for row in records]
    assert compact['results_compaction'] == {'shown':13,'total':13}
