import pytest

from nullion.tools import ToolInvocation, ToolResult, ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass


def calendar_result():
    return ToolResult('cal-37','calendar_list','completed',{'results':[{'id':'stay-37','summary':'Hotel',
        'start':{'dateTime':'2026-11-21T15:00:00-05:00'},'end':{'dateTime':'2026-11-22T12:00:00-05:00'}}]})


@pytest.mark.parametrize('label',['Hotel details','ホテルの詳細','تفاصيل الفندق'])
def test_link_state_omits_wrong_dates_without_rewriting_or_changing_facts(label):
    from nullion.source_link_state import review_links
    bad='https://property.test/booking?arrival=2026-12-13&departure=2026-12-14'
    good='https://property.test/details'
    draft=f'Check-in: 2026-11-21. Check-out: 2026-11-22.\n[{label}]({bad})\n[Reference]({good})'
    calls=[]
    class Client:
        def create(self,**kw):
            calls.append(kw)
            assert kw['tools'][0]['name']=='review_source_link_state'
            return {'content':[{'type':'tool_use','name':'review_source_link_state','input':{
                'decisions':[{'id':'0','disposition':'omit'}]}}]}
    cache={}
    revised,receipt=review_links(draft,request='宿泊の詳細を調べてください。',results=[calendar_result()],client=Client(),cache=cache)
    assert bad not in revised and good in revised
    assert '2026-11-21' in revised and '2026-11-22' in revised
    assert receipt['omitted_link_count']==1
    assert review_links(draft,request='宿泊の詳細を調べてください。',results=[calendar_result()],client=Client(),cache=cache)[0]==revised
    assert len(calls)==1


def test_link_state_keeps_background_references_and_skips_matching_or_unrelated_turns():
    from nullion.source_link_state import review_links
    called=[]
    class Client:
        def create(self,**kw):
            called.append(kw)
            return {'content':[{'type':'tool_use','name':'review_source_link_state','input':{
                'decisions':[{'id':'0','disposition':'background_reference'}]}}]}
    background='Background article: [Archived source](https://news.test/?published=2024-01-01)'
    assert review_links(background,request='Compare background',results=[calendar_result()],client=Client(),cache={})[0]==background
    for text,results in [('[Stay](https://site.test/?a=2026-11-21&b=2026-11-22)',[calendar_result()]),
                         (background,[]),('Hello',[calendar_result()])]:
        assert review_links(text,request='x',results=results,client=Client(),cache={})[0]==text
    assert len(called)==1


def test_unreviewable_bare_link_is_omitted_without_research_or_changing_other_links():
    from nullion.source_link_state import review_links
    wrong='https://hotel.test/?a=2026-12-13&b=2026-12-14'
    text='2026-11-21 → 2026-11-22\n'+wrong+'\nhttps://hotel.test/info'
    revised,receipt=review_links(text,request='宿泊情報',results=[calendar_result()],client=None,cache={})
    assert wrong not in revised and 'https://hotel.test/info' in revised
    assert '2026-11-21' in revised and receipt['omitted_link_count']==1


def test_cache_invalidates_observation_data_after_a_native_page_operation():
    from nullion.plugins.browser_plugin.browser_reads import BrowserReadFirstPolicy
    policy=BrowserReadFirstPolicy();inv=ToolInvocation('read','browser_read_pages','owner',{},capsule_id='request37')
    native=ToolResult('read','browser_read_pages','completed',{'pages':[{'requested_url':'https://page.test'}]})
    policy.remember_result(inv,native)
    assert policy.read_result(inv,'https://page.test') is not None
    policy.invalidate_reads(inv)
    assert policy.read_result(inv,'https://page.test') is None


def test_adaptive_execution_requires_an_explicit_reason_before_loading_worker():
    from nullion.plugins.browser_plugin.browser_use_adapter import browser_task_handler
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    result=browser_task_handler(lambda:pytest.fail('Invalid arguments must not load a model'),BrowserPolicy())(
        ToolInvocation('bad','browser_run_task','owner',{'url':'https://page.test','task':'確認',
            'execution_kind':'adaptive_interaction'}))
    assert result.status=='failed' and result.output['recovery']['kind']=='correct_arguments'


def scoped_capture(request, *, broad_helpers=False):
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision
    registry=ToolRegistry()
    for name,formats,source,effect in [('c7',(),True,ToolSideEffectClass.READ),
            ('g9',('.png',),False,ToolSideEffectClass.WRITE),('p3',('.pdf',),False,ToolSideEffectClass.WRITE)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,5,
            produced_artifact_extensions=formats,source_artifact_only=source,
            source_artifact_extensions=('.png',) if source else (),capability_tags=('local_files','explicit_scope_only','required_scope_only')),
            lambda inv:pytest.fail('Scope inspection must not execute tools'))
    decision=TurnToolScopeDecision(valid=True,source_artifact_delivery_requested=True,
        source_artifact_request_evidence=request,requested_artifact_extensions=('.png',),
        requested_tool_names=('c7','g9') if broad_helpers else ('c7',),required_tool_names=('c7',))
    return ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(current_user_message=request),tool_scope_decision=decision)


@pytest.mark.parametrize('prompt',['Return the source PNG','元のPNGを返してください。','Devuelve la captura original.'])
def test_capture_contract_survives_scope_reset_and_does_not_trust_discovered_writers(prompt):
    scoped=scoped_capture(prompt,broad_helpers=True)
    assert not scoped.can_invoke_tool('g9')
    result,next_scope=scoped.apply_scope_request(ToolInvocation('expand','request_tool_scope','operator',{
        'capabilities':['local_files'],'tool_names':['g9'],'required_tool_names':['g9'],
        'artifact_extensions':['.png'],'source_artifact_delivery_requested':False}))
    assert result.status=='failed' and result.output['reason']=='source_artifact_generation_conflict'
    assert next_scope is scoped and not next_scope.can_invoke_tool('g9')


def test_capture_contract_allows_a_distinct_report_format_and_survives_serialization():
    from nullion.turn_context_policy import _tool_scope_decision_from_payload,_tool_scope_decision_to_payload
    scoped=scoped_capture('元の画像と資料')
    result,widened=scoped.apply_scope_request(ToolInvocation('report','request_tool_scope','operator',{
        'capabilities':['local_files'],'tool_names':['p3'],'required_tool_names':['p3'],
        'artifact_extensions':['.pdf'],'source_artifact_delivery_requested':False}))
    assert result.status=='completed' and widened.can_invoke_tool('p3'), result
    assert widened.turn_tool_scope_decision.source_artifact_extensions==('.png',)
    restored=_tool_scope_decision_from_payload(_tool_scope_decision_to_payload(widened.turn_tool_scope_decision))
    assert restored.source_artifact_extensions==('.png',)


@pytest.mark.parametrize('kind',['replacement','source','earlier','missing','different_format'])
def test_method_reconciliation_requires_a_current_later_generated_file_and_keeps_actions(tmp_path,kind):
    from nullion.producer_completion import reconcile_artifact_method
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision
    registry=ToolRegistry()
    for name,formats,source in [('f2',('.png',),False),('r7',('.pdf',) if kind=='different_format' else ('.png',),kind=='source'),('a9',(),False)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,
            ToolSideEffectClass.READ if source else ToolSideEffectClass.WRITE,False,5,
            produced_artifact_extensions=formats,source_artifact_only=source,
            capability_tags=('local_files','explicit_scope_only','required_scope_only')),
            lambda inv:pytest.fail('Reconciliation must not execute any effect'))
    decision=TurnToolScopeDecision(valid=True,requested_artifact_extensions=('.png',),
        requested_tool_names=('f2','r7','a9'),required_tool_names=('f2','r7','a9'))
    scope=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(current_user_message='資料を作成'),tool_scope_decision=decision)
    path=tmp_path/('new.pdf' if kind=='different_format' else 'new.png')
    if kind!='missing':path.write_bytes(b'unit fixture')
    failed=ToolResult('failure','f2','failed',{'failure_scope':'tool','terminal_user_action_required':True},
        response_requirements={'_native_unavailable_file_producer_formats':['.png']})
    rendered=ToolResult('replacement','r7','completed',{'artifact_descriptors':[{'path':str(path),'role':'deliverable'}]})
    state={'tool_registry':scope,'user_message':'資料を作成','artifacts':[str(path)],
        'tool_results':[rendered,failed] if kind=='earlier' else [failed,rendered]}
    updated,changed=reconcile_artifact_method(state,('f2','r7','a9'))
    assert changed is (kind=='replacement')
    if changed:
        result=updated['tool_results'][-1]
        assert 'a9' in result.output['required_tool_names']
        assert 'f2' not in result.output['required_tool_names']
        assert result.output['native_artifact_method_reconciliation']['replacement_invocation_ids']==['replacement']


@pytest.mark.parametrize('kind', ['embedded', 'text_only', 'explicit_image', 'source_only', 'earlier', 'missing', 'other_action', 'unreplaced_dependency', 'unregistered', 'unregistered_text_only'])
def test_intermediate_image_replacement_requires_current_embedded_bytes_and_preserves_outputs(tmp_path, kind):
    from nullion.producer_completion import reconcile_artifact_method
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision
    registry = ToolRegistry()
    for name, effect, formats, source in [
        ('p7', ToolSideEffectClass.ACCOUNT_WRITE if kind == 'other_action' else ToolSideEffectClass.WRITE, ('.png',), False),
        ('d2', ToolSideEffectClass.READ if kind == 'source_only' else ToolSideEffectClass.WRITE, ('.html',), kind == 'source_only'),
        ('a9', ToolSideEffectClass.ACCOUNT_WRITE, (), False),
        ('q4', ToolSideEffectClass.WRITE, ('.wav',), False),
    ]:
        registry.register(ToolSpec(name, name, ToolRiskLevel.LOW, effect, False, 5,
            produced_artifact_extensions=formats, source_artifact_only=source,
            capability_tags=('local_files', 'explicit_scope_only', 'required_scope_only')),
            lambda inv: pytest.fail('Reconciliation must not execute any effect'))
    required = ('p7', 'd2', 'a9') + (('q4',) if kind == 'unreplaced_dependency' else ())
    decision = TurnToolScopeDecision(valid=True,
        requested_artifact_extensions=('.html', '.png') if kind == 'explicit_image' else ('.html',),
        requested_tool_names=required, required_tool_names=required)
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(current_user_message='資料'), tool_scope_decision=decision)
    if kind in {'unregistered', 'unregistered_text_only'}:
        registry.unregister('p7')
    path = tmp_path/'report.html'
    if kind != 'missing':
        path.write_text('<html><body><p>37</p>' + ('' if kind in {'text_only', 'unregistered_text_only'} else
            '<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><rect width="100" height="100" fill="navy"/></svg>') + '</body></html>')
    failed = ToolResult('failure', 'p7', 'failed', {'failure_scope':'tool', 'terminal_user_action_required':True},
        response_requirements={'_native_unavailable_file_producer_formats':['.png', '.jpg', '.jpeg', '.webp', '.svg']})
    rendered = ToolResult('rendered', 'd2', 'completed', {'artifact_descriptors':[{'path':str(path), 'role':'deliverable'}]})
    results = [rendered, failed] if kind == 'earlier' else [failed, rendered]
    if kind == 'unreplaced_dependency':
        results.insert(0, ToolResult('sound-failure', 'q4', 'failed', {'failure_scope':'tool', 'terminal_user_action_required':True},
            response_requirements={'_native_unavailable_file_producer_formats':['.wav']}))
    state = {'tool_registry':scoped, 'user_message':'資料', 'artifacts':[str(path)], 'tool_results':results}
    updated, changed = reconcile_artifact_method(state, required)
    assert changed is (kind in {'embedded', 'unreplaced_dependency', 'unregistered'})
    if changed:
        final = updated['tool_results'][-1].output
        assert final['retired_unavailable_artifact_producers'] == ['p7']
        assert 'a9' in final['required_tool_names']
        assert updated['tool_registry'].turn_tool_scope_decision.requested_artifact_extensions == ('.html',)
        if kind == 'unreplaced_dependency': assert 'q4' in final['required_tool_names']
