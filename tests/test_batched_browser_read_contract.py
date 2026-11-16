import asyncio

import pytest


def test_read_batch_requires_explicit_independence_and_validates_before_navigation(monkeypatch):
    from nullion.plugins.browser_plugin.browser_reads import read_pages
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolInvocation
    class Owner:
        _policy = BrowserPolicy()
        def _session_id(self, invocation): raise AssertionError('Invalid batches must not open a browser')
    for arguments in [
        {'pages': [{'url':'https://a.test'}, {'url':'https://b.test'}]},
        {'pages': [{'url':'https://a.test'}, {}], 'independent_pages':True},
        {'pages': [{'url':'https://a.test'}, {'url':'file:///etc/passwd'}], 'independent_pages':True},
        {'pages': [{'url':'https://a.test'}], 'independent_pages':'yes'},
    ]:
        result = read_pages(Owner(), ToolInvocation('read','browser_read_pages','ordinary-user',arguments))
        assert result.status == 'failed' and result.output['reason'] == 'invalid_tool_arguments'


@pytest.mark.asyncio
async def test_batch_children_reuse_headless_context_without_shutting_the_parent():
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    class Browser:
        BACKEND_NAME = 'cdp'
        closed = []
        shutdowns = 0
        async def close_session(self, session): self.closed.append(session)
        async def shutdown(self): self.shutdowns += 1
    configured, selected = Browser(), Browser()
    backend = SessionRecoveryBackend(configured)
    backend._headless['parent'] = selected
    await backend.share_session_context('parent','child-a')
    await backend.share_session_context('parent','child-b')
    await backend.close_session('child-a')
    await backend.close_session('child-b')
    assert selected.shutdowns == 0 and backend.mode_for_session('parent') == 'headless'
    await backend.close_session('parent')
    assert selected.shutdowns == 1


def test_batch_source_projection_retains_all_structured_rows_and_links():
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    output = {'result_kind':'browser_page_collection',
        'items':[{'source_index':i, 'title':f'項目 {i}', 'url':f'https://example.test/{i}',
                  'source_page_url':'https://example.test/list'} for i in range(20)],
        'source_record_collections':[{'path':['items'], 'identity_fields':['source_page_url','source_index'], 'cardinality':'many'}]}
    compact = _compact_tool_output_for_model_context('browser_read_pages',output)
    assert compact['items'] == output['items']
    assert compact['source_record_collections'] == output['source_record_collections']


@pytest.mark.parametrize('effect', ['read', 'write'])
def test_native_composite_receipts_cannot_claim_writes_or_read_proof_from_page_output(effect):
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, ToolInvocation, completed_runtime_tool_names
    from nullion.request_delivery_requirements import record_request_delivery_requirements
    registry = ToolRegistry()
    for name, child_effect in [('read_child',ToolSideEffectClass.READ),('write_child',ToolSideEffectClass.WRITE)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,child_effect,False,1),lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{}))
    def composite(inv):
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'completed_tools':['unregistered','write_child']},
            response_requirements={'_native_completed_reads':{'tool_names':['read_child','write_child','unregistered'],
                'read_only':True}})
    registry.register(ToolSpec('batch','batch',ToolRiskLevel.LOW,ToolSideEffectClass(effect),False,1,
        delegated_tools=('read_child','write_child','unregistered')),composite)
    result = registry.invoke(ToolInvocation('parent','batch','operator',{}))
    assert completed_runtime_tool_names([result]) == {'batch','read_child'}
    assert not result.response_requirements['_native_completed_reads'].get('read_only')
    from dataclasses import asdict
    assert completed_runtime_tool_names([asdict(result)]) == {'batch','read_child'}
    record_request_delivery_requirements([result],'続けて',[])
    assert completed_runtime_tool_names([result]) == {'batch','read_child'}
    raw = ToolResult('raw','source','completed',{'_native_completed_reads':result.response_requirements['_native_completed_reads']})
    assert completed_runtime_tool_names([raw]) == {'source'}


def test_failed_batch_tab_close_is_retained_for_exact_scope_cleanup():
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_config import DEFAULT_AGENT_BROWSER_SESSION_ID
    from nullion.tools import ToolInvocation
    class Browser:
        BACKEND_NAME = 'cdp'
        closes = []
        async def navigate(self, session, url): return url
        async def run_js(self, session, script):
            if script == '() => location.href': return 'https://example.test/'
            return {'url':'https://example.test/', 'items':[{'source_index':0,'title':'Observed row'}]}
        async def close_session(self, session):
            self.closes.append(session)
            if len(self.closes)==1: raise TimeoutError('temporary close failure')
    backend = Browser()
    tools = BrowserTools(backend, BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_read_pages(ToolInvocation('batch','browser_read_pages','member',
        {'pages':[{'url':'https://example.test/'}]}, capsule_id='owner',
        flow_context={'browser_session_id':DEFAULT_AGENT_BROWSER_SESSION_ID}))
    assert result.status == 'completed'
    assert len(tools._sessions_by_scope['owner']) == 1
    tools.close_tracked_sessions('owner')
    assert len(backend.closes)==2 and backend.closes[0]==backend.closes[1]
    assert DEFAULT_AGENT_BROWSER_SESSION_ID not in backend.closes
    assert not tools._sessions_by_scope


def test_shared_preflight_cannot_be_bypassed_by_client_or_history_and_invalidates_amendments(monkeypatch):
    from dataclasses import replace
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_use_adapter import browser_task_handler
    from nullion.tools import ToolInvocation
    class Browser:
        BACKEND_NAME = 'cdp'
        reads = 0
        async def navigate(self, session, url): return url
        async def extract_text(self, session, selector):
            self.reads += 1
            return '現在の値: 17'
        async def run_js(self, session, script):
            if script == '() => location.href': return 'https://example.test/current'
            return {'url':'https://example.test/current', 'links':[]}
        async def close_session(self, session): pass
    backend = Browser()
    owner = BrowserTools(backend, BrowserSessionPool(), BrowserPolicy())
    def adaptive_model(): raise AssertionError('First read must not start an adaptive model')
    handler = browser_task_handler(adaptive_model, BrowserPolicy(), read_preflight=owner.browser_task_read_preflight)
    for client in ('web', 'telegram', 'slack', 'discord'):
        invocation = ToolInvocation(client, 'browser_run_task', 'member',
            {'task':'現在の値を確認', 'url':'https://example.test/current'}, capsule_id=client,
            flow_context={'user_request':'現在の値を確認', 'surface':client,
                'prior_source_steps':[{'tool_name':'browser_run_task','url':'https://example.test/current'}]})
        result = handler(invocation)
        assert result.status == 'completed' and '17' in result.output['documents'][0]['text']
        strategy = result.output['strategy']
        assert strategy['kind'] == 'native_read_preflight' and strategy['adaptive_browser_started'] is False
        assert strategy['continuation']['execution_kind'] == 'adaptive_interaction'
        assert strategy['continuation']['url'] == invocation.arguments['url']
        assert owner._read_first_policy.contains(invocation, invocation.arguments['url'])
        next_read=owner.browser_task_read_preflight(replace(invocation, invocation_id=client+'-next'))
        assert next_read.output['strategy']['read_reused'] is True
        adaptive=replace(invocation,arguments={**invocation.arguments,'execution_kind':'adaptive_interaction','interaction_reason':'Observed control requires interaction'})
        assert owner.browser_task_read_preflight(adaptive) is None
        amended = replace(invocation, flow_context={**invocation.flow_context,'user_request':'別の確認'})
        assert not owner._read_first_policy.contains(amended, invocation.arguments['url'])
        assert handler(amended).output['strategy']['kind'] == 'native_read_preflight'
        owner.close_tracked_sessions(client)
        assert not owner._read_first_policy.contains(invocation, invocation.arguments['url'])
    assert backend.reads == 8


@pytest.mark.parametrize('url,observed',[('https://example.test/form',True),('about:blank',False)])
def test_native_snapshot_counts_as_current_request_observation_without_an_extra_read(url,observed):
    from dataclasses import replace
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_reads import BrowserReadFirstPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolInvocation
    class Browser:
        BACKEND_NAME='cdp'
        reads=0
        async def run_js(self,session,script):
            self.reads+=1
            return {'url':url,'elements':[],'element_count':0}
    backend=Browser();owner=BrowserTools(backend,BrowserSessionPool(),BrowserPolicy())
    invocation=ToolInvocation('snapshot','browser_snapshot','member',{},capsule_id='request',
        flow_context={'user_request':'入力内容を確認'})
    result=owner.browser_snapshot(invocation)
    assert result.status=='completed' and result.output['page_url']==url
    assert backend.reads==1 and owner._read_first_policy.contains(invocation,url) is observed
    assert not owner._read_first_policy.contains(replace(invocation,capsule_id='other-request'),url)
    assert not owner._read_first_policy.contains(replace(invocation,flow_context={'user_request':'別の内容'}),url)


@pytest.mark.parametrize('reason', ['choice','cancelled','read_error','visual'])
def test_preflight_preserves_recovery_consent_and_allows_adaptive_after_native_failure(reason):
    from nullion.plugins.browser_plugin.browser_reads import BrowserReadFirstPolicy, browser_task_read_preflight
    from nullion.tools import ToolInvocation, ToolResult
    class Owner:
        _read_first_policy = BrowserReadFirstPolicy()
        def browser_read_pages(self, invocation):
            return ToolResult(invocation.invocation_id,invocation.tool_name,'failed', {
                **({'requires_user_choice':True} if reason=='choice' else {}),
                'pages':[{'failure_reason':'request_cancelled' if reason=='cancelled' else 'source_timeout'}]})
    invocation = ToolInvocation('a','browser_run_task','member',{'url':'https://example.test/'},
        flow_context={'requested_artifact_extensions':['.png'],'source_artifact_delivery_requested':True} if reason=='visual' else {})
    owner = Owner()
    result = browser_task_read_preflight(owner, invocation)
    if reason=='visual': assert result is None
    else:
        assert result.status == 'failed'
        assert owner._read_first_policy.contains(invocation, invocation.arguments['url']) == (reason=='read_error')


def test_browser_delegation_owns_preflight_while_page_javascript_keeps_its_prerequisite(monkeypatch):
    from nullion.plugins.browser_plugin import register_browser_tools,browser_use_adapter
    from nullion.tools import ToolRegistry
    monkeypatch.setattr(browser_use_adapter,'browser_use_available',lambda:True)
    registry=ToolRegistry()
    register_browser_tools(registry,model_client_getter=lambda:object())
    assert registry.get_spec('browser_run_task').code_mode_observation_tools==()
    assert registry.get_spec('browser_run_js').code_mode_observation_tools


@pytest.mark.parametrize('failure',['source_error','source_timeout','request_cancelled','choice'])
def test_failed_native_preflight_records_that_delegated_interaction_did_not_execute(failure):
    from nullion.plugins.browser_plugin.browser_reads import BrowserReadFirstPolicy,browser_task_read_preflight
    from nullion.tools import ToolInvocation,ToolResult
    class Owner:
        _read_first_policy=BrowserReadFirstPolicy()
        def browser_read_pages(self,inv):
            return ToolResult(inv.invocation_id,inv.tool_name,'failed',{
                'result_kind':'browser_page_collection','completed_pages':0,
                'pages':[{'failure_reason':failure}],
                **({'requires_user_choice':True} if failure=='choice' else {})})
    owner=Owner()
    inv=ToolInvocation('preflight','browser_run_task','operator',{'url':'https://example.test/',
        'task':'入力','execution_kind':'adaptive_interaction','interaction_reason':'Use the selected form'},
        capsule_id='request',flow_context={'user_request':'入力'})
    result=browser_task_read_preflight(owner,inv)
    assert result.status=='failed' and result.output['executed'] is False
    assert result.output['execution_scope']=='delegated_interaction'
    assert result.output['strategy']['adaptive_browser_started'] is False
    assert owner._read_first_policy.contains(inv,inv.arguments['url']) is (failure not in {'choice','request_cancelled'})


@pytest.mark.parametrize('observed', ['', '確認済み'])
def test_batched_text_scope_is_preserved_even_when_empty(observed):
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolInvocation
    class Browser:
        BACKEND_NAME = 'cdp'
        async def navigate(self, session, url): return url
        async def extract_text(self, session, selector):
            assert selector == '#対象'
            return observed
        async def run_js(self, session, script):
            if script == '() => location.href': return 'https://example.test/'
            return {'links': []}
        async def close_session(self, session): pass
    tools = BrowserTools(Browser(), BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_read_pages(ToolInvocation('batch', 'browser_read_pages', 'member',
        {'pages': [{'url': 'https://example.test/', 'read_kind': 'text', 'selector': '#対象'}]},
        capsule_id='owner'))
    assert result.status == 'completed'
    document = result.output['documents'][0]
    assert document['selector'] == '#対象'
    assert document['text'] == observed and document['text_length'] == len(observed)


@pytest.mark.parametrize('independent',[False,True])
def test_delegated_multi_source_reads_require_typed_independence(independent):
    from nullion.plugins.browser_plugin.browser_reads import BrowserReadFirstPolicy,browser_task_read_preflight
    from nullion.tools import ToolInvocation,ToolResult
    calls=[]
    class Owner:
        _read_first_policy=BrowserReadFirstPolicy()
        def browser_read_pages(self,invocation):
            calls.append(invocation.arguments)
            return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'pages':[]})
    result=browser_task_read_preflight(Owner(),ToolInvocation('multi','browser_run_task','member',{
        'url':'https://a.test','read_urls':['https://a.test','https://b.test'],'independent_pages':independent}))
    assert result.status==('completed' if independent else 'failed')
    assert bool(calls)==independent
    if independent:assert [p['url'] for p in calls[0]['pages']]==['https://a.test','https://b.test']
