import time
import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous',[False,True])
async def test_session_context_inheritance_recovers_before_batch_navigation(asynchronous):
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    class Backend:
        BACKEND_NAME='configured';ready=False;attempts=0;reconnects=0
        def share(self,parent,child):
            self.attempts+=1
            if not self.ready:raise ConnectionError('closed transport')
        async def reconnect(self,session_id):
            self.reconnects+=1;self.ready=True
    backend=Backend()
    async def share(parent,child):backend.share(parent,child)
    backend.share_session_context=share if asynchronous else backend.share
    recovered=SessionRecoveryBackend(backend)
    await recovered.share_session_context('parent','child')
    assert backend.attempts==2 and backend.reconnects==1
    assert recovered.mode_for_session('child')=='configured'


@pytest.mark.parametrize('pages,iframes,workers,expected',[(19,53,10,False),(50,0,0,True),(0,150,100,False)])
def test_sdk_capacity_counts_page_lifecycle_events_not_all_targets(monkeypatch,pages,iframes,workers,expected):
    import io,json
    from nullion.plugins.browser_plugin.browser_use_adapter import _cdp_startup_exceeds_sdk_capacity
    targets=[{'type':kind,'id':str(i)} for kind,count in [('page',pages),('iframe',iframes),('worker',workers)] for i in range(count)]
    monkeypatch.setattr('urllib.request.urlopen',lambda *a,**kw:io.BytesIO(json.dumps(targets).encode()))
    assert _cdp_startup_exceeds_sdk_capacity('http://localhost:1234') is expected


@pytest.mark.asyncio
async def test_failed_navigation_waits_for_error_document_before_fallback():
    import asyncio
    from types import SimpleNamespace
    from nullion.plugins.browser_plugin.browser_session import goto_with_settled_failure
    original=RuntimeError('native navigation failure')
    class Page:
        listener=None
        pending=False
        def on(self,event,listener):
            assert event=='domcontentloaded';self.listener=listener
        def remove_listener(self,event,listener):
            assert self.listener is listener;self.listener=None
        async def goto(self,url,**kwargs):
            if url.endswith('/bad'):
                self.pending=True
                def error_commits():
                    self.pending=False;self.listener()
                asyncio.get_running_loop().call_later(.02,error_commits)
                raise original
            assert not self.pending,'Error redirect interrupted fallback'
            return SimpleNamespace(status=200)
    page=Page()
    with pytest.raises(RuntimeError) as failure:
        await goto_with_settled_failure(page,'https://example.test/bad')
    assert failure.value is original and page.listener is None
    assert (await goto_with_settled_failure(page,'https://example.test/good')).status==200
    assert page.listener is None


@pytest.mark.parametrize('backend,primary,legacy,expected', [
    ('cdp','http://managed.test:1','http://old.test:2','http://managed.test:1'),
    ('auto',None,'http://old.test:2','http://old.test:2'),
    ('auto',None,None,'http://127.0.0.1:9222'),
    ('playwright','http://managed.test:1',None,None),
])
def test_browser_controller_endpoint_follows_selected_mode(monkeypatch, backend, primary, legacy, expected):
    from nullion.plugins.browser_plugin.browser_use_adapter import _browser_task_connection
    monkeypatch.setenv('NULLION_BROWSER_BACKEND',backend)
    for key,value in [('NULLION_BROWSER_CDP_URL',primary),('NULLION_BROWSER_USE_CDP_URL',legacy)]:
        if value is None: monkeypatch.delenv(key,raising=False)
        else: monkeypatch.setenv(key,value)
    assert _browser_task_connection()[0] == expected


def test_control_handoff_keeps_session_and_resolves_after_real_alternative_tool():
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask, _same_browser_control_handoff
    from nullion.source_observation_contract import pending_source_continuation
    from nullion.tools import ToolResult
    output = _same_browser_control_handoff(BrowserTask(task='続けて',url='https://example.test'), 'same_session_capacity_handoff',time.monotonic())
    result = ToolResult('sdk','browser_run_task','completed',output)
    assert output['steps']==0 and output['artifact_paths']==[]
    assert output['continuation']['preserve_session'] is True
    assert 'skipped' in output['blocker'] and 'could not attach' not in output['blocker']
    assert pending_source_continuation([result])['kind']=='alternate_tool'
    failed = ToolResult('open','browser_open','failed',{})
    assert pending_source_continuation([result,failed])
    good = ToolResult('open','browser_open','completed',{'session_id':'agent-browser-default'})
    assert pending_source_continuation([result,good]) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('method', ['open', 'navigate', 'extract_text', 'click'])
async def test_reconnect_is_bounded_and_never_replays_an_uncertain_action(method):
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    class Configured:
        BACKEND_NAME = 'cdp'
        attempts = 0
        reconnects = 0
        async def action(self, *args):
            self.attempts += 1
            if self.attempts == 1: raise ConnectionError('disconnected')
            return 'same-session'
        open = navigate = extract_text = click = action
        async def reconnect(self, session_id):
            assert session_id == 'owned'
            self.reconnects += 1
    configured = Configured()
    backend = SessionRecoveryBackend(configured)
    if method == 'click':
        with pytest.raises(ConnectionError): await backend.click('owned')
        assert configured.attempts == 1
    else:
        assert await getattr(backend, method)('owned') == 'same-session'
        assert configured.attempts == 2
    assert configured.reconnects == 1 and not backend._headless


@pytest.mark.asyncio
async def test_failed_reconnect_offers_choices_without_assuming_login_or_switching():
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend, BrowserRecoveryRequired
    class Configured:
        BACKEND_NAME = 'cdp'
        async def open(self, session): raise ConnectionError('offline')
        async def reconnect(self, session): raise ConnectionError('still offline')
    backend = SessionRecoveryBackend(Configured())
    with pytest.raises(BrowserRecoveryRequired) as failure: await backend.open('owned')
    output = failure.value.recovery_output()
    assert output['reconnect_attempted'] is True
    assert output['login_requirement'] == 'task_dependent'
    assert output['requires_user_choice'] is True
    assert [row['number'] for row in output['recovery_options']] == [1, 2]
    assert not backend._headless


@pytest.mark.asyncio
async def test_cdp_reconnect_retains_exact_task_targets_and_refuses_url_only_matches(monkeypatch):
    from types import SimpleNamespace
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    class Page:
        def __init__(self, url, target_id): self.url, self.target_id = url, target_id
        def is_closed(self): return False
    backend = CDPBackend()
    backend._pages = {'a': Page('https://a.test', 'a'), 'b': Page('https://b.test', 'b'), 'ambiguous': Page('https://same.test', 'gone')}
    pages = [Page('https://a.test', 'a'), Page('https://b.test', 'b'), Page('https://same.test', 'user-1'), Page('https://same.test', 'user-2')]
    async def connect(): return SimpleNamespace(contexts=[SimpleNamespace(pages=pages)])
    monkeypatch.setattr(backend, '_ensure_browser', connect)
    await backend.reconnect('a')
    assert backend._pages['a'] is pages[0] and backend._pages['b'] is pages[1]
    with pytest.raises(ConnectionError): await backend._get_page('ambiguous')
    assert backend._reconnect_page_urls == {'ambiguous': 'https://same.test'}


def test_headless_choice_never_overrides_the_shared_browser_for_other_tasks():
    from types import SimpleNamespace
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_config import DEFAULT_AGENT_BROWSER_SESSION_ID
    from nullion.tools import ToolInvocation
    tools = BrowserTools(SimpleNamespace(BACKEND_NAME='cdp'), BrowserSessionPool(), BrowserPolicy())
    def invocation(tool, scope, arguments):
        return ToolInvocation('call', tool, 'same-user', arguments, capsule_id=scope,
            flow_context={'browser_session_id': DEFAULT_AGENT_BROWSER_SESSION_ID,
                'browser_mode_selection':{'mode':'headless','principal_id':'same-user',
                    'source_turn_id':'native-choice','source_invocation_id':'native-recovery'}})
    selected = tools._session_id(invocation('browser_open', 'a', {'mode':'headless'}))
    assert selected != DEFAULT_AGENT_BROWSER_SESSION_ID
    assert tools._session_id(invocation('browser_navigate', 'a', {})) == selected
    assert tools._session_id(invocation('browser_open', 'b', {})) == DEFAULT_AGENT_BROWSER_SESSION_ID
    tools._forget_session(selected)
    assert not tools._recovery_sessions_by_scope


def test_model_cannot_switch_desktop_to_headless_without_native_user_choice():
    from types import SimpleNamespace
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolInvocation
    tools=BrowserTools(SimpleNamespace(BACKEND_NAME='auto'),BrowserSessionPool(),BrowserPolicy())
    result=tools.browser_open(ToolInvocation('model-only','browser_open','user',{'mode':'headless'},capsule_id='one'))
    assert result.status=='failed' and result.output['executed'] is False
    assert result.output['recovery']['argument_constraints']['mode']['enum']==['configured']
    assert not tools._recovery_sessions_by_scope


@pytest.mark.parametrize('reply',['2','２','٢'])
@pytest.mark.parametrize('tool',['browser_open','browser_read_pages'])
def test_numeric_headless_choice_uses_parent_native_receipt(reply,tool):
    from nullion.plugins.browser_plugin.browser_config import selected_browser_recovery
    from nullion.runtime_store import RuntimeStore
    from nullion.plugins.browser_plugin.backends.session_recovery import BrowserRecoveryRequired
    store=RuntimeStore()
    store.add_conversation_event({'conversation_id':'web:c','event_type':'conversation.chat_turn','turn_id':'parent',
        'tool_results':[{'invocation_id':'native','tool_name':tool,'status':'failed',
            'output':BrowserRecoveryRequired().recovery_output()}]})
    selection=selected_browser_recovery(store,reply=reply,conversation_id='web:c',parent_turn_id='parent',principal_id='user')
    assert selection['mode']=='headless' and selection['source_invocation_id']=='native'
    assert selected_browser_recovery(store,reply=reply,conversation_id='web:c',parent_turn_id='unrelated',principal_id='user') is None
    store.conversation_events[0]['tool_results'][0]['tool_name']='web_fetch'
    assert selected_browser_recovery(store,reply=reply,conversation_id='web:c',parent_turn_id='parent',principal_id='user') is None


def test_scheduled_choice_projection_keeps_binding_without_internal_source_output():
    from types import SimpleNamespace
    from nullion.cron_delivery import record_cron_delivery_chat_turn
    from nullion.runtime_store import RuntimeStore
    from nullion.plugins.browser_plugin.browser_config import selected_browser_recovery
    from nullion.plugins.browser_plugin.backends.session_recovery import BrowserRecoveryRequired
    output=BrowserRecoveryRequired().recovery_output();output['private_connector_payload']={'secret':'do-not-copy'}
    store=RuntimeStore()
    event=record_cron_delivery_chat_turn(store,SimpleNamespace(id='job37',name='確認'),
        conversation_id='telegram:123',delivery_channel='telegram',delivered_text='1. 再試行\n2. 続行',
        browser_recovery_choices=[{'invocation_id':'reader37','tool_name':'browser_read_pages','status':'failed','output':output}])
    assert 'do-not-copy' not in str(event)
    selected=selected_browser_recovery(store,reply='2',conversation_id='telegram:123',parent_turn_id=event['turn_id'],principal_id='user')
    assert selected['mode']=='headless' and selected['source_invocation_id']=='reader37'
