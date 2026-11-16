"""Real HTTP/SQLite/native browser boundaries; external model/connector scripted."""
import asyncio
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.fixture
def source_browser(tmp_path,monkeypatch):
    from playwright.sync_api import sync_playwright
    from tests.e2e.test_web_surface_contracts import _free_port
    reads=[]
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/record':reads.append(self.path)
            self.send_response(200);self.end_headers();self.wfile.write(b'<main><h1>Verified value: 37</h1></main>')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=server.serve_forever,daemon=True).start();port=_free_port()
    for key,value in {'NULLION_BROWSER_BACKEND':'cdp','NULLION_BROWSER_CDP_URL':f'http://127.0.0.1:{port}',
            'NULLION_BROWSER_BLOCK_PRIVATE':'false','NULLION_CODE_MODE':'on',
            'NULLION_BROWSER_USE_PYTHON':os.environ['NULLION_BROWSER_USE_TEST_PYTHON']}.items():monkeypatch.setenv(key,value)
    try:
        with sync_playwright() as p:
            browser=p.chromium.launch_persistent_context(str(tmp_path/'profile'),headless=True,args=[f'--remote-debugging-port={port}'])
            try:yield f'http://localhost:{server.server_port}/record',reads
            finally:browser.close()
    finally:server.shutdown();server.server_close()


def test_ordinary_research_cannot_enter_the_slow_worker_or_repeat_its_read(tmp_path,monkeypatch,source_browser,record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);save_preferences({'outbound_request_mode':'allow_all'})
    url,reads=source_browser
    def slow_worker():
        time.sleep(3)
        pytest.fail('Ordinary public research entered the nested browser model')
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_read_pages','browser_run_task'],'required_tool_names':['browser_run_task']})
            if self.phase==2:return call('code_mode',{'tool_names':['browser_read_pages','browser_run_task'],'code':
                "call_tool('browser_read_pages', "+repr({'pages':[{'url':url,'read_kind':'text'}]})+")\n"+
                "call_tool('browser_run_task', "+repr({'url':url,'task':'現在の値を確認'})+")"})
            assert '37' in json.dumps(kw['messages'])
            return text('Verified value: 37. '+url)
    model=Model();registry=ToolRegistry()
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=slow_worker)
    samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for i,prompt in enumerate(['現在の値を調べてください。','Comprueba el valor actual.','Check the current value.']):
            model.phase=0;before=len(reads);start=time.monotonic();cid='web:lightweight-'+str(i)
            response=client.post('/api/chat',json={'text':prompt+' '+url,'conversation_id':cid})
            samples.append(time.monotonic()-start)
            assert response.status_code==200 and '37' in response.json()['text'],response.text
            assert len(reads)==before+1
            turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
            worker=next(r for r in turn['tool_results'] if r['tool_name']=='browser_run_task')
            assert worker['output']['strategy']['adaptive_browser_started'] is False
            assert worker['output']['strategy']['read_reused'] is True
    assert sum(samples)/len(samples)<2.5 and max(samples)<3,samples
    record_property('mean_public_research_seconds',sum(samples)/len(samples))


@pytest.mark.parametrize('surface',['web','telegram'])
def test_wrong_date_booking_url_is_removed_at_shared_final_delivery(tmp_path,monkeypatch,surface):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    bad='https://hotel.test/book?x=2026-12-13&y=2026-12-14';reviews=[]
    registry=ToolRegistry()
    def calendar(inv):return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'results':[{'id':'stay37','summary':'Stay',
        'start':{'dateTime':'2026-11-21T15:00:00-05:00'},'end':{'dateTime':'2026-11-22T12:00:00-05:00'}}]})
    registry.register(ToolSpec('calendar_list','Read selected calendar',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
        capability_tags=('calendar','explicit_scope_only'),code_mode_allowed=True,input_schema={'type':'object'}),calendar)
    class Model:
        phase=0
        def create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            if 'review_source_link_state' in names:
                reviews.append(True)
                return call('review_source_link_state',{'decisions':[{'id':'0','disposition':'omit'}]})
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['calendar'],'tool_names':['calendar_list'],
                'required_tool_names':['calendar_list']})
            if self.phase==2:return call('code_mode',{'tool_names':['calendar_list'],'code':"call_tool('calendar_list', {})"})
            return text('Stay: 2026-11-21 at 3 PM to 2026-11-22 at noon.\n[ホテルの公式ページ]('+bad+')')
    model=Model();orchestrator=AgentOrchestrator(model_client=model)
    with TestClient(create_app(runtime,orchestrator,registry)) as client:
        if surface=='web':
            response=client.post('/api/chat',json={'text':'予定の宿泊先を調べてください。','conversation_id':'web:hotel-state'})
            assert response.status_code==200;visible=response.json()['text']
        else:
            from nullion.config import load_settings
            from nullion.telegram_app import ChatOperatorService
            settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
                'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
            service=ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,
                operator_chat_id='77',model_client=model,agent_orchestrator=orchestrator)
            sent=[]
            class Message:
                text='宿泊先の詳細を調べてください。';message_id=937;reply_to_message=None
                chat=SimpleNamespace(id=77,send_action=lambda **kwargs:asyncio.sleep(0))
                async def reply_text(self,value,**kwargs):sent.append(value);return SimpleNamespace(message_id=938)
            asyncio.run(service.on_message(SimpleNamespace(update_id=937,message=Message()),SimpleNamespace(bot=None)))
            visible=sent[-1]
        assert bad not in visible and '2026-11-21' in visible and '2026-11-22' in visible
        assert reviews==[True]
        cid='web:hotel-state' if surface=='web' else 'telegram:77'
        turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
        assert bad not in turn['assistant_reply']
        timings=runtime.store.list_recent_conversation_events(cid,event_type='conversation.link_state_review')
        assert timings[-1]['omitted_link_count']==1 and timings[-1]['duration_ms']>=0


def test_capture_scope_rejects_generated_replacement_and_delivers_original_pixels(tmp_path,monkeypatch,source_browser):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);save_preferences({'outbound_request_mode':'allow_all'})
    url,_=source_browser;cid='web:capture-fidelity';prompt='元のページのPNG撮影だけを返してください。'
    registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy())
    registry.register(ToolSpec('image_generate','External image generation',ToolRiskLevel.MEDIUM,ToolSideEffectClass.WRITE,
        False,5,produced_artifact_extensions=('.png',),capability_tags=('image_generation',)),
        lambda inv:pytest.fail('A screenshot request must not invoke an image-generation provider'))
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_navigate','browser_screenshot'],'required_tool_names':['browser_navigate','browser_screenshot'],
                'artifact_extensions':['.png'],'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt})
            if self.phase==2:return call('code_mode',{'tool_names':['browser_navigate','browser_screenshot'],'code':
                "call_tool('browser_navigate', "+repr({'url':url})+")\ncall_tool('browser_screenshot', {})"})
            if self.phase==3:return call('request_tool_scope',{'capabilities':['image_generation'],
                'tool_names':['image_generate'],'required_tool_names':['image_generate'],'artifact_extensions':['.png'],
                'source_artifact_delivery_requested':False})
            assert 'source_artifact_generation_conflict' in json.dumps(kw['messages'])
            paths=list(artifact_root_for_principal(cid).glob('*.png'));assert len(paths)==1
            return text('元の撮影です。\n\nMEDIA:'+str(paths[0]))
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=Model()),registry)) as client:
        response=client.post('/api/chat',json={'text':prompt+' '+url,'conversation_id':cid})
        assert response.status_code==200 and len(response.json()['artifacts'])==1,response.text
        path=next(artifact_root_for_principal(cid).glob('*.png'))
        assert client.get(response.json()['artifacts'][0]['url']).content==path.read_bytes()
        turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
        assert not any(r['tool_name']=='image_generate' for r in turn['tool_results'])
        assert any(r['tool_name']=='browser_screenshot' and r['status']=='completed' for r in turn['tool_results'])
