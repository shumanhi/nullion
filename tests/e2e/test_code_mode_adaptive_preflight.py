"""Real chat/VM/native preflight and submitted form; provider/worker IO scripted."""
import asyncio
import json
import os
import sys
import threading
import time
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import parse_qs,urlsplit

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('initial_open',[False,True])
def test_explicit_interaction_submits_form_after_preflight_in_one_program(tmp_path,monkeypatch,record_property,initial_open,surface):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools,browser_use_adapter,browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    save_preferences({'outbound_request_mode':'allow_all'})
    for k,v in {'NULLION_CODE_MODE':'on','NULLION_BROWSER_BACKEND':'playwright',
            'NULLION_BROWSER_HEADLESS':'true','NULLION_BROWSER_BLOCK_PRIVATE':'false'}.items():
        monkeypatch.setenv(k,v)
    submitted=[];workers=[]
    observe_status = surface == 'telegram' and not initial_open
    step_one_visible = threading.Event()
    step_two_visible = threading.Event()
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            query=parse_qs(urlsplit(self.path).query)
            content=('<form action="/search"><label>Origin<input name="origin"></label>'
                '<label>Destination<input name="destination"></label><label>Date<input name="date"></label>'
                '<button>Search</button></form>')
            if urlsplit(self.path).path=='/search':
                submitted.append(query)
                content='<main>Flight 37, EWR to LAX, 2031-04-08, 18:00, $173.</main>'
            self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers()
            self.wfile.write(('<html><body>'+content+'</body></html>').encode())
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=site.serve_forever,daemon=True).start()
    url=f'http://localhost:{site.server_port}/'
    # The external worker boundary is scripted; the browser actually fills and
    # submits a form. Ingress, scope, preflight, VM and durable receipts stay real.
    def worker(task,**kwargs):
        workers.append(task)
        assert task.prior_source_steps[-1]['status']=='completed'
        assert task.prior_source_steps[-1]['output']['completed_pages']==1
        assert task.request_time_context['accepted_local_date']
        if observe_status and len(workers) == 1:
            progress = kwargs['progress_callback']
            for step, published in ((1, step_one_visible), (2, step_two_visible)):
                progress({'pages_checked': 1, 'findings_found': 0,
                    'activity_phase': 'planning_browser_step', 'browser_step': step})
                assert published.wait(6), f'Browser step {step} never reached Telegram'
        with sync_playwright() as p:
            browser=p.chromium.launch(headless=True,args=['--no-sandbox'])
            try:
                page=browser.new_page();page.goto(task.url)
                page.locator('[name=origin]').fill('EWR')
                page.locator('[name=destination]').fill('LAX')
                page.locator('[name=date]').fill('2031-04-08')
                page.get_by_role('button',name='Search',exact=True).click()
                finding=page.locator('main').inner_text();result_url=page.url
            finally:browser.close()
        return {'result_kind':'source_observations','report_status':'completed','goal_status':'answered',
            'observations':[{'label':'Flight','value':finding,'quote':finding,'source_url':result_url}],
            'page_url':result_url,'artifact_paths':[]}
    monkeypatch.setattr(browser_use_adapter,'browser_use_python',lambda:sys.executable)
    monkeypatch.setattr(browser_use_worker,'run_isolated_browser_task',worker)
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_open','browser_run_task'] if initial_open else ['browser_run_task'],
                'required_tool_names':['browser_open','browser_run_task'] if initial_open else ['browser_run_task']})
            if self.phase==2:return call('code_mode',{'tool_names':['browser_open','browser_run_task'] if initial_open else ['browser_run_task'],'code':
                ("call_tool('browser_open', {})\n" if initial_open else '')+
                "r=call_tool('browser_run_task', "+repr({'url':url,'task':'EWR → LAX, 2031-04-08, 18:00',
                    'execution_kind':'adaptive_interaction','interaction_reason':'Submit the observed route and date controls'})+")\nr['output']"})
            assert self.phase==3,'No extra primary call or program needed for the handoff'
            assert '$173' in json.dumps(kwargs['messages']),kwargs['messages'][-1]
            return text('Flight 37, 2031-04-08 at 18:00, $173. '+url+'search')
    model=Model();registry=ToolRegistry()
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    samples=[];message_number=[17000]
    if surface=='telegram':
        from nullion.config import load_settings
        from nullion.telegram_app import ChatOperatorService
        settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
            'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
        service=ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,
            operator_chat_id='123',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            for n,prompt in enumerate(['検索フォームを送信して','Envía el formulario','أرسل نموذج البحث']):
                model.phase=0;cid=f'web:preflight-form-{n}' if surface=='web' else 'telegram:123'
                started=time.monotonic()
                if surface=='web':
                    response=client.post('/api/chat',json={'text':prompt+' '+url,'conversation_id':cid})
                    assert response.status_code==200,response.text
                    visible=response.json()['text']
                else:
                    message_number[0]+=1;sent=[]
                    class Message:
                        text=prompt+' '+url;message_id=message_number[0];reply_to_message=None
                        chat=SimpleNamespace(id=123,send_action=lambda **kw:asyncio.sleep(0))
                        async def reply_text(self,value,**kw):
                            def record(value):
                                sent.append(value)
                                if 'Planning browser step 1' in value and '1 page checked' in value:
                                    step_one_visible.set()
                                if 'Planning browser step 2' in value and '1 page checked' in value:
                                    step_two_visible.set()
                            async def edit_text(value, **kwargs):
                                record(value)
                            record(value)
                            return SimpleNamespace(message_id=message_number[0]+100, edit_text=edit_text,
                                delete=lambda: asyncio.sleep(0))
                    asyncio.run(service.on_message(SimpleNamespace(update_id=message_number[0],message=Message()),SimpleNamespace(bot=None)))
                    assert sent
                    visible=sent[-1]
                samples.append(time.monotonic()-started)
                assert '$173' in visible,{'reply':visible,'workers':len(workers),'submissions':submitted,
                    'turns':[r for r in runtime.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn']}
                turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
                results=turn['tool_results']
                native=[r for r in results if r['tool_name']=='browser_run_task']
                assert len(native)==1 and native[0]['status']=='completed'
                assert native[0]['response_requirements']['_native_preflight']['adaptive_browser_started'] is True
                assert native[0]['output']['goal_status']=='answered'
                assert all(r['status']=='completed' for r in results),results
                assert not any(r['tool_name'] in {'browser_navigate','browser_extract_text','browser_read_pages'} for r in results),results
                assert len([r for r in results if r['tool_name']=='code_mode'])==1
                assert model.phase==3
        assert len(workers)==len(submitted)==3
        assert all(q=={'origin':['EWR'],'destination':['LAX'],'date':['2031-04-08']} for q in submitted)
        assert sum(samples)/len(samples)<10 and max(samples)<15,samples
        record_property('preflight_form_mean_seconds',sum(samples)/len(samples))
    finally:
        registry.run_cleanup_hooks();site.shutdown();site.server_close()
