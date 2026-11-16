"""Real API/Monty/CDP reads survive delayed external tab-close acknowledgements."""
import asyncio
import json
import os
import threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('mode',['off','on'])
def test_parallel_reads_keep_all_three_results_when_a_previous_tab_is_closing(tmp_path,monkeypatch,mode):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from playwright.async_api import Page
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.preferences import save_preferences
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    save_preferences({'outbound_request_mode':'allow_all'})
    port=_free_port()
    for key,value in {'NULLION_CODE_MODE':mode,'NULLION_BROWSER_BACKEND':'cdp',
            'NULLION_BROWSER_CDP_URL':f'http://127.0.0.1:{port}',
            'NULLION_BROWSER_BLOCK_PRIVATE':'false'}.items():monkeypatch.setenv(key,value)
    first_closing=threading.Event();third_requested=threading.Event();first_closed=threading.Event()
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/two':first_closing.wait(3)
            if self.path=='/three':third_requested.set();first_closed.wait(3)
            self.send_response(200);self.end_headers()
            try:self.wfile.write(('<main>Verified record '+self.path.strip('/')+'</main>').encode())
            except (BrokenPipeError,ConnectionResetError):pass
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site)
    threading.Thread(target=site.serve_forever,daemon=True).start()
    original_close=Page.close
    async def delayed_close(page,*args,**kwargs):
        if page.url.endswith('/one'):
            first_closing.set()
            await asyncio.to_thread(third_requested.wait,3)
            try:return await original_close(page,*args,**kwargs)
            finally:first_closed.set()
        return await original_close(page,*args,**kwargs)
    monkeypatch.setattr(Page,'close',delayed_close)
    base=f'http://localhost:{site.server_port}'
    args={'independent_pages':True,'pages':[{'url':base+'/'+name,'read_kind':'text'} for name in ('one','two','three')]}
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['browser'],
                'tool_names':['browser_read_pages'],'required_tool_names':['browser_read_pages']})
            if self.phase==2:
                return (call('code_mode',{'tool_names':['browser_read_pages'],
                    'code':"call_tool('browser_read_pages', "+repr(args)+")"}) if mode=='on'
                    else call('browser_read_pages',args))
            assert self.phase==3,'A closed-tab retry must not add model rounds'
            wire=json.dumps(kwargs['messages'])
            assert all('Verified record '+name in wire for name in ('one','two','three'))
            return text('Verified records one, two and three. '+base+'/one')
    model=Model();registry=ToolRegistry()
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    try:
        with sync_playwright() as pw:
            context=pw.chromium.launch_persistent_context(str(tmp_path/'profile'),headless=True,
                args=[f'--remote-debugging-port={port}'])
            try:
                with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
                    response=client.post('/api/chat',json={'text':'Read the three selected records.','conversation_id':'web:parallel-close'})
                    assert response.status_code==200 and 'one, two and three' in response.json()['text'],response.text
                    results=[r for e in runtime.store.list_conversation_events('web:parallel-close')
                        if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
                    native=next(r['output'] for r in results if r['tool_name']=='browser_read_pages')
                    assert native['completed_pages']==3 and all(p['status']=='completed' for p in native['pages']),native
                    assert first_closing.is_set() and third_requested.is_set() and first_closed.is_set()
                    assert model.phase==3
            finally:context.close()
    finally:site.shutdown();site.server_close()
