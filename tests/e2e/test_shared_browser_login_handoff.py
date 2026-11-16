"""A controller attach failure must retain the actual signed-in browser cookie."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
from urllib.request import urlopen

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1', reason='Docker E2E')


def test_sdk_attaches_existing_cookie_session_with_many_iframe_targets(tmp_path,monkeypatch):
    """The adapter must allow a real SDK attach above 50 non-tab targets."""
    import subprocess
    import time
    from playwright.sync_api import sync_playwright
    from tests.e2e.test_web_surface_contracts import _free_port
    from nullion.plugins.browser_plugin.browser_use_adapter import _cdp_startup_exceeds_sdk_capacity
    worker=os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    if not worker:pytest.skip('installed isolated SDK runtime required')
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            body=(('<h1>Existing session</h1>'+''.join(f'<iframe src="http://127.0.0.1:{self.server.server_port}/frame?i={i}"></iframe>' for i in range(55))).encode()
                  if self.path=='/' else b'<h1>Frame</h1>')
            self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site)
    threading.Thread(target=site.serve_forever,daemon=True).start()
    port=_free_port();endpoint=f'http://127.0.0.1:{port}';url=f'http://localhost:{site.server_port}/'
    try:
        with sync_playwright() as pw:
            context=pw.chromium.launch_persistent_context(str(tmp_path/'profile'),headless=True,
                args=[f'--remote-debugging-port={port}','--site-per-process'])
            try:
                context.add_cookies([{'name':'session','value':'existing','url':url}])
                page=context.new_page();page.goto(url,wait_until='load')
                deadline=time.monotonic()+5
                while True:
                    with urlopen(endpoint+'/json/list',timeout=3) as wire:targets=json.load(wire)
                    if len(targets)>50 or time.monotonic()>deadline:break
                    page.wait_for_timeout(100)
                assert len(targets)>50,[(t['type'],t['url']) for t in targets]
                assert not _cdp_startup_exceeds_sdk_capacity(endpoint)
                script='''import asyncio,sys
from browser_use import Browser
async def main():
    browser=Browser(cdp_url=sys.argv[1],keep_alive=True,enable_default_extensions=False)
    try:
        await asyncio.wait_for(browser.start(),20)
        assert browser.session_manager.get_all_page_targets()
        print('ATTACHED_EXISTING_BROWSER')
    finally:
        await asyncio.wait_for(browser.stop(),5)
asyncio.run(main())
'''
                env={**os.environ,'ANONYMIZED_TELEMETRY':'false'}
                result=subprocess.run([worker,'-c',script,endpoint],capture_output=True,text=True,timeout=35,env=env)
                assert result.returncode==0 and 'ATTACHED_EXISTING_BROWSER' in result.stdout,(result.stdout,result.stderr)
                assert not page.is_closed()
                assert any(c['name']=='session' and c['value']=='existing' for c in context.cookies(url))
            finally:context.close()
    finally:site.shutdown();site.server_close()


@pytest.mark.parametrize('failure', ['capacity','root_stalled'])
def test_sdk_controller_handoff_reads_same_signed_in_browser(tmp_path, monkeypatch, source_summary_model_reply, failure):
    worker = os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    if not worker: pytest.skip('installed isolated SDK runtime required')
    from playwright.sync_api import sync_playwright
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path,monkeypatch)
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            body = b'<h1>SIGNED_IN</h1><p>Protected option: 37</p>' if 'session=existing' in self.headers.get('Cookie','') else b'<h1>LOGIN_REQUIRED</h1>'
            self.send_response(200);self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    site = ThreadingHTTPServer(('127.0.0.1',0),Site)
    threading.Thread(target=site.serve_forever,daemon=True).start()
    url = f'http://localhost:{site.server_port}/'
    port = _free_port()
    stalled_server = None
    if failure == 'root_stalled':
        from websockets.sync.server import serve
        def silent(connection):
            try:
                for _ in connection: pass
            except Exception: pass
        stalled_server = serve(silent, '127.0.0.1', 0)
        threading.Thread(target=stalled_server.serve_forever, daemon=True).start()
    class Proxy(BaseHTTPRequestHandler):
        def do_GET(self):
            with urlopen(f'http://127.0.0.1:{port}'+self.path,timeout=3) as response: raw=response.read()
            if self.path=='/json/list' and failure=='capacity': raw=json.dumps(json.loads(raw)*55).encode()
            if self.path=='/json/version' and failure=='root_stalled':
                value=json.loads(raw);value['webSocketDebuggerUrl']=f'ws://127.0.0.1:{stalled_server.socket.getsockname()[1]}/devtools/browser/stalled'
                raw=json.dumps(value).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(raw)
        def do_PUT(self):
            from urllib.request import Request
            with urlopen(Request(f'http://127.0.0.1:{port}'+self.path,method='PUT'),timeout=3) as response:raw=response.read()
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(raw)
        def log_message(self,*args):pass
    proxy = ThreadingHTTPServer(('127.0.0.1',0),Proxy)
    threading.Thread(target=proxy.serve_forever,daemon=True).start()
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','cdp')
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL',f'http://127.0.0.1:{proxy.server_port}')
    monkeypatch.setenv('NULLION_BROWSER_USE_CDP_URL','http://127.0.0.1:1')
    monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON',worker)
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    registry = ToolRegistry()
    class Model:
        phase = 0
        def create(self,**kwargs):
            assert not any(t['name']=='browser_decision' for t in kwargs.get('tools',[])), 'An attach failure must not start an independent worker browser'
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            summary=source_summary_model_reply(kwargs)
            if summary is not None:return summary
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task']})
            if self.phase==2:return call('browser_run_task',{'task':'Read the protected option from the existing browser session.','url':url})
            if self.phase==3:
                # The first call performs the registered native read preflight.
                # Exercise the declared adaptive continuation to reach SDK attach.
                assert 'native_read_preflight' in json.dumps(kwargs['messages'])
                return call('browser_run_task',{'task':'Read the protected option from the existing browser session.','url':url,
                    'execution_kind':'adaptive_interaction','interaction_reason':'Inspect the protected session after its native read.'})
            if self.phase==4:
                assert 'alternate_tool' in json.dumps(kwargs['messages'])
                return call('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_open','browser_navigate','browser_extract_text'],'required_tool_names':['browser_navigate','browser_extract_text']})
            if self.phase==5:return call('browser_open',{})
            if self.phase==6:return call('browser_navigate',{'url':url})
            if self.phase==7:return call('browser_extract_text',{})
            assert self.phase==8 and 'SIGNED_IN' in json.dumps(kwargs['messages'])
            return text('Signed-in session retained. Protected option: 37.')
    model=Model()
    try:
        with sync_playwright() as pw:
            context = pw.chromium.launch_persistent_context(str(tmp_path/'user-profile'),headless=True,
                args=[f'--remote-debugging-port={port}'])
            try:
                context.add_cookies([{'name':'session','value':'existing','url':url}])
                user_page=context.new_page();user_page.goto(url)
                assert 'SIGNED_IN' in user_page.inner_text('body')
                register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
                with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
                    response=client.post('/api/chat',json={'text':'既存のログイン済みブラウザで選択肢を読んで','conversation_id':'web:shared-login'})
                assert response.status_code==200 and '37' in response.json()['text'],response.text
                assert 'LOGIN_REQUIRED' not in response.json()['text'] and 'could not attach' not in response.json()['text']
                assert not user_page.is_closed()
                assert any(c['name']=='session' and c['value']=='existing' for c in context.cookies(url))
                turns=[e for e in runtime.store.list_conversation_events('web:shared-login') if e.get('event_type')=='conversation.chat_turn']
                results=turns[-1]['tool_results']
                assert any(r['tool_name']=='browser_run_task' and r['output'].get('continuation',{}).get('preserve_session') is True for r in results),json.dumps(results)
                assert any(r['tool_name']=='browser_extract_text' and r['status']=='completed' for r in results)
            finally:context.close()
    finally:
        site.shutdown();site.server_close();proxy.shutdown();proxy.server_close()
        if stalled_server is not None:stalled_server.shutdown()
