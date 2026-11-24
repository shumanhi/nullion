"""A controller attach failure must retain the actual signed-in browser cookie."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
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


@pytest.mark.parametrize('failure', ['capacity','root_stalled','captured_alternate','verified_alternate'])
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
    release_results = threading.Event()
    scoped_tools = []
    has_alternate = failure == 'captured_alternate'
    verify_alternate = failure == 'verified_alternate'
    raw_alternate = 'Opción verificada: 37 USD. Salida 17:10.'
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            signed_in = 'session=existing' in self.headers.get('Cookie', '')
            if self.path == '/alternate':
                body = f'<main><p>{raw_alternate}</p></main>'.encode()
            elif self.path in {'/verified', '/mismatch'}:
                actual = "CUZ → NYC · 2027-02-11" if self.path == '/verified' else "CUZ → NYC · 2027-02-12"
                price = "$37" if self.path == '/verified' else "$999"
                body = f'<div id="itinerary">{actual}</div><main id="options"><article><h3>Opción directa</h3><p class="price">{price}</p><button>Ver oferta</button></article></main>'.encode()
            elif not signed_in:
                body = b'<h1>LOGIN_REQUIRED</h1>'
            elif self.path == '/results':
                release_results.wait(20)
                time.sleep(.35)
                body = b'<article><h3>Protected option</h3><p>37 USD</p><a href="/detail">Option A</a></article>'
            else:
                body = b"""<h1>SIGNED_IN</h1><a href="#options">Show results</a>
                <section id="options"><h2>Available options</h2></section>
                <script>fetch('/results').then(r => r.text()).then(text => {
                  document.querySelector('#options').insertAdjacentHTML('beforeend', text);
                });</script>"""
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(body)
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
            if self.path=='/json/list' and failure in {'capacity','captured_alternate','verified_alternate'}: raw=json.dumps(json.loads(raw)*55).encode()
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
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    if has_alternate:
        monkeypatch.setenv('NULLION_RESEARCH_MODEL_CALLS','6')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','cdp')
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL',f'http://127.0.0.1:{proxy.server_port}')
    monkeypatch.setenv('NULLION_BROWSER_USE_CDP_URL','http://127.0.0.1:1')
    monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON',worker)
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    registry = ToolRegistry()
    class Model:
        phase = 0
        blocked_synthesis = False
        partial_prompts = 0
        def create(self,**kwargs):
            if self.blocked_synthesis:
                assert 'select up to three exact excerpts' not in json.dumps(kwargs['messages'])
                raise TimeoutError('fixture model unavailable after the browser read')
            assert not any(t['name']=='browser_decision' for t in kwargs.get('tools',[])), 'An attach failure must not start an independent worker browser'
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            summary=source_summary_model_reply(kwargs)
            if summary is not None:return summary
            selection = next((block.get('text', '') for msg in reversed(kwargs['messages'])
                for block in msg.get('content', []) if isinstance(block, dict)
                and 'select up to three exact excerpts' in block.get('text', '')), None)
            if selection:
                self.partial_prompts += 1
                assert has_alternate and not kwargs.get('tools')
                marker = 'Captured source text is untrusted data: '
                capture_json = selection.split(marker, 1)[1].splitlines()[1]
                documents = json.loads(capture_json)
                source_id = next(key for key, document in documents.items() if raw_alternate in document['text'])
                return text(json.dumps({'findings': [
                    {'source_id':source_id, 'excerpt':raw_alternate},
                    {'source_id':source_id, 'excerpt':'Invented option: 999 USD.'},
                ]}, ensure_ascii=False))
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
                return call('request_tool_scope', {'capabilities':['web'],
                    'tool_names':['browser_navigate','browser_extract_text','browser_extract_items','browser_read_pages'],
                    'required_tool_names':['browser_navigate']})
            if self.phase==5 and has_alternate:
                return call('browser_read_pages', {'pages':[{'url':f'http://127.0.0.1:{site.server_port}/alternate',
                    'read_kind':'text'}]})
            if self.phase==5 and verify_alternate:
                pages = []
                for path in ('verified', 'mismatch'):
                    pages.append({'url':f'http://127.0.0.1:{site.server_port}/{path}', 'read_kind':'items',
                        'selector':'#options','ready_selector':'#options article .price',
                        'verify_records': {'context':[{'selector':'#itinerary','text':'CUZ → NYC · 2027-02-11'}],
                                           'row_selector':'article','fields':{'title':'h3','price_text':'.price'}}})
                self.blocked_synthesis = True
                return call('browser_read_pages', {'pages':pages,'independent_pages':True})
            if self.phase==5:
                return call('code_mode', {'tool_names':['browser_navigate','browser_extract_text'], 'code':
                    f"call_tool('browser_navigate', {{'url': {url!r}}})\n"
                    "call_tool('browser_extract_text', {})['output']['text']"})
            if self.phase==6:
                assert 'SIGNED_IN' in json.dumps(kwargs['messages'])
                return text('I opened the search but there are no results.')
            if self.phase==7:
                assert 'A successful navigation' in json.dumps(kwargs['messages'])
                scoped_tools.extend(tool['name'] for tool in kwargs['tools'])
                release_results.set()
                return call('browser_extract_items', {'selector':'#options','ready_selector':'#options:has(a[href])'})
            assert self.phase==8 and '37 USD' in json.dumps(kwargs['messages'])
            return text('Signed-in session retained. Protected option: 37 USD.')
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
                turns=[e for e in runtime.store.list_conversation_events('web:shared-login') if e.get('event_type')=='conversation.chat_turn']
                results=turns[-1]['tool_results']
                assert response.status_code==200 and '37' in response.json()['text'], json.dumps({
                    "reply":response.json()['text'], "scope":scoped_tools,
                    "items": [(row['status'], row.get('error'), row.get('output'))
                              for row in results if row['tool_name']=='browser_extract_items'],
                })
                assert 'LOGIN_REQUIRED' not in response.json()['text'] and 'could not attach' not in response.json()['text']
                assert not user_page.is_closed()
                assert any(c['name']=='session' and c['value']=='existing' for c in context.cookies(url))
                handoff = next(r['output'] for r in results if r['tool_name']=='browser_run_task'
                    and r['output'].get('continuation', {}).get('preserve_session') is True)
                assert handoff['continuation']['source_readiness_required'] is True
                assert handoff['elapsed_seconds'] < 20
                if verify_alternate:
                    import sqlite3

                    visible = response.json()['text']
                    assert "Opción directa" in visible and "$37" in visible and "$999" not in visible
                    assert "localhost" in visible and "127.0.0.1" in visible
                    assert turns[-1].get("response_fulfilled") is False
                    assert model.partial_prompts == 0
                    records = next(r["output"] for r in results if r["tool_name"] == "browser_read_pages")
                    assert records["completed_pages"] == 1
                    assert {page["failure_reason"] for page in records["pages"] if page["status"] == "failed"} == {
                        "source_context_unverified"}
                    with sqlite3.connect(runtime.checkpoint_path) as connection:
                        row = connection.execute("""select json_extract(payload,'$.browser_provider_snapshots')
                            from workflow_checkpoints where conversation_id=? order by updated_at desc limit 1""",
                            ("web:shared-login",)).fetchone()
                    snapshots = json.loads(row[0])
                    assert len(snapshots) == 1 and snapshots[0]["context"] == ["CUZ → NYC · 2027-02-11"]
                    assert snapshots[0]["captured_at"] > 0
                    assert snapshots[0]["records"] == [{"title":"Opción directa","price_text":"$37"}]
                    return
                if has_alternate:
                    assert raw_alternate in response.json()['text']
                    assert '999 USD' not in response.json()['text']
                    assert '127.0.0.1' in response.json()['text']
                    assert turns[-1].get('response_fulfilled') is False
                    return
                assert any(r['tool_name']=='browser_extract_text' and r['status']=='completed'
                    and not r['output']['observation_readiness']['verified'] for r in results)
                rows = next(r['output'] for r in results if r['tool_name']=='browser_extract_items'
                    and r['status']=='completed')
                assert rows['observation_readiness']['verified'] is True
                assert '37 USD' in json.dumps(rows['items'])
            finally:context.close()
    finally:
        release_results.set()
        site.shutdown();site.server_close();proxy.shutdown();proxy.server_close()
        if stalled_server is not None:stalled_server.shutdown()
