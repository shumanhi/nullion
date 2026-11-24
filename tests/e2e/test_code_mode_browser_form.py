"""Real UI controls in one program; fake only the external site and model."""
import os
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('landing', [False, True])
def test_native_form_interactions_compose_without_booking_or_extra_model_turns(tmp_path,monkeypatch,landing):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from nullion.preferences import save_preferences
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    save_preferences({'outbound_request_mode':'allow_all'})
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false')
    posts=[]
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers()
            if landing and self.path == '/':
                self.wfile.write(
                    b'<html><body><a href="/form">Formulario</a>'
                    b'<input name="account" type="password" value="FixturePassword482">'
                    b'<input name="background" style="visibility:hidden" value="FixtureHidden093">'
                    b'<input name="transparent" style="opacity:0" value="FixtureTransparent145">'
                    b'<input name="code" autocomplete="one-time-code" value="908754">'
                    b'<input name="public" value="Visible route">'
                    b'</body></html>'
                )
                return
            self.wfile.write(b'''<html><body><form onsubmit="event.preventDefault(); document.querySelector('#results').textContent='Available for '+document.querySelector('#party').value+' on '+document.querySelector('#date').value+' at 6:00 PM';">
            <label>Party<input id="party" name="party"></label><label>Date<input id="date" name="date"></label>
            <button id="search">Find availability</button></form><main id="results"></main></body></html>''')
        def do_POST(self):
            posts.append(self.path);self.send_response(500);self.end_headers()
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site)
    threading.Thread(target=site.serve_forever,daemon=True).start()
    url=f'http://localhost:{site.server_port}/'
    names=['browser_navigate','browser_snapshot','browser_type_field','browser_click','browser_extract_text']
    code=("page = call_tool('browser_navigate', {'url':"+repr(url)+"})\n"
        "session = page['output']['session_id']\n"
        + ("snapshot = call_tool('browser_snapshot', {'session_id':session})['output']['snapshot']\n"
            "destination = [e['href'] for e in snapshot['elements'] if e.get('tag') == 'a'][0]\n"
            "call_tool('browser_navigate', {'url':destination,'session_id':session})\n" if landing else "")
        + "call_tool('browser_type_field', {'selector':'#party','text':'2','session_id':session})\n"
        "call_tool('browser_type_field', {'selector':'#date','text':'2026-11-15','session_id':session})\n"
        "call_tool('browser_click', {'selector':'#search','session_id':session})\n"
        "call_tool('browser_extract_text', {'selector':'#results','ready_selector':'#results','session_id':session})['output']")
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':names,'required_tool_names':names})
            if self.phase==2:
                visible={d['name'] for d in kwargs['tools']}
                assert not visible.intersection(names) and 'code_mode' in visible
                return call('code_mode',{'tool_names':names,'code':code})
            assert self.phase==3, 'Native interactions re-entered the model'
            return text('Available for 2 on November 15 at 6:00 PM. Nothing was booked.')
    model=Model();registry=ToolRegistry()
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            started=time.monotonic()
            response=client.post('/api/chat',json={'text':'Consulta disponibilidad para dos personas sin reservar','conversation_id':'web:code-ui'})
            assert response.status_code==200 and '6:00 PM' in response.json()['text'],response.text
            results=[r for e in runtime.store.list_conversation_events('web:code-ui')
                if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
            navigation=[r for r in results if r['tool_name']=='browser_navigate']
            assert navigation and all(r['output']['state_verification'] == {
                'navigation_completed': True, 'controls_verified': False} for r in navigation)
            if landing:
                snapshot = next(r["output"]["snapshot"] for r in results if r["tool_name"] == "browser_snapshot")
                controls = {row["name"]: row for row in snapshot["elements"] if row.get("name")}
                assert controls["public"]["value"] == "Visible route"
                assert controls["background"]["visible"] is False
                for name in ("account", "background", "transparent", "code"):
                    assert controls[name]["element_id"] and controls[name]["value"] == ""
                for private_value in ("FixturePassword482", "FixtureHidden093", "FixtureTransparent145", "908754"):
                    assert private_value not in str(results)
            program=next(r for r in results if r['tool_name']=='code_mode')
            assert program['status']=='completed',program
            assert '2026-11-15' in str(program['output']['value']) and '6:00 PM' in str(program['output']['value'])
            from nullion.conversation_history_tools import _recent_browser_snapshot
            capture = _recent_browser_snapshot(runtime.store, 'web:code-ui', 'workspace_admin', 60, now=time.time())
            assert '6:00 PM' in capture['page']['text'] and capture['page']['source_url'].startswith(url)
            assert len(program['output']['operations'])==(7 if landing else 5)
            assert [r['tool_name'] for r in program['output']['reads']]==(['browser_navigate','browser_snapshot','browser_navigate','browser_extract_text'] if landing else ['browser_navigate','browser_extract_text'])
            assert sum(r['operation_kind']=='ui_interaction' for r in program['output']['operations'])==3
            assert model.phase==3 and time.monotonic()-started<5
            assert not posts
    finally:
        registry.run_cleanup_hooks();site.shutdown();site.server_close()


def test_failed_blank_page_capture_can_navigate_and_deliver_png_in_same_session(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from nullion.preferences import save_preferences
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);save_preferences({'outbound_request_mode':'allow_all'})
    for key,value in [('NULLION_CODE_MODE','on'),('NULLION_BROWSER_BACKEND','playwright'),
        ('NULLION_BROWSER_HEADLESS','true'),('NULLION_BROWSER_BLOCK_PRIVATE','false')]:monkeypatch.setenv(key,value)
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers()
            self.wfile.write(b'<html><body style="margin:0;background:rgb(17,34,51)">Verified capture</body></html>')
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site);threading.Thread(target=site.serve_forever,daemon=True).start()
    url=f'http://localhost:{site.server_port}/'
    prompt=f'Capture {url} as a .png image.'
    names=['browser_open','browser_snapshot','browser_navigate','browser_screenshot']
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if any(t['name']=='select_attachment_format' for t in kwargs.get('tools',[])):
                return call('select_attachment_format',{'extension':'.png','confidence':1})
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':names,
                'required_tool_names':names,'artifact_extensions':['.png'],
                'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt})
            if self.phase==2:
                offered = next(t for t in kwargs['tools'] if t['name']=='code_mode')['input_schema']['properties']['tool_names']['items']['enum']
                assert set(names).issubset(offered), offered
                return call('code_mode',{'tool_names':names,
                'code':"page = call_tool('browser_open', {})\nsession = page['output']['session_id']\n"
                    "call_tool('browser_snapshot', {'session_id':session})\n"
                    "call_tool('browser_screenshot', {'session_id':session,'mode':'viewport'})"})
            if self.phase==3:
                result=call('code_mode',{'tool_names':['browser_navigate','browser_screenshot'],
                    'code':f"call_tool('browser_navigate', {{'url':{url!r},'session_id':session}})\n"
                        "call_tool('browser_screenshot', {'session_id':session,'mode':'viewport'})['output']"})
                result['content'][0]['id']='corrected-capture';return result
            assert self.phase==4
            return text('Captured the verified page image.')
    model=Model();reg=ToolRegistry();register_browser_tools(reg,policy=BrowserPolicy(),model_client_getter=lambda:model)
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),reg)) as client:
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:capture-repair'})
            assert response.status_code==200 and response.json()['artifacts'], (response.text,model.phase,[(r['tool_name'],r['status'],r.get('output',{})) for e in runtime.store.list_conversation_events('web:capture-repair') if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])])
            artifacts=response.json()['artifacts'];assert len(artifacts)==1
            downloaded=client.get(artifacts[0]['url']);assert downloaded.content.startswith(b'\x89PNG\r\n\x1a\n')
            import io
            with Image.open(io.BytesIO(downloaded.content)) as image:assert image.convert('RGB').getpixel((10,100))==(17,34,51)
            results=[r for e in runtime.store.list_conversation_events('web:capture-repair')
                if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
            programs=[r for r in results if r['tool_name']=='code_mode']
            assert [r['status'] for r in programs]==['failed','completed']
            assert programs[0]['output']['session_id']==programs[1]['output']['session_id']
            assert programs[0]['output']['failures'][0]['required_tool_names']==['browser_navigate']
            assert model.phase==4
    finally:
        reg.run_cleanup_hooks();site.shutdown();site.server_close()


def test_numeric_page_checks_reuse_currency_index_and_remain_fast(tmp_path,monkeypatch,record_property):
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
    for key,value in [('NULLION_CODE_MODE','on'),('NULLION_BROWSER_BACKEND','playwright'),
        ('NULLION_BROWSER_HEADLESS','true'),('NULLION_BROWSER_BLOCK_PRIVATE','false')]:monkeypatch.setenv(key,value)
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers()
            rows=''.join(f'<section><p>Route {i} departs at {10+i%10}:53. Fare USD {i+9}.00</p></section>' for i in range(30))
            self.wfile.write(('<html lang="en"><body>'+rows+'<p>東京 37 · Madrid 73 · Paris 91</p><p>Price $34.00</p><p>Return 2030-06-10</p><span style="display:none">Secret 402</span></body></html>').encode())
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site);threading.Thread(target=site.serve_forever,daemon=True).start()
    url=f'http://localhost:{site.server_port}/';names=['browser_navigate','browser_assert_page_state']
    class Model:
        phase=0
        errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':names,'required_tool_names':names})
            if self.phase==2:return call('code_mode',{'tool_names':names,'code':
                "page=call_tool('browser_navigate',{'url':"+repr(url)+"})\n"
                "call_tool('browser_assert_page_state',{'session_id':page['output']['session_id'],"
                "'required':['USD 34.00','2030-06-10','東京 37'],'forbidden':['Secret 402']})['output']"})
            if self.phase!=3:self.errors.append(self.phase)
            return text('USD 34.00 · 2030-06-10 · 東京 37')
    model=Model();registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    samples=[]
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            for i,prompt in enumerate(['ページの値と日付を確認してください。','Comprueba los valores y la fecha de la página.','Vérifie les valeurs et la date de la page.']):
                model.phase=0;cid='web:numeric-page-'+str(i);started=time.monotonic()
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});samples.append(time.monotonic()-started)
                assert response.status_code==200 and '34.00' in response.json()['text'] and not model.errors,response.text
                turn=next(e for e in reversed(runtime.store.list_conversation_events(cid)) if e.get('event_type')=='conversation.chat_turn')
                assertion=next(r for r in turn['tool_results'] if r['tool_name']=='browser_assert_page_state')
                assert assertion['status']=='completed' and assertion['output']['result']['ok'],assertion
                assert turn['response_fulfilled'] is True
        assert sum(samples)/len(samples)<4 and max(samples)<6,samples
        record_property('mean_numeric_page_check_seconds',sum(samples)/len(samples))
    finally:registry.run_cleanup_hooks();site.shutdown();site.server_close()


@pytest.mark.parametrize('backend',['playwright','cdp'])
def test_destination_controls_distinguish_link_page_from_open_form(tmp_path,monkeypatch,record_property,backend):
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
    for key,value in [('NULLION_CODE_MODE','on'),('NULLION_BROWSER_BACKEND',backend),
        ('NULLION_BROWSER_HEADLESS','true'),('NULLION_BROWSER_BLOCK_PRIVATE','false')]:monkeypatch.setenv(key,value)
    posts=[]
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers()
            if self.path=='/feedback':body='<form><label>Order<input name="order"></label><label>Message<textarea name="message"></textarea></label><button>Send</button></form>'
            else:body=''.join(f'<a href="/menu/{n}">Menu {n}</a>' for n in range(85))+'<h1>Service center</h1><a href="/feedback">意見フォーム</a>'
            self.wfile.write(('<html><body>'+body+'</body></html>').encode())
        def do_POST(self):posts.append(self.path);self.send_error(500)
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site);threading.Thread(target=site.serve_forever,daemon=True).start()
    url=f'http://localhost:{site.server_port}/';names=['browser_navigate','browser_extract_detail','browser_click_element','browser_snapshot','browser_assert_page_state']
    class Model:
        phase=0
        errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':names,'required_tool_names':names})
            if self.phase==2:return call('code_mode',{'tool_names':names,'code':
                "page=call_tool('browser_navigate',{'url':"+repr(url)+"})\nsid=page['output']['session_id']\n"
                "landing=call_tool('browser_extract_detail',{'session_id':sid})['output']\n"
                "assert landing['page_controls']['form_count']==0, 'No form is open yet'\n"
                "assert landing['page_controls']['editable_control_count']==0, 'No editable controls yet'\n"
                "call_tool('browser_click_element',{'session_id':sid,'text':'意見フォーム','role':'link'})\n"
                "form=call_tool('browser_snapshot',{'session_id':sid})['output']\n"
                "checked=call_tool('browser_assert_page_state',{'session_id':sid,'selector':'form','required':['Order','Message']})['output']\n"
                "assert checked['result']['page_controls']['form_count']==1, 'Actual form required'\n"
                "assert checked['result']['page_controls']['editable_control_count']==2, 'Actual fields required'\nform"})
            if self.phase!=3:self.errors.append(self.phase)
            return text('Form open; fields are blank. Nothing submitted.')
    from tests.e2e.test_web_surface_contracts import _free_port
    port=_free_port()
    if backend=='cdp':monkeypatch.setenv('NULLION_BROWSER_CDP_URL',f'http://127.0.0.1:{port}')
    model=Model();registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model);samples=[]
    controller=None;desktop=None
    try:
        if backend=='cdp':
            from playwright.sync_api import sync_playwright
            controller=sync_playwright().start()
            desktop=controller.chromium.launch_persistent_context(str(tmp_path/'profile'),headless=True,args=[f'--remote-debugging-port={port}'])
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            for i,prompt in enumerate(['フォームを開くだけにして、記入や送信はしないでください。','Abre el formulario, sin rellenarlo ni enviarlo.','Ouvre le formulaire sans le remplir ni l’envoyer.']):
                model.phase=0;cid='web:destination-form-'+str(i);started=time.monotonic()
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});samples.append(time.monotonic()-started)
                assert response.status_code==200 and not model.errors,response.text
                turn=next(e for e in reversed(runtime.store.list_conversation_events(cid)) if e.get('event_type')=='conversation.chat_turn')
                assert turn['response_fulfilled'] is True,turn
                snapshot=next(r['output']['snapshot'] for r in turn['tool_results'] if r['tool_name']=='browser_snapshot' and r['status']=='completed' and isinstance(r.get('output',{}).get('snapshot'),dict))
                assert snapshot['url']==url+'feedback'
                fields=[e for e in snapshot['elements'] if e.get('name') in {'order','message'}]
                assert {e['name'] for e in fields}=={'order','message'} and all(e.get('value')=='' for e in fields)
                assert not posts
        assert sum(samples)/len(samples)<5 and max(samples)<8,samples
        record_property('mean_destination_form_seconds',sum(samples)/len(samples))
    finally:
        registry.run_cleanup_hooks()
        if desktop:desktop.close()
        if controller:controller.stop()
        site.shutdown();site.server_close()
