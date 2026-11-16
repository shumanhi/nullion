"""Real chat reopens a closed configured CDP browser with its saved profile."""
import asyncio
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface',['web','scheduled'])
@pytest.mark.parametrize('explicit_preference',[True,False])
@pytest.mark.parametrize('backend',['auto','cdp'])
def test_chat_reopens_same_browser_profile_after_each_close(tmp_path,monkeypatch,record_property,surface,explicit_preference,backend):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.backends.cdp_backend import _RawCDPClient,_read_json_url
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);save_preferences({'outbound_request_mode':'allow_all'})
    cookies=[]
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            cookies.append(self.headers.get('Cookie',''))
            self.send_response(200);self.end_headers()
            self.wfile.write(b'<main>Saved profile value: 37</main>')
        def log_message(self,*args):pass
    source=ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=source.serve_forever,daemon=True).start()
    url=f'http://localhost:{source.server_port}/record';port=_free_port();endpoint=f'http://127.0.0.1:{port}'
    profile=(tmp_path/'saved-profile' if explicit_preference else tmp_path/'home'/'browser-profiles'/'brave')
    launches=tmp_path/'launches.jsonl'
    with sync_playwright() as p:
        executable=p.chromium.executable_path
        seeded=p.chromium.launch_persistent_context(str(profile),headless=True)
        seeded.add_cookies([{'url':url,'name':'saved-login','value':'37','expires':time.time()+3600}])
        seeded.close()
    # Only the external desktop executable boundary differs in Docker. Nullion
    # supplies the same visible-browser argv and never sets a headless flag.
    wrapper=tmp_path/'Brave'
    wrapper.write_text('#!/usr/local/bin/python\nimport json,os,sys\n'
        +f'with open({str(launches)!r},"a") as f: f.write(json.dumps(sys.argv[1:])+"\\n")\n'
        +f'os.execv({executable!r},[{executable!r},"--headless","--no-sandbox",*sys.argv[1:]])\n')
    wrapper.chmod(0o700)
    if not explicit_preference:
        from nullion.plugins.browser_plugin.backends import auto_backend
        chrome=tmp_path/'Chrome';chrome.write_text(wrapper.read_text());chrome.chmod(0o700)
        monkeypatch.setattr(auto_backend,'_CHROME_CANDIDATES',{'Linux':[str(chrome),str(wrapper)]})
    for key,value in {'NULLION_HOME':str(tmp_path/'home'),'NULLION_CODE_MODE':'on','NULLION_BROWSER_BACKEND':backend,
            'NULLION_BROWSER_CDP_URL':endpoint,'NULLION_BROWSER_PREFERRED':'brave','NULLION_BROWSER_PATH':str(wrapper),
            'NULLION_BROWSER_USER_DATA_DIR':str(profile),'NULLION_BROWSER_BLOCK_PRIVATE':'false'}.items():monkeypatch.setenv(key,value)
    monkeypatch.delenv('NULLION_BROWSER_HEADLESS',raising=False)
    if not explicit_preference:
        for key in ['NULLION_BROWSER_PREFERRED','NULLION_BROWSER_PATH','NULLION_BROWSER_USER_DATA_DIR']:
            monkeypatch.delenv(key,raising=False)
    async def close_browser():
        client=None
        try:
            version=await asyncio.to_thread(_read_json_url,endpoint+'/json/version')
            client=_RawCDPClient(version['webSocketDebuggerUrl'])
            await client.send('Browser.close',timeout=3)
        except (OSError,RuntimeError,TimeoutError):pass
        finally:
            if client:await client.close()
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.1):pass
            except OSError:return
            await asyncio.sleep(.05)
        pytest.fail('Fixture browser did not close')
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_read_pages'],'required_tool_names':['browser_read_pages']})
            if self.phase==2:return call('code_mode',{'tool_names':['browser_read_pages'],
                'code':"call_tool('browser_read_pages', "+repr({'pages':[{'url':url,'read_kind':'text'}]})+")"})
            assert 'Saved profile value: 37' in json.dumps(kw['messages'])
            return text('Saved profile value: 37. '+url)
    model=Model();registry=ToolRegistry()
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    if surface=='scheduled':
        from nullion.crons import CronJob
        from tests.e2e.test_scheduled_report_completion import run_scheduled,telegram_boundary
        deliveries,send=telegram_boundary(monkeypatch)
    samples=[]
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            for i,prompt in enumerate(['このページの値を確認して。','Consulta el valor de esta página.','Read this source value.']):
                model.phase=0;start=time.monotonic()
                if surface=='web':
                    response=client.post('/api/chat',json={'text':prompt+' '+url,'conversation_id':'web:reopen-'+str(i)})
                    assert response.status_code==200 and '37' in response.json()['text'],response.text
                else:
                    before=len(deliveries)
                    result=run_scheduled(CronJob(id='reopen-'+str(i),name='確認',task=prompt+' '+url,
                        schedule='* * * * *',delivery_channel='telegram',delivery_target='123'),model,runtime,registry,send)
                    assert result['response_fulfilled'] is True,result
                    assert not result.get('cron_report_failed'),result
                    assert len(deliveries)==before+1 and '37' in deliveries[-1].get('text',''),deliveries[-1:]
                samples.append(time.monotonic()-start)
                assert os.environ['NULLION_BROWSER_CDP_URL']==endpoint
                # Normal request cleanup leaves the signed-in process alive.
                # Closing it below is an explicit failure injection only.
                assert _read_json_url(endpoint+'/json/version')['webSocketDebuggerUrl']
                asyncio.run(close_browser())
        arguments=[json.loads(line) for line in launches.read_text().splitlines()]
        assert len(arguments)==3
        assert all(f'--remote-debugging-port={port}' in args and f'--user-data-dir={profile}' in args for args in arguments)
        assert not any('--headless' in args for args in arguments)
        assert len(cookies)>=3 and all('saved-login=37' in cookie for cookie in cookies),cookies
        assert sum(samples)/len(samples)<8 and max(samples)<12,samples
        record_property('mean_reopen_seconds',sum(samples)/len(samples))
    finally:
        asyncio.run(close_browser());source.shutdown();source.server_close()
