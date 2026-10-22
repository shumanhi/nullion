"""Real API, SQLite, browser, scheduler, and transport boundary regressions."""
from __future__ import annotations
import asyncio
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from playwright.sync_api import expect

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E only')


@pytest.fixture
def environment(tmp_path,monkeypatch):
    from nullion import crons,users,connections
    home=tmp_path/'lane';home.mkdir()
    for key,value in {'NULLION_HOME':home,'NULLION_ENV_FILE':home/'.env','NULLION_CHECKPOINT_PATH':home/'runtime.db',
        'NULLION_CHAT_STARTUP_WARMUP':'0','NULLION_KEY_STORAGE':'local','NULLION_MEMORY_ENABLED':'0',
        'NULLION_WORKSPACE_STORAGE_ROOT':home/'workspaces','NULLION_DATA_DIR':home}.items():
        monkeypatch.setenv(key,str(value))
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',home/'runtime.db')
    monkeypatch.setattr(users,'_USERS_PATH',home/'users.json')
    monkeypatch.setattr(connections,'_CONNECTIONS_PATH',home/'connections.json')
    from nullion.runtime import bootstrap_persistent_runtime
    runtime=bootstrap_persistent_runtime(home/'runtime.db');runtime.checkpoint()
    return home,runtime


def _app(runtime):
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry
    return create_app(runtime,None,ToolRegistry())


def _local_release(tmp_path,monkeypatch):
    source=tmp_path/'source';source.mkdir()
    def git(*args,cwd=source):
        return subprocess.run(['git',*args],cwd=cwd,check=True,text=True,capture_output=True)
    git('init','-b','main');(source/'fixture.txt').write_text('Fixture')
    git('add','fixture.txt');git('-c','user.name=Fixture','-c','user.email=fixture@example.test','commit','-m','Fixture')
    git('tag','v1.0.0')
    remote=tmp_path/'remote.git';git('clone','--bare',str(source),str(remote))
    git('remote','add','origin',str(remote))
    monkeypatch.setenv('NULLION_SRC_DIR',str(source))
    monkeypatch.setenv('NULLION_INSTALL_DIR',str(tmp_path/'install'))
    return source


def test_update_api_is_observation_only_until_protected_post(environment,tmp_path,monkeypatch):
    home,runtime=environment;_local_release(tmp_path,monkeypatch)
    with TestClient(_app(runtime)) as client:
        assert client.get('/api/update',headers={'Sec-Fetch-Site':'cross-site'}).status_code==405
        assert client.post('/api/update',json={'request_key':'fixture'}).status_code==403
        csrf=client.get('/api/session').json()['csrf_token']
        headers={'x-nullion-csrf':csrf}
        first=client.post('/api/update',json={'request_key':'fixture'},headers=headers)
        second=client.post('/api/update',json={'request_key':'fixture'},headers=headers)
        assert first.status_code==second.status_code==202
        assert first.json()['operation_id']==second.json()['operation_id']
        stream=client.get(first.json()['stream_url'])
        assert stream.status_code==200 and '"success": true' in stream.text
        assert client.get(first.json()['stream_url'],headers={'Last-Event-ID':'1'}).status_code==200
    with sqlite3.connect(home/'runtime.db') as conn:
        assert conn.execute('SELECT COUNT(*) FROM update_operations').fetchone()==(1,)
        assert conn.execute('SELECT status FROM update_operations').fetchone()==('completed',)


def test_real_update_browser_uses_one_post_then_observes(environment,tmp_path,monkeypatch):
    from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
    home,runtime=environment;_local_release(tmp_path,monkeypatch)
    with _UvicornThread(_app(runtime),_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        requests=[];page.on('request',lambda req:requests.append((req.method,req.url)))
        try:
            page.locator('#update-btn').click()
            page.get_by_role('button',name='Check for updates',exact=True).click()
            expect(page.locator('#update-done-title')).to_have_text('Already up to date',timeout=15000)
            posts=[url for method,url in requests if method=='POST' and url.endswith('/api/update')]
            assert len(posts)==1
            assert any(method=='GET' and '/api/update/' in url for method,url in requests)
            assert errors==[]
        finally:
            browser.close();playwright.stop()
    with sqlite3.connect(home/'runtime.db') as conn:
        assert conn.execute('SELECT COUNT(*) FROM update_operations').fetchone()==(1,)


def test_profile_browser_saves_only_active_lane(environment,tmp_path,monkeypatch):
    from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
    home,runtime=environment
    monkeypatch.setattr(Path,'home',classmethod(lambda cls:tmp_path/'operator'))
    app=_app(runtime)
    @app.middleware('http')
    async def slow_profile_hydration(request,call_next):
        response=await call_next(request)
        if request.url.path=='/api/profile' and request.method=='GET':await asyncio.sleep(.35)
        return response
    with _UvicornThread(app,_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator('#settings-btn').click()
            page.locator('.settings-nav button',has_text='Profile').click()
            expect(page.locator('#cfg-profile-name')).to_be_disabled()
            page.locator('#cfg-profile-name').fill('Fixture Person')
            expect(page.locator('#cfg-profile-name')).to_have_value('Fixture Person')
            # Settings uses the same visible save action as other preferences.
            with page.expect_response(lambda response:response.url.endswith('/api/profile') and response.request.method=='POST') as saved:
                page.locator('#settings-overlay').get_by_role('button',name='Save changes',exact=True).click()
            assert saved.value.ok
            assert json.loads((home/'profile.json').read_text())['name']=='Fixture Person'
            assert not (tmp_path/'operator'/'.nullion'/'profile.json').exists()
        finally:
            browser.close();playwright.stop()


@pytest.mark.parametrize('tab,selector,endpoint,key,original,discarded',[
    ('Profile','#cfg-profile-name','/api/profile','name','Fixture Person','Nombre descartado'),
    ('Preferences','#pref-persona','/api/preferences','persona','Fixture context','Contexte abandonné'),
])
def test_discarded_lazy_settings_never_reappear_or_save_from_another_tab(
    environment,tab,selector,endpoint,key,original,discarded,
):
    from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
    _,runtime=environment;app=_app(runtime);writes=[]
    @app.middleware('http')
    async def record_setting_writes(request,call_next):
        if request.method=='POST' and request.url.path==endpoint:writes.append(request.url.path)
        return await call_next(request)
    with TestClient(app) as client:
        assert client.post(endpoint,json={key:original}).is_success
    writes.clear()
    with _UvicornThread(app,_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator('#settings-btn').click()
            page.locator('.settings-nav button',has_text=tab).click()
            expect(page.locator(selector)).to_have_value(original)
            page.locator(selector).fill(discarded)
            page.locator('#settings-overlay').get_by_role('button',name='Close',exact=True).click()
            page.get_by_role('button',name='Discard changes',exact=True).click()
            expect(page.locator('#settings-overlay')).to_be_hidden()
            page.locator('#settings-btn').click()
            expect(page.locator(selector)).to_have_value(original)
            page.locator(selector).fill(discarded)
            page.locator('.settings-nav button',has_text='General').click()
            page.locator('#settings-overlay').get_by_role('button',name='Close',exact=True).click()
            page.get_by_role('button',name='Discard changes',exact=True).click()
            expect(page.locator('#settings-overlay')).to_be_hidden()
            page.locator('#settings-btn').click()
            with page.expect_response(lambda response:response.url.endswith('/api/config') and response.request.method=='POST') as saved:
                page.locator('#settings-overlay').get_by_role('button',name='Save changes',exact=True).click()
            assert saved.value.ok
            expect(page.locator('#save-feedback')).to_contain_text('Saved')
            assert page.request.get(server.base_url+endpoint).json()[key]==original
            assert not writes, writes
        finally:
            browser.close();playwright.stop()


@pytest.mark.parametrize('prompt',[
    'What is the Doctor monitoring threshold for the Cedar fixture scheduled task?',
    '¿Cuántas comprobaciones consecutivas usa Doctor para la tarea Cedar fixture?',
    'Cedar fixture の定期タスクについて、Doctorの連続チェック数を教えてください。',
])
def test_scheduler_monitor_threshold_reaches_agent_and_visible_chat(environment,monkeypatch,prompt):
    from nullion.crons import add_cron
    from nullion.tools import ToolRegistry,register_cron_tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    _,runtime=environment
    job=add_cron(name='Cedar fixture',task='Fixture',schedule='0 0 1 1 *',enabled=False,
                 artifact_delivery_options={'doctor_streak_limit':12})
    registry=ToolRegistry();register_cron_tools(registry)
    class Model(ScriptedProvider):
        steps=0
        def create(self,**kw):
            auxiliary=self.auxiliary(kw)
            if auxiliary:return auxiliary
            self.steps+=1;assert self.steps<=4
            if self.steps==1:return tool('request_tool_scope',{'capabilities':['scheduler_read'],
                'tool_names':['list_crons'],'scheduler_action':'inspect'})
            if self.steps==2:return tool('list_crons',{'query':'Cedar fixture'})
            evidence=json.dumps(kw['messages'],ensure_ascii=False)
            assert 'doctor_streak_limit' in evidence and '12' in evidence,evidence
            return response('Cedar fixture is paused. Doctor monitoring threshold: 12 consecutive checks.')
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=Model()),registry)) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:doctor-threshold'})
        assert result.status_code==200 and '12 consecutive checks' in result.json()['text'],result.text
    event=next(row for row in reversed(runtime.store.list_conversation_events('web:doctor-threshold'))
               if row.get('event_type')=='conversation.chat_turn')
    rows=next(row['output']['crons'] for row in event['tool_results'] if row['tool_name']=='list_crons')
    assert len(rows)==1 and rows[0]['id']==job.id and rows[0]['tracker_status']['doctor_streak_limit']==12


def test_cron_api_cannot_report_commit_after_rejected_sqlite_write(environment):
    home,runtime=environment
    with sqlite3.connect(home/'runtime.db') as conn:
        conn.execute("CREATE TRIGGER fail_cron BEFORE INSERT ON reminders_crons WHEN NEW.collection='cron_jobs' BEGIN SELECT RAISE(ABORT,'fixture write rejected'); END")
    with TestClient(_app(runtime)) as client:
        result=client.post('/api/crons',json={'name':'Fixture','task':'Fixture','schedule':'0 9 * * *'})
        assert result.status_code==500 and result.json()['ok'] is False
        assert client.get('/api/crons').json()==[]


def test_real_tool_executor_enforces_member_cron_workspace(environment):
    from nullion.users import save_user_registry,UserRegistry,NullionUser
    from nullion.tools import ToolRegistry,ToolExecutor,ToolInvocation,register_cron_tools
    from nullion.crons import list_crons
    home,runtime=environment
    save_user_registry(UserRegistry(True,[NullionUser('member','Fixture',workspace_id='workspace_member')]))
    registry=ToolRegistry();register_cron_tools(registry)
    executor=ToolExecutor(store=runtime.store,registry=registry)
    from nullion.approvals import create_permission_grant
    for name in ('list_crons','create_cron'):
        runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id='user:member',
            permission='tool:'+name,granted_by='fixture',granted_at=datetime.now(UTC)))
    for name,args in [('list_crons',{'include_all_workspaces':True}),('create_cron',{'workspace_id':'workspace_admin','name':'Fixture','task':'Fixture','schedule':'0 9 * * *'})]:
        result=executor.invoke(ToolInvocation(name,name,'user:member',args))
        assert result.status in {'denied','failed'}
        assert result.output.get('reason')=='workspace_access_denied',result
        assert not result.output.get('action_receipt')
    assert list_crons(refresh_next_runs=False)==[]
    assert runtime.store.list_events()


def test_scheduled_report_publication_uploads_safe_bytes(environment,tmp_path,monkeypatch):
    from nullion.crons import CronJob
    from nullion.cron_delivery import CronRunDeliveryCallbacks,run_cron_delivery_workflow
    from test_scheduled_report_completion import telegram_boundary
    home,runtime=environment
    directory=home/'workspaces'/'workspace_admin'/'artifacts';directory.mkdir(parents=True)
    report=directory/'fixture.html';media=directory.parent/'media';media.mkdir()
    collected=media/'collected.png';collected.write_bytes(b'\x89PNG\r\n\x1a\nSAFE-FIXTURE')
    private=tmp_path/'private.png';private.write_bytes(b'PRIVATE-FIXTURE')
    url='https://images.example.test/fixture.png'
    report.write_text(f"<!doctype html><html><body><h1>Fixture report</h1><p>Verified fixture content.</p><img src='{url}'><img src='{private}'></body></html>")
    def forbidden(*a,**kw):raise AssertionError('Publication attempted extra network I/O')
    monkeypatch.setattr('nullion.cron_delivery.urlopen',forbidden)
    deliveries,send=telegram_boundary(monkeypatch)
    job=CronJob('fixture','Fixture','0 9 * * *','Fixture',delivery_channel='telegram',delivery_target='fixture',html_image_delivery_mode='self_contained')
    payload={'final_text':'Fixture report ready.','tool_results':[
        {'tool_name':'file_download','status':'completed','output':{'url':url,'path':str(collected)}},
        {'tool_name':'file_write','status':'completed','output':{'path':str(report),'bytes_written':report.stat().st_size}}]}
    result=run_cron_delivery_workflow(job,label='Scheduled task',callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda j:'telegram',delivery_target=lambda j,c:'fixture',run_agent_turn=lambda j,c:payload,
        record_event=lambda *a,**kw:None,block_reason=lambda *a:None,save_web_delivery=lambda *a:False,
        send_platform_delivery=send,health_store_path=home/'runtime.db'))
    assert result['cron_delivery_status']=='sent'
    uploaded=[row['bytes'] for row in deliveries if 'bytes' in row]
    import base64,re
    assert len(uploaded)==1
    images=[base64.b64decode(value) for value in re.findall(rb'data:image/[^;]+;base64,([^\"\']+)',uploaded[0])]
    assert any(b'SAFE-FIXTURE' in value for value in images)
    assert all(b'PRIVATE-FIXTURE' not in value for value in images)
    with sqlite3.connect(home/'runtime.db') as conn:
        assert conn.execute('SELECT finished_at FROM cron_tracker_runs').fetchone()[0]


def test_restart_reconciles_tracker_at_status_api_and_ui(environment):
    from nullion.crons import add_cron
    from nullion.tracker_status import start_check
    from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
    home,runtime=environment
    job=add_cron(name='Fixture monitor',task='Fixture',schedule='0 9 * * *')
    start_check(home/'runtime.db',job,'abandoned',now=datetime.now(UTC)-timedelta(days=7))
    with _UvicornThread(_app(runtime),_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator('#settings-btn').click()
            page.locator('.settings-nav button',has_text='Crons').click()
            expect(page.locator('.cron-health').first).to_contain_text('Check interrupted',timeout=10000)
            with sqlite3.connect(home/'runtime.db') as conn:
                assert conn.execute("SELECT status FROM cron_tracker_runs WHERE run_id='abandoned'").fetchone()==('interrupted',)
        finally:
            browser.close();playwright.stop()


def test_web_startup_delivers_persisted_outbox_without_new_agent_work(environment):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.delivery_outbox import DeliveryOutbox
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    home,runtime=environment
    store=DeliveryOutbox(home/'runtime.db')
    store.put('fixture-group',{'conversation_id':'web:recovery','summary':'Persisted result recovered.',
        'artifacts':[],'delivered_artifacts':[],'text_delivered':False,'work_complete':True,
        'status':'failed','attempts':1,'retry_at':0})
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=None),ToolRegistry())) as client:
        deadline=time.monotonic()+5
        while time.monotonic()<deadline and store.get('fixture-group')['status']!='delivered':time.sleep(.02)
        assert store.get('fixture-group')['status']=='delivered'
        history=client.get('/api/chat/history/web:recovery')
        assert 'Persisted result recovered.' in history.text,history.text
    assert not [event for event in runtime.store.list_conversation_events('web:recovery') if event.get('event_type')=='conversation.chat_turn']


def test_actual_http_redirects_strip_secrets_and_enforce_each_endpoint():
    from contextlib import ExitStack
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    import threading
    from urllib.request import Request
    from urllib.error import HTTPError
    from nullion.tools import _build_web_fetch_opener
    seen=[];location={}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            seen.append((self.server.server_port,self.path,self.headers.get('Authorization'),self.headers.get('Cookie')))
            if self.path=='/redirect':
                self.send_response(302);self.send_header('Location',location['target']);self.end_headers()
            else:
                self.send_response(200);self.end_headers();self.wfile.write(b'fixture')
    with ExitStack() as stack:
        servers=[]
        for _ in range(3):
            server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
            stack.callback(server.server_close);stack.callback(server.shutdown)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            servers.append(server)
        origins=[f'http://127.0.0.1:{server.server_port}' for server in servers]
        opener=_build_web_fetch_opener()
        def request(target):
            location['target']=target
            req=Request(origins[0]+'/redirect',headers={'Authorization':'Bearer fixture-secret','Cookie':'fixture=secret'})
            req._nullion_allowed_base_urls=tuple(origins[:2])
            return opener.open(req,timeout=2)
        with request(origins[0]+'/same') as response:assert response.read()==b'fixture'
        assert seen[-1][2:]==('Bearer fixture-secret','fixture=secret')
        with request(origins[1]+'/other') as response:assert response.read()==b'fixture'
        assert seen[-1][2:]==(None,None)
        with pytest.raises(HTTPError):request(origins[2]+'/blocked')
        assert not any(row[0]==servers[2].server_port for row in seen)
        req=Request(origins[0]+'/private')
        with pytest.raises(Exception):opener.open(req,timeout=2)
        assert not any(row[1]=='/private' for row in seen)


@pytest.mark.parametrize('prompt,deliver',[
    ('Find fixture.pdf; show its name without attaching it.',False),
    ('Busca fixture.pdf; muestra el nombre sin adjuntarlo.',False),
    ('fixture.pdf を探して名前だけ表示し、添付しないでください。',False),
    ('Find and attach fixture.pdf.',True),
    ('Busca y adjunta fixture.pdf.',True),
    ('fixture.pdf を探して添付してください。',True)])
def test_multilingual_source_delivery_follows_typed_scope(environment,tmp_path,monkeypatch,prompt,deliver):
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import create_plugin_tool_registry,register_workspace_plugin
    from nullion.web_app import create_app
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    from pypdf import PdfWriter
    from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject
    home,runtime=environment
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    artifact=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'fixture.pdf'
    pdf=PdfWriter();pdf_page=pdf.add_blank_page(width=200,height=200)
    font=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
    pdf_page[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):DictionaryObject({NameObject('/F1'):pdf._add_object(font)})})
    content=DecodedStreamObject();content.set_data(b'BT /F1 12 Tf 20 100 Td (Fixture document) Tj ET')
    pdf_page[NameObject('/Contents')]=pdf._add_object(content);pdf.write(artifact)
    registry=register_workspace_plugin(create_plugin_tool_registry(workspace_root=home),workspace_root=home)
    class Provider(ScriptedProvider):
        scope=False
        searched=False
        read=False
        def create(self,**kw):
            auxiliary=self.auxiliary(kw)
            if auxiliary:return auxiliary
            if not self.scope:
                self.scope=True
                return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_search','file_read'] if deliver else ['file_search'],
                    'artifact_extensions':['.pdf'] if deliver else [],'excluded_artifact_extensions':[] if deliver else ['.pdf'],
                    'source_artifact_delivery_requested':deliver,'source_artifact_request_evidence':prompt})
            if not self.searched:
                self.searched=True
                return tool('file_search',{'pattern':'fixture.pdf','root':str(artifact.parent)})
            if deliver and not self.read:
                self.read=True
                return tool('file_read',{'path':str(artifact)})
            return response('fixture.pdf'+ ('\nMEDIA:'+str(artifact) if deliver else ''))
    provider=Provider()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=provider),registry)) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:typed-delivery'})
        assert result.status_code==200,result.text
        payload=result.json();assert 'fixture.pdf' in (json.dumps(payload) if deliver else payload['text']),payload
        evidence=runtime.store.list_conversation_events('web:typed-delivery')
        assert bool(payload.get('artifacts')) is deliver,(payload,evidence)
        if deliver:
            descriptor=payload['artifacts'][0]
            assert client.get(descriptor.get('url') or descriptor['download_url']).content==artifact.read_bytes()
    events=runtime.store.list_conversation_events('web:typed-delivery')
    turn=next(row for row in reversed(events) if row.get('event_type')=='conversation.chat_turn')
    assert any(row.get('tool_name')=='file_search' and row['status']=='completed' for row in turn['tool_results']),turn
    assert any(row.get('tool_name')=='request_tool_scope' for row in turn['tool_results'])


def test_telegram_outbox_failure_restart_and_slow_transport_leave_api_responsive(environment,monkeypatch,record_property):
    import threading
    import httpx
    import telegram
    from nullion.telegram_app import _send_operator_telegram_message,_run_confirmed_telegram_delivery
    from nullion.result_aggregator import ResultAggregator
    from nullion.task_queue import TaskRegistry
    home,runtime=environment
    started=threading.Event();release=threading.Event();sent=[]
    state={'fail':True}
    class Bot:
        def __init__(self,*a,**kw):pass
        async def __aenter__(self):return self
        async def __aexit__(self,*a):pass
        async def send_message(self,**kw):
            if state['fail']:raise RuntimeError('Fixture transport unavailable')
            started.set()
            assert await asyncio.to_thread(release.wait,3),'Fixture never released external transport'
            sent.append(kw['text'])
            return SimpleNamespace(message_id=len(sent))
    monkeypatch.setattr(telegram,'Bot',Bot)
    monkeypatch.setenv('NULLION_MESSAGING_DELIVERY_MAX_ATTEMPTS','1')
    def deliver(conversation_id,text,**kw):
        return _run_confirmed_telegram_delivery(lambda:_send_operator_telegram_message(
            '123456:fixture-token','2222',text,principal_id='operator'))
    database=home/'runtime.db'
    first=ResultAggregator(deliver_fn=deliver,task_registry=TaskRegistry(),outbox_path=database)
    first._outbox.enqueue('telegram-fixture',{'conversation_id':'telegram:2222','summary':'Persisted fixture result.',
        'artifacts':[],'delivered_artifacts':[],'text_delivered':False,'work_complete':True,
        'status':'pending','attempts':0,'retry_at':0})
    async def scenario():
        await first._deliver_outbox('telegram-fixture')
        assert first._outbox.get('telegram-fixture')['status']=='failed'
        state['fail']=False
        restarted=ResultAggregator(deliver_fn=deliver,task_registry=TaskRegistry(),outbox_path=database)
        before=time.perf_counter()
        recovery=asyncio.create_task(restarted._deliver_outbox('telegram-fixture'))
        try:
            assert await asyncio.to_thread(started.wait,2)
            latencies=[]
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=_app(runtime)),base_url='http://testserver') as client:
                for _ in range(6):
                    stamp=time.perf_counter();response=await client.get('/api/health')
                    latencies.append(time.perf_counter()-stamp)
                    assert response.status_code==200
            assert time.perf_counter()-before<1.5
            assert sum(latencies)/len(latencies)<.2 and max(latencies)<.5
            record_property('health_latency_seconds',json.dumps(latencies))
        finally:
            release.set();await recovery
        assert restarted._outbox.get('telegram-fixture')['status']=='delivered'
        assert sent==['Persisted fixture result.']
        await restarted._deliver_outbox('telegram-fixture')
        assert len(sent)==1
    asyncio.run(scenario())


@pytest.mark.parametrize('followup',[
    'Find the .html file you just created and tell me only its filename. Do not attach it.',
    'Busca el archivo .html que acabas de crear y dime solo su nombre. No lo adjuntes.',
    '先ほど作成した .html ファイルの名前だけを教えてください。添付しないでください。'])
@pytest.mark.parametrize('scope_timeout',[False,True])
def test_attachment_followup_replaces_prior_delivery_contract_and_plain_chat_stays_fast(environment,monkeypatch,followup,scope_timeout,record_property):
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import create_plugin_tool_registry,register_workspace_plugin
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    from nullion.web_app import create_app
    home,runtime=environment
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    registry=register_workspace_plugin(create_plugin_tool_registry(workspace_root=home),workspace_root=home)
    artifact=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'fixture-continuation.html'
    class Provider(ScriptedProvider):
        mode='create';written=False;searched=False;scope_calls=[]
        def create(self,**kw):
            names={item['name'] for item in kw.get('tools',[])};system=kw.get('system','')
            if not names and 'web_action' in system and 'scheduler_action' in system:
                self.scope_calls.append(self.mode)
                if self.mode=='plain':time.sleep(2)
                if scope_timeout and self.mode=='followup':raise TimeoutError('Fixture scope classifier unavailable')
                return response(json.dumps({'requested_tool_names':['file_search'],'required_tool_names':[],
                    'requested_artifact_extensions':[],'excluded_artifact_extensions':['.html'],'confidence':1}))
            auxiliary=self.auxiliary(kw)
            if auxiliary:
                if 'relationship' in system and 'target_index' in system and self.mode=='followup':
                    return response(json.dumps({'relationship':'follow_up','effect':'continue','confidence':1,'target_index':0}))
                return auxiliary
            if self.mode=='plain':return response('Hello.')
            if self.mode=='followup':
                current_blocks=[block.get('text') for message in kw['messages'] if message.get('role')=='user'
                    for block in (message.get('content') if isinstance(message.get('content'),list) else []) if isinstance(block,dict)]
                assert followup in current_blocks,'The task frame replaced the current user request'
            wanted='file_write' if self.mode=='create' else 'file_search'
            if wanted not in names:
                return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':[wanted],
                    'artifact_extensions':['.html'] if self.mode=='create' else [],
                    'excluded_artifact_extensions':['.html'] if self.mode=='followup' else []})
            if self.mode=='create':
                if not self.written:
                    self.written=True
                    return tool('file_write',{'path':str(artifact),'content':'<!doctype html><html><body><h1>Fixture continuation</h1><p>Fixture content.</p></body></html>'})
                return response('Created the file.\nMEDIA:'+str(artifact))
            if not self.searched:
                self.searched=True
                return tool('file_search',{'pattern':artifact.name,'root':str(artifact.parent)})
            return response(artifact.name)
    provider=Provider()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=provider),registry)) as client:
        created=client.post('/api/chat',json={'text':'Create fixture-continuation.html with one heading and one paragraph. Attach it here.',
            'conversation_id':'web:artifact-followup'})
        assert created.status_code==200 and created.json().get('artifacts'),created.text
        # Reproduce a persisted fetch frame carrying an old delivery contract.
        from nullion.task_frames import (TaskFrame,TaskFrameStatus,TaskFrameOperation,TaskFrameTarget,
            TaskFrameExecutionContract,TaskFrameOutputContract,TaskFrameFinishCriteria)
        previous=next(event for event in reversed(runtime.store.list_conversation_events('web:artifact-followup'))
            if event.get('event_type')=='conversation.chat_turn')
        stamp=datetime.now(UTC)
        runtime.store.add_task_frame(TaskFrame('fixture-prior-frame','web:artifact-followup',previous['branch_id'],
            previous['turn_id'],None,TaskFrameStatus.ACTIVE,TaskFrameOperation.FETCH_RESOURCE,
            TaskFrameTarget('url','https://example.test/fixture'),TaskFrameExecutionContract(preferred_tool_family='terminal_exec'),
            TaskFrameOutputContract(artifact_kind='html'),TaskFrameFinishCriteria(requires_artifact_delivery=True,required_artifact_kind='html'),
            'Fixture prior resource request',stamp,stamp))
        runtime.store.set_active_task_frame_id('web:artifact-followup','fixture-prior-frame')
        runtime.checkpoint(force=True)
        original_bytes=artifact.read_bytes();provider.mode='followup'
        result=client.post('/api/chat',json={'text':followup,'conversation_id':'web:artifact-followup'})
        assert result.status_code==200,result.text
        assert result.json()['artifacts']==[] and artifact.name in result.json()['text'],result.text
        assert artifact.read_bytes()==original_bytes
        turns=[event for event in runtime.store.list_conversation_events('web:artifact-followup') if event.get('event_type')=='conversation.chat_turn']
        assert turns[-1]['parent_turn_id']==turns[-2]['turn_id']
        assert not any(row['tool_name']=='file_write' for row in turns[-1]['tool_results'])
        assert any(event.get('event_type')=='conversation.artifact_contract'
            and event.get('resolution')=='pending_model_scope'
            for event in runtime.store.list_conversation_events('web:artifact-followup'))
        repeated=client.post('/api/chat',json={'text':followup,'conversation_id':'web:artifact-followup'})
        diagnostics=[]
        for event in runtime.store.list_conversation_events('web:artifact-followup'):
            if event.get('event_type')=='conversation.artifact_contract':diagnostics.append(event)
            elif event.get('event_type')=='conversation.chat_turn':
                diagnostics.append({key:event.get(key) for key in ('event_type','turn_id','parent_turn_id','user_message','assistant_reply')})
                for row in event.get('tool_results',[]):
                    diagnostics.append({'tool':row['tool_name'],'status':row['status'],
                        'scope':{key:row.get('output',{}).get(key) for key in ('artifact_extensions','excluded_artifact_extensions','required_artifact_extensions')}})
        if repeated.status_code!=200 or repeated.json()['artifacts']:
            print('REPEATED_DIAGNOSTICS '+json.dumps(diagnostics,ensure_ascii=False))
        assert repeated.status_code==200 and repeated.json()['artifacts']==[],repeated.text
        assert artifact.read_bytes()==original_bytes
        scope_calls_before_plain=list(provider.scope_calls)
        provider.mode='plain';elapsed=[]
        for i,prompt in enumerate(['Hello','Hola','こんにちは','Thanks','Gracias','ありがとう']):
            before=time.perf_counter();result=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:plain-audit-{i}'})
            elapsed.append(time.perf_counter()-before)
            assert result.status_code==200 and 'Hello.' in result.json()['text'],result.text
        assert provider.scope_calls==scope_calls_before_plain,provider.scope_calls
        assert sum(elapsed)/len(elapsed)<.6 and max(elapsed)<1.5,elapsed
        record_property('plain_chat_latency_seconds',json.dumps(elapsed))


@pytest.mark.parametrize('flow_flag',['requires_new_artifact_content','requires_artifact_delivery'])
@pytest.mark.parametrize('prompt',['Condense the previous report into a mobile flyer.','前の資料をスマートフォン向けの画像にまとめてください。','Resume el informe anterior como una imagen para el móvil.'])
def test_interactive_artifact_helper_recovers_scratch_and_delivers_real_image(environment,tmp_path,monkeypatch,prompt,flow_flag):
    from uuid import uuid4
    from nullion.tools import create_core_tool_registry,TerminalBackendDescriptor,TerminalExecutionResult
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    from test_scheduled_report_completion import telegram_boundary
    home,runtime=environment
    monkeypatch.setenv('NULLION_TERMINAL_ENABLED','1');monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1')
    roots=workspace_storage_roots_for_principal('telegram:90200')
    artifact=roots.artifacts/'fixture-mobile-flyer.png'
    script=roots.scratch/'fixture-mobile-flyer.py'
    unmanaged=Path('/tmp')/('nullion-unused-helper-'+uuid4().hex+'.py')
    class Backend:
        calls=[]
        def describe(self):return TerminalBackendDescriptor(mode='subprocess',attested_capabilities=(),metadata={})
        def run(self,command,*,cwd,timeout,policy):
            self.calls.append(command)
            result=subprocess.run(command,shell=True,cwd=cwd,timeout=timeout,capture_output=True,text=True)
            return TerminalExecutionResult(result.returncode,result.stdout,result.stderr)
    backend=Backend()
    registry=create_core_tool_registry(workspace_root=home,terminal_executor_backend=backend)
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)))
    class Model(ScriptedProvider):
        calls=0
        def create(self,**kw):
            system=kw.get('system','')
            if 'material-requirements planner' in system:
                return response(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Create the requested mobile flyer image','requirement_kind':'deliverable',
                     'evidence_subject_id':'flyer','eligible_evidence_tool_names':['terminal_exec']}]}))
            if 'semantic completion reviewer' in system:
                payload=json.loads(kw['messages'][0]['content'][0]['text'])
                ids=[row['invocation_id'] for row in payload['tool_evidence']
                     if row.get('tool_name')=='terminal_exec' and row.get('status')=='completed']
                assert artifact.exists() and ids,payload
                return response(json.dumps({'disposition':'complete','unresolved_requirements':[],
                    'retry_tool_names':[],'all_material_requirements_enumerated':True,'material_requirements':[
                        {**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids[-1:]}
                        for row in payload['material_requirement_contract']]}))
            auxiliary=self.auxiliary(kw)
            if auxiliary:return auxiliary
            names={t['name'] for t in kw.get('tools',[])}
            if 'terminal_exec' not in names:
                return tool('request_tool_scope',{'capabilities':['local_shell'],'tool_names':['terminal_exec'],
                    'required_tool_names':['terminal_exec'],'artifact_extensions':['.png']})
            self.calls+=1
            if self.calls==1:return tool('terminal_exec',{'command':f"printf fixture > {unmanaged}", 'network_mode':'none'})
            if self.calls==2:
                evidence=json.dumps(kw['messages'],ensure_ascii=False)
                assert 'workspace_scratch_path_required' in evidence and str(roots.scratch) in evidence,evidence
                content=f"from PIL import Image, ImageDraw\nimage=Image.new('RGB',(400,600),'white')\nImageDraw.Draw(image).text((20,20),'Fixture mobile flyer',fill='black')\nimage.save({str(artifact)!r})\nprint('MEDIA:'+{str(artifact)!r})\n"
                command=f"cat > {script} <<'PY'\n{content}PY\npython {script}"
                return tool('terminal_exec',{'command':command,'network_mode':'none'})
            return response('Created the mobile flyer.\nMEDIA:'+str(artifact))
    model=Model()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='telegram:90200',principal_id='telegram:90200',
        user_message=prompt,conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
        max_iterations=10,tool_flow_context={flow_flag:True,
            'required_artifact_extensions':['.png'],'user_request':prompt})
    assert not result.suspended_for_approval and not runtime.store.list_approval_requests()
    attempts=[r for r in result.tool_results if r.tool_name=='terminal_exec']
    assert attempts[0].output['reason']=='workspace_scratch_path_required' and attempts[0].output['requires_approval'] is False
    assert attempts[-1].status=='completed' and len(backend.calls)==1
    assert script.exists() and not unmanaged.exists()
    expected=artifact.read_bytes();assert expected.startswith(b'\x89PNG\r\n\x1a\n')
    assert str(artifact) in result.artifacts,(result.final_text,result.artifacts)
    assert result.response_fulfilled is not False,result.final_text
    from nullion.chat_operator import _append_chat_artifacts_to_reply
    visible=_append_chat_artifacts_to_reply(runtime,reply=result.final_text,artifact_paths=result.artifacts,
        prompt=prompt,principal_id='telegram:90200',tool_results=result.tool_results,required_attachment_extensions=('.png',))
    deliveries,send=telegram_boundary(monkeypatch)
    assert send(None,'telegram','90200',visible)
    uploads=[delivery for delivery in deliveries if 'bytes' in delivery]
    assert len(uploads)==1 and uploads[0]['bytes']==expected,(visible,deliveries)
    assert not any('Allow file access?' in delivery.get('text','') for delivery in deliveries)


@pytest.mark.parametrize('prompt',[
    'Transform fixture-invitation.html into a PNG named fixture-mobile.png. Deliver only the PNG.',
    'Transforme fixture-invitation.html en une image PNG nommée fixture-mobile.png. Livrez uniquement le PNG.',
    'fixture-invitation.html を fixture-mobile.png に変換し、PNGだけ添付してください。',
])
@pytest.mark.parametrize('surface',['api','browser'])
def test_history_source_lookup_cannot_override_real_conversion_or_publish_input(environment,monkeypatch,prompt,surface):
    from nullion import chat_store
    monkeypatch.setattr(chat_store,'_store',None)
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import create_core_tool_registry,TerminalBackendDescriptor,TerminalExecutionResult
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    home,runtime=environment
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1');monkeypatch.setenv('NULLION_TERMINAL_ENABLED','1')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    roots=workspace_storage_roots_for_workspace('workspace_admin')
    source=roots.artifacts/'fixture-invitation.html';artifact=roots.artifacts/'fixture-mobile.png'
    source.write_text('<!doctype html><html><head><title>Cedar</title></head><body><h1>Cedar Picnic</h1><p>11:30</p></body></html>')
    os.utime(source,(1_790_812_800,1_790_812_800))  # Match the older saved-history source.
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'web:conversion',
        'workspace_id':'workspace_admin','created_at':'2026-10-01T10:00:00Z','turn_id':'prior-invitation',
        'user_message':'Create Cedar invitation','assistant_reply':'Attached fixture-invitation.html.',
        'artifact_paths':[str(source)]})
    class Backend:
        def describe(self):return TerminalBackendDescriptor(mode='subprocess',attested_capabilities=(),metadata={})
        def run(self,command,*,cwd,timeout,policy):
            result=subprocess.run(command,shell=True,cwd=cwd,timeout=timeout,capture_output=True,text=True)
            return TerminalExecutionResult(result.returncode,result.stdout,result.stderr)
    registry=create_core_tool_registry(workspace_root=home,terminal_executor_backend=Backend())
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)))
    class Model(ScriptedProvider):
        steps=0
        def create(self,**kw):
            system=kw.get('system','')
            if 'material-requirements planner' in system:
                return response(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Produce and attach the PNG conversion','requirement_kind':'deliverable',
                     'evidence_subject_id':'png','eligible_evidence_tool_names':['terminal_exec']}]}))
            if 'semantic completion reviewer' in system:
                payload=json.loads(kw['messages'][0]['content'][0]['text'])
                ids=[row['invocation_id'] for row in payload['tool_evidence']
                     if row.get('tool_name')=='terminal_exec' and row.get('status')=='completed']
                assert artifact.read_bytes().startswith(b'\x89PNG\r\n\x1a\n') and ids,payload
                return response(json.dumps({'disposition':'complete','unresolved_requirements':[],
                    'retry_tool_names':[],'all_material_requirements_enumerated':True,'material_requirements':[
                        {**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids[-1:]}
                        for row in payload['material_requirement_contract']]}))
            if 'requires_new_or_modified_content' in system:
                return response(json.dumps({'action':'use_tools','requires_new_or_modified_content':True,'confidence':1}))
            auxiliary=self.auxiliary(kw)
            if auxiliary:return auxiliary
            self.steps+=1
            assert self.steps<=14,'Conversion was overridden by history lookup completion'
            if self.steps==1:return tool('request_tool_scope',{'capabilities':['local_files','local_shell','conversation_history'],
                'tool_names':['chat_history_search','file_read','terminal_exec'],'artifact_extensions':['.png'],
                'excluded_artifact_extensions':['.html']})
            if self.steps==2:return tool('chat_history_search',{'query':source.name,'artifact_extensions':['.html']})
            if self.steps==3:return tool('file_read',{'path':str(source)})
            if self.steps==4:
                code=f"from PIL import Image,ImageDraw\nim=Image.new('RGB',(400,600),'white')\nImageDraw.Draw(im).text((20,20),'Cedar Picnic 11:30',fill='black')\nim.save({str(artifact)!r})\nprint({str(artifact)!r})\n"
                return tool('terminal_exec',{'command':"python - <<'PY'\n"+code+'PY','network_mode':'none'})
            return response('MEDIA:'+str(artifact))
    model=Model()
    from nullion.web_app import create_app
    app=create_app(runtime,AgentOrchestrator(model_client=model),registry)
    if surface=='browser':
        from test_web_surface_contracts import _UvicornThread,_free_port
        from playwright.sync_api import sync_playwright
        with _UvicornThread(app,_free_port()) as server:
            playwright=sync_playwright().start();browser=playwright.chromium.launch(headless=True)
            page=browser.new_page()
            page.add_init_script("localStorage.setItem('nullion_conv_id','web:conversion'); localStorage.removeItem('nullion_chat_restore_suppressed');")
            page.goto(server.base_url,wait_until='domcontentloaded')
            page.locator('#user-input').wait_for(state='visible')
            try:
                page.locator('#user-input').fill(prompt)
                page.get_by_role('button',name='Send',exact=True).click()
                image=page.locator('#messages img[alt="fixture-mobile.png"]')
                expect(image).to_have_count(1,timeout=30000)
                expect(page.locator('#messages')).not_to_contain_text('could not confirm')
                expect(page.locator('#messages .artifact-list')).to_have_count(1)
                page.wait_for_timeout(4500)  # Cross both terminal-recovery timer intervals.
                expect(image).to_have_count(1)
                assert not page.locator('#messages a').filter(has_text=source.name).count()
                page.reload()
                expect(image).to_have_count(1)
                expect(page.locator('#messages .artifact-list')).to_have_count(1)
            finally:
                browser.close();playwright.stop()
        return
    with TestClient(app) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:conversion'})
        assert result.status_code==200,result.text
        payload=result.json()
        diagnostic=[row for row in runtime.store.list_conversation_events('web:conversion') if row.get('event_type')=='conversation.chat_turn'][-1]
        assert [item['name'] for item in payload['artifacts']]==[artifact.name],(payload,diagnostic)
        assert 'could not confirm' not in payload['text'] and 'possible files' not in payload['text'],payload
        assert client.get(payload['artifacts'][0]['url']).content==artifact.read_bytes()
        turns=[row for row in runtime.store.list_conversation_events('web:conversion') if row.get('event_type')=='conversation.chat_turn']
        turn=turns[-1]
        assert not any(row['output'].get('lookup_selection_unresolved') for row in turn['tool_results'])
        assert {'chat_history_search','file_read','terminal_exec'}.issubset({row['tool_name'] for row in turn['tool_results']})
        recovered=client.get('/api/chat/history/web:conversion',params={'turn_ids':json.dumps([turn['turn_id']])}).json()
        assert [item['name'] for item in recovered['terminal_replies'][0]['artifacts']]==[artifact.name],recovered


@pytest.mark.parametrize('prompt',[
    'Read fixture-source.pdf, create fixture-result.html with the heading Cedar, and attach only fixture-result.html.',
    'Lee fixture-source.pdf, crea fixture-result.html con el título Cedar y adjunta solo fixture-result.html.',
    'fixture-source.pdf を読み、見出し Cedar の fixture-result.html を作成し、その HTML ファイルだけ添付してください。'])
def test_standalone_existing_source_is_not_a_second_output_requirement(environment,monkeypatch,prompt):
    from pypdf import PdfWriter
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import create_plugin_tool_registry,register_workspace_plugin
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    from nullion.web_app import create_app
    from test_memory_and_document_recall_contract import ScriptedProvider,response,tool
    home,runtime=environment;monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    roots=workspace_storage_roots_for_workspace('workspace_admin')
    source=roots.artifacts/'fixture-source.pdf';out=roots.artifacts/'fixture-result.html'
    writer=PdfWriter();writer.add_blank_page(width=200,height=200);writer.write(source)
    registry=register_workspace_plugin(create_plugin_tool_registry(workspace_root=home),workspace_root=home)
    class Model(ScriptedProvider):
        read=False;written=False;calls=0
        def create(self,**kw):
            system=kw.get('system','');names={t['name'] for t in kw.get('tools',[])}
            if 'material-requirements planner' in system:
                return response(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Create the requested HTML with the Cedar heading','requirement_kind':'deliverable',
                     'evidence_subject_id':'html','eligible_evidence_tool_names':['file_write']}]}))
            if 'semantic completion reviewer' in system:
                payload=json.loads(kw['messages'][0]['content'][0]['text'])
                ids=[row['invocation_id'] for row in payload['tool_evidence']
                     if row.get('tool_name')=='file_write' and row.get('status')=='completed']
                assert out.exists() and '<h1>Cedar</h1>' in out.read_text() and ids,payload
                return response(json.dumps({'disposition':'complete','unresolved_requirements':[],
                    'retry_tool_names':[],'all_material_requirements_enumerated':True,'material_requirements':[
                        {**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids[-1:]}
                        for row in payload['material_requirement_contract']]}))
            if 'requires_new_or_modified_content' in system:
                return response(json.dumps({'action':'use_tools','requires_new_or_modified_content':True,'confidence':1}))
            auxiliary=self.auxiliary(kw)
            if auxiliary:return auxiliary
            self.calls+=1;assert self.calls<8,'Source suffix was revived as an output requirement'
            wanted='file_read' if not self.read else 'file_write'
            if not self.written and wanted not in names:
                return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':[wanted],
                    'artifact_extensions':['.html']})
            if not self.read:
                self.read=True;return tool('file_read',{'path':str(source)})
            if not self.written:
                self.written=True;return tool('file_write',{'path':str(out),'content':'<!doctype html><html><head><meta charset="utf-8"><title>Cedar fixture</title></head><body><h1>Cedar</h1><p>This synthetic document verifies that the existing source file is not attached as a second output.</p></body></html>'})
            return response('Created the HTML.\nMEDIA:'+str(out))
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=Model()),registry)) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:standalone-transform'})
        assert result.status_code==200,result.text
        assert [artifact['name'] for artifact in result.json()['artifacts']]==[out.name],result.text
        artifact_response=client.get(result.json()['artifacts'][0]['url'])
        assert b'<h1>Cedar</h1>' in artifact_response.content
    turns=[event for event in runtime.store.list_conversation_events('web:standalone-transform')
           if event.get('event_type')=='conversation.chat_turn']
    assert turns[-1]['parent_turn_id'] is None
    assert {'file_read','file_write'}.issubset({row['tool_name'] for row in turns[-1]['tool_results']})
    assert source.exists() and not any(row['tool_name']=='pdf_create' for row in turns[-1]['tool_results'])
