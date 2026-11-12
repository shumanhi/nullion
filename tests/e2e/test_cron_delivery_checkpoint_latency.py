"""Real Web cron delivery commits receipts without a whole-runtime save."""
from tests.workflow_provider_protocol import workflow_provider_reply
import inspect,json,os,statistics,time
from pathlib import Path
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker Chromium/API/SQLite boundary')


def test_cron_report_ui_and_terminal_status_skip_slow_full_checkpoints(tmp_path,monkeypatch,unused_tcp_port):
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import PersistentRuntime,bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from tests.e2e.test_web_surface_contracts import _UvicornThread
    from playwright.sync_api import sync_playwright,expect
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'home/runtime.db');registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(tools.ToolSpec('file_write','Write requested HTML',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,5),tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    class Provider:
        job='';path=None;started=False;written=False;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names={r['name'] for r in kw.get('tools',[])}
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+self.job,'name':n,'input':args}]}
            if not names:return text('{}')
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.html','confidence':1})
            if 'run_cron' in names:
                if not self.started:self.started=True;return call('run_cron',{'id':self.job})
                return text('Started.')
            if 'file_write' in names:
                if not self.written:
                    self.written=True
                    return call('file_write',{'path':str(self.path),'content':'<!doctype html><html><head><meta charset="utf-8"><title>Report</title></head><body><h1>Current data</h1><table><tr><th>Area</th><th>Value</th></tr><tr><td>North</td><td>12</td></tr><tr><td>South</td><td>19</td></tr></table><p>Total: 31. '+self.job+'</p></body></html>'})
                return text('Report ready.\nMEDIA:'+str(self.path))
            return call('request_tool_scope',{'capabilities':['scheduler_run'],'scheduler_action':'run','tool_names':['run_cron'],'required_tool_names':['run_cron']})
    provider=Provider();runtime.model_client=provider;app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=registry)
    full_calls=[];original=PersistentRuntime.checkpoint
    def slow_full(self,**kw):
        frame=inspect.currentframe().f_back;callback=False
        while frame is not None:
            callback|=frame.f_code.co_name in {'_record_cron_delivery_event','_record_cron_delivery_chat_turn'}
            frame=frame.f_back
        if self is runtime and callback:full_calls.append(True);time.sleep(1.5)
        return original(self,**kw)
    monkeypatch.setattr(PersistentRuntime,'checkpoint',slow_full);samples=[];identities=[]
    with _UvicornThread(app,unused_tcp_port) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        try:
            for i,label in enumerate(['Generate the saved report.','Genera el informe guardado.','保存した報告書を生成してください。']):
                cid='web:cron-receipt-'+str(i);root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);provider.path=root/('scheduled_'+str(i)+'.html');provider.started=False;provider.written=False
                context=browser.new_context();context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
                page=context.new_page();page.goto(server.base_url,wait_until='domcontentloaded');page.locator('#user-input').wait_for();page.wait_for_function('()=>ws && ws.readyState===WebSocket.OPEN')
                response=page.request.post(server.base_url+'/api/crons',data={'name':'Report '+str(i),'schedule':'0 0 1 1 *','enabled':False,'delivery_channel':'web','delivery_target':cid,'task':'Create one self-contained HTML report with North=12, South=19 and total=31. Deliver only that HTML.','artifact_delivery_options':{'required_artifact_extensions':['.html'],'requires_attachment_delivery':True,'doctor_monitoring_enabled':False}})
                assert response.ok,response.text();provider.job=response.json()['job']['id'];identities.append((cid,provider.job));started=time.monotonic()
                try:
                    page.locator('#user-input').fill(label+' '+provider.job);page.locator('#user-input').press('Enter')
                    expect(page.locator('#messages a[download]').filter(has_text=provider.path.name)).to_have_count(1,timeout=20000)
                    deadline=time.monotonic()+10
                    while True:
                        job=next(r for r in page.request.get(server.base_url+'/api/crons').json() if r['id']==provider.job)
                        if job['last_result']=='ok':break
                        assert time.monotonic()<deadline,job
                        page.wait_for_timeout(30)
                    samples.append(time.monotonic()-started);assert not job['enabled']
                    history=page.request.get(server.base_url+'/api/chat/history/'+cid).json();files=[a for row in history['messages'] for a in row.get('artifacts',[]) if a['name']==provider.path.name];assert len(files)==1,history
                    data=page.request.get(server.base_url+files[0]['url']).body();assert all(v in data for v in [b'North',b'South',b'12',b'19',b'31',provider.job.encode()])
                finally:page.request.delete(server.base_url+'/api/crons/'+provider.job);context.close()
        finally:browser.close()
    assert not full_calls,'Cron delivery waited on a whole-runtime snapshot'
    assert statistics.mean(samples)<4 and max(samples)<6,samples
    restored=bootstrap_persistent_runtime(tmp_path/'home/runtime.db')
    saved=[e for e in restored.store.events if e.event_type=='cron.delivery.saved'];assert {e.payload['cron_id'] for e in saved}=={job for cid,job in identities}
    audits=[a for a in restored.store.list_audit_records() if a.action=='cron.delivery.saved'];assert {a.details['cron_id'] for a in audits}=={job for cid,job in identities}
    for cid,job in identities:assert any(e.get('source')=='cron' and e.get('cron_id')==job for e in restored.store.list_conversation_events(cid))
