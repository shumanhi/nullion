"""Real browser, scheduler graph, runtime and delivery; scripted external model."""
import json
import os
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='container E2E only')


@pytest.mark.parametrize('task',['指定日の劇場と形式を確認し、予約可能な席があれば通知。','Comprueba fecha, sala y formato; avisa solo si hay plazas verificadas.'])
@pytest.mark.parametrize('outcome',['match','no_match','unverified'])
def test_monitor_verifies_selected_scope_before_alerting(tmp_path,monkeypatch,task,outcome,record_property):
    from nullion import crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import (run_single_agent_cron_turn,run_cron_delivery_workflow,CronRunDeliveryCallbacks,
        cron_delivery_block_reason)
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.events import make_event
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright');monkeypatch.setenv('NULLION_BROWSER_CDP_URL','')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false');monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','false');monkeypatch.setenv('NULLION_CRON_AGENT_MAX_ITERATIONS','20')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    date_label = '日付' if task.startswith('指定') else 'Fecha'
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers()
            disabled=' disabled' if outcome=='unverified' else ''
            result=('No qualifying screenings for the selected date.' if outcome=='no_match' else
                    '<button id="showtime" onclick="document.querySelector(\'#seats\').hidden=false">IMAX 20:00</button>')
            html=f'''<html><body><h1>Venue Z</h1><label>{date_label}<select aria-label="{date_label}" onchange="document.querySelector('#selected').textContent='Selected date: '+this.value;document.querySelector('#results').hidden=false">
            <option value="2026-10-13" selected>2026-10-13</option><option value="2026-10-16"{disabled}>2026-10-16</option></select></label>
            <p id="selected">Selected date: 2026-10-13</p><section id="results" hidden>{result}</section>
            <section id="seats" hidden>Venue Z — IMAX — 2026-10-16 — 20:00 — Available seat A1 — Working purchase page.</section></body></html>'''
            self.wfile.write(html.encode())
        def log_message(self,*_):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Page);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    url=f'http://127.0.0.1:{server.server_port}/showtimes?date=2026-10-16'
    db=tmp_path/'runtime.db';monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',db);runtime=bootstrap_persistent_runtime(db)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='local',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='*',created_by='owner',created_at=datetime.now(UTC)))
    registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy());observations=[];reviews=[]
    class Provider:
        main=0;calls=0
        def create(self,**kw):
            self.calls+=1;system=str(kw.get('system') or '')
            if system.startswith('You are a material-requirements planner'):
                payload=json.loads(kw['messages'][0]['content'][0]['text']);assert payload['original_request']==task
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Verify date, venue, format and booking condition','requirement_kind':'fact',
                     'evidence_subject_id':'screening','eligible_evidence_tool_names':['browser_extract_text']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                payload=json.loads(kw['messages'][0]['content'][0]['text']);reviews.append(payload)
                evidence=json.dumps(payload['tool_evidence'],ensure_ascii=False)
                good=('Selected date: 2026-10-16' in evidence and
                      ('Available seat A1' in evidence if outcome=='match' else 'No qualifying screenings' in evidence))
                if outcome=='unverified':good=False
                ids=[r['invocation_id'] for r in payload['tool_evidence'] if r.get('tool_name')=='browser_extract_text']
                row=payload['material_requirement_contract'][0]
                return text(json.dumps({'disposition':'complete' if good else 'retry',
                    'unresolved_requirements':[] if good else ['Verify applied date and conditional booking evidence'],
                    'retry_tool_names':[] if good else ['browser_select_combobox','browser_click','browser_extract_text'],
                    'all_material_requirements_enumerated':True,'material_requirements':[
                     {'requirement_id':row['requirement_id'],'requirement':row['requirement'],'requirement_kind':row['requirement_kind'],
                      'satisfied':good,'draft_covered':good,'evidence_invocation_ids':ids[-1:] if good else []}]}))
            if system.startswith('Classify delivery'):
                return text(json.dumps({'decision':'send' if outcome=='match' else 'silent',
                                       'reason':'verified_match' if outcome=='match' else 'no_verified_match'}))
            if system.startswith('You repair scheduled-task final answers'):
                return text(json.dumps({'requirements_satisfied':False,'source_state':'available','evidence_refs':['e0'],
                    'delivery_decision':'send','delivery_reason':'report','report':'Requested date could not be verified.'}))
            if not kw.get('tools'):return text('{}')
            self.main+=1
            actions=[('browser_navigate',{'url':url}),('browser_extract_text',{}),None,
                     ('browser_select_combobox',{'label':date_label,'query':'2026-10-16','expected_text':'2026-10-16'})]
            if outcome=='match':actions += [('browser_click',{'selector':'#showtime'}),('browser_extract_text',{})]
            else:actions += [('browser_extract_text',{})]
            action=actions[self.main-1] if self.main<=len(actions) else None
            if action:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.main),'name':action[0],'input':action[1]}]}
            if self.main==3 or outcome=='unverified':return text('The requested screening has not yet been verified.')
            return text('Verified screening: Venue Z, IMAX, October 16 at 20:00, seat A1 available.' if outcome=='match'
                        else 'The selected date was checked and there are no qualifying screenings.')
    def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
    provider=Provider();job=crons.add_cron('Monitor','0 0 1 1 *',task,enabled=False,delivery_channel='telegram',delivery_target='83',
        artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'})
    def agent(job,cid):return run_single_agent_cron_turn(job,cid,label='Run',orchestrator=AgentOrchestrator(model_client=provider),
        runtime=runtime,tool_registry=registry,settings=SimpleNamespace(enabled_skill_packs=()),model_client=provider,
        tool_result_callback=lambda r:observations.append(r))
    def record(kind,job,channel,target,cid,**extra):runtime.store.add_event(make_event(kind,'scheduler',{'cron_id':job.id,**extra}))
    sent=[]
    try:
        result=run_cron_delivery_workflow(job,label='Run',callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:j.delivery_channel,
            delivery_target=lambda j,c:j.delivery_target,run_agent_turn=agent,record_event=record,block_reason=cron_delivery_block_reason,
            save_web_delivery=lambda *a:False,send_platform_delivery=lambda *a:sent.append(a[-1]) or True,
            health_store_path=db))
        from nullion.tracker_status import tracker_statuses
        health=tracker_statuses(db,[job])[job.id]
        assert health['successful_checks_24h']==(0 if outcome=='unverified' else 1)
        assert health['no_match_checks_24h']==(1 if outcome=='no_match' else 0)
        assert bool(health['last_success_at']) is (outcome!='unverified')
        (tmp_path/'browser-monitor-evidence.json').write_text(json.dumps({'reviews':reviews,'tools':[(r.tool_name,r.status,r.output) for r in observations if r.status!='running']},ensure_ascii=False,default=str))
        record_property('model_calls',provider.calls)
        record_property('completion_reviews',len(reviews))
        assert reviews and reviews[0]['draft_final_response']=='The requested screening has not yet been verified.'
        assert any(r.tool_name=='browser_select_combobox' for r in observations)
        assert len(sent)==(1 if outcome=='match' else 0), {'result':result,'reviews':reviews,'tools':[(r.tool_name,r.status,r.output) for r in observations if r.status!='running']}
        if outcome=='unverified':
            assert result['cron_report_failed'] and not result['execution_outcome']['execution_succeeded']
            assert result['cron_alert_delivery_reason']=='insufficient_evidence'
        else:
            assert result['response_fulfilled'] is True,result
            assert result['execution_outcome']['execution_succeeded'] is True
        if outcome=='match':assert 'seat A1' in sent[0]
        runtime.checkpoint(force=True)
        loaded=load_runtime_store(db)
        deliveries=[e for e in loaded.events if e.event_type=='cron.delivery.'+('sent' if sent else 'silent')]
        assert deliveries
        if outcome=='unverified':
            assert deliveries[-1].payload['execution_succeeded'] is False
            assert deliveries[-1].payload['reason']
        assert provider.calls<30
    finally:server.shutdown();server.server_close();thread.join(timeout=2)


def test_plain_web_chat_keeps_one_model_call_with_optional_browser(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','false')
    class Provider:
        calls=0
        def create(self,**kw):
            if kw.get('system','').startswith(('You are a material-requirements planner','You are a semantic completion reviewer')):
                time.sleep(2);raise AssertionError('Scheduled browser verification entered plain chat')
            self.calls+=1
            return {'content':[{'type':'text','text':'24'}]}
    p=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=ToolRegistry();register_browser_tools(registry)
    app=create_app(runtime=runtime,registry=registry,orchestrator=AgentOrchestrator(model_client=p))
    samples=[]
    with TestClient(app) as client:
        token=client.get('/api/session').json()['csrf_token']
        for index,prompt in enumerate(['4 × 6 だけ。','Solo 4 × 6.','Seulement 4 × 6.']):
            start=time.monotonic();r=client.post('/api/chat',headers={'x-nullion-csrf':token},json={'text':prompt,'conversation_id':f'web:plain-{index}'})
            assert r.status_code==200 and r.json()['text']=='24';samples.append(time.monotonic()-start)
    assert p.calls==3
    assert sum(samples)/len(samples)<1.5 and max(samples)<2.5
    record_property('plain_average_seconds',sum(samples)/len(samples))
