"""Real SDK/Chromium -> native report -> Web API/store for disabled choices."""
import json,os,threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest
from tests.workflow_provider_protocol import workflow_provider_reply,fixture_source_plan,fixture_source_assessment
from tests.e2e.test_browser_evidence_repair_runtime import page_feedback
from tests.e2e.test_browser_use_runtime_contract import response
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker SDK/API boundary')


@pytest.mark.parametrize('replan',[False,True])
@pytest.mark.parametrize('detail',['2027-01-04 休業','2027-01-04 Cerrado'])
def test_disabled_requested_choice_reaches_web_as_grounded_blocker(tmp_path,monkeypatch,detail,replan):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    python=os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    if not python:pytest.skip('Isolated real SDK required')
    for k,v in {'NULLION_HOME':str(tmp_path/'home'),'NULLION_ENV_FILE':str(tmp_path/'none.env'),
        'NULLION_BROWSER_USE_PYTHON':python,'NULLION_BROWSER_BLOCK_PRIVATE':'false',
        'NULLION_BROWSER_HEADLESS':'true','NULLION_BROWSER_USE_SANDBOX':'false',
        'NULLION_WORKSPACE_STORAGE_ROOT':str(tmp_path/'workspaces'),'NULLION_MEMORY_ENABLED':'0',
        'NULLION_TASK_PLANNER_FEED_MODE':'off','ANONYMIZED_TELEMETRY':'false'}.items():monkeypatch.setenv(k,v)
    monkeypatch.delenv('NULLION_BROWSER_USE_CDP_URL',raising=False);monkeypatch.setattr(chat_store,'_store',None)
    for name in ('_schedule_web_builder_reflection','_schedule_web_no_tool_memory_capture'):
        monkeypatch.setattr(web_app,name,lambda *a,**kw:None)
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers()
            self.wfile.write(('<h1>Calendar</h1><p>Location A</p><button disabled aria-label="'+detail+'" value="2027-01-04">'+detail+'</button>').encode())
        def log_message(self,*_):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Page)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    url=f'http://localhost:{server.server_port}/calendar'
    class Model:
        model='external-provider-fixture';browser_calls=0;outer_calls=0;plans=0;reviews=0
        def create(self,**kw):
            try:return self._create(**kw)
            except Exception:
                import traceback
                traceback.print_exc()
                raise
        def _create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            system=str(kw.get('system',''))
            if system.startswith('You are a material-requirements planner'):
                self.plans+=1
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                plan=fixture_source_plan(packet,scope='evidence_summary')
                assert 'search_coverage' in plan,packet
                plan['search_coverage']['source_blocker_alternative']={'request_quote':'Si no se puede seleccionar, indica el bloqueo mostrado.'}
                return response(json.dumps({**plan,
                    'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Establish the requested choice or its displayed blocker',
                    'requirement_kind':'fact','evidence_subject_id':'choice','eligible_evidence_tool_names':['browser_run_task']},*([{'requirement':'Also establish inventory from the unchosen branch','requirement_kind':'fact','evidence_subject_id':'unchosen','eligible_evidence_tool_names':['browser_run_task']}] if replan and self.plans==1 else [])]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                reports=[r for r in packet['tool_evidence'] if r['output'].get('result_kind')=='source_observations']
                assert reports and reports[0]['output']['goal_status']=='blocked'
                records=reports[0]['output']['observations']
                assert records[0]['quote']==detail and records[0]['control_state']['disabled'] is True
                assert records[0]['evidence_kind']=='source_blocker'
                native=[r for r in packet['tool_evidence'] if r['output'].get('source_record_binding')]
                assert native,packet
                ident=native[-1]['invocation_id']
                assessment=fixture_source_assessment(packet,conclusion='limited')
                if packet.get('search_coverage_context'):
                    assessment['search_coverage'].update(match_evidence_invocation_ids=[ident],outcome_kind='accepted_blocker',coverage_sufficient=False)
                if replan and self.plans==1:
                    assessment['search_coverage']['scope_matches_request']=False
                    return response(json.dumps({**assessment,'disposition':'retry','all_material_requirements_enumerated':True,
                        'unresolved_requirements':['The plan incorrectly requires both alternative branches.'],'retry_tool_names':[],
                        'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':r['evidence_subject_id']!='unchosen',
                            'draft_covered':r['evidence_subject_id']!='unchosen','evidence_invocation_ids':[] if r['evidence_subject_id']=='unchosen' else [ident]}
                            for r in packet['material_requirement_contract']]}))
                return response(json.dumps({**assessment,'disposition':'complete','all_material_requirements_enumerated':True,
                    'unresolved_requirements':[],'retry_tool_names':[],'material_requirements':[
                        {'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[ident]}
                        for r in packet['material_requirement_contract']]}))
            schemas=[m['content'] for m in kw['messages'] if isinstance(m.get('content'),str) and m['content'].startswith('Return valid JSON matching this schema: ')]
            if schemas:
                self.browser_calls+=1
                if self.browser_calls==1:action={'read_page_state':{}}
                else:
                    assert self.browser_calls==2
                    page=page_feedback(kw['messages']);control=next(c for c in page['controls'] if c['disabled'] is True)
                    action={'done':{'success':False,'data':{'goal_status':'blocked','next_action':'report','blocker':detail,
                        'observations':[{'label':'Requested choice','semantic_role':'interface_control','value':detail,'quote':detail,
                            'source_id':page['source_id'],'control_index':control['control_index']}],'unknowns':['Inventory and price']}}}
                schema=json.loads(schemas[-1].split(': ',1)[1]);payload={'evaluation_previous_goal':'Read current page','memory':'Keep captured blocker',
                    'next_goal':'Report exact blocker','action':[action]}
                if 'thinking' in schema.get('properties',{}):payload['thinking']='Read only'
                for key in ('current_plan_item','plan_update'):
                    if key in schema.get('properties',{}):payload[key]=None
                return response(json.dumps(payload,ensure_ascii=False))
            names={r['name'] for r in kw.get('tools',[])}
            if 'present_source_answer' in names:
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                context=json.loads(raw)
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'present','name':'present_source_answer',
                    'input':{'intro':detail,'intro_evidence_ids':[r['id'] for r in context['evidence']],
                        'focus_roles':['interface_control'],'points':[],'answer_status':'no_verified_results',
                        'limitation_ids':[]}}]}
            if 'select_attachment_format' in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':None,'confidence':1}}]}
            self.outer_calls+=1
            if self.outer_calls==1:name,args='request_tool_scope',{'capabilities':['web'],'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task']}
            elif self.outer_calls==2:name,args='browser_run_task',{'task':'2027-01-04: report the selected choice or the displayed blocker. Do not book.','url':url,'max_steps':3}
            else:return response(detail+' — '+url)
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.outer_calls),'name':name,'input':args}]}
    model=Model();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    cid='web:disabled-choice'
    try:
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
            result=client.post('/api/chat',json={'text':'2027-01-04: ¿Disponible? Si no se puede seleccionar, indica el bloqueo mostrado. '+url,'conversation_id':cid})
            assert result.status_code==200 and detail in result.json()['text'],result.json()
            turns=[r for r in runtime.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn']
            report=next(r['output'] for r in turns[-1]['tool_results'] if r['tool_name']=='browser_run_task')
            assert report['observations'],json.dumps({'report':report,'browser_calls':model.browser_calls,'turn':turns[-1]},ensure_ascii=False)
            assert report['report_status']=='blocked' and report['observations'][0]['control_state']['disabled'] is True
            assert report['observations'][0]['evidence_kind']=='source_blocker' and not result.json()['artifacts']
            assert turns[-1].get('response_fulfilled') is True,json.dumps({'turn':turns[-1],'events':runtime.store.list_conversation_events(cid)},ensure_ascii=False)
            assert model.browser_calls==2 and model.plans==(2 if replan else 1)
            history=client.get('/api/chat/history/'+cid+'?sync_runtime=1').json()['messages']
            assert any(r['role']=='bot' and detail in r['text'] for r in history)
    finally:server.shutdown();server.server_close()
