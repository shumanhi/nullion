"""One real tool invocation may prove several independently bound report subjects."""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
import copy,json,os
from pathlib import Path
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

@pytest.mark.parametrize('prompt',['Compare the three destinations and attach one HTML report.','Compara los tres destinos y adjunta un informe HTML.','3つの目的地を比較し、HTMLレポートを1つ添付してください。'])
@pytest.mark.parametrize('long_excerpt',[False,True])
def test_single_browser_capture_supports_distinct_subjects_in_one_final_report(tmp_path,monkeypatch,prompt,long_excerpt):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from nullion.plugins.browser_plugin import register_browser_tools,browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    image=tmp_path/'source.png';Image.new('RGB',(64,64),'blue').save(image)
    facts=['Cedar station: 31 minutes.','Maple station: 22 minutes.','Aspen station: 17 minutes.']
    if long_excerpt:
        facts=[(fact+' '+('Captured comparison description. '*30)).strip() for fact in facts]
    report={'result_kind':'source_observations','report_status':'completed','goal_status':'answered','next_action':'report',
        'unknowns':[],'blocker':None,'observations':[{'label':fact,'value':fact,'quote':fact,'semantic_role':'entity',
            'source_url':'https://example.com/'+str(i),'captured_at':100+i,'evidence_path':str(image)} for i,fact in enumerate(facts)],
        'execution_audit':{'version':1,'complete':True,'read_only_actions_only':True,'host_file_actions_enabled':False,
            'downloaded_file_count':0,'deliverable_file_count':0,'captures_are_internal':True},'artifact_paths':[str(image)]}
    worker_calls=[]
    def worker(*args,**kw):worker_calls.append(1);return copy.deepcopy(report)
    monkeypatch.setattr(browser_use_worker,'run_isolated_browser_task',worker)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available',lambda:True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python',lambda:'/external/python')
    conversation='web:three-subject-report';root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True);path=root/'report.html'
    expected='<!doctype html><html><body><h1>Comparison</h1>'+''.join('<p>'+fact+'</p>' for fact in facts)+'</body></html>'
    class Provider:
        phase=0;reviews=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self,**kw):
            names={row['name'] for row in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(v):return {'stop_reason':'end_turn','content':[{'type':'text','text':v}]}
            def tool(n,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':data}]}
            if system.startswith('You are a material-requirements planner'):
                rows=[{'requirement':fact,'requirement_kind':'comparison_side','evidence_subject_id':'subject-'+str(i),'eligible_evidence_tool_names':['browser_run_task']} for i,fact in enumerate(facts)]
                rows.append({'requirement':'Deliver the HTML comparison','requirement_kind':'deliverable','evidence_subject_id':'report','eligible_evidence_tool_names':['file_write']})
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'),
                    'all_material_requirements_enumerated':True,'material_requirements':rows}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);coverage=[]
                source=[row for row in data['tool_evidence'] if isinstance(row.get('output'),dict) and row['output'].get('source_record_binding',{}).get('record_kind')=='source_observation']
                assert len(source)==3, 'Distinct captured subjects disappeared from artifact review: '+str(data['tool_evidence'])
                for i,requirement in enumerate(data['material_requirement_contract']):
                    if i<3:
                        matching=[r for r in source if r['output']['observations'][0]['value']==facts[i]]
                    else:matching=[r for r in data['tool_evidence'] if isinstance(r.get('output'),dict) and r['output'].get('artifact_record_binding')]
                    assert len(matching)==1,matching
                    coverage.append({**requirement,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[matching[0]['invocation_id']]})
                return text(json.dumps({**fixture_source_assessment(data,conclusion='limited'),
                    'disposition':'complete','source_presentation_valid':True if data.get('source_answer_selection') else None,
                    'all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'present_source_answer' in names:
                pytest.fail('Generated files must reach selection/content review without an unrelated forced chat summary')
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['web','local_files'],'tool_names':['browser_run_task','file_write'],
                'required_tool_names':['browser_run_task','file_write'],'artifact_extensions':['.html']})
            if self.phase==2:return tool('browser_run_task',{'task':'Compare the three destination stations.','url':'https://example.com/'})
            if self.phase==3:return tool('file_write',{'path':str(path),'content':expected})
            assert self.phase==4,'The reviewer restarted completed research or generation'
            return text('The comparison report is attached.\nMEDIA:'+str(path))
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    registry.register(ToolSpec('file_write','Write the requested report',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation});assert response.status_code==200,response.text;data=response.json()
        assert not model.errors,model.errors
        assert len(data['artifacts']) == 1 and model.phase == 4 and (model.reviews == 0) and (len(worker_calls) == 1), data
        assert client.get(data['artifacts'][0]['url']).content==expected.encode()
        restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
        turns=[r for r in restored.store.list_conversation_events(conversation) if r.get('event_type')=='conversation.chat_turn']
        assert len([r for r in turns[-1]['tool_results'] if r['tool_name']=='file_write'])==1

        reviews=[r for r in restored.store.list_conversation_events(conversation) if r.get('event_type')=='conversation.completion_review']
        assert not reviews
