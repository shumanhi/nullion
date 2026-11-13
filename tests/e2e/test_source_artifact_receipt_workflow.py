"""Primary source answers retain the verified requested file at the Web route."""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
import copy
import json
import os
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1', reason='Docker E2E only')


@pytest.mark.parametrize('prompt', ['Send a screenshot of this page.', 'Envía una captura de esta página.', 'このページのスクリーンショットを送ってください。'])
def test_browser_source_composition_knows_verified_deliverable_exists(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from PIL import Image, ImageDraw
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.plugins.browser_plugin import register_browser_tools, browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    chat_store._store=None
    image=Image.new('RGB',(64,64),'white');ImageDraw.Draw(image).rectangle((8,8,48,48),fill='blue')
    source=tmp_path/'capture.png';image.save(source)
    report={'result_kind':'source_observations','report_status':'completed','goal_status':'answered',
        'next_action':'report','blocker':None,'unknowns':[],'validation_errors':[],
        'artifact_paths':[str(source)],'observations':[{'label':'title','semantic_role':'context',
        'value':'Fixture page','quote':'Fixture page','source_url':'https://example.com/',
        'captured_at':100,'main_document_text':'Fixture page','evidence_path':str(source)}]}
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available',lambda:True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python',lambda:'/external/python')
    monkeypatch.setattr(browser_use_worker,'run_isolated_browser_task',lambda *args,**kwargs:copy.deepcopy(report))
    def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
    def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':data}]}
    class Provider:
        phase=0;receipt_seen=False;reviewed=False
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={item['name'] for item in kw.get('tools',[])};system=str(kw.get('system') or '')
            if system.startswith('You are a material-requirements planner'):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'), 'all_material_requirements_enumerated':True,
                    'requested_artifact_extensions':['.png'],'material_requirements':[
                    {'requirement':'Deliver the captured page image','requirement_kind':'deliverable', 'artifact_member_subject_ids':[],
                     'eligible_evidence_tool_names':['browser_run_task'],'evidence_subject_id':'capture'}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviewed=True;data=json.loads(kw['messages'][0]['content'][0]['text'])
                assert len(data['prepared_delivery_receipts'])==1
                assert data['prepared_delivery_receipts'][0]['extension']=='.png'
                assert 'path' not in data['prepared_delivery_receipts'][0]
                ids=[item['invocation_id'] for item in data['tool_evidence'] if item['tool_name']=='browser_run_task' and item.get('output',{}).get('artifact_record_binding')]
                return text(json.dumps({**fixture_source_assessment(data,conclusion='limited'), 'disposition':'complete','source_presentation_valid':True,
                    'all_material_requirements_enumerated':True,'unresolved_requirements':[], 'retry_tool_names':[],
                    'material_requirements':[{**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids[:1]}
                        for row in data['material_requirement_contract']]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.png','confidence':1})
            if 'present_source_answer' in names:
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                data=json.loads(raw);receipts=data['verified_artifact_receipts']
                assert len(receipts)==1 and receipts[0]['extension']=='.png' and receipts[0]['size_bytes']>0
                assert 'path' not in receipts[0]
                self.receipt_seen=True
                return tool('present_source_answer',{'intro':'Attached the screenshot of Fixture page.',
                    'intro_evidence_ids':[data['evidence'][0]['id']],'focus_roles':['context'],
                    'points':[],'limitation_ids':[],'summary_point_indices':[]})
            if not names and system:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_run_task'],
                'required_tool_names':['browser_run_task'],'artifact_extensions':['.png'],
                'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt})
            if self.phase==2: return tool('browser_run_task',{'task':prompt,'url':'https://example.com/'})
            return text('Attached the screenshot of Fixture page.')
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:receipt'})
        assert response.status_code==200,response.text
        data=response.json();assert 'Attached the screenshot' in data['text'],data
        assert len(data['artifacts'])==1 and not model.receipt_seen and not model.reviewed,data
        # Prepared receipt bytes, rather than an optional editor call, prove
        # this capture was delivered. The next assertion checks the real route.
        delivered=client.get(data['artifacts'][0]['url']);assert delivered.status_code==200
        assert delivered.content==source.read_bytes()
