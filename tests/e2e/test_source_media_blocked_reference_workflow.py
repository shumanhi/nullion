"""Provided source context can answer a bounded request despite a blocked page."""
from tests.workflow_provider_protocol import workflow_provider_reply
import base64
import json
import os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real API/store')

@pytest.mark.parametrize('prompt',['Keep the attached reference and compare it with the linked page.',
    'Conserva la referencia adjunta y compárala con la página enlazada.',
    '添付した資料を保持し、リンク先のページと比較してください。'])
def test_blocked_reference_does_not_discard_selected_input_media(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.plugins.browser_plugin import register_browser_tools,browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    image=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jL1sAAAAASUVORK5CYII=')
    source=tmp_path/'reference.png';source.write_bytes(image);calls=[]
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available',lambda:True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python',lambda:'/external/python')
    def read(*a,**kw):
        calls.append(1);return {'result_kind':'source_observations','report_status':'blocked','goal_status':'blocked',
            'completion_scope':'tool_step','next_action':'report','page_url':'https://reference.example/page',
            'observations':[],'unknowns':[],'blocker':'The reference page returned an access challenge.',
            'execution_audit':{'complete':True,'read_only_actions_only':True,'visited_urls':['https://reference.example/page']}}
    monkeypatch.setattr(browser_use_worker,'run_isolated_browser_task',read)
    def text(v):return {'stop_reason':'end_turn','content':[{'type':'text','text':v}]}
    def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Provider:
        phase=0;reviews=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            system=str(kw.get('system') or '');names={t['name'] for t in kw.get('tools',[])}
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if system.startswith(('You are a material-requirements planner','You are a semantic completion reviewer')) or 'present_source_answer' in names:
                media=[b for m in kw['messages'] for b in m.get('content',[]) if isinstance(b,dict) and b.get('type')=='image']
                assert len(media)==1 and base64.b64decode(media[0]['source']['data'])==image
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Retain provided source and report the reference page access outcome','requirement_kind':'fact',
                     'evidence_subject_id':'reference','eligible_evidence_tool_names':['browser_run_task']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;p=json.loads(kw['messages'][0]['content'][0]['text'])
                ids=[r['invocation_id'] for r in p['tool_evidence'] if r['tool_name']=='browser_run_task' and r['status']=='completed'];assert ids
                return text(json.dumps({'disposition':'blocked','source_presentation_valid':True,
                    'all_material_requirements_enumerated':True,'unresolved_requirements':['The remote comparison is blocked'],
                    'user_facing_failure':'The supplied reference is retained, but the remote comparison page is blocked.',
                    'material_requirements':[{**r,'satisfied':False,'draft_covered':False,'evidence_invocation_ids':ids[:1]} for r in p['material_requirement_contract']]}))
            if 'present_source_answer' in names:
                definition=next(t for t in kw['tools'] if t['name']=='present_source_answer')
                limit_id=definition['input_schema']['properties']['limitation_ids']['items']['enum'][0]
                return tool('present_source_answer',{'intro':'The supplied reference image is retained.',
                    'focus_roles':['context'],'points':[],'limitation_ids':[limit_id]})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task']})
            assert self.phase==2
            return tool('browser_run_task',{'task':prompt,'url':'https://reference.example/page'})
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    captured=[]
    class Capture(AgentOrchestrator):
        def run_turn(self,**kw):
            result=super().run_turn(**kw);captured.append(result);return result
    with TestClient(web_app.create_app(runtime,orchestrator=Capture(model_client=model),registry=registry)) as client:
        result=client.post('/api/chat',json={'text':prompt+' https://reference.example/page','conversation_id':'web:reference',
            'attachments':[{'path':str(source),'name':source.name,'media_type':'image/png'}]})
        assert result.status_code==200,result.text
        reply=result.json()['text'];assert 'reference image is retained' in reply and 'access challenge' in reply,reply
        assert not result.json()['artifacts'] and calls==[1] and model.reviews==1
        assert captured[-1].response_fulfilled is False and captured[-1].response_presentation['source_answer_verified'] is True
