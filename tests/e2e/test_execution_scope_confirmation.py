"""A current scoped status read must not become an unrelated historical audit."""
import json,os
import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker workflow')

@pytest.mark.parametrize('prompt,followup',[
    ('このタスクだけ有効にして。他は変更しないで。','現在の状態を改めて確認して。'),
    ('Activa solo esta tarea sin cambiar las demás.','Vuelve a comprobar su estado actual.')])
def test_readonly_confirmation_uses_current_selected_state_without_global_baseline(tmp_path,monkeypatch,prompt,followup):
    from fastapi.testclient import TestClient
    from nullion import crons
    from nullion.tools import register_cron_tools
    jobs=[]
    def text(v):return {'content':[{'type':'text','text':json.dumps(v) if isinstance(v,dict) else v}]}
    def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n,'name':n,'input':a}]}
    class Model:
        mode='mutate';phase=0;plans=0;reviews=0
        def create(self,**kw):
            system=kw.get('system','');tool='toggle_cron' if self.mode=='mutate' else 'list_crons'
            if 'material-requirements planner' in system:
                assert "An exact-target mutation receipt" in system and "Do not claim that other actors made no changes" in system
                self.plans+=1
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'選択した対象の状態','requirement_kind':'action' if self.mode=='mutate' else 'fact',
                        'evidence_subject_id':jobs[0].id,'eligible_evidence_tool_names':[tool]}]})
            if 'semantic completion reviewer' in system:
                assert "An exact-target mutation receipt" in system
                self.reviews+=1;p=json.loads(kw['messages'][0]['content'][0]['text'])
                proof=next(r['invocation_id'] for r in p['tool_evidence'] if r['tool_name']==tool)
                return text({'disposition':'complete','all_material_requirements_enumerated':True,'unresolved_requirements':[],'retry_tool_names':[],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[proof]} for r in p['material_requirement_contract']]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('選択したタスクは有効です。')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['scheduler_mutate' if self.mode=='mutate' else 'scheduler_read'],
                'scheduler_action':'mutate' if self.mode=='mutate' else 'inspect','tool_names':[tool],'required_tool_names':[tool],
                **({'scheduler_toggle_enabled':True} if self.mode=='mutate' else {})})
            if self.phase==2:return call(tool,{'id':jobs[0].id,'enabled':True} if self.mode=='mutate' else {'query':jobs[0].name})
            return text('選択したタスクは有効です。')
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    register_cron_tools(app.state.live_tracker_engine.registry,skip_existing=True)
    jobs.extend(crons.add_cron(n,'0 0 1 1 *','Ready',enabled=False,workspace_id='workspace_admin') for n in ['Selected cedar','Unrelated birch'])
    with TestClient(app) as client:
        first=client.post('/api/chat',json={'text':prompt+' '+jobs[0].id,'conversation_id':'web:scope-confirm'})
        assert first.status_code==200 and crons.get_cron(jobs[0].id).enabled,first.text
        model.mode='inspect';model.phase=0;before=(model.plans,model.reviews)
        second=client.post('/api/chat',json={'text':followup,'conversation_id':'web:scope-confirm'})
        assert second.status_code==200 and '選択したタスクは有効です。' in second.json()['text'],second.text
        turn=runtime.store.list_recent_conversation_events('web:scope-confirm',event_type='conversation.chat_turn')[-1]
        assert turn['response_fulfilled'] is True and not crons.get_cron(jobs[1].id).enabled,turn
        assert [r['tool_name'] for r in turn['tool_results']]==['request_tool_scope','list_crons'],turn
        assert model.phase==3 and (model.plans,model.reviews)==(before[0]+1,before[1]+1)
