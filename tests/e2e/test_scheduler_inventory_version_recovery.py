"""Real API/SQLite stale-snapshot recovery keeps the same read tool available."""
import json,os
import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker API')

@pytest.mark.parametrize('prompt',['このIDの現在の状態を確認して。','Comprueba el estado actual de este ID.'])
def test_changed_inventory_recovers_without_retiring_reader(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import crons
    target=[]
    def text(v):return {'content':[{'type':'text','text':json.dumps(v) if isinstance(v,dict) else v}]}
    def call(args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':'list_crons','input':args}]}
    def output(kw):
        for msg in reversed(kw['messages']):
            for block in reversed(msg.get('content',[]) if isinstance(msg.get('content'),list) else []):
                if block.get('type')=='tool_result':
                    raw=block.get('content')
                    if isinstance(raw,list):raw=''.join(part.get('text','') for part in raw if isinstance(part,dict))
                    p=json.loads(raw) if isinstance(raw,str) else raw
                    if isinstance(p,dict) and isinstance(p.get('output'),dict) and 'inventory_version' in p['output']:return p['output']
        raise AssertionError('No source receipt in provider messages')
    class Model:
        phase=0;reviews=0
        def create(self,**kw):
            system=kw.get('system','')
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'現在の状態','requirement_kind':'fact','evidence_subject_id':target[0].id,'eligible_evidence_tool_names':['list_crons']}]})
            if 'semantic completion reviewer' in system:
                self.reviews+=1;p=json.loads(kw['messages'][0]['content'][0]['text'])
                proof=next(r['invocation_id'] for r in reversed(p['tool_evidence']) if r['tool_name']=='list_crons' and r['status']=='completed')
                return text({'disposition':'complete','all_material_requirements_enumerated':True,'unresolved_requirements':[],'retry_tool_names':[],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[proof]} for r in p['material_requirement_contract']]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('現在は有効です。')
            self.phase+=1
            if self.phase==1:return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope','input':{'capabilities':['scheduler_read'],'scheduler_action':'inspect','tool_names':['list_crons'],'required_tool_names':['list_crons']}}]}
            if self.phase==2:return call({'query':target[0].id,'limit':1})
            if self.phase==3:
                prior=output(kw);assert prior['crons'][0]['enabled'] is False
                crons.toggle_cron(target[0].id,True) # Independent fixture writer changes the snapshot.
                return call({'query':target[0].id,'limit':1,'inventory_version':prior['inventory_version']})
            if self.phase==4:
                stale=output(kw);assert stale['reason']=='inventory_changed'
                assert 'list_crons' in {r['name'] for r in kw['tools']}
                return call(stale['recovery']['arguments'])
            assert self.phase==5 and output(kw)['crons'][0]['enabled'] is True
            return text('現在は有効です。')
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    target.append(crons.add_cron('Selected record','0 0 1 1 *','Ready',enabled=False))
    with TestClient(app) as client:
        response=client.post('/api/chat',json={'text':prompt+' '+target[0].id,'conversation_id':'web:inventory-repair'})
        assert response.status_code==200 and '現在は有効です。' in response.json()['text'],response.text
        turn=runtime.store.list_recent_conversation_events('web:inventory-repair',event_type='conversation.chat_turn')[-1]
        assert turn['response_fulfilled'] is True,turn
        reads=[r for r in turn['tool_results'] if r['tool_name']=='list_crons']
        assert [r['status'] for r in reads]==['completed','failed','completed']
        assert model.phase==5 and model.reviews==1
        assert not any(r['tool_name'] in {'toggle_cron','run_cron','list_reminders'} for r in turn['tool_results'])
