"""Batch mutations retain per-object proof through the real API and SQLite."""
import json
import os
import time
import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

@pytest.mark.parametrize('prompt',['この二件だけ有効にして。','Activa únicamente estas dos tareas.'])
def test_batch_toggle_has_distinct_bound_action_records_without_rereads(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import crons
    from nullion.tools import register_cron_tools
    jobs=[];seen=[]
    def text(value):return {'content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}
    def call(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Model:
        phase=0;reviews=0;plans=0
        def create(self,**kw):
            system=kw.get('system','')
            if 'material-requirements planner' in system:
                self.plans+=1
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'有効にする: '+job.id,'requirement_kind':'action',
                        'evidence_subject_id':job.id,'eligible_evidence_tool_names':['toggle_cron']} for job in jobs]})
            if 'semantic completion reviewer' in system:
                self.reviews+=1;packet=json.loads(kw['messages'][0]['content'][0]['text'])
                records=[row for row in packet['tool_evidence'] if row.get('output',{}).get('result_kind')=='structured_source_record']
                assert len(records)==2,packet['tool_evidence']
                seen.extend(row['output']['record']['id'] for row in records)
                return text({'disposition':'complete','all_material_requirements_enumerated':True,'unresolved_requirements':[],'retry_tool_names':[],
                    'material_requirements':[{'requirement_id':row['requirement_id'],'satisfied':True,'draft_covered':True,
                        'evidence_invocation_ids':[next(r['invocation_id'] for r in records if r['output']['record']['id']==row['evidence_subject_id'])]} for row in packet['material_requirement_contract']]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('指定された二件を有効にしました。')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['scheduler_mutate'],'scheduler_action':'mutate',
                'tool_names':['toggle_cron'],'required_tool_names':['toggle_cron'],'scheduler_toggle_enabled':True})
            if self.phase==2:return call('toggle_cron',{'ids':[j.id for j in jobs],'enabled':True})
            return text('指定された二件を有効にしました。')
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    registry=app.state.live_tracker_engine.registry
    register_cron_tools(registry,skip_existing=True)
    for name in ['target-one','target-two','untouched']:
        jobs.append(crons.add_cron(name,'0 0 1 1 *','Return ready.',enabled=False,workspace_id='workspace_admin'))
    untouched=jobs.pop()
    with TestClient(app) as client:
        started=time.perf_counter()
        response=client.post('/api/chat',json={'text':prompt+' '+', '.join(j.id for j in jobs),'conversation_id':'web:batch-action'})
        assert time.perf_counter()-started<3
        assert response.status_code==200,response.text
        assert all(crons.get_cron(j.id).enabled for j in jobs) and not crons.get_cron(untouched.id).enabled
        turn=runtime.store.list_recent_conversation_events('web:batch-action',event_type='conversation.chat_turn')[-1]
        assert turn['response_fulfilled'] is True,turn
        assert [r['tool_name'] for r in turn['tool_results']]==['request_tool_scope','toggle_cron'],turn
        assert set(seen)=={j.id for j in jobs} and model.plans==model.reviews==1 and model.phase==3
