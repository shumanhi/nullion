"""Web artifact delivery persists receipts without a slow full-runtime snapshot."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
import statistics
import time

import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real route/store only')


def test_web_artifact_latency_and_receipts_survive_runtime_restart(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import PersistentRuntime,bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');monkeypatch.delenv('NULLION_CHAT_FORCE_CHECKPOINT_ON_EVERY_TURN',raising=False);chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    from nullion import tools as tool_api
    owner_reader=getattr(tool_api, "tool_execution_owner", None)
    owners=[]
    native_write=_build_file_write_handler(tmp_path,allowed_roots=[tmp_path])
    def write(invocation):
        owners.append(owner_reader() if owner_reader is not None else None);return native_write(invocation)
    registry.register(ToolSpec('file_write','Write one artifact',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,20),
        write)
    class Provider:
        phase=0;path=None
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            system=str(kw.get('system') or '');names={row['name'] for row in kw.get('tools',[])}
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver the requested data file','requirement_kind':'deliverable','evidence_subject_id':'data','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                data=json.loads(kw['messages'][0]['content'][0]['text']);ids=[row['invocation_id'] for row in data['tool_evidence'] if row['tool_name']=='file_write' and row['status']=='completed']
                return text(json.dumps({'disposition':'complete','unresolved_requirements':[],'all_material_requirements_enumerated':True,
                    'material_requirements':[{**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids} for row in data['material_requirement_contract']]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.csv','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],
                'required_tool_names':['file_write'],'artifact_extensions':['.csv']})
            if self.phase==2:return tool('file_write',{'path':str(self.path),'content':'Item,Quantity\nCedar,7\nMaple,11\n'})
            assert self.phase==3
            return text('Prepared the file.\nMEDIA:'+str(self.path))
    model=Provider();runtime.model_client=model;full_checkpoints=[];original=PersistentRuntime.checkpoint
    def slow_full_checkpoint(self,**kw):
        if self is runtime:
            full_checkpoints.append(kw);time.sleep(1.5)
        return original(self,**kw)
    monkeypatch.setattr(PersistentRuntime,'checkpoint',slow_full_checkpoint)
    samples=[];receipts=[]
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        for index,prompt in enumerate(['Create one CSV with Cedar 7 and Maple 11.','Crea un CSV con Cedar 7 y Maple 11.','Cedar 7 と Maple 11 の CSV を一つ作成。']):
            conversation='web:checkpoint-'+str(index);root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
            model.phase=0;model.path=root/('data-'+str(index)+'.csv');started=time.perf_counter()
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation,'turn_id':'artifact-'+str(index)})
            samples.append(time.perf_counter()-started);assert response.status_code==200,response.text
            data=response.json();assert len(data['artifacts'])==1,data
            download=client.get(data['artifacts'][0]['url']);assert download.status_code==200 and b'Cedar,7' in download.content
            receipts.append((conversation,data['artifacts'][0]))
    assert not full_checkpoints,'A full snapshot re-entered the artifact delivery hot path'
    if owner_reader is not None:
        assert [owner.get('conversation_id') for owner in owners]==[r[0] for r in receipts]
        assert all(owner.get('invocation_id') and owner.get('scope_id') for owner in owners)
        assert owner_reader()=={},'Owner ids leaked outside their tool invocation'
    record_property('artifact_mean_seconds',statistics.mean(samples));record_property('artifact_p95_seconds',max(samples))
    assert statistics.mean(samples)<1.0 and max(samples)<1.5,samples
    monkeypatch.setattr(PersistentRuntime,'checkpoint',original);chat_store._store=None
    restarted=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    with TestClient(web_app.create_app(restarted,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        for conversation,artifact in receipts:
            turns=[e for e in restarted.store.list_conversation_events(conversation) if e.get('event_type')=='conversation.chat_turn']
            assert len(turns)==1 and turns[0].get('artifact_paths'),turns
            head=restarted.store.get_conversation_head(conversation)
            assert head and head['active_turn_id']==turns[0]['turn_id']
            assert restarted.store.get_conversation_turn(head['active_turn_id']) is not None
            assert restarted.store.get_conversation_branch(head['active_branch_id']) is not None
            assert any(r['tool_name']=='file_write' and r['status']=='completed' for r in turns[0]['tool_results'])
            history=client.get('/api/chat/history/'+conversation,params={'turn_ids':json.dumps([turns[0]['turn_id']])})
            assert history.status_code==200 and artifact['name'] in history.text,history.text
            download=client.get(artifact['url']);assert download.status_code==200 and b'Maple,11' in download.content

    # Explicit operator policy still forces a full checkpoint; it must not
    # accidentally become impossible when Web joins the bounded chat path.
    monkeypatch.setenv('NULLION_CHAT_FORCE_CHECKPOINT_ON_EVERY_TURN','1')
    monkeypatch.setattr(PersistentRuntime,'checkpoint',slow_full_checkpoint)
    model.phase=0;model.path=artifact_root_for_principal('web:forced-checkpoint')/'forced.csv'
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':'Create one CSV with Cedar 7 and Maple 11.','conversation_id':'web:forced-checkpoint'})
        assert response.status_code==200 and len(response.json()['artifacts'])==1,response.text
    assert full_checkpoints and any(call.get('force') is True for call in full_checkpoints)
