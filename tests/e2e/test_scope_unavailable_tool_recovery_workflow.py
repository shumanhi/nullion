"""An invalid tool name cannot poison later valid scope and file delivery."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('prompt',['Create one CSV with North 12 and South 19.',
    'Crea un CSV con North 12 y South 19.','North 12、South 19を含むCSVを一つ作ってください。'])
@pytest.mark.parametrize('inherited_unknown',[False,True])
def test_invalid_required_tool_scope_is_atomic_and_recoverable(tmp_path,monkeypatch,prompt,inherited_unknown):
    from fastapi.testclient import TestClient
    from nullion import chat_store,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolScopeDecision,TurnToolEvidence
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    conversation='web:scope-correction';root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    path=root/'report.csv'
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_write','Create the requested file',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,10),_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    if inherited_unknown:
        registry=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(requested_extensions=('.csv',)),
            tool_scope_decision=TurnToolScopeDecision(valid=True,requested_artifact_extensions=('.csv',),
                requested_tool_names=('web_fetch',),required_tool_names=('web_fetch',)))
    class Provider:
        phase=0;scope_failure_observed=False
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            system=str(kw.get('system') or '');names={row['name'] for row in kw.get('tools',[])}
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':data}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver the requested CSV values','requirement_kind':'deliverable',
                     'evidence_subject_id':'csv','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                data=json.loads(kw['messages'][0]['content'][0]['text'])
                records=[row for row in data['tool_evidence'] if isinstance(row.get('output'),dict) and row['output'].get('artifact_record_binding')]
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'unresolved_requirements':[],
                    'material_requirements':[{**row,'satisfied':True,'draft_covered':True,
                      'evidence_invocation_ids':[records[0]['invocation_id']]} for row in data['material_requirement_contract']]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.csv','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_write'],'required_tool_names':['file_write','web_fetch'],'artifact_extensions':['.csv']})
            if self.phase==2:
                payload=json.dumps(kw['messages'])
                assert 'invalid_tool_arguments' in payload and 'scope_changed' in payload and 'web_fetch' in payload
                self.scope_failure_observed=True
                return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],
                    'required_tool_names':['file_write'],'artifact_extensions':['.csv']})
            if self.phase==3:return tool('file_write',{'path':str(path),'content':'Region,Value\nNorth,12\nSouth,19\n'})
            return text('CSV attached.\nMEDIA:'+str(path))
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation})
        assert response.status_code==200,response.text
        data=response.json();assert len(data['artifacts'])==1,data
        downloaded=client.get(data['artifacts'][0]['url']);assert downloaded.content==b'Region,Value\nNorth,12\nSouth,19\n'
        turns=[row for row in runtime.store.list_conversation_events(conversation) if row.get('event_type')=='conversation.chat_turn']
        scopes=[row for row in turns[-1]['tool_results'] if row['tool_name']=='request_tool_scope']
        assert model.scope_failure_observed, 'The real scoping feedback must reach the provider for correction'
        assert scopes[-1]['status']=='completed' and scopes[-1]['output']['required_tool_names']==['file_write']
        assert scopes[-1]['output']['unavailable_required_tool_names']==[]
        assert len([row for row in turns[-1]['tool_results'] if row['tool_name']=='file_write'])==1
        assert model.phase==4
