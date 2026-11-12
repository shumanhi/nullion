"""A failed final artifact review cannot restart the entire generation workflow."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
from pathlib import Path

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('extension',['.csv','.html','.svg'])
@pytest.mark.parametrize('prompt',['Read the current source value and create the requested file only after verifying it.',
    'Lee el valor actual de la fuente y crea el archivo solicitado solo tras verificarlo.',
    '情報源の現在の値を確認した後にだけ、指定されたファイルを作成してください。'])
def test_unverified_source_condition_stops_file_actions_through_real_web_route(tmp_path,monkeypatch,extension,prompt):
    from fastapi.testclient import TestClient
    from nullion import chat_store,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler,_build_file_read_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');monkeypatch.setenv('NULLION_AGENT_TURN_MAX_ITERATIONS','40')
    chat_store._store=None;conversation='web:terminal-artifact';root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    path=root/('report'+extension)
    content={'.csv':'Name,Value\nNorth,unavailable\n','.html':'<!doctype html><html><body><h1>Value unavailable</h1></body></html>',
        '.svg':'<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100"><text x="10" y="30">Value unavailable</text></svg>'}[extension]
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    # Only the external source boundary is deterministic. Graph, persistence,
    # real file production/reads, review, retry budgets and HTTP delivery stay real.
    def source(invocation):
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{
            'location':'London','current':{},'reason':'source_value_missing',
            'verified':False})
    for name,effect,handler in [('weather_forecast',ToolSideEffectClass.READ,source),
        ('file_write',ToolSideEffectClass.WRITE,_build_file_write_handler(tmp_path,allowed_roots=[tmp_path])),
        ('file_read',ToolSideEffectClass.READ,_build_file_read_handler(tmp_path,allowed_roots=[tmp_path]))]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,10),handler)
    class Provider:
        phase=0
        def create(self,**kw):
            if str(kw.get('system') or '').startswith('Workflow action preflight.'):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                assert any(row['output'].get('verified') is False for row in packet['execution_evidence'])
                return {'content':[{'type':'text','text':json.dumps({'decision':'revise',
                    'issues':['The required source value is still unverified; do not create the conditional file.']})}]}
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={row['name'] for row in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':data}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Verify the current source value','requirement_kind':'fact','evidence_subject_id':'source',
                     'eligible_evidence_tool_names':['weather_forecast']},
                    {'requirement':'Deliver a file containing that verified value','requirement_kind':'deliverable',
                     'evidence_subject_id':'file','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                data=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({'disposition':'retry','all_material_requirements_enumerated':True,
                    'unresolved_requirements':['The current source value is unavailable.',
                        'Replace the draft with a concise delivery message and exclude duplicate working files.'],
                    'retry_tool_names':['file_read','weather_forecast'],
                    'material_requirements':[{**row,'satisfied':False,'draft_covered':False,'evidence_invocation_ids':[]}
                        for row in data['material_requirement_contract']]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':extension,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['weather','local_files'],
                'tool_names':['weather_forecast','file_write','file_read'],'required_tool_names':['file_write'],
                'artifact_extensions':[extension]})
            if self.phase==2:return tool('weather_forecast',{'location':'London'})
            if self.phase==3:return tool('file_write',{'path':str(path),'content':content})
            if self.phase==4:return text('The report is ready.\nMEDIA:'+str(path))
            if self.phase>=13:return tool('file_write',{'path':str(root/(f'unwanted-restart-{self.phase}'+extension)),'content':content})
            return tool('file_read',{'path':str(path)})
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt+' '+extension,'conversation_id':conversation})
        assert response.status_code==200,response.text
        data=response.json();assert not data.get('artifacts') and not data.get('suspended_for_approval'),data
        assert 'Replace the draft' not in data['text'] and 'duplicate working files' not in data['text'],data
        assert model.phase<=15,'Outer recovery restarted a terminal artifact workflow'
        assert not list(root.glob('unwanted-restart-*'))
        turns=[row for row in runtime.store.list_conversation_events(conversation) if row.get('event_type')=='conversation.chat_turn']
        created=[row for row in turns[-1]['tool_results'] if row['tool_name']=='file_write']
        assert created and all(row['status']=='failed' and row['output'].get('executed') is False for row in created)
        assert not path.exists()
        from nullion.artifact_lifecycle import delivery_manifest
        assert not delivery_manifest(turns[-1]['tool_results'])
