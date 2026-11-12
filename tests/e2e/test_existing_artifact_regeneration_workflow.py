"""Existing output context cannot erase a typed new-content decision."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
import io

import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('prompt',['Regenerate output.xlsx with exactly 20 new product rows.',
    'Regenera output.xlsx con exactamente 20 filas nuevas.', 'output.xlsx を新しい20行のデータで作り直してください。'])
def test_named_existing_workbook_can_be_regenerated_through_real_chat_route(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from openpyxl import Workbook,load_workbook
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_spreadsheet_create_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    conversation='web:regenerate';root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    path=root/'output.xlsx';old=Workbook();old.active.append(['stale',999]);old.save(path)
    generated=[];handler=_build_spreadsheet_create_handler(None,allowed_roots=[root])
    def produce(invocation):
        generated.append(invocation.tool_name)
        return handler(invocation)
    registry=ToolRegistry(filesystem_allowed_roots=[root])
    registry.register(ToolSpec(name='spreadsheet_create',description='Create structured workbook',
        risk_level=ToolRiskLevel.LOW,side_effect_class=ToolSideEffectClass.WRITE,requires_approval=False,
        timeout_seconds=20),produce)
    class Provider:
        scoped=False
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={item['name'] for item in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':data}]}
            if system.startswith("Classify the user's requested outcome for exact existing workspace"):
                return text(json.dumps({'action':'use_tools','requires_new_or_modified_content':True}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.xlsx','confidence':1})
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[]}))
            if system.startswith('You are a semantic completion reviewer'):
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,
                    'material_requirements':[],'unresolved_requirements':[]}))
            if not names:return text('{}')
            if not self.scoped:
                self.scoped=True
                return tool('request_tool_scope',{'capabilities':['local_files'], 'tool_names':['spreadsheet_create'],
                    'required_tool_names':['spreadsheet_create'],'artifact_extensions':['.xlsx']})
            if not generated:
                return tool('spreadsheet_create',{'output_path':str(path),'columns':['Product','Value'],
                    'rows':[[f'Product {i}',i] for i in range(20)],'expected_rows':20})
            return text('Created the workbook with 20 new rows.\nMEDIA:'+str(path))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation})
        assert result.status_code==200,result.text
        data=result.json();assert len(data['artifacts'])==1 and generated==['spreadsheet_create'],data
        download=client.get(data['artifacts'][0]['url']);assert download.status_code==200
        rows=list(load_workbook(io.BytesIO(download.content),data_only=True).active.values)
        assert len(rows)==21 and rows[1]==('Product 0',0) and rows[-1]==('Product 19',19)
        turns=[item for item in runtime.store.list_conversation_events(conversation) if item.get('event_type')=='conversation.chat_turn']
        scope=next(item['output'] for item in turns[-1]['tool_results'] if item['tool_name']=='request_tool_scope')
        assert scope['required_tool_names']==['spreadsheet_create'] and not scope['unavailable_required_tool_names']
