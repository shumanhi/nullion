"""Real Web/store recovery preserves an adapter's source-opening prerequisite."""
import json
import os

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker Web/store boundary')


@pytest.mark.parametrize('prompt',['別の情報源で現在の値を確認してください。','Comprueba el valor actual en la otra fuente.'])
def test_reviewed_source_read_can_open_a_different_source(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass

    runtime=setup_runtime(tmp_path,monkeypatch)
    for key,value in {'NULLION_ENV_FILE':str(tmp_path/'none.env'),'NULLION_MEMORY_ENABLED':'0',
        'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_CHAT_STARTUP_WARMUP':'0'}.items():monkeypatch.setenv(key,value)
    registry=ToolRegistry();calls=[];current={'source':'first'}
    def open_source(invocation):
        current['source']=invocation.arguments['source'];calls.append(('open',current['source']))
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'selected':current['source']})
    def read(invocation):
        calls.append(('read',current['source']))
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',
            {'record':{'id':current['source'],'text':'37' if current['source']=='second' else '利用不可'},
             'source_record_collections':[{'path':['record'],'identity_fields':['id'],'cardinality':'one'}]})
    registry.register(ToolSpec('source_read','Read selected source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,
        observation_setup_tools=('source_open',)),read)
    registry.register(ToolSpec('source_open','Open selected source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,
        input_schema={'type':'object','properties':{'source':{'type':'string'}},'required':['source']}),open_source)

    class Model:
        phase=0;reviews=0
        def create(self,**kwargs):
            system=str(kwargs.get('system',''));names={row['name'] for row in kwargs.get('tools',[])}
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated':True,
                    'material_requirements':[{'requirement':'現在の値','requirement_kind':'fact',
                        'evidence_subject_id':'value','eligible_evidence_tool_names':['source_read']}]}))
            if 'semantic completion reviewer' in system:
                self.reviews+=1
                packet=json.loads(kwargs['messages'][0]['content'][0]['text'])
                complete=current['source']=='second'
                proof=[row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name']=='source_read' and row['status']=='completed']
                return text_response(json.dumps({'disposition':'complete' if complete else 'retry',
                    'all_material_requirements_enumerated':True,
                    'unresolved_requirements':[] if complete else ['現在の値が必要です'],
                    'retry_tool_names':[] if complete else ['source_read'],
                    'material_requirements':[{'requirement_id':row['requirement_id'],'satisfied':complete,
                        'draft_covered':complete,'evidence_invocation_ids':proof[-1:] if complete else []}
                        for row in packet['material_requirement_contract']]}))
            reply=workflow_provider_reply(kwargs,completion=False)
            if reply is not None:return reply
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return text_response('{}')
            self.phase+=1
            if self.phase==1:return tool_response('request_tool_scope',{'capabilities':['web'],'tool_names':['source_read','source_open']})
            if self.phase==2:return tool_response('source_read',{})
            if self.phase==3:
                assert 'source_open' in names,sorted(names)
                return tool_response('source_open',{'source':'second'})
            if self.phase==4:return tool_response('source_read',{})
            assert self.phase==5,'Recovery repeated an unchanged source'
            return text_response('37')

    model=Model();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:reader-setup-recovery'})
        assert response.status_code==200 and '37' in response.json()['text'],response.text
        assert calls==[('read','first'),('open','second'),('read','second')]
        turns=[row for row in runtime.store.list_conversation_events('web:reader-setup-recovery') if row.get('event_type')=='conversation.chat_turn']
        assert turns[-1]['response_fulfilled'] is True
        assert model.reviews == 0
        history=client.get('/api/chat/history/web:reader-setup-recovery?sync_runtime=1').json()['messages']
        assert any(row['role']=='bot' and '37' in row['text'] for row in history)
