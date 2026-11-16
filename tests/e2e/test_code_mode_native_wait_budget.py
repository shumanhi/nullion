"""Real API programs use native tool timeouts without spending Python's budget."""
import os
import time

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('prompt',['確認してください','Comprueba los datos'])
def test_native_wait_does_not_force_a_second_program_or_research_round(tmp_path,monkeypatch,prompt,record_property):
    from fastapi.testclient import TestClient
    from nullion import code_mode
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setattr(code_mode,'MAX_ORCHESTRATION_SECONDS',1)
    calls=[];registry=ToolRegistry()
    def read(invocation):
        time.sleep(1.1);calls.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'value':37})
    registry.register(ToolSpec('read_fixture','Read a remote record',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,5,capability_tags=('web',)),read)
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['read_fixture'],'required_tool_names':['read_fixture']})
            if self.phase==2:return call('code_mode',{'tool_names':['read_fixture'],'code':
                "a=call_tool('read_fixture', {})\nb=call_tool('read_fixture', {})\n"
                "a['output']['value']+b['output']['value']"})
            assert self.phase==3,'A valid native wait must not trigger recovery inference'
            return text('Verified total: 74.')
    model=Model();started=time.monotonic()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:native-wait'})
        assert response.status_code==200 and '74' in response.json()['text'],response.text
    elapsed=time.monotonic()-started
    results=[r for e in runtime.store.list_conversation_events('web:native-wait')
        if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
    programs=[r for r in results if r['tool_name']=='code_mode']
    assert len(programs)==1 and programs[0]['status']=='completed'
    assert len(calls)==2 and model.phase==3
    assert programs[0]['output']['execution_timing']['native_wait_seconds']>=2.2
    assert elapsed<7,elapsed
    record_property('native_wait_workflow_seconds',elapsed)
