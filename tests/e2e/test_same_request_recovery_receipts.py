"""Current-request receipts must survive a narrower follow-up run before validation."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json, os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product-boundary E2E')

@pytest.mark.parametrize('prompt',['Read the saved values and check the reference file.','Lee los valores guardados y comprueba el archivo de referencia.','保存された数値と参照ファイルを確認してください。'])
def test_real_chat_followup_keeps_completed_prerequisite_without_repeating_it(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_read_handler,_build_file_search_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    from datetime import UTC, datetime
    from nullion.artifacts import artifact_root_for_principal
    source=artifact_root_for_principal('web:retry-receipts')/'values.txt';source.parent.mkdir(parents=True,exist_ok=True);source.write_text('Price: 51.77; Stock: 22')
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);calls=[]
    reader=_build_file_read_handler(tmp_path,allowed_roots=[tmp_path])
    def read(inv):
        calls.append('read');result=reader(inv);result.output['next_tool']='file_search'
        # The original request's typed finish contract survives narrower scope.
        from nullion.task_frames import (TaskFrame,TaskFrameStatus,TaskFrameOperation,
            TaskFrameExecutionContract,TaskFrameOutputContract,TaskFrameFinishCriteria)
        now=datetime.now(UTC)
        frame=TaskFrame('current-request','web:retry-receipts','current-branch','current-turn',None,
            TaskFrameStatus.ACTIVE,TaskFrameOperation.FETCH_RESOURCE,None,
            TaskFrameExecutionContract(),TaskFrameOutputContract(),
            TaskFrameFinishCriteria(required_tool_completion=('file_read',)),
            'Read saved file before finding reference',now,now)
        runtime.store.add_task_frame(frame);runtime.store.set_active_task_frame_id(frame.conversation_id,frame.frame_id)
        return result
    executor=_build_file_search_handler(tmp_path,allowed_roots=[tmp_path])
    def lookup(inv):calls.append('lookup');return executor(inv)
    registry.register(ToolSpec('file_read','Read saved values',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10,continuation_tools=('file_search',)),read)
    registry.register(ToolSpec('file_search','Check local reference',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),lookup)
    class Provider:
        phase=0;retry_seen=False
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(v):return {'stop_reason':'end_turn','content':[{'type':'text','text':v}]}
            def tool(n,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':n,'input':args}]}
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_read','file_search'],'required_tool_names':['file_read']})
            if self.phase==2:return tool('file_read',{'path':str(source)})
            if self.phase==3:return text('Read price 51.77 and stock 22 from the saved values.')
            if self.phase==4:
                self.retry_seen=True;assert names=={'file_search'},names
                return tool('file_search',{'pattern':'values','root':str(source.parent)})
            assert self.phase==5,'Recovery lost a completed prerequisite and retried again'
            return text('Price 51.77; Stock 22; reference values.txt.')
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:retry-receipts'})
        assert response.status_code==200,response.text;data=response.json()
        assert model.retry_seen and calls==['read','lookup'],(data,calls,model.phase)
        assert all(value in data['text'] for value in ['51.77','22','values.txt']) and model.phase==5,data
        assert "didn't run" not in data['text'] and not data['artifacts'],data
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    turn=next(t for t in restored.store.list_conversation_events('web:retry-receipts') if t.get('event_type')=='conversation.chat_turn')
    assert [t['tool_name'] for t in turn['tool_results']].count('file_read')==1
    assert [t['tool_name'] for t in turn['tool_results']].count('file_search')==1
