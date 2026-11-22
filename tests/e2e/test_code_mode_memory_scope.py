"""Real API/SQLite memory recall uses the same owner inside native programs."""
import json
import os

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker workflow')


@pytest.mark.parametrize('prompt',['Recall my saved mail confirmation preference.','メール送信の確認設定を思い出してください。'])
def test_code_mode_memory_recall_preserves_owner_and_native_receipt(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.memory import remember_text_fact,memory_owner_for_workspace,UserMemoryKind
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','1');monkeypatch.setenv('NULLION_CODE_MODE','on')
    own=remember_text_fact(runtime.store,owner=memory_owner_for_workspace('workspace_admin'),
        key='mail_consent',value='Confirm before sending emails.',source='owned-memory-fixture',kind=UserMemoryKind.PREFERENCE)
    foreign=remember_text_fact(runtime.store,owner=memory_owner_for_workspace('workspace_foreign'),
        key='mail_consent',value='FOREIGN-WORKSPACE-PRIVATE-73',source='foreign-memory-fixture',kind=UserMemoryKind.PREFERENCE)
    runtime.checkpoint(force=True)
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            names={row['name'] for row in kwargs.get('tools',[])}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:
                assert {'memory_search','chat_history_search','code_mode'}<=names
                return call('code_mode',{'tool_names':['memory_search'],
                    'code':"call_tool('memory_search', {'query':'mail_consent','limit':5})['output']['matches']"})
            assert self.phase==2
            observed=json.dumps(kwargs['messages'],ensure_ascii=False)
            assert own.entry_id in observed and 'Confirm before sending emails.' in observed
            assert foreign.entry_id not in observed and 'FOREIGN-WORKSPACE-PRIVATE-73' not in observed
            return text('Confirm before sending emails.')
    model=Model();cid='web:code-memory-owner'
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),ToolRegistry())) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
        assert response.status_code==200 and response.json()['text']=='Confirm before sending emails.',response.text
        assert not response.json()['artifacts']
    turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
    result=next(row for row in turn['tool_results'] if row['tool_name']=='memory_search')
    program=next(row for row in turn['tool_results'] if row['tool_name']=='code_mode')
    assert result['status']==program['status']=='completed'
    assert [row['memory_id'] for row in result['output']['matches']]==[own.entry_id]
    assert program['output']['reads'][0]['invocation_id']==result['invocation_id']
    assert turn['response_fulfilled'] is True and model.phase==2
    assert not any(row['tool_name']=='memory_save' for row in turn['tool_results'])
