"""CommonMark ordered lists retain distinct choices across a real Web turn."""
from tests.workflow_provider_protocol import workflow_provider_reply
import os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E')

@pytest.mark.parametrize('labels',[('Gmail','Another account'),('Correo','Otra cuenta'),('メール','別のアカウント')])
def test_repeated_markdown_ordinals_are_visible_and_resolvable(tmp_path,monkeypatch,labels):
    from fastapi.testclient import TestClient
    from nullion import chat_store,web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    class Provider:
        calls=0
        def create(self,**kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            if kwargs.get('tools') and any(t['name']=='select_attachment_format' for t in kwargs['tools']):
                return {'content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':None,'confidence':1}}]}
            self.calls+=1
            content='1. '+labels[0]+'\n1. '+labels[1] if self.calls==1 else 'Selected: '+labels[1]
            if self.calls>1:
                payload=str(kwargs.get('messages'))
                assert 'Selected option 2' in payload and labels[1] in payload,payload
                assert 'numbered-option ambiguity' not in payload,payload
            return {'stop_reason':'end_turn','content':[{'type':'text','text':content}]}
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');model=Provider()
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=ToolRegistry())) as client:
        first=client.post('/api/chat',json={'text':'?','conversation_id':'web:choices'}).json()
        assert first['text']=='1. '+labels[0]+'\n2. '+labels[1],first
        second=client.post('/api/chat',json={'text':'2','conversation_id':'web:choices'}).json()
        assert second['text']=='Selected: '+labels[1],second
    assert model.calls==2


def test_separate_groups_and_code_are_not_reinterpreted():
    from nullion.response_sanitizer import sanitize_user_visible_reply
    raw='A\n1. Uno\n2. Dos\n\nB\n1. Tres\n2. Cuatro\n\n```text\n1. literal\n1. literal\n```'
    assert sanitize_user_visible_reply(user_message='?',reply=raw)==raw
