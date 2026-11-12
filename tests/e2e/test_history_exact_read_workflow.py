"""Search-to-full-read-to-visible-answer through the real Web route/store."""
from tests.workflow_provider_protocol import workflow_provider_reply
from datetime import UTC,datetime
import json
import os
import time
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real API/store')

@pytest.mark.parametrize('prompt',['Read the final section and its source from our saved Cedar report.',
    'Lee la última sección y su fuente de nuestro informe Cedar guardado.',
    '保存したCedarレポートの最後の節と出典を読んでください。'])
def test_saved_reply_tail_and_citations_can_be_read_after_search_excerpt(tmp_path,monkeypatch,prompt,record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.conversation_history_tools import with_conversation_history_tool
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    body='Cedar report\n'+('First section. '*160)+'\nFinal section: Cedar uses house music. Source: https://example.org/cedar'
    runtime.store.add_conversation_event({'conversation_id':'web:old','workspace_id':'workspace_admin','turn_id':'cedar-source',
        'event_type':'conversation.chat_turn','created_at':datetime.now(UTC).isoformat(),'user_message':'Cedar report','assistant_reply':body})
    class Provider:
        step=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            def text(v):return {'stop_reason':'end_turn','content':[{'type':'text','text':v}]}
            def tool(args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'lookup-'+str(self.step),'name':'chat_history_search','input':args}]}
            system=str(kw.get('system') or '');names={row['name'] for row in kw.get('tools',[])}
            if 'select_attachment_format' in names:return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':None,'confidence':1}}]}
            if 'relationship' in system:return text(json.dumps({'relationship':'separate','effect':'continue','confidence':1,'target_index':None,'target_is_explicit':False}))
            if not names:return text('{}')
            self.step+=1
            assert 'chat_history_search' in names
            if self.step==1:return tool({'query':'Cedar report','limit':1})
            history=json.dumps(kw['messages'],ensure_ascii=False)
            if self.step==2:
                assert 'assistant_reply_truncated' in history and 'cedar-source' in history
                return tool({'read_turn_id':'cedar-source','start_char':0,'max_chars':6000})
            assert self.step==3 and 'conversation_history_read' in history
            assert 'Final section: Cedar uses house music.' in history and 'https://example.org/cedar' in history
            return text('The saved final section says Cedar uses house music. [Saved source](https://example.org/cedar)')
    model=Provider();runtime.model_client=model;registry=with_conversation_history_tool(ToolRegistry(),runtime=runtime,conversation_id='web:read')
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        started=time.perf_counter()
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:read','turn_id':'read-tail'})
        elapsed=time.perf_counter()-started;record_property('history_read_route_seconds',elapsed)
        assert elapsed<1.5,elapsed
        assert response.status_code==200,response.text
        reply=response.json();assert 'house music' in reply['text'] and 'https://example.org/cedar' in reply['text'],reply
        assert not reply['artifacts'] and model.step==3
    rows=runtime.store.list_recent_conversation_events('web:read',event_type='conversation.chat_turn',limit=1)
    results=[r for r in rows[0]['tool_results'] if r['tool_name']=='chat_history_search']
    assert len(results)==2 and results[-1]['output']['result_kind']=='conversation_history_read'
    assert results[-1]['output']['matches'][0]['assistant_reply']==body
