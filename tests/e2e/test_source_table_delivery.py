"""A real chat response must retain the data table its tool evidence supports."""
from tests.workflow_provider_protocol import workflow_provider_reply
import os
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Native API/SQLite boundary')


@pytest.mark.parametrize('heading',['Market quote','Cotizaciones market_quote','相場 market_quote'])
@pytest.mark.parametrize('layout',['table','sentence'])
def test_native_quote_table_survives_status_cleanup(tmp_path,monkeypatch,heading,layout):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools.ToolRegistry()
    def read(inv):return tools.ToolResult(inv.invocation_id,inv.tool_name,'completed',{'quotes':[{'symbol':'MSFT','price':37,'currency':'USD','regular_market_date':'2026-10-23'}],'source_record_collections':[{'path':['quotes'],'identity_fields':['symbol']}]})
    registry.register(tools.ToolSpec('market_quote','Current market data',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.READ,False,5),read)
    draft='| '+heading+' | Value |\n|---|---|\n| MSFT | $37 USD |\n\n2026-10-23.'
    if layout=='sentence':draft=heading+': MSFT $37 USD.\n\n2026-10-23.'
    class Provider:
        phase=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            self.phase+=1
            if self.phase==1:return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'quote','name':'market_quote','input':{'symbols':['MSFT']}}]}
            assert self.phase==2
            return {'stop_reason':'end_turn','content':[{'type':'text','text':draft}]}
    provider=Provider();runtime.model_client=provider
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=registry)) as client:
        result=client.post('/api/chat',json={'text':'MSFT: '+heading,'conversation_id':'web:quote-table'}).json()
        assert 'MSFT' in result['text'] and '$37' in result['text'] and '2026-10-23' in result['text'],result
        assert not result['artifacts']
        turn=next(e for e in runtime.store.list_conversation_events('web:quote-table') if e.get('event_type')=='conversation.chat_turn')
        assert turn['tool_results'][0]['tool_name']=='market_quote' and 'MSFT' in turn['assistant_reply']
