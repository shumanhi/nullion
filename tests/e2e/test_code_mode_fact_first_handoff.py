"""Real Web/Telegram ingress, SQLite and Monty; only external IO is scripted."""
import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface',['web','telegram'])
def test_multiple_large_sources_finish_without_inspection_or_research_restart(tmp_path,monkeypatch,surface,record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    from tests.test_source_finding_compaction import source_bundle
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    registry=ToolRegistry();reads=[];originals=[]
    def external(inv):
        reads.append(inv.arguments['selection'])
        output=source_bundle(inv.arguments['selection'],capture_chars=700000,count=8)
        originals.append(output)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',output)
    registry.register(ToolSpec('source_lookup','Read the selected source',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,5,capability_tags=('web',),input_schema={
            'type':'object','required':['selection'],'properties':{'selection':{'type':'string'}}}),external)
    class Model:
        phase=0;errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            try:return self.respond(kw)
            except Exception as e:self.errors.append(repr(e));raise
        def respond(self,kw):
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['source_lookup'],'required_tool_names':['source_lookup']})
            if self.phase==2:return call('code_mode',{'tool_names':['source_lookup'],'code':
                "a=call_tool('source_lookup', {'selection':'vol'})\n"
                "b=call_tool('source_lookup', {'selection':'宿泊'})\n"
                "{'first':a['output'],'second':b['output']}"})
            if self.phase>3:
                time.sleep(6)
                raise AssertionError('Completed source work was restarted or inspected again')
            packets=[json.loads(b['content'][0]['text']) for m in kw['messages']
                if isinstance(m.get('content'),list) for b in m['content']
                if isinstance(b,dict) and b.get('type')=='tool_result' and b.get('tool_use_id')=='code_mode-id']
            output=packets[-1]['output'];encoded=json.dumps(output,ensure_ascii=False)
            assert output['reason']=='code_mode_completed'
            assert output['value_limit']['projected'] is True and output['value_limit']['omitted'] is False
            assert all(f'{name}-verified-{i}' in encoded for name in ('vol','宿泊') for i in range(8)),encoded
            assert '2031-04-08' in encoded and '18:00' in encoded and '€73' in encoded
            assert len(encoded)<=32000
            return text('vol-verified-0 and 宿泊-verified-7: 2031-04-08, 18:00, €73. https://source.test/')
    model=Model()
    if surface=='telegram':
        from nullion.config import load_settings
        from nullion.telegram_app import ChatOperatorService
        settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
            'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
        service=ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,
            operator_chat_id='123',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
    samples=[];message_number=[15000]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,prompt in enumerate(['選択した資料を比較。','Compara las fuentes seleccionadas.','قارن المصادر المحددة.']):
            cid=f'web:fact-first-{index}' if surface=='web' else 'telegram:123'
            model.phase=0;before=len(reads);started=time.monotonic()
            if surface=='web':
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
                assert response.status_code==200,response.text
                visible=response.json()['text']
            else:
                message_number[0]+=1;sent=[]
                class Message:
                    text=prompt;message_id=message_number[0];reply_to_message=None
                    chat=SimpleNamespace(id=123,send_action=lambda **kw:asyncio.sleep(0))
                    async def reply_text(self,value,**kw):
                        sent.append(value);return SimpleNamespace(message_id=message_number[0]+100)
                asyncio.run(service.on_message(SimpleNamespace(update_id=message_number[0],message=Message()),SimpleNamespace(bot=None)))
                assert sent
                visible=sent[-1]
            samples.append(time.monotonic()-started)
            assert not model.errors,model.errors
            assert '宿泊-verified-7' in visible and '€73' in visible and '2031-04-08' in visible
            assert reads[before:]==['vol','宿泊'] and model.phase==3
            turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
            assert len([r for r in turn['tool_results'] if r['tool_name']=='code_mode'])==1
            assert not any(r['tool_name']=='inspect_retained_evidence' for r in turn['tool_results'])
            assert len([r for r in turn['tool_results'] if r['tool_name']=='source_lookup'])==2
            assert originals[-1]['captured_documents'][0]['main_document_text']=='x'*700000
    assert sum(samples)/len(samples)<5 and max(samples)<8,samples
    record_property('fact_handoff_mean_seconds',sum(samples)/len(samples))


@pytest.mark.asyncio
async def test_langchain_harness_uses_the_shared_finding_projection(tmp_path,monkeypatch):
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.test_source_finding_compaction import source_bundle
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=ToolRegistry();calls=[]
    def source(inv):
        calls.append(inv.arguments['selection'])
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',
            source_bundle(inv.arguments['selection'],capture_chars=700000,count=8))
    reg.register(ToolSpec('source_lookup','Read selected source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
        input_schema={'type':'object','required':['selection'],'properties':{'selection':{'type':'string'}}}),source)
    receipts=[]
    tool=nullion_tools_as_langchain_tools(reg,allowed_tools=['source_lookup'],principal_id='operator',
        cleanup_scope='source-projection',policy_store=runtime.store,tool_result_callback=receipts.append,
        tool_flow_context={'conversation_id':'web:langchain-source-projection','user_request':'選択した資料を比較'})[0]
    try:
        raw=await tool.ainvoke({'tool_names':['source_lookup'],'code':
            "a=call_tool('source_lookup', {'selection':'vol'})\n"
            "b=call_tool('source_lookup', {'selection':'宿泊'})\n{'one':a['output'],'two':b['output']}"})
        summary=next(r for r in receipts if r.tool_name=='code_mode')
        assert raw==_tool_result_message_payload(summary)
        assert len(raw)<=32500
        assert all(name+'-verified-'+str(i) in raw for name in ('vol','宿泊') for i in range(8))
        assert calls==['vol','宿泊']
    finally:tool.coroutine._nullion_code_mode_finish()
