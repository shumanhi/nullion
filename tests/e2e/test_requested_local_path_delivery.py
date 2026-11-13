"""Requested technical paths survive routing, review, persistence and Web delivery."""
import json
import os
import time

import pytest
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('prompt',['Identify the default runtime and give its exact executable path.',
    '既定の実行環境とその実行ファイルの正確なパスを示して。'])
def test_exact_local_path_survives_real_web_turn(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    chat_store._store=None
    path='/home/example/runtime/bin/python'
    registry=ToolRegistry();reads=[];reviews=[]
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'stdout':path,'exit_code':0})
    registry.register(ToolSpec('terminal_exec','Read local runtime information',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2),read)
    class Provider:
        calls=0
        def create(self,**kw):
            if 'semantic completion reviewer' in str(kw.get('system')):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                assert path in packet['draft_final_response']
                reviews.append(packet)
            auxiliary=workflow_provider_reply(kw)
            if auxiliary is not None:return auxiliary
            if kw.get('tools'):
                self.calls+=1
            def tool(name,args):return {'stop_reason':'tool_use','content':[{
                'type':'tool_use','id':str(self.calls),'name':name,'input':args}]}
            if self.calls==1:return tool('request_tool_scope',{'capabilities':['local_shell'],
                'tool_names':['terminal_exec'],'required_tool_names':['terminal_exec'],'include_local_path_details':True})
            if self.calls==2:return tool('terminal_exec',{'command':'read-runtime'})
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'Default runtime: `'+path+'`.'}]}
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');provider=Provider();runtime.model_client=provider
    elapsed=[]
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=registry)) as client:
        for index in range(3):
            provider.calls=0
            conversation='web:requested-path-'+str(index)
            started=time.perf_counter()
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation})
            elapsed.append(time.perf_counter()-started)
            assert response.status_code==200,response.text
            assert path in response.json()['text'],response.json()
            assert not response.json()['artifacts']
            assert provider.calls==3
        turns=[r for r in runtime.store.list_conversation_events(conversation)
            if r.get('event_type')=='conversation.chat_turn']
        assert path in turns[-1]['assistant_reply']
        scope=next(r for r in turns[-1]['tool_results'] if r['tool_name']=='request_tool_scope')
        assert scope['output']['include_local_path_details'] is True
        evidence=turns[-1]['tool_results']
    # Exercise the real Telegram formatter and common Slack/Discord transport
    # preparation too; only their final network send is substituted.
    import asyncio
    from types import SimpleNamespace
    from nullion.telegram_app import _deliver_reply
    from nullion.messaging_adapters import execute_platform_reply_delivery
    sent=[]
    class Message:
        chat=SimpleNamespace(id=123)
        message_id=7
        async def reply_text(self,text,**kwargs):
            sent.append(text)
    async def send(text):
        sent.append(text)
        return True
    async def deliver():
        await _deliver_reply(Message(),response.json()['text'],principal_id='telegram:123',
            tool_results=evidence,quote_reply=False)
        for channel in ('slack','discord'):
            await execute_platform_reply_delivery(response.json()['text'],channel=channel,
                target_id='123',tool_results=evidence,send_text=send)
    asyncio.run(deliver())
    assert len(sent)==3 and all(path in text for text in sent),sent
    assert len(reads)==3 and reviews==[]
    assert sum(elapsed)/len(elapsed)<1.5 and max(elapsed)<3,elapsed
