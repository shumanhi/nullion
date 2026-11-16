"""Selected composite observations survive real Web/Telegram follow-up delivery."""
import asyncio
from datetime import UTC, datetime
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from urllib.request import urlopen

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface', ['web','telegram'])
def test_source_audit_answer_keeps_selected_composite_evidence_and_is_not_replaced(tmp_path,monkeypatch,surface):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from nullion.preferences import save_preferences
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    save_preferences({'outbound_request_mode':'allow_all','emoji_level':'none','markdown_style':'plain'})
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,GLOBAL_PERMISSION_PRINCIPAL,PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('public-read',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    reads=[]
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            reads.append(self.path)
            self.send_response(200);self.end_headers();self.wfile.write(b'PRIMARY-OBSERVATION-53')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source)
    Thread(target=server.serve_forever,daemon=True).start()
    registry=ToolRegistry()
    registry.register(ToolSpec('browser_extract_text','Read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3,
        capability_tags=('public_web',)),lambda inv:None)
    def composite(inv):
        with urlopen(f'http://127.0.0.1:{server.server_port}/primary',timeout=2) as response:
            body=response.read().decode()
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'text':body,'url':'https://example.test/primary'},
            response_requirements={'_native_completed_reads':{'tool_names':['browser_extract_text'],'read_only':True}})
    registry.register(ToolSpec('browser_run_task','Read a selected source',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,3,capability_tags=('public_web',),delegated_tools=('browser_extract_text',),
        input_schema={'type':'object','properties':{'url':{'type':'string'},'task':{'type':'string'}},
            'required':['url','task']}),composite)
    from nullion.tools import register_search_plugin
    def search(query,limit):
        return [{'title':'Primary source','url':'https://example.test/primary','snippet':'Venue primary source information.'}]
    monkeypatch.setenv('NULLION_PROVIDER_BINDINGS','search_plugin=brave_search_provider')
    monkeypatch.setenv('NULLION_BRAVE_SEARCH_API_KEY','local-fixture-key')
    monkeypatch.setattr('nullion.providers.resolve_plugin_provider_kwargs',lambda **kwargs:{'web_searcher':search})
    register_search_plugin(registry,web_searcher=search)
    retained=False;step=0;contexts=[]
    first_prompt='Consulta la fuente seleccionada: https://example.test/primary'
    answer='No consulté esa otra fuente; la observación anterior procede de la fuente primaria.'
    class Provider:
        def create(self,**kwargs):
            nonlocal step
            system=kwargs.get('system','')
            if 'keys relationship, effect' in system:
                return text(json.dumps({'relationship':'follow_up','effect':'revise','confidence':1,
                    'target_index':0,'target_is_explicit':True}))
            if '"evidence_policy"' in system:
                return text(json.dumps({'evidence_policy':'retained' if retained else 'fresh',
                    'web_action':'none' if retained else 'live_research',
                    'requested_tool_names':[] if retained else ['browser_run_task','web_search'],
                    'required_tool_names':[] if retained else ['browser_run_task','web_search'],'confidence':1}))
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if retained:
                context=json.dumps(kwargs['messages'],ensure_ascii=False);contexts.append(context)
                assert 'PRIMARY-OBSERVATION-53' in context and 'observed_at' in context, {
                    'records':[[ (r['tool_name'],r.get('response_requirements')) for r in t.get('tool_results',())]
                        for t in runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn',limit=3)],
                    'observations':runtime.store.list_recent_conversation_events(cid,event_type='conversation.observation_policy',limit=3)}
                return text(answer)
            step+=1
            if step==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_run_task','web_search'],'required_tool_names':['browser_run_task','web_search']})
            if step==2:
                result=call('web_search',{'query':'neutral venue'})
                result['content'].extend(call('browser_run_task',{'url':'https://example.test/primary','task':'Lee la fuente seleccionada.'})['content'])
                return result
            return text('Primary source recorded: 53. https://example.test/primary')
    provider=Provider();orchestrator=AgentOrchestrator(model_client=provider)
    cid='web:source-audit' if surface=='web' else 'telegram:77'
    try:
        if surface=='web':
            from fastapi.testclient import TestClient
            from nullion.web_app import create_app
            with TestClient(create_app(runtime,orchestrator,registry)) as client:
                first=client.post('/api/chat',json={'text':first_prompt,'conversation_id':cid}).json()
                assert '53' in first['text'],first
                for role,value in [('user',first_prompt),('bot',first['text'])]:
                    assert client.post('/api/chat/history/'+cid+'/message',json={'role':role,'text':value}).status_code==200
                retained=True
                second=client.post('/api/chat',json={'text':'¿Y la otra fuente?','conversation_id':cid}).json()
                assert second['text']==answer,second
        else:
            from telegram import Bot,Chat,Message,Update,User
            from nullion.config import load_settings
            from nullion.telegram_app import ChatOperatorService
            from nullion.telegram_transport import build_telegram_bot
            from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN,TelegramWire,install_wire
            import httpx
            wire=TelegramWire();original=wire.handle
            async def handle(request):
                response=await original(request)
                if request.url.path.endswith('/sendMessage'):
                    data=response.json();data['result']['message_id']=200+len(wire.texts)
                    return httpx.Response(200,json=data)
                return response
            wire.handle=handle;install_wire(monkeypatch,wire)
            settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77',
                'NULLION_TELEGRAM_CHAT_ENABLED':'true'})
            runtime.active_tool_registry=registry
            service=ChatOperatorService(runtime=runtime,settings=settings,bot_token=TOKEN,operator_chat_id='77',
                model_client=provider,agent_orchestrator=orchestrator)
            async def replay():
                nonlocal retained
                async with build_telegram_bot(Bot,TOKEN) as bot:
                    for index,prompt in enumerate([first_prompt,'¿Y la otra fuente?','前の観測の情報源は？'],1):
                        quote=None
                        if index>1:
                            quote=Message(200+len(wire.texts),datetime.now(UTC),Chat(77,'private'),text=wire.texts[-1],
                                from_user=User(12345,'Fixture bot',True))
                            quote.set_bot(bot)
                        message=Message(index,datetime.now(UTC),Chat(77,'private'),text=prompt,from_user=User(77,'Fixture',False),
                            reply_to_message=quote)
                        message.set_bot(bot);message.chat.set_bot(bot)
                        await service.on_message(Update(index,message),SimpleNamespace(bot=bot))
                        if index==1:
                            first_turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn',limit=1)[0]
                            assert all(r['status']=='completed' for r in first_turn['tool_results']), [
                                (r['tool_name'],r['status'],r.get('error'),r.get('output',{}).get('reason'))
                                for r in first_turn['tool_results']]
                        retained=True
            asyncio.run(replay())
            assert wire.texts[-1]==answer,wire.texts
        expected=2 if surface=='web' else 3
        assert len(reads)==1 and len(contexts)==expected-1
        turns=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn',limit=3)
        assert len(turns)==expected and turns[-1]['parent_turn_id']==turns[-2]['turn_id']
        for selected in turns[1:]:
            for row in selected['tool_results']:
                if row['tool_name'] in {'browser_run_task','web_search'}:
                    origin=row['response_requirements']['_retained_observation']
                    assert origin['observed_at']==turns[0]['created_at']
                    assert origin['parent_turn_id']==turns[0]['turn_id']
        assert {row['tool_name'] for row in turns[-1]['tool_results']} >= {'browser_run_task','web_search'}, [
            [(row['tool_name'],row['status'],row.get('error'),row.get('output',{}).get('reason'))
             for row in turn['tool_results']] for turn in turns]
    finally:
        server.shutdown();server.server_close()
