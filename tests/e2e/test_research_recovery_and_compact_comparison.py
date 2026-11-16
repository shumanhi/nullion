"""Real ingress/persistence/delivery; only the source and inference boundaries are fixtures."""
import asyncio
from datetime import UTC,datetime
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
from threading import Thread
import time
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('prompt',['Compara las opciones equivalentes y su cantidad.','同等の選択肢の量を比較してください。'])
def test_failed_source_recovers_with_compact_exact_evidence_and_plain_turns_stay_fast(tmp_path,monkeypatch,surface,prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from nullion.preferences import save_preferences
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,GLOBAL_PERMISSION_PRINCIPAL,PolicyMode
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    from tests.test_source_context_projection import source
    runtime=isolate(tmp_path,monkeypatch);save_preferences({'outbound_request_mode':'allow_all','emoji_level':'none','markdown_style':'plain'})
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('source-read',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    observed=source(13)
    for i,row in enumerate(observed['items']):
        row.update(quantity=20 if i==0 else 4,unit='g',price=6 if i==0 else 3,compatibility_group='A' if i<2 else 'B')
    observed['source_record_collections'][0]['context_fields']+=['unit','price','compatibility_group']
    requests=[]
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(503 if self.path=='/blocked' else 200);self.end_headers()
            self.wfile.write(json.dumps(observed).encode())
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);Thread(target=server.serve_forever,daemon=True).start()
    root=f'http://127.0.0.1:{server.server_port}';registry=ToolRegistry()
    def read(inv):
        path='/blocked' if inv.arguments['pages'][0]['url'].endswith('/blocked') else '/alternative'
        try:
            with urlopen(root+path,timeout=2) as response:payload=json.load(response)
        except HTTPError as exc:
            return ToolResult(inv.invocation_id,inv.tool_name,'failed',{'reason':'http_error',
                'status_code':exc.code,'source_candidates':[{'url':'https://example.test/alternative'}]},'Source unavailable')
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',payload)
    registry.register(ToolSpec('browser_read_pages','Read selected sources',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,3,capability_tags=('public_source_discovery',),
        input_schema={'type':'object','properties':{'pages':{'type':'array'}},'required':['pages']}),read)
    stage=0;primary=[];plain=False
    answer='20 g / 4 g = 5. Unit costs: $0.30/g and $0.75/g. Group B is a different functional alternative, not a like-for-like value comparison.'
    class Model:
        def create(self,**kwargs):
            try:return self.respond(**kwargs)
            except Exception:
                import traceback
                print(traceback.format_exc())
                raise
        def respond(self,**kwargs):
            nonlocal stage
            if plain and '"evidence_policy"' in str(kwargs.get('system','')):
                raise AssertionError('Unrelated chat loaded the optional source classifier')
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            primary.append(kwargs)
            if plain:return text('Bonjour !')
            stage+=1;context=json.dumps(kwargs['messages'],ensure_ascii=False)
            assert len(context)<65000,len(context)
            if stage==1:return call('browser_read_pages',{'pages':[{'url':'https://example.test/blocked'}]})
            if stage==2:
                assert 'try a viable alternative entry point or provider' in context
                return call('browser_read_pages',{'pages':[{'url':'https://example.test/alternative'}]})
            if stage==3:
                assert 'collection_views' in context and 'record_count' in context
                assert 'inspect_retained_evidence' in {t['name'] for t in kwargs.get('tools',[])}
                # The runtime-owned retained index exposes the exact invocation id.
                from nullion.retained_work_context import INSPECT_TOOL
                ids=[]
                for message in kwargs['messages']:
                    for block in message.get('content',[]) if isinstance(message.get('content'),list) else []:
                        value=block.get('text','')
                        if block.get('type')=='tool_result':
                            value=block.get('content',[{}])[0].get('text','')
                        try:payload=json.loads(value)
                        except (ValueError,TypeError):continue
                        ref=payload.get('output',{}).get('retained_evidence',{})
                        if ref.get('invocation_id'):ids.append(ref['invocation_id'])
                assert ids,context[-2000:]
                return call(INSPECT_TOOL,{'requests':[{'invocation_id':ids[-1],
                    'json_pointer':f'/items/{i}','offset':0,'length':5000} for i in range(3)]})
            assert 'retained_evidence_inspection' in context and observed['items'][0]['url'] in context,context[-3000:]
            assert 'compatibility_group' in context and '20' in context and '4' in context
            return text(answer)
    model=Model();orchestrator=AgentOrchestrator(model_client=model);samples=[]
    try:
        if surface=='web':
            from fastapi.testclient import TestClient
            from nullion.web_app import create_app
            with TestClient(create_app(runtime,orchestrator,registry)) as client:
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:compact-recovery'}).json()
                assert response['text']==answer,response
                plain=True;before=len(primary)
                for i,value in enumerate(['Bonjour','Hola','こんにちは']):
                    started=time.monotonic()
                    reply=client.post('/api/chat',json={'text':value,'conversation_id':f'web:plain-{i}'}).json()
                    samples.append(time.monotonic()-started);assert reply['text']=='Bonjour !',reply
                assert len(primary)-before==3
        else:
            from telegram import Bot,Chat,Message,Update,User
            from nullion.config import load_settings
            from nullion.telegram_app import ChatOperatorService
            from nullion.telegram_transport import build_telegram_bot
            from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN,TelegramWire,install_wire
            wire=TelegramWire();install_wire(monkeypatch,wire);runtime.active_tool_registry=registry
            settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
            service=ChatOperatorService(runtime=runtime,settings=settings,bot_token=TOKEN,operator_chat_id='77',model_client=model,agent_orchestrator=orchestrator)
            async def replay():
                async with build_telegram_bot(Bot,TOKEN) as bot:
                    message=Message(1,datetime.now(UTC),Chat(77,'private'),text=prompt,from_user=User(77,'Fixture',False))
                    message.set_bot(bot);message.chat.set_bot(bot)
                    await service.on_message(Update(1,message),SimpleNamespace(bot=bot))
            asyncio.run(replay());assert wire.texts[-1]==answer,wire.texts
        assert requests==['/blocked','/alternative'],requests
        turns=[r for r in runtime.store.list_conversation_events() if r.get('event_type')=='conversation.chat_turn']
        research=next(r for r in turns if r.get('user_message')==prompt)
        assert any(r['tool_name']=='inspect_retained_evidence' and r['status']=='completed' for r in research['tool_results']), [
            (r['tool_name'],r['status'],r.get('error')) for r in research['tool_results']]
        assert all(r['tool_name']!='calculate' for r in research['tool_results'])
        if samples:assert sum(samples)/len(samples)<1 and max(samples)<1.5,samples
    finally:server.shutdown();server.server_close()
