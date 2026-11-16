"""Measured shared chat boundaries with real SQLite, SDK and browser state."""
import asyncio
from datetime import UTC, datetime
import json
import os
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1', reason='Docker E2E')


def test_telegram_completion_persists_incremental_turns_while_snapshots_coalesce(tmp_path, monkeypatch, record_property):
    from telegram import Bot, Chat, Message, Update, User
    from nullion import telegram_app, runtime_checkpoints
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.runtime_persistence import runtime_store_file_lock, load_runtime_store
    from nullion.telegram_transport import build_telegram_bot
    from nullion.tools import ToolRegistry
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch); runtime.active_tool_registry = ToolRegistry()
    wire = TelegramWire(); original = wire.handle
    async def handle(request):
        if request.url.path.endswith('/sendChatAction'):
            return httpx.Response(200, json={'ok':True, 'result':True})
        return await original(request)
    wire.handle = handle; install_wire(monkeypatch, wire)
    class Model:
        def create(self, **kwargs):
            reply = workflow_provider_reply(kwargs, completion=True)
            return reply if reply is not None else text('確認しました。')
    model = Model(); settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings, bot_token=TOKEN,
        operator_chat_id='77', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    held = threading.Event(); release = threading.Event()
    def writer():
        with runtime_store_file_lock(runtime.checkpoint_path):
            held.set(); assert release.wait(15)
    owner = threading.Thread(target=writer); owner.start(); assert held.wait(2)
    samples = []
    async def run():
        async with build_telegram_bot(Bot,TOKEN) as bot:
            for index, prompt in enumerate(['こんにちは', 'Bonjour', 'Hola'],1):
                message = Message(message_id=index,date=datetime.now(UTC),chat=Chat(77,'private'),
                    from_user=User(77,'Fixture',False),text=prompt)
                message.set_bot(bot); message.chat.set_bot(bot)
                started = time.monotonic()
                await asyncio.wait_for(service.on_message(Update(update_id=index,message=message),SimpleNamespace(bot=bot)),3)
                samples.append(time.monotonic()-started)
                assert not release.is_set() and len(wire.texts)==index
            assert id(runtime) in runtime_checkpoints._CHECKPOINT_INFLIGHT
    try:
        asyncio.run(run())
        assert sum(samples)/len(samples)<1 and max(samples)<1.5, samples
    finally:
        release.set(); owner.join(2)
        deadline=time.monotonic()+8
        while id(runtime) in runtime_checkpoints._CHECKPOINT_INFLIGHT and time.monotonic()<deadline:
            time.sleep(.02)
    assert id(runtime) not in runtime_checkpoints._CHECKPOINT_INFLIGHT
    deadline=time.monotonic()+5
    while True:
        restored=load_runtime_store(runtime.checkpoint_path)
        turns=restored.list_recent_conversation_events('telegram:77',event_type='conversation.chat_turn')
        if len(turns)==3: break
        assert time.monotonic()<deadline, turns
        time.sleep(.05)
    assert wire.texts == ['確認しました。']*3
    assert len(restored.list_conversation_ingress_ids('telegram:77'))==3
    record_property('mean_complete_handler_seconds',sum(samples)/len(samples))
    record_property('max_complete_handler_seconds',max(samples))


def test_initial_context_is_compact_and_keeps_user_context_on_real_web_api(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolSideEffectClass, ToolRiskLevel
    from nullion import preferences
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    from nullion.memory import UserMemoryEntry, UserMemoryKind
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','true')
    runtime.store.add_user_memory_entry(UserMemoryEntry('consent','workspace:workspace_admin',
        UserMemoryKind.PREFERENCE,'outbound_consent','Ask before sending emails',
        created_at=datetime.now(UTC),use_score=10))
    runtime.checkpoint()
    profile=tmp_path/'profile.json';profile.write_text(json.dumps({'address':'Montréal, Canada'}))
    monkeypatch.setattr(preferences,'_PROFILE_PATH',profile)
    registry=ToolRegistry()
    from nullion.tools import register_weather_forecast_tool, register_market_quote_tool
    # These native reads are included in cold-start scope before any cached
    # scope decision exists. Availability must not inflate a plain first turn.
    register_weather_forecast_tool(registry)
    register_market_quote_tool(registry)
    # Optional installed capabilities must be discoverable without loading
    # full provider inventories or adding another model call to plain turns.
    for i in range(60):
        registry.register(ToolSpec(f'optional_read_{i}','Optional provider capability',ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,False,5,capability_tags=('public_source_discovery',)),
            lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{}))
    class Model:
        payloads=[]
        def create(self,**kwargs):
            reply=workflow_provider_reply(kwargs,completion=True)
            if reply is not None:return reply
            self.payloads.append(kwargs)
            assert 'Montréal' in json.dumps(kwargs['messages'],ensure_ascii=False)
            assert 'Ask before sending emails' in json.dumps(kwargs['messages'],ensure_ascii=False)
            return text('Bonjour !')
    model=Model();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for i,prompt in enumerate(['Bonjour','こんにちは','Hola']):
            started=time.monotonic()
            result=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:compact-{i}'})
            samples.append(time.monotonic()-started)
            assert result.status_code==200 and 'Bonjour' in result.json()['text'],result.text
    assert len(model.payloads)==3
    sizes=[sum(len(str(row.get('content',''))) for row in kw['messages']) for kw in model.payloads]
    assert max(sizes)<20000, [(len(str(row.get('content',''))),str(row.get('content',''))[:140])
                             for row in model.payloads[0]['messages']]
    assert sum(samples)/len(samples)<.6 and max(samples)<1,samples
    record_property('max_initial_context_chars',max(sizes))
