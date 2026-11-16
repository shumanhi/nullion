"""Real chat ingress/SQLite with a slow external key-storage boundary.

Telegram cases cover its handler and transport contract, not a live user app.
"""
import asyncio
from dataclasses import replace
from datetime import UTC, datetime
import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface',['web','telegram'])
def test_changed_memory_snapshot_has_bounded_pre_model_latency_and_owner_isolation(tmp_path,monkeypatch,surface,record_property):
    from fastapi.testclient import TestClient
    from nullion import runtime_persistence as persistence
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.memory import UserMemoryEntry, UserMemoryKind
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0')
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','true')
    monkeypatch.setenv('NULLION_KEY_STORAGE','local')
    for index in range(49):
        runtime.store.add_user_memory_entry(UserMemoryEntry(str(index),'workspace:workspace_admin',
            UserMemoryKind.FACT,'記録'+str(index),'観測'+str(index)))
    consent=UserMemoryEntry('consent','workspace:workspace_admin',UserMemoryKind.PREFERENCE,
        'consent','Confirm before sending. Revision 0')
    runtime.store.add_user_memory_entry(consent)
    runtime.store.add_user_memory_entry(UserMemoryEntry('other','workspace:other',UserMemoryKind.PREFERENCE,
        'private','OTHER_OWNER_PRIVATE_VALUE'))
    runtime.checkpoint(force=True)
    load=persistence._runtime_memory_cipher_for;loads=[]
    def slow_key_storage(path):
        loads.append(path)
        time.sleep(.05)
        return load(path)
    monkeypatch.setattr(persistence,'_runtime_memory_cipher_for',slow_key_storage)
    class Model:
        revision=0
        calls=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            self.calls+=1
            packet=json.dumps(kwargs.get('messages'),ensure_ascii=False)
            assert 'Confirm before sending. Revision '+str(self.revision) in packet
            assert 'OTHER_OWNER_PRIVATE_VALUE' not in packet
            return text('Acknowledged revision '+str(self.revision)+'.')
    model=Model();samples=[];registry=ToolRegistry()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as api:
        if surface=='telegram':
            from nullion.config import load_settings
            from nullion.telegram_app import ChatOperatorService
            settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
                'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
            service=ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,
                operator_chat_id='77',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
        for index,prompt in enumerate(['こんにちは。','Buenos días.','مرحبا.'],1):
            model.revision=index
            current=replace(consent,value='Confirm before sending. Revision '+str(index),updated_at=datetime.now(UTC))
            persistence.save_user_memory_entry(current,runtime.checkpoint_path)
            before=len(loads);calls=model.calls;delivered=[]
            started=time.monotonic()
            if surface=='web':
                response=api.post('/api/chat',json={'text':prompt,'conversation_id':'web:startup-'+str(index)})
                assert response.status_code==200,response.text
                reply=response.json()['text'];cid='web:startup-'+str(index)
            else:
                class Message:
                    text=prompt;message_id=1000+index;reply_to_message=None
                    chat=SimpleNamespace(id=77,send_action=lambda **kwargs:asyncio.sleep(0))
                    async def reply_text(self,value,**kwargs):
                        delivered.append(value);return SimpleNamespace(message_id=2000+len(delivered))
                asyncio.run(service.on_message(SimpleNamespace(update_id=1000+index,message=Message()),SimpleNamespace(bot=None)))
                assert delivered
                reply=delivered[-1];cid='telegram:77'
            samples.append(time.monotonic()-started)
            assert 'Acknowledged revision '+str(index) in reply,reply
            assert len(loads)-before<=7, 'Resolve keys per verified snapshot/checkpoint batch, not once per row'
            assert model.calls-calls==1, 'Plain chat must not add a routing or scope model call'
            turns=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')
            assert turns and not turns[-1].get('tool_results')
    assert sum(samples)/len(samples)<.8 and max(samples)<1.4,samples
    record_property(surface+'_startup_mean_seconds',sum(samples)/len(samples))
    record_property(surface+'_startup_max_seconds',max(samples))
