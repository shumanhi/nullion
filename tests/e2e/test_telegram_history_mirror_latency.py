"""Real Telegram SDK and locked encrypted SQLite mirror, with external model/HTTP fakes."""
import asyncio
from datetime import UTC, datetime
import os
import sqlite3
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


def test_history_mirror_cannot_delay_answer_or_starve_typing(tmp_path,monkeypatch,record_property):
    from telegram import Bot,Chat,Message,Update,User
    from nullion import telegram_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.telegram_transport import build_telegram_bot
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN,TelegramWire,install_wire
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);runtime.active_tool_registry=ToolRegistry()
    mirror=chat_store.ChatStore(tmp_path/'history.db');monkeypatch.setattr(chat_store,'_store',mirror)
    mirror.save_message('telegram:77','user','Earlier context',channel='telegram:77')
    monkeypatch.setattr(telegram_app,'_TYPING_KEEPALIVE_INTERVAL_SECONDS',.025)
    wire=TelegramWire();original=wire.handle;typing=[];delivered=asyncio.Event()
    async def handle(request):
        if request.url.path.endswith('/sendChatAction'):
            typing.append(time.monotonic());return httpx.Response(200,json={'ok':True,'result':True})
        if request.url.path.endswith('/sendMessage'):
            await asyncio.sleep(.12)  # Final HTTP acknowledgement remains pending.
        response=await original(request)
        if request.url.path.endswith('/sendMessage'):delivered.set()
        return response
    wire.handle=handle;install_wire(monkeypatch,wire)
    class Model:
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            model_entered.set();assert locked.wait(2)
            return text('確認結果：青です。')
    model=Model();settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token=TOKEN,
        operator_chat_id='77',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
    samples=[]
    async def replay():
        async with build_telegram_bot(Bot,TOKEN) as bot:
            for index,prompt in enumerate(['好きな色は？','¿Cuál es mi color?','Quelle couleur ?'],1):
                nonlocal model_entered,locked
                model_entered,locked=threading.Event(),threading.Event()
                release=threading.Event();delivered.clear()
                def writer():
                    assert model_entered.wait(3)
                    with sqlite3.connect(tmp_path/'history.db') as db:
                        db.execute('BEGIN EXCLUSIVE');locked.set();release.wait(3)
                thread=threading.Thread(target=writer);thread.start()
                message=Message(message_id=index,date=datetime.now(UTC),chat=Chat(77,'private'),
                    from_user=User(77,'Fixture',False),text=prompt)
                message.set_bot(bot);message.chat.set_bot(bot)
                before=len(typing);started=time.monotonic()
                task=asyncio.create_task(service.on_message(Update(update_id=index,message=message),SimpleNamespace(bot=bot)))
                try:
                    await asyncio.wait_for(delivered.wait(),2)
                    samples.append(time.monotonic()-started)
                    assert not release.is_set()
                    assert len(typing)>=before+2,'Typing stopped before the final HTTP acknowledgement'
                finally:
                    release.set()
                    try:
                        await asyncio.wait_for(task,3)
                    finally:
                        thread.join(2)
            assert sum(samples)/len(samples)<.7 and max(samples)<1.2,samples
    model_entered,locked=threading.Event(),threading.Event()
    asyncio.run(replay())
    assert wire.texts==['確認結果：青です。']*3,wire.texts
    assert not service._turn_dispatch_tracker._order_by_conversation
    rows=mirror.load_messages('telegram:77')
    assert len([r for r in rows if r['role']=='bot'])==3
    record_property('mean_answer_seconds',sum(samples)/len(samples))
    record_property('max_answer_seconds',max(samples))


@pytest.mark.parametrize('transport', ['websocket', 'http'])
def test_shared_initial_notice_is_visible_during_slow_preparation_provider(tmp_path,monkeypatch,record_property,transport):
    from playwright.sync_api import expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _remember_chat_turn
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text
    from tests.e2e.test_web_surface_contracts import _UvicornThread,_free_port,_open_page
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    primary=threading.Event();preparing=threading.Event()
    class Model:
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if not kwargs.get('tools'):
                preparing.set();time.sleep(3.8)
                return text('{}')
            primary.set();return text('確認済みの結果です。')
    app=create_app(runtime,AgentOrchestrator(model_client=Model()),ToolRegistry())
    samples=[]
    frames=[]
    def configure(page):
        page.on('websocket',lambda socket: socket.on('framereceived',lambda value: frames.append(value)))
        if transport=='http':
            page.add_init_script("window.WebSocket = class extends WebSocket { constructor() { throw new Error('Fixture: upgrade unavailable'); } };")
    with _UvicornThread(app,_free_port()) as server:
        pw,browser,page,errors=_open_page(server.base_url,configure_page=configure)
        try:
            for prompt in ['確認してください','Vérifiez ceci','Comprueba esto']:
                # A real unresolved context frame gates an auxiliary provider
                # decision before primary inference, as in the cold prod turn.
                cid=page.evaluate('conversationId')
                _remember_chat_turn(runtime,chat_id=cid,conversation_id=cid,
                    user_message='Inspect https://example.test/source',
                    assistant_reply='Quelle source ?\n1. https://example.test/source\n2. Une autre source')
                primary.clear();preparing.clear();started=time.monotonic()
                page.locator('#user-input').fill(prompt);page.locator('#send-btn').click()
                expect(page.locator('#messages .status-bubble').filter(has_text='I can handle multiple at once')).to_have_count(1,timeout=3300)
                samples.append(time.monotonic()-started)
                for _ in range(20):
                    if preparing.is_set():break
                    page.wait_for_timeout(50)
                assert preparing.is_set() and not primary.is_set(),'The notice waited until primary inference'
                expect(page.locator('#messages .msg.bot .bubble').filter(has_text='確認済みの結果です。')).to_have_count(len(samples),timeout=20000)
            assert sum(samples)/len(samples)<3.1 and max(samples)<3.3,samples
            assert not errors
        except Exception:
            from pathlib import Path
            import json
            evidence=Path('.e2e-artifacts/initial-progress')/transport;evidence.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(evidence/'failure.png'))
            (evidence/'failure.json').write_text(json.dumps({'frames':frames,
                'bubbles':page.locator('#messages .bubble').all_text_contents()},ensure_ascii=False,indent=2))
            raise
        finally:browser.close();pw.stop()
    record_property('mean_initial_notice_seconds',sum(samples)/len(samples))
    record_property('max_initial_notice_seconds',max(samples))
