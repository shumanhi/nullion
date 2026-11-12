"""Real messaging runner, process-owned SQLite leases, and Web delivery history.

Only the model and Telegram SDK transport boundaries are faked. This is not a
native Telegram app acceptance test.
"""
import asyncio
from datetime import datetime, timezone
import json
import multiprocessing
import os
from types import SimpleNamespace

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker boundary E2E')


@pytest.mark.asyncio
@pytest.mark.parametrize('answer',['**Completed**','**完了**','**Terminado**'])
@pytest.mark.parametrize('reject_edit', [False, True])
async def test_real_telegram_runner_delayed_ack_keeps_one_visible_reply_and_receipt(tmp_path,monkeypatch,answer,reject_edit):
    from nullion import chat_store,telegram_app as t
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    from tests.workflow_provider_protocol import workflow_provider_reply
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'data'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setattr(chat_store,'_store',None)
    class Provider:
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,reasoned=True)
            return protocol if protocol is not None else {'stop_reason':'end_turn','content':[{'type':'text','text':answer}]}
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    model=Provider();runtime.model_client=model;runtime.active_tool_registry=ToolRegistry()
    settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service=t.ChatOperatorService(runtime=runtime,settings=settings,bot_token='fixture',operator_chat_id='77',
        agent_orchestrator=AgentOrchestrator(model_client=model))
    wire=[];edits=[]
    class Draft:
        def __init__(self,index):self.index=index;self.message_id=999+index
        async def edit_text(self,text,**kwargs):
            if reject_edit:
                from telegram.error import BadRequest
                raise BadRequest('Message cannot be edited')
            edits.append((text,kwargs));wire[self.index]=text
        async def delete(self):
            assert reject_edit, 'Successful draft must be edited, not withdrawn'
            wire[self.index]=None
    class Message:
        text='α';caption=None;message_id=5;from_user=SimpleNamespace(id=77,first_name='Fixture',last_name='',username='fixture')
        async def send_action(self,*args,**kwargs):pass
        async def reply_text(self,text,**kwargs):
            wire.append(text)
            index=len(wire)-1
            if len(wire)==1:await asyncio.sleep(2.3)
            return Draft(index)
    message=Message();message.chat=SimpleNamespace(id=77,send_action=message.send_action)
    update=SimpleNamespace(update_id=42,message=message)
    await asyncio.wait_for(service.on_message(update,SimpleNamespace(bot=None)),15)
    await asyncio.sleep(.35)  # no late unowned send may arrive after finalization
    assert [text for text in wire if text is not None]==[answer.replace('**','<b>',1).replace('**','</b>',1)],wire
    if not reject_edit:assert edits and edits[-1][1].get('parse_mode')=='HTML'
    receipts=[json.loads(row) for row in (tmp_path/'data/delivery_receipts.jsonl').read_text().splitlines()]
    receipts=[r for r in receipts if r.get('request_id')=='telegram-update:42']
    assert len(receipts)==1 and receipts[0]['message_id']==('1000' if reject_edit else '999') and receipts[0]['status']=='succeeded'
    fresh=bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert fresh.store.has_conversation_ingress_id('telegram:77','telegram-update:42')
    turns=fresh.store.list_recent_conversation_events('telegram:77',event_type='conversation.chat_turn')
    assert len(turns)==1 and turns[0]['assistant_reply']==answer


def worker(path,job,ready):
    from nullion.cron_scheduler_state import execution_lease,begin_manual_run
    from nullion.tracker_status import start_check
    assert begin_manual_run(path, job)
    with execution_lease(path,job.id) as acquired:
        assert acquired
        start_check(path,job,'crashed-worker')
        ready.send(True)
        import time
        time.sleep(60)


def test_dead_worker_reboot_clears_lock_and_delivers_one_interruption_notice(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import crons,web_app,chat_store,config
    from nullion.config import NullionSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.cron_scheduler_state import leases,begin_manual_run
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'data'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'runtime.db')
    monkeypatch.setattr(chat_store,'_store',None)
    settings=NullionSettings(doctor_enabled=False)
    monkeypatch.setattr(config,'load_settings',lambda *a,**k:settings)
    monkeypatch.setattr(web_app,'load_settings',lambda *a,**k:settings)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    with TestClient(web_app.create_app(runtime,None,ToolRegistry())) as client:
        created=client.post('/api/crons',json={'name':'通知','task':'確認','schedule':'0 0 1 1 *',
            'delivery_channel':'web','delivery_target':'web:recovery'})
        assert created.status_code==200,created.text
        job=crons.get_cron(created.json()['job']['id'])
    ctx=multiprocessing.get_context('fork');read,write=ctx.Pipe(duplex=False)
    process=ctx.Process(target=worker,args=(runtime.checkpoint_path,job,write));process.start()
    try:
        assert read.poll(5) and read.recv() is True
        assert job.id in leases(runtime.checkpoint_path)
        process.terminate();process.join(5);assert not process.is_alive()
        restarted=bootstrap_persistent_runtime(runtime.checkpoint_path)
        app=web_app.create_app(restarted,None,ToolRegistry())
        with TestClient(app) as client:
            assert leases(runtime.checkpoint_path)=={}
            assert app.state.tracker_status_tick(crons.load_crons(refresh_next_runs=False),datetime.now(timezone.utc))==1
            assert app.state.tracker_status_tick(crons.load_crons(refresh_next_runs=False),datetime.now(timezone.utc))==0
            history=client.get('/api/chat/history/web:recovery').json()['messages']
            assert len(history)==1 and 'No completed report' in history[0]['text'],history
            assert 'Ask me to run' in history[0]['text']
            current=next(j for j in client.get('/api/crons').json() if j['id']==job.id)
            assert current['tracker_status']['last_outcome']=='interrupted'
            assert current['scheduler_status']!='running' and current['enabled']
            assert begin_manual_run(runtime.checkpoint_path,crons.get_cron(job.id))
    finally:
        if process.is_alive():process.terminate();process.join(3)
