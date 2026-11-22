"""Real Telegram ingress and SQLite, with a contending runtime checkpoint."""
import asyncio
import base64
import json
import os
import threading
import time
from types import SimpleNamespace
import pytest
from tests.e2e.test_telegram_quoted_image_recovery import PIXEL

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker Telegram ingress')


@pytest.mark.parametrize('prompt',['この画像を説明','Describe esta imagen','Décris cette image'])
def test_checkpoint_contention_keeps_attachment_and_processes_the_request(tmp_path,monkeypatch,prompt,record_property):
    from nullion import chat_store,telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.config import load_settings
    from nullion.runtime_persistence import _runtime_store_file_lock
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','false')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED','false')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setattr(chat_store,'_store',None)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    sent=[];downloads=[];primary=[]
    class File:
        async def download_as_bytearray(self):downloads.append(True);return bytearray(PIXEL)
    class Photo:
        file_size=len(PIXEL)
        async def get_file(self):return File()
    class Message:
        message_id=41;text=None;caption=prompt;photo=[Photo()];reply_to_message=None
        from_user=SimpleNamespace(id=123,first_name='Ada',last_name='',username='ada',is_bot=False)
        def __init__(self):self.chat=SimpleNamespace(id=123,send_action=self.send_action)
        async def send_action(self,*args):pass
        async def reply_text(self,text,**kwargs):sent.append(text);return SimpleNamespace(message_id=100+len(sent))
    class Model:
        def create(self,**kwargs):
            names={t['name'] for t in kwargs.get('tools',[])}
            if 'select_attachment_format' in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':None,'output_kind':'inline','embedded_media_extensions':[],'confidence':1}}]}
            if not kwargs.get('tools') and 'timeout' not in kwargs:return {'content':[{'type':'text','text':'{}'}]}
            assert base64.b64encode(PIXEL).decode() in json.dumps(kwargs.get('messages',[]))
            primary.append(True)
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'Image received and saved.'}]}
    settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz','NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    model=Model();service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,operator_chat_id='123',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
    held,stop=threading.Event(),threading.Event()
    def checkpoint_writer():
        with _runtime_store_file_lock(runtime.checkpoint_path):
            held.set()
            deadline=time.monotonic()+10
            directory=tmp_path/'.runtime.db.attachment-inputs'
            while not stop.is_set() and time.monotonic()<deadline:
                if directory.exists() and list(directory.glob('*.json')):
                    stop.wait(.3);return
                stop.wait(.01)
    thread=threading.Thread(target=checkpoint_writer);thread.start();assert held.wait(5)
    started=time.monotonic()
    try:asyncio.run(service.on_message(SimpleNamespace(update_id=1,message=Message()),SimpleNamespace(bot=None)))
    finally:stop.set();thread.join(5)
    elapsed=time.monotonic()-started;record_property('attachment_ingress_seconds',elapsed)
    assert primary==[True] and downloads==[True]
    assert any('Image received and saved.' in text for text in sent),sent
    assert not any("couldn't save the request safely" in text for text in sent)
    assert elapsed<8,elapsed
    # Secondary history receipts retry asynchronously after SQLITE_BUSY. Wait
    # for that real durable commit instead of racing its worker with restart.
    deadline=time.monotonic()+3
    while True:
        restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
        turns=restored.store.list_recent_conversation_events('telegram:123',event_type='conversation.chat_turn')
        if turns or time.monotonic()>=deadline:break
        time.sleep(.02)
    assert turns and 'Image received and saved.' in turns[-1]['assistant_reply']
