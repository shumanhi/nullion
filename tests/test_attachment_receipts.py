import asyncio
import json
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize('surface', ['telegram', 'slack', 'discord'])
async def test_durable_attachment_save_keeps_each_client_event_loop_responsive(tmp_path, monkeypatch, surface):
    import importlib
    import httpx
    from nullion.messaging_adapters import save_messaging_attachment
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    adapter = importlib.import_module('nullion.'+surface+'_app')
    monkeypatch.setattr(adapter, 'resolve_messaging_user', lambda *a: SimpleNamespace(role='admin', user_id='admin'))
    started, heartbeat = threading.Event(), threading.Event()
    def durable_save(**kwargs):
        started.set()
        assert heartbeat.wait(.5), 'Durable write blocked the client event loop'
        return save_messaging_attachment(**kwargs)
    monkeypatch.setattr(adapter, 'save_messaging_attachment', durable_save)
    data = b'\x89PNG\r\n\x1a\noriginal'
    class Client:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, *args, **kwargs):
            return SimpleNamespace(content=data, headers={'content-type':'image/png'}, raise_for_status=lambda: None)
    monkeypatch.setattr(httpx, 'AsyncClient', Client)
    async def pulse():
        while not started.is_set():
            await asyncio.sleep(.001)
        heartbeat.set()
    pulse_task = asyncio.create_task(pulse())
    try:
        if surface == 'telegram':
            class File:
                async def download_as_bytearray(self): return bytearray(data)
            class Photo:
                file_size = len(data)
                async def get_file(self): return File()
            result = await adapter._download_telegram_attachments(SimpleNamespace(
                chat=SimpleNamespace(id=123), photo=[Photo()]), None, settings=None)
        elif surface == 'slack':
            result = await adapter._download_slack_attachments({'user':'123', 'files':[
                {'name':'image.png', 'mimetype':'image/png', 'url_private':'https://files.example/image.png'}]}, bot_token='fixture')
        else:
            result = await adapter._download_discord_attachments(SimpleNamespace(author=SimpleNamespace(id=123),
                attachments=[SimpleNamespace(filename='image.png', content_type='image/png', url='https://files.example/image.png')]))
        assert len(result) == 1 and heartbeat.is_set()
        assert Path(result[0]['path']).read_bytes() == data
        assert result[0]['principal_id'] == 'telegram_chat'
    finally:
        pulse_task.cancel()
        await asyncio.gather(pulse_task, return_exceptions=True)


def test_concurrent_same_name_inputs_keep_distinct_bytes_and_native_identities(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from hashlib import sha256
    from nullion.messaging_adapters import save_messaging_attachment
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    destination = workspace_storage_roots_for_principal('telegram_chat').uploads/'same.png'
    existed = Path.exists
    overlap = threading.Barrier(8)
    def concurrent_exists(path):
        result = existed(path)
        if path == destination:
            # Reproduce the old check/write race deterministically. Exclusive
            # allocation never needs this existence check.
            overlap.wait(timeout=5)
        return result
    monkeypatch.setattr(Path, 'exists', concurrent_exists)
    def save(index):
        data = f'入力 {index}'.encode()
        saved = save_messaging_attachment(filename='same.png', data=data,
            media_type='image/png', principal_id='telegram_chat')
        return data, saved
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(save, range(8)))
    assert len({saved['path'] for _, saved in results}) == 8
    for data, saved in results:
        assert Path(saved['path']).read_bytes() == data
        assert saved['content_sha256'] == sha256(data).hexdigest()
        assert int(saved['size_bytes']) == len(data)


def test_attachment_intake_is_durable_and_fast_during_runtime_checkpoint_contention(tmp_path, monkeypatch):
    from nullion import telegram_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import _runtime_store_file_lock
    from nullion.attachment_receipts import load_attachment_receipt
    from tests.test_telegram_reply_media_context import _message
    from nullion.messaging_adapters import save_messaging_attachment
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    held,release=threading.Event(),threading.Event()
    def writer():
        with _runtime_store_file_lock(runtime.checkpoint_path):
            held.set();assert release.wait(15)
    thread=threading.Thread(target=writer);thread.start();assert held.wait(5)
    elapsed=[];attachments=[]
    try:
        for index in range(5):
            item=save_messaging_attachment(filename=f'image-{index}.png',data=b'original image bytes',
                media_type='image/png',principal_id='telegram_chat');attachments.append(item)
            start=time.perf_counter()
            telegram_app._checkpoint_telegram_attachment_receipt(runtime,conversation_id='telegram:123',
                message_id=f'telegram-message:123:{41+index}',request_id=f'update:{index}',text='画像',attachments=[item])
            elapsed.append(time.perf_counter()-start)
            receipt=load_attachment_receipt(runtime.checkpoint_path,'telegram:123',f'telegram-message:123:{41+index}')
            assert receipt['attachments']==[item]
        assert sum(elapsed)/len(elapsed)<0.3,elapsed
        assert sorted(elapsed)[-1]<0.5,elapsed
    finally:
        release.set();thread.join(5)
    restarted=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert not restarted.store.list_recent_conversation_events('telegram:123',event_type='conversation.attachment_received')
    for index,item in enumerate(attachments):
        message=_message(photo=False);message.reply_to_message=_message(41+index)
        result=asyncio.run(telegram_app._telegram_reply_attachments(message,None,runtime=restarted,settings=None))
        assert result==[item]
    assert load_attachment_receipt(runtime.checkpoint_path,'telegram:456','telegram-message:123:41') is None


def test_interrupted_receipt_write_keeps_the_previous_exact_input(tmp_path,monkeypatch):
    from nullion import attachment_receipts as receipts
    original={'conversation_id':'telegram:123','message_id':'message:1','attachments':[{'path':'first.png'}]}
    receipts.save_attachment_receipt(tmp_path/'runtime.db',original)
    def failed(*args):raise OSError('interrupted replace')
    monkeypatch.setattr(receipts.os,'replace',failed)
    with pytest.raises(OSError):receipts.save_attachment_receipt(tmp_path/'runtime.db',{**original,'attachments':[{'path':'other.png'}]})
    assert receipts.load_attachment_receipt(tmp_path/'runtime.db','telegram:123','message:1')==original
    directory=tmp_path/'.runtime.db.attachment-inputs'
    assert not list(directory.glob('*.tmp'))
    if os.name!='nt':assert (next(directory.glob('*.json')).stat().st_mode & 0o777)==0o600


def test_primary_receipt_failure_is_not_misreported_as_success(tmp_path,monkeypatch):
    from nullion import attachment_receipts,telegram_app
    from nullion.runtime import bootstrap_persistent_runtime
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    def failed(*args):raise PermissionError('read only input directory')
    monkeypatch.setattr(attachment_receipts,'save_attachment_receipt',failed)
    with pytest.raises(PermissionError):
        telegram_app._checkpoint_telegram_attachment_receipt(runtime,conversation_id='telegram:123',message_id='message:1',request_id='update:1',text=None,attachments=[])
    assert not runtime.store.list_recent_conversation_events('telegram:123',event_type='conversation.attachment_received')
