"""Adjacent platform senders retain real bytes and shared durable receipts."""
import asyncio
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import httpx
import pytest

from tests.e2e.test_delivery_acknowledgement_boundary import prepared
from nullion.delivery_acknowledgement import UnconfirmedDelivery

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E')


@pytest.mark.parametrize('variant',['v2','legacy'])
@pytest.mark.parametrize('mode',['unknown','multi_unknown'])
def test_slack_upload_path_records_acknowledged_prefix_and_does_not_replay(tmp_path,monkeypatch,variant,mode):
    from nullion.slack_app import send_slack_platform_delivery
    from nullion.messaging_adapters import list_platform_delivery_receipts
    runtime,paths=prepared(tmp_path,monkeypatch,count=3)
    accepted=[];texts=[]
    async def upload(**kw):
        accepted.append(Path(kw['file']).read_bytes())
        if mode=='unknown' or len(accepted)==2:raise httpx.ReadTimeout('Accepted before response loss')
        return {'ok':True,'file':{'id':'fixture'}}
    async def post(**kw):texts.append(kw['text']);return {'ok':True,'ts':'fixture'}
    client=SimpleNamespace(files_upload_v2=upload if variant=='v2' else None,files_upload=upload,chat_postMessage=post)
    from slack_sdk.web import async_client
    monkeypatch.setattr(async_client,'AsyncWebClient',lambda **kw:client)
    reply='確認結果\n'+'\n'.join('MEDIA:'+str(p) for p in paths)
    with pytest.raises(UnconfirmedDelivery):
        asyncio.run(send_slack_platform_delivery(bot_token='synthetic',channel='fixture',text=reply,principal_id='telegram:77'))
    assert len(accepted)==(1 if mode=='unknown' else 2) and len(texts)==1
    assert accepted[0]==paths[0].read_bytes()
    receipt=list_platform_delivery_receipts()[0]
    assert receipt['status']=='unknown' and receipt['attachment_count']==int(mode=='multi_unknown')


@pytest.mark.parametrize('mode',['unknown','multi_unknown'])
def test_discord_rest_does_not_replay_accepted_multipart(tmp_path,monkeypatch,mode):
    from nullion.discord_app import send_discord_platform_delivery
    from nullion.messaging_adapters import list_platform_delivery_receipts
    runtime,paths=prepared(tmp_path,monkeypatch,count=3)
    accepted=[];texts=[]
    async def handle(request):
        body=await request.aread()
        if request.headers.get('content-type','').startswith('multipart/'):
            accepted.append(body)
            if mode=='unknown' or len(accepted)==2:raise httpx.ReadTimeout('Lost response',request=request)
        else:texts.append(json.loads(body)['content'])
        return httpx.Response(200,json={'id':'fixture'})
    native_client=httpx.AsyncClient
    monkeypatch.setattr(httpx,'AsyncClient',lambda **kw:native_client(**kw,transport=httpx.MockTransport(handle)))
    with pytest.raises(UnconfirmedDelivery):
        asyncio.run(send_discord_platform_delivery(bot_token='synthetic',channel_id='77',
            text='確認結果\n'+'\n'.join('MEDIA:'+str(p) for p in paths),principal_id='telegram:77'))
    assert len(accepted)==(1 if mode=='unknown' else 2) and len(texts)==1
    assert paths[0].read_bytes() in accepted[0]
    receipt=list_platform_delivery_receipts()[0]
    assert receipt['status']=='unknown' and receipt['attachment_count']==int(mode=='multi_unknown')


def test_discord_sdk_preserves_partial_batch_progress(tmp_path,monkeypatch):
    from nullion.discord_app import _send_discord_reply_files
    runtime,paths=prepared(tmp_path,monkeypatch,count=3)
    # A file object at the external SDK boundary; it exposes actual bytes.
    class File:
        def __init__(self,path,**kw):self.fp=open(path,'rb')
        def close(self):self.fp.close()
    monkeypatch.setitem(sys.modules,'discord',SimpleNamespace(File=File))
    accepted=[]
    async def send(**kw):
        accepted.append(kw['file'].fp.read())
        if len(accepted)==2:raise httpx.ReadTimeout('Lost acknowledgement')
        return SimpleNamespace(message_id=7)
    with pytest.raises(UnconfirmedDelivery) as caught:
        asyncio.run(_send_discord_reply_files(SimpleNamespace(send=send),text='確認結果',paths=tuple(paths)))
    assert len(accepted)==2 and caught.value.confirmed==(str(paths[0]),)
    assert caught.value.uncertain==(str(paths[1]),) and caught.value.unsent==(str(paths[2]),)
