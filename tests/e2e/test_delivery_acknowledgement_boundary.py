"""Real SDK multipart uploads, scheduled graph, receipts and SQLite outbox.

Only the external HTTP transport is simulated. Accepted bytes and lost replies
are separate events, so a second upload cannot hide behind a true/false probe.
"""
import asyncio
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from types import SimpleNamespace
import zipfile

import httpx
import pytest

from nullion.delivery_acknowledgement import UnconfirmedDelivery

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')
TOKEN = '12345:synthetic-test-token'


def make_artifact(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == '.pdf':
        from pypdf import PdfWriter
        writer = PdfWriter(); writer.add_blank_page(width=400, height=400)
        writer.add_metadata({'/Title':'Verified fixture value: 7'})
        with path.open('wb') as output: writer.write(output)
    elif path.suffix == '.docx':
        from docx import Document
        d = Document(); d.add_paragraph('Verified fixture value: 7'); d.save(path)
    elif path.suffix == '.xlsx':
        from openpyxl import Workbook
        w = Workbook(); w.active.append(['value', 7]); w.save(path)
    elif path.suffix == '.pptx':
        from pptx import Presentation
        p = Presentation(); slide = p.slides.add_slide(p.slide_layouts[0]); slide.shapes.title.text = 'Value: 7'; p.save(path)
    elif path.suffix == '.png':
        from PIL import Image
        Image.new('RGB', (20,20), 'blue').save(path)
    elif path.suffix == '.zip':
        with zipfile.ZipFile(path, 'w') as z: z.writestr('selected.txt', 'value: 7')
    elif path.suffix == '.html': path.write_text('<!doctype html><html><body><h1>Verified report</h1><p>Selected fixture value: 7. Evidence was collected for this run and retained with this generated report.</p></body></html>')
    else: path.write_text('value,amount\nselected,7\n')
    return path


class TelegramWire:
    def __init__(self, mode='ok'):
        self.errors = []
        self.mode = mode; self.attempts = []; self.accepted = []; self.texts = []
    async def handle(self, request):
        method = request.url.path.rsplit('/', 1)[-1]
        if method == 'getMe':
            return httpx.Response(200, json={'ok':True, 'result':{'id':12345,'is_bot':True,'first_name':'Fixture'}})
        body = await request.aread()
        if method == 'sendDocument':
            self.attempts.append(body)
            index = len(self.attempts)
            if self.mode == 'connect' and index == 1:
                raise httpx.ConnectTimeout('No connection established', request=request)
            if self.mode == 'rate' and index == 1:
                return httpx.Response(429, json={'ok':False,'error_code':429,'description':'Rejected before execution',
                    'parameters':{'retry_after':1}})
            if self.mode == 'rejected':
                return httpx.Response(400, json={'ok':False,'error_code':400,'description':'Explicit upload rejection'})
            if self.mode == 'multi_rejected' and index == 2:
                return httpx.Response(400, json={'ok':False,'error_code':400,'description':'Explicit second-file rejection'})
            self.accepted.append(body)
            if self.mode in {'ok','delayed','connect','rate'}:
                assert request.extensions['timeout']['read'] == 120
                assert request.extensions['timeout']['write'] == 120
            if self.mode == 'cancel':raise asyncio.CancelledError()
            if self.mode in {'unknown','unknown_notice'} or self.mode == 'multi_unknown' and index == 2:
                raise httpx.ReadTimeout('Server accepted bytes, acknowledgement lost', request=request)
            if self.mode == 'delayed': await asyncio.sleep(.015)
            return httpx.Response(200, json={'ok':True,'result':{'message_id':100+index,'date':1,
                'chat':{'id':77,'type':'private'},'document':{'file_id':'f','file_unique_id':'u'}}})
        if method == 'sendMessage':
            # PTB currently uses form encoding for a text-only send.
            from urllib.parse import parse_qs
            fields = parse_qs(body.decode())
            self.texts.append(fields.get('text',[''])[0])
            if self.mode == 'unknown_notice':raise httpx.ReadTimeout('Notice accepted before response loss',request=request)
            return httpx.Response(200, json={'ok':True,'result':{'message_id':200+len(self.texts),
                'date':1,'chat':{'id':77,'type':'private'},'text':self.texts[-1]}})
        raise AssertionError('Unexpected external endpoint '+method)


def install_wire(monkeypatch, wire):
    import traceback
    from nullion import telegram_entrypoint
    monkeypatch.setattr(telegram_entrypoint.logger, 'warning', lambda *a, **kw: wire.errors.append(traceback.format_exc()))
    from telegram.request import HTTPXRequest
    from nullion import telegram_transport
    monkeypatch.setattr(telegram_transport, 'build_telegram_httpx_request',
        lambda: HTTPXRequest(httpx_kwargs={'transport':httpx.MockTransport(wire.handle)}))
    for key in ('NULLION_TELEGRAM_BOT_API_BASE_URL','NULLION_TELEGRAM_LOCAL_BOT_API_URL',
        'NULLION_TELEGRAM_BOT_API_URL','NULLION_TELEGRAM_MEDIA_READ_TIMEOUT_SECONDS',
        'NULLION_TELEGRAM_MEDIA_WRITE_TIMEOUT_SECONDS'):
        monkeypatch.delenv(key, raising=False)


def prepared(tmp_path, monkeypatch, suffix='.html', count=1):
    from tests.e2e.test_scheduled_report_completion import setup_runtime
    runtime = setup_runtime(tmp_path, monkeypatch)
    from nullion.artifacts import artifact_root_for_principal
    root = Path(artifact_root_for_principal('telegram:77')); assert tmp_path in root.parents
    return runtime, [make_artifact(root/f'selected-{i}{suffix}') for i in range(count)]


@pytest.mark.parametrize('surface', ['scheduled', 'operator'])
@pytest.mark.parametrize('suffix', ['.html','.pdf','.docx','.xlsx','.pptx','.png','.zip','.csv','.txt'])
def test_all_artifact_formats_survive_delayed_ack_without_reupload(tmp_path, monkeypatch, surface, suffix):
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    from nullion.telegram_app import _send_operator_telegram_message
    from nullion.messaging_adapters import list_platform_delivery_receipts
    runtime, paths = prepared(tmp_path, monkeypatch, suffix)
    wire = TelegramWire('delayed'); install_wire(monkeypatch, wire)
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    candidate = prepare_reply_for_platform_delivery('確認結果\nMEDIA:'+str(paths[0]), principal_id='telegram:77')
    assert candidate.attachments == tuple(paths), candidate
    sender = _send_operator_telegram_delivery if surface == 'scheduled' else _send_operator_telegram_message
    assert asyncio.run(sender(TOKEN,'77', '確認結果\nMEDIA:'+str(paths[0]), principal_id='telegram:77')) is True, wire.errors
    assert len(wire.attempts) == len(wire.accepted) == 1
    assert paths[0].read_bytes() in wire.accepted[0]
    receipt = list_platform_delivery_receipts()[0]
    assert receipt['status'] == 'succeeded' and receipt['attachment_count'] == 1 and receipt['message_id'] == '101'


@pytest.mark.parametrize('mode', ['unknown','unknown_notice','multi_unknown','multi_rejected','connect','rate','rejected','delayed'])
def test_real_cron_graph_keeps_transport_outcome_separate_from_execution(tmp_path, monkeypatch, mode):
    from nullion.cron_delivery import run_cron_delivery_workflow, CronRunDeliveryCallbacks
    from nullion.crons import CronJob
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    from nullion.messaging_adapters import list_platform_delivery_receipts
    from nullion.events import make_event
    runtime, paths = prepared(tmp_path, monkeypatch, count=3 if mode.startswith('multi') else 1)
    wire = TelegramWire(mode); install_wire(monkeypatch, wire)
    job = CronJob(id='transport-fixture', name='確認', task='確認結果を添付。',schedule='* * * * *',
        delivery_channel='telegram',delivery_target='77')
    def record(kind, job, channel, target, conversation, **extra):
        runtime.store.add_event(make_event(kind,'fixture',{'cron_id':job.id,**extra}))
    # The fixture starts at the delivery graph's documented completed-execution
    # boundary. Actual report generation/recovery is exercised in the companion
    # real-orchestrator test, rather than replaced inside that workflow.
    result = run_cron_delivery_workflow(job, label='Scheduled task',callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda j:j.delivery_channel,delivery_target=lambda j,c:j.delivery_target,
        run_agent_turn=lambda j,c:{'text':'検証した値は7です。\n'+'\n'.join('MEDIA:'+str(p) for p in paths),'artifacts':[str(p) for p in paths],
            'response_fulfilled':True,'artifact_delivery_required':True,'artifact_delivery_satisfied':True},
        record_event=record,block_reason=lambda result,text,artifacts:None,save_web_delivery=lambda *a:None,
        send_platform_delivery=lambda j,c,t,text:asyncio.run(_send_operator_telegram_delivery(
            TOKEN,t,text,principal_id='telegram:'+t))))
    receipts = list_platform_delivery_receipts()
    events = [e.event_type for e in runtime.store.list_events()]
    if mode in {'unknown','unknown_notice','multi_unknown','multi_rejected'}:
        status = 'partial' if mode=='multi_rejected' else 'unknown'
        assert result['cron_delivery_status'] == status and result['response_fulfilled'] is False, (result,len(wire.attempts),wire.texts,wire.errors)
        assert 'cron.delivery.'+status in events and 'cron.delivery.failed' not in events
        assert 'cron.delivery.sent' not in events and 'cron.delivery.retry' not in events
        assert len(wire.attempts) == (2 if mode.startswith('multi') else 1)
        assert len(wire.texts) == 1 and 'not been delivered' not in wire.texts[0]
        receipt = next(r for r in receipts if r['status'] == status)
        assert receipt['attachment_count'] == int(mode.startswith('multi'))
        assert bool(receipt['delivery_progress']['unsent_artifacts']) is mode.startswith('multi')
        assert result['execution_outcome']['delivery_succeeded'] is False
    elif mode == 'rejected':
        assert result['cron_delivery_status'] == 'failed' and not wire.accepted and len(wire.attempts)==1
        assert 'cron.delivery.failed' in events
    else:
        assert result['cron_delivery_status'] == 'sent' and len(wire.accepted)==1 and not wire.texts
        assert len(wire.attempts)==(2 if mode in {'connect','rate'} else 1)


@pytest.mark.parametrize('surface',['reply','callback'])
@pytest.mark.parametrize('mode',['unknown','multi_unknown','multi_rejected'])
def test_interactive_and_callback_do_not_replay_uncertain_uploads(tmp_path, monkeypatch, surface, mode):
    from telegram import Bot, Chat, Message
    from nullion.telegram_transport import build_telegram_bot
    from nullion.telegram_app import _deliver_reply, _send_callback_follow_up
    from nullion.messaging_adapters import list_platform_delivery_receipts
    runtime, paths = prepared(tmp_path, monkeypatch, count=3)
    wire=TelegramWire(mode);install_wire(monkeypatch,wire)
    async def run():
        async with build_telegram_bot(Bot,TOKEN) as bot:
            message=Message(message_id=1,date=datetime.now(UTC),chat=Chat(id=77,type='private'),text='確認')
            message.set_bot(bot)
            reply='結果\n'+'\n'.join('MEDIA:'+str(p) for p in paths)
            if surface=='reply':await _deliver_reply(message,reply,principal_id='telegram:77',quote_reply=False)
            else:await _send_callback_follow_up(message,reply,principal_id='telegram:77')
    asyncio.run(run())
    assert len(wire.attempts)==(1 if mode=='unknown' else 2) and len(wire.texts)==1
    receipt=list_platform_delivery_receipts()[0]
    assert receipt['status']==('partial' if mode=='multi_rejected' else 'unknown')
    assert receipt['attachment_count']==int(mode!='unknown')


@pytest.mark.parametrize('interrupted',[False,True])
def test_sqlite_outbox_restart_never_resends_unknown_or_inflight_file(tmp_path,monkeypatch,interrupted):
    from nullion.result_aggregator import ResultAggregator
    from nullion.task_queue import TaskRegistry
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    runtime,paths=prepared(tmp_path,monkeypatch,count=2)
    wire=TelegramWire('unknown');install_wire(monkeypatch,wire)
    async def deliver(conversation,text,**kw):
        return await _send_operator_telegram_delivery(TOKEN,'77',('MEDIA:'+text) if kw.get('is_artifact') else text,
            principal_id='telegram:77')
    def owner():return ResultAggregator(deliver_fn=deliver,task_registry=TaskRegistry(),outbox_path=tmp_path/'runtime.db')
    first=owner();record={'conversation_id':'telegram:77','summary':'確認結果','artifacts':list(map(str,paths)),
        'delivered_artifacts':[],'text_delivered':False,'work_complete':True,'status':'pending','attempts':0,'retry_at':0}
    if interrupted:record.update(status='delivering',in_flight={'kind':'artifact','path':str(paths[0])})
    first._outbox.enqueue('fixture',record)
    asyncio.run(first._deliver_outbox('fixture'))
    before=len(wire.attempts); assert before==int(not interrupted)
    second=owner();asyncio.run(second.retry_pending_deliveries());asyncio.run(second._deliver_outbox('fixture'))
    assert len(wire.attempts)==before and len(wire.texts)==1
    assert second._outbox.get('fixture')['status']=='unknown'
    assert not second._outbox.due()


@pytest.mark.parametrize('surface',['scheduled','operator'])
def test_long_caption_is_not_repeated_when_file_acknowledgement_is_lost(tmp_path,monkeypatch,surface):
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    from nullion.telegram_app import _send_operator_telegram_message
    from nullion.messaging_adapters import list_platform_delivery_receipts
    runtime,paths=prepared(tmp_path,monkeypatch)
    wire=TelegramWire('unknown');install_wire(monkeypatch,wire)
    caption='確認済みの記録。'*160
    sender=_send_operator_telegram_delivery if surface=='scheduled' else _send_operator_telegram_message
    with pytest.raises(UnconfirmedDelivery):
        asyncio.run(sender(TOKEN,'77',caption+'\nMEDIA:'+str(paths[0]),principal_id='telegram:77'))
    assert len(wire.attempts)==1 and wire.texts==[caption]
    receipt=list_platform_delivery_receipts()[0]
    assert receipt['status']=='unknown' and receipt['text_delivered'] is True and receipt['attachment_count']==0


@pytest.mark.parametrize('surface',['scheduled','operator'])
def test_client_cleanup_error_after_acknowledgement_does_not_replay_or_claim_failure(tmp_path,monkeypatch,surface):
    import telegram
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    from nullion.telegram_app import _send_operator_telegram_message
    from nullion.messaging_adapters import list_platform_delivery_receipts
    runtime,paths=prepared(tmp_path,monkeypatch)
    wire=TelegramWire();install_wire(monkeypatch,wire)
    class CleanupFailureBot(telegram.Bot):
        async def __aexit__(self,*args):
            await super().__aexit__(*args)
            raise httpx.CloseError('Cleanup failed after the SDK returned a message id')
    monkeypatch.setattr(telegram,'Bot',CleanupFailureBot)
    sender=_send_operator_telegram_delivery if surface=='scheduled' else _send_operator_telegram_message
    assert asyncio.run(sender(TOKEN,'77','確認結果\nMEDIA:'+str(paths[0]),principal_id='telegram:77')) is True
    assert len(wire.attempts)==1 and not wire.texts
    assert list_platform_delivery_receipts()[0]['status']=='succeeded'


def test_cancellation_after_acceptance_leaves_durable_inflight_proof(tmp_path,monkeypatch):
    from nullion.result_aggregator import ResultAggregator
    from nullion.task_queue import TaskRegistry
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    runtime,paths=prepared(tmp_path,monkeypatch,count=2)
    wire=TelegramWire('cancel');install_wire(monkeypatch,wire)
    async def deliver(conversation,text,**kw):
        return await _send_operator_telegram_delivery(TOKEN,'77','MEDIA:'+text if kw.get('is_artifact') else text,
            principal_id='telegram:77')
    def owner():return ResultAggregator(deliver_fn=deliver,task_registry=TaskRegistry(),outbox_path=tmp_path/'runtime.db')
    first=owner();first._outbox.enqueue('cancelled-send',{'conversation_id':'telegram:77','summary':'確認結果',
        'artifacts':list(map(str,paths)),'delivered_artifacts':[],'text_delivered':False,
        'work_complete':True,'status':'pending','attempts':0,'retry_at':0})
    with pytest.raises(asyncio.CancelledError):asyncio.run(first._deliver_outbox('cancelled-send'))
    assert len(wire.accepted)==1 and first._outbox.get('cancelled-send')['in_flight']['path']==str(paths[0])
    wire.mode='ok';second=owner();asyncio.run(second.retry_pending_deliveries())
    assert len(wire.accepted)==1 and len(wire.texts)==1
    assert second._outbox.get('cancelled-send')['status']=='unknown'
