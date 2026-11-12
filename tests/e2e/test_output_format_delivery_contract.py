"""Real report routing, SQLite history, Web rendering and Telegram HTTP bytes."""
import asyncio
from http.server import BaseHTTPRequestHandler
import json
import os
from types import SimpleNamespace
from urllib.parse import parse_qs

import pytest

from tests.test_chat_output_layout import visible
from tests.e2e.test_web_surface_contracts import _MockWebServer, _UvicornThread, _free_port, _web_app

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')

# Same structure as the incident, without private account or message data.
PARAGRAPH = 'Two refunds were issued to the account balance; no action is needed. Another statement is ready for review, and the old appointment has already passed.'
REPORT = ('**Upcoming obligations**\n\n'
          '• **Subscription** — Received Oct 13. Cancel by Nov 10. A matching event exists. **No events\n  created:** matching events already exist.\n'
          '• **Account** — Received Oct 21. Payment is due Nov 14. Review the balance before the due date.\n\n'
          '**Other findings**\n\n' + PARAGRAPH + '\n\n'
          '**Calendar:** **0 new events created; 3 existing events verified.**')


class TelegramBoundary(BaseHTTPRequestHandler):
    calls = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        data = self.rfile.read(int(self.headers['Content-Length']))
        if 'application/json' in self.headers.get('Content-Type', ''):
            payload = json.loads(data)
        elif 'multipart' in self.headers.get('Content-Type', ''):
            payload = {'upload': data}
        else:
            payload = {key: value[0] for key, value in parse_qs(data.decode()).items()}
        self.calls.append((self.path, payload))
        if self.path.endswith('/getMe'):
            result = {'id': 42, 'is_bot': True, 'first_name': 'Stage', 'username': 'stage_bot'}
        else:
            result = {'message_id': len(self.calls), 'date': 1, 'chat': {'id': 42, 'type': 'private'}}
        body = json.dumps({'ok': True, 'result': result}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def test_scheduled_report_reaches_telegram_with_natural_paragraphs(tmp_path, monkeypatch):
    from telegram import Bot
    from nullion import telegram_entrypoint
    from nullion.cron_delivery import CronRunDeliveryCallbacks, run_cron_delivery_workflow
    from nullion.telegram_formatting import format_telegram_text
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path))
    TelegramBoundary.calls = []
    with _MockWebServer(TelegramBoundary, _free_port()) as server:
        monkeypatch.setattr(telegram_entrypoint, 'build_telegram_bot',
                            lambda cls, token: Bot(token, base_url=server.base_url + '/bot'))
        sent = []
        def send(job, channel, target, text):
            sent.append(text)
            return asyncio.run(telegram_entrypoint._send_operator_telegram_delivery('123456:test-token', target, text))
        events = []
        callbacks = CronRunDeliveryCallbacks(
            effective_channel=lambda job: 'telegram', delivery_target=lambda job, channel: '42',
            run_agent_turn=lambda job, cid: {'final_text': REPORT},
            record_event=lambda kind, *a, **kw: events.append(kind),
            block_reason=lambda *a: None, save_web_delivery=lambda *a: False, send_platform_delivery=send,
        )
        job = SimpleNamespace(id='format-report', name='Synthetic account report', task='Read-only report', delivery_channel='telegram')
        outcome = run_cron_delivery_workflow(job, label='Scheduled task', callbacks=callbacks)
    assert outcome['cron_delivery_status'] == 'sent', outcome
    messages = [payload for path, payload in TelegramBoundary.calls if path.endswith('/sendMessage')]
    assert len(messages) == 1
    payload = messages[0]
    assert payload['parse_mode'] == 'HTML'
    text = visible(payload['text'], {'parse_mode': 'HTML'})
    assert '**' not in text and PARAGRAPH in text
    assert all(phrase in text for phrase in ('Oct 13.', 'Nov 10.', 'Oct 21.', 'Nov 14.'))
    import re
    assert re.search(r'<b>No events\n[ \t]*created:</b>', payload['text'])
    assert '\n\nOther findings\n\n' in text
    assert 'cron.delivery.sent' in events or 'cron.delivery.succeeded' in events
    receipts = [json.loads(row) for row in (tmp_path / 'delivery_receipts.jsonl').read_text().splitlines()]
    assert receipts[-1]['status'] == 'succeeded'


def test_web_restored_report_renders_bold_and_paragraph_spacing(tmp_path, monkeypatch):
    from playwright.sync_api import sync_playwright, expect
    from nullion.cron_delivery import scheduled_task_delivery_text
    from nullion import chat_store
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'none.env'))
    chat_store._store = None
    app, runtime, registry = _web_app(tmp_path)
    cid = 'web:format-report'
    report = scheduled_task_delivery_text(SimpleNamespace(name='Synthetic report'), REPORT)
    with _UvicornThread(app, _free_port()) as server, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(viewport={'width': 430, 'height': 932})
            context.add_init_script("localStorage.setItem('nullion_conv_id', " + json.dumps(cid) + ");")
            response = context.request.post(server.base_url + '/api/chat/history/' + cid + '/message',
                                            data={'role': 'bot', 'text': report})
            assert response.ok, response.text()
            page = context.new_page()
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto(server.base_url, wait_until='domcontentloaded')
            bubble = page.locator('#messages .msg.bot .bubble').filter(has_text='Two refunds')
            expect(bubble).to_have_count(1)
            expect(bubble.locator('strong').filter(has_text='No events')).to_have_count(1)
            expect(bubble).not_to_contain_text('**')
            # The prose remains one responsive paragraph, not fixed-column rows.
            html = bubble.inner_html()
            assert PARAGRAPH in html
            assert all(phrase in bubble.inner_text() for phrase in ('Oct 13.', 'Nov 10.', 'Oct 21.', 'Nov 14.'))
            assert bubble.locator('ol').count() == 0
            assert '<br><br><br>' not in html
            saved = context.request.get(server.base_url + '/api/chat/history/' + cid).json()['messages']
            assert any(message['text'] == report for message in saved)
            page.reload(wait_until='domcontentloaded')
            expect(page.locator('#messages')).not_to_contain_text('**')
            assert not errors, errors
            path = os.environ.get('NULLION_OUTPUT_FORMAT_SCREENSHOT')
            if path:
                page.screenshot(path=path, full_page=True)
        finally:
            browser.close()


def test_operator_long_caption_is_sent_whole_and_upload_bytes_are_preserved(tmp_path, monkeypatch):
    from telegram import Bot
    from nullion import telegram_entrypoint
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path))
    artifact = tmp_path / 'outputs' / 'nullion-artifact-layout.csv'
    artifact.parent.mkdir()
    artifact.write_bytes(b'name,value\nCedar,37\n')
    caption = '**' + ('Verified detail ' * 350).strip() + '**\n\nEND-OF-CAPTION'
    TelegramBoundary.calls = []
    with _MockWebServer(TelegramBoundary, _free_port()) as server:
        monkeypatch.setattr(telegram_entrypoint, 'build_telegram_bot',
                            lambda cls, token: Bot(token, base_url=server.base_url + '/bot'))
        ok = asyncio.run(telegram_entrypoint._send_operator_telegram_delivery(
            '123456:test-token', '42', caption + '\nMEDIA:' + str(artifact)))
    assert ok
    messages = [payload for path, payload in TelegramBoundary.calls if path.endswith('/sendMessage')]
    uploads = [payload for path, payload in TelegramBoundary.calls if path.endswith('/sendDocument')]
    assert len(messages) > 1 and len(uploads) == 1
    assert all(len(payload['text']) <= 3900 for payload in messages)
    rendered = ''.join(visible(payload['text'], {'parse_mode': payload.get('parse_mode')}) for payload in messages)
    assert 'Verified detail ' * 349 in rendered
    assert 'END-OF-CAPTION' in rendered and '**' not in rendered
    assert artifact.read_bytes() in uploads[0]['upload']


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in real stage Web replay')
def test_live_stage_output_layout(tmp_path):
    import time
    from uuid import uuid4
    from playwright.sync_api import sync_playwright, expect
    from nullion.cron_delivery import scheduled_task_delivery_text
    base = os.environ.get('NULLION_LIVE_STAGE_BASE_URL', 'http://127.0.0.1:8753')
    cid = 'web:output-format:' + uuid4().hex
    report = scheduled_task_delivery_text(SimpleNamespace(name='Synthetic formatting acceptance'), REPORT)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(viewport={'width': 430, 'height': 932})
            context.add_init_script("localStorage.setItem('nullion_conv_id', " + json.dumps(cid) + ");")
            response = context.request.post(base + '/api/chat/history/' + cid + '/message', data={'role': 'bot', 'text': report})
            assert response.ok, response.text()
            page = context.new_page()
            frames, errors = [], []
            page.on('pageerror', lambda error: errors.append(str(error)))
            def received(frame):
                data = json.loads(frame)
                if isinstance(data, dict): frames.append(data)
            page.on('websocket', lambda socket: socket.on('framereceived', received))
            page.goto(base, wait_until='domcontentloaded')
            expect(page.locator('#messages')).to_contain_text('Oct 13.')
            expect(page.locator('#messages')).to_contain_text('Nov 10.')
            expect(page.locator('#messages')).to_contain_text('Oct 21.')
            expect(page.locator('#messages .msg.bot .bubble').filter(has_text='Two refunds')).to_have_count(1)
            expect(page.locator('#messages')).not_to_contain_text('**')
            prompt = ('This is a formatting acceptance check using synthetic data only. Reformat the supplied report '
                      'into a concise chat reply with a bold heading, two bullet points and a final prose paragraph. '
                      'Keep the amounts and dates. Use no tools or files. '
                      'Report: Two refunds, $12.34 and $56.78, reached the balance on October 24. '
                      'The November 14 payment reminder already exists. No new calendar events were needed.')
            started = time.monotonic()
            page.locator('#user-input').fill(prompt)
            page.locator('#user-input').press('Enter')
            terminal = None
            while time.monotonic() - started < 120:
                done = [frame for frame in frames if frame.get('type') in {'done', 'error', 'approval_required'}
                        and not frame.get('superseded')]
                if done:
                    terminal = done[-1]
                    break
                page.wait_for_timeout(100)
            assert terminal and terminal['type'] == 'done', frames
            bubble = page.locator('#messages .msg.bot .bubble').last
            expect(bubble).to_contain_text('$12.34')
            expect(bubble).to_contain_text('$56.78')
            expect(bubble).not_to_contain_text('**')
            assert bubble.locator('strong').count() >= 1
            assert bubble.locator('li').count() == 2
            assert '<br><br><br>' not in bubble.inner_html()
            assert not errors, errors
            evidence = os.environ.get('NULLION_OUTPUT_FORMAT_EVIDENCE_DIR')
            if evidence:
                from pathlib import Path
                directory = Path(evidence); directory.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(directory / 'stage-web.png'), full_page=True)
                history = context.request.get(base + '/api/chat/history/' + cid).json()
                (directory / 'stage-web.json').write_text(json.dumps({'conversation_id': cid, 'elapsed': time.monotonic()-started,
                    'frames': frames, 'history': history, 'page_errors': errors}, ensure_ascii=False, indent=2))
        finally:
            browser.close()
