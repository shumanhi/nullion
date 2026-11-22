"""Verify native preview bytes through MIME, Telegram upload and a browser."""
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import threading

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('source', ['html_body', 'html_path'])
@pytest.mark.parametrize('heading', ['Montréal → Québec · été — €', '東京 → 大阪 · 旅程'])
def test_email_preview_upload_and_mime_keep_unicode(tmp_path, monkeypatch, source, heading):
    from email import policy
    from email.parser import BytesParser
    from playwright.sync_api import sync_playwright, expect
    from telegram import Bot, Message
    from nullion.approvals import create_approval_request
    from nullion.artifacts import artifact_root_for_principal
    from nullion.telegram_app import _approval_decision_card, _deliver_reply
    from nullion.tools import ToolInvocation, _email_html_preview_path_for_invocation, _email_message_for_invocation
    from tests.e2e.test_generated_artifact_protocol_recovery import upload_boundary
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path,monkeypatch)
    document = '<!doctype html><html><body style="color:navy"><h1>'+heading+'</h1><p>Full report</p></body></html>'
    root = artifact_root_for_principal('telegram:42')
    root.mkdir(parents=True,exist_ok=True)
    original = root/'source.html'
    original.write_text(document,encoding='utf-8')
    args = {'to':['reader@example.test'],'subject':'Unicode report','body':'Plain fallback',
        source:document if source=='html_body' else str(original)}
    invocation = ToolInvocation('draft','email_send','telegram:42',args)
    preview = Path(_email_html_preview_path_for_invocation(invocation))
    expected = preview.read_bytes()
    assert b'<meta charset="utf-8">' in expected[:1024]
    assert original.read_text(encoding='utf-8') == document
    mime, _ = _email_message_for_invocation(invocation)
    parsed = BytesParser(policy=policy.default).parsebytes(mime.as_bytes())
    html_part = next(part for part in parsed.walk() if part.get_content_type()=='text/html')
    assert html_part.get_content_charset() == 'utf-8'
    assert heading in html_part.get_content()
    assert '<meta charset="utf-8">' in html_part.get_content()
    approval = create_approval_request(requested_by='telegram:42',action='use_tool',resource='email_send',
        context={'tool_name':'email_send','tool_arguments':args,'html_preview_path':str(preview)})
    runtime.store.add_approval_request(approval)
    card = _approval_decision_card(approval)
    wire = []
    with upload_boundary(wire) as (url, uploads):
        async def deliver():
            async with Bot('123456:synthetic',base_url=url) as bot:
                message = Message.de_json({'message_id':1,'date':1,'chat':{'id':42,'type':'private'},'text':'Review draft'},bot)
                await _deliver_reply(message,card.text,principal_id='telegram:42',decision_card=card)
        asyncio.run(deliver())
    assert expected in uploads
    assert any('Send email' in str(payload) for _,payload in wire)
    class Preview(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            # Model the standalone attachment viewer: no charset HTTP override.
            self.send_header('Content-Type','text/html')
            self.end_headers()
            self.wfile.write(expected)
        def log_message(self,*args): pass
    server = ThreadingHTTPServer(('127.0.0.1',0),Preview)
    thread = threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto('http://127.0.0.1:'+str(server.server_port),wait_until='domcontentloaded')
            assert page.evaluate('document.characterSet') == 'UTF-8'
            expect(page.locator('h1')).to_have_text(heading)
            browser.close()
    finally:
        server.shutdown();server.server_close();thread.join(2)
