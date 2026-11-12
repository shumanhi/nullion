"""Controlled real-review result through the running stage Web surface."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, APP

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1'
    or os.environ.get('NULLION_LIVE_REVIEW_LAYOUT_REPLAY') != '1', reason='Opt-in controlled stage replay')


def test_reviewed_recommendation_preserves_actionable_links_after_reload(live_web):
    from playwright.sync_api import expect
    record = json.loads((APP/'.e2e-artifacts/product-links-20261105/live-review.json').read_text())
    assert record['review']['execution_satisfied'] and not record['source_calls']
    page, frames, errors, database, cid, base = live_web
    text = record['text']
    assert page.request.post(base+'/api/chat/history/'+cid+'/message', data={
        'role': 'user', 'text': 'Controlled recommendation replay; fictional catalog, no live shopping.'}).ok
    assert page.request.post(base+'/api/chat/history/'+cid+'/message', data={'role': 'bot', 'text': text}).ok
    for _ in range(2):
        page.reload(wait_until='domcontentloaded')
        bubble = page.locator('#messages .msg.bot .bubble')
        expect(bubble).to_have_count(1)
        for url in record['expected_urls']:
            expect(bubble.locator('a[href="'+url+'"]')).to_have_count(1)
        expect(bubble).to_contain_text('CedarBond')
        assert not errors and 'Detailed application and compatibility notes' not in bubble.inner_text()
        history = page.request.get(base+'/api/chat/history/'+cid).json()['messages']
        assert [r['text'] for r in history if r['role'] == 'bot'] == [text]
    out = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    bubble.screenshot(path=str(out/'review-links-web.png'))
    (out/'review-links-web.json').write_text(json.dumps({'text': text, 'links': record['expected_urls'],
        'fixture_data': True, 'ui_model_calls': 0, 'source_calls': 0}, indent=2))
