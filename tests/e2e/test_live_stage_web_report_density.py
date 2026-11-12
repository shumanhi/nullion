"""Compact native report replay on the running Web surface, no account IO."""
import json
import os
from pathlib import Path

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, APP

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1'
    or os.environ.get('NULLION_LIVE_REVIEW_LAYOUT_REPLAY') != '1', reason='Opt-in controlled stage replay')


def test_native_report_is_compact_and_keeps_decision_facts(live_web):
    from playwright.sync_api import expect
    record = json.loads((APP/'.e2e-artifacts/model-timeout-20261106/live-native-review.json').read_text())
    assert record['review']['execution_satisfied'] and not record['source_calls']
    page, frames, errors, database, cid, base = live_web
    assert page.request.post(base+'/api/chat/history/'+cid+'/message', data={
        'role': 'user', 'text': 'Controlled compact report replay; fictional records, no account lookup.'}).ok
    assert page.request.post(base+'/api/chat/history/'+cid+'/message', data={
        'role': 'bot', 'text': record['text']}).ok
    for _ in range(2):
        page.reload(wait_until='domcontentloaded')
        bubble = page.locator('#messages .msg.bot .bubble')
        expect(bubble).to_have_count(1)
        for fact in ('$420', '$460', '$150', '$2,000', '$1,000', '11:00 PM', 'placeholder'):
            expect(bubble).to_contain_text(fact)
        assert bubble.locator('li').count() >= 6 and not errors
        assert len(bubble.inner_text().split()) < record['original_words']*.7
        assert 'Full optional notification wording' not in bubble.inner_text()
        for i in range(6): expect(bubble.locator('a[href="https://records.example/'+str(i)+'"]')).to_have_count(1)
    out = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    bubble.screenshot(path=str(out/'compact-report-web.png'))
    (out/'compact-report-web.json').write_text(json.dumps({'text': record['text'], 'fixture_data': True,
        'source_calls': 0, 'ui_model_calls': 0, 'words': record['words']}, indent=2))
