"""Render the real model's replayed report through stage's saved-chat boundary."""
import json
import os
from pathlib import Path

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, APP

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1'
    or os.environ.get('NULLION_LIVE_REVIEW_LAYOUT_REPLAY')!='1',reason='Explicit captured-answer stage replay')


def test_live_replayed_amazon_report_is_compact_and_readable_in_web_and_saved_history(live_web):
    from playwright.sync_api import expect
    record=json.loads((APP/'.e2e-artifacts/review-layout-20261105/amazon-live-review.json').read_text())
    # Durable history intentionally compacts source observations. This replay
    # proves presentation of the retained partial evidence, not fresh research.
    assert record['review']['disposition']=='retry' and record['review']['execution_satisfied'] is False
    assert record['retained_snapshot_omitted_observations']>0
    assert record['review']['report_layout_required'] and not record['review']['report_layout_fallback']
    assert not record['source_calls'] and record['words']<=275
    page,frames,errors,database,cid,base=live_web
    body=record['text']
    assert page.request.post(base+'/api/chat/history/'+cid+'/message',data={'role':'user',
        'text':'Replay of the captured comparison; no new source lookup.'}).ok
    assert page.request.post(base+'/api/chat/history/'+cid+'/message',data={'role':'bot','text':body}).ok
    for _ in range(2):
        page.reload(wait_until='domcontentloaded')
        bubble=page.locator('#messages .msg.bot .bubble')
        expect(bubble).to_have_count(1)
        expect(bubble).to_contain_text('$9.98')
        assert bubble.inner_text().count('📊')==1 and '**' not in bubble.inner_text()
        assert bubble.locator('li').count()>=1
        expect(bubble).to_contain_text('What remains')
        assert len(bubble.inner_text().split())<=275 and not errors
        saved=page.request.get(base+'/api/chat/history/'+cid).json()['messages']
        assert [row['text'] for row in saved if row['role']=='bot']==[body]
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    page.screenshot(path=str(out/'amazon-report-web.png'))
    (out/'amazon-report-web.json').write_text(json.dumps({'conversation_id':cid,'text':body,
        'words':record['words'],'model_calls_in_ui_check':0,'source_calls':0},ensure_ascii=False,indent=2))
