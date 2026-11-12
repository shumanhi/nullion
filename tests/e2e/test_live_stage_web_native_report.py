"""Native corrected-report replay through the stage Web publication boundary."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, APP

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1'
    or os.environ.get('NULLION_LIVE_REVIEW_LAYOUT_REPLAY')!='1',reason='Explicit native report replay')


def test_native_corrected_report_has_one_title_and_preserves_required_content(live_web):
    from playwright.sync_api import expect
    record=json.loads((APP/'.e2e-artifacts/triage-detail-20261105/live-native-review-final.json').read_text())
    assert record['review']['execution_satisfied'] and record['review']['source_presentation_valid'] is None
    assert record['review']['disposition']=='complete' and not record['source_calls']
    page,frames,errors,database,cid,base=live_web
    body=record['delivery_text']
    assert page.request.post(base+'/api/chat/history/'+cid+'/message',data={'role':'user',
        'text':'Controlled native obligation report replay; fictional records, no account lookup.'}).ok
    assert page.request.post(base+'/api/chat/history/'+cid+'/message',data={'role':'bot','text':body}).ok
    for _ in range(2):
        page.reload(wait_until='domcontentloaded')
        bubble=page.locator('#messages .msg.bot .bubble')
        expect(bubble).to_have_count(1)
        visible=bubble.inner_text()
        for amount in ('$200','$720','$760'):expect(bubble).to_contain_text(amount)
        assert visible.count('Native report replay')==1 and '**' not in visible
        assert 'Historical detail' not in visible and 'Full subject:' not in visible
        assert len(visible.split())<record['original_words']*.7
        assert bubble.locator('li').count()>=5 and not errors
        history=page.request.get(base+'/api/chat/history/'+cid).json()['messages']
        assert [r['text'] for r in history if r['role']=='bot']==[body]
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    page.screenshot(path=str(out/'native-report-web.png'))
    (out/'native-report-web.json').write_text(json.dumps({'conversation_id':cid,'text':body,
        'fixture_data':True,'source_calls':0,'ui_model_calls':0},ensure_ascii=False,indent=2))
