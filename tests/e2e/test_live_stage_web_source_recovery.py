"""Publish the controlled real-model source-review result on stage Web."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, APP

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1'
    or os.environ.get('NULLION_LIVE_REVIEW_LAYOUT_REPLAY')!='1',reason='Explicit source-review publication replay')


def test_reviewed_long_source_answer_survives_web_publication_and_reload(live_web):
    from playwright.sync_api import expect
    record=json.loads((APP/'.e2e-artifacts/reply-followthrough-20261105/live-source-review.json').read_text())
    assert record['result']['response_fulfilled'] is True and not record['source_calls']
    reviews=[r for r in record['events'] if r.get('event_type')=='conversation.completion_review']
    assert len(reviews)==1 and reviews[0]['presentation_repaired']
    page,frames,errors,database,cid,base=live_web
    body=record['result']['final_text']
    assert page.request.post(base+'/api/chat/history/'+cid+'/message',data={'role':'user',
        'text':'Controlled retained-source composition replay; fictional catalog.'}).ok
    assert page.request.post(base+'/api/chat/history/'+cid+'/message',data={'role':'bot','text':body}).ok
    for _ in range(2):
        page.reload(wait_until='domcontentloaded')
        bubble=page.locator('#messages .msg.bot .bubble')
        expect(bubble).to_have_count(1)
        for amount in ('$9.98','$9.88','$5.97'):expect(bubble).to_contain_text(amount)
        visible=bubble.inner_text()
        assert '**' not in visible and 'Optional specification catalog' not in visible
        assert len(visible.split())<=300
        # The producer may choose a relevant emoji; the shared policy keeps it
        # rather than forcing a second generic chart icon onto the title.
        assert any(ord(char)>=0x1F300 for char in visible)
        assert bubble.locator('li').count()>=3 and not errors
        history=page.request.get(base+'/api/chat/history/'+cid).json()['messages']
        assert [r['text'] for r in history if r['role']=='bot']==[body]
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    page.screenshot(path=str(out/'source-review-web.png'))
    (out/'source-review-web.json').write_text(json.dumps({'conversation_id':cid,'text':body,
        'source_calls':0,'ui_check_model_calls':0,'fixture_data':True},ensure_ascii=False,indent=2))
