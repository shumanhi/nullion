"""Opt-in rendering/attachment replay on live stage; no provider/account calls."""
import hashlib
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import APP,live_web

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1'
    or os.environ.get('NULLION_LIVE_REVIEW_LAYOUT_REPLAY')!='1',reason='Controlled live stage replay only')


def test_review_outage_notice_and_report_render_once_without_losing_attachment(live_web):
    from playwright.sync_api import expect
    page,frames,errors,database,cid,base=live_web
    fixture=APP/'.e2e-artifacts/social-review-20261106'
    record=json.loads((fixture/'stage-notice.json').read_text())
    content=(fixture/'delivered-report.html').read_bytes()
    path=Path(os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT'])/'controlled-report.html'
    path.write_bytes(content)
    for message in [
        {'role':'user','text':'Controlled replay of a report review outage; no account access or new model calls.'},
        {'role':'bot','text':record['caption'],'metadata':{'artifacts':[{'path':str(path)}]}},
        {'role':'bot','text':record['doctor_notice'],'metadata':{'source':'tracker_status'}}]:
        assert page.request.post(base+'/api/chat/history/'+cid+'/message',data=message).ok
    for _ in range(2):
        page.reload(wait_until='domcontentloaded')
        bubbles=page.locator('#messages .msg.bot .bubble')
        expect(bubbles).to_have_count(2)
        expect(bubbles.first).to_contain_text('completion remains unconfirmed')
        expect(bubbles.last).to_contain_text('final review timed out')
        expect(bubbles.last).to_contain_text('latest result was delivered')
        assert 'source evidence could not be verified' not in bubbles.last.inner_text()
        history=page.request.get(base+'/api/chat/history/'+cid).json()['messages']
        reports=[row for row in history if row.get('artifacts')]
        assert len(reports)==1 and len(reports[0]['artifacts'])==1
        url=reports[0]['artifacts'][0]['url']
        assert page.request.get(base+url if url.startswith('/') else url).body()==content
        expect(bubbles.first.locator('a[href="'+url+'"]')).to_have_count(1)
        assert not errors
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    page.locator('#messages').screenshot(path=str(out/'report-review-outage-web.png'))
    (out/'report-review-outage-web.json').write_text(json.dumps({'conversation_id':cid,'fixture_data':True,
        'ui_model_calls':0,'source_calls':0,'sha256':hashlib.sha256(content).hexdigest(),
        'caption':record['caption'],'doctor_notice':record['doctor_notice'],'attachments':1},indent=2))
