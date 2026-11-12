"""Real Web setup and alert for changed existing values, with a controlled source."""
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit
from tests.e2e.test_live_stage_live_tracking import _tracking_evidence, _stop_owned_tracker

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in stage Web')


def test_live_stage_web_snapshot_tracker_ignores_clock_and_alerts_on_changed_total(live_web):
    page, frames, errors, database, conversation, base = live_web
    source = os.environ.get('NULLION_LIVE_BOUNDED_COMPARISON_SOURCE')
    if not source:pytest.skip('Disposable source is required')
    owner = uuid4().hex; name = 'Snapshot comparison ' + owner[:8]
    url = source + '/state-watch/?owner=' + owner
    def evidence():return _tracking_evidence(database, name)
    def wait_for(predicate, seconds=60):
        end = time.monotonic()+seconds
        while time.monotonic()<end:
            data=evidence()
            if predicate(data):return data
            page.wait_for_timeout(700)
        raise AssertionError(evidence())
    try:
        reply, turn, _ = _submit(live_web,
            f'Start a live tracker named "{name}" at {url} . Check every 5 seconds for 4 minutes. '
            'Alert when the total vote count changes; ignore the Updated clock. Read only the complete #results region. '
            'Use a semantic comparison of successive saved observations of that text. Establish the current reading '
            'as a baseline and do not alert for the initial value or clock-only changes. '
            'Keep reporting new changes until the deadline, without stopping after a match. '
            'Refresh this ordinary non-streaming page in the same worker tab each check. Create it now.', timeout=240)
        data=wait_for(lambda d:len(d['jobs'])==1 and d['jobs'][0]['checks']>=1)
        job=data['jobs'][0]
        assert job['matcher']['history_mode']=='snapshot',job
        assert job['stop_after_match'] is False,job
        assert job['state']=='active' and job['last_outcome']=='no_match' and job['consecutive_errors']==0,job
        assert job['comparison_snapshot']['outputs'] and not any(n[0]=='matched' for n in data['notices']),data
        before=job['checks']
        assert page.request.get(source+'/state-watch/data?owner='+owner).ok
        data=wait_for(lambda d:d['jobs'][0]['checks']>before)
        assert data['jobs'][0]['state']=='active' and not any(n[0]=='matched' for n in data['notices']),data
        changed=page.request.post(source+'/state-watch/advance?owner='+owner)
        assert changed.ok
        def sent(d):return [n for n in d['notices'] if n[0]=='matched' and n[1]=='sent']
        data=wait_for(lambda d:len(sent(d))==1)
        assert data['jobs'][0]['state']=='active' and '11' in sent(data)[0][2],data
        from playwright.sync_api import expect
        # Stored notices contain Markdown; assert rendered content and links,
        # never literal ** delimiters that the renderer correctly removes.
        alerts=page.locator('#messages .msg.bot .bubble').filter(has=page.locator('a[href="'+url+'"]'))
        expect(alerts).to_have_count(1,timeout=10000)
        expect(alerts.first).to_contain_text(name)
        expect(alerts.first).to_contain_text('11')
        assert page.request.post(source+'/state-watch/advance?owner='+owner).ok
        data=wait_for(lambda d:len(sent(d))==2)
        assert data['jobs'][0]['state']=='active' and any('12' in n[2] for n in sent(data)),data
        before=data['jobs'][0]['checks']
        data=wait_for(lambda d:d['jobs'][0]['checks']>before)
        assert len(sent(data))==2 and data['jobs'][0]['state']=='active',data
        expect(alerts).to_have_count(2,timeout=10000)
        expect(alerts.last).to_contain_text('12')
        assert not data['jobs'][0].get('extension_offer_id'),data
        page.reload(wait_until='domcontentloaded')
        expect(alerts).to_have_count(2,timeout=15000)
        assert not errors
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'snapshot-tracker-result.json'
        out.write_text(json.dumps({'conversation_id':conversation,'setup':turn,'evidence':data,
            'visible_notices':[n[2] for n in sent(data)]},ensure_ascii=False,indent=2))
    finally:
        _stop_owned_tracker(base,database,name,conversation)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_SNAPSHOT_REPLAY_NAME'), reason='An owned delivered snapshot alert is required')
def test_live_stage_web_snapshot_alert_survives_reload():
    from playwright.sync_api import sync_playwright, expect
    name=os.environ['NULLION_LIVE_SNAPSHOT_REPLAY_NAME']
    data=_tracking_evidence(Path(os.environ['NULLION_LIVE_RUNTIME_DB']),name)
    assert len(data['jobs'])==1
    job=data['jobs'][0]
    assert job['state'] in {'matched','active','stopped'} and job['notification_status']=='sent'
    text=next(n[2] for n in data['notices'] if n[0]=='matched' and n[1]=='sent')
    assert text.strip()
    cid=job['conversation_id'];assert cid.startswith('web:live-ui-audit:')
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        context=browser.new_context()
        context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');"
            "localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
        page=context.new_page()
        try:
            from playwright.sync_api import Error
            deadline=time.monotonic()+20
            while True:
                try:
                    page.goto(os.environ['NULLION_LIVE_BASE_URL'],wait_until='domcontentloaded')
                    break
                except Error:
                    if time.monotonic()>=deadline:raise
                    page.wait_for_timeout(250)  # Retry a read only; never submit chat again.
            # Stored notices are Markdown; visible HTML must not contain its
            # literal delimiters. Bind to the complete saved notice instead.
            page.wait_for_function('text => Array.from(document.querySelectorAll("#messages .msg.bot .bubble"))'
                '.filter(b => (b.dataset.rawText || "").trim() === text.trim()).length === 1',arg=text,timeout=20000)
            notices=page.locator('#messages .msg.bot .bubble')
            index=notices.evaluate_all('(rows,text) => rows.findIndex(b => (b.dataset.rawText || "").trim() === text.trim())',text)
            bubble=notices.nth(index)
            expect(bubble).to_be_visible()
            expect(bubble).to_contain_text(name)
            expect(bubble).not_to_contain_text('**')
            assert bubble.locator('a[href]').count()>0
            out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR']);out.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(out/'snapshot-alert-reloaded.png'))
            (out/'snapshot-alert-reloaded.json').write_text(json.dumps(data,ensure_ascii=False,indent=2))
        finally:browser.close()


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_SNAPSHOT_PUBLIC_SOURCE'), reason='Explicit public incident source required')
def test_live_stage_web_snapshot_tracker_checks_the_public_incident_source(live_web):
    page, frames, errors, database, conversation, base=live_web
    source=os.environ['NULLION_LIVE_SNAPSHOT_PUBLIC_SOURCE']
    ready_selector=os.environ.get('NULLION_LIVE_SNAPSHOT_READY_SELECTOR')
    readiness_context=(f'A prior rendered-source inspection verified the actual results landmark selector {json.dumps(ready_selector)}; '
        'use that observed selector as ready_selector, not navigation text.' if ready_selector else '')
    name='Public results snapshot '+uuid4().hex[:8]
    def evidence():return _tracking_evidence(database,name)
    try:
        response, turn, _=_submit(live_web,
            f'Create a temporary live tracker named "{name}" using {source} . '
            'Check every 30 seconds for 4 minutes. Monitor substantive election-result changes: '
            'new vote counts, leads, race calls, winners, or chamber totals. Ignore clocks, ads and navigation. '
            'Use complete scoped results text with semantic comparison against the saved previous observation. '
            'Prepare an observed stable results-readiness landmark through the source tool readiness parameter so a fresh worker waits for rendered results. '
            + readiness_context + ' '
            'The first reading is a baseline, not an alert. Deliver only in this Web chat.',timeout=240)
        if not evidence()['jobs']:
            from nullion.chat_operator import selected_numbered_option_context
            confirmation=('1' if selected_numbered_option_context('1',response['text']) else
                f'I confirm creation of "{name}" with the source, frequency, duration and condition just specified. Create it now.')
            response,turn,_=_submit(live_web,confirmation,timeout=240)
        assert evidence()['jobs'],response
        end=time.monotonic()+70
        while time.monotonic()<end:
            data=evidence()
            if data['jobs'] and data['jobs'][0]['checks']>=1:break
            page.wait_for_timeout(700)
        assert data['jobs'],response
        job=data['jobs'][0]
        assert job['matcher']['history_mode']=='snapshot',job
        assert job['checks']>=1 and job['last_outcome'] in {'matched','no_match'},job
        assert job['consecutive_errors']==0 and job['comparison_snapshot']['outputs'],job
        assert not any(p['tool_name']=='browser_find' for p in job['probe']),job
        if job['state']=='matched':
            delivery_end=time.monotonic()+25
            while job['notification_status']!='sent' and time.monotonic()<delivery_end:
                page.wait_for_timeout(300);data=evidence();job=data['jobs'][0]
            assert job['notification_status']=='sent',data
            notice=next(n[2] for n in data['notices'] if n[0]=='matched' and n[1]=='sent')
            from playwright.sync_api import expect
            page.wait_for_function('text => Array.from(document.querySelectorAll("#messages .msg.bot .bubble"))'
                '.some(b => (b.dataset.rawText || "").trim() === text.trim())',arg=notice,timeout=10000)
        assert not errors
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'public-snapshot-result.json'
        out.write_text(json.dumps({'conversation_id':conversation,'setup':turn,'evidence':data},ensure_ascii=False,indent=2))
    finally:
        _stop_owned_tracker(base,database,name,conversation)
