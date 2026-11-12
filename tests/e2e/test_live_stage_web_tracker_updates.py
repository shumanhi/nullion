"""Real stage UI: a changed filter controls continuing, deduplicated alerts."""
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue, _turns
from tests.e2e.test_live_stage_live_tracking import _tracking_evidence, _stop_owned_tracker

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in real stage UI')


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_NEWS_SOURCE'),reason='Controlled external source required')
def test_live_web_amended_filter_continues_for_new_matches_without_duplicate_alerts(live_web,record_property):
    from playwright.sync_api import expect
    page,frames,errors,database,conversation,base=live_web
    owner=uuid4().hex;name='Continuing filter '+owner[:8]
    source=os.environ['NULLION_LIVE_NEWS_SOURCE'].rstrip('/')
    url=source+'/?observation_owner='+owner
    def control(action,**data):
        response=page.request.post(source+'/'+action+'?owner='+owner,data=data)
        assert response.ok,response.text()
    def evidence():return _tracking_evidence(database,name)
    def wait(predicate,seconds=75):
        end=time.monotonic()+seconds
        while time.monotonic()<end:
            value=evidence()
            if predicate(value):return value
            page.wait_for_timeout(500)
        raise AssertionError(evidence())
    def matched(value):return [n for n in value['notices'] if n[0]=='matched' and n[1]=='sent']
    try:
        control('reset')
        reply,created,setup_ms=_submit(live_web,
            f'Create a live tracker named "{name}" for new headlines in the News section of {url}. '
            'Check every 10 seconds for 10 minutes and keep monitoring after each match until that deadline. '
            'Use semantic snapshot comparison, with the initial reading as a baseline; ignore timestamps. '
            'Prepare this read-only source and create it now; use no recurring cron.',timeout=240)
        data=wait(lambda d:len(d['jobs'])==1 and d['jobs'][0]['checks']>=1)
        before=data['jobs'][0]
        assert before['state']=='active' and before['stop_after_match'] is False,(reply,before)
        reply,updated,amend_ms=_submit(live_web,
            'Cambia únicamente la condición de este mismo seguimiento: avisa solo de noticias nuevas sobre '
            'mejoras del paseo marítimo, no de otros temas. Conserva la fuente, la frecuencia, la fecha límite '
            'y la comparación de instantáneas. Aplica el cambio ahora sin recrearlo.',timeout=150)
        data=wait(lambda d:d['jobs'][0]['condition_revision']==1)
        after=data['jobs'][0]
        assert after['id']==before['id'] and after['expires_at']==before['expires_at']
        assert after['interval_seconds']==before['interval_seconds']==10 and after['stop_after_match'] is False
        results=updated.get('tool_results',[])
        assert any(r['tool_name']=='update_live_tracker' and r['status']=='completed' for r in results),updated
        assert not any(r['tool_name'] in {'create_live_tracker','stop_live_tracker','create_cron'} for r in results),updated
        # Complete the new condition baseline before changing the source.
        data=wait(lambda d:d['jobs'][0]['checks']>after['checks'] and d['jobs'][0]['last_outcome']=='no_match')
        assert not matched(data),data
        control('publish',title='School chess club wins regional tournament',url='/news/chess-'+owner)
        checks=data['jobs'][0]['checks']
        data=wait(lambda d:d['jobs'][0]['checks']>checks and d['jobs'][0]['last_outcome']=='no_match')
        assert not matched(data),data
        control('publish',title='Waterfront promenade gains accessible ramps',url='/news/ramps-'+owner)
        data=wait(lambda d:len(matched(d))==1)
        assert data['jobs'][0]['state']=='active' and not data['jobs'][0].get('extension_offer_id'),data
        expect(page.locator('#messages')).to_contain_text('accessible ramps',timeout=15000)
        checks=data['jobs'][0]['checks']
        data=wait(lambda d:d['jobs'][0]['checks']>checks and d['jobs'][0]['last_outcome']=='no_match')
        assert len(matched(data))==1,data
        control('publish',title='Waterfront promenade opens a new public garden',url='/news/garden-'+owner)
        data=wait(lambda d:len(matched(d))==2)
        assert data['jobs'][0]['state']=='active' and data['jobs'][0]['expires_at']==before['expires_at'],data
        texts=[n[2] for n in matched(data)]
        assert sum('/news/ramps-'+owner in t for t in texts)==1,texts
        assert sum('/news/garden-'+owner in t for t in texts)==1,texts
        assert not any('/news/chess-'+owner in t for t in texts),texts
        expect(page.locator('#messages')).to_contain_text('public garden',timeout=15000)
        # Reload must show both deliveries, each once, without another model turn.
        turn_ids=[t['turn_id'] for t in _turns(database,conversation)]
        page.reload(wait_until='domcontentloaded')
        expect(page.locator('#messages .msg.bot .bubble').filter(has=page.locator('a[href*="/news/ramps-'+owner+'"]'))).to_have_count(1)
        expect(page.locator('#messages .msg.bot .bubble').filter(has=page.locator('a[href*="/news/garden-'+owner+'"]'))).to_have_count(1)
        assert [t['turn_id'] for t in _turns(database,conversation)]==turn_ids
        record_property('tracker_setup_ms',setup_ms)
        record_property('tracker_amendment_ms',amend_ms)
        assert setup_ms<120000 and amend_ms<90000,(setup_ms,amend_ms)
        assert not errors,errors
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'continuing-filter';out.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(out/'web.png'))
        (out/'result.json').write_text(json.dumps({'tracking':data,'turns':_turns(database,conversation)},ensure_ascii=False,indent=2))
    finally:
        _stop_owned_tracker(base,database,name,conversation)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_SNAPSHOT_REPLAY_NAME'),reason='Owned interrupted replay required')
def test_live_web_saved_filter_replay_finishes_alert_delivery_without_recreating(live_web):
    """Recover the visible acceptance after transport interruption; no new chat."""
    from urllib.parse import parse_qs,urlsplit
    from playwright.sync_api import expect
    page,frames,errors,database,conversation,base=live_web
    name=os.environ['NULLION_LIVE_SNAPSHOT_REPLAY_NAME']
    assert name.startswith('Continuing filter ')
    def evidence():return _tracking_evidence(database,name)
    data=evidence();assert len(data['jobs'])==1,data
    job=data['jobs'][0];cid=job['conversation_id']
    assert cid.startswith('web:live-ui-audit:') and job['state']=='active'
    assert job['condition_revision']==1 and job['stop_after_match'] is False
    turns=_turns(database,cid)
    def configuration(tool):
        return next(r['output']['deferred_execution']['configuration'] for t in turns for r in t.get('tool_results',[])
            if r['tool_name']==tool and r['status']=='completed')
    original=configuration('create_live_tracker');updated=configuration('update_live_tracker')
    assert original['matcher']!=updated['matcher']==job['matcher']
    assert original['expires_at']==updated['expires_at']==job['expires_at']
    assert original['interval_seconds']==updated['interval_seconds']==job['interval_seconds']==10
    source_url=job['probe'][0]['arguments']['url'];parts=urlsplit(source_url)
    owner=parse_qs(parts.query)['observation_owner'][0];source=parts.scheme+'://'+parts.netloc
    page=page.context.browser.new_page()
    page.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
    page.goto(base,wait_until='domcontentloaded')
    try:
        expect(page.locator('#messages')).to_contain_text('accessible ramps',timeout=15000)
        response=page.request.post(source+'/publish?owner='+owner,data={
            'title':'Waterfront promenade opens a new public garden','url':'/news/garden-'+owner})
        assert response.ok
        end=time.monotonic()+75
        while time.monotonic()<end:
            data=evidence();notices=[n for n in data['notices'] if n[0]=='matched' and n[1]=='sent']
            if len(notices)==2:break
            page.wait_for_timeout(500)
        assert len(notices)==2,data
        text='\n'.join(n[2] for n in notices)
        for title,path in [('accessible ramps','/news/ramps-'),('public garden','/news/garden-')]:
            assert title in text and path+owner in text,text
        assert '/news/chess-'+owner not in text and data['jobs'][0]['state']=='active'
        assert data['jobs'][0]['expires_at']==original['expires_at']
        page.reload(wait_until='domcontentloaded')
        for path in ['/news/ramps-','/news/garden-']:
            expect(page.locator('#messages .msg.bot .bubble').filter(has=page.locator('a[href*="'+path+owner+'"]'))).to_have_count(1)
        assert [t['turn_id'] for t in _turns(database,cid)]==[t['turn_id'] for t in turns]
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'recovered-filter';out.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(out/'web.png'))
        (out/'result.json').write_text(json.dumps({'tracking':data,'turns':turns,'new_chat_requests':0},ensure_ascii=False,indent=2))
    finally:
        _stop_owned_tracker(base,database,name)
        page.close()
