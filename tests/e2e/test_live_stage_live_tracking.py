"""Opt-in real-provider stage Web UI acceptance, with durable observation evidence.

Run in the E2E container against a local stage-only forwarding URL on port 8753.
No API chat calls substitute for the browser conversation.
"""

import json
import os
import re
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web

pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_LIVE_STAGE_E2E") != "1",
    reason="Authorized live stage UI only",
)


def _controlled_source_preparation():
    if not os.environ.get('NULLION_LIVE_NEWS_SOURCE'):
        return ''
    return (' This controlled local fixture is served to the configured local browser. '
        'Prepare it with browser_navigate and browser_extract_items directly, without browser_run_task. '
        'Save the exact prepared extraction as the observation probe; its adapter-provided '
        'observation_setup supplies navigation when polling. Do not disable network security settings.')


def _fixture_source_page_ids(endpoint, sources):
    from urllib.request import Request,urlopen
    # The explicitly selected test CDP proxy expects an IP Host header.
    request=Request(endpoint.rstrip('/')+'/json/list',headers={'Host':'127.0.0.1'})
    with urlopen(request,timeout=5) as response: targets=json.load(response)
    expected={urlsplit(source)[:4] for source in sources}
    return {row['id'] for row in targets if row.get('type')=='page'
            and urlsplit(row.get('url',''))[:4] in expected}


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_NEWS_SOURCE'),reason='Controlled external source required')
def test_live_stage_web_any_finite_window_and_same_tracker_amendments(live_web):
    from datetime import datetime, timedelta, timezone
    from tests.e2e.test_live_stage_web_matrix import _submit,catalogue,_turns
    page,frames,errors,database,conversation,base=live_web
    name='Finite window '+uuid4().hex[:8]
    source=os.environ['NULLION_LIVE_NEWS_SOURCE'].rstrip('/')+'/?observation_owner='+uuid4().hex
    marker='WINDOW_CANARY_'+uuid4().hex
    def owned():return [r for r in catalogue._get_json(base+'/api/live-trackers',timeout=15)['trackers'] if r['name']==name]
    def receipt(turn,tool):
        assert turn.get('response_fulfilled') is True,turn
        rows=[r for r in turn.get('tool_results',[]) if r['tool_name']==tool and r['status']=='completed']
        assert len(rows)==1,turn
        return rows[0]['output']['deferred_execution']['configuration']
    try:
        first,turn,_=_submit(live_web,f'Start a finite live tracker named "{name}". Monitor the News section of {source} '
            f'for a headline containing the exact marker {marker}. Check every 15 minutes for 14 days. '
            'Prepare the source and create it now. Do not use a recurring cron.'+_controlled_source_preparation(),timeout=240)
        jobs=owned();assert len(jobs)==1 and jobs[0]['state']=='active',(first,jobs)
        ident=jobs[0]['id'];config=receipt(turn,'create_live_tracker')
        assert config['duration_seconds']==14*86400 and config['interval_seconds']==900
        assert datetime.fromisoformat(config['expires_at'])-datetime.fromisoformat(config['accepted_at'])==timedelta(days=14)
        end=datetime.now(timezone.utc)+timedelta(days=30)
        second,changed,_=_submit(live_web,'Keep this same tracker, source, condition and frequency. Change only its end time to '+end.isoformat()+'. Apply the change now.',timeout=120)
        config=receipt(changed,'update_live_tracker')
        assert datetime.fromisoformat(config['expires_at'])==end and config['deadline_kind']=='absolute'
        third,corrected,_=_submit(live_web,'Mantén el mismo seguimiento y la frecuencia. Cambia la ventana a tres horas completas desde ahora; aplica el cambio.',timeout=120)
        config=receipt(corrected,'update_live_tracker')
        assert config['duration_seconds']==10800
        assert datetime.fromisoformat(config['expires_at'])-datetime.fromisoformat(config['accepted_at'])==timedelta(hours=3)
        assert len(owned())==1 and owned()[0]['id']==ident and owned()[0]['interval_seconds']==900
        for update in (changed,corrected):
            assert not any(r['tool_name'].startswith('browser_') or r['tool_name'] in {'web_fetch','web_search','create_live_tracker','stop_live_tracker','create_cron'} for r in update.get('tool_results',[])),update
        reviews=catalogue._conversation_event_payloads(database,conversation,event_type='conversation.completion_review')
        assert reviews and reviews[-1]['disposition']=='complete',reviews
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'finite-window-contract.json'
        out.write_text(json.dumps({'tracking':_tracking_evidence(database,name),'turns':_turns(database,conversation)},ensure_ascii=False,indent=2))
        assert not errors
    finally:
        for job in owned():catalogue._post_json(base+'/api/live-trackers/'+job['id']+'/stop',{},timeout=30)


def _open_tracking_page(base):
    from tests.e2e.test_web_surface_contracts import _open_page
    frames=[]
    def configure(page):
        def received(raw):
            try:frames.append(json.loads(raw))
            except (ValueError,TypeError):pass
        page.on('websocket',lambda socket:socket.on('framereceived',received))
        def response(item):
            if item.request.method=='POST' and item.url==base+'/api/chat':
                value=item.json();frames.append({**value,'type':'done' if value.get('type')=='message' else value.get('type')})
        page.on('response',response)
    return (*_open_page(base,configure_page=configure),frames)


def _submit_tracker_creation(page,frames,errors,database,base,prompt,jobs,*,timeout=120):
    from tests.e2e.test_live_stage_web_matrix import _submit
    cid=page.evaluate("localStorage.getItem('nullion_conv_id')");context=(page,frames,errors,database,cid,base)
    response,turn,_=_submit(context,prompt,timeout=timeout)
    if not jobs():
        # Complete a real numbered clarification rather than treating that
        # intermediate reply as terminal creation. Verify the stored job below.
        options=re.findall(r'(?m)^\s*(\d+)[.)]\s+',response['text'])
        assert options and options[0]=='1',response
        _,turn,_=_submit(context,'1',timeout=timeout)
    assert jobs(),'Creation did not reach durable tracker state'
    assert turn.get('response_fulfilled') is True,turn


def _tracking_evidence(database,name):
    connection=os.environ.get("NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION")
    if connection:
        from urllib.request import Request,urlopen
        from urllib.parse import urlencode
        settings=json.loads(Path(connection).read_text())
        request=Request("http://host.docker.internal:"+str(settings['port'])+"/tracking?"+
            urlencode({'name':name}),headers={'Authorization':'Bearer '+settings['token']})
        from scripts.live_stage_http import read_json
        return read_json(request,timeout=3,opener=urlopen)
    with sqlite3.connect(database.resolve().as_uri()+"?mode=ro",uri=True,timeout=10) as db:
        jobs=[json.loads(row[0]) for row in db.execute("SELECT payload FROM live_trackers WHERE json_extract(payload,'$.name')=?",(name,))]
        notices=[];events=[]
        for job in jobs:
            notices.extend(db.execute('SELECT kind,state,text FROM live_tracker_notifications WHERE tracker_id=?',(job['id'],)).fetchall())
            events.extend(json.loads(row[0]) for row in db.execute("SELECT payload FROM runtime_events WHERE json_extract(payload,'$.payload.capsule_id')=?",('live-tracker:'+job['id'],)))
    return {'jobs':jobs,'notices':notices,'events':events}


def _stop_owned_tracker(base,database,name,conversation_id=None):
    import urllib.request
    from urllib.error import URLError
    try:
        jobs = _tracking_evidence(database,name)['jobs']
    except (URLError, OSError):
        # A dead read-only bridge must not leave the accepted source polling.
        # The live API still identifies the exact fixture owner; never stop
        # unrelated trackers or infer cancellation from a disconnected tab.
        from tests.e2e.test_live_stage_web_matrix import catalogue
        jobs = [job for job in catalogue._get_json(base+'/api/live-trackers',timeout=10)['trackers']
            if job.get('name')==name and (conversation_id is None or job.get('conversation_id')==conversation_id)]
    for job in jobs:
        assert job['name']==name
        if conversation_id is not None:
            assert job['conversation_id']==conversation_id
        if job.get('state') in {'active','paused','blocked'}:
            request=urllib.request.Request(base+"/api/live-trackers/"+job['id']+"/stop",data=b"{}",
                headers={"Content-Type":"application/json"},method="POST")
            with urllib.request.urlopen(request,timeout=15) as response:assert json.load(response).get('ok')


def _record_tracking_evidence(page,database,name):
    conversation=page.evaluate("localStorage.getItem('nullion_conv_id')")
    output=Path(os.environ.get("NULLION_LIVE_EVIDENCE_DIR","/tmp/live-tracking-evidence"))/('tracker-'+name.replace(' ','-'))
    output.mkdir(parents=True,exist_ok=True,mode=0o700)
    page.screenshot(path=str(output/'web.png'),full_page=True)
    data={'conversation_id':conversation,'tracker_name':name,'visible_text':page.locator('#messages').inner_text()}
    try:
        rows=_tracking_evidence(database,name)['jobs']
        data['jobs']=[{key:j.get(key) for key in ('id','name','state','checks','consecutive_errors','last_error','notification_status','expires_at')} for j in rows]
    except (sqlite3.DatabaseError,OSError) as exc:data['read_error']=type(exc).__name__
    (output/'result.json').write_text(json.dumps(data,ensure_ascii=False,indent=2))
    for path in output.iterdir():path.chmod(0o600)
    return conversation



def _submit_tracking_message(page, database, base, prompt, timeout=120):
    """Wait for this request's terminal receipt, never a prior numbered offer."""
    from tests.e2e.test_live_stage_web_matrix import _turns
    from playwright.sync_api import expect
    conversation=page.evaluate("localStorage.getItem('nullion_conv_id')")
    page.locator('#user-input').fill(prompt);page.locator('#user-input').press('Enter')
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        rows=[row for row in _turns(database,conversation) if row.get('user_message')==prompt]
        if rows:
            turn=rows[-1]
            bubble=page.locator('.msg.bot .bubble[data-turn-id='+json.dumps(turn['turn_id'])+']')
            expect(bubble).to_be_visible(timeout=10000)
            assert str(turn.get('assistant_reply') or '').strip(),turn
            return turn
        page.wait_for_timeout(300)
    raise AssertionError('No terminal receipt for the new tracking request')


def test_live_stage_web_temporary_tracking_and_numbered_clarification():
    from playwright.sync_api import expect
    from tests.e2e.test_web_surface_contracts import _open_page

    base = os.environ.get("NULLION_LIVE_BASE_URL", "http://127.0.0.1:8753")
    assert urlsplit(base).port == 8753, "This test must never target production"
    database = Path(
        os.environ.get(
            "NULLION_LIVE_RUNTIME_DB", str(Path.home() / ".nullion-stage/runtime.db")
        )
    )
    name = "Observation " + uuid4().hex[:8]

    def jobs():
        return _tracking_evidence(database,name)['jobs']

    before = {j["id"] for j in jobs()}
    playwright, browser, page, errors, frames = _open_tracking_page(base)
    try:
        prompt = (
            f"Create a temporary live tracker named {name} for 90 seconds, checking every 5 seconds. "
            "Alert me once the current temperature in London is above minus 100 degrees Celsius. "
            "Verify the source first and stop after the first matching observation."
        )
        _submit_tracker_creation(page,frames,errors,database,base,prompt,jobs)
        expect(page.locator("#messages")).to_contain_text(
            "Live tracker", timeout=120000
        )
        expect(page.locator("#messages")).to_contain_text(
            "Tracking has stopped.", timeout=100000
        )
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            new = [j for j in jobs() if j["id"] not in before and j.get("name")==name]
            if new and new[0]["notification_status"]=="sent":break
            time.sleep(.1)
        assert (
            len(new) == 1
            and new[0]["state"] == "matched"
            and new[0]["notification_status"] == "sent"
        ), new
        assert new[0]["interval_seconds"] == 5 and new[0]["checks"] >= 1
        assert new[0]["matcher"]["kind"] == "structured"
        conversation = page.evaluate("localStorage.getItem('nullion_conv_id')")
        # The job/notice ledger commits before the secondary runtime audit.
        # Wait for its bounded deferred checkpoint without rerunning the probe.
        audit_deadline=time.monotonic()+30
        while True:
            events=_tracking_evidence(database,name)['events']
            calls=[r for r in events if r["event_type"]=="tool.invocation.completed"
                and r["payload"].get("capsule_id")=="live-tracker:"+new[0]["id"]]
            if calls or time.monotonic()>=audit_deadline:break
            page.wait_for_timeout(200)
        assert calls and all(
            r["payload"]["tool_name"] in [s["tool_name"] for s in new[0]["probe"]]
            for r in calls
        )
        assert new[0]["conversation_id"] == conversation
        original_count = len(page.request.get(base+'/api/live-trackers').json()['trackers'])
        clarification=_submit_tracking_message(page,database,base,
            "Track the current London temperature and alert me when it changes. "
            "I have not decided whether this should be temporary or recurring, or how long it should last. "
            "Ask me to choose before creating anything.")
        import re
        options=re.findall(r"(?m)^\s*(\d+)[.)]\s+",clarification['assistant_reply'])
        assert options[:2]==['1','2'],clarification
        assert not any(r['tool_name']=='create_live_tracker' and r['status']=='completed'
                       for r in clarification.get('tool_results',[])),clarification
        assert len(page.request.get(base+'/api/live-trackers').json()['trackers']) == original_count
        assert not errors, errors
        output = Path(
            os.environ.get("NULLION_LIVE_EVIDENCE_DIR", "/tmp/live-tracking-evidence")
        )
        output.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(output / "live-tracker-stage.png"), full_page=True)
        (output / "live-tracker-stage.json").write_text(
            json.dumps(
                {
                    "tracker_id": new[0]["id"],
                    "state": new[0]["state"],
                    "checks": new[0]["checks"],
                    "source_calls": len(calls),
                },
                indent=2,
            )
        )
    finally:
        try:
            try:_record_tracking_evidence(page,database,name)
            finally:_stop_owned_tracker(base,database,name)
        finally:
            browser.close();playwright.stop()


def test_live_stage_cnn_developing_section_delivers_one_grounded_match():
    """Real configured model and public page; positive alert, not just polling."""
    from playwright.sync_api import expect
    from tests.e2e.test_web_surface_contracts import _open_page
    base=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753')
    assert urlsplit(base).port==8753, 'Never run this acceptance against production'
    database=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage/runtime.db')))
    name='CNN section '+uuid4().hex[:8]
    pw,browser,page,errors,frames=_open_tracking_page(base)
    try:
        prompt=(
            f'Create a temporary live tracker named {name} for 90 seconds with 5-second checks. '
            'Watch https://www.cnn.com and alert once for an article in its Developing section '
            'other than the baseline Faith Hedgepeth verdict story at '
            'https://www.cnn.com/2026/10/23/us/faith-hedgepeth-murder-trial-verdict. '
            'Section membership qualifies even if the headline does not repeat the label. '
            'Verify the section and direct article URLs before starting. Include the headline and URL, then stop.')
        _submit_tracker_creation(page,frames,errors,database,base,prompt,lambda:_tracking_evidence(database,name)['jobs'],timeout=240)
        expect(page.locator('#messages')).to_contain_text('Live tracker: '+name,timeout=150000)
        expect(page.locator('#messages')).to_contain_text('Tracking has stopped.',timeout=20000)
        deadline=time.monotonic()+10
        while True:
            evidence=_tracking_evidence(database,name);jobs=evidence['jobs'];notices=evidence['notices']
            job=next(j for j in jobs if j['name']==name)
            if job['notification_status']=='sent' and notices and notices[0][1]=='sent':break
            assert time.monotonic()<deadline, (job,notices)
            page.wait_for_timeout(100)
        assert job['state']=='matched' and job['notification_status']=='sent' and job['consecutive_errors']==0
        coverage=job['last_evaluation']['source_coverage']
        assert coverage and any(c['coverage']['sections'] for c in coverage)
        assert len(notices)==1 and tuple(notices[0][0:2])==('matched','sent')
        assert 'https://www.cnn.com/' in notices[0][2] and not errors, notices
        out=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-tracking-evidence'));out.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(out/'cnn-section-match.png'),full_page=True)
        (out/'cnn-section-match.json').write_text(json.dumps({'job':job,'notices':notices},indent=2))
    finally:
        try:
            try:_record_tracking_evidence(page,database,name)
            finally:_stop_owned_tracker(base,database,name)
        finally:
            browser.close();pw.stop()


def test_live_stage_web_expiry_followup_renews_saved_window():
    """Real-provider Web replay: expiry -> ordinary follow-up -> numeric choice."""
    from playwright.sync_api import expect
    from tests.e2e.test_web_surface_contracts import _open_page
    base=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753')
    assert urlsplit(base).port==8753
    database=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage/runtime.db')))
    name='Renewal '+uuid4().hex[:8]
    def job():
        return next(iter(_tracking_evidence(database,name)['jobs']),None)
    playwright,browser,page,errors,frames=_open_tracking_page(base)
    try:
        prompt=(
            f'Create a temporary live tracker named "{name}" for 30 seconds. Check the current temperature in London every 5 seconds, '
            'alert only above 1000 degrees Celsius. Verify the source first, start now, and notify me at expiry.')
        _submit_tracker_creation(page,frames,errors,database,base,prompt,lambda:[job()] if job() else [])
        expect(page.locator('#messages')).to_contain_text(f'Live tracker expired: {name}',timeout=150000)
        before=job();assert before['state']=='expired'
        question=(f'Continue the saved tracker {before["id"]}, but I have not selected the new duration. '
            'Ask me to choose before starting it. Offer exactly these numbered choices: '
            '1. Continue for 30 minutes. 2. Continue for 2 hours. 3. Keep it stopped.')
        page.locator('#user-input').fill(question)
        page.locator('#user-input').press('Enter')
        # An older expiration menu already contains numbered rows. Wait for
        # this actual UI turn to commit before sending a new numeric message.
        deadline=time.monotonic()+90
        turn=None
        while time.monotonic()<deadline:
            from tests.e2e.test_live_stage_web_matrix import _turns
            cid=page.evaluate("localStorage.getItem('nullion_conv_id')")
            rows=[row for row in _turns(database,cid) if row.get('user_message')==question]
            if rows:
                turn=rows[-1];break
            time.sleep(.2)
        assert turn and any(line.strip().startswith('1.') for line in turn['assistant_reply'].splitlines())
        bubble=page.locator('.msg.bot .bubble[data-turn-id='+json.dumps(turn['turn_id'])+']')
        expect(bubble).to_contain_text(before['id'],timeout=15000)
        # Ordered-list numbers are browser markers, not innerText characters.
        expect(bubble.locator('ol li')).to_have_count(3,timeout=15000)
        assert bubble.locator('ol li').first.get_attribute('value')=='1'
        assert job()['state']=='expired'
        page.locator('#user-input').fill('1')
        page.locator('#user-input').press('Enter')
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            active=job()
            if active['state']=='active' and active.get('extension_count',0)>before.get('extension_count',0):break
            time.sleep(.2)
        expect(page.locator('#messages')).to_contain_text('Checking every 5 seconds until',timeout=15000)
        active=job()
        assert active['id']==before['id'] and active['state']=='active'
        assert active['extension_count']==before.get('extension_count',0)+1
        from datetime import datetime
        assert active['deadline_kind']=='duration',active
        assert (datetime.fromisoformat(active['expires_at'])-
                datetime.fromisoformat(active['window_accepted_at'])).total_seconds()==1800,active
        assert active['probe']==before['probe'] and active['matcher']==before['matcher']
        page.locator('#user-input').fill(f'Stop the live tracker "{name}" now.')
        page.locator('#user-input').press('Enter')
        expect(page.locator('#messages')).to_contain_text(f'Live tracker “{name}” is stopped.',timeout=60000)
        assert job()['state']=='stopped' and not errors
    finally:
        try:
            try:_record_tracking_evidence(page,database,name)
            finally:_stop_owned_tracker(base,database,name)
        finally:
            browser.close();playwright.stop()


from tests.e2e.test_live_stage_web_matrix import live_web


@pytest.mark.parametrize('interval,followup',[(60,'Every minute'),(300,'Cada cinco minutos')])
def test_live_stage_web_cadence_clarification_uses_actual_tracker_contract(live_web,interval,followup):
    """Real model, Web composer, source preparation and durable tracker cadence."""
    from tests.e2e.test_live_stage_web_matrix import _submit,catalogue,_turns
    page,frames,errors,database,conversation,base=live_web
    name='Cadence '+uuid4().hex[:8]
    source=os.environ.get('NULLION_LIVE_NEWS_SOURCE','https://www.cnn.com').rstrip('/')
    section='News' if os.environ.get('NULLION_LIVE_NEWS_SOURCE') else 'Developing'
    marker='CANARY_'+uuid4().hex
    def owned():
        return [row for row in catalogue._get_json(base+'/api/live-trackers',timeout=15)['trackers'] if row['name']==name]
    try:
        reply,turn,_=_submit(live_web,
            f'I want a temporary live tracker named "{name}" for the {section} section at {source}. '
            f'Alert only if a headline contains the exact marker {marker}. '
            'Ask me to choose the check frequency first, then the tracking lifetime, using numbered options. '
            'I have not selected either yet. Do not open the source or create anything until I provide both.'
            +_controlled_source_preparation())
        assert '1.' in reply['text'] and not owned(),reply
        reply,turn,_=_submit(live_web,followup)
        assert '1.' in reply['text'] and not owned(),reply
        reply,turn,_=_submit(live_web,'For fifteen minutes. Start the prepared tracker now and keep the check frequency I selected.',timeout=240)
        jobs=owned();assert len(jobs)==1,(reply,jobs)
        assert jobs[0]['interval_seconds']==interval,(reply,jobs)
        assert jobs[0]['state']=='active',(reply,jobs)
        results=[r for event in _turns(database,conversation) for r in event.get('tool_results',[])]
        assert any(r['tool_name']=='create_live_tracker' and r['status']=='completed' for r in results),results
        assert not any(r['tool_name']=='create_cron' for r in results),results
        assert not errors
    finally:
        for job in owned():
            catalogue._post_json(base+'/api/live-trackers/'+job['id']+'/stop',{},timeout=30)


def test_live_stage_web_numeric_lifetime_and_cadence_creates_article_tracker(live_web):
    """The failed production reply shape selects both duration and cadence."""
    from datetime import datetime
    from tests.e2e.test_live_stage_web_matrix import _submit,catalogue,_turns
    page,frames,errors,database,conversation,base=live_web
    name='Article continuation '+uuid4().hex[:8]
    source=os.environ.get('NULLION_LIVE_NEWS_SOURCE','https://www.cnn.com').rstrip('/')
    def owned():
        return [row for row in catalogue._get_json(base+'/api/live-trackers',timeout=15)['trackers'] if row['name']==name]
    try:
        reply,turn,_=_submit(live_web,
            f'Alert me when there is any new article on {source}. Name this temporary live tracker "{name}". '
            'First ask me to choose its tracking lifetime with these numbered choices: 1. 30 minutes, 2. 1 hour, 3. 6 hours. '
            'I have not chosen the lifetime or frequency yet. Do not create anything until I answer. '
            'Existing headlines are the baseline; alert for a new article rather than an article already present. '
            'After I answer the numbered lifetime question, verify the source and start it without asking for a separate confirmation.'
            +_controlled_source_preparation())
        assert '3.' in reply['text'] and not owned(),reply
        reply,turn,_=_submit(live_web,'3 , check every 5 mins',timeout=300)
        jobs=owned();assert len(jobs)==1,(reply,jobs)
        job=jobs[0]
        assert job['interval_seconds']==300 and job['state']=='active',(reply,job)
        duration=(datetime.fromisoformat(job['expires_at'])-datetime.fromisoformat(job['created_at'])).total_seconds()
        assert 21595<=duration<=21605,job
        results=[r for event in _turns(database,conversation) for r in event.get('tool_results',[])]
        assert any(r['tool_name']=='create_live_tracker' and r['status']=='completed' for r in results),results
        assert not any(r['tool_name']=='create_cron' for r in results),results
        reads=[r['output'] for r in results if r['tool_name']=='browser_extract_items' and r['status']=='completed']
        assert any(r.get('item_count',0)>0 or r.get('source_identity_record_count',0)>0 for r in reads),reads
        deadline=time.monotonic()+45
        while True:
            job=owned()[0]
            if job['checks']>=1:break
            assert time.monotonic()<deadline,job
            page.wait_for_timeout(200)
        assert job['state']=='active' and job['last_outcome']=='no_match' and not job['last_problem'],job
        evidence={'tracking':_tracking_evidence(database,name),'turns':_turns(database,conversation)}
        saved=evidence['tracking']['jobs'][0]
        if saved['matcher']['kind']=='structured':
            assert saved['baseline'] and all(value not in (None,'',[],{}) for value in saved['baseline']),saved
        else:
            assert saved['observation_baseline_set'] and saved['seen_observation_keys'],saved
        out=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-tracking-evidence'));out.mkdir(parents=True,exist_ok=True)
        (out/'numeric-article-runtime.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2))
        assert not errors
    finally:
        for job in owned():catalogue._post_json(base+'/api/live-trackers/'+job['id']+'/stop',{},timeout=30)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_NEWS_SOURCE'),reason='Explicit controlled external news fixture required')
def test_live_stage_web_real_semantic_tracker_alerts_only_for_a_new_url(live_web):
    from playwright.sync_api import expect
    from tests.e2e.test_live_stage_web_matrix import _submit,catalogue,_turns
    page,frames,errors,database,conversation,base=live_web
    source=os.environ['NULLION_LIVE_NEWS_SOURCE'].rstrip('/')
    name='New URL live acceptance '+uuid4().hex[:8]
    observation_owner=uuid4().hex
    observation_url=source+'/?observation_owner='+observation_owner
    stats_url=source+'/stats?owner='+observation_owner
    cdp_endpoint=os.environ.get('NULLION_LIVE_OBSERVATION_CDP_URL')
    def owned_page_ids():
        if not cdp_endpoint:return None
        return _fixture_source_page_ids(cdp_endpoint,[observation_url])
    def control(action):
        response=page.request.post(source+'/'+action,data={});assert response.ok,response.text()
    def job():return next(iter(_tracking_evidence(database,name)['jobs']),None)
    try:
        control('reset')
        _submit(live_web,
            f'Create a temporary live tracker named "{name}" on {observation_url} for 150 seconds with 5-second checks. '
            'Use a bounded semantic condition over the complete article collection: alert only when a new article URL '
            'appears after the first poll, include its headline and direct URL, then stop. Existing article URLs are the baseline. '
            'An edited headline on the same URL must not trigger an alert. Prepare and verify the read, start now without another confirmation.'
            +_controlled_source_preparation())
        assert job(), 'No durable tracker was created'
        assert job()['matcher']['kind']=='semantic',job()
        deadline=time.monotonic()+50
        while job()['checks']<1:
            assert time.monotonic()<deadline,job();page.wait_for_timeout(200)
        first=job();assert first['state']=='active' and first['observation_baseline_set'] and len(first['seen_observation_keys'])==1,first
        initial_pages=owned_page_ids()
        control('clear-sessions');control('edit')
        deadline=time.monotonic()+45
        while job()['checks']<=first['checks']:
            assert time.monotonic()<deadline,job();page.wait_for_timeout(200)
        edited=job();assert edited['state']=='active' and edited['last_outcome']=='no_match' and not edited['last_problem'],edited
        # Chat setup owns a separate page from the background worker. Count
        # both, then require the total to stay fixed across another poll; a
        # fresh worker tab on every navigation would add a new session token.
        stats_deadline = time.monotonic() + (0 if cdp_endpoint else 5)
        while True:
            edit_stats=page.request.get(stats_url).json()
            if edit_stats['session_count'] or time.monotonic() >= stats_deadline: break
            page.wait_for_timeout(100)
        portable=any(context.get('observation_setup') for context in edited.get('probe_contexts',[]))
        if cdp_endpoint:
            assert 1 <= len(initial_pages) <= (2 if portable else 1),initial_pages
            assert owned_page_ids()==initial_pages,'A poll replaced the owned browser tab'
        else:
            assert 1 <= edit_stats['session_count'] <= (2 if portable else 1),edit_stats
        control('publish')
        expect(page.locator('#messages')).to_contain_text('New museum opens on waterfront',timeout=50000)
        # The Web message is already visible; allow the existing deferred
        # runtime checkpoint to finish before checking its durable receipt.
        deadline=time.monotonic()+45
        while job()['notification_status']!='sent':
            assert time.monotonic()<deadline,job();page.wait_for_timeout(200)
        saved=job();assert saved['state']=='matched' and len(saved['seen_observation_keys'])==2,saved
        assert saved['last_evaluation']['observation_history']['new_records']==1,saved
        evidence=_tracking_evidence(database,name)
        notices=[n for n in evidence['notices'] if n[0]=='matched']
        assert len(notices)==1 and notices[0][1]=='sent' and '/news/new-museum' in notices[0][2],notices
        stats=page.request.get(stats_url).json()
        if cdp_endpoint:
            # A terminal one-shot match may close its own worker tab.
            assert owned_page_ids() <= initial_pages
        else:
            assert stats['session_count']==edit_stats['session_count'],stats
        out=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-tracking-evidence'));out.mkdir(parents=True,exist_ok=True)
        (out/'real-semantic-news-runtime.json').write_text(json.dumps({'tracking':evidence,'turns':_turns(database,conversation),'source':stats,
            'owned_page_ids':sorted(initial_pages) if initial_pages is not None else None},ensure_ascii=False,indent=2))
        assert not errors
    finally:
        _stop_owned_tracker(base,database,name)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_NEWS_SOURCE') or not os.environ.get('NULLION_LIVE_OBSERVATION_CDP_URL'),
                   reason='Explicit controlled source and test-only CDP observation required')
def test_live_stage_two_source_tracker_does_not_substitute_the_last_page(live_web):
    from playwright.sync_api import expect
    from tests.e2e.test_live_stage_web_matrix import _submit,_turns
    page,frames,errors,database,conversation,base=live_web
    source=os.environ['NULLION_LIVE_NEWS_SOURCE'].rstrip('/')
    endpoint=os.environ['NULLION_LIVE_OBSERVATION_CDP_URL']
    owners=[uuid4().hex,uuid4().hex]
    urls=[source+'/?observation_owner='+owner for owner in owners]
    name='Independent sources '+uuid4().hex[:8]
    def control(action,index):
        response=page.request.post(source+'/'+action+'?owner='+owners[index],data={})
        assert response.ok,response.text()
    def evidence():return _tracking_evidence(database,name)
    def job():return next(iter(evidence()['jobs']),None)
    def wait_checks(count):
        deadline=time.monotonic()+45
        while job()['checks']<count:
            assert time.monotonic()<deadline,job()
            page.wait_for_timeout(200)
    try:
        for index in range(2):control('reset',index)
        _submit(live_web,
            f'Create a temporary tracker named "{name}" for these two independent feeds: {urls[0]} and {urls[1]}. '
            'Run for four minutes, check every five seconds, and stop after the first match. '
            'Alert only when BOTH News sections contain at least two article records. Either feed alone is insufficient. '
            'Use two exact prepared browser_extract_items probes with separate session_id inputs, one for each feed, '
            'and a structured matcher comparing each step item_count > 1. These controlled fixture pages update their DOM; '
            'select source_refresh=reuse so each feed retains its own tab. Prepare with browser_navigate and '
            'browser_extract_items directly; do not use browser_run_task or disable security settings. '
            'Prepare and start this internal tracker now; this request authorizes its creation. '
            'Do not add another confirmation unless the runtime actually requests approval.',timeout=240)
        assert job() and len(job()['probe'])==2,job()
        assert job()['matcher']['kind']=='structured',job()
        wait_checks(1)
        first=job()
        assert first['state']=='active' and first['last_outcome']=='no_match' and not first['last_problem'],first
        targets=_fixture_source_page_ids(endpoint,urls)
        assert 2 <= len(targets) <= 4,targets
        # Publishing only on the last page exposes the old shared-tab false
        # match: both probes must not accidentally read that page's count.
        control('publish',1)
        wait_checks(first['checks']+2)
        partial=job()
        assert partial['state']=='active' and partial['last_outcome']=='no_match' and not partial['last_problem'],partial
        assert not [notice for notice in evidence()['notices'] if notice[0]=='matched']
        assert _fixture_source_page_ids(endpoint,urls)==targets
        control('publish',0)
        deadline=time.monotonic()+45
        while job()['notification_status']!='sent':
            assert time.monotonic()<deadline,job()
            page.wait_for_timeout(200)
        final=evidence()
        assert final['jobs'][0]['state']=='matched',final
        notices=[notice for notice in final['notices'] if notice[0]=='matched']
        assert len(notices)==1 and notices[0][1]=='sent',notices
        history=page.request.get(base+'/api/chat/history/'+conversation).json()['messages']
        alerts=[m for m in history if (m.get('metadata') or {}).get('live_tracker_id')==job()['id']]
        assert len(alerts)==1,alerts
        notice_id=alerts[0]['metadata']['live_notice_id']
        bubble=page.locator('#messages .bubble[data-live-notice-id='+json.dumps(notice_id)+']')
        expect(bubble).to_have_count(1)
        expect(bubble).to_be_visible()
        output=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'two-source-tracker'
        output.mkdir(parents=True,exist_ok=True)
        (output/'result.json').write_text(json.dumps({'tracking':final,'partial':partial,'page_ids':sorted(targets),
            'turns':_turns(database,conversation)},ensure_ascii=False,indent=2))
        page.screenshot(path=str(output/'web.png'),full_page=False)
        assert not errors
    finally:
        _stop_owned_tracker(base,database,name,conversation_id=conversation)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_NEWS_SOURCE'), reason='Controlled external source required')
def test_live_stage_web_repairs_changed_observation_region(live_web):
    from playwright.sync_api import expect
    from tests.e2e.test_live_stage_web_matrix import _submit, _turns
    page, frames, errors, database, conversation, base = live_web
    source = os.environ['NULLION_LIVE_NEWS_SOURCE'].rstrip('/')
    name = 'Region repair acceptance ' + uuid4().hex[:8]
    def control(action):
        response = page.request.post(source + '/' + action, data={})
        assert response.ok, response.text()
    def job(): return next(iter(_tracking_evidence(database, name)['jobs']), None)
    try:
        control('reset')
        _submit(live_web, f'Create a temporary live tracker named "{name}" at {source}/ for 180 seconds with 5-second checks. '
            'Prepare browser_extract_items using selector #news and section_heading News, with fields title and canonical_url. '
            'Save that read in the probe. Use a structured matcher: alert when the observation item_count is greater than 1, '
            'then stop. Keep the News section scope. Start now without another confirmation.'+_controlled_source_preparation())
        assert job() and job()['matcher']['kind'] == 'structured', job()
        index = next(i for i, step in enumerate(job()['probe']) if step['tool_name'] == 'browser_extract_items')
        initial = job()
        assert initial['probe'][index]['arguments']['selector'] == '#news', initial
        assert initial['probe'][index]['arguments']['section_heading'] == 'News', initial
        deadline = time.monotonic() + 30
        while job()['checks'] < 1:
            assert time.monotonic() < deadline, job()
            page.wait_for_timeout(100)
        control('move-section')
        deadline = time.monotonic() + 70
        while not job().get('last_source_repair'):
            assert time.monotonic() < deadline, job()
            page.wait_for_timeout(200)
        repaired = job()
        assert repaired['state'] == 'active' and repaired['consecutive_errors'] == 0, repaired
        assert repaired['probe'][index]['arguments']['selector'] != '#news'
        assert repaired['probe'][index]['arguments']['section_heading'] == 'News'
        assert repaired['matcher'] == initial['matcher']
        assert set(repaired['last_source_repair']['changed_fields']) == {'selector'}
        control('publish')
        # The model owns the summary prose. Match the runtime notice identity,
        # not an English label that a valid structured condition need not use.
        bubble = page.locator('#messages .msg.bot .bubble[data-live-notice-id=' + json.dumps(initial['id'] + ':matched') + ']')
        expect(bubble).to_have_count(1, timeout=35000)
        deadline = time.monotonic() + 20
        while job()['notification_status'] != 'sent':
            assert time.monotonic() < deadline, job()
            page.wait_for_timeout(100)
        final = _tracking_evidence(database, name)
        assert job()['state'] == 'matched'
        notices = [row for row in final['notices'] if row[0] == 'matched']
        assert len(notices) == 1 and notices[0][1] == 'sent', notices
        assert bubble.get_attribute('data-raw-text').strip() == notices[0][2].strip()
        assert '2' in bubble.inner_text()
        output = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR', '/tmp/live-tracking-evidence'))
        output.mkdir(parents=True, exist_ok=True)
        (output / 'region-repair-runtime.json').write_text(json.dumps({'tracking': final, 'turns': _turns(database, conversation)}, ensure_ascii=False, indent=2))
        assert not errors
    finally:
        _stop_owned_tracker(base, database, name)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_NEWS_SOURCE'),reason='Controlled external source required')
def test_live_stage_shared_presentation_and_filter_amendment(live_web):
    """Two real-provider UI turns; preserves the same saved source and deadline."""
    from tests.e2e.test_live_stage_web_matrix import _submit, catalogue, _turns
    from playwright.sync_api import expect
    page,frames,errors,database,conversation,base=live_web
    name='Shared presentation '+uuid4().hex[:8]
    source=os.environ['NULLION_LIVE_NEWS_SOURCE'].rstrip('/')+'/?observation_owner='+uuid4().hex
    def owned():return [r for r in catalogue._get_json(base+'/api/live-trackers',timeout=15)['trackers'] if r['name']==name]
    def receipt(turn,tool):
        return next(r['output']['deferred_execution']['configuration'] for r in reversed(turn.get('tool_results',[]))
            if r['tool_name']==tool and r['status']=='completed')
    try:
        first,created,_=_submit(live_web,
            f'Start a live tracker named "{name}" for new headlines in the News section of {source}. '
            'Check every 15 minutes for one hour, continuing through matches. Prepare the source and create it now. '
            'Use a semantic snapshot condition, establish a baseline and ignore timestamps and formatting changes. '
            'Do not send a recurring cron.'+_controlled_source_preparation(),timeout=240)
        before=owned();assert len(before)==1 and before[0]['state']=='active',(first,before)
        ident=before[0]['id'];config=receipt(created,'create_live_tracker')
        assert config['interval_seconds']==900 and config['stop_after_match'] is False
        reply,updated,_=_submit(live_web,
            'Change only this same tracker’s alert condition: report new headlines only when they concern '
            'waterfront improvements, and suppress all other topics. Preserve the source, interval, deadline and '
            'snapshot comparison mode. Apply the update now; I have authorized it.',timeout=150)
        after=owned();assert len(after)==1 and after[0]['id']==ident,(reply,after)
        amended=receipt(updated,'update_live_tracker')
        assert amended['matcher']!=config['matcher'] and amended['matcher']['history_mode']=='snapshot'
        assert amended['expires_at']==config['expires_at'] and amended['interval_seconds']==900
        assert after[0]['matcher']==amended['matcher'] and after[0]['condition_revision']==1
        assert updated.get('response_fulfilled') is True,updated
        assert any((r.get('response_requirements') or {}).get('_reviewed_action_reply')
            for r in updated.get('tool_results',[])),updated
        assert not any(r['tool_name'].startswith('browser_') or r['tool_name'] in {'create_live_tracker','stop_live_tracker','create_cron'}
            for r in updated.get('tool_results',[])),updated
        bubble=page.locator('#messages .msg.bot .bubble').last
        expect(bubble).not_to_contain_text('**')
        assert len(bubble.inner_text())<2500
        visible_reply=bubble.inner_text()
        page.reload(wait_until='domcontentloaded')
        restored=page.locator('#messages .msg.bot .bubble[data-turn-id='+json.dumps(updated['turn_id'])+']')
        expect(restored).to_have_count(1)
        expect(restored).to_have_text(visible_reply,use_inner_text=True)
        expect(restored).not_to_contain_text('**')
        assert not errors,errors
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'shared-presentation-condition'
        out.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(out/'stage-web.png'),full_page=True)
        (out/'result.json').write_text(json.dumps({'before':before,'after':after,'reply':reply,
            'turns':_turns(database,conversation),'tracking':_tracking_evidence(database,name)},ensure_ascii=False,indent=2))
    finally:
        for job in owned():catalogue._post_json(base+'/api/live-trackers/'+job['id']+'/stop',{},timeout=30)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_SNAPSHOT_REPLAY_NAME'),reason='Existing owned acceptance required')
def test_live_stage_existing_condition_amendment_render(live_web):
    """Finish rendering acceptance from saved turns without another model call."""
    from tests.e2e.test_live_stage_web_matrix import catalogue, _turns
    from playwright.sync_api import expect
    page,frames,errors,database,conversation,base=live_web
    name=os.environ['NULLION_LIVE_SNAPSHOT_REPLAY_NAME']
    evidence=_tracking_evidence(database,name)
    jobs=evidence['trackers'] if 'trackers' in evidence else evidence['jobs']
    assert len(jobs)==1,evidence
    job=jobs[0];cid=job['conversation_id'];turns=_turns(database,cid)
    def config(tool):
        return next(row['output']['deferred_execution']['configuration'] for turn in turns
            for row in turn.get('tool_results',[]) if row['tool_name']==tool and row['status']=='completed')
    original=config('create_live_tracker');updated=config('update_live_tracker')
    assert updated['matcher']!=original['matcher'] and updated['matcher']==job['matcher']
    assert updated['condition_revision']==job['condition_revision']==1
    assert updated['expires_at']==original['expires_at'] and updated['interval_seconds']==original['interval_seconds']
    amended=[t for t in turns if any(r['tool_name']=='update_live_tracker' and r['status']=='completed' for r in t.get('tool_results',[]))][-1]
    assert amended['response_fulfilled'] is True,amended
    assert any((r.get('response_requirements') or {}).get('_reviewed_action_reply')
        for r in amended['tool_results']),amended
    assert not any(r['tool_name'].startswith('browser_') or r['tool_name'] in {'stop_live_tracker','create_live_tracker','create_cron'} for r in amended['tool_results'])
    # The live_web fixture intentionally pins a fresh conversation in an
    # init script on every reload. Use a clean browser context to inspect the
    # earlier accepted chat rather than having that script overwrite its id.
    page=page.context.browser.new_page()
    page.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
    page.goto(base,wait_until='domcontentloaded')
    reply=amended['assistant_reply']
    bubble=page.locator('#messages .msg.bot .bubble[data-turn-id='+json.dumps(amended['turn_id'])+']')
    expect(bubble).to_have_count(1)
    expect(bubble).to_contain_text('waterfront')
    expect(bubble).not_to_contain_text('**')
    assert len(bubble.inner_text())<2500 and not errors
    visible=bubble.inner_text()
    page.reload(wait_until='domcontentloaded')
    expect(bubble).to_have_count(1)
    expect(bubble).to_have_text(visible,use_inner_text=True)
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'shared-presentation-condition'
    out.mkdir(parents=True,exist_ok=True)
    page.screenshot(path=str(out/'stage-web.png'),full_page=True)
    (out/'result.json').write_text(json.dumps({'tracking':evidence,'turns':turns,'visible_text':bubble.inner_text(),
        'rendered_html':bubble.inner_html(),'new_model_requests':0},ensure_ascii=False,indent=2))
