"""Opt-in replay through the real stage Web UI with backing SQLite evidence.

Set NULLION_LIVE_STAGE_E2E=1 to use an owned paused read-only fixture.
NULLION_STAGE_CRON_ID may select an authorized task belonging to this chat.
For an action case also set NULLION_STAGE_CRON_EXPECTED_WRITE;
for a monitor set NULLION_STAGE_CRON_EXPECTED_MODE=verified_matches_only.
The replay never approves account actions or enables a schedule.
"""
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit, _turns, catalogue
from tests.e2e.test_live_stage_web_cron_recovery import _diagnostics

pytestmark = pytest.mark.skipif(
    os.environ.get('NULLION_LIVE_STAGE_E2E') != '1',
    reason='Requires authorized stage services and real provider quota',
)


def test_live_stage_saved_cron_has_valid_scope_and_visible_terminal_outcome(live_web, request):
    from playwright.sync_api import expect
    page, frames, errors, db, cid, base = live_web
    cron_id = os.environ.get('NULLION_STAGE_CRON_ID')
    owned = not cron_id
    expected_write = os.environ.get('NULLION_STAGE_CRON_EXPECTED_WRITE')
    expected_mode = os.environ.get('NULLION_STAGE_CRON_EXPECTED_MODE')
    marker='Verified scheduled value '+uuid4().hex[:10]
    if owned:
        from scripts.live_stage_chat_contract_probe import _stage_workspace_upload_root
        note=_stage_workspace_upload_root()/('cron-scope-'+uuid4().hex+'.txt')
        note.write_text(marker+'\n')
        response=page.request.post(base+'/api/crons',data={
            'name':'Read-only scope '+uuid4().hex[:8], 'schedule':'0 0 1 1 *','enabled':False,
            'delivery_channel':'web','delivery_target':cid,
            'task':f'Read {note} using file_read and report its exact value here. No external lookups, writes or attachments.',
            'artifact_delivery_options':{'doctor_monitoring_enabled':False}})
        assert response.ok,response.text()
        job=response.json()['job'];cron_id=job['id']
    else:
        job=next(row for row in catalogue._get_json(base+'/api/crons',timeout=30) if row['id']==cron_id)
        assert job['delivery_channel']=='web' and job['delivery_target']==cid, 'Use a task owned by this Web replay'
    assert job['enabled'] is False, 'Select a paused task; this replay does not alter schedules'
    request.node._nullion_saved_background_execution={'cron_id':cron_id,'terminal':False}
    terminal=None
    try:
        _submit(live_web,f'Run task id {cron_id} once. Keep it paused.',timeout=180)
        deadline = time.monotonic()+240
        guard=request.getfixturevalue('live_stage_provider_guard')
        next_check=time.monotonic()
        while time.monotonic()<deadline:
            if time.monotonic()>=next_check:
                blocked=guard.check()
                if blocked:pytest.exit(blocked,returncode=1)
                next_check=time.monotonic()+15
            events=_diagnostics(cid)
            terminal=next((event for event in reversed(events) if event.get('event_type')=='conversation.cron_run'
                and event.get('cron_id')==cron_id),None)
            if terminal: break
            page.wait_for_timeout(500)
        assert terminal, 'The UI-triggered task never reached a terminal/approval boundary'
        runtime_events=catalogue._runtime_event_payloads(db,cid)
        events=[event for event in runtime_events if event.get('payload',{}).get('cron_id')==cron_id]
        scope = next(event['payload'] for event in events
                     if event.get('payload',{}).get('stage')=='connector_scope')
        if expected_write:
            assert expected_write in scope['selected_write_tools']
            approvals=catalogue._get_json(base+'/api/status',timeout=30)['approvals']
            assert any(row.get('resource')==expected_write for row in approvals), approvals
            page.reload()
            expect(page.get_by_role('button',name='Allow once',exact=True).first).to_be_visible(timeout=15000)
        else:
            assert not terminal.get('report_failed'),terminal
            assert terminal.get('response_fulfilled') is not False,terminal
            delivered=[row for row in _turns(db,cid) if row.get('cron_id')==cron_id]
            assert len(delivered)==1,delivered
            if owned:
                assert 'file_read' in {r['tool_name'] for r in terminal['tool_results'] if r['status']=='completed'}
                assert marker in delivered[0]['assistant_reply'],delivered
                assert not scope.get('selected_write_tools'),scope
                expect(page.locator('#messages .msg.bot .bubble').filter(has_text=marker)).to_have_count(1,timeout=15000)
                page.reload()
                expect(page.locator('#messages .msg.bot .bubble').filter(has_text=marker)).to_have_count(1,timeout=15000)
        if expected_mode:
            assert scope['alert_delivery_mode']==expected_mode
            decisions = [event['payload'] for event in events if event.get('payload',{}).get('stage')=='alert_delivery_policy_decided']
            assert decisions and decisions[-1]['reason'] in {'verified_match','no_verified_match'},decisions
        after=next(row for row in catalogue._get_json(base+'/api/crons',timeout=30) if row['id']==cron_id)
        assert after['enabled'] is False
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'saved-cron-scope'
        out.mkdir(parents=True,exist_ok=True)
        (out/'result.json').write_text(json.dumps({'conversation_id':cid,'scope':scope,
            'terminal':terminal,'stored_job':after},ensure_ascii=False,indent=2))
        page.screenshot(path=str(out/'web.png'),full_page=False)
        assert not errors,errors
    finally:
        if terminal is not None:
            request.node._nullion_saved_background_execution['terminal']=True
            if owned:
                response=page.request.delete(base+'/api/crons/'+cron_id)
                assert response.ok,response.text()
                note.unlink(missing_ok=True)
