"""Real Web follow-up cancellation; mutates only its new paused fixture."""
import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web,_submit,catalogue

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Authorized live stage only')


@pytest.mark.parametrize('template',[
    'Before creating the disabled daily task {name} to report 7 at noon, ask me to confirm. Do not create it before my answer.',
    'Antes de crear la tarea diaria desactivada {name} que informe 7 al mediodía, pídeme confirmación. No la crees sin mi respuesta.',
    '毎日正午に7を報告する無効な定期タスク「{name}」を作成する前に、私の確認を求めてください。返答前に作成しないでください。'])
def test_live_confirmation_questions_offer_numeric_choices(live_web,template):
    import re
    page,frames,errors,database,conversation,base=live_web
    name='Consent fixture '+uuid4().hex[:8]
    try:
        reply,turn,_=_submit(live_web,template.format(name=name),timeout=90)
        options=re.findall(r'(?m)^\s*(\d+)[.)]\s+',reply['text'])
        assert options[:2]==['1','2'],reply
        assert not any(r['tool_name']=='create_cron' and r['status']=='completed' for r in turn.get('tool_results',[])),turn
        assert not any(r['name']==name for r in catalogue._get_json(base+'/api/crons',timeout=15))
        assert not errors,errors
    finally:
        for row in catalogue._get_json(base+'/api/crons',timeout=15):
            if row['name']==name:catalogue._delete_json(base+'/api/crons/'+row['id'],timeout=30)


def test_live_stage_completed_scheduler_followup_and_adjacent_edit(live_web):
    page,frames,errors,database,conversation,base=live_web
    name='Sensor Birch '+uuid4().hex[:8];renamed=name+' revised';ident=None
    def job():
        return next((r for r in catalogue._get_json(base+'/api/crons',timeout=15) if r['id']==ident),None)
    try:
        created,event,_=_submit(live_web,
            f'Create a paused scheduled task named {name}. Every day at noon report the number 7. Keep it disabled. '
            'I explicitly confirm creation of this exact disabled task now, in the current workspace and my saved timezone.',timeout=180)
        if not any(r['tool_name']=='create_cron' and r['status']=='completed' for r in event['tool_results']):
            # Respect the saved confirmation preference for creating tasks.
            assert all(r.get('output',{}).get('reason')=='request_plan_conflict'
                for r in event['tool_results'] if r['status']=='failed'),event
            assert '1.' in created['text'] and '2.' in created['text'],created
            created,event,_=_submit(live_web,'1',timeout=180)
        receipt=next(r for r in event['tool_results'] if r['tool_name']=='create_cron' and r['status']=='completed')
        ident=receipt['output']['id']
        assert job()['name']==name and job()['enabled'] is False,created
        edited,edit_event,_=_submit(live_web,
            f'Renómbralo a “{renamed}”. Déjalo pausado. '
            'Si cancelar una tarea pausada necesita aclaración, ofrece 1. Eliminarla permanentemente, '
            '2. Mantenerla pausada. Espera mi elección.',timeout=180)
        assert any(r['tool_name']=='update_cron' and r['status']=='completed' for r in edit_event['tool_results']),edited
        assert job()['name']==renamed and job()['enabled'] is False
        def unrelated():
            return {r['id']:{k:r.get(k) for k in ('name','enabled','schedule','task','schedule_timezone')}
                for r in catalogue._get_json(base+'/api/crons',timeout=15) if r['id']!=ident}
        other_jobs=unrelated()
        before=len(frames);page.locator('#user-input').fill('Cancel it');page.locator('#user-input').press('Enter')
        deadline=time.monotonic()+150;approved=False;chose=False;cancelled=None
        while time.monotonic()<deadline:
            page.wait_for_timeout(150)
            approvals=[r for r in frames[before:] if r.get('type')=='approval_required']
            if approvals and not approved:
                pending=approvals[-1]
                assert ident in json.dumps(pending) or renamed in json.dumps(pending),pending
                page.get_by_role('button',name='Allow once',exact=True).last.click();approved=True
            terminals=[r for r in frames[before:] if r.get('type')=='done']
            if job() is None:
                cancelled=terminals[-1] if terminals else None
                if cancelled:break
            if terminals and job() is not None and not approved and not chose:
                reply=terminals[-1].get('text') or ''.join(r.get('text','') for r in frames[before:]
                    if r.get('type')=='chunk' and r.get('turn_id')==terminals[-1].get('turn_id'))
                assert '1.' in reply and '2.' in reply,reply
                before=len(frames);page.locator('#user-input').fill('1');page.locator('#user-input').press('Enter');chose=True
        assert cancelled and job() is None,page.locator('#messages').inner_text()
        assert unrelated()==other_jobs,'Cancellation changed an unrelated scheduled task'
        from playwright.sync_api import expect
        bubble=page.locator('#messages .msg.bot .bubble[data-turn-id='+json.dumps(cancelled['turn_id'])+']')
        expect(bubble).to_have_count(1);expect(bubble).to_be_visible()
        assert not errors,errors
        out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'scheduler-followup-cancellation';out.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(out/'web.png'),full_page=False)
        (out/'result.json').write_text(json.dumps({'conversation_id':conversation,'owned_cron_id':ident,
            'created':event,'edited':edit_event,'terminal':cancelled,'deleted':True,'numeric_choice':chose,
            'approval_used':approved},ensure_ascii=False,indent=2))
    finally:
        for row in catalogue._get_json(base+'/api/crons',timeout=30):
            if row['id']==ident or row['name'] in {name,renamed}:
                catalogue._delete_json(base+'/api/crons/'+row['id'],timeout=30)
