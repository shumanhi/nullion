"""Real calendar, browser, source progress and PDF delivery for the reported trip."""
import io
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1'
    or not os.environ.get('NULLION_LIVE_TRIP_REPLAY_PROMPT'),reason='Explicit real stage trip replay')


def test_trip_research_completes_with_source_progress_and_verified_pdf(live_web,record_property):
    from playwright.sync_api import expect
    from pypdf import PdfReader
    page,frames,errors,database,cid,base=live_web
    out=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'trip-completion';out.mkdir(parents=True,exist_ok=True)
    root=os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT']
    prompt=os.environ['NULLION_LIVE_TRIP_REPLAY_PROMPT']+'\nSave the final PDF under '+root+'/weekend-trip.pdf.'
    proof={'conversation_id':cid,'status':'running','prompt':prompt}
    try:
        config=page.request.get(base+'/api/config').json()
        assert config.get('browser_backend') in {'cdp','auto'},config.get('browser_backend')
        proof['browser']={key:config.get(key) for key in ('browser_backend','browser_cdp_url','browser_preferred')}
        response,turn,elapsed=_submit(live_web,prompt,timeout=720)
        proof.update(response=response,turn=turn,elapsed_ms=elapsed)
        results=turn.get('tool_results',[])
        assert any(r['tool_name']=='calendar_list' and r['status']=='completed' for r in results),results
        assert any(r['tool_name'].startswith('browser_') and r['status']=='completed' for r in results),results
        assert not any(r['tool_name'] in {'calendar_create','calendar_update','calendar_delete','email_send',
            'create_cron','create_live_tracker'} for r in results),results
        assert turn.get('response_fulfilled') is True,turn
        artifacts=response.get('artifacts',[])
        assert len(artifacts)==1 and catalogue._artifact_name(artifacts[0]).lower().endswith('.pdf'),response
        data,mime=catalogue._artifact_bytes(base,artifacts[0],timeout=30)
        assert data.startswith(b'%PDF-') and len(data)>2000
        reader=PdfReader(io.BytesIO(data));assert reader.pages
        for p in reader.pages:
            contents=p.get('/Contents')
            if contents is not None:assert hasattr(contents.get_object(),'get_data') or isinstance(contents.get_object(),list)
        text='\n'.join(p.extract_text() or '' for p in reader.pages)
        assert len(text.strip())>250,text
        (out/'weekend-trip.pdf').write_bytes(data)
        statuses=[f.get('text','') for f in frames
            if f.get('type')=='progress' or f.get('transport')=='web-http-progress']
        proof['visible_progress']=list(dict.fromkeys(statuses))
        assert any('I can handle multiple at once' in s for s in statuses),statuses
        assert any('Calendar checked' in s for s in statuses),statuses
        assert any('page' in s for s in statuses),statuses
        source_start=next(i for i,s in enumerate(statuses) if s.startswith((
            'Searching the web','Opening a page','Reading pages','Reading page',
            'Reviewing page','Checking sources','Collecting images')))
        assert all('Calendar checked' not in s for s in statuses[source_start:]),statuses
        expect(page.locator('#messages a[download]')).to_have_count(1)
        expect(page.locator('#messages .status-bubble')).to_have_count(0)
        page.reload(wait_until='domcontentloaded')
        expect(page.locator('#messages a[download]')).to_have_count(1)
        assert not errors,errors
        models=catalogue._conversation_event_payloads(database,cid,event_type='conversation.model_timing')
        proof['outer_model_calls']=len(models)
        proof['outer_model_ms']=sum(r.get('duration_ms',0) for r in models)
        proof['pdf_pages']=len(reader.pages)
        record_property('trip_completion_ms',elapsed)
        assert elapsed<360000,{'elapsed_ms':elapsed,'outer_model_calls':len(models)}
        proof['status']='passed'
    except BaseException as error:
        proof.update(status='failed',error=str(error));raise
    finally:
        proof['frames']=frames
        page.screenshot(path=str(out/'web.png'),timeout=10000)
        (out/'result.json').write_text(json.dumps(proof,ensure_ascii=False,indent=2)+'\n')
