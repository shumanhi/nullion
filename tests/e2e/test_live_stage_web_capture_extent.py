"""Opt-in real-provider Web capture: visible image, exact native extent and bytes."""
import io
import json
import os
from pathlib import Path

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue
from tests.e2e.live_web_artifact_assertions import capture_web, save_artifact_through_ui, retained_plain_latencies

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1'
    or (not os.environ.get('NULLION_LIVE_CAPTURE_SOURCE') and not os.environ.get('NULLION_LIVE_CAPTURE_REPLAY')), reason='Opt-in stage capture URL required')


def test_live_web_ordinary_capture_is_viewport_and_plain_turns_stay_fast(capture_web, record_property):
    from PIL import Image
    from playwright.sync_api import expect
    live_web=capture_web.surface
    page, frames, errors, database, conversation, base = live_web
    timing_evidence={'evidence_kind':'fresh_generation_timing'}
    if capture_web.replay:
        samples,timing_evidence=retained_plain_latencies(capture_web.replay)
        response,turn,elapsed=capture_web.saved
    else:
        samples=[]
        for prompt in ['Responde solamente con 37.', '37 とだけ答えてください。', 'Reply with only 37.']:
            response, turn, elapsed = _submit(live_web,prompt,timeout=90)
            assert '37' in response['text'] and not response.get('artifacts'),response
            assert not turn.get('tool_results'),turn
            samples.append(elapsed)
        source=os.environ['NULLION_LIVE_CAPTURE_SOURCE']
        response, turn, elapsed = _submit(live_web,f'Send me a screenshot of {source}',timeout=180)
    assert sum(samples)/len(samples) < 45000 and max(samples) < 60000,samples
    results=turn.get('tool_results',[])
    screenshots=[r['output'] for r in results if r['tool_name']=='browser_screenshot' and r['status']=='completed']
    assert len(screenshots)==1,results
    capture=screenshots[0]
    assert capture['capture_extent_contract'] in {'auto','viewport'},capture
    assert capture['mode']=='viewport' and capture['effective_requested_mode'] in {'auto','viewport'},capture
    assert not any(r['tool_name']=='terminal_exec' for r in results),results
    assert turn.get('response_fulfilled') is True,turn
    artifacts=response.get('artifacts',[]);assert len(artifacts)==1,response
    artifact=artifacts[0];filename=catalogue._artifact_name(artifact)
    card=page.locator('.image-artifact-card').filter(has_text=Path(filename).name)
    expect(card).to_have_count(1);card.scroll_into_view_if_needed()
    download_control=card.locator('button.image-artifact-btn').nth(1)
    expect(download_control).to_have_text('Download')
    expect(download_control).to_be_visible()
    card.locator('img').evaluate('image => image.decode()')
    body,save_receipt=save_artifact_through_ui(page,base,artifact,download_control)
    with Image.open(io.BytesIO(body)) as image:
        assert image.size==(capture['viewport_width'],capture['viewport_height']),capture
        assert image.size==(capture['image_width'],capture['image_height']),capture
    evidence=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))/'capture-extent'
    evidence.mkdir(parents=True,exist_ok=True)
    (evidence/'capture.png').write_bytes(body)
    page.screenshot(path=str(evidence/'web.png'),full_page=False)
    (evidence/'result.json').write_text(json.dumps({'conversation_id':conversation,'turn_id':turn['turn_id'],
        'capture':capture,'simple_latency_ms':samples,'timing_evidence':timing_evidence,
        'capture_latency_ms':elapsed,'artifacts':artifacts,'save_receipt':save_receipt,
        'evidence_kind':'saved_boundary_reuse' if capture_web.replay else 'fresh_generation',
        'replay':capture_web.replay},indent=2))
    record_property('simple_latency_ms',json.dumps(samples));record_property('capture_latency_ms',elapsed)
