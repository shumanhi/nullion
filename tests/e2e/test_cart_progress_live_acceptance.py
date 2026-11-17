"""Opt-in stage UI cart read and current-activity progress, with real inference."""
import json
import os
from pathlib import Path

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in live stage')


def test_live_cart_read_and_progress_keep_plain_turns_fast(live_web, record_property):
    page, frames, errors, database, conversation, base = live_web
    source = os.environ.get('NULLION_LIVE_CAPTURE_SOURCE')
    if not source: pytest.skip('Disposable HTTP cart source is required')
    samples=[]
    for prompt, answer in [('Reply only ALPHA37.', 'ALPHA37'), ('Responde solo BRAVO37.', 'BRAVO37'), ('CHARLIE37だけ返してください。','CHARLIE37')]:
        reply,turn,elapsed=_submit(live_web,prompt,timeout=60)
        assert answer in reply['text'] and not turn.get('tool_results'),turn
        samples.append(elapsed/1000)
    assert sum(samples)/len(samples)<10 and max(samples)<20,samples
    before=len(frames)
    reply,turn,elapsed=_submit(live_web,
        'Whats in my cart? Open this cart overview in the browser and follow its cart-items link to list the actual items, quantities, prices and total: '+source,timeout=180)
    assert turn['response_fulfilled'] is True,(reply,turn)
    for fact in ['A37','B73','$7','$12','$19']:
        assert fact.casefold() in reply['text'].casefold(),reply
    tools=turn['tool_results']; assert any(r['tool_name'].startswith('browser_') and r['status']=='completed' for r in tools)
    assert not any(r.get('output',{}).get('reason')=='code_mode_unknown_write_outcome' for r in tools)
    assert elapsed/1000<90,elapsed
    progress=[r['text'] for r in frames[before:] if r.get('type')=='progress' and isinstance(r.get('text'),str)]
    assert any('Reviewing results and planning the next step' in text for text in progress),progress
    assert all(not text.lstrip('🔔 ').startswith(('1 step completed','2 steps completed')) for text in progress),progress
    assert not errors
    record_property('simple_average_seconds',sum(samples)/len(samples))
    record_property('cart_seconds',elapsed/1000)
    Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'],'cart-progress-contract.json').write_text(json.dumps({
        'simple_seconds':samples,'cart_seconds':elapsed/1000,'turn':turn,'progress':progress},ensure_ascii=False,indent=2))
