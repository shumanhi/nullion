"""Opt-in real-provider stage Web coverage replay; never runs by default."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit

pytestmark = [pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in real stage Web'),
    pytest.mark.skipif(not os.environ.get('NULLION_LIVE_COVERAGE_CASE'), reason='Controlled source case required')]


def test_live_web_follows_discovered_alternate_after_primary_negative(live_web):
    page, frames, errors, database, conversation, base = live_web
    # A controlled public fixture supplies a discovery page, a scoped empty
    # primary catalog and an alternate with one known match. No booking writes.
    case = json.loads(Path(os.environ['NULLION_LIVE_COVERAGE_CASE']).read_text())
    prompt, expected_url, expected_text = (case[k] for k in ('prompt', 'expected_source_url', 'expected_text'))
    response, turn, elapsed = _submit(live_web, prompt, timeout=300)
    assert expected_text in response['text'] and expected_url in response['text'], response
    assert any(expected_url in json.dumps(r.get('output'), ensure_ascii=False)
               for r in turn['tool_results'] if r['status'] == 'completed'), turn
    assert turn.get('response_fulfilled') is True,turn
    readers={'browser_read_pages','browser_extract_text','browser_extract_detail','browser_extract_items','browser_run_task','web_fetch'}
    observed=[json.dumps(r.get('output'),ensure_ascii=False) for r in turn['tool_results']
        if r['status']=='completed' and r['tool_name'] in readers]
    assert any(expected_url in output and expected_text in output for output in observed),observed
    assert 'Some requested work remains incomplete.' not in response['text'], response
    assert not errors and not response.get('artifacts')
