"""Opt-in real-provider replay of absolute service-restoration alert setup."""
import os
from uuid import uuid4
import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit
from tests.e2e.test_live_stage_live_tracking import _tracking_evidence, _stop_owned_tracker

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in stage')


def test_live_stage_absolute_restoration_alert(live_web):
    page, frames, errors, database, conversation, base = live_web
    name = 'Restoration replay '+uuid4().hex[:8]
    try:
        reply, turn, _ = _submit(live_web,
            'Alert me when all Chase banking services generally have been fully restored, check every 30 mins for up to 24 hours. '
            'Stop after the first confirmed restoration alert. Create it now, named "'+name+'".', timeout=300)
        evidence = _tracking_evidence(database, name)
        assert len(evidence['jobs']) == 1, (reply, turn, evidence)
        job = evidence['jobs'][0]
        assert job['interval_seconds'] == 1800 and job['stop_after_match'] is True
        assert turn['response_fulfilled'] is True, turn
        import time
        deadline = time.monotonic()+60
        while not job['checks'] and time.monotonic() < deadline:
            page.wait_for_timeout(700)
            evidence = _tracking_evidence(database, name)
            job = evidence['jobs'][0]
        assert job['checks'] >= 1 and job['last_outcome'] in {'matched', 'no_match'}, evidence
        assert any(r['tool_name'] == 'create_live_tracker' and r['status'] == 'completed'
                   for r in turn['tool_results'])
        assert not errors
    finally:
        _stop_owned_tracker(base, database, name)
