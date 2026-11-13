"""Opt-in stage UI contracts: primary ownership and native source consent."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _turns, catalogue

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1',
    reason='Explicit real-provider stage Web acceptance only')


def _record_primary_boundary(live_web, name, *, expected_turns=1, allow_deferred=False):
    page, _, errors, database, cid, base = live_web
    turns = _turns(database, cid)
    if not allow_deferred:
        assert len(turns) == expected_turns, turns
    events = catalogue._conversation_event_payloads(database, cid)
    quality = [row for row in events if row.get('event_type') in {
        'conversation.completion_review', 'conversation.completion_requirement_contract',
        'conversation.action_review'}]
    assert quality == [], quality
    assert not errors
    history = page.request.get(base + '/api/chat/history/' + cid).json()
    assert sum(row.get('role') == 'bot' for row in history['messages']) == expected_turns, history
    assert sum(row.get('role') == 'user' for row in history['messages']) == expected_turns, history
    output = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    (output / (name + '.json')).write_text(json.dumps({
        'conversation_id': cid, 'turns': turns, 'quality_events': quality,
        'history': history}, ensure_ascii=False, indent=2))
    page.locator('#messages').screenshot(path=str(output / (name + '.png')))


def test_live_web_primary_report_download_without_quality_grading(live_web):
    from tests.e2e.test_live_stage_web_extended import (
        test_live_web_compact_pdf_summary_keeps_heading_and_bullets_together as replay)
    replay(live_web)
    _record_primary_boundary(live_web, 'primary-pdf-report')


def test_live_web_primary_keeps_native_account_source_consent(live_web):
    from tests.e2e.test_live_stage_web_extended import (
        test_live_web_account_read_waits_for_explicit_source_choice as replay)
    replay(live_web, 'en')
    _record_primary_boundary(live_web, 'primary-native-source-consent')
