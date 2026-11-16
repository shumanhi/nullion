"""Read-only ownership/history assertions shared by canonical live workflows."""
import json
import os
from pathlib import Path

from tests.e2e.test_live_stage_web_matrix import _turns, catalogue


def record_primary_boundary(live_web, name, *, expected_turns=1, allow_deferred=False):
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
