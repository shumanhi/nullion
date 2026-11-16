"""Focused real-provider Web acceptance for scoped, reversible state changes."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in live stage quota')


def _committed_tool(live_web, turn_id, name):
    """Visible replies may precede secondary transcript persistence; require the real receipt."""
    import time
    page, frames, errors, database, conversation, base = live_web
    deadline = time.monotonic()+15
    while True:
        turns = catalogue._conversation_event_payloads(database, conversation, event_type='conversation.chat_turn')
        for row in turns:
            if row.get('turn_id') == turn_id:
                for result in row.get('tool_results', []):
                    if result['tool_name'] == name and result['status'] == 'completed':
                        return result
        assert time.monotonic() < deadline, {'turn_id':turn_id, 'missing_tool':name}
        page.wait_for_timeout(100)


def test_live_reminder_changes_commit_quickly_and_clean_up_the_owned_record(live_web, record_property):
    page, frames, errors, database, conversation, base = live_web
    label = 'stage persistence check '+conversation.rsplit(':',1)[-1]
    before = datetime.now(timezone.utc).isoformat()
    evidence = []
    task_id = None
    try:
        response, turn, elapsed = _submit(live_web,
            f'Set one reminder for June 10, 2030 at 9:00 AM in my saved local timezone. '
            f'The reminder text is "{label}". Confirm it here.', timeout=100)
        if not any(row['tool_name']=='set_reminder' for row in turn.get('tool_results', [])) and '1. Create the reminder' in response['text']:
            response, turn, extra_elapsed = _submit(live_web, '1', timeout=100)
            elapsed += extra_elapsed
        created = _committed_tool(live_web, turn['turn_id'], 'set_reminder')
        task_id = created['output']['task_id']
        evidence.append({'operation':'create', 'elapsed_ms':elapsed, 'reply':response['text']})
        response, turn, elapsed = _submit(live_web,
            f'Change only that reminder, task id {task_id}, to June 10, 2030 at 10:00 AM in my local timezone. Keep its text.', timeout=100)
        updated = _committed_tool(live_web, turn['turn_id'], 'update_reminder')
        assert updated['output']['task_id'] == task_id, turn
        evidence.append({'operation':'update', 'elapsed_ms':elapsed, 'reply':response['text']})
    finally:
        if task_id:
            response, turn, elapsed = _submit(live_web,
                f'Cancel only the test reminder with task id {task_id}. Leave every other reminder and schedule unchanged.', timeout=100)
            deleted = _committed_tool(live_web, turn['turn_id'], 'delete_reminder')
            assert deleted['output']['task_id'] == task_id, turn
            evidence.append({'operation':'delete', 'elapsed_ms':elapsed, 'reply':response['text']})
    timings = [row for row in catalogue._conversation_event_payloads(database, conversation, event_type='conversation.tool_timing')
        if row.get('created_at', '') >= before and row.get('tool_name') in {'set_reminder', 'update_reminder', 'delete_reminder'}]
    assert len(timings) == 3 and all(row.get('duration_ms', 999999) < 1500 for row in timings), timings
    root = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR', '/tmp/live-web-evidence'))
    root.mkdir(parents=True, exist_ok=True)
    (root/'reminder-small-writes.json').write_text(json.dumps({'conversation_id':conversation,
        'task_id':task_id, 'operations':evidence, 'tool_timings':timings}, ensure_ascii=False, indent=2))
    record_property('reminder_max_tool_ms', max(row['duration_ms'] for row in timings))
    assert sum(row['elapsed_ms'] for row in evidence)/3 < 45000 and not errors


def test_live_status_update_does_not_invent_a_personal_task(live_web):
    page, frames, errors, database, conversation, base = live_web
    response, turn, elapsed = _submit(live_web,
        'My observatory calibration personal task is done. Mark that existing item completed. '
        'If you cannot identify it, ask me; do not add a new task.', timeout=100)
    results = turn.get('tool_results', [])
    assert not any(row['tool_name']=='personal_task_save' and row['status']=='completed' for row in results), turn
    assert any(row['tool_name']=='personal_tasks_list' and row['status']=='completed' for row in results), turn
    assert '1.' in response['text'] and '2.' in response['text'], response
    root = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR', '/tmp/live-web-evidence'))
    (root/'personal-task-status-choice.json').write_text(json.dumps({'conversation_id':conversation,
        'reply':response['text'], 'elapsed_ms':elapsed, 'tools':[(row['tool_name'], row['status']) for row in results]},
        ensure_ascii=False, indent=2))
    assert not errors
