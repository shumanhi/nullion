"""Focused real-provider scheduled report acceptance using owned source fixtures."""
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit, _turns, catalogue
from tests.e2e.test_live_stage_web_cron_recovery import _diagnostics
from tests.report_presentation_fixture import report_fixture

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1',
                               reason='Explicit stage-only Web acceptance')


def test_live_compact_scheduled_report_preserves_required_findings(live_web, request):
    page, frames, errors, database, conversation, base = live_web
    output = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    _, verbose, source = report_fixture()
    fixture = Path(os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT']) / 'verified-records.json'
    fixture.write_text(json.dumps(source))
    name = 'Compact source report ' + uuid4().hex[:8]
    response = page.request.post(base + '/api/crons', data={
        'name': name, 'schedule': '0 8 * * *', 'enabled': False,
        'task': ('Read the verified fixture records in ' + str(fixture) + ' once. '
                 'Report all ten findings concisely with each sender, identifying subject and received date. '
                 'Keep required amounts, deadlines, uncertainties and discrepancies. '
                 'Group the calendar outcome once and omit optional historical expansions. '
                 'This is a source-fixture report, not a request to inspect accounts or change the calendar. '
                 'Deliver chat text only; no output file is requested.'),
        'delivery_channel': 'web', 'delivery_target': conversation,
        'artifact_delivery_options': {'requires_attachment_delivery': False}})
    assert response.ok, response.text()
    job = response.json()['job']
    request.node._nullion_saved_background_execution = {'cron_id': job['id'], 'terminal': False}
    proof = {'conversation_id': conversation, 'cron_id': job['id']}
    terminal = None
    try:
        receipt, _, elapsed = _submit(live_web,
            'Run the saved scheduled task "' + name + '" (ID ' + job['id'] +
            ') once now. Leave the schedule disabled and deliver its report here.', timeout=180)
        proof.update(receipt=receipt, startup_ms=elapsed)
        deadline = time.monotonic() + 360
        guard = request.getfixturevalue('live_stage_provider_guard')
        next_check = 0
        while time.monotonic() < deadline:
            if time.monotonic() >= next_check:
                blocked = guard.check()
                if blocked:
                    pytest.exit(blocked, returncode=1)
                next_check = time.monotonic() + 15
            events = _diagnostics(conversation)
            runs = [row for row in events if row.get('event_type') == 'conversation.cron_run'
                    and row.get('cron_id') == job['id']]
            if runs:
                terminal = runs[-1]
                break
            page.wait_for_timeout(1000)
        assert terminal, 'No terminal report within the observation window'
        proof['terminal'] = terminal
        assert terminal['report_presentation']['report_presentation'] == 'structured', terminal
        assert terminal['response_fulfilled'] is True, terminal
        tools = terminal['tool_results']
        assert any(row['tool_name'] == 'file_read' and row['status'] == 'completed' for row in tools), tools
        assert not any(row['tool_name'].startswith(('email_', 'calendar_')) for row in tools), tools
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            delivered = [row for row in _turns(database, conversation) if row.get('cron_id') == job['id']]
            if delivered:
                break
            page.wait_for_timeout(300)
        assert len(delivered) == 1, delivered
        text = delivered[0]['assistant_reply'].strip()
        proof['delivered_text'] = text
        assert len(text) < len(verbose) * .8, text
        for value in ('$200', '$60', '$35', '$720', '$760', '$5', '$3,800', '$1,000', '$10'):
            assert value in text, (value, text)
        for row in source['records']:
            assert row['title'].casefold() in text.casefold(), (row['title'], text)
        assert 'unconfirmed' in text.lower() and 'unavailable' in text.lower(), text
        assert 'Historical detail' not in text and 'Full subject:' not in text
        for row in source['sources']:
            assert row['expanded_label'].removeprefix('Full subject: ') not in text, text
        assert 'nullion.report.v1' not in text and 'shared_detail_ids' not in text
        page.wait_for_function("""text => Array.from(document.querySelectorAll('#messages .msg.bot .bubble'))
            .some(el => (el.getAttribute('data-raw-text') || '').trim().startsWith(text))""", arg=text, timeout=15000)
        matches = [el for el in page.locator('#messages .msg.bot .bubble').all()
                   if (el.get_attribute('data-raw-text') or '').strip().startswith(text)]
        assert len(matches) == 1 and matches[0].is_visible()
        assert matches[0].locator('li').count() >= 3, matches[0].inner_text()
        assert '**' not in matches[0].inner_text()
        matches[0].screenshot(path=str(output / 'compact-report-message.png'))
        history = page.request.get(base + '/api/chat/history/' + conversation).json()['messages']
        assert not any(row.get('artifacts') for row in history), history
        assert not errors, errors
    finally:
        (output / 'compact-report.json').write_text(json.dumps(proof, ensure_ascii=False, indent=2))
        page.screenshot(path=str(output / 'compact-report.png'), timeout=10000)
        if terminal is not None:
            request.node._nullion_saved_background_execution['terminal'] = True
            deleted = page.request.delete(base + '/api/crons/' + job['id'])
            assert deleted.ok, deleted.text()
