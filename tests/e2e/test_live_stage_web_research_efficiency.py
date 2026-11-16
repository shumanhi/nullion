"""Opt-in, real-provider research acceptance through the stage composer."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web
from tests.e2e.test_live_stage_code_mode import _submit

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Explicit live stage acceptance')


def _research_budget(live_web):
    from urllib.request import Request, urlopen
    from urllib.parse import urlencode
    from scripts.live_stage_http import read_json
    settings = json.loads(Path(os.environ['NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION']).read_text())
    request = Request('http://host.docker.internal:'+str(settings['port'])+'/conversation-events?'+
        urlencode({'conversation_id':live_web[4], 'event_type':'conversation.research_budget'}),
        headers={'Authorization':'Bearer '+settings['token']})
    events = read_json(request, timeout=15, opener=urlopen)['events']
    assert events, 'Research must have a durable request budget receipt'
    budget = events[-1]
    assert budget['usage']['research_native_calls'] <= budget['limits']['native_calls']
    assert budget['usage']['research_model_calls'] <= budget['limits']['model_calls']
    return budget


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_OBSERVATION_CDP_URL'), reason='Desktop tab observation endpoint required')
def test_live_stage_research_releases_only_its_desktop_tabs(live_web, record_property):
    from urllib.request import Request, urlopen
    endpoint = os.environ['NULLION_LIVE_OBSERVATION_CDP_URL'].rstrip('/')
    def targets():
        with urlopen(Request(endpoint+'/json/list', headers={'Host': '127.0.0.1'}), timeout=5) as response:
            return {row['id']: row.get('url', '') for row in json.load(response) if row.get('type') == 'page'}
    before = targets()
    response, turn, elapsed = _submit(live_web,
        'Use browser_open, then browser_navigate and browser_extract_text on https://example.com and report the page title. '
        'This is research for this answer; no browser handoff or screenshot is needed.', timeout=120)
    after = targets()
    results = turn.get('tool_results') or []
    assert any(r['tool_name'] in {'browser_navigate', 'browser_read_pages'} and r['status'] == 'completed'
        for r in results), results
    assert not any(r['tool_name'] == 'browser_close' for r in results), 'Prove runtime cleanup without a model close call'
    assert 'example domain' in response['text'].lower(), response
    assert turn.get('response_fulfilled') is True
    assert after == before, {'before': before, 'after': after}
    target = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    target.mkdir(parents=True, exist_ok=True)
    (target/'browser-cleanup.json').write_text(json.dumps({'before': before, 'after': after,
        'response': response, 'turn': turn, 'elapsed_ms': elapsed}, indent=2))
    live_web[0].screenshot(path=str(target/'browser-cleanup.png'), full_page=True)
    record_property('browser_cleanup_seconds', elapsed/1000)


@pytest.mark.parametrize('prompt', [
    'Find true crime documentary on netflix to watch that are recently added',
    'Recomiéndame algunos museos de Nueva York para visitar este fin de semana, con enlaces oficiales.',
])
def test_live_stage_bounded_research_delivers_without_unnecessary_expansion(live_web, prompt, record_property):
    response, turn, elapsed = _submit(live_web, prompt, timeout=180)
    results = turn.get('tool_results') or []
    clarification_turn_id = None
    if not results and turn.get('response_fulfilled') is None:
        # Sunday can legitimately require a date choice. Exercise the normal
        # numeric continuation instead of mistaking clarification for failure.
        assert '1.' in response['text'] and '2.' in response['text'], response
        clarification_turn_id = turn['turn_id']
        response, turn, followup_elapsed = _submit(live_web, '2', timeout=180)
        assert turn.get('parent_turn_id') == clarification_turn_id, turn.get('parent_turn_id')
        elapsed += followup_elapsed
        results = turn.get('tool_results') or []
    external = [r for r in results if r['tool_name'] in {'browser_read_pages', 'browser_navigate', 'web_search', 'web_fetch'}]
    assert external, turn
    assert response['text'].strip() and 'http' in response['text'], response
    assert any(r['status'] == 'completed' for r in external)
    assert not any(r['tool_name'] == 'inspect_retained_evidence' and r['status'] == 'failed' for r in results), results
    page_count = 0
    for row in external:
        if row['tool_name'] != 'browser_read_pages': continue
        output = row.get('output') or {}
        if 'completed_pages' in output:
            page_count += output['completed_pages']
        elif isinstance(output.get('preview'), str):
            # Compatibility with installed durable audit previews; live facts
            # are still checked in the native completed receipt, never prose.
            import re
            match = re.search(r'"completed_pages":\s*(\d+)', output['preview'])
            assert match, output
            page_count += int(match[1])
    proof = {'prompt': prompt, 'elapsed_ms': elapsed, 'page_reads': page_count, 'turn': turn, 'response': response,
        'research_budget': _research_budget(live_web),
        'clarification_turn_id': clarification_turn_id}
    target = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    target.mkdir(parents=True, exist_ok=True)
    import hashlib
    name = hashlib.sha256(prompt.encode()).hexdigest()[:12]
    (target/f'research-{name}.json').write_text(json.dumps(proof, ensure_ascii=False, indent=2))
    live_web[0].screenshot(path=str(target/f'research-{name}.png'), full_page=True)
    assert page_count <= 16, f'Ordinary bounded research expanded to {page_count} batch reads'
    assert elapsed < 150_000, elapsed
    record_property('research_seconds', elapsed/1000)
    record_property('research_page_reads', page_count)


def test_live_stage_readonly_deadline_summary_keeps_account_findings(live_web, record_property):
    response, turn, elapsed = _submit(live_web,
        'Check my recent email for upcoming dated obligations and compare them with my calendar. '
        'Read up to ten relevant messages and give a concise summary with the dates, amounts when present, '
        'and whether matching calendar entries already exist. This is read-only: do not create or change '
        'calendar entries, send email, or create files.', timeout=180)
    results = turn.get('tool_results') or []
    assert any(r['tool_name'] in {'email_read','connector_request'} and r['status']=='completed' for r in results), results
    assert any(r['tool_name'] in {'calendar_list','connector_request'} and r['status']=='completed' for r in results), results
    assert not any(r['tool_name'] in {'calendar_create','calendar_update','email_send'} for r in results), results
    assert response['text'].strip() and turn.get('response_fulfilled') is True, {
        'reply':response['text'][:1500], 'fulfilled':turn.get('response_fulfilled'),
        'tools':[(r['tool_name'],r['status']) for r in results]}
    assert not response.get('artifacts')
    target = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    (target/'deadline-summary.json').write_text(json.dumps({'elapsed_ms':elapsed,'response':response,'turn':turn,
        'research_budget':_research_budget(live_web)},ensure_ascii=False,indent=2))
    live_web[0].screenshot(path=str(target/'deadline-summary.png'),full_page=True)
    assert elapsed < 150000, elapsed
    record_property('deadline_summary_seconds',elapsed/1000)


def test_live_stage_research_then_artifact_preserves_source_and_delivers_file(live_web, record_property):
    from tests.e2e.test_live_stage_web_matrix import catalogue
    root = os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT']
    response, turn, elapsed = _submit(live_web,
        'Read https://example.com and verify its title and description. Then create a compact, standalone HTML '
        'report containing those verified facts and a link to that source. '
        'Save and attach the HTML file at '+root+'/source-report.html. No account access is needed.', timeout=240)
    results = turn.get('tool_results') or []
    assert any(row['tool_name'] in {'web_fetch','browser_navigate','browser_read_pages'}
        and row['status'] == 'completed' for row in results), results
    artifacts = response.get('artifacts') or []
    assert len(artifacts) == 1 and catalogue._artifact_name(artifacts[0]).endswith('.html'), response
    data, _ = catalogue._artifact_bytes(live_web[5], artifacts[0], timeout=30)
    text = data.decode('utf-8').lower()
    assert 'example domain' in text, text
    report = live_web[0].context.new_page()
    try:
        report.set_content(data.decode('utf-8'))
        assert report.locator('a[href="https://example.com/"], a[href="https://example.com"]').count() >= 1
        report.screenshot(path=str(Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'source-report-rendered.png'))
    finally:
        report.close()
    assert turn.get('response_fulfilled') is True, turn
    assert not any(row['tool_name'] == 'file_write' and row['status'] == 'failed' for row in results), results
    target = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    (target/'source-report.html').write_bytes(data)
    (target/'source-report.json').write_text(json.dumps({'elapsed_ms':elapsed, 'response':response, 'turn':turn,
        'research_budget':_research_budget(live_web)}, ensure_ascii=False, indent=2))
    live_web[0].screenshot(path=str(target/'source-report.png'), full_page=True)
    assert elapsed < 180000, elapsed
    record_property('source_report_seconds', elapsed/1000)
