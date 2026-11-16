"""Small real-provider Web UI latency/quality acceptance for rendered research."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in live stage quota')


def test_live_visual_page_and_text_use_shared_production_and_generic_preparation(live_web, record_property):
    from urllib.parse import urljoin
    page, frames, errors, database, conversation, base = live_web
    frame_start = len(frames)
    response, turn, elapsed = _submit(live_web,
        'Create and attach one readable single-page PNG itinerary using only these supplied facts. '
        'Fixed commitments: Saturday 1:30 PM lunch, 3:00 PM hotel check-in, 5:00 PM evening event; '
        'Sunday 2:45 PM train. Keep those times unchanged. Optional Sunday plan: 10:00 AM garden walk, '
        '12:30 PM lunch near the station. Use separate panels with wrapped descriptions, generous '
        'margins and no text overlap. Do not browse websites, access accounts or change a calendar. '
        'Inspect the final image before delivering it.', timeout=220)
    assert response.get('artifacts'), response
    assert turn.get('response_fulfilled') is True, turn
    tools = turn.get('tool_results', [])
    assert any(row['tool_name']=='file_read' and row['status']=='completed' for row in tools), tools
    assert not any(row['tool_name'].startswith(('calendar_', 'email_', 'browser_', 'web_')) for row in tools), tools
    root = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR', '/tmp/live-web-evidence'))
    root.mkdir(parents=True, exist_ok=True)
    delivered = []
    for artifact in response['artifacts']:
        assert str(artifact.get('filename') or artifact.get('name') or artifact['url']).lower().endswith('.png'), artifact
        data = page.request.get(urljoin(base, artifact['url'])).body()
        assert data.startswith(b'\x89PNG\r\n\x1a\n')
        path = root/('visual-page-'+str(len(delivered))+'.png')
        path.write_bytes(data)
        delivered.append(str(path))
    plain_response, plain_turn, plain_elapsed = _submit(live_web,
        'Using only the supplied itinerary, what time is the Sunday train? One sentence, no tools.', timeout=45)
    assert '2:45' in plain_response['text'] and not plain_response.get('artifacts'), plain_response
    assert not plain_turn.get('tool_results'), plain_turn
    notices = [row.get('text', '') for row in frames[frame_start:] if row.get('type')=='progress']
    assert not any('Preparing files for delivery.' in notice for notice in notices), notices
    assert not errors
    (root/'visual-page-live.json').write_text(json.dumps({'conversation_id':conversation,
        'elapsed_ms':elapsed, 'plain_elapsed_ms':plain_elapsed, 'reply':response['text'],
        'plain_reply':plain_response['text'], 'notices':notices, 'artifacts':delivered,
        'tools':[(row['tool_name'], row['status']) for row in tools]}, ensure_ascii=False, indent=2))
    record_property('visual_page_latency_ms', elapsed)
    assert elapsed < 220000 and plain_elapsed < 30000


def test_live_history_lookup_updates_the_shared_card_without_fresh_source_work(live_web, record_property):
    page, frames, errors, database, conversation, base = live_web
    page.evaluate("""() => {
        window.historyProgressProbe = [];
        window.historyProgressNodes = new WeakSet();
        new MutationObserver(() => {
            for (const node of document.querySelectorAll('#messages .status-bubble')) {
                window.historyProgressNodes.add(node);
                const text = node.innerText;
                if (text && !window.historyProgressProbe.includes(text)) window.historyProgressProbe.push(text);
            }
        }).observe(document.querySelector('#messages'), {subtree:true, childList:true, characterData:true});
    }""")
    response, turn, elapsed = _submit(live_web,
        'Use chat_history_search to find the earlier saved Sala Thai reservation availability '
        'for Monday November 16, 2026 for two people. Give the saved times and source link. '
        'Do not check any live websites; this is only a saved-history lookup.', timeout=100)
    tools = turn.get('tool_results', [])
    assert any(r['tool_name']=='chat_history_search' and r['status']=='completed' for r in tools), turn
    assert not any(r['tool_name'].startswith(('browser_', 'web_')) for r in tools), turn
    assert 'https://' in response['text'] and any(t in response['text'] for t in ('6:00', '6 PM')), response
    notices = page.evaluate('window.historyProgressProbe')
    assert any('saved information' in text for text in notices), notices
    # Completion replaces the provisional card with the actual answer in the
    # same node. A fleeting English "Completed" status is not required.
    bubble=page.locator('#messages .msg.bot .bubble[data-turn-id='+json.dumps(turn['turn_id'])+']')
    assert bubble.count()==1 and bubble.is_visible()
    assert bubble.evaluate('(node) => window.historyProgressNodes.has(node)')
    assert not bubble.evaluate('(node) => node.classList.contains("status-bubble") || node.classList.contains("typing-bubble")')
    assert (bubble.get_attribute('data-raw-text') or '').strip()==turn['assistant_reply'].strip()
    assert not errors and not response.get('artifacts')
    root = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))
    (root/'history-progress-live.json').write_text(json.dumps({
        'conversation_id':conversation, 'elapsed_ms':elapsed, 'notices':notices,
        'tools':[(r['tool_name'],r['status']) for r in tools], 'reply':response['text'],
    }, ensure_ascii=False, indent=2))
    record_property('saved_history_lookup_ms', elapsed)
    assert elapsed < 60000, elapsed


def test_live_comparison_uses_visible_quantities_without_an_extra_tool_round(live_web, record_property):
    response, turn, elapsed = _submit(live_web,
        'Compare these supplied facts without looking anything up: A is 20 g for $6, B is 4 g for $3. '
        'Both A and B have identical specifications. C is 20 g for $2 but has different specifications '
        'and is not interchangeable. Give the A:B quantity ratio, cost per gram and the better value '
        'among equivalent options. Distinguish C. Keep it short.', timeout=45)
    text = response['text']
    assert any(value in text for value in ('5:', '5×', '5 ×', '5 times', '5x')), response
    assert '0.30' in text and '0.75' in text and 'C' in text, response
    assert not turn.get('tool_results') and not response.get('artifacts'), turn
    assert elapsed < 30000, elapsed
    record_property('comparison_latency_ms', elapsed)


def test_live_availability_reconsiders_sources_before_giving_up(live_web, record_property):
    page, frames, errors, database, conversation, base = live_web
    started = datetime.now(timezone.utc).isoformat()
    response, turn, elapsed = _submit(live_web,
        'Find available dinner reservation times at Sala Thai in Manhattan for 2 people on '
        'Monday November 16, 2026 around 6 PM. Only check availability; do not reserve or book anything.', timeout=240)
    results = turn.get('tool_results', [])
    assert any(row['status']=='completed' and row['tool_name'].startswith(('browser_', 'web_'))
        for row in results), turn
    # This acceptance checks actual availability, not a reassuring fallback.
    assert any(value in response['text'] for value in ('6:00', '6 PM', '6 pm', '5:30', '6:30')), response
    assert not any(value in response['text'].lower() for value in (
        'couldn’t verify', "couldn't verify", 'could not verify', 'unable to verify',
        'couldn’t confirm', "couldn't confirm", 'could not confirm', 'no verified availability',
    )), response
    source_text = json.dumps([r.get('output', {}) for r in results], ensure_ascii=False)
    assert any(value in source_text for value in ('6:00', '5:30', '6:30')), source_text[-2000:]
    assert 'November 16' in source_text and 'for a party of 2' in source_text, source_text[-2000:]
    assert 'https://' in response['text'] and not response.get('artifacts'), response
    timings = [row for row in catalogue._conversation_event_payloads(
        database, conversation, event_type='conversation.model_timing') if row.get('created_at', '') >= started]
    root = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))
    (root/'source-recovery-live-metrics.json').write_text(json.dumps({
        'conversation_id': conversation, 'elapsed_ms': elapsed, 'reply': response['text'],
        'tools': [(r['tool_name'], r['status']) for r in results],
        'model_context_chars': [r.get('text_chars') for r in timings],
    }, ensure_ascii=False, indent=2))
    record_property('availability_latency_ms', elapsed)
    assert elapsed < 180000 and not errors, (elapsed, errors)


def test_live_compact_plain_turns_keep_one_model_call_and_fast_feedback(live_web, record_property):
    import time
    from tests.e2e.test_live_stage_web_matrix import watch_chat_frames
    page, frames, errors, database, conversation, base = live_web
    samples=[];sizes=[];notice_times=[]
    for prompt in ['Bonjour', 'こんにちは', 'Hola']:
        seen=[];started=time.monotonic()
        watch_chat_frames(page,[],errors,base,on_frame=lambda frame:
            seen.append(time.monotonic()-started) if frame.get('type')=='progress'
            and 'I can handle multiple at once' in frame.get('text','') else None)
        before=datetime.now(timezone.utc).isoformat()
        response,turn,elapsed=_submit(live_web,prompt,timeout=45)
        samples.append(elapsed)
        assert response['text'].strip() and not response.get('artifacts') and not turn.get('tool_results'),response
        timings=[row for row in catalogue._conversation_event_payloads(database,conversation,event_type='conversation.model_timing')
                 if row.get('created_at','')>=before]
        assert len(timings)==1,timings
        sizes.append(timings[0]['text_chars'])
        if seen:notice_times.append(seen[0])
    metrics={'conversation_id':conversation,'plain_latency_ms':samples,'model_context_chars':sizes,
             'initial_progress_seconds':notice_times}
    root=Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))
    (root/'compact-turn-metrics.json').write_text(json.dumps(metrics,ensure_ascii=False,indent=2))
    record_property('mean_plain_latency_ms',sum(samples)/len(samples))
    record_property('max_plain_latency_ms',max(samples))
    record_property('max_initial_model_context_chars',max(sizes))
    assert sum(samples)/len(samples)<20000 and max(samples)<30000,metrics
    assert max(sizes)<24000 and (not notice_times or max(notice_times)<3.5),metrics
    assert not errors


def test_live_source_audit_uses_retained_native_composite_reads(live_web,record_property):
    import re
    page,frames,errors,database,conversation,base=live_web
    first,turn,_=_submit(live_web,
        'Use web_search to locate https://example.com, then read that exact page with browser_run_task. '
        'Return its first visible paragraph. Do not create files.',timeout=90)
    # This acceptance covers retained execution provenance. The live page no
    # longer has the historical heading; source-field recovery is a separate
    # contract and must not prevent exercising the actual audit follow-up.
    assert first['text'].strip(),first
    sources=turn.get('tool_results',[])
    assert any(r['tool_name']=='web_search' and r['status']=='completed' for r in sources),sources
    assert any(r['tool_name']=='browser_run_task' and r['status']=='completed'
        and r.get('response_requirements',{}).get('_native_completed_reads',{}).get('read_only') is True
        for r in sources),sources
    second,audit,_=_submit(live_web,
        'Did that check open https://example.org? Answer this source audit from the recorded actions only. '
        'Do not make any new source checks.',timeout=45)
    assert re.search(r'\bno\b|did not|didn.t',second['text'],re.I),second
    assert 'source snippets' not in second['text'] and not second.get('artifacts'),second
    policies=catalogue._conversation_event_payloads(database,conversation,event_type='conversation.observation_policy')
    assert policies and policies[-1]['retained_read_available'] is True,policies
    record_property('retained_scope_valid',policies[-1]['scope_valid'])
    if policies[-1]['scope_valid']:
        assert policies[-1]['evidence_policy']=='retained' and policies[-1]['retained_read_count']>=2,policies
    # An optional classifier timeout is allowed to fall back. The required
    # outcome is the correct audit answer without reopening a source.
    assert audit['parent_turn_id']==turn['turn_id'],audit
    assert all(r['invocation_id'].startswith('continuation:') for r in audit.get('tool_results',[])
        if r['tool_name'] in {'web_search','browser_run_task','browser_read_pages'}),audit
    assert not errors


def test_live_screenshot_followup_keeps_the_current_page_context(live_web, record_property):
    import io
    from urllib.parse import urljoin
    from PIL import Image
    page, frames, errors, database, conversation, base = live_web
    response, turn, elapsed = _submit(live_web,
        'Open https://example.com in the configured browser and tell me its main heading. Keep the page open.', timeout=90)
    assert 'Example Domain' in response['text'], response
    assert any(r['tool_name']=='browser_navigate' and r['status']=='completed'
        for r in turn.get('tool_results', [])), turn
    response, turn, followup_elapsed = _submit(live_web, 'Send me screenshot', timeout=120)
    artifacts = response.get('artifacts', [])
    assert artifacts and 'Which request' not in response['text'], response
    assert any(r['tool_name']=='browser_screenshot' and r['status']=='completed'
        for r in turn.get('tool_results', [])), turn
    downloaded = False
    for artifact in artifacts:
        data = page.request.get(urljoin(base, artifact['url'])).body()
        image = Image.open(io.BytesIO(data)); image.load()
        assert image.width >= 100 and image.height >= 100
        root = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))
        (root/'current-page-screenshot.png').write_bytes(data)
        downloaded = True
    assert downloaded and not errors
    record_property('page_latency_ms', elapsed)
    record_property('followup_latency_ms', followup_elapsed)


def test_live_research_uses_short_reads_and_keeps_plain_and_weather_turns_fast(live_web, record_property):
    page, frames, errors, database, conversation, base = live_web
    simple = []
    for prompt in ['Réponds uniquement : prêt', '次の一語だけ返してください：準備完了']:
        response, turn, elapsed = _submit(live_web, prompt, timeout=45)
        simple.append(elapsed)
        assert response['text'].strip() and not turn.get('tool_results'), turn
    assert sum(simple)/len(simple) < 12000 and max(simple) < 20000, simple
    research_started = datetime.now(timezone.utc).isoformat()
    response, turn, elapsed = _submit(live_web,
        'Find me some protein full healthy fast food around me. I want ubereat that. No salad', timeout=140)
    tools = turn.get('tool_results', [])
    timings = [row for row in catalogue._conversation_event_payloads(database,conversation,event_type='conversation.model_timing') if row.get('created_at','') >= research_started]
    native = catalogue._conversation_event_payloads(database,conversation,event_type='conversation.tool_timing')
    inner = [row for row in native if row.get('tool_name') == 'browser_run_task' and row.get('created_at','') >= research_started]
    primary_calls, inner_calls = len(timings), sum(row.get('performance',{}).get('model_calls',0) for row in inner)
    usage = {}
    for source in [row.get('usage',{}) for row in timings] + [row.get('performance',{}) for row in inner]:
        for key in ('input_tokens','output_tokens','total_tokens','reasoning_tokens'):
            if type(source.get(key)) is int:
                usage[key] = usage.get(key,0) + source[key]
    metrics = {'conversation_id':conversation,'plain_latency_ms':simple,'research_latency_ms':elapsed,
        'primary_model_calls':primary_calls,'inner_model_calls':inner_calls,'recorded_usage':usage,
        'tools':[row['tool_name'] for row in tools], 'research_reply':response['text']}
    root = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))
    (root/'batched-research-metrics.json').write_text(json.dumps(metrics,ensure_ascii=False,indent=2))
    record_property('research_latency_ms',elapsed)
    record_property('research_model_calls',primary_calls+inner_calls)
    assert any(row['tool_name'] in {'browser_read_pages','browser_extract_items','browser_extract_text','web_fetch'} and row['status']=='completed' for row in tools), metrics
    assert 'https://' in response['text'] and len(response['text'])>300, response
    assert primary_calls+inner_calls <= 10 and elapsed < 100000, metrics
    assert not response.get('artifacts') and not errors
    # An explicit batch probe verifies the new tool on real public pages too.
    batch_response, batch_turn, batch_elapsed = _submit(live_web,
        'Use browser_read_pages to read https://example.com and https://example.org as independent pages in the configured browser. Give each page heading and its exact information link. Do not create files.',timeout=90)
    assert any(row['tool_name']=='browser_read_pages' and row['status']=='completed' for row in batch_turn.get('tool_results',[])), batch_turn
    record_property('independent_batch_latency_ms',batch_elapsed)
    assert 'https://' in batch_response['text'] and batch_elapsed < 45000, batch_response
    # Adjacent native-tool family must remain direct after browser research.
    response, turn, weather_elapsed = _submit(live_web,'Check tomorrow’s weather near my saved home location. Give a short forecast.',timeout=90)
    assert any(row['tool_name']=='weather_forecast' and row['status']=='completed' for row in turn.get('tool_results',[])), turn
    assert not any(row['tool_name'] in {'browser_read_pages','browser_run_task'} for row in turn.get('tool_results',[])), turn
    record_property('adjacent_weather_latency_ms',weather_elapsed)
    assert weather_elapsed < 45000 and not errors, weather_elapsed


def test_live_text_reader_preserves_exact_links(live_web):
    response, turn, elapsed = _submit(live_web,
        'Open https://example.com in the configured browser and read it with browser_extract_text. Return its heading and exact information link from the visible link index. No files.', timeout=75)
    reads = [row for row in turn.get('tool_results',[]) if row['tool_name']=='browser_extract_text' and row['status']=='completed']
    assert reads and reads[-1]['output'].get('links'), turn
    assert any(link['url'] in response['text'] for link in reads[-1]['output']['links']), response
    assert 'Example Domain' in response['text'] and not response.get('artifacts') and elapsed < 45000


def test_live_native_reads_show_meaningful_shared_progress(live_web):
    page, frames, errors, database, conversation, base = live_web
    # Measure the rendered notice from the real send action. _submit performs
    # an evidence lookup before pressing Enter; that database latency is not
    # part of the user's wait for acknowledgement.
    page.evaluate("""() => {
        window.noticeProbe = {start: null, times: []};
        document.querySelector('#user-input').addEventListener('keydown', e => {
            if (e.key === 'Enter') window.noticeProbe.start = performance.now();
        });
        new MutationObserver(() => {
            const p = window.noticeProbe;
            if (p.start !== null && !p.times.length &&
                [...document.querySelectorAll('#messages .status-bubble')].some(
                    node => node.innerText.includes('I can handle multiple at once'))) {
                p.times.push((performance.now() - p.start) / 1000);
            }
        }).observe(document.querySelector('#messages'), {subtree: true, childList: true, characterData: true});
    }""")
    response, turn, elapsed = _submit(live_web,
        'Use browser_read_pages to read https://example.com and https://example.org as independent pages. '
        'Give both page headings and their exact information links. Do not create files.', timeout=90)
    reads = [r for r in turn.get('tool_results', []) if r['tool_name']=='browser_read_pages' and r['status']=='completed']
    assert reads and any(r['output'].get('completed_pages')==2 for r in reads), turn
    progress = [f for f in frames if f.get('type')=='progress' and '2 pages read' in f.get('text','')]
    assert progress, frames
    assert 'https://' in response['text'] and not errors and not response.get('artifacts')
    first_notices=page.evaluate('window.noticeProbe.times')
    assert first_notices and first_notices[0]<3.5, first_notices
    assert elapsed < 60000


def test_live_readable_news_uses_shared_native_read_policy(live_web, record_property):
    response, turn, elapsed = _submit(live_web, 'Whats latest on cnn?', timeout=100)
    results = turn.get('tool_results', [])
    reads = [row for row in results if row.get('status') == 'completed' and (
        row.get('tool_name') in {'browser_read_pages','browser_extract_text','browser_extract_items','web_fetch'}
        or row.get('output', {}).get('strategy', {}).get('kind') == 'native_read_preflight')]
    assert reads, turn
    for row in results:
        if row.get('tool_name') == 'browser_run_task':
            assert row.get('output', {}).get('strategy', {}).get('adaptive_browser_started') is False, row
    assert len(response['text']) > 200 and 'https://' in response['text'] and not response.get('artifacts'), response
    record_property('news_latency_ms', elapsed)
    assert elapsed < 60000, (elapsed, response)


def test_live_research_preserves_schedule_and_delivers_generated_page(live_web, record_property):
    from urllib.parse import urljoin
    calendar_response, calendar_turn, _ = _submit(live_web,
        'Read my calendar for November 21 and 22, 2026. Show my fixed commitments and do not change anything.', timeout=100)
    assert any(row['tool_name']=='calendar_list' and row['status']=='completed'
        for row in calendar_turn.get('tool_results',[])), calendar_turn
    response, turn, elapsed = _submit(live_web,
        'For sat Nov 21 trip, i want you to plan a trip with nice restaurants, pick the best ones '
        'and things to do around. Create a infographic style page. Keep whatever is scheduled as it is', timeout=320)
    artifacts = response.get('artifacts', [])
    if not artifacts:
        # A normal user can resolve optional output-format ambiguity without
        # abandoning the original research/file objective or its source state.
        response, turn, extra_elapsed = _submit(live_web,
            'Use the Poughkeepsie commitments just read from my calendar for November 21–22. '
            'Keep every scheduled time unchanged. Choose the additional restaurants and activities '
            'and create the finished infographic as a shareable HTML file. Attach it here.', timeout=320)
        elapsed += extra_elapsed
        artifacts = response.get('artifacts', [])
    assert artifacts, {'elapsed_ms':elapsed,'response':response,'tools':[(r['tool_name'],r['status']) for r in turn.get('tool_results',[])]}
    assert any(r['tool_name'] in {'file_write','terminal_exec','image_generate','document_create','pdf_create'}
        and r['status']=='completed' for r in turn.get('tool_results',[])), turn
    assert not any(r['tool_name'] in {'calendar_create','calendar_update','calendar_delete','email_send'}
        for r in turn.get('tool_results',[])), turn
    page, frames, errors, database, conversation, base = live_web
    root = Path(os.environ.get('NULLION_LIVE_EVIDENCE_DIR','/tmp/live-web-evidence'))
    for artifact in artifacts:
        data = page.request.get(urljoin(base,artifact['url'])).body()
        assert len(data)>500
        name = Path(artifact.get('filename') or artifact.get('name') or 'delivered-page').name
        (root/name).write_bytes(data)
    record_property('artifact_latency_ms',elapsed)
    assert not errors
