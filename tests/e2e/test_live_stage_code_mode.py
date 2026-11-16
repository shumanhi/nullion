"""Opt-in real-provider Web UI acceptance for stage Code Mode sessions."""
import os
import time

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit as _submit_ui, _turns

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1',
    reason='Explicit live stage acceptance only')


def _submit(live_web,prompt,**kwargs):
    response,turn,elapsed=_submit_ui(live_web,prompt,**kwargs)
    if turn.get('evidence_kind')=='deferred_runtime_checkpoint':
        # UI delivery precedes the secondary audit flush. Wait for its exact
        # turn receipt without resending the request or changing UI latency.
        page,_,_,database,conversation,_=live_web
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            saved=next((row for row in _turns(database,conversation) if row.get('turn_id')==turn['turn_id']),None)
            if saved:
                return response,{**saved,'delivered_reply':turn.get('delivered_reply',saved.get('assistant_reply'))},elapsed
            page.wait_for_timeout(250)
        pytest.fail('The visible result has no durable tool receipt after 60 seconds')
    return response,turn,elapsed


def test_live_stage_pdf_email_shows_native_approval_without_sending(live_web):
    """Inspect a real pending draft, then deny only this probe-owned action."""
    from io import BytesIO
    from pathlib import Path
    from pypdf import PdfReader
    from playwright.sync_api import expect
    from scripts.live_stage_provider_guard import process_provider_guard
    page,frames,errors,database,conversation,base=live_web
    prompt=('Use code_mode to send an email to nullion-validation@example.invalid saying '
        'hi how are you in a one-page PDF. Use the normal native email confirmation.')
    before=len(frames);started=time.monotonic();terminal=None;approval_id=None
    provider_guard=process_provider_guard()
    page.locator('#user-input').fill(prompt);page.locator('#user-input').press('Enter')
    try:
        while time.monotonic()-started<180:
            candidates=[f for f in frames[before:] if f.get('type') in {'done','error','approval_required'}
                and f.get('conversation_id',conversation)==conversation and not f.get('superseded')]
            if candidates:terminal=candidates[-1];break
            if provider_guard.check_pending:
                blocked=provider_guard.poll()
                if blocked:pytest.exit(blocked,returncode=1)
            page.wait_for_timeout(100)
        assert terminal and terminal['type']=='approval_required',terminal
        assert 'nullion-validation@example.invalid' in terminal.get('tool_detail',''),terminal
        approval_id=terminal['approval_id']
        card=page.locator('.approval-bubble').filter(has_text='nullion-validation@example.invalid').last
        expect(card).to_be_visible()
        artifacts=terminal.get('artifacts') or []
        assert len(artifacts)==1 and artifacts[0]['name'].endswith('.pdf'),terminal
        content=page.request.get(base+artifacts[0]['url']).body()
        assert content.startswith(b'%PDF') and len(PdfReader(BytesIO(content)).pages)==1
        page.screenshot(path=str(Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'pdf-email-native-approval.png'),full_page=True)
        deadline=time.monotonic()+45;turn=None
        while time.monotonic()<deadline:
            turn=next((t for t in _turns(database,conversation) if t.get('user_message')==prompt),None)
            if turn:break
            page.wait_for_timeout(200)
        assert turn, 'Missing native approval evidence for this exact request'
        results=turn.get('tool_results') or []
        assert any(r['tool_name']=='code_mode' and r['output'].get('reason')=='code_mode_awaiting_approval' for r in results)
        sends=[r for r in results if r['tool_name']=='email_send']
        assert sends and all(r['status']=='denied' and r['output'].get('reason')=='approval_required' for r in sends)
        assert any(r['output'].get('approval_id')==approval_id for r in sends)
        assert not errors
    finally:
        if approval_id:
            # Never approve a live email during validation. This id was bound
            # above to our exact recipient and request; existing user drafts
            # and their saved VM continuations are left untouched.
            card=page.locator('.approval-bubble').filter(has_text='nullion-validation@example.invalid').last
            card.get_by_role('button',name='Deny',exact=True).click()
        elif terminal is None:
            page.locator('#user-input').fill('/stop');page.locator('#user-input').press('Enter')


def test_live_stage_plain_turns_keep_the_fast_path(live_web,record_property):
    samples=[]
    for prompt,marker in [('Reply with only ALPHA37.','ALPHA37'),
            ('Responde solo BRAVO37.','BRAVO37'),('CHARLIE37だけ返してください。','CHARLIE37')]:
        response,turn,elapsed=_submit(live_web,prompt,timeout=60)
        assert marker in response['text'],response
        assert not turn.get('tool_results'),turn.get('tool_results')
        samples.append(elapsed/1000)
    assert sum(samples)/len(samples)<15 and max(samples)<30,samples
    record_property('mean_plain_turn_seconds',sum(samples)/len(samples))
    record_property('max_plain_turn_seconds',max(samples))


def test_live_same_pdf_keeps_its_bytes_and_thread_identity(live_web,record_property):
    import hashlib
    from uuid import uuid4
    name='retained_flight_'+uuid4().hex[:10]+'.pdf'
    first,created,_=_submit(live_web,
        'Create a one-page PDF named '+name+' from these fictional test records: '
        'Flight comparison. Trip A: June 10-14, 2030, $137. Trip B: June 17-21, 2030, $173. '
        'Include both trips and prices exactly. Attach only that PDF. No research is needed.',timeout=120)
    assert len(first['artifacts'])==1,first
    page,_,_,_,_,base=live_web
    before=page.request.get(base+first['artifacts'][0]['url']).body()
    proof=hashlib.sha256(before).hexdigest()
    response,turn,elapsed=_submit(live_web,
        'Keep the exact trips already in that PDF. Do not repeat research or regenerate it. '
        'Confirm the dates it covers and attach that same PDF.',timeout=90)
    assert turn.get('parent_turn_id')==created.get('turn_id'),turn
    assert len(response['artifacts'])==1 and response['artifacts'][0]['name']==name,response
    after=page.request.get(base+response['artifacts'][0]['url']).body()
    assert hashlib.sha256(after).hexdigest()==proof
    results=turn.get('tool_results') or []
    assert not any(r['tool_name'] in {'web_search','web_fetch','browser_run_task','pdf_create','pdf_edit','terminal_exec','file_write'}
        for r in results),[(r['tool_name'],r['status']) for r in results]
    assert elapsed<30000,elapsed
    record_property('retained_pdf_seconds',elapsed/1000)


def test_live_stage_read_only_code_mode(live_web):
    response, turn, elapsed = _submit(live_web,
        'Use code_mode with browser_read_pages to read https://example.com/ and '
        'https://www.iana.org/help/example-domains. Use one Python program to collect '
        'the page findings, then explain what these example domains are for with source links. '
        'This is read-only; do not create files or modify either site.', timeout=180)
    results = turn.get('tool_results') or []
    composed = [r for r in results if r.get('tool_name') == 'code_mode']
    assert composed and composed[-1]['status'] == 'completed', results
    reads = composed[-1]['output']['reads']
    assert reads and all(r['status'] == 'completed' for r in reads), reads
    native = {r['invocation_id']: r for r in results if r.get('tool_name') == 'browser_read_pages'}
    assert all(r['invocation_id'] in native for r in reads), (reads, native)
    assert 'example' in response['text'].lower() and 'https://' in response['text']
    assert not response['artifacts']


def test_live_stage_output_projection_preserves_native_findings(live_web, record_property):
    """A rejected display pointer must not restart real source execution."""
    import json
    response, turn, elapsed = _submit(live_web,
        'This is a read-only recovery check. Run one code_mode call with tool_names=["browser_read_pages"], '
        'code exactly as follows:\n'
        'saved = call_tool("browser_read_pages", {"pages":[{"url":"https://www.iana.org/help/example-domains",'
        '"read_kind":"text"}],"independent_pages":True})\n'
        'saved["output"]\n'
        'Set output_fields=["/output"]. That deliberately incorrect display pointer tests preservation '
        'of the successfully read result. Use the returned native observations or preserved value to '
        'briefly explain what example domains are for, with the source URL. Do not run another '
        'program to fix formatting, reread the page, create files, or access accounts.', timeout=150)
    results = turn.get('tool_results') or []
    programs = [row for row in results if row.get('tool_name') == 'code_mode']
    assert len(programs) == 1 and programs[0]['status'] == 'completed', results
    output = programs[0]['output']
    assert output['projection_error']['repair_scope'] == 'output_projection', output
    reads = [row for row in results if row.get('tool_name') == 'browser_read_pages']
    assert len(reads) == 1 and reads[0]['status'] == 'completed', reads
    if output.get('native_observations'):
        assert output['native_observations'][0]['inspection_request']['invocation_id'] == reads[0]['invocation_id']
    else:
        # The shared compactor drops duplicate observations when the exact
        # successful value fits. Either representation must remain useful.
        assert output['value']['completed_pages'] == 1, output
        assert output['omitted_native_observation_count'] >= 1, output
    assert 'example' in response['text'].lower() and 'https://www.iana.org/' in response['text'], response
    assert not response['artifacts'] and turn.get('response_fulfilled') is True
    assert not any(row.get('tool_name') in {'inspect_retained_evidence', 'browser_run_task'} for row in results)
    assert 'invalid_output_fields' in json.dumps(output)
    record_property('projection_handoff_seconds', elapsed/1000)


def test_live_stage_email_uses_code_session(live_web):
    response, turn, elapsed = _submit(live_web,
        'What is latest in my email? Give me a brief summary of the newest messages. '
        'Only read my mailbox; do not send, delete or mark anything.', timeout=180)
    results = turn.get('tool_results') or []
    composed = [r for r in results if r.get('tool_name') == 'code_mode']
    assert composed and all(r['status'] == 'completed' for r in composed), results
    reads = [read for r in composed for read in r['output']['reads']]
    assert any(r['tool_name'] == 'email_search' for r in reads), reads
    native = {r['invocation_id']:r for r in results}
    assert all(r['invocation_id'] in native for r in reads)
    assert response['text'] and not response['artifacts']



def test_live_stage_reservation_composes_reads(live_web, record_property):
    import json
    from pathlib import Path
    from urllib.request import Request, urlopen
    from urllib.parse import urlencode
    response, turn, elapsed = _submit(live_web,
        os.environ.get('NULLION_LIVE_RESERVATION_PROMPT') or
        'Can you check if Sala Thai at 307 Amsterdam Ave has reservations available for 2 people '
        'around 6pm tomorrow? Only check availability; do not book anything.', timeout=240)
    results = turn.get('tool_results') or []
    programs = [r for r in results if r.get('tool_name') == 'code_mode']
    assert programs and any(r['status'] == 'completed' for r in programs), results
    assert any(len(r['output'].get('operations', r['output'].get('reads', []))) > 1 and r['status'] == 'completed' for r in programs), programs
    # These are real model-produced programs, not compatibility wrappers around
    # individual native tool selections from the previous execution strategy.
    assert any(not r['output'].get('model_tool_use_ids') for r in programs)
    assert response['text'] and not response['artifacts']
    _, _, _, _, conversation, _ = live_web
    settings = json.loads(Path(os.environ['NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION']).read_text())
    url = 'http://host.docker.internal:'+str(settings['port'])+'/conversation-events?'+urlencode({
        'conversation_id':conversation, 'event_type':'conversation.model_timing'})
    with urlopen(Request(url,headers={'Authorization':'Bearer '+settings['token']}),timeout=15) as wire:
        timings = json.load(wire)['events']
    record_property('model_roundtrips',len(timings))
    record_property('total_seconds',elapsed/1000)
    record_property('model_seconds',sum(r.get('duration_ms',0) for r in timings)/1000)
    assert len(timings) < 28, timings


def test_live_stage_inline_read_recovery_stays_in_one_program(live_web,record_property):
    response,turn,elapsed=_submit(live_web,
        'Verify the document title on https://example.com/ using one Code Mode program. '
        'First open the configured desktop browser using browser_open(mode="configured") and attempt navigation to https://nullion-recovery-check.invalid/ '
        'with call_tool(..., on_error="return"). Check the returned status. If that source fails, '
        'navigate to https://example.com/ and read document.title and location.href using browser_run_js '
        'in that same Python program. Do not depend on heading tags being present. '
        'Do not extract the failed source or create files. Return the verified title and its source URL.',timeout=180)
    results=turn.get('tool_results') or []
    programs=[r for r in results if r.get('tool_name')=='code_mode']
    assert len(programs)==1 and programs[0]['status']=='completed',programs
    operations=programs[0]['output']['operations']
    opened=[r for r in results if r.get('tool_name')=='browser_open' and r.get('status')=='completed']
    assert opened and all(r['output'].get('browser_mode')=='configured' for r in opened),opened
    assert any(r['tool_name']=='browser_navigate' and r['status']=='failed' for r in operations),operations
    assert any(r['tool_name']=='browser_navigate' and r['status']=='completed' for r in operations),operations
    assert any(r['tool_name']=='browser_run_js' and r['status']=='completed' for r in operations),operations
    assert 'Example Domain' in response['text'] and 'https://example.com' in response['text'],response
    assert not response['artifacts']
    record_property('total_seconds',elapsed/1000)


def test_live_stage_progress_is_compact_and_visibly_smaller(live_web,record_property):
    page,*_=live_web
    page.evaluate("""() => {
        window.__compactProgress=[];
        window.__progressObserver=new MutationObserver(() => {
            for (const node of document.querySelectorAll('.status-bubble.request-progress-bubble')) {
                const row={text:node.dataset.statusText,font:getComputedStyle(node).fontSize};
                if (row.text && JSON.stringify(window.__compactProgress.at(-1))!==JSON.stringify(row))
                    window.__compactProgress.push(row);
            }
        });
        window.__progressObserver.observe(document.querySelector('#messages'),{childList:true,subtree:true,characterData:true,attributes:true});
    }""")
    response,turn,elapsed=_submit(live_web,
        'Read https://pydantic.dev/docs/monty/concepts/host-functions/ and '
        'https://developers.cloudflare.com/agents/tools/codemode/ and briefly explain '
        'how code calls host tools, with source links. Read only these public pages; do not create files.',timeout=180)
    snapshots=page.evaluate('() => { window.__progressObserver.disconnect(); return window.__compactProgress; }')
    assert snapshots,snapshots
    assert all(row['font']=='12px' and len(row['text'])<=120 and '\n' not in row['text'] for row in snapshots),snapshots
    assert not page.locator('.status-bubble.request-progress-bubble').count()
    assert response['text'] and 'https://' in response['text']
    record_property('progress_snapshots',snapshots)
    record_property('total_seconds',elapsed/1000)


def test_live_stage_code_mode_writes_and_delivers_html(live_web):
    page, _, _, _, _, base = live_web
    marker = 'code-session-write-verified-37'
    response, turn, _ = _submit(live_web,
        'Create one self-contained .html page with a navy background, three clear sections, '
        'and the visible text code-session-write-verified-37. Use file_write through Code Mode: '
        'store the complete markup in a Python variable, then write the file once and deliver it. '
        'Do not use connected accounts, browse websites, or send messages.', timeout=180)
    results = turn.get('tool_results') or []
    programs = [r for r in results if r.get('tool_name') == 'code_mode' and r.get('status') == 'completed']
    assert any(any(op['tool_name'] == 'file_write' and op['status'] == 'completed'
        and op['operation_kind'] == 'write' for op in r['output']['operations']) for r in programs), results
    artifacts = response.get('artifacts') or []
    assert len(artifacts) == 1, response
    url = artifacts[0]['url']
    downloaded = page.request.get(url if url.startswith('http') else base+url, timeout=15000)
    assert downloaded.ok and marker in downloaded.text()
    assert '<html' in downloaded.text().lower() and '<section' in downloaded.text().lower()
    assert response['text']


def test_live_stage_recovers_capture_in_one_code_session(live_web):
    import io
    from PIL import Image
    page, _, _, _, _, base=live_web
    response,turn,_=_submit(live_web,
        'Capture https://example.com/ as a PNG screenshot. This is an existing-page capture, not image generation. '
        'When requesting scope, declare source_artifact_delivery_requested=true and use this exact source evidence: '
        'Capture https://example.com/ as a PNG screenshot. Use two Code Mode programs to verify recovery. '
        'In the first program, save a Python variable with value 37, open a fresh browser session, snapshot its '
        'blank page, and try browser_screenshot before navigating; expect the loaded-page precondition failure. '
        'In the corrected program reuse the saved Python variable and browser session, navigate to https://example.com/, '
        'then capture a viewport PNG. Return saved_number=37 together with its screenshot receipt from the program. '
        'Deliver only the final screenshot. Do not generate images, create HTML, run terminal commands or access connected accounts.',
        timeout=180)
    results=turn.get('tool_results') or []
    programs=[r for r in results if r.get('tool_name')=='code_mode']
    failed=[r for r in programs if any(f.get('tool_name')=='browser_screenshot' and f.get('status')=='failed'
        for f in r['output'].get('failures',[]))]
    done=[r for r in programs if r['status']=='completed']
    assert failed and done,(response,programs)
    assert len(programs)==2,programs
    assert failed[0]['output']['session_id']==done[-1]['output']['session_id']
    value=done[-1]['output']['value']
    # output_fields preserves the selected JSON pointer as its result key.
    assert value.get('saved_number', value.get('/saved_number'))==37,programs
    assert any(r['tool_name']=='browser_screenshot' and r['status']=='failed' for r in results)
    assert any(r['tool_name']=='browser_screenshot' and r['status']=='completed' for r in results)
    assert not any(r['tool_name'] in {'image_generate','terminal_exec'} for r in results)
    artifacts=response.get('artifacts') or [];assert len(artifacts)==1,response
    url=artifacts[0]['url'];download=page.request.get(url if url.startswith('http') else base+url,timeout=15000)
    assert download.ok and download.body().startswith(b'\x89PNG\r\n\x1a\n')
    with Image.open(io.BytesIO(download.body())) as picture:
        assert picture.width>200 and picture.height>100


def test_live_stage_assignment_only_program_returns_page_evidence(live_web):
    response, turn, _ = _submit(live_web,
        'Read https://example.com/ and https://www.iana.org/help/example-domains with '
        'browser_read_pages inside Code Mode, then summarize the purpose of example domains. '
        'For this execution check, assign the native result to a variable and end the Python program '
        'with that assignment: do not add a final expression or print. Use the returned native '
        'observations to answer, without fetching either page again.', timeout=180)
    results = turn.get('tool_results') or []
    programs = [r for r in results if r['tool_name'] == 'code_mode']
    assert programs and programs[-1]['status'] == 'completed', results
    assert all(r['status'] == 'completed' or not r['output']['operations'] for r in programs), programs
    assert any(isinstance(r['output'].get('value'), dict) and r['output']['value'].get('observations')
        for r in programs), programs
    reads = [r for r in results if r['tool_name'] == 'browser_read_pages']
    assert len(reads) == 1 and reads[0]['status'] == 'completed', reads
    assert 'example' in response['text'].lower() and not response['artifacts']


@pytest.mark.parametrize('repair', [False, True])
def test_live_stage_local_renderer_delivers_png_without_image_provider(live_web, repair):
    import io
    from PIL import Image
    page, _, _, _, _, base = live_web
    root = os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT']
    response, turn, _ = _submit(live_web,
        'Create a 640 by 480 PNG infographic with a navy background and three labeled boxes: '
        'Plan, Explore, Enjoy. Render it locally using Python and Pillow through terminal_exec '
        'inside Code Mode. Save it as ' + root + '/local-infographic.png and deliver the PNG. '
        'This is a layout task; do not call an image generation provider or browse websites. '
        'Use the installed Python that has Pillow available. ' + (
        'For this recovery check, first run a Code Mode program containing only import unavailable_example_library. '
        'Read its failure diagnostic, then continue in a corrected Code Mode program using '
        'terminal_exec for the installed Python renderer. Do not retry the unsupported sandbox import.'
        if repair else ''), timeout=180)
    results = turn.get('tool_results') or []
    assert not any(r['tool_name'] == 'image_generate' for r in results), results
    assert any(r['tool_name'] == 'terminal_exec' and r['status'] == 'completed' for r in results), results
    assert any(r['tool_name'] == 'code_mode' and r['status'] == 'completed' for r in results), results
    if repair:
        programs = [r for r in results if r['tool_name'] == 'code_mode']
        failures = [r for r in programs if r['output'].get('reason') == 'code_mode_python_error']
        assert len(failures) == 1 and 'unavailable_example_library' in failures[0]['output']['execution_error']['message'], programs
        assert failures[0]['output']['session_id'] == programs[-1]['output']['session_id']
        assert len([r for r in results if r['tool_name'] == 'terminal_exec' and r['status'] == 'completed']) == 1
    artifacts = response.get('artifacts') or []
    assert len(artifacts) == 1, response
    url = artifacts[0]['url']
    download = page.request.get(url if url.startswith('http') else base + url, timeout=15000)
    assert download.ok and download.body().startswith(b'\x89PNG\r\n\x1a\n')
    with Image.open(io.BytesIO(download.body())) as picture:
        assert picture.size == (640, 480)


@pytest.mark.parametrize('helper_style',['imported','direct'])
def test_live_stage_host_modules_create_and_deliver_html(live_web,record_property,helper_style):
    from urllib.parse import quote,quote_plus
    from html import escape
    from pathlib import Path
    import json
    page, _, _, _, _, base=live_web
    source='東京 & café <37>'
    helpers=(
        'Include an internal link whose href is # followed by the URL-encoded label. '
        'Use Code Mode with import urllib.parse, from html import escape, and from textwrap import dedent. '
        'Build the URL with urllib.parse.quote(label, safe=""), escape the heading using html.escape, and dedent '
        'the markup using textwrap.dedent. '
        if helper_style=='imported' else
        'Include a relative query link whose href is ?q= followed by quote_plus(label). '
        'Use Code Mode and call quote_plus(label) directly, without importing urllib or defining a replacement function. '
        'Use from html import escape and from textwrap import dedent for the heading and markup. ')
    output=os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT']+'/host-'+helper_style+'.html'
    response,turn,elapsed=_submit(live_web,
        'Create one downloadable HTML page with a navy background and a visible heading made from this exact label: '
        +source+'. '+helpers+'Save the completed HTML once through file_write at '+output+' and deliver it. '
        'These are declared host modules; use them directly in the same program. No terminal, network reads, '
        'connected accounts, or image-generation provider is needed.',timeout=180)
    results=turn.get('tool_results') or []
    programs=[r for r in results if r['tool_name']=='code_mode']
    assert programs and all(r['status']=='completed' for r in programs),programs
    calls={name:sum(r['output'].get('host_calls',{}).get(name,0) for r in programs)
        for name in ['urllib.parse.'+('quote' if helper_style=='imported' else 'quote_plus'),'html.escape','textwrap.dedent']}
    assert all(count>=1 for count in calls.values()),calls
    assert not any(r['tool_name'] in {'terminal_exec','image_generate'} for r in results),results
    assert len([r for r in results if r['tool_name']=='file_write'])==1
    artifacts=response.get('artifacts') or [];assert len(artifacts)==1,response
    url=artifacts[0]['url'];download=page.request.get(url if url.startswith('http') else base+url,timeout=15000)
    encoded=quote(source,safe='') if helper_style=='imported' else quote_plus(source)
    assert download.ok and escape(source) in download.text() and encoded in download.text()
    evidence=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    (evidence/('host-'+helper_style+'.html')).write_bytes(download.body())
    (evidence/('host-'+helper_style+'.json')).write_text(json.dumps({'status':'passed','elapsed_ms':elapsed,
        'host_calls':calls,'response':response,'turn':turn},ensure_ascii=False,indent=2))
    record_property('host_'+helper_style+'_workflow_ms',elapsed)


def test_live_stage_calendar_trip_infographic_delivers_real_image(live_web,record_property):
    import io
    from pathlib import Path
    from PIL import Image
    page, _, _, _, _, base=live_web
    response,turn,elapsed=_submit(live_web,
        'For sat Nov 21 weekend trip on my calendar, i want you to plan a trip with nice restaurants, '
        'pick the best ones and things to do around. Create a infographic style page. '
        'Keep whatever is scheduled as it is. And Image',timeout=240)
    results=turn.get('tool_results') or []
    assert any(r['tool_name']=='calendar_list' and r['status']=='completed' for r in results),results
    designed=[r for r in results if r['tool_name']=='image_render' and r['status']=='completed']
    assert len(designed)==1,[(r['tool_name'],r['status'],r.get('error')) for r in results]
    assert designed[0]['output']['pixel_ratio']==2
    assert designed[0]['output']['quality_profile']=='designed_image_v1'
    programs=[r for r in results if r['tool_name']=='code_mode']
    assert programs and len({r['output']['session_id'] for r in programs})==1,programs
    assert len([r for r in results if r['tool_name']=='image_generate'
        and r.get('output',{}).get('http_status_code') in {401,402}])<=1
    artifacts=response.get('artifacts') or [];image_bytes=None
    for artifact in artifacts:
        url=artifact['url'];download=page.request.get(url if url.startswith('http') else base+url,timeout=15000)
        assert download.ok
        try:
            with Image.open(io.BytesIO(download.body())) as picture:
                assert picture.width>=1440 and picture.height>=500
            image_bytes=download.body()
        except (OSError,ValueError):
            continue
    assert image_bytes is not None,(response,[(r['tool_name'],r['status'],r.get('error')) for r in results])
    assert turn.get('response_fulfilled') is True
    assert 'I need one more step before I can complete this' not in response['text'],response
    Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'],'trip-infographic.png').write_bytes(image_bytes)
    record_property('elapsed_seconds',elapsed/1000)
    record_property('programs',len(programs))
    assert elapsed < 180000, 'The single-image workflow must not regress to lengthy repeated authoring'


def test_live_stage_visual_worker_closes_its_tabs(live_web, record_property):
    """Exercise the actual optional worker, not only ordinary browser helpers."""
    import json
    quote='Return one PNG screenshot of https://example.com/ and its visible heading.'
    scope={'capabilities':['web'],'tool_names':['browser_read_pages','browser_run_task'],
        'required_tool_names':['browser_run_task'],'artifact_extensions':['.png'],
        'source_artifact_delivery_requested':True,'source_artifact_request_evidence':quote}
    program=("call_tool('browser_read_pages', {'pages':[{'url':'https://example.com/','read_kind':'text'}]})\n"
        "call_tool('browser_run_task', {'url':'https://example.com/', 'task':'Capture the visible heading and requested PNG', 'max_steps':4})")
    response, turn, elapsed = _submit(live_web, quote+'\nThis is an explicit worker validation: '
        'first call request_tool_scope with '+json.dumps(scope)+'. Then execute this program in one code_mode session:\n'
        +program+'\nKeep the selected desktop browser and return the native screenshot. Do not generate a replacement image.', timeout=180)
    workers = [r for r in turn.get('tool_results', []) if r['tool_name']=='browser_run_task']
    assert len(workers)==1 and workers[0]['status']=='completed', workers
    cleanup = workers[-1]['output']['browser_tab_cleanup']
    assert cleanup['verified'] is True and cleanup['created_target_count'] >= 1, cleanup
    assert cleanup.get('remaining_target_count', 0) == 0
    assert len(response.get('artifacts') or []) == 1, response
    import io
    from PIL import Image
    page, _, _, _, _, base = live_web
    url=response['artifacts'][0]['url']
    download=page.request.get(url if url.startswith('http') else base+url,timeout=15000)
    assert download.ok and download.body().startswith(b'\x89PNG\r\n\x1a\n')
    with Image.open(io.BytesIO(download.body())) as picture:
        assert picture.width>200 and picture.height>100
    assert turn.get('response_fulfilled') is True
    assert not any(r['tool_name'] in {'image_generate','image_render'} for r in turn.get('tool_results',[]))
    record_property('worker_request_seconds', elapsed/1000)


def test_live_stage_recent_production_run_regressions(live_web, record_property):
    """Replay the slow follow-up and wrong-state link through the real composer."""
    from nullion.source_link_state import calendar_context, conflicting_links
    from nullion.tools import ToolResult
    samples=[]
    for prompt,expected in [('Reply with only RIVER41.','RIVER41'),
            ('次の文字だけを返してください: RIVER42','RIVER42'),('Responde solo con RIVER43.','RIVER43')]:
        response,turn,elapsed=_submit(live_web,prompt,timeout=45)
        assert response['text'].strip()==expected,response
        assert not turn.get('tool_results'),turn.get('tool_results')
        samples.append(elapsed/1000)
    assert sum(samples)/len(samples)<12 and max(samples)<20,samples
    record_property('simple_turn_mean_seconds',sum(samples)/len(samples))
    for name,prompt in [('hbo','Find true crime documentary on hbo max to watch that are recently added'),
            ('apple','What about on apple tv?'),
            ('hotel','For my November 21–22 weekend trip, read my connected calendar now to verify the hotel '
                'I am staying at. Then read its official property page for a short current overview and an exact '
                'official link. Preserve the calendar schedule as it is.')]:
        response,turn,elapsed=_submit(live_web,prompt,timeout=180)
        results=turn.get('tool_results') or []
        assert response['text'].strip() and turn.get('response_fulfilled') is True,(name,response,turn)
        assert _completed_native_source_urls(results),(name,results)
        adaptive=[r for r in results if r['tool_name']=='browser_run_task'
            and r.get('output',{}).get('backend')=='browser-use']
        assert not adaptive,(name,adaptive)
        assert elapsed<120000,(name,elapsed)
        if name=='hotel':
            assert any(r['tool_name']=='calendar_list' and r['status']=='completed' for r in results)
            native=[ToolResult(r['invocation_id'],r['tool_name'],r['status'],r.get('output')) for r in results]
            assert not conflicting_links(response['text'],calendar_context(native)),response
        record_property(name+'_elapsed_seconds',elapsed/1000)


def test_live_stage_calendar_trip_pdf_efficiency(live_web, record_property):
    """Replay the production request through the real stage Web composer."""
    import io
    import json
    from pathlib import Path
    from pypdf import PdfReader
    page, _, _, _, _, base = live_web
    response, turn, elapsed = _submit(live_web,
        'For sat Nov 21 weekend trip on my calendar, i want you to plan a trip with nice restaurants, '
        'pick the best ones and things to do around. Create a infographic style page. '
        'Keep whatever is scheduled as it is. A pdf', timeout=240)
    tools = turn.get('tool_results') or []
    assert any(t['tool_name'] == 'calendar_list' and t['status'] == 'completed' for t in tools)
    pdfs = [t for t in tools if t['tool_name'] == 'pdf_create' and t['status'] == 'completed']
    assert pdfs and not any(t.get('output', {}).get('last_mile_materialized') for t in pdfs), pdfs
    assert any(t['output'].get('visual_preview_available') for t in pdfs), pdfs
    artifacts = response.get('artifacts') or []
    assert len(artifacts) == 1, response
    url = artifacts[0]['url']; downloaded = page.request.get(url if url.startswith('http') else base + url)
    assert downloaded.ok
    data = downloaded.body(); document = PdfReader(io.BytesIO(data))
    assert len(document.pages) == 1
    content = '\n'.join(p.extract_text() or '' for p in document.pages)
    assert '10:53' in content and ('2:46' in content or '14:46' in content), content
    assert 'Tamarind' in content and 'Locust Grove' in content
    assert turn.get('response_fulfilled') is True, response
    evidence = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    (evidence / 'trip-efficiency.pdf').write_bytes(data)
    (evidence / 'trip-efficiency.json').write_text(json.dumps({'elapsed_ms': elapsed, 'turn': turn}, ensure_ascii=False))
    record_property('trip_pdf_seconds', elapsed / 1000)
    assert elapsed / 1000 < 150, 'Investigate any trip replay slower than the audited 176-second production request'


def _completed_native_source_urls(tools):
    visited = []
    for tool in tools:
        output = tool.get('output') or {}
        if tool['status'] != 'completed':
            continue
        if tool['tool_name'] in {'browser_navigate', 'web_fetch'}:
            visited.append(output.get('page_url') or output.get('url') or '')
        if tool['tool_name'] == 'browser_read_pages':
            visited.extend(p.get('page_url', '') for p in output.get('pages', []) if p.get('status') == 'completed')
        if tool['tool_name'] == 'code_mode':
            for native in output.get('native_observations', []):
                if native.get('tool_name') != 'browser_read_pages' or native.get('status') != 'completed':
                    continue
                for view in native.get('output', {}).get('collection_views', []):
                    columns = view.get('columns', [])
                    field = 'url' if view.get('json_pointer') == '/documents' else 'source_page_url'
                    if field in columns:
                        visited.extend(row[columns.index(field)] for row in view.get('rows', [])
                                       if isinstance(row[columns.index(field)], str))
    return visited


def test_live_stage_mixed_pdf_package_has_no_unused_fallback(live_web, record_property):
    from pathlib import Path
    import io
    from PIL import Image, ImageDraw
    from pypdf import PdfReader
    page, _, _, _, _, base = live_web
    root = Path(os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT'])
    image = Image.new('RGB', (600, 300), '#173e36')
    ImageDraw.Draw(image).text((30, 40), 'Value 37', fill='white')
    source = root / 'package-illustration.png'; image.save(source)
    response, turn, elapsed = _submit(live_web,
        'Create two separate one-page PDFs: an illustrated report with value 37 using this local image: '
        + str(source) + '. The second PDF must be a separate plain text memo with value 73. '
        'Inspect both finished PDFs and attach both files. No web research is needed.', timeout=180)
    artifacts = response.get('artifacts') or []
    assert len(artifacts) == 2, response
    texts = []; images = []
    for artifact in artifacts:
        url = artifact['url']; download = page.request.get(url if url.startswith('http') else base + url)
        assert download.ok
        pdf = PdfReader(io.BytesIO(download.body())); assert len(pdf.pages) == 1
        texts.append(pdf.pages[0].extract_text() or '')
        images.append(len(pdf.pages[0].images))
    assert any('37' in content for content in texts) and any('73' in content for content in texts), texts
    assert any(images) and any(count == 0 for count in images), images
    assert not any(t.get('output', {}).get('last_mile_materialized') for t in turn.get('tool_results', []))
    assert turn.get('response_fulfilled') is True
    record_property('mixed_pdf_seconds', elapsed / 1000)


def test_live_stage_recent_recommendations_open_underlying_sources(live_web, record_property):
    import json
    from pathlib import Path
    response, turn, elapsed = _submit(live_web,
        'Find true crime documentary on hbo max to watch that are recently added', timeout=180)
    tools = turn.get('tool_results') or []
    visited = _completed_native_source_urls(tools)
    from urllib.parse import urlparse
    if not any(urlparse(url).hostname not in {None, 'www.google.com', 'google.com'} for url in visited):
        failures = [t for t in tools if t['status'] == 'failed' and t.get('output', {}).get('reason') == 'web_search_failed']
        if failures:
            assert response.get('text'), response
            pytest.xfail('Public search returned no usable source evidence; the real run returned a limited answer. Inspect its source-final evidence before claiming verification.')
    assert any(urlparse(url).hostname in {'www.hbomax.com', 'www.hbo.com', 'press.wbd.com', 'www.youtube.com'}
               and urlparse(url).path not in {'/results', '/search'} for url in visited), visited
    assert turn.get('response_fulfilled') is True and response.get('text'), response
    Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'], 'recent-recommendations.json').write_text(
        json.dumps({'elapsed_ms': elapsed, 'turn': turn}, ensure_ascii=False))
    record_property('recent_recommendations_seconds', elapsed / 1000)


def test_live_stage_large_transfer_after_numeric_choice(live_web,record_property):
    """Real UI/provider, native file read and VM transfer above the old ceiling."""
    from pathlib import Path
    from uuid import uuid4
    root=Path(os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT'])
    source=root/('large-transfer-'+uuid4().hex[:10]+'.txt')
    payload='x'*3_200_000+'\nVERIFIED_TRANSFER_37'
    source.write_text(payload)
    try:
        _,question,_=_submit(live_web,
            'This is a read-only local validation. I want you to read '+str(source)+
            ' and report its character count and final marker. First ask me to choose numbered options '
            '1. Read this file or 2. Cancel, then wait for my numeric reply. After I choose 1, use '
            'exactly one code_mode program with tool_names=["file_read"] and code:\n'
            'saved=call_tool("file_read", {"path":'+repr(str(source))+'})\n'
            '{"characters":len(saved["output"]["content"]),"marker":saved["output"]["content"][-21:]}\n'
            'Do not read the file before my choice, create other files or use network sources.',timeout=90)
        assert not any(r['tool_name']=='file_read' for r in question.get('tool_results',[]))
        response,turn,elapsed=_submit(live_web,'1',timeout=120)
        assert turn.get('parent_turn_id')==question['turn_id'],turn
        programs=[r for r in turn.get('tool_results',[]) if r['tool_name']=='code_mode']
        assert len(programs)==1 and programs[0]['status']=='completed',turn.get('tool_results')
        output=programs[0]['output']
        assert output['reason']=='code_mode_completed'
        assert 3_200_000<output['transfer_usage']['transferred_bytes']<4_000_000
        assert output['transfer_usage']['limit_bytes']==4_000_000
        native=[r for r in turn['tool_results'] if r['tool_name']=='file_read']
        assert len(native)==1 and native[0]['status']=='completed'
        assert output['value']['characters']==len(payload)
        assert output['value']['marker'].endswith('VERIFIED_TRANSFER_37')
        # The durable chat audit intentionally clips huge native bodies. Exact
        # VM-derived size and marker prove the read; audit omission stays explicit.
        receipt=native[0]['output']
        if 'content' in receipt:
            assert len(receipt['content'])==len(payload)
        else:
            assert receipt.get('truncated') is True and receipt['original_chars']>=len(payload)
        assert 'VERIFIED_TRANSFER_37' in response['text'] and str(len(payload)) in response['text'].replace(',','')
        record_property('numeric_choice_large_transfer_seconds',elapsed/1000)
    finally:
        source.unlink(missing_ok=True)


def test_live_stage_flight_form_reaches_interactive_worker(live_web,record_property):
    """An interactive lookup cannot pass by returning only its initial page read."""
    from datetime import datetime,timedelta
    from zoneinfo import ZoneInfo
    today=datetime.now(ZoneInfo('America/New_York')).date()
    departure=today+timedelta(days=(1-today.weekday())%7 or 7)
    returned=departure+timedelta(days=1)
    response,turn,elapsed=_submit(live_web,
        f'Find round-trip flights EWR to LAX departing {departure.isoformat()} around 6pm local time '
        f'and returning {returned.isoformat()} around 6pm local time, one adult, economy. '
        'Use the live flight-search form, with browser_run_task and execution_kind=adaptive_interaction '
        'when interaction is needed. Enter the route and dates, submit the search, and return actual '
        'displayed options and round-trip prices with source links. Do not book, sign in, or submit '
        'personal information. A page read of an unsubmitted form is not a completed search. '
        'If the source blocks the search, report the exact observed blocker.',timeout=420)
    results=turn.get('tool_results',[])
    workers=[r for r in results if r['tool_name']=='browser_run_task' and
        r.get('output',{}).get('backend')=='browser-use']
    assert workers,'The requested interaction never reached its worker: '+str([
        (r['tool_name'],r['status'],r.get('output',{}).get('strategy')) for r in results])
    assert any(r['output'].get('performance',{}).get('model_calls',0)>0 for r in workers),workers
    assert response['text'] and 'https://' in response['text']
    assert not any(r.get('output',{}).get('reason')=='code_mode_input_limit' for r in results)
    for r in workers:
        cleanup=r['output'].get('browser_tab_cleanup')
        assert cleanup and cleanup.get('verified') is True and cleanup.get('remaining_target_count',
            len(cleanup.get('remaining_target_ids') or ()))==0,cleanup
    record_property('flight_interaction_seconds',elapsed/1000)


def test_live_stage_multi_source_facts_survive_compaction(live_web,record_property):
    """Real UI/model/native reads; repeated captures cannot displace facts."""
    import json
    from pathlib import Path
    from uuid import uuid4
    from tests.test_source_finding_compaction import source_bundle
    root=Path(os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT'])
    identity=uuid4().hex[:10]
    files=[root/(identity+'-'+str(i)+'.json') for i in range(2)]
    sources=[source_bundle(name,capture_chars=700000,count=8) for name in ('vol','宿泊')]
    for path,source in zip(files,sources):path.write_text(json.dumps(source,ensure_ascii=False))
    program=('import json\n'
        'a=call_tool("file_read", {"path":'+repr(str(files[0]))+'})\n'
        'b=call_tool("file_read", {"path":'+repr(str(files[1]))+'})\n'
        '{"first":json.loads(a["output"]["content"]),"second":json.loads(b["output"]["content"])}')
    try:
        response,turn,elapsed=_submit(live_web,
            'Read-only compaction validation. These two local JSON files contain fictional source records. '
            'Execute exactly this one code_mode program with tool_names=["file_read"]:\n'+program+'\n'
            'List all 16 exact record values (vol-verified-0 through vol-verified-7 and '
            '宿泊-verified-0 through 宿泊-verified-7), preserving their date, time and price. '
            'Use the returned findings directly. Do not inspect again, reread, browse, create files, '
            'or summarize the optional page captures.',timeout=120)
        for name in ('vol','宿泊'):
            assert all(name+'-verified-'+str(i) in response['text'] for i in range(8)),response['text']
        results=turn.get('tool_results',[])
        assert len([r for r in results if r['tool_name']=='file_read'])==2
        programs=[r for r in results if r['tool_name']=='code_mode']
        assert len(programs)==1 and programs[0]['status']=='completed',results
        assert programs[0]['output']['value_limit']['projected'] is True
        assert not any(r['tool_name'] in {'inspect_retained_evidence','browser_run_task','web_search'} for r in results)
        assert not response['artifacts']
        assert elapsed<60000,elapsed
        record_property('multi_source_finding_handoff_seconds',elapsed/1000)
    finally:
        for path in files:path.unlink(missing_ok=True)


def test_live_stage_delegation_runs_native_preflight_without_repair_program(live_web, record_property):
    import json
    args = {'url': 'https://example.com/', 'task':
        'Read the heading and the More information link. Record those observations with source URLs, then report. Do not follow links or change the page.',
        'execution_kind': 'adaptive_interaction', 'interaction_reason':
        'Validate the delegated browser handoff after its native page preflight', 'max_steps': 4}
    program = "r=call_tool('browser_run_task', " + repr(args) + ")\nr['output']"
    response, turn, elapsed = _submit(live_web,
        'Read-only execution validation. Request only browser_run_task scope, then run exactly this single '
        'Code Mode program. The native browser handler owns the initial page read; do not add separate '
        'navigation or read calls before delegation. Report the heading and link returned by the tool.\n' + program,
        timeout=180)
    results = turn.get('tool_results', [])
    programs = [r for r in results if r['tool_name'] == 'code_mode']
    workers = [r for r in results if r['tool_name'] == 'browser_run_task']
    assert len(programs) == len(workers) == 1, [(r['tool_name'], r['status']) for r in results]
    assert programs[0]['status'] == workers[0]['status'] == 'completed', results
    assert workers[0]['response_requirements']['_native_preflight']['adaptive_browser_started'] is True
    assert not any(r.get('output', {}).get('reason') == 'tool_precondition_unmet' for r in results)
    cleanup = workers[0]['output'].get('browser_tab_cleanup')
    assert cleanup and cleanup.get('verified') is True and cleanup.get('remaining_target_count',
        len(cleanup.get('remaining_target_ids') or ())) == 0, cleanup
    assert 'Example Domain' in response['text'] and 'https://' in response['text'], response
    record_property('first_delegation_seconds', elapsed / 1000)


def test_live_stage_local_request_keeps_profile_after_separate_trip(live_web, record_property):
    _submit(live_web, 'For a separate future trip, the destination is Los Angeles. '
        'Just acknowledge with LA_TRIP; do not research anything.', timeout=60)
    response, turn, elapsed = _submit(live_web,
        'Can you check if sala thai has reservations available around 6pm Wednesday?', timeout=180)
    visible = response['text']
    location = visible.casefold()
    # The restaurant's verified neighborhood is sufficient location evidence;
    # a correct local reply need not also repeat the city name.
    assert (any(city in location for city in ('new york', 'nyc', 'manhattan'))
        or ('sala thai' in location and any(place in location
            for place in ('upper west side', '307 amsterdam')))), visible
    assert 'los angeles' not in visible.casefold(), visible
    assert not any(r['tool_name'] in {'email_send', 'browser_click_element', 'browser_click_id'}
        for r in turn.get('tool_results', [])), turn
    record_property('saved_location_request_seconds', elapsed / 1000)


@pytest.mark.parametrize('instruction', [
    'Report actual elapsed usage days, the quoted billing interval, total, and effective total per actual elapsed day.',
    '実際の利用日数、見積の課金間隔、合計金額、実際の利用日数で割った一日あたりの合計金額を示してください。',
], ids=['en', 'ja'])
def test_live_stage_quote_keeps_billing_period_separate_from_elapsed_duration(live_web, instruction):
    import json
    from pathlib import Path
    from uuid import uuid4
    quote = {'start_at':'2027-06-29T12:30:00-04:00','end_at':'2027-07-05T12:30:00-04:00',
        'currency':'USD','quoted_billing_interval':'week','quoted_subtotal':429.99,
        'taxes_and_fees':66.91,'total':496.90,'fixture':True}
    path=Path(os.environ['NULLION_LIVE_STAGE_ARTIFACT_ROOT'])/('billing-'+uuid4().hex+'.json')
    path.write_text(json.dumps(quote))
    try:
        response,turn,_=_submit(live_web,
            f'Read the fictional quote in {path} with file_read. No website requests. '
            f'{instruction} Return only a JSON object with numeric elapsed_days, '
            'quoted_billing_interval, numeric total and numeric effective_total_per_day. Round the daily amount to two decimals.',timeout=90)
        value=response['text'].strip()
        if value.startswith('```'):value='\n'.join(value.splitlines()[1:-1])
        result=json.loads(value)
        assert result['elapsed_days']==6,result
        assert result['quoted_billing_interval']=='week',result
        assert result['total']==496.90 and result['effective_total_per_day']==82.82,result
        tools=turn.get('tool_results',[])
        assert any(r['tool_name']=='file_read' and r['status']=='completed' for r in tools),tools
        assert not any(r['tool_name'].startswith(('browser_','web_')) for r in tools),tools
    finally:path.unlink(missing_ok=True)


def test_live_stage_terminal_timeout_retains_partial_output_without_rescan(live_web, record_property):
    """Synthetic read-only diagnostic through the real provider and stage shell."""
    args = {'command': "printf 'OBSERVED_PARTIAL_37\\n'; sleep 30 | cat", 'timeout_seconds': 1, 'shell': 'bash'}
    program = "r=call_tool('terminal_exec', " + repr(args) + ")\nr['output']"
    response, turn, elapsed = _submit(live_web,
        'Run this synthetic read-only diagnostic exactly once in Code Mode. Its timeout is intentional. '
        'Report that the command timed out and retain any captured partial output; do not repeat it, '
        'scan files, create artifacts or ask to delete anything.\n' + program, timeout=90)
    results = turn.get('tool_results', [])
    commands = [r for r in results if r['tool_name'] == 'terminal_exec']
    assert len(commands) == 1, [(r['tool_name'], r['status']) for r in results]
    result = commands[0]
    assert result['status'] == 'failed', result
    assert result['output']['execution_outcome'] == 'timeout' and result['output']['output_complete'] is False
    assert 'OBSERVED_PARTIAL_37' in result['output']['stdout']
    assert 'OBSERVED_PARTIAL_37' in response['text'], response
    assert not response.get('artifacts') and not response.get('suspended_for_approval'), response
    assert elapsed < 60000, elapsed
    record_property('intentional_terminal_timeout_seconds', elapsed / 1000)
