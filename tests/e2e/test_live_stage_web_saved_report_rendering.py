"""Replay a captured report through real stage delivery without rereading sources."""
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web

pytestmark = pytest.mark.skipif(
    os.environ.get('NULLION_LIVE_STAGE_E2E') != '1' or not os.environ.get('NULLION_LIVE_REPORT_REPLAY_FILE'),
    reason='Explicit captured stage-report replay required')


def test_live_saved_report_preserves_facts_in_history_and_rendered_web(live_web):
    from playwright.sync_api import expect
    page, frames, errors, database, conversation, base = live_web
    report = json.loads(Path(os.environ['NULLION_LIVE_REPORT_REPLAY_FILE']).read_text())['report']
    response = page.request.post(base+'/api/chat/history/'+conversation+'/message',
        data={'role':'bot','text':report})
    assert response.ok, response.text()
    page.reload(wait_until='domcontentloaded')
    bubble = page.locator('#messages .msg.bot .bubble')
    expect(bubble).to_have_count(1)
    expect(bubble).to_be_visible()
    # The exact stored message is the content contract. Formatting must not
    # replace embedded dates with list ordinals, even across a reload.
    history = page.request.get(base+'/api/chat/history/'+conversation).json()['messages']
    assert [row['text'] for row in history if row['role']=='bot'] == [report]
    assert bubble.get_attribute('data-raw-text').strip() == report.strip()
    assert bubble.locator('ol').count() == 0
    expect(bubble).not_to_contain_text('**')
    assert not errors, errors


def test_live_completed_report_receipt_matches_caption_and_download_bytes(live_web):
    """Read a completed test-owned run without repeating research or delivery."""
    from playwright.sync_api import expect
    from tests.e2e.test_live_stage_web_cron_recovery import _transport_caption_and_paths
    from tests.e2e.test_live_stage_web_matrix import catalogue

    captured = json.loads(Path(os.environ['NULLION_LIVE_REPORT_REPLAY_FILE']).read_text())
    if not captured.get('delivered'):
        pytest.skip('Requires a completed saved-run receipt')
    assert captured['terminal']['response_fulfilled'] is True and not captured['terminal']['report_failed']
    original = captured['conversation_id']
    assert original.startswith('web:live-ui-audit:'), 'Only test-owned conversations may be inspected'
    page, frames, errors, database, conversation, base = live_web
    caption, paths = _transport_caption_and_paths(captured['delivered'][-1]['assistant_reply'])
    history = page.request.get(base + '/api/chat/history/' + original).json()['messages']
    reports = [m for m in history if m['role'] == 'bot' and m['text'].strip() == caption]
    assert len(reports) == 1, reports
    artifacts = reports[0].get('artifacts') or []
    assert paths and paths == {a['path'] for a in artifacts}, artifacts
    # A separate browser context selects the captured chat before app startup;
    # the main fixture intentionally pins its own new chat on every reload.
    # Its enclosing browser cleanup also owns this context if an assertion fails.
    context = page.context.browser.new_context()
    context.add_init_script('localStorage.setItem("nullion_chat_restore_suppressed", "true"); '
                            'localStorage.setItem("nullion_conv_id", ' + json.dumps(original) + ');')
    page = context.new_page()
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(base, wait_until='domcontentloaded')
    page.wait_for_function('expected => Array.from(document.querySelectorAll("#messages .msg.bot .bubble"))'
                          '.some(b => (b.getAttribute("data-raw-text") || "").trim() === expected)', arg=caption)
    matches = [b for b in page.locator('#messages .msg.bot .bubble').all()
               if (b.get_attribute('data-raw-text') or '').strip() == caption]
    assert len(matches) == 1 and matches[0].is_visible()
    assert '**' not in matches[0].inner_text() and 'MEDIA:' not in matches[0].inner_text()
    output = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR']) / 'completed-report-read-only'
    output.mkdir(parents=True, exist_ok=True)
    downloads = []
    for artifact in artifacts:
        link = page.locator('#messages a[href=' + json.dumps(artifact['url']) + ']')
        expect(link).to_have_count(1)
        expect(link).to_be_visible()
        body, media_type = catalogue._artifact_bytes(base, artifact, timeout=30)
        assert len(body) == artifact['size_bytes'] and body
        name = Path(artifact['name']).name
        if name.endswith('.html'):
            visible, _ = catalogue._validate_html_visual_content(name, body)
            for token in ('Repository metrics', 'Scope', 'Source freshness'):
                assert ''.join(token.casefold().split()) in ''.join(visible.casefold().split()), visible
        (output / name).write_bytes(body)
        downloads.append({'name': name, 'bytes': len(body), 'media_type': media_type})
    (output / 'result.json').write_text(json.dumps({'original_conversation': original, 'caption': caption,
                                                  'downloads': downloads}, indent=2))
    page.screenshot(path=str(output / 'web.png'), full_page=False)
    assert not errors
    context.close()


def test_live_captured_package_downloads_and_renders_without_regeneration(live_web):
    """Content-only replay keeps the original latency result unchanged."""
    import io
    import pypdfium2 as pdfium
    from pypdf import PdfReader
    from playwright.sync_api import expect
    from tests.e2e.test_live_stage_web_matrix import catalogue
    captured=json.loads(Path(os.environ['NULLION_LIVE_REPORT_REPLAY_FILE']).read_text())
    original=captured['conversation_id']
    assert original.startswith('web:live-ui-audit:')
    response=captured['terminal_frames'][-1]
    scenario=catalogue.SCENARIOS[captured['scenario']]
    content_contract=replace(scenario,max_single_ms=None,max_average_ms=None)
    results=[r for turn in captured['turns'] for r in turn.get('tool_results',[])]
    catalogue._assert_contract(content_contract,response=response,
        elapsed_ms=max(captured['elapsed_ms']),tool_names={r['tool_name'] for r in results},
        runtime_db_available=True,tool_output_texts=[json.dumps(r) for r in results])
    review_statuses = [manifest.get('review_status') for r in results
        if isinstance(r.get('output'), dict)
        and isinstance(manifest := r['output'].get('artifact_delivery_manifest'), dict)]
    fixture_page,frames,errors,database,conversation,base=live_web
    context=fixture_page.context.browser.new_context()
    try:
        context.add_init_script('localStorage.setItem("nullion_chat_restore_suppressed","true");'
            'localStorage.setItem("nullion_conv_id",'+json.dumps(original)+');')
        page=context.new_page();page.goto(base,wait_until='domcontentloaded')
        output=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'captured-package';output.mkdir(parents=True,exist_ok=True)
        for artifact in response['artifacts']:
            expect(page.locator('#messages a[href='+json.dumps(artifact['url'])+']')).to_have_count(1,timeout=15000)
        downloads=catalogue._download_and_check_artifacts(base,response['artifacts'],
            scenario=content_contract,timeout=60,download_dir=output)
        for artifact in response['artifacts']:
            name=Path(artifact['name']).name
            if not name.endswith('.pdf'):continue
            body=(output/name).read_bytes();reader=PdfReader(io.BytesIO(body))
            if captured['scenario']=='formatted_pdf_flyer_pack':
                assert len(reader.pages)==(2 if '_memo_' in name else 1)
            with pdfium.PdfDocument(body) as document:
                visible_parts=[]
                for index in range(len(document)):
                    pdf_page=document[index];bitmap=pdf_page.render(scale=1)
                    text=pdf_page.get_textpage()
                    try:
                        visible_parts.append(text.get_text_bounded())
                        bitmap.to_pil().save(output/(name+'.page-'+str(index+1)+'.png'))
                    finally:text.close();bitmap.close();pdf_page.close()
                if captured['scenario']=='formatted_pdf_flyer_pack':
                    visible=''.join(''.join(visible_parts).split()).upper()
                    assert 'COMMUNITY' in visible and 'STAGEFORMATQA' in visible,(name,visible)
        page.reload(wait_until='domcontentloaded')
        for artifact in response['artifacts']:
            expect(page.locator('#messages a[href='+json.dumps(artifact['url'])+']')).to_have_count(1,timeout=15000)
        page.screenshot(path=str(output/'web.png'),full_page=False)
        (output/'result.json').write_text(json.dumps({'original_conversation':original,
            'original_elapsed_ms':captured['elapsed_ms'],'original_status':captured['status'],
            'latency_checked':False,'review_statuses':review_statuses,
            'downloads':downloads},ensure_ascii=False,indent=2))
    finally:context.close()


def test_live_public_followup_retained_source_and_ui_after_optional_review_outage(live_web):
    from playwright.sync_api import expect
    from tests.e2e.test_live_stage_web_matrix import catalogue,_turns
    from tests.e2e.live_public_followup_assertions import assert_public_followup_outcome
    captured=json.loads(Path(os.environ['NULLION_LIVE_REPORT_REPLAY_FILE']).read_text())
    original=captured['conversation_id'];assert original.startswith('web:live-ui-audit:')
    assert captured['scenario']=='browser_followup_account_scope_guard'
    fixture,frames,errors,database,conversation,base=live_web
    turns=_turns(database,original)
    expected_ids=[r['turn_id'] for r in captured['turns']]
    turns=[r for r in turns if r['turn_id'] in expected_ids]
    assert [r['turn_id'] for r in turns]==expected_ids
    response={**captured['terminal_frames'][-1],
        'text':captured['turns'][-1]['delivered_reply']}
    reviews=catalogue._conversation_event_payloads(database,original,event_type='conversation.completion_review')
    result=assert_public_followup_outcome(response=response,turns=turns,reviews=reviews)
    context=fixture.context.browser.new_context()
    try:
        context.add_init_script('localStorage.setItem("nullion_chat_restore_suppressed","true");'
            'localStorage.setItem("nullion_conv_id",'+json.dumps(original)+');')
        page=context.new_page();page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(base,wait_until='domcontentloaded')
        bubble=page.locator('#messages .msg.bot .bubble[data-turn-id='+json.dumps(turns[-1]['turn_id'])+']')
        expect(bubble).to_have_count(1);expect(bubble).to_be_visible()
        assert bubble.get_attribute('data-raw-text').strip()==response['text'].strip()
        expect(bubble).not_to_contain_text('**')
        for source in result['observed_sources']:
            expect(bubble.locator('a[href='+json.dumps(source['url'])+']')).to_have_count(1)
        page.reload(wait_until='domcontentloaded');expect(bubble).to_have_count(1)
        output=Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'public-followup-read-only';output.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(output/'web.png'),full_page=False)
        (output/'result.json').write_text(json.dumps({'original_conversation':original,
            'additional_model_calls':0,'boundary':result},ensure_ascii=False,indent=2)+'\n')
        assert not errors,errors
    finally:context.close()
