"""Two real-provider browser workflows using disposable controlled sources."""
import json
import re
import os
from pathlib import Path

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1'
    or not os.environ.get('NULLION_LIVE_BOUNDED_COMPARISON_SOURCE'), reason='Explicit stage source acceptance')


@pytest.mark.parametrize('kind', ['rentals', 'products'])
def test_live_browser_groups_subject_fields_and_delivers_only_specific_limitations(live_web, kind):
    page, frames, errors, database, conversation, base = live_web
    origin = os.environ['NULLION_LIVE_BOUNDED_COMPARISON_SOURCE'].rstrip('/')
    if kind == 'rentals':
        prompt = ('First call request_tool_scope with capabilities [web], tool_names [browser_run_task], '
            'and required_tool_names [browser_run_task]. Use browser_run_task to '
            f'compare both fictional rental offers at {origin}/rentals, reading their detail pages. '
            'Give each vehicle, displayed total, pickup location, mileage, fuel policy and cancellation terms. '
            'Say what is unknown about engine fuel type and mandatory fees. Do not claim a gasoline-only '
            'or final all-in winner without proof. Keep each offer and its terms together. No bookings or files.')
        expected = ['Oak Compact', '$118', 'Trail SUV', '$137', '743 Sample Road']
    else:
        prompt = (f'Compara las dos lámparas de {origin}/products. Indica el nombre, precio y unidades '
            'disponibles de cada una, sin mezclar sus datos. Aclara si muestra una fecha de entrega. '
            'Incluye enlaces directos a las fuentes. No compres ni crees archivos.')
        expected = ['Maple Lamp', '$34', 'Birch Lamp', '$48']
    out = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR']) / ('source-groups-' + kind)
    out.mkdir(parents=True, exist_ok=True)
    evidence = {'conversation_id': conversation, 'prompt': prompt}
    try:
        response, turn, elapsed = _submit(live_web, prompt, timeout=360)
        evidence.update(response=response, turn=turn, elapsed_ms=elapsed)
        reply = response['text']
        assert all(value in reply for value in expected), reply
        assert 'Selected offer information' not in reply and 'Choose an option' not in reply
        assert 'Some selected details could not be safely combined or verified' not in reply
        assert 'could not turn it into a focused answer' not in reply and 'Review warning:' not in reply
        assert origin in reply and not response['artifacts'] and not errors
        assert any(row['tool_name'].startswith('browser_') and row['status'] == 'completed'
                   for row in turn['tool_results']), turn
        if kind == 'products':
            assert re.search(r'(?<!\d)4(?!\d)', reply) and re.search(r'(?<!\d)7(?!\d)', reply), reply
        validations = catalogue._conversation_event_payloads(database, conversation,
            event_type='conversation.source_answer_validation')
        if kind == 'rentals':
            assert any(row['tool_name'] == 'browser_run_task' and row['status'] == 'completed'
                       for row in turn['tool_results']), turn
            assert validations and validations[-1]['valid'] is True, validations
        reviews = catalogue._conversation_event_payloads(database, conversation,
            event_type='conversation.completion_review')
        assert reviews and reviews[-1]['disposition'] == 'complete', reviews
        assert reviews[-1].get('source_presentation_valid') is not False, reviews
        evidence.update(validations=validations, reviews=reviews)
        bubbles = page.locator('#messages .msg.bot .bubble')
        assert bubbles.last.is_visible()
        assert '**' not in bubbles.last.inner_text()
        assert bubbles.last.locator('li').count() >= 2
    finally:
        (out/'result.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
        page.screenshot(path=str(out/'web.png'), timeout=10000)


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_RECEIPT_REPLAY_ID'), reason='Owned completed reply required')
def test_live_completed_product_reply_keeps_fields_on_reload_without_another_model_call(live_web):
    from tests.e2e.test_live_stage_web_matrix import _turns
    page, frames, errors, database, conversation, base = live_web
    saved = os.environ['NULLION_LIVE_RECEIPT_REPLAY_ID']
    assert saved.startswith('web:live-ui-audit:')
    before = catalogue._conversation_event_payloads(database, saved, event_type='conversation.model_timing')
    turns = _turns(database, saved)
    assert len(turns) == 1, turns
    text = turns[0]['assistant_reply']
    assert all(value in text for value in ('Maple Lamp', '$34', 'Birch Lamp', '$48')), text
    assert re.search(r'(?<!\d)4(?!\d)', text) and re.search(r'(?<!\d)7(?!\d)', text), text
    assert 'safely combined or verified' not in text and 'Choose an option' not in text
    reviews = catalogue._conversation_event_payloads(database, saved, event_type='conversation.completion_review')
    assert reviews and reviews[-1]['disposition'] == 'complete', reviews
    # A fresh context avoids the live fixture's deliberate per-load selection
    # of a new conversation. Exercise the actual history restore path.
    restored = page.context.browser.new_context()
    restored.add_init_script('localStorage.setItem("nullion_chat_restore_suppressed","true");'
        'localStorage.setItem("nullion_conv_id",' + json.dumps(saved) + ');')
    page = restored.new_page()
    page.goto(base, wait_until='domcontentloaded')
    from playwright.sync_api import expect
    bubble = page.locator('#messages .msg.bot .bubble')
    expect(bubble).to_have_count(1)
    expect(bubble).to_be_visible()
    history = page.request.get(base+'/api/chat/history/'+saved).json()['messages']
    assert [row['text'] for row in history if row['role'] == 'bot'] == [text]
    for value in ('Maple Lamp', '$34', 'Birch Lamp', '$48'):
        expect(bubble).to_contain_text(value)
    assert re.search(r'(?<!\d)4(?!\d)', bubble.inner_text())
    assert re.search(r'(?<!\d)7(?!\d)', bubble.inner_text())
    assert '**' not in bubble.inner_text() and not errors
    after = catalogue._conversation_event_payloads(database, saved, event_type='conversation.model_timing')
    assert len(after) == len(before)
    out = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    page.screenshot(path=str(out/'source-groups-product-replay.png'), timeout=10000)
    (out/'source-groups-product-replay.json').write_text(json.dumps({
        'conversation_id': saved, 'reply': text, 'reviews': reviews, 'extra_model_calls': 0}, ensure_ascii=False, indent=2))
    restored.close()
