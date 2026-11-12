"""Real-provider Web receipt amendment; only a disposable local form is opened."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, _turns, watch_chat_frames, catalogue

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in stage UI')


def test_live_receipt_facts_survive_management_only_amendment(live_web, tmp_path):
    from PIL import Image, ImageDraw, ImageFont
    from playwright.sync_api import expect

    source = os.environ.get('NULLION_LIVE_BOUNDED_COMPARISON_SOURCE')
    if not source:
        pytest.skip('Disposable controlled source is required')
    page, frames, errors, database, conversation, base = live_web
    before = page.request.get(source + '/management/state').json()['submissions']
    replay=os.environ.get('NULLION_LIVE_RECEIPT_REPLAY_ID')
    if replay:
        assert replay.startswith('web:live-ui-audit:')
        turns=sorted(_turns(database,replay),key=lambda row:row['created_at'])
        assert len(turns) in {2,3},turns
        first_turn,second_turn=turns[:2]
        first={'text':first_turn['assistant_reply']};second={'text':second_turn['assistant_reply']}
        page=page.context.browser.new_page()
        page.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');localStorage.setItem('nullion_conv_id',"+json.dumps(replay)+");")
        watch_chat_frames(page,frames,errors,base)
        page.goto(base,wait_until='domcontentloaded')
        conversation=replay;live_web=page,frames,errors,database,conversation,base
        expect(page.locator('#messages')).to_contain_text('05B33')
    else:
        image = Image.new('RGB', (900, 700), 'white')
        font = ImageFont.truetype('/usr/share/fonts/truetype/freefont/FreeSans.ttf', 29)
        receipt = ('SAMPLE KITCHEN - TEST RECEIPT\n98th Street and Columbus Avenue\n'
                   'Customer: Test Customer\nOrder: 05B33\nTotal: $72.68\n'
                   '2 Honey Mustard sauces\n5 Polynesian sauces\n4 House sauces\n'
                   '11 sauces total\nAll 11 sauces were missing on delivery.')
        ImageDraw.Draw(image).multiline_text((30, 30), receipt, font=font, fill='black', spacing=15)
        path = tmp_path / 'test-receipt.png'; image.save(path)
        page.locator('#file-input').set_input_files(str(path))
        expect(page.locator('#attachments-bar')).to_contain_text(path.name)
        first, first_turn, _ = _submit(live_web,
            'Read this sample receipt for a complaint about the missing sauces. '
            'For now, summarize the order and missing items. Do not contact anyone.', timeout=120)
        assert '05B33' in first['text'] and '11' in first['text'], first
        second, second_turn, _ = _submit(live_web,
            'Just notify management. Prepare a draft from that receipt and open the feedback form at '
            + source + '/management/ . Do not submit anything; show the draft for approval first.', timeout=240)
    assert '05B33' in second['text'] and '11' in second['text'], second
    assert second_turn['parent_turn_id'], second_turn
    tools = second_turn.get('tool_results', [])
    # Navigation to a verified destination and clicking its observed link
    # are equivalent. Assert the form state, not a particular browser primitive.
    assert any(t['tool_name']=='browser_snapshot' and t['status']=='completed'
        and t.get('output',{}).get('snapshot',{}).get('url')==source+'/management/feedback' for t in tools),tools
    fields=[element for t in tools if t['tool_name']=='browser_snapshot' and t['status']=='completed'
        for element in t.get('output',{}).get('snapshot',{}).get('elements',[])
        if t.get('output',{}).get('snapshot',{}).get('url')==source+'/management/feedback'
        and element.get('name') in {'order','message'}]
    assert {field['name'] for field in fields}=={'order','message'} and all(field.get('value')=='' for field in fields),fields
    assert not any(t.get('output', {}).get('reason') == 'request_plan_conflict' for t in tools), tools
    if replay and len(turns)==3:
        third_turn=turns[2];third={'text':third_turn['assistant_reply']}
        history=catalogue._get_json(base+'/api/chat/history/'+conversation,timeout=15)['messages']
        persisted=[m for m in history if m.get('role')=='bot' and m.get('metadata',{}).get('runtime_turn_id')==third_turn['turn_id']]
        assert len(persisted)==1
        raw=persisted[0]['text'].strip()
        page.wait_for_function('text => Array.from(document.querySelectorAll(".msg.bot .bubble")).some(el => (el.dataset.rawText || "").trim() === text)',arg=raw,timeout=10000)
        matches=[b for b in page.locator('.msg.bot .bubble').all() if (b.get_attribute('data-raw-text') or '').strip()==raw]
        assert len(matches)==1 and matches[0].is_visible()
        expect(matches[0]).to_contain_text('05B33')
        expect(matches[0]).to_contain_text('11')
    else:
        third, third_turn, _ = _submit(live_web,
            'Continúa con el borrador en el chat, sin editar el formulario ni enviarlo. '
            'Incluye el número de pedido y todas las salsas que faltaron.',
            timeout=150)
    assert '05B33' in third['text'] and '11' in third['text'], third
    if third_turn.get('evidence_kind') == 'deferred_runtime_checkpoint':
        # Plain no-tool replies persist visible history immediately and defer
        # the runtime checkpoint. Normal scoped cleanup flushes that checkpoint;
        # it must not cause the successful user request to be resubmitted.
        stopped = page.request.post(base + '/api/chat', data={'text': '/stop', 'conversation_id': conversation})
        assert stopped.ok, stopped.text()
        third_turn = next(row for row in _turns(database, conversation)
                          if row['turn_id'] == third_turn['turn_id'])
    assert third_turn['parent_turn_id'] == second_turn['turn_id'], third_turn
    assert not any(t['tool_name'] in {'browser_type_id', 'browser_type_field'} and t['status'] == 'completed'
                   for t in third_turn.get('tool_results', [])), third_turn
    assert page.request.get(source + '/management/state').json()['submissions'] == before
    assert not errors
    out = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR']) / 'receipt-continuity.json'
    out.write_text(json.dumps({'conversation_id': conversation,
        'turns': [first_turn, second_turn, third_turn], 'submissions': 0}, ensure_ascii=False, indent=2))
