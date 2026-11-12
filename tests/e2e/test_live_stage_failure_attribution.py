"""Opt-in real-model Web UI fallback acceptance using an explicit controlled source."""
import json
import os
import time
from pathlib import Path

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit, _turns

pytestmark = pytest.mark.skipif(
    os.environ.get('NULLION_LIVE_STAGE_E2E') != '1'
    or not os.environ.get('NULLION_LIVE_FALLBACK_SOURCE_URL')
    or not os.environ.get('NULLION_LIVE_FALLBACK_EXPECTED_TEXT'),
    reason='Explicit authorized stage fallback fixture required')


@pytest.mark.parametrize('prompt,followup', [
    ('Compara los candidatos de esta página. Primero intenta extraer el registro de #empty; '
     'si no contiene datos, lee #observations y continúa con la evidencia disponible. '
     'Distingue lo confirmado de la comparación que no pudiste completar. No realices compras.',
     'Conserva lo confirmado y explica qué sigue sin comprobarse, sin repetir las consultas.'),
    ('このページの候補を比較してください。最初に #empty の詳細レコードを取得し、データがなければ '
     '#observations の本文を読んで続けてください。確認した内容と未完了の比較を区別してください。購入しないでください。',
     '情報源を再取得せず、確認済みの内容と残っている不確実性を説明してください。'),
])
def test_live_fallback_keeps_confirmed_findings_and_current_uncertainty(live_web, prompt, followup):
    from playwright.sync_api import expect
    page, frames, errors, database, conversation, base = live_web
    url = os.environ['NULLION_LIVE_FALLBACK_SOURCE_URL']
    expected = os.environ['NULLION_LIVE_FALLBACK_EXPECTED_TEXT']
    response, turn, elapsed = _submit(live_web, prompt + ' ' + url, timeout=300)
    results = turn['tool_results']
    failed = [r for r in results if r['tool_name'] == 'browser_extract_detail' and r['status'] == 'failed']
    reads = [r for r in results if r['tool_name'] in {'browser_extract_text', 'browser_extract_items'}
        and r['status'] == 'completed']
    assert failed and reads, turn
    assert expected in json.dumps([r['output'] for r in reads], ensure_ascii=False)
    assert expected in response['text']
    assert "The browser operation failed. The requested result wasn't confirmed." not in response['text']
    page.reload(wait_until='domcontentloaded')
    expect(page.locator('#messages')).to_contain_text(expected, timeout=10000)
    continued, continued_turn, continued_elapsed = _submit(live_web, followup, timeout=240)
    # Plain-text turns defer their secondary audit checkpoint and do not
    # manufacture an action-fulfillment flag. Require their actual saved
    # lineage and observations instead of treating an absent flag as failure.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        saved = next((row for row in _turns(database, conversation)
                      if row.get('turn_id') == continued_turn.get('turn_id')), None)
        if saved:
            continued_turn = saved
            break
        page.wait_for_timeout(250)
    assert saved and continued_turn.get('parent_turn_id') == turn['turn_id'], continued_turn
    assert expected in continued['text']
    assert all(value in continued['text'] for value in ('100', '9', '0.09', 'Sample B')), continued
    assert "The browser operation failed. The requested result wasn't confirmed." not in continued['text']
    assert not any((r['tool_name'].startswith('browser_') or r['tool_name'] in {'web_search','web_fetch'})
                   and not r.get('invocation_id','').startswith('continuation:')
                   for r in continued_turn.get('tool_results',[])), continued_turn
    assert continued_turn.get('response_fulfilled') is not False,continued_turn
    assert not errors, errors
    evidence = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / ('fallback-' + str(len(prompt)) + '.json')).write_text(json.dumps({
        'conversation_id': conversation, 'response': response, 'turn': turn, 'elapsed_ms': elapsed,
        'followup_response': continued, 'followup_turn': continued_turn,
        'followup_elapsed_ms': continued_elapsed}, ensure_ascii=False, indent=2))
