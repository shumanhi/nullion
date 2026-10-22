"""Opt-in real stage recovery after exhausted page-condition repair."""
import os
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Requires authorized real stage browser and model quota')


def test_live_stage_keeps_answer_after_two_failed_waits():
    from scripts import live_stage_chat_contract_probe as probe
    base = os.environ.get('NULLION_LIVE_BASE_URL', 'http://127.0.0.1:8753').rstrip('/')
    database = Path(os.environ['NULLION_LIVE_RUNTIME_DB'])
    conversation = 'web:live-repair-budget:'+uuid4().hex
    prompt = ('Read the actual price and copies in stock for '
        'https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html . '
        'Optional diagnostic: if you need to wait for RECOVERY-CHECK-NOT-PRESENT, try at most twice '
        'and inspect the page between attempts. Its absence is expected; successful waits are not '
        'required. Stop waiting and answer from the observed page. Do not create files or purchase anything.')
    response, elapsed = probe._post_json(base+'/api/chat', {'text': prompt, 'conversation_id': conversation}, timeout=180)
    assert elapsed < 120000
    assert '51.77' in response['text'] and '22' in response['text'], response['text']
    assert not response.get('artifacts')
    turn = probe._chat_turn_for_user_message_with_poll(database, conversation, prompt)
    assert turn, 'The final answer must retain runtime tool evidence'
    tools = turn['tool_results']
    waits = [t for t in tools if t['tool_name'] == 'browser_wait_for']
    assert len(waits) <= 2, waits
    assert all(t['status'] == 'failed' and t['output'].get('reason') == 'browser_condition_unmet' for t in waits)
    scopes = [t['output'] for t in tools if t['tool_name'] == 'request_tool_scope' and t['status'] == 'completed']
    if len(waits) == 2:
        assert any('browser_wait_for' not in s.get('available_tools', []) and s.get('available_tools') for s in scopes), scopes
    assert any(t['status'] == 'completed' and t['tool_name'] in {'browser_extract_text','browser_extract_detail','browser_run_js'} for t in tools)
