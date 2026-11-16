"""Opt-in stage acceptance for browser shopping, profile recall and adjacent tools."""
import json
import os
import re

import pytest
from tests.e2e.test_live_stage_web_matrix import live_web, _submit

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Requires authorized running stage and real provider quota')


@pytest.mark.skipif(not os.environ.get('NULLION_LIVE_PROFILE_CITY_NAMES'),reason='Expected saved-profile city required')
def test_live_stage_shopping_and_saved_profile_context(live_web):
    from scripts import live_stage_chat_contract_probe as probe
    page,frames,errors,database,conversation,base=live_web
    expected_cities = json.loads(os.environ['NULLION_LIVE_PROFILE_CITY_NAMES'])
    assert isinstance(expected_cities, list) and expected_cities
    assert all(isinstance(city, str) and city.strip() for city in expected_cities)
    prompt = ('Find me the cheapest psyllium husk powder on amazon. Use my saved profile location as the default. '
        'Read-only: do not sign in, purchase, or solve access challenges; do not send my street address to websites.')
    # Observe the delegated browser's existing 600-second execution budget
    # plus final composition. This does not extend any production work limit.
    response,turn,elapsed=_submit(live_web,prompt,timeout=720)
    assert turn, 'Completed shopping turn must retain runtime tool evidence'
    names = probe._tool_names_from_chat_turn(turn)
    scenario = probe.Scenario('profile_shopping', prompt, None, 120000,
        required_tool_any=('browser_read_pages', 'browser_extract_detail', 'browser_extract_items', 'browser_extract_text', 'browser_run_task'),
        forbidden_tool_any=('email_send', 'delete_cron', 'toggle_cron'), require_no_artifacts=True,
        forbidden_text=("can't access live Amazon", 'Send me 2–5 Amazon links'))
    probe._assert_contract(scenario, response=response, elapsed_ms=elapsed, tool_names=names,
        runtime_db_available=True, tool_output_texts=[json.dumps(t) for t in turn.get('tool_results', [])])
    if names.intersection({'browser_read_pages'}) and not names.intersection({
            'browser_extract_detail', 'browser_extract_items', 'browser_extract_text', 'browser_run_task'}):
        assert any(row.get('tool_name') == 'browser_read_pages'
            and row.get('status') == 'completed'
            and row.get('response_requirements', {}).get('_native_completed_reads', {}).get('read_only') is True
            and set(row['response_requirements']['_native_completed_reads'].get('tool_names', []))
                .intersection({'browser_extract_detail', 'browser_extract_items', 'browser_extract_text'})
            for row in turn.get('tool_results', [])), turn
    assert re.search(r'\$\s*\d|\d[.,]\d+\s*USD', response['text']), response['text']
    assert 'amazon.com/' in response['text'], response['text']
    profile_prompt = 'Which city is in my saved profile? Answer with the city only, without my street address or other contact information.'
    recalled,_,_=_submit(live_web,profile_prompt,timeout=90)
    assert any(city.casefold() in recalled['text'].casefold() for city in expected_cities), 'Saved city was not recalled'
    assert len(recalled['text']) < 160, 'City-only recall should not expose the whole profile'
    assert not errors,errors
