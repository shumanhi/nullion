"""Recheck an unchanged real report after a validation-only correction, without new model calls."""
import hashlib
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, APP, catalogue, _turns

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1'
    or not os.environ.get('NULLION_LIVE_REPORT_REPLAY_FILE'), reason='Explicit retained real-report receipt required')


def test_real_delivered_report_retains_its_ui_files_bytes_and_original_contract(live_web):
    from playwright.sync_api import expect
    path = Path(os.environ['NULLION_LIVE_REPORT_REPLAY_FILE'])
    assert not path.is_absolute() and '..' not in path.parts
    receipt = json.loads((APP/path).read_text())
    original = json.loads((APP/receipt['original_proof']).read_text())
    page, frames, errors, database, unused_cid, base = live_web
    cid = receipt['conversation_id']
    assert cid.startswith('web:live-ui-audit:') and original['transport'] == 'web-composer'
    assert original['conversation_id'] == cid
    before = _turns(database, cid)
    turn = next(row for row in before if row['turn_id'] == receipt['turn_id'])
    assert turn.get('response_fulfilled') is True, turn
    response = original['terminal_frames'][-1]
    expected = {row['name']: row for row in receipt['artifacts']}
    assert set(expected) == {catalogue._artifact_name(row) for row in response['artifacts']}
    for row in response['artifacts']:
        data, _ = catalogue._artifact_bytes(base, row, timeout=30)
        saved = expected[catalogue._artifact_name(row)]
        assert hashlib.sha256(data).hexdigest() == saved['sha256'] and len(data) == saved['bytes']
    scenario = catalogue.SCENARIOS[receipt['scenario']]
    scenario = catalogue._scenario_for_config(scenario, catalogue._get_json(base+'/api/config', timeout=15))
    catalogue._assert_turn_tools(scenario, turn)
    catalogue._assert_contract(scenario, response=response, elapsed_ms=max(original['elapsed_ms']),
        tool_names=catalogue._tool_names_from_chat_turn(turn), runtime_db_available=True,
        tool_output_texts=[json.dumps(row) for row in turn.get('tool_results', [])])
    output = Path(os.environ['NULLION_LIVE_EVIDENCE_DIR'])/'retained-report'
    output.mkdir(parents=True, exist_ok=True)
    downloads = catalogue._download_and_check_artifacts(base, response['artifacts'],
        scenario=scenario, timeout=30, download_dir=output/'downloads')
    # The standard fixture intentionally pins its fresh id on every reload.
    # Give this read-only inspection its own context pinned to the old id.
    context = page.context.browser.new_context(accept_downloads=True)
    context.add_init_script('localStorage.setItem("nullion_chat_restore_suppressed","true");'
        'localStorage.setItem("nullion_conv_id",'+json.dumps(cid)+');')
    page = context.new_page(); page.on('pageerror', lambda error: errors.append(str(error)))
    try:
        page.goto(base, wait_until='domcontentloaded')
        for cycle in range(2):
            if cycle:
                page.reload(wait_until='domcontentloaded')
            for row in receipt['artifacts']:
                link = page.locator('#messages a[download]').filter(has_text=row['name'])
                expect(link).to_have_count(1); expect(link).to_be_visible()
                if cycle == 0:
                    # The desktop button POSTs to /save rather than emitting a
                    # browser download event. Inspect its actual target bytes
                    # without writing copies into the host's Downloads folder.
                    target = link.get_attribute('href')
                    assert target == row['url']
                    delivered = page.request.get(base + target)
                    assert delivered.ok, delivered.status
                    assert hashlib.sha256(delivered.body()).hexdigest() == row['sha256']
        assert not errors
        assert {row['turn_id'] for row in _turns(database, cid)} == {row['turn_id'] for row in before}, 'Read-only verification generated a new turn'
    finally:
        try:
            page.screenshot(path=str(output/'web.png'), full_page=False)
        finally:
            context.close()
    (output/'result.json').write_text(json.dumps({'status':'passed','original_proof':receipt['original_proof'],
        'original_scenario':receipt['scenario'],'conversation_id':cid,'turn_id':receipt['turn_id'],
        'model_calls':0,'source_calls':0,'unchanged_artifact_hashes':expected,'downloads':downloads},indent=2)+'\n')
