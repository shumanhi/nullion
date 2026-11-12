"""Opt-in real Web replay of negotiated output after multiple selected replies."""
import os
from pathlib import Path

import pytest

from tests.e2e.test_live_stage_web_matrix import live_web, _submit, catalogue

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1',
    reason='Uses real stage services, browser and provider quota')


def test_live_web_negotiated_output_keeps_goal_and_delivers_generated_png(live_web, record_property):
    from scripts import live_stage_chat_contract_probe as probe
    page, frames, errors, database, cid, base = live_web
    # One shared baseline per live session includes ordinary-turn latency and
    # an adjacent family. Never run this paid probe implicitly in local tests.
    assert probe.shared_live_baseline(['--base-url',base,'--runtime-db',str(database),
        '--scenario','simple','--scenario','weather','--repeat','2']) == 0
    prompt = (
        'Read https://example.com and create a small visual reference page from its actual text. '
        'Before starting, ask me to choose the format: 1. PDF, 2. PNG. Wait for my reply. '
        'Then ask me separately to choose the heading language: 1. English, 2. Spanish. '
        'Wait for that reply before doing the research and creating the file. '
        'Use the available local rendering tools; keep the source facts in the finished page.'
    )
    first, first_turn, first_ms = _submit(live_web, prompt, timeout=180)
    assert not first.get('artifacts'), first
    second, second_turn, second_ms = _submit(live_web, '2', timeout=180)
    assert not second.get('artifacts'), second
    final, final_turn, final_ms = _submit(live_web, '1', timeout=900)
    artifacts = final.get('artifacts') or []
    assert len(artifacts) == 1, final
    content = probe._artifact_bytes(base, artifacts[0], timeout=30)
    assert content.startswith(b'\x89PNG\r\n\x1a\n'), artifacts
    import io
    from PIL import Image
    with Image.open(io.BytesIO(content)) as image:
        assert image.width >= 100 and image.height >= 100
    results = final_turn.get('tool_results') or []
    assert any(row.get('status') == 'completed' and row.get('tool_name') in
        {'image_generate','terminal_exec','local_exec'} for row in results), results
    assert final_turn.get('parent_turn_id') == second_turn['turn_id'], final_turn
    assert second_turn.get('parent_turn_id') == first_turn['turn_id'], second_turn
    assert not errors
    if evidence := os.environ.get('NULLION_LIVE_EVIDENCE_DIR'):
        page.locator('#messages').screenshot(path=str(Path(evidence)/'negotiated-generated-output.png'))
    record_property('request_latency_ms', [first_ms, second_ms, final_ms])
