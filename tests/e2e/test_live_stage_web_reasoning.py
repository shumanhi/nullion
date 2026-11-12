"""Opt-in real-provider Web UI acceptance for the shared request contract."""
import json
import os
import statistics
from pathlib import Path

import pytest
from test_live_stage_web_matrix import live_web, _submit

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in live stage UI')


def test_live_stage_web_plain_requests_retain_fast_path(live_web, record_property):
    elapsed = []
    for prompt in ['Reply with just: ready.', 'Responde solamente: listo.', '「準備完了」だけで答えてください。']:
        response, turn, ms = _submit(live_web, prompt, timeout=90)
        assert not turn['tool_results'], turn
        assert response['text'].strip() and not response['artifacts']
        elapsed.append(ms)
    record_property('plain_average_ms', statistics.mean(elapsed))
    record_property('plain_max_ms', max(elapsed))
    assert statistics.mean(elapsed) < 15000 and max(elapsed) < 25000, elapsed


@pytest.mark.parametrize('prompt', [
    'Plan my upcoming museum visit for January 1, 2020.',
    'Planea mi próxima visita al museo para el 1 de enero de 2020.',
])
def test_live_stage_web_checks_contradicted_premise_before_actions(live_web, prompt):
    response, turn, _ = _submit(live_web, prompt, timeout=120)
    assert '2020' in response['text'], response
    # An open request for a replacement date is also valid. Numbered choices
    # are required when alternatives are presented; the English replay covers
    # that form without requiring the model to invent a date in every language.
    assert '?' in response['text'] or all(f'{index}.' in response['text'] for index in (1, 2)), response
    assert not response['artifacts']
    from nullion.tools import create_core_tool_registry, ToolSideEffectClass
    registry = create_core_tool_registry(workspace_root=Path('/tmp/live-review-specs'))
    for result in turn['tool_results']:
        if result['status'] == 'completed':
            assert registry.get_spec(result['tool_name']).side_effect_class == ToolSideEffectClass.READ


def test_live_stage_web_generated_text_delivers_exact_bytes_once(live_web):
    from uuid import uuid4
    filename = 'measurement-' + uuid4().hex[:8] + '.txt'
    response, turn, _ = _submit(live_web,
        f'Create and attach a new UTF-8 plain-text file named {filename}. Its entire contents must be '
        'the four characters inside this JSON string: "7 kg". No other characters.', timeout=180)
    artifacts = response['artifacts']
    assert len(artifacts) == 1, response
    artifact = artifacts[0]
    assert artifact['name'] == filename, artifact
    page, _, _, _, _, base = live_web
    download = page.request.get(base + artifact['url'] if artifact['url'].startswith('/') else artifact['url'])
    assert download.ok and download.body() == b'7 kg'
    completed = [row for row in turn['tool_results'] if row['status'] == 'completed']
    assert any(row['tool_name'] == 'file_write' for row in completed), json.dumps(turn)
    assert len([row for row in completed if row['tool_name'] == 'file_write']) == 1


def test_live_stage_web_read_workflow_latency_uses_multiple_samples(live_web, record_property):
    elapsed = []
    for _ in range(3):
        response, turn, ms = _submit(live_web, "What's the weather like in 10024, New York?", timeout=90)
        results = [row for row in turn['tool_results'] if row['tool_name'] == 'weather_forecast' and row['status'] == 'completed']
        assert results and all(row['output']['location'].get('postal_code') == '10024' for row in results), turn
        assert '10024' in response['text'] and not response['artifacts'], response
        elapsed.append(ms)
    record_property('read_workflow_samples_ms', json.dumps(elapsed))
    from scripts.live_stage_chat_contract_probe import assess_live_latency
    assess_live_latency(statistics.mean(elapsed),30000,label='weather average')
    assess_live_latency(max(elapsed),45000,label='weather slowest sample')
