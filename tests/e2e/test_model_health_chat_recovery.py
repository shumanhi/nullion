from __future__ import annotations

import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


def test_web_chat_recovers_persisted_model_alert_with_valid_provider_request(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import model_clients, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolRegistry

    for key in ('NULLION_TASK_DECOMPOSITION_ENABLED', 'NULLION_MULTI_AGENT_ENABLED', 'NULLION_SKILL_LEARNING_ENABLED'):
        monkeypatch.setenv(key, '0')
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'missing.env'))
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *args, **kwargs: None)
    requests = []
    reply = '受信しました。続けてください。'

    class Response:
        def __init__(self, body):
            self.body = body
            self.status_code = 400 if body.get('max_output_tokens', 16) < 16 else 200
            self.reason_phrase = 'Invalid request' if self.status_code == 400 else 'OK'
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def read(self):
            return b'{"error":{"message":"Expected max_output_tokens >= 16","code":"integer_below_min_value"}}'
        def raise_for_status(self): assert self.status_code == 200
        def iter_lines(self):
            yield 'data: '+json.dumps({'type': 'response.output_text.delta', 'delta': reply})
            yield 'data: '+json.dumps({'type': 'response.completed', 'response': {'status': 'completed'}})

    class ProviderHTTP:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def stream(self, method, url, *, json, headers):
            requests.append(json)
            return Response(json)

    monkeypatch.setattr(model_clients, '_httpx', SimpleNamespace(Client=ProviderHTTP))
    model = model_clients.CodexResponsesModelClient(api_key='synthetic', account_id='synthetic', model='test')
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_doctor_action({'action_id': 'stale-provider-alert', 'action_type': 'inspect',
        'recommendation_code': 'model_api_unreachable', 'status': 'pending',
        'summary': 'Model API is unreachable', 'severity': 'high', 'owner': 'runtime'})
    runtime.checkpoint(force=True)
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=ToolRegistry())
    elapsed = []
    with TestClient(app) as client:
        for index, prompt in enumerate(('こんにちは', 'مرحبا', '続けてください')):
            started = time.monotonic()
            result = client.post('/api/chat', json={'text': prompt, 'conversation_id': f'web:health-recovery-{index}'})
            elapsed.append(time.monotonic()-started)
            assert result.status_code == 200, result.text
            assert result.json()['text'] == reply, result.json()
            assert result.json().get('artifacts', []) == []
    saved = load_runtime_store(tmp_path/'runtime.db')
    action = next(row for row in saved.list_doctor_actions() if row['action_id'] == 'stale-provider-alert')
    assert action['status'] == 'cancelled'
    probes = [body for body in requests if body.get('max_output_tokens') == 16]
    assert len(probes) == 1
    assert all(body.get('max_output_tokens', 16) >= 16 for body in requests)
    assert sum(elapsed)/len(elapsed) < 3 and max(elapsed) < 4, elapsed
