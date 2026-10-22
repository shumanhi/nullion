from datetime import UTC, datetime, timedelta
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('surface', ['telegram', 'web'])
@pytest.mark.parametrize('age_hours', [18, 0])
def test_real_chat_boundary_recovers_from_invalid_health_probe_flag(tmp_path, monkeypatch, surface, age_hours):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import ChatOperatorService
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'missing.env'))
    monkeypatch.setenv('NULLION_WEB_BACKGROUND_MEMORY_ENABLED', 'false')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    timestamp = (datetime.now(UTC) - timedelta(hours=age_hours)).isoformat()
    runtime.store.add_doctor_action({
        'action_id': 'invalid-probe-outage', 'status': 'pending',
        'owner': 'doctor', 'action_type': 'inspect', 'summary': 'Model API is unreachable', 'severity': 'high',
        'recommendation_code': 'model_api_unreachable',
        'error': 'HTTP 400: expected completion budget >=16, got1',
        'created_at': timestamp, 'updated_at': timestamp,
    })
    runtime.checkpoint(force=True)

    class Provider:
        probe_calls = 0
        chat_calls = 0
        def create(self, *, messages, tools, max_tokens=None, **kwargs):
            if max_tokens == 16:
                self.probe_calls += 1
                # Slow external boundary must not re-enter ordinary chat.
                time.sleep(.25)
                return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'pong'}]}
            if max_tokens is not None and max_tokens < 16:
                raise ValueError('Completion budget below the provider minimum')
            self.chat_calls += 1
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'The configured model answered this request.'}]}

    provider = Provider()
    orchestrator = AgentOrchestrator(model_client=provider)
    settings = NullionSettings(telegram=TelegramSettings(chat_enabled=True, operator_chat_id='123'))
    elapsed = []
    prompts = ['比較する候補を教えてください', 'اعرض الخيارات المتاحة', 'Explain the available choices']
    if surface == 'telegram':
        service = ChatOperatorService(runtime=runtime, bot_token='synthetic-token', operator_chat_id='123',
                                      settings=settings, model_client=provider, agent_orchestrator=orchestrator)
        for prompt in prompts:
            started = time.monotonic()
            reply = service.handle_text_message(text=prompt, chat_id='123', append_activity_trace=False)
            elapsed.append(time.monotonic() - started)
            assert 'configured model answered' in reply
    else:
        app = create_app(runtime, orchestrator=orchestrator, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))
        with TestClient(app) as client:
            for prompt in prompts:
                started = time.monotonic()
                response = client.post('/api/chat', json={'text': prompt, 'conversation_id': 'web:health-recovery'})
                elapsed.append(time.monotonic() - started)
                assert response.status_code == 200
                assert 'configured model answered' in response.json()['text']
    assert provider.chat_calls >= len(prompts)
    assert provider.probe_calls == (0 if age_hours else 1)
    assert sum(elapsed) / len(elapsed) < 2.5
    assert max(elapsed) < 3.5
    action = runtime.store.get_doctor_action('invalid-probe-outage')
    assert action['status'] == ('pending' if age_hours else 'cancelled')
    conversation_id = 'telegram:123' if surface == 'telegram' else 'web:health-recovery'
    turns = runtime.store.list_recent_conversation_events(conversation_id, event_type='conversation.chat_turn', limit=10)
    assert len(turns) >= len(prompts)
    assert all('configured model answered' in turn.get('assistant_reply', '') for turn in turns[:3])
