"""Saved profile and unrelated travel context across real Web/Telegram ingress."""
import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('surface', ['web', 'telegram'])
def test_local_request_keeps_saved_location_after_unrelated_trip(tmp_path, monkeypatch, record_property, surface):
    from fastapi.testclient import TestClient
    from nullion import preferences
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _remember_chat_turn
    from nullion.config import load_settings
    from nullion.telegram_app import ChatOperatorService
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    profile_path = tmp_path / 'profile.json'
    profile_path.write_text(json.dumps({'address': '京都市', 'name': 'Fixture'}))
    monkeypatch.setattr(preferences, '_PROFILE_PATH', profile_path)
    samples = []
    cases = [
        ('水曜日の18時、葵の空席を確認して。', '京都市'),
        ('تحقق من توفر طاولة في آوي يوم الأربعاء الساعة السادسة مساءً.', '京都市'),
        ('Check Aoi in Osaka around 6pm Wednesday.', 'Osaka'),
    ]
    class Model:
        primary_calls = 0
        expected = None
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None:
                return protocol
            if not kw.get('tools'):
                return text('{}')
            self.primary_calls += 1
            context = json.dumps(kw.get('messages'), ensure_ascii=False)
            assert 'Address: 京都市' in context
            assert 'A destination mentioned in a different task does not replace this saved default.' in context
            assert 'This task boundary does not erase saved user profile or preferences.' in context
            assert 'Ask one brief clarification question to identify the target or set' not in context
            return text(self.expected + ' — 何名ですか？')
    model = Model()
    registry = ToolRegistry()
    runtime.active_tool_registry = registry
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN': '123456:abcdefghijklmnopqrstuvwxyz',
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID': '123', 'NULLION_TELEGRAM_CHAT_ENABLED': 'true'})
    service = ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
        operator_chat_id='123', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, (prompt, expected) in enumerate(cases):
            cid = f'web:location-{index}' if surface == 'web' else 'telegram:123'
            _remember_chat_turn(runtime, chat_id='123' if surface == 'telegram' else cid,
                conversation_id=cid, user_message='Plan a trip to Los Angeles.',
                assistant_reply='Los Angeles trip options.', conversation_turn_id=f'trip-{index}')
            model.expected = expected
            started = time.monotonic()
            if surface == 'web':
                response = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
                assert response.status_code == 200, response.text
                visible = response.json()['text']
            else:
                sent = []
                class Message:
                    text = prompt
                    message_id = 19000 + index
                    reply_to_message = None
                    chat = SimpleNamespace(id=123, send_action=lambda **kw: asyncio.sleep(0))
                    async def reply_text(self, value, **kw):
                        sent.append(value)
                        return SimpleNamespace(message_id=20000 + len(sent))
                asyncio.run(service.on_message(SimpleNamespace(update_id=19000 + index,
                    message=Message()), SimpleNamespace(bot=None)))
                assert sent
                visible = sent[-1]
            samples.append(time.monotonic() - started)
            assert expected in visible and 'Los Angeles' not in visible, visible
            turns = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')
            turn = next(t for t in reversed(turns) if t.get('user_message') == prompt)
            assert not turn.get('tool_results'), turn
            assert expected in turn['assistant_reply']
    assert model.primary_calls == len(cases), 'Location defaults must not add another model round'
    assert sum(samples) / len(samples) < 2 and max(samples) < 4, samples
    record_property('mean_location_turn_seconds', sum(samples) / len(samples))
