"""Real API/graph/persistence/delivery; failures are injected at the external model boundary."""
import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest
from test_scheduled_report_completion import setup_runtime, text_response, tool_response
from test_source_and_status_delivery_contracts import source_presentation_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('prompt', ['Find the cheapest powder using my saved location.', '保存した住所で最安値を調べて。', 'Busca el polvo más barato con mi ubicación guardada.'])
@pytest.mark.parametrize('failure_phase', ['before_tools', 'after_tool', 'invalid_schema', 'persistent'])
def test_chat_retains_profile_and_completed_tools_after_model_failure(tmp_path, monkeypatch, record_property, prompt, failure_phase):
    from fastapi.testclient import TestClient
    from nullion import preferences, web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.telegram_app import _deliver_reply
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    runtime = setup_runtime(tmp_path, monkeypatch)
    profile = tmp_path/'profile.json'; profile.write_text(json.dumps({'address': 'Example City, NY'}))
    monkeypatch.setattr(preferences, '_PROFILE_PATH', profile)
    chat_store._store = None
    reads = []
    registry = ToolRegistry()
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', {
            'result_kind': 'source_observations', 'report_status': 'completed',
            'observations': [{'label': 'Powder A', 'value': '8.10 USD · 12 oz · Example City, NY',
                              'source_url': 'https://example.com/powder'}], 'limitations': []})
    registry.register(ToolSpec('lookup_source', 'Read live offers', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), read)
    class Model:
        steps = 0; failures = 0; normal_calls = 0
        failed_kwargs = None
        def create(self, **kw):
            names = {t['name'] for t in kw.get('tools', [])}
            system = kw.get('system', '')
            if system.startswith('You are a material-requirements planner'):
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement': 'Read the displayed offer',
                    'requirement_kind': 'fact', 'evidence_subject_id': 'powder',
                    'eligible_evidence_tool_names': ['lookup_source']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                return text_response(json.dumps({'disposition': 'complete', 'unresolved_requirements': [], 'retry_tool_names': []}))
            if names == {'present_source_answer'}: return source_presentation_response(kw)
            if not names: return text_response('Powder A: 8.10 USD, 12 oz, Example City, NY.')
            self.normal_calls += 1
            assert 'Example City, NY' in json.dumps(kw), kw
            if failure_phase == 'before_tools' and self.failures == 0:
                self.failures += 1
                raise RuntimeError('Synthetic external provider failure')
            if reads and failure_phase != 'before_tools':
                assert '8.10 USD' in json.dumps(kw)
                assert 'lookup_source' in names
                if self.failures == 0 or failure_phase == 'persistent':
                    self.failures += 1
                    if self.failed_kwargs is None: self.failed_kwargs = kw
                    else: assert kw == self.failed_kwargs
                    if failure_phase == 'invalid_schema': return None
                    raise RuntimeError('Synthetic external provider failure')
                assert kw == self.failed_kwargs
            self.steps += 1
            if self.steps == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['lookup_source'], 'required_tool_names': ['lookup_source']})
            if self.steps == 2: return tool_response('lookup_source', {})
            return text_response('Powder A: 8.10 USD, 12 oz, Example City, NY.')
    model = Model(); runtime.model_client = model
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    conversation = 'web:profile-recovery'
    started = time.perf_counter()
    with TestClient(app) as client:
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
        assert response.status_code == 200, response.text
        payload = response.json()
        if failure_phase == 'persistent':
            assert 'model request failed' in payload['text'], payload
        else:
            assert '8.10 USD' in payload['text'] and '12 oz' in payload['text'], payload
            assert 'Example City, NY' in payload['text'], payload
        assert not payload['artifacts']
        events = runtime.store.list_conversation_events(conversation)
        turn = [e for e in events if e.get('event_type') == 'conversation.chat_turn'][-1]
        assert len([t for t in turn['tool_results'] if t['tool_name'] == 'lookup_source']) == 1, turn
        failures = [e for e in events if e.get('event_type') == 'conversation.model_failure']
        assert len(failures) == (2 if failure_phase == 'persistent' else 1), failures
        assert 'Synthetic external' not in json.dumps(failures)
        if failure_phase != 'before_tools':
            assert 'lookup_source' in failures[0]['completed_tool_names']
        restored = client.get('/api/chat/history/'+conversation,
            params={'turn_ids': json.dumps([turn['turn_id']])})
        assert restored.status_code == 200
        assert restored.json()['terminal_replies'][0]['text'] == payload['text']
    assert len(reads) == 1
    assert model.failures == (2 if failure_phase == 'persistent' else 1)
    record_property('elapsed_seconds', time.perf_counter()-started)
    assert time.perf_counter()-started < 10
    wire = []
    class Message:
        chat = SimpleNamespace(id=733); message_id = 19
        async def reply_text(self, text, **kwargs):
            wire.append(text); return SimpleNamespace(message_id=20)
    asyncio.run(_deliver_reply(Message(), payload['text'], principal_id='telegram:733'))
    assert len(wire) == 1
    if failure_phase == 'persistent':
        assert 'model request failed' in wire[0]
    else:
        assert '8.10 USD' in wire[0] and 'Example City, NY' in wire[0]


def test_plain_chat_does_not_enter_recovery_or_add_provider_calls(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    runtime = setup_runtime(tmp_path, monkeypatch); chat_store._store = None
    registry = ToolRegistry()
    def unavailable(call):
        raise AssertionError('Unrelated external capability entered the plain-turn path')
    registry.register(ToolSpec('lookup_source', 'Optional remote capability', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), unavailable)
    class Model:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return text_response('こんにちは。')
    model = Model(); runtime.model_client = model
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    elapsed = []
    with TestClient(app) as client:
        for index, prompt in enumerate(['こんにちは', 'مرحبا', 'Hola']):
            started = time.perf_counter()
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': f'web:plain-{index}'})
            elapsed.append(time.perf_counter()-started)
            assert response.status_code == 200 and response.json()['text'] == 'こんにちは。', response.text
    assert model.calls == 3
    record_property('average_seconds', sum(elapsed)/len(elapsed))
    record_property('max_seconds', max(elapsed))
    assert sum(elapsed)/len(elapsed) < 2 and max(elapsed) < 3, elapsed
