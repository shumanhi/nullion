"""Real API, browser tool adapter, graph, SQLite and Telegram delivery boundaries."""
import asyncio
import json
import os
from types import SimpleNamespace

import pytest
from test_scheduled_report_completion import setup_runtime, text_response, tool_response
from test_source_and_status_delivery_contracts import source_presentation_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


@pytest.mark.parametrize('prompt', ['Check the claims in this source.', 'この資料の主張を検証して。', 'Verifica las afirmaciones de esta fuente.'])
@pytest.mark.parametrize('mode', ['terminal_partial', 'terminal_empty', 'presentation_invalid', 'review_timeout', 'alternative_method', 'explicit_continuation'])
def test_terminal_research_returns_findings_and_specific_blocker_without_repeat(tmp_path, monkeypatch, prompt, mode):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin import browser_use_adapter, browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.telegram_app import _deliver_reply
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0'); monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setattr(browser_use_adapter, 'browser_use_available', lambda: True)
    monkeypatch.setattr(browser_use_adapter, 'browser_use_python', lambda: '/external/worker')
    runtime = setup_runtime(tmp_path, monkeypatch); chat_store._store = None
    calls = []; alternatives = []
    def remote_browser(task, **kw):
        calls.append(task.url)
        continuation = mode == 'explicit_continuation' and len(calls) == 1
        answered = mode == 'explicit_continuation' and len(calls) == 2
        empty = mode == 'terminal_empty' or continuation
        return {'backend': 'browser-use', 'result_kind': 'source_observations',
            'goal_status': 'answered' if answered else 'blocked' if empty else 'partial',
            'report_status': 'completed' if answered else 'blocked' if empty else 'partial',
            'next_action': 'alternative_source' if continuation else 'report',
            'continuation': {'kind': 'alternative_source', 'tool_names': ['browser_run_task'], 'source_url': task.url} if continuation else None,
            'observations': [] if empty else [{'label': 'Source observation', 'value': 'Recorded statement: 42',
                'quote': 'Recorded statement: 42', 'source_url': task.url}],
            'blocker': None if answered else 'Login blocked access to the complete source text.',
            'unknowns': [] if answered else ['The statement has not been independently verified.'],
            'page_url': task.url, 'elapsed_seconds': 540, 'steps': 29,
            'artifact_paths': [], 'artifact_descriptors': []}
    monkeypatch.setattr(browser_use_worker, 'run_isolated_browser_task', remote_browser)
    registry = ToolRegistry()
    def secondary(call):
        alternatives.append(call.tool_name)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', {
            'result_kind': 'source_observations', 'report_status': 'completed',
            'observations': [{'value': 'Independent record: 39', 'quote': 'Independent record: 39',
                'source_url': 'https://example.net/record'}], 'unknowns': []})
    registry.register(ToolSpec('secondary_reader', 'Read independent source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), secondary)
    class Model:
        steps = 0; reviews = 0
        def create(self, **kw):
            names = {t['name'] for t in kw.get('tools', [])}; system = kw.get('system', '')
            if system.startswith('You are a material-requirements planner'):
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement': 'Establish and independently verify the source claims',
                    'requirement_kind': 'fact', 'evidence_subject_id': 'source',
                    'eligible_evidence_tool_names': ['browser_run_task', 'secondary_reader'] if mode == 'alternative_method' and alternatives else ['browser_run_task']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews += 1
                if mode == 'review_timeout': raise TimeoutError('Review provider timeout')
                if mode == 'explicit_continuation' and len(calls) == 2:
                    payload = json.loads(kw['messages'][0]['content'][0]['text'])
                    source = next(r for r in payload['tool_evidence'] if r['tool_name'] == 'browser_run_task'
                        and r['output'].get('report_status') == 'completed')
                    assert source['output']['observations'][0]['quote'] == 'Recorded statement: 42'
                    return text_response(json.dumps({'disposition': 'complete', 'unresolved_requirements': [],
                        'retry_tool_names': [], 'all_material_requirements_enumerated': True,
                        'material_requirements': [{**{k: row[k] for k in ('requirement_id', 'requirement', 'requirement_kind')},
                            'satisfied': True, 'draft_covered': True, 'evidence_invocation_ids': [source['invocation_id']]}
                            for row in payload['material_requirement_contract']]}))
                if mode == 'alternative_method' and alternatives:
                    payload = json.loads(kw['messages'][0]['content'][0]['text'])
                    source = next(r for r in payload['tool_evidence'] if r['tool_name'] == 'secondary_reader')
                    return text_response(json.dumps({'disposition': 'complete', 'unresolved_requirements': [],
                        'retry_tool_names': [], 'all_material_requirements_enumerated': True,
                        'material_requirements': [{**{k: row[k] for k in ('requirement_id', 'requirement', 'requirement_kind')},
                            'satisfied': True, 'draft_covered': True, 'evidence_invocation_ids': [source['invocation_id']]}
                            for row in payload['material_requirement_contract']]}))
                return text_response(json.dumps({'disposition': 'retry',
                    'unresolved_requirements': ['Independent verification is incomplete'],
                    'retry_tool_names': ['browser_run_task', 'secondary_reader'] if mode == 'alternative_method' else ['browser_run_task']}))
            if names == {'present_source_answer'}:
                if mode == 'presentation_invalid': return tool_response('present_source_answer', {'invalid': True})
                response = source_presentation_response(kw)
                if mode == 'alternative_method' and alternatives:
                    response['content'][0]['input']['limitation_ids'] = []
                return response
            if not names: return text_response('{}')
            if 'select_attachment_format' in names: return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            self.steps += 1
            if self.steps == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['browser_run_task', 'secondary_reader'], 'required_tool_names': ['browser_run_task']})
            if self.steps == 2:
                return tool_response('browser_run_task', {'url': 'https://example.com/source', 'task': prompt})
            if mode == 'alternative_method':
                assert names == {'secondary_reader'}, names
                return tool_response('secondary_reader', {})
            if mode == 'explicit_continuation':
                return tool_response('browser_run_task', {'url': 'https://example.net/source', 'task': prompt})
            pytest.fail('Terminal browser work was repeated')
    model = Model(); runtime.model_client = model
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    conversation = 'web:independent-research-user'
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    with TestClient(app) as client:
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
        assert response.status_code == 200, response.text
        payload = response.json(); text = payload['text']
        if mode not in {'explicit_continuation', 'alternative_method'}:
            assert 'Login blocked access' in text and 'not been independently verified' in text, text
        if mode not in {'terminal_empty', 'presentation_invalid'}: assert 'Recorded statement: 42' in text, text
        else: assert 'Recorded statement' not in text
        if mode == 'alternative_method': assert 'Independent record: 39' in text
        assert 'completion remains unverified' not in text and not payload['artifacts']
        events = runtime.store.list_conversation_events(conversation)
        turn = [e for e in events if e.get('event_type') == 'conversation.chat_turn'][-1]
        assert turn['assistant_reply'] == text
        assert len([r for r in turn['tool_results'] if r['tool_name'] == 'browser_run_task']) == (2 if mode == 'explicit_continuation' else 1)
        restored = client.get('/api/chat/history/'+conversation, params={'turn_ids': json.dumps([turn['turn_id']])})
        assert restored.json()['terminal_replies'][0]['text'] == text
        assert all(p['phase'] not in {'schema_followup_tool_retry', 'local_system_fallback_retry'}
                   for e in events if e.get('event_type') == 'conversation.latency_timing' for p in e.get('phase_timings', []))
    assert len(calls) == (2 if mode == 'explicit_continuation' else 1)
    assert model.reviews == (0 if mode == 'presentation_invalid' else 2 if mode == 'alternative_method' else 1)
    assert alternatives == (['secondary_reader'] if mode == 'alternative_method' else [])
    wire = []
    class Message:
        chat = SimpleNamespace(id=911); message_id = 12
        async def reply_text(self, text, **kw):
            wire.append(text); return SimpleNamespace(message_id=13)
    asyncio.run(_deliver_reply(Message(), text, principal_id='telegram:911'))
    assert wire and ('Login blocked access' in wire[0] if mode not in {'explicit_continuation', 'alternative_method'} else '42' in wire[0])
