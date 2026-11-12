"""Real Web/SQLite recovery distinguishes sources inside one browser session."""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker Web/store boundary')


@pytest.mark.parametrize('prompt', ['別の公開情報源も調べてください。', 'Consulta también las otras fuentes públicas.'])
@pytest.mark.parametrize('same_source', [False, True])
def test_readiness_repair_budget_tracks_observed_source(tmp_path, monkeypatch, prompt, same_source):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass

    runtime = setup_runtime(tmp_path, monkeypatch)
    for key, value in {'NULLION_ENV_FILE': str(tmp_path/'none.env'), 'NULLION_MEMORY_ENABLED': '0',
        'NULLION_TASK_PLANNER_FEED_MODE': 'off', 'NULLION_CHAT_STARTUP_WARMUP': '0'}.items():
        monkeypatch.setenv(key, value)
    urls = ['https://one.example/read', 'https://two.example/read', 'https://healthy.example/read']
    current = {}; calls = []; registry = ToolRegistry()
    def navigate(inv):
        current['url'] = inv.arguments['url']; calls.append(('nav', current['url']))
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'result_kind': 'browser_navigation', 'page_url': current['url'], 'session_id': 'one-session'})
    def read(inv):
        calls.append(('read', current['url']))
        if current['url'] != urls[2]:
            return ToolResult(inv.invocation_id, inv.tool_name, 'failed',
                {'reason': 'source_not_ready', 'failure_reason': 'source_not_ready', 'session_id': 'one-session',
                 'recovery': {'kind': 'correct_arguments', 'retry_tool_name': inv.tool_name,
                    'required_tool_names': ['browser_snapshot'], 'argument_constraints': {'supported_parameters': ['ready_text']}}},
                '選択した見出しはまだ表示されていません。')
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'session_id': 'one-session', 'page_url': urls[2], 'text': 'Verified value: 37',
             'record': {'id': 'value', 'text': 'Verified value: 37'},
             'source_record_collections': [{'path': ['record'], 'identity_fields': ['id'], 'cardinality': 'one'}]})
    for name, handler in [('browser_navigate', navigate), ('browser_extract_text', read)]:
        registry.register(ToolSpec(name, 'Requested public source', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2, observation_setup_tools=('browser_navigate',) if name.endswith('text') else ()), handler)
    class Model:
        phase = 0
        def create(self, **kwargs):
            reply = workflow_provider_reply(kwargs, completion=True)
            if reply is not None:
                return reply
            names = {row['name'] for row in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            if not names:
                return text_response('{}')
            self.phase += 1
            if self.phase == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['browser_navigate', 'browser_extract_text']})
            steps = [('browser_navigate', {'url': urls[0]}), ('browser_extract_text', {'ready_text': '観測'}),
                     ('browser_navigate', {'url': urls[0] if same_source else urls[1]}),
                     ('browser_extract_text', {'ready_text': 'Resultado'}),
                     ('browser_navigate', {'url': urls[2]}), ('browser_extract_text', {})]
            if 2 <= self.phase <= 7:
                name, args = steps[self.phase-2]
                return tool_response(name, args)
            return text_response('37 — '+urls[2])
    model = Model(); runtime.model_client = model
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
        cid = 'web:readiness-budget'
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
        assert response.status_code == 200, response.text
        turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
        if same_source:
            assert len([r for r in calls if r[0] == 'read']) == 2, calls
            assert not turn['response_fulfilled'], turn
        else:
            assert '37' in response.json()['text'], response.json()
            assert [r['status'] for r in turn['tool_results'] if r['tool_name'] == 'browser_extract_text'] == ['failed', 'failed', 'completed']
            history = client.get('/api/chat/history/'+cid+'?sync_runtime=1').json()['messages']
            assert any(r['role'] == 'bot' and '37' in r['text'] for r in history)
