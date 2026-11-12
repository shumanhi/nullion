"""Native browser timeout receipts retain navigation for another source."""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker API/store boundary')


@pytest.mark.parametrize('prompt', ['他の公開ページで調べてください。', 'Busca también en otra página pública.'])
@pytest.mark.parametrize('family', ['browser', 'http'])
def test_native_timeout_does_not_retire_navigation_globally(tmp_path, monkeypatch, prompt, family):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_web_fetch_handler
    runtime = setup_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED', '1' if family == 'http' else '0')
    if family == 'http':
        from datetime import UTC, datetime
        from nullion.policy import BoundaryPolicyRule, BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, PolicyMode
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-public-http',
            principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.ALLOW, selector='*', created_by='fixture', created_at=datetime.now(UTC)))
    for key, value in {'NULLION_ENV_FILE': str(tmp_path/'none.env'), 'NULLION_MEMORY_ENABLED': '0',
        'NULLION_TASK_PLANNER_FEED_MODE': 'off', 'NULLION_CHAT_STARTUP_WARMUP': '0'}.items():
        monkeypatch.setenv(key, value)
    calls = []
    class NetworkBoundary:
        BACKEND_NAME = 'cdp'
        async def navigate(self, session_id, url):
            calls.append(url)
            if url == 'https://slow.example/page':
                raise TimeoutError('時間切れ')
            return 'Opened'
        async def extract_text(self, session_id, selector):
            return 'Verified value: 37'
    browser = BrowserTools(NetworkBoundary(), BrowserSessionPool(), BrowserPolicy())
    registry = ToolRegistry()
    navigation = 'browser_navigate' if family == 'browser' else 'web_fetch'
    def network(url, timeout):
        calls.append(url)
        if url == 'https://slow.example/page':
            raise TimeoutError('Tiempo agotado')
        return {'url': url, 'source_url': url, 'text': 'Verified value: 37'}
    if family == 'http':
        from nullion import tools
        monkeypatch.setattr(tools, '_default_web_fetcher', network)
    for name in (['browser_navigate', 'browser_extract_text'] if family == 'browser' else ['web_fetch']):
        registry.register(ToolSpec(name, 'Read requested public source', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2), getattr(browser, name) if family == 'browser' else _build_web_fetch_handler(network))
    class Model:
        phase = 0
        def create(self, **kw):
            reply = workflow_provider_reply(kw, completion=True)
            if reply is not None:
                return reply
            names = {row['name'] for row in kw.get('tools', [])}
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            if not names:
                return text_response('{}')
            self.phase += 1
            if self.phase == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['browser_navigate', 'browser_extract_text'] if family == 'browser' else ['web_fetch']})
            if self.phase in (2, 3):
                assert navigation in names, sorted(names)
                return tool_response(navigation, {'url': 'https://'+('slow' if self.phase == 2 else 'healthy')+'.example/page'})
            if self.phase == 4 and family == 'browser':
                return tool_response('browser_extract_text', {})
            return text_response('37 — https://healthy.example/page')
    model = Model(); runtime.model_client = model; runtime.active_tool_registry = registry
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
        cid = 'web:native-source-timeout'
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
        assert response.status_code == 200 and '37' in response.json()['text'], response.json()
        turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
        navigations = [row for row in turn['tool_results'] if row['tool_name'] == navigation]
        assert [row['status'] for row in navigations] == ['failed', 'completed'], json.dumps(navigations, ensure_ascii=False)
        assert navigations[0]['output'].get('failure_reason', navigations[0]['output'].get('reason')) == (
            'source_timeout' if family == 'browser' else 'handler_timeout')
        assert navigations[0]['output']['failure_scope'] == 'invocation'
        assert calls == ['https://slow.example/page', 'https://healthy.example/page']
