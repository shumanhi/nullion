"""Explicit stops release persistent browser leases through real chat routes."""
import os
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


@pytest.mark.parametrize('transport', ['http', 'websocket'])
def test_stop_releases_only_own_conversation_tabs(tmp_path, monkeypatch, transport):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolInvocation
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool

    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'missing.env'))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    class ExternalBrowser:
        BACKEND_NAME = 'cdp'
        def __init__(self): self.open = set(); self.closed = []
        async def navigate(self, session_id, url):
            self.open.add(session_id)
            return 'Observed'
        async def close_session(self, session_id):
            self.open.discard(session_id); self.closed.append(session_id)
    backend = ExternalBrowser()
    browser = BrowserTools(backend, BrowserSessionPool(), BrowserPolicy())
    registry = ToolRegistry(); registry.register_cleanup_hook(browser.close_tracked_sessions)
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    app = create_app(runtime, AgentOrchestrator(model_client=None), registry)
    sessions = {}
    for conversation in ('web:one', 'web:two'):
        result = browser.browser_navigate(ToolInvocation('nav-' + conversation, 'browser_navigate', 'operator',
            {'url': 'https://example.com'}, capsule_id='turn-' + conversation,
            flow_context={'browser_session_scope': conversation, 'browser_session_owner': 'conversation:' + conversation}))
        assert result.status == 'completed'
        sessions[conversation] = result.output['session_id']
        registry.run_cleanup_hooks(scope_id='turn-' + conversation)
    assert len(backend.open) == 2
    with TestClient(app) as client:
        if transport == 'http':
            stopped = client.post('/api/chat', json={'text': '/stop', 'conversation_id': 'web:one'})
            assert stopped.status_code == 200
        else:
            with client.websocket_connect('/ws/chat') as socket:
                socket.send_json({'text': '/stop', 'conversation_id': 'web:one'})
                assert socket.receive_json()['type'] == 'session_stopped'
        assert backend.closed == [sessions['web:one']]
        assert backend.open == {sessions['web:two']}
        # A second stop is idempotent, without affecting other conversations.
        assert client.post('/api/chat', json={'text': '/stop', 'conversation_id': 'web:one'}).status_code == 200
        assert backend.closed == [sessions['web:one']]
        assert client.post('/api/chat', json={'text': '/stop', 'conversation_id': 'web:two'}).status_code == 200
        assert not backend.open
