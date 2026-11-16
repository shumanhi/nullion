"""Real request completion releases its CDP tabs, preserving other tab owners."""
from concurrent.futures import ThreadPoolExecutor
import asyncio
import json
import os
import threading
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('mode', ['off', 'on'])
@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('outcome,prompt', [
    ('completed', 'Read the selected source and report the value.'),
    ('failure', 'Lee la fuente seleccionada y resume el valor.'),
    ('cancelled', '選択した情報源の値を確認してください。'),
])
def test_request_closes_owned_tabs_on_every_terminal_exit(tmp_path, monkeypatch, mode, surface, outcome, prompt, unquoted_keep=False):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from playwright.async_api import Page
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.preferences import save_preferences
    from nullion.tools import ToolInvocation, ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'outbound_request_mode': 'allow_all'})
    port = _free_port()
    for key, value in {'NULLION_CODE_MODE': mode, 'NULLION_BROWSER_BACKEND': 'cdp',
            'NULLION_BROWSER_CDP_URL': f'http://127.0.0.1:{port}',
            'NULLION_BROWSER_BLOCK_PRIVATE': 'false'}.items():
        monkeypatch.setenv(key, value)
    # External source boundary only: use a real Chromium-served data page.
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers()
            self.wfile.write(b'<main>Verified source value: 37</main>')
        def log_message(self, *args): pass
    source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f'http://localhost:{source.server_port}/record'
    observed, release = threading.Event(), threading.Event()
    native_closes = []
    close = Page.close
    async def transient_close(page, *args, **kwargs):
        if page.url == url:
            native_closes.append(page.url)
            if len(native_closes) == 1:
                raise ConnectionError('simulated close acknowledgement failure')
        return await close(page, *args, **kwargs)
    monkeypatch.setattr(Page, 'close', transient_close)
    class Model:
        phase = 0
        def create(self, **kwargs):
            reply = workflow_provider_reply(kwargs, completion=True)
            if reply is not None: return reply
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['browser'],
                    'tool_names': ['browser_open', 'browser_navigate', 'browser_extract_text'],
                    'required_tool_names': ['browser_navigate', 'browser_extract_text']})
            operations = [('browser_open', {'keep_open':True} if unquoted_keep else {}), ('browser_navigate', {'url': url,**({'keep_open':True} if unquoted_keep else {})}), ('browser_extract_text', {})]
            if mode == 'on' and self.phase == 2:
                return call('code_mode', {'tool_names': [name for name, _ in operations],
                    'code': '\n'.join('call_tool('+repr(name)+', '+repr(args)+')' for name, args in operations)})
            if mode == 'off' and self.phase <= 4:
                name, args = operations[self.phase-2]
                return call(name, args)
            assert '37' in json.dumps(kwargs['messages'])
            observed.set()
            if outcome == 'failure': raise RuntimeError('simulated provider failure')
            if outcome == 'cancelled': assert release.wait(8)
            return text('Verified source value: 37. ' + url)
    model = Model()
    registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    sent = []
    def telegram_send(message, update_id):
        class Message:
            text = message
            message_id = update_id
            reply_to_message = None
            chat = SimpleNamespace(id=123, send_action=lambda **kwargs: asyncio.sleep(0))
            async def reply_text(self, value, **kwargs):
                sent.append(value)
                return SimpleNamespace(message_id=1000+len(sent))
        try:
            asyncio.run(service.on_message(SimpleNamespace(update_id=update_id, message=Message()), SimpleNamespace(bot=None)))
        except asyncio.CancelledError:
            assert outcome == 'cancelled' and update_id == 500
            return 'cancelled'
    try:
        with sync_playwright() as pw:
            context = pw.chromium.launch_persistent_context(str(tmp_path/'profile'), headless=True,
                args=[f'--remote-debugging-port={port}'])
            try:
                user_page = context.pages[0]
                user_page.goto('data:text/html,<title>User tab</title>Keep this tab')
                with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
                    if surface == 'telegram':
                        from nullion.config import load_settings
                        from nullion.telegram_app import ChatOperatorService
                        settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN': '123456:abcdefghijklmnopqrstuvwxyz',
                            'NULLION_TELEGRAM_OPERATOR_CHAT_ID': '123', 'NULLION_TELEGRAM_CHAT_ENABLED': 'true'})
                        service = ChatOperatorService(runtime=runtime, settings=settings,
                            bot_token=settings.telegram.bot_token, operator_chat_id='123', model_client=model,
                            agent_orchestrator=AgentOrchestrator(model_client=model))
                    # Another active task must also survive, even for the same principal.
                    tools = next(hook.__self__ for hook in registry._cleanup_hooks
                        if hasattr(hook, '__self__') and hasattr(hook.__self__, '_sessions_by_scope'))
                    other = tools.browser_navigate(ToolInvocation('other', 'browser_navigate', 'operator',
                        {'url': f'http://localhost:{source.server_port}/other'}, capsule_id='other-task'))
                    assert other.status == 'completed'
                    assert user_page.title() == 'User tab'
                    baseline = len(context.pages)
                    started = time.monotonic()
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        pending = (pool.submit(telegram_send, prompt, 500) if surface == 'telegram' else
                            pool.submit(client.post, '/api/chat', json={'text': prompt,
                                'conversation_id': 'web:cleanup', 'turn_id': 'cleanup-turn'}))
                        if outcome == 'cancelled':
                            assert observed.wait(12)
                            if surface == 'telegram':
                                pool.submit(telegram_send, '/stop', 501).result(timeout=8)
                                assert any('Stopped' in value for value in sent), sent
                            else:
                                stop = client.post('/api/chat', json={'text': '/stop', 'conversation_id': 'web:cleanup'})
                                assert stop.status_code == 200
                            release.set()
                        response = pending.result(timeout=20)
                    if surface == 'telegram':
                        assert sent
                        if outcome == 'completed': assert any('37' in value for value in sent), sent
                        if outcome == 'cancelled': assert response == 'cancelled'
                    else:
                        assert response.status_code == 200, response.text
                        if outcome == 'completed': assert '37' in response.json()['text']
                        if outcome == 'cancelled': assert response.json()['superseded'] is True
                    # Observe the actual browser boundary after the API has returned.
                    deadline = time.monotonic() + 2
                    while True:
                        assert user_page.title() == 'User tab'
                        if all(p.url != url for p in context.pages) or time.monotonic() >= deadline:
                            break
                        user_page.wait_for_timeout(20)
                    assert len(context.pages) == baseline, [(p.url, p.is_closed()) for p in context.pages]
                    assert not user_page.is_closed() and 'User tab' == user_page.title()
                    assert all(p.url != url for p in context.pages)
                    assert len(native_closes) == 2, 'Failed close must be retried without losing ownership'
                    assert time.monotonic()-started < 20
                    tools.close_tracked_sessions('other-task')
                    assert user_page.title() == 'User tab'
                    assert len(context.pages) == 1
            finally:
                release.set()
                context.close()
    finally:
        source.shutdown(); source.server_close()


@pytest.mark.parametrize('mode,prompt', [('off', 'Close the selected tab.'), ('on', '選択したタブを閉じて。')])
def test_close_only_request_never_opens_an_unrelated_page(tmp_path, monkeypatch, mode, prompt):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'outbound_request_mode': 'allow_all'})
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'cdp')
    class Model:
        phase = 0
        def create(self, **kwargs):
            reply = workflow_provider_reply(kwargs, completion=True)
            if reply is not None: return reply
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['browser_close'], 'required_tool_names': ['browser_close']})
            if self.phase == 2:
                return call('code_mode', {'tool_names': ['browser_close'],
                    'code': "call_tool('browser_close', {})"}) if mode == 'on' else call('browser_close', {})
            assert self.phase == 3, 'Closing must not invent another navigation round'
            assert 'session_not_open' in json.dumps(kwargs['messages'])
            return text('No selected browser session was open. Other tabs were not changed.')
    model = Model(); registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        result = client.post('/api/chat', json={'text': prompt, 'conversation_id': 'web:close-only'})
        assert result.status_code == 200 and 'No selected' in result.json()['text'], result.text
        results = [r for e in runtime.store.list_conversation_events('web:close-only')
            if e.get('event_type') == 'conversation.chat_turn' for r in e.get('tool_results', [])]
        assert not any(r['tool_name'] in {'browser_open', 'browser_navigate'} for r in results)
        closed = next(r['output'] for r in results if r['tool_name'] == 'browser_close')
        assert closed['closed_count'] == 0 and closed['closed'] is None


def test_resource_cleanup_keeps_plain_chat_on_the_fast_path(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    runtime = isolate(tmp_path, monkeypatch)
    class Model:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return text('37')
    model = Model(); registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    elapsed = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['Reply with 37.', 'Responde 37.', '37と答えて。']):
            started = time.monotonic()
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': f'web:plain-{index}'})
            elapsed.append(time.monotonic()-started)
            assert response.status_code == 200 and response.json()['text'] == '37'
    assert model.calls == 3, 'Resource cleanup must not add model decisions to ordinary turns'
    assert sum(elapsed)/len(elapsed) < 2 and max(elapsed) < 4, elapsed
    record_property('plain_chat_average_seconds', sum(elapsed)/len(elapsed))
    record_property('plain_chat_max_seconds', max(elapsed))


def test_unquoted_model_retention_does_not_escape_real_request_cleanup(tmp_path,monkeypatch):
    test_request_closes_owned_tabs_on_every_terminal_exit(tmp_path,monkeypatch,'on','web','completed',
        'Lee la fuente y resume el valor.',unquoted_keep=True)
