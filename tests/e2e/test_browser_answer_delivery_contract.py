"""Real browser -> runtime/SQLite -> Telegram presentation, no result-verification gate."""
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='requires container E2E')


@pytest.mark.parametrize("surface", ["telegram", "web"])
@pytest.mark.parametrize("web_action,mode", [("live_research", "page"), ("open_url", "page"), ("live_research", "weather"), ("live_research", "quote"), ("live_research", "failure"), ("live_research", "broad"), ("live_research", "wait"), ("live_research", "fresh"), ("live_research", "items"), ("live_research", "items_dense"), ("live_research", "interaction_failure"), ("live_research", "timed_out_check")])
def test_browser_text_reaches_chat_without_revalidation(tmp_path, monkeypatch, record_property, surface, web_action, mode):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            articles = ''.join(f'<article><h2><a href="/bulletin/{n}">Service item {n:02d}</a></h2><p>Route {n} resumes at 10:30.</p></article>' for n in range(1, 21))
            if mode == 'items_dense':
                articles = ''.join(f'<article><h2><a href="/bulletin/{n}?token={"x" * 700}">Service item {n:02d}</a></h2><p>Route {n} resumes at 10:30.</p></article>' for n in range(1, 21))
            self.wfile.write(('<html><body><h1>Service bulletin</h1><p>Route 47 resumes at 10:30.</p>' + '<button>Search</button><main>' + articles + '</main></body></html>').encode())
        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://bulletin.localhost:{server.server_port}/'
    if mode == 'failure':
        with socket.socket() as closed_port:
            closed_port.bind(('127.0.0.1', 0))
            url = f'http://bulletin.localhost:{closed_port.getsockname()[1]}/'
    original_dns = socket.getaddrinfo
    monkeypatch.setattr(socket, 'getaddrinfo', lambda host, *args, **kw: original_dns('127.0.0.1' if host == 'bulletin.localhost' else host, *args, **kw))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    if mode == 'budget':
        monkeypatch.setenv('NULLION_AGENT_TURN_MAX_ITERATIONS', '2')
        monkeypatch.setenv('NULLION_BROWSER_AGENT_TURN_MAX_ITERATIONS', '2')
    elapsed = []
    provider_samples = []
    backends = []
    try:
        for i, (prompt, reply) in enumerate([
            ('Read the service bulletin.', 'Route 47 resumes at 10:30.'),
            ('Lee el boletín de servicio.', 'La ruta 47 vuelve a funcionar a las 10:30.'),
            ('運行情報を読んでください。', '47番路線は10:30に再開します。'),
        ]):
            data_tool = {'weather': 'weather_forecast', 'quote': 'market_quote', 'broad': 'weather_forecast'}.get(mode)
            calls = [('browser_navigate', {'url': url}), ('browser_extract_text', {})]
            if mode in {'items', 'items_dense'}:
                calls = [('browser_navigate', {'url': url}), ('browser_extract_items', {'selector': 'main', 'max_items': 20})]
                reply = '\n'.join(f'{n}. Service item {n:02d}' for n in range(1, 21))
            if mode == 'wait':
                calls.insert(1, ('browser_wait_for', {'text': 'Route 47 resumes', 'timeout': 2}))
            if data_tool:
                calls.insert(0, (data_tool, {'location': 'Harbor City'} if data_tool == 'weather_forecast' else {'symbols': ['TEST']}))
                reply = ('18°C. ' if data_tool == 'weather_forecast' else 'TEST: 18 USD. ') + reply
            if mode == 'failure':
                calls = [('browser_navigate', {'url': url})]
                reply = ['The page could not be opened: connection refused.', 'No se pudo abrir la página: conexión rechazada.', 'ページを開けませんでした。接続が拒否されました。'][i]
            if mode in {'interaction_failure', 'timed_out_check'}:
                calls.insert(1, ('browser_click_element', {'text': 'Search'}))
                calls.insert(2, ('browser_extract_detail', {}))
                if mode == 'timed_out_check':
                    calls.insert(3, ('browser_assert_page_state', {'required': ['Ready']}))
                reply = ['The page interaction failed; I could not confirm the result.',
                         'La interacción con la página falló; no pude confirmar el resultado.',
                         'ページの操作に失敗し、結果を確認できませんでした。'][i]
            requested_names = [name for name, _ in calls]
            required_names = [data_tool] if data_tool else ['browser_navigate'] if mode == 'failure' else ['browser_extract_items'] if mode in {'items', 'items_dense'} else ['browser_extract_text']
            if mode in {'interaction_failure', 'timed_out_check'}:
                required_names = ['browser_extract_detail']
            if mode == 'broad':
                calls.insert(0, ('request_tool_scope', {'tool_names': ['weather_forecast'], 'required_tool_names': ['weather_forecast']}))
            if mode == 'fresh':
                calls.insert(0, ('request_tool_scope', {'capabilities': ['web'], 'tool_names': requested_names, 'required_tool_names': ['browser_extract_text']}))
            expected_model_calls = len(calls) + 1

            class Model:
                calls = 0
                def create(self, **kwargs):
                    if 'use_direct' in str(kwargs.get('system', '')):
                        raise TimeoutError('External argument planner unavailable')
                    names = {t['name'] for t in kwargs.get('tools', [])}
                    if 'select_attachment_format' in names:
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'format', 'name': 'select_attachment_format', 'input': {'extension': None, 'confidence': 1}}]}
                    if not names:
                        decision = {'web_action': web_action, 'scheduler_action': 'none', 'skill_pack_action': 'none',
                            'requested_tool_names': requested_names + (['browser_extract_text'] if mode == 'failure' else ['browser_run_js', 'browser_wait_for', 'browser_extract_detail', 'browser_extract_items', 'browser_snapshot', 'browser_assert_page_state'] if mode == 'broad' else []), 'required_tool_names': required_names,
                            'confidence': 1}
                        return {'content': [{'type': 'text', 'text': json.dumps(decision)}]}
                    self.calls += 1
                    if self.calls > expected_model_calls:
                        # Simulate the slow verification dependency that must never re-enter.
                        time.sleep(2)
                        raise AssertionError('Unexpected browser verification/model retry')
                    if self.calls == expected_model_calls:
                        evidence = json.dumps(kwargs['messages'])
                        if mode in {'items', 'items_dense'}:
                            for n in range(1, 21):
                                assert f'Service item {n:02d}' in evidence, 'Browser collection was sampled before reaching model'
                            if mode == 'items_dense':
                                payloads = [part['text'] for message in kwargs['messages'] for block in message.get('content', [])
                                            if isinstance(block, dict) and block.get('type') == 'tool_result'
                                            for part in block.get('content', []) if isinstance(part, dict) and '"item_count"' in str(part.get('text', ''))]
                                assert len(payloads) == 1
                                # Model latency grows with input. Simulate that
                                # external cost; complete record delivery stays real.
                                provider_start = time.monotonic()
                                time.sleep(len(payloads[0]) / 60_000)
                                provider_samples.append(time.monotonic() - provider_start)

                        else:
                            assert ('ERR_CONNECTION_REFUSED' if mode == 'failure' else 'Route 47 resumes at 10:30') in evidence, evidence
                        return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': reply}]}
                    name, args = calls[self.calls - 1]
                    return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': f'call-{self.calls}', 'name': name, 'input': args}]}

            model = Model()
            runtime = bootstrap_persistent_runtime(tmp_path / f'{i}.db')
            registry = ToolRegistry()
            register_browser_tools(registry)
            cancelled = threading.Event()
            if mode == 'timed_out_check':
                import asyncio
                from dataclasses import replace
                backend = registry._handlers['browser_assert_page_state'].__self__._backend
                original_run_js = backend.run_js
                pending_delay = [True]
                async def delayed_network_read(session, script):
                    if pending_delay[0] and json.dumps('Ready') in script:
                        pending_delay[0] = False
                        try:
                            await asyncio.sleep(10)
                        finally:
                            cancelled.set()
                    return await original_run_js(session, script)
                # The delay is at the external browser boundary; runtime, locks,
                # timeout cancellation, routing and persistence remain real.
                backend.run_js = delayed_network_read
                spec = replace(registry.get_spec('browser_assert_page_state'), timeout_seconds=0.15)
                registry._specs['browser_assert_page_state'] = spec
            backends.append(registry._handlers["browser_navigate"].__self__._backend)
            if data_tool:
                payload = ({'location': {'name': 'Harbor City'}, 'current': {'temperature_2m': 18}, 'units': {'temperature': 'celsius'}, 'daily': []}
                    if data_tool == 'weather_forecast' else {'quotes': [{'symbol': 'TEST', 'price': 18, 'currency': 'USD'}]})
                registry.register(ToolSpec(name=data_tool, description=data_tool, risk_level=ToolRiskLevel.LOW,
                    side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=5),
                    lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', payload))
            runtime.active_tool_registry = registry
            chat_id = str(90200 + i)
            settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuvwxyz', operator_chat_id=chat_id, chat_enabled=True))
            conversation_id = f'telegram:{chat_id}' if surface == 'telegram' else f'web:member-{i}'
            if mode == 'fresh':
                registry.register(ToolSpec(name='weather_forecast', description='Fetch weather', risk_level=ToolRiskLevel.LOW,
                    side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=5,
                    capability_tags=('weather',)),
                    lambda _: pytest.fail('An unrelated request must not execute the old weather tool'))
                for index in range(5):
                    runtime.store.add_conversation_event({'conversation_id': conversation_id,
                        'event_type': 'conversation.chat_turn', 'turn_id': f'old-weather-{index}',
                        'user_message': 'Forecast for Harbor City', 'assistant_reply': '18°C and clear.',
                        'tool_results': [{'tool_name': 'weather_forecast', 'status': 'completed',
                            'output': {'location': {'name': 'Harbor City'}, 'current': {'temperature_2m': 18}}}]})
            request_text = prompt if mode == 'fresh' else f'{prompt} {url}'
            start = time.monotonic()
            if surface == 'telegram':
                result = handle_chat_operator_message(runtime, request_text, chat_id=chat_id, settings=settings,
                    model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
                    append_activity_trace=False, allow_mini_agents=False)
            else:
                from fastapi.testclient import TestClient
                from nullion import web_app
                monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *args, **kwargs: None)
                monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *args, **kwargs: None)
                app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
                with TestClient(app) as client:
                    response = client.post('/api/chat', json={'text': request_text, 'conversation_id': conversation_id})
                    assert response.status_code == 200, response.text
                    result = response.json()['text']
                    if mode == 'budget':
                        approvals = runtime.store.list_approval_requests()
                        assert len(approvals) == 1
                        approval_id = approvals[0].approval_id
                        saved = runtime.store.get_suspended_turn(approval_id)
                        assert 'Route 47 resumes at 10:30' in json.dumps(saved.messages_snapshot)
                        assert [b['name'] for m in saved.messages_snapshot for b in m.get('content', []) if isinstance(b, dict) and b.get('type') == 'tool_use'] == requested_names
                        resumed = client.post(f'/api/approve/{approval_id}', json={'mode': 'limit_2x'})
                        assert resumed.status_code == 200
                        result = resumed.json()['resumed_text']
                        assert runtime.store.get_suspended_turn(approval_id) is None
            elapsed.append(time.monotonic() - start)
            delivered = prepare_reply_for_platform_delivery(result, principal_id='telegram_chat')
            assert delivered.text == reply
            assert model.calls == expected_model_calls
            turns = [e for e in runtime.store.list_conversation_events(conversation_id) if e.get('event_type') == 'conversation.chat_turn']
            if mode != 'budget':
                if mode == 'timed_out_check':
                    assert cancelled.wait(0.5)
                    assert any(r['status'] == 'failed' and r['output'].get('reason') == 'handler_timeout'
                               for r in turns[-1]['tool_results'])
                assert turns[-1]['assistant_reply'] == reply
                assert [r['tool_name'] for r in turns[-1]['tool_results'] if r['tool_name'] != 'request_tool_scope'] == requested_names
        record_property('elapsed_seconds', json.dumps(elapsed))
        if mode == 'items_dense':
            record_property('provider_seconds', json.dumps(provider_samples))
            assert len(provider_samples) == 3
            assert sum(provider_samples) / 3 < 0.5, provider_samples
            assert max(provider_samples) < 0.55, provider_samples
        assert sum(elapsed) / len(elapsed) < 5, elapsed
        assert max(elapsed) < 8, elapsed
    finally:
        from nullion.plugins.browser_plugin.browser_tools import _run
        for backend in backends:
            _run(backend.shutdown())
        server.shutdown()
        server.server_close()


def test_browser_budget_resume_keeps_completed_results(tmp_path, monkeypatch, record_property):
    test_browser_text_reaches_chat_without_revalidation(tmp_path, monkeypatch, record_property, 'web', 'live_research', 'budget')


@pytest.mark.parametrize('surface', ['telegram', 'web'])
def test_failed_browser_action_is_terminal_and_reports_cause(tmp_path, monkeypatch, surface):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.runtime import load_runtime_store

    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    registry = ToolRegistry()
    # External browser adapter failure; all runtime orchestration, scope,
    # required-action validation, delivery and SQLite state remain real.
    registry.register(ToolSpec(name='browser_click_element', description='Operate page control',
        risk_level=ToolRiskLevel.LOW, side_effect_class=ToolSideEffectClass.READ,
        requires_approval=False, timeout_seconds=1, capability_tags=('web',)),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
            {'reason': 'browser_action_failed'}, error='private provider payload'))
    class Model:
        calls = 0
        def create(self, **kwargs):
            names = {t['name'] for t in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'format',
                    'name': 'select_attachment_format', 'input': {'extension': None, 'confidence': 1}}]}
            self.calls += 1
            assert self.calls < 12, 'A failed browser action must reach a terminal result'
            history = json.dumps(kwargs.get('messages', []))
            if 'request_tool_scope' in names and '"tool_result"' not in history:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'scope',
                    'name': 'request_tool_scope', 'input': {'capabilities': ['web'],
                        'tool_names': ['browser_click_element'], 'required_tool_names': ['browser_click_element']}}]}
            if 'browser_action_failed' not in history:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'click',
                    'name': 'browser_click_element', 'input': {'text': 'Continue'}}]}
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'Done.'}]}
    model = Model()
    runtime = bootstrap_persistent_runtime(tmp_path / 'terminal.db')
    runtime.active_tool_registry = registry
    conversation = 'telegram:member' if surface == 'telegram' else 'web:member'
    settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuvwxyz',
        operator_chat_id='member', chat_enabled=True))
    if surface == 'telegram':
        reply = handle_chat_operator_message(runtime, '続けてください', chat_id='member', settings=settings,
            model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
            allow_mini_agents=False, append_activity_trace=False)
    else:
        from fastapi.testclient import TestClient
        from nullion import web_app
        app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
        with TestClient(app) as client:
            response = client.post('/api/chat', json={'text': '続けてください', 'conversation_id': conversation})
            assert response.status_code == 200, response.text
            reply = response.json()['text']
    assert 'page interaction failed' in reply, reply
    assert 'still open' not in reply and 'Done.' not in reply and 'private provider' not in reply
    runtime.checkpoint(force=True)
    persisted = load_runtime_store(runtime.checkpoint_path)
    turns = [e for e in persisted.list_conversation_events(conversation) if e.get('event_type') == 'conversation.chat_turn']
    assert turns[-1]['assistant_reply'] == reply
    assert any(r['tool_name'] == 'browser_click_element' and r['status'] == 'failed' for r in turns[-1]['tool_results'])
    timings = [e for e in persisted.list_conversation_events(conversation) if e.get('event_type') == 'conversation.latency_timing']
    if surface == 'telegram':
        assert timings[-1]['outcome'] != 'completed', timings
    assert not persisted.suspended_turns and not persisted.mini_agent_runs
