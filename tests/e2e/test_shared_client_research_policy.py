"""Same model decisions through Web UI and Telegram ingress/HTTP transport.

Telegram here is automated ingress/transport coverage, not a real user account
or a substitute for the final stage Telegram app replay.
"""
import asyncio
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import sys
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('mode', ['adaptive_first', 'direct_batch'])
def test_read_policy_is_enforced_even_when_primary_selects_adaptive_browser_first(
        tmp_path, monkeypatch, source_summary_model_reply, surface, mode):
    from playwright.sync_api import sync_playwright, expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry, register_search_plugin
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_web_surface_contracts import _free_port, _UvicornThread
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    # This replay measures an already-authorized public read. Keep the real
    # permission boundary active and persist consent exactly as settings do.
    from nullion.preferences import save_preferences
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, GLOBAL_PERMISSION_PRINCIPAL, PolicyMode
    save_preferences({'outbound_request_mode': 'allow_all'})
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        'fixture-public-read', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    seen_cookies = []
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            seen_cookies.append(self.headers.get('Cookie', ''))
            self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.end_headers()
            self.wfile.write(b'<main><h1>Current options</h1><article><a href="/item">Alpha option</a><p>Price: $17</p></article></main>')
        def log_message(self, *args): pass
    site = ThreadingHTTPServer(('127.0.0.1', 0), Site)
    threading.Thread(target=site.serve_forever, daemon=True).start()
    base = f'http://localhost:{site.server_port}'
    port = _free_port()
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'cdp')
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL', f'http://127.0.0.1:{port}')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    # Exercise the installed-worker registration path. This worker must never
    # be launched for the readable fixture; the native browser remains real.
    monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON', os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON', sys.executable))
    monkeypatch.setenv('NULLION_CHAT_STREAMING_ENABLED', 'false')
    class Model:
        phase = 0
        inner_calls = 0
        errors = []
        def create(self, **kwargs):
            try:
                return self.respond(**kwargs)
            except Exception:
                import traceback
                self.errors.append(traceback.format_exc())
                print(self.errors[-1])
                raise
        def respond(self, **kwargs):
            if any(tool['name'] == 'browser_decision' for tool in kwargs.get('tools', [])):
                self.inner_calls += 1
                time.sleep(2)
                raise AssertionError('Readable page must not start adaptive browser reasoning')
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            summary = source_summary_model_reply(kwargs)
            if summary is not None: return summary
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if mode == 'direct_batch' and self.phase == 1:
                assert {'web_search','browser_read_pages'} <= {tool['name'] for tool in kwargs['tools']}
                return call('web_search',{'queries':['現在の選択肢','otras opciones'], 'independent_queries':True})
            if mode == 'direct_batch' and self.phase == 2:
                return call('browser_read_pages',{'independent_pages':True,'pages':[
                    {'url':base+'/first','read_kind':'text'},{'url':base+'/second','read_kind':'text'}]})
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['web'],
                    'tool_names':['browser_run_task'], 'required_tool_names':['browser_run_task']})
            if self.phase == 2:
                return call('browser_run_task', {'task':'現在の値とリンクを確認', 'url':base, 'max_steps':10})
            assert self.phase == 3, f'Unexpected extra model decision: {self.phase}'
            evidence = json.dumps(kwargs['messages'])
            assert 'Alpha option' in evidence and '$17' in evidence and base+'/item' in evidence, evidence
            return text(f'**Current option**\n- Alpha: $17. [Open listing]({base}/item)')
    model = Model()
    registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda:model)
    searching = peak = 0
    search_lock = threading.Lock()
    def public_search(query, limit):
        nonlocal searching, peak
        with search_lock:searching += 1;peak = max(peak,searching)
        try:
            time.sleep(.12)
            return [{'title':'Current option','url':base+'/first','snippet':'Alpha option, price $17'}]
        finally:
            with search_lock:searching -= 1
    register_search_plugin(registry,web_searcher=public_search)
    monkeypatch.setenv('NULLION_PROVIDER_BINDINGS', 'search_plugin=brave_search_provider')
    monkeypatch.setenv('NULLION_BRAVE_SEARCH_API_KEY', 'local-fixture-not-a-real-key')
    monkeypatch.setattr('nullion.providers.resolve_plugin_provider_kwargs',
        lambda **kwargs: {'web_searcher': public_search})
    assert registry.get_spec('browser_run_task') is not None
    runtime.active_tool_registry = registry
    samples = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch_persistent_context(str(tmp_path/'profile'), headless=True,
                args=[f'--remote-debugging-port={port}'])
            browser.add_cookies([{'name':'login','value':'retained','url':base}])
            try:
                if surface == 'web':
                    with _UvicornThread(create_app(runtime, AgentOrchestrator(model_client=model), registry), _free_port()) as server:
                        page = browser.new_page(); page.goto(server.base_url)
                        for i, prompt in enumerate(['現在の値を確認', 'Verifica el valor actual', 'تحقق من القيمة الحالية']):
                            model.phase = 0; started = time.monotonic()
                            page.locator('#user-input').fill(prompt); page.locator('#send-btn').click()
                            answer = page.locator('#messages .msg.bot .bubble').filter(has_text='Alpha: $17')
                            expect(answer).to_have_count(i+1, timeout=5000)
                            expect(answer.last.locator('a[href="'+base+'/item"]')).to_have_count(1)
                            page.wait_for_function('() => _activeSendTurnIds.size === 0', timeout=2000)
                            samples.append(time.monotonic()-started)
                        page.close()
                else:
                    from telegram import Bot, Chat, Message, Update, User
                    from nullion.config import load_settings
                    from nullion.telegram_app import ChatOperatorService
                    from nullion.telegram_transport import build_telegram_bot
                    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
                    wire = TelegramWire(); original = wire.handle
                    async def handle(request):
                        if request.url.path.endswith('/sendChatAction'):
                            return httpx.Response(200, json={'ok':True, 'result':True})
                        return await original(request)
                    wire.handle = handle; install_wire(monkeypatch, wire)
                    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
                        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
                    service = ChatOperatorService(runtime=runtime, bot_token=TOKEN, operator_chat_id='77',
                        settings=settings, model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
                    async def replay():
                        async with build_telegram_bot(Bot, TOKEN) as bot:
                            for i, prompt in enumerate(['現在の値を確認','Verifica el valor actual','تحقق من القيمة الحالية']):
                                model.phase = 0; started = time.monotonic()
                                message = Message(message_id=i+1,date=datetime.now(UTC),chat=Chat(77,'private'),
                                    from_user=User(77,'Fixture',False),text=prompt)
                                message.set_bot(bot)
                                await service.on_message(Update(update_id=i+1,message=message),SimpleNamespace(bot=bot))
                                samples.append(time.monotonic()-started)
                    from concurrent.futures import ThreadPoolExecutor
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        executor.submit(lambda:asyncio.run(replay())).result(timeout=20)
                    answers = [value for value in wire.texts if 'Alpha: $17' in value]
                    assert len(answers) == 3 and all(base+'/item' in value for value in answers), (wire.texts, model.errors)
                turns = [event for event in runtime.store.list_conversation_events()
                         if event.get('event_type') == 'conversation.chat_turn']
                assert len(turns) == 3
                for turn in turns:
                    assert 'Alpha: $17' in turn['assistant_reply'] and base+'/item' in turn['assistant_reply']
                    results = [r for r in turn['tool_results'] if r['tool_name']=='browser_run_task']
                    if mode == 'adaptive_first':
                        assert len(results) == 1 and results[0]['status']=='completed'
                        strategy = results[0]['output']['strategy']
                        assert strategy['kind'] == 'native_read_preflight'
                        assert strategy['adaptive_browser_started'] is False
                        assert strategy['continuation']['tool_name'] == 'browser_run_task'
                        assert strategy['continuation']['execution_kind'] == 'adaptive_interaction'
                        assert results[0]['output']['documents'][0]['url'] == base+'/'
                        assert results[0]['response_requirements']['_native_completed_reads']['read_only'] is True
                        assert 'action_receipt' not in results[0]['output']
                    else:
                        assert not results and not any(r['tool_name']=='request_tool_scope' for r in turn['tool_results'])
                        search = next(r for r in turn['tool_results'] if r['tool_name']=='web_search')
                        read = next(r for r in turn['tool_results'] if r['tool_name']=='browser_read_pages')
                        assert search['output']['completed_queries']==2 and read['output']['completed_pages']==2
                        assert peak==2
                assert model.inner_calls == 0
                assert seen_cookies and all('login=retained' in cookie for cookie in seen_cookies)
                assert sum(samples)/len(samples) < 3 and max(samples) < 5, samples
            finally:
                registry.run_cleanup_hooks()
                browser.close()
    finally:
        site.shutdown(); site.server_close()
