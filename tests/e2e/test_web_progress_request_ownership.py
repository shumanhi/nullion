"""Concurrent browser requests retain their own progress and terminal state."""
import os
import json
import threading

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('terminal', ['http_network_failure', 'websocket_approval', 'websocket_reject'])
def test_one_request_terminal_event_preserves_other_request_progress(tmp_path, monkeypatch, terminal):
    from playwright.sync_api import expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'emoji_level':'none', 'markdown_style':'plain'})
    monkeypatch.setenv('NULLION_CODE_MODE', 'off')
    monkeypatch.setenv('NULLION_CHAT_STREAMING_ENABLED', 'false')
    entered, release = threading.Event(), threading.Event()
    registry = ToolRegistry()
    registry.register(ToolSpec('opaque_gate', 'Read the selected public source', ToolRiskLevel.HIGH,
        ToolSideEffectClass.READ, True, 5, capability_tags=('web',),
        input_schema={'type':'object', 'properties':{}, 'additionalProperties':False}),
        lambda invocation: pytest.fail('The unapproved search must not execute'))
    class Model:
        calls = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            if not kwargs.get('tools'):
                try:
                    packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                except (ValueError, KeyError, IndexError, TypeError):
                    packet = None
                if isinstance(packet, dict) and packet.get('allowed_relationships'):
                    return text(json.dumps({'relationship':'separate', 'effect':'continue',
                        'confidence':1, 'target_index':None, 'target_is_explicit':False,
                        'plausible_target_indexes':[]}))
                return text('{}')
            self.calls += 1
            if self.calls == 1:
                entered.set()
                assert release.wait(15), 'The independent request never released its provider call'
                return text('Résultat conservé : 17.')
            visible = {tool['name'] for tool in kwargs['tools']}
            if 'opaque_gate' in visible:
                return call('opaque_gate', {})
            return call('request_tool_scope', {'capabilities':['web'], 'tool_names':['opaque_gate'],
                'required_tool_names':['opaque_gate']})
    model = Model()
    runtime.active_tool_registry = registry
    def configure(page):
        if terminal == 'http_network_failure':
            page.add_init_script("""
                window.WebSocket = class extends WebSocket {
                  constructor() { throw new Error('Fixture: upgrade unavailable'); }
                };
                const originalFetch = window.fetch.bind(window);
                let chatPosts = 0;
                window.fetch = (url, options) => {
                  if (String(url) === '/api/chat' && options?.method === 'POST') {
                    if (++chatPosts === 1) {
                      return new Promise((resolve, reject) => {
                        window.rejectOwnedRequest = () => reject(new TypeError('Fixture network interruption'));
                      });
                    }
                    return originalFetch(url, options).then(response => new Promise(resolve => {
                      window.releaseFinalHttp = () => resolve(response);
                    }));
                  }
                  return originalFetch(url, options);
                };
            """)
    with _UvicornThread(create_app(runtime, AgentOrchestrator(model_client=model), registry), _free_port()) as server:
        pw, browser, page, errors = _open_page(server.base_url, configure_page=configure)
        try:
            if terminal.startswith('websocket_'):
                page.wait_for_function('ws && ws.readyState === WebSocket.OPEN')
            page.locator('#user-input').fill('保存されている結果を確認してください。')
            page.locator('#send-btn').click()
            first = page.locator('#messages .status-bubble').last.get_attribute('data-turn-id')
            if terminal.startswith('websocket_'):
                assert entered.wait(5)
            page.locator('#user-input').fill('Revisa la fuente seleccionada.')
            page.locator('#send-btn').click()
            second = page.locator('#messages .status-bubble').last.get_attribute('data-turn-id')
            assert first != second
            survivor = second if terminal == 'http_network_failure' else first
            retired = first if terminal == 'http_network_failure' else second
            if terminal == 'http_network_failure':
                assert entered.wait(5)
                page.evaluate('window.rejectOwnedRequest()')
                expect(page.locator(f'.bubble[data-turn-id="{retired}"]')).to_contain_text('Fixture network interruption')
            else:
                expect(page.locator(f'.bubble[data-turn-id="{retired}"]')).to_contain_text('Waiting for approval', timeout=5000)
            if terminal == 'websocket_reject':
                page.locator('#messages').get_by_role('button', name='Deny', exact=True).first.click()
                expect(page.locator(f'.bubble[data-turn-id="{retired}"]')).to_contain_text('Denied', timeout=5000)
                approval = runtime.store.list_approval_requests()[-1]
                assert approval.status.value == 'denied'
            assert page.evaluate('(id) => _activeSendTurnIds.has(id)', survivor)
            assert page.evaluate('(id) => _activeTurnTimers.has(id)', survivor)
            assert not page.evaluate('(id) => _activeSendTurnIds.has(id)', retired)
            expect(page.locator(f'.status-bubble[data-turn-id="{survivor}"]')).to_have_count(1)
            page.evaluate('() => refreshDashboard()')
            assert page.evaluate('(id) => _activeSendTurnIds.has(id) && _activeTurnTimers.has(id)', survivor)
            release.set()
            expect(page.locator(f'.bubble[data-turn-id="{survivor}"]')).to_contain_text('Résultat conservé : 17.', timeout=5000)
            expect(page.locator(f'.status-bubble[data-turn-id="{survivor}"]')).to_have_count(0)
            assert not page.evaluate('(id) => _activeSendTurnIds.has(id)', survivor)
            assert not page.evaluate('(id) => _activeTurnTimers.has(id)', survivor)
            turns = runtime.store.list_recent_conversation_events(event_type='conversation.chat_turn', limit=8)
            assert any(row.get('assistant_reply') == 'Résultat conservé : 17.' for row in turns)
            if terminal == 'http_network_failure':
                # The final HTTP response is held at the transport boundary.
                # Only the exact stored terminal reply can retire this turn.
                page.wait_for_function('typeof window.releaseFinalHttp === "function"')
                page.evaluate('window.releaseFinalHttp()')
                expect(page.locator('.bubble').filter(has_text='Résultat conservé : 17.')).to_have_count(1)
            assert not errors
        finally:
            release.set()
            browser.close()
            pw.stop()
