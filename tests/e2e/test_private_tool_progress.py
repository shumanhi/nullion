"""Real Web UI/history/SQLite; only external inference delays are scripted."""
from datetime import UTC, datetime
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('prompt', ['Busca en los mensajes guardados.', '保存した会話を調べてください。'])
def test_history_progress_advances_in_one_card_and_avoids_unrelated_history_copy(tmp_path, monkeypatch, prompt):
    from playwright.sync_api import expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.conversation_history_tools import with_conversation_history_tool
    from nullion.preferences import save_preferences
    from nullion.runtime_store import RuntimeStore
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'emoji_level': 'none', 'markdown_style': 'plain'})
    runtime.store.add_conversation_event({
        'event_type': 'conversation.chat_turn', 'workspace_id': 'workspace_admin',
        'conversation_id': 'web:previous', 'turn_id': 'saved', 'created_at': datetime.now(UTC).isoformat(),
        'user_message': 'Cedar document', 'assistant_reply': 'Saved result: Cedar 17.',
    })
    for i in range(1200):
        runtime.store.add_conversation_event({
            'event_type': 'conversation.tool_timing', 'conversation_id': 'web:noise',
            'event_id': f'noise-{i}', 'duration_ms': i, 'payload': {'private_data': 'x' * 2000},
        })
    original_list = RuntimeStore.list_conversation_events
    full_scans = []
    def guarded_list(self, conversation_id=None, *, event_types=None):
        if self is runtime.store and conversation_id is None and event_types is None:
            full_scans.append(True)
            time.sleep(1.3)  # Previously slow unrelated dependency.
        return original_list(self, conversation_id, event_types=event_types)
    monkeypatch.setattr(RuntimeStore, 'list_conversation_events', guarded_list)
    class Model:
        calls = 0
        plain = False
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            if not self.plain and 'chat_history_search' not in {t['name'] for t in kwargs.get('tools', [])}:
                return text('{}')
            self.calls += 1
            if self.plain:
                return text('Prêt.')
            if self.calls == 1:
                return call('chat_history_search', {'query': 'Cedar document', 'limit': 1})
            time.sleep(1.8)
            if self.calls == 2:
                return call('chat_history_search', {'read_turn_id': 'saved', 'max_chars': 500})
            return text('Saved result: Cedar 17.')
    model = Model()
    registry = with_conversation_history_tool(ToolRegistry(), runtime=runtime, conversation_id='web:previous')
    with _UvicornThread(create_app(runtime, AgentOrchestrator(model_client=model), registry), _free_port()) as server:
        pw, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator('#user-input').fill(prompt)
            page.locator('#send-btn').click()
            card = page.locator('#messages .status-bubble')
            expect(card.filter(has_text='1 step completed')).to_have_count(1, timeout=1700)
            expect(card.filter(has_text='2 steps completed')).to_have_count(1, timeout=2500)
            expect(card).to_have_count(1)
            assert card.inner_text().startswith('Reviewing')
            assert 'Cedar' not in card.inner_text() and 'private_data' not in card.inner_text()
            expect(page.locator('#messages .msg.bot .bubble').filter(has_text='Saved result: Cedar 17.')).to_have_count(1, timeout=3000)
            expect(card).to_have_count(0)
            assert not errors
            assert model.calls == 3
            model.plain = True
            before = model.calls
            samples = []
            for i, value in enumerate(('Bonjour', 'Hola', 'こんにちは')):
                started = time.monotonic()
                response = page.request.post(server.base_url + '/api/chat', data={
                    'text': value, 'conversation_id': f'web:plain-history-{i}',
                })
                samples.append(time.monotonic() - started)
                assert response.json()['text'] == 'Prêt.'
            assert model.calls - before == 3
            assert sum(samples) / len(samples) < .75 and max(samples) < 1.25, samples
        finally:
            browser.close()
            pw.stop()
    assert not full_scans
    timings = runtime.store.list_recent_conversation_events(event_type='conversation.tool_timing', limit=10)
    history = [r['duration_ms'] for r in timings if r.get('tool_name') == 'chat_history_search']
    assert len(history) == 2 and max(history) < 750, history
