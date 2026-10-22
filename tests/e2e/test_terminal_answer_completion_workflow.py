"""Real chat, shell, SQLite, Web UI and Telegram delivery; only the model/transport are faked."""
from datetime import UTC, datetime
from html import unescape
import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')

CASES = [
    ('What are some of the privacy concerns connecting Gemini with gmail?',
     '1. You need to review which services can access your email.\n2. Connected apps can process message content.'),
    ('¿Qué riesgos de privacidad tiene conectar un asistente con mi correo?',
     'Revisa los permisos y las políticas de conservación del correo antes de conectar una aplicación.'),
    ('メールとアシスタントを接続するときのプライバシー上の懸念は？',
     '接続するアプリの権限と、メールの内容がどのように処理されるかを確認してください。'),
]


def setup(tmp_path, monkeypatch, probe):
    from nullion import chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_core_tool_registry
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from test_memory_and_document_recall_contract import ScriptedProvider, response, tool
    home = tmp_path/'home'
    for name, value in {'NULLION_HOME':home, 'NULLION_ENV_FILE':home/'.env',
        'NULLION_WORKSPACE_STORAGE_ROOT':home/'workspaces', 'NULLION_MEMORY_ENABLED':'0',
        'NULLION_TASK_PLANNER_FEED_MODE':'off', 'NULLION_ACTIVITY_TRACE_ENABLED':'0',
        'NULLION_TERMINAL_ENABLED':'1', 'NULLION_CHAT_STARTUP_WARMUP':'0'}.items():
        monkeypatch.setenv(name, str(value))
    monkeypatch.setattr(chat_store, '_store', None)
    runtime = bootstrap_persistent_runtime(home/'runtime.db')
    registry = create_core_tool_registry(workspace_root=home)
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-consent',
        principal_id=OPERATOR_PERMISSION_PRINCIPAL, permission='tool:terminal_exec',
        granted_by='fixture', granted_at=datetime.now(UTC)))
    class Model(ScriptedProvider):
        scope_calls = 0
        scope_context = []
        chat_calls = 0
        answer = ''
        pending_probe = False
        def create(self, **kw):
            if 'assistant_no_tool_draft' in str(kw.get('messages', [])) and not kw.get('tools'):
                self.scope_calls += 1

                time.sleep(.8)  # Former slow recovery dependency must stay off the hot path.
                return response(json.dumps({'requested_tool_names':[], 'required_tool_names':[], 'confidence':1}))
            aux = self.auxiliary(kw)
            if aux is not None:
                return aux
            self.chat_calls += 1
            if self.pending_probe:
                self.pending_probe = False
                return tool('terminal_exec', {'command':"printf 'Local terminal capability verified.'", 'network_mode':'none'})
            return response(self.answer)
    model = Model()
    runtime.model_client = model
    orchestrator = AgentOrchestrator(model_client=model)
    def prepare(answer):
        model.answer = answer
        model.pending_probe = probe
    return runtime, registry, orchestrator, model, prepare


def assert_turn(runtime, conversation, answer, probe):
    events = runtime.store.list_conversation_events(conversation)
    turn = next(row for row in reversed(events) if row.get('event_type') == 'conversation.chat_turn')
    assert turn['assistant_reply'] == answer, turn
    results = turn['tool_results']
    assert [row['tool_name'] for row in results] == (['terminal_exec'] if probe else []), results
    if probe:
        assert results[0]['status'] == 'completed'
        assert results[0]['output']['stdout'] == 'Local terminal capability verified.'
    assert not turn.get('artifact_paths')
    assert not runtime.store.list_approval_requests()


@pytest.mark.parametrize('surface', ['api', 'telegram', 'browser'])
@pytest.mark.parametrize('probe', [False, True])
def test_full_answer_survives_shell_and_plain_turns_stay_fast(tmp_path, monkeypatch, record_property, surface, probe):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.telegram_app import _deliver_reply
    runtime, registry, orchestrator, model, prepare = setup(tmp_path, monkeypatch, probe)
    app = create_app(runtime, orchestrator=orchestrator, registry=registry)
    elapsed = []
    def run(index, prompt, answer, client=None, page=None):
        prepare(answer)
        conversation = 'telegram:90200' if surface == 'telegram' else f'web:answer-{index}'
        before = time.perf_counter()
        if surface == 'api':
            response = client.post('/api/chat', json={'text':prompt, 'conversation_id':conversation})
            assert response.status_code == 200, response.text
            assert response.json()['text'] == answer, response.json()
        elif surface == 'telegram':
            settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuvwxyz',
                operator_chat_id='90200', chat_enabled=True))
            reply = handle_chat_operator_message(runtime, prompt, chat_id='90200', settings=settings,
                model_client=model, agent_orchestrator=orchestrator, append_activity_trace=False, allow_mini_agents=False)
            assert reply == answer
            wire = []
            class Message:
                chat = SimpleNamespace(id=90200)
                message_id = index + 1
                async def reply_text(self, text, **kw):
                    wire.append(text)
                    return SimpleNamespace(message_id=index+100)
            asyncio.run(_deliver_reply(Message(), reply, principal_id='telegram:90200'))
            assert wire and unescape(wire[-1]) == answer
        else:
            conversation = page.evaluate("localStorage.getItem('nullion_conversation_id')") or 'web:browser'
            page.locator('#user-input').fill(prompt)
            page.get_by_role('button', name='Send', exact=True).click()
            from playwright.sync_api import expect
            for line in answer.splitlines():
                expect(page.locator('#messages')).to_contain_text(line, timeout=15000)
            conversation = next(row['conversation_id'] for row in reversed(runtime.store.list_conversation_events())
                if row.get('event_type') == 'conversation.chat_turn' and row.get('assistant_reply') == answer)
            assert page.get_by_text('Local terminal capability verified.', exact=True).count() == 0
        elapsed.append(time.perf_counter()-before)
        assert_turn(runtime, conversation, answer, probe)
    if surface == 'browser':
        from test_web_surface_contracts import _UvicornThread, _free_port, _open_page
        with _UvicornThread(app, _free_port()) as server:
            playwright, browser, page, errors = _open_page(server.base_url)
            try:
                for index, (prompt, answer) in enumerate(CASES): run(index, prompt, answer, page=page)
                assert not errors, errors
            finally:
                browser.close(); playwright.stop()
    else:
        with TestClient(app) as client:
            for index, (prompt, answer) in enumerate(CASES): run(index, prompt, answer, client=client)
    assert model.scope_calls == 0, model.scope_context
    record_property("provider_call_count", model.chat_calls)
    assert sum(elapsed)/len(elapsed) < (2 if surface == 'browser' else .6), elapsed
    assert max(elapsed) < (3 if surface == 'browser' else 1.2), elapsed
    record_property('chat_latency_seconds', json.dumps(elapsed))
    runtime.checkpoint(force=True)
    from nullion.runtime_persistence import load_runtime_store
    stored = load_runtime_store(runtime.checkpoint_path)
    assert any(row.get('assistant_reply') == CASES[-1][1] for row in stored.list_conversation_events())
