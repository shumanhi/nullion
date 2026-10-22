"""Real chat/SQLite/browser boundaries; only the external language model is scripted."""
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


@pytest.mark.parametrize('prompt,surface', [
    ('Any update on the observer we set?', 'api'),
    ('監視の状況はどうですか？', 'api'),
    ('¿Cómo va el seguimiento que configuramos?', 'api'),
    ('Condition Alpha の状態は？', 'browser'),
])
@pytest.mark.parametrize('observation', ['not_checked', 'incomplete', 'no_match'])
def test_inspection_unknown_observation_never_changes_scheduler(tmp_path, monkeypatch, prompt, surface, observation):
    from fastapi.testclient import TestClient
    from nullion import crons, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_default_tool_registry
    from nullion.web_app import create_app
    from test_memory_and_document_recall_contract import response, tool, ScriptedProvider

    for key, value in {'NULLION_HOME':tmp_path/'home', 'NULLION_ENV_FILE':tmp_path/'home'/'.env',
        'NULLION_WORKSPACE_STORAGE_ROOT':tmp_path/'storage', 'NULLION_TASK_PLANNER_FEED_MODE':'off',
        'NULLION_MEMORY_ENABLED':'0', 'NULLION_CHAT_STARTUP_WARMUP':'0'}.items():
        monkeypatch.setenv(key, str(value))
    monkeypatch.setattr(chat_store, '_store', None)
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', tmp_path/'runtime.db')
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    target = crons.add_cron('Condition Alpha', '*/30 * * * *', 'Observe condition', workspace_id='workspace_admin')
    target = crons.update_cron(target.id, artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'})
    crons.add_cron('Other observer', '0 8 * * *', 'Other condition', workspace_id='workspace_admin')
    if observation != 'not_checked':
        from nullion.tracker_status import start_check, finish_check
        start_check(tmp_path/'runtime.db', target, 'observed-check')
        outcome = ({'execution_outcome':{'execution_succeeded':True}, 'response_fulfilled':True,
            'cron_alert_delivery_decision':'silent', 'cron_alert_delivery_reason':'no_verified_match'}
            if observation=='no_match' else {'execution_outcome':{'execution_succeeded':False},
                'cron_source_verification_required':True, 'cron_source_verification_satisfied':False})
        finish_check(tmp_path/'runtime.db', target, 'observed-check', outcome)
    before = crons.get_cron(target.id).to_dict()
    registry = create_default_tool_registry(workspace_root=tmp_path)
    expected = ('Condition Alpha is enabled and checks every 30 minutes. '
        + ('The latest check completed successfully, with no matching result.' if observation=='no_match'
           else 'I cannot access the live source, so the current observation is unknown.'))
    answer_marker = 'no matching result' if observation=='no_match' else 'current observation is unknown'

    class Model(ScriptedProvider):
        phase = 0
        plain = False
        mutate = False
        ordinary_calls = 0
        def create(self, **kw):
            names = {d['name'] for d in kw.get('tools', [])}
            if not names:
                time.sleep(0.4)
                raise AssertionError('Read-only inspection/plain chat invoked a recovery classifier')
            aux = self.auxiliary(kw)
            if aux is not None: return aux
            self.ordinary_calls += 1
            if self.plain: return response('Hello.')
            self.phase += 1
            if self.mutate:
                if self.phase == 1: return tool('request_tool_scope', {
                    'capabilities':['scheduler_mutate'], 'scheduler_action':'mutate',
                    'tool_names':['list_crons','toggle_cron'], 'required_tool_names':['toggle_cron'],
                    'scheduler_toggle_enabled':False})
                if self.phase == 2: return tool('list_crons', {'query':'Alpha'})
                if self.phase == 3: return tool('toggle_cron', {'id':target.id, 'enabled':False})
                return response('Condition Alpha is paused.')
            if self.phase == 1:
                return tool('request_tool_scope', {'capabilities':['scheduler_read'],
                    'scheduler_action':'inspect', 'tool_names':['list_crons']})
            assert 'toggle_cron' not in names and 'run_cron' not in names
            assert 'terminal_exec' in names
            if self.phase == 2: return tool('list_crons', {'query':'Alpha'})
            if observation != 'not_checked':
                assert ('source_unverified' if observation=='incomplete' else 'no_match') in json.dumps(kw['messages'])
            return response(expected)

    model = Model(); runtime.model_client = model
    app = create_app(runtime, AgentOrchestrator(model_client=model), registry)
    if surface == 'api':
        with TestClient(app) as client:
            start = time.perf_counter()
            result = client.post('/api/chat', json={'text':prompt, 'conversation_id':'web:status'})
            elapsed = time.perf_counter()-start
            assert result.status_code == 200 and answer_marker in result.json()['text'], result.text
            assert elapsed < 3
            model.plain = True
            timings = []
            for i, greeting in enumerate(['Hello', 'Hola', 'こんにちは']):
                before_call = model.ordinary_calls; start = time.perf_counter()
                result = client.post('/api/chat', json={'text':greeting, 'conversation_id':f'web:plain-{i}'})
                timings.append(time.perf_counter()-start)
                assert result.status_code == 200 and result.json()['text'] == 'Hello.'
                assert model.ordinary_calls == before_call+1
            assert sum(timings)/len(timings) < 0.5 and max(timings) < 0.9, timings
    else:
        from test_web_surface_contracts import _UvicornThread, _free_port, _open_page
        from playwright.sync_api import expect
        with _UvicornThread(app, _free_port()) as server:
            playwright, browser, page, errors = _open_page(server.base_url)
            try:
                page.locator('#user-input').fill(prompt)
                page.get_by_role('button', name='Send', exact=True).click()
                expect(page.locator('#messages')).to_contain_text(answer_marker, timeout=20000)
                assert not errors, errors
            finally: browser.close(); playwright.stop()
    assert crons.get_cron(target.id).to_dict() == before
    turns = [e for e in runtime.store.list_conversation_events() if e.get('event_type')=='conversation.chat_turn'
        and e.get('user_message')==prompt]
    assert turns
    assert [r['tool_name'] for r in turns[-1]['tool_results']] == ['request_tool_scope','list_crons']
    runtime.checkpoint(force=True)
    from nullion.runtime_persistence import load_runtime_store
    assert any(e.get('assistant_reply') and answer_marker in e['assistant_reply']
        for e in load_runtime_store(tmp_path/'runtime.db').list_conversation_events())
    # The same runtime still carries out a genuinely requested modification.
    # This protects against implementing the negative case by disabling writes.
    model.plain = False; model.mutate = True; model.phase = 0
    with TestClient(app) as client:
        changed = client.post('/api/chat', json={'text':'Pausa Condition Alpha.', 'conversation_id':'web:explicit-change'})
        assert changed.status_code == 200 and any(word in changed.json()['text'] for word in ('paused','disabled')), changed.text
    assert crons.get_cron(target.id).enabled is False
    assert crons.get_cron(target.id).schedule == before['schedule']
    events = runtime.store.list_conversation_events('web:explicit-change')
    actual = next(e for e in reversed(events) if e.get('event_type')=='conversation.chat_turn')
    assert any(r['tool_name']=='toggle_cron' and r['status']=='completed' for r in actual['tool_results'])
