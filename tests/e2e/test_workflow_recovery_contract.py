"""Real graph + SQLite: recovery is shared by tool families and preserves consent.

Only external provider/source behavior is faked. The controller, tool executor,
continuation receipts, persistence and outcome classification remain real.
"""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
import statistics
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container product-boundary E2E')


@pytest.mark.parametrize('prompt', ['Finish the requested check.', '要求された確認を完了してください。', 'Termina la comprobación solicitada.'])
@pytest.mark.parametrize('family', ['browser', 'connector', 'artifact'])
def test_real_runner_recovers_same_call_and_persists_evidence(tmp_path, monkeypatch, prompt, family):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.execution_outcome import build_execution_outcome, ExecutionStatus
    from nullion.workflow_recovery import WorkflowCheckpointStore

    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    reg = ToolRegistry()
    calls = []
    name = family + '_probe'
    def source(inv):
        calls.append(inv)
        if len(calls) == 1:
            return ToolResult(inv.invocation_id, name, 'failed', {'reason': 'handler_timeout', 'retryable': True}, 'Remote source timed out')
        return ToolResult(inv.invocation_id, name, 'completed', {'value': 42})
    reg.register(ToolSpec(name, 'Observe remote state', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), source)
    class Provider:
        count = 0
        checkpoint_seen = False
        def create(self, **kw):
            from tests.workflow_provider_protocol import workflow_provider_reply
            reply = workflow_provider_reply(kw)
            if reply is not None: return reply
            self.count += 1
            if self.count == 2:
                self.checkpoint_seen = 'workflow_recovery' in json.dumps(kw['messages'])
            if self.count <= 2:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': str(self.count), 'name': name, 'input': {'resource': 1}}]}
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'Verified value: 42.'}]}
    model = Provider()
    result = AgentOrchestrator(model_client=model).run_turn(conversation_id='web:shared-recovery',
        principal_id='web:shared-recovery', user_message=prompt, conversation_history=[], tool_registry=reg,
        policy_store=runtime.store, approval_store=runtime.store, max_iterations=4)
    assert model.count == 3 and model.checkpoint_seen
    assert result.final_text == 'Verified value: 42.' and len(calls) == 2
    assert result.tool_results[0].recovered_by == (result.tool_results[1].invocation_id,)
    checkpoint = result.workflow_checkpoint
    saved = WorkflowCheckpointStore(tmp_path / 'runtime.db').load(checkpoint['workflow_id'], checkpoint['conversation_id'], checkpoint['goal_digest'])
    assert saved == checkpoint and saved['recovery_rounds'] == 1
    outcome = build_execution_outcome(tool_results=result.tool_results, user_visible_message=result.final_text, delivery_status='sent')
    assert outcome.status == ExecutionStatus.SUCCEEDED and outcome.delivery_succeeded


def test_plain_runner_latency_does_not_enter_recovery_dependencies(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    import nullion.workflow_recovery as recovery
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    calls = []
    class Provider:
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            calls.append(kw)
            return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': 'Hello.'}]}
    # The old slow dependency is simulated; touching it fails the latency bound.
    def forbidden(*args):
        time.sleep(.15)
        raise AssertionError('Plain chat entered the workflow checkpoint store')
    monkeypatch.setattr(recovery, '_checkpoint_store', forbidden)
    runner = AgentOrchestrator(model_client=Provider())
    elapsed = []
    for prompt in ('Hello', 'こんにちは', 'Hola', 'Bonjour', '안녕'):
        started = time.monotonic()
        result = runner.run_turn(conversation_id='web:fast', principal_id='web:fast', user_message=prompt,
            conversation_history=[], tool_registry=ToolRegistry(), policy_store=runtime.store, approval_store=runtime.store)
        elapsed.append(time.monotonic() - started)
        assert result.final_text == 'Hello.' and result.workflow_checkpoint == {}
    assert len(calls) == len(elapsed)
    assert statistics.mean(elapsed) < .1 and max(elapsed) < .15, elapsed


@pytest.mark.parametrize('explicit_navigation', [False, True])
@pytest.mark.parametrize('repair_selector', [False, True])
def test_worker_handoff_reloads_source_and_delivers_one_rendered_web_alert(tmp_path, monkeypatch, explicit_navigation, repair_selector):
    from datetime import datetime, timezone
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import threading
    from playwright.sync_api import sync_playwright, expect
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolInvocation
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.live_tracking import LiveTrackerStore
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion import web_app, chat_store
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    chat_store._store = None
    fresh = []
    region = ['news']
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            body = '<main><section id="' + region[0] + '"><h2>進展</h2><a href="/news/old">Original story</a>'
            if fresh: body += '<a href="/news/new">A newly verified story</a>'
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8'); self.end_headers()
            self.wfile.write((body + '</section></main>').encode())
        def log_message(self, *args): pass
    source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{source.server_port}/'
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    creator, worker = ToolRegistry(), ToolRegistry()
    register_browser_tools(creator); register_browser_tools(worker)
    register_live_tracker_tools(creator)
    store = LiveTrackerStore(tmp_path / 'runtime.db')
    creator.live_tracker_store_path = store.path
    owner = 'web:portable-source'
    context = {'conversation_id': owner, 'browser_session_scope': 'creator-process'}
    nav = creator.invoke(ToolInvocation('prepare-page', 'browser_navigate', owner, {'url': url, **({'session_id': 'creator-seed'} if explicit_navigation else {})}, flow_context=context))
    assert nav.status == 'completed', nav
    step = {'tool_name': 'browser_extract_items', 'arguments': {'selector': '#news', 'section_heading': '進展', 'fields': ['title', 'canonical_url']}}
    result = creator.invoke(ToolInvocation('prepare-read', step['tool_name'], owner, step['arguments'], flow_context=context))
    assert result.status == 'completed' and result.output['item_count'] == 1, result
    navigation = {'tool_name': 'browser_navigate', 'arguments': {'url': url, **({'session_id': 'creator-seed'} if explicit_navigation else {})}}
    probe = [navigation, step] if explicit_navigation else [step]
    prepared = {json.dumps(step, ensure_ascii=False, sort_keys=True): result.output}
    if explicit_navigation:
        prepared[json.dumps(navigation, ensure_ascii=False, sort_keys=True)] = nav.output
    created = creator.invoke(ToolInvocation('create-portable', 'create_live_tracker', owner,
        {'name': 'Portable observation', 'duration_seconds': 90, 'interval_seconds': 5, 'probe': probe,
         'stop_after_match': True,
         'matcher': {'kind': 'structured', 'conditions': [{'step': 1 if explicit_navigation else 0, 'pointer': '/item_count', 'op': 'greater_than', 'value': 1, 'label': 'Published stories'}], 'summary': 'A newly verified story'}},
        flow_context={**context, 'prepared_observations': prepared}))
    assert created.status == 'completed', created
    ident = created.output['id']; job = store.get(ident)
    store.confirm_start_receipt(ident, job['expires_at'])
    try:
        # The Web app's worker has a distinct backend/page cache. It cannot use
        # the creator's tab, even though both adapters share a principal.
        from nullion.agent_orchestrator import AgentOrchestrator
        class RepairProvider:
            calls = 0
            def create(self, **kw):
                protocol_reply = workflow_provider_reply(kw, completion=True)
                if protocol_reply is not None: return protocol_reply
                self.calls += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                assert 'Original story' in json.dumps(packet['inspections'])
                assert packet['original_observation']['arguments']['section_heading'] == '進展'
                assert set(packet['repair_schema']['properties']['updates']['properties']) == {'selector'}
                snapshot = next(row['evidence']['snapshot'] for row in packet['inspections'] if row['tool_name'] == 'browser_snapshot')
                region = next(row for row in snapshot['regions'] if row['tag'] == 'section' and row['headings'] == ['進展'])
                assert region['selector'] == '#updated-source' and region['link_count'] == 2
                return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': json.dumps({
                    'scope_preserved': True, 'updates': {'selector': region['selector']}})}]}
        repair_model = RepairProvider()
        app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=repair_model), registry=worker)
        with _UvicornThread(app, _free_port()) as server, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=['--no-sandbox'])
            try:
                ctx = browser.new_context()
                ctx.add_init_script('localStorage.setItem("nullion_conv_id", ' + json.dumps(owner) + ');')
                page = ctx.new_page(); page.goto(server.base_url)
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline and store.get(ident)['checks'] < 1:
                    page.wait_for_timeout(100)
                first = store.get(ident)
                assert first['checks'] >= 1 and first['consecutive_errors'] == 0, first
                if repair_selector: region[0] = 'updated-source'
                fresh.append(True)
                with store.connection() as db:
                    db.execute("UPDATE live_trackers SET next_check_at='' WHERE id=?", (ident,))
                app.state.live_tracker_engine.wake()
                expect(page.locator('#messages')).to_contain_text('A newly verified story', timeout=20000)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and store.get(ident).get('notification_status') != 'sent':
                    page.wait_for_timeout(100)
                final = store.get(ident)
                assert final['state'] == 'matched' and final['notification_status'] == 'sent', final
                assert final['checks'] >= 2 and final['consecutive_errors'] == 0
                assert repair_model.calls == (1 if repair_selector else 0)
                assert final['probe'][-1]['arguments']['selector'] == ('#updated-source' if repair_selector else '#news')
                assert final['matcher'] == job['matcher']
                from nullion.chat_store import get_chat_store
                delivered = [row for row in get_chat_store().load_messages(owner) if (row.get('metadata') or {}).get('source') == 'live_tracker']
                assert len(delivered) == 1
                page.reload(); expect(page.locator('#messages')).to_contain_text('A newly verified story')
            finally:
                browser.close()
    finally:
        source.shutdown(); source.server_close()
        for registry in (creator, worker):
            registry.run_cleanup_hooks(scope_id=None)


def test_real_cron_routes_persist_rolling_policy_and_expired_run_stops_before_model(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app, crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.cron_delivery import (run_single_agent_cron_turn, cron_agent_prompt,
        CronRunDeliveryCallbacks, run_cron_delivery_workflow, cron_delivery_block_reason,
        manual_cron_silent_delivery_text)
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', tmp_path / 'runtime.db')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    registry = ToolRegistry()
    class Provider:
        def run_turn(self, **kw): raise AssertionError('Expired tasks must not call a model')
    with TestClient(web_app.create_app(runtime, orchestrator=None, registry=registry)) as client:
        def deliver(ident, *, manual):
            return run_cron_delivery_workflow(crons.get_cron(ident), label='Manual' if manual else 'Scheduled',
                callbacks=CronRunDeliveryCallbacks(effective_channel=lambda job: 'web',
                    delivery_target=lambda job, channel: 'web:expiry',
                    run_agent_turn=lambda job, conversation: run_single_agent_cron_turn(job, conversation,
                        label='Scheduled', orchestrator=Provider(), runtime=runtime, tool_registry=registry, settings=SimpleNamespace()),
                    record_event=lambda *a, **kw: None, block_reason=cron_delivery_block_reason,
                    save_web_delivery=lambda job, conversation, text, artifacts, result:
                        client.post('/api/chat/history/web%3Aexpiry/message', json={'role': 'bot', 'text': text}).status_code == 200,
                    send_platform_delivery=lambda *a: False,
                    silent_delivery_text=manual_cron_silent_delivery_text if manual else None))
        policy = {'mode': 'rolling', 'timezone': 'America/New_York', 'lookahead_days': 14}
        response = client.post('/api/crons', json={'name': '今後の上映', 'schedule': '5 * * * *', 'task': '公開された上映を確認', 'execution_window': policy})
        assert response.status_code == 200, response.text
        ident = response.json()['job']['id']
        restored = crons.get_cron(ident)
        assert restored.execution_window == policy
        prompt = cron_agent_prompt(restored, label='Scheduled', run_context={'current_datetime': '2026-10-25T12:00:00Z'})
        assert '"start_date": "2026-10-25"' in prompt and '"end_date": "2026-11-08"' in prompt
        waiting = {'mode': 'fixed', 'timezone': 'UTC', 'starts_at': '2090-01-01T00:00:00Z', 'ends_at': '2090-02-01T00:00:00Z'}
        assert client.patch('/api/crons/' + ident, json={'execution_window': waiting}).status_code == 200
        result = run_single_agent_cron_turn(crons.get_cron(ident), 'web:expiry', label='Scheduled', orchestrator=Provider(), runtime=runtime, tool_registry=registry, settings=SimpleNamespace())
        assert result['reason'] == 'workflow_window_not_started'
        assert result['text'] == '' and result['cron_alert_delivery_decision'] == 'silent'
        assert crons.get_cron(ident).enabled is True
        assert deliver(ident, manual=False)['cron_delivery_status'] == 'silent'
        manual = deliver(ident, manual=True)
        assert manual['cron_delivery_status'] == 'saved'
        history = client.get('/api/chat/history/web%3Aexpiry').json()['messages']
        assert len(history) == 1 and '2090' in history[0]['text']
        assert 'nothing new' not in history[0]['text']
        assert 'tracking window has not started' in history[0]['text']
        fixed = {'mode': 'fixed', 'timezone': 'UTC', 'ends_at': '2090-01-01T00:00:00Z'}
        assert client.patch('/api/crons/' + ident, json={'execution_window': fixed}).status_code == 200
        from datetime import datetime, timezone
        import nullion.workflow_validity as validity
        monkeypatch.setattr(validity, 'current_time', lambda: datetime(2091, 1, 1, tzinfo=timezone.utc))
        result = run_single_agent_cron_turn(crons.get_cron(ident), 'web:expiry', label='Scheduled', orchestrator=Provider(), runtime=runtime, tool_registry=registry, settings=SimpleNamespace())
        assert result['reason'] == 'workflow_window_expired' and 'stopped' in result['text']
        assert crons.get_cron(ident).enabled is False
        expired = deliver(ident, manual=False)
        assert expired['cron_delivery_status'] == 'saved' and 'Scheduled task ended' in expired['text']
        assert client.patch('/api/crons/' + ident, json={'execution_window': {'mode': 'rolling'}}).status_code == 400
        assert crons.get_cron(ident).execution_window == fixed
