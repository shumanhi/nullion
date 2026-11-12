"""Actual Web/SQLite boundaries stay responsive and preserve pending saves."""
import os
import statistics
import threading
import time

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container API/SQLite E2E')


def setup(tmp_path,monkeypatch):
    from nullion import chat_store, config, web_app, crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.config import NullionSettings
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');monkeypatch.setattr(chat_store,'_store',None)
    monkeypatch.setattr(crons.CronScheduler,'start',lambda self:None)
    settings=NullionSettings(doctor_enabled=False)
    monkeypatch.setattr(config,'load_settings',lambda *a,**k:settings)
    monkeypatch.setattr(web_app,'load_settings',lambda *a,**k:settings)
    return bootstrap_persistent_runtime(tmp_path/'runtime.db')


def test_no_change_web_actions_skip_slow_history_merge_and_keep_external_writes(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app, runtime_persistence
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.events import make_event
    from nullion.audit import make_audit_record
    runtime=setup(tmp_path,monkeypatch)
    for i in range(2000):
        runtime.store.add_conversation_event({'event_id':str(i),'event_type':'fixture.history',
            'conversation_id':'web:history','created_at':'2028-04-09T10:00:00+00:00','value':'x'*100})
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    class NoModel:
        def create(self, **kw): raise AssertionError('Memory maintenance must not call a model')
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=NoModel()),registry=ToolRegistry())
    with TestClient(app) as client:
        runtime.checkpoint()
        original_signature=runtime.last_checkpoint_file_signature
        external=bootstrap_persistent_runtime(runtime.checkpoint_path)
        calls=[];original=runtime_persistence._load_runtime_store_sqlite_collections
        def slow(*a,**k):calls.append(True);time.sleep(1.2);return original(*a,**k)
        monkeypatch.setattr(runtime_persistence,'_load_runtime_store_sqlite_collections',slow)
        samples=[];events=[]
        for i,(method,path) in enumerate([('DELETE','/api/memory'),('POST','/api/memory/smart-cleanup'),('DELETE','/api/memory')]):
            event=make_event('fixture.external','fixture',{'index':i});events.append(event)
            external.checkpoint_runtime_events((event,))
            started=time.monotonic();response=client.request(method,path);samples.append(time.monotonic()-started)
            assert response.status_code==200,response.text
            assert response.json().get('deleted',response.json().get('removed'))==0
        assert not calls,'A no-change Web action reloaded and rewrote historical state'
        assert statistics.mean(samples)<.3 and max(samples)<.6,samples
        assert runtime.last_checkpoint_file_signature==original_signature
        local=make_audit_record('fixture.local','fixture');runtime.store.add_audit_record(local)
        runtime.checkpoint()
        assert len(calls)==1,'The next actual mutation must still merge external state'
    restored=bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert {e.event_id for e in events}.issubset({e.event_id for e in restored.store.events})
    assert local.record_id in {a.record_id for a in restored.store.audit_records}
    assert len(restored.store.list_conversation_events('web:history'))==2000
    record_property('request_seconds',samples)


def test_web_approval_completion_preserves_queued_forced_save(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.approvals import create_approval_request
    from nullion.runtime import PersistentRuntime, bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    runtime = setup(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_WEB_CHAT_TURN_CHECKPOINT_ENABLED', '1')
    registry = ToolRegistry()
    executions = []

    def complete(invocation):
        executions.append(invocation.arguments['value'])
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
                          {'message': invocation.arguments['value']})

    registry.register(ToolSpec(name='fixture_delivery', description='Complete approved operation',
        risk_level=ToolRiskLevel.HIGH, side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
        requires_approval=True, timeout_seconds=2), complete)
    approvals = []
    for i, value in enumerate(['Complete.', 'Terminado.', '完了。']):
        approval = create_approval_request(requested_by='web:coalesced-' + str(i), action='use_tool',
            resource='fixture_delivery', context={'tool_name': 'fixture_delivery',
                'requires_approval': True, 'tool_arguments': {'value': value},
                'trigger_flow': {'invocation_id': 'fixture-' + str(i)}})
        runtime.store.add_approval_request(approval)
        approvals.append(approval)
    runtime.active_tool_registry = registry
    app = web_app.create_app(runtime, orchestrator=None, registry=registry)
    entered = threading.Event()
    release = threading.Event()
    calls = []
    writer = []
    original = PersistentRuntime.checkpoint

    def blocked(self, *, force=False):
        if self is runtime and (not writer or threading.current_thread() is writer[0]):
            if not writer:
                writer.append(threading.current_thread())
            calls.append(force)
            if len(calls) == 1:
                entered.set()
                assert release.wait(12), 'Test failed to release writer'
        return original(self, force=force)

    with TestClient(app) as client:
        monkeypatch.setattr(PersistentRuntime, 'checkpoint', blocked)
        web_app._schedule_web_checkpoint(runtime)
        assert entered.wait(2)
        samples = []
        try:
            for approval in approvals:
                started = time.monotonic()
                response = client.post('/api/approve/' + approval.approval_id, json={'mode': 'once'})
                samples.append(time.monotonic() - started)
                assert response.status_code == 200, response.text
                payload = response.json()
                assert payload['ok'] and payload['resume']['text'] == approval.context['tool_arguments']['value'], payload
            assert statistics.mean(samples) < .7 and max(samples) < 1.5, samples
        finally:
            release.set()
        deadline = time.monotonic() + 8
        while id(runtime) in web_app._WEB_CHECKPOINT_INFLIGHT:
            assert time.monotonic() < deadline, 'Background writer did not finish'
            time.sleep(.01)
        assert calls == [False, True], calls
    restored = bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert executions == ['Complete.', 'Terminado.', '完了。']
    for approval in approvals:
        assert restored.store.get_approval_request(approval.approval_id).status.value == 'approved'
        rows = restored.store.list_recent_conversation_events(approval.requested_by, event_type='conversation.chat_turn')
        assert rows and rows[-1]['assistant_reply'] == approval.context['tool_arguments']['value']
    record_property('request_seconds', samples)
