"""Real chat route, scheduler store and core execution discovery; provider is fake."""
from __future__ import annotations
import json
import os
import time
import pytest
pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='container E2E only')

@pytest.mark.parametrize('prompt', ['Are you still tracking my flight? How often?', '¿Sigues vigilando mi vuelo? ¿Cada cuánto?', 'フライトの監視は続いていますか？頻度は？'])
def test_scheduler_question_synthesizes_relevant_answer(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app, crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_default_tool_registry
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path))
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', tmp_path/'runtime.db')
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    crons.add_cron('Condition Alpha', '*/30 * * * *', 'Observe configured condition',
        artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'})
    crons.add_cron('Unrelated email digest', '0 8 * * *', 'Summarize email')
    registry = create_default_tool_registry(workspace_root=tmp_path)
    class Model:
        def __init__(self): self.phase = 0; self.tools = []; self.plain = False
        def create(self, **kw):
            names = {tool['name'] for tool in kw.get('tools', [])}
            self.tools.append(names)
            if self.plain:
                if not names:
                    time.sleep(0.4)  # Previously slow scope dependency must stay off this path.
                    raise AssertionError('Plain chat requested a scope classifier')
                return {'stop_reason':'end_turn','content':[{'type':'text','text':'Hello.'}]}
            if not names:
                return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps({
                    'scheduler_action':'inspect', 'requested_tool_names':['list_crons'], 'confidence':1})}]}
            assert 'terminal_exec' in names
            if 'list_crons' not in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                    'input':{'capabilities':['scheduler_read'],'scheduler_action':'inspect','tool_names':['list_crons']}}]}
            if self.phase == 0:
                self.phase = 1
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':'list_crons','input':{'query':'Alpha'}}]}
            assert 'Condition Alpha' in str(kw['messages'])
            return {'stop_reason':'end_turn','content':[{'type':'text','text':
                'Your Condition Alpha is enabled and checks every 30 minutes. No successful check is recorded yet.'}]}
    model = Model(); runtime.model_client = model
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    with TestClient(app) as client:
        start=time.perf_counter()
        response=client.post('/api/chat', json={'text':prompt, 'conversation_id':'web:fare-status'})
        elapsed=time.perf_counter()-start
        model.plain = True
        latencies = []
        for index, greeting in enumerate(('Hello', 'Hola', 'こんにちは')):
            before = time.perf_counter()
            plain = client.post('/api/chat', json={'text':greeting, 'conversation_id':f'web:plain-{index}'})
            latencies.append(time.perf_counter()-before)
            assert plain.status_code == 200 and 'Hello' in plain.json()['text'], plain.text
            turns = runtime.store.list_conversation_events(f'web:plain-{index}')
            turn = next(event for event in reversed(turns) if event.get('event_type')=='conversation.chat_turn')
            assert not turn.get('tool_results'), turn
        assert sum(latencies)/len(latencies) < 0.4, latencies
        assert max(latencies) < 0.7, latencies
    assert response.status_code == 200, response.text
    payload=response.json()
    assert 'every 30 minutes' in payload['text'], payload
    assert 'Unrelated email' not in payload['text'], payload
    events=runtime.store.list_conversation_events('web:fare-status')
    turn=next(event for event in reversed(events) if event.get('event_type')=='conversation.chat_turn')
    assert any(tool['tool_name']=='list_crons' for tool in turn['tool_results'])
    assert elapsed < 4, elapsed


def test_core_exec_scope_preserves_real_runtime_approval_gate(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');executed=[]
    registry=ToolRegistry()
    registry.register(ToolSpec(name='terminal_exec',description='Core execution',risk_level=ToolRiskLevel.HIGH,
        side_effect_class=ToolSideEffectClass.WRITE,requires_approval=True,timeout_seconds=1,
        input_schema={'type':'object','properties':{'command':{'type':'string'}}}),
        lambda inv:executed.append(inv) or ToolResult(inv.invocation_id,inv.tool_name,'completed',{'stdout':'fixture'}))
    class Model:
        def create(self,**kw):
            assert 'terminal_exec' in {t['name'] for t in kw.get('tools',[])}
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'exec','name':'terminal_exec',
                'input':{'command':'printf fixture'}}]}
    provider=Model();runtime.model_client=provider
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=registry)
    with TestClient(app) as client:
        response=client.post('/api/chat',json={'text':'実行してください。','conversation_id':'web:core-approval'})
        assert response.status_code==200,response.text
        assert response.json()['type']=='approval_required',response.json()
        assert not executed
        events=[e for e in runtime.store.events if e.event_type=='tool.invocation.denied']
        assert events[-1].payload['tool_name']=='terminal_exec'
        assert events[-1].payload['reason']=='approval_required'
        approvals=runtime.store.list_approval_requests()
        assert len(approvals)==1 and approvals[0].context['tool_name']=='terminal_exec'
        runtime.checkpoint(force=True)
        from nullion.runtime_persistence import load_runtime_store
        stored=load_runtime_store(tmp_path/'runtime.db')
        assert stored.get_approval_request(approvals[0].approval_id) is not None
