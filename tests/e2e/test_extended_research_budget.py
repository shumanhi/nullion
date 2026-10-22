"""Default multi-round work completes through the real chat/persistence boundary."""
import json
import os
import time
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


def test_default_budget_completes_more_than_twenty_four_model_rounds(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from datetime import UTC, datetime
    from nullion.tools import ToolRegistry, register_web_fetch_tool
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED', '1')
    monkeypatch.delenv('NULLION_AGENT_TURN_MAX_ITERATIONS', raising=False)
    monkeypatch.delenv('NULLION_BROWSER_AGENT_TURN_MAX_ITERATIONS', raising=False)
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'home'/'.env'))
    from nullion import preferences
    monkeypatch.setattr(preferences, '_PREFS_PATH', tmp_path/'home'/'preferences.json')
    for key in ('NULLION_TASK_DECOMPOSITION_ENABLED', 'NULLION_MULTI_AGENT_ENABLED',
                'NULLION_SKILL_LEARNING_ENABLED', 'NULLION_DOCTOR_ENABLED'):
        monkeypatch.setenv(key, '0')
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *args, **kwargs: None)
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        rule_id='research-network', principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector='*',
        created_by='fixture-user', created_at=datetime.now(UTC)))
    registry = ToolRegistry()
    from nullion import tools as tools_module
    fetcher = lambda url, timeout: {'url':url, 'status_code':200, 'text':'Observation: 18°C.'}
    monkeypatch.setattr(tools_module, '_default_web_fetcher', fetcher)
    register_web_fetch_tool(registry, fetcher)
    class Model:
        rounds = 0
        def create(self, **kwargs):
            names = {t['name'] for t in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return {'content':[{'type':'tool_use','id':'format','name':'select_attachment_format',
                    'input':{'extension':None,'confidence':1}}]}
            if not names:
                return {'content':[{'type':'text','text':'{}'}]}
            if 'web_fetch' not in names:
                assert not any(m.get('role') == 'assistant' for m in kwargs.get('messages', [])), json.dumps(kwargs.get('messages', [])[-2:])
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope', 'name':'request_tool_scope',
                    'input':{'capabilities':['web'],'tool_names':['web_fetch'],'required_tool_names':['web_fetch']}}]}
            self.rounds += 1
            if self.rounds <= 30:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':f'read-{self.rounds}',
                    'name':'web_fetch','input':{'url':f'https://example.org/observations/{self.rounds}'}}]}
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'Collected all 30 observations: 18°C.'}]}
    elapsed = []
    for index, prompt in enumerate(('Collect the thirty station observations.', 'Recoge las treinta observaciones.', '三十地点の観測を集めてください。')):
        model = Model()
        app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
        cid = f'web:long-research-{index}'
        with TestClient(app) as client:
            started = time.perf_counter()
            result = client.post('/api/chat', json={'text':prompt, 'conversation_id':cid}).json()
            elapsed.append(time.perf_counter()-started)
        assert result.get('text') == 'Collected all 30 observations: 18°C.', result
        assert not result.get('approval_id') and not result.get('suspended_for_approval')
        assert model.rounds == 31
        turns = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')
        assert turns[-1]['assistant_reply'] == result['text']
        reads = [r for r in turns[-1]['tool_results'] if r['tool_name'] == 'web_fetch']
        assert len(reads) == 30 and all(r['status'] == 'completed' for r in reads)
    record_property('elapsed_seconds', json.dumps(elapsed))
    assert sum(elapsed)/len(elapsed) < 2 and max(elapsed) < 3, elapsed
    assert not runtime.store.list_approval_requests()
