"""A model attempt timeout cannot retire a still-budgeted native source read."""
import json
import os
import statistics
import threading
import time

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


def test_retried_model_can_read_once_without_resetting_the_request_budget(tmp_path, monkeypatch, record_property):
    from datetime import UTC, datetime
    from fastapi.testclient import TestClient
    from nullion import agent_orchestrator, preferences, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, register_web_fetch_tool

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'home'/'.env'))
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED', '1')
    for key in ('NULLION_TASK_DECOMPOSITION_ENABLED', 'NULLION_MULTI_AGENT_ENABLED',
                'NULLION_SKILL_LEARNING_ENABLED', 'NULLION_DOCTOR_ENABLED'):
        monkeypatch.setenv(key, '0')
    monkeypatch.setattr(preferences, '_PREFS_PATH', tmp_path/'home'/'preferences.json')
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *a, **kw: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *a, **kw: None)
    monkeypatch.setattr(agent_orchestrator, '_agent_model_timeout_seconds', lambda: .04)
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        rule_id='fixture-network', principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector='*',
        created_by='fixture-user', created_at=datetime.now(UTC)))
    registry = ToolRegistry(); reads=[]
    def fetch(url, timeout):
        reads.append(url)
        return {'url':url, 'status_code':200, 'text':'Verified sample value: 37.'}
    from nullion import tools as tools_module
    monkeypatch.setattr(tools_module, '_default_web_fetcher', fetch)
    register_web_fetch_tool(registry, fetch)

    class Model:
        attempts = 0
        completed_read = False
        def __init__(self, slow):
            self.slow=slow; self.release=threading.Event()
        def create(self, **kw):
            protocol=workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            names={tool['name'] for tool in kw.get('tools', [])}
            def text(value): return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name, arguments):
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name+str(self.attempts),
                    'name':name,'input':arguments}]}
            if 'select_attachment_format' in names:
                return tool('select_attachment_format', {'extension':None,'confidence':1})
            if not names: return text('{}')
            if not self.slow: return text('READY37')
            if 'web_fetch' not in names:
                assert self.attempts == 0, 'Source tools were retired after one attempt timeout'
                return tool('request_tool_scope', {'capabilities':['web'], 'tool_names':['web_fetch'],
                    'required_tool_names':['web_fetch']})
            self.attempts+=1
            if self.attempts == 1:
                self.release.wait(2)
                return text('LATE_RESPONSE_MUST_BE_DISCARDED')
            if self.attempts == 2:
                return tool('web_fetch', {'url':'https://source.test/value'})
            assert self.attempts == 3
            self.completed_read=True
            return text('Verified sample value: 37. Source: https://source.test/value')

    samples=[]; retry_counts=[]
    for index,prompt in enumerate(('Read the sample source and report its value.',
            'Lee la fuente de muestra e informa su valor.', '資料を読み、その値を報告してください。')):
        for slow in (True, False):
            model=Model(slow); runtime.model_client=model
            cid=f'web:attempt-recovery:{index}:{slow}'
            try:
                with TestClient(web_app.create_app(runtime,
                        orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
                    started=time.perf_counter()
                    response=client.post('/api/chat',json={'text':prompt if slow else 'READY37', 'conversation_id':cid})
                    samples.append(time.perf_counter()-started)
                    data=response.json(); assert response.status_code == 200, data
                    assert not data.get('artifacts') and 'LATE_RESPONSE' not in data['text'], data
                    turns=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')
                    turn=turns[-1]; results=turn.get('tool_results', [])
                    source_reads=[r for r in results if r.get('tool_name')=='web_fetch']
                    if slow:
                        assert len(source_reads)==1 and source_reads[0]['status']=='completed', turn
                        assert turn['response_fulfilled'] is True and '37' in data['text'], turn
                        assert model.completed_read and model.attempts==3
                        failures=runtime.store.list_recent_conversation_events(cid,event_type='conversation.model_failure')
                        assert len(failures)==1 and failures[0]['error_type']=='TimeoutError', failures
                        retry_counts.append(len(failures))
                    else:
                        assert data['text']=='READY37' and not results, turn
            finally:
                model.release.set()
    assert len(reads)==3 and retry_counts==[1,1,1]
    record_property('elapsed_seconds',json.dumps(samples))
    assert statistics.mean(samples)<1.5 and max(samples)<3, samples
