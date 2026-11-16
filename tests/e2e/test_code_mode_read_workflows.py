"""Whole read workflows through the real API/SQLite and native source handlers."""
import json
import os
import time
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('mode', ['off', 'read_only', 'on'])
@pytest.mark.parametrize('explicit', [False, True])
def test_email_and_calendar_share_the_selected_execution_layer(tmp_path, monkeypatch, mode, explicit, record_property):
    from fastapi.testclient import TestClient
    from nullion import code_mode
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, register_email_plugin, register_calendar_plugin
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, OPERATOR_PERMISSION_PRINCIPAL
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    from nullion import connections, connector_prompt_context
    monkeypatch.setattr(connections, '_CONNECTIONS_PATH', tmp_path/'connections.json')
    connections.save_connection_registry({'connections':[{'connection_id':'fixture-mail',
        'workspace_id':'workspace_admin','provider_id':'fixture_connector_provider',
        'display_name':'Fixture account','active':True,'permission_mode':'read'}]})
    for key in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE'):
        monkeypatch.setattr(connector_prompt_context,key,None)
    for key in ('_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE_EXPIRES_AT','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE_EXPIRES_AT'):
        monkeypatch.setattr(connector_prompt_context,key,0.0)
    for kind in (BoundaryKind.ACCOUNT_ACCESS, BoundaryKind.OUTBOUND_NETWORK):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-'+kind.value,
            OPERATOR_PERMISSION_PRINCIPAL, kind, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    native_calls = []
    def emails(query, limit):
        native_calls.append('email_search')
        return [{'id':'mail-37', 'subject':'Observatory invoice', 'snippet':'Paid 37 USD.', 'from':'billing@example.org'}]
    def calendar(start, end, maximum):
        native_calls.append('calendar_list')
        return [{'id':'event-47', 'summary':'Observatory visit', 'start':start, 'end':end}]
    registry = ToolRegistry()
    register_email_plugin(registry, email_searcher=emails)
    register_calendar_plugin(registry, calendar_lister=calendar)
    args = {'start':'2026-11-15T09:00:00Z', 'end':'2026-11-16T09:00:00Z'}
    class Model:
        phase = 0
        errors = []
        def create(self, **kwargs):
            try: return self.respond(**kwargs)
            except Exception as error:
                self.errors.append(repr(error)); raise
        def respond(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['connector'],
                    'tool_names':['email_search','calendar_list'], 'required_tool_names':['email_search','calendar_list'],
                    'source_user_requested':True, 'connector_app_ids':['google-mail','google-calendar'], 'source_evidence':self.prompt})
            if self.phase == 2:
                if explicit and mode != 'off':
                    return call(code_mode.TOOL_NAME, {'tool_names':['email_search','calendar_list'],
                        'code': "mail = call_tool('email_search', {'query':'newer_than:1d'})\n"
                            + "agenda = call_tool('calendar_list', " + repr(args) + ")\n[mail['output'], agenda['output']]"})
                result = call('email_search', {'query':'newer_than:1d'})
                second = call('calendar_list', args)['content'][0]; second['id'] = 'calendar-second'
                result['content'].append(second)
                return result
            assert self.phase == 3, 'Unexpected model call'
            evidence = json.dumps(kwargs['messages'])
            if 'Paid 37 USD.' not in evidence or 'Observatory visit' not in evidence:
                raise RuntimeError(json.dumps({'calls':native_calls, 'protocol':[b for m in kwargs['messages'] for b in m.get('content', []) if isinstance(b, dict) and b.get('type') == 'tool_result'][-3:]}))
            return text('Your observatory email reports a payment of 37 USD. Your calendar lists an observatory visit.')
    model = Model(); samples = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['メールとカレンダーを確認', 'Consulta mi correo y calendario', 'Consulte mes courriels et mon calendrier']):
            model.phase = 0; model.prompt = prompt
            conversation = 'web:code-read-'+str(index)
            started = time.monotonic()
            response = client.post('/api/chat', json={'text':prompt, 'conversation_id':conversation})
            samples.append(time.monotonic()-started)
            if model.errors: print('MODEL ERRORS', json.dumps(model.errors))
            assert not model.errors, model.errors
            assert response.status_code == 200 and '37 USD' in response.json()['text'], response.text
            results = [r for e in runtime.store.list_conversation_events(conversation)
                if e.get('event_type') == 'conversation.chat_turn' for r in e.get('tool_results', [])]
            composed = [r for r in results if r['tool_name'] == code_mode.TOOL_NAME]
            assert bool(composed) == (mode != 'off' and explicit), results
            if composed:
                assert composed[-1]['status'] == 'completed', composed
                assert {r['tool_name'] for r in composed[-1]['output']['reads']} == {'email_search','calendar_list'}
            assert all(r['status'] == 'completed' for r in results), results
    assert native_calls == ['email_search','calendar_list']*3
    assert sum(samples)/len(samples) < 3 and max(samples) < 5, samples
    record_property('mean_read_workflow_seconds', sum(samples)/len(samples))


def test_gate_preserves_real_pending_read_approval_without_running_handler(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    from nullion.preferences import save_preferences
    save_preferences({'sentinel_mode':'ask_all'})
    called = []
    registry = ToolRegistry()
    registry.register(ToolSpec('protected_read', 'Read a protected source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, True, 5, input_schema={'type':'object'}),
        lambda invocation: called.append(invocation) or ToolResult(invocation.invocation_id,
            invocation.tool_name, 'completed', {'text':'protected value'}))
    class Model:
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            return protocol if protocol is not None else call('protected_read', {})
    result = AgentOrchestrator(model_client=Model()).run_turn(
        conversation_id='web:protected-read', principal_id='workspace_admin', user_message='確認',
        conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=2)
    assert result.suspended_for_approval and result.approval_id, [(r.tool_name,r.status,r.output) for r in result.tool_results]
    assert not called
    approval = runtime.store.get_approval_request(result.approval_id)
    assert approval is not None and approval.status.value == 'pending'


def test_primary_code_layer_reduces_dependent_read_roundtrips(tmp_path, monkeypatch, record_property):
    """A slow external model makes re-entering it per message measurably costly."""
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, register_email_plugin
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, OPERATOR_PERMISSION_PRINCIPAL
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    for kind in (BoundaryKind.ACCOUNT_ACCESS, BoundaryKind.OUTBOUND_NETWORK):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-'+kind.value,
            OPERATOR_PERMISSION_PRINCIPAL, kind, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    records = [{'id':'mail-'+str(i),'subject':'Record '+str(i),'from':'source@example.org'} for i in range(3)]
    native_calls = []
    def search(query, limit):
        native_calls.append('search'); return records
    def read(identifier):
        native_calls.append(identifier)
        return {'id':identifier,'subject':identifier,'body':'Verified amount 17 USD.'}
    registry = ToolRegistry()
    register_email_plugin(registry,email_searcher=search,email_reader=read)
    class Model:
        rounds = 0
        errors = []
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            time.sleep(.25)  # External provider boundary, not a Nullion mock.
            self.rounds += 1
            names = {t['name'] for t in kwargs.get('tools', [])}
            if self.rounds == 1:
                if 'code_mode' in names:
                    assert 'email_search' not in names and 'email_read' not in names
                    return call('code_mode',{'tool_names':['email_search','email_read'],
                        'code':"hits = call_tool('email_search', {'query':'newer_than:1d'})['output']['results']\n"
                            "bodies = [call_tool('email_read', {'id':hit['id']})['output']['message']['body'] for hit in hits]\n"
                            + ("" if getattr(self,"omit_final_expression",False) else "bodies")})
                return call('email_search',{'query':'newer_than:1d'})
            if 'code_mode' not in names and self.rounds <= 4:
                return call('email_read',{'id':records[self.rounds-2]['id']})
            assert 'Verified amount 17 USD.' in json.dumps(kwargs['messages'])
            return text('All three emails report 17 USD each.')
    timings = {}
    for mode in ['off','on']:
        monkeypatch.setenv('NULLION_CODE_MODE',mode)
        samples = []
        for index, prompt in enumerate(['メール本文を確認','Lee los mensajes','Lis les messages']):
            model = Model(); model.omit_final_expression = index==1; started = time.monotonic()
            result = AgentOrchestrator(model_client=model).run_turn(
                conversation_id=f'web:roundtrips-{mode}-{index}',principal_id='workspace_admin',
                user_message=prompt,conversation_history=[],tool_registry=registry,
                policy_store=runtime.store,approval_store=runtime.store)
            samples.append(time.monotonic()-started)
            assert model.rounds == (2 if mode != 'off' else 5)
            assert '17 USD' in result.final_text and not result.suspended_for_approval
            reads = [r for r in result.tool_results if r.tool_name in {'email_search','email_read'}]
            assert len(reads) == 4 and all(r.status == 'completed' for r in reads)
            if mode != 'off':
                programs = [r for r in result.tool_results if r.tool_name == 'code_mode']
                assert len(programs) == 1 and len(programs[0].output['reads']) == 4
        timings[mode] = sum(samples)/len(samples)
        record_property(mode+'_mean_seconds',timings[mode])
    assert native_calls == ['search','mail-0','mail-1','mail-2']*6
    assert timings['on'] < timings['off']*.85, timings
    assert timings['on'] < 2, timings
