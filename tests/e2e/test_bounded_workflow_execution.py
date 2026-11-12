"""Mailbox-scale execution through real graph/SQLite and Telegram delivery.

Only the external model, account provider and Telegram network are faked.
"""
import json
import os
import time

import pytest

from test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, text_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def calls(name, indexes):
    return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': f'{name}-{i}',
        'name': name, 'input': {'index': i}} for i in indexes]}


def account_registry():
    from nullion.tools import ToolRegistry, ToolSpec, ToolSideEffectClass, ToolRiskLevel, ToolResult
    registry = ToolRegistry()
    executed = []
    for name in ('collection_scan', 'record_read', 'calendar_inspect'):
        def read(inv, name=name):
            index = inv.arguments['index']; executed.append((name, index))
            output = ({'record_id': index, 'body': 'a' * 5000 + f'EXACT_VALUE={index + 700}' + 'z' * 5000}
                if name == 'record_read' else {'scope': index, 'records': [{'id': f'existing-{index}', 'date': '2028-04-09'}], 'checked': True})
            return ToolResult(inv.invocation_id, name, 'completed', output)
        registry.register(ToolSpec(name, 'Read an authorized source', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2, same_request_read_cache=name == 'record_read'), read)
    return registry, executed


@pytest.mark.parametrize('prompt', ['確認した値と予定表の照合結果を報告。', 'Informa el valor verificado y la comprobación del calendario.'])
def test_large_saved_report_finishes_once_with_bounded_context_and_no_source_rereads(tmp_path, monkeypatch, prompt, record_property):
    from nullion import crons
    from nullion.retained_work_context import INSPECT_TOOL
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry, executed = account_registry()
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Account report', '* * * * *', prompt, enabled=False,
        delivery_channel='telegram', delivery_target='2222')
    class Provider:
        rounds = 0
        sizes = []
        reviews = 0
        def create(self, **kwargs):
            system = kwargs.get('system', '')
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [
                        {'requirement': 'Selected source value', 'requirement_kind': 'fact',
                         'evidence_subject_id': 'source', 'eligible_evidence_tool_names': ['record_read']},
                        {'requirement': 'Calendar checked', 'requirement_kind': 'action',
                         'evidence_subject_id': 'check', 'eligible_evidence_tool_names': ['calendar_inspect']},
                        {'requirement': 'Report selected value and calendar check', 'requirement_kind': 'constraint',
                         'evidence_subject_id': 'report', 'eligible_evidence_tool_names': ['record_read', 'calendar_inspect']}]}))
            if 'semantic completion reviewer' in system:
                self.reviews += 1
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                assert '749' in packet['draft_final_response']
                source = next(r for r in packet['tool_evidence'] if r['tool_name'] == 'record_read'
                    and isinstance(r.get('output'), dict) and r['output'].get('record_id') == 49)
                assert any(w['content'] == 'EXACT_VALUE=749' and w['invocation_id'] == source['invocation_id']
                    for w in packet['previously_inspected_evidence'])
                calendar = next(r for r in packet['tool_evidence'] if r['tool_name'] == 'calendar_inspect' and r['status'] == 'completed')
                return text_response(json.dumps({'disposition': 'complete', 'unresolved_requirements': [],
                    'retry_tool_names': [], 'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': True,
                        'draft_covered': True, 'evidence_invocation_ids': [
                            (calendar if row['requirement_kind'] == 'action' else source)['invocation_id']]}
                        for row in packet['material_requirement_contract']]}))
            protocol = workflow_provider_reply(kwargs, completion=False)
            if protocol is not None: return protocol
            self.rounds += 1
            encoded = json.dumps(kwargs['messages'])
            self.sizes.append(len(encoded))
            if self.rounds == 1: return calls('collection_scan', range(30))
            if 2 <= self.rounds <= 4: return calls('record_read', range((self.rounds-2)*32, (self.rounds-1)*32))
            if self.rounds == 5: return calls('calendar_inspect', range(4))
            if self.rounds == 6:
                assert 'EXACT_VALUE=749' not in encoded
                assert INSPECT_TOOL in {t['name'] for t in kwargs['tools']}
                # Resolve the exact previously read record using the real receipts.
                # Invocation ids, rather than source ids, are the inspection boundary.
                index_text = kwargs['messages'][-1]['content'][0]['text']
                index = json.loads(index_text.split('\n', 1)[1])['records']
                identity = [r for r in index if r['tool_name'] == 'record_read'][49]['invocation_id']
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'inspect',
                    'name': INSPECT_TOOL, 'input': {'requests': [{'invocation_id': identity,
                        'json_pointer': '/body', 'offset': 5000, 'length': 15}]}}]}
            assert 'EXACT_VALUE=749' in encoded
            # Repeat reads in the same batch are served from owned snapshots.
            if self.rounds == 7:
                response = calls('record_read', [49] * 8)
                for i, block in enumerate(response['content']): block['id'] = f'repeat-{i}'
                return response
            return text_response('Verified value: 749. Calendar checked; no new events were needed.')
    provider = Provider()
    started = time.monotonic()
    result = run_scheduled(job, provider, runtime, registry, send)
    elapsed = time.monotonic() - started
    assert result['response_fulfilled'] is True and result['cron_delivery_status'] == 'sent', result
    assert len(deliveries) == 1 and '749' in deliveries[0]['text']
    assert len(executed) == 130 and len({i for name, i in executed if name == 'record_read'}) == 96
    # The fresh 32-record batch must be seen before it is archived. Bounds
    # therefore include that batch; the later small-batch view stays compact.
    assert max(provider.sizes) < 500_000 and sum(provider.sizes) < 2_000_000
    assert provider.sizes[5] < 180_000
    assert provider.rounds == 8 and provider.reviews == 0
    timings = [e for e in runtime.store.list_conversation_events('telegram:2222') if e.get('event_type') == 'conversation.tool_timing']
    assert len(timings) == 130 and all(e.get('argument_fingerprint') for e in timings)
    # This measures local orchestration only; there is no live provider latency.
    assert elapsed < 15
    record_property('workflow_metrics', json.dumps({'source_calls': len(executed), 'source_reads': 96,
        'producer_rounds': provider.rounds, 'reviews': provider.reviews,
        'max_message_json_chars': max(provider.sizes), 'total_message_json_chars': sum(provider.sizes),
        'local_elapsed_seconds': round(elapsed, 3), 'deliveries': len(deliveries)}))


@pytest.mark.parametrize('prompt', ['全記録を確認。', 'Revisa todos los registros.'])
def test_large_batch_and_same_request_recovery_ignore_removed_quotas(tmp_path, monkeypatch, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator, _serialize_pending_tool_calls
    from nullion.chat_operator import _registry_with_request_retry_evidence
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry, executed = account_registry()
    for name in ('TOOL_CALLS', 'MODEL_INPUT_CHARS', 'EVIDENCE_INSPECTIONS'):
        monkeypatch.setenv('NULLION_REQUEST_MAX_' + name, '1')
    class Provider:
        rounds = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            self.rounds += 1
            self.snapshot = kwargs["messages"]
            if self.rounds == 1: return calls('record_read', range(260))
            return text_response('260 records checked.')
    provider = Provider(); runner = AgentOrchestrator(model_client=provider)
    kwargs = dict(conversation_id='web:volume', principal_id='web:volume', user_message=prompt,
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=8)
    result = runner.run_turn(conversation_history=[], **kwargs)
    assert len(executed) == 260 and provider.rounds == 2
    assert result.response_fulfilled is True and result.final_text == '260 records checked.'
    assert result.work_usage['tool_calls'] == 260
    # Historical counters survive recovery for diagnostics, never as a stop condition.
    result.work_usage.update(model_input_chars=2_000_001, inspections=33)
    # Finished turns discard snapshots; exercise the actual unfinished-resume
    # boundary with the real protocol history captured before finalization.
    result.messages_snapshot = provider.snapshot
    resumed = runner.resume_turn(messages_snapshot=result.messages_snapshot, previous_result=result, **kwargs)
    assert resumed.response_fulfilled is True and len(executed) == 260
    retry_registry = _registry_with_request_retry_evidence(registry, result=resumed,
        conversation_id='web:volume', principal_id='web:volume', user_message=prompt)
    retried = runner.run_turn(conversation_history=[], **{**kwargs, 'tool_registry': retry_registry})
    assert retried.response_fulfilled is True and len(executed) == 260
    persisted = json.loads(json.dumps(_serialize_pending_tool_calls(result.tool_results,
        result.invocation_metadata, work_usage=result.work_usage)))
    restored = runner.resume_turn(messages_snapshot=result.messages_snapshot, suspended_tool_calls=persisted, **kwargs)
    assert restored.response_fulfilled is True and len(executed) == 260
    assert restored.work_usage['tool_calls'] == 260


def test_cumulative_input_above_former_limit_does_not_stop_report(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry, executed = account_registry()
    monkeypatch.setenv('NULLION_REQUEST_MAX_MODEL_INPUT_CHARS', '1000')
    class Provider:
        rounds = 0
        def create(self, **kwargs):
            if 'material-requirements planner' in kwargs.get('system', ''):
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [{
                        'requirement': 'Report checked collections', 'requirement_kind': 'other',
                        'evidence_subject_id': 'collections', 'eligible_evidence_tool_names': ['collection_scan']}]}))
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            self.rounds += 1
            if self.rounds <= 28: return calls('collection_scan', [self.rounds])
            return text_response('28 source collections checked.')
    provider = Provider()
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:size', principal_id='web:size',
        user_message='記録を照合。' + 'a'*75_000, conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, max_iterations=32)
    assert len(executed) == 28 and provider.rounds == 29
    assert result.work_usage['model_input_chars'] > 2_000_000
    assert result.response_fulfilled is True and result.final_text == '28 source collections checked.'


def test_more_than_thirty_two_retained_inspections_complete_without_new_source_io(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.retained_work_context import INSPECT_TOOL
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry, executed = account_registry()
    monkeypatch.setenv('NULLION_REQUEST_MAX_EVIDENCE_INSPECTIONS', '1')
    class Provider:
        rounds = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            self.rounds += 1
            if self.rounds == 1: return calls('record_read', range(10))
            if self.rounds == 2:
                assert INSPECT_TOOL in {t['name'] for t in kwargs['tools']}
                index = json.loads(kwargs['messages'][-1]['content'][0]['text'].split('\n', 1)[1])
                identity = index['records'][0]['invocation_id']
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': f'inspect-{i}',
                    'name': INSPECT_TOOL, 'input': {'requests': [{'invocation_id': identity,
                        'json_pointer': '/body', 'offset': i, 'length': 1}]}} for i in range(33)]}
            return text_response('Retained evidence checked.')
    result = AgentOrchestrator(model_client=Provider()).run_turn(conversation_id='web:inspect',
        principal_id='web:inspect', user_message='保存した証拠を確認。', conversation_history=[],
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
    assert result.response_fulfilled is True and len(executed) == 10
    assert result.work_usage['inspections'] == 33
    assert sum(r.tool_name == INSPECT_TOOL and r.status == 'completed' for r in result.tool_results) == 33


@pytest.mark.parametrize('mode,expected_reads', [('snapshot', 1), ('fresh', 2), ('write', 2), ('legacy', 2), ('failed', 2)])
def test_read_reuse_is_adapter_declared_and_invalidated_by_writes_or_failure(tmp_path, monkeypatch, mode, expected_reads):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(); reads = []; writes = []
    read_name = 'calendar_list' if mode == 'legacy' else 'opaque_read'
    if mode == 'legacy':
        from datetime import UTC, datetime
        from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
        for kind in (BoundaryKind.ACCOUNT_ACCESS, BoundaryKind.OUTBOUND_NETWORK):
            runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='authorized-'+kind.value,
                principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=kind, mode=PolicyMode.ALLOW,
                selector='*', created_by='fixture', created_at=datetime.now(UTC)))
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name,
            'failed' if mode == 'failed' and len(reads) == 1 else 'completed', {'value': len(reads)})
    def write(inv):
        writes.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'id': 'written', 'value': 2})
    registry.register(ToolSpec(read_name, 'Read a value', ToolRiskLevel.LOW, ToolSideEffectClass.READ,
        False, 2, same_request_read_cache=mode not in {'fresh', 'legacy'}), read)
    registry.register(ToolSpec('opaque_write', 'Change a value', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE,
        False, 2), write)
    class Provider:
        rounds = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            self.rounds += 1
            if mode in {'write', 'legacy'} and self.rounds == 2: return calls('opaque_write', [0])
            if self.rounds <= (4 if mode == 'legacy' else 3 if mode == 'write' else 2):
                response = calls(read_name, [0]); response['content'][0]['id'] = f'read-{self.rounds}'
                if mode == 'legacy':
                    response['content'][0]['input'].update(start='2028-04-09T00:00:00Z', end='2028-04-10T00:00:00Z')
                return response
            if mode == 'legacy':
                result = kwargs['messages'][-1]['content'][0]
                assert json.JSONDecoder().raw_decode(result['content'][0]['text'])[0]['output']['value'] == 2
            return text_response(f'Value: {len(reads)}.')
    result = AgentOrchestrator(model_client=Provider()).run_turn(conversation_id='web:reuse', principal_id='web:reuse',
        user_message='値を確認。', conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store)
    assert len(reads) == expected_reads and len(writes) == (mode in {'write', 'legacy'}), [
        (r.tool_name, r.status, r.output, r.error) for r in result.tool_results]
    assert result.response_fulfilled is True and result.final_text == f'Value: {expected_reads}.'


def test_scheduled_large_report_delivers_result_without_work_limit_notice(tmp_path, monkeypatch):
    from nullion import crons
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry, executed = account_registry()
    deliveries, send = telegram_boundary(monkeypatch)
    monkeypatch.setenv('NULLION_REQUEST_MAX_TOOL_CALLS', '5')
    job = crons.add_cron('Source report', '* * * * *', '確認して報告。', enabled=False,
        delivery_channel='telegram', delivery_target='2222')
    class Provider:
        rounds = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            self.rounds += 1
            if self.rounds == 1: return calls('record_read', range(260))
            return text_response('260 records checked; report complete.')
    provider = Provider()
    result = run_scheduled(job, provider, runtime, registry, send)
    assert len(executed) == 260 and provider.rounds == 2
    assert result['response_fulfilled'] is True and result['cron_delivery_status'] == 'sent'
    assert len(deliveries) == 1 and '260 records checked' in deliveries[0]['text']
    assert 'work limit' not in deliveries[0]['text']


def test_native_long_email_survives_adapter_and_per_result_compaction_without_rereading(tmp_path, monkeypatch):
    import base64
    from types import SimpleNamespace
    from nullion import tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.retained_work_context import INSPECT_TOOL
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = tools.ToolRegistry(); tools.register_connector_plugin(registry)
    from datetime import UTC, datetime
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    for kind in (BoundaryKind.ACCOUNT_ACCESS, BoundaryKind.OUTBOUND_NETWORK):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='authorized-'+kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=kind, mode=PolicyMode.ALLOW,
            selector='*', created_by='fixture', created_at=datetime.now(UTC)))
    body = 'Source context. ' * 7000 + 'EXACT_DEADLINE=2028-04-09'
    raw = {'id': 'native-message', 'payload': {'mimeType': 'text/plain',
        'body': {'data': base64.urlsafe_b64encode(body.encode()).decode()}}}
    network = []
    monkeypatch.setattr(tools, '_default_email_connector_provider_id', lambda principal: 'fixture')
    monkeypatch.setattr(tools, '_connector_connection_for_invocation', lambda *a: SimpleNamespace())
    monkeypatch.setattr(tools, '_email_messages_endpoint_for_provider', lambda *a: 'https://provider.invalid/messages')
    def external_network(invocation, **kwargs):
        network.append(kwargs)
        return raw
    monkeypatch.setattr(tools, '_connector_json_request', external_network)
    class Provider:
        rounds = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            self.rounds += 1
            if self.rounds == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'read',
                    'name': 'email_read', 'input': {'id': 'native-message'}}]}
            if self.rounds == 2:
                assert INSPECT_TOOL in {t['name'] for t in kwargs['tools']}
                packet = json.loads(kwargs['messages'][-1]['content'][0]['text'].split('\n', 1)[1])
                identity = next(r['invocation_id'] for r in packet['records'] if r['tool_name'] == 'email_read')
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'inspect', 'name': INSPECT_TOOL,
                    'input': {'requests': [{'invocation_id': identity, 'json_pointer': '/message/body',
                        'offset': len('Source context. ')*7000, 'length': 100}]}}]}
            assert 'EXACT_DEADLINE=2028-04-09' in json.dumps(kwargs['messages'])
            inspection = next(b for m in kwargs['messages'] if isinstance(m.get('content'), list)
                for b in m['content'] if b.get('type') == 'tool_result' and b.get('tool_use_id') == 'inspect')
            assert json.loads(inspection['content'][0]['text'])['security']['untrusted_tool_output'] is True
            return text_response('Verified deadline: April 9, 2028.')
    result = AgentOrchestrator(model_client=Provider()).run_turn(conversation_id='web:native', principal_id='web:native',
        user_message='このメールの期限を確認。', conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store)
    assert result.response_fulfilled is True and result.final_text == 'Verified deadline: April 9, 2028.', [
        (r.status, r.error, {key: value for key, value in r.output.items() if key != 'message'}) for r in result.tool_results]
    assert len(network) == 1
    native = next(r for r in result.tool_results if r.tool_name == 'email_read')
    assert native.output['message']['body'].endswith('EXACT_DEADLINE=2028-04-09')


@pytest.mark.parametrize('owned', [True, False])
def test_amendment_can_inspect_owned_older_sources_without_getting_a_new_read_grant(tmp_path, monkeypatch, owned):
    from nullion.agent_orchestrator import AgentOrchestrator, TurnContinuationEvidence
    from nullion.tools import ToolResult
    from nullion.retained_work_context import INSPECT_TOOL
    from nullion.chat_operator import _RevisionContinuationRegistry
    runtime = setup_runtime(tmp_path, monkeypatch)
    base, executed = account_registry()
    source = ToolResult('earlier-source', 'record_read', 'completed', {'value': 749})
    registry = _RevisionContinuationRegistry(base, TurnContinuationEvidence(
        conversation_id='web:amended' if owned else 'web:foreign', principal_id='web:amended',
        parent_turn_ids=('selected-parent',), tool_results=(source,)))
    class Provider:
        rounds = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            self.rounds += 1
            names = {tool['name'] for tool in kwargs.get('tools', [])}
            assert (INSPECT_TOOL in names) is owned
            if not owned:
                assert 'earlier-source' not in json.dumps(kwargs['messages'])
                return text_response('No selected source is available.')
            if self.rounds == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'inspect', 'name': INSPECT_TOOL,
                    'input': {'requests': [{'invocation_id': 'earlier-source', 'json_pointer': '/value', 'offset': 0, 'length': 3}]}}]}
            assert '749' in json.dumps(kwargs['messages'])
            return text_response('Verified value: 749.')
    result = AgentOrchestrator(model_client=Provider()).run_turn(conversation_id='web:amended', principal_id='web:amended',
        user_message='前の条件を修正して値を確認。', conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store)
    assert not executed
    if owned: assert result.response_fulfilled is True and result.final_text == 'Verified value: 749.'
    else: assert not result.tool_results
