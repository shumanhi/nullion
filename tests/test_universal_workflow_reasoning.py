from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import _completion_review_required
from nullion.tools import ToolResult
from nullion.workflow_reasoning import WORKFLOW_REASONING_INSTRUCTION, outcome_binding


@pytest.mark.parametrize('family', ['sensor', 'catalog', 'document', 'schedule', 'account', 'opaque_adapter'])
@pytest.mark.parametrize('status', ['completed', 'failed', 'denied'])
def test_every_tool_workflow_keeps_quality_review_disabled(family, status):
    result = ToolResult('e1', family, status, {'value': 7})
    assert not _completion_review_required({}, tool_results=[result])


def test_plain_text_does_not_add_an_independent_review():
    assert not _completion_review_required({}, tool_results=[])


def test_old_review_budget_never_reopens_an_independent_grader():
    assert not _completion_review_required({'completion_review_count': 99},
        tool_results=[ToolResult('e', 'sensor', 'completed', {'value': 7})])


def test_review_binding_changes_with_request_context_evidence_and_file(tmp_path):
    file = tmp_path / 'result.txt'; file.write_text('one')
    result = ToolResult('e', 'sensor', 'completed', {'value': 7})
    def binding(request='request', context=None, draft='7'):
        return outcome_binding(request, context or {}, draft, [result], [str(file)])
    original = binding()
    assert original == binding()
    assert original != binding(request='amended request')
    assert original != binding(context={'quoted_message': 'different target'})
    assert original != binding(draft='8')
    result.output['value'] = 8
    assert original != binding()
    result.output['value'] = 7
    file.write_text('changed file')
    assert original != binding()


def test_plain_direct_and_backend_calls_share_reasoning_contract():
    from nullion.chat_operator import _plain_no_tool_direct_model_reply
    from nullion.chat_backend import _build_system_prompt
    calls = []
    class Provider:
        def create(self, **kw):
            calls.append(kw)
            return {'content': [{'type': 'text', 'text': 'Bonjour.'}]}
    assert _plain_no_tool_direct_model_reply(model_client=Provider(), prompt='Bonjour',
        memory_context=None, conversation_context=None) == 'Bonjour.'
    assert len(calls) == 1
    assert WORKFLOW_REASONING_INSTRUCTION in calls[0]['system']
    assert WORKFLOW_REASONING_INSTRUCTION in _build_system_prompt()


@pytest.mark.parametrize('change', ['file', 'tool', 'output'])
def test_review_cannot_cache_proof_for_evidence_that_changed_during_the_call(tmp_path,monkeypatch,change):
    import os
    import nullion.agent_orchestrator as module
    from nullion.tools import ToolRegistry
    path=tmp_path/'result.txt';path.write_text('one');stamp=path.stat()
    result=ToolResult('read','opaque_read','completed',{'value':7})
    state={'user_message':'指定した値を確認。','tool_registry':ToolRegistry()}
    def review(*args,**kwargs):
        # A provider boundary can overlap a real writer. Preserve timestamps
        # and size to prove the binding follows bytes, not just metadata.
        if change=='file':
            path.write_text('two');os.utime(path,ns=(stamp.st_atime_ns,stamp.st_mtime_ns))
        elif change=='tool':result.tool_name='different_reader'
        else:result.output={'value':8}
        return module._CompletionReviewDecision('complete',execution_satisfied=True)
    monkeypatch.setattr(module,'_perform_completion_review',review)
    decision=module._review_risky_agent_completion(state,final_text='値は7です。',
        tool_results=[result],artifacts=[str(path)],force=True)
    assert decision is None
    assert path.read_text() == 'one' and result.output == {'value':7}
    assert not state.get('outcome_review_cache')


def test_prepared_answer_accepts_legacy_receipts_without_losing_tool_or_recovery_identity():
    from dataclasses import asdict
    from nullion.execution_outcome import prepare_answer,prepared_answer_matches
    result=ToolResult('read','opaque_read','completed',{'value':7})
    answer=prepare_answer('対象の値','値は7です。',[asdict(result)])
    assert prepared_answer_matches(answer,'対象の値',[result])
    result.recovered_by=('later-read',)
    assert not prepared_answer_matches(answer,'対象の値',[result])


def test_action_receipt_optional_ids_are_not_empty_source_evidence():
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    from nullion.tools import _action_receipt
    receipt = _action_receipt(action='updated', object_type='resource', summary='Updated the selected resource.')
    result = ToolResult('a', 'resource_write', 'completed', {'action_receipt': receipt})
    assert _completion_review_evidence_result_is_positive(result)
    for invalid in ({'items': []}, {'status': 'failed'}, {'verified': False}, {'error': 'source rejected'}):
        result.output = {'action_receipt': receipt, **invalid}
        assert not _completion_review_evidence_result_is_positive(result)


def test_review_retains_loaded_profile_and_selected_reply_context():
    from nullion.agent_orchestrator import _completion_review_request_context
    state = {'tool_flow_context': {'known_user_context': 'Home city: Example City. Ask before sending messages.',
        'request_context': {'quoted_message': {'platform': 'web', 'reply_to_text': 'Use the selected departure.'}}}}
    packet = _completion_review_request_context(state)
    assert 'Example City' in packet['known_user_context']
    assert packet['request_context']['quoted_message']['reply_to_text'] == 'Use the selected departure.'


def test_corrupt_native_pdf_is_incomplete_evidence_instead_of_crashing(tmp_path):
    from nullion.artifact_evidence import artifact_structure_evidence
    path = tmp_path/'invalid.pdf'; path.write_bytes(b'%PDF-1.4\ncorrupt')
    evidence = artifact_structure_evidence(path)
    assert evidence['layout_inspection_complete'] is False


def test_action_review_context_is_scoped_and_read_tools_do_not_enter_it():
    from nullion.workflow_reasoning import action_reasoning_scope, active_action_review
    from nullion.tools import ToolInvocation, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    calls = []
    inv = ToolInvocation('i', 'resource', 'p', {})
    read = ToolSpec('resource', 'Read resource', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2)
    write = ToolSpec('resource', 'Write resource', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2)
    with action_reasoning_scope(lambda spec, invocation: calls.append(invocation) or 'guarded'):
        assert active_action_review(read, inv) is None
        assert active_action_review(write, inv) == 'guarded'
    assert active_action_review(write, inv) is None and calls == [inv]


@pytest.mark.asyncio
async def test_summary_critique_cannot_restart_completed_child_or_suppress_file(tmp_path, monkeypatch):
    import json
    from nullion.agent_orchestrator import TurnResult
    from nullion.result_aggregator import ResultAggregator, GroupState
    from nullion.task_queue import TaskGroup, TaskRecord, TaskRegistry, TaskResult, TaskStatus, TaskPriority
    draft = tmp_path/'draft.html'
    draft.write_text('<html><body><p>Draft value: 7. Missing the requested unit.</p></body></html>')
    task = TaskRecord('t', 'g', 'web:g', 'web:g', 'Value', 'Report the selected value', TaskStatus.COMPLETE,
        TaskPriority.NORMAL, ['source_read'], [], result=TaskResult('t', 'success', 'Value: 7.', artifacts=[str(draft)]))
    group = TaskGroup('g', 'web:g', 'Report the selected value and unit', tasks=[task])
    class Provider:
        def create(self, **kw):
            return {'content': [{'type': 'text', 'text': json.dumps({'status': 'partial', 'answer': 'The unit is missing.',
                'evaluated_task_ids': ['t'], 'missing_outcomes': ['Report the unit.'],
                'assistance_plan':{'kind':'execution_improvement','task_ids':['t'],'tool_names':['source_read']}})}]}
    async def recover(*args):
        return TurnResult('reviewed', 'The unit source is unavailable.', artifacts=[], response_fulfilled=False)
    wire = []
    async def deliver(_conversation, value, **kw):
        wire.append((value, kw))
        return True
    registry = TaskRegistry(); await registry.add_group(group)
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, model_client=Provider(),
        outbox_path=tmp_path/'outbox.db', recover_outcome=recover)
    await aggregator._on_group_complete(GroupState('g', 'web:g', group.original_message), group)
    assert any(kw.get('is_artifact') for _, kw in wire)
    assert aggregator._outbox.get('g')['artifacts'] == [str(draft)]
    assert aggregator._outbox.get('g')['work_complete'] is True


@pytest.mark.asyncio
async def test_group_repair_without_owned_child_receipts_cannot_replay_writes():
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.task_queue import TaskGroup, TaskRecord, TaskResult, TaskStatus, TaskPriority
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    registry = ToolRegistry()
    registry.register(ToolSpec('change', 'Change resource', ToolRiskLevel.LOW, ToolSideEffectClass.ACCOUNT_WRITE,
        False, 2), lambda invocation: pytest.fail('A completed write must not be replayed without its receipts'))
    task = TaskRecord('t', 'g', 'web:g', 'web:g', 'Change', 'Apply change', TaskStatus.COMPLETE,
        TaskPriority.NORMAL, ['change'], [], result=TaskResult('t', 'success', 'Changed.'))
    group = TaskGroup('g', 'web:g', 'Change and report status', tasks=[task])
    group.planner_metadata['outcome_assistance_plan']={'kind':'execution_improvement','task_ids':['t'],'tool_names':['change']}
    runner = AgentOrchestrator(model_client=None)
    runner._group_execution_contexts = {'g': (registry, None, None)}
    assert await runner._recover_group_outcome(group, 'The status is missing.', []) is None


@pytest.mark.parametrize('wrapper', ['record', 'message', 'data'])
def test_complete_record_envelopes_are_evidence_without_flattening_source_fields(wrapper):
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    output = {wrapper: {'id': 'record-1', 'body': 'Requested value: 7.', 'status': 'failed'}}
    # A failed business transaction is a source fact, not a failed read.
    assert _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque_adapter', 'completed', output))
    assert not _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque_adapter', 'failed', output))
    assert not _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque_adapter', 'completed',
        {**output, 'verified': False}))


def test_temporary_completion_focus_retains_permitted_scope_discovery():
    from nullion.agent_orchestrator import _focus_tools_for_completion_recovery
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    registry = ToolRegistry()
    for name in ['source_read', 'request_tool_scope', 'unrelated_write']:
        registry.register(ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), lambda inv: None)
    focused = _focus_tools_for_completion_recovery(registry, ['source_read'])
    assert {row['name'] for row in focused.list_tool_definitions()} == {'source_read', 'request_tool_scope'}
    assert {row['name'] for row in _focus_tools_for_completion_recovery(focused, ['source_read']).list_tool_definitions()} == {'source_read', 'request_tool_scope'}


@pytest.mark.parametrize('output', [{'count': 7}, {'json': {'files': [{'id': 'a', 'name': 'Lease.pdf'}]}}])
def test_native_counts_and_structured_connector_json_are_evidence(output):
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    assert _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque', 'completed', output))
    assert not _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque', 'completed',
        {**output, 'verified': False}))


@pytest.mark.parametrize('output', [{'count': -1}, {'count': True}, {'json': {'items': []}},
    {'json': {'records': [{'id': 'a'}], 'verified': False}}, {'json': {'error': 'unavailable'}}])
def test_nested_connector_envelopes_cannot_rescue_invalid_evidence(output):
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    assert not _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque', 'completed', output))


@pytest.mark.parametrize('state', ['active', 'paused', '失敗', 'pending'])
def test_scalar_resource_state_is_not_a_malformed_runtime_envelope(state):
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    assert _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque', 'completed', {'state': state}))
    assert not _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque', 'completed',
        {'state': state, 'results': []}))
    assert not _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque', 'completed',
        {'state': state, 'verified': False}))


def test_background_receipts_are_bound_to_actual_completed_dispatch():
    from nullion.workflow_reasoning import deferred_execution_receipts
    output = {'action_receipt': {'type': 'action_receipt', 'object_id': 'job-a'},
        'deferred_execution': {'version': 1, 'phase': 'accepted', 'owner': 'opaque_worker',
            'handle': 'job-a', 'configuration': {'window_seconds': 90}}}
    assert deferred_execution_receipts([ToolResult('r', 'opaque', 'completed', output)])[0]['handle'] == 'job-a'
    assert not deferred_execution_receipts([ToolResult('r', 'opaque', 'failed', output)])
    assert not deferred_execution_receipts([ToolResult('r', 'opaque', 'completed',
        {**output, 'deferred_execution': {**output['deferred_execution'], 'handle': 'other-job'}})])


@pytest.mark.parametrize('output', [{'match_count': 0, 'matches': []}, {'match_count': 1, 'matches': [{'id': 'a'}]}])
def test_history_match_counts_prove_the_actual_lookup_scope(output):
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    assert _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque', 'completed', output))
    assert not _completion_review_evidence_result_is_positive(ToolResult('r', 'opaque', 'completed',
        {**output, 'verified': False}))


@pytest.mark.parametrize('value,expected', [('1. Create it', 'Create it'), ('٢) متابعة', 'متابعة'),
    ('1.\n1. Continue', 'Continue'), ('1.2% threshold', '1.2% threshold'), ('1 hour', '1 hour')])
def test_structured_choices_are_numbered_once_without_changing_labels(value, expected):
    from nullion.workflow_reasoning import option_label
    assert option_label(value) == expected


@pytest.mark.parametrize('effect,durable,expected', [('write', False, False), ('dangerous_exec', False, False),
    ('account_write', False, True), ('write', True, True), ('external_write', True, True)])
def test_transient_interactions_are_not_reused_as_committed_writes(effect, durable, expected):
    from nullion.tools import ToolSpec, ToolSideEffectClass, ToolRiskLevel
    from nullion.workflow_reasoning import durable_action_receipt
    spec = ToolSpec('opaque', 'Selected operation', ToolRiskLevel.LOW, ToolSideEffectClass(effect), False, 2,
        replay_safe_after_completion=durable)
    result = ToolResult('r', 'opaque', 'completed', {'action_receipt': {'type': 'action_receipt',
        'action': 'changed', 'object_type': 'opaque', 'summary': 'Changed selected state.'}})
    assert durable_action_receipt(spec, result) is expected


def test_execution_constraint_needs_proof_but_not_unrequested_narration():
    from nullion.agent_orchestrator import (_CompletionRequirementContractRow, _CompletionReviewDecision,
        _CompletionReviewMaterialRequirement, _forced_completion_review_is_grounded)
    row = _CompletionRequirementContractRow('r1', 'Prepare the selected source', 'constraint',
        ('opaque_read',), 'source', delivery_required=False)
    result = ToolResult('source-read', 'opaque_read', 'completed', {'value': 7})
    decision = _CompletionReviewDecision('complete', all_material_requirements_enumerated=True,
        material_requirements=(_CompletionReviewMaterialRequirement('r1', row.requirement, 'constraint',
            True, False, ('source-read',)),))
    assert _forced_completion_review_is_grounded(decision, tool_results=[result], requirement_contract=(row,))
    from dataclasses import replace
    assert not _forced_completion_review_is_grounded(decision, tool_results=[replace(result, status='failed')], requirement_contract=(row,))
    assert not _forced_completion_review_is_grounded(decision, tool_results=[result],
        requirement_contract=(replace(row, delivery_required=True),))


def test_action_preflight_receives_registered_argument_semantics():
    import json
    from nullion.workflow_reasoning import assess_action
    from nullion.tools import ToolInvocation, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    schema = {'type': 'object', 'properties': {'mode': {'type': 'string', 'enum': ['reuse', 'refresh'],
        'description': 'Reuse the live session; both modes read a fresh observation.', 'default': 'reuse'}}}
    spec = ToolSpec('opaque_watch', 'Accept selected observation', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2, input_schema=schema)
    class Provider:
        def create(self, **kw):
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            assert packet['proposed_action']['input_schema'] == schema
            assert 'ordinary declared default' in kw['system']
            return {'content': [{'type': 'text', 'text': '{"decision":"proceed","issues":[]}'}]}
    review = assess_action(model_client=Provider(), request='選択した条件を監視して。', context={},
        invocation=ToolInvocation('a', spec.name, 'p', {'mode': 'reuse'}), spec=spec, evidence=[], now='2026-10-26')
    assert review.decision == 'proceed'


@pytest.mark.parametrize('reply', ['Observed value: 7.', '観測された値は7です。'])
def test_required_receipt_preserves_synthesis_and_is_idempotent(reply):
    from nullion.response_sanitizer import sanitize_user_visible_reply
    result = ToolResult('a', 'opaque_accept', 'completed', {'action_receipt': {
        'type': 'action_receipt', 'delivery_required': True,
        'object_type': 'job', 'object_id': 'selected', 'summary': 'Configured the selected observation.'}})
    rendered = sanitize_user_visible_reply(user_message='指定された観測を確認して開始して。', reply=reply, tool_results=[result])
    assert reply in rendered and rendered.count('Configured the selected observation.') == 1
    assert sanitize_user_visible_reply(user_message='指定された観測を確認して開始して。', reply=rendered, tool_results=[result]) == rendered


@pytest.mark.parametrize('kind', ['fact', 'action', 'deliverable', 'reasoned_answer'])
def test_requested_outcomes_cannot_be_hidden_as_execution_only_constraints(kind):
    import json
    from nullion.agent_orchestrator import _parse_completion_requirement_contract
    response = {'content': [{'type': 'text', 'text': json.dumps({
        'all_material_requirements_enumerated': True, 'material_requirements': [{
            'requirement': 'Deliver the selected outcome', 'requirement_kind': kind,
            'delivery_required': False, 'evidence_subject_id': 'selected',
            'eligible_evidence_tool_names': ['opaque']}],
        'requested_artifact_extensions': ['.html'] if kind == 'deliverable' else []})}]}
    diagnostics = []
    assert _parse_completion_requirement_contract(response, registered_tool_names={'opaque'}, diagnostics=diagnostics) == ()
    assert diagnostics == ['invalid_delivery_requirement']


@pytest.mark.parametrize('name', ['browser_extract_items', 'opaque_collection', 'inventory_read'])
def test_action_evidence_preserves_contract_fields_around_large_collections(name):
    from nullion.workflow_reasoning import action_review_evidence
    output = {'items': [{'title': 'x' * 5000, 'url': '/selected'} for _ in range(100)],
        'item_count': 100, 'coverage': {'scope_found': True, 'candidate_count': 100,
            'returned_count': 100, 'truncated': False},
        'source_record_collections': [{'path': ['items'], 'change_identity_fields': ['url']} ]}
    packet = action_review_evidence([ToolResult('source', name, 'completed', output)])[0]
    assert packet['output']['item_count'] == 100
    assert packet['output']['coverage'] == output['coverage']
    assert packet['output']['source_record_collections'] == output['source_record_collections']
    assert ['items'] in packet['omitted_paths']
    assert len(packet['output']['items']) <= 3


def test_blocked_read_is_retired_after_later_scope_changes_but_mutation_is_not():
    from nullion.agent_orchestrator import _BlockedToolRegistry
    from nullion.tools import ToolSpec, ToolSideEffectClass, ToolRiskLevel, ToolInvocation
    class Delegate:
        def get_spec(self, name):
            return ToolSpec(name, name, ToolRiskLevel.LOW,
                ToolSideEffectClass.ACCOUNT_WRITE if name == 'effect' else ToolSideEffectClass.READ, False, 2)
        def apply_scope_request(self, invocation):
            return ToolResult('scope', 'request_tool_scope', 'completed', {
                'required_tool_names': ['read', 'effect', 'alternate'],
                'requested_required_tool_names': ['read', 'effect', 'alternate'],
                'available_tools': ['read', 'effect', 'alternate']}), self
    registry = _BlockedToolRegistry(Delegate(), {'read', 'effect'})
    result, widened = registry.apply_scope_request(ToolInvocation('scope', 'request_tool_scope', 'p', {}))
    assert result.output['required_tool_names'] == ['alternate']
    assert result.output['requested_required_tool_names'] == ['read', 'effect', 'alternate']
    assert result.output['retired_preparatory_read_tools'] == ['read']
    assert not widened.can_invoke_tool('read') and not widened.can_invoke_tool('effect')
    assert widened.can_invoke_tool('alternate')


def test_retired_read_does_not_excuse_a_missing_effect():
    from nullion.runtime_store import RuntimeStore
    from nullion.response_fulfillment_contract import evaluate_response_fulfillment
    results = [ToolResult('read', 'read', 'failed', {}), ToolResult('effect', 'effect', 'failed', {}),
        ToolResult('scope', 'request_tool_scope', 'completed', {
            'retired_preparatory_read_tools': ['read'], 'required_tool_names': ['effect', 'alternate']}),
        ToolResult('replacement', 'alternate', 'completed', {'value': 7})]
    decision = evaluate_response_fulfillment(store=RuntimeStore(), conversation_id='web:p', user_message='選択した変更を実行して。',
        reply='Observed value: 7.', tool_results=results, required_tool_names=('read', 'effect', 'alternate'))
    assert not decision.satisfied and 'tool:effect' in decision.missing_requirements
    assert 'tool:read' not in decision.missing_requirements



def test_argument_repairs_group_by_typed_failure_not_wording_or_changed_arguments():
    from nullion.agent_orchestrator import _tool_failure_fingerprint
    def failed(code, detail):
        return ToolResult('bad', 'opaque_setup', 'failed', {'reason':'invalid_tool_arguments',
            'validation':{'code':code}, 'recovery':{'kind':'correct_arguments','retry_tool_name':'opaque_setup',
                'argument_constraints':{'required':['selected']}}}, detail)
    one=_tool_failure_fingerprint(result=failed('wrong_index','Incorrect index'), invocation_signature='arguments-a')
    same=_tool_failure_fingerprint(result=failed('wrong_index','添字が違います'), invocation_signature='arguments-b')
    next_step=_tool_failure_fingerprint(result=failed('unprepared_observation','Prepare the observation'), invocation_signature='arguments-c')
    assert one==same and one!=next_step



@pytest.mark.parametrize('name,effect', [('opaque_read','read'),('opaque_write','write'),('opaque_account','account_write')])
def test_missing_arguments_advertise_generic_repair_before_any_handler(name, effect):
    from nullion.tools import ToolRegistry, ToolSpec, ToolInvocation, ToolRiskLevel, ToolSideEffectClass
    from nullion.tool_repair_contract import tool_repair_kind
    calls=[];registry=ToolRegistry()
    registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass(effect),False,2,
        input_schema={'type':'object','required':['selected'],'properties':{'selected':{'type':'string'}}}),
        lambda inv:calls.append(inv))
    result=registry.invoke(ToolInvocation('bad',name,'p',{}))
    assert not calls and result.output['executed'] is False
    assert tool_repair_kind(result)=='correct_arguments'
    assert result.output['recovery']['argument_constraints']['properties']['selected']=={'type':'string'}
