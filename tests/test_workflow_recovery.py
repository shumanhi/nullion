"""Shared recovery consumes typed evidence, never user language or tool prose."""
from types import SimpleNamespace

import pytest

from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
from nullion.workflow_recovery import (
    WorkflowCheckpoint, WorkflowCheckpointStore, observe_state, persist_state,
    recovered_failure_ids,
)
from nullion.execution_outcome import build_execution_outcome, ExecutionStatus


def registry():
    value = ToolRegistry()
    for name in ('read', 'inspect', 'other'):
        value.register(ToolSpec(name, 'Read state', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2), lambda invocation: None)
    return value


def failure(ident='f', *, status='failed'):
    return ToolResult(ident, 'read', status, {'reason': 'tool_precondition_unmet',
        'recovery': {'kind': 'repair_precondition', 'retry_tool_name': 'read',
            'required_tool_names': ['inspect']}}, 'Unavailable source')


def test_only_same_call_success_or_verified_outcome_closes_failure():
    checkpoint = WorkflowCheckpoint('w', 'c', 'g')
    failed = failure()
    unrelated = ToolResult('other', 'other', 'completed', {})
    checkpoint.observe([failed, unrelated], {'f': {'arguments': {'id': 3}}}, registry())
    assert not failed.recovered_by and checkpoint.open_issues
    success = ToolResult('s', 'read', 'completed', {'value': 9})
    checkpoint.observe([failed, unrelated, success], {'s': {'arguments': {'id': 4}}}, registry())
    assert not failed.recovered_by
    checkpoint.finish([failed, success], verified=True)
    assert failed.recovered_by == ('s',)
    outcome = build_execution_outcome(tool_results=[failed, success], user_visible_message='9', delivery_status='sent')
    assert outcome.status == ExecutionStatus.SUCCEEDED and outcome.delivery_succeeded
    assert 'tool_recovered:read' in outcome.evidence


def test_identical_retry_resolves_without_another_reviewer():
    checkpoint = WorkflowCheckpoint('w', 'c', 'g')
    failed, success = failure(), ToolResult('s', 'read', 'completed', {'value': 9})
    checkpoint.observe([failed, success], {key: {'arguments': {'id': 3}} for key in ('f', 's')}, registry())
    assert recovered_failure_ids([failed, success]) == {'f'}
    assert not checkpoint.open_issues


def test_approved_invocation_transition_reconciles_persisted_denial():
    checkpoint = WorkflowCheckpoint('w', 'c', 'g')
    denied = failure(status='denied')
    checkpoint.observe([denied], {'f': {'arguments': {'id': 3}}}, registry())
    completed = ToolResult('f', 'read', 'completed', {'value': 9})
    checkpoint.observe([completed], {'f': {'arguments': {'id': 3}}}, registry())
    assert checkpoint.observations['f']['status'] == 'completed'
    assert not checkpoint.open_issues


def test_verified_outcome_does_not_erase_denial_or_missing_delivery():
    checkpoint = WorkflowCheckpoint('w', 'c', 'g')
    denied = failure(status='denied')
    denied.output.update(reason='source_consent_required', terminal_user_action_required=True)
    success = ToolResult('s', 'other', 'completed', {})
    checkpoint.observe([denied, success], {}, registry())
    checkpoint.finish([denied, success], verified=True)
    assert not denied.recovered_by and checkpoint.open_issues
    assert build_execution_outcome(tool_results=[denied, success], user_visible_message='Some data').status != ExecutionStatus.SUCCEEDED
    assert not build_execution_outcome(tool_results=[success]).delivery_succeeded


def test_receipts_require_runtime_top_level_and_present_success():
    failed = failure()
    failed.output['recovered_by'] = ['s']
    success = ToolResult('s', 'read', 'completed', {})
    assert not recovered_failure_ids([failed, success])
    failed.recovered_by = ('missing',)
    assert not recovered_failure_ids([failed, success])


def test_budget_no_progress_and_identity_survive_restore(tmp_path):
    reg = registry()
    checkpoint = WorkflowCheckpoint('w', 'c', 'g')
    checkpoint.observe([failure()], {}, reg)
    assert checkpoint.recovery_context(reg)
    assert checkpoint.recovery_context(reg) is None
    store = WorkflowCheckpointStore(tmp_path / 'runtime.db')
    store.save(checkpoint)
    saved = store.load('w', 'c', 'g')
    resumed = WorkflowCheckpoint.restore(saved, workflow_id='w', conversation_id='c', goal_digest='g')
    resumed.observe([failure('f2')], {}, reg)
    assert resumed.recovery_context(reg)
    resumed.observe([failure('f3')], {}, reg)
    assert resumed.recovery_context(reg) is None
    store.save(resumed)
    store.save(checkpoint)  # stale writer cannot replenish the budget
    assert store.load('w', 'c', 'g')['recovery_rounds'] == 2
    assert store.load('w', 'another-chat', 'g') is None
    assert not WorkflowCheckpoint.restore(saved, workflow_id='w', conversation_id='another-chat', goal_digest='g').observations
    assert not WorkflowCheckpoint.restore({**saved, 'issues': []}, workflow_id='w', conversation_id='c', goal_digest='g').observations


def test_plain_turn_has_no_checkpoint_io_and_lock_does_not_restart_actions(tmp_path, monkeypatch):
    state = {'principal_id': 'p', 'conversation_id': 'c', 'user_message': 'こんにちは',
        'cleanup_scope': 'w', 'tool_results': [], 'tool_registry': registry(),
        'runtime_store': SimpleNamespace(_memory_checkpoint_path=tmp_path / 'runtime.db')}
    assert observe_state(state) is None
    persist_state(state)
    assert not (tmp_path / 'runtime.db').exists()
    state['tool_results'] = [failure()]
    assert observe_state(state) is not None
    import nullion.workflow_recovery as module
    import sqlite3
    monkeypatch.setattr(module, '_checkpoint_store', lambda path: (_ for _ in ()).throw(sqlite3.OperationalError('database is locked')))
    persist_state(state)
    assert state['workflow_checkpoint'].open_issues


def test_replay_rejects_new_write_tools_and_invalid_arguments():
    from nullion.observation_replay import validated_observation_setup
    reg = registry()
    reg.get_spec('read').observation_setup_tools = ('inspect',)
    reg.get_spec('inspect').input_schema = {'type': 'object', 'required': ['url']}
    output = {'observation_setup': [{'tool_name': 'inspect', 'arguments': {'url': 'https://example.org'}}]}
    assert validated_observation_setup(reg, 'read', output) == output['observation_setup']
    reg.get_spec('inspect').side_effect_class = ToolSideEffectClass.WRITE
    with pytest.raises(ValueError): validated_observation_setup(reg, 'read', output)
    reg.get_spec('inspect').side_effect_class = ToolSideEffectClass.READ
    output['observation_setup'][0]['arguments'] = {}
    with pytest.raises(ValueError): validated_observation_setup(reg, 'read', output)


def test_inline_read_preconditions_are_not_replayed_before_the_source_can_repair_them(tmp_path):
    from nullion.observation_replay import replay_observation_setup
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation
    reg=registry();spec=reg.get_spec('read')
    spec.observation_setup_tools=('inspect',)
    spec.observation_inline_setup_tools=('inspect',)
    runtime=bootstrap_persistent_runtime(tmp_path/'inline.db')
    result=replay_observation_setup(reg,ToolInvocation('read','read','p',{}),
        [{'tool_name':'inspect','arguments':{}}],runtime.store)
    assert result is None
    reg.get_spec('inspect').side_effect_class=ToolSideEffectClass.WRITE
    denied=replay_observation_setup(reg,ToolInvocation('read','read','p',{}),
        [{'tool_name':'inspect','arguments':{}}],runtime.store)
    assert denied.status=='failed' and denied.output['reason']=='observation_setup_contract'


def test_approval_receipt_serialization_keeps_budget_and_terminal_failures():
    from nullion.agent_orchestrator import _serialize_pending_tool_calls
    checkpoint = WorkflowCheckpoint('w', 'c', 'g')
    checkpoint.observe([failure()], {}, registry())
    checkpoint.recovery_context(registry())
    rows = _serialize_pending_tool_calls([failure()], workflow_checkpoint=checkpoint.to_dict())
    assert rows[0]['status'] == 'failed'
    restored = WorkflowCheckpoint.restore(rows[0]['workflow_checkpoint'], workflow_id='w', conversation_id='c', goal_digest='g')
    assert restored.recovery_rounds == 1 and restored.recovery_context(registry()) is None


def test_unregistered_source_payload_cannot_add_replay_actions():
    from nullion.observation_replay import validated_observation_setup
    reg = registry()
    assert validated_observation_setup(reg, 'read', {
        'observation_setup': [{'tool_name': 'send_email', 'arguments': {}}]}) == []


def test_cancelled_observation_repair_does_not_inspect_or_call_model():
    from nullion.observation_recovery import repair_observation
    from nullion.tools import ToolInvocation
    class Provider:
        calls = 0
        def create(self, **kw):
            self.calls += 1
            raise AssertionError('Cancelled work reached the provider')
    provider = Provider()
    failed = failure()
    result, proposal = repair_observation(registry(), ToolInvocation('f', 'read', 'p', {}), failed,
        runtime_store=None, model_client=provider, condition={}, cancellation_checker=lambda: True)
    assert result is failed and proposal is None and provider.calls == 0


def test_oversized_repair_evidence_never_enters_model_context():
    from nullion.observation_recovery import repair_observation
    from nullion.tools import ToolInvocation
    reg = registry()
    spec = reg.get_spec('read')
    spec.observation_repair_parameters = ('selector',)
    spec.input_schema = {'type': 'object', 'properties': {'selector': {'type': 'string'}}}
    from nullion.runtime_store import RuntimeStore
    inspections = []
    def inspect(inv):
        inspections.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'scope': 'current'})
    reg.unregister('inspect')
    reg.register(ToolSpec('inspect', 'Read source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), inspect)
    failed = failure()
    failed.output['unbounded_evidence'] = 'x' * 25000
    class Provider:
        calls = 0
        def create(self, **kw):
            self.calls += 1
            raise AssertionError('Oversized evidence reached the provider')
    provider = Provider()
    result, proposal = repair_observation(reg, ToolInvocation('f', 'read', 'p', {'selector': '#old'}), failed,
        runtime_store=RuntimeStore(), model_client=provider, condition={})
    assert result is failed and proposal is None
    assert len(inspections) == 1 and provider.calls == 0


def test_context_bound_alias_does_not_repeat_an_already_completed_prerequisite():
    from nullion.observation_replay import replay_observation_setup
    from nullion.tools import ToolInvocation
    reg = registry()
    reg.get_spec('read').observation_setup_tools = ('inspect',)
    reg.get_spec('inspect').context_argument_bindings = (('handle', 'worker_handle'),)
    setup = [{'tool_name': 'inspect', 'arguments': {'url': 'https://example.org'}}]
    completed = [{'tool_name': 'inspect', 'arguments': {'url': 'https://example.org', 'handle': 'creator'},
                  'flow_context': {'worker_handle': 'worker'}}]
    # The handler returns no result and would fail if accidentally invoked.
    result = replay_observation_setup(reg, ToolInvocation('r', 'read', 'p', {}, flow_context={'worker_handle': 'worker'}),
        setup, None, completed_steps=completed)
    assert result is None


def test_completed_prerequisite_on_another_target_is_replayed():
    from nullion.observation_replay import replay_observation_setup
    from nullion.tools import ToolInvocation
    from nullion.runtime_store import RuntimeStore
    reg = registry()
    reg.get_spec('read').observation_setup_tools = ('inspect',)
    reg.unregister('inspect')
    calls = []
    def inspect(inv):
        calls.append(inv.flow_context['browser_session_id'])
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {})
    reg.register(ToolSpec('inspect', 'Read state', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2,
                         context_argument_bindings=(('handle', 'browser_session_id'),)), inspect)
    setup = [{'tool_name': 'inspect', 'arguments': {'url': 'https://example.org'}}]
    completed = [{**setup[0], 'flow_context': {'browser_session_id': 'page-a'}}]
    invocation = ToolInvocation('r', 'read', 'p', {}, flow_context={'browser_session_id': 'page-b'})
    store = RuntimeStore()
    assert replay_observation_setup(reg, invocation, setup, store, completed_steps=completed) is None
    assert calls == ['page-b']
    assert replay_observation_setup(reg, invocation, setup, store, completed_steps=completed) is None
    assert calls == ['page-b'], 'The same target must reuse its verified setup within one check'


def test_source_session_keys_group_equivalent_recipes_and_preserve_source_arguments():
    from nullion.observation_replay import observation_source_session_key
    reg = registry()
    reg.get_spec('inspect').context_argument_bindings = (('handle', 'worker_handle'),)
    setup = [{'tool_name': 'inspect', 'arguments': {'source': '資料α'}}]
    explicit = {'tool_name': 'inspect', 'arguments': {'source': '資料α', 'handle': 'creator'}}
    probe = [explicit, {'tool_name': 'read', 'arguments': {'field': 'one'}},
             {'tool_name': 'read', 'arguments': {'field': 'two'}},
             {'tool_name': 'read', 'arguments': {'field': 'three'}}]
    contexts = [{}, {'observation_setup': setup}, {'observation_setup': setup},
                {'observation_setup': [{'tool_name': 'inspect', 'arguments': {'source': '資料β'}}]}]
    keys = [observation_source_session_key(reg, probe, contexts, i) for i in range(len(probe))]
    assert keys[0] == keys[1] == keys[2] and keys[3] != keys[0]
    assert observation_source_session_key(reg, probe, [{}] * 4, 1) is None


def test_normalization_preserves_runtime_recovery_receipts():
    from nullion.tools import normalize_tool_result
    failed = failure()
    failed.recovered_by = ('s',)
    normalized = normalize_tool_result(failed)
    assert normalized.recovered_by == ('s',)
    assert recovered_failure_ids([normalized, ToolResult('s', 'read', 'completed', {})]) == {'f'}
