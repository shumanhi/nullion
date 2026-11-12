from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import TurnResult
from nullion.completion_pipeline import publish_completion, published_completion, recovery_closed
from nullion.tools import ToolResult


@pytest.mark.parametrize('prompt', ['確認してください。', 'Comprueba los datos.', 'تحقق من البيانات.'])
@pytest.mark.parametrize('fulfilled', [True, False, None])
def test_terminal_ownership_is_not_a_completion_or_delivery_certificate(prompt, fulfilled):
    result = TurnResult('turn', 'Observed value: 7.', response_fulfilled=fulfilled,
        tool_results=[ToolResult('read', 'sensor_read', 'completed', {'value': 7})])
    assert publish_completion(result, prompt, reason='producer_finalized')
    assert recovery_closed(result, prompt)
    assert result.response_fulfilled is fulfilled
    assert not hasattr(result, 'delivery_succeeded')
    assert not recovery_closed(result, prompt + '?')


@pytest.mark.parametrize('prompt', ['選択してください。', 'Elige el destino.', 'اختر الوجهة.'])
def test_typed_input_requirement_is_terminal_until_the_user_replies(prompt):
    result = TurnResult('turn', '1. 甲\n2. 乙', response_fulfilled=False,
        completion_failure_reply='1. 甲\n2. 乙', response_presentation={'kind': 'clarification', 'needs_input': True},
        tool_results=[ToolResult('proposal', 'opaque_write', 'failed', {'reason': 'request_plan_conflict',
            'review_decision': 'clarify', 'executed': False})])
    assert publish_completion(result, prompt)
    assert recovery_closed(result, prompt, pending_tool_names=('opaque_write',))
    assert result.response_fulfilled is False
    result.response_presentation['needs_input'] = False
    assert not recovery_closed(result, prompt)


@pytest.mark.parametrize('change', ['draft', 'evidence', 'fulfillment', 'approval', 'selection', 'bytes'])
def test_a_changed_result_cannot_keep_a_stale_terminal_decision(tmp_path, change):
    path = tmp_path / 'report.txt'; path.write_text('value: 7')
    result = TurnResult('turn', 'Observed value: 7.', artifacts=[str(path)],
        response_fulfilled=False, response_presentation={'recovery_exhausted': True},
        tool_results=[ToolResult('read', 'sensor_read', 'completed', {'value': 7})])
    publish_completion(result, 'request')
    assert recovery_closed(result, 'request')
    if change == 'draft': result.final_text = 'Different answer'
    elif change == 'evidence': result.tool_results[0].output['value'] = 8
    elif change == 'fulfillment': result.response_fulfilled = True
    elif change == 'approval': result.suspended_for_approval = True
    elif change == 'selection': result.artifacts = []
    else:
        import os
        timestamp = path.stat().st_mtime_ns
        path.write_text('value: 8')
        os.utime(path, ns=(timestamp, timestamp))
    assert published_completion(result, 'request') is None
    # Legacy flags remain, but cannot authorize using a stale checkpoint.
    assert not recovery_closed(result, 'request')


def test_transport_annotations_do_not_reopen_finished_work():
    result = TurnResult('turn', '7', tool_results=[ToolResult('r', 'read', 'completed', {'value': 7})])
    publish_completion(result, 'request', reason='producer_finalized')
    result.tool_results[0].output['artifact_delivery_manifest'] = {'paths': []}
    assert recovery_closed(result, 'request')


def test_a_completed_lookup_does_not_discharge_a_registered_pending_action():
    result = TurnResult('turn', 'Found it.', response_presentation={'outcome_review_role': 'helper'},
        tool_results=[ToolResult('r', 'read', 'completed', {'value': 7})])
    publish_completion(result, 'request')
    assert recovery_closed(result, 'request')
    assert not recovery_closed(result, 'request', pending_tool_names=('update',))


def test_unfinished_execution_does_not_get_a_checkpoint_from_a_usable_draft():
    result = TurnResult('turn', 'Found it.', tool_results=[ToolResult('r', 'read', 'completed', {'value': 7})])
    assert publish_completion(result, 'request') is None
    assert not recovery_closed(result, 'request')


def test_checkpoint_is_runtime_owned_not_a_tool_output_or_presentation_flag():
    result = TurnResult('turn', '7', tool_results=[ToolResult('r', 'read', 'completed', {
        'completion_checkpoint': {'request': 'request', 'reason': 'producer_finalized'}})])
    assert not recovery_closed(result, 'request')


def test_completion_checkpoint_is_owned_by_the_original_user_and_conversation():
    result = TurnResult('turn', '7')
    publish_completion(result, 'request', reason='producer_finalized', conversation_id='c', principal_id='p')
    assert published_completion(result, 'request', conversation_id='c', principal_id='p')
    assert not published_completion(result, 'request', conversation_id='other', principal_id='p')
    assert not published_completion(result, 'request', conversation_id='c', principal_id='other')


def test_scheduled_artifact_descriptors_bind_actual_bytes_without_becoming_paths(tmp_path):
    path = tmp_path / 'report.txt'; path.write_text('7')
    result = {'text': 'Report attached.', 'artifacts': [{'path': str(path), 'name': 'Report'},
        {'artifact_id': 'remote-receipt'}], 'response_fulfilled': None}
    publish_completion(result, 'request', reason='scheduled_result_finalized')
    assert published_completion(result, 'request')
    result['artifacts'][1]['artifact_id'] = 'another'
    assert not published_completion(result, 'request')
    publish_completion(result, 'request', reason='scheduled_result_finalized')
    path.write_text('8')
    assert not published_completion(result, 'request')


def test_recovery_handoff_preserves_spent_window_without_expanding_tool_authority():
    from nullion.completion_pipeline import capture_recovery, restore_recovery
    state = {'conversation_id': 'c', 'principal_id': 'p', 'completion_review_count': 2,
        'completion_review_progress_fingerprints': ['proof'], 'completion_review_recovery_tool_names': ['read']}
    checkpoint = capture_recovery(state, 'request')
    update = restore_recovery(checkpoint, state, 'request')
    assert update == {**{key: state[key] for key in state if key.startswith('completion_review_')},
        'completion_composition_count': 0}
    assert not restore_recovery(checkpoint, {**state, 'principal_id': 'other'}, 'request')
    assert not restore_recovery(checkpoint, state, 'changed request')
    assert 'max_iterations' not in update and 'tool_registry' not in update


def test_saved_history_cannot_restart_a_published_partial_answer():
    from nullion.chat_operator import _run_chat_turn_saved_history_retry
    result = TurnResult('turn', 'Useful partial answer.', response_fulfilled=False)
    publish_completion(result, 'request', reason='producer_finalized')
    def forbidden(*args):
        raise AssertionError('Published work was restarted')
    registry = object()
    actual, actual_registry, retried = _run_chat_turn_saved_history_retry(initial_result=result,
        automatic_history_context=None, allow_saved_history_retry=True, build_history_context=forbidden,
        base_history=[], active_tool_registry=registry, base_tool_registry=registry,
        turn_tool_evidence=SimpleNamespace(), run_turn=forbidden)
    assert actual is result and actual_registry is registry and not retried


def test_graph_has_one_terminal_review_owner():
    """Architecture guard complements the real runner and delivery contracts."""
    import ast
    from pathlib import Path
    import nullion.agent_orchestrator as agent
    tree = ast.parse(Path(agent.__file__).read_text())
    owners = [node.name for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
            and call.func.id == '_review_risky_agent_completion' for call in ast.walk(node))]
    assert owners == ['_complete_agent_turn']


def test_content_preparation_cannot_change_source_receipts_or_action_authority():
    from nullion.completion_pipeline import CompletionDraft, prepare_completion
    row = ToolResult('read', 'sensor_read', 'completed', {'value': 7})
    def altered(*, state, text, review):
        state['tool_results'][0].output['value'] = 99
        return CompletionDraft('99', (), tuple(state['tool_results']))
    state = {'tool_results': [row], 'completion_preparer': altered}
    with pytest.raises(ValueError, match='execution receipts'):
        prepare_completion(state, '7')
    assert row.output == {'value': 7}
    state['completion_preparer'] = lambda **kwargs: CompletionDraft('7', (), (row,),
        presentation={'needs_input': True, 'source_selection_required': True, 'report_layout': 'compact'})
    actual, text, changed = prepare_completion(state, '7')
    assert changed and text == '7'
    assert actual['response_presentation'] == {'report_layout': 'compact'}


def test_content_preparation_cannot_append_a_successful_account_action():
    from nullion.completion_pipeline import CompletionDraft, prepare_completion
    action = ToolResult('fake', 'calendar_create', 'completed', {'action_receipt': {'id': 'new-event'}})
    state = {'completion_preparer': lambda **kwargs: CompletionDraft('Done', (), (action,))}
    with pytest.raises(ValueError, match='artifact receipts'):
        prepare_completion(state, '')


@pytest.mark.parametrize('budget,used,expected', [
    (None, 20, None), ({'limit': 10, 'baseline': 17}, 20, 7),
    ({'limit': 10, 'baseline': 0}, 20, 0), ({'limit': 4, 'baseline': 20}, 20, 4),
    ({'limit': 4, 'baseline': 21}, 20, 0), ({'limit': -1, 'baseline': 0}, 0, 0),
    ({'limit': True, 'baseline': 0}, 0, 0), ({}, 0, 0),
])
def test_execution_allowance_is_shared_across_attempts_and_approved_resume(budget, used, expected):
    from nullion.completion_pipeline import remaining_execution_calls
    state = {'work_usage': {'model_calls': used}, 'tool_flow_context': {}}
    if budget is not None:
        state['tool_flow_context']['execution_model_call_budget'] = budget
    assert remaining_execution_calls(state) == expected


def test_scheduler_has_no_independent_review_or_continuation_controller():
    import ast
    import inspect
    from nullion.cron_delivery import _run_single_agent_cron_turn, _ScheduledCompletionPreparer
    tree = ast.parse(inspect.getsource(_run_single_agent_cron_turn))
    assert not [call for call in ast.walk(tree) if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name) and call.func.id == '_review_risky_agent_completion']
    # The only resume dispatch belongs to an explicitly approved saved execution.
    resumes = [node for node in ast.walk(tree) if isinstance(node, ast.If)
        and isinstance(node.test, ast.Name) and node.test.id == 'saved_execution'
        and any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
            and call.func.attr == 'resume_turn' for call in ast.walk(node))]
    assert len(resumes) == 1
    strategy = ast.parse(inspect.getsource(_ScheduledCompletionPreparer))
    assert not [call for call in ast.walk(strategy) if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute) and call.func.attr in {'run_turn', 'resume_turn', 'execute'}]


@pytest.mark.parametrize('change', ['request', 'draft', 'source', 'file'])
def test_prepared_answer_crosses_transports_only_with_current_proof(tmp_path, change):
    from nullion.execution_outcome import prepare_answer, owned_prepared_answer
    path = tmp_path / 'report.txt'
    path.write_text('value 7')
    row = ToolResult('read', 'sensor_read', 'completed', {'value': 7})
    result = TurnResult('turn', 'Observed value: 7.', tool_results=[row], artifacts=[str(path)])
    result.prepared_answer = prepare_answer('確認', result.final_text, result.tool_results, result.artifacts)
    assert owned_prepared_answer(result, '確認') is result.prepared_answer
    request = '確認'
    if change == 'request': request = '別の依頼'
    elif change == 'draft': result.final_text = '8'
    elif change == 'source': row.output['value'] = 8
    else: path.write_text('value 8')
    assert owned_prepared_answer(result, request) is None


def test_scope_retry_refreshes_latest_work_without_nesting_or_resetting_recovery():
    from nullion.agent_orchestrator import _CompletionReviewHandoff
    from nullion.chat_operator import _registry_with_request_retry_evidence
    from nullion.completion_pipeline import capture_recovery
    registry = SimpleNamespace()
    request = '確認'
    row = ToolResult('first', 'sensor_read', 'completed', {'value': 7})
    first = TurnResult('first-turn', '7', tool_results=[row])
    kwargs = dict(conversation_id='web:1', principal_id='web:1', user_message=request)
    wrapped = _registry_with_request_retry_evidence(registry, result=first, **kwargs)
    state = {**kwargs, 'completion_review_count': 2, 'completion_composition_count': 1,
        'completion_review_progress_fingerprints': ['first-proof']}
    checkpoint = capture_recovery(state, request)
    second = TurnResult('second-turn', '7 and 8', tool_results=[row,
        ToolResult('second', 'sensor_read', 'completed', {'value': 8})],
        completion_review_handoff=_CompletionReviewHandoff({}, None, None, checkpoint))
    refreshed = _registry_with_request_retry_evidence(wrapped, result=second, **kwargs)
    assert refreshed._registry is registry
    assert len(refreshed.turn_continuation_evidence.tool_results) == 2
    assert refreshed.turn_continuation_evidence.completion_review_handoff.recovery_checkpoint == checkpoint
    assert _registry_with_request_retry_evidence(refreshed, result=second, **kwargs) is refreshed


@pytest.mark.parametrize('change', ['read', 'failed_read', 'write', 'denied', 'changed_source', 'duplicate_id', 'file'])
def test_retained_answer_is_only_valid_across_unchanged_or_new_read_evidence(tmp_path, change):
    from nullion.completion_pipeline import retain_completion_answer, retained_completion_answer
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    registry = ToolRegistry()
    registry.register(ToolSpec('sensor_read', 'Read selected records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), lambda inv: None)
    path = tmp_path / 'report.txt'
    path.write_text('Observed 7')
    row = ToolResult('original', 'sensor_read', 'completed', {'value': 7})
    state = {'tool_registry': registry, 'tool_results': [row], 'artifacts': [str(path)]}
    retained = retain_completion_answer(state, '確認', 'Observed 7. No changes made.')
    rows = [row]
    if change == 'read': rows.append(ToolResult('new', 'sensor_read', 'completed', {'value': 8}))
    elif change == 'failed_read': rows.append(ToolResult('new', 'sensor_read', 'failed', {}, 'Unavailable'))
    elif change == 'write': rows.append(ToolResult('new', 'record_update', 'completed', {'action_receipt': {'id': 'changed'}}))
    elif change == 'denied': rows.append(ToolResult('new', 'sensor_read', 'denied', {}))
    elif change == 'changed_source': row.output['value'] = 8
    elif change == 'duplicate_id': rows.append(ToolResult('original', 'sensor_read', 'completed', {'value': 8}))
    else: path.write_text('Observed 8')
    actual = retained_completion_answer(retained, '確認', rows, [str(path)])
    assert (actual is not None) is (change in {'read', 'failed_read'})
