import json
from types import SimpleNamespace

from nullion.agent_orchestrator import (
    _CompletionReviewDecision, _completion_review_handoff_for_state,
    _completion_review_request_context, _extract_completion_requirement_contract,
)
from nullion.tools import ToolRegistry, ToolResult


def test_review_handoff_excludes_execution_objects_and_does_not_certify_plain_turns():
    assert _completion_review_handoff_for_state({'tool_results': []}) is None
    review = _CompletionReviewDecision('retry', ('未完了の項目を確認。',))
    state = {'tool_results': [ToolResult('read', 'source_read', 'completed', {'value': 37})],
        'orchestrator': object(), 'runtime_store': object(),
        'completion_requirement_contract_cache': {'key': 'request-key', 'rows': ()}}
    handoff = _completion_review_handoff_for_state(state, review=review)
    assert handoff.latest_review is review
    assert not hasattr(handoff, 'orchestrator') and not hasattr(handoff, 'runtime_store')
    handoff.requirement_contract_cache.clear()
    assert state['completion_requirement_contract_cache']['key'] == 'request-key'


def test_saved_delivery_history_is_explicit_in_every_review_context():
    history = {'job_id': 'job-1', 'conversation_id': 'web:one', 'receipt_count': 0,
        'no_prior_delivered_messages': True, 'receipts': []}
    execution = {'phase': 'execute_saved_task', 'saved_task_id': 'job-1', 'prior_delivery_history': history}
    context = _completion_review_request_context({'tool_flow_context': {
        'scheduled_task_run': True, 'scheduled_execution_context': execution}})
    assert context['prior_delivery_history'] == history
    assert context['scheduled_execution_context']['prior_delivery_history'] == history
    assert 'prior_delivery_history' not in _completion_review_request_context({'tool_flow_context': {
        'scheduled_task_run': False, 'scheduled_execution_context': execution}})






def test_only_typed_saved_execution_requires_review_without_tool_calls():
    from nullion.agent_orchestrator import _completion_review_required
    flow = {'scheduled_task_run':True, 'scheduled_execution_context':{
        'phase':'execute_saved_task','saved_task_id':'selected-job'}}
    state = {'tool_flow_context':flow, 'completion_requirement_contract_cache':{'rows':()}}
    assert not _completion_review_required(state, tool_results=[])
    assert _completion_review_handoff_for_state(state) is not None
    assert not _completion_review_required({'tool_flow_context':{'scheduled_task_run':True}}, tool_results=[])
    assert not _completion_review_required({'tool_flow_context':{}}, tool_results=[])


def test_saved_source_context_survives_but_a_new_pending_choice_takes_precedence():
    from nullion.agent_orchestrator import _completion_review_scope_contract
    from nullion.cron_execution_tools import CronExecutionToolRegistry
    selected = {'source_evidence_verified':True, 'connector_source_user_requested':True,
        'source_selection_required':False,'active_connector_providers':[{'provider_id':'selected'}]}
    registry = CronExecutionToolRegistry(ToolRegistry(), source_scope_contract=selected)
    scope = _completion_review_scope_contract(registry, [])
    assert scope['source_evidence_verified'] and scope['connector_source_user_requested']
    scope = _completion_review_scope_contract(registry, [ToolResult('choose','request_tool_scope','completed',
        {'source_selection_required':True,'source_evidence_verified':False,'connector_source_user_requested':False})])
    assert scope['source_selection_required'] and not scope['source_evidence_verified']
    assert not scope['connector_source_user_requested']


def test_source_selection_handoff_keeps_binding_context_without_shared_mutation():
    state={'tool_results':[ToolResult('read','source_read','completed',{'value':37})],
        'source_answer_checkpoint':{'presentation':{'intro':'Selected facts','points':[]},'tool_result_count':1}}
    handoff=_completion_review_handoff_for_state(state)
    assert handoff.source_answer_checkpoint==state['source_answer_checkpoint']
    handoff.source_answer_checkpoint['presentation']['intro']='Changed copy'
    assert state['source_answer_checkpoint']['presentation']['intro']=='Selected facts'
