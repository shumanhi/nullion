"""Output-stage transitions use contracts and native metadata, not reply words."""
from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import (
    _artifact_tool_delivery_nudge_for_missing_requirements,
    _maybe_widen_scope_for_missing_artifacts,
)
from nullion.recovery_contract import RequestRecoveryContract
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision


def test_review_recovery_keeps_execution_ownership_without_replanning(monkeypatch):
    from nullion import agent_orchestrator as agent
    monkeypatch.setattr(agent, '_extract_completion_requirement_contract',
        lambda *a, **kw: pytest.fail('An intermediate repair must not replan presentation'))
    assert not agent._partial_source_report_delivery_ready({
        'completion_review_count': 1, 'source_answer_checkpoint': {'tool_result_count': 1},
        'runtime_store': object(), 'orchestrator': object()}, [])


def registry(extension, prompt):
    base = ToolRegistry()
    for name, effect, formats, tags in [
        ('r7', ToolSideEffectClass.READ, (), ('web',)),
        ('p9', ToolSideEffectClass.WRITE, (extension,), ()),
        ('a8', ToolSideEffectClass.ACCOUNT_WRITE, (extension,), ()),
        ('c3', ToolSideEffectClass.READ, (extension,), ()),
    ]:
        base.register(ToolSpec(name, name, ToolRiskLevel.LOW, effect, False, 5,
            produced_artifact_extensions=formats, capability_tags=tags), lambda inv: pytest.fail('Discovery must not execute'))
    return ScopedTurnToolRegistry(base, evidence=TurnToolEvidence(current_user_message=prompt),
        tool_scope_decision=TurnToolScopeDecision(valid=True, requested_tool_names=('r7',)))


@pytest.mark.parametrize('extension', ['.pdf', '.png', '.html', '.xlsx', '.pptx', '.docx'])
@pytest.mark.parametrize('prompt', ['1', '変更して続けてください。', 'Sí, con el formato elegido.'])
def test_validated_output_stage_exposes_native_producer_without_account_actions(extension, prompt):
    scoped = registry(extension, prompt)
    # The existing recovery contract may have been created during research.
    scoped = RequestRecoveryContract.from_registry(scoped).bind(scoped)
    state = {'tool_registry': scoped, 'principal_id': 'web:a', 'cleanup_scope': 'request-a',
        'completion_requirement_contract_cache': {'requested_artifact_extensions': [extension]}}
    result = _maybe_widen_scope_for_missing_artifacts(state, missing_requirements=('attachment:'+extension,))
    assert result is not None
    widened, receipt, scope = result
    assert widened.can_invoke_tool('p9') and not widened.can_invoke_tool('a8')
    assert 'p9' in receipt.output['available_tools']
    assert 'a8' not in receipt.output['available_tools']
    assert widened.turn_tool_scope_decision.requested_artifact_extensions == (extension,)
    state.update(tool_registry=widened, tool_recovery_scopes_attempted=[scope])
    assert _maybe_widen_scope_for_missing_artifacts(state, missing_requirements=('attachment:'+extension,)) is None


@pytest.mark.parametrize('mode', ['unplanned', 'replaced', 'lookup', 'exhausted'])
def test_outputs_cannot_be_invented_by_a_source_capture_or_revive_a_replaced_request(mode):
    scoped = registry('.png', '選択した出力')
    if mode == 'lookup':
        from dataclasses import replace
        scoped.turn_tool_scope_decision = replace(scoped.turn_tool_scope_decision, source_artifact_retrieval_only=True)
    state = {'tool_registry': scoped, 'principal_id': 'web:a', 'cleanup_scope': 'request-a',
        'completion_requirement_contract_cache': {'requested_artifact_extensions':
            [] if mode in {'unplanned', 'replaced'} else ['.png']},
        'tool_recovery_scopes_attempted': ['artifact:.png'] if mode == 'exhausted' else []}
    assert _maybe_widen_scope_for_missing_artifacts(state, missing_requirements=('attachment:.png',)) is None


@pytest.mark.parametrize('extension', ['.png', '.pdf', '.xlsx', '.pptx', '.docx'])
def test_source_observations_handoff_to_available_executor_when_no_generator_is_registered(extension):
    scoped = SimpleNamespace(list_tool_definitions=lambda: [{'name': 'terminal_exec'}])
    source = ToolResult('source', 'r7', 'completed', {'result_kind':'source_observations', 'report_status':'partial'})
    nudge = _artifact_tool_delivery_nudge_for_missing_requirements(missing_requirements=('attachment:'+extension,),
        tool_registry=scoped, tool_results=[source])
    assert nudge and 'terminal_exec' in nudge and 'r7' in nudge and extension in nudge
    assert _artifact_tool_delivery_nudge_for_missing_requirements(missing_requirements=('attachment:'+extension,),
        tool_registry=scoped, tool_results=[source, ToolResult('exec', 'terminal_exec', 'failed', {})]) is None


@pytest.mark.parametrize('capability', ['image_generation', 'weather', 'market_data', 'personal_tasks'])
def test_discovery_reports_absent_capabilities_instead_of_silent_success(capability):
    scoped = registry('.pdf', '続けて')
    result, _ = scoped.apply_scope_request(ToolInvocation('scope', 'request_tool_scope', 'web:a',
        {'capabilities':[capability]}))
    assert result.status == 'completed'
    assert result.output['unavailable_capabilities'] == [capability]


@pytest.mark.parametrize('surface', ['chat', 'web', 'startup'])
def test_source_compaction_preserves_terminal_delivery_decision_and_cause(surface):
    from nullion.chat_operator import _compact_tool_output_for_context
    from nullion.web_app import _compact_web_tool_output_for_context
    from nullion.startup_maintenance import _compact_tool_output
    from nullion.artifact_lifecycle import artifact_terminal_failure_reply
    manifest = {'version':1,'selected_paths':[],'candidate_paths':[], 'candidate_versions':{},
        'failure_message':'The requested file has not been created.', 'required_artifact_extensions':['.png']}
    output = {'result_kind':'source_observations','report_status':'partial',
        'sources':[{'source_url':'https://example.test/records'}],
        'observations':[{'value':'37','quote':'Cedar 37','source_index':0}],
        'artifact_delivery_manifest':manifest,'artifact_producer_receipt_id':'native-source'}
    compact = {'chat':_compact_tool_output_for_context,'web':_compact_web_tool_output_for_context,
        'startup':_compact_tool_output}[surface]('browser_run_task', output)
    assert compact['artifact_delivery_manifest'] == manifest
    assert compact['artifact_producer_receipt_id'] == 'native-source'
    assert artifact_terminal_failure_reply([ToolResult('source','browser_run_task','completed',compact)]) == manifest['failure_message']


def test_page_capture_scope_does_not_expose_synthetic_image_producers():
    scoped = registry('.png', 'Capture sélectionnée')
    scoped._delegate.register(ToolSpec('browser_screenshot', 'Capture page', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, source_artifact_only=True),
        lambda inv: pytest.fail('Scope selection cannot execute'))
    from dataclasses import replace
    scoped._evidence = replace(scoped._evidence, has_url_target=True)
    result, capture = scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:a',
        {'capabilities':['web'], 'tool_names':['browser_screenshot'], 'artifact_extensions':['.png']}))
    assert result.status == 'completed'
    assert capture.can_invoke_tool('browser_screenshot')
    assert not capture.can_invoke_tool('p9')
    assert 'p9' not in result.output['available_tools']


def test_registered_connector_writer_cannot_gain_account_authority_via_output_format():
    scoped = registry('.pdf', '資料を作成')
    scoped._delegate.register(ToolSpec('z8', 'Connected writer', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 5, produced_artifact_extensions=('.pdf',),
        capability_tags=('connector',)), lambda inv: pytest.fail('Cannot execute'))
    assert scoped.artifact_production_tools(('.pdf',)) == ('p9',)


@pytest.mark.parametrize('kind', ['action', 'deliverable'])
def test_partial_source_is_not_a_terminal_text_answer_for_an_unfinished_outcome(monkeypatch, kind):
    from nullion import agent_orchestrator as agent
    scoped = registry('.pdf', '資料を処理')
    source = ToolResult('source', 'r7', 'completed', {'result_kind':'source_observations',
        'report_status':'partial','observations':[{'value':'37','quote':'Cedar 37'}]})
    row = agent._CompletionRequirementContractRow('r1', 'Selected outcome', kind, ('p9',))
    def plan(state, **kwargs):
        state['completion_requirement_contract_cache'] = {'rows':(row,),
            'requested_artifact_extensions':['.pdf'] if kind == 'deliverable' else []}
        return (row,)
    monkeypatch.setattr(agent, '_extract_completion_requirement_contract', plan)
    state = {'runtime_store':object(),'orchestrator':object(),'tool_registry':scoped,'tool_results':[source]}
    assert not agent._partial_source_report_delivery_ready(state, [source])


def test_failed_dedicated_producer_can_handoff_once_to_existing_executor():
    scoped = registry('.pdf', '資料を作成')
    scoped._delegate.register(ToolSpec('terminal_exec', 'Execution', ToolRiskLevel.LOW,
        ToolSideEffectClass.DANGEROUS_EXEC, False, 5), lambda inv: pytest.fail('Cannot execute'))
    from dataclasses import replace
    scoped.turn_tool_scope_decision = replace(scoped.turn_tool_scope_decision,
        requested_tool_names=('p9',), requested_artifact_extensions=('.pdf',))
    source = ToolResult('source','r7','completed', {'result_kind':'source_observations','report_status':'partial'})
    failed = ToolResult('render','p9','failed', {'reason':'renderer_unavailable'})
    nudge = _artifact_tool_delivery_nudge_for_missing_requirements(missing_requirements=('attachment:.pdf',),
        tool_registry=scoped, tool_results=[source, failed])
    assert nudge and 'terminal_exec' in nudge
    assert _artifact_tool_delivery_nudge_for_missing_requirements(missing_requirements=('attachment:.pdf',),
        tool_registry=scoped, tool_results=[source, failed, ToolResult('exec','terminal_exec','failed',{})]) is None
