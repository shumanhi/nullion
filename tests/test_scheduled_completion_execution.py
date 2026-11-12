from copy import deepcopy
from dataclasses import replace
import json

import pytest
from jsonschema import Draft202012Validator, ValidationError

from nullion import agent_orchestrator as agent
from nullion.cron_delivery import _cron_remaining_execution_tools
from nullion.report_completion import MARKER, bind_report_outcome, generated_report_outcome, report_outcome_contract
from nullion.tools import ToolResult


def reply(value):
    return {'content': [{'type': 'text', 'text': json.dumps(value)}]}


def negative_review():
    return agent._CompletionReviewDecision('retry', ('不要な下書きを抑制',), (),
        all_material_requirements_enumerated=True,
        material_requirements=(agent._CompletionReviewMaterialRequirement('r1', '確認', 'fact', True, False, ('source',)),),
        scheduled_alert_delivery=('silent', 'no_verified_match'))


@pytest.mark.parametrize('change', [None, 'source_gap', 'action_gap', 'failure', 'retry_tools', 'review_error',
    'report_mode', 'file', 'approval', 'pagination'])
def test_quiet_completion_requires_typed_satisfied_source_work(change):
    decision = negative_review()
    contract = (agent._CompletionRequirementContractRow('r1', '確認', 'fact', ('opaque_read',), 'selected'),)
    context = {'scheduled_task_run': True, 'scheduled_alert_delivery_mode': 'verified_matches_only'}
    results = [ToolResult('source', 'opaque_read', 'completed', {'items': [], 'count': 0})]
    if change in {'source_gap', 'action_gap'}:
        decision = replace(decision, material_requirements=(replace(decision.material_requirements[0], satisfied=False),))
    if change == 'failure':
        decision = replace(decision, material_requirements=(replace(decision.material_requirements[0], blocking_failure_invocation_ids=('failed',)),))
    if change == 'retry_tools': decision = replace(decision, retry_tool_names=('opaque_read',))
    if change == 'review_error': decision = replace(decision, review_error='invalid_response')
    if change == 'approval': decision = replace(decision, disposition='needs_approval')
    if change == 'report_mode': context['scheduled_alert_delivery_mode'] = 'report_each_run'
    if change == 'file': context['requires_artifact_delivery'] = True
    if change == 'pagination':
        results[0].output['source_pagination'] = {'type': 'cursor_page', 'scope_id': 'selected',
            'cursor': None, 'next_cursor': 'next', 'result_count': 1}
    normalized = agent._normalize_silent_monitor_review(decision, context=context,
        tool_results=results, requirement_contract=contract)
    assert (normalized.disposition == 'complete') is (change is None)
    if change is None:
        assert agent._forced_completion_review_is_grounded(normalized, tool_results=results,
            requirement_contract=contract)
        results[0].status = 'failed'
        assert not agent._forced_completion_review_is_grounded(normalized, tool_results=results,
            requirement_contract=contract), 'Negative conclusions still require actual positive source evidence'


@pytest.mark.parametrize('label', ['記録された値を回答', 'Informa del valor observado.'])
@pytest.mark.parametrize('explicit_owner', [True, False])
def test_explicit_inline_output_contract_cannot_require_a_file(label, explicit_owner):
    value = {'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
        'material_requirements': [{'requirement': label, 'completion_owner': 'producer',
            'delivery_required': True, 'requirement_kind': 'deliverable', 'evidence_subject_id': 'selected',
            'artifact_member_subject_ids': [], 'eligible_evidence_tool_names': ['opaque_read']}]}
    schema = agent._completion_requirement_response_schema(['opaque_read'])
    Draft202012Validator.check_schema(schema)
    with pytest.raises(ValidationError): Draft202012Validator(schema).validate(value)
    if not explicit_owner: value['material_requirements'][0].pop('completion_owner')
    diagnostics = []
    assert not agent._parse_completion_requirement_contract(reply(value), registered_tool_names={'opaque_read'}, diagnostics=diagnostics)
    assert diagnostics == ['file_requirement_without_requested_format']
    value['material_requirements'][0]['requirement_kind'] = 'fact'
    if explicit_owner: Draft202012Validator(schema).validate(value)
    assert agent._parse_completion_requirement_contract(reply(value), registered_tool_names={'opaque_read'})


def test_report_can_propose_exact_remaining_tools_without_granting_execution(tmp_path):
    contract = report_outcome_contract({'request': '確認'}, recovery_tool_names=['opaque_read'])
    value = {'contract_id': contract['contract_id'], 'status': 'partial',
        'all_material_requirements_enumerated': True, 'remaining_work': ['残りの記録を確認'],
        'retry_tool_names': ['opaque_read']}
    outcome = generated_report_outcome('<!--'+MARKER+json.dumps(value)+'-->', contract)
    assert outcome['retry_tool_names'] == ['opaque_read']
    path = tmp_path/'report.html'; path.write_text('<!doctype html><html><body>7</body></html>')
    result = {'cron_task': '確認', 'artifacts': [str(path)], 'cron_recovery_tool_names': ['opaque_read'],
        'cron_report_content_receipt': bind_report_outcome('確認', outcome, [str(path)])}
    assert _cron_remaining_execution_tools(result) == ['opaque_read']
    for mutate in ('changed_file', 'foreign_request', 'unavailable', 'review_failed', 'blocked'):
        modified = deepcopy(result)
        if mutate == 'changed_file': modified['cron_report_content_receipt']['files'][str(path.resolve())] = 'old'
        if mutate == 'foreign_request': modified['cron_task'] = '別の依頼'
        if mutate == 'unavailable': modified['cron_recovery_tool_names'] = []
        if mutate == 'review_failed': modified['cron_source_verification_feedback'] = {'review_error': 'invalid_coverage'}
        if mutate == 'blocked': modified['cron_source_verification_feedback'] = {'disposition': 'needs_approval'}
        assert _cron_remaining_execution_tools(modified) == [], mutate
    value['retry_tool_names'] = ['unavailable']
    assert generated_report_outcome('<!--'+MARKER+json.dumps(value)+'-->', contract)['status'] == 'unknown'


def test_silent_retry_schema_accepts_only_negative_monitor_decision():
    schema = agent._completion_review_response_schema([negative_review().material_requirements[0]], scheduled_alerts_only=True)
    Draft202012Validator.check_schema(schema)
    value = {'disposition': 'retry', 'all_material_requirements_enumerated': True,
        'unresolved_requirements': ['下書きを抑制'], 'retry_tool_names': [], 'user_facing_failure': '診断',
        'material_requirements': [{'requirement_id': 'r1', 'satisfied': True, 'draft_covered': False, 'evidence_invocation_ids': ['source']}],
        'scheduled_alert_delivery': {'decision': 'silent', 'reason': 'no_verified_match'}}
    Draft202012Validator(schema).validate(value)
    for change in ('send', 'blocked', 'other_mode'):
        modified = deepcopy(value)
        if change == 'send': modified['scheduled_alert_delivery'] = {'decision': 'send', 'reason': 'verified_match'}
        if change == 'blocked': modified['disposition'] = 'blocked'
        selected = agent._completion_review_response_schema([negative_review().material_requirements[0]]) if change == 'other_mode' else schema
        with pytest.raises(ValidationError): Draft202012Validator(selected).validate(modified)


@pytest.mark.parametrize('extension', ['.html', '.csv', '.pdf', '.docx'])
def test_known_partial_file_cannot_trigger_completion_shortcut_before_it_changes(tmp_path, extension):
    import hashlib
    path = tmp_path/('selected'+extension)
    path.write_bytes(b'partial file bytes')
    state = {'user_message': '選択された出力', 'tool_results': [], 'tool_flow_context': {
        'requires_artifact_delivery': True, 'required_artifact_extensions': [extension],
        'incomplete_artifact_versions': {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest()}}}
    assert agent._completed_required_artifact_paths_for_turn(state, [str(path)]) == ()
    path.write_bytes(b'updated file bytes')
    assert agent._completed_required_artifact_paths_for_turn(state, [str(path)]) == (str(path),)
    # This early shortcut only offers a candidate. It is not content validation
    # or a delivery receipt; malformed PDF/DOCX bytes still fail later gates.


@pytest.mark.parametrize('change', [None, 'request', 'evidence', 'file', 'forged', 'delivery', 'approval'])
def test_report_completion_authority_is_bound_to_current_sources_files_and_delivery(tmp_path, monkeypatch, change):
    from nullion.crons import CronJob
    from nullion.cron_delivery import (_VerifiedCronReportReview, _cron_report_review_evidence_binding,
        _attach_cron_execution_outcome)
    monkeypatch.setattr('nullion.artifacts.artifact_root_for_principal', lambda _: str(tmp_path))
    path = tmp_path/'report.html'; path.write_text('<!doctype html><html><body><p>7</p></body></html>')
    job = CronJob(id='selected', name='確認', task='選択された結果を報告。', schedule='* * * * *')
    result = {'text': 'Report attached.', 'response_fulfilled': True, 'cron_delivery_status': 'saved',
        'artifacts': [str(path)], 'tool_results': [
            ToolResult('optional', 'optional_read', 'failed', {'reason': 'source_unavailable'}, 'Unavailable'),
            ToolResult('source', 'opaque_read', 'completed', {'value': 7}),
            ToolResult('write', 'file_write', 'completed', {'path': str(path)})],
        'cron_report_content_receipt': bind_report_outcome(job.task, {'status': 'complete', 'remaining_work': []}, [str(path)])}
    result['_verified_cron_report_review'] = _VerifiedCronReportReview(_cron_report_review_evidence_binding(job, result))
    if change == 'request': job.task = '別の依頼'
    if change == 'evidence': result['tool_results'][1].output['value'] = 8
    if change == 'file': path.write_text('<!doctype html><html><body><p>8</p></body></html>')
    if change == 'forged': result['_verified_cron_report_review'] = {'evidence_binding': result['_verified_cron_report_review'].evidence_binding}
    if change == 'delivery': result['cron_delivery_status'] = 'failed'; result['cron_delivery_failed'] = True
    if change == 'approval': result['suspended_for_approval'] = True
    _attach_cron_execution_outcome({'job': job, 'conversation_id': 'web:report'}, result,
        delivered_artifacts=[str(path)])
    assert (result['execution_outcome']['status'] == 'succeeded') is (change is None), result['execution_outcome']
