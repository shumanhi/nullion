from dataclasses import replace

import pytest

from nullion.agent_orchestrator import (_CompletionReviewDecision, _CompletionReviewMaterialRequirement,
    _CompletionRequirementContractRow, _completion_execution_is_grounded)
from nullion.tools import ToolResult


@pytest.mark.parametrize('problem', [None, 'unknown_ref', 'unsatisfied', 'blocked', 'approval', 'needs_input',
                                    'retry_tool', 'review_error', 'runtime_failure', 'blocking_failure', 'contract_mismatch'])
def test_work_coverage_never_grants_publication_or_bypasses_evidence(problem):
    contract = (_CompletionRequirementContractRow('r1', 'Selected reading', 'fact', ('sensor',), 'selected'),)
    row = _CompletionReviewMaterialRequirement('r1', 'Selected reading', 'fact', True, False, ('read1',))
    decision = _CompletionReviewDecision('retry', ('Correct the summary',),
        all_material_requirements_enumerated=True, material_requirements=(row,))
    if problem == 'unknown_ref': row = replace(row, evidence_invocation_ids=('unknown',))
    if problem == 'unsatisfied': row = replace(row, satisfied=False)
    if problem == 'blocking_failure': row = replace(row, blocking_failure_invocation_ids=('read1',))
    if problem == 'contract_mismatch': row = replace(row, requirement='Different requirement')
    decision = replace(decision, material_requirements=(row,))
    if problem in {'blocked', 'approval', 'needs_input'}:
        decision = replace(decision, disposition={'approval':'needs_approval', 'needs_input':'needs_user_input'}.get(problem, problem))
    if problem == 'retry_tool': decision = replace(decision, retry_tool_names=('sensor',))
    if problem == 'review_error': decision = replace(decision, review_error='unavailable')
    if problem == 'runtime_failure': decision = replace(decision, runtime_failure_code='missing')
    assert _completion_execution_is_grounded(decision, requirement_contract=contract,
        tool_results=[ToolResult('read1', 'sensor', 'completed', {'value': 7})]) is (problem is None)
    assert decision.disposition != 'complete' and not decision.material_requirements[0].draft_covered


@pytest.mark.parametrize('content_status', ['complete', 'partial', 'unknown'])
@pytest.mark.parametrize('gap', ['Replace the draft and identify two files.', '内部の要約を編集してください。'])
def test_public_file_caption_never_publishes_controller_instructions(tmp_path, content_status, gap):
    from nullion.cron_delivery import _complete_typed_html_artifact_recovery
    path = tmp_path/'report.html'; path.write_text('<!doctype html><html><body>Selected: 7</body></html>')
    result = {'response_fulfilled': False}
    _complete_typed_html_artifact_recovery(result, artifact_path=path, required_extensions=('.html',), request='selected',
        content_outcome={'status': 'partial', 'content_status': content_status, 'presentation_status': 'partial',
                         'remaining_work': [gap]})
    assert gap not in result['text'] and result['artifacts'] == [str(path)]
    assert result['cron_remaining_work'] == [gap], 'Retain private correction context'
    assert result['response_fulfilled'] is False
    assert result['cron_report_partial'] is (content_status != 'complete')
