from copy import deepcopy
import pytest
from nullion.agent_orchestrator import _completion_review_retry_progress_update as renew,_CompletionReviewDecision as Review
from nullion.tools import ToolResult


def initial():
    return {'completion_review_count':0,'iterations':3,'max_iterations':20,
        'tool_results':[ToolResult('one','opaque_read','completed',{'value':10})]}


def test_new_review_requested_content_can_continue_after_the_fixed_rewrite_allowance():
    state=initial();review=Review('retry',('未完了',),('opaque_read',))
    state.update(renew(state,review));state['completion_review_count']=2
    state['tool_results'].append(ToolResult('new','opaque_read','completed',{'value':20}))
    update=renew(state,review)
    assert update is not None and 'max_iterations' not in update


@pytest.mark.parametrize('change',['same_content','failed','denied','error','unrelated','new_scope_only','hard_limit','review_error','complete'])
def test_repeated_or_unverified_work_cannot_renew_completion_review(change):
    state=initial();review=Review('retry',('未完了',),('opaque_read',));state.update(renew(state,review));state['completion_review_count']=2
    row=ToolResult('different-id','opaque_read','completed',{'value':10})
    if change in {'failed','denied'}:row.status=change;row.output={'value':20}
    if change=='error':row.error='unavailable';row.output={'value':20}
    if change=='unrelated':row.tool_name='other_read';row.output={'value':20}
    if change=='new_scope_only':review=Review('retry',('未完了',),('other_read',));row.tool_name='other_read';row.output={'value':20}
    if change=='hard_limit':state['iterations']=state['max_iterations'];row.output={'value':20}
    if change=='review_error':review=Review('retry',review_error='invalid_response');row.output={'value':20}
    if change=='complete':review=Review('complete');row.output={'value':20}
    state['tool_results'].append(row)
    assert renew(state,review) is None


def test_recaptured_same_quote_does_not_gain_progress_from_paths_labels_or_timestamps():
    row=ToolResult('first','opaque_read','completed',{'result_kind':'source_observations','report_status':'completed',
        'observations':[{'source_url':'https://source.example/','value':'10','quote':'Price 10','label':'価格','captured_at':100}]})
    state=initial();state['tool_results']=[row];review=Review('retry',retry_tool_names=('opaque_read',));state.update(renew(state,review));state['completion_review_count']=2
    again=deepcopy(row);again.invocation_id='again';again.output['observations'][0].update(label='Precio',captured_at=999);again.output['artifact_paths']=['/new/screenshot.png'];state['tool_results'].append(again)
    assert renew(state,review) is None


def test_plain_or_completed_turn_never_scans_recovery_history():
    class NoScan:
        def __iter__(self):raise AssertionError('Unrelated turn scanned recovery history')
    assert renew({'tool_results':NoScan()},None) is None
    assert renew({'tool_results':NoScan()},Review('complete')) is None


@pytest.mark.parametrize('change',['navigation','uncited_content','limitation_only'])
def test_new_pages_need_review_bound_relevant_facts_to_renew(change):
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement, _CompletionReviewSupportedFinding as Finding
    state=initial();review=Review('retry',retry_tool_names=('opaque_read',),material_requirements=(
        Requirement('r1','記録','fact',False,True,('one',)),))
    state.update(renew(state,review));state['completion_review_count']=2
    output={'value':20}
    if change=='navigation':output={'result_kind':'browser_navigation','page_url':'https://source.example/other'}
    if change=='limitation_only':review=Review('retry',retry_tool_names=('opaque_read',),material_requirements=review.material_requirements,
        supported_findings=(Finding('The page was blocked',('new',),kind='limitation'),))
    state['tool_results'].append(ToolResult('new','opaque_read','completed',output))
    assert renew(state,review) is None


def test_new_review_cited_fact_can_renew_even_when_the_goal_is_not_yet_complete():
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement
    state=initial();review=Review('retry',retry_tool_names=('opaque_read',),material_requirements=(
        Requirement('r1','記録','fact',False,True,('one',)),))
    state.update(renew(state,review));state['completion_review_count']=2
    state['tool_results'].append(ToolResult('new','opaque_read','completed',{'value':20}))
    review=Review('retry',retry_tool_names=('opaque_read',),material_requirements=(Requirement('r1','記録','fact',False,True,('new',)),))
    assert renew(state,review) is not None
