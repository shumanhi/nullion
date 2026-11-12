from copy import deepcopy

import pytest

from nullion.agent_orchestrator import (
    _should_block_failed_tool_during_recovery, _tool_failure_fingerprint,
    _scope_request_stalled_after_completed_tool,
)
from nullion.tools import ToolResult, ToolRegistry
from nullion.tool_repair_contract import plan_revision_recovery, resolve_plan_revisions, tool_repair_kind
from nullion.workflow_recovery import WorkflowCheckpoint, recovered_failure_ids


def rejection(tool='browser_run_task', ident='rejected'):
    return ToolResult(ident, tool, 'failed', {'reason':'request_plan_conflict', 'executed':False,
        'review_decision':'revise', 'retryable':True, 'issues':['対象を修正してください。'],
        'recovery':plan_revision_recovery(tool)})


@pytest.mark.parametrize('tool', ['browser_run_task','file_write','calendar_create','terminal_exec','opaque_adapter'])
def test_rejected_proposal_keeps_same_capability_and_changed_arguments_can_be_reviewed(tool):
    first = rejection(tool)
    translated = deepcopy(first); translated.output['issues'] = ['Corrige la cible.']
    assert tool_repair_kind(first) == 'revise_plan'
    assert not _should_block_failed_tool_during_recovery(first)
    key = _tool_failure_fingerprint(result=first, invocation_signature='proposal-A')
    assert key == _tool_failure_fingerprint(result=translated, invocation_signature='proposal-A')
    assert key != _tool_failure_fingerprint(result=translated, invocation_signature='proposal-B')


@pytest.mark.parametrize('change', ['denied','executed','clarify','not_retryable','bypass','wrong_tool','missing_contract','wrong_reason'])
def test_repair_contract_never_grants_authority_to_other_failures(change):
    row = rejection()
    if change == 'denied': row.status = 'denied'
    elif change == 'executed': row.output['executed'] = True
    elif change == 'clarify': row.output['review_decision'] = 'clarify'
    elif change == 'not_retryable': row.output['retryable'] = False
    elif change == 'bypass': row.output['recovery']['approval_bypass'] = True
    elif change == 'wrong_tool': row.output['recovery']['retry_tool_name'] = 'other'
    elif change == 'missing_contract': row.output.pop('recovery')
    else: row.output['reason'] = 'policy_denied'
    assert tool_repair_kind(row) is None


@pytest.mark.parametrize('case', ['corrected','failed','denied','unreviewed','other_tool','executed_failure','later_rejection'])
def test_only_admitted_completed_execution_retires_prior_rejected_proposals(case):
    failed = rejection()
    success = ToolResult('executed', failed.tool_name, 'completed', {'value':7})
    admitted = True
    if case in {'failed','denied'}: success.status = case
    elif case == 'unreviewed': admitted = False
    elif case == 'other_tool': success.tool_name = 'other'
    elif case == 'executed_failure': failed.output['executed'] = True
    results = [success, failed] if case == 'later_rejection' else [failed, success]
    resolve_plan_revisions(results, success, preflight_proceeded=admitted)
    expected = case == 'corrected'
    assert bool(recovered_failure_ids(results)) is expected
    checkpoint = WorkflowCheckpoint('w','c','g')
    checkpoint.observe(results, {'rejected':{'arguments':{'target':'A'}}, 'executed':{'arguments':{'target':'B'}}}, ToolRegistry())
    assert (not checkpoint.open_issues) is expected
    restored = WorkflowCheckpoint.restore(checkpoint.to_dict(), workflow_id='w',conversation_id='c',goal_digest='g')
    failed.recovered_by = ()
    restored.observe(results, {}, ToolRegistry())
    assert bool(failed.recovered_by) is expected


def test_scope_stall_compares_execution_contract_and_resets_on_real_work():
    read = ToolResult('read','opaque_read','completed',{'value':7})
    a = ToolResult('scope-A','request_tool_scope','completed',{'available_tools':['b','a'],'reason':'選択'})
    b = ToolResult('scope-B','request_tool_scope','completed',{'available_tools':['a','b'],'reason':'Selection'})
    assert _scope_request_stalled_after_completed_tool([read,a,b])
    assert not _scope_request_stalled_after_completed_tool([read,a,b,ToolResult('new','opaque_read','completed',{})])
    b.output['available_tools'].append('new_tool')
    assert not _scope_request_stalled_after_completed_tool([read,a,b])


@pytest.mark.parametrize('decision',['revise','clarify','unavailable'])
def test_langchain_fallback_cannot_execute_an_unreviewed_plan_through_another_tool(decision):
    from nullion.langchain_adapters import _result_allows_fallback
    row=rejection();row.output['review_decision']=decision
    assert not _result_allows_fallback(row)
    assert _result_allows_fallback(ToolResult('network','opaque_read','failed',{'reason':'network_error'}))
