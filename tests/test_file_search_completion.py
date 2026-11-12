"""A complete bounded filename search proves absence; stale scope is not consent."""
from types import SimpleNamespace
import pytest
from nullion.agent_orchestrator import (_completion_review_evidence_result_is_positive,
    _completion_review_scope_contract,_CompletionRequirementContractRow)
from nullion.tools import ToolResult,ToolInvocation,_build_file_search_handler


def search(tmp_path, **args):
    return _build_file_search_handler(tmp_path,include_principal_workspace=False)(
        ToolInvocation('search','file_search','operator',{'pattern':'不存在.pdf',**args}))


def test_complete_empty_filename_search_is_scoped_negative_evidence(tmp_path):
    result=search(tmp_path)
    assert result.output['searched_roots']==[str(tmp_path)]
    assert result.output['searched_file_count']==0
    assert _completion_review_evidence_result_is_positive(result)


@pytest.mark.parametrize('case',['truncated','content','missing_root','walk_error','malformed','legacy'])
def test_partial_or_unqualified_absence_is_not_completion_evidence(tmp_path,monkeypatch,case):
    if case=='truncated':
        (tmp_path/'one').write_text('x');monkeypatch.setenv('NULLION_FILE_SEARCH_MAX_ENTRIES','1')
    if case=='missing_root':tmp_path=tmp_path/'missing'
    if case=='walk_error':
        import nullion.tools as tools
        def walk(*args,onerror,**kwargs):onerror(PermissionError('denied'));return iter(())
        monkeypatch.setattr(tools.os,'walk',walk)
    result=search(tmp_path,search_contents=case=='content')
    if case=='malformed':result.output['searched_file_count']=True
    if case=='legacy':result.output.pop('search_coverage')
    assert not _completion_review_evidence_result_is_positive(result)


@pytest.mark.parametrize('material_tool',['file_search','pdf_create'])
@pytest.mark.parametrize('executed,reason,decision',[(False,'request_plan_conflict','revise'),
    (True,'request_plan_conflict','revise'),(False,'permission_denied','deny'),(False,'request_plan_conflict',[])])
def test_only_rejected_unrequested_tools_retire_from_scope(material_tool,executed,reason,decision):
    registry=SimpleNamespace(scheduled_source_scope={'required_tool_names':['file_search','pdf_create']})
    rejected=ToolResult('reject','pdf_create','failed',{'executed':executed,'reason':reason,'review_decision':decision})
    scope=_completion_review_scope_contract(registry,[rejected],requirement_contract=[
        _CompletionRequirementContractRow('r1','要求','action' if material_tool=='pdf_create' else 'fact',(material_tool,))])
    retired=material_tool=='file_search' and not executed and reason=='request_plan_conflict' and decision in ('revise','deny')
    assert ('pdf_create' in scope['missing_callable_required_tool_names']) is not retired
    if retired:assert scope['retired_rejected_unrequested_tools']==['pdf_create']


def test_missing_independent_contract_never_retires_required_tools():
    registry=SimpleNamespace(scheduled_source_scope={'required_tool_names':['pdf_create']})
    result=ToolResult('reject','pdf_create','failed',{'executed':False,'reason':'request_plan_conflict','review_decision':'revise'})
    assert _completion_review_scope_contract(registry,[result])['missing_callable_required_tool_names']==['pdf_create']
