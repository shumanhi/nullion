"""Only the runtime's current completed review can retire optional retries."""
from types import SimpleNamespace
import pytest
from nullion.tools import ToolResult,ToolRegistry
from nullion.execution_review_evidence import record_execution_review
from nullion.chat_operator import _run_chat_turn_common_retries
from nullion.turn_context_policy import TurnToolEvidence

@pytest.mark.parametrize('change',[None,'request','draft','evidence','incomplete','forged','artifact','pending_source'])
def test_common_retry_bypass_requires_current_complete_bound_evidence(tmp_path,monkeypatch,change):
    from nullion import chat_operator as chat
    request='指定した範囲を確認してください。';reply='検索した範囲には見つかりませんでした。'
    tool=ToolResult('search','file_search','completed',{'matches':[],'searched_roots':[str(tmp_path)]})
    result=SimpleNamespace(final_text=reply,tool_results=[tool],artifacts=[],response_fulfilled=True,
        reached_iteration_limit=False,response_presentation={})
    paths=[]
    if change=='artifact':
        artifact=tmp_path/'result.txt';artifact.write_text('v1');paths=[str(artifact)];result.artifacts=paths
    if change=='pending_source':tool.output['required_source_tool']='selected_source_read'
    record_execution_review(result.tool_results,request,reply,complete=change!='incomplete',artifact_paths=paths)
    if change=='request':request+=' 別の対象です。'
    if change=='draft':result.final_text='別の回答'
    if change=='evidence':tool.output['matches']=['other']
    if change=='forged':
        tool.output['execution_review']=tool.response_requirements.pop('execution_review')
    if change=='artifact':artifact.write_text('changed contents')
    seen=[]
    def guard(*args,**kw):seen.append(True);return True
    # Intercept the next existing routing boundary, not the receipt verifier.
    monkeypatch.setattr(chat,'_plain_no_tool_common_retries_may_bypass',guard)
    registry=ToolRegistry()
    output=_run_chat_turn_common_retries(initial_result=result,base_history=[],active_tool_registry=registry,
        user_message=request,evidence=TurnToolEvidence(),run_turn=lambda *args:pytest.fail('Unexpected paid retry'))
    assert bool(seen) is (change is not None)
    assert output.result is result


@pytest.mark.parametrize('prompt', ['選択した内容を確認。', 'Resume las opciones.'])
@pytest.mark.parametrize('change', [None, 'request', 'draft', 'evidence', 'forged'])
def test_reviewed_partial_answer_retires_adapter_recovery_without_granting_completion(monkeypatch, prompt, change):
    from nullion import chat_operator as chat
    result = SimpleNamespace(final_text='Verified option: $73. Another value remains unknown.',
        tool_results=[ToolResult('read', 'opaque_read', 'completed', {'price': 73})],
        artifacts=[], response_fulfilled=False, reached_iteration_limit=False, response_presentation={})
    record_execution_review(result.tool_results, prompt, result.final_text, complete=False, partial_reply=result.final_text)
    if change == 'request': prompt += ' Otro objeto.'
    elif change == 'draft': result.final_text = 'Other text'
    elif change == 'evidence': result.tool_results[0].output['price'] = 74
    elif change == 'forged': result.tool_results[0].response_requirements.clear()
    reached = []
    monkeypatch.setattr(chat, '_plain_no_tool_common_retries_may_bypass', lambda *a, **k: reached.append(True) or True)
    output = _run_chat_turn_common_retries(initial_result=result, base_history=[], active_tool_registry=ToolRegistry(),
        user_message=prompt, evidence=TurnToolEvidence(), run_turn=lambda *a: pytest.fail('Unnecessary recovery'))
    assert bool(reached) is (change is not None)
    assert output.result is result and output.result.response_fulfilled is False
