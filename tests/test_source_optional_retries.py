"""Completed discovery is evidence, not a mandate to run every browser option."""
from dataclasses import replace
from types import SimpleNamespace
import pytest
from nullion import chat_operator as chat
from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision


def fixture():
    tools=ToolRegistry()
    for name,effect,tags,continuations in [('browser_navigate',ToolSideEffectClass.READ,(),('browser_run_task',)),
        ('web_search',ToolSideEffectClass.READ,('public_source_discovery',),()),
        ('browser_run_task',ToolSideEffectClass.WRITE,(),())]:
        tools.register(ToolSpec(name=name,description=name,risk_level=ToolRiskLevel.LOW,side_effect_class=effect,
            requires_approval=False,timeout_seconds=5,capability_tags=tags,continuation_tools=continuations),
            lambda invocation:ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{}))
    result=SimpleNamespace(final_text='The requested subjects are described with source links.',artifacts=[],response_fulfilled=True,
        reached_iteration_limit=False,suspended_for_approval=False,tool_results=[
            ToolResult('page','browser_navigate','completed',{'http_status':403,'next_tools':['browser_navigate']}),
            ToolResult('search','web_search','completed',{'result_kind':'public_search_results','results':[
                {'url':'https://source.example/subject','snippet':'A grounded subject description'}]})])
    evidence=TurnToolEvidence(has_url_target=True,context_linked=True)
    scope=TurnToolScopeDecision(web_action='live_research',requested_tool_names=('browser_navigate','web_search'),
        required_tool_names=('browser_navigate',),valid=True)
    return tools,result,evidence,scope


def test_completed_public_research_does_not_follow_optional_browser_metadata():
    tools,result,evidence,scope=fixture();scoped=ScopedTurnToolRegistry(tools,evidence=evidence,tool_scope_decision=scope)
    def forbidden(*args):raise AssertionError('Optional browser metadata restarted completed research')
    actual,_,retried=chat._run_chat_turn_schema_followup_tool_retry(initial_result=result,base_history=[],
        active_tool_registry=scoped,base_tool_registry=tools,evidence=evidence,run_turn=forbidden,user_message='この資料の対象を調べてください')
    assert actual is result and not retried


@pytest.mark.parametrize('pending',['incomplete','iteration_limit','approval','artifact','embedded_media','required_read','empty_evidence','bad_url','non_read'])
def test_public_source_fast_completion_does_not_override_pending_work_or_invalid_evidence(pending):
    tools,result,evidence,scope=fixture()
    if pending=='incomplete':result.response_fulfilled=False
    elif pending=='iteration_limit':result.reached_iteration_limit=True
    elif pending=='approval':result.suspended_for_approval=True
    elif pending=='artifact':evidence=replace(evidence,requested_extensions=('.pdf',))
    elif pending=='embedded_media':scope=replace(scope,required_embedded_media_extensions=('.pdf',))
    elif pending=='required_read':scope=replace(scope,required_tool_names=('browser_run_task',))
    elif pending=='empty_evidence':result.tool_results[-1].output['results'][0]['snippet']=''
    elif pending=='bad_url':result.tool_results[-1].output['results'][0]['url']='file:///private/source'
    else:
        original=tools.get_spec('web_search');tools.unregister('web_search')
        tools.register(replace(original,side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE),lambda invocation:None)
    assert not chat._completed_public_source_answer_needs_no_optional_browser_retry(result,tools,evidence=evidence,scope=scope)


def test_an_explicit_unfinished_source_read_still_runs_its_continuation():
    tools,result,evidence,scope=fixture();scoped=ScopedTurnToolRegistry(tools,evidence=evidence,tool_scope_decision=scope)
    result.tool_results[0].output['required_source_tool']='browser_run_task'
    calls=[]
    def continue_required(history,registry):
        calls.append([spec.name for spec in registry.list_specs()])
        return SimpleNamespace(final_text='The additional required source was checked.',artifacts=[],response_fulfilled=True,
            suspended_for_approval=False,tool_results=[ToolResult('required','browser_run_task','completed',{'message':'Required source checked'})])
    _,_,retried=chat._run_chat_turn_schema_followup_tool_retry(initial_result=result,base_history=[],
        active_tool_registry=scoped,base_tool_registry=tools,evidence=evidence,run_turn=continue_required,user_message='Check the required source')
    assert retried and len(calls)==1 and 'browser_run_task' in calls[0]
