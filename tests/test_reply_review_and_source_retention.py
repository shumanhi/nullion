import json
from pathlib import Path
from types import SimpleNamespace
import pytest

from nullion.agent_orchestrator import _completion_review_request_context, _extract_completion_requirement_contract
from nullion.runtime_source_retention import retained_runtime_source_cleanup_result
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass



def test_plain_turn_has_no_new_review_context_and_linked_turns_are_bounded():
    assert _completion_review_request_context({'user_message':'hello'})=={}
    assert _completion_review_request_context({'tool_flow_context':{'request_context':{'linked_turns':[{'assistant':'unrelated'}]}}})=={}
    result=_completion_review_request_context({'tool_flow_context':{'request_context':{'parent_turn_id':'p',
        'linked_turns':[{'user':'revise','assistant':'x'*20000}]}}})
    assert len(result['request_context']['linked_turns'][0]['assistant'])==12000

@pytest.mark.parametrize('scheduled,role,kind,command,retained',[
    (True,'source','terminal_output','rm -f {source}',True),
    (True,'source','terminal_output','/bin/rm -- {source}',True),
    (False,'source','terminal_output','rm -f {source}',False),
    (True,'deliverable','terminal_output','rm -f {source}',False),
    (True,'source','other','rm -f {source}',False),
    (True,'source','terminal_output','rm -f {source} {personal}',False),
    (True,'source','terminal_output','rm -f {source}; echo done',False),
    (True,'source','terminal_output','rm -rf {source}',False),
    (True,'source','terminal_output','rm -f *',False),
])
def test_only_owned_source_cleanup_is_retained_without_deleting(tmp_path,scheduled,role,kind,command,retained):
    source=tmp_path/'random-name.txt';source.write_text('source evidence')
    personal=tmp_path/'personal.txt';personal.write_text('keep')
    invocation=ToolInvocation('delete','terminal_exec','telegram:123',{'command':command.format(source=source,personal=personal)},
        flow_context={'scheduled_task_run':scheduled})
    result=ToolResult('source','terminal_exec','completed',{'artifact_descriptors':[{'path':str(source),'role':role,'kind':kind}]})
    output=retained_runtime_source_cleanup_result(invocation,[result],registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,)))
    assert (output is not None) is retained
    if output:
        assert output.output['command_executed'] is False and output.output['deleted_paths']==[]
    assert source.read_text()=='source evidence' and personal.read_text()=='keep'


def test_saved_history_lookup_is_available_to_completion_without_reclassifying():
    state={'tool_flow_context':{'request_context':{'reference_kind':'saved_chat_lookup','saved_history_context':'Selected earlier answer: '+ 'x'*20000}}}
    result=_completion_review_request_context(state)
    assert result['request_context']['reference_kind']=='saved_chat_lookup'
    assert len(result['request_context']['saved_history_context'])==16000


def test_execution_background_remains_available_but_is_not_marked_as_linked():
    state={'tool_flow_context':{'request_context':{'reference_kind':'conversation_background','parent_turn_id':None,
        'linked_turns':[{'user':'Previous question','assistant':'Earlier answer'}]}}}
    context=_completion_review_request_context(state)['request_context']
    assert context['reference_kind']=='conversation_background' and context['parent_turn_id'] is None
    assert context['linked_turns'][0]['assistant']=='Earlier answer'
