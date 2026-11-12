"""Review clarification types cannot contradict validated source state."""
import json
import pytest
from nullion.agent_orchestrator import _parse_completion_review_decision

CONSENT={'source_evidence_verified':True,'connector_source_user_requested':True,
    'source_selection_required':False,'source_resolution_required':False}


def parsed(kind, contract=CONSENT):
    question={'question':'選択してください','options':['A','B']}
    if kind is not None:question['input_kind']=kind
    payload={'disposition':'needs_user_input','unresolved_requirements':['選択'],
        'retry_tool_names':[],'clarification':question}
    response={'content':[{'type':'text','text':json.dumps(payload)}]}
    errors=[]
    result=_parse_completion_review_decision(response, registered_tool_names=set(),
        source_action_contract=contract, diagnostics=errors)
    return result,errors


@pytest.mark.parametrize('kind',['source_selection','source_resolution'])
def test_rejects_new_gate_over_verified_source(kind):
    decision,errors=parsed(kind)
    assert decision is None and errors==['clarification_contradicts_verified_source_contract']


@pytest.mark.parametrize('kind',['task_parameters','other'])
def test_preserves_unrelated_user_choices(kind):
    decision,errors=parsed(kind)
    assert decision and not errors and decision.disposition=='needs_user_input'


@pytest.mark.parametrize('kind,flag',[('source_selection','source_selection_required'),('source_resolution','source_resolution_required')])
def test_preserves_pending_source_choice(kind,flag):
    decision,errors=parsed(kind,{**CONSENT,flag:True})
    assert decision and not errors


def test_requires_typed_reason_only_when_verified_source_contract_available():
    decision,errors=parsed(None)
    assert decision is None and errors==['missing_clarification_input_kind']
    assert parsed(None,{})[0]


@pytest.mark.parametrize('kind',[{},[],42])
def test_bad_input_kind_is_protocol_error(kind):
    decision,errors=parsed(kind)
    assert decision is None and errors==['invalid_clarification_input_kind']


def test_verified_scope_needs_input_requires_a_typed_clarification():
    response={'content':[{'type':'text','text':json.dumps({'disposition':'needs_user_input',
        'unresolved_requirements':['Missing input'], 'retry_tool_names':[]})}]}
    errors=[]
    assert _parse_completion_review_decision(response,registered_tool_names=set(),
        source_action_contract=CONSENT,diagnostics=errors) is None
    assert errors==['missing_clarification_input_kind']
