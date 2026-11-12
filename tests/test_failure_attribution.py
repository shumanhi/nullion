"""Operational notices require current causal evidence, not tool-family overlap."""
import json
from dataclasses import replace

import pytest

from nullion.agent_orchestrator import (
    _CompletionRequirementContractRow as Contract,
    _CompletionReviewDecision as Decision,
    _CompletionReviewMaterialRequirement as Verdict,
    _CompletionReviewSupportedFinding as Finding,
    _completion_review_open_task_reply,
    _completion_review_with_runtime_failure,
    _completion_review_response_schema,
    _parse_completion_review_decision,
    _resolve_completion_review_references,
)
from nullion.tools import ToolResult


def review(*, bindings=(), findings=True):
    row = Verdict('selected', 'Compare the selected records', 'fact', False, False, ('read',))
    if bindings:
        row = replace(row, blocking_failure_invocation_ids=bindings)
    return Decision('retry', ('Internal editing instructions',),
        all_material_requirements_enumerated=True, material_requirements=(row,),
        supported_findings=(Finding('候補 A: 価格 28、評価 4.2。', ('read',)),) if findings else ())


@pytest.mark.parametrize('failed_tool,read_tool,reason', [
    ('browser_extract_detail', 'browser_extract_text', None),
    ('source_primary', 'source_fallback', 'network_error'),
    ('connector_request', 'connector_request', 'source_connection_required'),
    ('browser_extract_detail', 'browser_extract_items', 'handler_timeout'),
])
@pytest.mark.parametrize('findings', [True, False])
def test_successful_alternative_does_not_make_an_old_failure_the_cause_of_remaining_work(
        failed_tool, read_tool, reason, findings):
    failed = ToolResult('old', failed_tool, 'failed',
        {'session_id': 'one', **({'reason': reason} if reason else {})}, 'Private transport detail')
    read = ToolResult('read', read_tool, 'completed', {'text': '候補 A: 価格 28、評価 4.2。'})
    contract = (Contract('selected', 'Compare the selected records', 'fact', (failed_tool, read_tool)),)
    before = review(findings=findings)
    after = _completion_review_with_runtime_failure(before, requirement_contract=contract, tool_results=[failed, read])
    assert after.runtime_failure_code is None
    assert after.disposition == 'retry' and not after.material_requirements[0].satisfied
    assert after.supported_findings == before.supported_findings
    answer = _completion_review_open_task_reply(after)
    assert 'browser operation failed' not in answer and 'network error' not in answer
    assert 'A required browser step failed.' not in answer
    assert 'Private transport detail' not in answer and 'Internal editing instructions' not in answer


@pytest.mark.parametrize('reason,expected', [('network_error', 'network_error'),
    ('source_connection_required', 'source_connection_required'), ('handler_timeout', 'browser_timeout')])
def test_explicit_failed_subject_binding_preserves_genuine_failure_with_other_successes(reason, expected):
    result = _completion_review_with_runtime_failure(review(bindings=('blocked',)),
        requirement_contract=(Contract('selected', 'Compare the selected records', 'fact',
            ('browser_extract_text',)),),
        tool_results=[ToolResult('read', 'browser_extract_text', 'completed', {'text': 'Record A: 28.'}),
            ToolResult('blocked', 'browser_extract_text', 'failed', {'reason': reason}, 'Private details')])
    assert result.runtime_failure_code == expected
    assert not result.material_requirements[0].satisfied and result.disposition == 'retry'


@pytest.mark.parametrize('binding', ['unknown', 'read', 'unrelated', 'recovered'])
def test_invalid_or_recovered_failure_bindings_do_not_select_operational_copy(binding):
    failures = [ToolResult('read', 'browser_extract_text', 'completed', {'text': 'Record A: 28.'}),
        ToolResult('unrelated', 'other_source', 'failed', {'reason': 'network_error'}),
        ToolResult('recovered', 'browser_extract_text', 'failed', {'reason': 'network_error'},
            recovered_by=('read',))]
    result = _completion_review_with_runtime_failure(review(bindings=(binding,)),
        requirement_contract=(Contract('selected', 'Compare selected records', 'fact', ('browser_extract_text',)),),
        tool_results=failures)
    assert result.runtime_failure_code is None


def test_completed_write_without_readback_is_not_proof_of_the_requested_state():
    before = review()
    result = _completion_review_with_runtime_failure(before,
        requirement_contract=(Contract('selected', 'Check the selected current state', 'fact',
            ('browser_extract_detail', 'browser_extract_text')),),
        tool_results=[ToolResult('old', 'browser_extract_detail', 'failed', {}),
            ToolResult('read', 'browser_extract_text', 'completed', {'text': 'Previous state: A.'}),
            ToolResult('write', 'browser_click_element', 'completed', {'clicked': {'selector': '#apply'}})])
    assert result.runtime_failure_code is None and not result.material_requirements[0].satisfied
    assert result.disposition == 'retry'


def test_a_real_terminal_failure_is_still_reported_without_any_substantive_success():
    result = _completion_review_with_runtime_failure(review(findings=False),
        requirement_contract=(Contract('selected', 'Read selected state', 'fact', ('browser_extract_text',)),),
        tool_results=[ToolResult('navigation', 'browser_navigate', 'completed', {'result': 'Loaded source'}),
            ToolResult('blocked', 'browser_extract_text', 'failed', {'reason': 'handler_timeout'})])
    assert result.runtime_failure_code == 'browser_timeout'


@pytest.mark.parametrize('kind,expected', [('other', 'browser_operation_failed'), ('reasoned_answer', None)])
def test_all_failed_execution_is_distinct_from_an_incidental_failure_during_reasoning(kind, expected):
    before = Decision('blocked', all_material_requirements_enumerated=True,
        material_requirements=(Verdict('r', 'Selected outcome', kind, False, False),))
    after = _completion_review_with_runtime_failure(before,
        requirement_contract=(Contract('r', 'Selected outcome', kind, ()),),
        tool_results=[ToolResult('failed', 'browser_navigate', 'failed', {'reason': 'adapter_failure'})])
    assert after.runtime_failure_code == expected


def test_old_derived_failure_code_is_recomputed_when_new_evidence_arrives():
    result = _completion_review_with_runtime_failure(replace(review(), runtime_failure_code='browser_operation_failed'),
        requirement_contract=(Contract('selected', 'Compare selected records', 'fact',
            ('browser_extract_detail', 'browser_extract_text')),),
        tool_results=[ToolResult('old', 'browser_extract_detail', 'failed', {}),
            ToolResult('read', 'browser_extract_text', 'completed', {'text': 'Record A: 28.'})])
    assert result.runtime_failure_code is None
    deadline = _completion_review_with_runtime_failure(replace(review(), runtime_failure_code='deadline'),
        requirement_contract=(), tool_results=[])
    assert deadline.runtime_failure_code == 'deadline'


def test_bound_denial_stays_unapproved_when_another_source_read_succeeds():
    before = review(bindings=('denied',))
    after = _completion_review_with_runtime_failure(before,
        requirement_contract=(Contract('selected', 'Check selected state', 'fact', ('browser_extract_text',)),),
        tool_results=[ToolResult('read', 'browser_extract_text', 'completed', {'text': 'A: 28.'}),
            ToolResult('denied', 'browser_extract_text', 'denied', {'requires_approval': True})])
    assert after.runtime_failure_code == 'action_denied'
    assert after.disposition == 'retry' and not after.material_requirements[0].satisfied


def test_a_satisfied_requirement_cannot_attribute_an_unrelated_failed_attempt():
    before = review(bindings=('old',))
    before = replace(before, material_requirements=(replace(before.material_requirements[0], satisfied=True),))
    after = _completion_review_with_runtime_failure(before,
        requirement_contract=(Contract('selected', 'Check selected state', 'fact', ('browser_extract_text',)),),
        tool_results=[ToolResult('old', 'browser_extract_text', 'failed', {'reason': 'network_error'}),
            ToolResult('read', 'browser_extract_text', 'completed', {'text': 'A: 28.'})])
    assert after.runtime_failure_code is None and after.material_requirements[0].satisfied


def test_failure_bindings_are_optional_typed_and_resolve_packet_aliases():
    contract = (Contract('selected', 'Compare selected records', 'fact', ('browser_extract_text',)),)
    payload = {'disposition': 'retry', 'unresolved_requirements': ['Private notes'],
        'retry_tool_names': [], 'all_material_requirements_enumerated': True, 'user_facing_failure': 'Private notes',
        'material_requirements': [{'requirement_id': 'selected', 'satisfied': False, 'draft_covered': False,
            'evidence_invocation_ids': ['e0000'], 'blocking_failure_invocation_ids': ['e0001']}]}
    from jsonschema import Draft202012Validator, ValidationError
    schema = _completion_review_response_schema(contract, citable_evidence_ids=['e0000'],
        limitation_evidence_ids=['e0000', 'e0001'])
    Draft202012Validator(schema).validate(payload)
    decision = _parse_completion_review_decision({'content': [{'type': 'text', 'text': json.dumps(payload)}]},
        registered_tool_names={'browser_extract_text'}, requirement_contract=contract)
    assert decision is not None
    resolved = _resolve_completion_review_references(decision, {'e0000': 'read', 'e0001': 'failed'})
    assert resolved.material_requirements[0].blocking_failure_invocation_ids == ('failed',)
    payload['material_requirements'][0]['blocking_failure_invocation_ids'] = ['e0000']
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(payload)
    payload['material_requirements'][0]['blocking_failure_invocation_ids'] = 'e0001'
    assert _parse_completion_review_decision({'content': [{'type': 'text', 'text': json.dumps(payload)}]},
        registered_tool_names={'browser_extract_text'}, requirement_contract=contract) is None
