import json

import pytest

from nullion.agent_orchestrator import (
    _CompletionReviewDecision, _CompletionReviewSupportedFinding,
    _completion_review_open_task_reply, _completion_review_response_schema,
    _forced_completion_review_is_grounded, _parse_completion_review_decision,
    _resolve_completion_review_references,
)
from nullion.tools import ToolResult


@pytest.mark.parametrize('status,name', [('failed', 'source_read'), ('denied', 'source_read'),
    ('completed', 'inspect_retained_evidence'), ('completed', 'request_tool_scope')])
def test_supported_findings_cannot_use_nonproof_receipts(status, name):
    decision = _CompletionReviewDecision('retry', supported_findings=(
        _CompletionReviewSupportedFinding('Selected value: 7.', ('source',)),))
    assert not _forced_completion_review_is_grounded(decision,
        tool_results=[ToolResult('source', name, status, {'value': 7})], requirement_contract=())


def test_supported_findings_bind_aliases_and_preserve_details_without_reviving_draft():
    decision = _CompletionReviewDecision('retry', user_facing_failure='One date remains unknown.',
        supported_findings=(_CompletionReviewSupportedFinding('Item A: 7, due tomorrow.', ('e0001',)),))
    decision = _resolve_completion_review_references(decision, {'e0001': 'owned-source'})
    assert _forced_completion_review_is_grounded(decision,
        tool_results=[ToolResult('owned-source', 'source_read', 'completed', {'value': 7})], requirement_contract=())
    assert _completion_review_open_task_reply(decision) == 'Item A: 7, due tomorrow.\n\nSome requested work remains incomplete.'
    assert decision.user_facing_failure == 'One date remains unknown.'
    assert decision.disposition == 'retry'


@pytest.mark.parametrize('finding', [None, {}, {'text': 'A', 'evidence_invocation_ids': []},
    {'text': '', 'evidence_invocation_ids': ['source']},
    {'text': 'A', 'evidence_invocation_ids': ['source'], 'extra': 'private'}])
def test_supported_findings_require_a_bounded_structured_contract(finding):
    response = {'content': [{'type': 'text', 'text': json.dumps({'disposition': 'retry',
        'unresolved_requirements': ['A date is missing'], 'supported_findings': [finding]})}]}
    assert _parse_completion_review_decision(response, registered_tool_names=set()) is None


def test_supported_finding_schema_excludes_unknown_receipts():
    from jsonschema import Draft202012Validator, ValidationError
    schema = _completion_review_response_schema([], citable_evidence_ids=['owned-source'])
    value = {'disposition': 'retry', 'unresolved_requirements': ['Missing date'], 'retry_tool_names': [],
        'all_material_requirements_enumerated': True, 'material_requirements': [],
        'user_facing_failure': 'One date remains unknown.',
        'supported_findings': [{'text': 'A: 7.', 'presentation':'item', 'evidence_invocation_ids': ['owned-source']}]}
    Draft202012Validator(schema).validate(value)
    value['supported_findings'][0]['evidence_invocation_ids'] = ['unowned']
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)
