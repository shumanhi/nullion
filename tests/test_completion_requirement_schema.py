import copy

import pytest
from jsonschema import Draft202012Validator, ValidationError

from nullion.agent_orchestrator import _completion_requirement_response_schema


@pytest.mark.parametrize('allowed', [[], ['source-alias']])
def test_review_schema_only_accepts_explicit_proof_references(allowed):
    from nullion.agent_orchestrator import _completion_review_response_schema, _CompletionRequirementContractRow
    schema = _completion_review_response_schema(
        [_CompletionRequirementContractRow('r1', '確認', 'constraint', ('opaque_read',))],
        citable_evidence_ids=allowed)
    Draft202012Validator.check_schema(schema)
    validate = Draft202012Validator(schema).validate
    value = {'disposition': 'complete', 'unresolved_requirements': [], 'retry_tool_names': [],
        'all_material_requirements_enumerated': True, 'material_requirements': [{
            'requirement_id': 'r1', 'satisfied': True, 'draft_covered': True,
            'evidence_invocation_ids': allowed}]}
    validate(value)
    value['material_requirements'][0]['evidence_invocation_ids'] = ['inspection-receipt']
    with pytest.raises(ValidationError):
        validate(value)


def test_citation_catalog_excludes_inspections_failures_and_scope_receipts():
    from nullion.agent_orchestrator import _completion_review_citation_catalog
    from nullion.tools import ToolResult
    records = [ToolResult('original-source', 'opaque_read', 'completed', {'value': 7}),
        ToolResult('inspection', 'inspect_retained_evidence', 'completed', {'value': 7}),
        ToolResult('scope', 'request_tool_scope', 'completed', {'value': 7}),
        ToolResult('failed', 'opaque_read', 'failed', {'value': 7}),
        ToolResult('denied', 'opaque_read', 'denied', {'value': 7})]
    assert _completion_review_citation_catalog(records, {'e0001': 'original-source'}) == [
        {'invocation_id': 'e0001', 'tool_name': 'opaque_read'}]


@pytest.mark.parametrize('label', ['選択した処理を設定する', 'تهيئة المهمة المحددة'])
@pytest.mark.parametrize('scheduled', [False, True])
def test_requirement_schema_checks_owner_tools_and_constraint_delivery(label, scheduled):
    schema = _completion_requirement_response_schema(['opaque_accept'], scheduler_owns_delivery=scheduled)
    Draft202012Validator.check_schema(schema)
    validate = Draft202012Validator(schema).validate
    value = {'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
        'material_requirements': [{'requirement': label, 'completion_owner': 'producer',
            'delivery_required': True, 'requirement_kind': 'action', 'evidence_subject_id': 'setup',
            'artifact_member_subject_ids': [], 'eligible_evidence_tool_names': ['opaque_accept']}]}
    validate(value)
    bad = copy.deepcopy(value)
    bad['material_requirements'][0]['eligible_evidence_tool_names'] = ['unregistered']
    with pytest.raises(ValidationError): validate(bad)
    bad = copy.deepcopy(value)
    bad['material_requirements'][0]['delivery_required'] = False
    with pytest.raises(ValidationError): validate(bad)
    bad['material_requirements'][0]['requirement_kind'] = 'constraint'
    validate(bad)
    transport = copy.deepcopy(value)
    transport['material_requirements'][0].update(completion_owner='cron_delivery', eligible_evidence_tool_names=[])
    if scheduled:
        validate(transport)
        transport['material_requirements'][0]['requirement_kind'] = 'fact'
        with pytest.raises(ValidationError): validate(transport)
    else:
        with pytest.raises(ValidationError): validate(transport)


def test_no_tool_reasoned_requirement_schema_is_valid_json_schema():
    schema = _completion_requirement_response_schema([])
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate({'all_material_requirements_enumerated': True,
        'requested_artifact_extensions': [], 'material_requirements': [{
            'requirement': '説明', 'completion_owner': 'producer', 'delivery_required': True,
            'requirement_kind': 'reasoned_answer', 'evidence_subject_id': 'answer',
            'artifact_member_subject_ids': [], 'eligible_evidence_tool_names': []}]})
