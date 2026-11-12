import json
from dataclasses import replace

import pytest

from nullion.agent_orchestrator import (_CompletionRequirementContractRow as Contract,
    _parse_completion_review_decision, _publishable_review_correction)
from nullion.chat_text import REPORT_MESSAGE_FORMAT, render_report_message
from tests.report_presentation_fixture import report_fixture


def correction_payload():
    message, _, _ = report_fixture()
    return {'disposition': 'complete', 'reply_target': 'corrected_findings',
        'source_presentation_valid': True, 'all_material_requirements_enumerated': True,
        'unresolved_requirements': [], 'retry_tool_names': [],
        'material_requirements': [{'requirement_id': 'r', 'satisfied': True,
            'draft_covered': True, 'evidence_invocation_ids': ['source']}],
        'supported_findings': [{'text': {'format': REPORT_MESSAGE_FORMAT, 'message': message},
            'presentation': 'paragraph', 'requirement_ids': ['r'], 'evidence_invocation_ids': ['source']}]}


def parse(payload):
    errors = []
    result = _parse_completion_review_decision({'content': [{'type': 'text', 'text': json.dumps(payload)}]},
        registered_tool_names={'opaque_read'},
        requirement_contract=(Contract('r', '選択した情報を報告', 'fact', ('opaque_read',)),),
        diagnostics=errors)
    return result


def test_corrected_report_uses_existing_compact_contract_and_needs_runtime_grounding():
    payload = correction_payload()
    review = parse(payload)
    assert review is not None
    assert review.corrected_reply == render_report_message(payload['supported_findings'][0]['text']['message'])
    assert '$720' in review.corrected_reply and '$760' in review.corrected_reply
    assert 'Historical detail' not in review.corrected_reply
    assert _publishable_review_correction(review) is None
    grounded = replace(review, execution_satisfied=True)
    assert _publishable_review_correction(grounded) == review.corrected_reply
    for changed in [replace(grounded, disposition='blocked'), replace(grounded, execution_satisfied=False),
            replace(grounded, runtime_failure_code='action_denied'), replace(grounded, review_error='bad_evidence')]:
        assert _publishable_review_correction(changed) is None


def test_caption_cites_relevant_facts_without_repeating_file_execution_proof():
    payload = correction_payload()
    payload['material_requirements'][0]['evidence_invocation_ids'].append('second-source')
    payload['material_requirements'].extend([
        {'requirement_id': identity, 'satisfied': True, 'draft_covered': True,
         'evidence_invocation_ids': ['file-inspection']} for identity in ('file', 'layout')])
    contract = (Contract('r', '要約', 'fact', ('opaque_read',)),
        Contract('file', 'Archivo', 'deliverable', ('file_write',)),
        Contract('layout', '形式', 'constraint', ('file_read',), artifact_member_subject_ids=('file',)))
    errors = []
    parsed = _parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
        registered_tool_names={'opaque_read','file_write','file_read'}, requirement_contract=contract, diagnostics=errors)
    assert parsed and parsed.corrected_reply, errors
    assert _publishable_review_correction(parsed) is None
    # Presentation acceptance never substitutes for independent execution/file grounding.
    assert _publishable_review_correction(replace(parsed, execution_satisfied=False)) is None
    for row in payload['material_requirements'][1:]:
        row['satisfied'] = False
        assert _parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
            registered_tool_names={'opaque_read','file_write','file_read'}, requirement_contract=contract) is None
        row['satisfied'] = True


@pytest.mark.parametrize('defect', ['unknown_requirement', 'missing_requirement', 'wrong_evidence',
    'unmet', 'uncovered', 'blocking_failure', 'legacy_prose', 'malformed_layout', 'retry', 'invalid_presentation'])
def test_correction_cannot_promote_missing_or_unbound_work(defect):
    payload = correction_payload(); row = payload['supported_findings'][0]
    requirement = payload['material_requirements'][0]
    if defect == 'unknown_requirement': row['requirement_ids'].append('stale')
    elif defect == 'missing_requirement': row['requirement_ids'] = []
    elif defect == 'wrong_evidence': row['evidence_invocation_ids'] = ['unrelated']
    elif defect == 'unmet': requirement['satisfied'] = False
    elif defect == 'uncovered': requirement['draft_covered'] = False
    elif defect == 'blocking_failure': requirement['blocking_failure_invocation_ids'] = ['failed']
    elif defect == 'legacy_prose': row['text'] = 'Verified facts but no corrected content contract.'
    elif defect == 'malformed_layout': row['text']['message']['sections'][0]['layout'] = 'unsupported'
    elif defect == 'retry': payload.update(disposition='retry',unresolved_requirements=['A requested result remains unconfirmed.'])
    else: payload['source_presentation_valid'] = False
    if defect == 'retry':
        partial=parse(payload)
        assert partial and partial.disposition=='retry' and partial.corrected_reply is None
        assert partial.supported_findings
    else:assert parse(payload) is None


def test_corrected_complete_report_can_retain_a_caveat_without_using_it_as_proof():
    payload=correction_payload()
    payload['supported_findings'].append({'kind':'limitation','text':'Optional finish color was not recorded.',
        'evidence_invocation_ids':['source']})
    complete=parse(payload)
    assert complete and complete.corrected_reply and 'Optional finish color' in complete.corrected_reply
    payload['supported_findings']=payload['supported_findings'][1:]
    assert parse(payload) is None


def test_legacy_review_and_typed_partial_findings_still_preserve_their_verdict():
    payload = correction_payload(); payload.pop('reply_target')
    payload['disposition'] = 'retry'; payload['source_presentation_valid'] = False
    payload['unresolved_requirements'] = ['Presentation still needs correction.']
    review = parse(payload)
    assert review.disposition == 'retry' and review.corrected_reply is None
    assert 'Historical detail' not in review.supported_findings[0].text
    payload['supported_findings'][0]['text'] = 'Uno; dos. 1. Texto literal. 2. Texto literal.'
    assert parse(payload).supported_findings[0].text == payload['supported_findings'][0]['text']


@pytest.mark.parametrize('changed', ['request', 'evidence', 'reply', 'file', 'selection'])
def test_replacement_review_cache_reuses_only_the_same_outcome(tmp_path, monkeypatch, changed):
    from nullion import agent_orchestrator as agent
    from nullion.tools import ToolResult
    review = replace(parse(correction_payload()), execution_satisfied=True)
    artifact = tmp_path/'report.txt'; artifact.write_text('version one')
    state = {'user_message': '対象を報告。', 'principal_id': 'web:one', 'conversation_id': 'web:one',
             'tool_flow_context': {}, 'tool_results': [], 'artifacts': []}
    results = [ToolResult('source', 'opaque_read', 'completed', {'value': 7})]
    calls = []
    def perform(*args, **kwargs):
        calls.append(True)
        return review
    monkeypatch.setattr(agent, '_perform_completion_review', perform)
    paths = [str(artifact)]
    first = agent._review_risky_agent_completion(state, final_text='Old draft', tool_results=results, artifacts=paths, force=True)
    assert first is review
    assert agent._review_risky_agent_completion(state, final_text=review.corrected_reply,
        tool_results=results, artifacts=paths, force=True) is review
    assert len(calls) == 1
    text = review.corrected_reply
    if changed == 'request': state['user_message'] += ' 別の対象。'
    elif changed == 'evidence': results[0].output['value'] = 8
    elif changed == 'reply': text += ' Additional claim.'
    elif changed == 'selection': state['source_answer_checkpoint'] = {'presentation': {'points': []}}
    else:
        import os
        stamp = artifact.stat()
        artifact.write_text('version two')
        os.utime(artifact, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    agent._review_risky_agent_completion(state, final_text=text, tool_results=results, artifacts=paths, force=True)
    assert len(calls) == 2


def test_planner_rejects_an_execution_prerequisite_with_no_possible_evidence():
    from nullion.agent_orchestrator import _parse_completion_requirement_contract, _completion_requirement_response_schema
    from jsonschema import validate, ValidationError
    payload = {'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
        'material_requirements': [{'requirement': '指定された実行条件', 'requirement_kind': 'constraint',
            'completion_owner': 'producer', 'delivery_required': False, 'evidence_subject_id': 'scope',
            'artifact_member_subject_ids': [], 'eligible_evidence_tool_names': []}]}
    response = lambda: {'content': [{'type': 'text', 'text': json.dumps(payload)}]}
    diagnostics = []
    assert not _parse_completion_requirement_contract(response(), registered_tool_names={'opaque_read'}, diagnostics=diagnostics)
    assert diagnostics == ['execution_constraint_without_evidence_path']
    with pytest.raises(ValidationError):
        validate(payload, _completion_requirement_response_schema({'opaque_read'}))
    payload['material_requirements'][0]['eligible_evidence_tool_names'] = ['opaque_read']
    validate(payload, _completion_requirement_response_schema({'opaque_read'}))
    assert _parse_completion_requirement_contract(response(), registered_tool_names={'opaque_read'})


def test_real_model_partial_correction_preserves_findings_without_completion():
    from pathlib import Path
    payload=json.loads((Path(__file__).parent/'fixtures/review_partial_correction.json').read_text())
    requirements=tuple(Contract(row['requirement_id'],'Recorded option '+row['requirement_id'],
        'reasoned_answer' if row['requirement_id']=='r4' else 'comparison_side',
        () if row['requirement_id']=='r4' else ('opaque_capture',)) for row in payload['material_requirements'])
    review=_parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
        registered_tool_names={'opaque_capture'},requirement_contract=requirements)
    assert review is not None and review.disposition=='retry' and review.review_error is None
    assert review.reply_target=='draft' and review.corrected_reply is None
    assert _publishable_review_correction(review) is None
    from nullion.review_publication import render_incomplete_review
    body=render_incomplete_review(review)
    assert all(amount in body for amount in ('$9.98','$9.88','$5.97'))
    assert 'Optional specification catalog' not in body and len(body.split())<300


@pytest.mark.parametrize('selected',[False,True])
def test_native_correction_does_not_require_a_nonexistent_source_selection(selected):
    payload=correction_payload();payload['source_presentation_valid']=None;errors=[]
    review=_parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
        registered_tool_names={'opaque_read'},requirement_contract=(Contract('r','要約','fact',('opaque_read',)),),
        source_answer_selection_present=selected,diagnostics=errors)
    if selected:
        assert review is None and 'corrected_source_selection_not_verified' in errors
    else:
        assert review and review.corrected_reply and review.disposition=='complete'
    payload['source_presentation_valid']=False
    assert _parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
        registered_tool_names={'opaque_read'},requirement_contract=(Contract('r','要約','fact',('opaque_read',)),),
        source_answer_selection_present=selected) is None


def test_correction_diagnostics_distinguish_missing_content_without_echoing_it():
    payload=correction_payload();payload['material_requirements'][0]['draft_covered']=False
    errors=[]
    assert _parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},
        registered_tool_names={'opaque_read'},requirement_contract=(Contract('r','PRIVATE REQUEST','fact',('opaque_read',)),),
        diagnostics=errors) is None
    assert 'corrected_required_outcome_not_covered' in errors
    assert all('PRIVATE REQUEST' not in value for value in errors)


@pytest.mark.parametrize('selected', [False, True])
@pytest.mark.parametrize('verdict', [None, False, True])
def test_review_response_schema_matches_correction_target_validation(selected, verdict):
    from jsonschema import Draft202012Validator, ValidationError
    from nullion.agent_orchestrator import _completion_review_response_schema
    requirements = (Contract('r', '選択した記録を訂正', 'fact', ('opaque_read',)),)
    payload = correction_payload()
    payload['source_presentation_valid'] = verdict
    schema = _completion_review_response_schema(requirements,
        source_answer_selection_present=selected)
    Draft202012Validator.check_schema(schema)
    parser_accepts = _parse_completion_review_decision(
        {'content': [{'type': 'text', 'text': json.dumps(payload)}]},
        registered_tool_names={'opaque_read'}, requirement_contract=requirements,
        source_answer_selection_present=selected) is not None
    if parser_accepts:
        Draft202012Validator(schema).validate(payload)
    else:
        with pytest.raises(ValidationError):
            Draft202012Validator(schema).validate(payload)
