from dataclasses import replace

import pytest

from nullion.agent_orchestrator import _CompletionReviewDecision as Review
from nullion.review_assistance import assisted_reply,assisted_fulfillment,record_assistance


@pytest.mark.parametrize('draft',['観測値は7です。','El valor observado es 7.','القيمة المرصودة هي 7.'])
@pytest.mark.parametrize('review',[None,Review('blocked'),Review('retry'),
    Review('needs_user_input',clarification_question='Invented question',clarification_options=('A','B')),
    Review('blocked',review_error='invalid_json'),Review('blocked',review_error='timeout',review_timed_out=True)])
def test_critic_cannot_replace_a_usable_answer_with_status_or_its_own_question(draft,review):
    assert assisted_reply(draft,review)==draft
    assert assisted_fulfillment(None,review) is None
    assert assisted_fulfillment(False,review) is False
    metadata={};record_assistance(metadata,review)
    assert metadata['outcome_review_role']=='helper'


def test_unavailable_editor_is_internal_and_cannot_grant_execution():
    from nullion.workflow_reasoning import mark_review_unavailable,review_warning_reply
    metadata={};mark_review_unavailable(metadata,Review('blocked',review_error='timeout',review_timed_out=True))
    assert review_warning_reply('Observed value: 7.',metadata)=='Observed value: 7.'
    assert metadata['outcome_review_status']=='unavailable'
    assert not metadata.get('outcome_review_warning_rendered')
    assert assisted_fulfillment(False,Review('complete',execution_satisfied=True)) is False
    assert assisted_fulfillment(None,Review('complete',execution_satisfied=False)) is None


def test_material_shortlist_gap_is_specific_and_does_not_discard_the_answer():
    review = Review('blocked', runtime_failure_code='search_coverage_incomplete',
        search_coverage_errors=('comparison_frontier_unchecked',), search_coverage={
            'comparison_scope': {'frontier_candidate_ids': ['material']},
            'candidates': [{'candidate_id': 'material', 'disposition': 'pending'},
                *[{'candidate_id': str(i), 'disposition': 'pending'} for i in range(13)]]})
    reply = assisted_reply('Among checked options, A costs 9 cents/g.', review)
    assert reply.startswith('Among checked options')
    assert '1 shortlisted alternative still needs verification' in reply
    assert '13' not in reply
    assert assisted_reply(reply, review) == reply
    optional = replace(review, search_coverage_errors=('unobserved_candidate',))
    assert assisted_reply('The result.', optional) == 'The result.'


def test_attributed_operation_failure_keeps_findings_and_replaces_only_an_unsupported_draft():
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement
    review = Review('blocked', runtime_failure_code='browser_operation_failed',
        material_requirements=(Requirement('r', 'Selected result', 'fact', False, False, ()),))
    reply = assisted_reply('Unsupported draft.', review)
    assert 'Unsupported' not in reply and 'A required browser step failed.' in reply
    useful = replace(review, material_requirements=())
    reply = assisted_reply('Observed value: 7.', useful)
    assert 'Observed value: 7.' in reply and 'A required browser step failed.' in reply


def test_editor_can_apply_a_grounded_improvement():
    from tests.test_reviewed_presentation_correction import parse,correction_payload
    review=replace(parse(correction_payload()),execution_satisfied=True)
    assert assisted_reply('Verbose draft',review)==review.corrected_reply
    assert '$720' in assisted_reply('Verbose draft',review)
    assert assisted_fulfillment(None,review) is True


def test_a_bound_composer_answer_survives_an_operational_blocker_with_no_replacement_findings():
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement
    from nullion.execution_outcome import prepare_answer
    from nullion.tools import ToolResult
    draft='観測値は7です。'
    results=[ToolResult('read','opaque_read','completed',{'value':7})]
    prepared=prepare_answer('選択した値',draft,results)
    review=Review('blocked',runtime_failure_code='source_connection_required',
        material_requirements=(Requirement('r','Remaining observation','fact',False,False,()),))
    reply=assisted_reply(draft,review,prepared_answer=prepared)
    assert draft in reply and 'Reconnect' in reply
    assert 'Changed draft' not in assisted_reply('Changed draft',review,prepared_answer=prepared)


@pytest.mark.parametrize('draft',['ページを開けませんでした。接続が拒否されました。',
    'No se pudo abrir la página: conexión rechazada.', 'The page could not be opened: connection refused.'])
def test_an_already_described_failure_does_not_get_a_second_generic_warning(draft):
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement
    review=Review('blocked',runtime_failure_code='browser_operation_failed',
        material_requirements=(Requirement('r','Selected page','fact',False,True,()),))
    assert assisted_reply(draft,review)==draft


def test_grounded_question_keeps_findings_and_supports_numbered_followup_state():
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement
    review=Review('needs_user_input',execution_satisfied=False,
        material_requirements=(Requirement('r','対象の日付','fact',False,False,('current-source',)),),
        clarification_question='日付を選んでください。',clarification_options=('次の期間','元の日付'))
    body=assisted_reply('観測値は7です。',review)
    assert '観測値は7です。' in body and '1.' in body and '2.' in body
    metadata={};record_assistance(metadata,review)
    assert metadata['needs_input'] is True


@pytest.mark.parametrize('kind', ['user_input', 'fact', 'action'])
def test_only_typed_missing_user_input_can_ask_without_source_receipts(kind):
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement
    review = Review('needs_user_input', execution_satisfied=False,
        material_requirements=(Requirement('r', '作成前の確認', kind, False, False, ()),),
        clarification_question='指定した条件で作成しますか？', clarification_options=('作成する', '作成しない'))
    draft = 'Unsupported claim that setup completed.'
    metadata = {}; record_assistance(metadata, review)
    body = assisted_reply(draft, review)
    if kind == 'user_input':
        assert draft not in body and review.clarification_question in body
        assert '1.' in body and '2.' in body and metadata['needs_input'] is True
    else:
        assert body == draft and not metadata.get('needs_input')


def test_critique_without_execution_progress_cannot_extend_the_producer_budget():
    from nullion.agent_orchestrator import _completion_review_retry_progress_update
    for count in (2,100):
        assert _completion_review_retry_progress_update({'completion_review_count':count},Review('retry')) is None


@pytest.mark.parametrize('plan', [None, {'kind':'critique'},
    {'kind':'execution_improvement','task_ids':['foreign'],'tool_names':['read']},
    {'kind':'execution_improvement','task_ids':['owned'],'tool_names':['write']},
    {'kind':'execution_improvement','task_ids':['owned'],'tool_names':['read'],'approval':True}])
def test_critique_cannot_expand_task_or_tool_scope(plan):
    from types import SimpleNamespace
    from nullion.review_assistance import validated_execution_improvement
    assert validated_execution_improvement(plan,[SimpleNamespace(task_id='owned',allowed_tools=['read'])]) is None


def test_owned_execution_plan_keeps_exact_selected_scope():
    from types import SimpleNamespace
    from nullion.review_assistance import validated_execution_improvement
    plan={'kind':'execution_improvement','task_ids':['owned'],'tool_names':['read']}
    assert validated_execution_improvement(plan,[SimpleNamespace(task_id='owned',allowed_tools=['read'])])==plan


def test_partial_helper_selection_cannot_discard_the_larger_producer_answer():
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement, _CompletionReviewSupportedFinding as Finding
    draft='\n'.join('Result '+str(index) for index in range(13))
    rows=tuple(Requirement('r'+str(i),'対象','fact',True,True,('source',)) for i in range(13))
    rows+= (Requirement('missing','未取得','fact',False,False,()),)
    review=Review('blocked',material_requirements=rows,supported_findings=(Finding('One extra fact',('source',),draft_relation='additional'),))
    reply=assisted_reply(draft,review)
    assert all('Result '+str(i) in reply for i in range(13))
    assert 'One extra fact' in reply


@pytest.mark.parametrize('draft,alternate,limitation', [
    ('要点\n項目A: 7\n項目B: 8', '要点\n項目Aは7です\n項目Bは8です', '予定表に重複が残っています。'),
    ('Resumen\nA: 7\nB: 8', 'Resumen\nA tiene 7\nB tiene 8', 'Queda un recordatorio duplicado.'),
])
@pytest.mark.parametrize('disposition', ['retry', 'blocked'])
@pytest.mark.parametrize('message_format', [None, 'nullion.report.v1'])
def test_reworded_full_report_is_not_appended_to_primary_answer(draft, alternate, limitation, disposition, message_format):
    from nullion.agent_orchestrator import _CompletionReviewMaterialRequirement as Requirement, _CompletionReviewSupportedFinding as Finding
    rows=(Requirement('facts','結果','fact',True,True,('read',)),
          Requirement('action','操作','action',False,False,('write',)))
    review=Review(disposition,material_requirements=rows,supported_findings=(
        Finding(alternate,('read',),message_format=message_format),
        Finding(limitation,('write',),kind='limitation')))
    answer=assisted_reply(draft,review)
    assert answer.count(draft)==1 and alternate not in answer
    assert answer.count(limitation)==1
    assert assisted_reply(answer,review)==answer


def test_a_whole_structured_report_cannot_be_appended_even_when_mislabeled_additional():
    from nullion.agent_orchestrator import _CompletionReviewSupportedFinding as Finding
    review=Review('blocked',supported_findings=(Finding('Another whole report',('read',),
        draft_relation='additional',message_format='nullion.report.v1'),))
    assert assisted_reply('Original useful report',review)=='Original useful report'


def test_already_covered_fact_is_not_appended():
    from nullion.agent_orchestrator import _CompletionReviewSupportedFinding as Finding
    review=Review('blocked',supported_findings=(Finding('A rephrased fact',('read',),draft_relation='already_covered'),))
    assert assisted_reply('Original useful report',review)=='Original useful report'


def test_supported_partial_findings_remain_available_when_producer_has_no_answer():
    from nullion.agent_orchestrator import _CompletionReviewSupportedFinding as Finding
    review=Review('blocked',supported_findings=(Finding('観測値は7です。',('read',)),))
    assert assisted_reply('',review)=='観測値は7です。'


def test_parser_treats_a_structured_report_as_replacement_even_if_provider_marks_additional():
    from tests.test_reviewed_presentation_correction import correction_payload, parse
    payload=correction_payload();payload['supported_findings'][0]['draft_relation']='additional'
    assert parse(payload).supported_findings[0].draft_relation=='replacement'
    payload['supported_findings'][0]['draft_relation']='unrecognized'
    assert parse(payload) is None


def test_cosmetic_report_fallback_cannot_be_appended_as_a_narrow_limitation():
    from nullion.agent_orchestrator import _CompletionReviewSupportedFinding as Finding
    review=Review('blocked',supported_findings=(Finding('Whole fallback report',('read',),
        kind='limitation',draft_relation='additional',is_report=True),))
    assert assisted_reply('Original useful report',review)=='Original useful report'


def test_parser_preserves_report_identity_after_cosmetic_layout_failure():
    from tests.test_reviewed_presentation_correction import correction_payload, parse
    payload=correction_payload();payload.update(disposition='blocked',reply_target='draft')
    row=payload['supported_findings'][0]
    row['draft_relation']='additional';row['text']['message']['sections'][0]['layout']='unsupported'
    review=parse(payload)
    assert review.supported_findings[0].message_format is None
    assert review.supported_findings[0].is_report is True
    assert review.supported_findings[0].draft_relation=='replacement'


def test_completed_batch_errors_support_a_limitation_but_cannot_certify_success():
    from nullion.agent_orchestrator import _completion_review_limitation_evidence_ids,_completion_review_evidence_result_is_positive
    from nullion.tools import ToolResult
    row=ToolResult('batch','opaque_batch','completed',{'items':[{'value':7}],
        'errors':[{'record_id':'missing','error':'Source unavailable'}]})
    assert _completion_review_limitation_evidence_ids([row])=={'batch'}
    assert not _completion_review_evidence_result_is_positive(row)


@pytest.mark.parametrize('complete',[True,False])
def test_grounded_composition_improvement_preserves_producer_attachment_selection(complete):
    from nullion.agent_orchestrator import _CompletionReviewSupportedFinding as Finding
    if complete:
        from tests.test_reviewed_presentation_correction import parse,correction_payload
        review=replace(parse(correction_payload()),execution_satisfied=True)
    else:
        review=Review('blocked',source_presentation_valid=False,
            supported_findings=(Finding('One selected source is unavailable.',('failed-source',),kind='limitation'),))
    reply=assisted_reply('The file is attached.\nMEDIA:/owned/artifacts/result.csv',review)
    assert reply.count('MEDIA:/owned/artifacts/result.csv')==1
    assert ('One selected source is unavailable.' in reply) if not complete else ('$720' in reply)


def test_helper_composition_uses_only_the_current_selected_artifact_revision():
    review=Review('blocked')
    reply=assisted_reply('Selected result.\nMEDIA:/owned/old.csv',review,
        artifact_paths=['/owned/current.csv'])
    assert 'Selected result.' in reply and 'MEDIA:/owned/current.csv' in reply
    assert '/owned/old.csv' not in reply
