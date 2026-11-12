"""Only certified findings and typed outcome state can become public copy."""
import json
from dataclasses import replace

import pytest
from nullion.agent_orchestrator import (_CompletionReviewDecision, _CompletionReviewMaterialRequirement,
    _CompletionReviewSupportedFinding, _completion_review_open_task_reply,
    _completion_review_request_context, _parse_completion_review_decision)

NOTES = ['Remove the unsupported draft explanation and write a corrected answer.',
         'Elimina la explicación sin pruebas y vuelve a redactar la respuesta.',
         '下書きの未確認の説明を削除して書き直してください。', 'أزل الشرح غير المدعوم من المسودة.']


def review(note, *, kind='fact', satisfied=True):
    return _CompletionReviewDecision('retry', (note,), all_material_requirements_enumerated=True,
        user_facing_failure=note, source_presentation_valid=False,
        material_requirements=(_CompletionReviewMaterialRequirement('r','Internal scope',kind,satisfied,False,('source',)),),
        supported_findings=(_CompletionReviewSupportedFinding('Selected hall: 20:30; 3 of 30 seats available.',('source',)),))


@pytest.mark.parametrize('note',NOTES)
@pytest.mark.parametrize('disposition',['retry','blocked'])
def test_editing_diagnostics_never_become_a_footer_when_requested_facts_are_satisfied(note,disposition):
    decision=replace(review(note),disposition=disposition)
    assert _completion_review_open_task_reply(decision)=='Selected hall: 20:30; 3 of 30 seats available.'
    assert decision.disposition==disposition  # publication is not a completion grant
    assert decision.user_facing_failure==note and decision.unresolved_requirements==(note,)


@pytest.mark.parametrize('note',NOTES)
@pytest.mark.parametrize('kind,notice',[
    ('fact','Some requested information remains unconfirmed.'),
    ('action','The remaining requested actions have not been confirmed.'),
    ('deliverable','The requested output is not yet complete.')])
def test_genuine_gaps_use_typed_copy_and_preserve_every_verified_detail(note,kind,notice):
    decision=review(note,kind=kind,satisfied=False)
    assert _completion_review_open_task_reply(decision)==decision.supported_findings[0].text+'\n\n'+notice


@pytest.mark.parametrize('note',NOTES)
@pytest.mark.parametrize('disposition',['needs_user_input','needs_approval','blocked'])
def test_no_findings_and_malformed_clarifications_never_dump_internal_checklists(note,disposition):
    decision=_CompletionReviewDecision(disposition,(note,),user_facing_failure=note)
    message=_completion_review_open_task_reply(decision)
    assert note not in message
    if disposition in {'needs_user_input','needs_approval'}:
        assert '1. ' in message and '2. ' in message


def test_real_numbered_clarification_keeps_its_question_and_numeric_answers():
    decision=_CompletionReviewDecision('needs_user_input',('internal',),user_facing_failure='private',
        clarification_question='Which source should I use?',clarification_options=('Selected source','Choose another'))
    assert _completion_review_open_task_reply(decision)=='Which source should I use?\n\n1. Selected source\n2. Choose another'


def test_runtime_timeout_is_preserved_without_publishing_model_diagnostics():
    decision=replace(review('private correction'),runtime_failure_code='deadline')
    assert _completion_review_open_task_reply(decision).endswith('before the response time limit.')
    payload={'disposition':'blocked','unresolved_requirements':['private'],
        'user_facing_failure':'private','runtime_failure_code':'deadline'}
    parsed=_parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(payload)}]},registered_tool_names=set())
    assert parsed is None or parsed.runtime_failure_code is None


def test_review_outage_cannot_publish_uncertified_findings():
    decision=replace(review('private'),review_error='invalid_citations')
    assert 'Selected hall' not in _completion_review_open_task_reply(decision)


def test_request_clock_is_typed_context_shared_with_review_without_parsing_profile_prose():
    clock={'timezone':'Pacific/Honolulu','accepted_at_utc':'2026-11-01T01:10:00+00:00',
        'accepted_local_datetime':'2026-10-31T15:10:00-10:00','accepted_local_date':'2026-10-31'}
    context=_completion_review_request_context({'tool_flow_context':{'request_time_context':clock,
        'user_profile_context':'Opaque profile context'}})
    assert context['request_time_context']==clock
    assert _completion_review_request_context({'tool_flow_context':{}})=={}


def test_saved_task_execution_uses_its_run_clock_instead_of_old_creation_clock():
    context=_completion_review_request_context({'tool_flow_context':{'scheduled_task_run':True,
        'request_time_context':{'accepted_local_date':'2024-01-01'},
        'scheduled_execution_context':{'local_datetime':'2028-05-04T08:00:00+09:00'}}})
    assert 'request_time_context' not in context
    assert context['scheduled_execution_context']['local_datetime']=='2028-05-04T08:00:00+09:00'


@pytest.mark.parametrize('disposition',['needs_user_input','needs_approval'])
def test_clarification_and_approval_do_not_erase_certified_findings(disposition):
    decision=replace(review('private editing instructions'),disposition=disposition,
        clarification_question='Which target?',clarification_options=('First target','Another target'))
    answer=_completion_review_open_task_reply(decision)
    assert answer.startswith(decision.supported_findings[0].text)
    assert '1. ' in answer and '2. ' in answer and 'private editing instructions' not in answer


@pytest.mark.parametrize('zone,instant,expected_date',[
    ('Pacific/Honolulu','2026-11-01T01:10:00+00:00','2026-10-31'),
    ('Pacific/Kiritimati','2026-10-31T22:10:00+00:00','2026-11-01'),
    ('UTC','2026-11-01T01:10:00+00:00','2026-11-01')])
def test_request_clock_handles_both_sides_of_utc_midnight(zone,instant,expected_date):
    from datetime import datetime
    from nullion.preferences import Preferences,build_request_time_context
    clock=build_request_time_context(Preferences(timezone=zone),now=datetime.fromisoformat(instant))
    assert clock['timezone']==zone and clock['accepted_local_date']==expected_date
    assert clock['accepted_at_utc']==instant


@pytest.mark.parametrize('relevant,satisfied,disposition,expected',[
    (True,False,'blocked',True), (False,False,'blocked',False),
    (True,True,'retry',False), (True,False,'complete',False)])
def test_actionable_connection_notice_requires_a_relevant_failed_source_receipt(relevant,satisfied,disposition,expected):
    from nullion.agent_orchestrator import (_CompletionRequirementContractRow,
        _completion_review_with_runtime_failure)
    from nullion.tools import ToolResult
    decision=replace(review('Private diagnostic',satisfied=satisfied),disposition=disposition)
    contract=(_CompletionRequirementContractRow('r','Read selected data','fact',('selected_read',)),)
    failure=ToolResult('failed','selected_read' if relevant else 'optional_read','failed',
        {'reason':'source_connection_required'},'Untrusted raw error payload')
    actual=_completion_review_with_runtime_failure(decision,requirement_contract=contract,tool_results=[failure])
    assert (actual.runtime_failure_code=='source_connection_required') is expected
    answer=_completion_review_open_task_reply(actual)
    assert ('Reconnect it before retrying.' in answer) is expected
    assert 'Private diagnostic' not in answer and 'Untrusted raw error payload' not in answer
    assert actual.supported_findings==decision.supported_findings


@pytest.mark.parametrize('status,reason,code',[
    ('failed','browser_action_failed','browser_action_failed'),
    ('failed','handler_timeout','browser_timeout'),
    ('failed','adapter_failure','browser_operation_failed'),
    ('denied','permission_required','action_denied')])
def test_required_failed_operations_retain_typed_causes_without_error_payloads(status,reason,code):
    from nullion.agent_orchestrator import _completion_review_with_runtime_failure
    from nullion.tools import ToolResult
    decision=_CompletionReviewDecision('blocked',('private editor note',),user_facing_failure='private editor note')
    failure=ToolResult('failed','browser_navigate',status,{'reason':reason},'秘密のプロバイダー応答')
    actual=_completion_review_with_runtime_failure(decision,requirement_contract=(),tool_results=[failure],
        missing_required_tool_names=['browser_navigate'])
    assert actual.runtime_failure_code==code
    answer=_completion_review_open_task_reply(actual)
    assert 'private editor note' not in answer and '秘密' not in answer
    useful=review("private",satisfied=True)
    unrelated=_completion_review_with_runtime_failure(useful,requirement_contract=(),tool_results=[failure])
    assert unrelated.runtime_failure_code is None


@pytest.mark.parametrize('message',['Connection refused','接続できません','Connexion refusée'])
def test_source_adapter_exposes_network_failure_by_exception_type_not_message(message):
    import urllib.error
    from nullion.tools import _build_web_fetch_handler,ToolInvocation
    def fail(url,timeout):
        raise urllib.error.URLError(message)
    result=_build_web_fetch_handler(fail)(ToolInvocation(invocation_id='read',tool_name='web_fetch',
        principal_id='web:fixture',arguments={'url':'https://example.org/source'},capsule_id='fixture'))
    assert result.status=='failed' and result.output=={'reason':'network_error','error_type':'URLError'}
    from nullion.agent_orchestrator import _completion_review_with_runtime_failure
    decision=_CompletionReviewDecision('blocked',('Internal editing note',),user_facing_failure='Internal editing note')
    actual=_completion_review_with_runtime_failure(decision,requirement_contract=(),tool_results=[result])
    reply=_completion_review_open_task_reply(actual)
    assert 'network error' in reply and message not in reply and 'Internal editing note' not in reply


@pytest.mark.parametrize('status',['completed','failed'])
def test_dispatch_receipt_preserves_uncertain_result_without_claiming_success(status):
    from nullion.tools import ToolResult
    from nullion.agent_orchestrator import _completion_review_with_runtime_failure
    result=ToolResult('dispatch','opaque_operation',status,{'action_receipt':{'type':'action_receipt',
        'action':'started','object_type':'operation','summary':'Started selected operation','object_id':'selected'}})
    decision=_CompletionReviewDecision('blocked',('repair the final response',))
    actual=_completion_review_with_runtime_failure(decision,requirement_contract=(),tool_results=[result])
    assert actual.runtime_failure_code=='operation_pending' and actual.disposition=='blocked'
    assert 'dispatched' in _completion_review_open_task_reply(actual)
    assert 'unavailable' in _completion_review_open_task_reply(actual)


@pytest.mark.parametrize('kind,status,tool,allowed',[
    ('limitation','failed','opaque_source',True),
    ('limitation','denied','opaque_action',True),
    ('finding','failed','opaque_source',False),
    ('limitation','failed','inspect_retained_evidence',False),
    ('limitation','denied','request_tool_scope',False)])
def test_failure_citations_explain_limitations_but_never_certify_source_success(kind,status,tool,allowed):
    from nullion.agent_orchestrator import _forced_completion_review_is_grounded
    from nullion.tools import ToolResult
    receipt=ToolResult('current-operation',tool,status,{'reason':'adapter_unavailable'})
    decision=_CompletionReviewDecision('blocked',('Internal editor note',),
        supported_findings=(_CompletionReviewSupportedFinding('The selected operation is unavailable.',
            ('current-operation',),kind),))
    assert _forced_completion_review_is_grounded(decision,tool_results=[receipt],requirement_contract=()) is allowed
    assert not _forced_completion_review_is_grounded(replace(decision,disposition='complete'),
        tool_results=[receipt],requirement_contract=())
    assert not _forced_completion_review_is_grounded(decision,tool_results=[],requirement_contract=())


def test_public_limitation_schema_requires_its_explicit_role_for_failed_citations():
    from jsonschema import validate,ValidationError
    from nullion.agent_orchestrator import _completion_review_response_schema
    schema=_completion_review_response_schema((),citable_evidence_ids=(),limitation_evidence_ids=('failure',))
    value={'disposition':'blocked','all_material_requirements_enumerated':True,'material_requirements':[],
        'unresolved_requirements':['Internal editor note'],'retry_tool_names':[],
        'user_facing_failure':'Internal editor note','supported_findings':[
            {'kind':'limitation','text':'The selected source is unavailable.','presentation':'paragraph','evidence_invocation_ids':['failure']}]}
    validate(value,schema)
    parsed=_parse_completion_review_decision({'content':[{'type':'text','text':json.dumps(value)}]},registered_tool_names=set())
    assert parsed.supported_findings[0].kind=='limitation'
    assert 'source is unavailable' in _completion_review_open_task_reply(parsed)
    del value['supported_findings'][0]['kind']
    with pytest.raises(ValidationError):validate(value,schema)
