import pytest
from types import SimpleNamespace
from nullion.agent_orchestrator import _CompletionReviewDecision, _CompletionReviewMaterialRequirement
from nullion.cron_delivery import _apply_cron_alert_delivery_policy, _retain_verified_cron_alert_decision
from nullion.tools import ToolResult


def fixture():
    job = SimpleNamespace(id='job-1', task='選択した条件だけ通知。', delivery_channel='telegram', delivery_target='123',
        artifact_delivery_options={'alert_delivery_mode': 'verified_matches_only'})
    result = {'text': 'Verified current condition.', 'response_fulfilled': True,
        'tool_results': [ToolResult('read', 'collection_read', 'completed', {'value': 37})],
        'cron_run_time_context': {'current_datetime': '2028-04-09T12:00:00Z'},
        'cron_prior_delivery_history': {'no_prior_delivered_messages': True},
        'cron_known_user_context': 'Collection A', 'cron_source_action_contract': {}}
    review = _CompletionReviewDecision('complete', all_material_requirements_enumerated=True,
        material_requirements=(_CompletionReviewMaterialRequirement('r1', 'Verified condition', 'fact', True, True, ('read',)),),
        scheduled_alert_delivery=('send', 'verified_match'))
    _retain_verified_cron_alert_decision(job, result, review)
    return job, result


def test_valid_bound_alert_needs_no_second_model_decision():
    job, result = fixture()
    _apply_cron_alert_delivery_policy(job, result, model_client=None)
    assert result['cron_alert_delivery_decision'] == 'send'
    assert result['response_fulfilled'] is True and not result.get('cron_report_failed')


def test_silent_unverified_check_is_not_successful_just_because_it_retained_text():
    from nullion.cron_delivery import _attach_cron_execution_outcome
    job, result = fixture()
    result.update(cron_delivery_status='silent', cron_alert_delivery_decision='silent',
        cron_alert_delivery_reason='insufficient_evidence', cron_completion_required=True,
        cron_report_failed=True, response_fulfilled=False)
    _attach_cron_execution_outcome({'job': job}, result)
    assert result['execution_outcome']['execution_succeeded'] is False


def test_unknown_producer_completion_is_not_promoted_by_successful_text_delivery():
    from nullion.cron_delivery import _attach_cron_execution_outcome
    job, result = fixture()
    result.update(response_fulfilled=None, tool_results=[], cron_delivery_status='sent')
    _attach_cron_execution_outcome({'job':job},result)
    assert result['execution_outcome']['delivery_succeeded'] is True
    assert result['execution_outcome']['execution_succeeded'] is False


@pytest.mark.parametrize('change', ['draft', 'source', 'request', 'target', 'history', 'clock', 'context', 'scope', 'forged'])
def test_changed_or_forged_review_never_reuses_alert_authority(change):
    job, result = fixture()
    if change == 'draft': result['text'] = 'A different unsupported claim.'
    elif change == 'source': result['tool_results'][0].output['value'] = 38
    elif change == 'request': job.task = '別の条件。'
    elif change == 'target': job.delivery_target = 'another-recipient'
    elif change == 'history': result['cron_prior_delivery_history'] = {'no_prior_delivered_messages': False}
    elif change == 'clock': result['cron_run_time_context'] = {'current_datetime': '2028-04-10T12:00:00Z'}
    elif change == 'context': result['cron_known_user_context'] = 'Collection B'
    elif change == 'scope': result['cron_source_action_contract'] = {'missing_callable_required_tool_names': ['other_read']}
    else: result['_verified_cron_alert_decision'] = {'decision': 'send', 'reason': 'verified_match'}
    _apply_cron_alert_delivery_policy(job, result, model_client=None)
    assert result['cron_alert_delivery_decision'] == 'silent'
    assert result['response_fulfilled'] is False and result['cron_report_failed'] is True


@pytest.mark.parametrize('intent', [[], {'decision': [], 'reason': 'verified_match'},
    {'decision': 'send', 'reason': 'no_verified_match'}, {'decision': 'silent', 'reason': 'verified_match'},
    {'decision': 'send', 'reason': 'verified_match', 'extra': True}])
def test_malformed_alert_intent_is_a_protocol_error(intent):
    import json
    from nullion.agent_orchestrator import _parse_completion_review_decision
    response = {'content': [{'type': 'text', 'text': json.dumps({
        'disposition': 'complete', 'scheduled_alert_delivery': intent})}]}
    assert _parse_completion_review_decision(response, registered_tool_names=set()) is None


def test_invalid_review_reports_only_validation_codes():
    import json
    from nullion.agent_orchestrator import _parse_completion_review_decision
    errors = []
    response = {'content': [{'type': 'text', 'text': json.dumps({
        'disposition': 'complete', 'scheduled_alert_delivery': {'decision':'silent','reason':'private-source-text'}})}]}
    assert _parse_completion_review_decision(response, registered_tool_names=set(), diagnostics=errors) is None
    assert errors == ['invalid_scheduled_alert_delivery']


@pytest.mark.parametrize('status', ['incomplete', 'failed', 'cancelled'])
def test_incomplete_transport_never_certifies_valid_review_json(status):
    import json
    from nullion.agent_orchestrator import _parse_completion_review_decision
    errors = []
    response = {'content':[{'type':'text','text':json.dumps({'disposition':'complete'})}],
        'completion':{'status':status}}
    assert _parse_completion_review_decision(response, registered_tool_names=set(), diagnostics=errors) is None
    assert errors == ['incomplete_response']


def test_review_prompt_schema_is_valid_json_schema_with_typed_alert_pairs():
    from jsonschema import Draft202012Validator, ValidationError
    from nullion.agent_orchestrator import _completion_review_response_schema
    schema = _completion_review_response_schema([SimpleNamespace(requirement_id='r1')], scheduled_alerts_only=True)
    Draft202012Validator.check_schema(schema)
    payload = {'disposition':'complete', 'unresolved_requirements':[], 'retry_tool_names':[],
        'all_material_requirements_enumerated':True, 'material_requirements':[
            {'requirement_id':'r1','satisfied':True,'draft_covered':True,'evidence_invocation_ids':['e0000']}],
        'scheduled_alert_delivery':{'decision':'send','reason':'verified_match'}}
    validator = Draft202012Validator(schema);validator.validate(payload)
    payload['scheduled_alert_delivery']['reason'] = 'no_verified_match'
    with pytest.raises(ValidationError):validator.validate(payload)
    payload['scheduled_alert_delivery'] = None
    with pytest.raises(ValidationError):validator.validate(payload)
    payload.update(disposition='retry',unresolved_requirements=['未完了'], user_facing_failure='確認できた結果は7。残りの確認が必要です。')
    validator.validate(payload)


def test_review_schema_allows_only_current_bounded_retained_inspection_before_final_review():
    from jsonschema import Draft202012Validator, ValidationError
    from nullion.agent_orchestrator import _completion_review_response_schema
    requirements = [SimpleNamespace(requirement_id='r1')]
    schema = _completion_review_response_schema(requirements, inspection_invocation_ids=['e0001'])
    Draft202012Validator.check_schema(schema)
    request = {'disposition':'inspect_evidence', 'evidence_requests':[
        {'invocation_id':'e0001','json_pointer':'/message/body','offset':0,'length':12000}]}
    validator = Draft202012Validator(schema)
    validator.validate(request)
    request['evidence_requests'][0]['invocation_id'] = 'another-request'
    with pytest.raises(ValidationError):validator.validate(request)
    request['evidence_requests'][0].update(invocation_id='e0001', length=12001)
    with pytest.raises(ValidationError):validator.validate(request)
    request['evidence_requests'][0]['length'] = 12000
    final_schema = _completion_review_response_schema(requirements)
    with pytest.raises(ValidationError):Draft202012Validator(final_schema).validate(request)


@pytest.mark.parametrize('status', [[], {}, 17])
def test_malformed_completion_metadata_is_rejected_without_raising(status):
    import json
    from nullion.agent_orchestrator import _parse_completion_review_decision
    errors = []
    response = {'content':[{'type':'text','text':json.dumps({'disposition':'complete'})}],
        'completion':{'status':status}}
    assert _parse_completion_review_decision(response, registered_tool_names=set(), diagnostics=errors) is None
    assert errors == ['invalid_completion_status']
