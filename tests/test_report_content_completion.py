import json
from types import SimpleNamespace

import pytest

from nullion.report_completion import (MARKER, report_outcome_contract, generated_report_outcome,
    bind_report_outcome, partial_report_receipt)


def document(contract, status='complete', remaining=()):
    metadata = {'contract_id': contract['contract_id'], 'status': status,
        'all_material_requirements_enumerated': True, 'remaining_work': list(remaining)}
    return '<!doctype html><html><body>観測値 12<!--'+MARKER+json.dumps(metadata)+'--></body></html>'


@pytest.mark.parametrize('gap', ['現在の観測値が不足しています。', 'Faltan observaciones actuales.'])
def test_producer_reports_partial_without_english_intent_rules(gap):
    contract = report_outcome_contract({'request': '対象を確認', 'source': {'count': 2}})
    assert generated_report_outcome(document(contract, 'partial', [gap]), contract) == {
        'status': 'partial', 'remaining_work': [gap]}
    assert generated_report_outcome(document(contract), contract)['status'] == 'complete'


@pytest.mark.parametrize('failure', ['missing', 'foreign', 'duplicate', 'complete_with_gap', 'invalid_json'])
def test_invalid_or_absent_producer_receipt_never_certifies_completion(failure):
    contract = report_outcome_contract({'request': 'one'})
    doc = document(contract)
    if failure == 'missing': doc = '<!doctype html><html><body>one</body></html>'
    if failure == 'foreign': doc = document(report_outcome_contract({'request': 'other'}))
    if failure == 'duplicate': doc += doc
    if failure == 'complete_with_gap': doc = document(contract, remaining=['Missing current observations'])
    if failure == 'invalid_json': doc = '<!doctype html><html><!--'+MARKER+'{--></html>'
    assert generated_report_outcome(doc, contract)['status'] == 'unknown'


def test_partial_publication_receipt_is_bound_to_request_selection_and_bytes(tmp_path):
    path = tmp_path/'one.html'; path.write_text('<!doctype html><html><body>one</body></html>')
    receipt = bind_report_outcome('request', {'status': 'partial', 'remaining_work': ['Missing one source']}, [str(path)])
    assert partial_report_receipt('request', receipt, [str(path)])
    assert not partial_report_receipt('other request', receipt, [str(path)])
    assert not partial_report_receipt('request', receipt, [])
    path.write_text('<!doctype html><html><body>changed</body></html>')
    assert not partial_report_receipt('request', receipt, [str(path)])


@pytest.mark.parametrize('blocker', ['suspended_for_approval', 'cron_run_cancelled', 'raw_tool_payload_blocked',
    'cron_delivery_failed', 'cron_verification_failed', 'verified_match'])
def test_partial_report_does_not_bypass_execution_or_alert_boundaries(tmp_path, blocker):
    from nullion.cron_delivery import _complete_typed_html_artifact_recovery, _partial_cron_report_available
    path = tmp_path/'report.html'; path.write_text('<!doctype html><html><body>one</body></html>')
    result = {'response_fulfilled': False}
    job = SimpleNamespace(task='request', artifact_delivery_options={})
    _complete_typed_html_artifact_recovery(result, artifact_path=path, required_extensions=('.html',),
        request=job.task, content_outcome={'status': 'partial', 'remaining_work': ['Missing source']})
    assert result['response_fulfilled'] is False and _partial_cron_report_available(job, result)
    if blocker == 'verified_match': result['cron_alert_delivery_mode'] = 'verified_matches_only'
    else: result[blocker] = True
    assert not _partial_cron_report_available(job, result)


def test_pending_source_choice_is_not_bypassed_by_a_partial_file(tmp_path):
    from nullion.cron_delivery import _complete_typed_html_artifact_recovery, _partial_cron_report_available
    path = tmp_path/'report.html'; path.write_text('<!doctype html><html><body>one</body></html>')
    job = SimpleNamespace(task='request', artifact_delivery_options={})
    state = {'tool_results':[{'tool_name':'request_tool_scope', 'status':'completed',
        'output':{'source_selection_required':True}}]}
    _complete_typed_html_artifact_recovery(state, artifact_path=path, required_extensions=('.html',),
        request=job.task, content_outcome={'status':'partial', 'remaining_work':['Missing source']})
    assert not _partial_cron_report_available(job, state)


def test_existing_unfinished_checks_survive_a_completed_file(tmp_path):
    from nullion.cron_delivery import _complete_typed_html_artifact_recovery
    path = tmp_path/'report.html'; path.write_text('<!doctype html><html><body>one</body></html>')
    result = {'response_fulfilled': True, 'cron_completion_required': True,
        'cron_remaining_work': ['Faltan datos actuales']}
    _complete_typed_html_artifact_recovery(result, artifact_path=path, required_extensions=('.html',), request='request')
    assert result['response_fulfilled'] is False
    assert result['cron_report_content_receipt']['status'] == 'partial'
    assert 'Faltan datos actuales' not in result['text'] and result['artifacts'] == [str(path)]
    assert result['cron_remaining_work'] == ['Faltan datos actuales']


def test_partial_fulfillment_is_distinct_from_failed_delivery():
    from nullion.execution_outcome import build_execution_outcome
    outcome = build_execution_outcome(user_visible_message='Partial report attached.', delivery_status='sent',
        fulfillment_satisfied=False, partial_fulfillment=True, missing_requirements=['Missing source'])
    assert outcome.status.value == 'partially_succeeded' and outcome.delivery_succeeded
    assert outcome.missing_requirements == ('Missing source',)
    failed = build_execution_outcome(user_visible_message='Partial report attached.', delivery_status='failed',
        fulfillment_satisfied=False, partial_fulfillment=True)
    assert failed.status.value == 'failed' and not failed.delivery_succeeded


@pytest.mark.parametrize('tampered', [False, True])
def test_css_delivery_transform_renews_only_an_unchanged_owned_report(tmp_path, tampered):
    from nullion.cron_delivery import _apply_responsive_html_delivery_guard
    path = tmp_path/'report.html'; path.write_text('<!doctype html><html><body>one</body></html>')
    result = {'cron_report_request': 'request', 'cron_report_content_receipt': bind_report_outcome(
        'request', {'status':'partial', 'remaining_work':['Missing current observations']}, [str(path)])}
    if tampered: path.write_text('<!doctype html><html><body>different facts</body></html>')
    _apply_responsive_html_delivery_guard([str(path)], result=result)
    assert partial_report_receipt('request', result['cron_report_content_receipt'], [str(path)]) is not tampered


def test_missing_required_call_cannot_be_satisfied_by_a_generated_document():
    from nullion.cron_delivery import _instruction_aware_cron_html
    from nullion.tools import ToolResult
    state = {'tool_results':[ToolResult('read', 'sensor_read', 'completed',
        {'items':[{'name':'one', 'value':12}]})],
        'cron_source_action_contract':{'missing_callable_required_tool_names':['other_observation']}}
    class Provider:
        def create(self, **kwargs):
            packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
            return {'content':[{'type':'text', 'text':document(packet['report_outcome_contract'])}]}
    report = _instruction_aware_cron_html(SimpleNamespace(name='観測', task='必要な観測をすべて確認',
        artifact_delivery_options={}), state, model_client=Provider())
    assert report and state['cron_report_generated_outcome']['status'] == 'partial'
    assert 'other_observation' in state['cron_report_generated_outcome']['remaining_work'][0]


def test_completed_report_changed_before_delivery_loses_its_receipt(tmp_path):
    from nullion.cron_delivery import _complete_typed_html_artifact_recovery, cron_artifact_validation_block_reason
    path = tmp_path/'report.html'; path.write_text('<!doctype html><html><body>one</body></html>')
    state = {'response_fulfilled': True}
    _complete_typed_html_artifact_recovery(state, artifact_path=path, required_extensions=('.html',), request='request')
    path.write_text('<!doctype html><html><body>unselected facts</body></html>')
    assert cron_artifact_validation_block_reason(state) == 'cron_report_content_receipt_changed'


@pytest.mark.parametrize('status',['partial','unknown'])
def test_reaccepting_unchanged_owned_report_preserves_real_producer_receipt_and_caption(tmp_path,status):
    from nullion.cron_delivery import _recover_typed_html_artifact_after_terminal_limit
    from nullion.tools import ToolResult
    path=tmp_path/'report.html';path.write_text('<!doctype html><html><body><h1>49 views</h1><p>14 unique visitors</p></body></html>')
    job=SimpleNamespace(task='資料を報告',artifact_delivery_options={
        'requires_attachment_delivery':True,'required_artifact_extensions':['.html']})
    write=ToolResult('real-write','file_write','completed',{'path':str(path),'artifact_path':str(path),
        'bytes_written':path.stat().st_size})
    caption='Report attached. Final review is unavailable.'
    state={'tool_results':[write],'artifacts':[str(path)],'artifact_delivery_satisfied':True,
        'response_presentation':{'outcome_review_status':'unavailable'},
        'response_fulfilled':False,'text':caption,'cron_report_content_receipt':bind_report_outcome(
            job.task,{'status':status,'remaining_work':['Unconfirmed review']},[str(path)])}
    assert _recover_typed_html_artifact_after_terminal_limit(job,'web:fixture',state)
    assert state['tool_results']==[write] and state['tool_results'][0] is write
    assert state['text']==caption and state['response_fulfilled'] is False
    assert state['cron_report_content_receipt']['status']==status


def test_reaccepting_reviewed_file_with_failed_summary_uses_safe_caption_and_real_receipt(tmp_path):
    from nullion.cron_delivery import _recover_typed_html_artifact_after_terminal_limit
    from nullion.tools import ToolResult
    path=tmp_path/'report.html';path.write_text('<!doctype html><html><body><h1>49 views</h1><p>14 unique visitors</p></body></html>')
    job=SimpleNamespace(task='資料を報告',artifact_delivery_options={
        'requires_attachment_delivery':True,'required_artifact_extensions':['.html']})
    write=ToolResult('real-write','file_write','completed',{'path':str(path),'artifact_path':str(path)})
    state={'tool_results':[write],'artifacts':[str(path)],'artifact_delivery_satisfied':True,
        'response_fulfilled':False,'text':'Rejected draft','cron_report_content_receipt':bind_report_outcome(
            job.task,{'status':'partial','content_status':'complete','presentation_status':'partial',
                'remaining_work':['Fix summary']},[str(path)])}
    assert _recover_typed_html_artifact_after_terminal_limit(job,'web:fixture',state)
    assert state['tool_results']==[write] and state['tool_results'][0] is write
    assert "couldn't complete the chat summary" in state['text']
    assert state['response_fulfilled'] is False
