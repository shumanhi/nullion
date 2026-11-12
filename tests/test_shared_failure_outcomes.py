"""Request-bound review authority across fallback, status and artifact failures."""
from types import SimpleNamespace

import pytest

from nullion.tools import ToolResult
from nullion.execution_review_evidence import record_execution_review, reviewed_execution_evidence
from nullion.execution_outcome import build_execution_outcome
from nullion.response_fulfillment_contract import evaluate_response_execution_outcome, evaluate_response_fulfillment


class Store:
    def get_active_task_frame_id(self, *_):
        return None


def receipts(reason='network_error'):
    return [ToolResult('old', 'source_read', 'failed', {'reason': reason,
        **({'connection_state': 'pending_or_failed', 'connector_app_id': 'selected'}
           if reason == 'account_connection_reconnect_required' else {})}),
        ToolResult('read', 'source_read', 'completed', {'text': '記録 A: 28。'})]


@pytest.mark.parametrize('prompt', ['選択した情報を確認して。', 'راجع البيانات المحددة.', 'Consulta los datos.'])
@pytest.mark.parametrize('complete', [True, False])
def test_reviewed_useful_results_survive_old_connector_state(prompt, complete):
    results = receipts('account_connection_reconnect_required')
    reply = '記録 A: 28。' + ('' if complete else '\n\n比較はまだ未完了です。')
    record_execution_review(results, prompt, reply, complete=complete,
        partial_reply=reply if not complete else None)
    result = evaluate_response_execution_outcome(store=Store(), conversation_id='chat:one',
        user_message=prompt, reply=reply, tool_results=results)
    assert reply == result.reply
    assert result.outcome.status.value == 'succeeded' if complete else result.outcome.status.value != 'needs_user_action'
    assert result.outcome.failed_tools == ('source_read',)  # Historical attempts stay observable.
    assert result.satisfied is complete


@pytest.mark.parametrize('change', ['request', 'reply', 'output', 'status', 'id', 'order', 'recovery'])
def test_review_binding_rejects_changed_evidence_and_text(change):
    results=receipts(); request='選択した記録';reply='記録 A: 28。'
    record_execution_review(results,request,reply,complete=True)
    if change=='request': request+='別'
    elif change=='reply': reply+='別'
    elif change=='output':results[0].output={'reason':'approval_required'}
    elif change=='status':results[0].status='denied'
    elif change=='id':results[0].invocation_id='other'
    elif change=='order':results.reverse()
    else:results[0].recovered_by=('read',)
    assert reviewed_execution_evidence(results,request,reply) is None


def test_provider_payload_cannot_carry_execution_authority():
    results=receipts();record_execution_review(results,'request','reply',complete=True)
    payload=results[-1].response_requirements;results[-1].response_requirements=None
    results[-1].output['response_requirements']=payload
    assert reviewed_execution_evidence(results,'request','reply') is None


def test_canonical_result_normalization_preserves_runtime_review_metadata():
    from nullion.tools import normalize_tool_result
    results=receipts();record_execution_review(results,'request','reply',complete=True)
    restored=[normalize_tool_result(r) for r in results]
    assert reviewed_execution_evidence(restored,'request','reply').complete


def test_execution_receipt_does_not_duplicate_large_partial_report_text():
    import json
    results=receipts();reply='確認済みの情報。'*10000
    record_execution_review(results,'request',reply,complete=False,partial_reply=reply)
    assert len(json.dumps(results[-1].response_requirements))<1000
    assert reviewed_execution_evidence(results,'request',reply).partial_reply==reply


@pytest.mark.parametrize('status',['error','denied'])
def test_normalized_failure_statuses_can_bind_current_blockers(status):
    from nullion.execution_review_evidence import is_failure_or_user_action
    result=ToolResult('attempt','opaque_adapter',status,{})
    assert is_failure_or_user_action(result)
    record_execution_review([result],'request','reply',complete=False,blocking_failure_ids=['attempt'])
    assert reviewed_execution_evidence([result],'request','reply').blocking_failure_ids==('attempt',)


def test_selected_file_version_change_invalidates_execution_review(tmp_path):
    path=tmp_path/'selected.txt';path.write_text('before')
    results=receipts();record_execution_review(results,'request','reply',complete=True,artifact_paths=[path])
    assert reviewed_execution_evidence(results,'request','reply').complete
    path.write_text('a changed selected file')
    assert reviewed_execution_evidence(results,'request','reply') is None


def test_same_size_preserved_timestamp_rewrite_invalidates_execution_review(tmp_path):
    import os
    path = tmp_path / 'selected.txt'
    path.write_text('before')
    stamp = path.stat()
    results = receipts()
    record_execution_review(results, 'request', 'reply', complete=True, artifact_paths=[path])
    assert reviewed_execution_evidence(results, 'request', 'reply').complete
    path.write_text('after!')
    os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    assert reviewed_execution_evidence(results, 'request', 'reply') is None


def test_delivery_annotation_does_not_reopen_review_but_source_change_does():
    results = receipts()
    record_execution_review(results, 'request', 'reply', complete=True)
    results[0].output['artifact_delivery_manifest'] = {'status': 'pending'}
    assert reviewed_execution_evidence(results, 'request', 'reply').complete
    results[0].output['text'] = 'changed source'
    assert reviewed_execution_evidence(results, 'request', 'reply') is None


@pytest.mark.parametrize('name',['opaque_adapter','connector_request','weather_forecast','browser_extract_text'])
def test_review_compaction_preserves_typed_connection_control_state(name):
    from nullion.agent_orchestrator import _compact_tool_output_for_repair
    output={'text':'A provider returned text.', 'connection_state':'pending_or_failed',
        'terminal_user_action_required':True,'connector_app_id':'selected','reason':'source_connection_required'}
    compact=_compact_tool_output_for_repair(name,output)
    for key in ('connection_state','terminal_user_action_required','connector_app_id','reason'):
        assert compact[key]==output[key]


def test_current_bound_connector_failure_preserves_findings_and_user_action():
    results=receipts('account_connection_reconnect_required')
    reply='記録 A: 28。\n\nReconnect the selected source before retrying its remaining checks.'
    record_execution_review(results,'request',reply,complete=False,
        blocking_failure_ids=['old'],partial_reply=reply)
    result=evaluate_response_execution_outcome(store=Store(),conversation_id='chat:one',
        user_message='request',reply=reply,tool_results=results)
    assert result.satisfied is False and result.outcome.status.value=='needs_user_action'
    assert reply in result.reply and 'Reconnect' in result.reply


def test_completed_adapter_receipt_can_still_require_a_current_user_action():
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    results=receipts('account_connection_reconnect_required')
    results[0].status='completed';results[0].output['terminal_user_action_required']=True
    results[0].output['text']='A provider returned text alongside its pending connection state.'
    assert not _completion_review_evidence_result_is_positive(results[0])
    reply='記録 A: 28。\n\nReconnect the selected source before retrying its remaining checks.'
    record_execution_review(results,'request',reply,complete=False,blocking_failure_ids=['old'],partial_reply=reply)
    result=evaluate_response_execution_outcome(store=Store(),conversation_id='chat:one',
        user_message='request',reply=reply,tool_results=results)
    assert result.satisfied is False and result.outcome.status.value=='needs_user_action'
    assert result.reply==reply
    # The Web and messaging boundaries can evaluate the same answer more
    # than once. Neither may invalidate its receipt or discard the findings.
    second=evaluate_response_execution_outcome(store=Store(),conversation_id='chat:one',
        user_message='request',reply=result.reply,tool_results=results)
    assert second.reply==reply and not second.satisfied
    assert reviewed_execution_evidence(results,'request',second.reply).partial_reply==reply


@pytest.mark.parametrize('missing', ['action', 'artifact'])
def test_complete_review_never_bypasses_missing_action_or_file(missing):
    results=receipts();record_execution_review(results,'request','reply',complete=True)
    args={'required_tool_names':['calendar_create']} if missing=='action' else {'required_attachment_extensions':['.pdf']}
    result=evaluate_response_execution_outcome(store=Store(),conversation_id='chat:one',user_message='request',
        reply='reply',tool_results=results,**args)
    assert result.satisfied is False and result.missing_requirements
    assert result.outcome.status.value=='failed'


@pytest.mark.parametrize('reason', ['cancelled','approval','delivery'])
def test_verified_completion_does_not_override_terminal_boundaries(reason):
    results=receipts();record_execution_review(results,'request','reply',complete=True)
    proof=reviewed_execution_evidence(results,'request','reply')
    kw={'cancelled':True} if reason=='cancelled' else {'suspended_for_approval':True} if reason=='approval' else {'delivery_status':'failed'}
    result=build_execution_outcome(tool_results=results,reviewed_execution=proof,**kw)
    assert result.status.value=={'cancelled':'cancelled','approval':'needs_user_action','delivery':'failed'}[reason]


def test_unreviewed_success_does_not_infer_same_subject_recovery():
    result=build_execution_outcome(tool_results=receipts(),fulfillment_satisfied=True,user_visible_message='A')
    assert result.status.value=='partially_succeeded'


@pytest.mark.parametrize('delivery',['sent','saved'])
def test_delivered_work_with_unknown_completion_is_not_a_fictional_source_failure(delivery):
    result=build_execution_outcome(tool_results=[ToolResult('read','record_read','completed',{'value':7})],
        fulfillment_satisfied=None, delivery_status=delivery, user_visible_message='観測値は7です。')
    assert result.status.value=='partially_succeeded'
    assert result.delivery_succeeded and result.failure_reason is None
    failed=build_execution_outcome(tool_results=receipts(), fulfillment_satisfied=None,
        delivery_status=delivery,user_visible_message='観測値は7です。')
    assert failed.failure_reason, 'A real source failure must remain observable'


def test_missing_action_is_not_blame_for_old_browser_error():
    results=[ToolResult('old','browser_extract_detail','failed',{}),ToolResult('read','browser_extract_text','completed',{'text':'A'})]
    result=evaluate_response_fulfillment(store=Store(),conversation_id='chat:one',user_message='予定を更新',
        reply='A',tool_results=results,required_tool_names=['calendar_create'])
    assert result.satisfied is False and result.missing_requirements==('tool:calendar_create',)
    assert 'browser' not in result.reply


@pytest.mark.parametrize('tool,extension', [('image_generate','.png'),('pdf_create','.pdf'),('document_create','.docx'),('spreadsheet_create','.xlsx')])
@pytest.mark.parametrize('reason', ['handler_timeout','network_error','request_plan_conflict'])
def test_artifact_failure_copy_uses_typed_cause_and_hides_private_diagnostics(tool,extension,reason):
    result=evaluate_response_fulfillment(store=Store(),conversation_id='chat:one',user_message='作成してください',
        reply='未完了',required_attachment_extensions=[extension],
        tool_results=[ToolResult('attempt',tool,'failed',{'reason':reason},'PRIVATE token=xyz /private/path')])
    assert result.satisfied is False and 'PRIVATE' not in result.reply and 'xyz' not in result.reply
    assert 'API key' not in result.reply and 'provider is not' not in result.reply
    if reason=='handler_timeout':assert 'timed out' in result.reply


def test_provider_setup_copy_requires_explicit_provider_setup_reason():
    result=evaluate_response_fulfillment(store=Store(),conversation_id='chat:one',user_message='作成',reply='未完了',
        required_attachment_extensions=['.png'],tool_results=[ToolResult('attempt','image_generate','failed',
            {'reason':'provider_not_configured'},'PRIVATE')])
    assert 'provider is not configured' in result.reply and 'PRIVATE' not in result.reply
    assert '1.' in result.reply and '2.' in result.reply


def test_cron_status_uses_bound_task_review_without_header_invalidating_it():
    from nullion.cron_delivery import _attach_cron_execution_outcome
    results=receipts();record_execution_review(results,'依頼','記録 A: 28。',complete=True)
    payload={'text':'記録 A: 28。','tool_results':results,'response_fulfilled':True,'cron_delivery_status':'sent'}
    _attach_cron_execution_outcome({'job':SimpleNamespace(name='定期確認',task='依頼')},payload,text='見出し\n記録 A: 28。')
    assert payload['execution_outcome']['status']=='succeeded'


def test_private_producer_diagnostic_copy_is_removed_in_non_english_surrounding_text(tmp_path):
    path=tmp_path/'report.pdf';path.write_bytes(b'%PDF-1.4\n%completed\n')
    diagnostic='Private generator diagnostic with a confidential path /private/source/document'
    result=evaluate_response_fulfillment(store=Store(),conversation_id='chat:one',user_message='添付資料を作成',
        reply='生成エラー: '+diagnostic,required_attachment_extensions=['.pdf'],artifact_roots=[tmp_path],
        tool_results=[ToolResult('failed','document_create','failed',{},diagnostic),
            ToolResult('ready','pdf_create','completed',{'artifact_path':str(path)})])
    assert result.satisfied is True and 'Private' not in result.reply
    assert str(path) in result.reply


@pytest.mark.parametrize('name',['image_generate','terminal_exec','web_fetch','opaque_adapter'])
def test_last_tool_fallback_does_not_echo_private_provider_diagnostics(name):
    from nullion.agent_orchestrator import _last_useful_tool_message,_untrusted_tool_result_safe_fallback_text
    result=ToolResult('failed',name,'failed',{},'PRIVATE token=xyz /private/source')
    for reply in (_last_useful_tool_message([result]),_untrusted_tool_result_safe_fallback_text(result)):
        assert 'PRIVATE' not in reply and 'xyz' not in reply and '/private/source' not in reply
