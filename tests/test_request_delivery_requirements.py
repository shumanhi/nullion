from types import SimpleNamespace

import pytest

from nullion.tools import ToolResult
from nullion.request_delivery_requirements import (
    record_request_delivery_requirements, reviewed_request_delivery_requirements,
)
from nullion.response_fulfillment_contract import evaluate_response_fulfillment


def evidence():
    return [
        ToolResult('scope-1', 'request_tool_scope', 'completed', {'artifact_extensions': ['.png']}),
        ToolResult('scope-2', 'request_tool_scope', 'completed', {'artifact_extensions': [], 'required_tool_names': ['calendar_list']}),
        ToolResult('calendar-1', 'calendar_list', 'completed', {'results': [{'summary': 'Cedar appointment'}]}),
    ]


@pytest.mark.parametrize('prompt', ['Read the schedule.', '予定を確認して。', 'راجع الجدول.'])
def test_current_reviewed_empty_output_contract_retires_old_scope_formats(prompt):
    from nullion.task_frames import TaskFrameStatus
    results = evidence()
    old_frame = SimpleNamespace(status=TaskFrameStatus.WAITING_INPUT,
        finish=SimpleNamespace(requires_artifact_delivery=True, required_artifact_kind='pdf', required_tool_completion=('file_write',)),
        output=SimpleNamespace(artifact_kind='pdf'))
    store = SimpleNamespace(get_active_task_frame_id=lambda cid:'old-frame', get_task_frame=lambda frame_id:old_frame)
    assert record_request_delivery_requirements(results, prompt, [])
    decision = evaluate_response_fulfillment(store=store, conversation_id='chat:1', user_message=prompt,
        reply='Cedar appointment is on the schedule.', tool_results=results, required_attachment_extensions=['.png'],
        required_tool_names=['calendar_list'])
    assert decision.satisfied is True
    assert decision.reply == 'Cedar appointment is on the schedule.'


def test_contract_rejects_other_requests_and_changed_execution_evidence():
    results = evidence()
    record_request_delivery_requirements(results, 'request one', [])
    assert reviewed_request_delivery_requirements(results, 'request two') is None
    assert reviewed_request_delivery_requirements([*results, ToolResult('later', 'file_write', 'completed', {})], 'request one') is None
    results[-1].status = 'failed'
    assert reviewed_request_delivery_requirements(results, 'request one') is None


def test_provider_payload_cannot_introduce_reviewed_requirements():
    results = evidence()
    record_request_delivery_requirements(results, 'selected request', [])
    payload = results[-1].response_requirements
    results[-1].response_requirements = None
    results[-1].output['response_requirements'] = payload
    assert reviewed_request_delivery_requirements(results, 'selected request') is None


def test_contract_is_available_without_a_tool_scope_call():
    results = [ToolResult('read-1', 'calendar_list', 'completed', {'results':[]})]
    assert record_request_delivery_requirements(results, 'Read the selected source.', [])
    assert reviewed_request_delivery_requirements(results, 'Read the selected source.') is not None


def test_scope_compaction_preserves_runtime_contract_without_losing_binding():
    from nullion.chat_operator import _compact_tool_results_for_context, _tool_results_from_compact_event
    from nullion.messaging_turn_graph import _coerce_stored_tool_results

    results = evidence()
    results[1].output['large_inventory'] = 'x'*100000
    record_request_delivery_requirements(results, 'selected request', [])
    compact = _compact_tool_results_for_context(results, user_message='selected request')
    for restored in [_tool_results_from_compact_event(compact), _coerce_stored_tool_results(compact)]:
        assert reviewed_request_delivery_requirements(restored, 'selected request')['required_attachment_extensions'] == []


def test_real_missing_file_keeps_grounded_partial_text_and_incomplete_status():
    results = evidence()
    text = 'Cedar appointment is confirmed.\n\nThe requested PDF could not be attached.'
    record_request_delivery_requirements(results, 'Provide the findings and a PDF.', ['.pdf'], partial_reply=text)
    decision = evaluate_response_fulfillment(store=object(), conversation_id='chat:1',
        user_message='Provide the findings and a PDF.', reply=text, tool_results=results)
    assert decision.satisfied is False
    assert decision.reply == text
    assert any(item.startswith('attachment:') for item in decision.missing_requirements)


def test_partial_reply_does_not_satisfy_a_missing_write_receipt():
    results = evidence()
    record_request_delivery_requirements(results, 'Send the report.', ['.pdf'],
        partial_reply='A verified observation; PDF unavailable.', eligible_tool_names=['calendar_list','email_send'])
    decision = evaluate_response_fulfillment(store=object(), conversation_id='chat:1', user_message='Send the report.',
        reply='A verified observation; PDF unavailable.', tool_results=results, required_tool_names=['email_send'])
    assert decision.satisfied is False
    assert 'tool:email_send' in decision.missing_requirements


def test_requirement_contract_refuses_invalid_formats():
    with pytest.raises(ValueError):
        record_request_delivery_requirements(evidence(), 'selected request', ['.invalid-format'])
