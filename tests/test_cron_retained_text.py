import json
from types import SimpleNamespace
from dataclasses import replace

import pytest

from nullion import cron_delivery as delivery
from nullion.tools import ToolResult


def captured():
    row = ToolResult('read-a', 'opaque_read', 'completed', {'text': 'Appointment at 7 PM.'})
    state = {'tool_results': [row], 'text': 'Appointment at 7 PM.', 'cron_report_request': '確認',
             'response_fulfilled': False, 'reached_iteration_limit': True}
    receipt = delivery._RetainedCronTextReport('確認', state['text'],
        tuple(delivery._cron_text_evidence_versions(state).items()), frozenset({'opaque_read'}))
    state['_cron_retained_text_report'] = receipt
    return state


def test_source_bound_partial_survives_more_reads_and_preserves_failure_diagnostics():
    state = captured()
    state['tool_results'].append(ToolResult('read-b', 'opaque_read', 'failed', {}, 'Unavailable'))
    assert delivery._partial_cron_report_available(None, state)
    assert delivery.cron_delivery_block_reason(state, state['text'], []) is None
    assert state['reached_iteration_limit'] and state['response_fulfilled'] is False


def test_retaining_an_authored_answer_does_not_downgrade_completed_work():
    state = captured()
    state.update(response_fulfilled=True, reached_iteration_limit=False)
    assert not delivery._partial_cron_report_available(None, state)
    assert delivery.cron_delivery_block_reason(state, state['text'], []) is None


@pytest.mark.parametrize('change', ['request', 'text', 'evidence', 'removed', 'write', 'unknown',
    'receipt', 'approval', 'cancelled', 'files', 'required_file', 'alert', 'denied', 'source_selection'])
def test_retained_text_cannot_authorize_changed_or_unsafe_outcomes(change):
    state = captured()
    if change == 'request': state['cron_report_request'] = 'otra tarea'
    if change == 'text': state['text'] = 'Invented answer'
    if change == 'evidence': state['tool_results'][0].output['text'] = 'Cancelled appointment'
    if change == 'removed': state['tool_results'] = []
    if change in {'write', 'unknown', 'receipt', 'denied'}:
        state['tool_results'].append(ToolResult('next', 'opaque_read' if change in {'receipt', 'denied'} else change,
            'denied' if change == 'denied' else 'completed',
            {'action_receipt': {}} if change == 'receipt' else {}))
    if change == 'approval': state['suspended_for_approval'] = True
    if change == 'cancelled': state['cron_run_cancelled'] = True
    if change == 'files': state['artifacts'] = ['/missing/report.html']
    if change == 'required_file': state['artifact_delivery_required'] = True
    if change == 'alert': state['cron_alert_delivery_mode'] = 'verified_matches_only'
    if change == 'source_selection': state['cron_source_verification_feedback'] = {'disposition': 'needs_user_input'}
    assert not delivery._partial_cron_report_available(None, state)


def test_provider_json_cannot_forge_runtime_receipt():
    state = captured()
    state['_cron_retained_text_report'] = {'request': '確認', 'text': state['text']}
    assert not delivery._partial_cron_report_available(None, state)


def test_owned_partial_answer_keeps_its_current_clarification_without_granting_an_action():
    from nullion.execution_outcome import prepare_answer
    state = captured()
    text = 'Appointment at 7 PM.\n\nWhich collection?\n1. First\n2. Second'
    state['text'] = text
    state['cron_source_verification_feedback'] = {'disposition': 'needs_user_input'}
    state['_cron_retained_text_report'] = replace(state['_cron_retained_text_report'], text=text,
        prepared_answer=prepare_answer('確認', text, state['tool_results']))
    assert delivery._partial_cron_report_available(None, state)
    assert state['response_fulfilled'] is False
    state['suspended_for_approval'] = True
    assert not delivery._partial_cron_report_available(None, state)


def test_later_connection_failure_does_not_erase_previous_findings():
    state = captured()
    state['_cron_retained_text_report'] = replace(state['_cron_retained_text_report'],
        read_tools=frozenset({'opaque_read', 'email_read'}))
    state['tool_results'].append(ToolResult('unavailable', 'email_read', 'failed',
        {'connection_state': 'unavailable'}, 'Unavailable'))
    assert delivery.cron_delivery_block_reason(state, state['text'], []) is None
    assert 'connection is unavailable' in state['error']
    assert state['response_fulfilled'] is False


def test_report_writer_inspection_and_transport_share_one_deadline(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(delivery.time, 'monotonic', lambda: clock[0])
    state = {'text': 'Preview', 'response_presentation': {'kind': 'source_preview'},
             'tool_results': [ToolResult('read-a', 'web_fetch', 'completed', {'text': 'Appointment at 7 PM.'})]}
    calls = []
    class Provider:
        def create(self, **kw):
            calls.append(kw['timeout'])
            if len(calls) == 1:
                clock[0] += 10
                return {'content': [{'type': 'text', 'text': json.dumps({'disposition': 'inspect_evidence',
                    'evidence_requests': [{'invocation_id': 'e0', 'json_pointer': '', 'offset': 0, 'length': 100}]})}]}
            clock[0] += kw['timeout']
            raise TimeoutError('provider writer timed out')
    delivery._repair_cron_agent_final_text_if_needed(SimpleNamespace(name='確認', task='確認'), state,
        model_client=Provider())
    assert calls == [45, 45, 5] and clock[0] == 60
    assert state['cron_agent_final_repair_failed'] and state['response_fulfilled'] is False


@pytest.mark.parametrize("contradicted", [False, True])
def test_missing_report_coverage_does_not_mean_existing_findings_are_wrong(contradicted):
    state = captured()
    state.update(cron_agent_final_repair_failed=True, cron_source_verification_feedback={
        "disposition": "retry", "material_requirements": [{"satisfied": True,
            "draft_covered": False, "draft_contradicted": contradicted}]})
    assert delivery._partial_cron_report_available(None, state) is (not contradicted)
