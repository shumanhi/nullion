"""Report metadata stays present without requiring a paragraph per field."""
from types import SimpleNamespace

import pytest

from nullion.cron_delivery import (
    _scheduled_chat_presentation_instructions,
    _cron_final_repair_system_prompt,
    cron_agent_prompt,
)


@pytest.mark.parametrize('task', [
    '送信者、件名、受信日、期限、金額、予定表の結果を簡潔に報告。',
    'Resume remitente, asunto, fecha, vencimiento, importe y resultado del calendario.',
    '逐一の詳細な監査と完全な件名を残してください。',
])
def test_original_and_recovery_share_compact_presentation_without_erasing_task(task):
    from nullion.chat_text import report_presentation_instructions
    guidance = _scheduled_chat_presentation_instructions()
    prompt = cron_agent_prompt(SimpleNamespace(name='記録', task=task), label='Scheduled task')
    assert task in prompt
    assert guidance in prompt and guidance in _cron_final_repair_system_prompt()
    # The scheduler supplies execution context, while the common contract owns
    # style. Do not require a duplicate scheduler-specific copy of that policy.
    shared = report_presentation_instructions()
    assert guidance.count(shared) == prompt.count(shared) == 1


@pytest.mark.parametrize('required', [False, True])
def test_typed_delivery_context_distinguishes_attached_summary_from_standalone_report(required):
    import json
    from nullion.chat_text import report_delivery_context
    from nullion.agent_orchestrator import _completion_review_request_context
    job = SimpleNamespace(name='資料', task='Contenido acordado.', artifact_delivery_options={
        'requires_attachment_delivery': required, 'required_artifact_extensions': ['.html'] if required else []})
    expected = report_delivery_context(artifact_delivery_required=required)
    assert json.dumps(expected) in cron_agent_prompt(job, label='Scheduled task')
    context = _completion_review_request_context({'tool_flow_context': {
        'scheduled_task_run': True, 'requires_artifact_delivery': required}})
    assert context['report_presentation_contract']['report_delivery_context'] == expected


def test_source_capture_alone_does_not_turn_an_ordinary_answer_into_an_attachment_summary():
    from nullion.agent_orchestrator import _completion_review_request_context
    from nullion.tools import ToolResult
    context = _completion_review_request_context({'artifacts': ['/retained/source.png'],
        'tool_results': [ToolResult('owned', 'record_read', 'completed', {'value': 7})]})
    assert context['report_presentation_contract']['report_delivery_context']['delivery_role'] == 'standalone_report'
