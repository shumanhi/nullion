"""All artifact families retain a typed producer rejection at delivery."""
import pytest

from nullion.messaging_adapters import DeliveryContract, prepare_reply_for_platform_delivery
from nullion.response_fulfillment_contract import evaluate_response_fulfillment
from nullion.runtime_store import RuntimeStore
from nullion.tools import ToolResult


@pytest.mark.parametrize('tool', ['image_generate','document_create','pdf_create','spreadsheet_create',
    'presentation_create','html_create','file_write'])
@pytest.mark.parametrize('stored_mapping', [False, True])
def test_missing_artifact_reports_latest_provider_payment_failure(tool, stored_mapping):
    rows=[ToolResult('plan',tool,'failed',{'reason':'request_plan_conflict','executed':False}),
        ToolResult('provider',tool,'failed',{'reason':'generation_failed','http_status_code':402,
            'retryable':False},error='PRIVATE provider payload token=secret')]
    decision=evaluate_response_fulfillment(store=RuntimeStore(),conversation_id='web:producer-rejection',
        user_message='依頼したファイル。',reply='Result unavailable.',tool_results=rows,
        required_attachment_extensions=('.png',))
    assert not decision.satisfied
    assert '402' in decision.reply and 'credits' in decision.reply.lower(),decision
    materialized=[{'invocation_id':r.invocation_id,'tool_name':r.tool_name,'status':r.status,
        'output':r.output,'error':r.error} for r in rows] if stored_mapping else rows
    delivery=prepare_reply_for_platform_delivery('Result unavailable.',
        delivery_contract=DeliveryContract.attachment_required(source='current_request'),tool_results=materialized)
    assert not delivery.attachments and delivery.unavailable_attachment_count
    assert '402' in delivery.text and 'credits' in delivery.text.lower(),delivery
    assert 'PRIVATE' not in delivery.text and 'token=secret' not in delivery.text
