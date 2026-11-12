"""Separate requested linked and embedded reports remain separate deliverables."""
import base64
import io

import pytest


@pytest.mark.parametrize('prompt', ['Deliver both reports.', 'Entrega ambos informes.', '両方のレポートを渡してください。'])
def test_requested_linked_report_survives_once_selected_embedded_counterpart_is_verified(tmp_path, prompt):
    from PIL import Image
    from nullion.tools import ToolResult
    from nullion.response_fulfillment_contract import evaluate_response_fulfillment
    image = io.BytesIO(); Image.new('RGB', (40, 30), 'navy').save(image, format='PNG')
    linked = tmp_path / 'linked.html'; embedded = tmp_path / 'embedded.html'
    linked.write_text('<!doctype html><html><body><a href="https://example.org">Source</a></body></html>')
    encoded = base64.b64encode(image.getvalue()).decode()
    embedded.write_text('<!doctype html><html><body><img src="data:image/png;base64,' + encoded + '"></body></html>')
    results = [ToolResult('scope', 'request_tool_scope', 'completed', {
        'artifact_extensions': ['.html', '.html'], 'embedded_media_artifact_extensions': ['.html']}),
        ToolResult('prior', 'file_write', 'failed', {'reason': 'artifact_media_embed_failed', 'path': str(linked)}),
        ToolResult('linked', 'file_write', 'completed', {'artifact_path': str(linked), 'html_image_mode': 'linked'}),
        ToolResult('embedded', 'file_write', 'completed', {'artifact_path': str(embedded), 'html_image_mode': 'self_contained'})]
    class Store:
        def get_active_task_frame_id(self, conversation_id): return None
    decision = evaluate_response_fulfillment(store=Store(), conversation_id='case', user_message=prompt,
        reply='Reports attached.', tool_results=results, artifact_paths=[str(linked), str(embedded)],
        artifact_roots=[tmp_path], required_attachment_extensions=['.html', '.html'],
        required_embedded_media_extensions=['.html'])
    assert decision.satisfied, decision
    # A linked/plain file alone still cannot satisfy an embedded-media request.
    decision = evaluate_response_fulfillment(store=Store(), conversation_id='case', user_message=prompt,
        reply='Report attached.', tool_results=results[:-1], artifact_paths=[str(linked)],
        artifact_roots=[tmp_path], required_attachment_extensions=['.html'],
        required_embedded_media_extensions=['.html'])
    assert not decision.satisfied and 'attachment_with_embedded_media:.html' in decision.missing_requirements
