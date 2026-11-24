"""Native failure accounting must preserve the primary's authored answer."""
import pytest

from nullion.execution_outcome import ExecutionStatus
from nullion.response_fulfillment_contract import evaluate_response_execution_outcome
from nullion.tools import ToolResult


@pytest.mark.parametrize('caption', [
    'Price £51.77; 22 copies available.',
    'Precio £51.77; quedan 22 ejemplares.',
    '価格は£51.77、在庫は22冊です。',
])
@pytest.mark.parametrize('file_count', [1, 2])
@pytest.mark.parametrize('binding', ['selected_paths', 'media_marker'])
def test_partial_native_outcome_retains_caption_and_selected_files(tmp_path, caption, file_count, binding):
    from PIL import Image

    paths = []
    results = [ToolResult('earlier-attempt', 'code_mode', 'failed',
        {'reason': 'code_mode_operation_stopped'}, 'Earlier operation stopped')]
    for index in range(file_count):
        path = tmp_path / f'final-{index}.png'
        Image.new('RGB', (300, 300), 'white').save(path)
        paths.append(str(path))
        results.append(ToolResult(f'render-{index}', 'image_render', 'completed',
            {'artifact_path': str(path), 'artifact_paths': [str(path)]}))
    reply = caption
    if binding == 'media_marker':
        reply += '\n\n' + '\n'.join('MEDIA:' + path for path in paths)
    result = evaluate_response_execution_outcome(store=None, conversation_id='web:caption',
        user_message='公開情報から図を作成。', reply=reply, tool_results=results,
        artifact_paths=paths if binding == 'selected_paths' else None,
        artifact_roots=[tmp_path], platform_artifact_count=file_count,
        required_attachment_extensions=['.png'], delivery_status='saved')
    assert result.outcome.status is ExecutionStatus.PARTIALLY_SUCCEEDED
    assert result.outcome.failed_tools
    assert result.reply == reply
    assert result.satisfied
