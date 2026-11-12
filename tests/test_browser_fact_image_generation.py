"""Browser facts do not silently impose a source-photograph requirement."""
import pytest
from nullion.response_fulfillment_contract import artifact_media_plain_replacement_guard_result
from nullion.tools import ToolInvocation, ToolResult


@pytest.mark.parametrize('tool_name,output', [
    ('browser_extract_text', {'value': 7}),
    ('browser_run_js', {'result': [{'label': '観測', 'value': 7}]}),
    ('browser_extract_text', {'text': 'Observation: 7. Source image not requested.'}),
])
@pytest.mark.parametrize('prompt', ['観測値を図にする。', 'Representa el valor observado.'])
def test_numeric_browser_observations_allow_a_generated_visual(tool_name, output, prompt):
    prior = ToolResult('observed', tool_name, 'completed', output)
    invocation = ToolInvocation('visual', 'image_generate', 'web:member', {'prompt': prompt})
    assert artifact_media_plain_replacement_guard_result(invocation, [prior]) is None
