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


@pytest.mark.parametrize('source_copy,formats,allowed',[(False,['.png'],True),(True,['.png'],False),
    (False,['.docx','.png'],False)])
def test_incidental_cover_image_does_not_change_typed_generated_visual_goal(source_copy,formats,allowed):
    prior=ToolResult('observed','browser_run_task','completed',{'details':[{'record':{
        'title':'Book','price_text':'51.77','image_url':'https://source.test/cover.jpg'}}]})
    invocation=ToolInvocation('visual','image_generate','member',{'prompt':'価格と在庫の図'},
        flow_context={'required_artifact_extensions':formats,'source_artifact_delivery_requested':source_copy})
    result=artifact_media_plain_replacement_guard_result(invocation,[prior])
    assert (result is None)==allowed
