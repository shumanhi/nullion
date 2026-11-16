"""A supporting file cannot replace consent or a native action's outcome."""
import pytest

from nullion.tools import ToolResult
from nullion.response_sanitizer import _artifact_completion_reply_over_drift, sanitize_user_visible_reply


@pytest.mark.parametrize('phase',['approval','completed','required_receipt'])
@pytest.mark.parametrize('text',['確認結果 37','Resultado confirmado 37','Résultat confirmé 37'])
def test_artifact_caption_preserves_pending_or_completed_action(tmp_path,phase,text):
    from PIL import Image
    path=tmp_path/'prepared.png'
    Image.new('RGB',(10,10),'navy').save(path)
    results=[ToolResult('scope','request_tool_scope','completed',{'artifact_extensions':['.png']}),
        ToolResult('file','file_write','completed',{'path':str(path),'artifact_paths':[str(path)]})]
    if phase=='approval':
        results.append(ToolResult('action','publish_record','denied',{'reason':'approval_required','approval_id':'native-approval'}))
        text='Tool approval requested: native-approval'
    else:
        receipt={'type':'action_receipt','action':'publish','object_type':'record','object_id':'native-object',
            'summary':'Verified publication 37.'}
        if phase=='required_receipt':receipt['delivery_required']=True
        results.append(ToolResult('action','publish_record','completed',{'action_receipt':receipt}))
    assert _artifact_completion_reply_over_drift(text,results,user_message='依頼') is None
    visible=sanitize_user_visible_reply(user_message='依頼',reply=text,tool_results=results,defer_action_receipts=True)
    assert visible==text
