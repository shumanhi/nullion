from PIL import Image
from nullion.tools import ToolInvocation,_build_file_read_handler
from nullion.agent_orchestrator import _tool_result_visual_reference_blocks


def test_preview_rejects_changed_file_and_never_upgrades_metadata_to_visual_proof(tmp_path):
    path=tmp_path/'approved.png';Image.new('RGB',(60,40),'blue').save(path)
    result=_build_file_read_handler(tmp_path)(ToolInvocation('read','file_read','web:read',{'path':str(path)}))
    assert result.status=='completed'
    assert result.output['visual_content_verified'] is False
    assert len(_tool_result_visual_reference_blocks(result))==2
    Image.new('RGB',(60,40),'red').save(path)
    assert not _tool_result_visual_reference_blocks(result)
    assert result.output['visual_content_verified'] is False
