"""Repairing bytes at one path supersedes the old media failure receipt."""
import pytest
from nullion.tools import ToolResult
from nullion.response_fulfillment_contract import artifact_plain_replacement_artifact_paths,evaluate_response_fulfillment

@pytest.mark.parametrize('extension',['.html','.docx','.pdf','.pptx','.xlsx'])
def test_latest_media_receipt_clears_same_path_failure_but_not_a_new_plain_overwrite(tmp_path,extension):
    from PIL import Image
    source=tmp_path/'source.png';Image.new('RGB',(20,20),'blue').save(source)
    path=str(tmp_path/('final'+extension))
    collect=ToolResult('source','browser_image_collect','completed',{'artifact_paths':[str(source)],'image_paths':[str(source)],'images':[{'path':str(source)}]})
    producer={'.html':'file_write','.docx':'document_create','.pdf':'pdf_create','.pptx':'presentation_create','.xlsx':'spreadsheet_create'}[extension]
    plain=ToolResult('plain',producer,'completed',{'artifact_path':path,'path':path})
    repaired=ToolResult('repair',producer,'completed',{'artifact_path':path,'path':path,'embedded_images':[str(source)],'embedded_image_count':1})
    assert artifact_plain_replacement_artifact_paths([plain,collect])==(path,)
    assert artifact_plain_replacement_artifact_paths([plain,collect,repaired])==()
    overwritten=ToolResult('overwrite',producer,'completed',{'artifact_path':path,'path':path})
    assert artifact_plain_replacement_artifact_paths([plain,collect,repaired,overwritten])==(path,)
