"""Selection restoration is path/version bound; inspection is not generation."""
from pathlib import Path
import pytest
from nullion.tools import ToolResult, ToolInvocation, _build_file_read_handler
from nullion.artifact_lifecycle import record_delivery_manifest, restored_delivery_paths
from nullion.artifacts import filter_source_artifact_paths
from nullion.response_fulfillment_contract import artifact_plain_replacement_artifact_paths


def generated(path, key='producer'):
    return ToolResult(key,'file_write','completed',{'artifact_path':str(path),'artifact_paths':[str(path)]})


def test_selected_sibling_restoration_is_versioned_and_accepts_iterator(tmp_path):
    a,b=tmp_path/'separate.html',tmp_path/'unselected.html'
    scope=ToolResult('scope','request_tool_scope','completed',{'excluded_artifact_extensions':['.html']})
    results=[scope,generated(a),generated(b,'other')]
    record_delivery_manifest(results,[str(a)],candidate_paths=[str(a),str(b)],restored_format_paths=[str(a),str(b)])
    assert restored_delivery_paths(iter(results))=={str(a)}
    assert filter_source_artifact_paths([str(a),str(b)],iter(results))==[str(a)]
    record_delivery_manifest(results,[str(a)],review_status='complete')
    assert restored_delivery_paths(results)=={str(a)}
    results.append(generated(a,'new-version'))
    assert not restored_delivery_paths(results)
    assert filter_source_artifact_paths([str(a),str(b)],results)==[]


@pytest.mark.parametrize('selection',[[],['other.html']])
def test_changed_or_failed_selection_revokes_sibling_restoration(tmp_path,selection):
    a,b=tmp_path/'separate.html',tmp_path/'other.html';results=[generated(a),generated(b,'other')]
    record_delivery_manifest(results,[str(a)],restored_format_paths=[str(a)])
    record_delivery_manifest(results,[str(tmp_path/p) for p in selection],review_status='blocked')
    assert not restored_delivery_paths(results)


@pytest.mark.parametrize('extension',['.pdf','.pptx','.xlsx'])
def test_native_inspection_cannot_invalidate_media_but_overwrite_can(tmp_path,extension):
    from PIL import Image
    image=tmp_path/'source.png';Image.new('RGB',(64,64),'red').save(image)
    path=tmp_path/('report'+extension)
    if extension=='.pdf':
        with Image.open(image) as content: content.save(path,'PDF')
    elif extension=='.pptx':
        from pptx import Presentation
        from pptx.util import Inches
        doc=Presentation();doc.slides.add_slide(doc.slide_layouts[6]).shapes.add_picture(str(image),0,0,Inches(1),Inches(1));doc.save(path)
    else:
        from openpyxl import Workbook
        from openpyxl.drawing.image import Image as XLImage
        doc=Workbook();doc.active.add_image(XLImage(image),'A1');doc.save(path)
    producer=generated(path)
    producer.output.update({'source_image_paths':[str(image)],'embedded_image_count':1})
    collector=ToolResult('collect','browser_image_collect','completed',{'image_paths':[str(image)]})
    read=_build_file_read_handler(allowed_roots=[tmp_path],include_principal_workspace=False)(
        ToolInvocation('inspect','file_read','user:fixture',{'path':str(path)}))
    assert read.status=='completed',read.error
    assert not artifact_plain_replacement_artifact_paths([collector,producer,read])
    overwrite=generated(path,'overwrite')
    assert str(path) in artifact_plain_replacement_artifact_paths([collector,producer,read,overwrite])
