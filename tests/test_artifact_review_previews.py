from zipfile import ZIP_DEFLATED, ZipFile

from nullion.agent_orchestrator import _completion_review_artifact_text
from nullion.artifact_evidence import artifact_structure_evidence


def test_html_preview_uses_visible_content_instead_of_css_and_image_bytes(tmp_path):
    path = tmp_path / "report.html"
    path.write_text('<html><head><style>' + 'unrelated-css;' * 5000
                    + '</style></head><body><img src="data:image/png;base64,' + 'A' * 20000
                    + '"><h1>地域別データ</h1><p>North 12 South 19 Total 31</p>'
                    + '<a href="https://example.org/source">Source</a></body></html>')
    content, method = _completion_review_artifact_text(path, limit=2000)
    assert method == "html_visible_text"
    assert "地域別データ" in content and "Total 31" in content
    assert "https://example.org/source" in content
    assert "unrelated-css" not in content and "base64" not in content


def test_archive_preview_checks_member_content_without_extracting_paths(tmp_path):
    path = tmp_path / "bundle.zip"
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("north.csv", "Region,Value\nNorth,12\n")
        archive.writestr("../south.csv", "Region,Value\nSouth,19\n")
        archive.writestr("binary.bin", b"\xff" * 5000)
    content, method = _completion_review_artifact_text(path, limit=2000)
    assert method == "zip_member_preview"
    assert "North,12" in content and "South,19" in content and "Archive members: 3" in content
    assert not (tmp_path.parent / "south.csv").exists()
    assert sorted(item.name for item in tmp_path.iterdir()) == ["bundle.zip"]
    assert len(_completion_review_artifact_text(path, limit=30)[0]) <= 30


def test_presentation_content_preview_reads_real_slide_text(tmp_path):
    from pptx import Presentation
    path = tmp_path / "briefing.pptx"
    presentation = Presentation()
    slide = presentation.slides.add_slide(presentation.slide_layouts[1])
    slide.shapes.title.text = "Résumé régional"
    slide.placeholders[1].text = "Total 31"
    presentation.save(path)
    content, method = _completion_review_artifact_text(path, limit=2000)
    assert method == "office_content"
    assert "Résumé régional" in content and "Total 31" in content


def test_html_review_structure_decodes_images_without_exposing_payload(tmp_path):
    import base64
    import io
    from PIL import Image
    stream = io.BytesIO()
    Image.new('RGB', (32,32), 'blue').save(stream,format='PNG')
    encoded = base64.b64encode(stream.getvalue()).decode()
    path = tmp_path/'report.html'
    path.write_text('<html><body><img src="data:image/png;base64,'+encoded+'">'
        '<img src="data:image/png;base64,bm90LWFuLWltYWdl"><img src="photo.jpg">'
        '<svg></svg><img src="data:image/svg+xml,%3Csvg/%3E"></body></html>')
    evidence = artifact_structure_evidence(path)
    assert evidence == {'inspection_complete':True,'image_count':4,
        'decoded_embedded_raster_image_count':1,'invalid_embedded_image_count':1,
        'linked_image_count':1,'inline_svg_count':1,'svg_image_count':1,
        'linked_local_image_assets':[{'reference':'photo.jpg','available_in_artifact_directory':False,'portable_reference':True}],
        'linked_local_image_assets_complete':True}
    assert encoded not in str(evidence)


def test_office_review_structure_reports_embedded_media_from_actual_package(tmp_path):
    from openpyxl import Workbook
    from openpyxl.drawing.image import Image as SheetImage
    from PIL import Image
    image = tmp_path/'chart.png'
    Image.new('RGB',(30,30),'green').save(image)
    book = Workbook()
    book.active.add_image(SheetImage(image),'C1')
    path = tmp_path/'values.xlsx'
    book.save(path)
    structure = artifact_structure_evidence(path)
    assert {key:structure[key] for key in ('inspection_complete','embedded_media_count',
        'decoded_embedded_raster_image_count','invalid_embedded_image_count','uninspected_media_count')} == {
        'inspection_complete':True,'embedded_media_count':1,'decoded_embedded_raster_image_count':1,
        'invalid_embedded_image_count':0,'uninspected_media_count':0}
    assert structure['workbook_inspection_complete'] and structure['worksheet_count']==1
    from zipfile import ZipFile
    with ZipFile(path,'a') as archive:
        archive.writestr('xl/media/broken.png',b'not an image')
    assert artifact_structure_evidence(path)['invalid_embedded_image_count']==1
