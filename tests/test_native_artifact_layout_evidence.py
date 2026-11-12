"""Native evidence reports file facts without inventing a rendered inspection."""
from pathlib import Path
import pytest
from nullion.artifact_evidence import artifact_structure_evidence


def test_docx_fonts_colors_styles_and_page_dimensions_are_from_bytes(tmp_path):
    from docx import Document
    from docx.shared import Pt,RGBColor
    path=tmp_path/'report.docx';doc=Document();p=doc.add_paragraph('Actual heading',style='Heading 1');r=p.runs[0];r.font.size=Pt(28);r.font.color.rgb=RGBColor.from_string('237AB2');doc.save(path)
    result=artifact_structure_evidence(path)
    assert result['layout_evidence_kind']=='native_format_structure' and result['rendered_visual_inspection'] is False
    assert 28 in result['explicit_font_sizes_pt'] and '237AB2' in result['explicit_font_colors_rgb']
    assert 'Heading1' in result['paragraph_style_ids'] and result['sections'][0]['page_width_twips']>0


def test_pptx_reports_actual_shape_overflow_and_type_sizes(tmp_path):
    from pptx import Presentation
    from pptx.util import Inches,Pt
    from pptx.dml.color import RGBColor
    path=tmp_path/'deck.pptx';deck=Presentation();slide=deck.slides.add_slide(deck.slide_layouts[6]);box=slide.shapes.add_textbox(-Inches(1),Inches(1),Inches(3),Inches(1));r=box.text_frame.paragraphs[0].add_run();r.text='Actual heading';r.font.size=Pt(30);r.font.color.rgb=RGBColor.from_string('237AB2');deck.save(path)
    result=artifact_structure_evidence(path);actual=result['slide_layouts'][0]
    assert result['slide_count']==1 and actual['shapes_outside_slide']==1
    assert actual['explicit_font_sizes_pt']==[30] and actual['explicit_font_colors_rgb']==['237AB2']
    assert result['rendered_visual_inspection'] is False


def test_pdf_reports_text_beyond_the_actual_page(tmp_path):
    from pypdf import PdfWriter
    from pypdf.generic import NameObject,DictionaryObject,DecodedStreamObject
    path=tmp_path/'report.pdf';writer=PdfWriter();page=writer.add_blank_page(width=200,height=200)
    font=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
    page[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):DictionaryObject({NameObject('/F1'):writer._add_object(font)})})
    content=DecodedStreamObject();content.set_data(b'BT /F1 24 Tf 10 160 Td (Actual heading) Tj ET BT /F1 18 Tf 220 80 Td (Outside the page) Tj ET')
    page[NameObject('/Contents')]=writer._add_object(content);writer.write(path)
    result=artifact_structure_evidence(path);page=result['page_layouts'][0]
    assert result['page_count']==1 and 24 in page['font_sizes_pt']
    assert page['text_origins_outside_page']>0 and result['rendered_visual_inspection'] is False


def test_office_layout_inspection_is_bounded_before_library_parse(tmp_path):
    from zipfile import ZipFile,ZIP_DEFLATED
    path=tmp_path/'oversized.docx'
    with ZipFile(path,'w',compression=ZIP_DEFLATED) as package:package.writestr('word/document.xml','x'*(4*1024*1024+1))
    result=artifact_structure_evidence(path)
    assert result['layout_inspection_complete'] is False and result['layout_inspection_reason']=='xml_limit'


def test_native_presentation_producer_paragraph_fonts_are_retained(tmp_path):
    from nullion.tools import _build_presentation_create_handler,ToolInvocation
    path=tmp_path/'native.pptx'
    handler=_build_presentation_create_handler(tmp_path,allowed_roots=[tmp_path])
    result=handler(ToolInvocation(invocation_id='native',tool_name='presentation_create',principal_id='fixture',
        arguments={'output_path':str(path),'title':'Actual native deck','slides':[{'title':'Readable heading','body':'Readable body','bullets':['One point']}]}))
    assert result.status=='completed',result
    slide=artifact_structure_evidence(path)['slide_layouts'][0]
    assert set(slide['explicit_font_sizes_pt'])=={15,17,28}
    assert {'111827','1F2937'}.issubset(slide['explicit_font_colors_rgb'])


@pytest.mark.parametrize('extension',['.docx','.pptx'])
def test_malformed_office_xml_reports_unavailable_evidence_without_crashing(tmp_path,extension):
    from zipfile import ZipFile
    path=tmp_path/('broken'+extension)
    with ZipFile(path,'w') as package:package.writestr('[Content_Types].xml','<bad><')
    evidence=artifact_structure_evidence(path)
    assert evidence['layout_inspection_complete'] is False
    assert evidence['rendered_visual_inspection'] is False


def test_buffered_pdf_text_uses_draw_time_transform_not_later_flush_transform():
    fixture=Path(__file__).parent/'fixtures/pdf/transformed-table-page.pdf'
    result=artifact_structure_evidence(fixture)
    page=result['page_layouts'][0]
    assert page['text_run_count']>20
    assert page['text_origins_outside_page']==0
    assert result['rendered_visual_inspection'] is False
