"""Inline document illustrations are structured media, like SVG file inputs."""
import pytest

from nullion.response_fulfillment_contract import artifact_media_plain_replacement_guard_result
from nullion.tools import ToolInvocation

SVG = ('<svg viewBox="0 0 200 100" role="img"><title>Four colors</title>'
       '<rect width="100" height="50" fill="#ff0000"/>'
       '<rect x="100" width="100" height="50" fill="#0000ff"/>'
       '<rect y="50" width="100" height="50" fill="#00ff00"/>'
       '<rect x="100" y="50" width="100" height="50" fill="#ffff00"/></svg>')


def test_pdf_accepts_a_real_inline_svg_as_declared_visual_input():
    invocation=ToolInvocation('illustration','pdf_create','web:illustration',{
        'html_pages':['<h1>INLINE VECTOR 53</h1>'+SVG]})
    assert artifact_media_plain_replacement_guard_result(invocation,[],
        required_embedded_media_extensions=('.pdf',)) is None


@pytest.mark.parametrize('body',['<svg></svg>','<svg><title>Only a label</title></svg>',
    '<svg><defs><rect width="20" height="20"/></defs></svg>',
    '<svg><rect width="20" height="20">','&lt;svg&gt;not actual markup&lt;/svg&gt;'])
def test_empty_or_malformed_svg_does_not_claim_a_visual(body):
    invocation=ToolInvocation('empty','pdf_create','web:illustration',{'html_pages':[body]})
    result=artifact_media_plain_replacement_guard_result(invocation,[],required_embedded_media_extensions=('.pdf',))
    assert result and result.status=='failed'


def test_inline_svg_source_spans_preserve_unicode_newlines_and_multiple_drawings():
    from nullion.artifact_evidence import inline_svg_fragments
    source='見出し\n<p>Content</p>\n'+SVG+'\n'+SVG
    fragments=inline_svg_fragments(source)
    assert len(fragments)==2
    assert all(source[start:end]==fragment==SVG for start,end,fragment in fragments)


@pytest.mark.parametrize('body',[SVG,'<svg><defs><rect width="20" height="20"/></defs></svg>',
    '<svg><rect/><image href="https://invalid.example/asset"/></svg>'])
def test_html_native_media_requirement_and_file_evidence_use_the_same_svg_contract(tmp_path,body):
    from nullion.tools import _build_file_write_handler
    from nullion.response_fulfillment_contract import artifact_file_embedded_media_count
    path=tmp_path/'visual.html'
    result=_build_file_write_handler(tmp_path)(ToolInvocation('html','file_write','web:html',
        {'path':str(path),'content':'<html><body>'+body+'</body></html>'},
        flow_context={'required_embedded_media_extensions':['.html']}))
    if body==SVG:
        assert result.status=='completed',result
        assert artifact_file_embedded_media_count(path)==1
    else:
        assert result.status=='failed' and not path.exists(),result


def illustration_html(form):
    from base64 import b64encode
    from urllib.parse import quote
    if form=='inline':return SVG
    document=SVG.replace('<svg ','<svg xmlns="http://www.w3.org/2000/svg" ')
    if form=='base64':source='data:image/svg+xml;base64,'+b64encode(document.encode()).decode()
    else:source='data:image/svg+xml,'+quote(document,safe='')
    return '<img alt="Four colors" src="'+source+'">'


@pytest.mark.parametrize('form',['base64','percent'])
def test_valid_svg_data_image_is_native_html_media(tmp_path,form):
    from nullion.tools import _build_file_write_handler
    from nullion.response_fulfillment_contract import artifact_file_embedded_media_count
    path=tmp_path/'visual.html';body='<html><body>'+illustration_html(form)+'</body></html>'
    result=_build_file_write_handler(tmp_path)(ToolInvocation('svg-data','file_write','web:svg-data',
        {'path':str(path),'content':body},flow_context={'required_embedded_media_extensions':['.html']}))
    assert result.status=='completed',result
    assert path.read_text()==body
    assert artifact_file_embedded_media_count(path)==1


@pytest.mark.parametrize('document',[
    '<svg xmlns="http://www.w3.org/2000/svg"><defs><rect width="20" height="20"/></defs></svg>',
    '<svg xmlns="http://www.w3.org/2000/svg"><rect/><image href="file:///private/secret.png"/></svg>',
    '<svg xmlns="http://www.w3.org/2000/svg"><rect/><style>@import "https://invalid.example/style.css";</style></svg>',
    '<svg><rect width="20" height="20"/></svg>',
])
def test_svg_data_image_rejects_empty_or_non_self_contained_drawings(tmp_path,document):
    from base64 import b64encode
    from nullion.tools import _build_file_write_handler
    path=tmp_path/'invalid.html';source='data:image/svg+xml;base64,'+b64encode(document.encode()).decode()
    result=_build_file_write_handler(tmp_path)(ToolInvocation('bad-data','file_write','web:svg',
        {'path':str(path),'content':'<html><body><img src="'+source+'"></body></html>'},
        flow_context={'required_embedded_media_extensions':['.html']}))
    assert result.status=='failed' and not path.exists(),result
    summary=result.output['media_input_summary']
    assert summary['svg_data_image_count']==1 and summary['valid_svg_data_image_count']==0
    assert all(isinstance(value,int) for value in summary.values())
    assert source not in str(result.output)


def test_svg_data_image_supports_unicode_and_xml_namespace_prefixes():
    from base64 import b64encode
    from nullion.artifact_evidence import svg_data_image_document,self_contained_html_svg_count
    document=SVG.replace('<svg ','<v:svg xmlns:v="http://www.w3.org/2000/svg" xmlns="http://www.w3.org/2000/svg" ').replace('</svg>','</v:svg>').replace('Four colors','四つの色')
    source='data:image/svg+xml;base64,'+b64encode(document.encode()).decode()
    assert svg_data_image_document(source)==document
    assert self_contained_html_svg_count('<img src="'+source+'">')==1


def test_svg_data_image_does_not_treat_an_unescaped_url_fragment_as_document_bytes():
    from nullion.artifact_evidence import svg_data_image_document
    # The browser would terminate the document at '#', unlike a naive decoder.
    document=SVG.replace('<svg ','<svg xmlns="http://www.w3.org/2000/svg" ')
    assert svg_data_image_document('data:image/svg+xml,'+document) is None


@pytest.mark.parametrize('source',['data:image/svg+xml;base64,@@','data:image/svg+xml,%FF',
    'data:image/svg+xml,','data:image/svg+xml;base64,PHN2Zy8+',None,42])
def test_svg_data_image_malformed_values_are_not_media(source):
    from nullion.artifact_evidence import svg_data_image_document
    assert svg_data_image_document(source) is None


@pytest.mark.parametrize('label',['R&D','Food &amp; music','Food&nbsp;&copy;','Price &#128;',
    '<![CDATA[Food & music]]>'])
def test_inline_svg_uses_html_character_reference_rules_for_its_xml_validation(label):
    from nullion.artifact_evidence import self_contained_html_svg_count,inline_svg_xml_document
    from xml.etree import ElementTree as ET
    source=SVG.replace('Four colors',label)
    assert self_contained_html_svg_count(source)==1
    root=ET.fromstring(inline_svg_xml_document(source))
    assert root.find('title').text
