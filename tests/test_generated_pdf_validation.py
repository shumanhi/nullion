"""Native generated documents must have readable PDF page containers."""
import zipfile

import pytest


def write_pdf(path, kind):
    from pypdf import PdfWriter
    from pypdf.generic import NameObject, DictionaryObject, DecodedStreamObject, ArrayObject
    writer = PdfWriter()
    page = writer.add_blank_page(200, 200)
    if kind != 'blank':
        content = DictionaryObject() if kind == 'invalid' else DecodedStreamObject()
        if kind != 'invalid':
            content.set_data(b'q 1 0 0 rg 10 10 50 50 re f Q')
        reference = writer._add_object(content)
        page[NameObject('/Contents')] = ArrayObject([reference]) if kind == 'array' else reference
    writer.write(path)


@pytest.mark.parametrize('kind', ['blank', 'stream', 'array', 'invalid'])
@pytest.mark.parametrize('packed', [False, True])
def test_native_generated_pdf_structure_including_archive_members(tmp_path, kind, packed):
    from nullion.artifact_validation import validate_generated_document_paths
    pdf = tmp_path / 'source.pdf'
    write_pdf(pdf, kind)
    target = pdf
    if packed:
        target = tmp_path / 'pack.zip'
        with zipfile.ZipFile(target, 'w') as archive:
            archive.write(pdf, pdf.name)
    result = validate_generated_document_paths([target])
    assert result.ok is (kind != 'invalid')
    if not result.ok:
        assert result.issues[0].path == str(target)
        assert 'Contents' in result.issues[0].message


def test_invalid_native_output_cannot_be_selected_until_a_new_write(tmp_path):
    from nullion.artifact_lifecycle import filter_delivery_paths
    from nullion.tools import ToolResult
    pdf = str(tmp_path / 'report.pdf')
    csv = str(tmp_path / 'rows.csv')
    bad = ToolResult('one', 'terminal_exec', 'completed', {'artifact_paths': [pdf, csv]},
        response_requirements={'_native_artifact_validation': {'version': 1, 'invocation_id': 'one', 'invalid_paths': [pdf]}})
    assert filter_delivery_paths([pdf, csv], [bad]) == [csv]
    read = ToolResult('two', 'file_read', 'completed', {'path': pdf})
    assert filter_delivery_paths([pdf, csv], [bad, read]) == [csv]
    fixed = ToolResult('three', 'pdf_create', 'completed', {'artifact_path': pdf})
    assert filter_delivery_paths([pdf, csv], [bad, read, fixed]) == [pdf, csv]
    assert filter_delivery_paths([pdf], [read]) == [pdf]


def test_invalid_native_output_is_not_fulfilled_by_an_explicit_media_pointer(tmp_path):
    from nullion.response_fulfillment_contract import evaluate_response_fulfillment
    from nullion.tools import ToolResult
    path = tmp_path / 'bad.pdf'
    write_pdf(path, 'invalid')
    receipt = ToolResult('one', 'terminal_exec', 'completed', {'artifact_paths': [str(path)]},
        response_requirements={'_native_artifact_validation': {'version': 1, 'invocation_id': 'one', 'invalid_paths': [str(path)]}})
    result = evaluate_response_fulfillment(store=None, conversation_id='web:pdf', user_message='資料', reply='MEDIA:'+str(path),
        tool_results=[receipt], artifact_paths=[str(path)], artifact_roots=[tmp_path],
        required_attachment_extensions=['.pdf'])
    assert not result.satisfied
