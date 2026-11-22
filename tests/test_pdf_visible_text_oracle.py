"""Native glyph tracking must not invent spaces or erase real word boundaries."""
import io
import shutil

import pytest

pytestmark = pytest.mark.skipif(not shutil.which('pdftotext'), reason='Poppler is installed in the E2E Docker image')


def tracked_pdf(phrase):
    from pypdf import PdfWriter
    from pypdf.generic import NameObject, DictionaryObject, DecodedStreamObject
    writer = PdfWriter(); page = writer.add_blank_page(612, 792)
    font = DictionaryObject({NameObject('/Type'): NameObject('/Font'),
        NameObject('/Subtype'): NameObject('/Type1'), NameObject('/BaseFont'): NameObject('/Helvetica')})
    page[NameObject('/Resources')] = DictionaryObject({NameObject('/Font'): DictionaryObject({NameObject('/F1'): font})})
    stream = DecodedStreamObject()
    stream.set_data(('BT /F1 12 Tf 30 740 Td ['+' -350 '.join('('+char+')' for char in phrase)+'] TJ ET').encode())
    page[NameObject('/Contents')] = writer._add_object(stream)
    data = io.BytesIO(); writer.write(data); return data.getvalue()


@pytest.mark.parametrize('phrase', ['NORTH SOUTH VERIFIED REPORT', 'REGISTRO PUBLICO DEL NORTE'])
def test_pdf_oracle_retains_logical_words_despite_glyph_tracking(phrase):
    from pypdf import PdfReader
    from scripts.live_stage_chat_contract_probe import _validate_pdf_visual_content
    data = tracked_pdf(phrase)
    assert phrase not in PdfReader(io.BytesIO(data)).pages[0].extract_text(), 'Fixture must reproduce the former extractor failure'
    text, _ = _validate_pdf_visual_content('tracked.pdf', data)
    assert phrase in text


@pytest.mark.parametrize('phrase', ['NORTH SOUTH VERIFIED REPORT', 'REGISTRO PUBLICO DEL NORTE'])
def test_pdf_oracle_does_not_fuse_or_invent_required_word_boundaries(phrase):
    from scripts.live_stage_chat_contract_probe import _validate_pdf_visual_content
    fused = phrase.replace(' ', '')
    text, _ = _validate_pdf_visual_content('fused.pdf', tracked_pdf(fused))
    assert fused in text and phrase not in text


@pytest.mark.parametrize('source,logical,raw',[
    ('CHECK-01-COMPLETE','CHECK-01- COMPLETE','CHECK-01-\r\n  COMPLETE'),
    ('確認-完了','確認- 完了','確認-\n完了'),
    ('código-verificado','código- verificado','código-\n  verificado'),
    ('確認‐完了','確認‐ 完了','確認‐\n完了'),
])
def test_source_dash_token_may_cross_only_a_physical_pdf_line_break(source,logical,raw):
    from tests.e2e.live_web_artifact_assertions import pdf_source_phrase_visible
    assert pdf_source_phrase_visible(source,logical,raw)


@pytest.mark.parametrize('source,actual',[
    ('CHECK-01-COMPLETE','CHECK-01- COMPLETE'),
    ('REGISTRO PUBLICO código-verificado','REGISTROPUBLICO código-\nverificado'),
    ('NORTH SOUTH CHECK-37','NORTHSOUTH CHECK-\n37'),
    ('確認 完了 code-37','確認完了 code-\n37'),
])
def test_source_dash_wrap_never_erases_literal_space_or_fuses_required_words(source,actual):
    from tests.e2e.live_web_artifact_assertions import pdf_source_phrase_visible
    assert not pdf_source_phrase_visible(source,actual,actual)
