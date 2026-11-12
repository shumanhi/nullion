"""Real Chromium PDFs keep section headings with the content they introduce."""
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('rows', [19, 21])
@pytest.mark.parametrize('heading', ['Recommendations', 'Recomendaciones'])
def test_pdf_print_keeps_heading_with_next_block_and_preserves_all_text(tmp_path, monkeypatch, rows, heading):
    from playwright.sync_api import sync_playwright
    from pypdf import PdfReader
    from nullion.tools import ToolInvocation, _build_pdf_create_handler
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    with sync_playwright() as playwright:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE', playwright.chromium.executable_path)
    output = tmp_path / 'section-report.pdf'
    markup = ''.join(f'<p style="height:80px;margin:0">Record {i}</p>' for i in range(rows))
    markup += f'<h2>{heading}</h2><p style="height:350px;margin:0">DETAILS FOLLOW</p>'
    result = _build_pdf_create_handler(tmp_path)(ToolInvocation(
        invocation_id='pdf-pagination', tool_name='pdf_create', principal_id='operator',
        arguments={'output_path': str(output), 'html_pages': [markup], 'title': 'Section report'}))
    assert result.status == 'completed', result
    pages = [page.extract_text() for page in PdfReader(output).pages]
    assert len(pages) == 2, pages
    assert any(heading in page and 'DETAILS FOLLOW' in page for page in pages), pages
    visible = '\n'.join(pages)
    assert all(f'Record {index}' in visible for index in range(rows)), visible
    delivery = prepare_reply_for_platform_delivery('Report attached.\nMEDIA:' + str(output), principal_id='operator')
    assert delivery.attachments == (output,)
    assert delivery.attachments[0].read_bytes().startswith(b'%PDF-')
