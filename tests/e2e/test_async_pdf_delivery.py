"""Real PDF tools on the async host used by approval continuations."""
import os
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.asyncio
@pytest.mark.parametrize('content_kind', ['text_pages', 'html_pages'])
async def test_pdf_creation_on_async_host_preserves_text_and_links(tmp_path, content_kind):
    from nullion.tools import ToolInvocation, _build_pdf_create_handler
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from pypdf import PdfReader
    path = tmp_path/'report.pdf'
    link = 'https://example.org/search?date=2026-10-28&return=2026-11-01'
    text = '# Fare comparison\n\n| Window | Fare |\n| --- | --- |\n| **October 28–November 1** | USD 398 |\n\n[Source]('+link+')'
    markup = '<h1>Fare comparison</h1><table><tr><td>October 28–November 1</td><td>USD 398</td></tr></table><a href="'+link+'">Source</a>'
    result = _build_pdf_create_handler(tmp_path)(ToolInvocation(
        invocation_id='pdf-async', tool_name='pdf_create', principal_id='operator',
        arguments={'output_path':str(path), content_kind:[text if content_kind == 'text_pages' else markup]}))
    assert result.status == 'completed', result
    assert result.output['text_layer'] is True
    reader = PdfReader(path)
    visible = '\n'.join(p.extract_text() for p in reader.pages)
    assert 'October 28' in visible and 'November 1' in visible and 'USD 398' in visible
    assert '**' not in visible and '# ' not in visible and '| ---' not in visible
    assert link in [a.get_object().get('/A', {}).get('/URI') for p in reader.pages for a in p.get('/Annots', [])]
    delivery = prepare_reply_for_platform_delivery('Report attached.\nMEDIA:'+str(path), principal_id='operator')
    assert path in delivery.attachments and delivery.attachments[0].read_bytes() == path.read_bytes()
