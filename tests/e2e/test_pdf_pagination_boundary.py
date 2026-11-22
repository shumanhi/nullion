"""Delivered PDF pagination uses native Chromium, storage and the chat API."""
import io
import os
from pathlib import Path

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Native PDF boundary in Docker')


def _download_report(tmp_path, monkeypatch, arguments, *, label, repair_arguments=None):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_pdf_create_handler
    runtime = setup_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    with sync_playwright() as playwright:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE', playwright.chromium.executable_path)
    cid = 'web:pagination-' + label
    root = artifact_root_for_principal(cid); root.mkdir(parents=True, exist_ok=True)
    output = root / 'report.pdf'
    native = _build_pdf_create_handler(tmp_path, allowed_roots=[tmp_path]); receipts = []
    def create(invocation):
        result = native(invocation)
        if result.output.get('reason') == 'html_pdf_unplaced_media':
            assert not output.exists() and not result.output.get('artifact_paths')
        receipts.append(result); return result
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('pdf_create', 'Create a local PDF report', ToolRiskLevel.LOW,
                              ToolSideEffectClass.WRITE, False, 30), create)
    class Provider:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            names = {tool['name'] for tool in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': '.pdf', 'confidence': 1})
            if not names: return text_response('{}')
            self.phase += 1
            if self.phase == 1:
                return tool_response('request_tool_scope', {'capabilities': ['local_files'],
                    'tool_names': ['pdf_create'], 'required_tool_names': ['pdf_create'], 'artifact_extensions': ['.pdf']})
            if self.phase == 2:
                return tool_response('pdf_create', {'output_path': str(output), **arguments})
            if self.phase == 3 and repair_arguments is not None:
                return tool_response('pdf_create', {'output_path': str(output), **repair_arguments})
            assert self.phase == (4 if repair_arguments is not None else 3), receipts
            return text_response('Listo.\nMEDIA:' + str(output))
    model = Provider(); runtime.model_client = model
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as api:
        response = api.post('/api/chat', json={'text': 'Prepara el informe PDF adjunto.', 'conversation_id': cid}).json()
        assert len(receipts) == (2 if repair_arguments is not None else 1) and receipts[-1].status == 'completed', (response, receipts)
        if repair_arguments is not None:
            assert receipts[0].status == 'failed' and receipts[0].output['reason'] == 'html_pdf_unplaced_media'
        assert len(response.get('artifacts', [])) == 1, response
        downloaded = api.get(response['artifacts'][0]['url'])
        assert downloaded.status_code == 200 and downloaded.content == output.read_bytes()
        turn = next(event for event in reversed(runtime.store.list_conversation_events(cid))
                    if event.get('event_type') == 'conversation.chat_turn')
        assert turn['response_fulfilled'] is True, turn
        assert [result for result in turn['tool_results'] if result['tool_name'] == 'pdf_create'][-1]['status'] == 'completed'
    receipt = dict(receipts[-1].output)
    if repair_arguments is not None: receipt['prior_failure'] = receipts[0].output
    return downloaded.content, receipt


def _inspect(data, label):
    import pypdfium2 as pdfium
    destination = Path('/outputs/pdf-pagination') if Path('/outputs').is_dir() else Path('.e2e-artifacts/pdf-pagination')
    destination.mkdir(parents=True, exist_ok=True)
    (destination / (label + '.pdf')).write_bytes(data)
    pages = []
    with pdfium.PdfDocument(data) as document:
        for number in range(len(document)):
            page = document[number]; bitmap = page.render(scale=1.25); text = page.get_textpage()
            try:
                bitmap.to_pil().save(destination / f'{label}-{number + 1}.png')
                chars = []
                for index in range(text.count_chars()):
                    left, bottom, right, top = text.get_charbox(index)
                    chars.append({'text': text.get_text_range(index, 1), 'size': pdfium.raw.FPDFText_GetFontSize(text.raw, index),
                                  'left': left, 'bottom': bottom, 'right': right, 'top': top})
                pages.append({'text': text.get_text_range(), 'chars': chars,
                              'width': page.get_width(), 'height': page.get_height()})
            finally: text.close(); bitmap.close(); page.close()
    return pages


def test_compact_sections_flow_without_reserving_five_sheets(tmp_path, monkeypatch):
    fragments = [f'<article><h2>Sección {i}</h2><p>Registro {i} — precio 25 EUR. '
                 'Una descripción breve y una referencia verificada para esta selección.</p>'
                 f'<p>Referencia {i}: https://example.test/item/{i}</p></article>' for i in range(1, 6)]
    data, receipt = _download_report(tmp_path, monkeypatch, {'html_pages': fragments}, label='compact')
    pages = _inspect(data, 'compact')
    assert len(pages) == 1, [(p['text'], p['height']) for p in pages]
    for i in range(1, 6): assert f'Registro {i}' in pages[0]['text']
    assert min(char['size'] for char in pages[0]['chars'] if char['text'].strip()) >= 10
    assert receipt['pagination'] == 'flow'


@pytest.mark.parametrize('paper', ['letter', 'a4'])
def test_long_blocks_tables_and_headings_flow_at_readable_size(tmp_path, monkeypatch, paper):
    paragraph = ' '.join(f'P{i:03d} descripción' for i in range(260))
    oversized = ' '.join(f'C{i:03d} contenido' for i in range(260))
    rows = ''.join(f'<tr><td>R{i:03d}</td><td>Dato verificado {i} — detalle y fuente.</td></tr>' for i in range(40))
    markup = (f'<main style="break-inside:avoid"><h1>Informe largo — análisis</h1><p>{paragraph}</p>'
              f'<h2>Tabla de resultados</h2><table><thead><tr><th>Código</th><th>Descripción</th></tr></thead><tbody>{rows}</tbody></table>'
              f'<article style="height:130px;overflow:hidden;break-inside:avoid"><h2>Bloque extenso</h2><p>{oversized}</p></article>'
              '<h2>Conclusión enlazada</h2><p>FIRST FOLLOWING LINE. Todos los registros están presentes.</p></main>')
    data, receipt = _download_report(tmp_path, monkeypatch, {'html_pages': [markup], 'page_size': paper}, label='long-' + paper)
    pages = _inspect(data, 'long-' + paper)
    assert 3 <= len(pages) <= 5, [p['text'] for p in pages]
    visible = '\n'.join(page['text'] for page in pages)
    for prefix, count in [('P', 260), ('C', 260), ('R', 40)]:
        for i in range(count): assert f'{prefix}{i:03d}' in visible, (prefix, i, visible)
    assert sum('Código' in page['text'] for page in pages) >= 2  # Native Chromium repeats thead.
    assert any('Conclusión enlazada' in p['text'] and 'FIRST FOLLOWING LINE' in p['text'] for p in pages)
    for index, page in enumerate(pages):
        expected_size = (612, 792) if paper == 'letter' else (595.28, 841.89)
        assert abs(page['width'] - expected_size[0]) < 1 and abs(page['height'] - expected_size[1]) < 1
        chars = [c for c in page['chars'] if c['text'].strip()]
        assert chars and min(c['size'] for c in chars) >= 10
        assert all(c['left'] >= 28 and c['right'] <= page['width'] - 28 and c['bottom'] >= 28 and c['top'] <= page['height'] - 28 for c in chars)
        if index < len(pages) - 1:
            assert min(c['bottom'] for c in chars) < page['height'] * .32, (index, page['text'])
    assert receipt['pagination_receipt']['scale'] == 1


@pytest.mark.parametrize('paper', ['letter', 'a4'])
def test_explicit_section_boundaries_are_preserved_without_shrinking(tmp_path, monkeypatch, paper):
    data, receipt = _download_report(tmp_path, monkeypatch, {'text_pages': ['# Primera sección\n\nContenido UNO.', '# Deuxième section\n\nContenu DEUX.'],
        'page_size': paper, 'pagination': 'pages', 'expected_page_count': 2}, label='pages-' + paper)
    pages = _inspect(data, 'pages-' + paper)
    assert len(pages) == 2
    assert 'UNO' in pages[0]['text'] and 'DEUX' not in pages[0]['text'] and 'DEUX' in pages[1]['text']
    assert receipt['pagination'] == 'pages'
    assert min(c['size'] for p in pages for c in p['chars'] if c['text'].strip()) >= 10


def test_oversized_exact_page_constraint_is_repairable_without_losing_content(tmp_path, monkeypatch):
    from playwright.sync_api import sync_playwright
    from nullion.tools import ToolInvocation, _build_pdf_create_handler
    with sync_playwright() as playwright:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE', playwright.chromium.executable_path)
    target = tmp_path / 'overflow.pdf'
    markup = '<article><h1>Informe completo</h1><p>' + ' '.join(f'COMPLETE{i:04d}' for i in range(1500)) + '</p></article>'
    result = _build_pdf_create_handler(tmp_path)(ToolInvocation('overflow', 'pdf_create', 'operator', {
        'output_path': str(target), 'html_pages': [markup], 'expected_page_count': 1}))
    assert result.status == 'failed' and result.output['reason'] == 'artifact_page_count_mismatch', result
    assert result.output['recovery']['kind'] == 'repair_existing_artifact'
    assert not result.output.get('artifact_paths')
    pages = _inspect(target.read_bytes(), 'exact-overflow')
    assert len(pages) > 1
    visible = '\n'.join(p['text'] for p in pages)
    assert all(f'COMPLETE{i:04d}' in visible for i in range(1500))
    assert min(c['size'] for p in pages for c in p['chars'] if c['text'].strip()) >= 10


def test_indexed_media_are_embedded_once_when_sections_are_fewer_than_images(tmp_path, monkeypatch):
    from PIL import Image
    from pypdf import PdfReader
    colors = [(210, 30, 40), (20, 170, 60), (30, 60, 220)]
    sources = []
    for index, color in enumerate(colors):
        path = tmp_path / f'source-{index}.png'; Image.new('RGB', (90 + index * 10, 70), color).save(path); sources.append(str(path))
    data, receipt = _download_report(tmp_path, monkeypatch, {'html_pages': [
        '<h2>Primer conjunto</h2><p>Registro asociado UNO.</p>',
        '<h2>Segundo conjunto</h2><p>Registros asociados DOS y TRES.</p>'],
        'image_paths': sources}, label='indexed-media')
    pages = _inspect(data, 'indexed-media')
    assert receipt['source_image_paths'] == sources
    images = [Image.open(io.BytesIO(image.data)).convert('RGB') for page in PdfReader(io.BytesIO(data)).pages for image in page.images]
    try:
        assert len(images) == 3
        assert sorted(image.getpixel((5, 5)) for image in images) == sorted(colors)
    finally:
        for image in images: image.close()
    assert all(label in '\n'.join(p['text'] for p in pages) for label in ('UNO', 'DOS', 'TRES'))


@pytest.mark.parametrize('design', ['original-flyer', 'responsive-extra-media'])
def test_authored_oversized_posters_preserve_content_and_report_count_constraint(tmp_path, monkeypatch, design):
    from playwright.sync_api import sync_playwright
    from nullion.tools import ToolInvocation, _build_pdf_create_handler
    from tests.test_inline_svg_media import SVG
    with sync_playwright() as playwright:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE', playwright.chromium.executable_path)
    directory = Path(__file__).parents[1] / 'fixtures/artifacts'
    if design == 'original-flyer':
        markup = (directory / 'inline_svg_flyer.html').read_text(); images = []
    else:
        import re
        from PIL import Image
        markup = re.sub(r'<img\s[^>]+>', SVG, (directory / 'responsive_poster.html').read_text())
        source = tmp_path / 'extra.png'; Image.new('RGB', (200, 100), 'purple').save(source); images = [str(source)]
    output = tmp_path / 'poster.pdf'
    result = _build_pdf_create_handler(tmp_path)(ToolInvocation('poster-size', 'pdf_create', 'operator', {
        'output_path': str(output), 'html_pages': [markup], 'image_paths': images, 'expected_page_count': 1},
        flow_context={'required_embedded_media_extensions': ['.pdf']}))
    assert result.status == 'failed' and result.output['reason'] == 'artifact_page_count_mismatch', result
    pages = _inspect(output.read_bytes(), design)
    compact = ''.join(''.join(page['text'] for page in pages).split())
    assert 'STAGEFORMATQA' in compact and 'Community'.lower() in compact.lower()
    assert result.output['page_count'] == len(pages) > 1
    assert result.output['inspection']['embedded_image_count'] == (1 if design == 'original-flyer' else 2)


def test_typed_card_sections_flow_together_with_readable_bodies(tmp_path, monkeypatch):
    content = [{'title': f'Colección {section}', 'cards': [
        {'heading': f'Ficha {index}', 'text': f'REGISTRO{index} — detalles verificados, precio 25 EUR y referencia de la selección.'}
        for index in range(section * 4, section * 4 + 4)]} for section in range(2)]
    data, receipt = _download_report(tmp_path, monkeypatch, {'card_pages': content}, label='cards')
    pages = _inspect(data, 'cards'); assert len(pages) == 1
    assert all(f'REGISTRO{i}' in pages[0]['text'] for i in range(8))
    assert min(c['size'] for c in pages[0]['chars'] if c['text'].strip()) >= 10
    assert receipt['pagination_receipt']['section_count'] == 2


def test_long_pdf_edit_appendix_preserves_base_media_and_every_text_record(tmp_path, monkeypatch):
    from PIL import Image
    from pypdf import PdfReader
    from playwright.sync_api import sync_playwright
    from nullion.tools import ToolInvocation, _build_pdf_create_handler, _build_pdf_edit_handler
    with sync_playwright() as playwright:
        monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE', playwright.chromium.executable_path)
    create = _build_pdf_create_handler(tmp_path)
    base, extra = tmp_path / 'base.pdf', tmp_path / 'extra.pdf'
    for path, label in [(base, 'ORIGINAL BASE'), (extra, 'APPENDED PDF')]:
        result = create(ToolInvocation(label, 'pdf_create', 'operator', {'output_path': str(path), 'text_pages': [label]}))
        assert result.status == 'completed', result
    original = base.read_bytes(); output = tmp_path / 'edited.pdf'
    picture = tmp_path / 'append.png'; Image.new('RGB', (80, 60), 'orange').save(picture)
    text = ' '.join(f'APPEND{i:04d}' for i in range(1300))
    result = _build_pdf_edit_handler(tmp_path)(ToolInvocation('edit-long', 'pdf_edit', 'operator', {
        'input_path': str(base), 'output_path': str(output), 'append_pdf_paths': [str(extra)],
        'append_image_paths': [str(picture)], 'append_text_pages': [text]}))
    assert result.status == 'completed', result
    pages = _inspect(output.read_bytes(), 'edited-long')
    assert base.read_bytes() == original and 'ORIGINAL BASE' in pages[0]['text'] and 'APPENDED PDF' in pages[1]['text']
    assert len(PdfReader(output).pages[2].images) == 1
    visible = '\n'.join(p['text'] for p in pages[3:])
    assert all(f'APPEND{i:04d}' in visible for i in range(1300))
    assert len(pages) >= 6 and not list(tmp_path.glob('.*-text-*.pdf'))
    assert min(c['size'] for p in pages[3:] for c in p['chars'] if c['text'].strip()) >= 10


def test_short_fixed_height_card_cannot_hide_its_last_lines(tmp_path, monkeypatch):
    from PIL import Image
    markup = '<main><article style="height:50px;overflow:hidden"><h2>Ficha breve</h2>' + ''.join(
        f'<p>Registro corto {index} — contenido legible.</p>' for index in range(4)) + '<p style="color:#c00000">FINAL VISIBLE</p></article></main>'
    data, _ = _download_report(tmp_path, monkeypatch, {'html_pages': [markup]}, label='short-clipped-card')
    pages = _inspect(data, 'short-clipped-card')
    assert len(pages) == 1 and 'FINAL VISIBLE' in pages[0]['text']
    destination = Path('/outputs/pdf-pagination') if Path('/outputs').is_dir() else Path('.e2e-artifacts/pdf-pagination')
    with Image.open(destination / 'short-clipped-card-1.png') as bitmap:
        assert sum(1 for r, g, b in bitmap.convert('RGB').getdata() if r > 120 and g < 60 and b < 60) > 80


def test_mixed_authored_media_require_exact_placement_before_download(tmp_path, monkeypatch):
    from PIL import Image
    from pypdf import PdfReader
    sources = []
    colors = [(180, 25, 35), (25, 150, 65)]
    for index, color in enumerate(colors):
        path = tmp_path / f'authored-{index}.png'; Image.new('RGB', (80, 60), color).save(path); sources.append(str(path))
    args = {'html_pages': [f'<h1>Selección completa</h1><img src="{sources[0]}">'], 'image_paths': sources}
    repaired = {'html_pages': [f'<h1>Selección completa</h1><img src="{sources[0]}"><img src="{sources[1]}">'], 'image_paths': sources}
    data, receipt = _download_report(tmp_path, monkeypatch, args, label='mixed-media', repair_arguments=repaired)
    assert receipt['prior_failure']['unplaced_media_paths'] == [sources[1]]
    assert receipt['prior_failure']['recovery']['kind'] == 'correct_arguments'
    images = [Image.open(io.BytesIO(image.data)).convert('RGB') for page in PdfReader(io.BytesIO(data)).pages for image in page.images]
    try:
        assert len(images) == 2 and sorted(image.getpixel((5, 5)) for image in images) == sorted(colors)
    finally:
        for image in images: image.close()
    _inspect(data, 'mixed-media')


def test_native_card_image_keeps_authored_max_height(tmp_path, monkeypatch):
    import math
    from PIL import Image
    from pypdf import PdfReader
    path = tmp_path / 'portrait.png'; Image.new('RGB', (400, 1600), '#e8a53b').save(path)
    data, _ = _download_report(tmp_path, monkeypatch, {'card_pages': [{'title': 'Tarjeta compacta', 'cards': [
        {'heading': 'Imagen completa', 'text': 'Conservar proporciones y tamaño editorial.', 'image_path': str(path)}]}]}, label='image-height')
    pages = _inspect(data, 'image-height'); assert len(pages) == 1
    heights = []
    document = PdfReader(io.BytesIO(data))
    def capture(operator, operands, matrix, text_matrix):
        if operator == b'Do':
            image = document.pages[0]['/Resources']['/XObject'][operands[0]].get_object()
            if image.get('/Subtype') == '/Image': heights.append(math.hypot(matrix[2], matrix[3]))
    document.pages[0].extract_text(visitor_operand_before=capture)
    assert len(heights) == 1 and 90 <= heights[0] <= 98, heights  # Native 130 CSS px = 97.5 pt.


@pytest.mark.parametrize('named', [False, True])
def test_typed_paper_size_cannot_be_silently_scaled_by_authored_page_rules(tmp_path, monkeypatch, named):
    selector = 'oversized:first' if named else ''
    markup = (f'<style>@page {selector} {{size:A3!important;margin:2in!important}}'
              + ('main{page:oversized}' if named else '')
              + '</style><main><p style="font-size:12pt">PAPER CONTRACT — tamaño legible</p></main>')
    label = 'paper-contract-' + ('named' if named else 'ordinary')
    data, _ = _download_report(tmp_path, monkeypatch, {'html_pages': [markup], 'page_size': 'letter'}, label=label)
    pages = _inspect(data, label); assert len(pages) == 1
    assert abs(pages[0]['width'] - 612) < 1 and abs(pages[0]['height'] - 792) < 1
    assert min(c['size'] for c in pages[0]['chars'] if c['text'].strip()) >= 11.9
    assert min(c['top'] - c['bottom'] for c in pages[0]['chars'] if c['text'] in 'PAPER') >= 7.8
    assert min(c['left'] for c in pages[0]['chars'] if c['text'].strip()) < 35  # fixed .42in margin
