"""Typed PDF pagination defaults and failure boundaries."""
import pytest


@pytest.mark.parametrize('arguments,expected', [({}, 'flow'), ({'pagination': 'flow', 'expected_page_count': 2}, 'flow'),
    ({'expected_page_count': 2}, 'pages'), ({'media_alignment': 'align_pages'}, 'pages'),
    ({'media_alignment': 'preserve_text_pages'}, 'flow'), ({'pagination': 'pages'}, 'pages')])
def test_typed_pagination_defaults(arguments, expected):
    from nullion.tools import _pdf_pagination_mode
    assert _pdf_pagination_mode(arguments) == expected


@pytest.mark.parametrize('value', ['compact', True, 2, ['flow']])
def test_invalid_pagination_is_actionable_before_any_artifact_write(tmp_path, value):
    from nullion.tools import ToolInvocation, _build_pdf_create_handler
    result = _build_pdf_create_handler(tmp_path)(ToolInvocation('bad-pagination', 'pdf_create', 'operator', {
        'output_path': str(tmp_path / 'invalid.pdf'), 'text_pages': ['Contenido'], 'pagination': value}))
    assert result.status == 'failed' and result.output['reason'] == 'invalid_tool_arguments'
    assert result.output['recovery']['kind'] == 'correct_arguments'
    assert not (tmp_path / 'invalid.pdf').exists()


def test_unavailable_text_renderer_cannot_fall_back_to_a_clipped_canvas(tmp_path, monkeypatch):
    from nullion import tools
    monkeypatch.setattr(tools, '_save_text_pdf_with_chromium', lambda *args, **kwargs: False)
    def forbidden(*args, **kwargs):
        raise AssertionError('Fixed-height raster fallback must not render report text')
    monkeypatch.setattr(tools, '_build_pdf_pages', forbidden)
    result = tools._build_pdf_create_handler(tmp_path)(tools.ToolInvocation('no-renderer', 'pdf_create', 'operator', {
        'text_pages': [' '.join(f'Registro{i}' for i in range(2000))], 'output_path': str(tmp_path / 'long.pdf')}))
    assert result.status == 'failed' and result.output['reason'] == 'text_pdf_render_failed'
    assert not result.output.get('artifact_paths') and not (tmp_path / 'long.pdf').exists()


def test_pdf_edit_unavailable_text_renderer_does_not_publish_partial_output(tmp_path, monkeypatch):
    from pypdf import PdfWriter
    from nullion import tools
    original = tmp_path / 'original.pdf'; writer = PdfWriter(); writer.add_blank_page(612, 792); writer.write(original)
    before = original.read_bytes(); target = tmp_path / 'edited.pdf'
    monkeypatch.setattr(tools, '_save_text_pdf_with_chromium', lambda *args, **kwargs: False)
    result = tools._build_pdf_edit_handler(tmp_path)(tools.ToolInvocation('no-edit-renderer', 'pdf_edit', 'operator', {
        'input_path': str(original), 'output_path': str(target), 'append_text_pages': ['Mucho contenido ' * 2000]}))
    assert result.status == 'failed' and result.output['reason'] == 'text_pdf_render_failed'
    assert original.read_bytes() == before and not target.exists()
    assert not list(tmp_path.glob('.*-text-*.pdf')) and not result.output.get('artifact_paths')
