"""Dedicated renderers use shipped browser discovery without changing policy."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from nullion import tools


@pytest.mark.parametrize('preferred', [True,False])
def test_renderer_preserves_configured_system_precedence_before_managed(tmp_path,monkeypatch,preferred):
    system=tmp_path/'system-chromium';system.write_text('binary')
    managed=tmp_path/'managed-chromium';managed.write_text('binary')
    monkeypatch.setattr(tools,'_pdf_chromium_executable',lambda:str(system) if preferred else None)
    playwright=SimpleNamespace(chromium=SimpleNamespace(executable_path=str(managed)))
    assert tools._chromium_renderer_executable(playwright)==str(system if preferred else managed)


@pytest.mark.parametrize('candidate',['missing','directory',None])
def test_renderer_never_claims_an_uninstalled_managed_binary(tmp_path,monkeypatch,candidate):
    monkeypatch.setattr(tools,'_pdf_chromium_executable',lambda:None)
    path=tmp_path/'not-a-browser'
    if candidate=='directory':path.mkdir()
    value=str(path) if candidate is not None else None
    assert tools._chromium_renderer_executable(SimpleNamespace(chromium=SimpleNamespace(executable_path=value))) is None


@pytest.mark.parametrize('renderer',['text','designed','image'])
def test_no_browser_fails_before_launch_or_artifact_creation(tmp_path,monkeypatch,renderer):
    from playwright import sync_api
    monkeypatch.setattr(tools,'_pdf_chromium_executable',lambda:None)
    class Chromium:
        executable_path=str(tmp_path/'not-installed')
        def launch(self,**kwargs):pytest.fail('Missing browser must not be launched')
    @contextmanager
    def playwright():yield SimpleNamespace(chromium=Chromium())
    monkeypatch.setattr(sync_api,'sync_playwright',playwright)
    output=tmp_path/('report.png' if renderer=='image' else 'report.pdf')
    if renderer=='text':
        assert tools._save_text_pdf_with_chromium(output,title='Report',text_pages=['Native content'],image_paths=[],page_size_name='a4') is False
    elif renderer=='designed':
        assert tools._save_html_pdf_with_chromium(output,title='Report',html_pages=['<p>Native content</p>'],page_size_name='a4') is False
    else:
        from nullion.image_render import _render
        with pytest.raises(RuntimeError,match='installed Chromium renderer is unavailable'):_render('<p>Native content</p>',output,800,1)
    assert not output.exists()
