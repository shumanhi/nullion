"""Real review, Web DOM, Telegram HTTP bytes, SQLite and restored history."""
import os
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('surface',['web_api','web_ui','telegram'])
@pytest.mark.parametrize('note',['The draft needs correction.','Corrige la presentación.','表示を修正してください。'])
def test_partial_comparison_keeps_each_pair_readable_on_delivery_and_reload(tmp_path,monkeypatch,surface,note):
    from tests.e2e.test_reviewer_publication_workflow import test_review_notes_never_reach_ui_delivery_or_saved_history as replay
    replay(tmp_path,monkeypatch,surface,note,structured=True)


@pytest.mark.parametrize('body,expected',[
    ('- **A**\n  Detail A\n\n  More A\n- **B**\n  Detail B\n\nAfter.', ['A\nDetail A\n\nMore A','B\nDetail B']),
    ('- 外側\n  - 内側\n    詳細\n- 次', ['外側','内側\n詳細','次']),
    ('Un párrafo; no se divide. 1. Literal. 2. Literal.\n\n- Uno\n  Valor: 7\n- Dos\n  Valor: pendiente', ['Uno\nValor: 7','Dos\nValor: pendiente']),
])
def test_web_keeps_indented_details_inside_their_items_after_history_reload(tmp_path,monkeypatch,body,expected):
    from playwright.sync_api import sync_playwright,expect
    from tests.e2e.test_web_surface_contracts import _web_app,_UvicornThread,_free_port
    from nullion import chat_store
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));chat_store._store=None
    app,_,_=_web_app(tmp_path)
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        context=browser.new_context(viewport={'width':430,'height':932})
        cid='web:list-continuations'
        context.add_init_script("localStorage.setItem('nullion_conv_id', "+__import__('json').dumps(cid)+");")
        assert context.request.post(server.base_url+'/api/chat/history/'+cid+'/message',
            data={'role':'bot','text':body}).ok
        page=context.new_page();page.goto(server.base_url,wait_until='domcontentloaded')
        for _ in range(2):
            items=page.locator('#messages .msg.bot li')
            expect(items).to_have_count(len(expected))
            for index,value in enumerate(expected):assert items.nth(index).inner_text()==value
            bubble=page.locator('#messages .msg.bot .bubble')
            assert '**' not in bubble.inner_text() and '<br><br><br>' not in bubble.inner_html()
            if body.startswith('Un párrafo'):assert 'Un párrafo; no se divide. 1. Literal. 2. Literal.' in bubble.inner_text()
            if body.endswith('After.'):assert 'After.' not in bubble.locator('ul').inner_text()
            page.reload(wait_until='domcontentloaded')
        browser.close()
