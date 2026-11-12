"""Persisted replies retain native turn identity through real Web reloads."""
import json
import os

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker browser E2E')


@pytest.mark.parametrize('copy',['条件を保存しました。','La condición está guardada.'])
@pytest.mark.parametrize('archived',[False,True])
def test_web_history_restores_terminal_reply_identity(tmp_path,monkeypatch,copy,archived):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright,expect
    from tests.e2e.test_web_surface_contracts import _web_app,_UvicornThread,_free_port
    import nullion.chat_store as chat_store
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setattr(chat_store,'_store',None)
    app,runtime,_=_web_app(tmp_path)
    cid='web:restore-owned';tid='turn:restored-owned'
    with TestClient(app) as client:
        response=client.post('/api/chat/history/'+cid+'/message',json={
            'role':'bot','text':copy,'metadata':{'runtime_turn_id':tid}})
        assert response.status_code==200,response.text
        if archived:
            assert client.post('/api/chat/history/'+cid+'/archive').status_code==200
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        context=browser.new_context()
        context.add_init_script("localStorage.setItem('nullion_chat_restore_suppressed','true');"
            "localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
        page=context.new_page();errors=[]
        page.on('pageerror',lambda e:errors.append(str(e)))
        try:
            page.goto(server.base_url,wait_until='domcontentloaded')
            for _ in range(2):
                if archived:page.evaluate('(cid) => viewArchivedConv(cid,"Archived owned")',cid)
                bubble=page.locator('#messages .msg.bot .bubble[data-turn-id='+json.dumps(tid)+']')
                expect(bubble).to_have_count(1)
                expect(bubble).to_contain_text(copy)
                assert page.evaluate('(id) => _botTurnBubbles.get(id) === findBotBubbleByTurnId(id)',tid)
                assert page.evaluate('(id) => _finishedTurnIds.has(id)',tid)
                page.reload(wait_until='domcontentloaded')
            assert not errors
        finally:browser.close()
