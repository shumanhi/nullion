"""Actual restored Web cards save through native POST /save, without generation."""
import io
import json
import os
from pathlib import Path

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker Web UI')


@pytest.mark.parametrize('extension',['.png','.pdf'])
def test_restored_artifact_ui_save_receipt_matches_native_file_bytes(tmp_path,monkeypatch,extension):
    from PIL import Image
    from pypdf import PdfWriter
    from playwright.sync_api import sync_playwright,expect
    from nullion import web_app
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.e2e.test_web_surface_contracts import _UvicornThread,_free_port
    from tests.e2e.live_web_artifact_assertions import save_artifact_through_ui, selected_artifact_conversation_script
    runtime=isolate(tmp_path,monkeypatch)
    conversation='web:native-save:'+extension[1:];identity='owned-artifact-turn'
    root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    source=root/('owned-save'+extension)
    if extension=='.png':Image.new('RGB',(120,80),'blue').save(source)
    else:
        writer=PdfWriter();writer.add_blank_page(width=595.28,height=841.89)
        with source.open('wb') as stream:writer.write(stream)
    body=source.read_bytes()
    from nullion import chat_store
    chat_store._store=None
    mirror=chat_store.get_chat_store()
    mirror.save_message(conversation,'bot','Saved owned artifact.',metadata={
        'runtime_turn_id':identity,'history_message_kind':'terminal_reply','artifacts':[{'path':str(source)}]})
    mirror.save_message('web:later-unrelated','bot','Later unrelated conversation.')
    # Native files remain real; only the OS destination directory is isolated.
    unique=web_app._unique_download_path
    destination=tmp_path/'Downloads'
    monkeypatch.setattr(web_app,'_unique_download_path',lambda name:unique(name,downloads_dir=destination))
    app=web_app.create_app(runtime,orchestrator=None,registry=ToolRegistry())
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        context=browser.new_context();context.add_init_script(selected_artifact_conversation_script(conversation))
        page=context.new_page();requests=[];page.on('request',lambda request:requests.append(request.url)
            if request.method=='POST' and request.url==server.base_url+'/api/chat' else None)
        try:
            page.goto(server.base_url,wait_until='domcontentloaded')
            history=page.request.get(server.base_url+'/api/chat/history/'+conversation).json()
            message=next(row for row in history['messages'] if row.get('metadata',{}).get('runtime_turn_id')==identity)
            artifact=message['artifacts'][0]
            if extension=='.png':
                card=page.locator('.image-artifact-card').filter(has_text=source.name)
                expect(card).to_have_count(1)
                control=card.locator('button.image-artifact-btn').nth(1)
                expect(control).to_have_text('Download')
            else:
                control=page.locator('#messages .artifact-list a.artifact-link[href='+json.dumps(artifact['url'])+']')
                expect(control).to_have_text('Download '+source.name)
            expect(control).to_be_visible()
            served,receipt=save_artifact_through_ui(page,server.base_url,artifact,control)
            assert served==body and Path(receipt['path']).read_bytes()==body
            assert source.read_bytes()==body and not requests
        finally:browser.close()
