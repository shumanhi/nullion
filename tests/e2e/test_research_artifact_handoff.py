"""Research -> native producer -> actual download/upload, including interruption.

Only model/image-provider and Telegram HTTP boundaries are simulated. Browser,
scope, orchestration, SQLite, filesystem and delivery contracts remain real.
"""
import asyncio
from datetime import datetime, UTC
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

import httpx
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('interrupted', [False, True])
def test_generated_image_goal_survives_research_and_interruption_keeps_its_cause(
        tmp_path, monkeypatch, source_summary_model_reply, surface, interrupted):
    from fastapi.testclient import TestClient
    from PIL import Image, ImageDraw, PngImagePlugin
    from playwright.sync_api import sync_playwright
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.model_clients import ModelClientTimeoutError
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry, register_media_plugin
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.end_headers()
            self.wfile.write(b'<html><main><h1>Current value</h1><p>Confirmed value: 17</p></main></html>')
        def log_message(self, *args): pass
    site = ThreadingHTTPServer(('127.0.0.1',0),Site)
    threading.Thread(target=site.serve_forever,daemon=True).start()
    url = f'http://localhost:{site.server_port}/'
    port = _free_port()
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','cdp')
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL',f'http://127.0.0.1:{port}')
    monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON',sys.executable)
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false')
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED','true')
    output = Path(os.environ['NULLION_WORKSPACE_STORAGE_ROOT'])/'workspace_admin'/'artifacts'/'current-value.png'
    generated = []
    def image_provider(prompt, path, size):
        assert '17' in prompt
        image = Image.new('RGB',(600,300),'#102b42'); ImageDraw.Draw(image).text((30,30),'Confirmed value: 17',fill='white')
        info = PngImagePlugin.PngInfo(); info.add_text('confirmed_value','17')
        output.parent.mkdir(parents=True,exist_ok=True); image.save(path,pnginfo=info)
        generated.append(Path(path).read_bytes())
        return {'path':path,'provider':'fixture-image-provider'}
    class Model:
        phase = 0
        def create(self, **kwargs):
            assert not any(tool['name']=='browser_decision' for tool in kwargs.get('tools',[]))
            protocol = workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            summary = source_summary_model_reply(kwargs)
            if summary is not None:return summary
            if not kwargs.get('tools'):return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_run_task'],
                    'required_tool_names':['browser_run_task'],'artifact_extensions':['.png'],
                    'source_artifact_delivery_requested':False})
            names = {row['name'] for row in kwargs['tools']}
            assert 'image_generate' in names and kwargs['timeout'] == 120
            if self.phase == 2:
                return call('browser_run_task',{'task':'現在の値を確認','url':url})
            assert 'Confirmed value: 17' in json.dumps(kwargs['messages'])
            if interrupted:raise ModelClientTimeoutError('External image-plan response timed out')
            if self.phase == 3:
                return call('image_generate',{'prompt':'Infographic of confirmed value 17','output_path':str(output)})
            return text('確認結果: 17\n\nMEDIA:'+str(output))
    model = Model(); registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    register_media_plugin(registry,image_generator=image_provider)
    runtime.active_tool_registry = registry
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch_persistent_context(str(tmp_path/'profile'),headless=True,
                args=[f'--remote-debugging-port={port}'])
            try:
                if surface == 'web':
                    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
                        response = client.post('/api/chat',json={'text':'現在の値を図にしてください','conversation_id':'web:generated'})
                        assert response.status_code == 200,response.text
                        payload = response.json(); answer = payload['text']
                        attachments = payload.get('artifacts',[])
                        if not interrupted:
                            assert len(attachments)==1 and client.get(attachments[0]['url']).content==generated[0],payload
                else:
                    from telegram import Bot, Chat, Message, User, Update
                    from nullion.telegram_app import ChatOperatorService
                    from nullion.telegram_transport import build_telegram_bot
                    from nullion.config import load_settings
                    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
                    wire = TelegramWire(); original = wire.handle
                    async def handle(request):
                        if request.url.path.endswith('/sendChatAction'):
                            return httpx.Response(200,json={'ok':True,'result':True})
                        if request.url.path.endswith('/sendPhoto'):
                            body = await request.aread();wire.attempts.append(body);wire.accepted.append(body)
                            return httpx.Response(200,json={'ok':True,'result':{'message_id':900,'date':1,
                                'chat':{'id':77,'type':'private'},'photo':[{'file_id':'fixture','file_unique_id':'fixture',
                                'width':600,'height':300}]}})
                        return await original(request)
                    wire.handle=handle; install_wire(monkeypatch,wire)
                    settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
                        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
                    service=ChatOperatorService(runtime=runtime,bot_token=TOKEN,operator_chat_id='77',settings=settings,
                        model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
                    async def replay():
                        async with build_telegram_bot(Bot,TOKEN) as bot:
                            message=Message(message_id=1,date=datetime.now(UTC),chat=Chat(77,'private'),
                                from_user=User(77,'Fixture',False),text='現在の値を図にしてください')
                            message.set_bot(bot)
                            await service.on_message(Update(update_id=1,message=message),SimpleNamespace(bot=bot))
                    from concurrent.futures import ThreadPoolExecutor
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        executor.submit(lambda:asyncio.run(replay())).result(timeout=20)
                    from email.parser import BytesParser
                    from email.policy import default
                    captions = []
                    for body in wire.accepted:
                        boundary = body.split(b'\r\n',1)[0][2:]
                        mime = BytesParser(policy=default).parsebytes(
                            b'Content-Type: multipart/form-data; boundary='+boundary+b'\r\nMIME-Version: 1.0\r\n\r\n'+body)
                        captions.extend(part.get_payload(decode=True).decode() for part in mime.iter_parts()
                            if part.get_param('name',header='content-disposition') == 'caption')
                    answer='\n'.join([*wire.texts,*captions]);attachments=wire.accepted
                    if not interrupted:
                        assert len(attachments)==1 and generated[0] in attachments[0]
                turns=[event for event in runtime.store.list_conversation_events() if event.get('event_type')=='conversation.chat_turn']
                assert len(turns)==1 and "couldn't attach" not in answer
                if interrupted:
                    assert 'timed out' in answer.lower() and not attachments and not generated,answer
                else:
                    assert '17' in answer and output.is_file() and Image.open(output).info['confirmed_value']=='17'
                source=next(row for row in turns[0]['tool_results'] if row['tool_name']=='browser_run_task')
                assert source['output']['strategy']['adaptive_browser_started'] is False
                assert not source['output'].get('artifact_paths')
            finally:
                registry.run_cleanup_hooks();browser.close()
    finally:
        site.shutdown();site.server_close()
