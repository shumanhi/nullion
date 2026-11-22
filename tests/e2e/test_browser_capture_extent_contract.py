"""Real API, scoped native browser/Code Mode, PNG bytes and advertised delivery."""
import io
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('execution,extent', [('native','auto'),('native','full_page'),('code_mode','auto'),('code_mode','full_page')])
def test_web_capture_delivers_selected_extent_despite_full_page_tool_argument(tmp_path, monkeypatch, record_property, execution, extent):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.send_header('Content-Type','text/html'); self.end_headers()
            self.wfile.write(b'<title>Extent fixture</title><body style="margin:0;background:rgb(17,34,51)"><h1>CAPTUREFIXTURE</h1><main style="height:9000px"></main><footer style="height:200px;background:rgb(64,128,192)">BOTTOM</footer></body>')
        def log_message(self,*args): pass
    server = ThreadingHTTPServer(('127.0.0.1',0),Source)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    url = f'http://127.0.0.1:{server.server_port}/'
    quote = 'ページ全体を画像でください。' if extent == 'full_page' else '現在の画面を画像でください。'
    prompt = quote+' '+url
    names = ['browser_navigate','browser_wait_for','browser_screenshot']
    class Model:
        phase = 0
        def create(self,**kwargs):
            protocol = workflow_provider_reply(kwargs,completion=True)
            if protocol is not None: return protocol
            if any(t['name']=='select_attachment_format' for t in kwargs.get('tools',[])):
                return call('select_attachment_format',{'extension':'.png','confidence':1})
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope',{'capabilities':['web'],'tool_names':names,'required_tool_names':names,
                    'browser_capture_extent':extent,'browser_capture_request_evidence':quote if extent=='full_page' else '',
                    'source_artifact_delivery_requested':True,'source_artifact_request_evidence':quote})
            if execution == 'code_mode' and self.phase == 2:
                return call('code_mode',{'tool_names':names,'code':
                    "nav=call_tool('browser_navigate',"+repr({'url':url})+")\nsid=nav['output']['session_id']\n"
                    "call_tool('browser_wait_for',{'session_id':sid,'text':'CAPTUREFIXTURE'})\n"
                    "call_tool('browser_screenshot',{'session_id':sid,'mode':'full_page'})['output']"})
            if execution == 'native':
                if self.phase == 2: return call('browser_navigate',{'url':url})
                if self.phase == 3: return call('browser_wait_for',{'text':'CAPTUREFIXTURE'})
                if self.phase == 4: return call('browser_screenshot',{'mode':'full_page'})
            return text('画像を添付しました。')
    model = Model(); registry = ToolRegistry()
    policy = BrowserPolicy(); policy.block_private = False
    register_browser_tools(registry,policy=policy,model_client_getter=lambda:model)
    started = time.monotonic()
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            response = client.post('/api/chat',json={'text':prompt,'conversation_id':'web:extent'})
            assert response.status_code == 200 and response.json()['artifacts'], response.text
            artifacts = response.json()['artifacts']; assert len(artifacts) == 1
            downloaded = client.get(artifacts[0]['url']); assert downloaded.status_code == 200
            events = runtime.store.list_conversation_events('web:extent')
            results = [r for e in events if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
            screenshot = next(r for r in results if r['tool_name']=='browser_screenshot')
            output = screenshot['output']; assert screenshot['status'] == 'completed',screenshot
            assert output['requested_mode'] == 'full_page'
            assert output['capture_extent_contract'] == extent
            assert output['mode'] == ('full_page' if extent=='full_page' else 'viewport')
            assert not any(r['tool_name']=='terminal_exec' for r in results)
            scope = next(r['output'] for r in results if r['tool_name']=='request_tool_scope')
            assert 'browser_screenshot' not in scope['unavailable_required_tool_names']
            with Image.open(io.BytesIO(downloaded.content)) as picture:
                assert picture.width == output['viewport_width']
                if extent == 'full_page':
                    assert picture.height == output['document_height'] and picture.height > 9000
                    assert picture.convert('RGB').getpixel((10,picture.height-10)) == (64,128,192)
                else:
                    assert picture.height == output['viewport_height'] and picture.height < 2000
                    assert output['capture_mode_adjustment_reason'] == 'structured_capture_extent'
            record_property('capture_elapsed_seconds',time.monotonic()-started)
    finally:
        registry.run_cleanup_hooks(); server.shutdown(); server.server_close()


def test_local_rendered_document_retains_auto_full_document_capture(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on'); monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    prompt='ローカル文書を描画して .png でください。'
    names=['browser_open','browser_extract_text','browser_run_js','browser_screenshot']
    markup='<body style="margin:0;background:rgb(17,34,51);height:4500px"><h1>LOCAL DOCUMENT</h1></body>'
    script='document.open();document.write('+json.dumps(markup)+');document.close();true'
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if any(t['name']=='select_attachment_format' for t in kwargs.get('tools',[])):
                return call('select_attachment_format',{'extension':'.png','confidence':1})
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':names,
                'required_tool_names':names,'artifact_extensions':['.png'],
                'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt})
            if self.phase==2:return call('code_mode',{'tool_names':names,'code':
                "opened=call_tool('browser_open',{'mode':'headless'})\nsid=opened['output']['session_id']\n"
                "call_tool('browser_extract_text',{'session_id':sid})\n"
                f"call_tool('browser_run_js',{{'session_id':sid,'script':{script!r}}})\n"
                "call_tool('browser_screenshot',{'session_id':sid,'mode':'auto'})['output']"})
            return text('画像を添付しました。')
    model=Model();registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:local-extent'})
            assert response.status_code==200 and response.json()['artifacts'],response.text
            downloaded=client.get(response.json()['artifacts'][0]['url'])
            results=[r for e in runtime.store.list_conversation_events('web:local-extent')
                if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
            output=next(r['output'] for r in results if r['tool_name']=='browser_screenshot')
            assert output['capture_extent_contract']=='auto' and output['mode']=='full_page'
            assert output['has_document_content'] is True
            with Image.open(io.BytesIO(downloaded.content)) as image:
                assert image.height==output['document_height']==4500
    finally:registry.run_cleanup_hooks()


@pytest.mark.parametrize("valid_pdf,allow_download", [(True, True), (False, True), (True, False)])
def test_blank_pdf_viewport_does_not_deliver_blank_map(tmp_path, monkeypatch, valid_pdf, allow_download):
    """Run real native tool registration, artifact delivery, and the Web API."""
    from fastapi.testclient import TestClient
    from PIL import Image, ImageDraw
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserScreenshotResult
    from nullion.tools import ToolRegistry
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, GLOBAL_PERMISSION_PRINCIPAL, PolicyMode
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    from datetime import UTC, datetime

    pytest.importorskip("pypdfium2")
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("NULLION_CODE_MODE", "off")
    if allow_download:
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id="fixture-map-public-source", principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector="example.com",
            created_by="fixture-user", created_at=datetime.now(UTC),
        ))
    fetches = []
    url = "https://example.com/park-map.pdf"
    prompt = "90分の散歩ルートを提案して、地図のPNGと一緒に送ってください。 " + url
    route = "90分の散歩ルート：受付から庭園を通り、展望台を往復してください。"
    pdf = io.BytesIO()
    with Image.new("RGB", (300, 200), "white") as page:
        ImageDraw.Draw(page).line(((30, 30), (140, 130), (270, 70)), fill="#256328", width=8)
        page.save(pdf, format="PDF")
    blank = io.BytesIO()
    with Image.new("RGB", (1200, 800), "#262626") as view:
        chrome = ImageDraw.Draw(view)
        chrome.rectangle((0, 0, 1200, 56), fill="#383838")
        chrome.line((300, 56, 300, 800), fill="white")
        chrome.rectangle((408, 111, 1142, 767), fill="white")
        chrome.arc((719, 405, 811, 497), 0, 290, fill="#3069be", width=5)
        view.save(blank, format="PNG")

    from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
    async def navigate(*args, **kwargs):
        return url

    async def screenshot(*args, **kwargs):
        return BrowserScreenshotResult(blank.getvalue(), "viewport", "auto", url,
                                       viewport_width=1200, viewport_height=800,
                                       document_width=1200, document_height=800, has_document_content=True)

    async def run_js(*args, **kwargs):
        return url

    def fetch(requested, timeout, *, allow_redirects):
        assert requested == url and allow_redirects is False and timeout <= 5
        fetches.append(requested)
        return {"status_code": 200, "_body_bytes": pdf.getvalue() if valid_pdf else b"not a PDF"}

    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "playwright")
    monkeypatch.setattr(PlaywrightBackend, "navigate", navigate)
    monkeypatch.setattr(PlaywrightBackend, "screenshot", screenshot)
    async def extract_text(*args, **kwargs):
        return "受付は公園の入口です。庭園を抜ける道と、展望台へ向かう歩道がつながっています。"

    monkeypatch.setattr(PlaywrightBackend, "run_js", run_js)
    monkeypatch.setattr(PlaywrightBackend, "extract_text", extract_text)
    monkeypatch.setattr("nullion.tools._fetch_web_url_once", fetch)
    names = ["browser_navigate", "browser_extract_text", "browser_screenshot"]

    class Model:
        phase = 0

        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=False)
            if protocol is not None:
                return protocol
            tools = {t["name"] for t in kwargs.get("tools", [])}
            if "select_attachment_format" in tools:
                return call("select_attachment_format", {"extension": ".png", "confidence": 1})
            if not tools:
                return text("{}")
            self.phase += 1
            if self.phase == 1:
                return call("request_tool_scope", {"capabilities": ["web"], "tool_names": names,
                            "required_tool_names": names, "artifact_extensions": [".png"],
                            "source_artifact_delivery_requested": True, "source_artifact_request_evidence": "地図のPNG"})
            if self.phase == 2:
                return call("browser_navigate", {"url": url})
            if self.phase == 3:
                return call("browser_extract_text", {})
            if self.phase == 4:
                return call("browser_screenshot", {})
            if self.phase == 5:
                assert not fetches and "blank_browser_capture" in json.dumps(kwargs["messages"])
                retry = call("browser_screenshot", {"pdf_url": url})
                retry["content"][0]["id"] = "pdf-retry-id"
                return retry
            if valid_pdf and allow_download:
                from nullion.artifacts import artifact_root_for_principal
                artifact, = artifact_root_for_principal("operator").glob("screenshot-*.png")
                return text(route + "\n\nMEDIA:" + str(artifact))
            return text("地図を確認できませんでした。")

    model = Model()
    registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    cid = "web:public-pdf-map"
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        response = client.post("/api/chat", json={"text": prompt, "conversation_id": cid})
        assert response.status_code == 200, response.text
        payload = response.json()
        turns = runtime.store.list_recent_conversation_events(cid, event_type="conversation.chat_turn")
        first, result = [r for r in turns[-1]["tool_results"] if r["tool_name"] == "browser_screenshot"]
        assert first["status"] == "failed" and first["output"]["reason"] == "blank_browser_capture"
        assert first["output"]["recovery"]["retry_arguments"]["pdf_url"] == url
        if not allow_download:
            assert result["status"] == "denied" and not fetches, result
            assert not payload.get("artifacts"), payload
        elif valid_pdf:
            assert fetches == [url]
            assert result["status"] == "completed" and result["output"]["capture_method"] == "pdf_page_render", result
            assert route in payload["text"], payload
            assert len(payload["artifacts"]) == 1, payload
            downloaded = client.get(payload["artifacts"][0]["url"])
            assert downloaded.status_code == 200
            with Image.open(io.BytesIO(downloaded.content)) as map_image:
                assert map_image.size == (1200, 800)
                assert len(map_image.getcolors(maxcolors=1) or []) != 1
        else:
            assert result["status"] == "failed" and result["output"]["reason"] == "blank_browser_capture", result
            assert not payload.get("artifacts"), payload
