"""Actual PNG rendering and delivery through HTTP/Telegram and Code Mode."""
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


def _chromium(monkeypatch):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        monkeypatch.setenv("NULLION_CHROMIUM_EXECUTABLE", p.chromium.executable_path)


@pytest.mark.parametrize("surface", ["web", "telegram"])
@pytest.mark.parametrize('provider_failure,action_pending', [(False,False),(True,False),(True,True)])
def test_native_image_code_mode_delivers_original_png_without_browser_navigation(tmp_path, monkeypatch, record_property, surface, provider_failure, action_pending):
    import asyncio
    from io import BytesIO
    from types import SimpleNamespace
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.image_render import build_handler, input_schema
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    from nullion import chat_operator

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("NULLION_CODE_MODE", "on")
    monkeypatch.setenv("NULLION_CHAT_STARTUP_WARMUP", "0")
    monkeypatch.setenv('NULLION_BUILDER_ENABLED', '0')
    _chromium(monkeypatch)
    def unexpected(*args, **kwargs):
        pytest.fail("Artifact turns must not load scheduler metadata or a setup classifier")
    monkeypatch.setattr(chat_operator, "_user_turn_mentions_current_cron_reference", unexpected)
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec("image_render", "Render a designed PNG", ToolRiskLevel.MEDIUM,
        ToolSideEffectClass.WRITE, False, 30, input_schema=input_schema(), code_mode_allowed=True,
        capability_tags=("local_files",), produced_artifact_extensions=(".png",)), build_handler(tmp_path, [tmp_path]))
    if provider_failure:
        registry.register(ToolSpec('image_generate', 'External image producer', ToolRiskLevel.MEDIUM,
            ToolSideEffectClass.WRITE, False, 5, capability_tags=('image_generation',),
            input_schema={'type':'object'}, code_mode_allowed=True, produced_artifact_extensions=('.png',)),
            lambda inv:ToolResult(inv.invocation_id, inv.tool_name, 'failed', {
                'failure_scope':'tool','terminal_user_action_required':True,'retryable':False,
                'http_status_code':402,'reason':'generation_failed'}, 'External provider credits depleted'))
    if action_pending:
        registry.register(ToolSpec('a9','Required record action',ToolRiskLevel.MEDIUM,
            ToolSideEffectClass.WRITE,False,5,input_schema={'type':'object'},
            capability_tags=('local_files','explicit_scope_only','required_scope_only')),
            lambda inv:pytest.fail('Completion reconciliation cannot execute a pending action'))
    class Model:
        phase = 0
        path = None
        trace = []
        errors = []
        def create(self, **kw):
            try:
                return self._create(**kw)
            except Exception as error:
                self.errors.append((self.phase, str(error)))
                raise
        def _create(self, **kw):
            names = {t["name"] for t in kw.get("tools", [])}
            self.trace.append({'phase':self.phase,'tools':sorted(names), 'last':str(kw.get('messages',[])[-1:])[-1800:]})
            assert "select_attachment_format" not in names
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None:
                return protocol
            if not names:
                return text("{}")
            self.phase += 1
            def program(arguments):
                response=call('code_mode',arguments)
                response['content'][0]['id']='image-program-'+str(self.phase)
                return response
            if self.phase == 1:
                return call("request_tool_scope", {"capabilities": ["local_files", *(['image_generation'] if provider_failure else [])],
                    "tool_names": ["image_render", *(['image_generate'] if provider_failure else []), *(['a9'] if action_pending else [])],
                    "required_tool_names": ["image_generate" if provider_failure else "image_render", *(['a9'] if action_pending else [])],
                    "artifact_extensions": [".png"]})
            if provider_failure and self.phase == 2:
                return program({'tool_names':['image_generate'],'code':"call_tool('image_generate', {})"})
            if self.phase == 2 + int(provider_failure):
                page = {"title": "旅 / Weekend", "subtitle": "21 November 2026", "cards": [
                    {"heading": "固定の予定", "text": "10:53 AM – 12:58 PM\nOriginal schedule unchanged."},
                    {"heading": "Suggested restaurant", "text": "Restaurant 37 · after the scheduled event"}]}
                return program({"tool_names": ["image_render"], "code":
                    "call_tool('image_render', " + json.dumps({"card_pages": [page], "output_path": str(self.path)}) + ")"})
            assert self.phase == 3 + int(provider_failure), "No repeated screenshot/inspection/authoring rounds"
            assert any(b.get("type") == "image" for m in kw["messages"] for b in m.get("content", []) if isinstance(b, dict))
            return text(self.caption + "\n\nMEDIA:" + str(self.path))
    model = Model()
    orchestrator = AgentOrchestrator(model_client=model)
    samples = []
    with TestClient(create_app(runtime, orchestrator, registry)) as client:
        if surface == "telegram":
            from nullion.config import load_settings
            from nullion.telegram_app import ChatOperatorService
            settings = load_settings(env={"NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
                "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "77", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
            service = ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
                operator_chat_id="77", model_client=model, agent_orchestrator=orchestrator)
        for index, prompt in enumerate(["Create an infographic image with these facts.", "この予定を画像にして。", "Haz una infografía."]):
            cid = "web:designed-image-" + str(index) if surface == "web" else "telegram:77"
            root = artifact_root_for_principal(cid)
            root.mkdir(parents=True, exist_ok=True)
            model.phase = 0
            model.path = root / ("designed-" + str(index) + ".png")
            model.caption = [
                'Schedule 10:53 AM to 12:58 PM; restaurant 37 follows the event.',
                '予定は10:53 AMから12:58 PMまで。その後の候補はレストラン37です。',
                'Horario de 10:53 AM a 12:58 PM; restaurante 37 después del evento.',
            ][index]
            started = time.monotonic()
            if surface == "web":
                response = client.post("/api/chat", json={"text": prompt, "conversation_id": cid})
                assert response.status_code == 200, response.text
                artifacts = response.json()["artifacts"]
                assert len(artifacts) == 1, (response.json(),model.errors,model.trace[-3:])
                if not action_pending:
                    assert model.caption in response.json()['text'], response.json()
                response = client.get(artifacts[0]["url"])
                assert response.status_code == 200
                delivered = response.content
            else:
                uploads = []
                visible = []
                class Message:
                    text = prompt
                    message_id = 8300 + index
                    reply_to_message = None
                    chat = SimpleNamespace(id=77, send_action=lambda **kwargs: asyncio.sleep(0))
                    async def reply_text(self, value, **kwargs):
                        visible.append(value)
                        return SimpleNamespace(message_id=9300)
                    async def reply_document(self, document, caption=None, **kwargs):
                        visible.append(caption or '')
                        uploads.append(document.read())
                        return SimpleNamespace(message_id=9400)
                asyncio.run(service.on_message(SimpleNamespace(update_id=8300+index, message=Message()), SimpleNamespace(bot=None)))
                assert len(uploads) == 1,(model.errors,model.trace[-3:])
                from html import unescape
                if not action_pending:
                    assert model.caption in unescape('\n'.join(visible)), visible
                delivered = uploads[0]
            samples.append(time.monotonic() - started)
            assert delivered == model.path.read_bytes()
            with Image.open(BytesIO(delivered)) as image:
                assert image.width == 1600 and image.height > 700
                assert image.format == "PNG"
            turns = runtime.store.list_recent_conversation_events(cid, event_type="conversation.chat_turn")
            results = turns[-1]["tool_results"]
            assert turns[-1]['response_fulfilled'] is (not action_pending), turns[-1]
            rendered = [r for r in results if r["tool_name"] == "image_render"]
            assert len(rendered) == 1 and rendered[0]["status"] == "completed", results
            assert rendered[0]["output"]["artifact_descriptors"][0]["kind"] == "image"
            assert not any(r["tool_name"].startswith("browser_") for r in results)
            if provider_failure:
                assert len([r for r in results if r['tool_name']=='image_generate']) == 1
                repaired=[r for r in results if r['tool_name']=='request_tool_scope'
                    and r['output'].get('native_artifact_method_reconciliation')]
                if action_pending:
                    latest=[r for r in results if r['tool_name']=='request_tool_scope'][-1]['output']
                    assert 'a9' in latest['required_tool_names']
                    assert not any(r['tool_name']=='a9' for r in results)
                else:
                    assert len(repaired)==1
                if repaired:
                    assert 'image_generate' not in repaired[0]['output']['required_tool_names']
                    assert 'image_render' in repaired[0]['output']['required_tool_names']
    assert sum(samples) / len(samples) < 6 and max(samples) < 9, samples
    record_property(surface + "_mean_seconds", sum(samples) / len(samples))


def test_image_layout_contrast_bounds_and_scale_match_rendered_pixels(tmp_path, monkeypatch):
    from PIL import Image
    from playwright.sync_api import sync_playwright
    from nullion.pdf_design import render_card_pages
    from nullion.image_render import build_handler
    from nullion.tools import ToolInvocation
    _chromium(monkeypatch)
    content = [{"title": "POUGHKEEPSIE FULL MOON WEEKEND", "subtitle": "Saturday · 21 November 2026", "cards": [
        {"heading": "Fixed schedule · 10:53 AM – 12:58 PM", "text": "Original event unchanged.\n" + "Long required fact. " * 30},
        {"heading": "予定 / اقتراح", "text": "Later suggestions stay separate from fixed events."}]}]
    html = render_card_pages(content, output_kind="image")[0]
    def luminance(color):
        channels = [int(x) / 255 for x in color.removeprefix("rgb(").removesuffix(")").split(",")]
        channels = [c / 12.92 if c <= .04045 else ((c+.055)/1.055)**2.4 for c in channels]
        return sum(c*w for c,w in zip(channels, [.2126, .7152, .0722]))
    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            page = browser.new_page(viewport={"width": 800, "height": 900})
            page.set_content('<style>body{margin:0}</style>' + html)
            main = page.locator("main").bounding_box()
            for selector, background in [("h1", "main"), ("header p", "main"), ("article p", "article")]:
                fg = page.locator(selector).first.evaluate("e=>getComputedStyle(e).color")
                bg = page.locator(background).first.evaluate("e=>getComputedStyle(e).backgroundColor")
                a, b = sorted([luminance(fg), luminance(bg)])
                assert (b+.05)/(a+.05) >= 4.5, (selector, fg, bg)
            for locator in [page.locator("h1"), page.locator("article").first, page.locator("article").nth(1)]:
                box = locator.bounding_box()
                assert box["x"] >= 0 and box["x"]+box["width"] <= 800
                assert box["y"]+box["height"] <= main["height"]
            assert page.locator("article p").first.evaluate("e=>getComputedStyle(e).fontSize") == "26px"
            assert page.locator("article").nth(1).bounding_box()["y"] > page.locator("article").first.bounding_box()["y"]
        finally:
            browser.close()
    handler = build_handler(tmp_path, [tmp_path])
    sizes = []
    for ratio in [1, 2]:
        result = handler(ToolInvocation("scale-"+str(ratio), "image_render", "web:scale", {
            "output_path": str(tmp_path / (str(ratio)+".png")), "card_pages": content, "pixel_ratio": ratio}))
        assert result.status == "completed", result
        with Image.open(result.output["path"]) as image:
            sizes.append(image.size)
    assert sizes[1] == (sizes[0][0]*2, sizes[0][1]*2)


def test_oversized_image_rejects_without_partial_artifact(tmp_path, monkeypatch):
    from nullion.image_render import build_handler
    from nullion.tools import ToolInvocation
    _chromium(monkeypatch)
    path = tmp_path / "too-tall.png"
    result = build_handler(tmp_path, [tmp_path])(ToolInvocation("large", "image_render", "web:image", {
        "output_path": str(path), "card_pages": [{"title": "旅", "cards": [
            {"heading": str(i), "text": "Required fact.\n"*300} for i in range(12)]}]}))
    assert result.status == "failed" and result.output["recovery"]["kind"] == "correct_arguments"
    assert not path.exists()


def test_chat_selects_one_image_using_candidate_pixels(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.image_render import build_handler, input_schema
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path, monkeypatch);_chromium(monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    cid='web:candidate-images';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    paths=[root/'draft.png',root/'final.png'];selections=[]
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('image_render','Render designed image',ToolRiskLevel.MEDIUM,ToolSideEffectClass.WRITE,
        False,30,input_schema=input_schema(),code_mode_allowed=True,capability_tags=('local_files',),
        produced_artifact_extensions=('.png',)),build_handler(tmp_path,[tmp_path]))
    class Model:
        phase=0
        def create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            if 'select_artifact_delivery' in names:
                content=kw['messages'][0]['content']
                assert len([b for b in content if b.get('type')=='image'])==2
                assert all(any(f'candidate a{i}:' in b.get('text','') for b in content) for i in [0,1])
                selections.append(True)
                return call('select_artifact_delivery',{'status':'selected','selected_ids':['a1'],
                    'reason':'The displayed final version preserves the selected fact.'})
            reply=workflow_provider_reply(kw,completion=True)
            if reply is not None:return reply
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['image_render'],'required_tool_names':['image_render'],'artifact_extensions':['.png']})
            if self.phase==2:
                code=[]
                for i,path in enumerate(paths):
                    args={'output_path':str(path),'card_pages':[{'title':'Version '+str(i),
                        'cards':[{'heading':'Selected fact','text':'37' if i else 'Draft pending'}]}]}
                    code.append("call_tool('image_render', "+repr(args)+")")
                return call('code_mode',{'tool_names':['image_render'],'code':'\n'.join(code)})
            return text('Prepared the image versions.')
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=Model()),registry)) as client:
        result=client.post('/api/chat',json={'text':'画像を作成し、完成版を一つだけ添付してください。','conversation_id':cid})
        assert result.status_code==200 and len(result.json()['artifacts'])==1,result.text
        assert selections
        downloaded=client.get(result.json()['artifacts'][0]['url'])
        assert downloaded.content==paths[1].read_bytes()
