"""Real HTTP/SQLite/Code Mode/PDF boundary; external reads and model are scripted."""
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


@pytest.mark.parametrize('surface', ['web', 'telegram'])
def test_research_pdf_avoids_setup_classifiers_reinspection_and_duplicate_files(tmp_path, monkeypatch, record_property, surface):
    import asyncio
    from types import SimpleNamespace
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from pypdf import PdfReader
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult,
                               _build_pdf_create_handler, _build_file_read_handler)
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    from nullion.web_app import create_app
    from nullion import chat_operator

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("NULLION_CODE_MODE", "on")
    monkeypatch.setenv("NULLION_CHAT_STARTUP_WARMUP", "0")
    with sync_playwright() as p:
        monkeypatch.setenv("NULLION_CHROMIUM_EXECUTABLE", p.chromium.executable_path)
    def no_cron_scan(*args, **kwargs):
        pytest.fail("Ordinary artifact requests must not scan scheduler jobs during preparation")
    monkeypatch.setattr(chat_operator, "_user_turn_mentions_current_cron_reference", no_cron_scan)
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path]); external = []
    def calendar(inv):
        external.append(inv.tool_name)
        return ToolResult(inv.invocation_id, inv.tool_name, "completed", {"results": [
            {"summary": "固定の予定", "start": "2026-11-21T10:53:00-05:00", "end": "2026-11-21T12:58:00-05:00"}]})
    def sources(inv):
        external.append(inv.tool_name)
        return ToolResult(inv.invocation_id, inv.tool_name, "completed", {"findings": [
            {"label": "Restaurant 37", "source_url": "https://source.test/restaurant", "quote": "Brunch 10:30 AM - 2:00 PM"},
            {"label": "Activity 73", "source_url": "https://source.test/park", "quote": "Trails open 10:00 AM - 5:00 PM"}],
            "irrelevant": "large optional data " * 5000})
    for name, tags, handler in [("calendar_list", ("calendar",), calendar), ("read_sources", ("web",), sources)]:
        registry.register(ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5,
            input_schema={"type": "object"}, capability_tags=tags), handler)
    registry.register(ToolSpec("pdf_create", "Create PDF", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 30,
                              code_mode_allowed=True),
                      _build_pdf_create_handler(tmp_path, allowed_roots=[tmp_path]))
    registry.register(ToolSpec("file_read", "Inspect local file", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 10,
                              code_mode_allowed=True),
                      _build_file_read_handler(tmp_path, allowed_roots=[tmp_path]))
    class Model:
        phase = 0
        path = None
        errors = []
        def create(self, **kw):
            try:
                response = self.respond(**kw)
                for block in response.get("content", []):
                    if block.get("type") == "tool_use" and block.get("name") == "code_mode":
                        block["id"] = "program-" + str(self.phase)
                return response
            except Exception as exc:
                self.errors.append(repr(exc))
                raise
        def respond(self, **kw):
            names = {t["name"] for t in kw.get("tools", [])}
            assert "select_attachment_format" not in names, "Let the primary select output requirements"
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            if not names: return text("{}")
            self.phase += 1
            if self.phase == 1:
                return call("request_tool_scope", {"capabilities": ["web", "calendar", "local_files"],
                    "tool_names": ["calendar_list", "read_sources", "pdf_create", "file_read"],
                    "required_tool_names": ["calendar_list", "read_sources", "pdf_create", "file_read"],
                    "artifact_extensions": [".pdf"]})
            if self.phase == 2:
                return call("code_mode", {"tool_names": ["calendar_list", "read_sources"], "code":
                    "schedule=call_tool('calendar_list', {'start':'2026-11-21T00:00:00-05:00','end':'2026-11-23T00:00:00-05:00'})['output']['results']\n"
                    "sources=call_tool('read_sources', {})['output']['findings']\n"
                    "{'schedule':schedule,'findings':sources,'unresolved_questions':[]}"})
            if self.phase == 3:
                wire = json.dumps(kw["messages"], ensure_ascii=False)
                assert "Restaurant 37" in wire and "Activity 73" in wire
                assert "2026-11-21T10:53:00-05:00" in wire
                page = {"title": "Weekend plan", "cards": [{"heading": "Fixed schedule", "text": "10:53 AM - 12:58 PM"},
                        {"heading": "Suggested restaurant", "text": "Restaurant 37"},
                        {"heading": "Suggested activity", "text": "Activity 73"}]}
                args = json.dumps({"output_path": str(self.path), "card_pages": [page], "expected_page_count": 1})
                return call("code_mode", {"tool_names": ["pdf_create", "file_read"], "code":
                    "pdf=call_tool('pdf_create', " + args + ")\n"
                    "check=call_tool('file_read', {'path':pdf['output']['path']})\n"
                    "{'path':pdf['output']['path'],'text':check['output']['content']}"})
            assert self.phase == 4, "No extra inspection or artifact-repair reasoning rounds"
            assert any(b.get("type") == "image" for m in kw["messages"] for b in m.get("content", []) if isinstance(b, dict)), "Composer must receive actual PDF pixels"
            return text("Your PDF preserves the fixed schedule and includes a restaurant and an activity.\n\nMEDIA:" + str(self.path))
    model = Model(); samples = []; orchestrator = AgentOrchestrator(model_client=model)
    with TestClient(create_app(runtime, orchestrator, registry)) as client:
        if surface == 'telegram':
            from nullion.config import load_settings
            from nullion.telegram_app import ChatOperatorService
            settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN': '123456:abcdefghijklmnopqrstuvwxyz',
                'NULLION_TELEGRAM_OPERATOR_CHAT_ID': '77', 'NULLION_TELEGRAM_CHAT_ENABLED': 'true'})
            service = ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
                operator_chat_id='77', model_client=model, agent_orchestrator=orchestrator)
        for index, prompt in enumerate(["Use my schedule and recommend food and activities in a PDF.",
                                        "予定を守り、食事と観光を提案するPDFを作って。", "Crea un PDF con mis planes, restaurantes y actividades."]):
            cid = "web:efficient-pdf-" + str(index) if surface == 'web' else 'telegram:77'
            model.phase = 0; root = artifact_root_for_principal(cid); root.mkdir(parents=True, exist_ok=True)
            model.path = root / ("weekend-" + str(index) + ".pdf")
            started = time.monotonic()
            if surface == 'web':
                response = client.post("/api/chat", json={"text": prompt, "conversation_id": cid})
            else:
                delivered = []; uploads = []
                class Message:
                    text = prompt; message_id = 8000 + index; reply_to_message = None
                    chat = SimpleNamespace(id=77, send_action=lambda **kwargs: asyncio.sleep(0))
                    async def reply_text(self, value, **kwargs):
                        delivered.append(value); return SimpleNamespace(message_id=9000+len(delivered))
                    async def reply_document(self, document, caption=None, **kwargs):
                        uploads.append(document.read()); delivered.append(caption or '')
                        return SimpleNamespace(message_id=9500+len(uploads))
                asyncio.run(service.on_message(SimpleNamespace(update_id=8000+index, message=Message()), SimpleNamespace(bot=None)))
                assert uploads == [model.path.read_bytes()], delivered
                assert delivered and any(c.isalnum() for c in delivered[-1]), delivered
                response = SimpleNamespace(status_code=200, text=delivered[-1])
            samples.append(time.monotonic() - started)
            if model.errors:
                print("MODEL ERRORS", model.errors)
                print("TOOLS", [(t.get('tool_name'),t.get('status'),t.get('output',{}).get('reason'),
                    t.get('output',{}).get('validation_errors')) for e in runtime.store.list_recent_conversation_events(
                    cid,event_type='conversation.chat_turn') for t in e.get('tool_results',[])])
            assert response.status_code == 200 and model.phase == 4, (response.text, model.errors,
                [(t.get('tool_name'),t.get('status'), t.get('error'), t.get('output',{}).get('execution_error')) for e in
                 runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn') for t in e.get('tool_results',[])])
            turns = runtime.store.list_recent_conversation_events(cid, event_type="conversation.chat_turn")
            tools = turns[-1]["tool_results"]
            assert len([t for t in tools if t["tool_name"] == "pdf_create"]) == 1
            assert not any(t["tool_name"] == "inspect_retained_evidence" for t in tools)
            pdf_text = "\n".join(page.extract_text() for page in PdfReader(model.path).pages)
            assert all(value in pdf_text for value in ["10:53 AM", "Restaurant 37", "Activity 73"])
            assert not list(root.glob("requested_artifact_*.pdf"))
            delivery = prepare_reply_for_platform_delivery("📅\n\nMEDIA:" + str(model.path), principal_id=cid)
            assert delivery.attachments == (model.path,) and any(c.isalnum() for c in delivery.text)
            assert ".nullion-pdf-previews" not in delivery.text
    assert len(external) == 6
    assert sum(samples) / len(samples) < 6 and max(samples) < 9, samples
    record_property(surface + "_research_pdf_mean_seconds", sum(samples) / len(samples))


def test_designed_pdf_preserves_image_edges_and_exposes_actual_rendered_pixels(tmp_path, monkeypatch):
    from PIL import Image, ImageDraw
    from playwright.sync_api import sync_playwright
    from nullion.tools import ToolInvocation, _build_pdf_create_handler
    from nullion.agent_orchestrator import _tool_result_visual_reference_blocks
    with sync_playwright() as p:
        monkeypatch.setenv("NULLION_CHROMIUM_EXECUTABLE", p.chromium.executable_path)
    image = Image.new("RGB", (600, 90), "white")
    draw = ImageDraw.Draw(image); draw.rectangle((0, 0, 100, 90), fill="red"); draw.rectangle((500, 0, 599, 89), fill="blue")
    source = tmp_path / "wide-label.png"; image.save(source)
    output = tmp_path / "designed.pdf"
    result = _build_pdf_create_handler(tmp_path, allowed_roots=[tmp_path])(ToolInvocation(
        "visual", "pdf_create", "web:visual-pdf", {"output_path": str(output), "html_pages": [
            '<h1>Readable report</h1><img style="width:120px;height:70px;object-fit:cover" src="' + str(source) + '">'],
            "expected_page_count": 1}))
    assert result.status == "completed", result
    assert result.output["visual_preview_available"] and not result.output["visual_content_verified"]
    preview = result.output["rendered_page_previews"][0]
    with Image.open(preview["path"]) as rendered:
        colors = list(rendered.convert("RGB").getdata())
    assert sum(r > 180 and g < 90 and b < 90 for r, g, b in colors) > 30
    assert sum(b > 180 and g < 90 and r < 90 for r, g, b in colors) > 30
    assert any(b["type"] == "image" for b in _tool_result_visual_reference_blocks(result))


@pytest.mark.parametrize('media_field', ['image_paths', 'screenshot_paths'])
def test_separate_artwork_does_not_squeeze_card_report_columns(tmp_path, monkeypatch, media_field):
    import pypdfium2 as pdfium
    from PIL import Image
    from playwright.sync_api import sync_playwright
    from nullion.tools import ToolInvocation, _build_pdf_create_handler
    with sync_playwright() as p:
        monkeypatch.setenv("NULLION_CHROMIUM_EXECUTABLE", p.chromium.executable_path)
    source = tmp_path / "wide-art.png"; Image.new("RGB", (600, 300), "blue").save(source)
    marker = "abcdefghijklmnopqrstuvwxyzABCD"
    result = _build_pdf_create_handler(tmp_path, allowed_roots=[tmp_path])(ToolInvocation(
        "card-art", "pdf_create", "web:card-art", {"output_path": str(tmp_path / "cards.pdf"),
        "card_pages": [{"title": "Report", "cards": [{"heading": "First", "text": marker},
                                                     {"heading": "Second", "text": "Original facts unchanged."}]}],
        media_field: [str(source)], "expected_page_count": 1}))
    assert result.status == "completed", result
    with pdfium.PdfDocument(result.output["path"]) as document:
        page = document[0]; text_page = page.get_textpage()
        try:
            content = text_page.get_text_range()
            assert marker in content, content
            start = content.index(marker)
            first = text_page.get_charbox(start); last = text_page.get_charbox(start + len(marker) - 1)
            assert abs(first[1] - last[1]) < 2, "Artwork must not force the card body onto narrow wrapped lines"
        finally:
            text_page.close(); page.close()
