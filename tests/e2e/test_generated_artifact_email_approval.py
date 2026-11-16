"""Generate actual file bytes, prepare native email approval, upload to Telegram.

Only the model and remote mail/Telegram transports are replaced. The producer,
scope, approval creation, SQLite, presentation and multipart uploader are real.
"""
import asyncio
import json
import os
from pathlib import Path
import shlex
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


@pytest.mark.parametrize("prompt", [
    "Prepare an email to guest@example.test with a generated invitation .png for 8 PM at my place. Show the complete draft and attachment for confirmation; do not send yet.",
    "guest@example.test 宛のメールと招待状 .png を用意してください。午後8時、自宅です。本文と添付を確認してから送信します。",
])
@pytest.mark.parametrize("surface", ["telegram", "web_http", "web_ui"])
@pytest.mark.parametrize("clarifications", [0, 2])
@pytest.mark.parametrize("execution_layer", ["legacy", "code_mode"])
def test_generated_invitation_and_complete_draft_reach_native_confirmation_without_sending(tmp_path, monkeypatch, prompt, surface, clarifications, execution_layer, artifact_extension='.png'):
    from PIL import Image
    from telegram import Bot, Message
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
    from nullion.telegram_app import _approval_decision_card, _deliver_reply
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_terminal_exec_handler, register_email_plugin
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_generated_artifact_protocol_recovery import upload_boundary
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("NULLION_CODE_MODE", "on" if execution_layer == "code_mode" else "off")
    from nullion import connections
    from nullion.connections import ProviderConnection, ConnectionRegistry, save_connection_registry
    monkeypatch.setattr(connections, "_CONNECTIONS_PATH", tmp_path/"connections.json")
    save_connection_registry(ConnectionRegistry(connections=[ProviderConnection(
        connection_id="fixture-mail", workspace_id="workspace_admin", provider_id="fixture_connector_provider",
        display_name="Fixture mailbox", provider_profile="https://mail.example.test", permission_mode="write")]))
    monkeypatch.setenv("NULLION_TASK_DECOMPOSITION_ENABLED", "0")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec("terminal_exec", "Render a declared local artifact", ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC, False, 20), _build_terminal_exec_handler(tmp_path, allowed_roots=[tmp_path]))
    sent = []
    def send(message, paths):
        sent.append((message, list(paths)))
        return {"message_id":"fixture-sent", "status":"sent"}
    register_email_plugin(registry, email_searcher=lambda *a: [], email_sender=send)
    runtime.active_tool_registry = registry
    source = tmp_path / "legacy-output" / ("invitation"+artifact_extension)
    source.parent.mkdir()
    cid = "telegram:42" if surface == "telegram" else "web:invitation"
    current_prompt = prompt
    if clarifications:
        from nullion.chat_operator import _remember_chat_turn
        _remember_chat_turn(runtime, chat_id=cid, conversation_id=cid, conversation_turn_id="request",
            user_message=prompt, assistant_reply="Choose a style:\n1. Warm\n2. Simple")
        _remember_chat_turn(runtime, chat_id=cid, conversation_id=cid, conversation_turn_id="style",
            user_message="2", assistant_reply="Choose the start time:\n1. 7 PM\n2. 8 PM\n3. A different time")
        current_prompt = "2"
    script = ("from PIL import Image,ImageDraw; "
        "im=Image.new('RGB',(480,640),'navy'); "
        "ImageDraw.Draw(im).text((20,30),'8 PM - My place - Movie and takeout',fill='white'); "
        f"im.save({str(source)!r})")
    drafts = []
    scopes = []
    class Model:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            names = {row["name"] for row in kwargs.get("tools", [])}
            scopes.append(sorted(names))
            if "select_attachment_format" in names:
                return call("select_attachment_format", {"extension":artifact_extension, "confidence":1})
            if not names: return text("{}")
            if sent:
                return text("Email sent with the invitation attachment.")
            self.phase += 1
            assert self.phase <= 6
            if self.phase == 1:
                assert prompt in json.dumps(kwargs["messages"], ensure_ascii=False), "The selected clarification lost the full original request"
            if self.phase == 1:
                return call("request_tool_scope", {"capabilities": ["local_shell", "connector"],
                    "tool_names": ["terminal_exec", "email_send"], "required_tool_names": ["terminal_exec", "email_send"],
                    "artifact_extensions": [artifact_extension], "source_user_requested": True, "source_evidence": prompt})
            if self.phase == 2:
                if execution_layer == "code_mode":
                    draft = {"to": ["guest@example.test"], "subject": "Movie and dinner invitation",
                        "body": "Join me at my place at 8 PM for a movie and takeout dinner."}
                    drafts.append(draft)
                    return call("code_mode", {"tool_names": ["terminal_exec", "email_send"], "code":
                        "rendered = call_tool('terminal_exec', " + repr({"command": "python -c " + shlex.quote(script), "output_paths": [str(source)]}) + ")\n"
                        + "draft = " + repr(draft) + "\n"
                        + "draft['attachment_paths'] = rendered['output']['artifact_paths']\n"
                        + "call_tool('email_send', draft)"})
                return call("terminal_exec", {"command": "python -c " + shlex.quote(script), "output_paths": [str(source)]})
            # Use the actual returned, staged receipt, not a guessed source path.
            from nullion.artifacts import artifact_root_for_principal
            paths = list(Path(os.environ["NULLION_WORKSPACE_STORAGE_ROOT"]).rglob(source.name))
            assert len(paths) == 1
            draft = {"to": ["guest@example.test"], "subject": "Movie and dinner invitation",
                "body": "Join me at my place at 8 PM for a movie and takeout dinner.",
                "attachment_paths": [str(paths[0])]}
            drafts.append(draft)
            return call("email_send", draft)
    model = Model()
    def expected_draft():
        if execution_layer == "code_mode":
            paths = list(Path(os.environ["NULLION_WORKSPACE_STORAGE_ROOT"]).rglob(source.name))
            assert len(paths) == 1
            return {**drafts[-1], "attachment_paths": [str(paths[0])]}
        return drafts[-1]
    settings = NullionSettings(telegram=TelegramSettings(bot_token="123456:synthetic", operator_chat_id="42", chat_enabled=True))
    if surface in {'slack','discord'}:
        from types import SimpleNamespace
        from nullion import telegram_app,slack_app,discord_app
        from nullion.config import load_settings
        from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker
        settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:synthetic',
            'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'42','NULLION_TELEGRAM_CHAT_ENABLED':'true',
            'NULLION_SLACK_OPERATOR_USER_ID':'U1','NULLION_SLACK_ENABLED':'true',
            'NULLION_DISCORD_OPERATOR_USER_ID':'123','NULLION_DISCORD_ENABLED':'true'})
        service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token='123456:synthetic',
            operator_chat_id='42',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
        wire=[];uploads=[]
        class Receipt:
            async def edit(self,**kwargs):wire.append(kwargs.get('content',''))
            async def delete(self):pass
        class Channel:
            id='C1'
            async def send(self,content=None,**kwargs):
                if content:wire.append(content)
                if kwargs.get('file'):uploads.append(kwargs['file'].fp.read())
                return Receipt()
        async def say(value):wire.append(value);return {'ok':True,'ts':'final'}
        async def post(**kwargs):wire.append(kwargs.get('text',''));return {'ok':True,'ts':'receipt'}
        async def remove(**kwargs):return {'ok':True}
        async def upload(**kwargs):
            uploads.append(Path(kwargs['file']).read_bytes())
            if kwargs.get('initial_comment'):wire.append(kwargs['initial_comment'])
            return {'ok':True,'files':[{'id':'file-receipt'}]}
        async def replay():
            if surface=='slack':
                await slack_app.handle_slack_message(service,settings,
                    event={'user':'U1','text':current_prompt,'channel':'C1','event_ts':'1'},say=say,
                    client=SimpleNamespace(chat_postMessage=post,chat_update=post,chat_delete=remove,files_upload_v2=upload))
            else:
                await discord_app.handle_discord_message(service,settings,SimpleNamespace(
                    author=SimpleNamespace(id=123,bot=False),content=current_prompt,id='1',channel=Channel(),attachments=[]))
        monkeypatch.setattr(slack_app,'GLOBAL_TURN_DISPATCH_TRACKER',AsyncTurnDispatchTracker())
        monkeypatch.setattr(discord_app,'GLOBAL_TURN_DISPATCH_TRACKER',AsyncTurnDispatchTracker())
        asyncio.run(replay())
        pending=[a for a in runtime.store.list_approval_requests() if a.context.get('tool_name')=='email_send']
        assert len(pending)==1 and pending[0].status.value=='pending',wire
        assert pending[0].context['tool_arguments']==expected_draft()
        rendered='\n'.join(wire)
        assert 'Approval pending' in rendered and '/approve '+pending[0].approval_id in rendered,rendered
        assert source.read_bytes() in uploads
        assert not sent
        return
    if surface in {'web_http','web_ui'}:
        from nullion.web_app import create_app
        app = create_app(runtime, AgentOrchestrator(model_client=model), registry)
        if surface == "web_http":
            from fastapi.testclient import TestClient
            with TestClient(app) as client:
                response = client.post("/api/chat", json={"text":current_prompt, "conversation_id":cid, "turn_id":"preview"})
                assert response.status_code == 200, response.text
                payload = response.json()
                assert payload["type"] == "approval_required", payload
                assert "8 PM" in payload["tool_detail"] and "guest@example.test" in payload["tool_detail"]
                assert len(payload["artifacts"]) == 1, payload
                assert client.get(payload["artifacts"][0]["url"]).content == source.read_bytes()
                if not clarifications and prompt.startswith("Prepare an email"):
                    csrf=client.get('/api/session').json()['csrf_token']
                    approval_id=runtime.store.list_suspended_turns()[0].approval_id
                    approved=client.post('/api/approve/'+approval_id,json={'mode':'once'},headers={'x-nullion-csrf':csrf})
                    assert approved.status_code==200,approved.text
                    assert 'Email sent' in str(approved.json().get('resume',{}).get('text')),approved.text
        else:
            from playwright.sync_api import expect
            from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
            with _UvicornThread(app, _free_port()) as server:
                def select_conversation(page):
                    if clarifications:
                        page.add_init_script("localStorage.setItem('nullion_conv_id', " + json.dumps(cid) + ");")
                playwright, browser, page, errors = _open_page(server.base_url, configure_page=select_conversation)
                try:
                    page.wait_for_function("() => _chatSaveEnabled === true", timeout=10000)
                    page.locator("#user-input").fill(current_prompt)
                    page.locator("#send-btn").click()
                    card = page.locator(".approval-bubble").last
                    expect(card).to_contain_text("8 PM", timeout=15000)
                    expect(card).to_contain_text("guest@example.test")
                    if artifact_extension == '.png':
                        image = card.locator('.artifact-list img')
                        expect(image).to_have_count(1)
                        artifact_url = image.evaluate('element => element.src')
                    else:
                        artifact = card.locator('.artifact-list a[download]').first
                        expect(artifact).to_be_visible()
                        artifact_url = artifact.evaluate('element => element.href')
                    assert page.request.get(artifact_url).body() == source.read_bytes()
                    assert not sent, "Email reached transport before confirmation"
                    # Complete one real UI approval path as well as proving
                    # previews stay pending. Only the mail transport is fake.
                    if not clarifications and prompt.startswith("Prepare an email"):
                        card.get_by_role("button", name="Send email", exact=True).click()
                        deadline = time.monotonic() + 10
                        while not sent and time.monotonic() < deadline:
                            page.wait_for_timeout(50)
                        assert len(sent) == 1, [row.get('assistant_reply') for row in runtime.store.list_conversation_events() if row.get('event_type')=='conversation.chat_turn']
                        message, paths = sent[0]
                        assert message["Subject"] == drafts[-1]["subject"]
                        assert [part.get_payload(decode=True) for part in message.iter_attachments()] == [source.read_bytes()]
                        assert paths == expected_draft()["attachment_paths"]
                        try:
                            expect(page.locator(".msg.bot .bubble").filter(
                                has_text="Email sent")).to_be_visible(timeout=10000)
                        except AssertionError:
                            print('RESUME RECORDS',json.dumps([e for e in runtime.store.list_conversation_events()
                                if e.get('event_type')=='conversation.chat_turn'],default=str))
                            raise
                    assert not errors
                finally:
                    browser.close(); playwright.stop()
        approvals = [a for a in runtime.store.list_approval_requests() if a.context.get("tool_name") == "email_send"]
        assert len(approvals) == 1 and approvals[0].status.value == ("approved" if sent else "pending")
        assert approvals[0].context["tool_arguments"] == expected_draft()
        return
    result = run_platform_chat_request(runtime, PlatformChatRequest(platform="telegram", text=current_prompt,
        conversation_id="telegram:42", chat_id="42", turn_id="invitation-approval", settings=settings,
        model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model), append_activity_trace=False))
    pending = [approval for approval in runtime.store.list_approval_requests() if approval.context.get("tool_name") == "email_send"]
    assert len(pending) == 1, json.dumps({"reply":result.text, "scopes":scopes,
        "results":[{"tool":r.tool_name,"status":r.status,"output":r.output,"error":r.error} for r in result.tool_results]}, default=str)
    approval = pending[0]
    assert not sent, "Email reached transport before confirmation"
    assert approval.status.value == "pending"
    assert approval.context["tool_arguments"] == expected_draft()
    from nullion.approval_markers import split_tool_approval_marker
    marker = split_tool_approval_marker(result.text)
    assert marker and marker.approval_id == approval.approval_id, result.text
    from nullion.telegram_app import _new_decision_card, _capture_decision_snapshot
    from dataclasses import replace
    before = replace(_capture_decision_snapshot(runtime), pending_approval_ids=frozenset())
    card = _new_decision_card(runtime, before, reply=result.text)
    assert card is not None, result.text
    assert drafts[-1]["body"] in card.text and drafts[-1]["subject"] in card.text
    assert any(button.text == "Send email" for row in card.reply_markup.inline_keyboard for button in row)
    attachment = Path(expected_draft()["attachment_paths"][0])
    assert attachment.read_bytes() == source.read_bytes()
    if artifact_extension == '.png':
        assert Image.open(attachment).size == (480, 640)
    else:
        from pypdf import PdfReader
        assert len(PdfReader(attachment).pages) == 1
    assert "not available for delivery" not in result.text, result.text
    assert result.artifact_paths, (result.text, result.tool_results)
    wire = []
    from nullion.telegram_turn_graph import plan_telegram_post_run_delivery
    plan = plan_telegram_post_run_delivery(text_for_ack=prompt, reply=result.text,
        artifact_paths=tuple(result.artifact_paths), runtime=runtime, conversation_id="telegram:42",
        decision_card=card, stream_final_reply=False, streaming_mode=None, final_only_streaming_mode=None)
    with upload_boundary(wire) as (url, uploads):
        async def deliver():
            async with Bot("123456:synthetic", base_url=url) as bot:
                message = Message.de_json({"message_id":1,"date":1,"chat":{"id":42,"type":"private"},"text":prompt}, bot)
                await _deliver_reply(message, result.text, principal_id="telegram:42", artifact_paths=result.artifact_paths,
                    tool_results=result.tool_results, decision_card=plan.primary_card, delivery_contract=plan.delivery_contract)
        asyncio.run(deliver())
    assert source.read_bytes() in uploads
    assert any("Send email" in json.dumps(payload) for _, payload in wire)
    runtime.checkpoint()


@pytest.mark.parametrize('surface',['telegram','web_http','web_ui','slack','discord'])
@pytest.mark.parametrize('execution_layer',['legacy','code_mode'])
def test_generated_pdf_keeps_approval_and_sent_receipt(tmp_path,monkeypatch,surface,execution_layer):
    test_generated_invitation_and_complete_draft_reach_native_confirmation_without_sending(
        tmp_path,monkeypatch,
        'Prepare an email to guest@example.test with an invitation .pdf for 8 PM at my place. '
        'Show the draft and attachment for confirmation; do not send yet.',
        surface,0,execution_layer,artifact_extension='.pdf')
