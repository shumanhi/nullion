import asyncio
import io
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from PIL import Image

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="set NULLION_RUN_E2E=1 to run browser delivery E2E")


@pytest.mark.parametrize("scope_mode", ["preclassified", "preclassified_open_url", "on_demand", "returned_session", "followup", "followup_focus", "followup_orchestrated", "followup_navigation", "followup_read_failure"])
def test_telegram_capture_preserves_real_browser_session_and_finishes_without_retry(tmp_path, monkeypatch, record_property, scope_mode):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends import auto_backend
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import _deliver_reply
    from nullion.telegram_turn_graph import plan_telegram_post_run_delivery
    from nullion.tools import ToolRegistry, ToolInvocation

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b'<html><head><title>Capture fixture</title></head><body style="background:#136abd"><h1>Verified capture</h1><button id="change" onclick="document.body.style.background=\'#8040c0\'">Change</button></body></html>')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Page)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://capture.localhost:{server.server_port}/"
    # Only DNS is faked; Chromium and the HTTP server remain real.
    import socket
    original_getaddrinfo = socket.getaddrinfo
    monkeypatch.setattr(socket, "getaddrinfo", lambda host, *args, **kwargs: original_getaddrinfo("127.0.0.1" if host == "capture.localhost" else host, *args, **kwargs))

    class Model:
        agent_calls = 0
        phase = 0
        opening = False

        def create(self, **kwargs):
            names = {t["name"] for t in kwargs.get("tools", [])}
            if "select_attachment_format" in names:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "format", "name": "select_attachment_format", "input": {"extension": None if self.opening else ".png", "confidence": 1.0}}]}
            if int(kwargs.get("max_tokens", 0)) == 96 and not names:
                if scope_mode not in {"preclassified", "preclassified_open_url", "followup", "followup_focus", "followup_read_failure"}:
                    return {"content": [{"type": "text", "text": "{}"}]}
                decision = {"requested_outcome": "unspecified", "web_action": "browser_interaction",
                    "scheduler_action": "none", "skill_pack_action": "none", "connector_app_ids": [],
                    "requested_tool_names": ["browser_navigate", "browser_screenshot"], "required_tool_names": [],
                    "requested_artifact_extensions": [] if self.opening else [".png"],
                    "source_artifact_delivery_requested": not self.opening,
                    "source_artifact_request_evidence": prompt if not self.opening else "", "excluded_artifact_extensions": [],
                    "required_embedded_media_extensions": [], "confidence": 0.99}
                if scope_mode == "preclassified_open_url":
                    decision["web_action"] = "open_url"
                if scope_mode.startswith("followup"):
                    decision["web_action"] = "open_url" if self.opening else "browser_interaction"
                    decision["requested_tool_names"] = ["browser_open"] if self.opening else ["browser_screenshot"]
                return {"content": [{"type": "text", "text": json.dumps(decision)}]}
            if not names:
                return {"content": [{"type": "text", "text": "Screenshot attached."}]}
            self.agent_calls += 1
            if scope_mode in {"followup_orchestrated", "followup_navigation"} and self.opening:
                if self.phase == 0 and "request_tool_scope" in names:
                    self.phase = 1
                    opening_tools = ["browser_navigate"] if scope_mode == "followup_navigation" else ["browser_navigate", "browser_open"]
                    name, args = "request_tool_scope", {"capabilities": ["web"], "tool_names": opening_tools, "required_tool_names": opening_tools}
                elif self.phase == 1:
                    self.phase = 2
                    name, args = "browser_navigate", {"url": url}
                elif self.phase == 2 and scope_mode == "followup_orchestrated":
                    self.phase = 3
                    name, args = "browser_open", {}
                else:
                    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "The page is open."}]}
            elif scope_mode.startswith("followup") and not self.opening:
                if self.phase == 3:
                    for message in reversed(kwargs["messages"]):
                        for block in message.get("content", []):
                            if isinstance(block, dict) and block.get("type") == "tool_result":
                                receipt = json.loads(block["content"][0]["text"])
                                path = receipt.get("output", {}).get("artifact_path")
                                if path:
                                    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": f"Screenshot attached.\n\nMEDIA:{path}"}]}
                    raise AssertionError("Capture receipt missing")
                if scope_mode == "followup_focus" and self.phase == 0 and "browser_open" in names:
                    self.phase = 1
                    name, args = "browser_open", {}
                elif "browser_screenshot" in names:
                    self.phase = 3
                    name, args = "browser_screenshot", {}
                elif "request_tool_scope" in names:
                    name, args = "request_tool_scope", {"capabilities": ["web"], "tool_names": ["browser_screenshot"], "required_tool_names": ["browser_screenshot"]}
                else:
                    raise AssertionError(f"Capture scope unavailable: {names}")
            elif self.phase == 0 and "request_tool_scope" in names:
                self.phase = 1
                name, args = "request_tool_scope", {"capabilities": ["web"], "tool_names": ["browser_navigate", "browser_screenshot"], "required_tool_names": ["browser_navigate", "browser_screenshot"]}
            elif "browser_navigate" in names and self.phase <= 1:
                self.phase = 2
                name, args = "browser_navigate", {"url": url}
                if scope_mode != "returned_session":
                    args["session_id"] = "capture"
            elif "browser_screenshot" in names and self.phase == 2:
                self.phase = 3
                name, args = "browser_screenshot", {"session_id": "capture"}
                if scope_mode == "returned_session":
                    receipts = []
                    for message in kwargs["messages"]:
                        for block in message.get("content", []):
                            if isinstance(block, dict) and block.get("type") == "tool_result":
                                for content in block.get("content", []):
                                    if isinstance(content, dict) and content.get("type") == "text":
                                        receipts.append(json.loads(content["text"]))
                    args["session_id"] = next(receipt["output"]["session_id"] for receipt in reversed(receipts)
                        if isinstance(receipt.get("output"), dict) and "session_id" in receipt["output"])
            else:
                raise AssertionError(f"Unexpected model retry: tools={names}, max_tokens={kwargs.get('max_tokens')}")
            if name == "request_tool_scope" and not self.opening:
                args.update(source_artifact_delivery_requested=True, source_artifact_request_evidence=prompt, artifact_extensions=[".png"])
            if name in names:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": f"call-{self.agent_calls}", "name": name, "input": args}]}
            raise AssertionError(f"Unexpected model retry: tools={names}, max_tokens={kwargs.get('max_tokens')}")

    class Message:
        def __init__(self, text):
            self.text = text
            self.documents = []
            self.replies = []

        async def reply_document(self, document, **kwargs):
            self.documents.append((document.read(), kwargs))

        async def reply_text(self, text, **kwargs):
            self.replies.append(text)

    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "auto")
    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: False)
    monkeypatch.setattr(auto_backend, "_reuse_preferred_cdp_if_available", lambda: False)
    monkeypatch.setattr(auto_backend, "_find_chrome", lambda: None)
    model = Model()
    elapsed = []
    backends = []
    events = []
    navigation_sessions = set()
    try:
        for index, prompt in enumerate([f"Send a screenshot of {url}", f"このページの画像を添付してください {url}", f"Captura esta página y adjúntala: {url}"]):
            chat_id = str(100001 + index) if scope_mode.startswith("followup") else "100001"
            model.phase = 0
            runtime = bootstrap_persistent_runtime(tmp_path / f"runtime-{index}.db")
            registry = ToolRegistry()
            register_browser_tools(registry)
            backends.append(registry._handlers["browser_navigate"].__self__._backend)
            runtime.active_tool_registry = registry
            settings = NullionSettings(telegram=TelegramSettings(bot_token="123456:abcdefghijklmnopqrstuvwxyz", operator_chat_id=chat_id, chat_enabled=True))
            if scope_mode.startswith("followup"):
                model.opening = True
                opened = handle_chat_operator_message(runtime, f"Open {url} and keep it open.", chat_id=chat_id,
                    settings=settings, model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
                    append_activity_trace=False, allow_mini_agents=False)
                assert "Live-search blocker" not in opened
                opening_turn = next(e for e in reversed(runtime.store.list_conversation_events(f"telegram:{chat_id}")) if e.get("event_type") == "conversation.chat_turn")
                navigation = next(r for r in opening_turn["tool_results"] if r["tool_name"] == "browser_navigate")
                assert navigation["status"] == "completed", navigation
                session_id = navigation["output"]["session_id"]
                navigation_sessions.add(session_id)
                clicked = registry.invoke(ToolInvocation("change-page", "browser_click", "telegram_chat",
                    {"selector": "#change"}, flow_context={"browser_session_id": session_id}))
                assert clicked.status == "completed", clicked
                if scope_mode == "followup_read_failure":
                    extracted = registry.invoke(ToolInvocation("missing-detail", "browser_extract_detail", "telegram_chat",
                        {"selector": "#missing-detail"}, flow_context={"browser_session_id": session_id}))
                    assert extracted.status == "failed", extracted
                    assert extracted.output == {"session_id": session_id, "page_state_preserved": True}
                    runtime.store.add_conversation_event({"event_type": "conversation.chat_turn",
                        "conversation_id": f"telegram:{chat_id}", "tool_results": [{"tool_name": extracted.tool_name,
                            "status": extracted.status, "output": extracted.output}]})
                model.opening = False
                model.phase = 0
                prompt = ["Capture the current page.", "現在のページの画像を添付してください。", "Captura la página actual y adjúntala."][index]
            started = time.monotonic()
            reply = handle_chat_operator_message(runtime, prompt, chat_id=chat_id, settings=settings,
                model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
                append_activity_trace=False, allow_mini_agents=False)
            assert reply is not None
            plan = plan_telegram_post_run_delivery(text_for_ack=prompt, reply=reply,
                stream_final_reply=True, streaming_mode="stream", final_only_streaming_mode="final",
                runtime=runtime, conversation_id=f"telegram:{chat_id}")
            delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram_chat", delivery_contract=plan.delivery_contract)
            assert len(delivery.attachments) == 1, (prompt, runtime.store.list_approval_requests(), runtime.store.list_conversation_events(f"telegram:{chat_id}"))
            assert "still open" not in delivery.text and "verified evidence" not in delivery.text
            message = Message(prompt)
            asyncio.run(_deliver_reply(message, reply, principal_id="telegram_chat", delivery_contract=plan.delivery_contract))
            assert len(message.documents) == 1
            data, options = message.documents[0]
            assert data == delivery.attachments[0].read_bytes()
            image = Image.open(io.BytesIO(data)).convert("RGB")
            assert image.getpixel((image.width // 2, image.height // 2)) == ((128, 64, 192) if scope_mode.startswith("followup") else (19, 106, 189))
            elapsed.append(time.monotonic() - started)
            events.extend(runtime.store.list_conversation_events(f"telegram:{chat_id}"))
        if scope_mode.startswith("followup"):
            assert model.agent_calls <= (30 if scope_mode in {"followup_orchestrated", "followup_navigation"} else 12 if scope_mode == "followup_focus" else 9)
        else:
            assert model.agent_calls == (0 if scope_mode in {"preclassified", "preclassified_open_url"} else 9)
        record_property("capture_elapsed_seconds", json.dumps(elapsed))
        assert sum(elapsed) / len(elapsed) < 10, elapsed
        assert max(elapsed) < 12, elapsed
        turns = [e for e in events if e.get("event_type") == "conversation.chat_turn" and any(r["tool_name"] == "browser_screenshot" for r in e.get("tool_results", []))]
        assert len(turns) == 3
        sessions = set()
        for turn in turns:
            results = turn["tool_results"]
            results = [r for r in results if r["tool_name"] != "request_tool_scope"]
            if scope_mode.startswith("followup"):
                assert [r["tool_name"] for r in results] == (["browser_open", "browser_screenshot"] if scope_mode == "followup_focus" else ["browser_screenshot"])
                assert all(r["output"]["session_id"] in navigation_sessions for r in results)
                assert results[-1]["output"]["page_url"] == url
                sessions.add(results[-1]["output"]["session_id"])
                continue
            assert [r["tool_name"] for r in results] == ["browser_navigate", "browser_screenshot"]
            assert results[0]["output"]["session_id"] == results[1]["output"]["session_id"]
            assert results[1]["output"]["page_url"] == url
            sessions.add(results[0]["output"]["session_id"])
        assert len(sessions) == 3
    finally:
        from nullion.plugins.browser_plugin.browser_tools import _run
        for backend in backends:
            _run(backend.shutdown())
        server.shutdown()
        server.server_close()
