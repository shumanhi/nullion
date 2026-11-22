"""Compare the same dependent file workflow on the real Web and Telegram surfaces."""

import asyncio
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from email.parser import BytesParser
from email.policy import default
from types import SimpleNamespace
from urllib.parse import parse_qs
from statistics import mean

import httpx
import pytest

from tests.e2e.test_quoted_reply_review_and_cleanup import call, isolate, text
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Container E2E")


@pytest.mark.parametrize("surface", ["web", "telegram"])
def test_dependent_native_reads_and_artifact_delivery_cost(tmp_path, monkeypatch, surface, record_property):
    from fastapi.testclient import TestClient
    from telegram import Bot, Chat, Document, Message, Update, User

    from nullion import agent_orchestrator, telegram_app
    from nullion.artifacts import artifact_root_for_principal
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime_checkpoints import wait_runtime_checkpoints
    from nullion.telegram_transport import build_telegram_bot
    from nullion.tools import (
        ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec,
        _build_file_read_handler, _build_file_write_handler,
    )
    from nullion.web_app import create_app
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("NULLION_TASK_DECOMPOSITION_ENABLED", "0")
    monkeypatch.setenv("NULLION_CHAT_STREAMING_ENABLED", "false")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec("file_read", "Read a local file", ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, capability_tags=("local_files",)),
        _build_file_read_handler(tmp_path, allowed_roots=[tmp_path]))
    registry.register(ToolSpec("file_write", "Write a text artifact", ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 5, capability_tags=("local_files",),
        produced_artifact_extensions=(".txt",)),
        _build_file_write_handler(tmp_path, allowed_roots=[tmp_path]))
    runtime.active_tool_registry = registry

    rendered = []
    render = agent_orchestrator._tool_result_message_payload

    def record_render(receipt):
        rendered.append(receipt.tool_name)
        return render(receipt)

    monkeypatch.setattr(agent_orchestrator, "_tool_result_message_payload", record_render)
    names = ["file_read", "file_write"]
    expected = "Lima 37\nCusco 47\n".encode()

    class Model:
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            offered = {tool["name"]: tool for tool in kwargs.get("tools", [])}
            if protocol is not None:
                answer = protocol
            elif "select_attachment_format" in offered:
                answer = call("select_attachment_format", {"extension": ".txt", "confidence": 1})
            elif not offered:
                answer = text("{}")
            else:
                time.sleep(.15)  # Identical simulated provider latency for each execution round.
                self.rounds += 1
                answer = self.workflow_step(offered, kwargs["messages"])
            # This scripted provider reports deterministic token counts so the runtime
            # accounting is testable without claiming a commercial model's bill.
            def tokens(value):
                return len(re.findall(r"\w+|[^\w\s]", json.dumps(value, ensure_ascii=False)))
            answer = {**answer, "usage": {"input_tokens": tokens(kwargs), "output_tokens": tokens(answer)}}
            return answer

        def workflow_step(self, offered, messages):
            if self.rounds == 1:
                return call("request_tool_scope", {"capabilities": ["local_files"], "tool_names": names,
                    "required_tool_names": names, "artifact_extensions": [".txt"]})
            if self.rounds == 2 and "code_mode" in offered:
                assert '"produces":[".txt"]' in offered["code_mode"]["description"]
                return call("code_mode", {"tool_names": names, "code": (
                    f"index = call_tool('file_read', {{'path': {str(self.index)!r}}})['output']['content'].strip()\n"
                    "rows = call_tool('file_read', {'path': index})['output']['content'].splitlines()\n"
                    "selected = '\\n'.join(row for row in rows if row.rsplit(' ', 1)[1] != '0') + '\\n'\n"
                    f"call_tool('file_write', {{'path': {str(self.output)!r}, 'content': selected}})['output']"
                )})
            if self.rounds == 2:
                return call("file_read", {"path": str(self.index)})
            history = json.dumps(messages)
            if self.rounds == 3 and "code_mode" not in offered:
                assert str(self.source) in history
                response = call("file_read", {"path": str(self.source)})
                response["content"][0]["id"] = "read-dependent-data"
                return response
            if self.rounds == 4:
                assert "Cusco 47" in history and "Bogota 0" in history
                return call("file_write", {"path": str(self.output), "content": expected.decode()})
            assert self.rounds == (3 if "code_mode" in offered else 5)
            if surface == "telegram":
                # Exercise the normal threshold for publishing a status
                # message, using the same provider delay in both modes.
                time.sleep(2.7)
            return text(f"Resumen listo.\nMEDIA:{self.output}")

    model = Model()
    orchestrator = agent_orchestrator.AgentOrchestrator(model_client=model)
    samples = {}
    prompts = [
        "Usa {index} para encontrar los datos y guarda en un .txt las filas con más de cero.",
        "{index} を使ってデータを探し、ゼロより大きい行を .txt に保存してください。",
        "Lis l’index {index} et enregistre les lignes non nulles dans un fichier .txt.",
    ]
    for mode in ("off", "on"):
        samples[mode] = []
    with TestClient(create_app(runtime, orchestrator, registry)) as client, ThreadPoolExecutor() as pool:
        for i, template in enumerate(prompts):
            modes = ("off", "on") if i % 2 == 0 else ("on", "off")
            for mode in modes:
                monkeypatch.setenv("NULLION_CODE_MODE", mode)
                chat_id = str(4200 + i + (100 if mode == "on" else 0))
                conversation = f"{surface}:{chat_id}"
                root = artifact_root_for_principal(conversation)
                root.mkdir(parents=True, exist_ok=True)
                model.index, model.source, model.output = (root / name for name in ("index.txt", "source.txt", "summary.txt"))
                model.index.write_text(str(model.source), encoding="utf-8")
                model.source.write_bytes(expected + b"Bogota 0\n")
                prompt = template.format(index=model.index)
                model.rounds = 0
                rendered.clear()
                progress = []
                attachments = [{"name": model.index.name, "path": str(model.index)}]
                started = time.monotonic()
                if surface == "web":
                    payload = {"text": prompt, "conversation_id": conversation, "turn_id": "benchmark",
                        "attachments": attachments}
                    request = pool.submit(client.post, "/api/chat", json=payload)
                    while not request.done() and not progress:
                        event = client.get("/api/chat/progress", params={
                            "conversation_id": conversation, "turn_id": "benchmark"}).json()
                        if event["text"]:
                            progress.append(time.monotonic() - started)
                        else:
                            time.sleep(.01)
                    response = request.result(timeout=10)
                    assert response.status_code == 200, response.text
                    artifacts = response.json().get("artifacts", [])
                    assert len(artifacts) == 1, response.text
                    assert client.get(artifacts[0]["url"]).content == expected
                    wall = time.monotonic() - started
                else:
                    settings = NullionSettings(telegram=TelegramSettings(
                        bot_token=TOKEN, operator_chat_id=chat_id, chat_enabled=True))
                    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings,
                        bot_token=TOKEN, operator_chat_id=chat_id, model_client=model,
                        agent_orchestrator=orchestrator)
                    wire = TelegramWire()
                    respond = wire.handle
                    upload_times = []

                    async def handle(request):
                        method = request.url.path.rsplit("/", 1)[-1]
                        if method == "getFile":
                            return httpx.Response(200, json={"ok": True, "result": {
                                "file_id": "index", "file_unique_id": "index", "file_path": "fixture.txt"}})
                        if method == "fixture.txt":
                            return httpx.Response(200, content=model.index.read_bytes())
                        if method in {"sendChatAction", "deleteMessage"}:
                            return httpx.Response(200, json={"ok": True, "result": True})
                        if method == "sendDocument":
                            upload_times.append(time.monotonic() - started)
                        if method == "editMessageText":
                            fields = parse_qs((await request.aread()).decode())
                            return httpx.Response(200, json={"ok": True, "result": {
                                "message_id": 201, "date": 1, "chat": {"id": int(chat_id), "type": "private"},
                                "text": fields["text"][0]}})
                        response = await respond(request)
                        if method == "sendMessage" and "Resumen listo" not in wire.texts[-1]:
                            progress.append(time.monotonic() - started)
                        # The reusable fixture has a fixed operator id.
                        payload = response.json()
                        if isinstance(payload.get("result"), dict) and "chat" in payload["result"]:
                            payload["result"]["chat"]["id"] = int(chat_id)
                        return httpx.Response(response.status_code, json=payload)

                    wire.handle = handle
                    install_wire(monkeypatch, wire)

                    async def replay():
                        async with build_telegram_bot(Bot, TOKEN) as bot:
                            message = Message(message_id=1, date=datetime.now(UTC),
                                chat=Chat(int(chat_id), "private"), from_user=User(int(chat_id), "Fixture", False),
                                caption=prompt, document=Document("index", "index", file_name="index.txt",
                                    mime_type="text/plain", file_size=model.index.stat().st_size))
                            message.set_bot(bot)
                            message.chat.set_bot(bot)
                            await service.on_message(Update(update_id=1, message=message), SimpleNamespace(bot=bot))

                    started = time.monotonic()
                    asyncio.run(replay())
                    assert wait_runtime_checkpoints(runtime, timeout=5)
                    assert len(wire.accepted) == 1, (wire.texts, wire.errors)
                    boundary = wire.accepted[0].split(b"\r\n", 1)[0][2:]
                    multipart = BytesParser(policy=default).parsebytes(
                        b"Content-Type: multipart/form-data; boundary=" + boundary + b"\r\n\r\n" + wire.accepted[0])
                    assert [part.get_payload(decode=True) for part in multipart.iter_parts() if part.get_filename()] == [expected]
                    assert len(upload_times) == 1
                    wall = upload_times[0]
                events = runtime.store.list_conversation_events(conversation)
                timings = [event for event in events if event["event_type"] == "conversation.model_timing"]
                receipts = [receipt for event in events if event["event_type"] == "conversation.chat_turn"
                    for receipt in event.get("tool_results", []) if receipt["tool_name"] in names]
                assert [receipt["tool_name"] for receipt in receipts] == ["file_read", "file_read", "file_write"], receipts
                assert all(receipt["status"] == "completed" for receipt in receipts)
                expected_renders = ["file_read", "file_read", "file_write"] if mode == "off" else ["code_mode"]
                actual_renders = [name for name in rendered if name in {*names, "code_mode"}]
                assert actual_renders == expected_renders
                assert len(timings) == (5 if mode == "off" else 3), timings
                first_deadline = 4.0 if surface == "telegram" else 1.0
                assert progress and 0 <= progress[0] < min(wall, first_deadline) and wall < 8, (progress, wall)
                assert all(event["usage"].get("input_tokens", 0) > 0 for event in timings), timings
                samples[mode].append({"wall_seconds": wall, "model_rounds": len(timings),
                    "first_progress_seconds": progress[0],
                    "fixture_tokens": sum(sum(event["usage"].values()) for event in timings)})

    for mode, measurements in samples.items():
        aggregate = {field: round(mean(row[field] for row in measurements), 3) for field in measurements[0]}
        record_property(f"{surface}_{mode}", json.dumps(aggregate))
        print(f"{surface} Code Mode {mode}: {aggregate}")
    assert mean(row["wall_seconds"] for row in samples["on"]) < mean(row["wall_seconds"] for row in samples["off"])
