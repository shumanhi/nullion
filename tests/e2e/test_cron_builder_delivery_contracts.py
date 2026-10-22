from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run cron/builder delivery e2e tests",
)


class FakeAttachmentFormatModel:
    def __init__(self, extension: str | None) -> None:
        self.extension = extension

    def create(self, **kwargs):  # noqa: ANN003
        tools = kwargs.get("tools") or []
        if tools and tools[0].get("name") == "select_attachment_format":
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_attachment_format",
                        "input": {"extension": self.extension, "confidence": 0.96},
                    }
                ]
            }
        return {"content": [{"type": "text", "text": "ok"}]}


def test_platform_delivery_is_file_agnostic_for_json_audio_and_random_artifacts(tmp_path: Path) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    artifacts = [
        tmp_path / "nullion-artifact-output.json",
        tmp_path / "nullion-artifact-narration.mp3",
        tmp_path / "nullion-artifact-scene.blend",
    ]
    artifacts[0].write_text('{"ok": true}', encoding="utf-8")
    artifacts[1].write_bytes(b"ID3 e2e audio")
    artifacts[2].write_bytes(b"BLENDER e2e arbitrary file")

    for artifact in artifacts:
        delivery = prepare_reply_for_platform_delivery(f"Created.\nMEDIA:{artifact}")

        assert delivery.attachments == (artifact,)
        assert delivery.media_directive_count == 1
        assert delivery.unavailable_attachment_count == 0
        assert str(artifact) not in (delivery.text or "")
        assert "MEDIA" not in (delivery.text or "")


def test_platform_delivery_attaches_real_pdf_referenced_by_sandbox_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    data_dir = tmp_path / "data"
    output_root = data_dir / "outputs"
    output_root.mkdir(parents=True)
    pdf_artifact = output_root / "NYC_to_San_Francisco_by_Train_Guide.pdf"
    pdf_artifact.write_bytes(b"%PDF-1.4\n%e2e real pdf\n")
    monkeypatch.setenv("NULLION_DATA_DIR", str(data_dir))

    reply = (
        "Done — I verified the PDF artifact exists and is a real PDF file.\n\n"
        "[Download the NYC to San Francisco Train Guide PDF](sandbox:NYC_to_San_Francisco_by_Train_Guide.pdf)"
    )

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.attachments == (pdf_artifact,)
    assert delivery.media_directive_count == 1
    assert delivery.unavailable_attachment_count == 0
    assert "sandbox:" not in (delivery.text or "")
    assert str(pdf_artifact) not in (delivery.text or "")
    assert "Download the NYC to San Francisco Train Guide PDF" in (delivery.text or "")


def test_telegram_cron_delivery_uploads_bare_media_filename_and_records_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import telegram_app

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    artifact = (
        tmp_path
        / "data"
        / "workspaces"
        / "workspace_admin"
        / "artifacts"
        / "nullion_public_awareness_github_traffic_monitor.html"
    )
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<html><body>" + ("traffic " * 20) + "</body></html>", encoding="utf-8")

    class Message:
        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=100001)
            self.message_id = 700
            self.documents: list[dict] = []
            self.replies: list[tuple[str, dict]] = []

        async def reply_document(self, document, **kwargs):  # noqa: ANN001
            self.documents.append({"data": document.read(), "kwargs": kwargs})

        async def reply_text(self, text, **kwargs):  # noqa: ANN001
            self.replies.append((text, kwargs))

    message = Message()

    asyncio.run(
        telegram_app._deliver_reply(
            message,
            (
                "⏰ Manual scheduled task run: Nullion social + GitHub traffic monitor\n\n"
                "Run complete.\n\n"
                "MEDIA nullion_public_awareness_github_traffic_monitor.html"
            ),
            principal_id="telegram:100001",
        )
    )

    assert message.replies == []
    assert len(message.documents) == 1
    assert message.documents[0]["data"] == artifact.read_bytes()
    assert "MEDIA" not in message.documents[0]["kwargs"]["caption"]
    receipts = [
        json.loads(line)
        for line in (tmp_path / "data" / "delivery_receipts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert receipts[-1]["status"] == "succeeded"
    assert receipts[-1]["attachment_count"] == 1
    assert receipts[-1]["attachment_required"] is True


def test_cron_single_agent_runner_exposes_structured_connector_tools_without_gateway_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.runtime_store import RuntimeStore
    from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr("nullion.cron_execution_tools._connected_connector_provider_summaries", lambda principal_id: ())

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="vendor_record_search",
            description="Search records through a configured account provider.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=20,
            capability_tags=("records", "connector"),
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"query": invocation.arguments.get("query"), "results": [{"id": "msg-1"}]},
        ),
    )
    registry.register(
        ToolSpec(
            name="connector_request",
            description="Generic connector gateway.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=False,
            timeout_seconds=20,
            capability_tags=("connector",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        ToolSpec(
            name="vendor_record_send",
            description="Write a record through a configured account provider.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
            timeout_seconds=20,
            capability_tags=("records", "connector"),
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "denied",
            {"reason": "approval_required", "requires_approval": True, "approval_id": "approval-record-send"},
            error="Approval required for tool: vendor_record_send",
        ),
    )

    captured: dict[str, object] = {}

    class FakeOrchestrator:
        model_client = None

        def run_turn(self, **kwargs):  # noqa: ANN003
            scoped_registry = kwargs["tool_registry"]
            captured["definitions"] = [tool["name"] for tool in scoped_registry.list_tool_definitions()]
            read_result = scoped_registry.invoke(
                ToolInvocation(
                    "tool-record-search",
                    "vendor_record_search",
                    kwargs["principal_id"],
                    {"query": "newer_than:1d", "limit": 1},
                )
            )
            gateway_result = scoped_registry.invoke(
                ToolInvocation(
                    "tool-gateway",
                    "connector_request",
                    kwargs["principal_id"],
                    {"provider_id": "skill_pack_connector_example", "url": "https://example.com"},
                )
            )
            send_result = scoped_registry.invoke(
                ToolInvocation(
                    "tool-send",
                    "vendor_record_send",
                    kwargs["principal_id"],
                    {"target": "team-review", "body": "Review before writing."},
                )
            )
            return SimpleNamespace(
                final_text="Checked the configured structured tools.",
                tool_results=[read_result, gateway_result, send_result],
                artifacts=[],
            )

    events: list[tuple[str, dict[str, object]]] = []

    def record_event(event_type, *args, **extra):  # noqa: ANN001
        _ = args
        events.append((event_type, extra))

    job = SimpleNamespace(
        id="cron-structured-tools",
        name="Structured connector scheduled task",
        task="Use the configured structured tools for this scheduled run.",
    )
    runtime = SimpleNamespace(store=RuntimeStore(), checkpoint_path=tmp_path / "runtime.db")
    settings = SimpleNamespace(enabled_skill_packs=())

    result = run_single_agent_cron_turn(
        job,
        "telegram:100001",
        label="Scheduled task",
        orchestrator=FakeOrchestrator(),
        runtime=runtime,
        tool_registry=registry,
        settings=settings,
        record_event=record_event,
    )

    connector_preflight = [
        extra
        for event_type, extra in events
        if event_type == "cron.delivery.agent_preflight" and extra.get("stage") == "connector_scope"
    ]
    assert set(captured["definitions"]) == {"vendor_record_search"}
    assert result["cron_execution_mode"] == "single_agent"
    assert result["tool_results"][0].status == "completed"
    assert result["tool_results"][0].tool_name == "vendor_record_search"
    assert result["tool_results"][1].status == "denied"
    assert result["tool_results"][1].output["reason"] == "cron_execution_connector_scope_denied"
    assert result["tool_results"][2].status == "denied"
    assert result["tool_results"][2].output["reason"] == "cron_execution_account_write_denied"
    assert connector_preflight
    assert connector_preflight[0]["connector_allowed"] is False
    assert connector_preflight[0]["structured_passthrough_tools"] == 1


def test_cron_record_budget_preserves_research_tools_and_delivers_html(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sourced_report_model_reply,
) -> None:
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import (
        CronRunDeliveryCallbacks,
        cron_delivery_block_reason,
        run_cron_delivery_workflow,
        run_single_agent_cron_turn,
    )
    from nullion.crons import CronJob
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.runtime_store import RuntimeStore
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(
        "nullion.cron_execution_tools.build_cron_connector_scope_decision",
        lambda **_kwargs: SimpleNamespace(allow_connector_tools=False, provider_ids=()),
    )

    report_path = tmp_path / "data" / "workspaces" / "workspace_admin" / "artifacts" / "traffic-report.html"
    report_html = (
        "<!doctype html><html><head><meta charset='utf-8'><title>Traffic report</title></head>"
        "<body><main><h1>GitHub Traffic Trend</h1><section>X / Twitter</section>"
        "<section>Reddit</section><p>Verified current-run source records.</p></main></body></html>"
    )

    class RecordBudgetModel:
        def __init__(self) -> None:
            self.tool_sets: list[tuple[str, ...]] = []

        def create(self, **kwargs):  # noqa: ANN003
            review = sourced_report_model_reply(kwargs, source_tools=['browser_extract_items'],
                producer_tool='file_write', expected_tokens=['GitHub Traffic Trend', 'X / Twitter', 'Reddit'])
            if review is not None:
                return review
            tools = tuple(tool["name"] for tool in kwargs.get("tools") or ())
            self.tool_sets.append(tools)
            messages = kwargs.get("messages") or ()
            completed_ids = {
                str(block.get("tool_use_id") or "")
                for message in messages
                for block in (message.get("content") or ())
                if isinstance(block, dict) and block.get("type") == "tool_result"
            }
            if "write-report" in completed_ids:
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Report attached."}]}
            if not tools:
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": json.dumps({"requirements_satisfied": True, "html": ""})}]}
            if "source-records" in completed_ids:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "write-report",
                            "name": "file_write",
                            "input": {"path": str(report_path), "content": report_html},
                        }
                    ],
                }
            if "verify-source-page" not in completed_ids:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "verify-source-page",
                            "name": "browser_assert_page_state",
                            "input": {
                                "selector": "body",
                                "required": ["Verified source"],
                            },
                        }
                    ],
                }
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "source-records",
                        "name": "browser_extract_items",
                        "input": {},
                    }
                ],
            }

    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(
        ToolSpec(
            name="browser_assert_page_state",
            description="Verify the current source page before extracting records.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "schema_version": 1,
                "kind": "page_state_assertion",
                "session_id": "traffic-source-session",
                "verified": True,
                "contract": {
                    "required": ["Verified source"],
                    "forbidden": [],
                    "selector": "body",
                },
                "result": {
                    "ok": True,
                    "url": "https://example.test/report",
                    "selector": "body",
                    "required": [
                        {"expected": "Verified source", "match": "Verified source"}
                    ],
                    "forbidden": [],
                    "missing": [],
                    "forbidden_found": [],
                },
            },
        ),
    )
    registry.register(
        ToolSpec(
            name="browser_extract_items",
            description="Return structured current-page records.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "session_id": "traffic-source-session",
                "item_count": 12,
                "items": [
                    {"title": f"Verified source {index}", "url": f"https://example.test/{index}"}
                    for index in range(12)
                ],
            },
        ),
    )

    def _write_report(invocation):  # noqa: ANN001
        path = Path(invocation.arguments["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(invocation.arguments["content"]), encoding="utf-8")
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"path": str(path), "artifact_path": str(path), "artifact_paths": [str(path)]},
        )

    registry.register(
        ToolSpec(
            name="file_write",
            description="Create the requested artifact.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={
                "type": "object",
                "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
                "required": ["path", "content"],
            },
        ),
        _write_report,
    )

    model = RecordBudgetModel()
    orchestrator = AgentOrchestrator(model_client=model)
    runtime = SimpleNamespace(store=RuntimeStore(), checkpoint_path=tmp_path / "runtime.db")
    settings = SimpleNamespace(enabled_skill_packs=())
    job = CronJob(
        id="cron-record-budget",
        name="Structured traffic report",
        schedule="0 8 * * *",
        task="Build the configured report from current-run structured source records.",
        delivery_channel="telegram",
        delivery_target="100001",
        artifact_delivery_options={
            "requires_attachment_delivery": True,
            "required_artifact_extensions": [".html"],
            "deliver_required_extensions_only": True,
            "artifact_focus_min_source_results": 12,
            "required_artifact_content_tokens": ["GitHub Traffic Trend", "X / Twitter", "Reddit"],
        },
    )
    deliveries: list[object] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda _job: "telegram",
        delivery_target=lambda _job, _channel: "100001",
        run_agent_turn=lambda cron_job, conversation_id, cancellation_checker=None: run_single_agent_cron_turn(
            cron_job,
            conversation_id,
            label="Scheduled task",
            orchestrator=orchestrator,
            runtime=runtime,
            tool_registry=registry,
            settings=settings,
            cancellation_checker=cancellation_checker,
        ),
        record_event=lambda *_args, **_kwargs: None,
        block_reason=lambda result, text, artifacts: cron_delivery_block_reason(result, text, artifacts),
        save_web_delivery=lambda *_args, **_kwargs: False,
        send_platform_delivery=lambda _job, _channel, _target, text: deliveries.append(
            prepare_reply_for_platform_delivery(text, principal_id="telegram:100001")
        )
        or True,
    )

    result = run_cron_delivery_workflow(job, label="Scheduled task", callbacks=callbacks)

    assert set(model.tool_sets[2]) == {"browser_assert_page_state", "browser_extract_items", "file_write"}
    assert result.get("cron_delivery_failed") is not True
    assert len(deliveries) == 1
    assert deliveries[0].attachments == (report_path,)
    delivered_html = report_path.read_text(encoding="utf-8")
    assert "nullion-responsive-delivery-guard-v2" in delivered_html
    assert all(token in delivered_html for token in ("GitHub Traffic Trend", "X / Twitter", "Reddit"))


def test_telegram_background_artifact_delivery_awaits_document_upload_and_records_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import telegram_app

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    artifact = (
        tmp_path
        / "data"
        / "workspaces"
        / "workspace_admin"
        / "artifacts"
        / "nyc_nightlife_promotional.pdf"
    )
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"%PDF-1.4\n%e2e pdf\n")

    class FakeBot:
        instances: list["FakeBot"] = []

        def __init__(self, token: str) -> None:
            self.token = token
            self.documents: list[dict[str, object]] = []
            self.messages: list[dict[str, object]] = []
            FakeBot.instances.append(self)

        async def __aenter__(self) -> "FakeBot":
            return self

        async def __aexit__(self, *_args) -> None:  # noqa: ANN002
            return None

        async def send_document(self, *, chat_id, document, caption=None):  # noqa: ANN001
            self.documents.append({"chat_id": chat_id, "data": document.read(), "caption": caption})
            return SimpleNamespace(message_id=991)

        async def send_message(self, *, chat_id, text, **kwargs):  # noqa: ANN001, ANN003
            self.messages.append({"chat_id": chat_id, "text": text, "kwargs": kwargs})
            return SimpleNamespace(message_id=992)

    monkeypatch.setitem(sys.modules, "telegram", SimpleNamespace(Bot=FakeBot))

    delivered = asyncio.run(
        telegram_app._send_operator_telegram_message(
            "token",
            "100001",
            f"MEDIA:{artifact.name}",
            principal_id="telegram:100001",
        )
    )

    assert delivered is True
    assert len(FakeBot.instances) == 1
    assert FakeBot.instances[0].documents == [
        {"chat_id": "100001", "data": artifact.read_bytes(), "caption": None}
    ]
    receipts = [
        json.loads(line)
        for line in (tmp_path / "data" / "delivery_receipts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert receipts[-1]["status"] == "succeeded"
    assert receipts[-1]["attachment_count"] == 1
    assert receipts[-1]["attachment_required"] is True
    assert receipts[-1]["message_id"] == "991"


def test_telegram_chat_turn_uploads_named_workspace_artifact_instead_of_filename_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import chat_operator, telegram_app
    from nullion.artifacts import artifact_root_for_principal
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-named-artifact.db")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    artifact_root = artifact_root_for_principal("telegram:100001")
    artifact_root.mkdir(parents=True, exist_ok=True)
    artifact = artifact_root / "nullion_bug_tracker_2026-05-08.xlsx"
    artifact.write_bytes(b"PK\x03\x04xlsx e2e bytes")

    class FilenameOnlyOrchestrator:
        model_client = None

        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="nullion_bug_tracker_2026-05-08.xlsx",
                artifacts=[],
                tool_results=[
                    ToolResult(
                        invocation_id="file-search",
                        tool_name="file_search",
                        status="completed",
                        output={"matches": [str(artifact)]},
                    )
                ],
                thinking_text="",
            )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat 添付 42",
        chat_id="100001",
        settings=settings,
        agent_orchestrator=FilenameOnlyOrchestrator(),
        append_activity_trace=False,
    )

    class Message:
        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=100001)
            self.message_id = 7198
            self.documents: list[dict] = []
            self.replies: list[tuple[str, dict]] = []

        async def reply_document(self, document, **kwargs):  # noqa: ANN001
            self.documents.append({"data": document.read(), "kwargs": kwargs})

        async def reply_text(self, text, **kwargs):  # noqa: ANN001
            self.replies.append((text, kwargs))

    message = Message()
    asyncio.run(telegram_app._deliver_reply(message, reply, principal_id="telegram:100001"))

    assert isinstance(reply, str) and reply.strip()
    assert (
        reply == f"Done — attached the requested spreadsheet.\n\nMEDIA:{artifact}"
        or reply == "nullion_bug_tracker_2026-05-08.xlsx"
    )
    assert message.replies == []
    assert len(message.documents) == 1
    assert message.documents[0]["data"] == artifact.read_bytes()
    assert "MEDIA" not in message.documents[0]["kwargs"]["caption"]
    receipts = [
        json.loads(line)
        for line in (tmp_path / "data" / "delivery_receipts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert receipts[-1]["status"] == "succeeded"
    assert receipts[-1]["attachment_count"] == 1
    assert receipts[-1]["attachment_required"] is True


def test_telegram_file_delivery_followup_does_not_reattach_existing_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-artifact-followup.db")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    artifact = (
        tmp_path
        / "data"
        / "workspaces"
        / "workspace_admin"
        / "artifacts"
        / "nullion_public_awareness_github_traffic_monitor.html"
    )
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<html><body>" + ("traffic " * 20) + "</body></html>", encoding="utf-8")

    chat_operator._remember_chat_turn(
        runtime,
        chat_id="100001",
        user_message="structured cron run",
        assistant_reply=f"Run complete.\nMEDIA {artifact.name}",
    )
    parent_turn_id = runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    parent_branch_id = runtime.store.get_conversation_head("telegram:100001")["active_branch_id"]
    unrelated_artifact = artifact.parent / "unrelated_train_guide.pdf"
    unrelated_artifact.write_bytes(b"%PDF-1.4\n%wrong follow-up target\n")
    runtime.store.add_conversation_event(
        {
            "conversation_id": "telegram:100001",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-05-07T22:30:00+00:00",
            "chat_id": "100001",
            "turn_id": "turn-unrelated-artifact",
            "parent_turn_id": None,
            "branch_id": "branch-unrelated-artifact",
            "user_message": "different branch",
            "assistant_reply": f"Created another file.\nMEDIA:{unrelated_artifact}",
            "tool_results": [],
        }
    )
    dispatch_decision = TurnDispatchDecision(
        policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
        dependency_turn_ids=(parent_turn_id,),
        disposition=ConversationTurnDisposition.CONTINUE,
        reason="model_structured_follow_up",
    )

    class NormalOrchestrator:
        model_client = None

        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(final_text="Handled as a normal follow-up.", tool_results=[])

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: chat_operator.ArtifactWorkflowResult("none", "not_matched"),
    )
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    before_files = sorted(path.name for path in artifact.parent.iterdir())
    activity: list[dict] = []

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat 添付ファイルをもう一度送って",
        chat_id="100001",
        settings=settings,
        agent_orchestrator=NormalOrchestrator(),
        activity_callback=activity.append,
        append_activity_trace=False,
        turn_dispatch_decision=dispatch_decision,
    )

    assert reply == "Handled as a normal follow-up."
    assert "MEDIA:" not in reply
    assert artifact.name not in reply
    assert unrelated_artifact.name not in reply
    assert runtime.store.get_conversation_head("telegram:100001")["active_branch_id"] == parent_branch_id
    assert sorted(path.name for path in artifact.parent.iterdir()) == before_files
    assert "Reusing existing artifact" not in [event["label"] for event in activity]


def test_telegram_bug_report_followup_does_not_reattach_stale_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-bug-report-followup.db")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    artifact_root = tmp_path / "data" / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    bug_report = artifact_root / "bug_report_unrequested_empty_codex_logs_2026-05-08.md"
    bug_report.write_text("# Bug Report\nCodex log attachment issue.\n", encoding="utf-8")
    stale_monitor = artifact_root / "nullion_monitor_2026-05-06_run3.html"
    stale_monitor.write_text("<html><body>" + ("monitor " * 20) + "</body></html>", encoding="utf-8")

    chat_operator._remember_chat_turn(
        runtime,
        chat_id="100001",
        user_message="file the bug report",
        assistant_reply=f"Created the bug report.\nMEDIA:{bug_report}",
    )
    head = runtime.store.get_conversation_head("telegram:100001")
    parent_turn_id = head["active_turn_id"]
    branch_id = head["active_branch_id"]
    runtime.store.add_conversation_event(
        {
            "conversation_id": "telegram:100001",
            "event_type": "conversation.chat_turn",
            "created_at": "2026-05-08T11:55:00+00:00",
            "chat_id": "100001",
            "turn_id": "turn-stale-monitor",
            "parent_turn_id": parent_turn_id,
            "branch_id": branch_id,
            "user_message": "previous unrelated monitor",
            "assistant_reply": f"Created monitor.\nMEDIA:{stale_monitor}",
            "tool_results": [],
        }
    )
    dispatch_decision = TurnDispatchDecision(
        policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
        dependency_turn_ids=(parent_turn_id,),
        disposition=ConversationTurnDisposition.CONTINUE,
        reason="model_structured_follow_up",
    )

    class NormalOrchestrator:
        model_client = None

        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(final_text="Handled as a normal bug-report follow-up.", tool_results=[])

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: chat_operator.ArtifactWorkflowResult("none", "not_matched"),
    )
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    before_files = sorted(path.name for path in artifact_root.iterdir())

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat Where is the report bug file?",
        chat_id="100001",
        settings=settings,
        agent_orchestrator=NormalOrchestrator(),
        append_activity_trace=False,
        turn_dispatch_decision=dispatch_decision,
    )

    assert reply == "Handled as a normal bug-report follow-up."
    assert "MEDIA:" not in reply
    assert bug_report.name not in reply
    assert stale_monitor.name not in reply
    assert sorted(path.name for path in artifact_root.iterdir()) == before_files


def test_unrequested_codex_log_media_is_not_delivered_as_chat_attachment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import _append_chat_artifacts_to_reply, _enforce_chat_response_fulfillment
    from nullion.messaging_adapters import delivery_contract_for_turn, prepare_reply_for_platform_delivery
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    principal_id = "telegram:100001"
    conversation_id = "telegram:100001"
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-diagnostic-log.db")
    artifact_root = artifact_root_for_principal(principal_id)
    log_artifact = artifact_root / "codex_20260508_113329.log"
    log_artifact.write_text("error: unexpected argument '--ask-for-approval' found\nEXIT_CODE:2\n", encoding="utf-8")
    tool_result = ToolResult(
        "codex-log",
        "terminal_exec",
        "completed",
        {"artifact_paths": [str(log_artifact)]},
    )

    reply = _append_chat_artifacts_to_reply(
        runtime,
        reply=f"Codex startup attempt failed.\nMEDIA:{log_artifact}",
        artifact_paths=[str(log_artifact)],
        prompt="start the background codex run",
        principal_id=principal_id,
        tool_results=[tool_result],
        required_attachment_extensions=(),
    )
    fulfilled = _enforce_chat_response_fulfillment(
        runtime,
        conversation_id=conversation_id,
        prompt="start the background codex run",
        reply=reply,
        artifact_paths=[str(log_artifact)],
        tool_results=[tool_result],
        principal_id=principal_id,
        required_attachment_extensions=(),
    )
    contract = delivery_contract_for_turn(
        "start the background codex run",
        reply=fulfilled,
        requires_attachment_delivery=False,
        required_attachment_extensions=(),
    )
    delivery = prepare_reply_for_platform_delivery(
        fulfilled,
        principal_id=principal_id,
        delivery_contract=contract,
    )

    assert "MEDIA" not in fulfilled
    assert str(log_artifact) not in fulfilled
    assert delivery.attachments == ()
    assert delivery.media_directive_count == 0
    assert delivery.unavailable_attachment_count == 0


def test_chat_json_file_delivery_survives_ellipsis_text_without_for_attachment_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion.artifacts import artifact_root_for_principal
    from nullion.attachment_format_graph import plan_attachment_format
    from nullion.chat_operator import _append_chat_artifacts_to_reply, _enforce_chat_response_fulfillment
    from nullion.messaging_adapters import delivery_contract_for_turn, prepare_reply_for_platform_delivery
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))

    principal_id = "telegram:100001"
    conversation_id = "telegram:100001"
    prompt = (
        "fetch yahoo.com for latest news and send me as a json file "
        "...for all the titles....strucure doesnt matter , just valid json"
    )
    requested_extension = plan_attachment_format(
        prompt,
        model_client=FakeAttachmentFormatModel(".json"),
    ).extension
    assert requested_extension == ".json"

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime-json-delivery.db")
    artifact_root = artifact_root_for_principal(principal_id)
    artifact = artifact_root / "yahoo_latest_news_titles_2026-05-04.json"
    artifact.write_text('{"titles": ["Market opens higher"]}', encoding="utf-8")
    tool_result = ToolResult(
        "write-json",
        "file_write",
        "completed",
        {"path": str(artifact), "artifact_path": str(artifact)},
    )

    reply = _append_chat_artifacts_to_reply(
        runtime,
        reply="Created the requested JSON file.",
        artifact_paths=(),
        prompt=prompt,
        principal_id=principal_id,
        tool_results=[tool_result],
        required_attachment_extensions=(requested_extension,),
    )
    fulfilled = _enforce_chat_response_fulfillment(
        runtime,
        conversation_id=conversation_id,
        prompt=prompt,
        reply=reply,
        tool_results=[tool_result],
        principal_id=principal_id,
        required_attachment_extensions=(requested_extension,),
    )
    contract = delivery_contract_for_turn(
        prompt,
        reply=fulfilled,
        requires_attachment_delivery=True,
        required_attachment_extensions=(requested_extension,),
    )
    delivery = prepare_reply_for_platform_delivery(
        fulfilled,
        principal_id=principal_id,
        delivery_contract=contract,
    )

    assert "for attachment" not in fulfilled
    assert "still needs" not in fulfilled
    assert delivery.attachments == (artifact,)
    assert delivery.media_directive_count == 1
    assert delivery.unavailable_attachment_count == 0
    assert str(artifact) not in (delivery.text or "")


def test_platform_delivery_rejects_wrong_media_extension_when_pdf_is_required(tmp_path: Path) -> None:
    from nullion.messaging_adapters import delivery_contract_for_turn, prepare_reply_for_platform_delivery

    wrong_artifact = tmp_path / "nullion-artifact-news.json"
    wrong_artifact.write_text('{"title": "not a pdf"}', encoding="utf-8")
    reply = f"作成しました\nMEDIA:{wrong_artifact}"
    contract = delivery_contract_for_turn(
        "artifact=reports/latest-news.pdf",
        reply=reply,
        requires_attachment_delivery=True,
        required_attachment_extensions=(".pdf",),
    )

    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert delivery.attachments == ()
    assert delivery.media_directive_count == 1
    assert delivery.unavailable_attachment_count == 1
    assert "couldn't attach" in (delivery.text or "")
    assert str(wrong_artifact) not in (delivery.text or "")


def test_platform_delivery_selects_only_requested_pdf_from_mixed_media_directives(tmp_path: Path) -> None:
    from nullion.messaging_adapters import delivery_contract_for_turn, prepare_reply_for_platform_delivery

    wrong_artifact = tmp_path / "nullion-artifact-news.json"
    pdf_artifact = tmp_path / "nullion-artifact-news.pdf"
    wrong_artifact.write_text('{"title": "sidecar"}', encoding="utf-8")
    pdf_artifact.write_bytes(b"%PDF-1.4\n%e2e\n")
    reply = f"作成しました\nMEDIA:{wrong_artifact}\nMEDIA:{pdf_artifact}"
    contract = delivery_contract_for_turn(
        "artifact=reports/latest-news.pdf",
        reply=reply,
        requires_attachment_delivery=True,
        required_attachment_extensions=(".pdf",),
    )

    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert delivery.attachments == (pdf_artifact,)
    assert delivery.media_directive_count == 2
    assert delivery.unavailable_attachment_count == 1
    assert str(wrong_artifact) not in (delivery.text or "")
    assert str(pdf_artifact) not in (delivery.text or "")
    assert "MEDIA" not in (delivery.text or "")


def test_platform_delivery_attaches_structured_relative_artifact_path_for_pdf_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import messaging_adapters as adapters

    workspace_root = tmp_path / "workspace"
    artifacts_root = workspace_root / "artifacts"
    files_root = workspace_root / "files"
    media_root = workspace_root / "media"
    artifacts_root.mkdir(parents=True)
    files_root.mkdir()
    media_root.mkdir()
    pdf_artifact = artifacts_root / "hantavirus_news_report.pdf"
    pdf_artifact.write_bytes(b"%PDF-1.4\n%e2e relative artifact\n")

    monkeypatch.setattr(
        adapters,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(
            root=workspace_root,
            artifacts=artifacts_root,
            files=files_root,
            media=media_root,
        ),
    )
    monkeypatch.setattr(adapters, "artifact_root_for_principal", lambda principal_id: artifacts_root)

    reply = "PDF file: artifacts/hantavirus_news_report.pdf"
    contract = adapters.delivery_contract_for_turn(
        "Please deliver as .pdf",
        reply=reply,
        requires_attachment_delivery=True,
        required_attachment_extensions=(".pdf",),
    )
    delivery = adapters.prepare_reply_for_platform_delivery(
        reply,
        principal_id="telegram_chat",
        delivery_contract=contract,
    )

    assert contract.requires_attachment_delivery is True
    assert contract.source == "task_contract"
    assert delivery.attachments == (pdf_artifact,)
    assert delivery.unavailable_attachment_count == 0
    assert "hantavirus_news_report.pdf" not in (delivery.text or "")


def test_cron_delivery_normalizes_split_media_and_does_not_show_local_paths(tmp_path: Path) -> None:
    from nullion.cron_delivery import cron_delivery_text_from_result, scheduled_task_delivery_text
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.tools import ToolResult

    artifact = tmp_path / "nullion-artifact-latest-news.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%cron e2e\n")
    result_payload = {
        "final_text": (
            "CNN latest news PDF generated:\n"
            "MEDIA\n"
            f"{artifact}\n"
            "Internal source: /Users/example/.nullion/workspaces/workspace_admin/artifacts/cnn_latest_news.pdf"
        ),
        "tool_results": [
            ToolResult("pdf", "pdf_create", "completed", {"artifact_path": str(artifact)})
        ],
    }

    cron_text = cron_delivery_text_from_result(result_payload)
    delivery = prepare_reply_for_platform_delivery(
        scheduled_task_delivery_text(SimpleNamespace(name="CNN latest news PDF every 5 minutes"), cron_text)
    )

    assert delivery.attachments == (artifact,)
    assert delivery.media_directive_count == 1
    assert "/Users/example" not in (delivery.text or "")
    assert str(artifact) not in (delivery.text or "")
    assert "MEDIA" not in (delivery.text or "")
    assert "artifacts/cnn_latest_news.pdf" in (delivery.text or "")


def test_cron_delivery_normalizes_same_line_media_and_does_not_show_local_paths(tmp_path: Path) -> None:
    from nullion.cron_delivery import cron_delivery_text_from_result, scheduled_task_delivery_text
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    artifact = tmp_path / "nullion-artifact-report.random"
    artifact.write_bytes(b"arbitrary e2e artifact\n")
    result_payload = {
        "final_text": (
            "生成しました\n"
            f"MEDIA {artifact}\n"
            "Internal source: /Users/example/.nullion/workspaces/workspace_admin/artifacts/random.report"
        ),
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(artifact), "artifact_path": str(artifact)},
            }
        ],
    }

    cron_text = cron_delivery_text_from_result(result_payload)
    delivery = prepare_reply_for_platform_delivery(
        scheduled_task_delivery_text(SimpleNamespace(name="Random artifact report"), cron_text)
    )

    assert delivery.attachments == (artifact,)
    assert delivery.media_directive_count == 1
    assert "/Users/example" not in (delivery.text or "")
    assert str(artifact) not in (delivery.text or "")
    assert "MEDIA" not in (delivery.text or "")
    assert "artifacts/random.report" in (delivery.text or "")


def test_cron_execution_cannot_create_or_run_other_crons_during_scheduled_run() -> None:
    from nullion.tools import ToolInvocation, ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from nullion.web_app import CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS, CronExecutionToolRegistry

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            "create_cron",
            "Create cron",
            ToolRiskLevel.MEDIUM,
            ToolSideEffectClass.WRITE,
            False,
            5,
            capability_tags=("scheduler", "cron"),
        ),
        lambda invocation: None,
    )
    registry.register(
        ToolSpec(
            "run_cron",
            "Run cron",
            ToolRiskLevel.MEDIUM,
            ToolSideEffectClass.WRITE,
            False,
            5,
            capability_tags=("scheduler", "cron"),
        ),
        lambda invocation: None,
    )
    registry.register(
        ToolSpec(
            "set_reminder",
            "Set reminder",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.WRITE,
            False,
            5,
            capability_tags=("scheduler", "reminder"),
        ),
        lambda invocation: None,
    )
    registry.register(
        ToolSpec("file_read", "Read file", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
        lambda invocation: None,
    )

    cron_registry = CronExecutionToolRegistry(registry)

    assert {spec.name for spec in cron_registry.list_specs()} == {"file_read"}
    for tool_name in ("create_cron", "run_cron", "set_reminder"):
        denied = cron_registry.invoke(ToolInvocation(f"inv-{tool_name}", tool_name, "cron", {}))
        assert denied.status == "denied"
        assert denied.output["reason"] == "cron_execution_capability_denied"
        assert (
            set(denied.output["denied_capability_tags"])
            == CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS
        )


def test_runtime_and_web_builder_auto_accept_skill_learning_by_default(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion.builder import BuilderDecision, BuilderDecisionType, BuilderProposal
    from nullion.conversation_analyzer import ConversationAnalysis
    import nullion.conversation_analyzer as analyzer
    import nullion.runtime as runtime_module
    from nullion.runtime import bootstrap_persistent_runtime, run_request
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    runtime_proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Capture workflow",
        summary="Save a repeated workflow.",
        confidence=0.92,
        approval_mode="skill",
        suggested_skill_title="Recurring JSON Audio Export",
        suggested_trigger="Use when this export repeats.",
        suggested_steps=("Create JSON output", "Create audio output", "Attach both artifacts"),
        suggested_tags=("builder", "export"),
    )
    monkeypatch.delenv("NULLION_BUILDER_AUTO_APPROVE_SKILLS", raising=False)
    monkeypatch.setenv("NULLION_SKILL_LEARNING_ENABLED", "true")
    monkeypatch.setattr(
        runtime_module,
        "evaluate_builder_decision_read_model",
        lambda packet: BuilderDecision(BuilderDecisionType.SKILL_PROPOSAL, True, "workflow_pattern"),
    )
    monkeypatch.setattr(runtime_module, "build_builder_proposal_read_model", lambda decision: runtime_proposal)

    store_runtime = bootstrap_persistent_runtime(tmp_path / "runtime-builder.db")
    run_request(store_runtime.store, owner="operator", goal="repeat export", action="run", resource="workflow")

    assert [record.status for record in store_runtime.store.builder_proposals.values()] == ["accepted"]
    assert any(skill.title == "Recurring JSON Audio Export" for skill in store_runtime.store.skills.values())

    web_runtime = bootstrap_persistent_runtime(tmp_path / "web-builder.db")
    web_runtime.model_client = object()
    web_skill = ConversationAnalysis(
        title="Random Artifact Export",
        summary="Create JSON, audio, and arbitrary artifact outputs.",
        trigger="repeat random artifact export",
        steps=["Create JSON output", "Create audio output", "Create arbitrary attachment"],
        tags=["builder", "artifact"],
        confidence=0.9,
        evidence="e2e structured proposal",
    )
    monkeypatch.setattr(analyzer, "analyze_conversation", lambda *args, **kwargs: [web_skill])
    monkeypatch.setattr(analyzer, "cache_proposals", lambda conv_id, proposals: None)
    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: SimpleNamespace())

    app = create_app(web_runtime, orchestrator=None, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))
    with TestClient(app) as client:
        response = client.post("/api/chat/analyze/web:e2e-builder")

    payload = response.json()
    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["auto_accepted"] is True
    assert payload["accepted"][0]["title"] == "Random Artifact Export"
    assert any(skill.title == "Random Artifact Export" for skill in web_runtime.store.skills.values())
