from __future__ import annotations

import asyncio
import base64
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
import zipfile

import pytest

from nullion.approvals import ApprovalRequest, ApprovalStatus
from nullion.config import DiscordSettings, NullionSettings, SlackSettings, TelegramSettings
from nullion.reminders import ReminderRecord


def test_messaging_roots_and_allowed_roots_use_data_dir(tmp_path, monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))

    upload_root, scratch_root = adapters.ensure_messaging_storage_roots()
    assert upload_root == tmp_path / "uploads"
    assert scratch_root == tmp_path / "tmp" / "media"
    assert upload_root.exists()
    assert scratch_root.exists()

    roots = adapters.messaging_file_allowed_roots(tmp_path / "extra", [tmp_path / "nested", ""])
    assert upload_root.resolve() in roots
    assert scratch_root.resolve() in roots
    assert (tmp_path / "extra").resolve() in roots
    assert (tmp_path / "nested").resolve() in roots


def test_platform_chat_waits_for_deferred_artifact_turn_evidence(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator, messaging_turn_graph
    from nullion.artifacts import artifact_root_for_principal

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = artifact_root_for_principal("1")
    artifact_root.mkdir(parents=True, exist_ok=True)
    html = artifact_root / "deferred-report.html"
    html.write_text("<html><body>Deferred artifact</body></html>", encoding="utf-8")
    request_text = "Opaque artifact request 771."
    reply = f"Created `{html.name}`."

    class Store:
        def __init__(self) -> None:
            self.calls = 0

        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=1):  # noqa: ANN001
            assert conversation_id == "telegram:1"
            self.calls += 1
            if self.calls < 3:
                return []
            return [
                {
                    "user_message": request_text,
                    "assistant_reply": reply,
                    "artifact_paths": [str(html)],
                    "tool_results": [
                        {
                            "invocation_id": "scope",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {"artifact_extensions": [".html"]},
                        },
                        {
                            "invocation_id": "write",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {"artifact_path": str(html), "path": str(html)},
                        },
                    ],
                }
            ]

    store = Store()
    runtime = SimpleNamespace(store=store)
    monkeypatch.setattr(chat_operator, "handle_chat_operator_message", lambda *args, **kwargs: reply)

    response = messaging_turn_graph.run_platform_chat_request(
        runtime,
        messaging_turn_graph.PlatformChatRequest(
            platform="telegram",
            text=request_text,
            chat_id="1",
        ),
    )

    assert store.calls >= 3
    assert response.artifact_paths == [str(html)]


def test_platform_chat_uses_exact_completed_turn_when_sibling_turn_finishes_later(monkeypatch) -> None:
    from nullion import chat_operator, messaging_turn_graph

    request_text = "List every scheduled task."
    full_reply = "Here are all scheduled tasks:\n\n" + "\n\n".join(
        (
            f"{index}. Scheduled task {index}\n"
            "   Status: enabled\n"
            f"   Schedule: At {index}:00 PM every weekday\n"
            f"   Next run: Jul 14, 2026 at {index}:00 PM"
        )
        for index in range(1, 9)
    )
    partial_preview = full_reply[:600]

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=1):  # noqa: ANN001
            assert conversation_id == "telegram:1"
            assert event_type == "conversation.chat_turn"
            assert limit >= 2
            return [
                {
                    "turn_id": "telegram-update:inventory",
                    "user_message": request_text,
                    "assistant_reply": full_reply,
                    "tool_results": [],
                },
                {
                    "turn_id": "telegram-update:catalog",
                    "user_message": "Find a ranked product.",
                    "assistant_reply": "A later sibling response.",
                    "tool_results": [],
                },
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: partial_preview,
    )

    response = messaging_turn_graph.run_platform_chat_request(
        runtime,
        messaging_turn_graph.PlatformChatRequest(
            platform="telegram",
            text=request_text,
            chat_id="1",
            turn_id="telegram-update:inventory",
        ),
    )

    assert response.text is not None
    assert "8. Scheduled task 8" in response.text
    assert "Next run: Jul 14, 2026 at 8:00 PM" in response.text
    assert len(response.text or "") > len(partial_preview)


def test_platform_chat_waits_for_deferred_format_only_artifact_evidence(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator, messaging_turn_graph
    from nullion.artifacts import artifact_root_for_principal

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = artifact_root_for_principal("1")
    artifact_root.mkdir(parents=True, exist_ok=True)
    pdf = artifact_root / "generated-summary.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")
    request_text = "Create one .pdf summary."

    class Store:
        def __init__(self) -> None:
            self.calls = 0

        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=1):  # noqa: ANN001
            self.calls += 1
            if self.calls < 3:
                return []
            return [
                {
                    "user_message": request_text,
                    "assistant_reply": f"Done.\n\nMEDIA:{pdf}",
                    "artifact_paths": [str(pdf)],
                    "tool_results": [
                        {
                            "invocation_id": "pdf",
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {"artifact_path": str(pdf)},
                        }
                    ],
                }
            ]

    store = Store()
    runtime = SimpleNamespace(store=store)
    monkeypatch.setattr(chat_operator, "handle_chat_operator_message", lambda *args, **kwargs: "Done.")

    response = messaging_turn_graph.run_platform_chat_request(
        runtime,
        messaging_turn_graph.PlatformChatRequest(platform="telegram", text=request_text, chat_id="1"),
    )

    assert store.calls >= 3
    assert response.artifact_paths == [str(pdf)]


def test_messaging_ingress_ids_and_authorization(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    ingress = adapters.MessagingIngress(channel="slack", user_id="U1", text="hi", delivery_target_id="C1")
    assert ingress.operator_chat_id == "slack:U1"
    assert ingress.reminder_chat_id == "slack:C1"

    monkeypatch.setattr(adapters, "is_authorized_messaging_identity", lambda channel, user_id, settings: channel == "slack")
    assert adapters.require_authorized_ingress(ingress, NullionSettings()) is True

    monkeypatch.setattr(adapters, "resolve_messaging_user", lambda channel, user_id, settings=None: SimpleNamespace(role="member", user_id="member-1"))
    assert adapters.principal_id_for_messaging_identity("slack", "U1") == "user:member-1"
    monkeypatch.setattr(adapters, "resolve_messaging_user", lambda channel, user_id, settings=None: SimpleNamespace(role="admin", user_id="admin"))
    assert adapters.principal_id_for_messaging_identity("telegram", "1") == "telegram_chat"

    settings = NullionSettings(
        telegram=TelegramSettings(chat_enabled=False),
        slack=SlackSettings(enabled=True),
        discord=DiscordSettings(enabled=True),
    )
    assert adapters.messaging_chat_enabled("telegram", settings) is False
    assert adapters.messaging_chat_enabled("slack", settings) is True
    assert adapters.messaging_chat_enabled("discord", settings) is True
    assert adapters.messaging_chat_enabled("web", settings) is True


def test_save_messaging_chat_history_is_best_effort(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    saved: list[tuple[str, str, str, str, str]] = []

    class Store:
        def save_message(self, conversation_id, role, text, *, channel, channel_label):
            saved.append((conversation_id, role, text, channel, channel_label))

    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: Store())

    adapters.save_messaging_chat_history(adapters.MessagingIngress("discord", "D1", "hello"), "reply")

    assert saved == [
        ("discord:D1", "user", "hello", "discord:D1", "Discord · D1"),
        ("discord:D1", "bot", "reply", "discord:D1", "Discord · D1"),
    ]

    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: (_ for _ in ()).throw(RuntimeError("disk")))
    adapters.save_messaging_chat_history(adapters.MessagingIngress("discord", "D1", "hello"), "reply")


def test_decision_snapshot_and_fallbacks_are_appended(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    approval = ApprovalRequest(
        "ap-new",
        "operator",
        "execute",
        "deploy",
        ApprovalStatus.PENDING,
        datetime(2026, 1, 1, tzinfo=UTC),
    )
    reminder = ReminderRecord("rem-new", "telegram:1", "check", datetime(2026, 1, 1, 12, tzinfo=UTC))
    action = {
        "action_id": "doc-new",
        "status": "pending",
        "severity": "high",
        "summary": "Fix runtime",
        "recommendation_code": "doctor:retry_model_api",
    }

    class Store:
        def __init__(self):
            self.approvals = []
            self.actions = []
            self.reminders = []

        def list_approval_requests(self):
            return list(self.approvals)

        def list_doctor_actions(self):
            return list(self.actions)

        def list_reminders(self):
            return list(self.reminders)

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            self.runtime.store.approvals = [approval]
            self.runtime.store.actions = [action]
            self.runtime.store.reminders = [reminder]
            self.runtime.list_pending_builder_proposals = lambda: [
                SimpleNamespace(proposal_id="prop-new", proposal=SimpleNamespace(title="Proposal", summary="Change code"))
            ]
            return "Base reply"

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    reply = adapters.handle_messaging_ingress(Service(), adapters.MessagingIngress("telegram", "1", "do it"))

    assert reply.startswith("Base reply")
    assert "Approval pending" in reply
    assert "Builder suggestion" in reply
    assert "Doctor action pending" in reply
    assert "Reminder set" in reply


def test_messaging_turn_graph_uses_channel_chat_enablement(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters
    from nullion import messaging_turn_graph

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    class Runtime:
        store = SimpleNamespace(
            list_approval_requests=lambda: [],
            list_doctor_actions=lambda: [],
            list_reminders=lambda: [],
        )

        def checkpoint(self, **kwargs):
            pass

    class Service:
        runtime = Runtime()
        settings = NullionSettings(
            telegram=TelegramSettings(chat_enabled=False),
            slack=SlackSettings(enabled=True),
        )
        model_client = object()

        def handle_text_message(self, **kwargs):
            return "handler reply"

    result = messaging_turn_graph.run_messaging_turn_graph(
        Service(),
        adapters.MessagingIngress("slack", "U1", "hello"),
    )

    assert result.reply == "handler reply"


def test_messaging_turn_graph_preserves_platform_chat_id_for_shared_gateway(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters
    from nullion import messaging_turn_graph

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)
    seen_chat_ids: list[str | None] = []
    seen_platforms: list[str | None] = []

    class Runtime:
        store = SimpleNamespace(
            list_approval_requests=lambda: [],
            list_doctor_actions=lambda: [],
            list_reminders=lambda: [],
        )

        def checkpoint(self, **kwargs):
            pass

    class Service:
        runtime = Runtime()
        settings = NullionSettings(
            telegram=TelegramSettings(chat_enabled=False),
            slack=SlackSettings(enabled=True),
            discord=DiscordSettings(enabled=True),
        )
        model_client = object()

        def handle_text_message(self, **kwargs):
            seen_chat_ids.append(kwargs.get("chat_id"))
            seen_platforms.append(kwargs.get("platform"))
            return "handler reply"

    for ingress in (
        adapters.MessagingIngress("slack", "U1", "hello"),
        adapters.MessagingIngress("discord", "D1", "hello"),
    ):
        result = messaging_turn_graph.run_messaging_turn_graph(Service(), ingress)
        assert result.reply == "handler reply"

    assert seen_chat_ids == ["slack:U1", "discord:D1"]
    assert seen_platforms == ["slack", "discord"]


def test_platform_conversation_id_keeps_canonical_platform_prefixes() -> None:
    from nullion import messaging_turn_graph

    assert messaging_turn_graph.platform_conversation_id("web", "web:browser-1", "browser-1") == "web:browser-1"
    assert messaging_turn_graph.platform_conversation_id("telegram", "100001", "100001") == "telegram:100001"
    assert (
        messaging_turn_graph.platform_conversation_id(
            "telegram",
            "telegram:100001",
            "telegram:100001",
        )
        == "telegram:100001"
    )


def test_messaging_turn_graph_returns_delivery_contract(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    artifact = tmp_path / "result.anything"

    class Store:
        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return []

        def list_reminders(self):
            return []

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            return f"Finished.\nMEDIA:{artifact}"

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)
    monkeypatch.setattr(adapters, "media_candidate_paths_from_text", lambda text: [artifact])

    result = adapters.handle_messaging_ingress_result(
        Service(),
        adapters.MessagingIngress("slack", "U1", "arbitrary wording"),
    )

    assert result.reply == f"Finished.\nMEDIA:{artifact}"
    assert result.status == "reply_ready"
    assert result.delivery_contract.requires_attachment_delivery
    assert result.delivery_contract.source == "media_directive"


def test_messaging_turn_graph_preserves_structured_artifact_paths(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters
    from nullion.platform_chat import PlatformChatResponse

    artifact = tmp_path / "surface_delivery.xlsx"
    artifact.write_bytes(b"workbook")

    class Store:
        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return []

        def list_reminders(self):
            return []

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            return PlatformChatResponse(
                text="Done — attached the requested XLSX.",
                artifact_paths=[str(artifact)],
            )

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    result = adapters.handle_messaging_ingress_result(
        Service(),
        adapters.MessagingIngress("slack", "U1", "arbitrary wording"),
    )

    assert result.reply == "Done — attached the requested XLSX."
    assert result.artifact_paths == (str(artifact),)
    assert result.delivery_contract.requires_attachment_delivery
    assert result.delivery_contract.source == "task_contract"


def test_messaging_turn_graph_typed_requested_artifact_beats_discovery_cleanup(tmp_path) -> None:
    from nullion import messaging_turn_graph

    artifact = tmp_path / "stage_requested_delivery.xlsx"
    artifact.write_bytes(b"workbook")
    tool_results = [
        SimpleNamespace(
            tool_name="file_search",
            status="completed",
            output={
                "matches": [str(artifact)],
                "match_details": [{"path": str(artifact), "match_type": "filename"}],
            },
        ),
        SimpleNamespace(
            tool_name="spreadsheet_create",
            status="completed",
            output={
                "artifact_path": str(artifact),
                "artifact_paths": [str(artifact)],
                "artifact_descriptors": [
                    {
                        "path": str(artifact),
                        "extension": ".xlsx",
                        "role": "deliverable",
                    }
                ],
            },
        ),
    ]
    request_text = f"Adjunta el archivo exacto llamado {artifact.name}."

    assert messaging_turn_graph._tool_results_have_typed_deliverable_contract(tool_results)
    assert messaging_turn_graph._tool_results_have_requested_generated_artifact_path(
        tool_results,
        request_text=request_text,
    )
    assert not messaging_turn_graph._tool_results_are_local_discovery_or_sidecar(
        tool_results,
        request_text=request_text,
        stored_reply=f"Done.\nMEDIA:{artifact}",
        delivery_text=f"Done.\nMEDIA:{artifact}",
    )
    assert messaging_turn_graph._filter_local_discovery_artifact_paths(
        [str(artifact)],
        tool_results,
        request_text=request_text,
        stored_reply=f"Done.\nMEDIA:{artifact}",
    ) == [str(artifact)]


def test_messaging_turn_graph_filters_support_sidecars_from_structural_package_contract(tmp_path) -> None:
    from nullion import messaging_turn_graph

    docx_path = tmp_path / "stage_mixed_embedded_media_pack_report.docx"
    xlsx_path = tmp_path / "stage_mixed_embedded_media_pack_workbook.xlsx"
    csv_path = tmp_path / "stage_mixed_embedded_media_pack_export.csv"
    pdf_path = tmp_path / "stage_mixed_embedded_media_pack_summary.pdf"
    html_path = tmp_path / "stage_mixed_embedded_media_pack_report.html"
    json_path = tmp_path / "stage_mixed_embedded_media_pack_attachment.json"
    text_path = tmp_path / "stage_mixed_embedded_media_pack_attachment.txt"
    paths = [str(path) for path in (docx_path, xlsx_path, csv_path, pdf_path, html_path, json_path, text_path)]
    tool_results = [
        SimpleNamespace(tool_name="document_create", status="completed", output={"artifact_path": str(docx_path)}),
        SimpleNamespace(tool_name="terminal_exec", status="completed", output={"artifact_paths": [str(xlsx_path)]}),
        SimpleNamespace(tool_name="file_write", status="completed", output={"path": str(csv_path)}),
        SimpleNamespace(tool_name="pdf_create", status="completed", output={"artifact_path": str(pdf_path)}),
        SimpleNamespace(tool_name="file_write", status="completed", output={"path": str(html_path)}),
        SimpleNamespace(tool_name="file_write", status="completed", output={"path": str(json_path)}),
        SimpleNamespace(tool_name="file_write", status="completed", output={"path": str(text_path)}),
    ]

    assert messaging_turn_graph._filter_artifact_paths_for_tool_scope_contract(paths, tool_results) == [
        str(docx_path),
        str(xlsx_path),
        str(csv_path),
        str(pdf_path),
        str(html_path),
    ]


def test_messaging_turn_graph_readds_final_reply_media_without_sidecars(tmp_path) -> None:
    from nullion import messaging_turn_graph

    xlsx_path = tmp_path / "STAGE_SHEET_QA_cafe_popup_comparison.xlsx"
    csv_path = tmp_path / "STAGE_SHEET_QA_cafe_popup_comparison.csv"
    pdf_path = tmp_path / "STAGE_SHEET_QA_cafe_popup_executive_summary.pdf"
    json_path = tmp_path / "STAGE_SHEET_QA_cafe_popup_status.json"
    xlsx_path.write_bytes(b"PK\x03\x04 workbook")
    csv_path.write_text("name,value\nSTAGE SHEET QA,1\n", encoding="utf-8")
    pdf_path.write_bytes(b"%PDF-1.4\n")
    json_path.write_text("{}", encoding="utf-8")
    reply = (
        "Done — attached the requested files.\n\n"
        f"MEDIA:{xlsx_path}\n"
        f"MEDIA:{csv_path}\n"
        f"MEDIA:{pdf_path}\n"
        f"MEDIA:{json_path}"
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=1):  # noqa: ANN001
            assert conversation_id == "web:sheet"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "user_message": "Create the workbook package.",
                    "assistant_reply": reply,
                    "tool_results": [
                        {
                            "invocation_id": "scope",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {"artifact_extensions": [".xlsx", ".pdf"]},
                        },
                        {
                            "invocation_id": "xlsx",
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {"artifact_path": str(xlsx_path), "artifact_paths": [str(xlsx_path)]},
                        },
                        {
                            "invocation_id": "csv",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {"path": str(csv_path), "artifact_path": str(csv_path)},
                        },
                        {
                            "invocation_id": "pdf",
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {"artifact_path": str(pdf_path), "artifact_paths": [str(pdf_path)]},
                        },
                        {
                            "invocation_id": "json",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {"path": str(json_path), "artifact_path": str(json_path)},
                        },
                    ],
                }
            ]

    _reply, artifact_paths, _tool_results, _raw_paths = messaging_turn_graph._latest_stored_turn_delivery_evidence(
        SimpleNamespace(store=Store()),
        "web:sheet",
        request_text="Create the workbook package.",
    )

    assert artifact_paths == [str(xlsx_path), str(pdf_path), str(csv_path)]
    assert str(json_path) not in artifact_paths


def test_messaging_turn_graph_scoped_artifact_contract_ignores_source_filenames(tmp_path) -> None:
    from nullion import messaging_turn_graph

    workbook = tmp_path / "stage_artifact_followup_retry_guard.xlsx"
    helper_image = tmp_path / "alpha-sun.png"
    source_zip = tmp_path / "field-notes-images.zip"
    workbook.write_bytes(b"PK\x03\x04 workbook")
    helper_image.write_bytes(b"\x89PNG\r\n\x1a\n")
    source_zip.write_bytes(b"PK\x03\x04 zip")
    reply = (
        "Done - attached the requested files.\n\n"
        f"MEDIA:{workbook}\n"
        f"MEDIA:{helper_image}"
    )
    request_text = (
        f"Create a new XLSX workbook named {workbook.name}. "
        "For example alpha-sun.png becomes nus-ahpla. "
        f"Attach only {workbook.name}. ZIP path: {source_zip}"
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=1):  # noqa: ANN001
            assert conversation_id == "web:sheet"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "user_message": request_text,
                    "assistant_reply": reply,
                    "tool_results": [
                        {
                            "invocation_id": "scope",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {"artifact_extensions": [".xlsx"]},
                        },
                        {
                            "invocation_id": "xlsx",
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(workbook),
                                "artifact_paths": [str(workbook)],
                                "artifact_descriptors": [
                                    {
                                        "path": str(workbook),
                                        "extension": ".xlsx",
                                        "role": "deliverable",
                                    }
                                ],
                            },
                        },
                        {
                            "invocation_id": "helper",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {"path": str(helper_image), "artifact_path": str(helper_image)},
                        },
                    ],
                }
            ]

    _reply, artifact_paths, _tool_results, _raw_paths = messaging_turn_graph._latest_stored_turn_delivery_evidence(
        SimpleNamespace(store=Store()),
        "web:sheet",
        request_text=request_text,
    )

    assert artifact_paths == [str(workbook)]


def test_messaging_turn_graph_drops_visual_helper_html_when_final_html_exists(tmp_path) -> None:
    from nullion import messaging_turn_graph

    final_html = tmp_path / "stage_mixed_embedded_media_pack.html"
    visual_html = tmp_path / "stage_mixed_embedded_media_pack_visual.html"
    final_html.write_text("<html><body>" + ("Final report " * 20) + "</body></html>", encoding="utf-8")
    visual_html.write_text("<html><body>" + ("Visual helper " * 20) + "</body></html>", encoding="utf-8")

    assert messaging_turn_graph._filter_sparse_duplicate_html_artifacts(
        [str(final_html), str(visual_html)]
    ) == [str(final_html)]


def test_messaging_turn_graph_does_not_gate_chat_on_model_api_issues(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters
    from nullion.config import NullionSettings, TelegramSettings

    class Store:
        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return [
                {
                    "action_id": "act-1",
                    "owner": "system",
                    "status": "pending",
                    "action_type": "runtime",
                    "recommendation_code": "model_api_unreachable",
                    "summary": "Model API is unreachable",
                    "severity": "high",
                    "reason": None,
                    "error": None,
                }
            ]

        def list_reminders(self):
            return []

    class Service:
        def __init__(self) -> None:
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])
            self.settings = NullionSettings(telegram=TelegramSettings(chat_enabled=True))
            self.handle_text_message_called = False

        def handle_text_message(self, **kwargs):
            self.handle_text_message_called = True
            return "normal path handles failure if the model actually fails"

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    service = Service()
    result = adapters.handle_messaging_ingress_result(
        service,
        adapters.MessagingIngress("slack", "U1", "quick update"),
    )

    assert service.handle_text_message_called is True
    assert result.reply == "normal path handles failure if the model actually fails"


def test_messaging_turn_graph_forwards_structured_dispatch_decision(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    captured: dict[str, object] = {}
    decision = TurnDispatchDecision(
        policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
        dependency_turn_ids=("telegram-update:email",),
        disposition=ConversationTurnDisposition.CONTINUE,
        reason="model_structured_follow_up",
        target_active_turn_index=0,
    )

    class Store:
        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return []

        def list_reminders(self):
            return []

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            captured["turn_dispatch_decision"] = kwargs.get("turn_dispatch_decision")
            return "Structured dispatch forwarded."

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    result = adapters.handle_messaging_ingress_result(
        Service(),
        adapters.MessagingIngress("slack", "U1", "¿Eso requiere atención ahora?"),
        turn_dispatch_decision=decision,
    )

    assert result.reply == "Structured dispatch forwarded."
    assert captured["turn_dispatch_decision"] is decision


def test_messaging_turn_graph_ignores_text_only_file_verbs(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    class Store:
        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return []

        def list_reminders(self):
            return []

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            return "Here is the summary in chat."

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    result = adapters.handle_messaging_ingress_result(
        Service(),
        adapters.MessagingIngress("slack", "U1", "send that as a text file"),
    )

    assert result.status == "reply_ready"
    assert not result.delivery_contract.requires_attachment_delivery
    assert result.delivery_contract.source == "message"


def test_messaging_turn_graph_uses_active_task_frame_attachment_contract(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters
    from nullion.task_frames import (
        TaskFrame,
        TaskFrameExecutionContract,
        TaskFrameFinishCriteria,
        TaskFrameOperation,
        TaskFrameOutputContract,
        TaskFrameStatus,
        TaskFrameTarget,
    )

    frame = TaskFrame(
        frame_id="frame-1",
        conversation_id="slack:U1",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.ACTIVE,
        operation=TaskFrameOperation.GENERATE_ARTIFACT,
        target=TaskFrameTarget(kind="file", value="brief"),
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(artifact_kind="pdf", delivery_mode="attachment"),
        finish=TaskFrameFinishCriteria(requires_artifact_delivery=True, required_artifact_kind="pdf"),
        summary="Create brief",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    class Store:
        def get_active_task_frame_id(self, conversation_id):
            return "frame-1" if conversation_id == "slack:U1" else None

        def get_task_frame(self, frame_id):
            return frame if frame_id == "frame-1" else None

        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return []

        def list_reminders(self):
            return []

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            return "I couldn't attach the requested file. The task is still open."

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    result = adapters.handle_messaging_ingress_result(
        Service(),
        adapters.MessagingIngress("slack", "U1", "and those columns sounds good"),
    )

    assert result.delivery_contract.requires_attachment_delivery
    assert result.delivery_contract.source == "task_frame"
    assert result.delivery_contract.required_attachment_extensions == (".pdf",)

    waiting_frame = replace(frame, status=TaskFrameStatus.WAITING_INPUT)
    class WaitingStore(Store):
        def get_task_frame(self, frame_id):
            return waiting_frame if frame_id == "frame-1" else None

    waiting_contract = adapters.delivery_contract_for_runtime_turn(
        SimpleNamespace(store=WaitingStore()),
        "slack:U1",
        "and those columns sounds good",
        reply="Question: Which destination should I use?",
    )
    assert waiting_contract.requires_attachment_delivery is False


def test_delivery_contract_does_not_require_attachment_for_reply_path_text() -> None:
    from nullion import messaging_adapters as adapters

    contract = adapters.delivery_contract_for_turn(
        "deliver as .pdf",
        reply="PDF file: artifacts/hantavirus_news_report.pdf",
    )

    assert contract.requires_attachment_delivery is False
    assert contract.allow_attachment_delivery is False
    assert contract.allow_plain_path_attachment_delivery is False
    assert contract.source == "message"


def test_delivery_contract_preserves_repeated_required_attachment_extensions() -> None:
    from nullion import messaging_adapters as adapters

    contract = adapters.delivery_contract_for_turn(
        "Create a poster PDF, memo PDF, and HTML version.",
        artifact_paths=["/tmp/poster.pdf", "/tmp/poster.html"],
        requires_attachment_delivery=True,
        required_attachment_extensions=(".pdf", ".pdf", ".html"),
    )

    assert contract.requires_attachment_delivery
    assert contract.required_attachment_extensions == (".pdf", ".pdf", ".html")


def test_delivery_contract_keeps_produced_formats_when_prompt_mentions_supporting_png(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    workspace_root = tmp_path / "workspace"
    artifact_root = workspace_root / "artifacts"
    artifact_root.mkdir(parents=True)
    monkeypatch.setenv("NULLION_WORKSPACE_ROOT", str(workspace_root))
    artifact_paths = [
        artifact_root / "report.docx",
        artifact_root / "workbook.xlsx",
        artifact_root / "export.csv",
        artifact_root / "summary.pdf",
        artifact_root / "report.html",
    ]
    for path in artifact_paths:
        if path.suffix == ".html":
            path.write_text(
                "<html><body><h1>Artifact package</h1><p>"
                + ("Verified report content. " * 20)
                + "</p></body></html>",
                encoding="utf-8",
            )
        else:
            path.write_bytes(b"artifact")

    contract = adapters.delivery_contract_for_turn(
        "Create the package without attaching screenshots.",
        artifact_paths=[str(path) for path in artifact_paths],
        requires_attachment_delivery=True,
    )
    delivery = adapters.prepare_reply_for_platform_delivery(
        "Done.",
        delivery_contract=contract,
        artifact_paths=[str(path) for path in artifact_paths],
    )

    assert {".docx", ".xlsx", ".csv", ".pdf", ".html"} <= set(contract.required_attachment_extensions)
    assert delivery.attachments == tuple(artifact_paths)
    assert delivery.unavailable_attachment_count == 0


def test_prepare_reply_for_platform_delivery_sanitizes_and_reports_missing_attachments(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    attachment = tmp_path / "report.txt"
    attachment.write_text("hello", encoding="utf-8")
    monkeypatch.setattr(adapters, "media_candidate_paths_from_text", lambda text: [attachment])
    monkeypatch.setattr(adapters, "split_reply_for_platform_delivery", lambda text, principal_id=None: ("caption", (attachment,)))

    delivery = adapters.prepare_reply_for_platform_delivery(
        "<b>caption</b> [file]",
        delivery_contract=adapters.DeliveryContract.attachment_required(source="test"),
    )
    assert delivery.text == "caption"
    assert delivery.attachments == (attachment,)
    assert delivery.requires_attachment_delivery
    assert delivery.has_deliverable_attachments

    monkeypatch.setattr(adapters, "split_reply_for_platform_delivery", lambda text, principal_id=None: ("caption", ()))
    missing = adapters.prepare_reply_for_platform_delivery(
        "<b>caption</b> [file]",
        delivery_contract=adapters.DeliveryContract.attachment_required(source="test"),
    )
    assert missing.text == "I couldn't attach the requested file."
    assert missing.unavailable_attachment_count == 1

    assert adapters.prepare_reply_for_platform_delivery(None).text is None
    assert adapters.platform_delivery_failure_reply() == "I couldn't upload the requested attachment to this platform. I won't mark it delivered."


def test_prepare_reply_for_platform_delivery_attaches_explicit_media_temp_file(tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    report = tmp_path / "cron-contract-report.json"
    report.write_text('{"status":"ready"}', encoding="utf-8")

    delivery = adapters.prepare_reply_for_platform_delivery(
        f"⏰ **SCHEDULED TASK: Cron contract file-only**\n\nMEDIA:{report}",
        principal_id="telegram:contract-chat",
    )

    assert delivery.text == "⏰ **SCHEDULED TASK: Cron contract file-only**"
    assert delivery.attachments == (report,)
    assert delivery.media_directive_count == 1
    assert delivery.unavailable_attachment_count == 0

    text_and_file = adapters.prepare_reply_for_platform_delivery(
        f"⏰ **SCHEDULED TASK: Cron contract text-and-file**\n\nStatus ready\nMEDIA:{report}",
        principal_id="telegram:contract-chat",
    )
    assert text_and_file.text == "⏰ **SCHEDULED TASK: Cron contract text-and-file**\n\nStatus ready"
    assert text_and_file.attachments == (report,)


def test_prepare_reply_for_platform_delivery_rejects_explicit_media_script(tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    script = tmp_path / "build_nullion_report.py"
    script.write_text("print('internal helper')\n", encoding="utf-8")

    delivery = adapters.prepare_reply_for_platform_delivery(
        f"Helper script\nMEDIA:{script}",
        principal_id="telegram:contract-chat",
    )

    assert delivery.attachments == ()
    assert delivery.text == "I couldn't attach the requested file."
    assert delivery.unavailable_attachment_count == 1


def test_prepare_reply_for_platform_delivery_confirms_large_attachment_batches(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    attachments = tuple((tmp_path / f"report-{index}.pdf") for index in range(5))
    for path in attachments:
        path.write_bytes(b"%PDF-1.4\n")
    monkeypatch.setenv("NULLION_MAX_AUTOMATIC_ATTACHMENT_BATCH", "3")
    monkeypatch.setattr(adapters, "media_candidate_paths_from_text", lambda text: list(attachments))
    monkeypatch.setattr(
        adapters,
        "split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Attached requested files.", attachments),
    )

    delivery = adapters.prepare_reply_for_platform_delivery(
        "\n".join(f"MEDIA:{path}" for path in attachments),
        delivery_contract=adapters.DeliveryContract.attachment_required(source="task_frame"),
    )

    assert delivery.attachments == ()
    assert delivery.media_directive_count == 5
    assert delivery.pending_attachment_count == 5
    assert delivery.requires_attachment_delivery
    assert delivery.text is not None
    assert "I prepared **5 files**" in delivery.text
    assert "up to 3 files" in delivery.text
    assert "Reply with one option:" not in delivery.text


def test_prepare_reply_for_platform_delivery_confirms_oversized_attachment_before_upload(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    attachment = tmp_path / "large-report.xlsx"
    attachment.write_bytes(b"x" * 128)
    monkeypatch.setattr(adapters, "media_candidate_paths_from_text", lambda text: [attachment])
    monkeypatch.setattr(
        adapters,
        "split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Attached requested file.", (attachment,)),
    )

    delivery = adapters.prepare_reply_for_platform_delivery(
        f"MEDIA:{attachment}",
        delivery_contract=adapters.DeliveryContract.attachment_required(source="task_frame"),
        max_attachment_bytes=64,
    )

    assert delivery.attachments == ()
    assert delivery.pending_attachment_count == 1
    assert delivery.requires_attachment_delivery
    assert delivery.text is not None
    assert "attachment upload limit" in delivery.text
    assert "large-report.xlsx" in delivery.text
    assert "Reply with one option:" in delivery.text
    assert "1. Try a compressed archive" in delivery.text
    assert "2. Split into compressed parts" in delivery.text
    assert "3. Set up a cloud storage connector" in delivery.text
    assert "4. Log into the cloud storage web app" in delivery.text


def test_prepare_reply_for_platform_delivery_does_not_mark_contract_filtered_extra_as_unavailable(
    tmp_path, monkeypatch,
) -> None:
    from nullion import messaging_adapters as adapters

    pdf = tmp_path / "packet.pdf"
    docx = tmp_path / "packet.docx"
    extra = tmp_path / "extra.xlsx"
    pdf.write_bytes(b"%PDF-1.4\n")
    docx.write_bytes(b"PK\x03\x04")
    extra.write_bytes(b"PK\x03\x04")
    monkeypatch.setattr(
        adapters,
        "_is_deliverable_messaging_attachment",
        lambda path, principal_id=None, allow_explicit_media_path=False: True,
    )

    delivery = adapters.prepare_reply_for_platform_delivery(
        f"Done.\nMEDIA:{pdf}\nMEDIA:{docx}\nMEDIA:{extra}",
        delivery_contract=adapters.DeliveryContract.attachment_required(
            source="structured_artifact_request",
            required_attachment_extensions=(".pdf", ".docx"),
        ),
    )

    assert delivery.attachments == (pdf, docx)
    assert delivery.unavailable_attachment_count == 0
    assert "unavailable" not in (delivery.text or "").lower()


def test_prepare_reply_for_platform_delivery_names_files_for_generic_package_caption(
    tmp_path, monkeypatch,
) -> None:
    from nullion import messaging_adapters as adapters

    pdf = tmp_path / "space_gardeners_playbook.pdf"
    docx = tmp_path / "space_gardeners_playbook.docx"
    pdf.write_bytes(b"%PDF-1.4\n")
    docx.write_bytes(b"PK\x03\x04")
    monkeypatch.setattr(
        adapters,
        "_is_deliverable_messaging_attachment",
        lambda path, principal_id=None, allow_explicit_media_path=False: True,
    )

    delivery = adapters.prepare_reply_for_platform_delivery(
        f"Done — attached the requested files.\n\nMEDIA:{pdf}\nMEDIA:{docx}",
        delivery_contract=adapters.DeliveryContract.attachment_required(
            source="structured_artifact_request",
            required_attachment_extensions=(".pdf", ".docx"),
        ),
    )

    assert delivery.attachments == (pdf, docx)
    assert delivery.text is not None
    assert "Attached files:" in delivery.text
    assert "space_gardeners_playbook.pdf" in delivery.text
    assert "space_gardeners_playbook.docx" in delivery.text
    assert str(tmp_path) not in delivery.text


def test_prepare_reply_for_platform_delivery_names_file_for_single_generic_caption(
    tmp_path, monkeypatch,
) -> None:
    from nullion import messaging_adapters as adapters

    pdf = tmp_path / "stage_followup_reuse_summary.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")
    monkeypatch.setattr(
        adapters,
        "_is_deliverable_messaging_attachment",
        lambda path, principal_id=None, allow_explicit_media_path=False: True,
    )

    delivery = adapters.prepare_reply_for_platform_delivery(
        f"Attached the requested file.\n\nMEDIA:{pdf}",
        delivery_contract=adapters.DeliveryContract.attachment_required(
            source="structured_artifact_request",
            required_attachment_extensions=(".pdf",),
        ),
    )

    assert delivery.attachments == (pdf,)
    assert delivery.text is not None
    assert "Attached file:" in delivery.text
    assert "stage_followup_reuse_summary.pdf" in delivery.text
    assert str(tmp_path) not in delivery.text


def test_prepare_reply_for_platform_delivery_keeps_meaningful_filename_bullet(
    tmp_path, monkeypatch,
) -> None:
    from nullion import messaging_adapters as adapters

    workbook = tmp_path / "stage_archive_transform_field_notes.xlsx"
    workbook.write_bytes(b"PK\x03\x04 workbook")
    monkeypatch.setattr(
        adapters,
        "_is_deliverable_messaging_attachment",
        lambda path, principal_id=None, allow_explicit_media_path=False: True,
    )

    delivery = adapters.prepare_reply_for_platform_delivery(
        "Created the requested spreadsheet:\n"
        f"- `{workbook.name}`\n\n"
        f"MEDIA:{workbook}",
        delivery_contract=adapters.DeliveryContract.attachment_required(
            source="structured_artifact_request",
            required_attachment_extensions=(".xlsx",),
        ),
    )

    assert delivery.attachments == (workbook,)
    assert delivery.text == f"Created the requested spreadsheet:\n- `{workbook.name}`"


def test_prepare_reply_for_platform_delivery_suppresses_unrequested_data_sidecars(
    tmp_path, monkeypatch,
) -> None:
    from nullion import messaging_adapters as adapters

    workbook = tmp_path / "stage_archive_manifest_rebuilt.xlsx"
    csv_sidecar = tmp_path / "stage_archive_manifest_rebuilt.csv"
    json_sidecar = tmp_path / "stage_archive_manifest_rebuilt.json"
    workbook.write_bytes(b"PK\x03\x04 workbook")
    csv_sidecar.write_text("name,size\n", encoding="utf-8")
    json_sidecar.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        adapters,
        "_is_deliverable_messaging_attachment",
        lambda path, principal_id=None, allow_explicit_media_path=False: True,
    )

    delivery = adapters.prepare_reply_for_platform_delivery(
        "Done — attached the requested artifact package:\n\n"
        f"MEDIA:{workbook}\n"
        f"MEDIA:{csv_sidecar}\n"
        f"MEDIA:{json_sidecar}",
        delivery_contract=adapters.DeliveryContract.attachment_required(
            source="structured_artifact_request",
            required_attachment_extensions=(".xlsx",),
        ),
    )

    assert delivery.attachments == (workbook,)
    assert csv_sidecar.name not in (delivery.text or "")
    assert json_sidecar.name not in (delivery.text or "")


def test_prepare_reply_for_platform_delivery_recovers_listed_artifact_filename(
    tmp_path, monkeypatch,
) -> None:
    from nullion import messaging_adapters as adapters

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    report = artifact_root / "museum_report.docx"
    summary = artifact_root / "museum_visual_summary.pdf"
    report.write_bytes(b"PK\x03\x04")
    summary.write_bytes(b"%PDF-1.4\n")

    delivery = adapters.prepare_reply_for_platform_delivery(
        "Done — attached the requested artifact package:\n\n"
        "- museum_report.docx\n"
        "- museum_visual_summary.pdf\n\n"
        f"MEDIA:{report}",
        principal_id="telegram:100001",
        delivery_contract=adapters.DeliveryContract.attachment_required(
            source="media_directive",
            required_attachment_extensions=(".docx", ".pdf"),
        ),
    )

    assert delivery.attachments == (report, summary)
    assert delivery.unavailable_attachment_count == 0
    assert "museum_visual_summary.pdf" in (delivery.text or "")


def test_prepare_reply_for_platform_delivery_uses_structured_artifact_paths(tmp_path, monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    artifact = artifact_root / "structured_delivery.xlsx"
    artifact.write_bytes(b"workbook")
    contract = adapters.delivery_contract_for_turn(
        "create the workbook",
        reply="Done — attached the requested XLSX.",
        artifact_paths=[str(artifact)],
        requires_attachment_delivery=True,
    )

    delivery = adapters.prepare_reply_for_platform_delivery(
        "Done — attached the requested XLSX.",
        delivery_contract=contract,
        artifact_paths=[str(artifact)],
    )

    assert delivery.attachments == (artifact,)
    assert delivery.unavailable_attachment_count == 0
    assert "MEDIA:" not in (delivery.text or "")


def test_delivery_contract_does_not_require_unrequested_artifact_paths(tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    attachment = tmp_path / "incidental.pdf"
    attachment.write_bytes(b"%PDF-1.4\n")
    contract = adapters.delivery_contract_for_turn(None, reply="Finished.", artifact_paths=[str(attachment)])
    delivery = adapters.prepare_reply_for_platform_delivery("Finished.", delivery_contract=contract)

    assert delivery.text == "Finished."
    assert delivery.attachments == ()
    assert delivery.unavailable_attachment_count == 0
    assert not delivery.requires_attachment_delivery


def test_prepare_reply_for_platform_delivery_resolves_relative_artifact_paths(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    workspace_artifacts = tmp_path / "artifacts"
    workspace_files = tmp_path / "files"
    workspace_media = tmp_path / "media"
    workspace_artifacts.mkdir()
    workspace_files.mkdir()
    workspace_media.mkdir()
    pdf_path = workspace_artifacts / "hantavirus_news_report.pdf"
    pdf_path.write_text("pdf-bytes", encoding="utf-8")

    monkeypatch.setattr(
        adapters,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(
            artifacts=workspace_artifacts,
            files=workspace_files,
            media=workspace_media,
        ),
    )
    monkeypatch.setattr(adapters, "artifact_root_for_principal", lambda principal_id: workspace_artifacts)

    delivery = adapters.prepare_reply_for_platform_delivery(
        "PDF file: artifacts/hantavirus_news_report.pdf",
        principal_id="telegram_chat",
        delivery_contract=adapters.DeliveryContract.attachment_required(
            source="task_frame",
            allow_plain_paths=True,
            required_attachment_extensions=(".pdf",),
        ),
    )

    assert delivery.attachments == (pdf_path,)
    assert delivery.requires_attachment_delivery is True
    assert delivery.text is not None
    assert "hantavirus_news_report.pdf" not in delivery.text


def test_platform_delivery_hides_local_paths_for_text_only_messages() -> None:
    from nullion import messaging_adapters as adapters

    delivery = adapters.prepare_reply_for_platform_delivery(
        "Found artifact:\n"
        "- /Users/example/.nullion/workspaces/workspace_admin/artifacts/nullion_tracker_report_2026-05-03.html"
    )

    assert delivery.attachments == ()
    assert delivery.text is not None
    assert "/Users/example" not in delivery.text
    assert "artifacts/nullion_tracker_report_2026-05-03.html" in delivery.text


def test_platform_delivery_hides_local_paths_in_attachment_captions(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    attachment = tmp_path / "report.html"
    attachment.write_text("<html></html>", encoding="utf-8")
    monkeypatch.setattr(adapters, "media_candidate_paths_from_text", lambda text: [attachment])
    monkeypatch.setattr(
        adapters,
        "split_reply_for_platform_delivery",
        lambda text, principal_id=None: (
            "Found artifact: /Users/example/.nullion/workspaces/workspace_admin/artifacts/report.html",
            (attachment,),
        ),
    )

    delivery = adapters.prepare_reply_for_platform_delivery(f"MEDIA:{attachment}")

    assert delivery.attachments == (attachment,)
    assert delivery.text == "Found artifact: artifacts/report.html"


def test_platform_delivery_suppresses_images_already_embedded_in_primary_artifacts(tmp_path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    embedded_image = artifact_root / "source-image.png"
    embedded_image.write_bytes(b"\x89PNG\r\n\x1a\nembedded-image-bytes")
    standalone_image = artifact_root / "standalone-image.png"
    standalone_image.write_bytes(b"\x89PNG\r\n\x1a\nstandalone-image-bytes")
    report = artifact_root / "report.docx"
    with zipfile.ZipFile(report, "w") as archive:
        archive.writestr("word/media/image1.png", embedded_image.read_bytes())

    reply = "\n".join(
        [
            "Report ready.",
            f"MEDIA:{embedded_image}",
            f"MEDIA:{standalone_image}",
            f"MEDIA:{report}",
        ]
    )

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.attachments == (standalone_image, report)
    assert delivery.unavailable_attachment_count == 0


def test_platform_delivery_suppresses_images_referenced_by_html_artifact(tmp_path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    svg = artifact_root / "chart.svg"
    svg.write_text("<svg><text>ok</text></svg>", encoding="utf-8")
    png = artifact_root / "chart.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\nchart-png")
    html = artifact_root / "report.html"
    html.write_text(
        "<html><body>"
        f"<img src='{svg.name}'>"
        f"<img src='data:image/png;base64,{base64.b64encode(png.read_bytes()).decode()}'>"
        "</body></html>",
        encoding="utf-8",
    )

    reply = "\n".join(["Report ready.", f"MEDIA:{svg}", f"MEDIA:{png}", f"MEDIA:{html}"])

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.attachments == (html,)
    assert delivery.unavailable_attachment_count == 0


def test_filtered_artifact_reply_drops_stale_suppressed_svg_text(tmp_path, monkeypatch) -> None:
    from nullion.messaging_turn_graph import _strip_unselected_artifact_filename_lines

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    html = artifact_root / "train_day_trips.html"
    html.write_text("<html><body><img src='data:image/jpeg;base64,abc'></body></html>", encoding="utf-8")
    helper_svg = artifact_root / "train_day_trips_attachment.svg"
    helper_svg.write_text("<svg><text>helper</text></svg>", encoding="utf-8")
    text = (
        "Created the requested HTML file and .svg file:\n"
        f"- {html.name}\n"
        f"- {helper_svg.name}"
    )

    cleaned = _strip_unselected_artifact_filename_lines(
        text,
        selected_paths=[str(html)],
        candidate_paths=[str(html), str(helper_svg)],
    )

    assert cleaned is None


def test_stored_turn_evidence_keeps_suppressed_file_write_candidates_for_text_scrub(tmp_path) -> None:
    from nullion.messaging_turn_graph import (
        _latest_stored_turn_delivery_evidence,
        _strip_unselected_artifact_filename_lines,
    )
    from nullion.tools import ToolResult

    html = tmp_path / "train_accessible_outdoor_day_trips.html"
    helper_svg = tmp_path / "train_accessible_day_trips_summary.svg"
    html.write_text("<html><body><img src='data:image/png;base64,AAAA'></body></html>", encoding="utf-8")
    helper_svg.write_text("<svg><text>helper</text></svg>", encoding="utf-8")
    reply = (
        "Created the requested HTML file and.svg file:\n"
        f"- `{html.name}`\n"
        f"- `{helper_svg.name}`"
    )
    tool_results = [
        ToolResult(
            "scope",
            "request_tool_scope",
            "completed",
            {
                "artifact_extensions": [".html"],
                "embedded_media_artifact_extensions": [".html"],
            },
        ),
        ToolResult("html", "file_write", "completed", {"path": str(html), "artifact_path": str(html)}),
        ToolResult("svg-scope", "request_tool_scope", "completed", {"artifact_extensions": [".svg"]}),
        ToolResult("svg", "file_write", "completed", {"path": str(helper_svg), "artifact_path": str(helper_svg)}),
    ]
    event = {
        "user_message": "Create one self-contained HTML report.",
        "assistant_reply": reply,
        "tool_results": [
            {
                "invocation_id": result.invocation_id,
                "tool_name": result.tool_name,
                "status": result.status,
                "output": result.output,
                "error": result.error,
            }
            for result in tool_results
        ],
    }
    runtime = SimpleNamespace(
        list_recent_conversation_events=lambda conversation_id, event_type, limit: [event]
    )

    stored_reply, selected_paths, _stored_tool_results, raw_paths = _latest_stored_turn_delivery_evidence(
        runtime,
        "telegram:1",
        request_text="Create one self-contained HTML report.",
    )

    assert stored_reply == reply
    assert selected_paths == [str(html)]
    assert str(helper_svg) in raw_paths
    assert _strip_unselected_artifact_filename_lines(
        stored_reply,
        selected_paths=selected_paths,
        candidate_paths=raw_paths,
    ) is None


def test_platform_delivery_suppresses_loose_images_in_multi_primary_artifact_bundle(tmp_path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    image = artifact_root / "generated-summary.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\ngenerated-summary")
    pdf = artifact_root / "report.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")
    workbook = artifact_root / "report.xlsx"
    with zipfile.ZipFile(workbook, "w") as archive:
        archive.writestr("xl/workbook.xml", "<workbook/>")

    reply = "\n".join(["Bundle ready.", f"MEDIA:{image}", f"MEDIA:{pdf}", f"MEDIA:{workbook}"])

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.attachments == (pdf, workbook)
    assert delivery.unavailable_attachment_count == 0


def test_platform_delivery_suppresses_text_marker_in_primary_asset_folder(tmp_path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    asset_root = artifact_root / "random_website_linked_assets"
    asset_root.mkdir(parents=True)
    marker = asset_root / "placeholder.txt"
    marker.write_text("assets folder for linked HTML", encoding="utf-8")
    html = artifact_root / "random_website_report_linked.html"
    html.write_text("<html><body><img src='random_website_linked_assets/image.png'></body></html>", encoding="utf-8")
    workbook = artifact_root / "random_website_workbook.xlsx"
    with zipfile.ZipFile(workbook, "w") as archive:
        archive.writestr("xl/workbook.xml", "<workbook/>")

    reply = "\n".join(["Bundle ready.", f"MEDIA:{marker}", f"MEDIA:{html}", f"MEDIA:{workbook}"])

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.attachments == (html, workbook)
    assert delivery.unavailable_attachment_count == 0


def test_platform_delivery_includes_linked_html_sibling_image(tmp_path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    image = artifact_root / "space_explorer_badge.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\nbadge")
    html = artifact_root / "space_explorers_linked_page.html"
    html.write_text(
        "<!doctype html><html><body><img src='space_explorer_badge.png'></body></html>",
        encoding="utf-8",
    )

    reply = "\n".join(["Linked page ready.", f"MEDIA:{html}"])

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.attachments == (html, image)
    assert delivery.unavailable_attachment_count == 0


def test_platform_delivery_receipts_are_durable_and_contract_aware(tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    attachment = tmp_path / "report.txt"
    attachment.write_text("hello", encoding="utf-8")
    delivery = adapters.PlatformDelivery(
        text="Attached.",
        attachments=(attachment,),
        media_directive_count=1,
    )

    receipt = adapters.build_platform_delivery_receipt(
        channel="telegram",
        target_id="123",
        delivery=delivery,
        transport_ok=True,
        request_id="req-1",
        message_id="msg-1",
    )
    assert receipt.status == "succeeded"
    assert receipt.attachment_required is True
    assert receipt.attachment_count == 1
    assert receipt.delivered_text == "Attached."
    assert receipt.attachment_names == ("report.txt",)
    assert receipt.to_record()["attachment_names"] == ["report.txt"]

    missing = adapters.PlatformDelivery(
        text="I couldn't attach it.",
        attachments=(),
        media_directive_count=1,
        unavailable_attachment_count=1,
    )
    missing_receipt = adapters.build_platform_delivery_receipt(
        channel="slack",
        target_id="C1",
        delivery=missing,
        transport_ok=True,
        error="artifact_unavailable",
    )
    assert missing_receipt.status == "failed"
    assert missing_receipt.text_delivered is True
    assert missing_receipt.unavailable_attachment_count == 1
    assert missing_receipt.delivered_text == "I couldn't attach it."
    assert missing_receipt.attachment_names == ()

    pending = adapters.PlatformDelivery(
        text="Choose a smaller batch.",
        attachments=(),
        media_directive_count=8,
        pending_attachment_count=8,
    )
    pending_receipt = adapters.build_platform_delivery_receipt(
        channel="telegram",
        target_id="123",
        delivery=pending,
        transport_ok=True,
    )
    assert pending_receipt.status == "pending_confirmation"
    assert pending_receipt.attachment_required is True
    assert pending_receipt.pending_attachment_count == 8

    receipt_path = tmp_path / "receipts.jsonl"
    assert adapters.record_platform_delivery_receipt(receipt, path=receipt_path) is True
    assert adapters.record_platform_delivery_receipt(missing_receipt, path=receipt_path) is True
    line = receipt_path.read_text(encoding="utf-8").strip()
    assert '"channel": "telegram"' in line
    assert '"status": "succeeded"' in line
    failed = adapters.list_platform_delivery_receipts(path=receipt_path, status="failed")
    assert [record["channel"] for record in failed] == ["slack"]
    all_receipts = adapters.list_platform_delivery_receipts(path=receipt_path, status=None)
    assert [record["channel"] for record in all_receipts] == ["slack", "telegram"]


@pytest.mark.asyncio
async def test_execute_platform_reply_delivery_centralizes_attachment_success_and_failure(tmp_path, monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    attachment = tmp_path / "report.pdf"
    attachment.write_bytes(b"%PDF")
    delivery = adapters.PlatformDelivery(text="Attached report.", attachments=(attachment,), media_directive_count=1)
    recorded: list[adapters.PlatformDeliveryReceipt] = []
    monkeypatch.setattr(adapters, "prepare_reply_for_platform_delivery", lambda *args, **kwargs: delivery)
    monkeypatch.setattr(adapters, "record_platform_delivery_receipt", lambda receipt, **kwargs: recorded.append(receipt) or True)

    sent_text: list[str] = []
    uploaded: list[tuple[str | None, tuple[object, ...]]] = []

    async def send_text(text: str) -> bool:
        sent_text.append(text)
        return True

    async def send_attachments(initial_text: str | None, paths: tuple[object, ...]) -> bool:
        uploaded.append((initial_text, paths))
        return True

    result = await adapters.execute_platform_reply_delivery(
        "ignored",
        channel="slack",
        target_id="C1",
        request_id="req",
        message_id="msg",
        send_text=send_text,
        send_attachments=send_attachments,
    )

    assert result.ok is True
    assert result.attachments_sent is True
    assert sent_text == []
    assert uploaded == [("Attached report.", (attachment,))]
    assert recorded[-1].status == "succeeded"
    assert recorded[-1].attachment_count == 1

    async def fail_attachments(initial_text: str | None, paths: tuple[object, ...]) -> bool:
        uploaded.append((initial_text, paths))
        return False

    failure = await adapters.execute_platform_reply_delivery(
        "ignored",
        channel="discord",
        target_id="D1",
        send_text=send_text,
        send_attachments=fail_attachments,
    )

    assert failure.ok is False
    assert failure.fallback_text_sent is True
    assert sent_text[-1] == adapters.platform_delivery_failure_reply(delivery)
    assert recorded[-1].status == "failed"
    assert recorded[-1].error == "attachment_upload_failed"


@pytest.mark.asyncio
async def test_execute_platform_reply_delivery_records_pre_sent_streaming_text(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    delivery = adapters.PlatformDelivery(text="Already streamed.", attachments=())
    recorded: list[adapters.PlatformDeliveryReceipt] = []
    monkeypatch.setattr(adapters, "prepare_reply_for_platform_delivery", lambda *args, **kwargs: delivery)
    monkeypatch.setattr(adapters, "record_platform_delivery_receipt", lambda receipt, **kwargs: recorded.append(receipt) or True)

    async def send_text(_text: str) -> bool:
        raise AssertionError("pre-sent text should not be delivered twice")

    result = await adapters.execute_platform_reply_delivery(
        "ignored",
        channel="discord",
        target_id="D1",
        reply_already_sent=True,
        send_text=send_text,
    )

    assert result.ok is True
    assert result.text_sent is False
    assert recorded[-1].status == "succeeded"
    assert recorded[-1].text_delivered is True


def test_retry_messaging_delivery_operation_retries_async_and_stops_on_non_retryable(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(adapters.asyncio, "sleep", fake_sleep)

    attempts = {"count": 0}

    class TimeoutErrorForTest(Exception):
        pass

    async def run_success():
        def operation():
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise TimeoutErrorForTest("timeout")
            return "ok"

        return await adapters.retry_messaging_delivery_operation(operation, attempts=3, retry_delay_seconds=0.1)

    assert asyncio.run(run_success()) == "ok"
    assert sleeps == [0.1]

    with pytest.raises(ValueError):
        asyncio.run(adapters.retry_messaging_delivery_operation(lambda: (_ for _ in ()).throw(ValueError("bad")), attempts=3))

    class ResponseError(Exception):
        status_code = 503

    assert adapters.is_retryable_messaging_delivery_error(ResponseError()) is True
    assert adapters.is_retryable_messaging_delivery_error(ValueError("bad")) is False


def test_retry_messaging_delivery_operation_honors_retry_after(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(adapters.asyncio, "sleep", fake_sleep)

    attempts = {"count": 0}

    class RetryAfterForTest(Exception):
        retry_after = 7

    async def run_success():
        def operation():
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise RetryAfterForTest("rate limited")
            return "ok"

        return await adapters.retry_messaging_delivery_operation(operation, attempts=3, retry_delay_seconds=0.1)

    assert asyncio.run(run_success()) == "ok"
    assert sleeps == [7]


def test_split_reply_for_platform_and_save_attachment(tmp_path, monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    chunks = adapters.split_reply_for_platform("alpha beta gamma", limit=8)
    assert chunks == ["alpha", "beta", "gamma"]
    assert adapters.split_reply_for_platform(None, limit=8) == []
    assert adapters.split_reply_for_platform("   ", limit=8) == []

    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    monkeypatch.setattr(adapters, "ensure_messaging_storage_roots", lambda: (upload_dir, tmp_path / "scratch"))

    saved = adapters.save_messaging_attachment(filename="../note.txt", data=b"hello")
    assert saved["name"] == "note.txt"
    assert (upload_dir / "note.txt").read_bytes() == b"hello"

    saved_again = adapters.save_messaging_attachment(filename="../note.txt", data=b"again")
    assert saved_again["name"] != "note.txt"
    assert adapters.save_messaging_attachment(filename="x", data=b"", max_bytes=1) is None
    assert adapters.save_messaging_attachment(filename="x", data=b"too large", max_bytes=1) is None


@pytest.mark.asyncio
async def test_slack_delivery_retries_plain_text_after_formatted_send_failure() -> None:
    from nullion import slack_app

    class FormattingFailureSlackClient:
        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def chat_postMessage(self, **kwargs):
            self.messages.append(kwargs)
            if len(self.messages) == 1:
                raise RuntimeError("formatted send failed")

    client = FormattingFailureSlackClient()

    await slack_app._post_slack_message_with_plain_fallback(
        client,
        channel="C1",
        formatted_text="Hello *Ada*",
        plain_text="Hello **Ada**",
    )

    assert client.messages[0]["text"] == "Hello *Ada*"
    assert client.messages[1]["mrkdwn"] is False
    assert "```text\nHello **Ada**\n```" in client.messages[1]["text"]


@pytest.mark.asyncio
async def test_discord_delivery_retries_plain_text_after_send_failure() -> None:
    from nullion import discord_app

    class FormattingFailureChannel:
        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def send(self, *args, **kwargs):
            if args:
                kwargs["content"] = args[0]
            self.messages.append(kwargs)
            if len(self.messages) == 1:
                raise RuntimeError("formatted send failed")

    channel = FormattingFailureChannel()

    await discord_app._send_discord_chunks_with_plain_fallback(channel, "Hello **Ada**")

    assert channel.messages[0]["content"] == "Hello **Ada**"
    assert "```text\nHello **Ada**\n```" in channel.messages[1]["content"]
