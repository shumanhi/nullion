from __future__ import annotations

import base64
from pathlib import Path
from types import SimpleNamespace

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_descriptors_for_paths,
    is_safe_artifact_path,
    media_candidate_paths_from_text,
    split_media_reply_attachments,
)
from nullion.chat_attachments import (
    ChatAttachment,
    attachment_processing_failure_reply,
    audio_transcription_satisfied,
    chat_attachment_content_blocks,
    has_unavailable_chat_attachments,
    is_supported_chat_file,
    normalize_chat_attachments,
    unavailable_chat_attachment_count,
)
from nullion.messaging_adapters import (
    delivery_contract_for_turn,
    prepare_reply_for_platform_delivery,
    text_or_attachments_expect_attachment_delivery,
)


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


def test_chat_attachment_normalization_filters_missing_files_and_inlines_images(tmp_path) -> None:
    image = tmp_path / "photo.png"
    image.write_bytes(PNG_1X1)

    attachments = normalize_chat_attachments(
        [
            {"name": "../photo.png", "path": str(image), "media_type": "image/png"},
            {"name": "missing.wav", "path": str(tmp_path / "missing.wav"), "media_type": "audio/wav"},
        ]
    )

    assert attachments == [ChatAttachment(name="photo.png", path=str(image), media_type="image/png")]
    blocks = chat_attachment_content_blocks("describe this", attachments)
    assert blocks[0] == {"type": "text", "text": "describe this"}
    assert blocks[1]["type"] == "text"
    assert blocks[2]["type"] == "image"
    assert blocks[2]["source"]["media_type"] == "image/png"
    assert base64.b64decode(blocks[2]["source"]["data"]) == PNG_1X1


def test_audio_and_generic_attachments_include_exact_path_guidance(tmp_path) -> None:
    audio = tmp_path / "voice.wav"
    audio.write_bytes(b"RIFF....WAVE")
    doc = tmp_path / "notes.txt"
    doc.write_text("hello", encoding="utf-8")

    blocks = chat_attachment_content_blocks(
        "",
        [
            ChatAttachment(name="voice.wav", path=str(audio), media_type="audio/wav"),
            ChatAttachment(name="notes.txt", path=str(doc), media_type="text/plain"),
        ],
    )

    joined = "\n".join(block["text"] for block in blocks if block["type"] == "text")
    assert f"at {audio}" in joined
    assert "Use audio_transcribe with this exact path" in joined
    assert f"at {doc}" in joined
    assert "Do not ask the user to upload it again" in joined


def test_agent_attachment_blocks_cover_common_file_kinds_and_instructions(tmp_path) -> None:
    samples = {
        "notes.txt": (b"meeting notes", "text/plain"),
        "table.csv": (b"name,value\nalpha,1\n", "text/csv"),
        "payload.json": (b'{"alpha": 1}', "application/json"),
        "brief.pdf": (b"%PDF-1.4\n%test\n", "application/pdf"),
        "photo.png": (PNG_1X1, "image/png"),
        "voice.ogg": (b"OggS audio", "audio/ogg"),
        "clip.mp4": (b"\x00\x00\x00\x18ftypmp42", "video/mp4"),
    }
    raw_attachments = []
    for name, (content, media_type) in samples.items():
        path = tmp_path / name
        path.write_bytes(content)
        raw_attachments.append({"name": name, "path": str(path), "media_type": media_type})

    attachments = normalize_chat_attachments(raw_attachments)
    blocks = chat_attachment_content_blocks("Read these and summarize the important details.", attachments)
    text = "\n".join(block["text"] for block in blocks if block["type"] == "text")

    assert blocks[0] == {"type": "text", "text": "Read these and summarize the important details."}
    for name in samples:
        assert f"Attached file: {name}" in text
        assert f"at {tmp_path / name}" in text
    assert "Use audio_transcribe with this exact path" in text
    assert "This is a video attachment. Use the configured video-capable model/provider for analysis" in text
    assert "This file is already uploaded and available at the exact local path above" in text
    assert "Do not ask the user to upload it again" in text

    image_blocks = [block for block in blocks if block["type"] == "image"]
    assert len(image_blocks) == 1
    assert image_blocks[0]["source"]["media_type"] == "image/png"
    assert base64.b64decode(image_blocks[0]["source"]["data"]) == PNG_1X1


def test_unavailable_attachment_candidates_are_visible_to_agent_context() -> None:
    raw_attachments = [
        {
            "name": "New folder (2).zip",
            "path": "",
            "media_type": "application/zip",
            "source_kind": "document",
            "download_status": "download_failed",
            "error": "download_failed",
            "attempts": "3",
        }
    ]

    blocks = chat_attachment_content_blocks("Create a spreadsheet from the attached file.", [], raw_attachments=raw_attachments)
    text = "\n".join(block["text"] for block in blocks if block["type"] == "text")

    assert has_unavailable_chat_attachments(raw_attachments) is True
    assert unavailable_chat_attachment_count(raw_attachments) == 1
    assert "New folder (2).zip" in text
    assert "No workspace file path exists for this attachment" in text
    assert "Do not ask the user for the filename" in text


def test_audio_attachment_processing_contract_requires_transcription(tmp_path) -> None:
    audio = tmp_path / "voice.ogg"
    audio.write_bytes(b"audio")
    attachments = [ChatAttachment(name="voice.ogg", path=str(audio), media_type="audio/ogg")]

    assert attachment_processing_failure_reply("transcribe this", attachments, []) is not None

    completed = type(
        "ToolResultDouble",
        (),
        {
            "tool_name": "audio_transcribe",
            "status": "completed",
            "output": {"text": "hello world"},
        },
    )()
    assert audio_transcription_satisfied([completed]) is True
    assert attachment_processing_failure_reply("transcribe this", attachments, [completed]) is None


def test_messaging_platform_ingress_forwards_readable_attachments_to_agent_service(tmp_path, monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    sample_paths = []
    for name, content, media_type in [
        ("note.txt", b"alpha text", "text/plain"),
        ("table.csv", b"name,value\nbeta,2\n", "text/csv"),
        ("payload.json", b'{"gamma": 3}', "application/json"),
        ("brief.pdf", b"%PDF-1.4\n%test\n", "application/pdf"),
    ]:
        path = tmp_path / name
        path.write_bytes(content)
        sample_paths.append({"name": name, "path": str(path), "media_type": media_type})

    class Store:
        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return []

        def list_reminders(self):
            return []

        def get_active_task_frame_id(self, conversation_id):
            return None

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])
            self.calls = []

        def handle_text_message(self, **kwargs):
            attachments = list(kwargs["attachments"])
            self.calls.append(kwargs)
            summaries = []
            for attachment in attachments:
                path = Path(attachment["path"])
                assert path.is_file()
                summaries.append(f"{attachment['name']}:{path.read_bytes()[:8]!r}")
            return f"processed {kwargs['chat_id']} -> " + ", ".join(summaries)

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    for channel, user_id in [("telegram", "123"), ("slack", "U1"), ("discord", "D1")]:
        service = Service()
        result = adapters.handle_messaging_ingress_result(
            service,
            adapters.MessagingIngress(
                channel,
                user_id,
                "Summarize the attached files.",
                attachments=tuple(sample_paths),
                delivery_target_id=f"{channel}-target",
            ),
        )

        assert result.status == "reply_ready"
        assert f"processed {channel}:{user_id}" in (result.reply or "")
        assert "note.txt:b'alpha te'" in (result.reply or "")
        assert "brief.pdf:b'%PDF-1.4'" in (result.reply or "")
        assert service.calls[-1]["text"] == "Summarize the attached files."
        assert service.calls[-1]["chat_id"] == f"{channel}:{user_id}"
        assert service.calls[-1]["reminder_chat_id"] == f"{channel}:{channel}-target"
        assert service.calls[-1]["attachments"] == sample_paths


def test_web_chat_forwards_uploaded_attachment_metadata_to_turn_runner(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    upload = tmp_path / "web-note.txt"
    upload.write_text("web upload content", encoding="utf-8")
    attachments = [{"name": upload.name, "path": str(upload), "media_type": "text/plain"}]
    captured = {}

    def fake_run_platform_chat_request(runtime, request):  # noqa: ANN001
        captured["user_text"] = request.text
        captured["conv_id"] = request.conversation_id
        captured["attachments"] = request.attachments
        path = Path(captured["attachments"][0]["path"])
        return platform_chat.PlatformChatResponse(
            text=f"read {path.name}: {path.read_text(encoding='utf-8')}",
        )

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(runtime, orchestrator=object(), registry=ToolRegistry())

    response = TestClient(app).post(
        "/api/chat",
        json={
            "text": "Read the attachment and tell me what it says.",
            "conversation_id": "web:attachments",
            "attachments": attachments,
        },
    )

    assert response.status_code == 200
    assert response.json()["text"] == "read web-note.txt: web upload content"
    assert captured == {
        "user_text": "Read the attachment and tell me what it says.",
        "conv_id": "web:attachments",
        "attachments": attachments,
    }


def test_supported_chat_file_accepts_named_or_typed_uploads() -> None:
    assert is_supported_chat_file(filename="report.pdf")
    assert is_supported_chat_file(filename="", media_type="image/png")
    assert not is_supported_chat_file(filename="", media_type="")


def test_artifact_descriptors_only_expose_safe_downloadable_files(tmp_path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    html = root / "nullion-artifact-report.html"
    html.write_text("<html><body>" + ("report" * 20) + "</body></html>", encoding="utf-8")
    script = root / "nullion-artifact-tool.py"
    script.write_text("print('no')", encoding="utf-8")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")

    descriptor = artifact_descriptor_for_path(html, artifact_root=root)

    assert descriptor is not None
    assert descriptor.name == html.name
    assert artifact_descriptor_for_path(script, artifact_root=root) is None
    assert artifact_descriptor_for_path(outside, artifact_root=root) is None
    assert artifact_descriptors_for_paths([str(html), str(script), str(outside)], artifact_root=root) == [descriptor]


def test_artifact_descriptor_rejects_symlink_escape_and_deleted_files(tmp_path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    outside = tmp_path / "outside-report.txt"
    outside.write_text("outside", encoding="utf-8")
    escaped = root / "nullion-artifact-escape.txt"
    escaped.symlink_to(outside)
    deleted = root / "nullion-artifact-deleted.txt"
    deleted.write_text("deleted", encoding="utf-8")
    deleted.unlink()

    assert artifact_descriptor_for_path(escaped, artifact_root=root) is None
    assert artifact_descriptor_for_path(deleted, artifact_root=root) is None
    assert artifact_descriptors_for_paths([str(escaped), str(deleted)], artifact_root=root) == []


def test_temp_nullion_artifact_prefix_is_not_deliverable_without_artifact_root() -> None:
    temp_artifact = Path("/tmp/nullion-artifact-report.pdf")

    assert is_safe_artifact_path(temp_artifact) is False
    assert is_safe_artifact_path(temp_artifact, artifact_root=Path("/tmp")) is True


def test_media_directives_split_caption_and_existing_safe_attachments(tmp_path) -> None:
    artifact = tmp_path / "nullion-artifact-report.html"
    artifact.write_text("<html><body>" + ("report" * 20) + "</body></html>", encoding="utf-8")
    missing = tmp_path / "nullion-artifact-missing.html"
    reply = f"Here you go\n- MEDIA:{artifact}\nARTIFACT:{missing}"

    caption, paths = split_media_reply_attachments(
        reply,
        is_safe_attachment_path=lambda path: path.name.startswith("nullion-artifact-"),
    )

    assert caption == "Here you go\nAttachment unavailable: nullion-artifact-missing.html"
    assert paths == (artifact,)
    assert media_candidate_paths_from_text(reply) == [artifact, missing]


def test_media_directives_accept_same_line_path_directive_without_prompt_words(tmp_path) -> None:
    artifact = tmp_path / "nullion-artifact-report.bin"
    artifact.write_bytes(b"arbitrary artifact")
    reply = f"完了\n- MEDIA {artifact}"

    caption, paths = split_media_reply_attachments(
        reply,
        is_safe_attachment_path=lambda path: path.name.startswith("nullion-artifact-"),
    )

    assert caption == "完了"
    assert paths == (artifact,)
    assert media_candidate_paths_from_text(reply) == [artifact]


def test_platform_delivery_accepts_artifact_directive_for_workspace_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    workspace_file = tmp_path / "workspaces" / "workspace_admin" / "files" / "cnn_headlines.txt"
    workspace_file.parent.mkdir(parents=True)
    workspace_file.write_text("headline", encoding="utf-8")

    delivery = prepare_reply_for_platform_delivery(
        f"I already have that file ready.\n\nARTIFACT:{workspace_file}",
        principal_id="telegram_chat",
    )

    assert delivery.attachments == (workspace_file,)
    assert "ARTIFACT:" not in (delivery.text or "")
    assert str(workspace_file) not in (delivery.text or "")


def test_platform_delivery_accepts_relative_workspace_artifact_media_directive(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "cnn_latest_news_2026-05-04_0950.pdf"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")

    delivery = prepare_reply_for_platform_delivery(
        "⏰ Scheduled task: CNN latest news PDF every 5 minutes\n\n"
        "CNN latest news PDF generated.\n\n"
        "MEDIA artifacts/cnn_latest_news_2026-05-04_0950.pdf",
        principal_id="telegram_chat",
    )

    assert delivery.attachments == (artifact,)
    assert delivery.text == (
        "⏰ **SCHEDULED TASK: CNN latest news PDF every 5 minutes**\n\n"
        "CNN latest news PDF generated."
    )
    assert delivery.media_directive_count == 1
    assert "MEDIA" not in (delivery.text or "")
    assert "artifacts/cnn_latest_news_2026-05-04_0950.pdf" not in (delivery.text or "")


def test_platform_delivery_removes_attached_plain_path_from_media_caption(tmp_path) -> None:
    artifact = tmp_path / ".nullion-artifacts" / "nullion-artifact-yahoo-screenshot.png"
    artifact.parent.mkdir()
    artifact.write_bytes(b"\x89PNG\r\n\x1a\n")
    reply = f"Here's the screenshot of yahoo.com:\n\n{artifact}\n\nMEDIA:{artifact}"

    delivery = prepare_reply_for_platform_delivery(reply, allow_attachments=True)

    assert delivery.attachments == (artifact,)
    assert delivery.text == "Here's the screenshot of yahoo.com:"
    assert str(artifact) not in (delivery.text or "")


def test_platform_delivery_handles_same_line_media_path_directive(tmp_path) -> None:
    artifact = tmp_path / ".nullion-artifacts" / "nullion-artifact-latest-news.pdf"
    artifact.parent.mkdir()
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")

    delivery = prepare_reply_for_platform_delivery(f"生成しました\nMEDIA {artifact}")

    assert delivery.attachments == (artifact,)
    assert delivery.text == "生成しました"
    assert delivery.media_directive_count == 1
    assert str(artifact) not in (delivery.text or "")
    assert "MEDIA" not in (delivery.text or "")


def test_platform_delivery_strips_internal_builder_marker_and_backticked_attachment_path(tmp_path) -> None:
    artifact = tmp_path / ".nullion-artifacts" / "nullion-artifact-cnn.pdf"
    artifact.parent.mkdir()
    artifact.write_bytes(b"%PDF-1.4\n")
    reply = (
        "Scheduled task: Hourly CNN latest news PDF\n\n"
        "CNN latest news PDF created:\n\n"
        f"`{artifact}`\n\n"
        f"MEDIA:{artifact}\n\n"
        "::builder-skill::Create news summary PDF"
    )

    delivery = prepare_reply_for_platform_delivery(reply, allow_attachments=True)

    assert delivery.attachments == (artifact,)
    assert delivery.text == "⏰ **SCHEDULED TASK: Hourly CNN latest news PDF**\n\nCNN latest news PDF created:"
    assert str(artifact) not in (delivery.text or "")
    assert "::builder-skill::" not in (delivery.text or "")
    assert "``" not in (delivery.text or "")


def test_platform_delivery_only_attaches_when_requested(tmp_path, monkeypatch) -> None:
    artifact = tmp_path / "nullion-artifact-report.txt"
    artifact.write_text("artifact", encoding="utf-8")
    reply = f"Done.\nMEDIA:{artifact}"

    monkeypatch.setattr("nullion.messaging_adapters.media_candidate_paths_from_text", lambda text: [artifact])
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done.", (artifact,)),
    )

    suppressed = prepare_reply_for_platform_delivery(reply, allow_attachments=False)
    assert suppressed.text == "Done."
    assert suppressed.attachments == ()
    assert not suppressed.requires_attachment_delivery

    delivered = prepare_reply_for_platform_delivery(reply, allow_attachments=True)
    assert delivered.attachments == (artifact,)

    contracted = prepare_reply_for_platform_delivery(reply)
    assert contracted.attachments == (artifact,)


def test_platform_delivery_attaches_plain_artifact_path_when_requested(tmp_path, monkeypatch) -> None:
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True, exist_ok=True)
    artifact = artifact_root / "report.txt"
    artifact.write_text("artifact", encoding="utf-8")

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))

    reply = f"Himan, the text file is here:\n\n{artifact}\n\nAttachment/artifact link:"
    delivery = prepare_reply_for_platform_delivery(reply, allow_attachments=True)

    assert delivery.attachments == (artifact,)
    assert str(artifact) not in (delivery.text or "")
    assert "text file is here" in (delivery.text or "")


def test_chat_artifact_appender_attaches_named_workspace_artifact_without_english_prompt(
    tmp_path,
    monkeypatch,
) -> None:
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import _append_chat_artifacts_to_reply
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    artifact_root = artifact_root_for_principal("telegram:100001")
    artifact_root.mkdir(parents=True, exist_ok=True)
    artifact = artifact_root / "nullion_bug_tracker_2026-05-08.xlsx"
    artifact.write_bytes(b"PK\x03\x04xlsx bytes")

    reply = "添付: nullion_bug_tracker_2026-05-08.xlsx"

    rendered = _append_chat_artifacts_to_reply(
        runtime,
        reply=reply,
        artifact_paths=[],
        prompt="添付 42",
        principal_id="telegram:100001",
        tool_results=[],
    )

    assert rendered == f"Done — attached the requested spreadsheet.\n\nMEDIA:{artifact}"


def test_delivery_contract_ignores_text_only_file_delivery_heuristics() -> None:
    assert not text_or_attachments_expect_attachment_delivery(
        "please handle this",
        attachments=[{"name": "upload.bin", "path": "/tmp/upload.bin", "media_type": "application/octet-stream"}],
    )
    assert not text_or_attachments_expect_attachment_delivery("please handle this", attachments=[])
    assert not delivery_contract_for_turn("send that as a PDF file").requires_attachment_delivery
    assert not delivery_contract_for_turn("where is the file").requires_attachment_delivery
    assert not delivery_contract_for_turn("generate a peacock").requires_attachment_delivery


def test_delivery_contract_uses_structured_attachment_contract_not_user_verbs(tmp_path, monkeypatch) -> None:
    artifact = tmp_path / "nullion-artifact-arbitrary.weird"
    artifact.write_text("artifact", encoding="utf-8")
    reply = f"Finished.\nMEDIA:{artifact}"

    monkeypatch.setattr("nullion.messaging_adapters.media_candidate_paths_from_text", lambda text: [artifact])
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Finished.", (artifact,)),
    )

    contract = delivery_contract_for_turn(
        "frobnicate the attachment however tomorrow says it",
        reply=reply,
        requires_attachment_delivery=True,
    )
    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert contract.requires_attachment_delivery
    assert contract.source == "task_contract"
    assert delivery.attachments == (artifact,)


def test_delivery_contract_keeps_unrequested_artifact_paths_internal(tmp_path, monkeypatch) -> None:
    artifact = tmp_path / "peak_design_shipment_update.pdf"
    artifact.write_bytes(b"%PDF-1.4\n")
    reply = f"I created the PDF attachment successfully:\nPDF: {artifact.name}"

    contract = delivery_contract_for_turn(None, reply=reply, artifact_paths=[str(artifact)])
    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert contract.requires_attachment_delivery is False
    assert contract.source == "message"
    assert delivery.attachments == ()
    assert delivery.unavailable_attachment_count == 0
    assert "couldn't attach" not in (delivery.text or "")


def test_platform_delivery_rejects_wrong_requested_attachment_format(tmp_path, monkeypatch) -> None:
    html_artifact = tmp_path / "nullion-artifact-report.html"
    html_artifact.write_text("<html><body>report</body></html>", encoding="utf-8")
    reply = f"Done -- attached the requested PDF.\nMEDIA:{html_artifact}"

    monkeypatch.setattr("nullion.messaging_adapters.media_candidate_paths_from_text", lambda text: [html_artifact])
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done -- attached the requested PDF.", (html_artifact,)),
    )

    contract = delivery_contract_for_turn("send this as report.pdf", reply=reply)
    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert contract.requires_attachment_delivery
    assert contract.required_attachment_extensions == (".pdf",)
    assert delivery.attachments == ()
    assert delivery.unavailable_attachment_count == 1
    assert "couldn't attach" in (delivery.text or "")


def test_platform_delivery_selects_matching_requested_attachment_format(tmp_path, monkeypatch) -> None:
    html_artifact = tmp_path / "nullion-artifact-report.html"
    pdf_artifact = tmp_path / "nullion-artifact-report.pdf"
    html_artifact.write_text("<html><body>report</body></html>", encoding="utf-8")
    pdf_artifact.write_bytes(b"%PDF-1.4\n%test\n")
    reply = f"Done -- attached the requested PDF.\nMEDIA:{html_artifact}\nMEDIA:{pdf_artifact}"

    monkeypatch.setattr(
        "nullion.messaging_adapters.media_candidate_paths_from_text",
        lambda text: [html_artifact, pdf_artifact],
    )
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done -- attached the requested PDF.", (html_artifact, pdf_artifact)),
    )

    contract = delivery_contract_for_turn("send this as report.pdf", reply=reply)
    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert delivery.attachments == (pdf_artifact,)


def test_platform_delivery_keeps_all_explicit_mixed_requested_formats(tmp_path, monkeypatch) -> None:
    artifacts = [
        tmp_path / "launch_catalog.xlsx",
        tmp_path / "launch_report.pdf",
        tmp_path / "launch_memo.docx",
        tmp_path / "launch_deck.pptx",
    ]
    for artifact in artifacts:
        artifact.write_bytes(b"artifact")
    reply = "\n".join(f"MEDIA:{artifact}" for artifact in artifacts)

    monkeypatch.setattr(
        "nullion.messaging_adapters.media_candidate_paths_from_text",
        lambda text: artifacts,
    )
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done.", tuple(artifacts)),
    )

    contract = delivery_contract_for_turn(
        "Create launch_catalog.xlsx, launch_report.pdf, launch_memo.docx, and launch_deck.pptx.",
        reply=reply,
    )
    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert contract.required_attachment_extensions == (".xlsx", ".pdf", ".docx", ".pptx")
    assert delivery.attachments == tuple(artifacts)
    assert delivery.unavailable_attachment_count == 0


def test_platform_delivery_compacts_long_multi_artifact_caption(tmp_path, monkeypatch) -> None:
    artifacts = [
        tmp_path / "random_website_artifact_stress_set.docx",
        tmp_path / "random_website_artifact_stress_set.pptx",
        tmp_path / "random_website_artifact_stress_set.xlsx",
        tmp_path / "random_website_artifact_stress_set.csv",
        tmp_path / "random_website_artifact_stress_set.pdf",
        tmp_path / "random_website_artifact_stress_set_self_contained.html",
        tmp_path / "random_website_artifact_stress_set_linked.html",
    ]
    for artifact in artifacts:
        artifact.write_bytes(b"artifact")
    reply = (
        "Manual scheduled task run complete.\n\n"
        "Verification status\n"
        "All expected deliverables exist and are non-empty.\n\n"
        "Artifact files created\n"
        + "\n".join(f"- `{artifact.name}`" for artifact in artifacts)
        + "\n\n"
        + "\n".join(f"MEDIA:{artifact}" for artifact in artifacts)
    )

    monkeypatch.setattr(
        "nullion.messaging_adapters.media_candidate_paths_from_text",
        lambda text: artifacts,
    )
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: (reply, tuple(artifacts)),
    )

    delivery = prepare_reply_for_platform_delivery(
        reply,
        delivery_contract=delivery_contract_for_turn(None, reply=reply),
    )

    assert delivery.attachments == tuple(artifacts)
    assert "MEDIA:" not in (delivery.text or "")
    assert "Artifact files created" not in (delivery.text or "")
    assert artifacts[0].name not in (delivery.text or "")
    assert "Verification status" in (delivery.text or "")
    assert len(delivery.text or "") < 260


def test_platform_delivery_summarizes_long_generic_attachment_names(tmp_path, monkeypatch) -> None:
    artifacts = [
        tmp_path / "very_long_report_filename_that_should_not_wrap_badly.docx",
        tmp_path / "very_long_report_filename_that_should_not_wrap_badly.xlsx",
        tmp_path / "very_long_report_filename_that_should_not_wrap_badly.pdf",
        tmp_path / "very_long_report_filename_that_should_not_wrap_badly.html",
    ]
    for artifact in artifacts:
        artifact.write_bytes(b"artifact")
    reply = "\n".join(f"MEDIA:{artifact}" for artifact in artifacts)

    monkeypatch.setattr(
        "nullion.messaging_adapters.media_candidate_paths_from_text",
        lambda text: artifacts,
    )
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done — attached the requested files.", tuple(artifacts)),
    )

    delivery = prepare_reply_for_platform_delivery(
        reply,
        delivery_contract=delivery_contract_for_turn(None, reply=reply),
    )

    assert delivery.attachments == tuple(artifacts)
    assert delivery.text == "Done — attached the requested files.\n\nAttached 4 files."


def test_platform_delivery_reports_partial_attachment_availability(tmp_path, monkeypatch) -> None:
    txt_artifact = tmp_path / "nullion-artifact-report.txt"
    pdf_artifact = tmp_path / "nullion-artifact-report.pdf"
    txt_artifact.write_text("report", encoding="utf-8")
    pdf_artifact.write_bytes(b"%PDF-1.4\n%test\n")
    missing = tmp_path / "nullion-artifact-missing.pdf"
    reply = f"Done.\nMEDIA:{txt_artifact}\nMEDIA:{pdf_artifact}\nMEDIA:{missing}"

    monkeypatch.setattr(
        "nullion.messaging_adapters.media_candidate_paths_from_text",
        lambda text: [txt_artifact, pdf_artifact, missing],
    )
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done.", (txt_artifact, pdf_artifact)),
    )

    delivery = prepare_reply_for_platform_delivery(reply, allow_attachments=True)

    assert delivery.attachments == (txt_artifact, pdf_artifact)
    assert delivery.unavailable_attachment_count == 1
    assert "1 attachment was unavailable" in (delivery.text or "")


def test_platform_delivery_keeps_unrelated_image_assets_when_report_exists(tmp_path, monkeypatch) -> None:
    workbook = tmp_path / "topaz_jewelry_styles.xlsx"
    image_a = tmp_path / "01_topaz_ring.png"
    image_b = tmp_path / "02_topaz_earrings.png"
    workbook.write_bytes(b"PK\x03\x04xlsx bytes")
    image_a.write_bytes(PNG_1X1)
    image_b.write_bytes(PNG_1X1)
    reply = f"Done.\nMEDIA:{image_a}\nMEDIA:{image_b}\nMEDIA:{workbook}"

    monkeypatch.setattr(
        "nullion.messaging_adapters.media_candidate_paths_from_text",
        lambda text: [image_a, image_b, workbook],
    )
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done.", (image_a, image_b, workbook)),
    )

    delivery = prepare_reply_for_platform_delivery(reply, allow_attachments=True)

    assert delivery.attachments == (image_a, image_b, workbook)
    assert delivery.unavailable_attachment_count == 0


def test_platform_delivery_collapses_related_report_image_sidecars(tmp_path, monkeypatch) -> None:
    report = tmp_path / "provincetown_report.html"
    capture = tmp_path / "provincetown_report_screenshot.png"
    unrelated = tmp_path / "01_room_photo.png"
    report.write_text("<html></html>", encoding="utf-8")
    capture.write_bytes(PNG_1X1)
    unrelated.write_bytes(PNG_1X1)
    reply = f"Done.\nMEDIA:{report}\nMEDIA:{capture}\nMEDIA:{unrelated}"

    monkeypatch.setattr(
        "nullion.messaging_adapters.media_candidate_paths_from_text",
        lambda text: [report, capture, unrelated],
    )
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done.", (report, capture, unrelated)),
    )

    delivery = prepare_reply_for_platform_delivery(reply, allow_attachments=True)

    assert delivery.attachments == (report, unrelated)
    assert delivery.unavailable_attachment_count == 0


def test_delivery_contract_allows_plain_paths_only_with_artifact_contract(tmp_path, monkeypatch) -> None:
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    artifact = artifact_root / "result.custom"
    artifact.write_text("artifact", encoding="utf-8")
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))

    reply = f"Finished here:\n{artifact}"
    message_only = prepare_reply_for_platform_delivery(reply)
    contracted = prepare_reply_for_platform_delivery(
        reply,
        delivery_contract=delivery_contract_for_turn(
            "nonsense wording without known verbs",
            reply=reply,
            artifact_paths=[str(artifact)],
            requires_attachment_delivery=True,
        ),
    )

    assert message_only.attachments == ()
    assert contracted.attachments == (artifact,)


def test_platform_delivery_accepts_media_inside_configured_workspace_root(tmp_path, monkeypatch) -> None:
    stage_home = tmp_path / "stage-home"
    workspace_root = stage_home / "workspaces"
    artifact = workspace_root / "workspace_admin" / "artifacts" / "croton_gorge_park_day_trip_pamphlet.pdf"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"%PDF-1.4\n% pamphlet\n")
    monkeypatch.setenv("NULLION_HOME", str(stage_home))
    monkeypatch.setenv("NULLION_DATA_DIR", str(stage_home))
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(workspace_root))

    delivery = prepare_reply_for_platform_delivery(
        f"Created the PDF pamphlet.\nMEDIA:{artifact}",
        principal_id="telegram:test-chat",
        allow_attachments=True,
    )

    assert tuple(path.resolve() for path in delivery.attachments) == (artifact.resolve(),)
    assert delivery.unavailable_attachment_count == 0
    assert "MEDIA:" not in (delivery.text or "")


def test_platform_delivery_rejects_media_from_other_lane_even_if_path_shape_matches(tmp_path, monkeypatch) -> None:
    prod_home = tmp_path / "prod-home"
    stage_home = tmp_path / "stage-home"
    stage_artifact = (
        stage_home
        / "workspaces"
        / "workspace_admin"
        / "artifacts"
        / "croton_gorge_park_day_trip_pamphlet.pdf"
    )
    stage_artifact.parent.mkdir(parents=True)
    stage_artifact.write_bytes(b"%PDF-1.4\n% stage pamphlet\n")
    monkeypatch.setenv("NULLION_HOME", str(prod_home))
    monkeypatch.setenv("NULLION_DATA_DIR", str(prod_home))
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(prod_home / "workspaces"))

    delivery = prepare_reply_for_platform_delivery(
        f"Created the PDF pamphlet.\nMEDIA:{stage_artifact}",
        principal_id="telegram:test-chat",
        allow_attachments=True,
    )

    assert delivery.attachments == ()
    assert delivery.unavailable_attachment_count == 1
    assert delivery.requires_attachment_delivery is True
    assert "couldn't attach" in (delivery.text or "")
