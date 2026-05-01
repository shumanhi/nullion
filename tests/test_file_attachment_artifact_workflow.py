from __future__ import annotations

import base64
from pathlib import Path

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_descriptors_for_paths,
    media_candidate_paths_from_text,
    split_media_reply_attachments,
)
from nullion.chat_attachments import (
    ChatAttachment,
    attachment_processing_failure_reply,
    audio_transcription_satisfied,
    chat_attachment_content_blocks,
    is_supported_chat_file,
    normalize_chat_attachments,
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


def test_media_directives_split_caption_and_existing_safe_attachments(tmp_path) -> None:
    artifact = tmp_path / "nullion-artifact-report.html"
    artifact.write_text("<html><body>" + ("report" * 20) + "</body></html>", encoding="utf-8")
    missing = tmp_path / "nullion-artifact-missing.html"
    reply = f"Here you go\n- MEDIA:{artifact}\nMEDIA:{missing}"

    caption, paths = split_media_reply_attachments(
        reply,
        is_safe_attachment_path=lambda path: path.name.startswith("nullion-artifact-"),
    )

    assert caption == "Here you go\nAttachment unavailable: nullion-artifact-missing.html"
    assert paths == (artifact,)
    assert media_candidate_paths_from_text(reply) == [artifact, missing]


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
    artifact_root.mkdir(parents=True)
    artifact = artifact_root / "report.txt"
    artifact.write_text("artifact", encoding="utf-8")

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))

    reply = f"Himan, the text file is here:\n\n{artifact}\n\nAttachment/artifact link:"
    delivery = prepare_reply_for_platform_delivery(reply, allow_attachments=True)

    assert delivery.attachments == (artifact,)
    assert str(artifact) not in (delivery.text or "")
    assert "text file is here" in (delivery.text or "")


def test_delivery_contract_ignores_text_only_file_delivery_heuristics() -> None:
    assert not text_or_attachments_expect_attachment_delivery(
        "please handle this",
        attachments=[{"name": "upload.bin", "path": "/tmp/upload.bin", "media_type": "application/octet-stream"}],
    )
    assert not text_or_attachments_expect_attachment_delivery("please handle this", attachments=[])
    assert not delivery_contract_for_turn("send that as a PDF file").requires_attachment_delivery
    assert not delivery_contract_for_turn("where is the file").requires_attachment_delivery
    assert not delivery_contract_for_turn("generate a peacock").requires_attachment_delivery


def test_delivery_contract_uses_artifact_evidence_not_user_verbs(tmp_path, monkeypatch) -> None:
    artifact = tmp_path / "nullion-artifact-arbitrary.weird"
    artifact.write_text("artifact", encoding="utf-8")
    reply = f"Finished.\nMEDIA:{artifact}"

    monkeypatch.setattr("nullion.messaging_adapters.media_candidate_paths_from_text", lambda text: [artifact])
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Finished.", (artifact,)),
    )

    contract = delivery_contract_for_turn("frobnicate the attachment however tomorrow says it", reply=reply)
    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert contract.requires_attachment_delivery
    assert contract.source == "media_directive"
    assert delivery.attachments == (artifact,)


def test_platform_delivery_rejects_wrong_requested_attachment_format(tmp_path, monkeypatch) -> None:
    html_artifact = tmp_path / "nullion-artifact-report.html"
    html_artifact.write_text("<html><body>report</body></html>", encoding="utf-8")
    reply = f"Done -- attached the requested PDF.\nMEDIA:{html_artifact}"

    monkeypatch.setattr("nullion.messaging_adapters.media_candidate_paths_from_text", lambda text: [html_artifact])
    monkeypatch.setattr(
        "nullion.messaging_adapters.split_reply_for_platform_delivery",
        lambda text, principal_id=None: ("Done -- attached the requested PDF.", (html_artifact,)),
    )

    contract = delivery_contract_for_turn("send this as a pdf", reply=reply)
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

    contract = delivery_contract_for_turn("send this as a pdf", reply=reply)
    delivery = prepare_reply_for_platform_delivery(reply, delivery_contract=contract)

    assert delivery.attachments == (pdf_artifact,)


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
        ),
    )

    assert message_only.attachments == ()
    assert contracted.attachments == (artifact,)
