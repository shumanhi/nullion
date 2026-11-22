from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest


def _message(mid=41, *, chat_id=123, bot=False, photo=True):
    return SimpleNamespace(message_id=mid, text=None, caption=None,
        photo=[SimpleNamespace()] if photo else [], chat=SimpleNamespace(id=chat_id),
        from_user=SimpleNamespace(is_bot=bot))


@pytest.mark.parametrize("caption", [None, "この画像を確認", "Revisa esta imagen"])
def test_explicit_media_anchor_excludes_unrelated_history(caption):
    from nullion.telegram_app import _telegram_reply_context
    from nullion.chat_operator import _reply_context_is_explicit_anchor, _reply_context_prompt
    replied = _message()
    replied.caption = caption
    context = _telegram_reply_context(SimpleNamespace(reply_to_message=replied))
    assert context["reply_to_has_attachment"] is True
    assert _reply_context_is_explicit_anchor(context)
    assert "exact message" in _reply_context_prompt(context)


@pytest.mark.parametrize("bot,other_chat", [(True, False), (False, True)])
def test_quoted_bot_output_or_other_chat_is_not_downloaded(monkeypatch, bot, other_chat):
    from nullion import telegram_app
    async def forbidden(*args, **kwargs):
        pytest.fail("not an owned quoted user attachment")
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", forbidden)
    msg = _message(photo=False)
    msg.reply_to_message = _message(chat_id=456 if other_chat else 123, bot=bot)
    assert asyncio.run(telegram_app._telegram_reply_attachments(msg, None, runtime=None, settings=None)) == []


@pytest.mark.parametrize("media_group", [False, True])
def test_exact_attachment_receipt_survives_restart_and_avoids_redownload(tmp_path, monkeypatch, media_group):
    from nullion import telegram_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.messaging_adapters import save_messaging_attachment
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    attachment = save_messaging_attachment(filename="original.png", data=b"original bytes",
        media_type="image/png", principal_id="telegram_chat")
    telegram_app._checkpoint_telegram_attachment_receipt(runtime,
        conversation_id="telegram:123", message_id="telegram-media-group:123:album-a" if media_group else "telegram-message:123:41",
        request_id="telegram-update:1", text="画像", attachments=[attachment])
    # A newer image and unrelated task must not replace the selected identity.
    telegram_app._checkpoint_telegram_attachment_receipt(runtime,
        conversation_id="telegram:123", message_id="telegram-message:123:42",
        request_id="telegram-update:2", text="other", attachments=[{**attachment, "name": "other.png"}])
    restarted = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    async def forbidden(*args, **kwargs):
        pytest.fail("durable exact input already available")
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", forbidden)
    msg = _message(photo=False)
    msg.reply_to_message = _message()
    if media_group:
        msg.reply_to_message.media_group_id = "album-a"
    result = asyncio.run(telegram_app._telegram_reply_attachments(msg, None, runtime=restarted, settings=None))
    assert result == [attachment]
    assert not restarted.store.has_conversation_ingress_id("telegram:123", "telegram-update:1")


@pytest.mark.parametrize("expired", [False, True])
def test_missing_or_expired_receipt_recovers_embedded_quoted_media(tmp_path, monkeypatch, expired):
    from nullion import telegram_app
    from nullion.runtime import bootstrap_persistent_runtime
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    if expired:
        runtime.store.add_conversation_event({"event_type": "conversation.attachment_received",
            "conversation_id": "telegram:123", "message_id": "telegram-message:123:41",
            "attachments": [{"path": str(tmp_path / "gone.png")}]})
    msg = _message(photo=False)
    msg.reply_to_message = _message()
    async def download(message, context, *, settings):
        assert message is msg.reply_to_message
        return [{"path": "/owned/recovered.png"}]
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", download)
    assert asyncio.run(telegram_app._telegram_reply_attachments(msg, None, runtime=runtime, settings=None)) == [{"path": "/owned/recovered.png"}]


@pytest.mark.parametrize('source', ['history', 'journal'])
@pytest.mark.parametrize('fault', ['replaced', 'legacy', 'workspace', 'principal', 'outside_uploads'])
def test_quoted_input_requires_original_bytes_and_current_owner(tmp_path, monkeypatch, source, fault):
    from nullion import telegram_app
    from nullion.attachment_receipts import load_attachment_receipt
    from nullion.messaging_adapters import save_messaging_attachment
    from nullion.runtime import bootstrap_persistent_runtime
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    saved = save_messaging_attachment(filename='telegram-photo.png', data=b'original',
        media_type='image/png', principal_id='telegram_chat')
    if fault == 'legacy':
        saved.pop('content_sha256')
    elif fault == 'workspace':
        saved['workspace_id'] = 'another-workspace'
    elif fault == 'principal':
        saved['principal_id'] = 'user:another'
    elif fault == 'outside_uploads':
        external = tmp_path/'another-workspace.png'
        external.write_bytes(Path(saved['path']).read_bytes())
        saved['path'] = str(external)
    telegram_app._checkpoint_telegram_attachment_receipt(runtime, conversation_id='telegram:123',
        message_id='telegram-message:123:41', request_id='update:1', text=None, attachments=[saved])
    if fault == 'replaced':
        Path(saved['path']).unlink()
        replacement = save_messaging_attachment(filename='telegram-photo.png', data=b'replaced',
            media_type='image/png', principal_id='telegram_chat')
        assert replacement['path'] == saved['path']
        assert replacement['size_bytes'] == saved['size_bytes']
    if source == 'journal':
        runtime = SimpleNamespace(checkpoint_path=runtime.checkpoint_path,
            store=SimpleNamespace(list_recent_conversation_events=lambda *a, **kw: []))
    else:
        monkeypatch.setattr('nullion.attachment_receipts.load_attachment_receipt', lambda *a: None)
    message = _message(photo=False)
    message.reply_to_message = _message()
    downloads = []
    async def download(quoted, context, *, settings):
        downloads.append(quoted.message_id)
        return [{'path': '/owned/original-from-telegram.png'}]
    monkeypatch.setattr(telegram_app, '_download_telegram_attachments', download)
    result = asyncio.run(telegram_app._telegram_reply_attachments(message, None, runtime=runtime, settings=None))
    assert downloads == [41] and result == [{'path': '/owned/original-from-telegram.png'}]


@pytest.mark.parametrize("data,mime", [(b"\xff\xd8\xffjpeg", "image/jpeg"),
    (b"\x89PNG\r\n\x1a\nimage", "image/png"), (b"GIF89aimage", "image/gif"),
    (b"RIFF1234WEBPdata", "image/webp")])
def test_shared_image_input_uses_bytes_over_transport_mime(tmp_path, monkeypatch, data, mime):
    from nullion.chat_attachments import chat_attachment_content_blocks, normalize_chat_attachments
    from nullion.messaging_adapters import save_messaging_attachment
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    saved = save_messaging_attachment(filename="mislabelled.png", data=data, media_type="image/png", principal_id="telegram_chat")
    assert saved["media_type"] == mime
    # Existing uploads with old wrong metadata are corrected at the model boundary too.
    saved["media_type"] = "image/png"
    blocks = chat_attachment_content_blocks("画像", normalize_chat_attachments([saved]))
    assert next(block for block in blocks if block["type"] == "image")["source"]["media_type"] == mime


def test_uncaptioned_quote_identity_survives_request_context_snapshot():
    from nullion.agent_orchestrator import _completion_review_request_context
    quote = {"platform": "telegram", "reply_to_message_id": 41, "reply_to_from_bot": False,
        "reply_to_has_attachment": True}
    result = _completion_review_request_context({"tool_flow_context": {"request_context": {"quoted_message": quote}}})
    assert result["request_context"]["quoted_message"] == quote
    assert result["request_context"]["quoted_reference_role"] == "user"
