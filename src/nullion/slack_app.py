"""Slack Socket Mode adapter for Nullion."""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import re

from nullion.chat_attachments import is_supported_chat_file
from nullion.config import NullionSettings, load_settings
from nullion.entrypoint_guard import run_single_instance_entrypoint, run_user_facing_entrypoint
from nullion.messaging_adapters import (
    MessagingAdapterConfigurationError,
    MessagingAdapterDependencyError,
    MessagingIngress,
    build_platform_delivery_receipt,
    handle_messaging_ingress_result,
    platform_delivery_failure_reply,
    prepare_reply_for_platform_delivery,
    principal_id_for_messaging_identity,
    record_platform_delivery_receipt,
    require_authorized_ingress,
    retry_messaging_delivery_operation,
    save_messaging_attachment,
    split_reply_for_platform,
)
from nullion.messaging_runtime import build_messaging_runtime_service_from_settings
from nullion.turn_dispatch_graph import GLOBAL_TURN_DISPATCH_TRACKER
from nullion.users import resolve_messaging_user


logger = logging.getLogger(__name__)
_DEFAULT_ENV_PATH = Path.home() / ".nullion" / ".env"
_DEFAULT_CHECKPOINT_PATH = Path.home() / ".nullion" / "runtime.db"


def _record_slack_delivery_receipt(
    *,
    channel: str | None,
    delivery,
    transport_ok: bool,
    request_id: str | None = None,
    message_id: str | None = None,
    error: str | None = None,
) -> None:
    record_platform_delivery_receipt(
        build_platform_delivery_receipt(
            channel="slack",
            target_id=channel,
            delivery=delivery,
            transport_ok=transport_ok,
            request_id=request_id,
            message_id=message_id,
            error=error,
        )
    )


def _optional_event_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


# Slack mrkdwn formatting tokens that should be stripped before LLM ingestion.
# • <@UXXXXXXX> / <@UXXXXXXX|name> → mention → keep display name or drop
# • <#CXXXXXXX|channel-name>       → channel link → keep channel name
# • <!here>, <!channel>, <!everyone> → group mentions → drop
# • <URL|label> / <URL>            → hyperlink → keep label or URL
# • *bold*, _italic_, ~strike~, `code` → keep literal text, strip markers
_RE_SLACK_USER_MENTION = re.compile(r"<@[A-Z0-9]+(?:\|([^>]+))?>")
_RE_SLACK_CHANNEL_LINK = re.compile(r"<#[A-Z0-9]+\|([^>]+)>")
_RE_SLACK_GROUP_MENTION = re.compile(r"<!(?:here|channel|everyone)>")
_RE_SLACK_HYPERLINK = re.compile(r"<(https?://[^|>]+)(?:\|([^>]+))?>")
_RE_SLACK_STYLE_MARKERS = re.compile(r"(?<!\w)[*_~`]([^*_~`]+)[*_~`](?!\w)")
_RE_MARKDOWN_BOLD = re.compile(r"\*\*([^*\n]+)\*\*")
_RE_MARKDOWN_LINK = re.compile(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)")


def _normalize_slack_text(text: str) -> str:
    """Strip Slack mrkdwn formatting tokens, leaving plain human-readable text."""
    # Replace channel links with #channel-name
    text = _RE_SLACK_CHANNEL_LINK.sub(lambda m: f"#{m.group(1)}", text)
    # Replace user mentions with display name when present, otherwise drop
    text = _RE_SLACK_USER_MENTION.sub(lambda m: m.group(1) or "", text)
    # Drop group mentions
    text = _RE_SLACK_GROUP_MENTION.sub("", text)
    # Replace hyperlinks with label or bare URL
    text = _RE_SLACK_HYPERLINK.sub(lambda m: m.group(2) or m.group(1), text)
    # Strip simple mrkdwn emphasis/code markers while keeping the literal text.
    text = _RE_SLACK_STYLE_MARKERS.sub(lambda m: m.group(1), text)
    return text.strip()


def _format_slack_reply(text: str) -> str:
    """Adapt Nullion's Markdown-ish replies to Slack mrkdwn."""
    text = _RE_MARKDOWN_LINK.sub(lambda m: f"<{m.group(2)}|{m.group(1)}>", text)
    text = _RE_MARKDOWN_BOLD.sub(lambda m: f"*{m.group(1)}*", text)
    return text


def _slack_plain_format_fallback_text(plain_text: str) -> str:
    return (
        "Slack could not send the formatted reply, so here is the same text as plain output:\n\n"
        "```text\n"
        f"{plain_text}"
        "\n```"
    )


async def _post_slack_message_with_plain_fallback(client, *, channel: str, formatted_text: str, plain_text: str) -> None:
    try:
        await client.chat_postMessage(channel=channel, text=formatted_text or "")
    except Exception:
        logger.warning("Slack formatted message delivery failed; retrying as plain text.", exc_info=True)
        await client.chat_postMessage(
            channel=channel,
            text=_slack_plain_format_fallback_text(plain_text or ""),
            mrkdwn=False,
        )


async def _send_slack_callable_with_plain_fallback(sender, *, formatted_text: str, plain_text: str) -> None:
    try:
        await sender(formatted_text)
    except Exception:
        logger.warning("Slack formatted callback delivery failed; retrying as plain text.", exc_info=True)
        await sender(_slack_plain_format_fallback_text(plain_text or ""))


def _slack_response_field(response: object, name: str) -> str | None:
    if isinstance(response, dict):
        value = response.get(name)
    else:
        getter = getattr(response, "get", None)
        if getter is None:
            value = getattr(response, name, None)
        else:
            try:
                value = getter(name)
            except Exception:
                value = getattr(response, name, None)
    text = str(value or "").strip()
    return text or None


async def _update_slack_message(client, *, channel: str, ts: str, text: str) -> bool:
    if client is None:
        return False
    chat_update = getattr(client, "chat_update", None)
    if chat_update is None:
        return False
    try:
        result = chat_update(channel=channel, ts=ts, text=text)
        if asyncio.iscoroutine(result):
            await result
        return True
    except Exception:
        logger.debug("Slack working message update failed", exc_info=True)
        return False


async def _upload_slack_reply_files(client, *, channel: str, paths: tuple[Path, ...], initial_comment: str | None) -> bool:
    if client is None or not channel or not paths:
        return False
    upload_v2 = getattr(client, "files_upload_v2", None)
    upload_legacy = getattr(client, "files_upload", None)
    try:
        for index, path in enumerate(paths):
            comment = initial_comment if index == 0 else None
            if upload_v2 is not None:
                await retry_messaging_delivery_operation(
                    lambda path=path, comment=comment: upload_v2(
                        channel=channel,
                        file=str(path),
                        filename=path.name,
                        title=path.name,
                        initial_comment=comment,
                    )
                )
            elif upload_legacy is not None:
                await retry_messaging_delivery_operation(
                    lambda path=path, comment=comment: upload_legacy(
                        channels=channel,
                        file=str(path),
                        filename=path.name,
                        title=path.name,
                        initial_comment=comment,
                    )
                )
            else:
                return False
        return True
    except Exception:
        logger.warning("Slack file upload failed", exc_info=True)
        return False


async def send_slack_platform_delivery(
    *,
    bot_token: str,
    channel: str,
    text: str,
    principal_id: str | None = None,
) -> bool:
    """Send a platform delivery to Slack, uploading any MEDIA artifacts."""
    if not bot_token or not channel:
        return False
    delivery = None
    try:
        from slack_sdk.web.async_client import AsyncWebClient

        client = AsyncWebClient(token=bot_token)
        delivery = prepare_reply_for_platform_delivery(text, principal_id=principal_id)
        formatted_reply = _format_slack_reply(delivery.text or "")
        if delivery.attachments:
            uploaded = await _upload_slack_reply_files(
                client,
                channel=channel,
                paths=delivery.attachments,
                initial_comment=formatted_reply or None,
            )
            _record_slack_delivery_receipt(
                channel=channel,
                delivery=delivery,
                transport_ok=uploaded,
                error=None if uploaded else "attachment_upload_failed",
            )
            return uploaded
        await _post_slack_message_with_plain_fallback(
            client,
            channel=channel,
            formatted_text=formatted_reply or "",
            plain_text=delivery.text or "",
        )
        _record_slack_delivery_receipt(channel=channel, delivery=delivery, transport_ok=True)
        return True
    except Exception:
        logger.warning("Slack platform delivery failed", exc_info=True)
        if delivery is not None:
            _record_slack_delivery_receipt(
                channel=channel,
                delivery=delivery,
                transport_ok=False,
                error="platform_delivery_failed",
            )
        return False


def _nullion_slack_command_text(command: dict) -> str:
    suffix = str(command.get("text") or "").strip()
    return f"/{suffix}" if suffix else "/help"


def _require_slack_settings(settings: NullionSettings) -> tuple[str, str]:
    if not settings.slack.enabled:
        raise MessagingAdapterConfigurationError("Set NULLION_SLACK_ENABLED=true to run the Slack adapter.")
    if not settings.slack.bot_token:
        raise MessagingAdapterConfigurationError("NULLION_SLACK_BOT_TOKEN is required.")
    if not settings.slack.app_token:
        raise MessagingAdapterConfigurationError("NULLION_SLACK_APP_TOKEN is required for Socket Mode.")
    return settings.slack.bot_token, settings.slack.app_token


async def _download_slack_attachments(
    event: dict,
    *,
    bot_token: str,
    settings: NullionSettings | None = None,
) -> tuple[dict[str, str], ...]:
    files = event.get("files")
    if not isinstance(files, list):
        return ()
    try:
        import httpx
    except Exception:
        return ()
    attachments: list[dict[str, str]] = []
    user = resolve_messaging_user("slack", str(event.get("user") or "").strip(), settings)
    principal_id = f"user:{user.user_id}" if user.role == "member" else "telegram_chat"
    headers = {"Authorization": f"Bearer {bot_token}"}
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for file_info in files:
            if not isinstance(file_info, dict):
                continue
            url = str(file_info.get("url_private_download") or file_info.get("url_private") or "").strip()
            if not url:
                continue
            mimetype = str(file_info.get("mimetype") or "").strip() or None
            filename = str(file_info.get("name") or file_info.get("title") or "slack-upload")
            if not is_supported_chat_file(filename=filename, media_type=mimetype):
                continue
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
            except Exception:
                logger.warning("Could not download Slack attachment", exc_info=True)
                continue
            saved = save_messaging_attachment(
                filename=filename,
                data=response.content,
                media_type=mimetype or response.headers.get("content-type"),
                principal_id=principal_id,
            )
            if saved is not None:
                attachments.append(saved)
    return tuple(attachments)


async def handle_slack_message(service, settings: NullionSettings, *, event: dict, say, client=None) -> None:
    text = _normalize_slack_text(str(event.get("text") or ""))
    user_id = str(event.get("user") or "").strip()
    if not user_id or event.get("bot_id") or event.get("subtype") == "bot_message":
        return
    try:
        attachments = await _download_slack_attachments(event, bot_token=settings.slack.bot_token, settings=settings)
    except TypeError:
        attachments = await _download_slack_attachments(event, bot_token=settings.slack.bot_token)
    if not text and not attachments:
        return

    ingress = MessagingIngress(
        channel="slack",
        user_id=user_id,
        text=text or "Please analyze the attached file(s).",
        attachments=attachments,
        request_id=_optional_event_text(event.get("client_msg_id") or event.get("event_ts")),
        message_id=_optional_event_text(event.get("event_ts")),
        delivery_target_id=_optional_event_text(event.get("channel")),
    )
    if not require_authorized_ingress(ingress, settings):
        await say("Unauthorized messaging identity.")
        return

    working_channel = str(event.get("channel") or "").strip()
    working_ts = None
    if client is not None and working_channel:
        try:
            working_response = await say("Working...")
            working_ts = _slack_response_field(working_response, "ts")
            working_channel = _slack_response_field(working_response, "channel") or working_channel
        except Exception:
            logger.debug("Slack working message send failed", exc_info=True)

    turn_registration = await GLOBAL_TURN_DISPATCH_TRACKER.register(
        ingress.operator_chat_id,
        ingress.text,
        turn_id=ingress.message_id or ingress.request_id,
    )
    try:
        await turn_registration.wait_for_dependencies()
        turn_result = await asyncio.to_thread(handle_messaging_ingress_result, service, ingress)
        reply = turn_result.reply
        principal_id = principal_id_for_messaging_identity("slack", user_id, settings)
        delivery = prepare_reply_for_platform_delivery(
            reply,
            principal_id=principal_id,
            delivery_contract=turn_result.delivery_contract,
        )
        formatted_reply = _format_slack_reply(delivery.text or "")
        delivery_receipt_recorded = False
        if delivery.attachments:
            if await _upload_slack_reply_files(client, channel=working_channel, paths=delivery.attachments, initial_comment=formatted_reply or None):
                _record_slack_delivery_receipt(
                    channel=working_channel,
                    delivery=delivery,
                    transport_ok=True,
                    request_id=ingress.request_id,
                    message_id=ingress.message_id,
                )
                delivery_receipt_recorded = True
                if working_ts:
                    await _update_slack_message(client, channel=working_channel, ts=working_ts, text="Attached the requested file.")
                return
            _record_slack_delivery_receipt(
                channel=working_channel,
                delivery=delivery,
                transport_ok=False,
                request_id=ingress.request_id,
                message_id=ingress.message_id,
                error="attachment_upload_failed",
            )
            delivery_receipt_recorded = True
            formatted_reply = _format_slack_reply(platform_delivery_failure_reply(delivery))
        chunks = split_reply_for_platform(formatted_reply, limit=39000)
        first_chunk = chunks[0] if chunks else ""
        if working_ts and first_chunk and await _update_slack_message(client, channel=working_channel, ts=working_ts, text=first_chunk):
            chunks = chunks[1:]
        for chunk in chunks:
            await _send_slack_callable_with_plain_fallback(say, formatted_text=chunk, plain_text=chunk)
        if not delivery_receipt_recorded:
            _record_slack_delivery_receipt(
                channel=working_channel,
                delivery=delivery,
                transport_ok=True,
                request_id=ingress.request_id,
                message_id=ingress.message_id,
            )
    finally:
        await turn_registration.finish()


async def handle_slack_command(service, settings: NullionSettings, *, command: dict, respond) -> None:
    user_id = str(command.get("user_id") or "").strip()
    if not user_id:
        return

    ingress = MessagingIngress(
        channel="slack",
        user_id=user_id,
        text=_nullion_slack_command_text(command),
        request_id=_optional_event_text(command.get("trigger_id")),
        message_id=_optional_event_text(command.get("trigger_id")),
        delivery_target_id=_optional_event_text(command.get("channel_id")),
    )
    if not require_authorized_ingress(ingress, settings):
        await respond("Unauthorized messaging identity.")
        return

    turn_result = await asyncio.to_thread(handle_messaging_ingress_result, service, ingress)
    reply = turn_result.reply
    formatted_reply = _format_slack_reply(reply or "")
    chunks = split_reply_for_platform(formatted_reply, limit=39000)
    if not chunks:
        await respond("Done.")
        return
    for chunk in chunks:
        await _send_slack_callable_with_plain_fallback(respond, formatted_text=chunk, plain_text=chunk)


async def run_slack_app(
    *,
    checkpoint_path: str | Path = _DEFAULT_CHECKPOINT_PATH,
    env_path: str | Path | None = _DEFAULT_ENV_PATH,
    service_builder=build_messaging_runtime_service_from_settings,
) -> None:
    try:
        from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
        from slack_bolt.async_app import AsyncApp
    except ImportError as exc:
        raise MessagingAdapterDependencyError("Install Slack support with `pip install slack-bolt`.") from exc

    settings = load_settings(env_path=env_path)
    bot_token, app_token = _require_slack_settings(settings)
    service = service_builder(checkpoint_path=checkpoint_path, env_path=env_path)

    app = AsyncApp(token=bot_token, signing_secret=settings.slack.signing_secret)

    @app.event("message")
    async def _on_message(event, say, client):  # type: ignore[no-untyped-def]
        await handle_slack_message(service, settings, event=event, say=say, client=client)

    @app.command("/nullion")
    async def _on_nullion_command(ack, command, respond):  # type: ignore[no-untyped-def]
        await ack()
        await handle_slack_command(service, settings, command=command, respond=respond)

    async def _send_slack_reminder(chat_id: str, text: str) -> bool:
        prefix, _, channel = str(chat_id).partition(":")
        if prefix != "slack" or not channel:
            return False
        return await send_slack_platform_delivery(bot_token=bot_token, channel=channel, text=text)

    logger.info("Starting Nullion Slack adapter")
    from nullion.reminder_delivery import run_reminder_delivery_loop

    reminder_task = asyncio.create_task(
        run_reminder_delivery_loop(service.runtime, send=_send_slack_reminder, settings=settings)
    )
    try:
        await AsyncSocketModeHandler(app, app_token).start_async()
    finally:
        reminder_task.cancel()


def main(
    *,
    checkpoint_path: str | Path = _DEFAULT_CHECKPOINT_PATH,
    env_path: str | Path | None = _DEFAULT_ENV_PATH,
) -> None:
    asyncio.run(run_slack_app(checkpoint_path=checkpoint_path, env_path=env_path))


def cli() -> None:
    def _run() -> None:
        parser = argparse.ArgumentParser(description="Run the Nullion Slack adapter")
        parser.add_argument("--checkpoint", default=str(_DEFAULT_CHECKPOINT_PATH), help="Runtime checkpoint path")
        parser.add_argument("--env-file", default=str(_DEFAULT_ENV_PATH), help="Environment file path")
        args = parser.parse_args()
        return run_single_instance_entrypoint(
            "slack",
            lambda: main(checkpoint_path=args.checkpoint, env_path=args.env_file),
            wait_seconds=1.0,
            description="nullion-slack",
        )

    return run_user_facing_entrypoint(_run)


if __name__ == "__main__":
    cli()
