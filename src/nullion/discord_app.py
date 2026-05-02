"""Discord bot adapter for Nullion."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path

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
    sanitize_external_inline_markup,
    save_messaging_attachment,
    split_reply_for_platform,
)
from nullion.messaging_runtime import build_messaging_runtime_service_from_settings
from nullion.turn_dispatch_graph import GLOBAL_TURN_DISPATCH_TRACKER
from nullion.users import resolve_messaging_user


logger = logging.getLogger(__name__)
_DEFAULT_ENV_PATH = Path.home() / ".nullion" / ".env"
_DEFAULT_CHECKPOINT_PATH = Path.home() / ".nullion" / "runtime.db"


def _record_discord_delivery_receipt(
    *,
    channel_id: str | None,
    delivery,
    transport_ok: bool,
    request_id: str | None = None,
    message_id: str | None = None,
    error: str | None = None,
) -> None:
    record_platform_delivery_receipt(
        build_platform_delivery_receipt(
            channel="discord",
            target_id=channel_id,
            delivery=delivery,
            transport_ok=transport_ok,
            request_id=request_id,
            message_id=message_id,
            error=error,
        )
    )


def _optional_message_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _require_discord_settings(settings: NullionSettings) -> str:
    if not settings.discord.enabled:
        raise MessagingAdapterConfigurationError("Set NULLION_DISCORD_ENABLED=true to run the Discord adapter.")
    if not settings.discord.bot_token:
        raise MessagingAdapterConfigurationError("NULLION_DISCORD_BOT_TOKEN is required.")
    return settings.discord.bot_token


@asynccontextmanager
async def _discord_typing(channel):
    typing = getattr(channel, "typing", None)
    if typing is not None:
        context = typing()
        try:
            await context.__aenter__()
        except Exception:
            logger.debug("Discord typing indicator failed", exc_info=True)
            yield
            return
        try:
            yield
        finally:
            try:
                await context.__aexit__(None, None, None)
            except Exception:
                logger.debug("Discord typing indicator cleanup failed", exc_info=True)
        return

    trigger_typing = getattr(channel, "trigger_typing", None)
    if trigger_typing is not None:
        try:
            result = trigger_typing()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.debug("Discord typing indicator failed", exc_info=True)
    yield


def _discord_file_for_path(path: Path):
    try:
        import discord

        return discord.File(str(path), filename=path.name)
    except Exception:
        return path.open("rb")


def _discord_plain_format_fallback_text(plain_text: str) -> str:
    plain_text = sanitize_external_inline_markup(plain_text)
    return (
        "Discord could not send the formatted reply, so here is the same text as plain output:\n\n"
        "```text\n"
        f"{plain_text}"
        "\n```"
    )


async def _send_discord_text_with_plain_fallback(channel, text: str) -> None:
    text = sanitize_external_inline_markup(text or "")
    try:
        await channel.send(text)
    except Exception:
        logger.warning("Discord message delivery failed; retrying as plain text.", exc_info=True)
        await channel.send(_discord_plain_format_fallback_text(text))


async def _send_discord_chunks_with_plain_fallback(channel, text: str | None, *, limit: int = 1900) -> None:
    for chunk in split_reply_for_platform(sanitize_external_inline_markup(text or ""), limit=limit):
        await _send_discord_text_with_plain_fallback(channel, chunk)


async def _send_discord_reply_files(channel, *, text: str | None, paths: tuple[Path, ...]) -> bool:
    if not paths:
        return False
    try:
        for index, path in enumerate(paths):
            content = text if index == 0 else None
            await _send_discord_reply_file(channel, path=path, content=content)
        return True
    except Exception:
        logger.warning("Discord file send failed", exc_info=True)
        return False


async def _send_discord_reply_file(channel, *, path: Path, content: str | None) -> None:
    async def operation():
        file_obj = _discord_file_for_path(path)
        try:
            kwargs = {"file": file_obj}
            if content:
                kwargs["content"] = content
            await channel.send(**kwargs)
        finally:
            close = getattr(file_obj, "close", None)
            if close is not None:
                try:
                    close()
                except Exception:
                    pass

    await retry_messaging_delivery_operation(operation)


async def send_discord_platform_delivery(
    *,
    bot_token: str,
    channel_id: str,
    text: str,
    principal_id: str | None = None,
) -> bool:
    """Send a platform delivery to Discord over REST, uploading MEDIA artifacts."""
    if not bot_token or not channel_id:
        return False
    delivery = None
    try:
        import httpx

        delivery = prepare_reply_for_platform_delivery(text, principal_id=principal_id)
        url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        headers = {"Authorization": f"Bot {bot_token}"}
        async with httpx.AsyncClient(timeout=60.0) as client:
            if delivery.attachments:
                for index, attachment_path in enumerate(delivery.attachments):
                    content = delivery.text if index == 0 else None
                    async def operation(attachment_path=attachment_path, content=content):
                        with attachment_path.open("rb") as file_obj:
                            response = await client.post(
                                url,
                                headers=headers,
                                data={"content": content or ""},
                                files={"file": (attachment_path.name, file_obj)},
                            )
                        response.raise_for_status()

                    await retry_messaging_delivery_operation(operation)
                _record_discord_delivery_receipt(channel_id=channel_id, delivery=delivery, transport_ok=True)
                return True
            try:
                response = await client.post(url, headers=headers, json={"content": delivery.text or ""})
                response.raise_for_status()
            except Exception:
                logger.warning("Discord platform text delivery failed; retrying as plain text.", exc_info=True)
                response = await client.post(
                    url,
                    headers=headers,
                    json={"content": _discord_plain_format_fallback_text(delivery.text or "")},
                )
                response.raise_for_status()
            _record_discord_delivery_receipt(channel_id=channel_id, delivery=delivery, transport_ok=True)
            return True
    except Exception:
        logger.warning("Discord platform delivery failed", exc_info=True)
        if delivery is not None:
            _record_discord_delivery_receipt(
                channel_id=channel_id,
                delivery=delivery,
                transport_ok=False,
                error="platform_delivery_failed",
            )
        return False


async def _download_discord_attachments(
    message,
    *,
    settings: NullionSettings | None = None,
) -> tuple[dict[str, str], ...]:
    raw_attachments = list(getattr(message, "attachments", []) or [])
    if not raw_attachments:
        return ()
    try:
        import httpx
    except Exception:
        return ()
    attachments: list[dict[str, str]] = []
    author = getattr(message, "author", None)
    user = resolve_messaging_user("discord", getattr(author, "id", None), settings)
    principal_id = f"user:{user.user_id}" if user.role == "member" else "telegram_chat"
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for attachment in raw_attachments:
            media_type = str(getattr(attachment, "content_type", "") or "").strip() or None
            filename = str(getattr(attachment, "filename", "") or "discord-upload")
            if not is_supported_chat_file(filename=filename, media_type=media_type):
                continue
            url = str(getattr(attachment, "url", "") or "").strip()
            if not url:
                continue
            try:
                response = await client.get(url)
                response.raise_for_status()
            except Exception:
                logger.warning("Could not download Discord attachment", exc_info=True)
                continue
            saved = save_messaging_attachment(
                filename=filename,
                data=response.content,
                media_type=media_type or response.headers.get("content-type"),
                principal_id=principal_id,
            )
            if saved is not None:
                attachments.append(saved)
    return tuple(attachments)


async def handle_discord_message(service, settings: NullionSettings, message) -> None:
    if getattr(getattr(message, "author", None), "bot", False):
        return
    text = str(getattr(message, "content", "") or "").strip()
    user_id = str(getattr(getattr(message, "author", None), "id", "") or "").strip()
    if not user_id:
        return
    try:
        attachments = await _download_discord_attachments(message, settings=settings)
    except TypeError:
        attachments = await _download_discord_attachments(message)
    if not text and not attachments:
        return

    ingress = MessagingIngress(
        channel="discord",
        user_id=user_id,
        text=text or "Please analyze the attached file(s).",
        attachments=attachments,
        request_id=_optional_message_text(getattr(message, "id", None)),
        message_id=_optional_message_text(getattr(message, "id", None)),
        delivery_target_id=_optional_message_text(getattr(getattr(message, "channel", None), "id", None)),
    )
    if not require_authorized_ingress(ingress, settings):
        await message.channel.send("Unauthorized messaging identity.")
        return

    turn_registration = await GLOBAL_TURN_DISPATCH_TRACKER.register(
        ingress.operator_chat_id,
        ingress.text,
        turn_id=ingress.message_id or ingress.request_id,
        model_client=getattr(service, "model_client", None),
    )
    try:
        await turn_registration.wait_for_dependencies()
        async with _discord_typing(message.channel):
            turn_result = await asyncio.to_thread(handle_messaging_ingress_result, service, ingress)
            reply = turn_result.reply
        if await turn_registration.is_superseded():
            return
        principal_id = principal_id_for_messaging_identity("discord", user_id, settings)
        delivery = prepare_reply_for_platform_delivery(
            reply,
            principal_id=principal_id,
            delivery_contract=turn_result.delivery_contract,
        )
        reply_text = delivery.text
        delivery_receipt_recorded = False
        if delivery.attachments:
            if await _send_discord_reply_files(message.channel, text=reply_text, paths=delivery.attachments):
                _record_discord_delivery_receipt(
                    channel_id=_optional_message_text(getattr(message.channel, "id", None)),
                    delivery=delivery,
                    transport_ok=True,
                    request_id=ingress.request_id,
                    message_id=ingress.message_id,
                )
                return
            _record_discord_delivery_receipt(
                channel_id=_optional_message_text(getattr(message.channel, "id", None)),
                delivery=delivery,
                transport_ok=False,
                request_id=ingress.request_id,
                message_id=ingress.message_id,
                error="attachment_upload_failed",
            )
            delivery_receipt_recorded = True
            reply_text = platform_delivery_failure_reply(delivery)
        await _send_discord_chunks_with_plain_fallback(message.channel, reply_text, limit=1900)
        if not delivery_receipt_recorded:
            _record_discord_delivery_receipt(
                channel_id=_optional_message_text(getattr(message.channel, "id", None)),
                delivery=delivery,
                transport_ok=True,
                request_id=ingress.request_id,
                message_id=ingress.message_id,
            )
    finally:
        await turn_registration.finish()


async def run_discord_app(
    *,
    checkpoint_path: str | Path = _DEFAULT_CHECKPOINT_PATH,
    env_path: str | Path | None = _DEFAULT_ENV_PATH,
    service_builder=build_messaging_runtime_service_from_settings,
) -> None:
    try:
        import discord
    except ImportError as exc:
        raise MessagingAdapterDependencyError("Install Discord support with `pip install discord.py`.") from exc

    settings = load_settings(env_path=env_path)
    bot_token = _require_discord_settings(settings)
    service = service_builder(checkpoint_path=checkpoint_path, env_path=env_path)

    intents = discord.Intents.default()
    intents.message_content = True

    class NullionDiscordClient(discord.Client):
        async def on_ready(self) -> None:
            logger.info("Nullion Discord adapter connected as %s", self.user)
            if not hasattr(self, "_nullion_reminder_task"):
                from nullion.reminder_delivery import run_reminder_delivery_loop

                async def _send_discord_reminder(chat_id: str, text: str) -> bool:
                    prefix, _, channel_id = str(chat_id).partition(":")
                    if prefix != "discord" or not channel_id:
                        return False
                    channel = self.get_channel(int(channel_id)) if str(channel_id).isdigit() else None
                    if channel is None:
                        try:
                            channel = await self.fetch_channel(int(channel_id))
                        except Exception:
                            logger.warning("Could not resolve Discord reminder channel %s", channel_id, exc_info=True)
                            return False
                    await _send_discord_chunks_with_plain_fallback(channel, text, limit=1900)
                    return True

                self._nullion_reminder_task = asyncio.create_task(
                    run_reminder_delivery_loop(service.runtime, send=_send_discord_reminder, settings=settings)
                )

        async def on_message(self, message) -> None:  # type: ignore[no-untyped-def]
            try:
                await handle_discord_message(service, settings, message)
            except Exception:
                logger.exception(
                    "Unhandled error in Discord on_message (channel=%s, author=%s)",
                    getattr(getattr(message, "channel", None), "id", "?"),
                    getattr(getattr(message, "author", None), "id", "?"),
                )

    client = NullionDiscordClient(intents=intents)
    logger.info("Starting Nullion Discord adapter")
    await client.start(bot_token)


def main(
    *,
    checkpoint_path: str | Path = _DEFAULT_CHECKPOINT_PATH,
    env_path: str | Path | None = _DEFAULT_ENV_PATH,
) -> None:
    asyncio.run(run_discord_app(checkpoint_path=checkpoint_path, env_path=env_path))


def cli() -> None:
    def _run() -> None:
        parser = argparse.ArgumentParser(description="Run the Nullion Discord adapter")
        parser.add_argument("--checkpoint", default=str(_DEFAULT_CHECKPOINT_PATH), help="Runtime checkpoint path")
        parser.add_argument("--env-file", default=str(_DEFAULT_ENV_PATH), help="Environment file path")
        args = parser.parse_args()
        return run_single_instance_entrypoint(
            "discord",
            lambda: main(checkpoint_path=args.checkpoint, env_path=args.env_file),
            wait_seconds=1.0,
            description="nullion-discord",
        )

    return run_user_facing_entrypoint(_run)


if __name__ == "__main__":
    cli()
