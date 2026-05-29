"""Discord bot adapter for Nullion."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import inspect
import logging
import os
from pathlib import Path
import time

from nullion.chat_attachments import is_supported_chat_file
from nullion.config import NullionSettings, load_settings
from nullion.entrypoint_guard import run_single_instance_entrypoint, run_user_facing_entrypoint
from nullion.events import make_event
from nullion.latency_phases import record_surface_latency_timing
from nullion.messaging_adapters import (
    MessagingAdapterConfigurationError,
    MessagingAdapterDependencyError,
    MessagingIngress,
    build_platform_delivery_receipt,
    delivery_receipt_transport_succeeded,
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
from nullion.operator_commands import is_stop_command_text
from nullion.platform_activity import (
    PlatformTaskCardStore,
    platform_activity_capabilities,
    should_deliver_task_status,
)
from nullion.run_activity import activity_trace_enabled
from nullion.session_stop import stop_session_async, stop_session_reply
from nullion.turn_dispatch_graph import GLOBAL_TURN_DISPATCH_TRACKER
from nullion.users import resolve_messaging_user


logger = logging.getLogger(__name__)
_WORKING_ACK_TEXT = "⌛ On it! Feel free to send other tasks — I can handle multiple at once."


_DEFAULT_ENV_PATH = Path.home() / ".nullion" / ".env"
_DEFAULT_CHECKPOINT_PATH = Path.home() / ".nullion" / "runtime.db"
_NULLION_DISCORD_TURN_SLOW_LOG_MS = "NULLION_DISCORD_TURN_SLOW_LOG_MS"


def _float_env_ms(name: str, *, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        return default


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


def _handle_messaging_ingress_result_with_dispatch(
    service,
    ingress,
    *,
    turn_dispatch_decision=None,
    text_delta_callback=None,
    activity_callback=None,
):
    try:
        parameters = inspect.signature(handle_messaging_ingress_result).parameters
    except (TypeError, ValueError):
        return handle_messaging_ingress_result(service, ingress)
    accepts_dispatch = (
        "turn_dispatch_decision" in parameters
        or any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())
    )
    if accepts_dispatch:
        kwargs = {"turn_dispatch_decision": turn_dispatch_decision}
        if "text_delta_callback" in parameters or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
        ):
            kwargs["text_delta_callback"] = text_delta_callback
        if "activity_callback" in parameters or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
        ):
            kwargs["activity_callback"] = activity_callback
        return handle_messaging_ingress_result(service, ingress, **kwargs)
    return handle_messaging_ingress_result(service, ingress)


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


async def _edit_discord_message(message, text: str) -> bool:
    edit = getattr(message, "edit", None)
    if edit is None:
        return False
    try:
        await edit(content=sanitize_external_inline_markup(text or ""))
        return True
    except Exception:
        logger.debug("Discord task card edit failed", exc_info=True)
        return False


class _DiscordTextDeltaStreamer:
    def __init__(self, *, loop, channel) -> None:
        self._loop = loop
        self._channel = channel
        self._message = None
        self._parts: list[str] = []
        self._last_update_at = 0.0
        self._last_text = ""

    @property
    def text(self) -> str:
        return "".join(self._parts)

    async def _send_or_edit(self, text: str) -> bool:
        rendered = sanitize_external_inline_markup(text or "")
        if self._message is None:
            send = getattr(self._channel, "send", None)
            if send is None:
                return False
            self._message = await send(rendered)
            return True
        return await _edit_discord_message(self._message, rendered)

    def emit(self, delta: str) -> None:
        if not delta or self._channel is None:
            return
        self._parts.append(delta)
        text = self.text
        now = time.monotonic()
        if self._last_text and now - self._last_update_at < 0.35 and len(text) - len(self._last_text) < 48:
            return
        self._last_text = text
        self._last_update_at = now
        try:
            asyncio.run_coroutine_threadsafe(self._send_or_edit(text), self._loop).result(timeout=2)
        except Exception:
            logger.debug("Discord text streaming update failed", exc_info=True)

    async def finish(self, final_text: str | None) -> bool:
        text = str(final_text or "")
        if not text or self._message is None:
            return False
        if self._last_text == text:
            return True
        return await self._send_or_edit(text)


async def _deliver_discord_task_status(
    *,
    channel_id: str,
    channel,
    group_id: str,
    text: str,
    status_kind: str,
    activity_id: str,
    activity_label: str,
    task_card_store: PlatformTaskCardStore,
    status_messages: dict[tuple[str, str], object],
    status_locks: dict[tuple[str, str], asyncio.Lock],
    planner_feed_enabled: bool,
    include_activity: bool,
) -> bool:
    target = str(channel_id or "").strip()
    group = str(group_id or "").strip()
    if (
        channel is None
        or not target
        or not group
        or not should_deliver_task_status(
            status_kind=status_kind,
            planner_feed_enabled=planner_feed_enabled,
            include_activity=include_activity,
        )
    ):
        return False
    rendered_status = task_card_store.update(
        target_id=target,
        group_id=group,
        status_kind=status_kind,
        text=text,
        activity_id=activity_id,
        activity_label=activity_label,
        include_activity=include_activity,
    )
    if not rendered_status:
        return True
    key = (target, group)
    lock = status_locks.setdefault(key, asyncio.Lock())
    async with lock:
        existing = status_messages.get(key)
        if existing is not None and await _edit_discord_message(existing, rendered_status):
            return True
        try:
            status_messages[key] = await channel.send(sanitize_external_inline_markup(rendered_status))
            return True
        except Exception:
            logger.debug("Discord task card delivery failed", exc_info=True)
            return False


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
                receipt = build_platform_delivery_receipt(
                    channel="discord",
                    target_id=channel_id,
                    delivery=delivery,
                    transport_ok=True,
                )
                record_platform_delivery_receipt(receipt)
                return delivery_receipt_transport_succeeded(receipt)
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
            receipt = build_platform_delivery_receipt(
                channel="discord",
                target_id=channel_id,
                delivery=delivery,
                transport_ok=True,
            )
            record_platform_delivery_receipt(receipt)
            return delivery_receipt_transport_succeeded(receipt)
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
    started_at = time.perf_counter()
    timing_marks: list[str] = []
    timing_last_at = started_at

    def _mark_timing(label: str) -> None:
        nonlocal timing_last_at
        now = time.perf_counter()
        timing_marks.append(f"{label}:{round((now - timing_last_at) * 1000, 1)}ms")
        timing_last_at = now

    def _log_turn_timing(outcome: str) -> None:
        total_ms = (time.perf_counter() - started_at) * 1000
        if total_ms < _float_env_ms(_NULLION_DISCORD_TURN_SLOW_LOG_MS, default=1000.0):
            return
        channel_id = str(getattr(getattr(message, "channel", None), "id", "") or "")
        request_id = str(getattr(message, "id", "") or "")
        message_id = request_id
        logger.warning(
            "discord turn slow timing channel=%s request_id=%s message_id=%s outcome=%s total_ms=%.1f phases=%s",
            channel_id,
            request_id,
            message_id,
            outcome,
            total_ms,
            ", ".join(timing_marks),
        )
        try:
            runtime = getattr(service, "runtime", None)
            if runtime is not None:
                try:
                    turn_id_value = getattr(getattr(turn_registration, "decision", None), "turn_id", None)
                except NameError:
                    turn_id_value = None
                record_surface_latency_timing(
                    runtime.store,
                    surface="discord",
                    conversation_id=channel_id,
                    turn_id=turn_id_value,
                    request_id=request_id,
                    message_id=message_id,
                    outcome=outcome,
                    total_ms=total_ms,
                    phases=timing_marks,
                    logger=logger,
                )
                runtime.store.add_event(
                    make_event(
                        event_type="discord.turn_timing",
                        actor="discord",
                        payload={
                            "request_id": request_id,
                            "message_id": message_id,
                            "channel_id": channel_id,
                            "outcome": outcome,
                            "total_ms": round(total_ms, 1),
                            "phases": timing_marks,
                            "platform": "discord",
                        },
                    )
                )
        except Exception:
            logger.debug("Could not record Discord turn timing event", exc_info=True)

    turn_outcome = "completed"
    _mark_timing("received")
    if getattr(getattr(message, "author", None), "bot", False):
        turn_outcome = "ignored"
        _log_turn_timing(turn_outcome)
        return
    text = str(getattr(message, "content", "") or "").strip()
    user_id = str(getattr(getattr(message, "author", None), "id", "") or "").strip()
    if not user_id:
        turn_outcome = "ignored"
        _log_turn_timing(turn_outcome)
        return
    try:
        attachments = await _download_discord_attachments(message, settings=settings)
    except TypeError:
        attachments = await _download_discord_attachments(message)
    _mark_timing("attachments_downloaded")
    if not text and not attachments:
        turn_outcome = "empty"
        _log_turn_timing(turn_outcome)
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
        turn_outcome = "unauthorized"
        _log_turn_timing(turn_outcome)
        return

    if is_stop_command_text(ingress.text):
        stop_result = await stop_session_async(
            conversation_id=ingress.operator_chat_id,
            runtime=getattr(service, "runtime", None),
            agent_orchestrator=getattr(service, "agent_orchestrator", None),
            turn_tracker=GLOBAL_TURN_DISPATCH_TRACKER,
        )
        await message.channel.send(stop_session_reply(stop_result))
        turn_outcome = "stop_command"
        _log_turn_timing(turn_outcome)
        return

    turn_registration = await GLOBAL_TURN_DISPATCH_TRACKER.register(
        ingress.operator_chat_id,
        ingress.text,
        turn_id=ingress.message_id or ingress.request_id,
        model_client=getattr(service, "model_client", None),
    )
    _mark_timing("turn_registered")
    text_streamer = _DiscordTextDeltaStreamer(loop=asyncio.get_running_loop(), channel=message.channel)
    loop = asyncio.get_running_loop()
    tool_working_ack_sent = False

    def emit_working_ack_for_tool_activity(event: dict[str, object]) -> None:
        nonlocal tool_working_ack_sent
        event_id = str(event.get("id") or "")
        event_tool_name = str(event.get("tool_name") or "")
        if (
            tool_working_ack_sent
            or message.channel is None
            or not (event_id.startswith("tool-") or event_id == "mini-agents" or event_tool_name)
        ):
            return
        tool_working_ack_sent = True
        try:
            asyncio.run_coroutine_threadsafe(
                _send_discord_text_with_plain_fallback(message.channel, _WORKING_ACK_TEXT),
                loop,
            )
        except Exception:
            logger.debug("Discord working message scheduling failed", exc_info=True)

    try:
        await turn_registration.wait_for_dependencies()
        _mark_timing("wait_dependencies")
        async with _discord_typing(message.channel):
            turn_result = await asyncio.to_thread(
                _handle_messaging_ingress_result_with_dispatch,
                service,
                ingress,
                turn_dispatch_decision=turn_registration.decision,
                text_delta_callback=text_streamer.emit,
                activity_callback=emit_working_ack_for_tool_activity,
            )
            reply = turn_result.reply
        _mark_timing("handler_completed")
        if await turn_registration.is_superseded():
            turn_outcome = "superseded"
            _log_turn_timing(turn_outcome)
            return
        principal_id = principal_id_for_messaging_identity("discord", user_id, settings)
        delivery = prepare_reply_for_platform_delivery(
            reply,
            principal_id=principal_id,
            delivery_contract=turn_result.delivery_contract,
        )
        if getattr(turn_result, "reply_already_sent", False) and not delivery.attachments:
            await text_streamer.finish(reply)
            _record_discord_delivery_receipt(
                channel_id=_optional_message_text(getattr(message.channel, "id", None)),
                delivery=delivery,
                transport_ok=True,
                request_id=ingress.request_id,
                message_id=ingress.message_id,
            )
            _mark_timing("delivery_complete")
            _log_turn_timing(turn_outcome)
            return
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
                _mark_timing("delivery_complete")
                _log_turn_timing(turn_outcome)
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
        _mark_timing("delivery_complete")
        _log_turn_timing(turn_outcome)
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
    try:
        from nullion.startup_warmup import schedule_chat_startup_warmup

        schedule_chat_startup_warmup(
            service.runtime,
            registry=getattr(service, "tool_registry", None),
            settings=settings,
            surface="discord",
        )
    except Exception:
        logger.debug("Could not schedule Discord chat startup warmup", exc_info=True)

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
    _task_card_store = PlatformTaskCardStore(platform_activity_capabilities("discord"))
    _status_messages: dict[tuple[str, str], object] = {}
    _status_locks: dict[tuple[str, str], asyncio.Lock] = {}

    async def _resolve_discord_channel(channel_id: str):
        if not channel_id:
            return None
        channel = client.get_channel(int(channel_id)) if str(channel_id).isdigit() else None
        if channel is not None:
            return channel
        try:
            return await client.fetch_channel(int(channel_id))
        except Exception:
            logger.debug("Discord task card channel fetch failed", exc_info=True)
            return None

    async def _discord_deliver_fn(conversation_id: str, text: str, **kwargs) -> bool:
        prefix, _, channel_id = str(conversation_id or "").partition(":")
        if prefix != "discord" or not channel_id:
            return False
        channel = await _resolve_discord_channel(channel_id)
        if channel is None:
            return False
        if kwargs.get("is_status"):
            return await _deliver_discord_task_status(
                channel_id=channel_id,
                channel=channel,
                group_id=str(kwargs.get("group_id") or ""),
                text=text,
                status_kind=str(kwargs.get("status_kind") or "task_summary"),
                activity_id=str(kwargs.get("activity_id") or ""),
                activity_label=str(kwargs.get("activity_label") or ""),
                task_card_store=_task_card_store,
                status_messages=_status_messages,
                status_locks=_status_locks,
                planner_feed_enabled=True,
                include_activity=activity_trace_enabled(),
            )
        if kwargs.get("is_artifact"):
            path = Path(str(text or "")).expanduser()
            if path.exists() and path.is_file():
                await _send_discord_reply_file(channel, path=path, content="Attached the requested file.")
            else:
                await _send_discord_chunks_with_plain_fallback(channel, text, limit=1900)
        else:
            await _send_discord_chunks_with_plain_fallback(channel, text, limit=1900)
        return True

    if getattr(service, "agent_orchestrator", None) is not None and hasattr(service.agent_orchestrator, "set_deliver_fn"):
        service.agent_orchestrator.set_deliver_fn(_discord_deliver_fn)

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
