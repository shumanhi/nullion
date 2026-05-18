"""Slack Socket Mode adapter for Nullion."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import os
from pathlib import Path
import re
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
_WORKING_ACK_TEXT = "Working on your request now. You can keep sending requests."


_DEFAULT_ENV_PATH = Path.home() / ".nullion" / ".env"
_DEFAULT_CHECKPOINT_PATH = Path.home() / ".nullion" / "runtime.db"
_NULLION_SLACK_TURN_SLOW_LOG_MS = "NULLION_SLACK_TURN_SLOW_LOG_MS"


def _float_env_ms(name: str, *, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        return default


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
    text = sanitize_external_inline_markup(text)
    text = _RE_MARKDOWN_LINK.sub(lambda m: f"<{m.group(2)}|{m.group(1)}>", text)
    text = _RE_MARKDOWN_BOLD.sub(lambda m: f"*{m.group(1)}*", text)
    return text


def _slack_reply_chunks(text: str | None, *, limit: int = 39000) -> list[tuple[str, str]]:
    plain_chunks = split_reply_for_platform(sanitize_external_inline_markup(text or ""), limit=limit)
    return [(_format_slack_reply(chunk), chunk) for chunk in plain_chunks]


def _slack_plain_format_fallback_text(plain_text: str) -> str:
    plain_text = sanitize_external_inline_markup(plain_text)
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


def _slack_response_ok(response: object) -> bool:
    if response is None:
        return True
    if isinstance(response, dict):
        value = response.get("ok")
    else:
        getter = getattr(response, "get", None)
        if getter is None:
            value = getattr(response, "ok", None)
        else:
            try:
                value = getter("ok")
            except Exception:
                value = getattr(response, "ok", None)
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no"}


async def _update_slack_message(client, *, channel: str, ts: str, text: str) -> bool:
    if client is None:
        return False
    chat_update = getattr(client, "chat_update", None)
    if chat_update is None:
        return False
    try:
        result = chat_update(channel=channel, ts=ts, text=text)
        if asyncio.iscoroutine(result):
            result = await result
        return _slack_response_ok(result)
    except Exception:
        logger.debug("Slack working message update failed", exc_info=True)
        return False


class _SlackTextDeltaStreamer:
    def __init__(self, *, loop, client, channel: str, ts: str | None) -> None:
        self._loop = loop
        self._client = client
        self._channel = channel
        self._ts = ts
        self._parts: list[str] = []
        self._last_update_at = 0.0
        self._last_text = ""

    @property
    def text(self) -> str:
        return "".join(self._parts)

    def emit(self, delta: str) -> None:
        if not delta or self._client is None or not self._channel or not self._ts:
            return
        self._parts.append(delta)
        text = self.text
        now = time.monotonic()
        if self._last_text and now - self._last_update_at < 0.35 and len(text) - len(self._last_text) < 48:
            return
        self._last_text = text
        self._last_update_at = now
        try:
            asyncio.run_coroutine_threadsafe(
                _update_slack_message(self._client, channel=self._channel, ts=self._ts, text=_format_slack_reply(text)),
                self._loop,
            ).result(timeout=2)
        except Exception:
            logger.debug("Slack text streaming update failed", exc_info=True)

    async def finish(self, final_text: str | None) -> bool:
        text = str(final_text or "")
        if not self._client or not self._channel or not self._ts or not text:
            return False
        if self._last_text == text:
            return True
        return await _update_slack_message(self._client, channel=self._channel, ts=self._ts, text=_format_slack_reply(text))


async def _deliver_slack_task_status(
    *,
    client,
    channel: str,
    group_id: str,
    text: str,
    status_kind: str,
    activity_id: str,
    activity_label: str,
    task_card_store: PlatformTaskCardStore,
    status_messages: dict[tuple[str, str], str],
    status_locks: dict[tuple[str, str], asyncio.Lock],
    planner_feed_enabled: bool,
    include_activity: bool,
) -> bool:
    target = str(channel or "").strip()
    group = str(group_id or "").strip()
    if (
        not target
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
        ts = status_messages.get(key)
        formatted = _format_slack_reply(rendered_status)
        if ts and await _update_slack_message(client, channel=target, ts=ts, text=formatted):
            return True
        try:
            response = await client.chat_postMessage(channel=target, text=formatted)
            if not _slack_response_ok(response):
                return False
            sent_ts = _slack_response_field(response, "ts")
            if sent_ts:
                status_messages[key] = sent_ts
            return True
        except Exception:
            logger.debug("Slack task card delivery failed", exc_info=True)
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
        reply_source = delivery.text or ""
        formatted_reply = _format_slack_reply(reply_source)
        if delivery.attachments:
            uploaded = await _upload_slack_reply_files(
                client,
                channel=channel,
                paths=delivery.attachments,
                initial_comment=formatted_reply or None,
            )
            receipt = build_platform_delivery_receipt(
                channel="slack",
                target_id=channel,
                delivery=delivery,
                transport_ok=uploaded,
                error=None if uploaded else "attachment_upload_failed",
            )
            record_platform_delivery_receipt(receipt)
            return receipt.status == "succeeded"
        await _post_slack_message_with_plain_fallback(
            client,
            channel=channel,
            formatted_text=formatted_reply or "",
            plain_text=delivery.text or "",
        )
        receipt = build_platform_delivery_receipt(
            channel="slack",
            target_id=channel,
            delivery=delivery,
            transport_ok=True,
        )
        record_platform_delivery_receipt(receipt)
        return receipt.status == "succeeded"
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
        if total_ms < _float_env_ms(_NULLION_SLACK_TURN_SLOW_LOG_MS, default=1000.0):
            return
        channel_id = str(event.get("channel") or "")
        request_id = str(event.get("client_msg_id") or event.get("event_ts") or "")
        message_id = str(event.get("event_ts") or "")
        logger.warning(
            "slack turn slow timing channel=%s request_id=%s message_id=%s outcome=%s total_ms=%.1f phases=%s",
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
                    surface="slack",
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
                        event_type="slack.turn_timing",
                        actor="slack",
                        payload={
                            "request_id": request_id,
                            "message_id": message_id,
                            "channel_id": channel_id,
                            "outcome": outcome,
                            "total_ms": round(total_ms, 1),
                            "phases": timing_marks,
                            "platform": "slack",
                        },
                    )
                )
        except Exception:
            logger.debug("Could not record Slack turn timing event", exc_info=True)

    turn_outcome = "completed"
    _mark_timing("received")
    text = _normalize_slack_text(str(event.get("text") or ""))
    user_id = str(event.get("user") or "").strip()
    if not user_id or event.get("bot_id") or event.get("subtype") == "bot_message":
        turn_outcome = "ignored"
        _log_turn_timing(turn_outcome)
        return
    try:
        attachments = await _download_slack_attachments(event, bot_token=settings.slack.bot_token, settings=settings)
    except TypeError:
        attachments = await _download_slack_attachments(event, bot_token=settings.slack.bot_token)
    _mark_timing("attachments_downloaded")
    if not text and not attachments:
        turn_outcome = "empty"
        _log_turn_timing(turn_outcome)
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
        await say(stop_session_reply(stop_result))
        turn_outcome = "stop_command"
        _log_turn_timing(turn_outcome)
        return

    working_channel = str(event.get("channel") or "").strip()
    working_ts = None
    loop = asyncio.get_running_loop()
    tool_working_ack_sent = False

    def emit_working_ack_for_tool_activity(event: dict[str, object]) -> None:
        nonlocal tool_working_ack_sent, working_ts, working_channel
        event_id = str(event.get("id") or "")
        event_tool_name = str(event.get("tool_name") or "")
        if (
            tool_working_ack_sent
            or client is None
            or not working_channel
            or not (event_id.startswith("tool-") or event_id == "mini-agents" or event_tool_name)
        ):
            return
        tool_working_ack_sent = True

        async def _send() -> None:
            nonlocal working_ts, working_channel
            try:
                response = await say(_WORKING_ACK_TEXT)
                working_ts = _slack_response_field(response, "ts")
                working_channel = _slack_response_field(response, "channel") or working_channel
            except Exception:
                logger.debug("Slack working message send failed", exc_info=True)

        try:
            asyncio.run_coroutine_threadsafe(_send(), loop)
        except Exception:
            logger.debug("Slack working message scheduling failed", exc_info=True)

    _mark_timing("working_message")
    text_streamer = _SlackTextDeltaStreamer(
        loop=asyncio.get_running_loop(),
        client=client,
        channel=working_channel,
        ts=working_ts,
    )

    turn_registration = await GLOBAL_TURN_DISPATCH_TRACKER.register(
        ingress.operator_chat_id,
        ingress.text,
        turn_id=ingress.message_id or ingress.request_id,
        model_client=getattr(service, "model_client", None),
    )
    _mark_timing("turn_registered")
    try:
        await turn_registration.wait_for_dependencies()
        _mark_timing("wait_dependencies")
        turn_result = await asyncio.to_thread(
            _handle_messaging_ingress_result_with_dispatch,
            service,
            ingress,
            turn_dispatch_decision=turn_registration.decision,
            text_delta_callback=text_streamer.emit,
            activity_callback=emit_working_ack_for_tool_activity,
        )
        _mark_timing("handler_completed")
        if await turn_registration.is_superseded():
            if working_ts:
                await _update_slack_message(client, channel=working_channel, ts=working_ts, text="Updated by your follow-up.")
            turn_outcome = "superseded"
            _log_turn_timing(turn_outcome)
            return
        reply = turn_result.reply
        principal_id = principal_id_for_messaging_identity("slack", user_id, settings)
        delivery = prepare_reply_for_platform_delivery(
            reply,
            principal_id=principal_id,
            delivery_contract=turn_result.delivery_contract,
        )
        if getattr(turn_result, "reply_already_sent", False) and not delivery.attachments:
            await text_streamer.finish(reply)
            _record_slack_delivery_receipt(
                channel=working_channel,
                delivery=delivery,
                transport_ok=True,
                request_id=ingress.request_id,
                message_id=ingress.message_id,
            )
            _mark_timing("delivery_complete")
            _log_turn_timing(turn_outcome)
            return
        reply_source = delivery.text or ""
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
                _mark_timing("delivery_complete")
                _log_turn_timing(turn_outcome)
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
            reply_source = platform_delivery_failure_reply(delivery)
        chunks = _slack_reply_chunks(reply_source, limit=39000)
        first_formatted = chunks[0][0] if chunks else ""
        if working_ts and first_formatted and await _update_slack_message(client, channel=working_channel, ts=working_ts, text=first_formatted):
            chunks = chunks[1:]
        for formatted_chunk, plain_chunk in chunks:
            await _send_slack_callable_with_plain_fallback(say, formatted_text=formatted_chunk, plain_text=plain_chunk)
        if not delivery_receipt_recorded:
            _record_slack_delivery_receipt(
                channel=working_channel,
                delivery=delivery,
                transport_ok=True,
                request_id=ingress.request_id,
                message_id=ingress.message_id,
            )
        _mark_timing("delivery_complete")
        _log_turn_timing(turn_outcome)
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

    turn_result = await asyncio.to_thread(
        _handle_messaging_ingress_result_with_dispatch,
        service,
        ingress,
    )
    reply = turn_result.reply
    chunks = _slack_reply_chunks(reply or "", limit=39000)
    if not chunks:
        await respond("Done.")
        return
    for formatted_chunk, plain_chunk in chunks:
        await _send_slack_callable_with_plain_fallback(respond, formatted_text=formatted_chunk, plain_text=plain_chunk)


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
    try:
        from nullion.startup_warmup import schedule_chat_startup_warmup

        schedule_chat_startup_warmup(
            service.runtime,
            registry=getattr(service, "tool_registry", None),
            settings=settings,
            surface="slack",
        )
    except Exception:
        logger.debug("Could not schedule Slack chat startup warmup", exc_info=True)

    app = AsyncApp(token=bot_token, signing_secret=settings.slack.signing_secret)
    _task_card_store = PlatformTaskCardStore(platform_activity_capabilities("slack"))
    _status_messages: dict[tuple[str, str], str] = {}
    _status_locks: dict[tuple[str, str], asyncio.Lock] = {}

    async def _slack_deliver_fn(conversation_id: str, text: str, **kwargs) -> bool:
        prefix, _, channel = str(conversation_id or "").partition(":")
        if prefix != "slack" or not channel:
            return False
        if kwargs.get("is_status"):
            return await _deliver_slack_task_status(
                client=app.client,
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
        outbound_text = f"MEDIA:{text}" if kwargs.get("is_artifact") else text
        return await send_slack_platform_delivery(bot_token=bot_token, channel=channel, text=outbound_text)

    if getattr(service, "agent_orchestrator", None) is not None and hasattr(service.agent_orchestrator, "set_deliver_fn"):
        service.agent_orchestrator.set_deliver_fn(_slack_deliver_fn)

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
