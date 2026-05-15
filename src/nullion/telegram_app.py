"""Telegram polling service primitives for Nullion."""

from __future__ import annotations

import asyncio
from collections import defaultdict
import copy
from dataclasses import dataclass, field, fields, replace
from datetime import UTC, datetime
import inspect
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from nullion.approval_context import approval_trigger_flow_label
from nullion.approval_display import (
    approval_display_from_request,
    approval_inline_code,
    approval_title_for,
    format_approval_detail_markdown,
)
from nullion.approvals import ApprovalStatus
from nullion.approval_markers import split_tool_approval_marker, strip_tool_approval_marker
from nullion.artifacts import is_safe_artifact_path, split_media_reply_attachments
from nullion.chat_attachments import VIDEO_EXTENSIONS, is_supported_chat_file
from nullion.events import make_event
from nullion.latency_phases import record_surface_latency_timing
from nullion.config import NullionSettings, web_session_allow_duration_label, web_session_allow_expires_at
from nullion.doctor_playbooks import execute_doctor_playbook_command
from nullion.messaging_adapters import (
    DeliveryContract,
    build_platform_delivery_receipt,
    platform_delivery_failure_reply,
    principal_id_for_messaging_identity,
    prepare_reply_for_platform_delivery,
    record_platform_delivery_receipt,
    retry_messaging_delivery_operation,
    sanitize_external_inline_markup,
    save_messaging_attachment,
    split_reply_for_platform_delivery,
)
from nullion.telegram_turn_graph import plan_telegram_post_run_delivery
from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker, TurnDispatchDecision
from nullion.policy import permission_scope_principal
from nullion.chat_streaming import (
    ChatStreamingMode,
    TELEGRAM_CHAT_CAPABILITIES,
    iter_chat_text_chunks,
    select_chat_streaming_mode,
)
from nullion.doctor_actions import (
    CANCELLED as DOCTOR_ACTION_CANCELLED,
    COMPLETED as DOCTOR_ACTION_COMPLETED,
    FAILED as DOCTOR_ACTION_FAILED,
    PENDING as DOCTOR_ACTION_PENDING,
)
from nullion.platform_activity import (
    PlatformTaskCardStore,
    platform_activity_capabilities,
    should_deliver_task_status,
)
from nullion.run_activity import RunActivityPhase, classify_run_activity_phase, task_planner_feed_mode
from nullion.health import HealthIssueType
from nullion.operator_commands import (
    chat_model_option_for_token,
    chat_model_options,
    handle_operator_command,
    is_operator_command_text,
    is_stop_command_text,
    parse_planner_command,
    telegram_bot_command_menu,
)
from nullion.session_stop import stop_session_async, stop_session_reply
from nullion.remediation import remediation_buttons_for_recommendation_code
from nullion.users import resolve_messaging_user
from nullion.model_clients import (
    ModelClientConfigurationError,
    build_model_client_from_settings,
)
from nullion.agent_orchestrator import AgentOrchestrator
from nullion.builder import builder_proposal_acceptance_benefit
from nullion.runtime import PersistentRuntime, format_doctor_diagnosis_for_operator
from nullion.runtime_persistence import load_runtime_store
from nullion.telegram_formatting import format_telegram_text
from nullion.telegram_transport import build_telegram_bot, configure_telegram_application_builder, telegram_request_timeout_kwargs
from nullion.chat_operator import (
    _chat_model_issue_reply,
    _chat_prompt_for_message,
    activity_trace_enabled_for_chat,
    chat_streaming_enabled_for_chat,
    chat_streaming_status_text_for_chat,
    _is_authorized_chat,
    _local_chat_reply_body,
    handle_chat_operator_message,
    resume_approved_telegram_request,
    set_chat_streaming_enabled_for_chat,
    set_verbose_mode_for_chat,
    verbose_mode_status_text_for_chat,
)

try:
    from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import Application, CallbackQueryHandler, MessageHandler, filters
except ImportError:  # pragma: no cover - exercised only when dependency missing
    Application = None
    BotCommand = None
    CallbackQueryHandler = None
    InlineKeyboardButton = None
    InlineKeyboardMarkup = None
    MessageHandler = None
    filters = None


TELEGRAM_BOT_COMMANDS: tuple[tuple[str, str], ...] = telegram_bot_command_menu(include_private_aliases=False)


def build_telegram_bot_commands() -> list[object]:
    if BotCommand is None:
        return []
    return [BotCommand(command, description) for command, description in TELEGRAM_BOT_COMMANDS]


async def register_telegram_bot_commands(bot) -> bool:
    commands = build_telegram_bot_commands()
    if not commands:
        return False
    await bot.set_my_commands(commands)
    return True


def _get_telegram_channel_label(message, chat_id_text: str) -> str:
    if message is None:
        return f"Telegram · {chat_id_text}"
    from_user = getattr(message, "from_user", None)
    if from_user is not None:
        first = getattr(from_user, "first_name", None) or ""
        last = getattr(from_user, "last_name", None) or ""
        name = f"{first} {last}".strip()
        if name:
            return f"Telegram · {name}"
        uname = getattr(from_user, "username", None)
        if uname:
            return f"Telegram · @{uname}"
    return f"Telegram · {chat_id_text}"


def _message_text_or_caption(message) -> str | None:
    if message is None:
        return None
    text = getattr(message, "text", None)
    if text is not None:
        return text
    return getattr(message, "caption", None)


def _telegram_reply_context(message) -> dict[str, object] | None:
    replied = getattr(message, "reply_to_message", None)
    if replied is None:
        return None
    context: dict[str, object] = {
        "platform": "telegram",
        "reply_to_message_id": getattr(replied, "message_id", None),
    }
    replied_text = _message_text_or_caption(replied)
    if isinstance(replied_text, str) and replied_text.strip():
        context["reply_to_text"] = replied_text.strip()
    replied_user = getattr(replied, "from_user", None)
    if replied_user is not None:
        is_bot = getattr(replied_user, "is_bot", None)
        if is_bot is not None:
            context["reply_to_from_bot"] = bool(is_bot)
        username = getattr(replied_user, "username", None)
        if isinstance(username, str) and username.strip():
            context["reply_to_username"] = username.strip()
    return {key: value for key, value in context.items() if value is not None}


def _telegram_request_id(update) -> str | None:
    update_id = getattr(update, "update_id", None)
    if update_id is None:
        return None
    return f"telegram-update:{update_id}"


def _telegram_message_id(*, message, chat_id: str | None) -> str | None:
    message_id = getattr(message, "message_id", None)
    if message_id is None:
        return None
    if chat_id:
        return f"telegram-message:{chat_id}:{message_id}"
    return f"telegram-message:{message_id}"


def _ingress_dedupe_key(*, request_id: str | None, message_id: str | None) -> str | None:
    if request_id:
        return request_id
    return message_id



def _is_safe_media_attachment_path(attachment_path: Path) -> bool:
    return is_safe_artifact_path(attachment_path)


def _split_reply_attachments(reply: str, *, principal_id: str | None = None) -> tuple[str | None, tuple[Path, ...]]:
    if principal_id is not None:
        return split_reply_for_platform_delivery(reply, principal_id=principal_id)
    return split_media_reply_attachments(reply, is_safe_attachment_path=_is_safe_media_attachment_path)


def _principal_id_for_telegram_message(message, settings: NullionSettings | None) -> str:
    chat = None if message is None else getattr(message, "chat", None)
    chat_id = None if chat is None else getattr(chat, "id", None)
    return principal_id_for_messaging_identity("telegram", chat_id, settings)


_TELEGRAM_ATTACHMENT_CAPTION_LIMIT = 1024


def _telegram_document_timeout_kwargs() -> dict[str, float | int]:
    timeouts = telegram_request_timeout_kwargs()
    kwargs: dict[str, float | int] = {}
    for key in ("connect_timeout", "read_timeout", "pool_timeout"):
        value = timeouts.get(key)
        if value is not None:
            kwargs[key] = value
    media_write_timeout = timeouts.get("media_write_timeout") or timeouts.get("write_timeout")
    if media_write_timeout is not None:
        kwargs["write_timeout"] = media_write_timeout
    return kwargs


async def _reply_document_attachment(message, attachment_path: Path, **kwargs) -> None:
    delivery_kwargs = {**_telegram_document_timeout_kwargs(), **kwargs}

    async def operation() -> object:
        with attachment_path.open("rb") as document:
            return await message.reply_document(document, **delivery_kwargs)

    await retry_messaging_delivery_operation(operation, attempts=1)


def _telegram_attachment_caption_kwargs(caption: str | None) -> tuple[str | None, dict[str, Any], bool]:
    if caption is None:
        return None, {}, False
    formatted_caption, caption_kwargs = format_telegram_text(caption)
    if len(formatted_caption) <= _TELEGRAM_ATTACHMENT_CAPTION_LIMIT:
        return formatted_caption, caption_kwargs, False
    return None, {}, True


async def _send_telegram_delivery_failure(message, delivery, *, do_quote: bool) -> None:
    failure_text = platform_delivery_failure_reply(delivery)
    formatted_failure, failure_kwargs = format_telegram_text(failure_text)
    reply_text = getattr(message, "reply_text", None)
    if reply_text is not None:
        await _reply_text_in_chunks_with_plain_fallback(
            message,
            formatted_failure,
            failure_text,
            do_quote=do_quote,
            **failure_kwargs,
        )
        return
    edit_text = getattr(message, "edit_text", None)
    if edit_text is not None:
        await _edit_text_with_plain_fallback(edit_text, formatted_failure, failure_text, **failure_kwargs)
        return
    raise AttributeError("Telegram message object has no reply_text or edit_text method")


async def _send_telegram_delivery_failure_safely(message, delivery, *, do_quote: bool, stage: str) -> bool:
    try:
        await _send_telegram_delivery_failure(message, delivery, do_quote=do_quote)
        return True
    except Exception:
        logger.warning("Could not deliver Telegram %s failure notice", stage, exc_info=True)
        return False


async def _answer_callback_query_safely(callback_query, *args, **kwargs) -> bool:
    try:
        await retry_messaging_delivery_operation(lambda: callback_query.answer(*args, **kwargs))
        return True
    except Exception:
        logger.warning("Could not answer Telegram callback query after retries", exc_info=True)
        return False



logger = logging.getLogger(__name__)
_TYPING_KEEPALIVE_INTERVAL_SECONDS = 2.0
_TELEGRAM_ATTACHMENT_DOWNLOAD_ATTEMPTS = 3
_TELEGRAM_MESSAGE_CHUNK_SIZE = 3900
_TELEGRAM_BOT_TOKEN_PATTERN = re.compile(r"^\d{6,}:[A-Za-z0-9_-]{20,}$")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_NULLION_TELEGRAM_FLOW_SLOW_LOG_MS = "NULLION_TELEGRAM_FLOW_SLOW_LOG_MS"
_NULLION_TELEGRAM_TURN_SLOW_LOG_MS = "NULLION_TELEGRAM_TURN_SLOW_LOG_MS"
_NULLION_TELEGRAM_DELIVERY_SLOW_LOG_MS = "NULLION_TELEGRAM_DELIVERY_SLOW_LOG_MS"
_NULLION_TELEGRAM_CHECKPOINT_REFRESH_MIN_INTERVAL_MS = "NULLION_TELEGRAM_CHECKPOINT_REFRESH_MIN_INTERVAL_MS"
_CHECKPOINT_FILE_SIGNATURES: dict[str, tuple[int, int] | None] = {}
_CHECKPOINT_REFRESH_ATTEMPT_AT: dict[str, float] = {}


def _float_env_ms(name: str, *, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        return default
_ACTIVE_TASK_STATUS_PREFIXES = ("☐", "◐", "▤")


def _checkpoint_file_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return stat.st_mtime_ns, stat.st_size


def _cache_runtime_checkpoint_signature(runtime: PersistentRuntime) -> None:
    checkpoint_path = getattr(runtime, "checkpoint_path", None)
    if not checkpoint_path:
        return
    path = Path(checkpoint_path)
    signature = _checkpoint_file_signature(path)
    if signature is None:
        return
    _CHECKPOINT_FILE_SIGNATURES[str(path)] = signature
    try:
        runtime.last_checkpoint_file_signature = signature
    except Exception:
        pass


def _mark_runtime_checkpoint_signature_stale(runtime: PersistentRuntime) -> None:
    try:
        runtime.last_checkpoint_file_signature = None
    except Exception:
        pass
    try:
        runtime.last_checkpoint_fingerprint = None
    except Exception:
        pass


def _attachments_include_video(attachments: list[dict[str, str]] | None) -> bool:
    for attachment in attachments or []:
        media_type = str(attachment.get("media_type") or "").strip().lower()
        name = str(attachment.get("name") or attachment.get("path") or "")
        if media_type.startswith("video/") or Path(name).suffix.lower() in VIDEO_EXTENSIONS:
            return True
    return False


def _split_telegram_message_chunks(text: str, *, limit: int = _TELEGRAM_MESSAGE_CHUNK_SIZE) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        split_at = max(remaining.rfind("\n\n", 0, limit), remaining.rfind("\n", 0, limit))
        if split_at < max(1, limit // 2):
            split_at = remaining.rfind(" ", 0, limit)
        if split_at < max(1, limit // 2):
            split_at = limit
        chunk = remaining[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].strip()
    if remaining:
        chunks.append(remaining)
    return chunks


async def _reply_text_in_chunks(message, text: str, *, do_quote: bool, **kwargs) -> None:
    reply_text = getattr(message, "reply_text", None)
    if reply_text is None:
        raise AttributeError("Telegram message object has no reply_text method")
    chunks = _split_telegram_message_chunks(text)
    if len(chunks) == 1:
        await retry_messaging_delivery_operation(
            lambda: reply_text(text, do_quote=do_quote, **kwargs)
        )
        return
    # Long resumed tool output often contains raw command results. Send chunks as
    # plain text so HTML/Markdown tags cannot be split across message boundaries.
    safe_kwargs = {key: value for key, value in kwargs.items() if key not in {"parse_mode"}}
    for index, chunk in enumerate(chunks):
        await retry_messaging_delivery_operation(
            lambda chunk=chunk, index=index: reply_text(
                chunk,
                do_quote=do_quote if index == 0 else False,
                **safe_kwargs,
            )
        )


def _without_parse_mode(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in kwargs.items() if key != "parse_mode"}


def _is_telegram_parse_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return (
        exc.__class__.__name__ == "BadRequest"
        and (
            "parse entities" in text
            or "can't parse" in text
            or "unmatched" in text
            or "can't find end tag" in text
        )
    )


def _is_telegram_message_not_modified_error(exc: BaseException) -> bool:
    return exc.__class__.__name__ == "BadRequest" and "message is not modified" in str(exc).lower()


def _telegram_plain_format_fallback_text(plain_text: str) -> tuple[str, dict[str, str]]:
    fallback = (
        "Telegram could not render the formatted reply, so here is the same text as plain output:\n\n"
        "```text\n"
        f"{plain_text}"
        "\n```"
    )
    return sanitize_external_inline_markup(fallback), {}


async def _reply_text_in_chunks_with_plain_fallback(
    message,
    formatted_text: str,
    plain_text: str,
    *,
    do_quote: bool,
    **kwargs,
) -> None:
    if "parse_mode" in kwargs and len(formatted_text) > _TELEGRAM_MESSAGE_CHUNK_SIZE:
        reply_text = getattr(message, "reply_text", None)
        if reply_text is None:
            raise AttributeError("Telegram message object has no reply_text method")
        plain_chunks = _split_telegram_message_chunks(plain_text, limit=3000)
        index = 0
        while index < len(plain_chunks):
            plain_chunk = plain_chunks[index]
            chunk_text, chunk_kwargs = format_telegram_text(plain_chunk)
            if len(chunk_text) > _TELEGRAM_MESSAGE_CHUNK_SIZE and len(plain_chunk) > 1:
                plain_chunks[index:index + 1] = _split_telegram_message_chunks(
                    plain_chunk,
                    limit=max(1, len(plain_chunk) // 2),
                )
                continue
            chunk_kwargs = {
                **_without_parse_mode(kwargs),
                **chunk_kwargs,
            }
            try:
                await retry_messaging_delivery_operation(
                    lambda chunk_text=chunk_text, index=index, chunk_kwargs=chunk_kwargs: (
                        reply_text(
                            chunk_text,
                            do_quote=do_quote if index == 0 else False,
                            **chunk_kwargs,
                        )
                    )
                )
            except Exception as exc:
                if "parse_mode" not in chunk_kwargs or not _is_telegram_parse_error(exc):
                    raise
                logger.warning("Telegram rejected formatted chunk; retrying as plain text.", exc_info=True)
                await retry_messaging_delivery_operation(
                    lambda plain_chunk=plain_chunk, index=index, chunk_kwargs=chunk_kwargs: (
                        reply_text(
                            sanitize_external_inline_markup(plain_chunk),
                            do_quote=do_quote if index == 0 else False,
                            **_without_parse_mode(chunk_kwargs),
                        )
                    )
                )
            index += 1
        return
    try:
        await _reply_text_in_chunks(message, formatted_text, do_quote=do_quote, **kwargs)
    except Exception as exc:
        if "parse_mode" not in kwargs or not _is_telegram_parse_error(exc):
            raise
        logger.warning("Telegram rejected formatted text; retrying as plain text.", exc_info=True)
        fallback_text, fallback_kwargs = _telegram_plain_format_fallback_text(plain_text)
        fallback_delivery_kwargs = {**_without_parse_mode(kwargs), **fallback_kwargs}
        try:
            await _reply_text_in_chunks(
                message,
                fallback_text,
                do_quote=do_quote,
                **fallback_delivery_kwargs,
            )
        except Exception as fallback_exc:
            if "parse_mode" not in fallback_delivery_kwargs or not _is_telegram_parse_error(fallback_exc):
                raise
            await _reply_text_in_chunks(
                message,
                sanitize_external_inline_markup(plain_text),
                do_quote=do_quote,
                **_without_parse_mode(fallback_delivery_kwargs),
            )


async def _edit_text_with_plain_fallback(
    edit_text,
    formatted_text: str,
    plain_text: str,
    **kwargs,
) -> None:
    try:
        await retry_messaging_delivery_operation(
            lambda: edit_text(formatted_text, **kwargs)
        )
    except Exception as exc:
        if "parse_mode" not in kwargs or not _is_telegram_parse_error(exc):
            raise
        logger.warning("Telegram rejected formatted edit; retrying as plain text.", exc_info=True)
        fallback_text, fallback_kwargs = _telegram_plain_format_fallback_text(plain_text)
        fallback_delivery_kwargs = {**_without_parse_mode(kwargs), **fallback_kwargs}
        try:
            await retry_messaging_delivery_operation(
                lambda: edit_text(
                    fallback_text,
                    **fallback_delivery_kwargs,
                )
            )
        except Exception as fallback_exc:
            if "parse_mode" not in fallback_delivery_kwargs or not _is_telegram_parse_error(fallback_exc):
                raise
            await retry_messaging_delivery_operation(
                lambda: edit_text(
                    sanitize_external_inline_markup(plain_text),
                    **_without_parse_mode(fallback_delivery_kwargs),
                )
            )


async def _reply_text_with_streaming_edits(message, text: str, *, do_quote: bool, **kwargs) -> bool:
    reply_text = getattr(message, "reply_text", None)
    if reply_text is None:
        return False
    chunks = list(iter_chat_text_chunks(text, max_chars=96))
    if len(chunks) <= 1:
        return False
    safe_kwargs = {key: value for key, value in kwargs.items() if key not in {"parse_mode"}}
    sent = await retry_messaging_delivery_operation(
        lambda: reply_text(chunks[0], do_quote=do_quote, **safe_kwargs)
    )
    edit_text = getattr(sent, "edit_text", None)
    if edit_text is None:
        for chunk in chunks[1:]:
            await retry_messaging_delivery_operation(
                lambda chunk=chunk: reply_text(chunk, do_quote=False, **safe_kwargs)
            )
        return True
    rendered = chunks[0]
    for chunk in chunks[1:]:
        rendered += chunk
        await retry_messaging_delivery_operation(
            lambda rendered=rendered: edit_text(rendered, **safe_kwargs)
        )
        await asyncio.sleep(0.02)
    return True


class _TelegramTextDeltaStreamer:
    def __init__(self, *, loop, message) -> None:
        self._loop = loop
        self._message = message
        self._sent_message = None
        self._parts: list[str] = []
        self._last_update_at = 0.0
        self._last_text = ""

    @property
    def text(self) -> str:
        return "".join(self._parts)

    async def _send_or_edit(self, text: str) -> bool:
        rendered = sanitize_external_inline_markup(text or "")
        if self._sent_message is None:
            reply_text = getattr(self._message, "reply_text", None)
            if reply_text is None:
                return False
            self._sent_message = await retry_messaging_delivery_operation(
                lambda: reply_text(rendered, do_quote=_should_quote_reply(_message_text_or_caption(self._message)))
            )
            return True
        edit_text = getattr(self._sent_message, "edit_text", None)
        if edit_text is None:
            return False
        await retry_messaging_delivery_operation(lambda: edit_text(rendered))
        return True

    def emit(self, delta: str) -> None:
        if not delta or self._message is None:
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
            logger.debug("Telegram text streaming update failed", exc_info=True)

    async def finish(self, final_text: str | None) -> bool:
        text = str(final_text or "")
        if not text or self._sent_message is None:
            return False
        if self._last_text == text:
            return True
        return await self._send_or_edit(text)


def _is_valid_telegram_bot_token(value: str) -> bool:
    return bool(_TELEGRAM_BOT_TOKEN_PATTERN.fullmatch(value.strip()))


def _operator_visible_error(exc: BaseException) -> str:
    """Return a short, non-secret error message suitable for Telegram."""
    raw = str(exc).strip()
    lowered = raw.lower()
    if "cloudflare" in lowered or "<html" in lowered or "challenge" in lowered:
        detail = "The configured model endpoint returned a browser challenge instead of an API response."
    elif "event loop is closed" in lowered:
        detail = "Telegram's network client needs to reconnect."
    else:
        detail = _HTML_TAG_RE.sub("", raw)
        detail = " ".join(detail.split())
        if len(detail) > 220:
            detail = detail[:217].rstrip() + "..."
        if not detail:
            detail = exc.__class__.__name__
    return (
        "I received your message, but the runtime hit an error before I could answer.\n\n"
        f"{detail}\n\n"
        "Try /restart, or check the web dashboard health panel."
    )


@dataclass(slots=True)
class DecisionSnapshot:
    pending_approval_ids: frozenset[str]
    pending_builder_proposal_ids: frozenset[str]
    pending_doctor_action_ids: frozenset[str]


@dataclass(slots=True)
class DecisionCard:
    text: str
    reply_markup: object | None
    # When True this card is sent as a follow-up *after* the agent reply rather
    # than replacing it.  Use for doctor-action notifications so that the agent's
    # actual answer is never silently discarded.
    supplemental: bool = False


_CLOSED_DOCTOR_ACTION_STATUSES = {
    DOCTOR_ACTION_COMPLETED,
    DOCTOR_ACTION_CANCELLED,
    DOCTOR_ACTION_FAILED,
}
_TELEGRAM_NOTIFIED_DOCTOR_ACTION_IDS: set[str] = set()
_TELEGRAM_DOCTOR_NOTIFICATION_SEVERITIES = frozenset({"medium", "high", "critical"})


def _should_notify_telegram_doctor_action(action: dict[str, object]) -> bool:
    severity = str(action.get("severity") or "").strip().lower()
    return severity in _TELEGRAM_DOCTOR_NOTIFICATION_SEVERITIES


def _capture_decision_snapshot(runtime: PersistentRuntime) -> DecisionSnapshot:
    return DecisionSnapshot(
        pending_approval_ids=frozenset(
            approval.approval_id
            for approval in runtime.store.list_approval_requests()
            if approval.status is ApprovalStatus.PENDING
        ),
        pending_builder_proposal_ids=frozenset(
            proposal.proposal_id
            for proposal in runtime.list_pending_builder_proposals()
        ),
        pending_doctor_action_ids=frozenset(
            str(action["action_id"])
            for action in runtime.store.list_doctor_actions()
            if str(action.get("status")) == DOCTOR_ACTION_PENDING
        ),
    )


_CALLBACK_KIND_CODES = {
    "approval": "a",
    "proposal": "p",
    "doctor": "d",
    "nav": "n",
    "setting": "s",
    "reminder": "rm",    # Reminder confirmation card (cancel / edit-time)
    "suggestion": "sg",  # Safe-alternative quick-reply buttons
    "model": "m",        # Chat model selection card
}
_CALLBACK_KIND_CODES_REVERSE = {value: key for key, value in _CALLBACK_KIND_CODES.items()}
_CALLBACK_ACTION_CODES = {
    "approve": "ap",
    "reject": "rj",
    "allow_session": "as",
    "allow_once": "ao",
    "always_allow": "aa",
    "deny": "dn",
    "accept": "ac",
    "review": "rv",
    "archive": "ar",
    "start": "st",
    "complete": "cp",
    "cancel": "cx",
    "doctor:retry_model_api": "drm",
    "doctor:switch_fallback_model": "dsf",
    "doctor:pause_chat": "dpc",
    "doctor:reconnect_telegram": "drt",
    "doctor:restart_bot": "drb",
    "doctor:restart_plugin": "drp",
    "doctor:disable_plugin": "ddp",
    "doctor:reconnect_slack": "dsl",
    "doctor:restart_slack_adapter": "dsa",
    "doctor:reconnect_discord": "ddc",
    "doctor:restart_discord_adapter": "dda",
    "doctor:inspect_run": "dir",
    "doctor:cancel_run": "dcr",
    "doctor:retry_workflow": "drw",
    "doctor:open_schedule": "dos",
    "doctor:disable_task": "ddt",
    "doctor:review_approvals": "dra",
    "doctor:clear_stale_approvals": "dca",
    "doctor:run_diagnosis": "drd",
    "doctor:create_backup": "dcb",
    "doctor:repair_checkpoint": "drc",
    "doctor:retry_later": "drl",
    "show": "sh",
    "set": "stg",
    "edit_time": "et",  # Reminder: edit the scheduled time
    "send": "se",       # Suggestion: send the suggestion as a new message
    "select": "sl",     # Model selection
}
_CALLBACK_ACTION_CODES_REVERSE = {value: key for key, value in _CALLBACK_ACTION_CODES.items()}


def _build_callback_data(*, kind: str, action: str, record_id: str) -> str:
    kind_token = _CALLBACK_KIND_CODES.get(kind, kind)
    action_token = _CALLBACK_ACTION_CODES.get(action, action)
    return f"d:{kind_token}:{action_token}:{record_id}"


def _parse_callback_data(data: str | None) -> tuple[str, str, str] | None:
    if not isinstance(data, str):
        return None
    prefix, separator, remainder = data.partition(":")
    if prefix not in {"decision", "d"} or not separator:
        return None
    kind_token, separator, remainder = remainder.partition(":")
    if not separator or not kind_token:
        return None
    action_token, separator, record_id = remainder.partition(":")
    if not separator or not action_token or not record_id:
        return None
    kind = _CALLBACK_KIND_CODES_REVERSE.get(kind_token, kind_token)
    action = _CALLBACK_ACTION_CODES_REVERSE.get(action_token, action_token)
    return kind, action, record_id


def _build_decision_markup(
    *,
    kind: str,
    record_id: str,
    actions: tuple[tuple[str, str], ...],
    max_buttons_per_row: int = 2,
):
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    buttons = [
        InlineKeyboardButton(
            label,
            callback_data=_build_callback_data(kind=kind, action=action, record_id=record_id),
        )
        for label, action in actions
    ]
    row_size = max(1, max_buttons_per_row)
    rows = [buttons[index : index + row_size] for index in range(0, len(buttons), row_size)]
    return InlineKeyboardMarkup(rows)


def _doctor_decision_actions(action: dict[str, object]) -> tuple[tuple[str, str], ...]:
    remediation_actions = remediation_buttons_for_recommendation_code(
        str(action.get("recommendation_code") or "")
    )
    if remediation_actions:
        return remediation_actions + (("Mark resolved", "complete"), ("Dismiss", "cancel"))
    return (("Mark in progress", "start"), ("Mark resolved", "complete"), ("Dismiss", "cancel"))


def _builder_proposal_card_text(record) -> str:
    proposal = record.proposal
    summary = str(getattr(proposal, "summary", "") or "").strip()
    if summary and summary[-1] not in ".!?":
        summary += "."
    lines = [
        "Builder suggestion",
        str(getattr(proposal, "title", "") or "Optional improvement").strip(),
    ]
    if summary:
        lines.extend(["", summary])
    lines.extend(["", builder_proposal_acceptance_benefit(proposal), "", "Tap an action below."])
    return "\n".join(line for line in lines if line is not None)


def _doctor_action_is_closed(action: dict[str, object] | None) -> bool:
    return str((action or {}).get("status") or "").lower() in _CLOSED_DOCTOR_ACTION_STATUSES


_HELP_NAV_CATEGORIES = [
    ("💬 Chat", "chat"),
    ("🔔 Reminders", "reminders"),
    ("🌐 Look things up", "search"),
    ("📁 Files & code", "files"),
    ("🔐 Approvals", "approvals"),
    ("⚙️ Settings", "settings"),
    ("📋 All commands", "commands"),
]

_HELP_NAV_DETAIL: dict[str, str] = {
    "chat": (
        "💬 Chat\n\n"
        "Just type naturally — no slash commands needed.\n\n"
        "Examples:\n"
        "• \"What's the weather in NYC?\"\n"
        "• \"Write me a short poem about coffee\"\n"
        "• \"Summarize the last 5 emails about the Q3 report\"\n\n"
        "Type /new to clear the conversation and start fresh."
    ),
    "reminders": (
        "🔔 Reminders\n\n"
        "Just ask in plain language:\n\n"
        "• \"Remind me to call John at 3pm tomorrow\"\n"
        "• \"Set a reminder for Monday morning to review the docs\"\n"
        "• \"Remind me every day at 9am to drink water\"\n\n"
        "I'll send you a message at the right time. "
        "Type \"what reminders do I have?\" to see your list."
    ),
    "search": (
        "🌐 Look things up\n\n"
        "I can search the web or fetch a specific page:\n\n"
        "• \"What's the latest news about OpenAI?\"\n"
        "• \"Fetch the page at stripe.com/docs\"\n"
        "• \"Look up the Python docs for async/await\"\n\n"
        "For web fetches, I'll ask your permission first."
    ),
    "files": (
        "📁 Files & code\n\n"
        "I can read, write, and run commands in your project folder.\n\n"
        "• \"Read my README file\"\n"
        "• \"Create a file called notes.md with today's meeting notes\"\n"
        "• \"Run the tests\"\n\n"
        "Anything that writes or executes will ask for your approval first."
    ),
    "approvals": (
        "🔐 Approvals & permissions\n\n"
        "When I need to do something sensitive (run code, access the web, write files), "
        "I'll ask for your OK first.\n\n"
        "• /approvals — see what's waiting\n"
        "• /grants — see what you've already allowed\n\n"
        "You can approve right from the button, or type \"approve\" if there's only one pending."
    ),
    "settings": (
        "⚙️ Settings & status\n\n"
        "• /health — quick status check\n"
        "• /models — switch the AI model I use\n"
        "• /uptime — how long I've been running\n"
        "• /status — full details on what's happening\n"
        "• /restart — restart the bot cleanly\n"
        "• /backups — view and restore saved state\n"
        "• /version — what version I'm running"
    ),
    "commands": None,  # handled specially — shows the commands text
}


def _build_help_menu_card(*, back: bool = False) -> DecisionCard | None:
    """Build the main help menu as an inline keyboard card."""
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    # Build 2-column keyboard
    buttons: list[list] = []
    row: list = []
    for label, category in _HELP_NAV_CATEGORIES:
        button = InlineKeyboardButton(
            label,
            callback_data=_build_callback_data(kind="nav", action="show", record_id=category),
        )
        row.append(button)
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    markup = InlineKeyboardMarkup(buttons)
    text = (
        "Hey! I'm Nullion — here's what I can do.\n\n"
        "Tap a topic below to learn more, or just type a message to get started:"
    )
    return DecisionCard(text=text, reply_markup=markup)


def _build_help_nav_reply(category: str) -> str:
    """Return detail text for a help menu category tap."""
    from nullion.operator_commands import _HELP_COMMANDS_TEXT  # type: ignore[attr-defined]
    if category == "commands":
        return _HELP_COMMANDS_TEXT
    return _HELP_NAV_DETAIL.get(category) or "Tap a category above to learn more."


def _build_verbose_settings_card(runtime: PersistentRuntime, *, chat_id: str | None) -> DecisionCard | None:
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    status = verbose_mode_status_text_for_chat(runtime, chat_id=chat_id)
    streaming_status = chat_streaming_status_text_for_chat(runtime, chat_id=chat_id)
    buttons = [[
        InlineKeyboardButton(
            "On",
            callback_data=_build_callback_data(kind="setting", action="set", record_id="verbose_on"),
        ),
        InlineKeyboardButton(
            "Off",
            callback_data=_build_callback_data(kind="setting", action="set", record_id="verbose_off"),
        ),
    ], [
        InlineKeyboardButton(
            "Streaming on",
            callback_data=_build_callback_data(kind="setting", action="set", record_id="streaming_on"),
        ),
        InlineKeyboardButton(
            "Streaming off",
            callback_data=_build_callback_data(kind="setting", action="set", record_id="streaming_off"),
        ),
    ]]
    return DecisionCard(
        text=(
            f"Verbose mode is {status}.\n"
            f"Chat streaming is {streaming_status}.\n\n"
            "These settings apply to this session."
        ),
        reply_markup=InlineKeyboardMarkup(buttons),
    )


def _build_models_card(runtime: PersistentRuntime) -> DecisionCard | None:
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    from nullion.operator_commands import _render_models  # type: ignore[attr-defined]
    from nullion.runtime_config import current_runtime_config

    cfg = current_runtime_config(model_client=getattr(runtime, "model_client", None))
    options = chat_model_options(current_provider=cfg.provider, current_model=cfg.model)
    if not options:
        return None
    per_provider_index: dict[str, int] = defaultdict(int)
    rows: list[list] = []
    for option in options[:12]:
        provider = option["provider"]
        index = per_provider_index[provider]
        per_provider_index[provider] = index + 1
        label = f"{provider} · {option['model']}"
        if len(label) > 52:
            label = label[:49] + "..."
        rows.append([
            InlineKeyboardButton(
                label,
                callback_data=_build_callback_data(kind="model", action="select", record_id=f"{provider}.{index}"),
            )
        ])
    return DecisionCard(text=_render_models(runtime), reply_markup=InlineKeyboardMarkup(rows))


def _approval_card_actions(approval) -> tuple[tuple[str, str], ...]:
    tool_name, detail = _approval_card_fields(approval)
    if _approval_is_web_request(tool_name, detail):
        return (
            ("Allow all web domains", "allow_session"),
            ("Allow once", "allow_once"),
            ("Always allow", "always_allow"),
            ("Deny", "deny"),
        )
    return (("Allow once", "allow_once"), ("Always allow", "always_allow"), ("Deny", "deny"))


def _build_approval_markup(*, approval):
    return _build_decision_markup(
        kind="approval",
        record_id=approval.approval_id,
        actions=_approval_card_actions(approval),
    )


def _approval_title(tool_name: str) -> str:
    return approval_title_for(tool_name)


def _approval_description(approval) -> str:
    return approval_display_from_request(approval).detail


def _approval_detail(tool_name: str, tool_detail: str, approval_id: str) -> str:
    detail = tool_detail.strip()
    if detail:
        return detail
    tool = tool_name.strip() or "requested action"
    short_id = approval_id[:8]
    return f"{tool} · request {short_id}" if short_id else tool


def _approval_card_fields(approval) -> tuple[str, str]:
    display = approval_display_from_request(approval)
    return display.label, _approval_detail(display.label, display.detail, getattr(approval, "approval_id", ""))


def _approval_target_url(detail: str) -> str:
    match = re.search(r"https?://[^\s'\"`<>)]*", detail, flags=re.IGNORECASE)
    return match.group(0) if match else ""


def _approval_target_host(detail: str) -> str:
    raw = detail.strip()
    try:
        match = _approval_target_url(raw)
        parsed = urlparse(match or raw)
        host = parsed.hostname or ""
        return re.sub(r"^www\.", "", host) or "this domain"
    except Exception:
        stripped = re.sub(r"^https?://", "", raw, flags=re.IGNORECASE)
        stripped = re.sub(r"^www\.", "", stripped)
        return re.split(r"[/?#\s]", stripped)[0] or "this domain"


def _approval_is_web_request(tool_name: str, detail: str) -> bool:
    haystack = f"{tool_name} {detail}".lower()
    return (
        "outbound_network" in haystack
        or "allow_boundary" in haystack
        or bool(_approval_target_url(detail))
        or "web_fetch" in haystack
        or "web_search" in haystack
        or "web request" in haystack
        or "fetch a web page" in haystack
        or "search the web" in haystack
        or "allow web access" in haystack
        or "web access" in _approval_title(tool_name).lower()
    )


def _approval_copy_for(tool_name: str, detail: str) -> str:
    if _approval_is_web_request(tool_name, detail):
        label = _web_session_allow_duration_label()
        return (
            "Nullion may need a few external sites to finish this request. "
            f"Allow all web domains lasts {label}; choose the web access scope to continue."
        )
    return "Nullion paused before taking this step. Choose whether to allow this once, remember it, or stop here."


def _web_session_allow_expires_at(*, now: datetime | None = None) -> datetime | None:
    return web_session_allow_expires_at(os.environ.get("NULLION_WEB_SESSION_ALLOW_DURATION"), now=now)


def _web_session_allow_duration_label() -> str:
    return web_session_allow_duration_label(os.environ.get("NULLION_WEB_SESSION_ALLOW_DURATION"))


def _approval_card_text(approval) -> str:
    display = approval_display_from_request(approval)
    tool_name, detail = _approval_card_fields(approval)
    lines = [
        display.title,
        _approval_copy_for(tool_name, detail),
    ]
    trigger_label = approval_trigger_flow_label(approval)
    if trigger_label:
        lines.extend(["", f"Triggered by: {approval_inline_code(trigger_label)}"])
    target_url = _approval_target_url(detail)
    if _approval_is_web_request(tool_name, detail) and target_url:
        lines.extend(
            [
                "",
                f"Requested URL: {approval_inline_code(_approval_target_host(target_url))}",
                approval_inline_code(target_url),
            ]
        )
    else:
        lines.extend(["", format_approval_detail_markdown(detail)])
    return "\n".join(lines)


def _doctor_reason_fields(action: dict[str, object]) -> dict[str, str]:
    raw = str(action.get("reason") or action.get("source_reason") or "")
    fields: dict[str, str] = {}
    for part in raw.split(";"):
        key, separator, value = part.partition("=")
        if separator and key.strip():
            fields[key.strip()] = value.strip()
    return fields


def _doctor_detail_text(action: dict[str, object]) -> str:
    fields = _doctor_reason_fields(action)
    detail = (
        fields.get("detail")
        or fields.get("backend_detail")
        or fields.get("error")
        or str(action.get("error") or "").strip()
    )
    source = (fields.get("source") or "").replace("_", " ")
    issue = (fields.get("issue_type") or "").replace("_", " ")
    stage = (fields.get("stage") or "").replace("_", " ")
    parts = [part for part in (source, issue, stage) if part]
    context = " · ".join(parts)
    if detail and context:
        return f"{detail} — {context}"
    return detail or context or str(action.get("summary") or "Doctor action")


def _doctor_title_text(action: dict[str, object]) -> str:
    detail = _doctor_detail_text(action)
    detail_l = detail.lower()
    code = str(action.get("recommendation_code") or "").lower()
    reason = str(action.get("reason") or action.get("source_reason") or "").lower()
    if "insufficient_quota" in detail_l or "exceeded your current quota" in detail_l:
        return "Model quota exhausted"
    if "chat backend" in detail_l or "agent orchestrator error" in detail_l:
        return "Chat backend unavailable"
    if "telegram typing indicator" in detail_l:
        return "Telegram typing indicator failed"
    if code == "monitor_manual_cron":
        return "Manual scheduled task still running"
    if code == "investigate_timeout" or "timeout" in reason:
        return "Workflow timed out"
    if code == "investigate_stall" or "stalled" in reason:
        return "Stalled workflow detected"
    if code == "repair_missing_capsule_reference" or "missing_capsule" in reason:
        return "Missing task reference"
    raw_title = str(action.get("summary") or action.get("title") or action.get("action_id") or "Health item")
    if re.search(r"routed health issue", raw_title, flags=re.IGNORECASE) and detail:
        return detail.split(" — ", 1)[0].strip() or raw_title
    return raw_title


def _doctor_diagnosis_text(action: dict[str, object]) -> str:
    detail = _doctor_detail_text(action)
    detail_l = detail.lower()
    code = str(action.get("recommendation_code") or "").lower()
    reason = str(action.get("reason") or action.get("source_reason") or "").lower()
    if "insufficient_quota" in detail_l or "exceeded your current quota" in detail_l:
        return "The configured model provider rejected the request because the account has no available quota."
    if "chat backend" in detail_l or "agent orchestrator error" in detail_l:
        return "Nullion could not get a model response from the chat backend."
    if "telegram typing indicator" in detail_l:
        return "Nullion tried to send a Telegram typing indicator and Telegram rejected or timed out the request."
    if code == "monitor_manual_cron":
        return "A manually triggered scheduled task has been running longer than expected. Doctor is monitoring it instead of stopping it."
    if code == "investigate_timeout" or "timeout" in reason:
        return "A workflow ran longer than expected and crossed the timeout threshold."
    if code == "investigate_stall" or "stalled" in reason:
        return "A workflow stopped reporting progress, so Doctor marked it as stalled."
    if code == "repair_missing_capsule_reference" or "missing_capsule" in reason:
        return "A scheduled task points at a capsule that no longer exists."
    return detail or str(action.get("summary") or "Doctor found a health issue that needs a decision.")


def _doctor_suggestion_text(action: dict[str, object]) -> str:
    detail_l = _doctor_detail_text(action).lower()
    code = str(action.get("recommendation_code") or "").lower()
    reason = str(action.get("reason") or action.get("source_reason") or "").lower()
    if "insufficient_quota" in detail_l or "exceeded your current quota" in detail_l:
        return "Check provider billing/quota or switch Nullion to a configured model provider with available quota, then retry the request."
    if "chat backend" in detail_l or "agent orchestrator error" in detail_l:
        return "Review the backend error, confirm the configured model works, then retry the chat request."
    if code == "monitor_manual_cron":
        return "Let the run continue if progress is still expected, or inspect the run before deciding whether to cancel or retry."
    if code == "investigate_timeout" or "timeout" in reason:
        return "Inspect recent run activity and logs, then retry only after the timeout cause is understood."
    if code == "investigate_stall" or "stalled" in reason:
        return "Review the stalled run, identify the blocking step, and choose a safe retry or cleanup."
    if code == "repair_missing_capsule_reference" or "missing_capsule" in reason:
        return "Repair the schedule by selecting a valid capsule or remove the stale scheduled task."
    return "Ask Doctor to explain the evidence and suggest the safest repair path."


def _doctor_card_text(action: dict[str, object]) -> str:
    status = str(action.get("status") or "pending").replace("_", " ")
    severity = str(action.get("severity") or "unknown")
    meta = f"{status} · {severity} severity"
    lines = [
        f"Doctor: {_doctor_title_text(action)}",
        meta,
        "",
        "What Doctor saw",
        _doctor_diagnosis_text(action),
        "",
        "Suggested fix",
        _doctor_suggestion_text(action),
    ]
    detail = _doctor_detail_text(action)
    if detail and detail != _doctor_diagnosis_text(action):
        if len(detail) > 260:
            detail = detail[:257].rstrip() + "..."
        lines.extend(["", "Details", detail])
    return "\n".join(lines)


def _build_reminder_confirmation_card(reminder) -> "DecisionCard | None":
    """Build a confirmation card for a newly-set reminder with Cancel / Edit time buttons."""
    from nullion.reminders import format_reminder_due_at

    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    due_at = getattr(reminder, "due_at", None)
    text = getattr(reminder, "text", "")
    task_id = getattr(reminder, "task_id", "")
    time_str = format_reminder_due_at(due_at)
    card_text = f"⏰ Reminder set\n{time_str} — {text}"
    markup = _build_decision_markup(
        kind="reminder",
        record_id=task_id,
        actions=(
            ("❌ Cancel", "cancel"),
            ("🕐 Edit time", "edit_time"),
        ),
    )
    return DecisionCard(text=card_text, reply_markup=markup)


# Phrases that signal the bot is declining a dangerous/risky request.
_REFUSAL_PHRASES: tuple[str, ...] = (
    "i won't", "i will not", "i can't", "i cannot", "i'm not able to",
    "i shouldn't", "i should not", "not going to do that",
    "i won't do that", "i'm going to decline",
)
# Max chars for a suggestion in callback_data.  Total limit is 64 bytes;
# "d:sg:se:" = 8 bytes, leaving 56 for the record_id (suggestion text).
_SUGGESTION_MAX_CHARS = 54


def _extract_safe_alternatives(reply: str) -> list[str]:
    """Legacy prose extractor intentionally disabled.

    Telegram buttons must come from typed product state such as approval,
    doctor, help, or settings actions. Inferring tappable suggestions from model
    prose can turn ordinary bullets, addresses, filenames, or internal details
    into user actions.
    """
    return []


def _build_suggestion_markup(alternatives: list[str]):
    """Build an InlineKeyboardMarkup for safe-alternative suggestion buttons."""
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None or not alternatives:
        return None
    rows = []
    for alt in alternatives:
        label = alt if len(alt) <= 50 else alt[:47] + "..."
        record_id = alt[:_SUGGESTION_MAX_CHARS]
        rows.append([
            InlineKeyboardButton(
                label,
                callback_data=_build_callback_data(kind="suggestion", action="send", record_id=record_id),
            )
        ])
    return InlineKeyboardMarkup(rows)


def _check_new_reminder_card(runtime: "PersistentRuntime", reminder_ids_before: frozenset) -> "DecisionCard | None":
    """Return a reminder confirmation card if a new reminder was just created."""
    new_ids = set(runtime.store.reminders.keys()) - reminder_ids_before
    if not new_ids:
        return None
    task_id = next(iter(new_ids))
    reminder = runtime.store.get_reminder(task_id)
    if reminder is None:
        return None
    return _build_reminder_confirmation_card(reminder)


def _new_decision_card(runtime: PersistentRuntime, before: DecisionSnapshot) -> DecisionCard | None:
    pending_approvals = {
        approval.approval_id: approval
        for approval in runtime.store.list_approval_requests()
        if approval.status is ApprovalStatus.PENDING
    }
    new_approval_ids = sorted(set(pending_approvals) - set(before.pending_approval_ids))
    if new_approval_ids:
        approval = pending_approvals[new_approval_ids[0]]
        return DecisionCard(
            text=_approval_card_text(approval),
            reply_markup=_build_approval_markup(approval=approval),
        )

    pending_builder_proposals = {
        proposal.proposal_id: proposal
        for proposal in runtime.list_pending_builder_proposals()
        if proposal.status == "pending"
    }
    new_proposal_ids = sorted(set(pending_builder_proposals) - set(before.pending_builder_proposal_ids))
    if new_proposal_ids:
        proposal = pending_builder_proposals[new_proposal_ids[0]]
        proposal_id = proposal.proposal_id
        return DecisionCard(
            text=_builder_proposal_card_text(proposal),
            reply_markup=_build_decision_markup(
                kind="proposal",
                record_id=proposal_id,
                actions=(("Review", "review"), ("Approve", "accept"), ("Dismiss", "reject")),
            ),
            supplemental=True,
        )

    pending_doctor_actions = {
        str(action["action_id"]): action
        for action in runtime.store.list_doctor_actions()
        if str(action.get("status")) == DOCTOR_ACTION_PENDING
        and _should_notify_telegram_doctor_action(action)
    }
    new_doctor_action_ids = sorted(set(pending_doctor_actions) - set(before.pending_doctor_action_ids))
    new_doctor_action_ids = [
        action_id for action_id in new_doctor_action_ids if action_id not in _TELEGRAM_NOTIFIED_DOCTOR_ACTION_IDS
    ]
    if new_doctor_action_ids:
        action = pending_doctor_actions[new_doctor_action_ids[0]]
        action_id = str(action["action_id"])
        _TELEGRAM_NOTIFIED_DOCTOR_ACTION_IDS.add(action_id)
        return DecisionCard(
            text=_doctor_card_text(action),
            reply_markup=_build_decision_markup(
                kind="doctor",
                record_id=action_id,
                actions=_doctor_decision_actions(action),
            ),
            supplemental=True,
        )

    return None


def _existing_pending_approval_card(runtime: PersistentRuntime, reply: str | None) -> DecisionCard | None:
    if not isinstance(reply, str):
        return None
    marker = split_tool_approval_marker(reply)
    if marker is not None and marker.approval_id:
        approval = runtime.store.get_approval_request(marker.approval_id)
        if approval is not None and approval.status is ApprovalStatus.PENDING:
            return DecisionCard(
                text=_approval_card_text(approval),
                reply_markup=_build_approval_markup(approval=approval),
            )
    if marker is None and "approval required" not in reply.casefold():
        return None
    pending_approvals = [
        approval
        for approval in runtime.store.list_approval_requests()
        if approval.status is ApprovalStatus.PENDING
    ]
    if len(pending_approvals) != 1:
        return None
    approval = pending_approvals[0]
    return DecisionCard(
        text=_approval_card_text(approval),
        reply_markup=_build_approval_markup(approval=approval),
    )


def _should_disable_web_preview(reply: str) -> bool:
    return reply.startswith("📌 Nullion status") or reply.startswith("✅ Approval inbox")


def _record_telegram_delivery_receipt(message, delivery, *, transport_ok: bool, error: str | None = None) -> None:
    chat = None if message is None else getattr(message, "chat", None)
    chat_id = None if chat is None else getattr(chat, "id", None)
    message_id = None if message is None else getattr(message, "message_id", None)
    record_platform_delivery_receipt(
        build_platform_delivery_receipt(
            channel="telegram",
            target_id=None if chat_id is None else str(chat_id),
            delivery=delivery,
            transport_ok=transport_ok,
            message_id=None if message_id is None else str(message_id),
            error=error,
        )
    )


async def _deliver_reply(
    message,
    reply: str,
    *,
    decision_card: "DecisionCard | None" = None,
    additional_markup=None,
    streaming_mode: ChatStreamingMode = ChatStreamingMode.FINAL_ONLY,
    principal_id: str | None = None,
    allow_attachments: bool | None = None,
    delivery_contract: DeliveryContract | None = None,
    request_id: str | None = None,
    turn_id: str | None = None,
    phase: str = "primary",
) -> None:
    """Send a reply to a Telegram message.

    If *decision_card* is set, its text and reply_markup replace the main reply.
    If *additional_markup* is set (and no decision_card), the main reply text is
    kept but the inline keyboard is appended (used for safe-alternative suggestion
    buttons on refusal replies).
    """
    started_at = time.perf_counter()
    timing_marks: list[str] = []
    timing_last_at = started_at
    message_id = None if message is None else getattr(message, "message_id", None)
    chat = None if message is None else getattr(message, "chat", None)
    chat_id = None if chat is None else getattr(chat, "id", None)
    outcome = "ok"

    def _log_slow_delivery(total_ms: float) -> None:
        if total_ms < _float_env_ms(_NULLION_TELEGRAM_DELIVERY_SLOW_LOG_MS, default=1200.0):
            return
        logger.warning(
            "telegram delivery slow timing %s",
            {
                "chat_id": str(chat_id or ""),
                "message_id": str(message_id or ""),
                "request_id": request_id,
                "turn_id": str(turn_id or ""),
                "phase": phase,
                "outcome": outcome,
                "total_ms": round(total_ms, 1),
                "attachment_count": len(attachment_paths),
                "reply_len": len(reply or ""),
                "phases": ", ".join(timing_marks),
            },
        )

    def _mark_timing(label: str) -> None:
        nonlocal timing_last_at
        now = time.perf_counter()
        timing_marks.append(f"{label}:{round((now - timing_last_at) * 1000, 1)}ms")
        timing_last_at = now

    visible_reply = strip_tool_approval_marker(reply)
    delivery_kwargs: dict[str, object] = {"principal_id": principal_id}
    if allow_attachments is not None:
        delivery_kwargs["allow_attachments"] = allow_attachments
    if delivery_contract is not None:
        delivery_kwargs["delivery_contract"] = delivery_contract
    delivery = prepare_reply_for_platform_delivery(
        visible_reply or "",
        **delivery_kwargs,
    )
    caption = delivery.text
    attachment_paths = delivery.attachments
    _mark_timing("prepared")
    if attachment_paths:
        _mark_timing("delivery_attachment_mode")
        do_quote = _should_quote_reply(_message_text_or_caption(message))
        formatted_caption, caption_kwargs, caption_too_long = _telegram_attachment_caption_kwargs(caption)
        if caption is not None and caption_too_long:
            formatted_text, text_kwargs = format_telegram_text(caption)
            await _reply_text_in_chunks_with_plain_fallback(
                message,
                formatted_text,
                caption,
                do_quote=do_quote,
                **text_kwargs,
            )
            do_quote = False
        try:
            _mark_timing("upload_attachments")
            for index, attachment_path in enumerate(attachment_paths):
                await _reply_document_attachment(
                    message,
                    attachment_path,
                    caption=formatted_caption if index == 0 else None,
                    do_quote=do_quote if index == 0 else False,
                    **(caption_kwargs if index == 0 else {}),
                )
            _record_telegram_delivery_receipt(message, delivery, transport_ok=True)
        except Exception:
            logger.warning("Could not upload Telegram reply attachment", exc_info=True)
            outcome = "attachment_failed"
            _record_telegram_delivery_receipt(
                message,
                delivery,
                transport_ok=False,
                error="attachment_upload_failed",
            )
            await _send_telegram_delivery_failure_safely(
                message,
                delivery,
                do_quote=False,
                stage="attachment upload",
            )
            outcome = "attachment_failed_with_retry"
        _mark_timing("delivery_complete")
        total_ms = (time.perf_counter() - started_at) * 1000
        _log_slow_delivery(total_ms)
        return

    delivery_text = caption or ""
    _mark_timing("delivery_text_mode")
    formatted_reply, reply_kwargs = format_telegram_text(delivery_text)
    if _should_disable_web_preview(delivery_text):
        reply_kwargs = {**reply_kwargs, "disable_web_page_preview": True}
    if decision_card is not None and decision_card.reply_markup is not None:
        delivery_text = decision_card.text
        formatted_reply, card_kwargs = format_telegram_text(decision_card.text)
        reply_kwargs = {
            **card_kwargs,
            "reply_markup": decision_card.reply_markup,
            "disable_web_page_preview": True,
        }
    elif additional_markup is not None:
        # Suggestion mode: keep the LLM's reply text, just attach the inline buttons.
        reply_kwargs = {**reply_kwargs, "reply_markup": additional_markup}
    reply_text = getattr(message, "reply_text", None)
    if reply_text is not None:
        if (
            streaming_mode is ChatStreamingMode.MESSAGE_EDITS
            and decision_card is None
            and additional_markup is None
            and not reply_kwargs
            and len(formatted_reply) <= _TELEGRAM_MESSAGE_CHUNK_SIZE
        ):
            try:
                _mark_timing("streaming_text_retry")
                if await _reply_text_with_streaming_edits(
                    message,
                    formatted_reply,
                    do_quote=_should_quote_reply(_message_text_or_caption(message)),
                ):
                    _mark_timing("delivery_complete")
                    total_ms = (time.perf_counter() - started_at) * 1000
                    _log_slow_delivery(total_ms)
                    return
            except Exception:
                logger.warning("Telegram streaming reply failed; retrying final text delivery.", exc_info=True)
        try:
            _mark_timing("reply_text_fallback")
            await _reply_text_in_chunks_with_plain_fallback(
                message,
                formatted_reply,
                delivery_text,
                do_quote=_should_quote_reply(_message_text_or_caption(message)),
                **reply_kwargs,
            )
        except Exception:
            _record_telegram_delivery_receipt(
                message,
                delivery,
                transport_ok=False,
                error="text_delivery_failed",
            )
            outcome = "text_delivery_failed"
            logger.warning("Could not deliver Telegram text reply after retries", exc_info=True)
            _mark_timing("delivery_complete")
            total_ms = (time.perf_counter() - started_at) * 1000
            _log_slow_delivery(total_ms)
            return
        _record_telegram_delivery_receipt(message, delivery, transport_ok=True)
        _mark_timing("delivery_complete")
        total_ms = (time.perf_counter() - started_at) * 1000
        _log_slow_delivery(total_ms)
        return
    edit_text = getattr(message, "edit_text", None)
    if edit_text is not None:
        try:
            _mark_timing("edit_text_delivery")
            await _edit_text_with_plain_fallback(edit_text, formatted_reply, delivery_text, **reply_kwargs)
        except Exception:
            _record_telegram_delivery_receipt(
                message,
                delivery,
                transport_ok=False,
                error="text_delivery_failed",
            )
            outcome = "text_edit_failed"
            logger.warning("Could not edit Telegram text reply after retries", exc_info=True)
            _mark_timing("delivery_complete")
            total_ms = (time.perf_counter() - started_at) * 1000
            _log_slow_delivery(total_ms)
            return
        _record_telegram_delivery_receipt(message, delivery, transport_ok=True)
        _mark_timing("delivery_complete")
        total_ms = (time.perf_counter() - started_at) * 1000
        _log_slow_delivery(total_ms)
        return
    outcome = "missing_reply_api"
    _mark_timing("delivery_complete")
    total_ms = (time.perf_counter() - started_at) * 1000
    _log_slow_delivery(total_ms)
    raise AttributeError("Telegram message object has no reply_text or edit_text method")


def _log_background_delivery_error(task) -> None:  # noqa: ANN001
    if task.cancelled():
        return
    try:
        task.exception()
    except Exception:
        logger.warning("Background Telegram supplemental delivery failed", exc_info=True)


def _telegram_streaming_mode(runtime: PersistentRuntime, *, chat_id: str | None) -> ChatStreamingMode:
    return select_chat_streaming_mode(
        TELEGRAM_CHAT_CAPABILITIES,
        streaming_enabled=chat_streaming_enabled_for_chat(runtime, chat_id=chat_id),
    )


def _telegram_allows_status_streaming(runtime: PersistentRuntime, *, chat_id: str | None) -> bool:
    _ = (runtime, chat_id)
    return task_planner_feed_mode() != "off"


def _telegram_conversation_id(chat_id: str | None) -> str | None:
    normalized = str(chat_id or "").strip()
    if not normalized:
        return None
    return f"telegram:{normalized}"


def _latest_chat_turn_for_reply(
    runtime: PersistentRuntime,
    *,
    chat_id: str | None,
    reply: str | None,
) -> dict[str, object] | None:
    if not isinstance(reply, str) or not reply.strip():
        return None
    conversation_id = _telegram_conversation_id(chat_id)
    if conversation_id is None:
        return None
    try:
        events = runtime.store.list_conversation_events(conversation_id)
    except Exception:
        logger.debug("Could not inspect conversation events for planner ack suppression", exc_info=True)
        return None
    for event in reversed(events):
        if event.get("event_type") != "conversation.chat_turn":
            continue
        if event.get("assistant_reply") == reply:
            return event
    return None


def _chat_turn_is_running_planner_ack(event: dict[str, object] | None) -> bool:
    if not isinstance(event, dict):
        return False
    tool_results = event.get("tool_results")
    if not isinstance(tool_results, list):
        return False
    saw_planner = False
    saw_running_mini_agents = False
    for result in tool_results:
        if not isinstance(result, dict):
            continue
        tool_name = str(result.get("tool_name") or "")
        status = str(result.get("status") or "")
        if tool_name == "Planner":
            saw_planner = True
        elif tool_name == "Mini-Agents" and status in {"running", "pending"}:
            saw_running_mini_agents = True
    return saw_planner and saw_running_mini_agents


def _chat_turn_planner_status_was_delivered(event: dict[str, object] | None) -> bool:
    if not isinstance(event, dict):
        return False
    tool_results = event.get("tool_results")
    if not isinstance(tool_results, list):
        return False
    for result in tool_results:
        if not isinstance(result, dict) or result.get("tool_name") != "Mini-Agents":
            continue
        output = result.get("output")
        if isinstance(output, dict) and output.get("status_delivered") is True:
            return True
    return False


def _should_suppress_planner_status_ack(
    runtime: PersistentRuntime,
    *,
    chat_id: str | None,
    reply: str | None,
    status_stream_delivered: bool = False,
) -> bool:
    event = _latest_chat_turn_for_reply(runtime, chat_id=chat_id, reply=reply)
    if not _chat_turn_is_running_planner_ack(event):
        return False
    delivered = bool(status_stream_delivered) or _chat_turn_planner_status_was_delivered(event)
    return delivered and (
        activity_trace_enabled_for_chat(runtime, chat_id=chat_id)
        or _telegram_allows_status_streaming(runtime, chat_id=chat_id)
    )


def _should_skip_telegram_primary_reply_delivery(
    *,
    suppress_primary_reply_delivery: bool,
    decision_card: DecisionCard | None,
) -> bool:
    return bool(suppress_primary_reply_delivery and decision_card is None)


def _should_live_stream_telegram_activity(
    runtime: PersistentRuntime,
    *,
    chat_id: str | None,
    message,
    text: str | None,
    planner_status_requested: bool,
) -> bool:
    if message is None or not isinstance(text, str):
        return False
    if planner_status_requested:
        return _telegram_allows_status_streaming(runtime, chat_id=chat_id)
    return (
        activity_trace_enabled_for_chat(runtime, chat_id=chat_id)
        and (
            not is_operator_command_text(text)
            or text.strip().lower().startswith("/chat ")
        )
    )


async def _send_or_edit_telegram_status_message(
    bot,
    status_messages: dict[tuple[str, str], int],
    *,
    chat_id: str,
    group_id: str,
    text: str,
    status_texts: dict[tuple[str, str], str] | None = None,
    status_locks: dict[tuple[str, str], asyncio.Lock] | None = None,
) -> None:
    if bot is None or not chat_id or not group_id or not text:
        return
    key = (chat_id, group_id)
    if status_locks is not None:
        lock = status_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            status_locks[key] = lock
        async with lock:
            await _send_or_edit_telegram_status_message(
                bot,
                status_messages,
                chat_id=chat_id,
                group_id=group_id,
                text=text,
                status_texts=status_texts,
            )
        return
    if status_texts is not None and status_texts.get(key) == text:
        return
    if isinstance(bot, str):
        try:
            from telegram import Bot  # type: ignore[import]
        except Exception:
            logger.debug("Telegram planner status skipped because python-telegram-bot is unavailable", exc_info=True)
            return
        async with build_telegram_bot(Bot, bot) as fresh_bot:
            await _send_or_edit_telegram_status_message(
                fresh_bot,
                status_messages,
                chat_id=chat_id,
                group_id=group_id,
                text=text,
                status_texts=status_texts,
            )
        return
    formatted, kwargs = format_telegram_text(text)
    kwargs = {**kwargs, "disable_web_page_preview": True}
    message_id = status_messages.get(key)
    if message_id is not None:
        try:
            result = await retry_messaging_delivery_operation(
                lambda: bot.edit_message_text(
                    text=formatted,
                    chat_id=chat_id,
                    message_id=message_id,
                    **kwargs,
                )
            )
            if status_texts is not None:
                status_texts[key] = text
            return
        except Exception as exc:
            if "message is not modified" in str(exc).lower():
                if status_texts is not None:
                    status_texts[key] = text
                return
            error_name = exc.__class__.__name__.casefold()
            if "timedout" in error_name or "timed out" in str(exc).casefold():
                raise
            logger.debug("Telegram planner status edit failed; sending replacement status message", exc_info=True)
            status_messages.pop(key, None)
    sent_message = await retry_messaging_delivery_operation(
        lambda: bot.send_message(chat_id, formatted, **kwargs)
    )
    sent_message_id = getattr(sent_message, "message_id", None)
    if sent_message_id is not None:
        status_messages[key] = int(sent_message_id)
    if status_texts is not None:
        status_texts[key] = text


async def _send_or_edit_telegram_task_status_message(
    bot,
    status_messages: dict[tuple[str, str], int],
    *,
    chat_id: str,
    group_id: str,
    text: str,
    runtime: PersistentRuntime,
    bot_token: str,
    status_texts: dict[tuple[str, str], str] | None = None,
    status_locks: dict[tuple[str, str], asyncio.Lock] | None = None,
    typing_tasks: dict[tuple[str, str], asyncio.Task[None]] | None = None,
) -> None:
    key = (chat_id, group_id)
    has_active_work = _telegram_task_status_has_active_work(text)
    if has_active_work and typing_tasks is not None:
        existing = typing_tasks.get(key)
        if existing is None or existing.done():
            typing_tasks[key] = asyncio.create_task(
                _run_telegram_chat_typing_keepalive(
                    bot_token,
                    chat_id=chat_id,
                    runtime=runtime,
                    text=text,
                )
            )

    delivered_status = False
    for attempt in range(2):
        try:
            await _send_or_edit_telegram_status_message(
                bot,
                status_messages,
                chat_id=chat_id,
                group_id=group_id,
                text=text,
                status_texts=status_texts,
                status_locks=status_locks,
            )
            delivered_status = True
            break
        except Exception:
            if attempt == 0:
                await asyncio.sleep(0.75)
                continue
            logger.debug("Telegram task status delivery failed after retry", exc_info=True)

    if has_active_work:
        if delivered_status:
            try:
                await _send_telegram_chat_typing_indicator_by_token(
                    bot_token,
                    chat_id=chat_id,
                    runtime=runtime,
                    text=text,
                )
            except Exception:
                logger.debug("Telegram task status typing refresh failed", exc_info=True)
        return

    if typing_tasks is not None:
        await _stop_typing_keepalive(typing_tasks.pop(key, None))


def _schedule_or_run_telegram_status_delivery(delivery_factory) -> bool:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            asyncio.run(delivery_factory(False))
            return True
        except Exception:
            logger.debug("Telegram planner status delivery failed outside polling loop", exc_info=True)
            return False
    async def _deliver_in_background() -> None:
        try:
            await delivery_factory(True)
        except Exception:
            logger.debug("Telegram planner status delivery failed in polling loop", exc_info=True)

    loop.create_task(_deliver_in_background())
    return True


def _schedule_or_run_telegram_delivery(delivery) -> bool:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            asyncio.run(delivery)
            return True
        except Exception:
            logger.debug("Telegram delivery failed outside polling loop", exc_info=True)
            return False

    async def _deliver_in_background() -> None:
        try:
            await delivery
        except Exception:
            logger.debug("Telegram delivery failed in polling loop", exc_info=True)

    loop.create_task(_deliver_in_background())
    return True


def _telegram_task_status_has_active_work(text: str) -> bool:
    return any(line.strip().startswith(_ACTIVE_TASK_STATUS_PREFIXES) for line in str(text or "").splitlines())


async def _send_operator_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    *,
    principal_id: str | None = None,
) -> bool:
    if not bot_token or not chat_id or not text:
        return False
    delivery = None
    try:
        from telegram import Bot  # type: ignore[import]

        delivery = prepare_reply_for_platform_delivery(text, principal_id=principal_id)
        sent_message_id = None
        async with build_telegram_bot(Bot, bot_token) as bot:
            if delivery.attachments:
                caption = delivery.text
                for index, attachment_path in enumerate(delivery.attachments):
                    async def send_document(
                        attachment_path=attachment_path,
                        caption=caption,
                        index=index,
                    ):
                        with attachment_path.open("rb") as document:
                            return await bot.send_document(
                                chat_id=chat_id,
                                document=document,
                                caption=caption[:1024] if caption and index == 0 else None,
                            )

                    sent_message = await retry_messaging_delivery_operation(
                        send_document
                    )
                    if sent_message_id is None:
                        sent_message_id = getattr(sent_message, "message_id", None)
                _record_operator_telegram_delivery_receipt(
                    chat_id,
                    delivery,
                    transport_ok=True,
                    message_id=sent_message_id,
                )
                return True
            if delivery.text:
                formatted, kwargs = format_telegram_text(delivery.text)
                sent_message = await retry_messaging_delivery_operation(
                    lambda: bot.send_message(chat_id=chat_id, text=formatted, **kwargs)
                )
                sent_message_id = getattr(sent_message, "message_id", None)
            _record_operator_telegram_delivery_receipt(
                chat_id,
                delivery,
                transport_ok=True,
                message_id=sent_message_id,
            )
            return True
    except Exception as exc:
        logger.warning("Failed to deliver Telegram operator message", exc_info=True)
        if delivery is not None:
            _record_operator_telegram_delivery_receipt(
                chat_id,
                delivery,
                transport_ok=False,
                error=exc.__class__.__name__,
            )
        return False


def _record_operator_telegram_delivery_receipt(
    chat_id: str,
    delivery,
    *,
    transport_ok: bool,
    message_id: object | None = None,
    error: str | None = None,
) -> None:
    record_platform_delivery_receipt(
        build_platform_delivery_receipt(
            channel="telegram",
            target_id=str(chat_id),
            delivery=delivery,
            transport_ok=transport_ok,
            message_id=None if message_id is None else str(message_id),
            error=error,
        )
    )


def _activity_icon(status: str) -> str:
    if status == "done":
        return "✓"
    if status == "failed":
        return "⊗"
    if status == "blocked":
        return "⊘"
    if status == "running":
        return "→"
    return "•"


class _TelegramActivityStreamer:
    def __init__(
        self,
        message,
        *,
        runtime: PersistentRuntime | None = None,
        typing_text: str | None = None,
    ) -> None:
        self._message = message
        self._runtime = runtime
        self._typing_text = typing_text
        self._status_message = None
        self._events: dict[str, dict[str, str]] = {}
        self._loop = asyncio.get_running_loop()
        self._pending = []
        self._lock = asyncio.Lock()
        self._saw_planner_status = False
        self._stopped = threading.Event()

    def emit(self, event: dict[str, str]) -> None:
        if self._stopped.is_set():
            return
        try:
            event_id = str(event.get("id") or "").strip()
            if event_id in {"planner", "mini-agents"}:
                self._saw_planner_status = True
            self._pending.append(asyncio.run_coroutine_threadsafe(self._update(event), self._loop))
        except RuntimeError:
            logger.debug("Telegram activity update skipped because event loop is unavailable", exc_info=True)

    @property
    def saw_planner_status(self) -> bool:
        return self._saw_planner_status

    async def finish(self) -> None:
        if self._stopped.is_set():
            return
        while self._pending:
            future = self._pending.pop(0)
            try:
                await asyncio.wrap_future(future)
            except Exception:
                logger.debug("Telegram activity update failed", exc_info=True)
        if self._status_message is None:
            return
        if self._is_trivial_phase_only():
            await self._delete_status_message()
            return
        await self._update({"id": "respond", "label": "Writing response", "status": "done"})

    async def stop(self) -> bool:
        if self._stopped.is_set():
            return False
        self._stopped.set()
        while self._pending:
            future = self._pending.pop(0)
            future.cancel()
            try:
                await asyncio.wrap_future(future)
            except BaseException:
                pass
        if self._status_message is not None:
            await self._update(
                {"id": "stopped", "label": "Stopped by /stop", "status": "blocked"},
                force=True,
            )
        return True

    async def _update(self, event: dict[str, str], *, force: bool = False) -> None:
        if self._stopped.is_set() and not force:
            return
        async with self._lock:
            if self._stopped.is_set() and not force:
                return
            event_id = str(event.get("id") or event.get("label") or len(self._events))
            self._events[event_id] = {
                "label": str(event.get("label") or "Working"),
                "status": str(event.get("status") or "running"),
                "detail": str(event.get("detail") or ""),
            }
            text = self._render()
            formatted, kwargs = format_telegram_text(text)
            kwargs = {**kwargs, "disable_web_page_preview": True}
            if self._status_message is None:
                reply_text = getattr(self._message, "reply_text", None)
                if reply_text is None:
                    return
                self._status_message = await retry_messaging_delivery_operation(
                    lambda: reply_text(formatted, do_quote=False, **kwargs)
                )
                await self._refresh_typing_after_status_update()
                return
            edit_text = getattr(self._status_message, "edit_text", None)
            if edit_text is None:
                return
            try:
                await retry_messaging_delivery_operation(lambda: edit_text(formatted, **kwargs))
                await self._refresh_typing_after_status_update()
            except Exception:
                logger.debug("Telegram activity message edit failed", exc_info=True)

    async def _refresh_typing_after_status_update(self) -> None:
        if self._runtime is None:
            return
        try:
            await _send_typing_indicator(self._message, runtime=self._runtime, text=self._typing_text)
        except Exception:
            logger.debug("Telegram typing refresh after activity update failed", exc_info=True)

    async def _delete_status_message(self) -> None:
        status_message = self._status_message
        self._status_message = None
        delete = getattr(status_message, "delete", None) or getattr(status_message, "delete_message", None)
        if delete is None:
            return
        try:
            await retry_messaging_delivery_operation(delete)
        except Exception:
            logger.debug("Telegram trivial activity message delete failed", exc_info=True)

    @staticmethod
    def _detail_is_activity_sublist(detail: str) -> bool:
        for raw_line in str(detail or "").splitlines():
            line = raw_line.strip()
            if line.startswith(("→", "✓", "⊗", "⊘", "!", "•", "☐", "⧁")):
                return True
        return False

    @staticmethod
    def _compact_detail_lines(detail: str) -> str:
        counts: dict[str, int] = {}
        order: list[str] = []
        for raw_line in str(detail or "").splitlines():
            line = raw_line.rstrip()
            if not line.strip():
                continue
            if line not in counts:
                counts[line] = 0
                order.append(line)
            counts[line] += 1
        return "\n".join(
            f"{line} × {counts[line]}" if counts[line] > 1 else line
            for line in order
        )

    def _has_typed_phase_events(self) -> bool:
        return any(str(event_id).startswith("phase-") for event_id in self._events)

    def _has_phase_tool_detail(self) -> bool:
        event = self._events.get("phase-run-tools")
        if not event:
            return False
        return bool((event.get("detail") or "").strip())

    def _has_grouped_tool_detail(self) -> bool:
        orchestrate = self._events.get("orchestrate")
        if not orchestrate:
            return False
        detail = orchestrate.get("detail") or ""
        return self._detail_is_activity_sublist(detail)

    @staticmethod
    def _detail_has_meaningful_work(event_id: str, detail: str) -> bool:
        text = str(detail or "").strip()
        if not text:
            return False
        if event_id == "phase-check-attachments":
            return not re.match(r"^0 attachments?$", text, flags=re.IGNORECASE)
        if event_id in {"phase-prepare-artifacts", "artifacts"}:
            return not re.match(r"^0 artifacts?$", text, flags=re.IGNORECASE)
        if event_id in {"phase-run-tools", "orchestrate"}:
            return True
        return not re.match(r"^(handled by|known model health issue|save|context|model_start)$", text, flags=re.IGNORECASE)

    def _is_trivial_phase_only(self) -> bool:
        if self._saw_planner_status or not self._events:
            return False
        trivial_ids = {
            "queued",
            "prepare",
            "phase-check-task-state",
            "phase-check-attachments",
            "phase-build-context",
            "phase-start-model",
            "phase-run-tools",
            "phase-prepare-artifacts",
            "phase-save-conversation",
            "artifacts",
            "memory",
            "respond",
        }
        for event_id, event in self._events.items():
            if event_id not in trivial_ids:
                return False
            if str(event.get("status") or "") in {"failed", "blocked"}:
                return False
            if self._detail_has_meaningful_work(event_id, event.get("detail") or ""):
                return False
        return True

    @staticmethod
    def _should_hide_detail(event_id: str, event: dict[str, str]) -> bool:
        if event_id != "orchestrate" or event.get("status") != "running":
            return False
        return (event.get("detail") or "").strip().lstrip("→ ").startswith("Tools:")

    def _render(self) -> str:
        lines = ["Activity"]
        hide_tool_events = self._has_grouped_tool_detail() or self._has_phase_tool_detail()
        hide_orchestrate = self._has_typed_phase_events()
        for event_id, event in self._events.items():
            if hide_orchestrate and event_id == "orchestrate":
                continue
            if hide_tool_events and event_id.startswith("tool-"):
                continue
            detail = "" if self._should_hide_detail(event_id, event) else self._compact_detail_lines(event.get("detail") or "")
            prefix = f"{_activity_icon(event.get('status', ''))} {event.get('label', 'Working')}"
            if "\n" in detail or self._detail_is_activity_sublist(detail):
                lines.append(prefix)
                lines.extend(detail.splitlines())
            else:
                suffix = f" — {detail}" if detail else ""
                lines.append(f"{prefix}{suffix}")
        return "\n".join(lines)


class _TelegramActivityRegistry:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._streams_by_conversation: dict[str, set[_TelegramActivityStreamer]] = defaultdict(set)

    async def register(self, conversation_id: str | None, streamer: _TelegramActivityStreamer | None) -> None:
        if streamer is None:
            return
        conversation_key = str(conversation_id or "").strip()
        if not conversation_key:
            return
        async with self._lock:
            self._streams_by_conversation[conversation_key].add(streamer)

    async def unregister(self, conversation_id: str | None, streamer: _TelegramActivityStreamer | None) -> None:
        if streamer is None:
            return
        conversation_key = str(conversation_id or "").strip()
        if not conversation_key:
            return
        async with self._lock:
            streams = self._streams_by_conversation.get(conversation_key)
            if streams is None:
                return
            streams.discard(streamer)
            if not streams:
                self._streams_by_conversation.pop(conversation_key, None)

    async def stop_conversation(self, conversation_id: str | None) -> int:
        conversation_key = str(conversation_id or "").strip()
        if not conversation_key:
            return 0
        async with self._lock:
            streams = tuple(self._streams_by_conversation.pop(conversation_key, ()))
        if not streams:
            return 0
        stopped = await asyncio.gather(*(stream.stop() for stream in streams), return_exceptions=True)
        return sum(1 for result in stopped if result is True)


def _call_handle_update_with_activity(
    service,
    update,
    *,
    attachments: list[dict[str, str]] | None = None,
    activity_callback=None,
    text_delta_callback=None,
    append_activity_trace: bool = True,
    turn_dispatch_decision=None,
    cancellation_checker=None,
):
    started_at = time.perf_counter()
    timing_marks: list[str] = []
    timing_last_at = started_at
    message = getattr(update, "message", None) or getattr(update, "effective_message", None)
    chat = None if message is None else getattr(message, "chat", None)
    chat_id = None if chat is None else getattr(chat, "id", None)
    message_id = None if message is None else getattr(message, "message_id", None)
    request_id = getattr(update, "update_id", None)

    def _mark_timing(label: str) -> None:
        nonlocal timing_last_at
        now = time.perf_counter()
        timing_marks.append(f"{label}:{round((now - timing_last_at) * 1000, 1)}ms")
        timing_last_at = now

    parameters = inspect.signature(service.handle_update).parameters
    if "activity_callback" in parameters:
        _mark_timing("handle_update_activity_enabled")
        kwargs = {
            "activity_callback": activity_callback,
            "append_activity_trace": append_activity_trace,
        }
        if "text_delta_callback" in parameters:
            kwargs["text_delta_callback"] = text_delta_callback
        if "attachments" in parameters:
            kwargs["attachments"] = attachments
        if "turn_dispatch_decision" in parameters:
            kwargs["turn_dispatch_decision"] = turn_dispatch_decision
        if "cancellation_checker" in parameters:
            kwargs["cancellation_checker"] = cancellation_checker
        result = service.handle_update(update, **kwargs)
    else:
        _mark_timing("handle_update_plain")
        result = service.handle_update(update)
    _mark_timing("handle_update_done")
    total_ms = (time.perf_counter() - started_at) * 1000
    if total_ms >= _float_env_ms(_NULLION_TELEGRAM_FLOW_SLOW_LOG_MS, default=1200.0):
        logger.warning(
            "telegram handle_update timing chat_id=%s message_id=%s request_id=%s total_ms=%.1f phases=%s",
            str(chat_id or ""),
            str(message_id or ""),
            f"telegram-update:{request_id}" if request_id else None,
            total_ms,
            ", ".join(timing_marks),
        )
    return result


async def _send_callback_follow_up(
    message,
    reply: str,
    *,
    principal_id: str | None = None,
    allow_attachments: bool | None = None,
) -> None:
    visible_reply = strip_tool_approval_marker(reply)
    delivery = prepare_reply_for_platform_delivery(
        visible_reply or "",
        principal_id=principal_id,
        allow_attachments=allow_attachments,
    )
    caption = delivery.text
    attachment_paths = delivery.attachments
    if attachment_paths:
        formatted_caption, caption_kwargs, caption_too_long = _telegram_attachment_caption_kwargs(caption)
        if caption is not None and caption_too_long:
            formatted_text, text_kwargs = format_telegram_text(caption)
            reply_text = getattr(message, "reply_text", None)
            if reply_text is not None:
                await _reply_text_in_chunks_with_plain_fallback(
                    message,
                    formatted_text,
                    caption,
                    do_quote=False,
                    **text_kwargs,
                )
            else:
                edit_text = getattr(message, "edit_text", None)
                if edit_text is not None:
                    await _edit_text_with_plain_fallback(edit_text, formatted_text, caption, **text_kwargs)
        try:
            for index, attachment_path in enumerate(attachment_paths):
                await _reply_document_attachment(
                    message,
                    attachment_path,
                    caption=formatted_caption if index == 0 else None,
                    do_quote=False,
                    **(caption_kwargs if index == 0 else {}),
                )
        except Exception:
            logger.warning("Could not upload Telegram callback attachment", exc_info=True)
            await _send_telegram_delivery_failure_safely(
                message,
                delivery,
                do_quote=False,
                stage="callback attachment upload",
            )
        return
    delivery_text = caption or ""
    formatted_reply, reply_kwargs = format_telegram_text(delivery_text)
    reply_text = getattr(message, "reply_text", None)
    if reply_text is not None:
        try:
            await _reply_text_in_chunks_with_plain_fallback(
                message,
                formatted_reply,
                delivery_text,
                do_quote=False,
                **reply_kwargs,
            )
        except Exception:
            logger.warning("Could not deliver Telegram callback follow-up after retries", exc_info=True)
        return
    edit_text = getattr(message, "edit_text", None)
    if edit_text is not None:
        try:
            await _edit_text_with_plain_fallback(edit_text, formatted_reply, delivery_text, **reply_kwargs)
        except Exception:
            logger.warning("Could not edit Telegram callback follow-up after retries", exc_info=True)
        return
    raise AttributeError("Telegram message object has no reply_text or edit_text method")


async def _telegram_file_bytes(file_obj) -> bytes | None:
    if file_obj is None:
        return None
    download_as_bytearray = getattr(file_obj, "download_as_bytearray", None)
    if download_as_bytearray is not None:
        data = await download_as_bytearray()
        return bytes(data)
    download_to_memory = getattr(file_obj, "download_to_memory", None)
    if download_to_memory is not None:
        import io

        buffer = io.BytesIO()
        await download_to_memory(out=buffer)
        return buffer.getvalue()
    return None


async def _telegram_get_file(file_ref, context):
    get_file = getattr(file_ref, "get_file", None)
    if get_file is not None:
        return await get_file()
    file_id = getattr(file_ref, "file_id", None)
    bot = getattr(context, "bot", None)
    bot_get_file = getattr(bot, "get_file", None)
    if file_id and bot_get_file is not None:
        return await bot_get_file(file_id)
    return None


async def _download_telegram_attachments(message, context, *, settings: NullionSettings | None = None) -> list[dict[str, str]]:
    if message is None:
        return []
    started_at = time.perf_counter()
    timing_marks: list[str] = []
    timing_last_at = started_at
    candidates: list[tuple[object, str, str | None]] = []
    photos = list(getattr(message, "photo", []) or [])
    if photos:
        candidates.append((photos[-1], "telegram-photo.png", "image/png"))
    audio = getattr(message, "audio", None)
    if audio is not None:
        candidates.append(
            (
                audio,
                str(getattr(audio, "file_name", "") or "telegram-audio.mp3"),
                str(getattr(audio, "mime_type", "") or "audio/mpeg"),
            )
        )
    voice = getattr(message, "voice", None)
    if voice is not None:
        candidates.append((voice, "telegram-voice.ogg", str(getattr(voice, "mime_type", "") or "audio/ogg")))
    video = getattr(message, "video", None)
    if video is not None:
        candidates.append(
            (
                video,
                str(getattr(video, "file_name", "") or "telegram-video.mp4"),
                str(getattr(video, "mime_type", "") or "video/mp4"),
            )
        )
    document = getattr(message, "document", None)
    if document is not None:
        mime_type = str(getattr(document, "mime_type", "") or "")
        file_name = str(getattr(document, "file_name", "") or "")
        if is_supported_chat_file(filename=file_name, media_type=mime_type):
            candidates.append((document, file_name or "telegram-document", mime_type or None))
    attachments: list[dict[str, str]] = []
    chat = None if message is None else getattr(message, "chat", None)
    chat_id = None if chat is None else getattr(chat, "id", None)
    user = resolve_messaging_user("telegram", chat_id, settings)
    principal_id = f"user:{user.user_id}" if user.role == "member" else "telegram_chat"

    def _mark_timing(label: str) -> None:
        nonlocal timing_last_at
        now = time.perf_counter()
        timing_marks.append(f"{label}:{round((now - timing_last_at) * 1000, 1)}ms")
        timing_last_at = now

    for file_ref, filename, media_type in candidates:
        _mark_timing(f"file_attempt_start:{filename}")
        attempts = 0
        data = None
        for attempt in range(1, _TELEGRAM_ATTACHMENT_DOWNLOAD_ATTEMPTS + 1):
            attempts = attempt
            try:
                file_obj = await _telegram_get_file(file_ref, context)
                data = await _telegram_file_bytes(file_obj)
                break
            except Exception:
                if attempt >= _TELEGRAM_ATTACHMENT_DOWNLOAD_ATTEMPTS:
                    logger.warning("Could not download Telegram attachment", exc_info=True)
                else:
                    await asyncio.sleep(0.25 * attempt)
        if data is None:
            _mark_timing(f"file_download_failed:{filename}:attempts={attempts}")
            continue
        saved = save_messaging_attachment(
            filename=filename,
            data=data,
            media_type=media_type,
            principal_id=principal_id,
        )
        if saved is not None:
            attachments.append(saved)
            _mark_timing(f"file_saved:{filename}")
        else:
            _mark_timing(f"file_save_skipped:{filename}")
    _mark_timing("attachment_scan_complete")
    total_ms = (time.perf_counter() - started_at) * 1000
    if total_ms >= _float_env_ms(_NULLION_TELEGRAM_FLOW_SLOW_LOG_MS, default=1200.0):
        logger.warning(
            "telegram attachment timing chat_id=%s candidate_count=%s saved_count=%s total_ms=%.1f phases=%s",
            str(chat_id or ""),
            len(candidates),
            len(attachments),
            total_ms,
            ", ".join(timing_marks),
        )
    return attachments


async def _send_typing_indicator(message, *, runtime: PersistentRuntime, text: str | None) -> None:
    if message is None:
        return
    chat = getattr(message, "chat", None)
    if chat is None:
        return
    send_action = getattr(chat, "send_action", None)
    if send_action is None:
        return
    chat_id = getattr(chat, "id", None)
    chat_id_text = None if chat_id is None else str(chat_id)
    try:
        await send_action("typing")
    except Exception as exc:
        _report_runner_health_issue(
            runtime,
            issue_type=HealthIssueType.DEGRADED,
            message="Telegram typing indicator failed.",
            chat_id=chat_id_text,
            text=text,
            stage="typing_indicator",
            detail=_typing_indicator_failure_detail(exc),
        )
        raise


async def _send_telegram_chat_typing_indicator(
    bot,
    *,
    chat_id: str,
    runtime: PersistentRuntime,
    text: str | None,
) -> None:
    if bot is None or not chat_id:
        return
    send_chat_action = getattr(bot, "send_chat_action", None)
    if send_chat_action is None:
        return
    try:
        await retry_messaging_delivery_operation(
            lambda: send_chat_action(chat_id=chat_id, action="typing")
        )
    except Exception as exc:
        _report_runner_health_issue(
            runtime,
            issue_type=HealthIssueType.DEGRADED,
            message="Telegram typing indicator failed.",
            chat_id=chat_id,
            text=text,
            stage="typing_indicator",
            detail=_typing_indicator_failure_detail(exc),
        )
        raise


async def _send_telegram_chat_typing_indicator_by_token(
    bot_token: str,
    *,
    chat_id: str,
    runtime: PersistentRuntime,
    text: str | None,
) -> None:
    if not bot_token or not chat_id:
        return
    try:
        from telegram import Bot  # type: ignore[import]
    except Exception:
        logger.debug("Telegram typing indicator skipped because python-telegram-bot is unavailable", exc_info=True)
        return
    async with build_telegram_bot(Bot, bot_token) as bot:
        await _send_telegram_chat_typing_indicator(bot, chat_id=chat_id, runtime=runtime, text=text)


async def _run_typing_keepalive(message, *, runtime: PersistentRuntime, text: str | None) -> None:
    try:
        while True:
            await _send_typing_indicator(message, runtime=runtime, text=text)
            await asyncio.sleep(_TYPING_KEEPALIVE_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.debug("Stopped Telegram typing keepalive after send failure.", exc_info=True)


async def _run_telegram_chat_typing_keepalive(
    bot_token: str,
    *,
    chat_id: str,
    runtime: PersistentRuntime,
    text: str | None,
) -> None:
    try:
        while True:
            await _send_telegram_chat_typing_indicator_by_token(
                bot_token,
                chat_id=chat_id,
                runtime=runtime,
                text=text,
            )
            await asyncio.sleep(_TYPING_KEEPALIVE_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.debug("Stopped Telegram chat typing keepalive after send failure.", exc_info=True)


def _typing_indicator_failure_detail(exc: BaseException) -> str:
    message = str(exc).strip()
    exc_name = type(exc).__name__
    suffix = f"{exc_name}: {message}" if message else exc_name
    return f"Failed to send Telegram typing indicator: {suffix}"


async def _stop_typing_keepalive(task: asyncio.Task[None] | None) -> None:
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass



def _should_quote_reply(text: str | None) -> bool:
    if text is None:
        return False
    stripped = text.strip()
    if not stripped:
        return False
    if not stripped.startswith("/"):
        return True
    head = stripped.split()[0].partition("@")[0]
    if head in {"/chat", "/proposals", "/proposal", "/accept-proposal", "/reject-proposal", "/archive-proposal"}:
        return False
    return True



def _report_runner_health_issue(
    runtime: PersistentRuntime,
    *,
    issue_type: HealthIssueType,
    message: str,
    chat_id: str | None,
    text: str | None,
    stage: str,
    detail: str | None = None,
) -> None:
    runtime.report_health_issue(
        issue_type=issue_type,
        source="telegram_app",
        message=message,
        details={
            "chat_id": chat_id or "unknown",
            "source": "telegram_app",
            "stage": stage,
            "issue_type": issue_type.value,
            "message_text": text,
            "detail": detail,
        },
    )


def _refresh_runtime_from_checkpoint(
    runtime: PersistentRuntime,
    *,
    min_interval_seconds: float = 0.0,
    force: bool = False,
) -> bool:
    checkpoint_path = getattr(runtime, "checkpoint_path", None)
    if checkpoint_path is None:
        return False
    path = Path(checkpoint_path)
    if not path.exists():
        return False
    signature = _checkpoint_file_signature(path)
    if signature is None:
        return False
    path_key = str(path)
    if _CHECKPOINT_FILE_SIGNATURES.get(path_key) == signature:
        return False
    if not force and min_interval_seconds > 0:
        now = time.monotonic()
        last_attempt = _CHECKPOINT_REFRESH_ATTEMPT_AT.get(path_key, 0.0)
        if now - last_attempt < min_interval_seconds:
            return False
        _CHECKPOINT_REFRESH_ATTEMPT_AT[path_key] = now
    try:
        store = load_runtime_store(path)
        runtime.store = store
        # Keep refresh fast on Telegram ingress. Recomputing the full store
        # fingerprint here adds noticeable latency and is not required for
        # correctness; the next checkpoint will materialize a fresh fingerprint.
        runtime.last_checkpoint_fingerprint = None
        runtime.last_checkpoint_file_signature = signature
        _cache_runtime_checkpoint_signature(runtime)
        return True
    except Exception:
        logger.debug("Could not refresh Telegram runtime from checkpoint", exc_info=True)
        return False


def _approval_decision_emoji(display_title: str) -> str:
    first, separator, _rest = display_title.partition(" ")
    return first if separator else "✅"


def _approval_decision_subject(approval, detail: str) -> str:
    display = approval_display_from_request(approval)
    title = display.title.lower()
    if display.is_web_request or _approval_is_web_request(display.label, detail):
        return "web access"
    if "file access" in title:
        return "file access"
    if "command" in title:
        return "command"
    if "write action" in title:
        return "write action"
    if "message" in title:
        return "message"
    if "memory" in title:
        return "memory update"
    if "package" in title:
        return "package install"
    if "account access" in title:
        return "account access"
    if "external access" in title:
        return "external access"
    return display.label


def _approval_decision_target(approval, detail: str) -> str:
    display = approval_display_from_request(approval)
    target_url = _approval_target_url(detail)
    if display.is_web_request and target_url:
        return _approval_target_host(target_url)
    for key in ("URL", "Path", "Target", "Command", "Query", "Resource", "Operation"):
        match = re.search(rf"(?: · )?{key}:\s*(?P<value>.+)$", detail, flags=re.IGNORECASE)
        if match:
            value = match.group("value").strip()
            if key == "URL" and value:
                return _approval_target_host(value)
            return value
    if target_url:
        return _approval_target_host(target_url)
    return detail.strip().rstrip(".")


def _approval_decision_messages(approval, action: str) -> tuple[str, str]:
    display = approval_display_from_request(approval)
    detail = _approval_detail(display.label, display.detail, getattr(approval, "approval_id", ""))
    emoji = _approval_decision_emoji(display.title)
    subject = _approval_decision_subject(approval, detail)
    target = _approval_decision_target(approval, detail)
    target_suffix = f" for {approval_inline_code(target)}" if target else ""
    target_colon = f": {approval_inline_code(target)}" if target else ""

    if action == "allow_session":
        acknowledgement = "Approved web access"
        reply = f"✅ {emoji} Approved all web domains for this run. Continuing..."
    elif action == "always_allow":
        acknowledgement = f"Always allowed {subject}"
        reply = f"✅ {emoji} Always allowed {subject}{target_suffix}. Continuing..."
    elif action in {"allow_once", "approve"}:
        acknowledgement = f"Approved {subject}"
        once = " once" if action == "allow_once" else ""
        reply = f"✅ {emoji} Approved {subject}{once}{target_colon}. Continuing..."
    elif action in {"deny", "reject"}:
        acknowledgement = f"Denied {subject}"
        reply = f"🚫 {emoji} Denied {subject}{target_colon}. I'll stop here."
    else:
        acknowledgement = "Approved"
        reply = f"✅ {emoji} Approved {subject}{target_colon}. Continuing..."
    return acknowledgement, reply


def _approval_resume_fallback_reply(approval, reply: str) -> str:
    text = reply.strip()
    text = re.sub(r"\s+Continuing\.\.\.$", ".", text)
    if approval is None:
        return f"{text}\nPlease resend your message to continue."
    return f"{text}\nPlease resend your message if it does not continue automatically."


def _resume_delivery_channel_for_approval(runtime, approval) -> str:
    if approval is None:
        return "telegram"
    suspended_turn = runtime.store.get_suspended_turn(getattr(approval, "approval_id", ""))
    conversation_id = getattr(suspended_turn, "conversation_id", None)
    if isinstance(conversation_id, str) and ":" in conversation_id:
        channel, _, _ = conversation_id.partition(":")
        channel = channel.strip().lower()
        if channel:
            return channel
    requested_by = getattr(approval, "requested_by", None)
    if isinstance(requested_by, str) and ":" in requested_by:
        channel, _, _ = requested_by.partition(":")
        channel = channel.strip().lower()
        if channel:
            return channel
    return "telegram"


def _execute_decision_action(
    service: "ChatOperatorService",
    *,
    kind: str,
    action: str,
    record_id: str,
    chat_id: str | None = None,
) -> tuple[str, str]:
    if kind == "approval":
        from nullion.approval_decisions import approve_request_with_mode

        approval = service.runtime.store.get_approval_request(record_id)
        if approval is None:
            _refresh_runtime_from_checkpoint(service.runtime, force=True)
            approval = service.runtime.store.get_approval_request(record_id)
        if approval is None:
            return (
                "Approval expired",
                "⏳ That approval is no longer active. Please rerun the request if you still want Nullion to continue.",
            )
        if getattr(approval, "request_kind", None) == "boundary_policy":
            if action == "allow_session":
                approve_request_with_mode(
                    service.runtime,
                    record_id,
                    mode="run",
                    source="Telegram",
                    expires_at=_web_session_allow_expires_at(),
                    run_expires_at=_web_session_allow_expires_at(),
                    auto_approve_run_boundaries=True,
                )
                return _approval_decision_messages(approval, action)
            if action == "allow_once":
                approve_request_with_mode(
                    service.runtime,
                    record_id,
                    mode="once",
                    source="Telegram",
                )
                return _approval_decision_messages(approval, action)
            if action == "always_allow":
                approve_request_with_mode(
                    service.runtime,
                    record_id,
                    mode="always",
                    source="Telegram",
                )
                return _approval_decision_messages(approval, action)
            if action == "deny":
                service.runtime.deny_approval_request(record_id, actor="operator")
                return _approval_decision_messages(approval, action)
            raise ValueError(f"Unsupported approval action: {action}")
        if action in {"allow_session", "allow_once", "always_allow", "approve"}:
            mode = (
                "run"
                if action == "allow_session"
                else "always"
                if action == "always_allow"
                else "once"
            )
            approve_request_with_mode(
                service.runtime,
                record_id,
                mode=mode,
                source="Telegram",
                expires_at=_web_session_allow_expires_at() if action == "allow_session" else None,
                run_expires_at=_web_session_allow_expires_at() if action == "allow_session" else None,
                auto_approve_run_boundaries=action == "allow_session",
            )
            return _approval_decision_messages(approval, action)
        if action in {"deny", "reject"}:
            service.runtime.deny_approval_request(record_id, actor="operator")
            return _approval_decision_messages(approval, action)
        raise ValueError(f"Unsupported approval action: {action}")

    if kind == "proposal":
        command = {
            "review": f"/proposal {record_id}",
            "accept": f"/accept-proposal {record_id}",
            "reject": f"/reject-proposal {record_id}",
            "archive": f"/archive-proposal {record_id}",
        }.get(action)
        if command is None:
            raise ValueError(f"Unsupported proposal action: {action}")
        reply = handle_operator_command(service.runtime, command)
        acknowledgement = {
            "review": "Review",
            "accept": "Accepted",
            "reject": "Rejected",
            "archive": "Archived",
        }[action]
        return acknowledgement, reply

    if kind == "doctor":
        current = service.runtime.store.get_doctor_action(record_id)
        if current is None:
            _refresh_runtime_from_checkpoint(service.runtime, force=True)
            current = service.runtime.store.get_doctor_action(record_id)
        if current is None:
            return "Expired", "That Doctor action is no longer active."
        if _doctor_action_is_closed(current):
            status = str(current.get("status") or "closed").replace("_", " ")
            return "Already closed", f"{_doctor_card_text(current)}\n\nThis card is already {status}."
        if action == "start":
            updated = service.runtime.start_doctor_action(record_id)
            logger.info("Started Doctor action %s, status=%s", record_id, updated.get("status"))
            return "Started", _doctor_card_text(updated)
        if action == "complete":
            updated = service.runtime.complete_doctor_action(record_id)
            logger.info("Completed Doctor action %s, status=%s", record_id, updated.get("status"))
            return "Completed", "Action completed."
        if action == "cancel":
            service.runtime.cancel_doctor_action(record_id, reason="Dismissed from Telegram")
            logger.info("Dismissed Doctor action %s", record_id)
            return "Dismissed", "Action dismissed."
        # Playbook-specific commands like "doctor:restart_bot", "doctor:retry_model_api"
        if action.startswith("doctor:"):
            return _execute_doctor_command(service, command=action, action_id=record_id)
        raise ValueError(f"Unsupported doctor action: {action}")

    if kind == "nav":
        # Help menu category tapped — return detail text for that category
        detail = _build_help_nav_reply(record_id)
        return "ℹ️", detail

    if kind == "setting":
        if action == "set" and record_id.startswith("verbose_"):
            mode = record_id.removeprefix("verbose_")
            set_verbose_mode_for_chat(service.runtime, chat_id=chat_id, mode=mode)
            return f"Verbose {mode}", f"Verbose mode is {verbose_mode_status_text_for_chat(service.runtime, chat_id=chat_id)}."
        if action == "set" and record_id == "streaming_on":
            set_chat_streaming_enabled_for_chat(service.runtime, chat_id=chat_id, enabled=True)
            return "Streaming on", "Chat streaming is on."
        if action == "set" and record_id == "streaming_off":
            set_chat_streaming_enabled_for_chat(service.runtime, chat_id=chat_id, enabled=False)
            return "Streaming off", "Chat streaming is off."
        raise ValueError(f"Unsupported setting action: {action}:{record_id}")

    if kind == "model":
        if action == "select":
            from nullion.runtime_config import current_runtime_config

            cfg = current_runtime_config(model_client=getattr(service, "model_client", None))
            option = chat_model_option_for_token(record_id, current_provider=cfg.provider, current_model=cfg.model)
            if option is None:
                return "Not found", "That model option is no longer available. Send /models to refresh."
            reply = handle_operator_command(
                service.runtime,
                f"/model {option['provider']} {option['model']}",
                service=service,
            )
            return "Model selected", reply
        raise ValueError(f"Unsupported model action: {action}:{record_id}")

    if kind == "reminder":
        # record_id = task_id of the reminder
        if action == "cancel":
            removed = service.runtime.store.remove_reminder(record_id)
            # Also remove the associated scheduled task if possible
            service.runtime.store.scheduled_tasks.pop(record_id, None)
            service.runtime.checkpoint()
            _cache_runtime_checkpoint_signature(service.runtime)
            if removed:
                return "Cancelled", "⏰ Reminder cancelled."
            return "Not found", "This reminder has already been removed."
        if action == "edit_time":
            reminder = service.runtime.store.get_reminder(record_id)
            reminder_text = getattr(reminder, "text", "your reminder") if reminder else "your reminder"
            return "Edit", (
                f"Just tell me the new time for \"{reminder_text}\" and I'll update it."
            )
        raise ValueError(f"Unsupported reminder action: {action}")

    raise ValueError(f"Unsupported decision kind: {kind}")


def _execute_doctor_command(service: "ChatOperatorService", *, command: str, action_id: str) -> tuple[str, str]:
    """Dispatch a playbook-specific doctor command button press."""
    try:
        def _signal_restart() -> None:
            import os, signal as _sig
            os.kill(os.getpid(), _sig.SIGHUP)

        result = execute_doctor_playbook_command(
            service.runtime,
            action_id=action_id,
            command=command,
            source_label="Telegram",
            signal_current_process_restart=_signal_restart,
        )
        logger.info(
            "Doctor command %s handled for action %s, status=%s",
            command,
            action_id,
            result.action.get("status"),
        )
        return result.acknowledgement, result.message
    except Exception as exc:
        logger.warning("Doctor command %s failed for action %s: %s", command, action_id, exc)
        return "Error", "Action failed. See logs for details."


def _natural_overlap_ack(prompt: str, *, dispatch_decision: TurnDispatchDecision | None = None) -> str:
    if dispatch_decision is not None and dispatch_decision.should_wait:
        variants = (
            "Got it — I’ll use that after the active task is ready.",
            "Okay — I’ll attach that to the active task.",
            "On it — I’ll continue from the active result.",
        )
        return variants[sum(ord(char) for char in prompt.strip().lower()) % len(variants)]
    return ""



def _busy_chat_ack_text(
    text: str | None,
    *,
    chat_id: str | None,
    settings: NullionSettings,
    dispatch_decision: TurnDispatchDecision | None = None,
) -> str | None:
    if text is None:
        return None
    prompt = _chat_prompt_for_message(text)
    if prompt is None:
        return None
    if not settings.telegram.chat_enabled:
        return None
    if not _is_authorized_chat(chat_id, settings):
        return None
    if _local_chat_reply_body(prompt) is not None:
        return None
    ack = _natural_overlap_ack(prompt, dispatch_decision=dispatch_decision)
    return ack or None


def _foreground_working_ack_text(
    text: str | None,
    *,
    chat_id: str | None,
    settings: NullionSettings,
) -> str | None:
    if text is None:
        return None
    prompt = _chat_prompt_for_message(text)
    if prompt is None:
        return None
    if not settings.telegram.chat_enabled:
        return None
    if not _is_authorized_chat(chat_id, settings):
        return None
    if _local_chat_reply_body(prompt) is not None:
        return None
    return "Working on your request now. You can keep sending requests."


class MissingTelegramBotTokenError(ValueError):
    """Raised when Telegram runtime settings are missing a bot token."""


class MissingTelegramOperatorChatIDError(ValueError):
    """Raised when Telegram runtime settings are missing an operator chat id."""


class TelegramDependencyUnavailableError(RuntimeError):
    """Raised when python-telegram-bot is unavailable."""


@dataclass(slots=True)
class ChatOperatorService:
    runtime: PersistentRuntime
    bot_token: str
    operator_chat_id: str | None
    settings: NullionSettings
    model_client: object | None = None
    agent_orchestrator: object | None = None
    _inflight_chat_by_id: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _turn_dispatch_tracker: AsyncTurnDispatchTracker = field(default_factory=AsyncTurnDispatchTracker)
    _activity_streams: _TelegramActivityRegistry = field(default_factory=_TelegramActivityRegistry)
    _seen_ingress_ids: set[str] = field(default_factory=set)
    _first_run_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _live_config_signature_cache: tuple[object, ...] | None = None

    def __post_init__(self) -> None:
        self.settings = copy.deepcopy(self.settings)
        self._live_config_signature_cache = self._live_config_signature()

    def _live_config_signature(self) -> tuple[object, ...]:
        paths: list[Path] = []
        env_path = os.environ.get("NULLION_ENV_FILE")
        if env_path:
            paths.append(Path(env_path).expanduser())

        signature: list[object] = []
        for path in paths:
            try:
                stat = path.stat()
            except OSError:
                signature.append((str(path), None, None))
                continue
            signature.append((str(path), stat.st_mtime_ns, stat.st_size))
        runtime_checkpoint = getattr(self.runtime, "checkpoint_path", None)
        settings_checkpoint = getattr(self.settings, "checkpoint_path", None)
        checkpoint_path_raw = (
            os.environ.get("NULLION_CREDENTIALS_DB_PATH")
            or os.environ.get("NULLION_CHECKPOINT_PATH")
            or runtime_checkpoint
            or settings_checkpoint
        )
        if checkpoint_path_raw:
            checkpoint_path = Path(checkpoint_path_raw).expanduser()
        else:
            checkpoint_path = None
        if checkpoint_path is None:
            signature.append(("credentials", None, None))
        else:
            try:
                import hashlib
                import json

                from nullion.credential_store import load_encrypted_credentials

                credential_payload = load_encrypted_credentials(db_path=checkpoint_path) or {}
                credential_blob = json.dumps(credential_payload, sort_keys=True, default=str, separators=(",", ":"))
                signature.append(
                    (
                        "credentials",
                        str(checkpoint_path),
                        hashlib.sha256(credential_blob.encode("utf-8")).hexdigest(),
                    )
                )
            except Exception:
                signature.append(("credentials", str(checkpoint_path), None))
        env_snapshot = tuple(
            sorted(
                (key, value)
                for key, value in os.environ.items()
                if key.startswith("NULLION_")
                or key
                in {
                    "OPENAI_API_KEY",
                    "OPENAI_BASE_URL",
                    "OPENAI_MODEL",
                    "ANTHROPIC_API_KEY",
                    "ANTHROPIC_MODEL",
                    "OPENROUTER_API_KEY",
                    "GEMINI_API_KEY",
                    "GOOGLE_API_KEY",
                    "GROQ_API_KEY",
                    "MISTRAL_API_KEY",
                    "DEEPSEEK_API_KEY",
                    "XAI_API_KEY",
                    "TOGETHER_API_KEY",
                    "OLLAMA_API_KEY",
                }
            )
        )
        signature.append(env_snapshot)
        return tuple(signature)

    def _apply_live_settings(self, next_settings: NullionSettings) -> None:
        for settings_field in fields(NullionSettings):
            setattr(self.settings, settings_field.name, getattr(next_settings, settings_field.name))
        self.bot_token = next_settings.telegram.bot_token or ""
        operator_chat_id = next_settings.telegram.operator_chat_id
        self.operator_chat_id = operator_chat_id.strip() if isinstance(operator_chat_id, str) and operator_chat_id.strip() else None

    def refresh_live_configuration(self, *, force: bool = False) -> bool:
        signature = self._live_config_signature()
        if not force and signature == self._live_config_signature_cache:
            return False

        from nullion.agent_orchestrator import AgentOrchestrator
        from nullion.config import load_env_file_into_environ, load_settings

        env_path_raw = os.environ.get("NULLION_ENV_FILE")
        env_path = Path(env_path_raw).expanduser() if env_path_raw else None
        os.environ.setdefault("NULLION_CHECKPOINT_PATH", str(self.runtime.checkpoint_path))
        os.environ.setdefault("NULLION_HOME", str(self.runtime.checkpoint_path.parent))
        if env_path is not None:
            load_env_file_into_environ(env_path, override=True)

        next_settings = load_settings(env_path=env_path)
        self._apply_live_settings(next_settings)

        if self.settings.telegram.chat_enabled:
            next_client = _build_chat_model_client_with_fallback(self.settings, surface="Telegram")
        else:
            next_client = None

        if next_client is None:
            self.model_client = None
            self.agent_orchestrator = None
        else:
            self.model_client = next_client
            self.agent_orchestrator = AgentOrchestrator(model_client=next_client)
        try:
            self.runtime.model_client = self.model_client
        except Exception:
            pass
        self._live_config_signature_cache = self._live_config_signature()
        logger.info(
            "Telegram live configuration refreshed (chat_enabled=%s provider=%s model=%s operator_chat_id=%s)",
            self.settings.telegram.chat_enabled,
            getattr(getattr(self.settings, "model", None), "provider", "?"),
            getattr(getattr(self.settings, "model", None), "openai_model", "?"),
            self.operator_chat_id or "",
        )
        return True

    def refresh_model_client_if_configuration_changed(self, *, force: bool = False) -> bool:
        return self.refresh_live_configuration(force=force)

    def swap_model_client(self, model_name: str) -> None:
        """Hot-swap the live model client to use *model_name* — no restart required.

        Replaces ``self.model_client`` (with the model field rewritten) and
        rebuilds ``self.agent_orchestrator`` so all subsequent turns use the
        new model.  Does nothing if there is no active model client.

        Raises:
            ValueError: propagated from ``clone_model_client_with_model`` if
                the current client doesn't support model swapping.
        """
        if self.model_client is None:
            return
        from nullion.agent_orchestrator import AgentOrchestrator
        from nullion.model_clients import clone_model_client_with_model

        new_client = clone_model_client_with_model(self.model_client, model_name)
        self.model_client = new_client
        self.agent_orchestrator = AgentOrchestrator(model_client=new_client)

    def swap_provider_model_client(self, provider: str, model_name: str) -> None:
        """Hot-swap the live model client to a provider/model pair."""
        from nullion.agent_orchestrator import AgentOrchestrator
        from nullion.auth import load_stored_credentials
        from nullion.config import load_settings
        from nullion.model_clients import build_model_client_from_settings

        env = dict(os.environ)
        provider = provider.strip().lower()
        env["NULLION_MODEL_PROVIDER"] = provider
        env["NULLION_MODEL"] = model_name
        stored = load_stored_credentials() or {}
        stored_keys = stored.get("keys")
        if not isinstance(stored_keys, dict):
            stored_keys = {}
        stored_key = str(stored_keys.get(provider) or "")
        if not stored_key and str(stored.get("provider") or "").strip().lower() == provider:
            stored_key = str(stored.get("api_key") or "")
        if stored_key.strip():
            env["NULLION_OPENAI_API_KEY"] = stored_key.strip()
        if provider in {"openrouter", "openrouter-key"}:
            env.setdefault("NULLION_OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
        settings = load_settings(env=env)
        settings.model.provider = provider
        settings.model.openai_model = model_name
        new_client = build_model_client_from_settings(settings)
        self.model_client = new_client
        self.agent_orchestrator = AgentOrchestrator(model_client=new_client)

    def refresh_model_client_from_saved_settings(self) -> None:
        """Refresh the live model client from the persisted runtime model choice."""
        from nullion.runtime_config import current_runtime_config

        cfg = current_runtime_config(model_client=self.model_client)
        provider = (cfg.admin_forced_provider or cfg.provider or "").strip().lower()
        model_name = (cfg.admin_forced_model or cfg.model or "").strip()
        if not model_name:
            return
        if provider:
            self.swap_provider_model_client(provider, model_name)
            return
        self.swap_model_client(model_name)

    def _media_model_for_attachments(
        self,
        attachments: list[dict[str, str]] | None,
    ) -> tuple[object | None, object | None]:
        if not _attachments_include_video(attachments):
            return self.model_client, self.agent_orchestrator
        provider = os.environ.get("NULLION_VIDEO_INPUT_PROVIDER", "").strip()
        model_name = os.environ.get("NULLION_VIDEO_INPUT_MODEL", "").strip()
        enabled = os.environ.get("NULLION_VIDEO_INPUT_ENABLED")
        if enabled is not None and enabled.strip().lower() in {"0", "false", "no", "off"}:
            return self.model_client, self.agent_orchestrator
        if not provider or not model_name:
            return self.model_client, self.agent_orchestrator
        from nullion.providers import _media_settings_for_model

        settings = _media_settings_for_model(provider, model_name)
        media_client = build_model_client_from_settings(settings)
        return media_client, AgentOrchestrator(model_client=media_client)

    def handle_text_message(
        self,
        *,
        text: str,
        chat_id: str | None = None,
        reminder_chat_id: str | None = None,
        attachments: list[dict[str, str]] | None = None,
        request_id: str | None = None,
        message_id: str | None = None,
        activity_callback=None,
        text_delta_callback=None,
        append_activity_trace: bool = True,
        allow_mini_agents: bool = False,
        turn_dispatch_decision=None,
        cancellation_checker=None,
        reply_context: dict[str, object] | None = None,
    ) -> str | None:
        started_at = time.perf_counter()
        timing_marks: list[str] = []
        timing_last_at = started_at

        def _mark_timing(label: str) -> None:
            nonlocal timing_last_at
            now = time.perf_counter()
            timing_marks.append(f"{label}:{round((now - timing_last_at) * 1000, 1)}ms")
            timing_last_at = now

        from nullion.reminders import reminder_chat_context

        model_client, agent_orchestrator = self._media_model_for_attachments(attachments)
        _mark_timing("media_model_selection")
        with reminder_chat_context(reminder_chat_id or chat_id):
            reply = handle_chat_operator_message(
                self.runtime,
                text,
                chat_id=chat_id,
                attachments=attachments,
                settings=self.settings,
                request_id=request_id,
                message_id=message_id,
                reply_context=reply_context,
                model_client=model_client,
                agent_orchestrator=agent_orchestrator,
                service=self,
                activity_callback=activity_callback,
                text_delta_callback=text_delta_callback,
                append_activity_trace=append_activity_trace,
                allow_mini_agents=allow_mini_agents,
                turn_dispatch_decision=turn_dispatch_decision,
                cancellation_checker=cancellation_checker,
                conversation_ingress_id=_ingress_dedupe_key(request_id=request_id, message_id=message_id),
            )
        _mark_timing("handle_chat_operator_message")

        total_ms = (time.perf_counter() - started_at) * 1000
        if total_ms >= _float_env_ms(_NULLION_TELEGRAM_FLOW_SLOW_LOG_MS, default=1200.0):
            logger.warning(
                "telegram handle_text_message timing chat_id=%s request_id=%s message_id=%s total_ms=%.1f phases=%s",
                str(chat_id or ""),
                request_id,
                str(message_id or ""),
                total_ms,
                ", ".join(timing_marks),
            )
        return reply

    def handle_update(
        self,
        update,
        *,
        attachments: list[dict[str, str]] | None = None,
        activity_callback=None,
        text_delta_callback=None,
        append_activity_trace: bool = True,
        turn_dispatch_decision=None,
        cancellation_checker=None,
    ) -> str | None:
        started_at = time.perf_counter()
        timing_marks: list[str] = []
        timing_last_at = started_at

        def _mark_timing(label: str) -> None:
            nonlocal timing_last_at
            now = time.perf_counter()
            timing_marks.append(f"{label}:{round((now - timing_last_at) * 1000, 1)}ms")
            timing_last_at = now

        message = getattr(update, "message", None)
        if message is None:
            message = getattr(update, "effective_message", None)
        if message is None:
            logger.info("Ignored Telegram update without message")
            return None
        _mark_timing("message_read")

        text = _message_text_or_caption(message)
        if text is None and not attachments:
            chat = getattr(message, "chat", None)
            chat_id = None if chat is None else getattr(chat, "id", None)
            logger.info("Ignored Telegram update without text (chat_id=%s)", chat_id)
            return None
        if text is None:
            text = "Please analyze the attached file(s)."
        _mark_timing("message_text")

        chat = getattr(message, "chat", None)
        chat_id = None if chat is None else getattr(chat, "id", None)
        if chat_id is not None:
            chat_id = str(chat_id)
        _mark_timing("chat_id")

        request_id = _telegram_request_id(update)
        message_id = _telegram_message_id(message=message, chat_id=chat_id)
        reply_context = _telegram_reply_context(message)

        _mark_timing("ids_ready")

        result = self.handle_text_message(
            text=text,
            chat_id=chat_id,
            attachments=attachments,
            request_id=request_id,
            message_id=message_id,
            reply_context=reply_context,
            activity_callback=activity_callback,
            text_delta_callback=text_delta_callback,
            append_activity_trace=append_activity_trace,
            turn_dispatch_decision=turn_dispatch_decision,
            cancellation_checker=cancellation_checker,
        )
        _mark_timing("handle_text_message")
        total_ms = (time.perf_counter() - started_at) * 1000
        if total_ms >= _float_env_ms(_NULLION_TELEGRAM_FLOW_SLOW_LOG_MS, default=1200.0):
            logger.warning(
                "telegram handle_update detail timing chat_id=%s message_id=%s request_id=%s total_ms=%.1f phases=%s",
                str(chat_id or ""),
                str(message_id or ""),
                request_id,
                total_ms,
                ", ".join(timing_marks),
            )
        return result
    async def deliver_due_reminders(self, *, bot, now: datetime | None = None) -> None:
        from nullion.reminder_delivery import deliver_due_reminders_once

        async def _send(chat_id: str, text: str) -> bool:
            if ":" in str(chat_id):
                return False
            await retry_messaging_delivery_operation(
                lambda: bot.send_message(
                    chat_id,
                    text,
                    disable_web_page_preview=True,
                )
            )
            return True

        await deliver_due_reminders_once(self.runtime, send=_send, now=now, settings=self.settings)

    async def _maybe_do_first_run_setup(self, message, chat_id_text: str | None) -> bool:
        """If no operator chat ID is configured, lock in the first sender.

        Returns True if first-run setup was triggered (caller should not
        continue with normal message handling).

        Uses _first_run_lock to prevent a race where two simultaneous messages
        both observe operator_chat_id == None and both claim operator status.
        """
        if self.operator_chat_id is not None:
            return False  # already configured — fast path, no lock needed
        if not chat_id_text or message is None:
            return False

        async with self._first_run_lock:
            # Re-check inside the lock; another coroutine may have won the race.
            if self.operator_chat_id is not None:
                return False

            # Lock in this chat immediately — do all writes while still holding
            # the lock so no other coroutine can observe a partially-set state.
            self.operator_chat_id = chat_id_text
            logger.info("First-run setup: locked operator chat to %s", chat_id_text)

            # Also update the runtime settings in memory so _is_authorized_chat
            # passes for this and all future messages.
            try:
                self.settings.telegram.operator_chat_id = chat_id_text
            except Exception:
                pass

        # Persist to .env outside the lock (I/O — no data race concern here
        # because operator_chat_id is already committed above).
        try:
            from nullion.web_app import _find_env_path, _write_env_updates
            env_path = _find_env_path()
            if env_path:
                _write_env_updates(env_path, {"NULLION_TELEGRAM_OPERATOR_CHAT_ID": chat_id_text})
                logger.info("First-run setup: persisted operator_chat_id to %s", env_path)
        except Exception:
            logger.debug("First-run setup: could not persist chat ID to .env", exc_info=True)

        setup_text = (
            "👋 Hi! I'm Nullion — your personal AI assistant.\n\n"
            "I've locked myself to this chat so only you can talk to me. "
            "If you ever need to change this, edit `NULLION_TELEGRAM_OPERATOR_CHAT_ID` in your `.env` file.\n\n"
            "What would you like to do? You can just ask me anything — or type /help to see what I can do."
        )
        try:
            await retry_messaging_delivery_operation(
                lambda: message.reply_text(setup_text, do_quote=False)
            )
        except Exception:
            logger.debug("First-run setup: failed to send welcome message", exc_info=True)
        return True  # handled; skip normal flow

    async def on_message(self, update, context) -> None:
        started_at = time.perf_counter()
        timing_marks: list[str] = []
        timing_last_at = started_at
        turn_outcome = "completed"

        def _mark_timing(label: str) -> None:
            nonlocal timing_last_at
            now = time.perf_counter()
            timing_marks.append(f"{label}:{round((now - timing_last_at) * 1000, 1)}ms")
            timing_last_at = now

        def _log_turn_timing(outcome: str) -> None:
            total_ms = (time.perf_counter() - started_at) * 1000
            if total_ms < _float_env_ms(_NULLION_TELEGRAM_TURN_SLOW_LOG_MS, default=1000.0):
                return
            decision = getattr(turn_registration_local, "decision", None) if turn_registration_local is not None else None
            phase_data = ", ".join(timing_marks)
            logger.warning(
                "messaging turn slow timing telegram chat_id=%s request_id=%s message_id=%s dedupe=%s outcome=%s turn_id=%s dispatch_reason=%s dispatch_disposition=%s total_ms=%.1f phases=%s",
                chat_id_text,
                request_id_local,
                message_id_local,
                dedupe_key,
                outcome,
                getattr(decision, "turn_id", None) if decision is not None else None,
                getattr(decision, "reason", None),
                getattr(getattr(decision, "disposition", None), "value", getattr(decision, "disposition", None)),
                total_ms,
                phase_data,
            )
            try:
                record_surface_latency_timing(
                    self.runtime.store,
                    surface="telegram",
                    conversation_id=early_conversation_id,
                    turn_id=getattr(decision, "turn_id", None) if decision is not None else None,
                    request_id=request_id_local,
                    message_id=message_id_local,
                    outcome=outcome,
                    total_ms=total_ms,
                    phases=timing_marks,
                    logger=logger,
                )
                self.runtime.store.add_event(
                    make_event(
                        event_type="telegram.turn_timing",
                        actor="telegram",
                        payload={
                            "request_id": request_id_local,
                            "conversation_id": early_conversation_id,
                            "message_id": message_id_local,
                            "dedupe_key": dedupe_key,
                            "outcome": outcome,
                            "turn_id": getattr(decision, "turn_id", None) if decision is not None else None,
                            "dispatch_reason": getattr(decision, "reason", None),
                            "dispatch_disposition": getattr(
                                getattr(decision, "disposition", None), "value", getattr(decision, "disposition", None)
                            ),
                            "total_ms": round(total_ms, 1),
                            "phases": timing_marks,
                            "platform": "telegram",
                        },
                    )
                )
            except Exception:
                logger.debug("Failed to record telegram turn timing event", exc_info=True)
            # The text log is the durable latency record. Do not force a
            # checkpoint from the slow-log path itself, or the act of measuring
            # a slow turn adds another multi-second runtime save.

        turn_registration_local = None
        request_id_local = None
        message_id_local = None
        dedupe_key: str | None = None
        early_conversation_id = None

        message = getattr(update, "message", None)
        if message is None:
            message = getattr(update, "effective_message", None)
        text_for_ack = _message_text_or_caption(message)
        chat = None if message is None else getattr(message, "chat", None)
        chat_id = None if chat is None else getattr(chat, "id", None)
        chat_id_text = None if chat_id is None else str(chat_id)
        early_conversation_id = None if chat_id_text is None else f"telegram:{chat_id_text}"
        _mark_timing("received")

        # ── First-run: self-discover operator chat if not yet configured ────
        if await self._maybe_do_first_run_setup(message, chat_id_text):
            turn_outcome = "first_run_setup"
            _log_turn_timing(turn_outcome)
            return
        request_id = _telegram_request_id(update)
        request_id_local = request_id
        if message is not None and request_id_local is not None:
            message_id_local = _telegram_message_id(message=message, chat_id=chat_id_text)
        if isinstance(text_for_ack, str) and (
            not text_for_ack.startswith("/")
            or not is_operator_command_text(text_for_ack)
        ) and self.settings.telegram.chat_enabled:
            _mark_timing("model_health_precheck_started")
            health_reply = _chat_model_issue_reply(self.runtime, message=text_for_ack)
            _mark_timing("model_health_precheck_complete")
            if health_reply is not None:
                if message is not None:
                    await _deliver_reply(
                        message,
                        health_reply,
                        request_id=request_id,
                        phase="telegram_health_gate",
                    )
                turn_outcome = "model_health_gate"
                _log_turn_timing(turn_outcome)
                return
        if text_for_ack is not None and _refresh_runtime_from_checkpoint(
            self.runtime,
            min_interval_seconds=(
                _float_env_ms(_NULLION_TELEGRAM_CHECKPOINT_REFRESH_MIN_INTERVAL_MS, default=60000.0) / 1000.0
            ),
        ):
            _mark_timing("runtime_checkpoint_refreshed")
        if is_stop_command_text(text_for_ack):
            _mark_timing("stop_check")
            stop_reply = "Unauthorized messaging identity."
            if _is_authorized_chat(chat_id_text, self.settings):
                stopped_activity_count = await self._activity_streams.stop_conversation(early_conversation_id)
                stop_result = await stop_session_async(
                    conversation_id=early_conversation_id or f"telegram:{chat_id_text or 'default'}",
                    runtime=self.runtime,
                    agent_orchestrator=self.agent_orchestrator,
                    turn_tracker=self._turn_dispatch_tracker,
                )
                if stopped_activity_count:
                    stop_result = replace(stop_result, cancelled_activity_count=stopped_activity_count)
                stop_reply = stop_session_reply(stop_result)
            if message is not None:
                await retry_messaging_delivery_operation(
                    lambda: message.reply_text(stop_reply, do_quote=False)
                )
            turn_outcome = "stop_command"
            _log_turn_timing(turn_outcome)
            return
        self.refresh_live_configuration()
        _mark_timing("config_refreshed")

        telegram_attachments = await _download_telegram_attachments(message, context, settings=self.settings)
        if text_for_ack is None and telegram_attachments:
            text_for_ack = "Please analyze the attached file(s)."
        _mark_timing("attachments_downloaded")

        inflight_key = chat_id_text or "default"
        conversation_id = None if chat_id_text is None else f"telegram:{chat_id_text}"
        should_decrement = False
        if request_id is None:
            request_id = _telegram_request_id(update)
        message_id = _telegram_message_id(message=message, chat_id=chat_id_text)
        reply_context = _telegram_reply_context(message)
        dedupe_key = _ingress_dedupe_key(request_id=request_id, message_id=message_id)
        request_id_local = request_id
        message_id_local = message_id

        if dedupe_key is not None and dedupe_key in self._seen_ingress_ids:
            turn_outcome = "duplicate_ingress_inmemory"
            logger.info("Ignored duplicate Telegram ingress (chat_id=%s, ingress_id=%s)", chat_id_text, dedupe_key)
            _log_turn_timing(turn_outcome)
            return
        if (
            dedupe_key is not None
            and conversation_id is not None
            and self.runtime.store.has_conversation_ingress_id(conversation_id, dedupe_key)
        ):
            turn_outcome = "duplicate_ingress_store"
            logger.info("Ignored duplicate Telegram ingress (chat_id=%s, ingress_id=%s)", chat_id_text, dedupe_key)
            _log_turn_timing(turn_outcome)
            return

        busy_ack = None
        busy_ack_sent = False
        if self._inflight_chat_by_id[inflight_key] > 0:
            busy_ack = _foreground_working_ack_text(
                text_for_ack,
                chat_id=chat_id_text,
                settings=self.settings,
            )
        if busy_ack is not None and message is not None:
            try:
                await retry_messaging_delivery_operation(
                    lambda: message.reply_text(busy_ack, do_quote=_should_quote_reply(text_for_ack))
                )
                busy_ack_sent = True
            except Exception:
                _report_runner_health_issue(
                    self.runtime,
                    issue_type=HealthIssueType.ERROR,
                    message="Telegram operator queued acknowledgment delivery failed.",
                    chat_id=chat_id_text,
                    text=text_for_ack,
                    stage="ack_delivery",
                    detail="Failed to send queued chat acknowledgment.",
                )
                raise

        turn_registration = await self._turn_dispatch_tracker.register(
            conversation_id or f"telegram:{inflight_key}",
            text_for_ack or "",
            turn_id=dedupe_key or message_id or request_id,
            model_client=self.model_client,
            reply_context=reply_context,
        )
        turn_registration_local = turn_registration
        _mark_timing("turn_registered")

        if busy_ack is None and self._inflight_chat_by_id[inflight_key] > 0:
            busy_ack = _busy_chat_ack_text(
                text_for_ack,
                chat_id=chat_id_text,
                settings=self.settings,
                dispatch_decision=turn_registration.decision,
            )
        self._inflight_chat_by_id[inflight_key] += 1
        should_decrement = True

        if (
            busy_ack is not None
            and message is not None
            and not busy_ack_sent
            and turn_registration.decision.should_wait
        ):
            try:
                await retry_messaging_delivery_operation(
                    lambda: message.reply_text(busy_ack, do_quote=_should_quote_reply(text_for_ack))
                )
                busy_ack_sent = True
            except Exception:
                _report_runner_health_issue(
                    self.runtime,
                    issue_type=HealthIssueType.ERROR,
                    message="Telegram operator queued acknowledgment delivery failed.",
                    chat_id=chat_id_text,
                    text=text_for_ack,
                    stage="ack_delivery",
                    detail="Failed to send queued chat acknowledgment.",
                )
                await turn_registration.finish()
                raise

        typing_keepalive_task = asyncio.create_task(
            _run_typing_keepalive(message, runtime=self.runtime, text=text_for_ack)
        )
        _mark_timing("typing_keepalive_started")
        planner_status_requested = isinstance(text_for_ack, str) and parse_planner_command(text_for_ack).requested
        should_live_stream_activity = _should_live_stream_telegram_activity(
            self.runtime,
            chat_id=chat_id_text,
            message=message,
            text=text_for_ack,
            planner_status_requested=planner_status_requested,
        )
        activity_streamer = (
            _TelegramActivityStreamer(message, runtime=self.runtime, typing_text=text_for_ack)
            if should_live_stream_activity
            else None
        )
        tool_working_ack_sent = False
        telegram_loop = asyncio.get_running_loop()

        def _emit_activity_with_tool_ack(event: dict[str, str]) -> None:
            nonlocal tool_working_ack_sent
            if activity_streamer is not None:
                activity_streamer.emit(event)
            event_id = str(event.get("id") or "")
            event_tool_name = str(event.get("tool_name") or "")
            if (
                tool_working_ack_sent
                or busy_ack is not None
                or message is None
                or not (
                    event_id.startswith("tool-")
                    or event_id == "mini-agents"
                    or bool(event_tool_name)
                )
            ):
                return
            working_ack = _foreground_working_ack_text(
                text_for_ack,
                chat_id=chat_id_text,
                settings=self.settings,
            )
            if working_ack is None:
                return
            tool_working_ack_sent = True
            try:
                asyncio.run_coroutine_threadsafe(
                    retry_messaging_delivery_operation(
                        lambda: message.reply_text(working_ack, do_quote=_should_quote_reply(text_for_ack))
                    ),
                    telegram_loop,
                )
            except Exception:
                logger.debug("Telegram working acknowledgement delivery failed", exc_info=True)

        await self._activity_streams.register(conversation_id, activity_streamer)
        _mark_timing("activity_streamer_registered")

        reply = None
        decision_card = None
        _new_reminder_card = None
        _suggestion_markup = None
        reply_activity_phase = RunActivityPhase.ACTIVE
        keep_typing_until_delivery = False
        suppress_primary_reply_delivery = False
        ingress_checkpoint_deferred = False

        try:
            async with turn_registration:
                _mark_timing("handler_enter")
                if dedupe_key is not None and dedupe_key in self._seen_ingress_ids:
                    logger.info("Ignored duplicate Telegram ingress (chat_id=%s, ingress_id=%s)", chat_id_text, dedupe_key)
                    turn_outcome = "duplicate_ingress_inlock"
                    _log_turn_timing(turn_outcome)
                    return
                if (
                    dedupe_key is not None
                    and conversation_id is not None
                    and self.runtime.store.has_conversation_ingress_id(conversation_id, dedupe_key)
                ):
                    logger.info("Ignored duplicate Telegram ingress (chat_id=%s, ingress_id=%s)", chat_id_text, dedupe_key)
                    turn_outcome = "duplicate_ingress_store_inlock"
                    _log_turn_timing(turn_outcome)
                    return
                if dedupe_key is not None:
                    self._seen_ingress_ids.add(dedupe_key)
                _mark_timing("dedupe_locked_checked")

                decision_snapshot = _capture_decision_snapshot(self.runtime)
                # Snapshot reminder IDs before processing so we can detect newly-created reminders.
                _reminder_ids_before = frozenset(self.runtime.store.reminders.keys())
                # Intercept /help to deliver an inline keyboard menu instead of plain text
                _help_card = None
                _help_text_raw = _message_text_or_caption(
                    getattr(update, "message", None) or getattr(update, "effective_message", None)
                )
                if isinstance(_help_text_raw, str) and _help_text_raw.strip().lower() == "/help":
                    _help_card = _build_help_menu_card()
                    if _help_card is not None:
                        if dedupe_key is not None and conversation_id is not None:
                            self.runtime.store.add_conversation_ingress_id(conversation_id, dedupe_key)
                            self.runtime.checkpoint()
                            _cache_runtime_checkpoint_signature(self.runtime)
                        _mark_timing("help_card_delivery")
                        await _deliver_reply(
                            getattr(update, "message", None) or getattr(update, "effective_message", None),
                            _help_card.text,
                            decision_card=_help_card,
                            request_id=request_id,
                            turn_id=turn_registration.turn_id,
                            phase="telegram_help_card",
                        )
                        turn_outcome = "help_card"
                        _log_turn_timing(turn_outcome)
                        return
                if isinstance(_help_text_raw, str):
                    verbose_head = _help_text_raw.strip().split(maxsplit=1)[0].partition("@")[0].lower()
                    if verbose_head == "/models":
                        _models_card = _build_models_card(self.runtime)
                        if _models_card is not None:
                            if dedupe_key is not None and conversation_id is not None:
                                self.runtime.store.add_conversation_ingress_id(conversation_id, dedupe_key)
                                self.runtime.checkpoint()
                                _cache_runtime_checkpoint_signature(self.runtime)
                            _mark_timing("models_card_delivery")
                            await _deliver_reply(
                                getattr(update, "message", None) or getattr(update, "effective_message", None),
                                _models_card.text,
                                decision_card=_models_card,
                                request_id=request_id,
                                turn_id=turn_registration.turn_id,
                                phase="telegram_models_card",
                            )
                            turn_outcome = "models_card"
                            _log_turn_timing(turn_outcome)
                            return
                    if verbose_head == "/verbose":
                        _verbose_parts = _help_text_raw.strip().split()
                        if len(_verbose_parts) == 1 or _verbose_parts[1].lower() in {"status", "show"}:
                            _verbose_card = _build_verbose_settings_card(self.runtime, chat_id=chat_id_text)
                            if _verbose_card is not None:
                                if dedupe_key is not None and conversation_id is not None:
                                    self.runtime.store.add_conversation_ingress_id(conversation_id, dedupe_key)
                                    self.runtime.checkpoint()
                                    _cache_runtime_checkpoint_signature(self.runtime)
                                _mark_timing("verbose_card_delivery")
                                await _deliver_reply(
                                    getattr(update, "message", None) or getattr(update, "effective_message", None),
                                    _verbose_card.text,
                                    decision_card=_verbose_card,
                                    request_id=request_id,
                                    turn_id=turn_registration.turn_id,
                                    phase="telegram_verbose_card",
                                )
                                turn_outcome = "verbose_card"
                                _log_turn_timing(turn_outcome)
                                return
                handler_error = None
                try:
                    update_message = getattr(update, "message", None) or getattr(update, "effective_message", None)
                    text_streamer = _TelegramTextDeltaStreamer(
                        loop=asyncio.get_running_loop(),
                        message=update_message,
                    )
                    if activity_streamer is not None:
                        if planner_status_requested:
                            activity_streamer.emit({
                                "id": "planner",
                                "label": "Planner",
                                "status": "running",
                                "detail": "Building and running the plan",
                            })
                        activity_streamer.emit({"id": "prepare", "label": "Preparing request", "status": "running"})
                    _mark_timing("handler_dispatched")
                    reply = await asyncio.to_thread(
                        _call_handle_update_with_activity,
                        self,
                        update,
                        attachments=telegram_attachments,
                        activity_callback=_emit_activity_with_tool_ack if activity_streamer is not None else None,
                        text_delta_callback=text_streamer.emit,
                        append_activity_trace=activity_streamer is None and not planner_status_requested,
                        turn_dispatch_decision=turn_registration.decision,
                        cancellation_checker=turn_registration.is_cancelled,
                    )
                    _mark_timing("handler_completed")
                    # The chat handler may checkpoint the same in-memory runtime
                    # before returning. Cache that file signature here so the
                    # next Telegram update does not reload our own last save.
                    _cache_runtime_checkpoint_signature(self.runtime)
                except Exception as exc:
                    handler_error = exc
                    if dedupe_key is not None:
                        self._seen_ingress_ids.discard(dedupe_key)
                    _msg = getattr(update, "message", None)
                    if _msg is None:
                        effective_chat = getattr(update, "effective_chat", None)
                        effective_message = getattr(update, "effective_message", None)
                        _chat_id = None if effective_chat is None else getattr(effective_chat, "id", None)
                        text = _message_text_or_caption(effective_message)
                    else:
                        _chat = getattr(_msg, "chat", None)
                        _chat_id = None if _chat is None else getattr(_chat, "id", None)
                        text = _message_text_or_caption(_msg)

                    logger.exception(
                        "Failed to handle Telegram update (chat_id=%s, text=%s)",
                        _chat_id,
                        text,
                    )
                    reply = _operator_visible_error(exc)
                if reply is None:
                    if dedupe_key is not None and conversation_id is not None:
                        self.runtime.store.add_conversation_ingress_id(conversation_id, dedupe_key)
                        self.runtime.checkpoint(force=True)
                        _cache_runtime_checkpoint_signature(self.runtime)
                    turn_outcome = "empty_reply"
                    _log_turn_timing(turn_outcome)
                    return
                turn_superseded = handler_error is None and await turn_registration.is_superseded()
                suppress_primary_reply_delivery = handler_error is None and (
                    turn_superseded
                    or _should_suppress_planner_status_ack(
                        self.runtime,
                        chat_id=chat_id_text,
                        reply=reply,
                        status_stream_delivered=bool(
                            activity_streamer is not None and activity_streamer.saw_planner_status
                        ),
                    )
                )
                # Persist to unified chat history
                try:
                    from nullion.chat_store import get_chat_store as _get_chat_store
                    _store = _get_chat_store()
                    _channel = f"telegram:{chat_id_text}" if chat_id_text else "telegram:unknown"
                    _channel_label = _get_telegram_channel_label(message, chat_id_text or "unknown")
                    _user_text = _message_text_or_caption(message) or ""
                    _visible_reply_for_history = strip_tool_approval_marker(reply)
                    if _user_text:
                        _store.save_message(conversation_id or _channel, "user", _user_text,
                                           channel=_channel, channel_label=_channel_label)
                    if _visible_reply_for_history and not suppress_primary_reply_delivery:
                        _store.save_message(conversation_id or _channel, "bot", str(_visible_reply_for_history),
                                           channel=_channel, channel_label=_channel_label)
                except Exception:
                    pass  # best-effort, never break the telegram flow
                if handler_error is None and dedupe_key is not None and conversation_id is not None:
                    if not self.runtime.store.has_conversation_ingress_id(conversation_id, dedupe_key):
                        self.runtime.store.add_conversation_ingress_id(conversation_id, dedupe_key)
                        _mark_runtime_checkpoint_signature_stale(self.runtime)
                        ingress_checkpoint_deferred = True
                if handler_error is None:
                    decision_card = _new_decision_card(self.runtime, decision_snapshot)
                    if decision_card is None:
                        decision_card = _existing_pending_approval_card(self.runtime, reply)
                    # UX-16: detect newly-created reminders for a confirmation card.
                    _new_reminder_card = _check_new_reminder_card(self.runtime, _reminder_ids_before)
                    # UX-9: detect safe-alternative suggestions in refusal replies.
                    _suggestion_markup = None
                    if decision_card is None and isinstance(reply, str):
                        _alternatives = _extract_safe_alternatives(reply)
                        if _alternatives:
                            _suggestion_markup = _build_suggestion_markup(_alternatives)
                reply_activity_phase = classify_run_activity_phase(reply=reply)
                keep_typing_until_delivery = True
                _mark_timing("handler_success")
        finally:
            if not keep_typing_until_delivery:
                await _stop_typing_keepalive(typing_keepalive_task)
                await self._activity_streams.unregister(conversation_id, activity_streamer)
            if should_decrement:
                self._inflight_chat_by_id[inflight_key] -= 1
                if self._inflight_chat_by_id[inflight_key] <= 0:
                    self._inflight_chat_by_id.pop(inflight_key, None)

        try:
            if reply is None:
                _log_turn_timing(turn_outcome)
                return

            if reply_activity_phase is RunActivityPhase.WAITING_APPROVAL:
                logger.info("Telegram run entered waiting_approval phase (chat_id=%s)", chat_id_text)
            activity_finish_task = None
            if activity_streamer is not None:
                activity_finish_task = asyncio.create_task(activity_streamer.finish())
            if _should_skip_telegram_primary_reply_delivery(
                suppress_primary_reply_delivery=suppress_primary_reply_delivery,
                decision_card=decision_card,
            ):
                if activity_finish_task is not None:
                    await activity_finish_task
                if ingress_checkpoint_deferred:
                    await asyncio.to_thread(self.runtime.checkpoint)
                    _cache_runtime_checkpoint_signature(self.runtime)
                turn_outcome = "suppressed_by_planner_ack"
                _log_turn_timing(turn_outcome)
                return
            if getattr(reply, "reply_already_sent", False):
                if activity_finish_task is not None:
                    await activity_finish_task
                await text_streamer.finish(str(reply))
                if ingress_checkpoint_deferred:
                    await asyncio.to_thread(self.runtime.checkpoint)
                    _cache_runtime_checkpoint_signature(self.runtime)
                turn_outcome = "streamed_reply"
                _mark_timing("delivery_complete")
                _log_turn_timing(turn_outcome)
                return

            message = getattr(update, "message", None)
            if message is None:
                message = getattr(update, "effective_message", None)
                text = _message_text_or_caption(message)
                if message is None:
                    effective_chat = getattr(update, "effective_chat", None)
                    effective_message = getattr(update, "effective_message", None)
                    chat_id = None if effective_chat is None else getattr(effective_chat, "id", None)
                    text = _message_text_or_caption(effective_message)
                _report_runner_health_issue(
                    self.runtime,
                    issue_type=HealthIssueType.ISSUE,
                    message="Telegram operator reply was dropped because the update message was missing.",
                    chat_id=None if chat_id is None else str(chat_id),
                    text=text,
                    stage="reply_dropped",
                    detail="Update produced a reply but no Telegram message object was available for delivery.",
                )
                logger.warning(
                    "Dropped Telegram operator reply because update message was missing (chat_id=%s, text=%s)",
                    chat_id,
                    text,
                )
                if ingress_checkpoint_deferred:
                    await asyncio.to_thread(self.runtime.checkpoint)
                    _cache_runtime_checkpoint_signature(self.runtime)
                turn_outcome = "reply_dropped_missing_message"
                _log_turn_timing(turn_outcome)
                return

            stream_final_reply = (
                isinstance(text_for_ack, str)
                and not text_for_ack.strip().startswith("/")
                and _local_chat_reply_body(text_for_ack) is None
            )
            try:
                _mark_timing("delivery_dispatch")
                principal_id = _principal_id_for_telegram_message(message, self.settings)
                delivery_plan = plan_telegram_post_run_delivery(
                    text_for_ack=text_for_ack,
                    reply=reply,
                    inbound_attachments=telegram_attachments,
                    runtime=self.runtime,
                    conversation_id=conversation_id,
                    decision_card=decision_card,
                    suggestion_markup=_suggestion_markup,
                    stream_final_reply=stream_final_reply,
                    streaming_mode=_telegram_streaming_mode(self.runtime, chat_id=chat_id_text),
                    final_only_streaming_mode=ChatStreamingMode.FINAL_ONLY,
                )
                await _deliver_reply(
                    message,
                    reply,
                    decision_card=delivery_plan.primary_card,
                    additional_markup=delivery_plan.additional_markup,
                    streaming_mode=delivery_plan.streaming_mode,
                    principal_id=principal_id,
                    delivery_contract=delivery_plan.delivery_contract,
                    request_id=request_id,
                    turn_id=turn_registration.turn_id,
                    phase="telegram_primary",
                )
                if delivery_plan.supplemental_card is not None:
                    supplemental_task = asyncio.create_task(
                        _deliver_reply(
                            message,
                            delivery_plan.supplemental_card.text,
                            decision_card=delivery_plan.supplemental_card,
                            principal_id=principal_id,
                            request_id=request_id,
                            turn_id=turn_registration.turn_id,
                            phase="telegram_supplemental",
                        )
                    )
                    supplemental_task.add_done_callback(_log_background_delivery_error)
                _mark_timing("delivery_complete")
                if ingress_checkpoint_deferred:
                    await asyncio.to_thread(self.runtime.checkpoint)
                    _cache_runtime_checkpoint_signature(self.runtime)
            except Exception:
                if ingress_checkpoint_deferred:
                    try:
                        await asyncio.to_thread(self.runtime.checkpoint)
                        _cache_runtime_checkpoint_signature(self.runtime)
                    except Exception:
                        logger.debug("Failed to persist deferred Telegram ingress checkpoint", exc_info=True)
                chat = getattr(message, "chat", None)
                chat_id = None if chat is None else getattr(chat, "id", None)
                _report_runner_health_issue(
                    self.runtime,
                    issue_type=HealthIssueType.ERROR,
                    message="Telegram operator reply delivery failed.",
                    chat_id=None if chat_id is None else str(chat_id),
                    text=_message_text_or_caption(message),
                    stage="reply_delivery",
                    detail="Failed to send Telegram operator reply.",
                )
                logger.exception(
                    "Failed to send Telegram operator reply (chat_id=%s, text=%s)",
                    chat_id,
                    _message_text_or_caption(message),
                )
                turn_outcome = "reply_delivery_failed"
                _log_turn_timing(turn_outcome)
                if handler_error is not None:
                    raise handler_error
                raise

            # UX-16: send a separate reminder confirmation card if a reminder was just created.
            if _new_reminder_card is not None:
                try:
                    await _deliver_reply(
                        message,
                        _new_reminder_card.text,
                        decision_card=_new_reminder_card,
                        request_id=request_id,
                        turn_id=turn_registration.turn_id,
                        phase="telegram_reminder_card",
                    )
                except Exception:
                    logger.warning("Failed to send reminder confirmation card", exc_info=True)
            turn_outcome = "completed"
            _log_turn_timing(turn_outcome)
        finally:
            if "activity_finish_task" in locals() and activity_finish_task is not None and not activity_finish_task.done():
                def _consume_activity_finish_error(task) -> None:  # noqa: ANN001
                    if task.cancelled():
                        return
                    try:
                        task.exception()
                    except Exception:
                        logger.debug("Telegram activity finish task failed", exc_info=True)

                activity_finish_task.add_done_callback(_consume_activity_finish_error)
            await self._activity_streams.unregister(conversation_id, activity_streamer)
            await _stop_typing_keepalive(typing_keepalive_task)

    async def on_callback_query(self, update, context) -> None:
        del context
        callback_query = getattr(update, "callback_query", None)
        if callback_query is None:
            logger.info("Ignored Telegram callback without callback_query")
            return

        message = getattr(callback_query, "message", None)
        chat = None if message is None else getattr(message, "chat", None)
        chat_id = None if chat is None else getattr(chat, "id", None)
        chat_id_text = None if chat_id is None else str(chat_id)

        # Authorise against the *clicker's* user ID, not the chat the message
        # lives in.  Using chat_id would allow any member of a group to click
        # inline buttons and execute operator actions.
        from_user = getattr(callback_query, "from_user", None)
        from_user_id = None if from_user is None else getattr(from_user, "id", None)
        from_user_id_text = None if from_user_id is None else str(from_user_id)
        if not _is_authorized_chat(from_user_id_text, self.settings):
            await _answer_callback_query_safely(callback_query, "Unauthorized", show_alert=False)
            return

        parsed = _parse_callback_data(getattr(callback_query, "data", None))
        if parsed is None:
            await _answer_callback_query_safely(callback_query, "Unknown action", show_alert=False)
            return
        kind, action, record_id = parsed

        # UX-9: suggestion buttons — process the suggestion as a new user message.
        if kind == "suggestion" and action == "send":
            await _answer_callback_query_safely(callback_query, "On it!")
            suggestion_text = record_id  # text stored directly in callback record_id
            if suggestion_text and message is not None:
                self.refresh_live_configuration()
                typing_keepalive_task = asyncio.create_task(
                    _run_typing_keepalive(message, runtime=self.runtime, text=suggestion_text)
                )
                try:
                    from nullion.chat_operator import handle_chat_operator_message as _handle_msg
                    suggestion_reply = await asyncio.to_thread(
                        _handle_msg,
                        self.runtime,
                        suggestion_text,
                        chat_id=chat_id_text,
                        model_client=self.model_client,
                        agent_orchestrator=self.agent_orchestrator,
                        service=self,
                        settings=self.settings,
                    )
                    if suggestion_reply:
                        await _send_callback_follow_up(
                            message,
                            suggestion_reply,
                            principal_id=_principal_id_for_telegram_message(message, self.settings),
                        )
                except Exception:
                    logger.exception(
                        "Failed to process suggestion callback (chat_id=%s, suggestion=%r)",
                        chat_id_text,
                        suggestion_text,
                    )
                finally:
                    await _stop_typing_keepalive(typing_keepalive_task)
            return

        typing_keepalive_task = (
            asyncio.create_task(
                _run_typing_keepalive(message, runtime=self.runtime, text=f"{kind}:{action}:{record_id}")
            )
            if message is not None
            else None
        )
        try:
            try:
                acknowledgement, reply = await asyncio.to_thread(
                    _execute_decision_action,
                    self,
                    kind=kind,
                    action=action,
                    record_id=record_id,
                    chat_id=chat_id_text,
                )
            except Exception:
                logger.exception(
                    "Failed to handle Telegram callback query (chat_id=%s, data=%s)",
                    chat_id_text,
                    getattr(callback_query, "data", None),
                )
                await _answer_callback_query_safely(callback_query, "Action failed", show_alert=False)
                raise
            await _answer_callback_query_safely(callback_query, acknowledgement)
            if message is None:
                return
            formatted_reply, reply_kwargs = format_telegram_text(reply)
            reply_kwargs = {**reply_kwargs, "reply_markup": None}
            try:
                await retry_messaging_delivery_operation(
                    lambda: message.edit_text(formatted_reply, **reply_kwargs)
                )
            except Exception as exc:
                if not _is_telegram_message_not_modified_error(exc):
                    raise
                logger.debug("Telegram callback acknowledgement already reflected in message.")
            should_resume_approval = kind == "approval" and action in {"approve", "allow_session", "allow_once", "always_allow"}
            if should_resume_approval:
                approval = self.runtime.store.get_approval_request(record_id)
                resume_delivery_channel = _resume_delivery_channel_for_approval(self.runtime, approval)
                resumed_principal_id = (
                    approval.requested_by
                    if approval is not None
                    else _principal_id_for_telegram_message(message, self.settings)
                )
                try:
                    resume_kwargs = {}
                    if self.model_client is not None:
                        resume_kwargs["model_client"] = self.model_client
                    if self.agent_orchestrator is not None:
                        resume_kwargs["agent_orchestrator"] = self.agent_orchestrator
                    resumed_reply = await asyncio.to_thread(
                        resume_approved_telegram_request,
                        self.runtime,
                        approval_id=record_id,
                        chat_id=chat_id_text,
                        **resume_kwargs,
                    )
                except Exception:
                    logger.exception(
                        "Failed to resume approved request (approval_id=%s, chat_id=%s)",
                        record_id,
                        chat_id_text,
                    )
                    resumed_reply = None
                if resumed_reply is not None:
                    # If the resume itself suspended for another approval, show only
                    # the next decision card. Sending the raw "Tool approval
                    # requested: <id>" marker first creates duplicate Telegram
                    # messages for the same approval.
                    if isinstance(resumed_reply, str) and resumed_reply.startswith("Tool approval requested:"):
                        new_approval_id = resumed_reply.removeprefix("Tool approval requested:").strip()
                        if new_approval_id:
                            new_approval = self.runtime.store.get_approval_request(new_approval_id)
                            if new_approval is not None:
                                new_card = DecisionCard(
                                    text=_approval_card_text(new_approval),
                                    reply_markup=_build_approval_markup(approval=new_approval),
                                )
                                await _deliver_reply(
                                    message,
                                    new_card.text,
                                    decision_card=new_card,
                                    principal_id=new_approval.requested_by,
                                )
                            else:
                                await _send_callback_follow_up(
                                    message,
                                    "Approval required. Open /approvals to continue.",
                                    principal_id=resumed_principal_id,
                                )
                        else:
                            await _send_callback_follow_up(
                                message,
                                "Approval required. Open /approvals to continue.",
                                principal_id=resumed_principal_id,
                            )
                    elif resume_delivery_channel == "telegram":
                        await _send_callback_follow_up(message, resumed_reply, principal_id=resumed_principal_id)
                    else:
                        logger.info(
                            "Skipped Telegram resumed reply for approval_id=%s because origin channel is %s",
                            record_id,
                            resume_delivery_channel,
                        )
                else:
                    # Resume produced no output — notify the user so they know to retry.
                    logger.warning(
                        "Approval resume produced no output (approval_id=%s, chat_id=%s). "
                        "Sending fallback prompt.",
                        record_id,
                        chat_id_text,
                    )
                    if resume_delivery_channel == "telegram":
                        await _send_callback_follow_up(
                            message,
                            _approval_resume_fallback_reply(approval, reply),
                            principal_id=resumed_principal_id,
                        )
        finally:
            await _stop_typing_keepalive(typing_keepalive_task)

    def build_application(self):
        if Application is None:
            raise TelegramDependencyUnavailableError(
                "python-telegram-bot is not installed."
            )
        builder = Application.builder().token(self.bot_token).concurrent_updates(True)
        builder = configure_telegram_application_builder(builder)
        return builder.build()

    async def _ptb_error_handler(self, update, context) -> None:
        """PTB application-level error handler — logs unhandled exceptions without crashing the bot."""
        logger.error(
            "Unhandled PTB error (update=%r): %s",
            update,
            context.error,
            exc_info=context.error,
        )

    def register_handlers(self, application) -> None:
        if MessageHandler is None or CallbackQueryHandler is None or filters is None:
            raise TelegramDependencyUnavailableError(
                "python-telegram-bot is not installed."
            )
        application.add_handler(CallbackQueryHandler(self.on_callback_query))
        application.add_handler(MessageHandler(filters.ALL, self.on_message))
        if hasattr(application, "add_error_handler"):
            application.add_error_handler(self._ptb_error_handler)

    def _build_health_monitor(self, application):
        """Create a HealthMonitor with probes for the model API and Telegram bot."""
        try:
            from nullion.health_monitor import HealthMonitor
            from nullion.health_probes import make_model_api_probe, make_telegram_probe
        except ImportError:
            logger.debug("Health monitor dependencies not available — skipping")
            return None
        monitor = HealthMonitor(runtime=self.runtime, settings=self.settings)
        if self.model_client is not None:
            monitor.register_probe(make_model_api_probe(self.model_client))
        monitor.register_probe(make_telegram_probe(application))
        return monitor

    def run_polling(self, *, application=None):
        app = application if application is not None else self.build_application()
        if hasattr(app, "add_handler"):
            self.register_handlers(app)

        # ── Phase 5: wire deliver_fn so the result aggregator can push progress
        # back through Telegram.  We capture bot/token here (before run_polling
        # starts the event loop) and build an async-safe deliver_fn closure.
        if self.agent_orchestrator is not None and hasattr(self.agent_orchestrator, "set_deliver_fn"):
            _service_ref = self
            _status_messages: dict[tuple[str, str], int] = {}
            _status_texts: dict[tuple[str, str], str] = {}
            _status_locks: dict[tuple[str, str], _asyncio.Lock] = {}
            _status_typing_tasks: dict[tuple[str, str], _asyncio.Task[None]] = {}
            _task_card_store = PlatformTaskCardStore(platform_activity_capabilities("telegram"))

            def _telegram_deliver_fn(conversation_id: str, text: str, **kwargs) -> bool:
                """Route aggregator output back to the originating Telegram chat."""
                # conversation_id is "telegram:<chat_id>"
                chat_id = conversation_id.removeprefix("telegram:")
                if not chat_id or chat_id == conversation_id:
                    # Fallback: deliver to operator chat
                    chat_id = _service_ref.operator_chat_id or ""
                if not chat_id:
                    return False
                bot_token = _service_ref.bot_token
                if not bot_token:
                    return False
                if kwargs.get("is_status"):
                    group_id = str(kwargs.get("group_id") or "")
                    status_kind = str(kwargs.get("status_kind") or "task_summary")
                    include_activity = activity_trace_enabled_for_chat(_service_ref.runtime, chat_id=chat_id)
                    if (
                        not group_id
                        or not _telegram_allows_status_streaming(_service_ref.runtime, chat_id=chat_id)
                        or not should_deliver_task_status(
                            status_kind=status_kind,
                            planner_feed_enabled=task_planner_feed_mode() != "off",
                            include_activity=include_activity,
                        )
                    ):
                        return False
                    rendered_status = _task_card_store.update(
                        target_id=chat_id,
                        group_id=group_id,
                        status_kind=status_kind,
                        text=text,
                        activity_id=str(kwargs.get("activity_id") or ""),
                        activity_label=str(kwargs.get("activity_label") or ""),
                        include_activity=include_activity,
                    )
                    if not rendered_status:
                        return True
                    async def _deliver_status(use_loop_bound_state: bool) -> None:
                        await _send_or_edit_telegram_task_status_message(
                            bot_token,
                            _status_messages,
                            chat_id=chat_id,
                            group_id=group_id,
                            text=rendered_status,
                            runtime=_service_ref.runtime,
                            bot_token=bot_token,
                            status_texts=_status_texts,
                            status_locks=_status_locks if use_loop_bound_state else None,
                            typing_tasks=_status_typing_tasks if use_loop_bound_state else None,
                        )

                    return _schedule_or_run_telegram_status_delivery(_deliver_status)
                # Final results and artifacts are awaited so delivery failures
                # surface to the result aggregator instead of disappearing.
                principal_id = principal_id_for_messaging_identity("telegram", chat_id, _service_ref.settings)
                outbound_text = f"MEDIA:{text}" if kwargs.get("is_artifact") else text
                delivery = _send_operator_telegram_message(
                    bot_token,
                    chat_id,
                    outbound_text,
                    principal_id=principal_id,
                )
                return _schedule_or_run_telegram_delivery(delivery)

            self.agent_orchestrator.set_deliver_fn(_telegram_deliver_fn)
            if hasattr(self.agent_orchestrator, "set_checkpoint_fn"):
                self.agent_orchestrator.set_checkpoint_fn(self.runtime.checkpoint)
            logger.debug("Phase 5: deliver_fn wired to orchestrator")

        # Wire health monitor as PTB post_init / shutdown lifecycle hooks
        monitor = self._build_health_monitor(app)

        async def _post_init(app) -> None:
            # Register command hints so / shows the menu in the Telegram input bar
            try:
                registered = await register_telegram_bot_commands(app.bot)
                if registered:
                    logger.info("Registered Telegram bot command menu (%d commands)", len(TELEGRAM_BOT_COMMANDS))
            except Exception:
                logger.warning("set_my_commands failed; Telegram slash menu may be stale", exc_info=True)
            if monitor is not None:
                await monitor.start()

            async def _watch_live_config() -> None:
                try:
                    poll_seconds = max(float(os.environ.get("NULLION_TELEGRAM_CONFIG_POLL_SECONDS", "2") or "2"), 0.25)
                except ValueError:
                    poll_seconds = 2.0
                app_bot = getattr(app, "bot", None)
                app_bot_token = str(getattr(app_bot, "token", "") or "").strip()
                while True:
                    await asyncio.sleep(poll_seconds)
                    try:
                        self.refresh_live_configuration()
                    except Exception:
                        logger.debug("Could not refresh Telegram live config from polling watcher", exc_info=True)
                        continue
                    service_bot_token = str(self.bot_token or "").strip()
                    if app_bot_token and service_bot_token and service_bot_token != app_bot_token:
                        logger.info("Telegram bot token changed; restarting polling application to bind the new token")
                        stop_running = getattr(app, "stop_running", None)
                        if callable(stop_running):
                            stop_running()
                        else:
                            stop = getattr(app, "stop", None)
                            if callable(stop):
                                result = stop()
                                if inspect.isawaitable(result):
                                    await result
                        return

            app._nullion_live_config_task = asyncio.create_task(_watch_live_config())
            from nullion.reminder_delivery import run_reminder_delivery_loop

            async def _send(chat_id: str, text: str) -> bool:
                if ":" in str(chat_id):
                    return False
                await retry_messaging_delivery_operation(
                    lambda: app.bot.send_message(chat_id, text, disable_web_page_preview=True)
                )
                return True

            app._nullion_reminder_task = asyncio.create_task(
                run_reminder_delivery_loop(self.runtime, send=_send, settings=self.settings)
            )

        async def _post_shutdown(app) -> None:
            config_task = getattr(app, "_nullion_live_config_task", None)
            if config_task is not None:
                config_task.cancel()
                try:
                    await config_task
                except asyncio.CancelledError:
                    pass
            reminder_task = getattr(app, "_nullion_reminder_task", None)
            if reminder_task is not None:
                reminder_task.cancel()
                try:
                    await reminder_task
                except asyncio.CancelledError:
                    pass
            if monitor is not None:
                await monitor.stop()

        if hasattr(app, "post_init"):
            app.post_init = _post_init
        if hasattr(app, "post_shutdown"):
            app.post_shutdown = _post_shutdown

        app.run_polling(
            drop_pending_updates=True,
            close_loop=False,
            stop_signals=None,
        )
        return app



def _openai_platform_fallback_settings(settings: NullionSettings) -> NullionSettings | None:
    model_cfg = getattr(settings, "model", None)
    provider = str(getattr(model_cfg, "provider", "") or "").strip().lower()
    api_key = str(getattr(model_cfg, "openai_api_key", "") or "").strip()
    if provider != "codex" or not api_key.startswith("sk-"):
        return None
    return replace(settings, model=replace(model_cfg, provider="openai"))


def _build_chat_model_client_with_fallback(settings: NullionSettings, *, surface: str) -> object | None:
    try:
        return build_model_client_from_settings(settings)
    except ModelClientConfigurationError as exc:
        fallback_settings = _openai_platform_fallback_settings(settings)
        if fallback_settings is not None:
            try:
                client = build_model_client_from_settings(fallback_settings)
                logger.warning(
                    "Provider warning: %s — falling back to platform OpenAI credentials for %s chat.",
                    exc,
                    surface,
                )
                return client
            except ModelClientConfigurationError as fallback_exc:
                logger.warning(
                    "Provider warning: %s — %s chat fallback also failed: %s",
                    exc,
                    surface,
                    fallback_exc,
                )
                return None
        logger.warning(
            "Provider warning: %s — %s chat will be unavailable until credentials are refreshed.",
            exc,
            surface,
        )
        return None


def build_telegram_operator_service(
    runtime: PersistentRuntime,
    *,
    settings: NullionSettings,
) -> ChatOperatorService:
    bot_token = settings.telegram.bot_token
    if not bot_token:
        raise MissingTelegramBotTokenError("NULLION_TELEGRAM_BOT_TOKEN is required.")
    if not _is_valid_telegram_bot_token(bot_token):
        raise MissingTelegramBotTokenError("NULLION_TELEGRAM_BOT_TOKEN format is invalid.")

    operator_chat_id = settings.telegram.operator_chat_id
    _operator_chat_id_value = operator_chat_id.strip() if isinstance(operator_chat_id, str) else None
    # operator_chat_id is now optional at startup — the bot will self-discover it
    # from the first incoming message (first-run wizard).  An explicit value is
    # still accepted and takes priority.
    operator_chat_id = _operator_chat_id_value or None

    model_client = None
    if settings.telegram.chat_enabled:
        model_client = _build_chat_model_client_with_fallback(settings, surface="Telegram")

    agent_orchestrator = None
    if model_client is not None:
        agent_orchestrator = AgentOrchestrator(model_client=model_client)

    return ChatOperatorService(
        runtime=runtime,
        bot_token=bot_token,
        operator_chat_id=operator_chat_id,  # may be None — first-run wizard will discover it
        settings=settings,
        model_client=model_client,
        agent_orchestrator=agent_orchestrator,
    )


def build_messaging_operator_service(
    runtime: PersistentRuntime,
    *,
    settings: NullionSettings,
) -> ChatOperatorService:
    """Build the shared operator service for non-Telegram messaging adapters."""
    model_client = None
    if settings.telegram.chat_enabled:
        model_client = _build_chat_model_client_with_fallback(settings, surface="messaging")

    agent_orchestrator = None
    if model_client is not None:
        agent_orchestrator = AgentOrchestrator(model_client=model_client)

    return ChatOperatorService(
        runtime=runtime,
        bot_token="",
        operator_chat_id=None,
        settings=settings,
        model_client=model_client,
        agent_orchestrator=agent_orchestrator,
    )


__all__ = [
    "MissingTelegramBotTokenError",
    "MissingTelegramOperatorChatIDError",
    "TelegramDependencyUnavailableError",
    "ChatOperatorService",
    "build_messaging_operator_service",
    "build_telegram_operator_service",
]
