"""Shared helpers for Slack and Discord messaging adapters."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import html
from html.parser import HTMLParser
import logging
import os
from pathlib import Path
import re
from urllib.parse import unquote
from uuid import uuid4
import zipfile

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_root_for_principal,
    is_deliverable_explicit_media_path,
    is_safe_artifact_path,
    media_candidate_paths_from_text,
    nullion_data_home,
    split_media_reply_attachments,
)
from nullion.approval_context import approval_trigger_flow_label
from nullion.approval_markers import split_tool_approval_marker, strip_tool_approval_marker
from nullion.attachment_format_graph import ATTACHMENT_TOKEN_EXTENSIONS, VALID_ATTACHMENT_EXTENSIONS, plan_attachment_format
from nullion.builder import format_builder_proposal_notification
from nullion.chat_attachments import guess_media_type
from nullion.chat_text import make_markdown_tables_chat_readable
from nullion.config import NullionSettings
from nullion.remediation import remediation_buttons_for_recommendation_code
from nullion.response_sanitizer import _sanitize_local_paths, sanitize_user_visible_reply
from nullion.users import is_authorized_messaging_identity, resolve_messaging_user
from nullion.workspace_storage import workspace_storage_roots_for_principal
from nullion.task_frames import TaskFrameStatus


logger = logging.getLogger(__name__)
WORKING_ACK_TEXT = "⌛ On it! Feel free to send other tasks — I can handle multiple at once."


def should_emit_separate_working_ack(
    event: dict[str, object],
    *,
    visible_activity_stream: bool = False,
) -> bool:
    """Return whether a separate working acknowledgement adds useful signal."""
    if visible_activity_stream:
        return False
    event_id = str(event.get("id") or "")
    event_tool_name = str(event.get("tool_name") or "")
    if event_tool_name == "run_cron":
        return False
    return event_id == "foreground-working-ack"


class MessagingAdapterConfigurationError(ValueError):
    """Raised when a messaging adapter is enabled without required settings."""


class MessagingAdapterDependencyError(RuntimeError):
    """Raised when an adapter dependency is not installed."""


@dataclass(frozen=True, slots=True)
class MessagingIngress:
    channel: str
    user_id: str
    text: str
    attachments: tuple[dict[str, str], ...] = ()
    request_id: str | None = None
    message_id: str | None = None
    delivery_target_id: str | None = None

    @property
    def operator_chat_id(self) -> str:
        return f"{self.channel}:{self.user_id}"

    @property
    def reminder_chat_id(self) -> str:
        return f"{self.channel}:{self.delivery_target_id or self.user_id}"


@dataclass(frozen=True, slots=True)
class PlatformDelivery:
    text: str | None
    attachments: tuple[Path, ...]
    media_directive_count: int = 0
    unavailable_attachment_count: int = 0
    pending_attachment_count: int = 0

    @property
    def requires_attachment_delivery(self) -> bool:
        return self.media_directive_count > 0 or self.pending_attachment_count > 0

    @property
    def has_deliverable_attachments(self) -> bool:
        return bool(self.attachments)


@dataclass(frozen=True, slots=True)
class PlatformDeliveryReceipt:
    receipt_id: str
    channel: str
    target_id: str | None
    status: str
    text_delivered: bool
    attachment_count: int
    attachment_required: bool
    unavailable_attachment_count: int = 0
    pending_attachment_count: int = 0
    request_id: str | None = None
    message_id: str | None = None
    error: str | None = None
    created_at: datetime | None = None
    delivered_text: str | None = None
    attachment_names: tuple[str, ...] = ()

    def to_record(self) -> dict[str, object]:
        created_at = self.created_at or datetime.now(UTC)
        return {
            "receipt_id": self.receipt_id,
            "channel": self.channel,
            "target_id": self.target_id,
            "status": self.status,
            "text_delivered": self.text_delivered,
            "attachment_count": self.attachment_count,
            "attachment_required": self.attachment_required,
            "unavailable_attachment_count": self.unavailable_attachment_count,
            "pending_attachment_count": self.pending_attachment_count,
            "request_id": self.request_id,
            "message_id": self.message_id,
            "error": self.error,
            "created_at": created_at.isoformat(),
            "delivered_text": self.delivered_text,
            "attachment_names": list(self.attachment_names),
        }


@dataclass(frozen=True, slots=True)
class PlatformDeliveryResult:
    delivery: PlatformDelivery
    receipt: PlatformDeliveryReceipt
    text_sent: bool = False
    attachments_sent: bool = False
    fallback_text_sent: bool = False

    @property
    def ok(self) -> bool:
        return delivery_receipt_transport_succeeded(self.receipt)


PlatformTextSender = Callable[[str], Awaitable[bool | None]]
PlatformAttachmentSender = Callable[[str | None, tuple[Path, ...]], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class DeliveryContract:
    """Platform-neutral contract for the final delivery boundary.

    `requires_attachment_delivery` is authoritative. It must come from graph
    state, task contracts, generated artifact directives, or tool outputs by
    the time we send. User text classifiers are deliberately kept out of this
    boundary so delivery cannot depend on brittle verb matching.
    """

    requires_text_delivery: bool = True
    requires_attachment_delivery: bool = False
    allow_attachment_delivery: bool = False
    allow_plain_path_attachment_delivery: bool = False
    required_attachment_extensions: tuple[str, ...] = ()
    source: str = "message"

    @classmethod
    def message_only(cls) -> "DeliveryContract":
        return cls()

    @classmethod
    def attachment_required(
        cls,
        *,
        source: str,
        allow_plain_paths: bool = False,
        required_attachment_extensions: tuple[str, ...] = (),
    ) -> "DeliveryContract":
        return cls(
            requires_attachment_delivery=True,
            allow_attachment_delivery=True,
            allow_plain_path_attachment_delivery=allow_plain_paths,
            required_attachment_extensions=required_attachment_extensions,
            source=source,
        )


_ATTACHMENT_UNAVAILABLE_REPLY = (
    "I couldn't attach the requested file. The task is still open."
)
_ATTACHMENT_UPLOAD_FAILED_REPLY = (
    "I couldn't upload the requested attachment to this platform. "
    "I won't mark it delivered."
)
_PRIMARY_RENDERED_ATTACHMENT_SUFFIXES = frozenset(
    {".pdf", ".docx", ".pptx", ".xlsx", ".html", ".htm"}
)
_TEXT_SIDECAR_ATTACHMENT_SUFFIXES = frozenset({".txt", ".md"})
_INTERNAL_STATE_ATTACHMENT_SUFFIXES = frozenset({".json", ".jsonl", ".db", ".sqlite", ".sqlite3"})
_DATA_SIDECAR_ATTACHMENT_SUFFIXES = frozenset({".csv", ".tsv"}) | _INTERNAL_STATE_ATTACHMENT_SUFFIXES
_IMAGE_ATTACHMENT_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"})
_SUPPORT_IMAGE_ATTACHMENT_SUFFIXES = _IMAGE_ATTACHMENT_SUFFIXES | frozenset({".svg"})
_MESSAGING_ATTACHMENT_UPLOAD_ATTEMPTS = 3
_MESSAGING_ATTACHMENT_UPLOAD_RETRY_DELAY_SECONDS = 0.5
_MESSAGING_DELIVERY_RETRY_AFTER_MAX_SECONDS = 600.0
_DEFAULT_MAX_AUTOMATIC_ATTACHMENT_BATCH = 12
_DEFAULT_TELEGRAM_ATTACHMENT_UPLOAD_LIMIT_BYTES = 50 * 1024 * 1024
_EXTERNAL_INLINE_TAG_PATTERN = re.compile(
    r"</?(?:b|strong|i|em|u|s|strike|code|span)(?:\s+[^>]*)?>",
    re.IGNORECASE,
)


def messaging_upload_root() -> Path:
    return nullion_data_home() / "uploads"


def messaging_media_scratch_root() -> Path:
    return nullion_data_home() / "tmp" / "media"


def ensure_messaging_storage_roots() -> tuple[Path, Path]:
    upload_root = messaging_upload_root()
    media_scratch_root = messaging_media_scratch_root()
    upload_root.mkdir(parents=True, exist_ok=True)
    media_scratch_root.mkdir(parents=True, exist_ok=True)
    return upload_root, media_scratch_root


def messaging_delivery_receipts_path() -> Path:
    return nullion_data_home() / "delivery_receipts.jsonl"


def delivery_receipt_status(
    delivery: PlatformDelivery,
    *,
    transport_ok: bool,
) -> str:
    if not transport_ok:
        return "failed"
    unavailable = int(getattr(delivery, "unavailable_attachment_count", 0) or 0)
    required = bool(getattr(delivery, "requires_attachment_delivery", False))
    attachments = tuple(getattr(delivery, "attachments", ()) or ())
    pending = int(getattr(delivery, "pending_attachment_count", 0) or 0)
    if pending and not attachments:
        return "pending_confirmation"
    if required and not attachments:
        return "failed"
    if unavailable:
        return "partial"
    return "succeeded"


def delivery_receipt_transport_succeeded(receipt: PlatformDeliveryReceipt) -> bool:
    """Return whether the platform boundary accepted the message we sent.

    `pending_confirmation` means Nullion intentionally sent a confirmation or
    narrowing prompt instead of flooding the chat with attachments. That is a
    successful platform delivery even though the original attachment request is
    still pending user choice.
    """
    return receipt.status in {"succeeded", "partial", "pending_confirmation"}


def build_platform_delivery_receipt(
    *,
    channel: str,
    target_id: str | None,
    delivery: PlatformDelivery,
    transport_ok: bool,
    request_id: str | None = None,
    message_id: str | None = None,
    error: str | None = None,
) -> PlatformDeliveryReceipt:
    delivered_text = str(getattr(delivery, "text", None) or "").strip()
    delivered_attachments = tuple(getattr(delivery, "attachments", ()) or ())
    return PlatformDeliveryReceipt(
        receipt_id=f"dr-{uuid4().hex}",
        channel=str(channel or "unknown"),
        target_id=None if target_id is None else str(target_id),
        status=delivery_receipt_status(delivery, transport_ok=transport_ok),
        text_delivered=bool(delivered_text) and transport_ok,
        attachment_count=len(delivered_attachments) if transport_ok else 0,
        attachment_required=bool(getattr(delivery, "requires_attachment_delivery", False)),
        unavailable_attachment_count=int(getattr(delivery, "unavailable_attachment_count", 0) or 0),
        pending_attachment_count=int(getattr(delivery, "pending_attachment_count", 0) or 0),
        request_id=request_id,
        message_id=message_id,
        error=error,
        created_at=datetime.now(UTC),
        delivered_text=delivered_text if transport_ok and delivered_text else None,
        attachment_names=(
            tuple(path.name for path in delivered_attachments if path.name)
            if transport_ok
            else ()
        ),
    )


def record_platform_delivery_receipt(
    receipt: PlatformDeliveryReceipt,
    *,
    path: Path | None = None,
) -> bool:
    """Persist a best-effort platform delivery receipt without affecting delivery."""
    try:
        receipt_path = path or messaging_delivery_receipts_path()
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        with receipt_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(receipt.to_record(), sort_keys=True) + "\n")
        return True
    except Exception:
        return False


def list_platform_delivery_receipts(
    *,
    limit: int = 20,
    status: str | None = None,
    path: Path | None = None,
) -> list[dict[str, object]]:
    receipt_path = path or messaging_delivery_receipts_path()
    try:
        lines = receipt_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    except Exception:
        return []
    records: list[dict[str, object]] = []
    normalized_status = str(status or "").strip().lower()
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        if normalized_status and str(record.get("status") or "").strip().lower() != normalized_status:
            continue
        records.append(record)
        if len(records) >= max(1, limit):
            break
    return records


def messaging_file_allowed_roots(*extra_roots: object) -> tuple[Path, ...]:
    upload_root, media_scratch_root = ensure_messaging_storage_roots()
    roots: list[Path] = [upload_root, media_scratch_root]
    for root in extra_roots:
        if root is None:
            continue
        if isinstance(root, (list, tuple, set, frozenset)):
            roots.extend(Path(item).expanduser() for item in root if str(item).strip())
            continue
        if str(root).strip():
            roots.append(Path(root).expanduser())
    return tuple(dict.fromkeys(root.resolve() for root in roots))


def require_authorized_ingress(ingress: MessagingIngress, settings: NullionSettings) -> bool:
    return is_authorized_messaging_identity(ingress.channel, ingress.user_id, settings)


def messaging_chat_enabled(channel: str, settings: NullionSettings | None) -> bool:
    normalized = str(channel or "").strip().lower()
    if settings is None:
        return False
    if normalized == "telegram":
        return bool(getattr(getattr(settings, "telegram", None), "chat_enabled", False))
    if normalized == "slack":
        return bool(getattr(getattr(settings, "slack", None), "enabled", False))
    if normalized == "discord":
        return bool(getattr(getattr(settings, "discord", None), "enabled", False))
    if normalized == "web":
        return True
    return False


def _history_channel_label(ingress: MessagingIngress) -> str:
    labels = {
        "telegram": "Telegram",
        "slack": "Slack",
        "discord": "Discord",
        "web": "Web",
    }
    prefix = labels.get(ingress.channel, ingress.channel.title() or ingress.channel)
    return f"{prefix} · {ingress.user_id}" if ingress.user_id else prefix


def save_messaging_chat_history(ingress: MessagingIngress, reply: str | None) -> None:
    """Best-effort persistence for the admin all-channel history viewer."""
    try:
        from nullion.chat_store import get_chat_store

        conversation_id = ingress.operator_chat_id
        if not conversation_id:
            return
        store = get_chat_store()
        channel_label = _history_channel_label(ingress)
        if ingress.text:
            store.save_message(
                conversation_id,
                "user",
                ingress.text,
                channel=conversation_id,
                channel_label=channel_label,
            )
        if reply:
            store.save_message(
                conversation_id,
                "bot",
                str(reply),
                channel=conversation_id,
                channel_label=channel_label,
            )
    except Exception:
        # Chat delivery should never fail because local history persistence did.
        return


@dataclass(frozen=True, slots=True)
class _MessagingDecisionSnapshot:
    pending_approval_ids: frozenset[str]
    pending_builder_proposal_ids: frozenset[str]
    pending_doctor_action_ids: frozenset[str]
    reminder_ids: frozenset[str]


def _pending_doctor_action_ids(runtime) -> frozenset[str]:
    store = getattr(runtime, "store", None)
    list_actions = getattr(store, "list_doctor_actions", None)
    if list_actions is None:
        return frozenset()
    try:
        return frozenset(
            str(action.get("action_id") or "")
            for action in list_actions()
            if str(action.get("status") or "").lower() == "pending" and action.get("action_id")
        )
    except Exception:
        return frozenset()


def _pending_approval_ids(runtime) -> frozenset[str]:
    store = getattr(runtime, "store", None)
    list_approvals = getattr(store, "list_approval_requests", None)
    if list_approvals is None:
        return frozenset()
    try:
        return frozenset(
            str(getattr(approval, "approval_id", "") or "")
            for approval in list_approvals()
            if str(getattr(getattr(approval, "status", None), "value", getattr(approval, "status", ""))).lower() == "pending"
            and getattr(approval, "approval_id", None)
        )
    except Exception:
        return frozenset()


def _pending_builder_proposal_ids(runtime) -> frozenset[str]:
    list_proposals = getattr(runtime, "list_pending_builder_proposals", None)
    if list_proposals is None:
        return frozenset()
    try:
        return frozenset(
            str(getattr(proposal, "proposal_id", "") or "")
            for proposal in list_proposals()
            if getattr(proposal, "proposal_id", None)
        )
    except Exception:
        return frozenset()


def _reminder_ids(runtime) -> frozenset[str]:
    store = getattr(runtime, "store", None)
    list_reminders = getattr(store, "list_reminders", None)
    if list_reminders is None:
        return frozenset()
    try:
        return frozenset(
            str(getattr(reminder, "task_id", "") or "")
            for reminder in list_reminders()
            if getattr(reminder, "task_id", None)
        )
    except Exception:
        return frozenset()


def _capture_messaging_decision_snapshot(service) -> _MessagingDecisionSnapshot:
    runtime = getattr(service, "runtime", None)
    return _MessagingDecisionSnapshot(
        pending_approval_ids=_pending_approval_ids(runtime),
        pending_builder_proposal_ids=_pending_builder_proposal_ids(runtime),
        pending_doctor_action_ids=_pending_doctor_action_ids(runtime),
        reminder_ids=_reminder_ids(runtime),
    )


def _new_approval_text_fallbacks(runtime, before_ids: frozenset[str]) -> tuple[str, ...]:
    store = getattr(runtime, "store", None)
    list_approvals = getattr(store, "list_approval_requests", None)
    if list_approvals is None:
        return ()
    try:
        approvals = [
            approval
            for approval in list_approvals()
            if str(getattr(getattr(approval, "status", None), "value", getattr(approval, "status", ""))).lower() == "pending"
            and str(getattr(approval, "approval_id", "") or "") not in before_ids
        ]
    except Exception:
        return ()
    fallbacks: list[str] = []
    for approval in approvals:
        approval_id = str(getattr(approval, "approval_id", "") or "")
        if not approval_id:
            continue
        requested_by = str(getattr(approval, "requested_by", "") or "unknown")
        action = str(getattr(approval, "action", "") or "action")
        resource = str(getattr(approval, "resource", "") or "resource")
        trigger_label = approval_trigger_flow_label(approval)
        trigger_lines = [f"Triggered by: {trigger_label}"] if trigger_label else []
        fallbacks.append(
            "\n".join([
                "Approval pending",
                f"ID: {approval_id}",
                f"Request: {requested_by} wants {action} on {resource}",
                *trigger_lines,
                "",
                f"Inspect: /approval {approval_id}",
                f"Approve: /approve {approval_id}",
                f"Deny: /deny {approval_id}",
            ])
        )
    return tuple(fallbacks)


def _approval_status_value(approval) -> str:
    status = getattr(approval, "status", None)
    return str(getattr(status, "value", status)).lower()


def _approval_text_fallback_for_marker(runtime, approval_id: str | None) -> tuple[str, ...]:
    store = getattr(runtime, "store", None)
    if store is None:
        return ()
    approvals = []
    if approval_id:
        get_approval = getattr(store, "get_approval_request", None)
        if get_approval is None:
            return ()
        approval = get_approval(approval_id)
        if approval is not None:
            approvals.append(approval)
    else:
        list_approvals = getattr(store, "list_approval_requests", None)
        if list_approvals is None:
            return ()
        approvals = [
            approval
            for approval in list_approvals()
            if _approval_status_value(approval) == "pending"
        ]
        if len(approvals) != 1:
            return ()
    fallbacks = []
    for approval in approvals:
        if _approval_status_value(approval) != "pending":
            continue
        current_id = str(getattr(approval, "approval_id", "") or "")
        if not current_id:
            continue
        requested_by = str(getattr(approval, "requested_by", "") or "unknown")
        action = str(getattr(approval, "action", "") or "action")
        resource = str(getattr(approval, "resource", "") or "resource")
        trigger_label = approval_trigger_flow_label(approval)
        trigger_lines = [f"Triggered by: {trigger_label}"] if trigger_label else []
        fallbacks.append(
            "\n".join([
                "Approval pending",
                f"ID: {current_id}",
                f"Request: {requested_by} wants {action} on {resource}",
                *trigger_lines,
                "",
                f"Inspect: /approval {current_id}",
                f"Approve: /approve {current_id}",
                f"Deny: /deny {current_id}",
            ])
        )
    return tuple(fallbacks)


def _new_builder_proposal_text_fallbacks(runtime, before_ids: frozenset[str]) -> tuple[str, ...]:
    list_proposals = getattr(runtime, "list_pending_builder_proposals", None)
    if list_proposals is None:
        return ()
    try:
        proposals = [
            proposal
            for proposal in list_proposals()
            if str(getattr(proposal, "proposal_id", "") or "") not in before_ids
        ]
    except Exception:
        return ()
    fallbacks: list[str] = []
    for record in proposals:
        fallbacks.append(format_builder_proposal_notification(record))
    return tuple(fallbacks)


def _new_doctor_action_text_fallbacks(runtime, before_ids: frozenset[str]) -> tuple[str, ...]:
    store = getattr(runtime, "store", None)
    list_actions = getattr(store, "list_doctor_actions", None)
    if list_actions is None:
        return ()
    try:
        pending_actions = [
            action
            for action in list_actions()
            if str(action.get("status") or "").lower() == "pending"
            and str(action.get("action_id") or "") not in before_ids
        ]
    except Exception:
        return ()
    fallbacks: list[str] = []
    for action in pending_actions:
        action_id = str(action.get("action_id") or "")
        if not action_id:
            continue
        summary = str(action.get("summary") or "Doctor found a health issue that needs attention.")
        severity = str(action.get("severity") or "unknown")
        lines = [
            "Doctor action pending",
            f"Severity: {severity}",
            f"Summary: {summary}",
        ]
        remediation_actions = remediation_buttons_for_recommendation_code(str(action.get("recommendation_code") or ""))
        if remediation_actions:
            lines.extend(["", "Actions you can run from this chat:"])
            lines.extend(
                f"{index}. {label}: /doctor run latest {command}"
                for index, (label, command) in enumerate(remediation_actions, start=1)
            )
        lines.extend([
            "",
            "Inspect: /doctor latest",
            "Hide: /doctor dismiss latest",
        ])
        fallbacks.append("\n".join(lines))
    return tuple(fallbacks)


def _new_reminder_text_fallbacks(runtime, before_ids: frozenset[str]) -> tuple[str, ...]:
    from nullion.reminders import format_reminder_due_at

    store = getattr(runtime, "store", None)
    list_reminders = getattr(store, "list_reminders", None)
    if list_reminders is None:
        return ()
    try:
        reminders = [
            reminder
            for reminder in list_reminders()
            if str(getattr(reminder, "task_id", "") or "") not in before_ids
        ]
    except Exception:
        return ()
    fallbacks: list[str] = []
    for reminder in reminders:
        task_id = str(getattr(reminder, "task_id", "") or "")
        text = str(getattr(reminder, "text", "") or "Reminder")
        due_at = getattr(reminder, "due_at", None)
        due_text = format_reminder_due_at(due_at)
        fallbacks.append(
            "\n".join([
                "Reminder set",
                f"ID: {task_id}",
                f"When: {due_text}",
                f"Text: {text}",
                "",
                f"Cancel: /reminder cancel {task_id}",
                "Edit: send a new reminder request, then cancel this one if needed.",
            ])
        )
    return tuple(fallbacks)


def _new_decision_text_fallbacks(service, before: _MessagingDecisionSnapshot) -> tuple[str, ...]:
    runtime = getattr(service, "runtime", None)
    return (
        *_new_approval_text_fallbacks(runtime, before.pending_approval_ids),
        *_new_builder_proposal_text_fallbacks(runtime, before.pending_builder_proposal_ids),
        *_new_doctor_action_text_fallbacks(runtime, before.pending_doctor_action_ids),
        *_new_reminder_text_fallbacks(runtime, before.reminder_ids),
    )


def _append_decision_fallbacks(reply: str | None, fallbacks: tuple[str, ...]) -> str | None:
    if not fallbacks:
        return reply
    text = str(reply or "").strip()
    suffix = "\n\n".join(fallbacks)
    return f"{text}\n\n{suffix}" if text else suffix


def handle_messaging_ingress_result(
    service,
    ingress: MessagingIngress,
    *,
    turn_dispatch_decision=None,
    text_delta_callback=None,
    activity_callback=None,
):
    from nullion.messaging_turn_graph import run_messaging_turn_graph

    return run_messaging_turn_graph(
        service,
        ingress,
        turn_dispatch_decision=turn_dispatch_decision,
        text_delta_callback=text_delta_callback,
        activity_callback=activity_callback,
    )


def handle_messaging_ingress(service, ingress: MessagingIngress, *, turn_dispatch_decision=None) -> str | None:
    return handle_messaging_ingress_result(
        service,
        ingress,
        turn_dispatch_decision=turn_dispatch_decision,
    ).reply


def principal_id_for_messaging_identity(channel: str, user_id: object, settings: NullionSettings | None = None) -> str:
    user = resolve_messaging_user(channel, str(user_id or "").strip(), settings)
    return f"user:{user.user_id}" if user.role == "member" else "telegram_chat"


def _messaging_output_roots(*, principal_id: str | None) -> tuple[Path, ...]:
    roots = [artifact_root_for_principal(principal_id)]
    try:
        workspace_roots = workspace_storage_roots_for_principal(principal_id)
        roots.extend([workspace_roots.artifacts, workspace_roots.files, workspace_roots.media])
    except Exception:
        pass
    workspace_root = os.environ.get("NULLION_WORKSPACE_ROOT")
    if isinstance(workspace_root, str) and workspace_root.strip():
        workspace_path = Path(workspace_root).expanduser()
        roots.extend([workspace_path / "artifacts", workspace_path / "files", workspace_path / "media"])
    try:
        cwd = Path.cwd().resolve()
        roots.extend([cwd / "artifacts", cwd / "files", cwd / "media"])
    except Exception:
        pass
    data_root = nullion_data_home()
    roots.extend([data_root / "outputs", data_root / ".nullion-artifacts"])
    return tuple(dict.fromkeys(root.resolve() for root in roots))


def _is_deliverable_messaging_attachment(
    path: Path,
    *,
    principal_id: str | None,
    allow_explicit_media_path: bool = False,
) -> bool:
    for root in _messaging_output_roots(principal_id=principal_id):
        if artifact_descriptor_for_path(path, artifact_root=root) is not None:
            return True
    if _looks_like_workspace_artifact_path(path):
        return False
    if allow_explicit_media_path and is_deliverable_explicit_media_path(path):
        return True
    return is_safe_artifact_path(path) and is_deliverable_explicit_media_path(path)


def _looks_like_workspace_artifact_path(path: Path) -> bool:
    parts = path.expanduser().parts
    for index, part in enumerate(parts):
        if part != "workspaces":
            continue
        if index + 2 < len(parts) and parts[index + 2] in {"artifacts", "files", "media"}:
            return True
    return False


def _resolve_messaging_attachment_path(path: Path, *, principal_id: str | None) -> Path:
    if path.is_absolute():
        return path
    parts = path.parts
    if len(parts) == 1 and path.name == str(path) and path.suffix:
        candidate = _resolve_output_attachment_filename(path.name, principal_id=principal_id)
        if candidate is not None:
            return candidate
    if not parts or parts[0] not in {"artifacts", "files", "media"}:
        return path
    try:
        workspace_roots = workspace_storage_roots_for_principal(principal_id)
    except Exception:
        return path
    root_by_name = {
        "artifacts": workspace_roots.artifacts,
        "files": workspace_roots.files,
        "media": workspace_roots.media,
    }
    root = root_by_name.get(parts[0])
    if root is None:
        return path
    preferred_candidate = root.joinpath(*parts[1:]) if len(parts) > 1 else root
    if preferred_candidate.is_file():
        return preferred_candidate
    for output_root in _messaging_output_roots(principal_id=principal_id):
        if output_root.name != parts[0]:
            continue
        candidate = output_root.joinpath(*parts[1:]) if len(parts) > 1 else output_root
        if candidate.is_file():
            return candidate
    return preferred_candidate


def _resolve_output_attachment_filename(name: str, *, principal_id: str | None) -> Path | None:
    filename = Path(str(name or "").strip()).name
    if not filename or filename != str(name or "").strip() or not Path(filename).suffix:
        return None
    search_roots = list(_messaging_output_roots(principal_id=principal_id))
    try:
        from nullion.workspace_storage import workspace_storage_base

        for workspace_root in workspace_storage_base().glob("*"):
            if workspace_root.is_dir():
                search_roots.extend(
                    [
                        workspace_root / "artifacts",
                        workspace_root / "files",
                        workspace_root / "media",
                    ]
                )
    except Exception:
        pass
    candidates = []
    for root in tuple(dict.fromkeys(path.resolve() for path in search_roots)):
        candidate = root / filename
        if artifact_descriptor_for_path(candidate, artifact_root=root) is not None:
            candidates.append(candidate)
    unique = tuple(dict.fromkeys(path.resolve() for path in candidates))
    return unique[0] if len(unique) == 1 else None


def sanitize_external_inline_markup(text: str) -> str:
    """Strip lightweight HTML emphasis tags commonly copied from RSS/search results."""
    text = normalize_platform_code_fences(text)
    unescaped = html.unescape(text)
    if not _EXTERNAL_INLINE_TAG_PATTERN.search(unescaped):
        return text
    return _EXTERNAL_INLINE_TAG_PATTERN.sub("", unescaped)


_QUOTE_CODE_FENCE_LINE_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<fence>['\u2019]{3,})(?:[ \t]*(?P<language>[A-Za-z0-9_+.-]{1,32}))?[ \t]*$"
)


def normalize_platform_code_fences(text: str | None) -> str:
    """Canonicalize common code-fence variants before platform formatting."""

    value = "" if text is None else str(text)
    if "'''" not in value and "\u2019\u2019\u2019" not in value:
        return value
    lines = value.splitlines()
    normalized: list[str] = []
    active_quote_fence: str | None = None
    for line in lines:
        match = _QUOTE_CODE_FENCE_LINE_RE.match(line)
        if match is None:
            normalized.append(line)
            continue
        fence_char = str(match.group("fence") or "'")[0]
        language = str(match.group("language") or "").strip()
        indent = str(match.group("indent") or "")
        if active_quote_fence is None:
            active_quote_fence = fence_char
            normalized.append(f"{indent}```{language}" if language else f"{indent}```")
            continue
        if fence_char == active_quote_fence and not language:
            active_quote_fence = None
            normalized.append(f"{indent}```")
            continue
        normalized.append(line)
    suffix = "\n" if value.endswith("\n") else ""
    return "\n".join(normalized) + suffix


def split_reply_for_platform_delivery(
    reply: str | None,
    *,
    principal_id: str | None = None,
    allow_explicit_media_paths: bool = True,
) -> tuple[str | None, tuple[Path, ...]]:
    if reply is None:
        return None, ()
    return split_media_reply_attachments(
        str(reply),
        is_safe_attachment_path=lambda path: _is_deliverable_messaging_attachment(
            path,
            principal_id=principal_id,
            allow_explicit_media_path=allow_explicit_media_paths,
        ),
        resolve_attachment_path=lambda path: _resolve_messaging_attachment_path(path, principal_id=principal_id),
    )


_PLAIN_ABSOLUTE_PATH_RE = re.compile(r"(?<![\w./-])(/[^\s`'\"<>|]+)")
_PLAIN_RELATIVE_OUTPUT_PATH_RE = re.compile(r"(?<![\w./-])((?:artifacts|files|media)/[^\s`'\"<>|]+)")
_SANDBOX_ARTIFACT_LINK_RE = re.compile(r"\[([^\]\n]+)\]\(sandbox:([^)]+)\)|sandbox:([^\s)\]]+)")
_BUILDER_SKILL_MARKER_PREFIX = "::builder-skill::"
_VISIBLE_ATTACHMENT_FILENAME_RE = re.compile(
    r"(?<![\w./-])(?P<filename>[A-Za-z0-9][A-Za-z0-9._ -]{0,180}\.[A-Za-z0-9]{1,16})(?![\w/.-])"
)


def _resolve_sandbox_artifact_path(reference: str, *, principal_id: str | None) -> Path | None:
    raw = unquote(str(reference or "").strip().strip("`'\"<>"))
    if not raw:
        return None
    name = Path(raw.replace("\\", "/")).name
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        return None
    for root in _messaging_output_roots(principal_id=principal_id):
        path = root / name
        if _is_deliverable_messaging_attachment(path, principal_id=principal_id):
            return path
    return None


def _rewrite_sandbox_artifact_links_for_delivery(text: str, *, principal_id: str | None) -> str:
    def replace(match: re.Match[str]) -> str:
        label = str(match.group(1) or "").strip()
        reference = str(match.group(2) or match.group(3) or "").strip()
        path = _resolve_sandbox_artifact_path(reference, principal_id=principal_id)
        if path is None:
            return match.group(0)
        prefix = label or path.name
        return f"{prefix}\nMEDIA:{path}"

    return _SANDBOX_ARTIFACT_LINK_RE.sub(replace, str(text or ""))


def _visible_attachment_filename_paths(text: str | None, *, principal_id: str | None) -> tuple[Path, ...]:
    paths: list[Path] = []
    for match in _VISIBLE_ATTACHMENT_FILENAME_RE.finditer(str(text or "")):
        filename = Path(str(match.group("filename") or "").strip().strip("`'\"<>")).name
        if not filename:
            continue
        if Path(filename).suffix.lower() not in VALID_ATTACHMENT_EXTENSIONS:
            continue
        path = _resolve_output_attachment_filename(filename, principal_id=principal_id)
        if path is not None:
            paths.append(path)
    return tuple(dict.fromkeys(paths))


def text_or_attachments_expect_attachment_delivery(
    text: str | None,
    *,
    attachments: list[dict[str, str]] | tuple[dict[str, str], ...] | None = None,
) -> bool:
    """Compatibility wrapper around `delivery_contract_for_turn`.

    This intentionally ignores text-only heuristics. Attachment delivery must
    be backed by runtime evidence or a structured task contract.
    """
    return delivery_contract_for_turn(text, inbound_attachments=attachments).requires_attachment_delivery


def delivery_contract_for_turn(
    text: str | None,
    *,
    reply: str | None = None,
    inbound_attachments: list[dict[str, str]] | tuple[dict[str, str], ...] | None = None,
    artifact_paths: list[str] | tuple[str, ...] | None = None,
    requires_attachment_delivery: bool = False,
    required_attachment_extensions: tuple[str, ...] | list[str] | None = None,
) -> DeliveryContract:
    """Build a platform-neutral delivery contract for a messaging turn.

    Durable runtime evidence wins:
    - `MEDIA:` directives in the reply mean the runtime produced artifacts to send.
    - tool/result artifact paths mean a produced artifact must cross the platform boundary.
    - active task-frame requirements remain authoritative.

    User text is accepted for call-site compatibility only. It is not used to
    decide whether an attachment is required.
    """

    required_extensions = _required_attachment_extensions_from_contract(
        text,
        required_attachment_extensions=required_attachment_extensions,
    )
    produced_artifact_extensions = tuple(
        dict.fromkeys(
            Path(str(path or "")).suffix.lower()
            for path in (artifact_paths or ())
            if str(path or "").strip() and Path(str(path or "")).suffix
        )
    )
    if produced_artifact_extensions and (requires_attachment_delivery or required_extensions):
        required_extensions = (
            *tuple(
                extension
                for extension in produced_artifact_extensions
                if extension not in set(required_extensions)
            ),
            *required_extensions,
        )
    if requires_attachment_delivery:
        return DeliveryContract.attachment_required(
            source="task_contract",
            allow_plain_paths=True,
            required_attachment_extensions=required_extensions,
        )
    if artifact_paths and (requires_attachment_delivery or bool(required_extensions)):
        return DeliveryContract.attachment_required(
            source="artifact_result",
            allow_plain_paths=True,
            required_attachment_extensions=required_extensions,
        )
    if media_candidate_paths_from_text(str(reply or "")):
        return DeliveryContract.attachment_required(
            source="media_directive",
            allow_plain_paths=False,
            required_attachment_extensions=required_extensions,
        )
    return DeliveryContract.message_only()


def _active_task_frame_for_delivery_contract(runtime: object, conversation_id: str | None):
    store = getattr(runtime, "store", runtime)
    if store is None or not isinstance(conversation_id, str) or not conversation_id:
        return None
    try:
        active_frame_id = store.get_active_task_frame_id(conversation_id)
    except Exception:
        return None
    if not isinstance(active_frame_id, str) or not active_frame_id:
        return None
    try:
        frame = store.get_task_frame(active_frame_id)
    except Exception:
        return None
    if frame is None or getattr(frame, "status", None) not in {
        TaskFrameStatus.ACTIVE,
        TaskFrameStatus.RUNNING,
        TaskFrameStatus.WAITING_APPROVAL,
        TaskFrameStatus.WAITING_INPUT,
        TaskFrameStatus.VERIFYING,
    }:
        return None
    return frame


def _extension_for_artifact_kind(artifact_kind: object) -> str | None:
    normalized = str(artifact_kind or "").strip().lower().removeprefix(".")
    if not normalized or normalized == "file":
        return None
    extension = ATTACHMENT_TOKEN_EXTENSIONS.get(normalized)
    if extension is not None:
        return extension
    direct_extension = f".{normalized}"
    if re.fullmatch(r"\.[a-z0-9]{1,16}", direct_extension):
        return direct_extension
    return None


def delivery_contract_for_runtime_turn(
    runtime: object,
    conversation_id: str | None,
    text: str | None,
    *,
    reply: str | None = None,
    inbound_attachments: list[dict[str, str]] | tuple[dict[str, str], ...] | None = None,
    artifact_paths: list[str] | tuple[str, ...] | None = None,
    requires_attachment_delivery: bool = False,
    required_attachment_extensions: tuple[str, ...] | list[str] | None = None,
) -> DeliveryContract:
    """Build a delivery contract and fold in the active task-frame finish line."""
    base_contract = delivery_contract_for_turn(
        text,
        reply=reply,
        inbound_attachments=inbound_attachments,
        artifact_paths=artifact_paths,
        requires_attachment_delivery=requires_attachment_delivery,
        required_attachment_extensions=required_attachment_extensions,
    )
    frame = _active_task_frame_for_delivery_contract(runtime, conversation_id)
    finish = getattr(frame, "finish", None)
    if frame is None or not bool(getattr(finish, "requires_artifact_delivery", False)):
        return base_contract
    if getattr(frame, "status", None) == TaskFrameStatus.WAITING_INPUT and not base_contract.requires_attachment_delivery:
        # Clarification turns should be delivered as questions even when the
        # parent frame will eventually produce an attachment.
        return base_contract
    output = getattr(frame, "output", None)
    required_extension = _extension_for_artifact_kind(
        getattr(finish, "required_artifact_kind", None) or getattr(output, "artifact_kind", None)
    )
    required_extensions = _required_attachment_extensions_from_contract(
        text,
        required_attachment_extensions=(
            *(base_contract.required_attachment_extensions or ()),
            *((required_extension,) if required_extension else ()),
        ),
    )
    return DeliveryContract.attachment_required(
        source="task_frame",
        allow_plain_paths=True,
        required_attachment_extensions=required_extensions,
    )


def _required_attachment_extensions_from_contract(
    text: str | None,
    *,
    required_attachment_extensions: tuple[str, ...] | list[str] | None = None,
) -> tuple[str, ...]:
    extensions: list[str] = []
    for extension in required_attachment_extensions or ():
        normalized = str(extension or "").strip().lower()
        if not normalized:
            continue
        if not normalized.startswith("."):
            normalized = f".{normalized}"
        extensions.append(normalized)
    planned_extension = plan_attachment_format(text or "").extension
    if planned_extension and planned_extension.lower() not in extensions:
        extensions.append(planned_extension.lower())
    for extension in _contract_filename_extensions(text):
        if extension not in extensions:
            extensions.append(extension)
    return tuple(extensions)


def _contract_filename_extension(text: str | None) -> str | None:
    extensions = _contract_filename_extensions(text)
    return extensions[0] if extensions else None


def _contract_filename_extensions(text: str | None) -> tuple[str, ...]:
    extensions: list[str] = []
    for raw_token in re.split(r"[\s,;:]+", str(text or "")):
        token = raw_token.strip().strip("`'\"<>[](){}")
        if not token or "/" in token or "\\" in token or "=" in token:
            continue
        match = re.search(r"\.([A-Za-z0-9]{1,16})(?:[.!?])?$", token)
        if not match:
            continue
        if "/" in token or "\\" in token or "=" in token:
            continue
        extension = f".{match.group(1).lower()}"
        if extension in {".ai", ".app", ".co", ".com", ".dev", ".edu", ".gov", ".io", ".net", ".org", ".us"}:
            continue
        # This helper is only used after structured delivery evidence exists
        # (MEDIA, artifact result, or task contract). A bare filename here is a
        # format filter for an already-produced attachment, not task routing.
        extensions.append(extension)
    return tuple(dict.fromkeys(extensions))


def _attachments_matching_contract(
    attachments: tuple[Path, ...],
    delivery_contract: DeliveryContract,
) -> tuple[Path, ...]:
    required_extensions = tuple(
        extension.lower() for extension in delivery_contract.required_attachment_extensions if extension
    )
    if not required_extensions:
        return attachments
    return tuple(path for path in attachments if path.suffix.lower() in required_extensions)


def _drop_primary_artifact_text_sidecars(
    attachments: tuple[Path, ...],
    *,
    required_extensions: tuple[str, ...] | list[str] = (),
) -> tuple[Path, ...]:
    # Generated artifacts often have status/summary sidecars for internal QA.
    # Chat delivery should send the primary artifact unless the sidecar was
    # explicitly requested, otherwise users receive duplicate/confusing files.
    requested_extensions = {
        extension if str(extension or "").startswith(".") else f".{extension}"
        for extension in (str(item or "").strip().lower() for item in required_extensions)
        if extension
    }
    primary_artifacts: list[Path] = []
    primary_stems: set[str] = set()
    for path in attachments:
        if path.suffix.lower() in _PRIMARY_RENDERED_ATTACHMENT_SUFFIXES:
            primary_artifacts.append(_resolved_or_expanded_path(path))
            primary_stems.add(path.stem)
    if not primary_stems:
        return attachments

    def is_summary_sidecar(path: Path) -> bool:
        if path.suffix.lower() not in _TEXT_SIDECAR_ATTACHMENT_SUFFIXES:
            return False
        stem = path.stem.lower()
        return any(
            stem.startswith(f"{primary_stem.lower()}{separator}{sidecar_kind}")
            for primary_stem in primary_stems
            for separator in ("_", "-", ".")
            for sidecar_kind in ("summary", "completion", "status", "request", "note", "notes")
        )

    def is_support_asset_text_marker(path: Path) -> bool:
        if path.suffix.lower() not in _TEXT_SIDECAR_ATTACHMENT_SUFFIXES:
            return False
        return _asset_dir_belongs_to_primary_artifact(
            _resolved_or_expanded_path(path).parent,
            tuple(primary_artifacts),
        )

    def is_data_or_state_sidecar(path: Path) -> bool:
        suffix = path.suffix.lower()
        if suffix not in _DATA_SIDECAR_ATTACHMENT_SUFFIXES:
            return False
        if not requested_extensions:
            return False
        if suffix in requested_extensions:
            return False
        stem = path.stem.lower()
        return any(
            stem == primary_stem.lower()
            or any(stem.startswith(f"{primary_stem.lower()}{separator}") for separator in ("_", "-", "."))
            or any(stem.endswith(f"{separator}{primary_stem.lower()}") for separator in ("_", "-", "."))
            for primary_stem in primary_stems
        )

    return tuple(
        path
        for path in attachments
        if not is_summary_sidecar(path)
        and not is_support_asset_text_marker(path)
        and not is_data_or_state_sidecar(path)
        and (
            path.suffix.lower() not in _INTERNAL_STATE_ATTACHMENT_SUFFIXES
            or path.suffix.lower() in requested_extensions
        )
    )


def _resolved_or_expanded_path(path: Path) -> Path:
    candidate = path.expanduser()
    try:
        return candidate.resolve()
    except OSError:
        return candidate


def _asset_dir_belongs_to_primary_artifact(asset_dir: Path, primary_artifacts: tuple[Path, ...]) -> bool:
    dir_name = asset_dir.name.lower()
    if "asset" not in dir_name:
        return False
    for primary in primary_artifacts:
        if asset_dir.parent != primary.parent:
            continue
        primary_stem = primary.stem.lower()
        if dir_name in {
            f"{primary_stem}_assets",
            f"{primary_stem}-assets",
            f"{primary_stem}.assets",
            "assets",
        }:
            return True
        if dir_name.endswith("_assets") or dir_name.endswith("-assets") or dir_name.endswith(".assets"):
            return True
    return False


class _LinkedHtmlAssetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.references: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() not in {"img", "source", "video", "audio", "link", "script"}:
            return
        for name, value in attrs:
            if name and name.lower() in {"src", "href", "poster", "srcset"} and value:
                if name.lower() == "srcset":
                    self.references.extend(part.strip().split()[0] for part in value.split(",") if part.strip())
                else:
                    self.references.append(value)


def _local_linked_html_asset_paths(html_path: Path) -> tuple[Path, ...]:
    try:
        if not html_path.is_file() or html_path.stat().st_size > 40 * 1024 * 1024:
            return ()
        html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ()
    parser = _LinkedHtmlAssetParser()
    try:
        parser.feed(html_text)
    except Exception:
        return ()
    assets: list[Path] = []
    for reference in parser.references:
        raw = str(reference or "").strip().strip("'\"")
        if not raw or raw.startswith("#") or raw.lower().startswith(("data:", "http:", "https:", "mailto:", "tel:", "javascript:")):
            continue
        normalized = raw.split("#", 1)[0].split("?", 1)[0]
        if not normalized:
            continue
        candidate = (html_path.parent / unquote(normalized)).resolve(strict=False)
        try:
            if candidate.is_file():
                candidate.relative_to(html_path.parent)
                assets.append(candidate)
        except OSError:
            continue
        except ValueError:
            continue
    return tuple(dict.fromkeys(assets))


def _expand_linked_html_assets(attachments: tuple[Path, ...]) -> tuple[Path, ...]:
    expanded: list[Path] = list(attachments)
    for path in attachments:
        if path.suffix.lower() not in {".html", ".htm"}:
            continue
        for asset in _local_linked_html_asset_paths(_resolved_or_expanded_path(path)):
            if asset.suffix.lower() not in _SUPPORT_IMAGE_ATTACHMENT_SUFFIXES:
                continue
            if asset not in expanded:
                expanded.append(asset)
    return tuple(expanded)


def _collapse_mixed_image_sidecars(attachments: tuple[Path, ...]) -> tuple[Path, ...]:
    # Only suppress image files that are support captures/assets for a primary
    # rendered artifact. Unrelated images may be the actual deliverable set.
    primary_artifacts = tuple(
        path
        for path in attachments
        if path.suffix.lower() in _PRIMARY_RENDERED_ATTACHMENT_SUFFIXES
    )
    report_stems = tuple(
        path.stem.lower()
        for path in primary_artifacts
    )
    if not report_stems:
        return attachments
    embedded_or_referenced = _embedded_or_referenced_support_images(attachments)
    multi_primary_bundle = len(primary_artifacts) > 1

    def is_report_image_sidecar(path: Path) -> bool:
        if path.suffix.lower() not in _SUPPORT_IMAGE_ATTACHMENT_SUFFIXES:
            return False
        if multi_primary_bundle:
            return True
        if path in embedded_or_referenced:
            return True
        stem = path.stem.lower()
        for report_stem in report_stems:
            if stem == report_stem:
                return True
            if any(stem.startswith(f"{report_stem}{separator}") for separator in ("_", "-", ".")):
                return True
            if any(stem.endswith(f"{separator}{report_stem}") for separator in ("_", "-", ".")):
                return True
        return False

    collapsed = tuple(path for path in attachments if not is_report_image_sidecar(path))
    return collapsed or attachments


def _read_delivery_asset(path: Path, *, max_bytes: int = 20 * 1024 * 1024) -> bytes | None:
    try:
        if not path.is_file() or path.stat().st_size > max_bytes:
            return None
        return path.read_bytes()
    except OSError:
        return None


def _html_references_or_embeds_image(html_bytes: bytes, image_path: Path, image_bytes: bytes) -> bool:
    name = image_path.name.encode("utf-8", errors="ignore")
    if name and name in html_bytes:
        return True
    encoded = base64.b64encode(image_bytes)
    return bool(encoded and encoded in html_bytes)


def _zip_contains_image_bytes(container: Path, image_bytes: bytes) -> bool:
    try:
        with zipfile.ZipFile(container) as archive:
            for member in archive.infolist():
                member_suffix = Path(member.filename).suffix.lower()
                if member_suffix not in _SUPPORT_IMAGE_ATTACHMENT_SUFFIXES:
                    continue
                if archive.read(member) == image_bytes:
                    return True
    except (OSError, zipfile.BadZipFile, RuntimeError):
        return False
    return False


def _embedded_or_referenced_support_images(attachments: tuple[Path, ...]) -> set[Path]:
    primary_artifacts = tuple(
        path
        for path in attachments
        if path.suffix.lower() in _PRIMARY_RENDERED_ATTACHMENT_SUFFIXES and path.is_file()
    )
    image_assets = tuple(
        path
        for path in attachments
        if path.suffix.lower() in _SUPPORT_IMAGE_ATTACHMENT_SUFFIXES and path.is_file()
    )
    if not primary_artifacts or not image_assets:
        return set()
    image_bytes_by_path = {
        path: data
        for path in image_assets
        for data in (_read_delivery_asset(path),)
        if data
    }
    embedded: set[Path] = set()
    for primary in primary_artifacts:
        suffix = primary.suffix.lower()
        if suffix in {".html", ".htm"}:
            html_bytes = _read_delivery_asset(primary, max_bytes=40 * 1024 * 1024)
            if not html_bytes:
                continue
            for image_path, image_bytes in image_bytes_by_path.items():
                if image_path not in embedded and _html_references_or_embeds_image(html_bytes, image_path, image_bytes):
                    embedded.add(image_path)
            continue
        if suffix in {".docx", ".pptx", ".xlsx"}:
            for image_path, image_bytes in image_bytes_by_path.items():
                if image_path not in embedded and _zip_contains_image_bytes(primary, image_bytes):
                    embedded.add(image_path)
    return embedded


def _plain_candidate_paths_from_text(text: str) -> list[Path]:
    paths: list[Path] = []
    for match in _PLAIN_ABSOLUTE_PATH_RE.finditer(str(text or "")):
        raw = match.group(1).rstrip(").,;:")
        if raw:
            paths.append(Path(raw))
    for match in _PLAIN_RELATIVE_OUTPUT_PATH_RE.finditer(str(text or "")):
        raw = match.group(1).rstrip(").,;:")
        if raw:
            paths.append(Path(raw))
    return list(dict.fromkeys(paths))


def _caption_without_attached_paths(
    text: str,
    attachment_paths: tuple[Path, ...],
    *,
    candidate_path_texts: tuple[str, ...] = (),
) -> str | None:
    path_texts = {str(path) for path in attachment_paths}
    path_texts.update(str(candidate).strip() for candidate in candidate_path_texts if str(candidate).strip())
    caption_lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = raw_line
        removed_path = False
        for path_text in path_texts:
            if path_text in line:
                removed_path = True
            line = line.replace(path_text, "")
        stripped = line.strip()
        if not stripped:
            if not removed_path and caption_lines and caption_lines[-1] != "":
                caption_lines.append("")
            continue
        if removed_path and not stripped.strip("`'\"<>[]() "):
            continue
        if stripped.rstrip(":").strip().lower() in {
            "attachment/artifact link",
            "artifact link",
            "attachment link",
            "file",
            "file path",
            "media",
            "path",
        }:
            continue
        caption_lines.append(stripped)
    caption = "\n".join(caption_lines).strip()
    return caption or "Attached the requested file."


def _line_is_attachment_filename_only(line: str, attachment_names: set[str]) -> bool:
    stripped = str(line or "").strip()
    if not stripped:
        return False
    stripped = re.sub(r"^(?:[-*•]|\d+[.)])\s+", "", stripped).strip()
    stripped = stripped.strip("`'\" ")
    return stripped in attachment_names


def _line_is_attachment_list_heading(line: str) -> bool:
    normalized = _normalize_attachment_caption_for_comparison(
        str(line or "").strip().rstrip(":")
    )
    return normalized in {
        "attached file",
        "attached files",
        "attachment",
        "attachments",
        "artifact",
        "artifacts",
        "file",
        "files",
    }


def _line_is_redundant_attachment_block_heading(line: str) -> bool:
    normalized = _normalize_attachment_caption_for_comparison(
        str(line or "").strip().rstrip(":")
    )
    return _line_is_attachment_list_heading(line) or normalized in {
        "artifact files created",
        "artifact file created",
        "files created",
        "file created",
    }


def _caption_without_attached_filename_list(
    text: str | None,
    attachments: tuple[Path, ...],
) -> str | None:
    if not text or not attachments:
        return text
    names = {path.name for path in attachments if path.name}
    if not names:
        return text
    original_lines = str(text).splitlines()
    remove_indexes: set[int] = set()
    index = 0
    while index < len(original_lines):
        if not _line_is_attachment_filename_only(original_lines[index], names):
            index += 1
            continue
        block_indexes: list[int] = []
        cursor = index
        while cursor < len(original_lines):
            line = original_lines[cursor]
            if _line_is_attachment_filename_only(line, names):
                block_indexes.append(cursor)
                cursor += 1
                continue
            if not line.strip():
                lookahead = cursor + 1
                while lookahead < len(original_lines) and not original_lines[lookahead].strip():
                    lookahead += 1
                if lookahead < len(original_lines) and _line_is_attachment_filename_only(original_lines[lookahead], names):
                    cursor += 1
                    continue
            break
        previous_index = index - 1
        while previous_index >= 0 and not original_lines[previous_index].strip():
            previous_index -= 1
        if previous_index >= 0 and _line_is_redundant_attachment_block_heading(original_lines[previous_index]):
            remove_indexes.update(block_indexes)
            if previous_index not in remove_indexes:
                remove_indexes.add(previous_index)
        index = cursor
    lines = [
        line
        for index, line in enumerate(original_lines)
        if index not in remove_indexes
    ]
    caption = "\n".join(lines).strip()
    return caption or None


def _caption_without_unattached_filename_lines(
    text: str | None,
    *,
    attachments: tuple[Path, ...],
    candidates: tuple[Path, ...],
) -> str | None:
    if not text or not candidates:
        return text
    attached_names = {path.name for path in attachments if path.name}
    suppressed_names = {
        path.name
        for path in candidates
        if path.name and path.name not in attached_names
    }
    if not suppressed_names:
        return text
    lines = [
        line
        for line in str(text).splitlines()
        if not _line_is_attachment_filename_only(line, suppressed_names)
    ]
    caption = "\n".join(lines).strip()
    return caption or None


_GENERIC_ATTACHMENT_CAPTIONS = frozenset(
    {
        "attached the requested file",
        "attached the requested files",
        "done - attached the requested file",
        "done - attached the requested files",
        "done - attached the requested artifact",
        "done - attached the requested artifacts",
        "done - attached the requested artifact package",
    }
)


def _normalize_attachment_caption_for_comparison(text: str | None) -> str:
    normalized = " ".join(str(text or "").strip().rstrip(".").split())
    return normalized.replace("\u2014", "-").casefold()


def _caption_with_attached_file_names(text: str | None, attachments: tuple[Path, ...]) -> str | None:
    if not text or not attachments:
        return text
    if _normalize_attachment_caption_for_comparison(text) not in _GENERIC_ATTACHMENT_CAPTIONS:
        return text
    names = [path.name for path in attachments if path.name]
    if not names:
        return text
    if len(names) > 3 or any(len(name) > 48 for name in names):
        label = "file" if len(names) == 1 else "files"
        return f"{str(text).strip()}\n\nAttached {len(names)} {label}."
    preview_names = names[:8]
    label = "Attached files:" if len(names) != 1 else "Attached file:"
    lines = [str(text).strip(), "", label]
    lines.extend(f"- {name}" for name in preview_names)
    remaining = len(names) - len(preview_names)
    if remaining > 0:
        lines.append(f"...and {remaining} more file{'s' if remaining != 1 else ''}.")
    return "\n".join(lines)


def _strip_platform_internal_markers(text: str) -> str:
    lines = [
        line
        for line in str(text or "").splitlines()
        if not line.strip().startswith(_BUILDER_SKILL_MARKER_PREFIX)
    ]
    return "\n".join(lines).strip()


def _normalize_platform_scheduled_task_heading(line: str) -> str | None:
    value = str(line or "").strip()
    for icon in ("⏱️", "⏰"):
        if value.startswith(icon):
            value = value[len(icon) :].strip()
            break
    for marker in ("**", "__"):
        if value.startswith(marker) and value.endswith(marker) and len(value) >= len(marker) * 2:
            value = value[len(marker) : -len(marker)].strip()
            break
    collapsed = " ".join(value.split())
    upper = collapsed.upper()
    for label in ("MANUAL SCHEDULED TASK RUN", "SCHEDULED TASK"):
        raw_label = collapsed.split(":", 1)[0].strip()
        if raw_label.upper() != label:
            continue
        if upper == label:
            return f"⏰ **{label}**"
        if upper.startswith(f"{label}:"):
            suffix = collapsed.split(":", 1)[1].strip()
            return f"⏰ **{label}: {suffix}**" if suffix else f"⏰ **{label}**"
    return None


def _normalize_platform_product_heading_blocks(
    text: str | None,
    *,
    compact_spacing: bool = False,
) -> str | None:
    if text is None:
        return None
    lines = str(text or "").splitlines()
    if not lines:
        return text
    heading = _normalize_platform_scheduled_task_heading(lines[0])
    if heading is None:
        return text
    rest_lines = lines[1:]
    while rest_lines and not str(rest_lines[0] or "").strip():
        rest_lines.pop(0)
    body = "\n".join(rest_lines).strip()
    if not body:
        return heading
    separator = "\n" if compact_spacing else "\n\n"
    return f"{heading}{separator}{body}"


def _compact_platform_scheduled_task_caption_spacing(text: str | None) -> str | None:
    if text is None:
        return None
    lines = str(text or "").splitlines()
    if len(lines) < 3:
        return text
    first = lines[0].strip()
    first_normalized = first
    if first_normalized.startswith("⏰"):
        first_normalized = first_normalized[1:].strip()
    first_normalized = first_normalized.strip("*_ ")
    if not first_normalized.casefold().startswith(("scheduled task:", "manual scheduled task run:")):
        return text
    compacted = [lines[0]]
    index = 1
    while index < len(lines) and not lines[index].strip():
        index += 1
    compacted.extend(lines[index:])
    return "\n".join(compacted).strip()


def _sanitize_platform_visible_text(
    text: str | None,
    *,
    compact_scheduled_task_spacing: bool = False,
) -> str | None:
    if text is None:
        return None
    sanitized = sanitize_user_visible_reply(
        user_message=None,
        reply=text,
        tool_results=None,
        source="platform_delivery",
    ) or text
    sanitized = _sanitize_local_paths(sanitized)
    return _normalize_platform_product_heading_blocks(
        sanitized,
        compact_spacing=compact_scheduled_task_spacing,
    )


def _max_automatic_attachment_batch() -> int:
    raw = os.environ.get("NULLION_MAX_AUTOMATIC_ATTACHMENT_BATCH")
    try:
        value = int(str(raw or "").strip())
    except Exception:
        return _DEFAULT_MAX_AUTOMATIC_ATTACHMENT_BATCH
    return max(2, min(100, value))


def _attachment_batch_confirmation_text(
    attachments: tuple[Path, ...],
    *,
    batch_limit: int,
) -> str:
    total = len(attachments)
    preview_count = min(10, total)
    preview_lines = [
        f"{index + 1}. `{attachments[index].name or str(attachments[index])}`"
        for index in range(preview_count)
    ]
    remaining = total - preview_count
    if remaining > 0:
        preview_lines.append(f"...and {remaining} more file{'s' if remaining != 1 else ''}.")
    preview_block = "\n".join(preview_lines) if preview_lines else "(No files)"
    return (
        f"I prepared **{total} files**, and sending that many at once can fail on this chat surface.\n"
        "I did not send attachments yet.\n\n"
        f"Preview:\n{preview_block}\n\n"
        f"Ask me to send a smaller range, up to {batch_limit} files, or name the specific files you want."
    )


def _format_bytes_label(size_bytes: int) -> str:
    size = float(max(0, int(size_bytes)))
    units = ("bytes", "KB", "MB", "GB")
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    if unit == "bytes":
        return f"{int(size)} bytes"
    return f"{size:.1f} {unit}".replace(".0 ", " ")


def telegram_attachment_upload_limit_bytes() -> int:
    raw = os.environ.get("NULLION_TELEGRAM_ATTACHMENT_UPLOAD_LIMIT_BYTES")
    try:
        value = int(str(raw or "").strip())
    except Exception:
        return _DEFAULT_TELEGRAM_ATTACHMENT_UPLOAD_LIMIT_BYTES
    return max(1, value)


def _large_attachment_confirmation_text(
    attachments: tuple[Path, ...],
    *,
    max_attachment_bytes: int,
) -> str:
    oversized = []
    for path in attachments:
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size > max_attachment_bytes:
            oversized.append((path, size))
    preview = "\n".join(
        f"{index + 1}. `{path.name}` ({_format_bytes_label(size)})"
        for index, (path, size) in enumerate(oversized[:5])
    )
    if len(oversized) > 5:
        preview += f"\n...and {len(oversized) - 5} more oversized file{'s' if len(oversized) != 6 else ''}."
    limit_label = _format_bytes_label(max_attachment_bytes)
    return (
        f"I prepared **{len(oversized)} file{'s' if len(oversized) != 1 else ''}** that exceed this chat's "
        f"attachment upload limit of **{limit_label}**.\n"
        "I did not upload them yet.\n\n"
        f"Oversized file{'s' if len(oversized) != 1 else ''}:\n{preview}\n\n"
        "Reply with one option:\n"
        "1. Try a compressed archive if it fits under the limit.\n"
        "2. Split into compressed parts and send them here.\n"
        "3. Set up a cloud storage connector and upload there.\n"
        "4. Log into the cloud storage web app in the agent browser so I can upload through the web UI."
    )


def _attachments_over_limit(
    attachments: tuple[Path, ...],
    *,
    max_attachment_bytes: int | None,
) -> tuple[Path, ...]:
    if max_attachment_bytes is None or max_attachment_bytes <= 0:
        return ()
    oversized: list[Path] = []
    for path in attachments:
        try:
            if path.stat().st_size > max_attachment_bytes:
                oversized.append(path)
        except OSError:
            continue
    return tuple(oversized)


def _hold_large_attachments_for_confirmation(
    attachments: tuple[Path, ...],
    *,
    max_attachment_bytes: int | None,
) -> PlatformDelivery | None:
    oversized = _attachments_over_limit(attachments, max_attachment_bytes=max_attachment_bytes)
    if not oversized or max_attachment_bytes is None:
        return None
    return PlatformDelivery(
        text=_sanitize_platform_visible_text(
            _large_attachment_confirmation_text(attachments, max_attachment_bytes=max_attachment_bytes)
        ),
        attachments=(),
        media_directive_count=len(attachments),
        pending_attachment_count=len(oversized),
    )


def prepare_reply_for_platform_delivery(
    reply: str | None,
    *,
    principal_id: str | None = None,
    allow_attachments: bool | None = None,
    delivery_contract: DeliveryContract | None = None,
    artifact_paths: list[str] | tuple[str, ...] | None = None,
    max_attachment_bytes: int | None = None,
) -> PlatformDelivery:
    if reply is None:
        return PlatformDelivery(text=None, attachments=())
    text = make_markdown_tables_chat_readable(sanitize_external_inline_markup(str(reply)))
    text = _strip_platform_internal_markers(text)
    text = _rewrite_sandbox_artifact_links_for_delivery(text, principal_id=principal_id)
    if delivery_contract is None:
        if allow_attachments is False:
            delivery_contract = DeliveryContract.message_only()
        elif allow_attachments is True:
            delivery_contract = DeliveryContract.attachment_required(
                source="legacy_allow_attachments",
                allow_plain_paths=True,
            )
        else:
            delivery_contract = delivery_contract_for_turn(None, reply=text)
    structured_attachments = tuple(
        resolved_path
        for path in artifact_paths or ()
        for resolved_path in (_resolve_messaging_attachment_path(Path(str(path)), principal_id=principal_id),)
        if _is_deliverable_messaging_attachment(resolved_path, principal_id=principal_id)
    )
    if structured_attachments and delivery_contract.allow_attachment_delivery:
        attachments = _drop_primary_artifact_text_sidecars(
            _attachments_matching_contract(tuple(dict.fromkeys(structured_attachments)), delivery_contract),
            required_extensions=delivery_contract.required_attachment_extensions,
        )
        expanded_attachments = _expand_linked_html_assets(attachments)
        added_linked_assets = tuple(path for path in expanded_attachments if path not in attachments)
        attachments = tuple(
            dict.fromkeys(
                (
                    *_collapse_mixed_image_sidecars(attachments),
                    *added_linked_assets,
                )
            )
        )
        oversized_delivery = _hold_large_attachments_for_confirmation(
            attachments,
            max_attachment_bytes=max_attachment_bytes,
        )
        if oversized_delivery is not None:
            return oversized_delivery
        automatic_batch_limit = _max_automatic_attachment_batch()
        if len(attachments) > automatic_batch_limit:
            confirmation_text = _attachment_batch_confirmation_text(
                attachments,
                batch_limit=automatic_batch_limit,
            )
            return PlatformDelivery(
                text=_sanitize_platform_visible_text(confirmation_text),
                attachments=(),
                media_directive_count=len(attachments),
                pending_attachment_count=len(attachments),
            )
        if attachments:
            base_text = _caption_without_unattached_filename_lines(
                _sanitize_platform_visible_text(
                    text if text else None,
                ),
                attachments=attachments,
                candidates=tuple(dict.fromkeys(structured_attachments)),
            )
            visible_text = _caption_with_attached_file_names(
                base_text,
                attachments,
            )
            return PlatformDelivery(
                text=_sanitize_platform_visible_text(
                    visible_text,
                ),
                attachments=attachments,
                media_directive_count=len(attachments),
            )
    raw_media_candidate_paths = tuple(media_candidate_paths_from_text(text))
    raw_media_directive_count = len(raw_media_candidate_paths)
    resolved_raw_media_candidate_paths = tuple(
        _resolve_messaging_attachment_path(path, principal_id=principal_id)
        for path in raw_media_candidate_paths
    )
    media_candidates = list(
        _drop_primary_artifact_text_sidecars(
            raw_media_candidate_paths,
            required_extensions=delivery_contract.required_attachment_extensions,
        )
    )
    automatic_batch_limit = _max_automatic_attachment_batch()
    caption, attachments = split_reply_for_platform_delivery(text, principal_id=principal_id)
    split_attachment_identities = {
        str(_resolved_or_expanded_path(path))
        for path in attachments
    }
    undeliverable_raw_media_count = sum(
        1
        for path in resolved_raw_media_candidate_paths
        if str(_resolved_or_expanded_path(path)) not in split_attachment_identities
        and not _is_deliverable_messaging_attachment(
            path,
            principal_id=principal_id,
            allow_explicit_media_path=True,
        )
    )
    if media_candidates and delivery_contract.allow_attachment_delivery:
        attachments = tuple(
            dict.fromkeys(
                (
                    *attachments,
                    *_visible_attachment_filename_paths(caption or text, principal_id=principal_id),
                )
            )
        )
    plain_candidates = _plain_candidate_paths_from_text(text)
    if media_candidates and not delivery_contract.allow_attachment_delivery:
        return PlatformDelivery(
            text=_sanitize_platform_visible_text(caption if caption else None),
            attachments=(),
            media_directive_count=0,
        )
    if (
        not media_candidates
        and delivery_contract.allow_attachment_delivery
        and delivery_contract.allow_plain_path_attachment_delivery
    ):
        plain_attachments = tuple(
            resolved_path
            for path in plain_candidates
            for resolved_path in (_resolve_messaging_attachment_path(path, principal_id=principal_id),)
            if _is_deliverable_messaging_attachment(resolved_path, principal_id=principal_id)
        )
        plain_attachments = _attachments_matching_contract(plain_attachments, delivery_contract)
        plain_attachments = _drop_primary_artifact_text_sidecars(
            plain_attachments,
            required_extensions=delivery_contract.required_attachment_extensions,
        )
        plain_attachments = tuple(dict.fromkeys(plain_attachments))
        expanded_plain_attachments = _expand_linked_html_assets(plain_attachments)
        added_linked_assets = tuple(path for path in expanded_plain_attachments if path not in plain_attachments)
        plain_attachments = tuple(dict.fromkeys((*_collapse_mixed_image_sidecars(plain_attachments), *added_linked_assets)))
        if plain_attachments:
            oversized_delivery = _hold_large_attachments_for_confirmation(
                plain_attachments,
                max_attachment_bytes=max_attachment_bytes,
            )
            if oversized_delivery is not None:
                return oversized_delivery
            if len(plain_attachments) > automatic_batch_limit:
                confirmation_text = _attachment_batch_confirmation_text(
                    plain_attachments,
                    batch_limit=automatic_batch_limit,
                )
                return PlatformDelivery(
                    text=_sanitize_platform_visible_text(confirmation_text),
                    attachments=(),
                    media_directive_count=len(plain_attachments),
                    pending_attachment_count=len(plain_attachments),
                )
            visible_text = _caption_without_attached_paths(
                text,
                plain_attachments,
                candidate_path_texts=tuple(str(path) for path in plain_candidates),
            )
            visible_text = _caption_without_unattached_filename_lines(
                visible_text,
                attachments=plain_attachments,
                candidates=tuple(plain_candidates),
            )
            visible_text = _caption_with_attached_file_names(visible_text, plain_attachments) or visible_text
            return PlatformDelivery(
                text=_sanitize_platform_visible_text(
                    visible_text,
                ),
                attachments=plain_attachments,
                media_directive_count=len(plain_attachments),
            )
    if not media_candidates:
        if delivery_contract.requires_attachment_delivery:
            return PlatformDelivery(
                text=_ATTACHMENT_UNAVAILABLE_REPLY,
                attachments=(),
                media_directive_count=1,
                unavailable_attachment_count=1,
            )
        return PlatformDelivery(text=_sanitize_platform_visible_text(text if text else None), attachments=())
    attachments = _drop_primary_artifact_text_sidecars(
        _attachments_matching_contract(attachments, delivery_contract),
        required_extensions=delivery_contract.required_attachment_extensions,
    )
    expanded_attachments = _expand_linked_html_assets(attachments)
    added_linked_assets = tuple(path for path in expanded_attachments if path not in attachments)
    collapsed_attachments = _collapse_mixed_image_sidecars(attachments)
    collapsed_attachments = tuple(dict.fromkeys((*collapsed_attachments, *added_linked_assets)))
    suppressed_sidecar_count = max(0, len(attachments) - len(collapsed_attachments))
    attachments = collapsed_attachments
    oversized_delivery = _hold_large_attachments_for_confirmation(
        attachments,
        max_attachment_bytes=max_attachment_bytes,
    )
    if oversized_delivery is not None:
        return oversized_delivery
    if len(attachments) > automatic_batch_limit:
        confirmation_text = _attachment_batch_confirmation_text(
            attachments,
            batch_limit=automatic_batch_limit,
        )
        return PlatformDelivery(
            text=_sanitize_platform_visible_text(confirmation_text),
            attachments=(),
            media_directive_count=raw_media_directive_count,
            pending_attachment_count=len(attachments),
        )
    expected_media_candidates = _attachments_matching_contract(
        tuple(
            _resolve_messaging_attachment_path(path, principal_id=principal_id)
            for path in media_candidates
        ),
        delivery_contract,
    )
    expected_media_gap = (
        max(0, len(expected_media_candidates) - len(attachments))
        if delivery_contract.required_attachment_extensions
        else 0
    )
    missing_required_count = 1 if delivery_contract.requires_attachment_delivery and not attachments else 0
    delivered_attachment_identities = {
        str(_resolved_or_expanded_path(path))
        for path in attachments
    }
    filtered_required_sidecar_count = 0
    if delivery_contract.required_attachment_extensions:
        sidecar_suffixes = _TEXT_SIDECAR_ATTACHMENT_SUFFIXES | _DATA_SIDECAR_ATTACHMENT_SUFFIXES
        filtered_required_sidecar_count = sum(
            1
            for path in resolved_raw_media_candidate_paths
            if str(_resolved_or_expanded_path(path)) not in delivered_attachment_identities
            and path.suffix.lower() in sidecar_suffixes
        )
    unavailable_count = max(
        undeliverable_raw_media_count,
        expected_media_gap,
        missing_required_count,
        filtered_required_sidecar_count,
    )
    if not attachments:
        return PlatformDelivery(
            text=_ATTACHMENT_UNAVAILABLE_REPLY,
            attachments=(),
            media_directive_count=raw_media_directive_count,
            unavailable_attachment_count=unavailable_count or len(media_candidates),
        )
    delivery_text = _caption_without_attached_paths(caption, attachments) if caption else None
    delivery_text = _caption_without_unattached_filename_lines(
        delivery_text,
        attachments=attachments,
        candidates=tuple(
            resolved
            for path in raw_media_candidate_paths
            for resolved in (_resolve_messaging_attachment_path(path, principal_id=principal_id),)
            if resolved is not None
        ),
    )
    delivery_text = _caption_without_attached_filename_list(delivery_text, attachments)
    delivery_text = _caption_with_attached_file_names(delivery_text, attachments)
    delivery_text = _sanitize_platform_visible_text(
        delivery_text,
    )
    if unavailable_count:
        suffix = (
            f"I attached {len(attachments)} file{'s' if len(attachments) != 1 else ''}, "
            f"but {unavailable_count} attachment{'s were' if unavailable_count != 1 else ' was'} unavailable."
        )
        delivery_text = f"{delivery_text}\n\n{suffix}" if delivery_text else suffix
    return PlatformDelivery(
        text=delivery_text,
        attachments=attachments,
        media_directive_count=raw_media_directive_count,
        unavailable_attachment_count=unavailable_count,
    )


def platform_delivery_failure_reply(_delivery: PlatformDelivery | None = None) -> str:
    return _ATTACHMENT_UPLOAD_FAILED_REPLY


async def execute_platform_reply_delivery(
    reply: str | None,
    *,
    channel: str,
    target_id: str | None,
    principal_id: str | None = None,
    delivery_contract: DeliveryContract | None = None,
    artifact_paths: list[str] | tuple[str, ...] | None = None,
    request_id: str | None = None,
    message_id: str | None = None,
    reply_already_sent: bool = False,
    send_text: PlatformTextSender,
    send_attachments: PlatformAttachmentSender | None = None,
) -> PlatformDeliveryResult:
    """Run the common final-delivery boundary for chat platforms.

    Platform wrappers own the actual API calls. This helper owns the shared
    Nullion delivery contract: normalize/split the reply, choose attachment or
    text delivery, persist receipts, and use one failure message for upload
    failures.
    """
    delivery = prepare_reply_for_platform_delivery(
        reply,
        principal_id=principal_id,
        delivery_contract=delivery_contract,
        artifact_paths=artifact_paths,
    )
    target = None if target_id is None else str(target_id)
    if reply_already_sent and not delivery.attachments:
        receipt = build_platform_delivery_receipt(
            channel=channel,
            target_id=target,
            delivery=delivery,
            transport_ok=True,
            request_id=request_id,
            message_id=message_id,
        )
        record_platform_delivery_receipt(receipt)
        return PlatformDeliveryResult(delivery=delivery, receipt=receipt)

    if delivery.attachments:
        uploaded = False
        if send_attachments is not None:
            try:
                uploaded = bool(await send_attachments(delivery.text, delivery.attachments))
            except Exception:
                uploaded = False
        if uploaded:
            receipt = build_platform_delivery_receipt(
                channel=channel,
                target_id=target,
                delivery=delivery,
                transport_ok=True,
                request_id=request_id,
                message_id=message_id,
            )
            record_platform_delivery_receipt(receipt)
            return PlatformDeliveryResult(delivery=delivery, receipt=receipt, attachments_sent=True)

        receipt = build_platform_delivery_receipt(
            channel=channel,
            target_id=target,
            delivery=delivery,
            transport_ok=False,
            request_id=request_id,
            message_id=message_id,
            error="attachment_upload_failed",
        )
        record_platform_delivery_receipt(receipt)
        fallback_text = platform_delivery_failure_reply(delivery)
        try:
            await send_text(fallback_text)
            fallback_sent = True
        except Exception:
            fallback_sent = False
        return PlatformDeliveryResult(
            delivery=delivery,
            receipt=receipt,
            text_sent=fallback_sent,
            fallback_text_sent=fallback_sent,
        )

    text = delivery.text or ""
    try:
        text_ok = bool(await send_text(text))
    except Exception:
        text_ok = False
    receipt = build_platform_delivery_receipt(
        channel=channel,
        target_id=target,
        delivery=delivery,
        transport_ok=text_ok,
        request_id=request_id,
        message_id=message_id,
        error=None if text_ok else "text_delivery_failed",
    )
    record_platform_delivery_receipt(receipt)
    return PlatformDeliveryResult(delivery=delivery, receipt=receipt, text_sent=text_ok)


def is_retryable_messaging_delivery_error(exc: Exception) -> bool:
    error_name = type(exc).__name__.lower()
    if any(token in error_name for token in ("timeout", "timedout", "network", "connection", "retryafter")):
        return True
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None) or getattr(exc, "status_code", None)
    try:
        status = int(status_code)
    except (TypeError, ValueError):
        return False
    return status in {408, 409, 425, 429, 500, 502, 503, 504}


def _messaging_delivery_retry_after_seconds(exc: Exception) -> float | None:
    for attribute in ("retry_after", "retry_after_seconds"):
        value = getattr(exc, attribute, None)
        if value is None:
            continue
        if hasattr(value, "total_seconds"):
            value = value.total_seconds()
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            continue
        if seconds > 0:
            return min(seconds, _MESSAGING_DELIVERY_RETRY_AFTER_MAX_SECONDS)
    return None


def _messaging_delivery_retry_delay_seconds(exc: Exception, fallback_seconds: float) -> float:
    retry_after_seconds = _messaging_delivery_retry_after_seconds(exc)
    if retry_after_seconds is not None:
        return retry_after_seconds
    return fallback_seconds


async def retry_messaging_delivery_operation(
    operation,
    *,
    attempts: int = _MESSAGING_ATTACHMENT_UPLOAD_ATTEMPTS,
    retry_delay_seconds: float = _MESSAGING_ATTACHMENT_UPLOAD_RETRY_DELAY_SECONDS,
) -> object:
    last_exc: Exception | None = None
    for attempt in range(max(1, attempts)):
        try:
            result = operation()
            if asyncio.iscoroutine(result):
                return await result
            return result
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts - 1 or not is_retryable_messaging_delivery_error(exc):
                break
            await asyncio.sleep(
                _messaging_delivery_retry_delay_seconds(exc, retry_delay_seconds * (attempt + 1))
            )
    assert last_exc is not None
    raise last_exc


def split_reply_for_platform(reply: str | None, *, limit: int) -> list[str]:
    if reply is None:
        return []
    text = str(reply).strip()
    if not text:
        return []
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        split_at = remaining.rfind("\n", 0, limit)
        if split_at < limit // 2:
            split_at = remaining.rfind(" ", 0, limit)
        if split_at < limit // 2:
            split_at = limit
        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].strip()
    if remaining:
        chunks.append(remaining)
    return chunks


def platform_plain_format_fallback_text(platform: str, plain_text: str) -> str:
    label = str(platform or "platform").strip() or "platform"
    plain = sanitize_external_inline_markup(plain_text or "")
    return (
        f"{label} could not send the formatted reply, so here is the same text as plain output:\n\n"
        "```text\n"
        f"{plain}"
        "\n```"
    )


def formatted_reply_chunks(
    reply: str | None,
    *,
    limit: int,
    formatter: Callable[[str], str],
) -> list[tuple[str, str]]:
    chunks = split_reply_for_platform(sanitize_external_inline_markup(reply or ""), limit=limit)
    return [(formatter(chunk), chunk) for chunk in chunks]


async def send_with_plain_text_fallback(
    *,
    platform: str,
    formatted_text: str,
    plain_text: str,
    send_formatted: Callable[[str], Awaitable[object]],
    send_plain: Callable[[str], Awaitable[object]] | None = None,
) -> None:
    try:
        await send_formatted(formatted_text or "")
        return
    except Exception:
        logger.warning("%s formatted message delivery failed; retrying as plain text.", platform, exc_info=True)
    fallback_sender = send_plain or send_formatted
    await fallback_sender(platform_plain_format_fallback_text(platform, plain_text or ""))


def save_messaging_attachment(
    *,
    filename: str,
    data: bytes,
    media_type: str | None = None,
    max_bytes: int = 50 * 1024 * 1024,
    principal_id: str | None = None,
) -> dict[str, str] | None:
    if not data or len(data) > max_bytes:
        return None
    if principal_id:
        try:
            from nullion.workspace_storage import workspace_storage_roots_for_principal

            upload_dir = workspace_storage_roots_for_principal(principal_id).uploads
        except Exception:
            upload_dir, _ = ensure_messaging_storage_roots()
    else:
        upload_dir, _ = ensure_messaging_storage_roots()
    safe_name = Path(filename or "upload").name or "upload"
    dest = upload_dir / safe_name
    if dest.exists():
        dest = upload_dir / f"{dest.stem}-{uuid4().hex[:8]}{dest.suffix}"
    dest.write_bytes(data)
    return {
        "name": dest.name,
        "path": str(dest),
        "media_type": media_type or guess_media_type(dest.name),
    }


__all__ = [
    "MessagingAdapterConfigurationError",
    "MessagingAdapterDependencyError",
    "MessagingIngress",
    "PlatformDelivery",
    "PlatformDeliveryResult",
    "PlatformDeliveryReceipt",
    "DeliveryContract",
    "build_platform_delivery_receipt",
    "delivery_contract_for_runtime_turn",
    "delivery_contract_for_turn",
    "delivery_receipt_transport_succeeded",
    "delivery_receipt_status",
    "ensure_messaging_storage_roots",
    "execute_platform_reply_delivery",
    "formatted_reply_chunks",
    "handle_messaging_ingress",
    "handle_messaging_ingress_result",
    "list_platform_delivery_receipts",
    "messaging_file_allowed_roots",
    "messaging_media_scratch_root",
    "messaging_upload_root",
    "messaging_delivery_receipts_path",
    "normalize_platform_code_fences",
    "platform_delivery_failure_reply",
    "platform_plain_format_fallback_text",
    "prepare_reply_for_platform_delivery",
    "principal_id_for_messaging_identity",
    "record_platform_delivery_receipt",
    "require_authorized_ingress",
    "retry_messaging_delivery_operation",
    "save_messaging_attachment",
    "save_messaging_chat_history",
    "send_with_plain_text_fallback",
    "split_reply_for_platform_delivery",
    "split_reply_for_platform",
    "text_or_attachments_expect_attachment_delivery",
]
