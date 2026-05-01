"""Shared helpers for Slack and Discord messaging adapters."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import html
import os
from pathlib import Path
import re
from uuid import uuid4

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_root_for_principal,
    is_safe_artifact_path,
    media_candidate_paths_from_text,
    split_media_reply_attachments,
)
from nullion.approval_context import approval_trigger_flow_label
from nullion.approval_markers import split_tool_approval_marker, strip_tool_approval_marker
from nullion.attachment_format_graph import ATTACHMENT_TOKEN_EXTENSIONS, plan_attachment_format
from nullion.chat_attachments import guess_media_type
from nullion.chat_text import make_markdown_tables_chat_readable
from nullion.config import NullionSettings
from nullion.remediation import remediation_buttons_for_recommendation_code
from nullion.users import is_authorized_messaging_identity, resolve_messaging_user
from nullion.workspace_storage import workspace_storage_roots_for_principal
from nullion.task_frames import TaskFrameStatus


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

    @property
    def requires_attachment_delivery(self) -> bool:
        return self.media_directive_count > 0

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
    request_id: str | None = None
    message_id: str | None = None
    error: str | None = None
    created_at: datetime | None = None

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
            "request_id": self.request_id,
            "message_id": self.message_id,
            "error": self.error,
            "created_at": created_at.isoformat(),
        }


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
    "I couldn't attach the requested file because the artifact is unavailable. "
    "I won't mark it delivered."
)
_ATTACHMENT_UPLOAD_FAILED_REPLY = (
    "I couldn't upload the requested attachment to this platform. "
    "I won't mark it delivered."
)
_MESSAGING_ATTACHMENT_UPLOAD_ATTEMPTS = 3
_MESSAGING_ATTACHMENT_UPLOAD_RETRY_DELAY_SECONDS = 0.5
_EXTERNAL_INLINE_TAG_PATTERN = re.compile(
    r"</?(?:b|strong|i|em|u|s|strike|code|span)(?:\s+[^>]*)?>",
    re.IGNORECASE,
)


def messaging_upload_root() -> Path:
    data_dir = os.environ.get("NULLION_DATA_DIR")
    if isinstance(data_dir, str) and data_dir.strip():
        return Path(data_dir).expanduser() / "uploads"
    return Path.home() / ".nullion" / "uploads"


def messaging_media_scratch_root() -> Path:
    data_dir = os.environ.get("NULLION_DATA_DIR")
    if isinstance(data_dir, str) and data_dir.strip():
        return Path(data_dir).expanduser() / "tmp" / "media"
    return Path.home() / ".nullion" / "tmp" / "media"


def ensure_messaging_storage_roots() -> tuple[Path, Path]:
    upload_root = messaging_upload_root()
    media_scratch_root = messaging_media_scratch_root()
    upload_root.mkdir(parents=True, exist_ok=True)
    media_scratch_root.mkdir(parents=True, exist_ok=True)
    return upload_root, media_scratch_root


def messaging_delivery_receipts_path() -> Path:
    data_dir = os.environ.get("NULLION_DATA_DIR")
    root = Path(data_dir).expanduser() if isinstance(data_dir, str) and data_dir.strip() else Path.home() / ".nullion"
    return root / "delivery_receipts.jsonl"


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
    if required and not attachments:
        return "failed"
    if unavailable:
        return "partial"
    return "succeeded"


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
    return PlatformDeliveryReceipt(
        receipt_id=f"dr-{uuid4().hex}",
        channel=str(channel or "unknown"),
        target_id=None if target_id is None else str(target_id),
        status=delivery_receipt_status(delivery, transport_ok=transport_ok),
        text_delivered=bool((getattr(delivery, "text", None) or "").strip()) and transport_ok,
        attachment_count=len(tuple(getattr(delivery, "attachments", ()) or ())) if transport_ok else 0,
        attachment_required=bool(getattr(delivery, "requires_attachment_delivery", False)),
        unavailable_attachment_count=int(getattr(delivery, "unavailable_attachment_count", 0) or 0),
        request_id=request_id,
        message_id=message_id,
        error=error,
        created_at=datetime.now(UTC),
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
        proposal_id = str(getattr(record, "proposal_id", "") or "")
        proposal = getattr(record, "proposal", None)
        title = str(getattr(proposal, "title", "") or "Builder proposal")
        summary = str(getattr(proposal, "summary", "") or "")
        lines = [
            "Builder proposal pending",
            f"ID: {proposal_id}",
            f"Title: {title}",
        ]
        if summary:
            lines.append(f"Summary: {summary}")
        lines.extend([
            "",
            f"Inspect: /proposal {proposal_id}",
            f"Accept: /accept-proposal {proposal_id}",
            f"Reject: /reject-proposal {proposal_id}",
            f"Archive: /archive-proposal {proposal_id}",
        ])
        fallbacks.append("\n".join(lines))
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
            f"ID: {action_id}",
            f"Severity: {severity}",
            f"Summary: {summary}",
        ]
        remediation_actions = remediation_buttons_for_recommendation_code(str(action.get("recommendation_code") or ""))
        if remediation_actions:
            lines.extend(["", "Actions you can run from this chat:"])
            lines.extend(
                f"{index}. {label}: /doctor run {action_id} {command}"
                for index, (label, command) in enumerate(remediation_actions, start=1)
            )
        lines.extend([
            "",
            f"Inspect: /doctor {action_id}",
            f"Mark resolved: /doctor complete {action_id}",
            f"Dismiss: /doctor dismiss {action_id}",
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


def handle_messaging_ingress_result(service, ingress: MessagingIngress):
    from nullion.messaging_turn_graph import run_messaging_turn_graph

    return run_messaging_turn_graph(service, ingress)


def handle_messaging_ingress(service, ingress: MessagingIngress) -> str | None:
    return handle_messaging_ingress_result(service, ingress).reply


def principal_id_for_messaging_identity(channel: str, user_id: object, settings: NullionSettings | None = None) -> str:
    user = resolve_messaging_user(channel, str(user_id or "").strip(), settings)
    return f"user:{user.user_id}" if user.role == "member" else "telegram_chat"


def _messaging_output_roots(*, principal_id: str | None) -> tuple[Path, ...]:
    roots = [artifact_root_for_principal(principal_id)]
    try:
        workspace_roots = workspace_storage_roots_for_principal(principal_id)
        roots.extend([workspace_roots.artifacts, workspace_roots.media])
    except Exception:
        pass
    return tuple(dict.fromkeys(root.resolve() for root in roots))


def _is_deliverable_messaging_attachment(path: Path, *, principal_id: str | None) -> bool:
    for root in _messaging_output_roots(principal_id=principal_id):
        if artifact_descriptor_for_path(path, artifact_root=root) is not None:
            return True
    return is_safe_artifact_path(path)


def sanitize_external_inline_markup(text: str) -> str:
    """Strip lightweight HTML emphasis tags commonly copied from RSS/search results."""
    unescaped = html.unescape(text)
    if not _EXTERNAL_INLINE_TAG_PATTERN.search(unescaped):
        return text
    return _EXTERNAL_INLINE_TAG_PATTERN.sub("", unescaped)


def split_reply_for_platform_delivery(
    reply: str | None,
    *,
    principal_id: str | None = None,
) -> tuple[str | None, tuple[Path, ...]]:
    if reply is None:
        return None, ()
    return split_media_reply_attachments(
        str(reply),
        is_safe_attachment_path=lambda path: _is_deliverable_messaging_attachment(path, principal_id=principal_id),
    )


_PLAIN_ABSOLUTE_PATH_RE = re.compile(r"(?<![\w./-])(/[^\s`'\"<>|]+)")


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
    if requires_attachment_delivery:
        return DeliveryContract.attachment_required(
            source="task_contract",
            allow_plain_paths=True,
            required_attachment_extensions=required_extensions,
        )
    if artifact_paths:
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
    if inbound_attachments and _plain_candidate_paths_from_text(str(reply or "")):
        return DeliveryContract.attachment_required(
            source="uploaded_file_output",
            allow_plain_paths=True,
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
    if direct_extension in set(ATTACHMENT_TOKEN_EXTENSIONS.values()):
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
    if planned_extension:
        extensions.append(planned_extension.lower())
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


def _plain_candidate_paths_from_text(text: str) -> list[Path]:
    paths: list[Path] = []
    for match in _PLAIN_ABSOLUTE_PATH_RE.finditer(str(text or "")):
        raw = match.group(1).rstrip(").,;:")
        if raw:
            paths.append(Path(raw))
    return list(dict.fromkeys(paths))


def _caption_without_attached_paths(text: str, attachment_paths: tuple[Path, ...]) -> str | None:
    path_texts = {str(path) for path in attachment_paths}
    caption_lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = raw_line
        for path_text in path_texts:
            line = line.replace(path_text, "")
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.rstrip(":").strip().lower() in {
            "attachment/artifact link",
            "artifact link",
            "attachment link",
            "file",
            "file path",
            "path",
        }:
            continue
        caption_lines.append(stripped)
    caption = "\n".join(caption_lines).strip()
    return caption or "Attached the requested file."


def prepare_reply_for_platform_delivery(
    reply: str | None,
    *,
    principal_id: str | None = None,
    allow_attachments: bool | None = None,
    delivery_contract: DeliveryContract | None = None,
) -> PlatformDelivery:
    if reply is None:
        return PlatformDelivery(text=None, attachments=())
    text = make_markdown_tables_chat_readable(sanitize_external_inline_markup(str(reply)))
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
    media_candidates = media_candidate_paths_from_text(text)
    caption, attachments = split_reply_for_platform_delivery(text, principal_id=principal_id)
    if media_candidates and not delivery_contract.allow_attachment_delivery:
        return PlatformDelivery(text=caption if caption else None, attachments=(), media_directive_count=0)
    if (
        not media_candidates
        and delivery_contract.allow_attachment_delivery
        and delivery_contract.allow_plain_path_attachment_delivery
    ):
        plain_candidates = _plain_candidate_paths_from_text(text)
        plain_attachments = tuple(
            path
            for path in plain_candidates
            if _is_deliverable_messaging_attachment(path, principal_id=principal_id)
        )
        plain_attachments = _attachments_matching_contract(plain_attachments, delivery_contract)
        if plain_attachments:
            return PlatformDelivery(
                text=_caption_without_attached_paths(text, plain_attachments),
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
        return PlatformDelivery(text=text if text else None, attachments=())
    attachments = _attachments_matching_contract(attachments, delivery_contract)
    unavailable_count = max(0, len(media_candidates) - len(attachments))
    if not attachments:
        return PlatformDelivery(
            text=_ATTACHMENT_UNAVAILABLE_REPLY,
            attachments=(),
            media_directive_count=len(media_candidates),
            unavailable_attachment_count=unavailable_count or len(media_candidates),
        )
    delivery_text = caption
    if unavailable_count:
        suffix = (
            f"I attached {len(attachments)} file{'s' if len(attachments) != 1 else ''}, "
            f"but {unavailable_count} attachment{'s were' if unavailable_count != 1 else ' was'} unavailable."
        )
        delivery_text = f"{caption}\n\n{suffix}" if caption else suffix
    return PlatformDelivery(
        text=delivery_text,
        attachments=attachments,
        media_directive_count=len(media_candidates),
        unavailable_attachment_count=unavailable_count,
    )


def platform_delivery_failure_reply(_delivery: PlatformDelivery | None = None) -> str:
    return _ATTACHMENT_UPLOAD_FAILED_REPLY


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
            await asyncio.sleep(retry_delay_seconds * (attempt + 1))
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
    "PlatformDeliveryReceipt",
    "DeliveryContract",
    "build_platform_delivery_receipt",
    "delivery_contract_for_runtime_turn",
    "delivery_contract_for_turn",
    "delivery_receipt_status",
    "ensure_messaging_storage_roots",
    "handle_messaging_ingress",
    "handle_messaging_ingress_result",
    "list_platform_delivery_receipts",
    "messaging_file_allowed_roots",
    "messaging_media_scratch_root",
    "messaging_upload_root",
    "messaging_delivery_receipts_path",
    "platform_delivery_failure_reply",
    "prepare_reply_for_platform_delivery",
    "principal_id_for_messaging_identity",
    "record_platform_delivery_receipt",
    "require_authorized_ingress",
    "retry_messaging_delivery_operation",
    "save_messaging_attachment",
    "save_messaging_chat_history",
    "split_reply_for_platform_delivery",
    "split_reply_for_platform",
    "text_or_attachments_expect_attachment_delivery",
]
