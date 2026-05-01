"""Local user/workspace registry for Nullion.

The app stays single-admin by default.  When people/workspaces are enabled,
additional Telegram identities can be mapped to member workspaces without
granting them admin control over global settings.
"""
from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nullion.config import NullionSettings

_USERS_PATH = Path.home() / ".nullion" / "users.json"
_ADMIN_ID = "admin"
_ADMIN_WORKSPACE_ID = "workspace_admin"


@dataclass(slots=True)
class NullionUser:
    user_id: str
    display_name: str
    role: str = "member"
    workspace_id: str = ""
    telegram_chat_id: str | None = None
    messaging_channel: str = "telegram"
    messaging_user_id: str | None = None
    active: bool = True
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "display_name": self.display_name,
            "role": self.role,
            "workspace_id": self.workspace_id or _workspace_id_for_name(self.display_name),
            "telegram_chat_id": self.telegram_chat_id,
            "messaging_channel": self.messaging_channel,
            "messaging_user_id": self.messaging_user_id or self.telegram_chat_id,
            "active": self.active,
            "notes": self.notes,
        }


@dataclass(slots=True)
class UserRegistry:
    multi_user_enabled: bool = False
    users: list[NullionUser] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "multi_user_enabled": self.multi_user_enabled,
            "users": [user.to_dict() for user in self.users],
        }


@dataclass(frozen=True, slots=True)
class MessagingDeliveryTarget:
    channel: str
    target_id: str
    principal_id: str
    workspace_id: str
    user_id: str
    display_name: str


def _workspace_id_for_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "member"
    return f"workspace_{slug[:40]}"


def _normalize_chat_id(chat_id: str | int | None) -> str | None:
    if chat_id is None:
        return None
    text = str(chat_id).strip()
    return text or None


def _normalize_channel(value: object) -> str:
    channel = re.sub(r"[^a-z0-9_]+", "_", str(value or "telegram").strip().lower()).strip("_")
    return channel or "telegram"


def _admin_user(settings: NullionSettings | None = None) -> NullionUser:
    chat_id = None if settings is None else _normalize_chat_id(settings.telegram.operator_chat_id)
    return NullionUser(
        user_id=_ADMIN_ID,
        display_name="Admin",
        role="admin",
        workspace_id=_ADMIN_WORKSPACE_ID,
        telegram_chat_id=chat_id,
        messaging_channel="telegram",
        messaging_user_id=chat_id,
        active=True,
    )


def _coerce_user(raw: object) -> NullionUser | None:
    if not isinstance(raw, dict):
        return None
    display_name = str(raw.get("display_name") or raw.get("name") or "Member").strip() or "Member"
    role = str(raw.get("role") or "member").strip().lower()
    if role not in {"admin", "member"}:
        role = "member"
    user_id = str(raw.get("user_id") or uuid.uuid4().hex).strip() or uuid.uuid4().hex
    workspace_id = str(raw.get("workspace_id") or _workspace_id_for_name(display_name)).strip()
    if role == "admin":
        user_id = _ADMIN_ID
        workspace_id = _ADMIN_WORKSPACE_ID
    messaging_channel = _normalize_channel(raw.get("messaging_channel") or raw.get("channel") or "telegram")
    messaging_user_id = _normalize_chat_id(raw.get("messaging_user_id") or raw.get("external_user_id"))
    telegram_chat_id = _normalize_chat_id(raw.get("telegram_chat_id"))
    if messaging_channel == "telegram" and telegram_chat_id is None:
        telegram_chat_id = messaging_user_id
    if messaging_user_id is None:
        messaging_user_id = telegram_chat_id
    return NullionUser(
        user_id=user_id,
        display_name=display_name,
        role=role,
        workspace_id=workspace_id,
        telegram_chat_id=telegram_chat_id,
        messaging_channel=messaging_channel,
        messaging_user_id=messaging_user_id,
        active=bool(raw.get("active", True)),
        notes=str(raw.get("notes") or "").strip(),
    )


def load_user_registry(
    *,
    path: Path | str | None = None,
    settings: NullionSettings | None = None,
) -> UserRegistry:
    registry_path = Path(path) if path else _USERS_PATH
    raw: dict[str, Any] = {}
    if registry_path.exists():
        try:
            loaded = json.loads(registry_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                raw = loaded
        except Exception:
            raw = {}

    users = [_admin_user(settings)]
    seen = {_ADMIN_ID}
    for item in raw.get("users", []):
        user = _coerce_user(item)
        if user is None:
            continue
        if user.role == "admin":
            admin = users[0]
            admin.display_name = user.display_name or admin.display_name
            admin.notes = user.notes
            admin.active = True
            admin.telegram_chat_id = _normalize_chat_id(user.telegram_chat_id) or admin.telegram_chat_id
            continue
        if user.user_id in seen:
            user.user_id = uuid.uuid4().hex
        seen.add(user.user_id)
        users.append(user)

    return UserRegistry(
        multi_user_enabled=bool(raw.get("multi_user_enabled", False)),
        users=users,
    )


def save_user_registry(
    registry: UserRegistry | dict[str, Any],
    *,
    path: Path | str | None = None,
    settings: NullionSettings | None = None,
) -> None:
    registry_path = Path(path) if path else _USERS_PATH
    if isinstance(registry, dict):
        users = [_coerce_user(item) for item in registry.get("users", [])]
        cleaned = UserRegistry(
            multi_user_enabled=bool(registry.get("multi_user_enabled", False)),
            users=[user for user in users if user is not None],
        )
    else:
        cleaned = registry

    # Always persist exactly one admin row, synchronized with Telegram settings.
    admin = _admin_user(settings)
    members = [user for user in cleaned.users if user.role != "admin"]
    cleaned = UserRegistry(
        multi_user_enabled=cleaned.multi_user_enabled,
        users=[admin, *members],
    )
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(json.dumps(cleaned.to_dict(), indent=2) + "\n", encoding="utf-8")


def is_authorized_telegram_chat(chat_id: str | int | None, settings: NullionSettings | None) -> bool:
    """Return whether a Telegram chat may talk to this Nullion instance."""
    if settings is None:
        return True
    chat_id_text = _normalize_chat_id(chat_id)
    configured_admin_chat_id = _normalize_chat_id(settings.telegram.operator_chat_id)
    if configured_admin_chat_id is None:
        return True
    if chat_id_text == configured_admin_chat_id:
        return True

    registry = load_user_registry(settings=settings)
    if not registry.multi_user_enabled:
        return False
    return any(
        user.active
        and user.role == "member"
        and _normalize_chat_id(user.telegram_chat_id) == chat_id_text
        for user in registry.users
    )


def is_authorized_messaging_identity(
    channel: str,
    identity: str | int | None,
    settings: NullionSettings | None,
) -> bool:
    """Return whether a non-web messaging identity may talk to Nullion."""
    channel_name = _normalize_channel(channel)
    identity_text = _normalize_chat_id(identity)
    if channel_name == "telegram":
        return is_authorized_telegram_chat(identity_text, settings)
    if settings is None or identity_text is None:
        return False
    if channel_name == "slack" and _normalize_chat_id(settings.slack.operator_user_id) == identity_text:
        return True

    registry = load_user_registry(settings=settings)
    if not registry.multi_user_enabled:
        return False
    return any(
        user.active
        and user.role == "member"
        and _normalize_channel(user.messaging_channel) == channel_name
        and _normalize_chat_id(user.messaging_user_id) == identity_text
        for user in registry.users
    )


def resolve_telegram_user(chat_id: str | int | None, settings: NullionSettings | None) -> NullionUser:
    """Map a Telegram chat to a user/workspace identity."""
    chat_id_text = _normalize_chat_id(chat_id)
    registry = load_user_registry(settings=settings)
    for user in registry.users:
        if user.active and _normalize_chat_id(user.telegram_chat_id) == chat_id_text:
            return user
    return _admin_user(settings)


def resolve_messaging_user(
    channel: str,
    identity: str | int | None,
    settings: NullionSettings | None,
) -> NullionUser:
    """Map a messaging sender to a user/workspace identity."""
    channel_name = _normalize_channel(channel)
    identity_text = _normalize_chat_id(identity)
    if channel_name == "telegram":
        return resolve_telegram_user(identity_text, settings)
    if (
        channel_name == "slack"
        and settings is not None
        and _normalize_chat_id(settings.slack.operator_user_id) == identity_text
    ):
        return _admin_user(settings)

    registry = load_user_registry(settings=settings)
    for user in registry.users:
        if (
            user.active
            and _normalize_channel(user.messaging_channel) == channel_name
            and _normalize_chat_id(user.messaging_user_id) == identity_text
        ):
            return user
    return _admin_user(settings)


def workspace_users(
    workspace_id: str | None,
    *,
    settings: NullionSettings | None = None,
) -> tuple[NullionUser, ...]:
    requested_workspace = str(workspace_id or _ADMIN_WORKSPACE_ID).strip() or _ADMIN_WORKSPACE_ID
    registry = load_user_registry(settings=settings)
    return tuple(
        user
        for user in registry.users
        if user.active and (user.workspace_id or _workspace_id_for_name(user.display_name)) == requested_workspace
    )


def workspace_id_for_user(user: NullionUser) -> str:
    return user.workspace_id or _workspace_id_for_name(user.display_name)


def messaging_delivery_targets_for_workspace(
    workspace_id: str | None,
    *,
    settings: NullionSettings | None = None,
) -> tuple[MessagingDeliveryTarget, ...]:
    """Return structured chat delivery targets for a workspace.

    This intentionally relies on the user registry and platform settings rather
    than LLM text or prompt content.  A workspace can have more than one active
    user/adapter identity, and notifications should fan out to those identities.
    """
    requested_workspace = str(workspace_id or _ADMIN_WORKSPACE_ID).strip() or _ADMIN_WORKSPACE_ID
    targets: list[MessagingDeliveryTarget] = []
    seen: set[tuple[str, str]] = set()
    for user in workspace_users(requested_workspace, settings=settings):
        principal_id = f"user:{user.user_id}" if user.role == "member" else "telegram_chat"
        channel = _normalize_channel(user.messaging_channel)
        target_id = _normalize_chat_id(user.messaging_user_id)
        if channel == "telegram":
            target_id = _normalize_chat_id(user.telegram_chat_id) or target_id
        if target_id:
            key = (channel, target_id)
            if key not in seen:
                seen.add(key)
                targets.append(
                    MessagingDeliveryTarget(
                        channel=channel,
                        target_id=target_id,
                        principal_id=principal_id,
                        workspace_id=requested_workspace,
                        user_id=user.user_id,
                        display_name=user.display_name,
                    )
                )
    if requested_workspace == _ADMIN_WORKSPACE_ID and settings is not None:
        configured_targets = (
            ("telegram", _normalize_chat_id(settings.telegram.operator_chat_id), "telegram_chat"),
            ("slack", _normalize_chat_id(settings.slack.operator_user_id), "telegram_chat"),
            (
                "discord",
                _normalize_chat_id(os.environ.get("NULLION_DISCORD_OPERATOR_CHANNEL_ID")),
                "telegram_chat",
            ),
        )
        for channel, target_id, principal_id in configured_targets:
            if not target_id:
                continue
            key = (channel, target_id)
            if key in seen:
                continue
            seen.add(key)
            targets.append(
                MessagingDeliveryTarget(
                    channel=channel,
                    target_id=target_id,
                    principal_id=principal_id,
                    workspace_id=requested_workspace,
                    user_id=_ADMIN_ID,
                    display_name="Admin",
                )
            )
    return tuple(targets)


def build_messaging_user_context_prompt(
    channel: str,
    identity: str | int | None,
    settings: NullionSettings | None,
) -> str | None:
    """Build the prompt snippet for the current messaging user's local profile."""
    user = resolve_messaging_user(channel, identity, settings)
    parts = [
        f"Name: {user.display_name}",
        f"Role: {user.role}",
        f"Workspace: {user.workspace_id or _workspace_id_for_name(user.display_name)}",
        f"Messaging app: {user.messaging_channel}",
    ]
    messaging_id = user.messaging_user_id or user.telegram_chat_id
    if messaging_id:
        parts.append(f"Messaging user ID: {messaging_id}")
    if user.notes:
        parts.append(f"Notes: {user.notes}")
    if user.role == "member":
        parts.append("Treat this person as the current end user for this conversation.")
    return "Current messaging user:\n" + "\n".join(parts)


__all__ = [
    "build_messaging_user_context_prompt",
    "MessagingDeliveryTarget",
    "NullionUser",
    "UserRegistry",
    "is_authorized_messaging_identity",
    "is_authorized_telegram_chat",
    "load_user_registry",
    "messaging_delivery_targets_for_workspace",
    "resolve_messaging_user",
    "resolve_telegram_user",
    "save_user_registry",
    "workspace_id_for_user",
    "workspace_users",
]
