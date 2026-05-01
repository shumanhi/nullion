"""Per-workspace provider connection registry for Nullion."""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nullion.users import load_user_registry, resolve_messaging_user

_CONNECTIONS_PATH = Path.home() / ".nullion" / "connections.json"
_ADMIN_WORKSPACE_ID = "workspace_admin"
_NATIVE_EMAIL_PROVIDER_IDS = frozenset(
    {
        "google_workspace_provider",
        "custom_api_provider",
        "imap_smtp_provider",
    }
)


@dataclass(slots=True)
class ProviderConnection:
    connection_id: str
    workspace_id: str
    provider_id: str
    display_name: str
    provider_profile: str | None = None
    credential_ref: str | None = None
    active: bool = True
    notes: str = ""
    credential_scope: str = "workspace"
    permission_mode: str = "read"

    def to_dict(self) -> dict[str, Any]:
        return {
            "connection_id": self.connection_id,
            "workspace_id": self.workspace_id,
            "provider_id": self.provider_id,
            "display_name": self.display_name,
            "provider_profile": self.provider_profile,
            "credential_ref": self.credential_ref,
            "active": self.active,
            "notes": self.notes,
            "credential_scope": self.credential_scope,
            "permission_mode": self.permission_mode,
        }


@dataclass(slots=True)
class ConnectionRegistry:
    connections: list[ProviderConnection] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"connections": [connection.to_dict() for connection in self.connections]}


def _clean_id(value: object, *, default: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9_.:-]+", "_", str(value or "").strip()).strip("_")
    return text or default


def _clean_optional(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _clean_credential_scope(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"shared", "global", "admin_shared", "all_workspaces"}:
        return "shared"
    return "workspace"


def _clean_permission_mode(value: object) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"write", "read_write", "readwrite", "rw", "read_and_write"}:
        return "write"
    return "read"


def _coerce_connection(raw: object) -> ProviderConnection | None:
    if not isinstance(raw, dict):
        return None
    workspace_id = _clean_id(raw.get("workspace_id"), default="")
    provider_id = _clean_id(raw.get("provider_id"), default="")
    if not workspace_id or not provider_id:
        return None
    display_name = str(raw.get("display_name") or provider_id).strip() or provider_id
    return ProviderConnection(
        connection_id=_clean_id(raw.get("connection_id") or uuid.uuid4().hex, default=uuid.uuid4().hex),
        workspace_id=workspace_id,
        provider_id=provider_id,
        display_name=display_name,
        provider_profile=_clean_optional(raw.get("provider_profile")),
        credential_ref=_clean_optional(raw.get("credential_ref")),
        active=bool(raw.get("active", True)),
        notes=str(raw.get("notes") or "").strip(),
        credential_scope=_clean_credential_scope(raw.get("credential_scope")),
        permission_mode=_clean_permission_mode(raw.get("permission_mode")),
    )


def load_connection_registry(*, path: Path | str | None = None) -> ConnectionRegistry:
    registry_path = Path(path) if path else _CONNECTIONS_PATH
    raw: dict[str, Any] = {}
    if registry_path.exists():
        try:
            loaded = json.loads(registry_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                raw = loaded
        except Exception:
            raw = {}
    registry = ConnectionRegistry(
        connections=[
            connection
            for item in raw.get("connections", [])
            if (connection := _coerce_connection(item)) is not None
        ]
    )
    _append_inferred_env_connections(registry)
    return registry


def _has_connection(
    registry: ConnectionRegistry,
    *,
    workspace_id: str,
    provider_id: str,
    credential_ref: str | None = None,
) -> bool:
    return any(
        connection.workspace_id == workspace_id
        and connection.provider_id == provider_id
        and (credential_ref is None or (connection.credential_ref or "") == credential_ref)
        for connection in registry.connections
    )


def _connector_credential_ref_for_gateway(gateway: str) -> str | None:
    prefix = re.sub(r"[^A-Z0-9]+", "_", gateway.upper()).strip("_")
    if not prefix:
        return None
    for name in (f"{prefix}_API_KEY", f"{prefix}_TOKEN", f"{prefix}_SECRET_KEY"):
        if str(os.environ.get(name) or "").strip():
            return name
    return None


def _append_inferred_env_connections(registry: ConnectionRegistry) -> None:
    """Expose installer-saved connector credentials in the UI without storing secrets."""
    gateway = _clean_id(os.environ.get("NULLION_CONNECTOR_GATEWAY"), default="").lower()
    if not gateway:
        return
    provider_id = f"{gateway}_connector_provider"
    if _has_connection(
        registry,
        workspace_id=_ADMIN_WORKSPACE_ID,
        provider_id=provider_id,
    ):
        return
    registry.connections.append(
        ProviderConnection(
            connection_id=f"env_{gateway}_connector_admin",
            workspace_id=_ADMIN_WORKSPACE_ID,
            provider_id=provider_id,
            display_name=f"{gateway} connector",
            credential_ref=_connector_credential_ref_for_gateway(gateway),
            notes="Inferred from local environment.",
            credential_scope="shared",
            permission_mode=_clean_permission_mode(os.environ.get("NULLION_CONNECTOR_PERMISSION_MODE")),
        )
    )


def save_connection_registry(
    registry: ConnectionRegistry | dict[str, Any],
    *,
    path: Path | str | None = None,
) -> None:
    registry_path = Path(path) if path else _CONNECTIONS_PATH
    if isinstance(registry, dict):
        cleaned = ConnectionRegistry(
            connections=[
                connection
                for item in registry.get("connections", [])
                if (connection := _coerce_connection(item)) is not None
            ]
        )
    else:
        cleaned = registry
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(json.dumps(cleaned.to_dict(), indent=2) + "\n", encoding="utf-8")


def workspace_id_for_principal(principal_id: str | None) -> str:
    text = str(principal_id or "").strip()
    if text.startswith("workspace:"):
        return text.removeprefix("workspace:") or _ADMIN_WORKSPACE_ID
    if text.startswith("user:"):
        user_id = text.removeprefix("user:")
        registry = load_user_registry()
        for user in registry.users:
            if user.user_id == user_id:
                return user.workspace_id or _ADMIN_WORKSPACE_ID
    if ":" in text:
        channel, identity = text.split(":", 1)
        if channel in {"telegram", "slack", "discord"} and identity:
            user = resolve_messaging_user(channel, identity, None)
            return user.workspace_id or _ADMIN_WORKSPACE_ID
    return _ADMIN_WORKSPACE_ID


def multi_user_connections_active() -> bool:
    registry = load_user_registry()
    return registry.multi_user_enabled and any(user.role == "member" for user in registry.users)


def connection_for_workspace(
    workspace_id: str,
    provider_id: str,
    *,
    path: Path | str | None = None,
) -> ProviderConnection | None:
    normalized_workspace = _clean_id(workspace_id, default="")
    normalized_provider = _clean_id(provider_id, default="")
    registry = load_connection_registry(path=path)
    shared: ProviderConnection | None = None
    for connection in registry.connections:
        if (
            connection.active
            and connection.workspace_id == normalized_workspace
            and connection.provider_id == normalized_provider
        ):
            return connection
        if (
            connection.active
            and connection.workspace_id == _ADMIN_WORKSPACE_ID
            and connection.provider_id == normalized_provider
            and connection.credential_scope == "shared"
        ):
            shared = connection
    return shared


def connection_for_principal(
    principal_id: str | None,
    provider_id: str,
    *,
    path: Path | str | None = None,
) -> ProviderConnection | None:
    return connection_for_workspace(
        workspace_id_for_principal(principal_id),
        provider_id,
        path=path,
    )


def require_workspace_connection_for_principal(principal_id: str | None, provider_id: str) -> ProviderConnection | None:
    """Return a scoped connection, or allow legacy admin/global fallback.

    Single-user installs stay light and continue using the globally configured
    provider. Once multi-user is enabled with at least one member, non-admin
    principals must have their own workspace connection.
    """
    workspace_id = workspace_id_for_principal(principal_id)
    connection = connection_for_workspace(workspace_id, provider_id)
    if connection is not None:
        return connection
    if not multi_user_connections_active() or workspace_id == _ADMIN_WORKSPACE_ID:
        return None
    raise RuntimeError(f"{provider_id} is not connected for workspace {workspace_id}.")


def format_workspace_connections_for_prompt(*, principal_id: str | None = None) -> str:
    registry = load_connection_registry()
    active = [connection for connection in registry.connections if connection.active]
    if principal_id is not None:
        workspace_id = workspace_id_for_principal(principal_id)
        active = [
            connection
            for connection in active
            if connection.workspace_id in {workspace_id, _ADMIN_WORKSPACE_ID}
        ]
    if not active:
        if principal_id is not None and multi_user_connections_active():
            return (
                f"No workspace provider connections configured for workspace {workspace_id}. "
                "Provider-backed account actions require a workspace connection or shared admin connection."
            )
        return ""
    lines = [
        "Configured workspace connections are references only; they do not reveal raw secrets.",
        "When a relevant skill or provider needs an API key/token, use the listed env var reference from the process environment.",
        "Never print, log, or reveal token values.",
    ]
    for connection in active:
        parts = [
            f"workspace={connection.workspace_id}",
            f"provider={connection.provider_id}",
            f"label={connection.display_name}",
        ]
        if connection.provider_profile:
            parts.append(f"profile={connection.provider_profile}")
        if connection.credential_ref:
            parts.append(f"credential_ref={connection.credential_ref}")
        if connection.credential_scope == "shared":
            parts.append("credential_scope=shared_by_admin")
        parts.append(
            "permission_mode=read_write"
            if connection.permission_mode == "write"
            else "permission_mode=read_only"
        )
        lines.append("- " + "; ".join(parts))
    return "\n".join(lines)


def infer_email_plugin_provider(*, principal_id: str | None = None, path: Path | str | None = None) -> str | None:
    """Return the active native email provider implied by saved connections."""
    registry = load_connection_registry(path=path)
    active = [connection for connection in registry.connections if connection.active]
    if principal_id is not None:
        workspace_id = workspace_id_for_principal(principal_id)
        active = [
            connection
            for connection in active
            if connection.workspace_id in {workspace_id, _ADMIN_WORKSPACE_ID}
        ]
    for connection in active:
        if connection.provider_id in _NATIVE_EMAIL_PROVIDER_IDS:
            return connection.provider_id
    return None


__all__ = [
    "ConnectionRegistry",
    "ProviderConnection",
    "connection_for_principal",
    "connection_for_workspace",
    "format_workspace_connections_for_prompt",
    "infer_email_plugin_provider",
    "load_connection_registry",
    "multi_user_connections_active",
    "require_workspace_connection_for_principal",
    "save_connection_registry",
    "workspace_id_for_principal",
]
