"""Per-workspace provider connection registry for Nullion."""

from __future__ import annotations

import json
import os
import re
import unicodedata
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nullion.users import load_user_registry, resolve_messaging_user

def _nullion_home() -> Path:
    configured = str(os.environ.get("NULLION_HOME") or "").strip()
    return Path(configured).expanduser() if configured else Path.home() / ".nullion"


_CONNECTIONS_PATH = _nullion_home() / "connections.json"
_ADMIN_WORKSPACE_ID = "workspace_admin"
_NATIVE_EMAIL_PROVIDER_IDS = frozenset(
    {
        "google_workspace_provider",
        "custom_api_provider",
        "imap_smtp_provider",
    }
)


def normalize_connection_text(value: object, *, strip: bool = True) -> str:
    """Normalize copied connection fields before using them in protocol commands."""
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = "".join(
        " " if char in "\t\r\n\f\v"
        else " " if unicodedata.category(char).startswith("Z")
        else "" if unicodedata.category(char) in {"Cf", "Cc"}
        else char
        for char in text
    )
    text = re.sub(r"[\t\r\n\f\v]+", " ", text)
    return text.strip() if strip else text


def _provider_id_looks_external_connector(provider_id: object) -> bool:
    normalized = str(provider_id or "").strip().lower()
    return normalized.startswith("skill_pack_connector_") or normalized.endswith("_connector_provider")


def _structured_tools_for_connection(connection: "ProviderConnection") -> tuple[str, ...]:
    if not _provider_id_looks_external_connector(connection.provider_id):
        return ()
    tools = ["connector_request", "email_search", "email_read", "email_attachment_read", "calendar_list"]
    if connection.permission_mode == "write":
        tools.extend(["email_send", "calendar_create", "calendar_update", "calendar_respond", "calendar_delete"])
    return tuple(tools)


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
    text = re.sub(r"[^a-zA-Z0-9_.:-]+", "_", normalize_connection_text(value)).strip("_")
    return text or default


def _clean_optional(value: object) -> str | None:
    text = normalize_connection_text(value)
    return text or None


def _clean_credential_scope(value: object) -> str:
    text = normalize_connection_text(value).lower()
    if text in {"shared", "global", "admin_shared", "all_workspaces"}:
        return "shared"
    return "workspace"


def _clean_permission_mode(value: object) -> str:
    text = normalize_connection_text(value).lower().replace("-", "_")
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
    if path is None:
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
    names = [f"{prefix}_API_KEY", f"{prefix}_TOKEN", f"{prefix}_SECRET_KEY"]
    if prefix == "CUSTOM":
        names.insert(0, "NULLION_CUSTOM_CONNECTOR_TOKEN")
    for name in names:
        if str(os.environ.get(name) or "").strip():
            return name
    return None


def _connector_profile_for_gateway(gateway: str) -> str | None:
    prefix = re.sub(r"[^A-Z0-9]+", "_", gateway.upper()).strip("_")
    names = [f"{prefix}_BASE_URL"] if prefix else []
    if prefix == "CUSTOM":
        names.insert(0, "NULLION_CUSTOM_CONNECTOR_BASE_URL")
    for name in names:
        value = str(os.environ.get(name) or "").strip()
        if value.lower().startswith(("http://", "https://")):
            return value
    return None


def _external_provider_env_prefix(provider_id: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", str(provider_id or "").upper()).strip("_")


def _external_provider_credential_candidates(connection: "ProviderConnection") -> tuple[str, ...]:
    candidates: list[str] = []
    if connection.credential_ref:
        candidates.append(connection.credential_ref.removeprefix("env:"))
    prefix = _external_provider_env_prefix(connection.provider_id)
    if prefix:
        candidates.extend((f"{prefix}_API_KEY", f"{prefix}_TOKEN", f"{prefix}_SECRET_KEY"))
        for suffix in ("_CONNECTOR_PROVIDER", "_CONNECTOR"):
            if prefix.endswith(suffix):
                gateway_prefix = prefix.removesuffix(suffix).strip("_")
                if gateway_prefix:
                    candidates.extend(
                        (
                            f"{gateway_prefix}_API_KEY",
                            f"{gateway_prefix}_TOKEN",
                            f"{gateway_prefix}_SECRET_KEY",
                        )
                    )
        if prefix.startswith("SKILL_PACK_CONNECTOR_"):
            gateway_prefix = prefix.removeprefix("SKILL_PACK_CONNECTOR_").strip("_")
            if gateway_prefix:
                candidates.extend(
                    (
                        f"{gateway_prefix}_API_KEY",
                        f"{gateway_prefix}_TOKEN",
                        f"{gateway_prefix}_SECRET_KEY",
                    )
                )
    return tuple(dict.fromkeys(candidate for candidate in candidates if candidate))


def _imap_env_prefix(connection: "ProviderConnection") -> str:
    ref = connection.credential_ref or connection.provider_profile
    text = re.sub(r"[^A-Z0-9_]+", "_", normalize_connection_text(ref).upper()).strip("_") or "ACCOUNT"
    return f"NULLION_IMAP_{text}"


def connection_missing_runtime_credentials(connection: "ProviderConnection") -> tuple[str, ...]:
    """Return env references required before this saved connection is usable."""
    provider_id = str(connection.provider_id or "").strip()
    if provider_id == "custom_api_provider":
        missing: list[str] = []
        profile = normalize_connection_text(connection.provider_profile)
        if not profile.startswith(("http://", "https://")) and not os.environ.get("NULLION_CUSTOM_API_BASE_URL", "").strip():
            missing.append("NULLION_CUSTOM_API_BASE_URL")
        token_candidates = []
        if connection.credential_ref:
            token_candidates.append(connection.credential_ref.removeprefix("env:"))
        token_candidates.append("NULLION_CUSTOM_API_TOKEN")
        if not any(os.environ.get(candidate, "").strip() for candidate in token_candidates):
            missing.extend(token_candidates)
        return tuple(dict.fromkeys(missing))
    if provider_id == "imap_smtp_provider":
        prefix = _imap_env_prefix(connection)
        return tuple(
            f"{prefix}_{name}"
            for name in ("HOST", "USERNAME", "PASSWORD")
            if not os.environ.get(f"{prefix}_{name}", "").strip()
        )
    if _provider_id_looks_external_connector(provider_id):
        candidates = _external_provider_credential_candidates(connection)
        if candidates and not any(os.environ.get(candidate, "").strip() for candidate in candidates):
            return candidates
    return ()


def connection_has_runtime_credentials(connection: "ProviderConnection") -> bool:
    return not connection_missing_runtime_credentials(connection)


def _skill_pack_provider_id(pack_id: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(pack_id or "").strip().lower()).strip("_") or "custom_skill"
    return f"skill_pack_connector_{slug}"


def _skill_pack_required_env_vars(skill_file: Path) -> tuple[str, ...]:
    try:
        text = skill_file.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ()
    if not text.startswith("---"):
        return ()
    end = text.find("\n---", 3)
    if end < 0:
        return ()
    header = text[3:end]
    env_vars: list[str] = []
    in_env_block = False
    for raw_line in header.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if stripped == "env:":
            in_env_block = True
            continue
        if not in_env_block:
            continue
        if stripped.startswith("- "):
            name = stripped.removeprefix("- ").strip().strip("'\"")
            if re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
                env_vars.append(name)
            continue
        if stripped and not line.startswith((" ", "\t")):
            in_env_block = False
    return tuple(dict.fromkeys(env_vars))


def _append_inferred_skill_pack_connections(registry: ConnectionRegistry) -> None:
    try:
        from nullion.skill_pack_installer import list_installed_skill_packs
    except Exception:
        return
    try:
        packs = list_installed_skill_packs()
    except Exception:
        return
    for pack in packs:
        pack_id = str(getattr(pack, "pack_id", "") or "").strip()
        pack_path = Path(str(getattr(pack, "path", "") or ""))
        if not pack_id or not pack_path.exists():
            continue
        required_envs: list[str] = []
        for skill_file in sorted(pack_path.rglob("SKILL.md")):
            required_envs.extend(_skill_pack_required_env_vars(skill_file))
        credential_ref = next((name for name in dict.fromkeys(required_envs) if os.environ.get(name, "").strip()), None)
        if not credential_ref:
            continue
        provider_id = _skill_pack_provider_id(pack_id)
        if _has_connection(registry, workspace_id=_ADMIN_WORKSPACE_ID, provider_id=provider_id):
            continue
        if any(
            connection.active
            and _provider_id_looks_external_connector(connection.provider_id)
            and (connection.credential_ref or "").removeprefix("env:") == credential_ref
            for connection in registry.connections
        ):
            continue
        registry.connections.append(
            ProviderConnection(
                connection_id=f"env_{provider_id}_admin",
                workspace_id=_ADMIN_WORKSPACE_ID,
                provider_id=provider_id,
                display_name=f"{pack_id} connector",
                credential_ref=credential_ref,
                notes="Inferred from installed skill pack environment requirements.",
                credential_scope="shared",
                permission_mode=_clean_permission_mode(
                    os.environ.get("NULLION_CONNECTOR_PERMISSION_MODE") or "write"
                ),
            )
        )


def _append_inferred_env_connections(registry: ConnectionRegistry) -> None:
    """Expose installer-saved connector credentials in the UI without storing secrets."""
    gateway = _clean_id(os.environ.get("NULLION_CONNECTOR_GATEWAY"), default="").lower()
    if not gateway:
        _append_inferred_skill_pack_connections(registry)
        return
    provider_id = f"{gateway}_connector_provider"
    if _has_connection(
        registry,
        workspace_id=_ADMIN_WORKSPACE_ID,
        provider_id=provider_id,
    ):
        _append_inferred_skill_pack_connections(registry)
        return
    registry.connections.append(
        ProviderConnection(
            connection_id=f"env_{gateway}_connector_admin",
            workspace_id=_ADMIN_WORKSPACE_ID,
            provider_id=provider_id,
            display_name=f"{gateway} connector",
            provider_profile=_connector_profile_for_gateway(gateway),
            credential_ref=_connector_credential_ref_for_gateway(gateway),
            notes="Inferred from local environment.",
            credential_scope="shared",
            permission_mode=_clean_permission_mode(os.environ.get("NULLION_CONNECTOR_PERMISSION_MODE")),
        )
    )
    _append_inferred_skill_pack_connections(registry)


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


def principal_has_admin_access(principal_id: str | None) -> bool:
    """Return whether a structured principal is the workspace admin/operator."""

    text = str(principal_id or "").strip()
    if text in {"operator", "telegram_chat", "web:operator", "web:admin"}:
        return True
    if text.startswith("workspace:"):
        return (text.removeprefix("workspace:") or _ADMIN_WORKSPACE_ID) == _ADMIN_WORKSPACE_ID
    registry = load_user_registry()
    if text.startswith("user:"):
        user_id = text.removeprefix("user:").strip()
        return any(user.active and user.user_id == user_id and user.role == "admin" for user in registry.users)
    if ":" in text:
        channel, identity = text.split(":", 1)
        if channel in {"telegram", "slack", "discord"} and identity:
            for user in registry.users:
                if not user.active or user.role != "admin":
                    continue
                if channel == "telegram" and str(user.telegram_chat_id or "").strip() == identity:
                    return True
                if (
                    str(user.messaging_channel or "").strip().lower() == channel
                    and str(user.messaging_user_id or "").strip() == identity
                ):
                    return True
    return False


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


def default_email_connector_provider_id(
    principal_id: str | None = None,
    *,
    path: Path | str | None = None,
    require_write: bool = False,
) -> str | None:
    """Return the external connector most likely to handle email."""
    fallback_provider_id = ""
    for connection in load_connection_registry(path=path).connections:
        provider_id = str(connection.provider_id or "").strip()
        if not provider_id or not connection.active:
            continue
        if not _provider_id_looks_external_connector(provider_id):
            continue
        scoped_connection = connection_for_principal(principal_id, provider_id, path=path)
        if scoped_connection is None:
            continue
        if require_write and scoped_connection.permission_mode != "write":
            continue
        if not connection_has_runtime_credentials(scoped_connection):
            continue
        lowered_provider = provider_id.lower()
        display_name = str(scoped_connection.display_name or "").lower()
        if "mail" in lowered_provider or "gmail" in lowered_provider or "mail" in display_name:
            return provider_id
        if not fallback_provider_id:
            fallback_provider_id = provider_id
    return fallback_provider_id or None


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


def format_workspace_connections_for_prompt(
    *,
    principal_id: str | None = None,
    include_external_connectors: bool = True,
) -> str:
    registry = load_connection_registry()
    active = [connection for connection in registry.connections if connection.active]
    if principal_id is not None:
        workspace_id = workspace_id_for_principal(principal_id)
        active = [
            connection
            for connection in active
            if connection.workspace_id in {workspace_id, _ADMIN_WORKSPACE_ID}
        ]
    if not include_external_connectors:
        active = [
            connection
            for connection in active
            if not _provider_id_looks_external_connector(connection.provider_id)
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
        missing_credentials = connection_missing_runtime_credentials(connection)
        parts = [
            f"workspace={connection.workspace_id}",
            f"provider={connection.provider_id}",
            f"label={connection.display_name}",
        ]
        if connection.provider_profile:
            parts.append(f"profile={connection.provider_profile}")
        if connection.credential_ref:
            parts.append(f"credential_ref={connection.credential_ref}")
        if missing_credentials:
            parts.append("credential_status=missing_env:" + ",".join(missing_credentials))
        if connection.credential_scope == "shared":
            parts.append("credential_scope=shared_by_admin")
        parts.append(
            "permission_mode=read_write"
            if connection.permission_mode == "write"
            else "permission_mode=read_only"
        )
        structured_tools = _structured_tools_for_connection(connection)
        if structured_tools:
            parts.append("typed_tools=" + ",".join(structured_tools))
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
        if connection.provider_id in _NATIVE_EMAIL_PROVIDER_IDS and (path is not None or connection_has_runtime_credentials(connection)):
            return connection.provider_id
    return None


__all__ = [
    "ConnectionRegistry",
    "ProviderConnection",
    "connection_for_principal",
    "connection_for_workspace",
    "connection_has_runtime_credentials",
    "connection_missing_runtime_credentials",
    "format_workspace_connections_for_prompt",
    "infer_email_plugin_provider",
    "load_connection_registry",
    "multi_user_connections_active",
    "principal_has_admin_access",
    "require_workspace_connection_for_principal",
    "save_connection_registry",
    "workspace_id_for_principal",
]
