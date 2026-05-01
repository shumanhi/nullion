"""Encrypted credential persistence backed by the Nullion runtime database."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from cryptography.fernet import Fernet, InvalidToken

from nullion.secure_storage import load_or_create_fernet_key


DEFAULT_NULLION_HOME = Path.home() / ".nullion"
DEFAULT_CREDENTIALS_DB_PATH = DEFAULT_NULLION_HOME / "runtime.db"
DEFAULT_CREDENTIALS_KEY_PATH = DEFAULT_NULLION_HOME / "credentials.key"
KEYCHAIN_SERVICE = "Nullion Credentials Key"
KEYCHAIN_ACCOUNT = "credentials"
_ROW_ID = "default"
_DDL = """
CREATE TABLE IF NOT EXISTS encrypted_credentials (
    credential_id TEXT PRIMARY KEY,
    payload       TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
"""

_PROVIDER_KEY_ENV_NAMES: dict[str, tuple[str, ...]] = {
    "openai": ("NULLION_OPENAI_API_KEY", "OPENAI_API_KEY"),
    "anthropic": ("NULLION_ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"),
    "openrouter": ("NULLION_OPENROUTER_API_KEY", "OPENROUTER_API_KEY"),
    "gemini": ("NULLION_GEMINI_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY"),
    "groq": ("NULLION_GROQ_API_KEY", "GROQ_API_KEY"),
    "mistral": ("NULLION_MISTRAL_API_KEY", "MISTRAL_API_KEY"),
    "deepseek": ("NULLION_DEEPSEEK_API_KEY", "DEEPSEEK_API_KEY"),
    "xai": ("NULLION_XAI_API_KEY", "XAI_API_KEY"),
    "together": ("NULLION_TOGETHER_API_KEY", "TOGETHER_API_KEY"),
    "ollama": ("NULLION_OLLAMA_API_KEY", "OLLAMA_API_KEY"),
}


def _credentials_db_path() -> Path:
    explicit = os.environ.get("NULLION_CREDENTIALS_DB_PATH") or os.environ.get("NULLION_CHECKPOINT_PATH")
    if explicit:
        return Path(explicit).expanduser()
    return DEFAULT_CREDENTIALS_DB_PATH


def _credentials_key_path(db_path: Path) -> Path:
    explicit = os.environ.get("NULLION_CREDENTIALS_KEY_PATH")
    if explicit:
        return Path(explicit).expanduser()
    if db_path.parent != DEFAULT_NULLION_HOME:
        return db_path.with_name("credentials.key")
    return DEFAULT_CREDENTIALS_KEY_PATH


def _credential_key_storage(db_path: Path) -> str | None:
    raw = os.environ.get("NULLION_KEY_STORAGE")
    if raw:
        return raw
    if db_path.parent != DEFAULT_NULLION_HOME:
        return None
    env_path = DEFAULT_NULLION_HOME / ".env"
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() == "NULLION_KEY_STORAGE":
                return value.strip().strip('"').strip("'")
    except OSError:
        return None
    return None


def _cipher_for(db_path: Path) -> Fernet:
    key = load_or_create_fernet_key(
        _credentials_key_path(db_path),
        storage=_credential_key_storage(db_path),
        keychain_service=KEYCHAIN_SERVICE,
        keychain_account=KEYCHAIN_ACCOUNT,
    )
    return Fernet(key)


def _first_env_value(env: Mapping[str, str], *names: str) -> str:
    for name in names:
        value = str(env.get(name) or "").strip()
        if value:
            return value
    return ""


def _first_csv_value(value: str) -> str:
    return next((part.strip() for part in str(value or "").split(",") if part.strip()), "")


def _merge_credentials(
    base: dict[str, Any] | None,
    incoming: dict[str, Any],
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    if not base:
        return dict(incoming)
    merged = dict(base)
    for key, value in incoming.items():
        if value in ("", None):
            continue
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            nested = dict(merged[key])
            if overwrite:
                nested.update(value)
            else:
                for nested_key, nested_value in value.items():
                    nested.setdefault(nested_key, nested_value)
            merged[key] = nested
        elif overwrite or key not in merged or merged.get(key) in ("", None):
            merged[key] = value
    return merged


def save_encrypted_credentials(creds: dict[str, Any], *, db_path: str | Path | None = None) -> Path:
    target = Path(db_path).expanduser() if db_path is not None else _credentials_db_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(creds, sort_keys=True).encode("utf-8")
    encrypted = _cipher_for(target).encrypt(payload).decode("ascii")
    with sqlite3.connect(str(target), timeout=10) as conn:
        conn.executescript(_DDL)
        conn.execute(
            """INSERT OR REPLACE INTO encrypted_credentials
               (credential_id, payload, updated_at)
               VALUES (?, ?, ?)""",
            (_ROW_ID, encrypted, datetime.now(UTC).isoformat()),
        )
    return target


def load_encrypted_credentials(*, db_path: str | Path | None = None) -> dict[str, Any] | None:
    target = Path(db_path).expanduser() if db_path is not None else _credentials_db_path()
    if not target.exists():
        return None
    try:
        with sqlite3.connect(str(target), timeout=10) as conn:
            conn.executescript(_DDL)
            row = conn.execute(
                "SELECT payload FROM encrypted_credentials WHERE credential_id = ?",
                (_ROW_ID,),
            ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    try:
        decrypted = _cipher_for(target).decrypt(str(row[0]).encode("ascii"))
        data = json.loads(decrypted.decode("utf-8"))
    except (InvalidToken, json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def credentials_from_env(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Build a credential payload from installer/runtime env vars."""
    source = os.environ if env is None else env
    provider = str(source.get("NULLION_MODEL_PROVIDER") or "").strip().lower()
    model = _first_csv_value(
        _first_env_value(source, "NULLION_MODEL", "NULLION_OPENAI_MODEL", "OPENAI_MODEL")
    )
    base_url = _first_env_value(source, "NULLION_OPENAI_BASE_URL", "OPENAI_BASE_URL")
    keys: dict[str, str] = {}
    for provider_name, names in _PROVIDER_KEY_ENV_NAMES.items():
        value = _first_env_value(source, *names)
        if value:
            keys[provider_name] = value
    if not provider:
        if keys.get("anthropic"):
            provider = "anthropic"
        elif keys.get("openai") or base_url or model:
            provider = "openai"
    payload: dict[str, Any] = {}
    if provider:
        payload["provider"] = provider
    if model:
        payload["model"] = model
    if base_url:
        payload["base_url"] = base_url
    if keys:
        payload["keys"] = keys
    active_key = keys.get(provider) if provider else ""
    if active_key:
        payload["api_key"] = active_key
    refresh = _first_env_value(source, "NULLION_CODEX_REFRESH_TOKEN")
    if refresh:
        payload["refresh_token"] = refresh
    if provider and model:
        payload["models"] = {provider: model}
    return payload


def migrate_env_credentials_to_db(
    *,
    db_path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    overwrite: bool = False,
) -> dict[str, Any] | None:
    """Persist configured env credentials into the encrypted DB.

    Updates use ``overwrite=False`` so existing encrypted credentials stay
    authoritative. Installers can pass ``overwrite=True`` after writing a fresh
    .env so the DB reflects the just-selected provider.
    """
    incoming = credentials_from_env(env)
    if not incoming:
        return load_encrypted_credentials(db_path=db_path)
    existing = load_encrypted_credentials(db_path=db_path) or {}
    merged = _merge_credentials(existing, incoming, overwrite=overwrite)
    save_encrypted_credentials(merged, db_path=db_path)
    return merged


def migrate_credentials_json_to_db(json_path: str | Path, *, db_path: str | Path | None = None) -> dict[str, Any] | None:
    source = Path(json_path).expanduser()
    if not source.exists():
        return load_encrypted_credentials(db_path=db_path)
    try:
        data = json.loads(source.read_text(encoding="utf-8"))
    except Exception:
        return load_encrypted_credentials(db_path=db_path)
    if not isinstance(data, dict) or not data:
        return load_encrypted_credentials(db_path=db_path)
    existing = load_encrypted_credentials(db_path=db_path)
    if existing:
        data = _merge_credentials(data, existing, overwrite=True)
    save_encrypted_credentials(data, db_path=db_path)
    try:
        source.unlink()
    except OSError:
        pass
    return data
