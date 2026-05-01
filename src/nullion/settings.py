"""Nullion settings — reads from env vars and encrypted local credentials.

This is the single source of truth for runtime configuration. Both web_app.py
and cli.py call Settings() to get a fully resolved config object.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_CREDENTIALS_PATH = Path.home() / ".nullion" / "credentials.json"
_DEFAULT_CHECKPOINT = Path.home() / ".nullion" / "runtime.db"


def _first_model_entry(value: object) -> str:
    return next((part.strip() for part in str(value or "").split(",") if part.strip()), "")


# ── Model sub-config ──────────────────────────────────────────────────────────

@dataclass
class ModelSettings:
    provider: str = ""           # openai / anthropic / codex / openrouter / custom
    openai_api_key: str = ""
    openai_model: str = ""
    openai_base_url: str = ""
    codex_refresh_token: str = ""
    anthropic_api_key: str = ""
    anthropic_model: str = ""


# ── Top-level Settings ────────────────────────────────────────────────────────

@dataclass
class Settings:
    """All runtime settings, resolved from env vars + credentials file."""

    model: ModelSettings = field(default_factory=ModelSettings)
    checkpoint_path: Path = field(default_factory=lambda: _DEFAULT_CHECKPOINT)
    operator_name: str = ""
    data_dir: str = ""

    def __post_init__(self) -> None:
        self._load()

    # ── Internal loading ──────────────────────────────────────────────────────

    def _load(self) -> None:
        creds = _load_credentials()
        self._apply_credentials(creds)
        self._apply_env()

    def _apply_credentials(self, creds: dict[str, Any]) -> None:
        if not creds:
            return
        provider = creds.get("provider", "")
        keys = creds.get("keys")
        if not isinstance(keys, dict):
            keys = {}
        models = creds.get("models")
        if not isinstance(models, dict):
            models = {}
        api_key  = keys.get(provider, "") or creds.get("api_key", "")
        base_url = creds.get("base_url", "")
        model    = _first_model_entry(models.get(provider, "") or creds.get("model", ""))
        refresh_token = creds.get("refresh_token", "")

        if isinstance(refresh_token, str) and refresh_token:
            self.model.codex_refresh_token = refresh_token

        if provider in ("openai", "openrouter", "openrouter-key", "gemini", "custom", "codex"):
            self.model.provider       = provider
            self.model.openai_api_key = api_key
            self.model.openai_model   = model
            self.model.openai_base_url = base_url
        elif provider == "anthropic":
            self.model.provider          = "anthropic"
            self.model.anthropic_api_key = api_key
            self.model.anthropic_model   = model

    def _apply_env(self) -> None:
        m = self.model

        # Provider override
        env_provider = os.environ.get("NULLION_MODEL_PROVIDER", "").lower()
        if env_provider:
            m.provider = env_provider
            if env_provider == "codex" and not m.openai_api_key and m.anthropic_api_key:
                # Older/misaligned credentials may have stored the OAuth bearer
                # token in the generic key slot before the env override is
                # applied. Codex uses OAuth, not an OpenAI API key.
                m.openai_api_key = m.anthropic_api_key

        # OpenAI / OpenAI-compatible
        oai_key = os.environ.get("NULLION_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY", "")
        if oai_key:
            m.openai_api_key = oai_key
            if not m.provider:
                m.provider = "openai"

        oai_model = (
            os.environ.get("NULLION_MODEL")
            or os.environ.get("NULLION_OPENAI_MODEL")
            or os.environ.get("OPENAI_MODEL", "")
        )
        if oai_model:
            m.openai_model = _first_model_entry(oai_model)

        oai_base = os.environ.get("NULLION_OPENAI_BASE_URL") or os.environ.get("OPENAI_BASE_URL", "")
        if oai_base:
            m.openai_base_url = oai_base

        # Anthropic
        ant_key = os.environ.get("NULLION_ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_API_KEY", "")
        if ant_key:
            m.anthropic_api_key = ant_key
            if not m.provider:
                m.provider = "anthropic"

        ant_model = os.environ.get("NULLION_ANTHROPIC_MODEL", "")
        if ant_model:
            m.anthropic_model = _first_model_entry(ant_model)

        # Runtime / misc
        cp = os.environ.get("NULLION_CHECKPOINT_PATH", "")
        if cp:
            self.checkpoint_path = Path(cp)

        dd = os.environ.get("NULLION_DATA_DIR", "")
        if dd:
            self.data_dir = dd
            if not cp:
                self.checkpoint_path = Path(dd).expanduser() / "runtime.db"

        op = os.environ.get("NULLION_OPERATOR_NAME", "")
        if op:
            self.operator_name = op

    # ── Helpers ───────────────────────────────────────────────────────────────

    def has_llm(self) -> bool:
        m = self.model
        return bool(m.openai_api_key or m.anthropic_api_key)

    def to_dict(self) -> dict[str, Any]:
        """Serialisable snapshot (keys are masked)."""
        m = self.model
        return {
            "provider":     m.provider,
            "model":        m.openai_model or m.anthropic_model,
            "has_api_key":  self.has_llm(),
            "checkpoint":   str(self.checkpoint_path),
            "operator_name": self.operator_name,
        }


# ── Credentials file ──────────────────────────────────────────────────────────

def _load_credentials() -> dict[str, Any]:
    try:
        from nullion.credential_store import migrate_credentials_json_to_db

        return migrate_credentials_json_to_db(_CREDENTIALS_PATH, db_path=_CREDENTIALS_PATH.with_name("runtime.db")) or {}
    except Exception:
        return {}


__all__ = ["Settings", "ModelSettings"]
