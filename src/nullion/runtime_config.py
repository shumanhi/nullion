"""Runtime configuration helpers shared by chat surfaces."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from nullion import __version__
from nullion.settings import Settings


_CREDENTIALS_PATH = Path.home() / ".nullion" / "credentials.json"
_DEFAULT_CREDENTIALS_PATH = _CREDENTIALS_PATH


def _credentials_path() -> Path:
    if _CREDENTIALS_PATH != _DEFAULT_CREDENTIALS_PATH:
        return _CREDENTIALS_PATH
    return Path.home() / ".nullion" / "credentials.json"


@dataclass(frozen=True, slots=True)
class RuntimeConfigSnapshot:
    provider: str
    model: str
    admin_forced_model: str | None
    admin_forced_provider: str | None
    checkpoint: str
    data_dir: str
    operator_name: str
    telegram_configured: bool
    web_access: bool
    browser_enabled: bool
    file_access: bool
    terminal_enabled: bool
    memory_enabled: bool
    doctor_enabled: bool
    skill_learning_enabled: bool
    task_decomposition_enabled: bool
    multi_agent_enabled: bool


def _env_enabled(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _load_credentials() -> dict[str, object]:
    try:
        payload = json.loads(_credentials_path().read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _first_model_entry(value: object) -> str:
    return next((part.strip() for part in str(value or "").split(",") if part.strip()), "")


def current_runtime_config(*, model_client: object | None = None) -> RuntimeConfigSnapshot:
    settings = Settings()
    creds = _load_credentials()
    model_cfg = settings.model
    provider = (
        os.environ.get("NULLION_MODEL_PROVIDER")
        or str(creds.get("provider") or "")
        or model_cfg.provider
        or "unknown"
    )
    model = (
        _first_model_entry(os.environ.get("NULLION_MODEL"))
        or _first_model_entry(creds.get("model"))
        or _first_model_entry(model_cfg.openai_model)
        or _first_model_entry(model_cfg.anthropic_model)
        or str(getattr(model_client, "model", "") or "")
        or "unknown"
    )
    # Admin-forced model: pushed to all sessions as a broadcast default.
    # Takes priority over the global configured model, but session users can
    # override it with their own per-session preference.
    admin_forced_raw = (
        os.environ.get("NULLION_ADMIN_FORCED_MODEL")
        or str(creds.get("admin_forced_model") or "")
    )
    admin_forced_model: str | None = admin_forced_raw.strip() or None
    admin_forced_provider_raw = (
        os.environ.get("NULLION_ADMIN_FORCED_PROVIDER")
        or str(creds.get("admin_forced_provider") or "")
    )
    admin_forced_provider: str | None = admin_forced_provider_raw.strip() or None
    data_dir = os.environ.get("NULLION_DATA_DIR") or settings.data_dir
    return RuntimeConfigSnapshot(
        provider=provider,
        model=model,
        admin_forced_model=admin_forced_model,
        admin_forced_provider=admin_forced_provider,
        checkpoint=str(settings.checkpoint_path),
        data_dir=data_dir or str(settings.checkpoint_path.parent),
        operator_name=os.environ.get("NULLION_OPERATOR_NAME") or settings.operator_name or "operator",
        telegram_configured=bool(
            os.environ.get("NULLION_TELEGRAM_BOT_TOKEN")
            and os.environ.get("NULLION_TELEGRAM_OPERATOR_CHAT_ID")
        ),
        web_access=_env_enabled("NULLION_WEB_ACCESS_ENABLED", True),
        browser_enabled=_env_enabled("NULLION_BROWSER_ENABLED", True),
        file_access=_env_enabled("NULLION_FILE_ACCESS_ENABLED", True),
        terminal_enabled=_env_enabled("NULLION_TERMINAL_ENABLED", True),
        memory_enabled=_env_enabled("NULLION_MEMORY_ENABLED", True),
        doctor_enabled=_env_enabled("NULLION_DOCTOR_ENABLED", True),
        skill_learning_enabled=_env_enabled("NULLION_SKILL_LEARNING_ENABLED", True),
        task_decomposition_enabled=_env_enabled("NULLION_TASK_DECOMPOSITION_ENABLED", True),
        multi_agent_enabled=_env_enabled("NULLION_MULTI_AGENT_ENABLED", True),
    )


def format_runtime_config_for_prompt(*, model_client: object | None = None) -> str:
    cfg = current_runtime_config(model_client=model_client)
    enabled = []
    disabled = []
    for label, value in [
        ("web access", cfg.web_access),
        ("browser", cfg.browser_enabled),
        ("files", cfg.file_access),
        ("terminal", cfg.terminal_enabled),
        ("memory", cfg.memory_enabled),
        ("Doctor", cfg.doctor_enabled),
        ("Builder skill learning", cfg.skill_learning_enabled),
        ("task decomposition", cfg.task_decomposition_enabled),
        ("multi-agent delegation", cfg.multi_agent_enabled),
    ]:
        (enabled if value else disabled).append(label)
    lines = [
        "Runtime configuration:",
        f"- Nullion version: {__version__}",
        f"- Active model provider: {cfg.provider}",
        f"- Active model name: {cfg.model}",
        *(
            [
                "- Admin-forced model (broadcast default): "
                f"{cfg.admin_forced_provider + ' · ' if cfg.admin_forced_provider else ''}{cfg.admin_forced_model}"
            ]
            if cfg.admin_forced_model
            else []
        ),
        f"- Operator name: {cfg.operator_name}",
        f"- Data directory: {cfg.data_dir}",
        f"- Runtime database/checkpoint: {cfg.checkpoint}",
        f"- Telegram configured: {'yes' if cfg.telegram_configured else 'no'}",
        f"- Enabled capabilities: {', '.join(enabled) if enabled else 'none'}",
        f"- Disabled capabilities: {', '.join(disabled) if disabled else 'none'}",
        "",
        "If the user asks what model or provider is being used, answer from this runtime configuration.",
        "If the user asks to change model/provider/settings, only say it changed after an authorized command or config update confirms it.",
    ]
    return "\n".join(lines)


def persist_model_name(model_name: str) -> None:
    normalized = model_name.strip()
    if not normalized:
        raise ValueError("model name is required")
    creds = _load_credentials()
    creds["model"] = normalized
    path = _credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(creds, indent=2) + "\n", encoding="utf-8")
    os.environ["NULLION_MODEL"] = normalized


def persist_admin_forced_model(model_name: str) -> None:
    """Set the admin-forced model broadcast to all sessions.

    This becomes the effective model for any session that has not set its own
    per-session preference via ``/model``.  Session users can still override it.
    """
    normalized = model_name.strip()
    if not normalized:
        raise ValueError("model name is required")
    creds = _load_credentials()
    creds["admin_forced_model"] = normalized
    path = _credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(creds, indent=2) + "\n", encoding="utf-8")
    os.environ["NULLION_ADMIN_FORCED_MODEL"] = normalized


def clear_admin_forced_model() -> None:
    """Remove the admin-forced model so sessions fall back to the global default."""
    creds = _load_credentials()
    creds.pop("admin_forced_model", None)
    path = _credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(creds, indent=2) + "\n", encoding="utf-8")
    os.environ.pop("NULLION_ADMIN_FORCED_MODEL", None)


__all__ = [
    "RuntimeConfigSnapshot",
    "current_runtime_config",
    "persist_admin_forced_model",
    "clear_admin_forced_model",
    "format_runtime_config_for_prompt",
    "persist_model_name",
]
