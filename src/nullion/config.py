"""Configuration models for Project Nullion."""

from __future__ import annotations

import os
import shlex
import stat
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Mapping

from nullion.enums import TeamMode, VisibilityMode


@dataclass(slots=True)
class TelegramSettings:
    bot_token: str | None = field(default=None, repr=False)
    operator_chat_id: str | None = None
    chat_enabled: bool = False


@dataclass(slots=True)
class SlackSettings:
    enabled: bool = False
    bot_token: str | None = field(default=None, repr=False)
    app_token: str | None = field(default=None, repr=False)
    signing_secret: str | None = field(default=None, repr=False)
    operator_user_id: str | None = None


@dataclass(slots=True)
class DiscordSettings:
    enabled: bool = False
    bot_token: str | None = field(default=None, repr=False)


@dataclass(slots=True)
class TerminalExecutionSettings:
    backend_mode: str = "subprocess"
    launcher_command: str | None = None
    launcher_args: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class ProviderBinding:
    capability: str
    provider: str


_VALID_TERMINAL_BACKEND_MODES = frozenset({"subprocess", "launcher"})
_VALID_WEB_SESSION_ALLOW_DURATIONS = frozenset({"session", "15m", "30m", "1h", "2h", "4h", "today"})
_WEB_SESSION_ALLOW_LABELS = {
    "session": "for all workspaces",
    "15m": "15 minutes",
    "30m": "30 minutes",
    "1h": "1 hour",
    "2h": "2 hours",
    "4h": "4 hours",
    "today": "today",
}

_OPENAI_COMPAT_PROVIDER_BASE_URLS = {
    "openrouter": "https://openrouter.ai/api/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai/",
    "ollama": "http://127.0.0.1:11434/v1",
    "groq": "https://api.groq.com/openai/v1",
    "mistral": "https://api.mistral.ai/v1",
    "deepseek": "https://api.deepseek.com/v1",
    "xai": "https://api.x.ai/v1",
    "together": "https://api.together.xyz/v1",
}

_VALID_REASONING_EFFORTS = frozenset({"low", "medium", "high"})


@dataclass(slots=True)
class ModelSettings:
    """Provider credentials for the LLM backend.

    Precedence (highest to lowest):
      1. explicit env vars, including NULLION_MODEL_PROVIDER / NULLION_MODEL
      2. encrypted local credentials in ~/.nullion/runtime.db (set via `nullion-auth`):
         - provider=codex → CodexResponsesModelClient
         - provider=anthropic → AnthropicMessagesModelClient
         - anything else  → OpenAI-compatible client
    """
    openai_api_key: str | None = None
    openai_base_url: str | None = None
    openai_model: str | None = None
    provider: str | None = None   # "codex" | "openai" | "anthropic" | etc.
    codex_refresh_token: str | None = None
    reasoning_effort: str | None = None


@dataclass(slots=True)
class NullionSettings:
    project_name: str = "Project Nullion"
    team_mode: TeamMode = TeamMode.STANDARD
    pm_check_interval_minutes: int = 30
    pm_silent_when_no_change: bool = True
    doctor_enabled: bool = True
    nullion_enabled: bool = True
    visibility_mode: VisibilityMode = VisibilityMode.STANDARD
    telegram: TelegramSettings = field(default_factory=TelegramSettings)
    slack: SlackSettings = field(default_factory=SlackSettings)
    discord: DiscordSettings = field(default_factory=DiscordSettings)
    terminal_execution: TerminalExecutionSettings = field(default_factory=TerminalExecutionSettings)
    model: ModelSettings = field(default_factory=ModelSettings)
    enabled_plugins: tuple[str, ...] = ()
    provider_bindings: tuple[ProviderBinding, ...] = ()
    enabled_skill_packs: tuple[str, ...] = ()
    workspace_root: str | None = None
    allowed_roots: tuple[str, ...] = ()
    web_session_allow_duration: str = "session"



def _parse_env_line(raw_line: str) -> tuple[str, str] | None:
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
        return None
    if line.startswith("export "):
        line = line.removeprefix("export ").lstrip()
        if "=" not in line:
            return None
    key, value = line.split("=", 1)
    key = key.strip()
    if not key:
        return None
    normalized_value = value.strip()
    if len(normalized_value) >= 2 and normalized_value[0] == normalized_value[-1] and normalized_value[0] in {'"', "'"}:
        normalized_value = normalized_value[1:-1]
    return key, normalized_value


def _read_env_file(path: str | Path) -> dict[str, str]:
    env_values: dict[str, str] = {}
    env_path = Path(path)
    if not env_path.exists():
        return env_values
    if os.name != "nt":
        mode = stat.S_IMODE(env_path.stat().st_mode)
        if mode & 0o077:
            raise RuntimeError(
                f"Insecure permissions on env file {env_path}: {oct(mode)}. "
                "Run chmod 600 on this file before starting Nullion."
            )

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_env_line(raw_line)
        if parsed is None:
            continue
        key, normalized_value = parsed
        env_values[key] = normalized_value
    return env_values


def load_env_file_into_environ(path: str | Path, *, override: bool = False) -> dict[str, str]:
    env_values = _read_env_file(path)
    for key, value in env_values.items():
        if override or key not in os.environ:
            os.environ[key] = value
    return env_values


def load_default_env_file_into_environ(env_file: str | Path | None = None, *, override: bool = False) -> Path | None:
    candidates = [
        env_file,
        os.environ.get("NULLION_ENV_FILE"),
        Path.home() / ".nullion" / ".env",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.exists():
            load_env_file_into_environ(path, override=override)
            return path
    return None



def default_settings() -> NullionSettings:
    return NullionSettings()



def _first_nonempty_env_value(merged_env: Mapping[str, str], *keys: str) -> str | None:
    for key in keys:
        value = merged_env.get(key)
        if value is not None:
            stripped = value.strip()
            if stripped != "":
                return stripped
    return None


def _env_flag(value: str | None, *, default: bool = False) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}



def _terminal_launcher_args(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(shlex.split(value))



def _split_csv_setting(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    parts = [item.strip() for item in value.split(",")]
    return tuple(item for item in parts if item)


def _first_csv_setting(value: str | None) -> str | None:
    entries = _split_csv_setting(value)
    return entries[0] if entries else None


def normalize_reasoning_effort(value: str | None) -> str | None:
    key = (value or "").strip().lower()
    return key if key in _VALID_REASONING_EFFORTS else None


def normalize_web_session_allow_duration(value: str | None) -> str:
    key = (value or "session").strip().lower().replace("_", "-")
    aliases = {
        "until-new-session": "session",
        "new-session": "session",
        "current-session": "session",
        "15min": "15m",
        "15mins": "15m",
        "30min": "30m",
        "30mins": "30m",
        "hour": "1h",
        "1hour": "1h",
        "2hour": "2h",
        "2hours": "2h",
        "4hour": "4h",
        "4hours": "4h",
        "day": "today",
    }
    key = aliases.get(key, key)
    return key if key in _VALID_WEB_SESSION_ALLOW_DURATIONS else "session"


def web_session_allow_duration_label(value: str | None) -> str:
    return _WEB_SESSION_ALLOW_LABELS[normalize_web_session_allow_duration(value)]


def web_session_allow_expires_at(value: str | None, *, now: datetime | None = None) -> datetime | None:
    current = now or datetime.now(UTC)
    key = normalize_web_session_allow_duration(value)
    if key == "session":
        return None
    if key == "15m":
        return current + timedelta(minutes=15)
    if key == "30m":
        return current + timedelta(minutes=30)
    if key == "1h":
        return current + timedelta(hours=1)
    if key == "2h":
        return current + timedelta(hours=2)
    if key == "4h":
        return current + timedelta(hours=4)
    return current + timedelta(days=1)



def _parse_provider_bindings(value: str | None) -> tuple[ProviderBinding, ...]:
    bindings: list[ProviderBinding] = []
    seen_capabilities: set[str] = set()
    for item in _split_csv_setting(value):
        capability, separator, provider = item.partition("=")
        capability = capability.strip()
        provider = provider.strip()
        if separator != "=" or not capability or not provider:
            raise ValueError(f"invalid provider binding: {item}")
        if capability in seen_capabilities:
            raise ValueError(f"duplicate provider binding: {capability}")
        seen_capabilities.add(capability)
        bindings.append(ProviderBinding(capability=capability, provider=provider))
    return tuple(bindings)


def load_settings(
    *,
    env: Mapping[str, str] | None = None,
    env_path: str | Path | None = None,
) -> NullionSettings:
    explicit_env = env is not None
    merged_env: dict[str, str] = {}
    if env is None:
        if env_path is not None:
            merged_env.update(_read_env_file(env_path))
        else:
            merged_env.update(os.environ)
    else:
        if env_path is not None:
            merged_env.update(_read_env_file(env_path))
        merged_env.update(env)

    settings = default_settings()
    chat_enabled_raw = _first_nonempty_env_value(
        merged_env,
        "NULLION_TELEGRAM_CHAT_ENABLED",
    )
    # Default to enabled — opt out with NULLION_TELEGRAM_CHAT_ENABLED=false
    chat_enabled = _env_flag(chat_enabled_raw, default=True)

    settings.telegram = TelegramSettings(
        bot_token=_first_nonempty_env_value(
            merged_env,
            "NULLION_TELEGRAM_BOT_TOKEN",
        ),
        operator_chat_id=_first_nonempty_env_value(
            merged_env,
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID",
        ),
        chat_enabled=chat_enabled,
    )
    settings.slack = SlackSettings(
        enabled=_env_flag(_first_nonempty_env_value(merged_env, "NULLION_SLACK_ENABLED")),
        bot_token=_first_nonempty_env_value(merged_env, "NULLION_SLACK_BOT_TOKEN"),
        app_token=_first_nonempty_env_value(merged_env, "NULLION_SLACK_APP_TOKEN"),
        signing_secret=_first_nonempty_env_value(merged_env, "NULLION_SLACK_SIGNING_SECRET"),
        operator_user_id=_first_nonempty_env_value(merged_env, "NULLION_SLACK_OPERATOR_USER_ID"),
    )
    settings.discord = DiscordSettings(
        enabled=_env_flag(_first_nonempty_env_value(merged_env, "NULLION_DISCORD_ENABLED")),
        bot_token=_first_nonempty_env_value(merged_env, "NULLION_DISCORD_BOT_TOKEN"),
    )

    terminal_backend_mode = (
        _first_nonempty_env_value(
            merged_env,
            "NULLION_TERMINAL_BACKEND_MODE",
        )
        or "subprocess"
    ).strip().lower()
    if terminal_backend_mode not in _VALID_TERMINAL_BACKEND_MODES:
        raise ValueError(f"unknown terminal backend mode: {terminal_backend_mode}")
    terminal_launcher_command = _first_nonempty_env_value(
        merged_env,
        "NULLION_TERMINAL_LAUNCHER_COMMAND",
    )
    terminal_launcher_args = _terminal_launcher_args(
        _first_nonempty_env_value(
            merged_env,
            "NULLION_TERMINAL_LAUNCHER_ARGS",
        )
    )
    if terminal_backend_mode == "launcher" and not terminal_launcher_command:
        raise ValueError("terminal launcher mode requires a launcher command")

    settings.terminal_execution = TerminalExecutionSettings(
        backend_mode=terminal_backend_mode,
        launcher_command=terminal_launcher_command,
        launcher_args=terminal_launcher_args,
    )
    settings.enabled_plugins = _split_csv_setting(
        _first_nonempty_env_value(
            merged_env,
            "NULLION_ENABLED_PLUGINS",
        )
    )
    settings.provider_bindings = _parse_provider_bindings(
        _first_nonempty_env_value(
            merged_env,
            "NULLION_PROVIDER_BINDINGS",
        )
    )
    settings.enabled_skill_packs = _split_csv_setting(
        _first_nonempty_env_value(
            merged_env,
            "NULLION_ENABLED_SKILL_PACKS",
        )
    )
    settings.workspace_root = _first_nonempty_env_value(
        merged_env,
        "NULLION_WORKSPACE_ROOT",
    )
    settings.allowed_roots = _split_csv_setting(
        _first_nonempty_env_value(
            merged_env,
            "NULLION_ALLOWED_ROOTS",
        )
    )
    settings.web_session_allow_duration = normalize_web_session_allow_duration(
        _first_nonempty_env_value(
            merged_env,
            "NULLION_WEB_SESSION_ALLOW_DURATION",
        )
    )
    openai_api_key = _first_nonempty_env_value(
        merged_env,
        "NULLION_OPENAI_API_KEY",
        "OPENAI_API_KEY",
    )
    openai_base_url = _first_nonempty_env_value(
        merged_env,
        "NULLION_OPENAI_BASE_URL",
        "OPENAI_BASE_URL",
    )
    openai_model = _first_csv_setting(
        _first_nonempty_env_value(
            merged_env,
            "NULLION_MODEL",
            "NULLION_OPENAI_MODEL",
            "OPENAI_MODEL",
        )
    )
    codex_refresh_token = _first_nonempty_env_value(
        merged_env,
        "NULLION_CODEX_REFRESH_TOKEN",
    )
    raw_reasoning_effort = _first_nonempty_env_value(
        merged_env,
        "NULLION_REASONING_EFFORT",
        "NULLION_THINKING_LEVEL",
    )
    reasoning_effort = normalize_reasoning_effort(raw_reasoning_effort)
    provider = _first_nonempty_env_value(
        merged_env,
        "NULLION_MODEL_PROVIDER",
    )
    normalized_provider = provider.strip().lower() if isinstance(provider, str) else None
    if not openai_api_key:
        provider_key_envs = {
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
        if normalized_provider in provider_key_envs:
            openai_api_key = _first_nonempty_env_value(merged_env, *provider_key_envs[normalized_provider])
    if normalized_provider == "ollama" and not openai_api_key:
        openai_api_key = "ollama-local"
    if not openai_base_url and normalized_provider in _OPENAI_COMPAT_PROVIDER_BASE_URLS:
        openai_base_url = _OPENAI_COMPAT_PROVIDER_BASE_URLS[normalized_provider]

    # Fall back to encrypted local credentials (set via `nullion auth`).
    # We load stored credentials unconditionally and consult them for any
    # field that env vars didn't already cover. Previously this whole block
    # was guarded by `if not openai_api_key`, which meant the Codex OAuth
    # `refresh_token` would silently fail to load whenever an access token
    # was present in env vars (e.g. during the test-connection flow which
    # sets OPENAI_API_KEY=<oauth_token>). The result was a misleading
    # "Codex OAuth access token has no chatgpt_account_id claim and no
    # refresh token is saved" error even when the refresh token sat right
    # there in the encrypted credential store.
    from nullion.auth import load_stored_credentials
    stored = {} if explicit_env else (load_stored_credentials() or {})
    if stored:
        raw_provider = stored.get("provider")
        stored_provider = raw_provider.strip() if isinstance(raw_provider, str) and raw_provider.strip() else None
        effective_provider = provider or stored_provider
        stored_keys = stored.get("keys")
        if not isinstance(stored_keys, dict):
            stored_keys = {}
        stored_models = stored.get("models")
        if not isinstance(stored_models, dict):
            stored_models = {}
        if not openai_api_key:
            raw_key = (
                stored_keys.get(effective_provider, "")
                if effective_provider else ""
            ) or stored.get("api_key", "")
            if isinstance(raw_key, str) and raw_key.strip() and raw_key.strip() != "none":
                openai_api_key = raw_key.strip()
        if not openai_base_url:
            raw_url = stored.get("base_url")
            if isinstance(raw_url, str) and raw_url.strip():
                openai_base_url = raw_url.strip()
        if not openai_model:
            raw_model = stored.get("model") or (
                stored_models.get(effective_provider, "")
                if effective_provider else ""
            )
            if isinstance(raw_model, str) and raw_model.strip():
                openai_model = _first_csv_setting(raw_model)
        if not provider and stored_provider:
            provider = stored_provider
        if raw_reasoning_effort is None and not reasoning_effort:
            raw_reasoning = stored.get("reasoning_effort") or stored.get("thinking_level")
            if isinstance(raw_reasoning, str):
                reasoning_effort = normalize_reasoning_effort(raw_reasoning)
        raw_refresh = stored.get("refresh_token")
        if not codex_refresh_token and isinstance(raw_refresh, str) and raw_refresh.strip():
            codex_refresh_token = raw_refresh.strip()

    # Compatibility with installer builds that labeled browser OAuth as
    # provider=openai while storing the OAuth access token in OPENAI_API_KEY.
    if (
        isinstance(provider, str)
        and provider.strip().lower() == "openai"
        and isinstance(openai_api_key, str)
        and openai_api_key.strip()
        and not openai_api_key.strip().startswith("sk-")
    ):
        provider = "codex"

    settings.model = ModelSettings(
        openai_api_key=openai_api_key,
        openai_base_url=openai_base_url,
        openai_model=openai_model,
        provider=provider,
        codex_refresh_token=codex_refresh_token,
        reasoning_effort=reasoning_effort,
    )
    return settings


__all__ = [
    "DiscordSettings",
    "ModelSettings",
    "NullionSettings",
    "ProviderBinding",
    "SlackSettings",
    "TelegramSettings",
    "TerminalExecutionSettings",
    "default_settings",
    "load_settings",
    "normalize_reasoning_effort",
    "normalize_web_session_allow_duration",
    "web_session_allow_duration_label",
    "web_session_allow_expires_at",
]
