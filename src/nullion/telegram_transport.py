"""Telegram transport configuration shared by app and probe senders."""

from __future__ import annotations

import os
from typing import Any


_DEFAULT_CONNECT_TIMEOUT_SECONDS = 20.0
_DEFAULT_READ_TIMEOUT_SECONDS = 30.0
_DEFAULT_WRITE_TIMEOUT_SECONDS = 30.0
_DEFAULT_POOL_TIMEOUT_SECONDS = 10.0
_DEFAULT_MEDIA_READ_TIMEOUT_SECONDS = 120.0
_DEFAULT_MEDIA_WRITE_TIMEOUT_SECONDS = 120.0
_DEFAULT_GET_UPDATES_READ_TIMEOUT_SECONDS = 35.0
_DEFAULT_CONNECTION_POOL_SIZE = 256


def _env_text(name: str) -> str:
    return str(os.environ.get(name) or "").strip()


def _env_bool(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(1.0, value)


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, value)


def telegram_request_timeout_kwargs() -> dict[str, float | int]:
    """Return conservative Telegram API timeout settings for production delivery."""

    return {
        "connect_timeout": _float_env(
            "NULLION_TELEGRAM_CONNECT_TIMEOUT_SECONDS",
            _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        ),
        "read_timeout": _float_env(
            "NULLION_TELEGRAM_READ_TIMEOUT_SECONDS",
            _DEFAULT_READ_TIMEOUT_SECONDS,
        ),
        "write_timeout": _float_env(
            "NULLION_TELEGRAM_WRITE_TIMEOUT_SECONDS",
            _DEFAULT_WRITE_TIMEOUT_SECONDS,
        ),
        "pool_timeout": _float_env(
            "NULLION_TELEGRAM_POOL_TIMEOUT_SECONDS",
            _DEFAULT_POOL_TIMEOUT_SECONDS,
        ),
        "media_write_timeout": _float_env(
            "NULLION_TELEGRAM_MEDIA_WRITE_TIMEOUT_SECONDS",
            _DEFAULT_MEDIA_WRITE_TIMEOUT_SECONDS,
        ),
        "connection_pool_size": _int_env(
            "NULLION_TELEGRAM_CONNECTION_POOL_SIZE",
            _DEFAULT_CONNECTION_POOL_SIZE,
        ),
    }


def telegram_media_read_timeout_seconds() -> float:
    return _float_env(
        "NULLION_TELEGRAM_MEDIA_READ_TIMEOUT_SECONDS",
        _DEFAULT_MEDIA_READ_TIMEOUT_SECONDS,
    )


def telegram_bot_api_endpoint_kwargs() -> dict[str, str | bool]:
    """Return Bot API endpoint overrides for hosted or local Telegram transport."""

    local_root = (
        _env_text("NULLION_TELEGRAM_LOCAL_BOT_API_URL")
        or _env_text("NULLION_TELEGRAM_BOT_API_URL")
    ).rstrip("/")
    base_url = _env_text("NULLION_TELEGRAM_BOT_API_BASE_URL").rstrip("/")
    base_file_url = _env_text("NULLION_TELEGRAM_BOT_API_FILE_BASE_URL").rstrip("/")
    if local_root:
        base_url = base_url or f"{local_root}/bot"
        base_file_url = base_file_url or f"{local_root}/file/bot"

    kwargs: dict[str, str | bool] = {}
    if base_url:
        kwargs["base_url"] = base_url
    if base_file_url:
        kwargs["base_file_url"] = base_file_url
    if _env_bool("NULLION_TELEGRAM_BOT_API_LOCAL_MODE", default=bool(local_root)):
        kwargs["local_mode"] = True
    return kwargs


def telegram_uses_local_bot_api() -> bool:
    return bool(telegram_bot_api_endpoint_kwargs().get("local_mode"))


def build_telegram_httpx_request() -> object | None:
    try:
        from telegram.request import HTTPXRequest  # type: ignore[import]
    except Exception:
        return None
    return HTTPXRequest(**telegram_request_timeout_kwargs())


def build_telegram_bot(bot_cls: type[Any], token: str):
    request = build_telegram_httpx_request()
    endpoint_kwargs = telegram_bot_api_endpoint_kwargs()
    kwargs: dict[str, object] = dict(endpoint_kwargs)
    if request is not None:
        kwargs["request"] = request
    try:
        return bot_cls(token, **kwargs)
    except TypeError:
        kwargs.pop("local_mode", None)
        try:
            return bot_cls(token, **kwargs)
        except TypeError:
            return bot_cls(token)


def configure_telegram_application_builder(builder):
    endpoint_kwargs = telegram_bot_api_endpoint_kwargs()
    for method_name in ("base_url", "base_file_url", "local_mode"):
        if method_name not in endpoint_kwargs:
            continue
        method = getattr(builder, method_name, None)
        if callable(method):
            builder = method(endpoint_kwargs[method_name]) or builder

    request_kwargs = telegram_request_timeout_kwargs()
    for method_name in (
        "connect_timeout",
        "read_timeout",
        "write_timeout",
        "pool_timeout",
        "get_updates_connect_timeout",
        "get_updates_write_timeout",
        "get_updates_pool_timeout",
    ):
        method = getattr(builder, method_name, None)
        value = request_kwargs.get(method_name.removeprefix("get_updates_"))
        if callable(method) and value is not None:
            builder = method(value) or builder
    get_updates_read_timeout = _float_env(
        "NULLION_TELEGRAM_GET_UPDATES_READ_TIMEOUT_SECONDS",
        _DEFAULT_GET_UPDATES_READ_TIMEOUT_SECONDS,
    )
    method = getattr(builder, "get_updates_read_timeout", None)
    if callable(method):
        builder = method(get_updates_read_timeout) or builder
    return builder


__all__ = [
    "build_telegram_bot",
    "build_telegram_httpx_request",
    "configure_telegram_application_builder",
    "telegram_bot_api_endpoint_kwargs",
    "telegram_media_read_timeout_seconds",
    "telegram_uses_local_bot_api",
    "telegram_request_timeout_kwargs",
]
