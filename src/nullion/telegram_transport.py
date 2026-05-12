"""Telegram transport configuration shared by app and probe senders."""

from __future__ import annotations

import os
from typing import Any


_DEFAULT_CONNECT_TIMEOUT_SECONDS = 20.0
_DEFAULT_READ_TIMEOUT_SECONDS = 30.0
_DEFAULT_WRITE_TIMEOUT_SECONDS = 30.0
_DEFAULT_POOL_TIMEOUT_SECONDS = 10.0
_DEFAULT_MEDIA_WRITE_TIMEOUT_SECONDS = 120.0
_DEFAULT_GET_UPDATES_READ_TIMEOUT_SECONDS = 35.0
_DEFAULT_CONNECTION_POOL_SIZE = 256


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


def build_telegram_httpx_request() -> object | None:
    try:
        from telegram.request import HTTPXRequest  # type: ignore[import]
    except Exception:
        return None
    return HTTPXRequest(**telegram_request_timeout_kwargs())


def build_telegram_bot(bot_cls: type[Any], token: str):
    request = build_telegram_httpx_request()
    if request is None:
        return bot_cls(token)
    try:
        return bot_cls(token, request=request)
    except TypeError:
        return bot_cls(token)


def configure_telegram_application_builder(builder):
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
    "telegram_request_timeout_kwargs",
]
