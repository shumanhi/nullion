"""Standard health probes for built-in Nullion services.

Each probe is a zero-arg callable that returns a ProbeResult.
Use the factory functions to build probes that capture the target
(model_client, bot application) in a closure.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Awaitable, Callable

from nullion.health_monitor import ProbeResult

logger = logging.getLogger(__name__)

_AUTH_ERROR_CODE_RE = re.compile(r'"code"\s*:\s*"([^"]+)"')
_ERROR_TYPE_RE = re.compile(r'"type"\s*:\s*"([^"]+)"')
_HTTP_STATUS_RE = re.compile(r"\bHTTP\s+(\d{3})\b|\"status\"\s*:\s*(\d{3})")
_RESETS_IN_SECONDS_RE = re.compile(r'"resets_in_seconds"\s*:\s*(\d+)')
_TERMINAL_AUTH_CODES = {"token_invalidated", "token_revoked", "invalid_api_key", "invalid_token"}
_TERMINAL_QUOTA_TYPES = {"usage_limit_reached", "insufficient_quota"}


def _model_probe_error_details(exc: Exception) -> dict[str, object]:
    """Extract structured provider facts from model probe exceptions."""
    text = str(exc)
    status_code = getattr(exc, "status_code", None)
    provider_code = getattr(exc, "code", None)
    error_type = None
    if provider_code is None:
        match = _AUTH_ERROR_CODE_RE.search(text)
        if match:
            provider_code = match.group(1)
    match = _ERROR_TYPE_RE.search(text)
    if match:
        error_type = match.group(1)
    if status_code is None:
        match = _HTTP_STATUS_RE.search(text)
        if match:
            status_code = int(next(group for group in match.groups() if group))
    lowered = text.lower()
    terminal_auth = (
        status_code == 401
        and (
            str(provider_code or "") in _TERMINAL_AUTH_CODES
            or "token has been invalidated" in lowered
            or "invalidated oauth token" in lowered
            or "token_revoked" in lowered
            or "token_invalidated" in lowered
        )
    )
    details: dict[str, object] = {}
    if isinstance(status_code, int):
        details["http_status"] = status_code
    if provider_code:
        details["provider_code"] = str(provider_code)
    if error_type:
        details["error_type"] = str(error_type)
    if terminal_auth:
        details["category"] = "auth"
        details["terminal"] = True
    quota_exhausted = (
        status_code == 429
        and (
            str(error_type or "") in _TERMINAL_QUOTA_TYPES
            or "usage_limit_reached" in lowered
            or "insufficient_quota" in lowered
            or "exceeded your current quota" in lowered
        )
    )
    if quota_exhausted:
        details["category"] = "quota"
        details["terminal"] = True
        match = _RESETS_IN_SECONDS_RE.search(text)
        if match:
            details["retry_after_seconds"] = int(match.group(1))
    return details


def make_model_api_probe(model_client) -> "ProbeFunc":
    """Probe the model API with a minimal 1-token completion."""

    def _probe() -> ProbeResult:
        try:
            t0 = time.monotonic()
            # Pass only the arguments every model client supports.
            # Clients like CodexResponsesModelClient don't accept max_tokens.
            create_kwargs: dict = {
                "messages": [{"role": "user", "content": [{"type": "text", "text": "ping"}]}],
                "tools": [],
            }
            import inspect as _inspect
            if "max_tokens" in _inspect.signature(model_client.create).parameters:
                create_kwargs["max_tokens"] = 1
            model_client.create(**create_kwargs)
            return ProbeResult(
                service_id="model_api",
                ok=True,
                latency_ms=(time.monotonic() - t0) * 1000,
            )
        except Exception as exc:
            return ProbeResult(service_id="model_api", ok=False, error=str(exc), details=_model_probe_error_details(exc))

    setattr(_probe, "service_id", "model_api")
    return _probe


def make_telegram_probe(bot_application) -> "ProbeFunc":
    """Probe Telegram connectivity without touching the live polling client.

    python-telegram-bot's Application owns an HTTPX client bound to its polling
    lifecycle. Reusing that same bot object from a background probe can leave
    the live client in a bad state after loop shutdown/rebuild edges, producing
    ``RuntimeError('Event loop is closed')`` on real replies. A short-lived Bot
    keeps the health check isolated from message delivery.
    """

    async def _probe() -> ProbeResult:
        try:
            t0 = time.monotonic()
            from telegram import Bot  # type: ignore[import]

            token = getattr(getattr(bot_application, "bot", None), "token", None)
            if not isinstance(token, str) or not token:
                raise RuntimeError("Telegram bot token unavailable for probe")
            from nullion.messaging_adapters import retry_messaging_delivery_operation
            from nullion.telegram_transport import build_telegram_bot

            async with build_telegram_bot(Bot, token) as bot:
                await retry_messaging_delivery_operation(lambda: bot.get_me())
            return ProbeResult(
                service_id="telegram_bot",
                ok=True,
                latency_ms=(time.monotonic() - t0) * 1000,
            )
        except Exception as exc:
            return ProbeResult(service_id="telegram_bot", ok=False, error=str(exc))

    setattr(_probe, "service_id", "telegram_bot")
    return _probe


# Re-export ProbeResult for convenience
ProbeFunc = Callable[[], ProbeResult | Awaitable[ProbeResult]]

__all__ = ["ProbeResult", "make_model_api_probe", "make_telegram_probe"]
