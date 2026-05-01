"""Standard health probes for built-in Nullion services.

Each probe is a zero-arg callable that returns a ProbeResult.
Use the factory functions to build probes that capture the target
(model_client, bot application) in a closure.
"""
from __future__ import annotations

import logging
import time
from typing import Awaitable, Callable

from nullion.health_monitor import ProbeResult

logger = logging.getLogger(__name__)


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
            return ProbeResult(service_id="model_api", ok=False, error=str(exc))

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
            async with Bot(token) as bot:
                await bot.get_me()
            return ProbeResult(
                service_id="telegram_bot",
                ok=True,
                latency_ms=(time.monotonic() - t0) * 1000,
            )
        except Exception as exc:
            return ProbeResult(service_id="telegram_bot", ok=False, error=str(exc))

    return _probe


# Re-export ProbeResult for convenience
ProbeFunc = Callable[[], ProbeResult | Awaitable[ProbeResult]]

__all__ = ["ProbeResult", "make_model_api_probe", "make_telegram_probe"]
