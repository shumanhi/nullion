"""Owned synchronous connection pools; no request replay or global credentials."""
from __future__ import annotations

from contextlib import ExitStack
import logging
import threading
from typing import Any, Callable
import weakref

import httpx


def close_http_error_response(error: Any) -> None:
    """Release a caught urllib error's body without replacing its failure."""
    try:
        error.close()
    except (OSError, ValueError):
        logging.getLogger(__name__).debug("HTTP error response cleanup failed")

# Longer than the tray's normal ten-second polling gap. Keep the server idle
# timeout longer than the client expiry so the client retires idle sockets first.
HTTP_KEEPALIVE_EXPIRY_SECONDS = 60.0
WEB_KEEPALIVE_TIMEOUT_SECONDS = 75


def http_pool_limits(*, tray: bool = False) -> httpx.Limits:
    return httpx.Limits(
        max_connections=2 if tray else 100,
        max_keepalive_connections=1 if tray else 20,
        keepalive_expiry=HTTP_KEEPALIVE_EXPIRY_SECONDS,
    )


class SharedHttpClient:
    """Lazy pool shared by model variants, closed on explicit shutdown or GC.

    Initialization alone is serialized; HTTPX handles concurrent requests. The
    owner carries no headers or credentials. Callers supply those per request.
    Closing one shared owner retires all its variants; it cannot reopen itself.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._client: Any = None
        self._closed = False
        self._stack = ExitStack()
        self._finalizer = weakref.finalize(self, self._stack.close)

    def get(self, factory: Callable[[], Any]) -> Any:
        with self._lock:
            if self._closed:
                raise RuntimeError("HTTP client is closed")
            if self._client is None:
                self._client = self._stack.enter_context(factory())
            return self._client

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._finalizer()
