"""Optional latency tracing hooks with no hard ddtrace dependency."""

from __future__ import annotations

from contextlib import contextmanager
import os
from typing import Iterator


def latency_tracing_enabled() -> bool:
    raw = os.environ.get("NULLION_LATENCY_TRACE_ENABLED") or os.environ.get("NULLION_DD_TRACE_LATENCY")
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


@contextmanager
def latency_span(name: str, **tags: object) -> Iterator[None]:
    if not latency_tracing_enabled():
        yield
        return
    try:
        from ddtrace import tracer  # type: ignore[import]
    except Exception:
        yield
        return
    with tracer.trace(name) as span:
        for key, value in tags.items():
            if value is not None:
                span.set_tag(str(key), str(value))
        yield
