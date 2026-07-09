"""Startup warmup for chat surfaces.

The warmup path primes local/runtime caches only. It must not call a model,
send chat messages, checkpoint user-visible state, or write chat history.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import logging
import os
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StartupWarmupResult:
    surface: str
    ok: bool
    elapsed_ms: float
    warmed: tuple[str, ...]
    errors: tuple[str, ...] = ()


_WARMUP_LOCK = threading.Lock()
_WARMED_KEYS: set[tuple[str, str]] = set()


def _warmup_enabled() -> bool:
    raw = os.environ.get("NULLION_CHAT_STARTUP_WARMUP", "1")
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _checkpoint_identity(runtime: object) -> str:
    checkpoint = getattr(runtime, "checkpoint_path", None)
    return str(checkpoint or id(runtime))


def _record_warmup_event(runtime: object, result: StartupWarmupResult) -> None:
    store = getattr(runtime, "store", None)
    add_event = getattr(store, "add_conversation_event", None)
    if not callable(add_event):
        return
    try:
        add_event(
            {
                "event_id": f"startup-warmup:{result.surface}:{int(time.time() * 1000)}",
                "conversation_id": f"system:{result.surface}",
                "event_type": "system.startup_warmup",
                "created_at": datetime.now(UTC).isoformat(),
                "surface": result.surface,
                "ok": result.ok,
                "elapsed_ms": result.elapsed_ms,
                "warmed": list(result.warmed),
                "errors": list(result.errors),
            }
        )
    except Exception:
        logger.debug("startup warmup event recording failed", exc_info=True)


def run_chat_startup_warmup(
    runtime: object,
    *,
    registry: object | None = None,
    settings: object | None = None,
    surface: str = "chat",
    principal_ids: Iterable[str | None] | None = None,
    context_warmers: Iterable[tuple[str, Callable[[], object]]] | None = None,
    record_event: bool = True,
) -> StartupWarmupResult:
    started_at = time.perf_counter()
    warmed: list[str] = []
    errors: list[str] = []

    def step(name: str, fn) -> None:  # noqa: ANN001
        try:
            fn()
            warmed.append(name)
        except Exception as exc:
            errors.append(f"{name}:{type(exc).__name__}")
            logger.debug("startup warmup step failed: %s", name, exc_info=True)

    step("settings", lambda: __import__("nullion.config", fromlist=["load_settings"]).load_settings())
    step("chat_history_store", lambda: __import__("nullion.chat_store", fromlist=["get_chat_store"]).get_chat_store().list_channels())

    if registry is not None:
        step("tool_registry", lambda: registry.list_tool_definitions())
        step(
            "system_context",
            lambda: __import__("nullion.system_context", fromlist=["build_system_context_snapshot"]).build_system_context_snapshot(
                tool_registry=registry
            ),
        )

    def warm_chat_contexts() -> None:
        from nullion import chat_operator

        chat_operator._chat_capability_inventory_prompt(runtime, tool_registry=registry)
        chat_operator._enabled_skill_pack_index_prompt(settings)
        chat_operator._chat_delivery_contract_prompt(runtime, principal_id=None)
        seen_principals: set[str] = set()
        for principal_id in principal_ids or ():
            normalized = str(principal_id or "").strip()
            if not normalized or normalized in seen_principals:
                continue
            seen_principals.add(normalized)
            chat_operator._chat_delivery_contract_prompt(runtime, principal_id=normalized)

    step("chat_context", warm_chat_contexts)

    for warmer_name, warmer in tuple(context_warmers or ()):
        normalized_name = str(warmer_name or "").strip() or "context"
        if callable(warmer):
            step(f"context:{normalized_name}", warmer)

    def warm_tool_scope() -> None:
        from nullion.turn_context_policy import build_turn_tool_evidence, turn_tool_registry_for_evidence

        evidence = build_turn_tool_evidence(user_message="", conversation_result=None)
        if registry is not None:
            turn_tool_registry_for_evidence(
                registry,
                evidence=evidence,
                model_client=None,
                user_message="",
                skip_tool_scope_decision=True,
            )

    step("tool_scope", warm_tool_scope)

    step("cron_metadata", lambda: __import__("nullion.crons", fromlist=["list_crons"]).list_crons(refresh_next_runs=False))

    def warm_graph_modules() -> None:
        import nullion.artifact_workflow_graph  # noqa: F401
        import nullion.attachment_format_graph  # noqa: F401
        import nullion.task_frames  # noqa: F401
        import nullion.task_planner  # noqa: F401
        import nullion.telegram_turn_graph  # noqa: F401

    step("graph_modules", warm_graph_modules)

    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 1)
    result = StartupWarmupResult(
        surface=str(surface or "chat"),
        ok=not errors,
        elapsed_ms=elapsed_ms,
        warmed=tuple(warmed),
        errors=tuple(errors),
    )
    if record_event:
        _record_warmup_event(runtime, result)
    logger.info(
        "chat startup warmup finished surface=%s ok=%s elapsed_ms=%.1f warmed=%s errors=%s",
        result.surface,
        result.ok,
        result.elapsed_ms,
        ",".join(result.warmed),
        ",".join(result.errors),
    )
    return result


def schedule_chat_startup_warmup(
    runtime: object,
    *,
    registry: object | None = None,
    settings: object | None = None,
    surface: str = "chat",
    principal_ids: Iterable[str | None] | None = None,
    context_warmers: Iterable[tuple[str, Callable[[], object]]] | None = None,
) -> threading.Thread | None:
    if not _warmup_enabled():
        return None
    key = (str(surface or "chat"), _checkpoint_identity(runtime))
    with _WARMUP_LOCK:
        if key in _WARMED_KEYS:
            return None
        _WARMED_KEYS.add(key)

    thread = threading.Thread(
        target=run_chat_startup_warmup,
        kwargs={
            "runtime": runtime,
            "registry": registry,
            "settings": settings,
            "surface": surface,
            "principal_ids": tuple(principal_ids or ()),
            "context_warmers": tuple(context_warmers or ()),
        },
        name=f"nullion-{surface or 'chat'}-startup-warmup",
        daemon=True,
    )
    thread.start()
    return thread


__all__ = ["StartupWarmupResult", "run_chat_startup_warmup", "schedule_chat_startup_warmup"]
