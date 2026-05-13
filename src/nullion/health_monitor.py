"""Background health monitor — probes services and creates targeted doctor actions.

Runs every 30 seconds as an asyncio background task. Each registered probe is
called in a thread-pool executor so blocking I/O (HTTP pings, etc.) doesn't
stall the event loop. The monitor escalates to a doctor action card only after
2 consecutive failures — single transient blips are swallowed silently.

Usage (in telegram_app.py run_polling startup)::

    monitor = HealthMonitor(runtime=service.runtime, settings=settings)
    monitor.register_probe(make_model_api_probe(model_client))
    monitor.register_probe(make_telegram_probe(application))
    await monitor.start()
    # ... polling ...
    await monitor.stop()
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Awaitable, Callable, TypedDict

from langgraph.graph import END, START, StateGraph

logger = logging.getLogger(__name__)

PROBE_INTERVAL_SECONDS = 30
_ESCALATION_THRESHOLD = 2   # consecutive failures before creating a doctor action
_DEFAULT_TERMINAL_AUTH_BACKOFF_SECONDS = 15 * 60
_DEFAULT_TERMINAL_QUOTA_BACKOFF_SECONDS = 15 * 60
_MODEL_RECOVERY_RECOMMENDATION_CODES = {
    "model_api_unreachable",
    "model_quota_exhausted",
}


@dataclass(slots=True)
class ProbeResult:
    service_id: str          # e.g. "model_api", "telegram_bot", "plugin:gmail"
    ok: bool
    latency_ms: float | None = None
    error: str | None = None
    details: dict | None = None


ProbeFunc = Callable[[], ProbeResult | Awaitable[ProbeResult]]


class HealthMonitor:
    """Manages background service probes and escalates failures to doctor actions."""

    def __init__(self, *, runtime, settings=None) -> None:
        self._runtime = runtime
        self._settings = settings
        self._task: asyncio.Task | None = None
        self._probes: list[ProbeFunc] = []
        self._failure_counts: dict[str, int] = {}
        self._probe_paused_until: dict[str, float] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def register_probe(self, probe_fn: ProbeFunc) -> None:
        """Add a probe function.

        Synchronous probes are called in a thread executor. Async probes are
        awaited on the monitor's event loop so loop-affine clients such as
        python-telegram-bot's HTTPX client are not touched from a fresh loop.
        """
        self._probes.append(probe_fn)

    async def start(self) -> None:
        """Start the background monitoring loop."""
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_loop(), name="health-monitor")
        logger.info("Health monitor started (%d probes)", len(self._probes))

    async def stop(self) -> None:
        """Cancel the background loop and wait for it to finish."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Health monitor stopped")

    # ── Internal loop ─────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        while True:
            await asyncio.sleep(PROBE_INTERVAL_SECONDS)
            await self._run_all_probes()

    async def _run_all_probes(self) -> None:
        loop = asyncio.get_event_loop()
        for probe in self._probes:
            service_id = getattr(probe, "service_id", None)
            if isinstance(service_id, str) and self._probe_is_paused(service_id):
                continue
            try:
                if inspect.iscoroutinefunction(probe):
                    result = await probe()
                else:
                    result = await loop.run_in_executor(None, probe)
            except Exception as exc:
                result = ProbeResult(service_id="unknown", ok=False, error=str(exc))
            self._handle_result(result)

    def _handle_result(self, result: ProbeResult) -> None:
        _compiled_health_result_graph().invoke({"monitor": self, "result": result})

    def _probe_is_paused(self, service_id: str) -> bool:
        paused_until = self._probe_paused_until.get(service_id)
        if paused_until is None:
            return False
        if time.monotonic() < paused_until:
            return True
        self._probe_paused_until.pop(service_id, None)
        return False

    def _pause_probe(self, service_id: str, seconds: float) -> None:
        self._probe_paused_until[service_id] = time.monotonic() + max(0.0, seconds)

    def _escalate(self, result: ProbeResult) -> None:
        """Attempt auto-heal; if that fails, create a doctor action card."""
        from nullion.remediation import playbook_for_service
        details = result.details if isinstance(result.details, dict) else {}
        playbook_service_id = (
            "model_quota"
            if result.service_id == "model_api" and details.get("category") == "quota"
            else result.service_id
        )
        playbook = playbook_for_service(playbook_service_id)
        if playbook is None:
            logger.debug("Health monitor: no playbook for service %s — skipping escalation", playbook_service_id)
            return

        if playbook.auto_heal_fn is not None:
            try:
                healed = playbook.auto_heal_fn(self._runtime, result)
                if healed:
                    logger.info("Health monitor: auto-healed %s", result.service_id)
                    self._failure_counts.pop(result.service_id, None)
                    return
            except Exception as exc:
                logger.warning("Health monitor: auto-heal failed for %s: %s", result.service_id, exc)

        # Auto-heal didn't work — surface a doctor action card
        try:
            self._runtime.report_health_issue(
                issue_type=playbook.issue_type,
                source=result.service_id,
                message=playbook.summary,
                details={
                    "service_id": result.service_id,
                    "playbook_service_id": playbook_service_id,
                    "error": result.error or "",
                    "recommendation_code": playbook.recommendation_code,
                    "remediation_actions": playbook.button_labels,
                },
            )
            logger.info(
                "Health monitor: escalated %s to doctor action (%s)",
                result.service_id,
                playbook.recommendation_code,
            )
        except Exception as exc:
            logger.warning("Health monitor: failed to create doctor action for %s: %s", result.service_id, exc)

    def _auto_resolve_doctor_action(self, service_id: str) -> None:
        """Cancel any pending doctor action for this service now that it has recovered."""
        try:
            cleared = clear_recovered_service_doctor_actions(
                self._runtime,
                service_id,
                reason=f"{service_id} recovered automatically",
            )
            if cleared:
                logger.info("Health monitor: auto-resolved %d doctor action(s) for %s", cleared, service_id)
        except Exception as exc:
            logger.warning("Health monitor: failed to auto-resolve action for %s: %s", service_id, exc)


def _doctor_action_matches_recovered_service(action: dict, service_id: str) -> bool:
    if str(action.get("status") or "").strip().lower() != "pending":
        return False
    normalized_service_id = str(service_id or "").strip().lower()
    details = action.get("details") or {}
    details_service_id = str(details.get("service_id") or "").strip().lower() if isinstance(details, dict) else ""
    if details_service_id == normalized_service_id:
        return True
    recommendation_code = str(action.get("recommendation_code") or "").strip().lower()
    if normalized_service_id == "model_api" and recommendation_code in _MODEL_RECOVERY_RECOMMENDATION_CODES:
        return True
    text_fields = " ".join(
        str(action.get(field) or "")
        for field in ("reason", "source_reason", "error")
    ).lower()
    return bool(normalized_service_id and f"service_id={normalized_service_id}" in text_fields)


def clear_recovered_service_doctor_actions(runtime, service_id: str, *, reason: str | None = None) -> int:
    """Close stale Doctor service actions after a verified recovery signal."""
    cleared = 0
    for action in list(runtime.store.list_doctor_actions()):
        if not _doctor_action_matches_recovered_service(action, service_id):
            continue
        runtime.cancel_doctor_action(
            str(action["action_id"]),
            reason=reason or f"{service_id} recovered",
        )
        cleared += 1
    return cleared


class _HealthResultState(TypedDict, total=False):
    monitor: HealthMonitor
    result: ProbeResult
    service_id: str
    previous_failures: int
    failure_count: int
    terminal_failure: bool


def _terminal_auth_backoff_seconds() -> float:
    raw = os.environ.get("NULLION_HEALTH_MONITOR_AUTH_BACKOFF_SECONDS")
    if raw is None:
        return float(_DEFAULT_TERMINAL_AUTH_BACKOFF_SECONDS)
    try:
        return max(0.0, float(raw))
    except ValueError:
        return float(_DEFAULT_TERMINAL_AUTH_BACKOFF_SECONDS)


def _terminal_quota_backoff_seconds(result: ProbeResult) -> float:
    details = result.details if isinstance(result.details, dict) else {}
    retry_after = details.get("retry_after_seconds")
    if isinstance(retry_after, (int, float)):
        return max(0.0, float(retry_after) + 5.0)
    raw = os.environ.get("NULLION_HEALTH_MONITOR_QUOTA_BACKOFF_SECONDS")
    if raw is None:
        return float(_DEFAULT_TERMINAL_QUOTA_BACKOFF_SECONDS)
    try:
        return max(0.0, float(raw))
    except ValueError:
        return float(_DEFAULT_TERMINAL_QUOTA_BACKOFF_SECONDS)


def _is_terminal_model_failure(result: ProbeResult) -> bool:
    if result.service_id != "model_api":
        return False
    details = result.details if isinstance(result.details, dict) else {}
    if details.get("category") == "auth" and details.get("terminal") is True:
        return True
    if details.get("category") == "quota" and details.get("terminal") is True:
        return True
    if details.get("http_status") == 401 and str(details.get("provider_code") or "") in {
        "token_invalidated",
        "token_revoked",
        "invalid_api_key",
        "invalid_token",
    }:
        return True
    return False


def _health_result_start_node(state: _HealthResultState) -> dict[str, object]:
    result = state["result"]
    return {"service_id": result.service_id}


def _health_result_route_ok(state: _HealthResultState) -> str:
    return "success" if state["result"].ok else "failure"


def _health_result_success_node(state: _HealthResultState) -> dict[str, object]:
    service_id = state["service_id"]
    monitor = state["monitor"]
    previous_failures = monitor._failure_counts.pop(service_id, 0)
    monitor._probe_paused_until.pop(service_id, None)
    return {"previous_failures": previous_failures}


def _health_result_route_success(state: _HealthResultState) -> str:
    return "auto_resolve" if int(state.get("previous_failures") or 0) >= _ESCALATION_THRESHOLD else END


def _health_result_auto_resolve_node(state: _HealthResultState) -> dict[str, object]:
    service_id = state["service_id"]
    previous_failures = int(state.get("previous_failures") or 0)
    state["monitor"]._auto_resolve_doctor_action(service_id)
    logger.info("Health monitor: %s recovered after %d consecutive failures", service_id, previous_failures)
    return {}


def _health_result_failure_node(state: _HealthResultState) -> dict[str, object]:
    service_id = state["service_id"]
    monitor = state["monitor"]
    count = monitor._failure_counts.get(service_id, 0) + 1
    terminal_failure = _is_terminal_model_failure(state["result"])
    if terminal_failure:
        count = max(count, _ESCALATION_THRESHOLD)
    monitor._failure_counts[service_id] = count
    logger.warning(
        "Health monitor: %s probe failed (consecutive=%d, error=%s)",
        service_id,
        count,
        state["result"].error,
    )
    if terminal_failure:
        details = state["result"].details if isinstance(state["result"].details, dict) else {}
        backoff_seconds = (
            _terminal_quota_backoff_seconds(state["result"])
            if details.get("category") == "quota"
            else _terminal_auth_backoff_seconds()
        )
        monitor._pause_probe(service_id, backoff_seconds)
        logger.warning(
            "Health monitor: %s probe paused for %.0fs after terminal model failure",
            service_id,
            backoff_seconds,
        )
    return {"failure_count": count, "terminal_failure": terminal_failure}


def _health_result_route_failure(state: _HealthResultState) -> str:
    count = int(state.get("failure_count") or 0)
    return "escalate" if count == _ESCALATION_THRESHOLD else END


def _health_result_escalate_node(state: _HealthResultState) -> dict[str, object]:
    state["monitor"]._escalate(state["result"])
    return {}


@lru_cache(maxsize=1)
def _compiled_health_result_graph():
    graph = StateGraph(_HealthResultState)
    graph.add_node("start", _health_result_start_node)
    graph.add_node("success", _health_result_success_node)
    graph.add_node("auto_resolve", _health_result_auto_resolve_node)
    graph.add_node("failure", _health_result_failure_node)
    graph.add_node("escalate", _health_result_escalate_node)
    graph.add_edge(START, "start")
    graph.add_conditional_edges(
        "start",
        _health_result_route_ok,
        {"success": "success", "failure": "failure"},
    )
    graph.add_conditional_edges(
        "success",
        _health_result_route_success,
        {"auto_resolve": "auto_resolve", END: END},
    )
    graph.add_conditional_edges(
        "failure",
        _health_result_route_failure,
        {"escalate": "escalate", END: END},
    )
    graph.add_edge("auto_resolve", END)
    graph.add_edge("escalate", END)
    return graph.compile()


__all__ = ["HealthMonitor", "ProbeFunc", "ProbeResult"]
