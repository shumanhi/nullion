"""Builder operational route observations.

These records are not user memory. They are compact facts about measured tool
routes so future turns can prefer routes that recently worked and avoid routes
that recently failed or stalled.
"""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from typing import Any, Iterable
from urllib.parse import urlparse
from uuid import uuid4


MAX_ROUTE_OBSERVATIONS = 300
MAX_ROUTE_HINTS = 6
SLOW_TOOL_MS = 8_000
_WEB_ROUTE_TOOLS = frozenset(
    {
        "web_search",
        "web_fetch",
        "browser_navigate",
        "browser_extract_detail",
        "browser_extract_items",
        "browser_extract_text",
    }
)
_OPERATIONAL_ROUTE_TOOLS = frozenset(
    {
        "create_cron",
        "delete_cron",
        "delete_reminder",
        "disable_cron",
        "enable_cron",
        "list_crons",
        "list_reminders",
        "pause_cron",
        "resume_cron",
        "run_cron",
        "set_reminder",
        "toggle_cron",
        "update_cron",
        "update_reminder",
    }
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _short_text(value: object, *, limit: int = 220) -> str | None:
    if value is None:
        return None
    text = str(value).strip().replace("\n", " ")
    return text[:limit] if text else None


def _source_domain_from_url(raw_url: object) -> str | None:
    if not isinstance(raw_url, str) or not raw_url.strip():
        return None
    parsed = urlparse(raw_url.strip())
    if parsed.scheme not in {"http", "https"}:
        return None
    return parsed.netloc.lower()


def _source_domain_from_output(output: dict[str, object]) -> str | None:
    for key in ("url", "source_url", "final_url"):
        domain = _source_domain_from_url(output.get(key))
        if domain:
            return domain
    result = output.get("result")
    if isinstance(result, str):
        for token in result.split():
            domain = _source_domain_from_url(token.strip("()[]{}.,"))
            if domain:
                return domain
    candidates = output.get("candidates")
    if isinstance(candidates, list):
        for candidate in candidates:
            if isinstance(candidate, dict):
                domain = _source_domain_from_url(candidate.get("url"))
                if domain:
                    return domain
    return None


def _observation_reason(status: str, output: dict[str, object], error: str | None, duration_ms: float) -> str | None:
    if error:
        return _short_text(error)
    reason = output.get("reason")
    if reason:
        return _short_text(reason)
    if status not in {"completed", "approved"}:
        return status
    if duration_ms >= SLOW_TOOL_MS:
        return "slow_tool_route"
    return None


def _route_key(tool_name: str, source_domain: str | None) -> str:
    source = source_domain or "unknown_source"
    return f"{tool_name}:{source}"


def _observation_id(route_key: str, created_at: str, invocation_id: str) -> str:
    digest = hashlib.sha256(f"{route_key}:{created_at}:{invocation_id}".encode("utf-8")).hexdigest()[:16]
    return f"route-{digest}"


def build_route_observation(
    *,
    invocation: Any,
    result: Any,
    duration_ms: float,
    capability_tags: Iterable[str] = (),
    created_at: str | None = None,
) -> dict[str, object] | None:
    tool_name = str(getattr(invocation, "tool_name", "") or getattr(result, "tool_name", "") or "").strip()
    if not tool_name or tool_name in _OPERATIONAL_ROUTE_TOOLS:
        return None
    output = getattr(result, "output", None)
    output = output if isinstance(output, dict) else {}
    arguments = getattr(invocation, "arguments", None)
    arguments = arguments if isinstance(arguments, dict) else {}
    status = str(getattr(result, "status", "") or "").strip() or "unknown"
    source_domain = _source_domain_from_url(arguments.get("url")) or _source_domain_from_output(output)
    route_key = _route_key(tool_name, source_domain)
    created = created_at or _now_iso()
    error = _short_text(getattr(result, "error", None))
    reason = _observation_reason(status, output, error, duration_ms)
    return {
        "observation_id": _observation_id(route_key, created, str(getattr(result, "invocation_id", "") or uuid4().hex)),
        "created_at": created,
        "route_key": route_key,
        "tool_name": tool_name,
        "source_domain": source_domain,
        "status": status,
        "reason": reason,
        "duration_ms": round(max(0.0, float(duration_ms)), 1),
        "principal_id": _short_text(getattr(invocation, "principal_id", None), limit=80),
        "capsule_id": _short_text(getattr(invocation, "capsule_id", None), limit=80),
        "capability_tags": sorted({str(tag) for tag in capability_tags if str(tag or "").strip()}),
        "output_shape": sorted(str(key) for key in output.keys())[:20],
    }


def trim_route_observations(observations: Iterable[dict[str, object]], *, limit: int = MAX_ROUTE_OBSERVATIONS) -> list[dict[str, object]]:
    rows = [dict(row) for row in observations if isinstance(row, dict)]
    rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    return rows[: max(0, limit)]


def route_hints_for_prompt(observations: Iterable[dict[str, object]], *, available_tools: Iterable[str] = ()) -> str | None:
    available = {str(name) for name in available_tools if str(name or "").strip()}
    latest_by_key: dict[str, dict[str, object]] = {}
    for row in trim_route_observations(observations):
        tool_name = str(row.get("tool_name") or "")
        if available and tool_name and tool_name not in available:
            continue
        route_key = str(row.get("route_key") or "")
        if route_key and route_key not in latest_by_key:
            latest_by_key[route_key] = row
        if len(latest_by_key) >= MAX_ROUTE_HINTS:
            break
    if not latest_by_key:
        return None
    lines = [
        "Recent measured tool-route outcomes. These are operational hints, not user memory or routing rules:",
        "- Use these hints only to choose among already-appropriate tools/routes; do not introduce a new tool family or side-effectful tool solely because of a route hint.",
        "- Prefer recently successful lower-latency routes when they can satisfy the same typed tool need.",
        "- Avoid repeating recently failed, denied, blocked, or slow routes unless no better structured route is available.",
    ]
    for row in latest_by_key.values():
        tool = str(row.get("tool_name") or "tool")
        source = str(row.get("source_domain") or "unknown source")
        status = str(row.get("status") or "unknown")
        duration = row.get("duration_ms")
        reason = str(row.get("reason") or "").strip()
        detail = f"{tool} via {source}: {status}"
        if isinstance(duration, (int, float)):
            detail += f" in {duration:.0f} ms"
        if reason:
            detail += f" ({reason[:120]})"
        lines.append(f"- {detail}")
    return "\n".join(lines)


__all__ = [
    "MAX_ROUTE_OBSERVATIONS",
    "build_route_observation",
    "route_hints_for_prompt",
    "trim_route_observations",
]
