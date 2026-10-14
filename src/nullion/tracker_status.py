"""Durable scheduled-check health and notifications, without another source check.

Only the stored conditional-alert policy opts a job into tracker notifications.
Run outcomes are typed producer facts; neither task prose nor reply prose routes
this feature. Notification claims are shared by processes using the same DB.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import unicodedata
from typing import Callable, Iterable
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)
DAY = timedelta(days=1)
MISSED_CHECK_GRACE = timedelta(minutes=10)
SUCCESS = frozenset({"match", "no_match", "succeeded"})


def _utc(value: datetime | None = None) -> datetime:
    value = value or datetime.now(timezone.utc)
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _date(value: object) -> datetime | None:
    try:
        return _utc(datetime.fromisoformat(str(value))) if value else None
    except (TypeError, ValueError):
        return None


def is_tracker(job: object) -> bool:
    options = getattr(job, "artifact_delivery_options", {})
    return isinstance(options, dict) and str(options.get("alert_delivery_mode") or "").strip().lower() == "verified_matches_only"


def notifications_enabled(job: object) -> bool:
    options = getattr(job, "artifact_delivery_options", {})
    return is_tracker(job) and options.get("tracker_status_notifications", True) is not False


def _scope(job: object) -> str:
    # Editing the monitored condition or recipient invalidates prior checks.
    descriptor = {key: getattr(job, key, None) for key in
                  ("id", "workspace_id", "task", "delivery_channel", "delivery_target")}
    descriptor["tracker"] = is_tracker(job)
    descriptor["requirements"] = {k: v for k, v in (getattr(job, "artifact_delivery_options", {}) or {}).items()
                                  if k != "tracker_status_notifications"}
    return hashlib.sha256(json.dumps(descriptor, sort_keys=True).encode()).hexdigest()


@contextmanager
def _db(path: Path, *, write: bool = True):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=5)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE IF NOT EXISTS cron_tracker_runs (run_id TEXT PRIMARY KEY, scope TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL)")
        conn.execute("CREATE INDEX IF NOT EXISTS cron_tracker_runs_scope_time ON cron_tracker_runs(scope, started_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS cron_tracker_runs_scope_finished ON cron_tracker_runs(scope, finished_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS cron_tracker_runs_started ON cron_tracker_runs(started_at)")
        conn.execute("CREATE TABLE IF NOT EXISTS cron_tracker_state (scope TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS cron_tracker_notices (notice_key TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        conn.commit()
        conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def health_store_path(runtime: object) -> Path | None:
    path = getattr(runtime, "checkpoint_path", None)
    if path is None:
        return None
    path = Path(path)
    return path if path.suffix.lower() in {".db", ".sqlite", ".sqlite3"} else None


def _state(conn: sqlite3.Connection, scope: str) -> dict:
    row = conn.execute("SELECT payload FROM cron_tracker_state WHERE scope=?", (scope,)).fetchone()
    return json.loads(row[0]) if row else {}


def _save(conn: sqlite3.Connection, scope: str, state: dict) -> None:
    conn.execute("INSERT INTO cron_tracker_state VALUES (?,?) ON CONFLICT(scope) DO UPDATE SET payload=excluded.payload", (scope, json.dumps(state)))


def start_check(path: Path, job: object, run_id: str, *, now: datetime | None = None) -> None:
    now = _utc(now)
    with _db(path) as conn:
        conn.execute("INSERT OR IGNORE INTO cron_tracker_runs VALUES (?,?,?,NULL,'running')", (run_id, _scope(job), now.isoformat()))


def finish_check(path: Path, job: object, run_id: str, result: dict, *, now: datetime | None = None) -> None:
    now = _utc(now)
    scope = _scope(job)
    outcome = result.get("execution_outcome")
    verified = (isinstance(outcome, dict) and outcome.get("execution_succeeded") is True
                and result.get("response_fulfilled") is not False
                and not any(result.get(k) for k in ("cron_report_failed", "cron_run_failed", "cron_delivery_failed"))
                and (result.get("cron_source_verification_required") is not True
                     or result.get("cron_source_verification_satisfied") is True))
    if result.get("suspended_for_approval"):
        status = "waiting_for_approval"
    elif result.get("cron_run_cancelled"):
        status = "cancelled"
    elif result.get("cron_delivery_failed"):
        status = "delivery_failed"
    elif verified and is_tracker(job):
        reason = result.get("cron_alert_delivery_reason")
        decision = result.get("cron_alert_delivery_decision")
        status = ("match" if decision == "send" and reason == "verified_match" else
                  "no_match" if decision == "silent" and reason == "no_verified_match" else "incomplete")
    else:
        status = "succeeded" if verified else "incomplete"
    with _db(path) as conn:
        row = conn.execute("SELECT started_at,finished_at FROM cron_tracker_runs WHERE run_id=? AND scope=?", (run_id, scope)).fetchone()
        if not row or row["finished_at"]:
            return
        conn.execute("UPDATE cron_tracker_runs SET finished_at=?,status=? WHERE run_id=?", (now.isoformat(), status, run_id))
        state = _state(conn, scope)
        # Concurrent manual/automatic checks must not overwrite a newer attempt.
        if row["started_at"] >= str(state.get("last_attempt_at") or ""):
            state.update(last_attempt_at=row["started_at"], last_finished_at=now.isoformat(), outcome=status)
            if status in SUCCESS:
                state.update(last_success_at=now.isoformat(), consecutive_failures=0, failure_since=None)
            elif status in {"incomplete", "delivery_failed"}:
                state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
                state.setdefault("failure_since", row["started_at"])
                if not state.get("failure_since"):
                    state["failure_since"] = row["started_at"]
            else:
                state.update(consecutive_failures=0, failure_since=None)
            _save(conn, scope, state)
        # Keep bounded run history; the compact last-success state survives.
        conn.execute("DELETE FROM cron_tracker_runs WHERE started_at<?", ((now - timedelta(days=30)).isoformat(),))


def _status(conn: sqlite3.Connection, job: object, now: datetime) -> dict:
    scope = _scope(job)
    state = _state(conn, scope)
    recent = conn.execute("SELECT status,COUNT(*) AS n FROM cron_tracker_runs WHERE scope=? AND finished_at>=? GROUP BY status", (scope, (now - DAY).isoformat())).fetchall()
    counts = {row["status"]: row["n"] for row in recent}
    running = conn.execute("SELECT MAX(started_at) FROM cron_tracker_runs WHERE scope=? AND finished_at IS NULL", (scope,)).fetchone()[0]
    if running and running <= str(state.get("last_attempt_at") or ""):
        # A newer completed check supersedes an orphan left by a prior process.
        running = None
    last_attempt = max(filter(None, (state.get("last_attempt_at"), running)), default=None)
    next_check = _date(getattr(job, "next_run", None))
    overdue = ((running and now - _date(running) >= MISSED_CHECK_GRACE)
               or (next_check and now - next_check >= MISSED_CHECK_GRACE))
    if not getattr(job, "enabled", False):
        health = "paused"
    elif state.get("outcome") == "waiting_for_approval" and not running:
        health = "waiting_for_approval"
    elif overdue or int(state.get("consecutive_failures", 0)) >= 3:
        health = "needs_attention"
    elif running:
        health = "running"
    else:
        health = str(state.get("outcome") or "not_checked")
    return {"is_tracker": is_tracker(job), "notifications_enabled": notifications_enabled(job),
            "state": health, "last_attempt_at": last_attempt,
            "last_success_at": state.get("last_success_at"),
            "next_check_at": getattr(job, "next_run", None) if getattr(job, "enabled", False) else None,
            "successful_checks_24h": sum(counts.get(s, 0) for s in SUCCESS),
            "no_match_checks_24h": counts.get("no_match", 0), "matches_24h": counts.get("match", 0),
            "incomplete_checks_24h": counts.get("incomplete", 0) + counts.get("delivery_failed", 0),
            "consecutive_failures": int(state.get("consecutive_failures", 0)),
            "overdue": bool(overdue), "last_outcome": state.get("outcome")}


def tracker_statuses(path: Path | None, jobs: Iterable[object], *, now: datetime | None = None) -> dict[str, dict]:
    """One batched read for the requested scheduler/UI/tool surface only."""
    jobs = tuple(jobs)
    if not jobs or path is None:
        return {}
    with _db(path, write=False) as conn:
        return {str(job.id): _status(conn, job, _utc(now)) for job in jobs}


def _label(value: object) -> str:
    label = " ".join(str(value or "Tracker").split())[:120]
    label = "".join(c for c in label if unicodedata.category(c) not in {"Cc", "Cf"})
    # Render punctuation as data even in transports with inline directive syntax.
    label = label.replace(":", "：")
    # The shared presentation layer owns Markdown escaping. Re-escaping here
    # introduces visible backslashes after multiple platform preparation passes.
    return label


def _time(value: object, job: object) -> str:
    dt = _date(value)
    if dt is None:
        return "Not yet"
    try:
        tz = ZoneInfo(str(getattr(job, "schedule_timezone", "") or "UTC"))
    except (ValueError, KeyError):
        tz = timezone.utc
    return dt.astimezone(tz).strftime("%b %d, %I:%M %p %Z")


def _details(job: object, status: dict) -> str:
    return (f"Last successful check: {_time(status['last_success_at'], job)}\n"
            f"Next check: {_time(status['next_check_at'], job)}")


def _summary_line(job: object, status: dict) -> str:
    state = status["state"]
    if state == "needs_attention":
        explanation = "Needs attention — checks are overdue." if status["overdue"] else "Needs attention — repeated checks could not verify the result."
    elif state == "running":
        explanation = "Checking now."
    elif state == "delivery_failed":
        explanation = "Latest result could not be delivered."
    elif state == "incomplete":
        explanation = "Latest check could not verify the result."
    elif state == "waiting_for_approval":
        explanation = "Waiting for your approval."
    elif state == "cancelled":
        explanation = "Latest check was stopped."
    elif state == "not_checked":
        explanation = "Waiting for the first recorded check."
    elif status["matches_24h"]:
        explanation = f"Active — {status['matches_24h']} verified matching check(s) in the past 24 hours."
    elif status["no_match_checks_24h"]:
        explanation = "Active — no verified matches in the successful checks."
    else:
        explanation = "Active — no new successful checks in the past 24 hours."
    return (f"Tracker: {_label(job.name)}\n{explanation}\n"
            f"Past 24 hours: {status['successful_checks_24h']} successful, {status['incomplete_checks_24h']} incomplete.\n"
            + _details(job, status))


def _claim(conn: sqlite3.Connection, key: str, now: datetime) -> bool:
    row = conn.execute("SELECT payload FROM cron_tracker_notices WHERE notice_key=?", (key,)).fetchone()
    payload = json.loads(row[0]) if row else {}
    if payload.get("sent_at") or (_date(payload.get("retry_at")) or now) > now:
        return False
    payload = {"retry_at": (now + timedelta(minutes=5)).isoformat()}
    conn.execute("INSERT INTO cron_tracker_notices VALUES (?,?) ON CONFLICT(notice_key) DO UPDATE SET payload=excluded.payload", (key, json.dumps(payload)))
    return True


def dispatch_tracker_updates(path: Path | None, jobs: Iterable[object], *,
                             resolve_route: Callable[[object], tuple[str, str]],
                             send: Callable[[object, str, str, str, str], bool],
                             current_jobs: Callable[[], Iterable[object]] | None = None,
                             now: datetime | None = None) -> int:
    """Claim and deliver one daily digest per workspace/recipient plus transitions.

    Failed deliveries retry after five minutes. Claims prevent concurrent sends;
    an interrupted remote send can be retried because platforms offer no shared
    exactly-once receipt. A delivered warning is the prerequisite for recovery.
    """
    now = _utc(now)
    jobs = tuple(j for j in jobs if getattr(j, "enabled", False) and notifications_enabled(j))
    if not jobs or path is None:
        return 0
    groups = defaultdict(list)
    for job in jobs:
        channel, target = resolve_route(job)
        if channel and target:
            groups[(str(job.workspace_id), channel, target)].append(job)
    pending = []
    with _db(path) as conn:
        for route, group in groups.items():
            statuses = {j.id: _status(conn, j, now) for j in group}
            for job in group:
                status = statuses[job.id]
                scope = _scope(job)
                state = _state(conn, scope)
                notification_scope = scope + ":route:" + hashlib.sha256(json.dumps(route).encode()).hexdigest()
                notification_state = _state(conn, notification_scope)
                kind = None
                if status["state"] == "needs_attention" and not notification_state.get("warning_sent_at"):
                    kind = "warning"
                    anchor = state.get("failure_since") or status["last_attempt_at"] or job.next_run
                    text = f"Tracker needs attention: {_label(job.name)}\n\n"
                    text += ("A scheduled check is overdue. The monitored condition is not confirmed." if status["overdue"] else
                             "The latest result could not be delivered." if status["last_outcome"] == "delivery_failed" else
                             f"The last {status['consecutive_failures']} checks could not verify the result.")
                elif notification_state.get("warning_sent_at") and status["state"] in SUCCESS:
                    kind = "recovery"
                    anchor = notification_state["warning_sent_at"]
                    text = f"Tracker recovered: {_label(job.name)}\n\nThe latest check completed successfully."
                    text += " No verified match was found." if status["state"] == "no_match" else ""
                if kind:
                    key = f"{notification_scope}:{kind}:{anchor}"
                    if _claim(conn, key, now):
                        pending.append((key, notification_scope, kind, job, route, text + "\n\n" + _details(job, status)))
            group_key = "daily:" + hashlib.sha256(json.dumps(route).encode()).hexdigest()
            state = _state(conn, group_key)
            if not state.get("daily_since"):
                state["daily_since"] = now.isoformat()
                _save(conn, group_key, state)
            elif now - _date(state["daily_since"]) >= DAY:
                key = group_key + ":" + state["daily_since"]
                if _claim(conn, key, now):
                    text = "Daily tracker summary\n\n" + "\n\n".join(_summary_line(j, statuses[j.id]) for j in group)
                    pending.append((key, group_key, "daily", group[0], route, text))
    sent = 0
    for key, scope, kind, job, route, text in pending:
        if current_jobs is not None:
            current = tuple(j for j in current_jobs() if getattr(j, "enabled", False) and notifications_enabled(j)
                            and (str(j.workspace_id), *resolve_route(j)) == route)
            if kind == "daily":
                if not current:
                    continue
                job = current[0]
                with _db(path, write=False) as conn:
                    text = "Daily tracker summary\n\n" + "\n\n".join(_summary_line(j, _status(conn, j, now)) for j in current)
            elif not any(_scope(j) == _scope(job) for j in current):
                continue
        if kind != "daily":
            with _db(path, write=False) as conn:
                latest = _status(conn, job, now)
            if kind == "warning" and latest["state"] != "needs_attention":
                continue
            if kind == "recovery" and latest["state"] not in SUCCESS:
                continue
        try:
            delivered = send(job, route[1], route[2], text, kind) is True
        except Exception:
            log.warning("Tracker status delivery failed", exc_info=True)
            delivered = False
        if delivered:
            with _db(path) as conn:
                conn.execute("UPDATE cron_tracker_notices SET payload=? WHERE notice_key=?", (json.dumps({"sent_at": now.isoformat()}), key))
                state = _state(conn, scope)
                if kind == "daily":
                    state["daily_since"] = now.isoformat()
                else:
                    state["warning_sent_at"] = now.isoformat() if kind == "warning" else None
                _save(conn, scope, state)
            sent += 1
    return sent
