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
    return isinstance(options, dict) and options.get("doctor_monitoring_enabled", options.get("tracker_status_notifications", True)) is not False


def health_notifications_enabled(job: object) -> bool:
    return notifications_enabled(job)


def _streak_limit(job: object) -> int:
    value = (getattr(job, "artifact_delivery_options", {}) or {}).get("doctor_streak_limit", 12)
    return value if type(value) is int and 1 <= value <= 1000 else 12


def _route_scope(route: tuple[str, str, str]) -> str:
    return "recipient:" + hashlib.sha256(json.dumps(route).encode()).hexdigest()


def _scope(job: object) -> str:
    # Editing the monitored condition or recipient invalidates prior checks.
    descriptor = {key: getattr(job, key, None) for key in
                  ("id", "workspace_id", "task", "delivery_channel", "delivery_target")}
    descriptor["tracker"] = is_tracker(job)
    descriptor["requirements"] = {k: v for k, v in (getattr(job, "artifact_delivery_options", {}) or {}).items()
                                  if k not in {"tracker_status_notifications", "tracker_health_notifications", "doctor_monitoring_enabled", "doctor_streak_limit"}}
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
        conn.execute("CREATE TABLE IF NOT EXISTS cron_tracker_owners (run_id TEXT PRIMARY KEY, cron_id TEXT NOT NULL, lease_token TEXT NOT NULL)")
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
        from nullion.cron_scheduler_state import current_execution_token
        token = current_execution_token()
        if token:
            conn.execute("INSERT OR IGNORE INTO cron_tracker_owners VALUES (?,?,?)", (run_id, str(job.id), token))


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
            if status == "no_match":
                state["consecutive_no_matches"] = int(state.get("consecutive_no_matches", 0)) + 1
                state["no_match_since"] = state.get("no_match_since") or row["started_at"]
            else:
                state.update(consecutive_no_matches=0, no_match_since=None)
            state["last_issue"] = ("model_timeout" if result.get("reason") == "cron_run_model_timeout" else
                "source_unverified" if result.get("cron_source_verification_required") is True and result.get("cron_source_verification_satisfied") is not True else
                "delivery_failed" if status == "delivery_failed" else "incomplete" if status == "incomplete" else None)
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
        # Count completed runs in start order, including late completions. Keep
        # the latest 1000 per scope so sparse schedules aren't cut off at 30 days.
        history = conn.execute("SELECT status,started_at FROM cron_tracker_runs WHERE scope=? AND finished_at IS NOT NULL ORDER BY started_at DESC,run_id DESC LIMIT 1000", (scope,)).fetchall()
        for field, anchor, outcomes in (("consecutive_no_matches", "no_match_since", {"no_match"}),
                                        ("consecutive_failures", "failure_since", {"incomplete", "delivery_failed", "interrupted"})):
            count = 0
            since = None
            for item in history:
                if item["status"] not in outcomes:
                    break
                count += 1
                since = item["started_at"]
            previous_count = int(state.get(field, 0))
            if count and previous_count and count >= previous_count and state.get(anchor):
                since = state[anchor]
            state[field] = count
            state[anchor] = since
        _save(conn,scope,state)
        conn.execute("DELETE FROM cron_tracker_runs WHERE scope=? AND finished_at IS NOT NULL AND run_id NOT IN (SELECT run_id FROM cron_tracker_runs WHERE scope=? ORDER BY started_at DESC,run_id DESC LIMIT 1000)", (scope,scope))


def _reconcile_interrupted_checks(conn: sqlite3.Connection, job: object, now: datetime) -> None:
    scope = _scope(job)
    rows = conn.execute("""SELECT r.run_id,r.started_at,o.cron_id,o.lease_token FROM cron_tracker_runs r
        LEFT JOIN cron_tracker_owners o ON r.run_id=o.run_id
        WHERE r.scope=? AND r.finished_at IS NULL ORDER BY r.started_at""", (scope,)).fetchall()
    leases_exist = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cron_execution_leases'").fetchone()
    state = _state(conn, scope)
    changed = False
    for row in rows:
        if row["lease_token"]:
            active = leases_exist and conn.execute(
                "SELECT 1 FROM cron_execution_leases WHERE cron_id=? AND token=? AND expires_at>?",
                (row["cron_id"], row["lease_token"], now.isoformat())).fetchone()
            if active:
                continue
        elif now - _date(row["started_at"]) < MISSED_CHECK_GRACE:
            # Legacy/manual fixtures without ownership get a bounded startup grace.
            continue
        conn.execute("UPDATE cron_tracker_runs SET status='interrupted',finished_at=? WHERE run_id=? AND finished_at IS NULL",
                     (now.isoformat(), row["run_id"]))
        changed = True
        if row["started_at"] >= str(state.get("last_attempt_at") or ""):
            state.update(last_attempt_at=row["started_at"], last_finished_at=now.isoformat(), outcome="interrupted",
                         last_issue="interrupted", consecutive_no_matches=0, no_match_since=None,
                         consecutive_failures=int(state.get("consecutive_failures", 0)) + 1,
                         failure_since=state.get("failure_since") or row["started_at"])
    if changed:
        _save(conn, scope, state)


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
    elif running:
        # Elapsed time is not evidence that an active source check failed.
        health = "running"
    elif int(state.get("consecutive_failures", 0)) >= _streak_limit(job):
        health = "needs_attention"
    elif overdue:
        # A due occurrence can be waiting behind another scheduled job.
        # Keep the delay observable without turning it into a failure episode.
        health = "delayed"
    else:
        health = str(state.get("outcome") or "not_checked")
    return {"is_tracker": is_tracker(job), "notifications_enabled": notifications_enabled(job),
            "health_notifications_enabled": health_notifications_enabled(job),
            "doctor_streak_limit": _streak_limit(job),
            "state": health, "last_attempt_at": last_attempt,
            "last_success_at": state.get("last_success_at"),
            "next_check_at": getattr(job, "next_run", None) if getattr(job, "enabled", False) else None,
            "successful_checks_24h": sum(counts.get(s, 0) for s in SUCCESS),
            "no_match_checks_24h": counts.get("no_match", 0), "matches_24h": counts.get("match", 0),
            "incomplete_checks_24h": sum(counts.get(status, 0) for status in ("incomplete", "delivery_failed", "interrupted")),
            "consecutive_failures": int(state.get("consecutive_failures", 0)),
            "consecutive_no_matches": int(state.get("consecutive_no_matches", 0)),
            "no_match_since": state.get("no_match_since"), "failure_since": state.get("failure_since"),
            "last_issue": state.get("last_issue"),
            "overdue": bool(overdue), "last_outcome": state.get("outcome")}


def tracker_statuses(path: Path | None, jobs: Iterable[object], *, now: datetime | None = None) -> dict[str, dict]:
    """One batched read for the requested scheduler/UI/tool surface only."""
    jobs = tuple(jobs)
    if not jobs or path is None:
        return {}
    with _db(path) as conn:
        for job in jobs:
            _reconcile_interrupted_checks(conn, job, _utc(now))
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
            f"{'Scheduled check' if status['overdue'] else 'Next check'}: {_time(status['next_check_at'], job)}")


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
                             now: datetime | None = None,
                             observe: Callable | None = None) -> int:
    """Deliver one Doctor notice per completed 12-run streak; never daily digests.

    Failed deliveries retry after five minutes. Claims prevent concurrent sends;
    an interrupted remote send can be retried because platforms offer no shared
    exactly-once receipt. Recovery resets the incident without sending a notice.
    """
    now = _utc(now)
    all_jobs = tuple(jobs)
    if path is None:
        return 0
    inactive = tuple(j for j in all_jobs if not getattr(j,"enabled",False) or not notifications_enabled(j))
    if observe is not None and inactive:
        facts = tracker_statuses(path,inactive,now=now)
        for job in inactive:
            observe(job,facts[job.id],None,_scope(job))
    jobs = tuple(j for j in all_jobs if getattr(j,"enabled",False) and notifications_enabled(j))
    if not jobs:
        return 0
    groups = defaultdict(list)
    for job in jobs:
        channel, target = resolve_route(job)
        if channel and target:
            groups[(str(job.workspace_id), channel, target)].append(job)
    pending = []
    observations = []
    with _db(path) as conn:
        for route, group in groups.items():
            for job in group:
                _reconcile_interrupted_checks(conn, job, now)
            statuses = {j.id: _status(conn, j, now) for j in group}
            for job in group:
                from nullion.doctor_crons import cron_notice
                from nullion.crons import cron_display_fields
                status = statuses[job.id]
                scope = _scope(job)
                notice = cron_notice(job, status, label=_label(job.name),
                    schedule=cron_display_fields(job)["schedule_description"],
                    last_success=_time(status["last_success_at"],job), next_check=_time(status["next_check_at"],job))
                observations.append((job,status,notice,scope))
                if notice and health_notifications_enabled(job):
                    key = f"{scope}:{_route_scope(route)}:doctor:{notice.kind}:{notice.episode}"
                    if _claim(conn,key,now):
                        pending.append((key,scope,notice.kind,job,route,notice.message))
    if observe is not None:
        for observation in observations:
            observe(*observation)
    sent = 0
    for key, scope, kind, job, route, text in pending:
        if current_jobs is not None:
            current = tuple(j for j in current_jobs() if getattr(j, "enabled", False) and notifications_enabled(j)
                            and (str(j.workspace_id), *resolve_route(j)) == route)
            latest_job = next((j for j in current if _scope(j) == _scope(job)), None)
            if latest_job is None:
                continue
            job = latest_job
        if not health_notifications_enabled(job):
            continue
        with _db(path,write=False) as conn:
            latest = _status(conn,job,now)
        limit = _streak_limit(job)
        if latest["state"] == "running" or (kind == "failure" and latest["consecutive_failures"] < limit) or (kind == "no_match" and latest["consecutive_no_matches"] < limit):
            continue
        try:
            delivered = send(job, route[1], route[2], text, kind) is True
        except Exception:
            log.warning("Tracker status delivery failed", exc_info=True)
            delivered = False
        if delivered:
            with _db(path) as conn:
                conn.execute("UPDATE cron_tracker_notices SET payload=? WHERE notice_key=?", (json.dumps({"sent_at": now.isoformat()}), key))
            sent += 1
    return sent
