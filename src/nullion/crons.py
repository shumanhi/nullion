"""Cron job management for Nullion.

Jobs are persisted in the active Nullion runtime DB.
A CronScheduler background thread ticks every 30 s, fires due jobs by
calling the caller-supplied fire_fn(job), then updates last_run / next_run.

Usage
-----
    from nullion.crons import add_cron, load_crons, CronScheduler

    scheduler = CronScheduler(fire_fn=lambda job: orchestrator.send(job.task))
    scheduler.start()

next_run calculation uses ``croniter`` if installed; falls back to a
lightweight built-in parser that handles the most common expressions.
"""
from __future__ import annotations

from contextlib import nullcontext
import json
import logging
import os
import re
import sqlite3
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Callable

log = logging.getLogger(__name__)

_DOW_NAMES = ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")
_MONTH_NAMES = (
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)

def _nullion_home() -> Path:
    configured = str(os.environ.get("NULLION_HOME") or "").strip()
    return Path(configured).expanduser() if configured else Path.home() / ".nullion"


_RUNTIME_DB_PATH: Path | None = None
_DEFAULT_WORKSPACE_ID = "workspace_admin"
_CRON_COLLECTION = "cron_jobs"
_CRON_TABLE = "reminders_crons"
_LOCAL_CRON_SCHEDULE_CUTOFF = datetime(2026, 5, 15, 12, 45, tzinfo=timezone.utc)
_CRON_DELETED_MARKER = "_deleted"

# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class CronJob:
    id:          str
    name:        str
    schedule:    str          # 5-field cron expression, e.g. "0 9 * * 1-5"
    task:        str          # natural-language instruction sent to the agent
    workspace_id: str = _DEFAULT_WORKSPACE_ID
    delivery_channel: str = "" # web | telegram; blank means legacy/default routing
    delivery_target: str = ""  # chat id, conversation id, or other channel-specific target
    html_image_delivery_mode: str = ""  # linked | auto | self_contained (blank keeps runtime default)
    artifact_delivery_options: dict[str, object] = field(default_factory=dict)
    schedule_timezone: str = "" # blank means legacy UTC schedule for pre-local-time jobs
    enabled:     bool  = True
    created_at:  str   = ""
    last_run:    str | None = None
    last_result: str | None = None
    next_run:    str | None = None

    # ── convenience ──────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "CronJob":
        from nullion.cron_delivery import normalize_html_image_delivery_mode

        known = {f for f in cls.__dataclass_fields__}
        payload = {k: v for k, v in d.items() if k in known}
        payload["workspace_id"] = str(payload.get("workspace_id") or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID
        options = payload.get("artifact_delivery_options")
        payload["artifact_delivery_options"] = dict(options) if isinstance(options, dict) else {}
        mode_value = payload.get("html_image_delivery_mode")
        if mode_value in {None, ""} and d.get("html_image_mode") is not None:
            mode_value = d.get("html_image_mode")
        payload["html_image_delivery_mode"] = normalize_html_image_delivery_mode(mode_value)
        return cls(**payload)

    def next_run_dt(self) -> datetime | None:
        if not self.next_run:
            return None
        try:
            dt = datetime.fromisoformat(self.next_run)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _cron_timezone() -> tzinfo:
    try:
        from nullion.preferences import detect_system_timezone, load_preferences, resolve_timezone

        saved_timezone = str(load_preferences().timezone or "").strip()
        if saved_timezone.upper() == "UTC":
            detected_timezone = detect_system_timezone(default="UTC")
            if detected_timezone != "UTC":
                return resolve_timezone(detected_timezone)
        return resolve_timezone(saved_timezone)
    except Exception:
        log.debug("Could not resolve cron timezone; falling back to UTC.", exc_info=True)
        return timezone.utc


def cron_display_timezone() -> tzinfo:
    return _cron_timezone()


def _cron_base_time(after: datetime | None = None, *, tz: tzinfo | None = None) -> tuple[datetime, tzinfo]:
    tz = tz or _cron_timezone()
    if after is None:
        after = datetime.now(timezone.utc)
    if after.tzinfo is None:
        after = after.replace(tzinfo=tz)
    return after.astimezone(tz), tz


def _cron_fire_time_iso(dt: datetime, tz: tzinfo) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)  # type: ignore[arg-type]
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _compute_next_run(schedule: str, after: datetime | None = None, *, tz: tzinfo | None = None) -> str | None:
    """Return ISO-8601 string for the next fire time, or None on error."""
    local_after, cron_tz = _cron_base_time(after, tz=tz)
    # 1. Try croniter (best accuracy)
    try:
        from croniter import croniter          # type: ignore[import]
        cron = croniter(schedule, local_after)
        return _cron_fire_time_iso(cron.get_next(datetime), cron_tz)
    except ImportError:
        pass
    except Exception as exc:
        log.debug("croniter failed for %r: %s — falling back", schedule, exc)

    # 2. Lightweight fallback: parse and advance minute-by-minute (max 1 week)
    return _fallback_next_run(schedule, local_after, tz=cron_tz)


_CRON_RE = re.compile(
    r"^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$"
)

def _field_matches(spec: str, value: int, lo: int, hi: int) -> bool:
    """Check whether a cron field spec matches an integer value."""
    if spec == "*":
        return True
    for part in spec.split(","):
        if "-" in part and "/" not in part:
            a, b = part.split("-", 1)
            if int(a) <= value <= int(b):
                return True
        elif part.startswith("*/"):
            step = int(part[2:])
            if (value - lo) % step == 0:
                return True
        elif "-" in part and "/" in part:
            rng, step = part.split("/", 1)
            a, b = rng.split("-", 1)
            if int(a) <= value <= int(b) and (value - int(a)) % int(step) == 0:
                return True
        else:
            try:
                if int(part) == value:
                    return True
            except ValueError:
                pass
    return False


def _fallback_next_run(schedule: str, after: datetime, *, tz: tzinfo | None = None) -> str | None:
    m = _CRON_RE.match(schedule.strip())
    if not m:
        return None
    if tz is None:
        tz = after.tzinfo or timezone.utc
    if after.tzinfo is None:
        after = after.replace(tzinfo=tz)  # type: ignore[arg-type]
    min_spec, hr_spec, dom_spec, mon_spec, dow_spec = m.groups()
    # Advance minute by minute; cap at 1 week to avoid infinite loops
    dt = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    limit = after + timedelta(days=7)
    while dt <= limit:
        cron_dow = int(dt.strftime("%w"))
        if (
            _field_matches(mon_spec, dt.month,  1, 12)
            and _field_matches(dom_spec, dt.day,   1, 31)
            and _field_matches(dow_spec, cron_dow, 0, 7)
            and _field_matches(hr_spec,  dt.hour,  0, 23)
            and _field_matches(min_spec, dt.minute, 0, 59)
        ):
            return _cron_fire_time_iso(dt, tz)
        dt += timedelta(minutes=1)
    return None


def _timezone_display_name(tz: tzinfo) -> str:
    return str(getattr(tz, "key", None) or tz.tzname(datetime.now(tz)) or "local time")


def _format_local_time(hour: int, minute: int) -> str:
    suffix = "AM" if hour < 12 else "PM"
    display_hour = hour % 12 or 12
    return f"{display_hour}:{minute:02d} {suffix}"


def _parse_cron_field_values(spec: str, lo: int, hi: int) -> list[int] | None:
    spec = str(spec or "").strip()
    if not spec:
        return None
    if spec == "*":
        return list(range(lo, hi + 1))
    values: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            return None
        step = 1
        if "/" in part:
            base, step_text = part.split("/", 1)
            try:
                step = int(step_text)
            except ValueError:
                return None
            if step <= 0:
                return None
        else:
            base = part
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            try:
                start, end = int(start_text), int(end_text)
            except ValueError:
                return None
        else:
            try:
                start = end = int(base)
            except ValueError:
                return None
        if start < lo or end > hi or start > end:
            return None
        values.update(range(start, end + 1, step))
    return sorted(values)


def _join_words(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])}, and {items[-1]}"


def _parse_job_created_at(job: CronJob) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(job.created_at or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _resolve_timezone_name(name: str) -> tzinfo | None:
    try:
        from nullion.preferences import resolve_timezone

        return resolve_timezone(name)
    except Exception:
        return None


def _job_schedule_timezone(job: CronJob) -> tzinfo:
    configured = str(getattr(job, "schedule_timezone", "") or "").strip()
    if configured:
        return _resolve_timezone_name(configured) or timezone.utc
    created_at = _parse_job_created_at(job)
    if created_at is None or created_at < _LOCAL_CRON_SCHEDULE_CUTOFF:
        return timezone.utc
    return _cron_timezone()


def _cron_expression_for_display(schedule: str, *, source_tz: tzinfo, display_tz: tzinfo) -> str:
    if source_tz == display_tz:
        return schedule
    m = _CRON_RE.match(str(schedule or "").strip())
    if not m:
        return schedule
    min_spec, hr_spec, dom_spec, mon_spec, dow_spec = m.groups()
    minutes = _parse_cron_field_values(min_spec, 0, 59)
    hours = _parse_cron_field_values(hr_spec, 0, 23)
    dows = _parse_cron_field_values(dow_spec, 0, 7)
    if minutes is None or hours is None or dows is None:
        return schedule
    if dom_spec != "*" or mon_spec != "*":
        return schedule

    converted_hours: set[int] = set()
    converted_dows: set[int] = set()
    source_dates = [datetime(2026, 5, 10 + offset, tzinfo=source_tz) for offset in range(7)]
    dow_is_wildcard = len(dows) >= 7
    for source_date in source_dates:
        cron_dow = int(source_date.strftime("%w"))
        if not dow_is_wildcard and cron_dow not in {0 if day == 7 else day for day in dows}:
            continue
        for hour in hours:
            local_dt = source_date.replace(hour=hour, minute=0).astimezone(display_tz)
            converted_hours.add(local_dt.hour)
            if not dow_is_wildcard:
                converted_dows.add(int(local_dt.strftime("%w")))

    if not converted_hours:
        return schedule

    def _field(values: set[int]) -> str:
        return ",".join(str(value) for value in sorted(values))

    display_dow_spec = "*" if dow_is_wildcard else _field(converted_dows)
    return f"{min_spec} {_field(converted_hours)} * * {display_dow_spec}"


def describe_cron_schedule(schedule: str, *, tz: tzinfo | None = None) -> str:
    """Return a user-facing schedule summary for a 5-field cron expression."""
    m = _CRON_RE.match(str(schedule or "").strip())
    if not m:
        return "Custom schedule"
    min_spec, hr_spec, dom_spec, mon_spec, dow_spec = m.groups()
    minutes = _parse_cron_field_values(min_spec, 0, 59)
    hours = _parse_cron_field_values(hr_spec, 0, 23)
    doms = _parse_cron_field_values(dom_spec, 1, 31)
    months = _parse_cron_field_values(mon_spec, 1, 12)
    dows = _parse_cron_field_values(dow_spec, 0, 7)
    if None in (minutes, hours, doms, months, dows):
        return "Custom schedule"
    assert minutes is not None and hours is not None and doms is not None and months is not None and dows is not None

    every_minute = len(minutes) == 60
    every_hour = len(hours) == 24
    every_day = len(doms) == 31 and len(dows) >= 7
    every_month = len(months) == 12
    if every_minute and every_hour:
        phrase = "Every minute"
    elif min_spec.startswith("*/") and every_hour:
        phrase = f"Every {min_spec[2:]} minutes"
    elif len(minutes) == 1 and every_hour:
        phrase = "Every hour"
        if minutes[0]:
            phrase += f" at :{minutes[0]:02d}"
    elif len(minutes) == 1 and len(hours) == 1:
        phrase = f"At {_format_local_time(hours[0], minutes[0])}"
    elif len(minutes) == 1 and len(hours) <= 4:
        phrase = "At " + _join_words([_format_local_time(hour, minutes[0]) for hour in hours])
    else:
        phrase = "Custom schedule"

    qualifiers: list[str] = []
    if len(dows) < 7:
        normalized_dows = sorted({0 if day == 7 else day for day in dows})
        qualifiers.append("on " + _join_words([_DOW_NAMES[day] for day in normalized_dows]))
    elif len(doms) < 31:
        qualifiers.append("on day " + _join_words([str(day) for day in doms]))
    if len(months) < 12:
        qualifiers.append("in " + _join_words([_MONTH_NAMES[month] for month in months]))
    if qualifiers:
        phrase += " " + " ".join(qualifiers)
    tz_name = _timezone_display_name(tz or _cron_timezone())
    return f"{phrase} ({tz_name})"


def describe_cron_next_run(next_run: str | None, *, tz: tzinfo | None = None) -> str:
    if not next_run:
        return ""
    try:
        dt = datetime.fromisoformat(str(next_run))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone(tz or _cron_timezone())
        return f"{local_dt.strftime('%b')} {local_dt.day}, {local_dt.year} at {_format_local_time(local_dt.hour, local_dt.minute)}"
    except Exception:
        return ""


def cron_display_fields(job: CronJob, *, tz: tzinfo | None = None) -> dict[str, str]:
    tz = tz or _cron_timezone()
    schedule_tz = _job_schedule_timezone(job)
    display_schedule = _cron_expression_for_display(job.schedule, source_tz=schedule_tz, display_tz=tz)
    return {
        "schedule_description": describe_cron_schedule(display_schedule, tz=tz),
        "next_run_description": describe_cron_next_run(job.next_run, tz=tz),
    }


def _compute_job_next_run(job: CronJob, *, after: datetime | None = None) -> str | None:
    return _compute_next_run(job.schedule, after=after, tz=_job_schedule_timezone(job))


def _refresh_future_next_runs_for_timezone(jobs: list[CronJob]) -> bool:
    now = datetime.now(timezone.utc)
    changed = False
    for job in jobs:
        if not str(getattr(job, "schedule_timezone", "") or "").strip():
            job.schedule_timezone = _timezone_display_name(_job_schedule_timezone(job))
            changed = True
        if not job.enabled:
            continue
        current_next = job.next_run_dt()
        if current_next is not None and current_next.astimezone(timezone.utc) <= now:
            continue
        expected_next = _compute_job_next_run(job, after=now)
        if expected_next and expected_next != job.next_run:
            job.next_run = expected_next
            changed = True
    return changed


def _return_crons(jobs: list[CronJob], *, persist_refreshed: bool = True) -> list[CronJob]:
    if persist_refreshed and _refresh_future_next_runs_for_timezone(jobs):
        _save_crons_db(jobs)
    return jobs


# ── Storage ────────────────────────────────────────────────────────────────────

def _runtime_db_path() -> Path:
    if _RUNTIME_DB_PATH is not None:
        return _RUNTIME_DB_PATH
    return _nullion_home() / "runtime.db"


def _parse_store_timestamp(value: object) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _cron_delivery_started_since(cron_id: str, scheduled_for: datetime, *, now: datetime, db_path: Path | None = None) -> str | None:
    """Return the first recorded delivery start for this scheduled occurrence."""
    cron_id = str(cron_id or "").strip()
    if not cron_id:
        return None
    if scheduled_for.tzinfo is None:
        scheduled_for = scheduled_for.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    scheduled_iso = scheduled_for.astimezone(timezone.utc).isoformat()
    now_iso = now.astimezone(timezone.utc).isoformat()
    try:
        with sqlite3.connect(str(db_path or _runtime_db_path()), timeout=10) as conn:
            row = conn.execute(
                """
                SELECT json_extract(payload, '$.created_at')
                FROM runtime_events
                WHERE collection = 'events'
                  AND json_extract(payload, '$.event_type') = 'cron.delivery.started'
                  AND json_extract(payload, '$.payload.cron_id') = ?
                  AND json_extract(payload, '$.created_at') >= ?
                  AND json_extract(payload, '$.created_at') <= ?
                ORDER BY json_extract(payload, '$.created_at') ASC
                LIMIT 1
                """,
                (cron_id, scheduled_iso, now_iso),
            ).fetchone()
    except sqlite3.Error:
        return None
    if not row:
        return None
    started_at = str(row[0] or "").strip()
    return started_at or None


def _ensure_cron_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_CRON_TABLE} (
            collection TEXT NOT NULL,
            item_key   TEXT NOT NULL,
            payload    TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (collection, item_key)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{_CRON_TABLE}_collection ON {_CRON_TABLE} (collection)"
    )


def _load_crons_db_snapshot() -> tuple[list[CronJob], datetime | None] | None:
    db_path = _runtime_db_path()
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            _ensure_cron_table(conn)
            rows = conn.execute(
                f"SELECT payload, updated_at FROM {_CRON_TABLE} WHERE collection = ? ORDER BY rowid",
                (_CRON_COLLECTION,),
            ).fetchall()
    except sqlite3.Error as exc:
        log.warning("Failed to load crons from runtime DB %s: %s", db_path, exc)
        return None
    jobs: list[CronJob] = []
    for row in rows:
        payload = json.loads(str(row["payload"]))
        if isinstance(payload, dict) and payload.get(_CRON_DELETED_MARKER) is True:
            continue
        job = CronJob.from_dict(payload)
        job._persisted_updated_at = str(row["updated_at"] or "")
        jobs.append(job)
    updated_at = max(
        (dt for dt in (_parse_store_timestamp(row["updated_at"]) for row in rows) if dt is not None),
        default=None,
    )
    return jobs, updated_at


def _load_crons_db() -> list[CronJob] | None:
    snapshot = _load_crons_db_snapshot()
    if snapshot is None:
        return None
    return snapshot[0]


def load_crons(*, refresh_next_runs: bool = True) -> list[CronJob]:
    db_snapshot = _load_crons_db_snapshot()
    if db_snapshot is None:
        return []
    return _return_crons(db_snapshot[0], persist_refreshed=refresh_next_runs)


def list_crons(*, workspace_id: str | None = None, refresh_next_runs: bool = True) -> list[CronJob]:
    jobs = load_crons(refresh_next_runs=refresh_next_runs)
    if workspace_id is None:
        return jobs
    requested_workspace = str(workspace_id or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID
    return [job for job in jobs if (job.workspace_id or _DEFAULT_WORKSPACE_ID) == requested_workspace]


def _cron_deleted_payload(cron_id: str, *, deleted_at: str) -> str:
    return json.dumps(
        {
            "id": str(cron_id or "").strip(),
            _CRON_DELETED_MARKER: True,
            "deleted_at": deleted_at,
        },
        sort_keys=True,
    )


def _payload_is_deleted(value: object) -> bool:
    try:
        payload = json.loads(str(value or "{}"))
    except Exception:
        return False
    return isinstance(payload, dict) and payload.get(_CRON_DELETED_MARKER) is True


def _save_crons_db(
    jobs: list[CronJob],
    *,
    deleted_ids: tuple[str, ...] = (),
    replace_missing: bool = False,
) -> bool:
    db_path = _runtime_db_path()
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc).isoformat()
        job_ids = {str(job.id or "").strip() for job in jobs if str(job.id or "").strip()}
        try:
            from nullion.runtime_persistence import runtime_store_file_lock

            lock_context = runtime_store_file_lock(db_path)
        except Exception:
            lock_context = nullcontext()
        with lock_context:
            with sqlite3.connect(str(db_path), timeout=10) as conn:
                _ensure_cron_table(conn)
                effective_deleted_ids = {
                    str(cron_id or "").strip()
                    for cron_id in deleted_ids
                    if str(cron_id or "").strip()
                }
                if replace_missing:
                    rows = conn.execute(
                        f"SELECT item_key, payload FROM {_CRON_TABLE} WHERE collection = ?",
                        (_CRON_COLLECTION,),
                    ).fetchall()
                    effective_deleted_ids.update(
                        str(row[0] or "").strip()
                        for row in rows
                        if str(row[0] or "").strip()
                        and str(row[0] or "").strip() not in job_ids
                        and not _payload_is_deleted(row[1])
                    )
                for cron_id in sorted(effective_deleted_ids):
                    conn.execute(
                        f"""INSERT OR REPLACE INTO {_CRON_TABLE}
                            (collection, item_key, payload, updated_at)
                            VALUES (?, ?, ?, ?)""",
                        (
                            _CRON_COLLECTION,
                            cron_id,
                            _cron_deleted_payload(cron_id, deleted_at=now),
                            now,
                        ),
                    )
                for job in jobs:
                    existing = conn.execute(
                        f"SELECT payload, updated_at FROM {_CRON_TABLE} WHERE collection = ? AND item_key = ?",
                        (_CRON_COLLECTION, job.id),
                    ).fetchone()
                    if existing is not None and _payload_is_deleted(existing[0]):
                        raise RuntimeError(f"Scheduled task {job.id} was deleted during this update; reload before trying again.")
                    expected_updated_at = str(getattr(job, "_persisted_updated_at", "") or "")
                    existing_updated_at = str(existing[1] or "") if existing is not None else ""
                    if expected_updated_at and existing_updated_at and existing_updated_at != expected_updated_at:
                        raise RuntimeError(f"Scheduled task {job.id} changed during this update; reload before trying again.")
                    conn.execute(
                        f"""INSERT INTO {_CRON_TABLE}
                            (collection, item_key, payload, updated_at)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(collection, item_key) DO UPDATE SET
                                payload=excluded.payload, updated_at=excluded.updated_at""",
                        (_CRON_COLLECTION, job.id, json.dumps(job.to_dict(), sort_keys=True), now),
                    )
                    job._persisted_updated_at = now
        return True
    except sqlite3.Error as exc:
        log.warning("Failed to save crons to runtime DB %s: %s", db_path, exc)
        return False


def _persisted_cron_count() -> int:
    snapshot = _load_crons_db_snapshot()
    return 0 if snapshot is None else len(snapshot[0])


def save_crons(jobs: list[CronJob], *, allow_empty: bool = False) -> None:
    if not jobs and not allow_empty and _persisted_cron_count() > 0:
        log.error("Refusing to overwrite existing cron store with an implicit empty cron list.")
        raise RuntimeError("Refusing to overwrite existing cron store with an implicit empty cron list.")
    saved_to_db = _save_crons_db(jobs, replace_missing=allow_empty)
    if not saved_to_db:
        raise RuntimeError("The scheduled-task change could not be saved. No successful change was confirmed.")
    log.debug("Saved %d cron(s) to runtime DB.", len(jobs))


# ── Constants ─────────────────────────────────────────────────────────────────

# Maximum length for a cron task string. Longer strings are rejected to prevent
# stored prompt-injection payloads from accumulating unbounded context in the
# LLM turn triggered on each scheduled fire. Keep this aligned with the cron
# delivery artifact threshold so detailed report instructions can be stored
# without forcing a fail-then-retry tool loop.
_MAX_TASK_LEN = 12_000
_MAX_NAME_LEN = 200
_MAX_SCHEDULE_LEN = 64


def _validate_cron_fields(name: str, schedule: str, task: str) -> None:
    if len(name) > _MAX_NAME_LEN:
        raise ValueError(f"Cron name too long (max {_MAX_NAME_LEN} chars)")
    if len(schedule) > _MAX_SCHEDULE_LEN:
        raise ValueError(f"Cron schedule too long (max {_MAX_SCHEDULE_LEN} chars)")
    if len(task) > _MAX_TASK_LEN:
        raise ValueError(f"Cron task too long (max {_MAX_TASK_LEN} chars)")
    if not task.strip():
        raise ValueError("Cron task must not be empty")


# ── CRUD ───────────────────────────────────────────────────────────────────────

def add_cron(
    name: str,
    schedule: str,
    task: str,
    enabled: bool = True,
    delivery_channel: str = "",
    delivery_target: str = "",
    html_image_delivery_mode: str = "",
    artifact_delivery_options: dict[str, object] | None = None,
    workspace_id: str = _DEFAULT_WORKSPACE_ID,
) -> CronJob:
    """Create and persist a new cron job. Returns the saved CronJob."""
    from nullion.cron_delivery import normalize_cron_delivery_channel, normalize_html_image_delivery_mode

    _validate_cron_fields(name, schedule, task)
    _validate_tracker_options(artifact_delivery_options)
    jobs = load_crons()
    job = CronJob(
        id=str(uuid.uuid4())[:8],
        name=name,
        schedule=schedule,
        task=task,
        workspace_id=str(workspace_id or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID,
        delivery_channel=normalize_cron_delivery_channel(delivery_channel),
        delivery_target=str(delivery_target or "").strip(),
        html_image_delivery_mode=normalize_html_image_delivery_mode(html_image_delivery_mode),
        artifact_delivery_options=dict(artifact_delivery_options or {}),
        schedule_timezone=_timezone_display_name(_cron_timezone()),
        enabled=enabled,
        created_at=_now_iso(),
        next_run=_compute_next_run(schedule) if enabled else None,
    )
    save_crons([job])
    log.info("Cron created: %r (%s) schedule=%r", job.name, job.id, job.schedule)
    return job


def remove_cron(cron_id: str) -> bool:
    """Delete a cron by id. Returns True if removed."""
    jobs = load_crons()
    new_jobs = [j for j in jobs if j.id != cron_id]
    if len(new_jobs) == len(jobs):
        return False
    saved = _save_crons_db(new_jobs, deleted_ids=(cron_id,))
    if not saved:
        return False
    log.info("Cron deleted: %s", cron_id)
    return True


def toggle_cron(cron_id: str, enabled: bool) -> CronJob | None:
    """Enable or disable a cron. Returns updated job or None if not found."""
    jobs = load_crons()
    for job in jobs:
        if job.id == cron_id:
            job.enabled = enabled
            job.next_run = _compute_job_next_run(job) if enabled else None
            save_crons([job])
            return job
    return None


def update_cron(cron_id: str, **kwargs) -> CronJob | None:
    """Update mutable fields (name, schedule, task, enabled). Recomputes next_run."""
    from nullion.cron_delivery import normalize_cron_delivery_channel, normalize_html_image_delivery_mode

    _validate_tracker_options(kwargs.get("artifact_delivery_options"))

    jobs = load_crons()
    for job in jobs:
        if job.id == cron_id:
            mutable = {
                "name",
                "schedule",
                "task",
                "enabled",
                "delivery_channel",
                "delivery_target",
                "workspace_id",
                "html_image_delivery_mode",
                "artifact_delivery_options",
            }
            for k, v in kwargs.items():
                if k in mutable:
                    if k == "delivery_channel":
                        v = normalize_cron_delivery_channel(v)
                    elif k == "html_image_delivery_mode":
                        v = normalize_html_image_delivery_mode(v)
                    elif k == "artifact_delivery_options":
                        v = dict(v) if isinstance(v, dict) else {}
                    setattr(job, k, v)
            if "schedule" in kwargs:
                job.schedule_timezone = _timezone_display_name(_cron_timezone())
            job.workspace_id = str(job.workspace_id or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID
            # Validate after applying changes so we check the final state.
            _validate_cron_fields(job.name, job.schedule, job.task)
            if "schedule" in kwargs or "enabled" in kwargs:
                job.next_run = _compute_job_next_run(job) if job.enabled else None
            save_crons([job])
            return job
    return None


def _validate_tracker_options(options: object) -> None:
    if isinstance(options, dict):
        for key in ("tracker_status_notifications", "tracker_health_notifications", "doctor_monitoring_enabled"):
            if key in options and type(options[key]) is not bool:
                raise ValueError(f"{key} must be a boolean")
        limit = options.get("doctor_streak_limit",12)
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("doctor_streak_limit must be an integer from 1 to 1000")


def get_cron(cron_id: str) -> CronJob | None:
    for job in load_crons():
        if job.id == cron_id:
            return job
    return None


# ── Scheduler ──────────────────────────────────────────────────────────────────

class CronScheduler:
    """Bounded scheduled workers; due jobs remain visible until capacity is free."""

    def __init__(self,fire_fn: Callable[[CronJob],object],tick_interval: float=30.0,
                 status_fn=None,max_parallel_jobs: int | None=None,store_path: Path | None=None):
        from nullion.cron_scheduler_state import read_limit,validate_limit
        self._store_path=Path(store_path) if store_path is not None else _runtime_db_path()
        self._fire=fire_fn;self._tick_interval=tick_interval;self._status_fn=status_fn
        self._default_limit=validate_limit(max_parallel_jobs) if max_parallel_jobs is not None else None
        self.max_parallel_jobs=read_limit(self._store_path,self._default_limit)
        self._stop=threading.Event();self._wake=threading.Event();self._lock=threading.RLock()
        self._running={}
        self._thread=threading.Thread(target=self._loop,daemon=True,name='nullion-cron-scheduler')
        self._status_thread=threading.Thread(target=self._status_loop,daemon=True,name='nullion-tracker-status')

    def start(self):
        self._thread.start()
        if self._status_fn is not None:self._status_thread.start()

    def stop(self):
        self._stop.set();self._wake.set()

    def set_max_parallel_jobs(self,value):
        from nullion.cron_scheduler_state import write_limit
        self.max_parallel_jobs=write_limit(self._store_path,value);self._wake.set()
        return self.max_parallel_jobs

    def snapshot(self,jobs=None):
        from nullion.cron_scheduler_state import leases,read_limit
        active=leases(self._store_path)
        limit=read_limit(self._store_path,self._default_limit)
        count=sum(token.startswith('scheduler:') for token in active.values())
        states={}
        for job in jobs or []:
            due=job.next_run_dt()
            states[job.id]=('paused' if not job.enabled else 'running' if job.id in active else
                           'waiting_for_capacity' if due and due<=datetime.now(timezone.utc) and count>=limit else
                           'due' if due and due<=datetime.now(timezone.utc) else 'scheduled')
        return {'max_parallel_jobs':limit,'running_jobs':count,'jobs':states}

    def _status_loop(self):
        while not self._stop.wait(self._tick_interval):
            try:
                from nullion.cron_scheduler_state import load_jobs
                self._status_fn(load_jobs(self._store_path),datetime.now(timezone.utc))
            except Exception:log.warning('Tracker status tick failed',exc_info=True)

    def _loop(self):
        while not self._stop.is_set():
            self._wake.wait(self._tick_interval);self._wake.clear()
            if self._stop.is_set():break
            try:self._tick()
            except Exception:log.exception('CronScheduler tick failed')

    def _tick(self):
        from nullion.cron_scheduler_state import claim_scheduled,read_limit,renew,load_jobs,update_schedule_fields
        if self._stop.is_set():return
        with self._lock:
            self.max_parallel_jobs=read_limit(self._store_path,self._default_limit)
            for job_id,(_thread,token) in tuple(self._running.items()):
                renew(self._store_path,job_id,token)
            jobs=load_jobs(self._store_path)
            # Oldest due first; no unbounded executor queue or pre-claimed backlog.
            jobs.sort(key=lambda j:j.next_run_dt() or datetime.min.replace(tzinfo=timezone.utc))
            for job in jobs:
                if self._stop.is_set() or len(self._running)>=self.max_parallel_jobs:break
                if not job.enabled or job.id in self._running:continue
                if not job.next_run_dt():
                    update_schedule_fields(self._store_path,job,next_run=_compute_job_next_run(job));continue
                if job.next_run_dt()>datetime.now(timezone.utc):continue
                existing=_cron_delivery_started_since(job.id,job.next_run_dt(),now=datetime.now(timezone.utc),db_path=self._store_path)
                if existing:
                    update_schedule_fields(self._store_path,job,last_run=existing,next_run=_compute_job_next_run(job));continue
                claim=claim_scheduled(self._store_path,job,self.max_parallel_jobs)
                if claim is None:continue
                claimed,token=claim
                thread=threading.Thread(target=self._run_job,args=(claimed,token),daemon=True,name='nullion-cron-'+job.id)
                self._running[job.id]=(thread,token)
                try:thread.start()
                except Exception:
                    from nullion.cron_scheduler_state import release
                    from nullion.cron_scheduler_state import update_schedule_fields
                    self._running.pop(job.id,None)
                    update_schedule_fields(self._store_path,claimed,next_run=job.next_run,last_run=job.last_run)
                    release(self._store_path,job.id,token)
                    raise

    def _run_job(self,job,token):
        from nullion.cron_scheduler_state import release,save_result,scheduled_token,load_jobs
        try:
            current=next((j for j in load_jobs(self._store_path) if j.id==job.id),None)
            if current is None or not current.enabled or any(getattr(current,k)!=getattr(job,k) for k in ('task','workspace_id','delivery_channel','delivery_target')):return
            with scheduled_token(token):
                result=self._fire(job)
            if isinstance(result,dict) and result.get('cron_run_skipped'):return
            if not isinstance(result,dict):outcome='ok'
            elif result.get('suspended_for_approval'):outcome='waiting_for_approval'
            elif result.get('cron_run_cancelled'):outcome='cancelled'
            elif (any(result.get(k) for k in ('cron_run_failed','cron_report_failed','cron_delivery_failed'))
                  or (isinstance(result.get('execution_outcome'),dict) and result['execution_outcome'].get('execution_succeeded') is False)):
                outcome='error: '+str(result.get('reason') or 'scheduled task incomplete')
            elif result.get('cron_delivery_partial_success'):outcome='partial_success'
            else:outcome='ok'
            save_result(self._store_path,job,outcome)
        except Exception as exc:
            log.warning('Cron %s failed: %s',job.id,exc)
            save_result(self._store_path,job,'error: '+str(exc))
        finally:
            try:
                release(self._store_path,job.id,token)
            finally:
                with self._lock:self._running.pop(job.id,None)
                self._wake.set()
