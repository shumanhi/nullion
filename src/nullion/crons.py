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
_CRONS_PATH = _nullion_home() / "crons.json"
_DEFAULT_WORKSPACE_ID = "workspace_admin"
_CRON_COLLECTION = "cron_jobs"
_CRON_TABLE = "reminders_crons"
_STORE_FRESHNESS_SKEW = timedelta(seconds=1)
_LOCAL_CRON_SCHEDULE_CUTOFF = datetime(2026, 5, 15, 12, 45, tzinfo=timezone.utc)

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
        known = {f for f in cls.__dataclass_fields__}
        payload = {k: v for k, v in d.items() if k in known}
        payload["workspace_id"] = str(payload.get("workspace_id") or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID
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
    legacy_path = _legacy_crons_json_path()
    if legacy_path != _nullion_home() / "crons.json":
        return legacy_path.with_name("runtime.db")
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


def _load_crons_json() -> list[CronJob]:
    try:
        data = json.loads(_CRONS_PATH.read_text())
        return [CronJob.from_dict(d) for d in data]
    except FileNotFoundError:
        return []
    except Exception as exc:
        log.warning("Failed to load crons from %s: %s", _CRONS_PATH, exc)
        return []


def _has_crons_json_rows() -> bool:
    try:
        data = json.loads(_CRONS_PATH.read_text())
    except FileNotFoundError:
        return False
    except Exception as exc:
        log.warning("Failed to inspect cron JSON mirror %s: %s", _CRONS_PATH, exc)
        return False
    return isinstance(data, list) and bool(data)


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
    jobs = [CronJob.from_dict(json.loads(str(row["payload"]))) for row in rows]
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


def _legacy_crons_json_path() -> Path:
    return Path(_CRONS_PATH).expanduser()


def _crons_json_mtime() -> datetime | None:
    path = _legacy_crons_json_path()
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except FileNotFoundError:
        return None
    except OSError as exc:
        log.warning("Failed to stat cron JSON mirror %s: %s", path, exc)
        return None


def _load_legacy_crons_json() -> tuple[list[CronJob], datetime] | None:
    path = _legacy_crons_json_path()
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            return None
        stat = path.stat()
        jobs = [CronJob.from_dict(item) for item in raw if isinstance(item, dict)]
        return jobs, datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    except Exception as exc:
        log.warning("Failed to load legacy crons JSON %s: %s", path, exc)
        return None


def load_crons(*, refresh_next_runs: bool = True) -> list[CronJob]:
    db_snapshot = _load_crons_db_snapshot()
    legacy_snapshot = _load_legacy_crons_json()
    if db_snapshot is None:
        if legacy_snapshot is not None and legacy_snapshot[0]:
            _save_crons_db(legacy_snapshot[0])
            return _return_crons(legacy_snapshot[0], persist_refreshed=refresh_next_runs)
        return []
    if legacy_snapshot is not None:
        legacy_jobs, legacy_updated_at = legacy_snapshot
        db_jobs, db_updated_at = db_snapshot
        json_updated_at = _crons_json_mtime()
        if not db_jobs and _has_crons_json_rows():
            legacy_jobs = _load_crons_json()
            save_crons(legacy_jobs)
            log.warning(
                "Runtime DB had no cron rows; restored %d cron(s) from legacy JSON mirror.",
                len(legacy_jobs),
            )
            return _return_crons(legacy_jobs, persist_refreshed=refresh_next_runs)
        if json_updated_at is not None and (
            db_updated_at is None or json_updated_at > db_updated_at + _STORE_FRESHNESS_SKEW
        ):
            legacy_jobs = _load_crons_json()
            if not legacy_jobs and db_jobs:
                log.warning(
                    "Ignoring newer empty cron JSON mirror because runtime DB still has %d cron(s).",
                    len(db_jobs),
                )
                return _return_crons(db_jobs, persist_refreshed=refresh_next_runs)
            save_crons(legacy_jobs)
            log.info(
                "Imported %d cron(s) from newer legacy JSON mirror into runtime DB.",
                len(legacy_jobs),
            )
            return _return_crons(legacy_jobs, persist_refreshed=refresh_next_runs)
        if legacy_updated_at and db_updated_at is None and legacy_jobs and not db_jobs:
            _save_crons_db(legacy_jobs)
            return _return_crons(legacy_jobs, persist_refreshed=refresh_next_runs)
    return _return_crons(db_snapshot[0], persist_refreshed=refresh_next_runs)


def list_crons(*, workspace_id: str | None = None, refresh_next_runs: bool = True) -> list[CronJob]:
    jobs = load_crons(refresh_next_runs=refresh_next_runs)
    if workspace_id is None:
        return jobs
    requested_workspace = str(workspace_id or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID
    return [job for job in jobs if (job.workspace_id or _DEFAULT_WORKSPACE_ID) == requested_workspace]


def _save_crons_db(jobs: list[CronJob]) -> bool:
    db_path = _runtime_db_path()
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            _ensure_cron_table(conn)
            conn.execute(f"DELETE FROM {_CRON_TABLE} WHERE collection = ?", (_CRON_COLLECTION,))
            for job in jobs:
                conn.execute(
                    f"""INSERT OR REPLACE INTO {_CRON_TABLE}
                        (collection, item_key, payload, updated_at)
                        VALUES (?, ?, ?, ?)""",
                    (_CRON_COLLECTION, job.id, json.dumps(job.to_dict(), sort_keys=True), now),
                )
        return True
    except sqlite3.Error as exc:
        log.warning("Failed to save crons to runtime DB %s: %s", db_path, exc)
        return False


def _persisted_cron_count() -> int:
    snapshot = _load_crons_db_snapshot()
    db_count = 0 if snapshot is None else len(snapshot[0])
    json_count = len(_load_crons_json())
    return max(db_count, json_count)


def save_crons(jobs: list[CronJob], *, allow_empty: bool = False) -> None:
    if not jobs and not allow_empty and _persisted_cron_count() > 0:
        log.error("Refusing to overwrite existing cron store with an implicit empty cron list.")
        raise RuntimeError("Refusing to overwrite existing cron store with an implicit empty cron list.")
    saved_to_db = _save_crons_db(jobs)
    if saved_to_db:
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
    workspace_id: str = _DEFAULT_WORKSPACE_ID,
) -> CronJob:
    """Create and persist a new cron job. Returns the saved CronJob."""
    from nullion.cron_delivery import normalize_cron_delivery_channel

    _validate_cron_fields(name, schedule, task)
    jobs = load_crons()
    job = CronJob(
        id=str(uuid.uuid4())[:8],
        name=name,
        schedule=schedule,
        task=task,
        workspace_id=str(workspace_id or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID,
        delivery_channel=normalize_cron_delivery_channel(delivery_channel),
        delivery_target=str(delivery_target or "").strip(),
        schedule_timezone=_timezone_display_name(_cron_timezone()),
        enabled=enabled,
        created_at=_now_iso(),
        next_run=_compute_next_run(schedule) if enabled else None,
    )
    jobs.append(job)
    save_crons(jobs)
    log.info("Cron created: %r (%s) schedule=%r", job.name, job.id, job.schedule)
    return job


def remove_cron(cron_id: str) -> bool:
    """Delete a cron by id. Returns True if removed."""
    jobs = load_crons()
    new_jobs = [j for j in jobs if j.id != cron_id]
    if len(new_jobs) == len(jobs):
        return False
    save_crons(new_jobs, allow_empty=True)
    log.info("Cron deleted: %s", cron_id)
    return True


def toggle_cron(cron_id: str, enabled: bool) -> CronJob | None:
    """Enable or disable a cron. Returns updated job or None if not found."""
    jobs = load_crons()
    for job in jobs:
        if job.id == cron_id:
            job.enabled = enabled
            job.next_run = _compute_job_next_run(job) if enabled else None
            save_crons(jobs)
            return job
    return None


def update_cron(cron_id: str, **kwargs) -> CronJob | None:
    """Update mutable fields (name, schedule, task, enabled). Recomputes next_run."""
    from nullion.cron_delivery import normalize_cron_delivery_channel

    jobs = load_crons()
    for job in jobs:
        if job.id == cron_id:
            mutable = {"name", "schedule", "task", "enabled", "delivery_channel", "delivery_target", "workspace_id"}
            for k, v in kwargs.items():
                if k in mutable:
                    if k == "delivery_channel":
                        v = normalize_cron_delivery_channel(v)
                    setattr(job, k, v)
            if "schedule" in kwargs:
                job.schedule_timezone = _timezone_display_name(_cron_timezone())
            job.workspace_id = str(job.workspace_id or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID
            # Validate after applying changes so we check the final state.
            _validate_cron_fields(job.name, job.schedule, job.task)
            job.next_run = _compute_job_next_run(job) if job.enabled else None
            save_crons(jobs)
            return job
    return None


def get_cron(cron_id: str) -> CronJob | None:
    for job in load_crons():
        if job.id == cron_id:
            return job
    return None


# ── Scheduler ──────────────────────────────────────────────────────────────────

class CronScheduler:
    """Background thread that fires due cron jobs every ~30 seconds.

    Parameters
    ----------
    fire_fn:
        Called with a CronJob when it is due.  Should be non-blocking or
        hand off to a thread executor — the scheduler waits for it to return
        before continuing, so long-running tasks should be dispatched async.
    tick_interval:
        How often (seconds) the scheduler wakes to check for due jobs.
        Default 30 s gives at most 30 s of latency for minute-granularity crons.
    """

    def __init__(
        self,
        fire_fn: Callable[[CronJob], None],
        tick_interval: float = 30.0,
    ) -> None:
        self._fire = fire_fn
        self._tick_interval = tick_interval
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="nullion-cron-scheduler",
        )

    def start(self) -> None:
        self._thread.start()
        log.info("CronScheduler started (tick every %ss)", self._tick_interval)

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.wait(timeout=self._tick_interval):
            try:
                self._tick()
            except Exception as exc:
                log.error("CronScheduler tick error: %s (%s)", exc, type(exc).__name__, exc_info=True)

    def _tick(self) -> None:
        now = datetime.now(timezone.utc)
        jobs = load_crons()
        changed = False

        for job in jobs:
            if not job.enabled:
                continue

            # Ensure next_run is populated
            if not job.next_run:
                job.next_run = _compute_job_next_run(job, after=now)
                changed = True
                continue

            next_dt = job.next_run_dt()
            if next_dt is None:
                # Corrupt next_run — recompute
                job.next_run = _compute_job_next_run(job, after=now)
                changed = True
                continue

            if now >= next_dt:
                log.info("Firing cron %r [%s]: %s", job.name, job.id, job.task[:80])
                # Advance next_run and persist BEFORE firing so a crash mid-fire
                # doesn't cause a double-fire on the next scheduler tick.
                job.last_run = _now_iso()
                job.next_run = _compute_job_next_run(job, after=now)
                save_crons(jobs)
                changed = False  # already saved
                try:
                    self._fire(job)
                    job.last_result = "ok"
                except Exception as exc:
                    log.warning("Cron %r [%s] fire error: %s", job.name, job.id, exc)
                    job.last_result = f"error: {exc}"
                changed = True  # persist last_result update

        if changed:
            save_crons(jobs)
