"""Cron job management for Nullion.

Jobs are persisted to ~/.nullion/crons.json.
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
import re
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

log = logging.getLogger(__name__)

_CRONS_PATH = Path.home() / ".nullion" / "crons.json"
_DEFAULT_WORKSPACE_ID = "workspace_admin"

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


def _compute_next_run(schedule: str, after: datetime | None = None) -> str | None:
    """Return ISO-8601 string for the next fire time, or None on error."""
    if after is None:
        after = datetime.now(timezone.utc)
    # 1. Try croniter (best accuracy)
    try:
        from croniter import croniter          # type: ignore[import]
        cron = croniter(schedule, after)
        return cron.get_next(datetime).replace(tzinfo=timezone.utc).isoformat(timespec="seconds")
    except ImportError:
        pass
    except Exception as exc:
        log.debug("croniter failed for %r: %s — falling back", schedule, exc)

    # 2. Lightweight fallback: parse and advance minute-by-minute (max 1 week)
    return _fallback_next_run(schedule, after)


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


def _fallback_next_run(schedule: str, after: datetime) -> str | None:
    m = _CRON_RE.match(schedule.strip())
    if not m:
        return None
    min_spec, hr_spec, dom_spec, mon_spec, dow_spec = m.groups()
    # Advance minute by minute; cap at 1 week to avoid infinite loops
    dt = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    limit = after + timedelta(days=7)
    while dt <= limit:
        if (
            _field_matches(mon_spec, dt.month,  1, 12)
            and _field_matches(dom_spec, dt.day,   1, 31)
            and _field_matches(dow_spec, dt.weekday(), 0, 6)
            and _field_matches(hr_spec,  dt.hour,  0, 23)
            and _field_matches(min_spec, dt.minute, 0, 59)
        ):
            return dt.isoformat(timespec="seconds")
        dt += timedelta(minutes=1)
    return None


# ── Storage ────────────────────────────────────────────────────────────────────

def load_crons() -> list[CronJob]:
    try:
        data = json.loads(_CRONS_PATH.read_text())
        return [CronJob.from_dict(d) for d in data]
    except FileNotFoundError:
        return []
    except Exception as exc:
        log.warning("Failed to load crons from %s: %s", _CRONS_PATH, exc)
        return []


def list_crons(*, workspace_id: str | None = None) -> list[CronJob]:
    jobs = load_crons()
    if workspace_id is None:
        return jobs
    requested_workspace = str(workspace_id or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID
    return [job for job in jobs if (job.workspace_id or _DEFAULT_WORKSPACE_ID) == requested_workspace]


def save_crons(jobs: list[CronJob]) -> None:
    _CRONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _CRONS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps([j.to_dict() for j in jobs], indent=2))
    tmp.replace(_CRONS_PATH)


# ── Constants ─────────────────────────────────────────────────────────────────

# Maximum length for a cron task string.  Longer strings are rejected to
# prevent stored prompt-injection payloads from accumulating unbounded
# context in the LLM turn triggered on each scheduled fire.
_MAX_TASK_LEN = 2_000
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
    save_crons(new_jobs)
    log.info("Cron deleted: %s", cron_id)
    return True


def toggle_cron(cron_id: str, enabled: bool) -> CronJob | None:
    """Enable or disable a cron. Returns updated job or None if not found."""
    jobs = load_crons()
    for job in jobs:
        if job.id == cron_id:
            job.enabled = enabled
            job.next_run = _compute_next_run(job.schedule) if enabled else None
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
            job.workspace_id = str(job.workspace_id or _DEFAULT_WORKSPACE_ID).strip() or _DEFAULT_WORKSPACE_ID
            # Validate after applying changes so we check the final state.
            _validate_cron_fields(job.name, job.schedule, job.task)
            job.next_run = _compute_next_run(job.schedule) if job.enabled else None
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
                job.next_run = _compute_next_run(job.schedule, after=now)
                changed = True
                continue

            next_dt = job.next_run_dt()
            if next_dt is None:
                # Corrupt next_run — recompute
                job.next_run = _compute_next_run(job.schedule, after=now)
                changed = True
                continue

            if now >= next_dt:
                log.info("Firing cron %r [%s]: %s", job.name, job.id, job.task[:80])
                # Advance next_run and persist BEFORE firing so a crash mid-fire
                # doesn't cause a double-fire on the next scheduler tick.
                job.last_run = _now_iso()
                job.next_run = _compute_next_run(job.schedule, after=now)
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
