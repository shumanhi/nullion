from __future__ import annotations
from cron_scheduler_helpers import finish_scheduler

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from nullion import crons
from nullion.crons import CronJob, CronScheduler, add_cron, get_cron, list_crons, load_crons, remove_cron, save_crons, toggle_cron, update_cron
from nullion.runtime_persistence import restore_sqlite_auxiliary_rows_from_candidates, save_runtime_store
from nullion.runtime_store import RuntimeStore


@pytest.fixture
def cron_db_path(tmp_path, monkeypatch):
    path = tmp_path / "runtime.db"
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", path)
    return path


def test_cron_definitions_persist_in_runtime_db(cron_db_path) -> None:
    job = add_cron("DB Cron", "*/10 * * * *", "Run from sqlite")

    loaded = load_crons()
    assert [loaded_job.id for loaded_job in loaded] == [job.id]
    with sqlite3.connect(cron_db_path) as conn:
        row = conn.execute(
            "SELECT item_key, payload FROM reminders_crons WHERE collection = 'cron_jobs'"
        ).fetchone()
    assert row[0] == job.id
    assert json.loads(row[1])["name"] == "DB Cron"


def test_runtime_checkpoint_preserves_cron_job_rows(cron_db_path) -> None:
    job = add_cron("Checkpoint Safe", "0 * * * *", "Still exists after checkpoint")

    save_runtime_store(RuntimeStore(), cron_db_path)

    loaded = load_crons()
    assert [loaded_job.id for loaded_job in loaded] == [job.id]
    with sqlite3.connect(cron_db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM reminders_crons WHERE collection = 'cron_jobs'"
        ).fetchone()[0]
    assert count == 1


def test_recovery_restores_crons_from_best_runtime_db_candidate(cron_db_path, tmp_path) -> None:
    current = cron_db_path
    stale_source = tmp_path / "stale-runtime.db"
    rich_source = tmp_path / "rich-runtime.db"
    target = tmp_path / "recovered-runtime.db"

    stale_job = CronJob(
        id="stale",
        name="Stale",
        schedule="0 * * * *",
        task="Old",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    rich_jobs = [
        CronJob(
            id="rich-1",
            name="Rich 1",
            schedule="0 * * * *",
            task="Keep 1",
            created_at=datetime.now(timezone.utc).isoformat(),
        ),
        CronJob(
            id="rich-2",
            name="Rich 2",
            schedule="0 * * * *",
            task="Keep 2",
            created_at=datetime.now(timezone.utc).isoformat(),
        ),
    ]

    crons._save_crons_db([stale_job])
    current.replace(stale_source)
    crons._save_crons_db(rich_jobs)
    current.replace(rich_source)
    save_runtime_store(RuntimeStore(), target)

    restored = restore_sqlite_auxiliary_rows_from_candidates((stale_source, rich_source), target)

    assert restored == 2
    with sqlite3.connect(target) as conn:
        rows = conn.execute(
            "SELECT item_key FROM reminders_crons WHERE collection = 'cron_jobs' ORDER BY item_key"
        ).fetchall()
    assert [row[0] for row in rows] == ["rich-1", "rich-2"]


def test_next_run_uses_user_local_timezone_for_wall_clock_schedules(monkeypatch) -> None:
    user_tz = ZoneInfo("America/New_York")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: user_tz)

    next_run = crons._compute_next_run(
        "0 9 * * *",
        after=datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc),
    )

    assert next_run is not None
    next_run_dt = datetime.fromisoformat(next_run)
    assert next_run_dt.tzinfo is not None
    assert next_run_dt.astimezone(timezone.utc) == datetime(
        2026,
        5,
        15,
        13,
        0,
        tzinfo=timezone.utc,
    )
    assert next_run_dt.astimezone(user_tz).strftime("%Y-%m-%d %H:%M") == "2026-05-15 09:00"


def test_fallback_next_run_uses_cron_weekday_numbering(monkeypatch) -> None:
    monkeypatch.setattr(crons, "_cron_timezone", lambda: timezone.utc)

    next_run = crons._fallback_next_run(
        "0 9 * * 0",
        datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc),
        tz=timezone.utc,
    )

    assert next_run == "2026-05-17T09:00:00+00:00"


def test_default_utc_preference_falls_back_to_detected_local_timezone(monkeypatch) -> None:
    import nullion.preferences as preferences

    monkeypatch.setattr(preferences, "load_preferences", lambda: SimpleNamespace(timezone="UTC"))
    monkeypatch.setattr(preferences, "detect_system_timezone", lambda default="UTC": "America/New_York")

    assert crons._cron_timezone() == ZoneInfo("America/New_York")


def test_add_cron_persists_next_run_from_user_local_timezone(cron_db_path, monkeypatch) -> None:
    user_tz = ZoneInfo("America/New_York")

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc).astimezone(tz or timezone.utc)

    monkeypatch.setattr(crons, "_cron_timezone", lambda: user_tz)
    monkeypatch.setattr(crons, "datetime", FrozenDateTime)

    job = add_cron("Morning local", "0 9 * * *", "Run at local 9am")

    assert job.next_run is not None
    assert job.schedule_timezone == "America/New_York"
    next_run_dt = datetime.fromisoformat(job.next_run)
    assert next_run_dt.astimezone(user_tz).strftime("%Y-%m-%d %H:%M") == "2026-05-15 09:00"
    assert load_crons()[0].next_run == job.next_run


def test_load_crons_refreshes_future_local_next_run_after_timezone_change(cron_db_path, monkeypatch) -> None:
    user_tz = ZoneInfo("America/New_York")

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 5, 15, 8, 0, tzinfo=timezone.utc).astimezone(tz or timezone.utc)

    monkeypatch.setattr(crons, "_cron_timezone", lambda: user_tz)
    monkeypatch.setattr(crons, "datetime", FrozenDateTime)
    stale_job = CronJob(
        id="stale-next",
        name="Stale next",
        schedule="0 9 * * *",
        schedule_timezone="America/New_York",
        task="Run locally at 9",
        enabled=True,
        created_at=datetime(2026, 5, 15, 13, tzinfo=timezone.utc).isoformat(),
        next_run="2026-05-15T09:00:00+00:00",
    )
    save_crons([stale_job])

    loaded = load_crons()[0]

    assert loaded.next_run == "2026-05-15T13:00:00+00:00"
    assert datetime.fromisoformat(loaded.next_run).astimezone(user_tz).strftime("%H:%M") == "09:00"


def test_legacy_utc_schedule_displays_and_refreshes_as_local_time(cron_db_path, monkeypatch) -> None:
    user_tz = ZoneInfo("America/New_York")

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc).astimezone(tz or timezone.utc)

    monkeypatch.setattr(crons, "_cron_timezone", lambda: user_tz)
    monkeypatch.setattr(crons, "datetime", FrozenDateTime)
    legacy_job = CronJob(
        id="post-market",
        name="Daily Post-Market Stock Report",
        schedule="0 20 * * 1-5",
        task="Run after market close",
        enabled=True,
        created_at=datetime(2026, 5, 12, tzinfo=timezone.utc).isoformat(),
        next_run="2026-05-16T00:00:00+00:00",
    )
    save_crons([legacy_job])

    loaded = load_crons()[0]
    display = crons.cron_display_fields(loaded)

    assert loaded.schedule_timezone == "UTC"
    assert loaded.next_run == "2026-05-15T20:00:00+00:00"
    assert display["schedule_description"] == (
        "At 4:00 PM on Monday, Tuesday, Wednesday, Thursday, and Friday (America/New_York)"
    )
    assert display["next_run_description"] == "May 15, 2026 at 4:00 PM"


def test_load_crons_keeps_due_next_run_so_scheduler_can_fire(cron_db_path, monkeypatch) -> None:
    user_tz = ZoneInfo("America/New_York")

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc).astimezone(tz or timezone.utc)

    monkeypatch.setattr(crons, "_cron_timezone", lambda: user_tz)
    monkeypatch.setattr(crons, "datetime", FrozenDateTime)
    due_job = CronJob(
        id="due-next",
        name="Due next",
        schedule="0 13 * * *",
        task="Do not skip due run",
        enabled=True,
        created_at=datetime(2026, 5, 14, tzinfo=timezone.utc).isoformat(),
        next_run="2026-05-15T11:00:00+00:00",
    )
    save_crons([due_job])

    assert load_crons()[0].next_run == "2026-05-15T11:00:00+00:00"


def test_read_only_cron_load_does_not_refresh_next_run_or_write_db(cron_db_path, monkeypatch) -> None:
    user_tz = ZoneInfo("America/New_York")

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 5, 15, 8, 0, tzinfo=timezone.utc).astimezone(tz or timezone.utc)

    monkeypatch.setattr(crons, "_cron_timezone", lambda: user_tz)
    monkeypatch.setattr(crons, "datetime", FrozenDateTime)
    stale_job = CronJob(
        id="read-only-next",
        name="Read only next",
        schedule="0 9 * * *",
        schedule_timezone="America/New_York",
        task="Run locally at 9",
        enabled=True,
        created_at=datetime(2026, 5, 15, 13, tzinfo=timezone.utc).isoformat(),
        next_run="2026-05-15T09:00:00+00:00",
    )
    save_crons([stale_job])

    loaded = load_crons(refresh_next_runs=False)[0]

    assert loaded.next_run == "2026-05-15T09:00:00+00:00"
    assert load_crons(refresh_next_runs=False)[0].next_run == "2026-05-15T09:00:00+00:00"


def test_cron_display_fields_are_human_readable_local_time(monkeypatch) -> None:
    user_tz = ZoneInfo("America/New_York")
    monkeypatch.setattr(crons, "_cron_timezone", lambda: user_tz)
    job = CronJob(
        id="daily-brief",
        name="Daily brief",
        schedule="0 13 * * *",
        task="Summarize the day",
        schedule_timezone="America/New_York",
        created_at=datetime.now(timezone.utc).isoformat(),
        next_run="2026-05-15T17:00:00+00:00",
    )

    display = crons.cron_display_fields(job)

    assert display["schedule_description"] == "At 1:00 PM (America/New_York)"
    assert display["next_run_description"] == "May 15, 2026 at 1:00 PM"
    assert "0 13 * * *" not in display["schedule_description"]


def test_implicit_empty_cron_save_is_blocked_but_remove_can_clear_last_job(cron_db_path) -> None:
    job = CronJob(
        id="guarded-cron",
        name="Guarded tracker",
        schedule="*/10 * * * *",
        task="Keep this cron",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    save_crons([job])

    with pytest.raises(RuntimeError, match="Refusing to overwrite existing cron store"):
        save_crons([])

    assert [loaded.id for loaded in load_crons()] == ["guarded-cron"]
    assert remove_cron("guarded-cron") is True
    assert load_crons() == []


def test_deleted_cron_is_not_resurrected_by_stale_save(cron_db_path) -> None:
    keep = CronJob(
        id="keep-cron",
        name="Keep tracker",
        schedule="*/10 * * * *",
        task="Keep this cron",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    deleted = CronJob(
        id="deleted-cron",
        name="Deleted tracker",
        schedule="*/10 * * * *",
        task="This must not come back",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    save_crons([keep, deleted])
    stale_snapshot = load_crons()

    assert remove_cron("deleted-cron") is True
    with pytest.raises(RuntimeError):
        save_crons(stale_snapshot)

    assert [job.id for job in load_crons()] == ["keep-cron"]
    with sqlite3.connect(cron_db_path) as conn:
        row = conn.execute(
            "SELECT payload FROM reminders_crons WHERE collection = 'cron_jobs' AND item_key = 'deleted-cron'"
        ).fetchone()
    assert json.loads(row[0])["_deleted"] is True


def test_stale_cron_save_does_not_revert_newer_same_id_update(cron_db_path) -> None:
    job = CronJob(
        id="updated-cron",
        name="Original tracker",
        schedule="*/10 * * * *",
        task="old task",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    save_crons([job])
    stale_snapshot = load_crons()

    assert update_cron("updated-cron", task="new task", enabled=False) is not None
    with pytest.raises(RuntimeError):
        save_crons(stale_snapshot)

    updated = get_cron("updated-cron")
    assert updated is not None
    assert updated.task == "new task"
    assert updated.enabled is False


def test_cron_crud_persists_workspace_and_delivery_metadata(cron_db_path) -> None:
    job = add_cron(
        name="Morning Brief",
        schedule="0 8 * * *",
        task="Write the brief",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="123",
        html_image_delivery_mode="self_contained",
        artifact_delivery_options={"html_image_mode": "self_contained"},
    )

    loaded = get_cron(job.id)
    assert loaded is not None
    assert loaded.delivery_channel == "telegram"
    assert loaded.delivery_target == "123"
    assert loaded.html_image_delivery_mode == "self_contained"
    assert loaded.artifact_delivery_options == {"html_image_mode": "self_contained"}
    assert loaded.workspace_id == "workspace_admin"
    assert list_crons(workspace_id="workspace_admin") == [loaded]

    updated = update_cron(
        job.id,
        name="Morning Brief v2",
        delivery_channel="web",
        delivery_target="web:operator",
        html_image_delivery_mode="linked",
        artifact_delivery_options={"html_image_mode": "linked"},
    )
    assert updated is not None
    assert updated.name == "Morning Brief v2"
    assert updated.delivery_channel == "web"
    assert updated.html_image_delivery_mode == "linked"
    assert updated.artifact_delivery_options == {"html_image_mode": "linked"}
    assert toggle_cron(job.id, False).enabled is False
    assert load_crons()[0].next_run is None
    assert remove_cron(job.id) is True
    assert load_crons() == []


def test_cron_rejects_empty_or_oversized_tasks(cron_db_path) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        add_cron("Bad", "* * * * *", "   ")
    with pytest.raises(ValueError, match="too long"):
        add_cron("Bad", "* * * * *", "x" * (crons._MAX_TASK_LEN + 1))


def test_cron_scheduler_fires_due_jobs_once_and_persists_result(cron_db_path) -> None:
    fired: list[str] = []
    due_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    job = CronJob(
        id="cron-1",
        name="Due",
        schedule="*/5 * * * *",
        task="run",
        enabled=True,
        created_at=due_time.isoformat(timespec="seconds"),
        next_run=due_time.isoformat(timespec="seconds"),
    )
    save_crons([job])
    scheduler = CronScheduler(lambda fired_job: fired.append(fired_job.id), tick_interval=999)

    scheduler._tick()
    finish_scheduler(scheduler)

    persisted = load_crons()[0]
    assert fired == ["cron-1"]
    assert persisted.last_result == "ok"
    assert persisted.last_run is not None
    assert persisted.next_run != due_time.isoformat(timespec="seconds")


def test_cron_scheduler_advances_next_run_before_failed_fire_to_prevent_double_delivery(cron_db_path) -> None:
    fired: list[str] = []
    due_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    job = CronJob(
        id="cron-fail",
        name="Due once",
        schedule="*/5 * * * *",
        task="run",
        enabled=True,
        created_at=due_time.isoformat(timespec="seconds"),
        next_run=due_time.isoformat(timespec="seconds"),
    )
    save_crons([job])

    def fail_once(fired_job):
        fired.append(fired_job.id)
        raise RuntimeError("delivery failed")

    scheduler = CronScheduler(fail_once, tick_interval=999)

    scheduler._tick()
    finish_scheduler(scheduler)
    scheduler._tick()
    finish_scheduler(scheduler)

    persisted = load_crons()[0]
    assert fired == ["cron-fail"]
    assert persisted.last_result == "error: delivery failed"
    assert persisted.last_run is not None
    assert persisted.next_run != due_time.isoformat(timespec="seconds")


def test_cron_scheduler_skips_stale_due_occurrence_already_started(cron_db_path) -> None:
    fired: list[str] = []
    due_time = datetime.now(timezone.utc) - timedelta(minutes=90)
    started_at = (due_time + timedelta(minutes=6)).isoformat(timespec="seconds")
    job = CronJob(
        id="cron-stale-duplicate",
        name="Daily report",
        schedule="0 12 * * *",
        task="run",
        schedule_timezone="UTC",
        enabled=True,
        created_at=(due_time - timedelta(days=1)).isoformat(timespec="seconds"),
        next_run=due_time.isoformat(timespec="seconds"),
        last_run=(due_time - timedelta(days=1)).isoformat(timespec="seconds"),
    )
    save_crons([job])
    with sqlite3.connect(cron_db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_events (
                collection TEXT NOT NULL,
                item_key   TEXT NOT NULL,
                payload    TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (collection, item_key)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO runtime_events (collection, item_key, payload, updated_at)
            VALUES ('events', 'event-started', ?, ?)
            """,
            (
                json.dumps(
                    {
                        "actor": "cron_scheduler",
                        "created_at": started_at,
                        "event_id": "event-started",
                        "event_type": "cron.delivery.started",
                        "payload": {"cron_id": job.id, "cron_name": job.name},
                    }
                ),
                started_at,
            ),
        )

    scheduler = CronScheduler(lambda fired_job: fired.append(fired_job.id), tick_interval=999)

    scheduler._tick()
    finish_scheduler(scheduler)

    persisted = load_crons()[0]
    assert fired == []
    assert persisted.last_run == started_at
    assert persisted.next_run != due_time.isoformat(timespec="seconds")
