from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from nullion import crons
from nullion.crons import CronJob, CronScheduler, add_cron, get_cron, list_crons, load_crons, remove_cron, save_crons, toggle_cron, update_cron


@pytest.fixture
def cron_file(tmp_path, monkeypatch):
    path = tmp_path / "crons.json"
    monkeypatch.setattr(crons, "_CRONS_PATH", path)
    return path


def test_cron_crud_persists_workspace_and_delivery_metadata(cron_file) -> None:
    job = add_cron(
        name="Morning Brief",
        schedule="0 8 * * *",
        task="Write the brief",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="123",
    )

    loaded = get_cron(job.id)
    assert loaded is not None
    assert loaded.delivery_channel == "telegram"
    assert loaded.delivery_target == "123"
    assert loaded.workspace_id == "workspace_admin"
    assert list_crons(workspace_id="workspace_admin") == [loaded]

    updated = update_cron(job.id, name="Morning Brief v2", delivery_channel="web", delivery_target="web:operator")
    assert updated is not None
    assert updated.name == "Morning Brief v2"
    assert updated.delivery_channel == "web"
    assert toggle_cron(job.id, False).enabled is False
    assert load_crons()[0].next_run is None
    assert remove_cron(job.id) is True
    assert load_crons() == []


def test_cron_rejects_empty_or_oversized_tasks(cron_file) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        add_cron("Bad", "* * * * *", "   ")
    with pytest.raises(ValueError, match="too long"):
        add_cron("Bad", "* * * * *", "x" * 2001)


def test_cron_scheduler_fires_due_jobs_once_and_persists_result(cron_file) -> None:
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

    persisted = load_crons()[0]
    assert fired == ["cron-1"]
    assert persisted.last_result == "ok"
    assert persisted.last_run is not None
    assert persisted.next_run != due_time.isoformat(timespec="seconds")
