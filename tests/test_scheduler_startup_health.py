"""Startup reconciles health once, preserving active leases and startup service."""
from contextlib import nullcontext
from datetime import UTC,datetime,timedelta
import sqlite3
import pytest

@pytest.mark.parametrize('state',['abandoned','active','recent'])
def test_scheduler_startup_reconciles_only_interrupted_checks(tmp_path,monkeypatch,state):
    from nullion import crons
    from nullion.cron_scheduler_state import execution_lease
    from nullion.tracker_status import start_check
    from nullion.runtime import bootstrap_persistent_runtime
    path=tmp_path/'runtime.db';bootstrap_persistent_runtime(path).checkpoint()
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',path)
    job=crons.add_cron(name='監視',task='Read-only fixture',schedule='0 12 1 1 *')
    lease=execution_lease(path,job.id) if state=='active' else nullcontext(True)
    with lease as acquired:
        assert acquired
        stamp=datetime.now(UTC)-(timedelta(days=7) if state!='recent' else timedelta(seconds=1))
        start_check(path,job,'startup-fixture',now=stamp)
        scheduler=crons.CronScheduler(lambda j:pytest.fail('Startup ran a job'),tick_interval=3600,store_path=path)
        scheduler.start()
        try:
            with sqlite3.connect(path) as db:status,finished=db.execute("SELECT status,finished_at FROM cron_tracker_runs WHERE run_id='startup-fixture'").fetchone()
            assert status==('interrupted' if state=='abandoned' else 'running')
            assert bool(finished)==(state=='abandoned')
        finally:
            scheduler.stop();scheduler._thread.join(timeout=2)
            assert not scheduler._thread.is_alive()


def test_busy_health_store_does_not_prevent_scheduler_starting(tmp_path,monkeypatch,caplog):
    from nullion import crons,tracker_status
    def busy(*a,**k):raise sqlite3.OperationalError('database is locked')
    monkeypatch.setattr(tracker_status,'tracker_statuses',busy)
    scheduler=crons.CronScheduler(lambda j:None,tick_interval=3600,store_path=tmp_path/'runtime.db')
    scheduler.start()
    try:
        assert scheduler._thread.is_alive() and 'startup reconciliation deferred' in caplog.text
    finally:scheduler.stop();scheduler._thread.join(timeout=2)
