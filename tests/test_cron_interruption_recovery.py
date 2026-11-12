from datetime import datetime, timedelta, timezone
import json
import os
from types import SimpleNamespace

import pytest

from nullion import cron_scheduler_state as state


def seed(tmp_path, monkeypatch, token):
    from nullion import crons
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_CHECKPOINT_PATH', str(tmp_path/'runtime.db'))
    job = crons.CronJob(id='fixture', name='通知', task='確認', schedule='0 0 * * *',
        last_run=state.now().isoformat(), next_run=(state.now()+timedelta(days=1)).isoformat(),
        last_result='running', delivery_channel='web', delivery_target='web:recovery')
    crons.save_crons([job])
    path=tmp_path/'runtime.db'
    with state.connection(path) as conn:
        conn.execute('INSERT INTO cron_execution_leases VALUES (?,?,?)', (job.id,token,state.expiry()))
    return path, job


def test_dead_owner_is_reclaimed_before_expiry_and_manual_run_can_start(tmp_path,monkeypatch):
    path,job=seed(tmp_path,monkeypatch,'scheduler:987654321:dead')
    assert state.lease_owner_alive('scheduler:987654321:dead') is False
    assert state.leases(path) == {}
    assert state.begin_manual_run(path,job)
    with state.connection(path) as conn:
        assert not conn.execute('SELECT * FROM cron_execution_leases').fetchall()


def test_startup_reclaim_also_clears_stale_manual_running_marker(tmp_path,monkeypatch):
    from nullion.cron_scheduler_state import load_jobs
    path,job=seed(tmp_path,monkeypatch,'manual:987654321:dead')
    assert state.reclaim_stale_execution_leases(path)=={job.id}
    refreshed=load_jobs(path)[0]
    assert refreshed.last_result=='error: execution_interrupted'
    assert state.begin_manual_run(path,refreshed)


@pytest.mark.parametrize('token',['scheduler:legacy', 'scheduler:'+str(os.getpid())+':live'])
def test_live_and_unknown_owners_keep_lease_and_block_overlap(tmp_path,monkeypatch,token):
    path,job=seed(tmp_path,monkeypatch,token)
    assert state.reclaim_stale_execution_leases(path) == set()
    assert state.begin_manual_run(path,job) is None
    assert state.leases(path) == {job.id:token}


def test_non_posix_ownership_is_unknown_and_never_uses_kill(monkeypatch):
    monkeypatch.setattr(state.os,'name','nt')
    monkeypatch.setattr(state.os,'kill',lambda *args:pytest.fail('Non-POSIX signal probe'))
    assert state.lease_owner_alive('scheduler:123:owner') is None


def test_telegram_startup_does_not_mark_other_live_worker_interrupted(tmp_path,monkeypatch):
    from nullion.cron_delivery import record_interrupted_cron_delivery_runs
    from nullion.events import make_event
    path,job=seed(tmp_path,monkeypatch,'scheduler:'+str(os.getpid())+':live')
    events=[make_event('cron.delivery.started','fixture',{'cron_id':job.id})]
    store=SimpleNamespace(list_events=lambda:events,add_event=events.append)
    assert record_interrupted_cron_delivery_runs(store,checkpoint_path=path)==0
    assert len(events)==1


def test_interruption_notice_is_immediate_durable_retried_and_not_repeated(tmp_path,monkeypatch):
    from nullion.tracker_status import start_check, dispatch_tracker_updates, tracker_statuses
    path,job=seed(tmp_path,monkeypatch,'scheduler:987654321:dead')
    with state.scheduled_token('scheduler:987654321:dead'):
        start_check(path,job,'interrupted-run')
    sent=[]
    now=state.now()
    def dispatch(at,success=True):
        return dispatch_tracker_updates(path,[job],now=at,
            resolve_route=lambda j:(j.delivery_channel,j.delivery_target),
            send=lambda j,c,t,text,kind:sent.append((text,kind)) or success)
    assert dispatch(now,False)==0
    assert sent[-1][1]=='interrupted'
    assert dispatch(now+timedelta(minutes=1))==0
    assert dispatch(now+timedelta(minutes=6))==1
    assert dispatch(now+timedelta(minutes=7))==0
    assert len(sent)==2
    assert 'No completed report' in sent[-1][0] and 'Ask me to run' in sent[-1][0]
    assert tracker_statuses(path,[job])[job.id]['state']=='interrupted'


@pytest.mark.parametrize('token,owner,status', [('scheduler:legacy','unknown','recovery_pending'),
    ('scheduler:'+str(os.getpid())+':live','live','running')])
def test_run_tool_reports_verified_busy_state_without_launching(tmp_path,monkeypatch,token,owner,status):
    from nullion.tools import create_core_tool_registry,register_cron_tools,ToolInvocation
    from nullion import crons
    path,job=seed(tmp_path,monkeypatch,token)
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',path)
    registry=create_core_tool_registry(workspace_root=tmp_path)
    register_cron_tools(registry,cron_runner=lambda *a,**k:pytest.fail('A second run must not launch'))
    result=registry.invoke(ToolInvocation('fixture','run_cron','web:recovery',{'id':job.id}))
    assert result.status=='failed'
    assert result.output['reason']=='cron_already_running'
    assert result.output['execution_started'] is False
    assert result.output['execution_owner_state']==owner and result.output['state']==status
