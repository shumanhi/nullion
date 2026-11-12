"""A startup receipt cannot overwrite the owning background run's terminal state."""
import threading

import pytest


def stored_job(tmp_path,monkeypatch):
    from nullion import crons
    path=tmp_path/'runtime.db'
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',path)
    job=crons.add_cron('Owned fixture','0 0 1 1 *','Create the prepared report.',enabled=False,
        delivery_channel='web',delivery_target='web:owned')
    return path,job


@pytest.mark.parametrize('result,expected',[
    ({'cron_delivery_status':'saved','execution_outcome':{'execution_succeeded':True}},'ok'),
    ({'cron_run_failed':True,'reason':'source_unavailable'},'error: source_unavailable'),
    ({'cron_run_cancelled':True},'cancelled'),
    ({'suspended_for_approval':True},'waiting_for_approval'),
    ({'cron_delivery_partial_success':True},'partial_success'),
])
def test_manual_background_saves_terminal_result_without_enabling_job(tmp_path,monkeypatch,result,expected):
    from nullion import crons
    from nullion.cron_delivery import CronRunDeliveryCallbacks,start_manual_cron_background_delivery
    from nullion.cron_scheduler_state import begin_manual_run,load_jobs
    path,job=stored_job(tmp_path,monkeypatch);stamp=begin_manual_run(path,job);assert stamp
    job.last_run=stamp;terminal=threading.Event()
    callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:'web',delivery_target=lambda j,c:'web:owned',
        run_agent_turn=lambda *a:{},record_event=lambda *a,**kw:None,block_reason=lambda *a:None,
        save_web_delivery=lambda *a:True,send_platform_delivery=lambda *a:False,health_store_path=path)
    receipt=start_manual_cron_background_delivery(job,label='Manual',callbacks=callbacks,
        workflow_runner=lambda *a,**kw:result,background_start_grace_seconds=0,
        status_update_callback=lambda group,text,finished:terminal.set() if finished else None)
    assert receipt['cron_delivery_status']=='deferred' and terminal.wait(3)
    actual=next(j for j in load_jobs(path) if j.id==job.id)
    assert actual.last_result==expected and actual.last_run==stamp and not actual.enabled


def test_run_stamp_preserves_edits_deleted_jobs_and_rejects_duplicate_start(tmp_path,monkeypatch):
    from nullion import crons
    from nullion.cron_scheduler_state import begin_manual_run,load_jobs,save_result
    path,job=stored_job(tmp_path,monkeypatch);stamp=begin_manual_run(path,job);assert stamp
    current=next(j for j in load_jobs(path) if j.id==job.id)
    assert begin_manual_run(path,current) is None
    job.last_run=stamp
    crons.update_cron(job.id,name='Renamed while running')
    save_result(path,job,'ok')
    assert crons.get_cron(job.id).name=='Renamed while running' and crons.get_cron(job.id).last_result=='ok'
    crons.update_cron(job.id,task='Changed task')
    save_result(path,job,'stale result')
    assert crons.get_cron(job.id).last_result=='ok'
    crons.remove_cron(job.id);save_result(path,job,'stale result')
    assert crons.get_cron(job.id) is None


def test_run_cron_does_not_start_same_pending_job_twice(tmp_path,monkeypatch):
    from nullion import crons
    from nullion.tools import _build_run_cron_handler,ToolInvocation
    path,job=stored_job(tmp_path,monkeypatch);calls=[]
    def runner(job,invocation=None):
        calls.append(job.id)
        return {'cron_delivery_status':'deferred'}
    handler=_build_run_cron_handler(runner)
    first=handler(ToolInvocation('first','run_cron','web:owned',{'id':job.id}))
    stamp=crons.get_cron(job.id).last_run
    second=handler(ToolInvocation('second','run_cron','web:owned',{'id':job.id}))
    assert first.status=='completed' and second.status=='failed'
    assert calls==[job.id] and 'already running' in second.error
    assert crons.get_cron(job.id).last_run==stamp and crons.get_cron(job.id).last_result=='running'
