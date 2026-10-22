from datetime import datetime,timedelta,timezone
import threading,time
from dataclasses import replace
import pytest
from nullion import crons
from nullion.crons import CronJob,CronScheduler,save_crons,get_cron,update_cron,remove_cron
from nullion import cron_scheduler_state as state

@pytest.fixture
def db(tmp_path,monkeypatch):
    path=tmp_path/'runtime.db'
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',path)
    monkeypatch.setenv('NULLION_HOME',str(tmp_path))
    return path

def due(identity):
    return CronJob(id=identity,name=identity,task='Check',schedule='*/5 * * * *',next_run=(datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat())

def wait(predicate,timeout=3):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if predicate():return
        time.sleep(.01)
    assert predicate()


def test_slow_jobs_start_together_and_capacity_is_bounded(db):
    save_crons([due('a'),due('b'),due('c')]);entered=[];gate=threading.Event()
    def fire(job):entered.append(job.id);gate.wait(3);return {}
    scheduler=CronScheduler(fire,max_parallel_jobs=2)
    try:
        start=time.monotonic();scheduler._tick();wait(lambda:len(entered)==2)
        assert time.monotonic()-start<.5
        assert scheduler.snapshot(crons.load_crons())['jobs']['c']=='waiting_for_capacity'
        scheduler._tick();assert len(entered)==2
        assert get_cron('c').last_run is None
    finally:
        scheduler.stop();gate.set();wait(lambda:len(state.leases(db))==0)


def test_completion_wakes_scheduler_without_waiting_for_poll(db):
    save_crons([due('a'),due('b')]);first=threading.Event();release=threading.Event();second=threading.Event()
    def fire(job):
        if job.id=='a':first.set();release.wait(3)
        else:second.set()
    scheduler=CronScheduler(fire,tick_interval=30,max_parallel_jobs=1)
    scheduler.start()
    try:
        scheduler._wake.set();assert first.wait(1)
        start=time.monotonic();release.set();assert second.wait(.5)
        assert time.monotonic()-start<.5
    finally:
        scheduler.stop();release.set();wait(lambda:len(state.leases(db))==0)


def test_same_job_cannot_overlap_even_when_due_again(db):
    save_crons([due('a')]);entered=[];gate=threading.Event()
    scheduler=CronScheduler(lambda job:entered.append(job.id) or gate.wait(3),max_parallel_jobs=4)
    try:
        scheduler._tick();wait(lambda:entered==['a'])
        j=get_cron('a');j.next_run=due('a').next_run;save_crons([j])
        scheduler._tick();assert entered==['a']
        with state.execution_lease(db,'a') as acquired:assert not acquired
    finally:scheduler.stop();gate.set();wait(lambda:len(state.leases(db))==0)


def test_two_schedulers_share_atomic_claim_and_capacity(db):
    save_crons([due('a'),due('b')]);entered=[];gate=threading.Event()
    fire=lambda job:entered.append(job.id) or gate.wait(3)
    a=CronScheduler(fire,max_parallel_jobs=1);b=CronScheduler(fire,max_parallel_jobs=1)
    try:
        threads=[threading.Thread(target=s._tick) for s in (a,b)]
        for t in threads:t.start()
        for t in threads:t.join()
        wait(lambda:len(entered)==1)
        assert len(state.leases(db))==1
    finally:
        a.stop();b.stop();gate.set();wait(lambda:len(state.leases(db))==0)

@pytest.mark.parametrize('change',['disable','delete','task','schedule'])
def test_completion_preserves_concurrent_user_edits(db,change):
    save_crons([due('a')]);entered=threading.Event();gate=threading.Event()
    def fire(j):entered.set();gate.wait(3);return {}
    scheduler=CronScheduler(fire,max_parallel_jobs=1)
    try:
        scheduler._tick();assert entered.wait(1)
        if change=='delete':remove_cron('a')
        elif change=='disable':update_cron('a',enabled=False)
        elif change=='task':update_cron('a',task='Revised')
        else:update_cron('a',schedule='0 9 * * *')
        gate.set();wait(lambda:not state.leases(db))
        j=get_cron('a')
        if change=='delete':assert j is None
        elif change=='disable':assert j.enabled is False
        elif change=='task':assert j.task=='Revised' and j.last_result is None
        else:assert j.schedule=='0 9 * * *' and j.last_result=='ok'
    finally:scheduler.stop();gate.set();wait(lambda:not state.leases(db))


def test_limit_persists_and_lowering_does_not_cancel_running_jobs(db):
    save_crons([due('a'),due('b'),due('c')]);entered=[];gate=threading.Event()
    scheduler=CronScheduler(lambda j:entered.append(j.id) or gate.wait(3),max_parallel_jobs=2)
    try:
        scheduler._tick();wait(lambda:len(entered)==2)
        scheduler.set_max_parallel_jobs(1);scheduler._tick()
        assert len(entered)==2 and scheduler.snapshot()['max_parallel_jobs']==1
        assert CronScheduler(lambda j:None).max_parallel_jobs==1
    finally:scheduler.stop();gate.set();wait(lambda:not state.leases(db))

@pytest.mark.parametrize('value',[0,33,True,'4',1.5,None])
def test_invalid_capacity_is_rejected(db,value):
    with pytest.raises(ValueError):state.write_limit(db,value)


def test_stale_lease_can_recover_and_old_owner_cannot_overwrite(db):
    save_crons([due('a')]);first,token=state.claim_scheduled(db,get_cron('a'),1)
    j=get_cron('a');j.next_run=due('a').next_run;save_crons([j])
    with state.connection(db) as conn:conn.execute("UPDATE cron_execution_leases SET expires_at='2000-01-01T00:00:00+00:00'")
    fresh,new_token=state.claim_scheduled(db,get_cron('a'),1)
    state.save_result(db,first,'old');assert get_cron('a').last_result is None
    state.release(db,'a',token);assert 'a' in state.leases(db)
    state.save_result(db,fresh,'ok');state.release(db,'a',new_token)
    assert get_cron('a').last_result=='ok'

def test_scheduled_and_manual_runs_share_the_execution_gate(db):
    save_crons([due('a')])
    with state.execution_lease(db,'a') as acquired:
        assert acquired
        assert state.claim_scheduled(db,get_cron('a'),4) is None
        assert get_cron('a').last_run is None
    assert state.claim_scheduled(db,get_cron('a'),4) is not None
    for identity,token in state.leases(db).items():state.release(db,identity,token)


def test_cron_browser_context_cannot_reuse_another_runs_session():
    from nullion.cron_execution_tools import CronExecutionToolRegistry
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolInvocation,ToolResult
    registry=ToolRegistry();seen=[]
    spec=ToolSpec(name='browser_navigate',description='Browser',risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=1,
        capability_tags=('browser',),input_schema={'type':'object','properties':{},'additionalProperties':True})
    def handle(inv):seen.append(inv.flow_context);return ToolResult(inv.invocation_id,inv.tool_name,'completed',{})
    registry.register(spec,handle)
    for token in ('cron-run-a','cron-run-b'):
        scoped=CronExecutionToolRegistry(registry,browser_session_id=token)
        result=scoped.invoke(ToolInvocation(token,'browser_navigate','telegram:same-user',{},
            flow_context={'browser_session_id':'previous-shared-session'}))
        assert result.status=='completed'
    assert [context['browser_session_id'] for context in seen]==['cron-run-a','cron-run-b']
