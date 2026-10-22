from datetime import datetime,timedelta,timezone
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import pytest
from nullion.crons import CronJob
from nullion.tracker_status import start_check,finish_check,dispatch_tracker_updates,tracker_statuses

NOW=datetime(2026,10,15,12,tzinfo=timezone.utc)
@pytest.fixture
def isolated(tmp_path,monkeypatch):
    from nullion import crons
    monkeypatch.setenv('NULLION_HOME',str(tmp_path))
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'runtime.db')
    return tmp_path/'runtime.db'

def job(**kw):
    return replace(CronJob(id='watch',name='NYC–SFO fare tracker under $600',schedule='*/30 * * * *',task='Stored requirements',
        workspace_id='workspace_admin',delivery_channel='web',delivery_target='web:fixture',schedule_timezone='America/New_York',
        artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'},next_run=(NOW+timedelta(days=3)).isoformat()),**kw)

def record(db,j,kind,index):
    when=NOW+timedelta(minutes=index);key=str(index)
    start_check(db,j,key,now=when)
    result={'execution_outcome':{'execution_succeeded':True},'response_fulfilled':True,
        'cron_alert_delivery_decision':'send' if kind=='match' else 'silent',
        'cron_alert_delivery_reason':'verified_match' if kind=='match' else 'no_verified_match'}
    if kind=='failure':result={'cron_run_failed':True,'reason':'cron_run_model_timeout'}
    if kind=='approval':result={'suspended_for_approval':True}
    if kind=='cancelled':result={'cron_run_cancelled':True}
    finish_check(db,j,key,result,now=when+timedelta(seconds=1))
    return result

def owned_check(db, j, run_id, started):
    from nullion.cron_scheduler_state import connection, scheduled_token
    token='fixture:'+run_id
    with connection(db) as conn:
        conn.execute('INSERT OR REPLACE INTO cron_execution_leases VALUES (?,?,?)',
                     (j.id,token,(NOW+timedelta(days=2)).isoformat()))
    with scheduled_token(token):
        start_check(db,j,run_id,now=started)

def tick(db,j,sent,minute=30,**kwargs):
    return dispatch_tracker_updates(db,[j],now=NOW+timedelta(minutes=minute),
        resolve_route=lambda j:(j.delivery_channel,j.delivery_target),
        send=lambda j,c,t,text,kind:sent.append((text,kind)) or True,**kwargs)

@pytest.mark.parametrize('kind,notice_kind',[('no_match','no_match'),('failure','failure')])
def test_exact_threshold_once_per_episode_and_no_daily_or_recovery(isolated,kind,notice_kind):
    db=isolated;j=job();sent=[]
    for i in range(11):record(db,j,kind,i)
    assert tick(db,j,sent)==0
    record(db,j,kind,11)
    assert tick(db,j,sent)==1
    assert sent[0][1]==notice_kind and j.name in sent[0][0] and '12' in sent[0][0]
    if kind=='failure':assert 'model/provider' in sent[0][0] and 'No repair has been confirmed' in sent[0][0]
    else:assert 'successfully' in sent[0][0] and 'saved conditions' in sent[0][0]
    for i in range(12,25):record(db,j,kind,i)
    assert tick(db,j,sent,minute=1440)==0
    record(db,j,'match',1441)
    assert tick(db,j,sent,minute=1442)==0
    for i in range(1443,1455):record(db,j,kind,i)
    assert tick(db,j,sent,minute=1460)==1
    assert len(sent)==2 and all(k not in {'daily','recovery'} for _,k in sent)

@pytest.mark.parametrize('interrupt',['match','failure','approval','cancelled'])
def test_no_match_streak_requires_consecutive_verified_checks(isolated,interrupt):
    db=isolated;j=job();sent=[]
    for i in range(11):record(db,j,'no_match',i)
    record(db,j,interrupt,11);record(db,j,'no_match',12)
    assert tick(db,j,sent)==0
    assert tracker_statuses(db,[j],now=NOW+timedelta(hours=1))[j.id]['consecutive_no_matches']==1

def test_running_or_missed_check_does_not_create_failure(isolated):
    db=isolated;j=job(next_run=(NOW-timedelta(days=1)).isoformat());sent=[]
    owned_check(db,j,'long',NOW)
    assert tick(db,j,sent,minute=1440)==0
    assert tracker_statuses(db,[j],now=NOW+timedelta(days=1))[j.id]['consecutive_failures']==0

def test_doctor_action_persists_once_and_resolves_without_recovery_notice(isolated):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.doctor_crons import sync_cron_doctor_action
    db=isolated;j=job();sent=[];runtime=bootstrap_persistent_runtime(db)
    observer=lambda j,f,n,s:sync_cron_doctor_action(runtime,j,f,n,scope=s)
    for i in range(12):record(db,j,'failure',i)
    assert tick(db,j,sent,observe=observer)==1
    assert tick(db,j,sent,observe=observer)==0
    actions=runtime.store.list_doctor_actions()
    assert len(actions)==1 and actions[0]['status']=='pending'
    record(db,j,'no_match',31)
    assert tick(db,j,sent,minute=32,observe=observer)==0
    assert runtime.store.list_doctor_actions()[0]['status']=='cancelled'
    assert len(sent)==1

def test_monitor_setting_and_threshold_preserve_check_history(isolated):
    db=isolated;j=job();sent=[]
    for i in range(5):record(db,j,'failure',i)
    muted=replace(j,artifact_delivery_options={**j.artifact_delivery_options,'doctor_monitoring_enabled':False,'doctor_streak_limit':5})
    assert tick(db,muted,sent)==0
    enabled=replace(muted,artifact_delivery_options={**muted.artifact_delivery_options,'doctor_monitoring_enabled':True})
    assert tick(db,enabled,sent)==1
    assert '5 checks' in sent[0][0]

def test_ordinary_report_is_also_monitored_and_language_independent(isolated):
    db=isolated;j=job(name='確認',task='条件を確認する',artifact_delivery_options={});sent=[]
    for i in range(12):record(db,j,'failure',i)
    assert tick(db,j,sent)==1 and '確認' in sent[0][0]

def test_restart_and_concurrent_workers_do_not_duplicate(isolated):
    db=isolated;j=job();sent=[]
    for i in range(12):record(db,j,'failure',i)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _:tick(db,j,sent),range(4)))
    assert len(sent)==1
    assert tick(db,j,sent)==0

def test_late_completions_are_counted_in_run_order(isolated):
    db=isolated;j=job();sent=[]
    owned_check(db,j,'older',NOW-timedelta(minutes=1))
    for i in range(11):record(db,j,'no_match',i)
    assert tick(db,j,sent)==0
    finish_check(db,j,'older',{'execution_outcome':{'execution_succeeded':True},'response_fulfilled':True,
        'cron_alert_delivery_decision':'silent','cron_alert_delivery_reason':'no_verified_match'},now=NOW+timedelta(minutes=20))
    assert tick(db,j,sent)==1

@pytest.mark.parametrize('change',['pause','mute','edit'])
def test_doctor_incident_closes_when_monitoring_scope_is_removed(isolated,change):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.doctor_crons import sync_cron_doctor_action
    db=isolated;j=job();sent=[];runtime=bootstrap_persistent_runtime(db)
    observer=lambda j,f,n,s:sync_cron_doctor_action(runtime,j,f,n,scope=s)
    for i in range(12):record(db,j,'failure',i)
    tick(db,j,sent,observe=observer)
    changed=(replace(j,enabled=False) if change=='pause' else
        replace(j,artifact_delivery_options={**j.artifact_delivery_options,'doctor_monitoring_enabled':False}) if change=='mute' else
        replace(j,task='Revised requirements'))
    tick(db,changed,sent,observe=observer)
    assert runtime.store.list_doctor_actions()[0]['status']=='cancelled'
    assert len(sent)==1

@pytest.mark.parametrize('value',[True,0,-1,1001,1.5,'12'])
def test_threshold_validation_is_typed(isolated,value):
    from nullion.crons import add_cron
    with pytest.raises(ValueError):
        add_cron('Task','0 9 * * *','Check',artifact_delivery_options={'doctor_streak_limit':value})
