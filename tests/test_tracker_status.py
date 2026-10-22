from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path
import sqlite3

import pytest

from nullion.crons import CronJob
from nullion.tracker_status import (dispatch_tracker_updates, finish_check,
                                  start_check, tracker_statuses)

NOW = datetime(2026, 3, 8, 14, tzinfo=timezone.utc)


def owned_check(db,j,run_id,started):
    from nullion.cron_scheduler_state import connection,scheduled_token
    token='fixture:'+run_id
    with connection(db) as conn:
        conn.execute('INSERT OR REPLACE INTO cron_execution_leases VALUES (?,?,?)',
            (j.id,token,(NOW+timedelta(days=2)).isoformat()))
    with scheduled_token(token):start_check(db,j,run_id,now=started)


def job(**changes):
    return replace(CronJob(id='neutral', name='通知条件', task='条件を確認する', schedule='*/15 * * * *',
        delivery_channel='web', delivery_target='web:user-a', schedule_timezone='America/New_York',
        next_run=(NOW + timedelta(minutes=15)).isoformat(),
        artifact_delivery_options={'alert_delivery_mode':'verified_matches_only','tracker_health_notifications':True}), **changes)


def result(kind):
    if kind == 'approval': return {'suspended_for_approval':True}
    if kind == 'cancelled': return {'cron_run_cancelled':True}
    if kind == 'incomplete': return {'cron_run_failed':True, 'response_fulfilled':False}
    return {'execution_outcome':{'execution_succeeded':True}, 'response_fulfilled':True,
        'cron_alert_delivery_decision':'send' if kind == 'match' else 'silent',
        'cron_alert_delivery_reason':'verified_match' if kind == 'match' else 'no_verified_match'}


def check(path, j, kind, at, identity=None):
    identity = identity or at.isoformat()
    start_check(path,j,identity,now=at)
    finish_check(path,j,identity,result(kind),now=at + timedelta(seconds=5))


def dispatch(path,jobs,at,sent,deliver=True):
    return dispatch_tracker_updates(path,jobs,now=at,
        resolve_route=lambda j:(j.delivery_channel,j.delivery_target),
        send=lambda j,c,t,text,kind:sent.append((j.id,c,t,text)) or deliver)


@pytest.mark.parametrize('name,task', [('通知条件','条件を確認する'),('تنبيه','تحقق من الشرط'),('Aviso','Comprueba la condición')])
def test_verified_no_match_is_visible_but_does_not_alert_each_run(tmp_path,name,task):
    db=tmp_path/'runtime.db';j=job(name=name,task=task);sent=[]
    check(db,j,'no_match',NOW)
    status=tracker_statuses(db,[j],now=NOW+timedelta(seconds=6))[j.id]
    assert status['state']=='no_match' and status['successful_checks_24h']==1
    assert status['last_success_at'] and status['last_attempt_at']
    assert dispatch(db,[j],NOW+timedelta(seconds=6),sent)==0
    j.next_run=(NOW+timedelta(days=2)).isoformat()
    check(db,j,'no_match',NOW+timedelta(hours=23))
    assert dispatch(db,[j],NOW+timedelta(days=1,seconds=7),sent)==0
    assert sent == []
    assert dispatch(db,[j],NOW+timedelta(days=1,minutes=1),sent)==0




def test_unverified_or_delivery_failed_never_count_as_success(tmp_path):
    db=tmp_path/'r.db';j=job()
    variants=[{'cron_source_verification_required':True,'cron_source_verification_satisfied':False},
              {'cron_report_failed':True}, {'cron_delivery_failed':True},
              {'cron_alert_delivery_reason':'insufficient_evidence'},
              {'execution_outcome':{'execution_succeeded':'true'}}]
    for index,extra in enumerate(variants):
        run=str(index);start_check(db,j,run,now=NOW)
        finish_check(db,j,run,{**result('no_match'),**extra},now=NOW+timedelta(seconds=index))
    status=tracker_statuses(db,[j],now=NOW+timedelta(seconds=10))[j.id]
    assert status['successful_checks_24h']==0 and status['incomplete_checks_24h']==len(variants)
    assert status['last_success_at'] is None




def test_delivery_failure_retries_without_false_recovery(tmp_path):
    db=tmp_path/'r.db';j=job();sent=[]
    for i in range(12):check(db,j,'incomplete',NOW+timedelta(seconds=i))
    assert dispatch(db,[j],NOW+timedelta(seconds=10),sent,False)==0
    assert dispatch(db,[j],NOW+timedelta(minutes=1),sent)==0
    assert dispatch(db,[j],NOW+timedelta(minutes=6),sent)==1
    assert len(sent)==2
    check(db,j,'no_match',NOW+timedelta(minutes=7))
    assert dispatch(db,[j],NOW+timedelta(minutes=8),sent)==0


def test_no_recovery_message_for_warning_that_was_never_delivered(tmp_path):
    db=tmp_path/'r.db';j=job();sent=[]
    for i in range(12):check(db,j,'incomplete',NOW+timedelta(seconds=i))
    dispatch(db,[j],NOW+timedelta(seconds=10),sent,False)
    check(db,j,'match',NOW+timedelta(minutes=1))
    assert dispatch(db,[j],NOW+timedelta(minutes=2),sent)==0
    assert len(sent)==1


def test_atomic_claim_between_two_scheduler_instances(tmp_path):
    db=tmp_path/'r.db';j=job(next_run=(NOW+timedelta(days=3)).isoformat());sent=[]
    for i in range(12):check(db,j,'incomplete',NOW+timedelta(seconds=i))
    with ThreadPoolExecutor(max_workers=2) as pool:
        counts=list(pool.map(lambda _:dispatch(db,[j],NOW+timedelta(days=1),sent), range(2)))
    assert sum(counts)==1 and len(sent)==1


def test_scope_change_invalidates_success_and_cross_recipient_notice(tmp_path):
    db=tmp_path/'r.db';j=job();check(db,j,'no_match',NOW)
    for changed in [replace(j,task='別の条件'),replace(j,delivery_target='web:other'),replace(j,workspace_id='other')]:
        status=tracker_statuses(db,[changed],now=NOW+timedelta(seconds=6))[j.id]
        assert status['state']=='not_checked' and status['last_success_at'] is None


def test_duplicate_finish_and_out_of_order_completion(tmp_path):
    db=tmp_path/'r.db';j=job()
    start_check(db,j,'slow',now=NOW)
    check(db,j,'no_match',NOW+timedelta(minutes=1),identity='fresh')
    assert tracker_statuses(db,[j],now=NOW+timedelta(minutes=1,seconds=6))[j.id]['state']=='no_match'
    finish_check(db,j,'slow',result('incomplete'),now=NOW+timedelta(minutes=2))
    finish_check(db,j,'fresh',result('incomplete'),now=NOW+timedelta(minutes=3))
    status=tracker_statuses(db,[j],now=NOW+timedelta(minutes=4))[j.id]
    assert status['state']=='no_match' and status['consecutive_failures']==0
    assert status['successful_checks_24h']==1 and status['incomplete_checks_24h']==1


def test_running_and_missed_schedule_are_observable(tmp_path):
    db=tmp_path/'r.db';j=job();sent=[]
    owned_check(db,j,'hanging',NOW)
    assert tracker_statuses(db,[j],now=NOW+timedelta(minutes=1))[j.id]['state']=='running'
    assert tracker_statuses(db,[j],now=NOW+timedelta(minutes=11))[j.id]['state']=='running'
    assert dispatch(db,[j],NOW+timedelta(minutes=11),sent)==0
    finish_check(db,j,'hanging',result('no_match'),now=NOW+timedelta(minutes=12))
    assert dispatch(db,[j],NOW+timedelta(minutes=13),sent)==0
    missing=job(id='missing',next_run=NOW.isoformat())
    assert tracker_statuses(db,[missing],now=NOW+timedelta(minutes=11))[missing.id]['state']=='delayed'


@pytest.mark.parametrize('kind,state',[('approval','waiting_for_approval'),('cancelled','cancelled')])
def test_approval_and_cancel_are_not_success_or_failed_checks(tmp_path,kind,state):
    db=tmp_path/'r.db';j=job();check(db,j,kind,NOW)
    s=tracker_statuses(db,[j],now=NOW+timedelta(seconds=10))[j.id]
    assert s['state']==state and s['successful_checks_24h']==0 and s['consecutive_failures']==0


def test_sparse_schedule_digest_does_not_invent_a_negative(tmp_path):
    db=tmp_path/'r.db';j=job(next_run=(NOW+timedelta(days=7)).isoformat());sent=[]
    check(db,j,'no_match',NOW)
    dispatch(db,[j],NOW+timedelta(minutes=1),sent)
    dispatch(db,[j],NOW+timedelta(days=2),sent)
    assert sent == []


def test_in_memory_runtime_has_no_health_persistence(tmp_path):
    from nullion.tracker_status import health_store_path
    assert health_store_path(object()) is None
    assert tracker_statuses(None,[job()])=={}
    assert not list(tmp_path.iterdir())




def test_pending_notification_revalidates_deleted_muted_or_rerouted_jobs(tmp_path):
    db=tmp_path/'r.db';j=job(next_run=(NOW+timedelta(days=3)).isoformat());sent=[]
    dispatch(db,[j],NOW,sent)
    assert dispatch_tracker_updates(db,[j],now=NOW+timedelta(days=1),
        resolve_route=lambda j:(j.delivery_channel,j.delivery_target),current_jobs=lambda:[],
        send=lambda *args:sent.append(args) or True)==0
    assert not sent
    for i in range(12):check(db,j,'incomplete',NOW+timedelta(days=1,minutes=i))
    changed=replace(j,delivery_target='web:new')
    assert dispatch_tracker_updates(db,[j],now=NOW+timedelta(days=1,minutes=4),
        resolve_route=lambda j:(j.delivery_channel,j.delivery_target),current_jobs=lambda:[changed],
        send=lambda *args:sent.append(args) or True)==0
    assert not sent


def test_resolved_default_recipient_does_not_reuse_other_recipient_warning(tmp_path):
    db=tmp_path/'r.db';j=job(delivery_target='');sent=[]
    for i in range(12):check(db,j,'incomplete',NOW+timedelta(seconds=i))
    for target in ('user-a','user-b'):
        assert dispatch_tracker_updates(db,[j],now=NOW+timedelta(seconds=10),
            resolve_route=lambda j:('telegram',target),send=lambda *args:sent.append(args) or True)==1
    assert [args[2] for args in sent]==['user-a','user-b']


def test_status_names_cannot_be_attachment_directives(tmp_path):
    from nullion.artifacts import media_candidate_paths_from_text
    db=tmp_path/'r.db';j=job(name='MEDIA:/secret.pdf\n<script>value</script>',next_run=(NOW+timedelta(days=3)).isoformat());sent=[]
    for i in range(12):check(db,j,'incomplete',NOW+timedelta(seconds=i))
    dispatch(db,[j],NOW+timedelta(days=1),sent)
    assert not media_candidate_paths_from_text(sent[0][-1])


@pytest.mark.parametrize('value',['false',0,None,{}])
def test_notification_preference_requires_boolean(tmp_path,monkeypatch,value):
    from nullion import crons
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'r.db')
    with pytest.raises(ValueError,match='boolean'):
        crons.add_cron('条件','0 9 * * *','確認',artifact_delivery_options={'tracker_status_notifications':value})


@pytest.mark.parametrize('name,task',[('通知条件','条件を確認する'),('تنبيه','تحقق من الشرط')])
def test_delayed_occurrence_and_slow_check_never_start_failure_episode(tmp_path,name,task):
    db=tmp_path/'r.db';j=job(name=name,task=task,next_run=NOW.isoformat());sent=[]
    check(db,j,'no_match',NOW-timedelta(minutes=30))
    for minutes in (11,30,65):
        status=tracker_statuses(db,[j],now=NOW+timedelta(minutes=minutes))[j.id]
        assert status['state']=='delayed' and status['overdue']
        assert status['consecutive_failures']==0 and status['last_success_at']
        assert dispatch(db,[j],NOW+timedelta(minutes=minutes),sent)==0
    owned_check(db,j,'slow',NOW+timedelta(minutes=66))
    assert tracker_statuses(db,[j],now=NOW+timedelta(minutes=90))[j.id]['state']=='running'
    assert dispatch(db,[j],NOW+timedelta(minutes=90),sent)==0
    finish_check(db,j,'slow',result('no_match'),now=NOW+timedelta(minutes=91))
    assert dispatch(db,[j],NOW+timedelta(minutes=92),sent)==0
    assert not sent


def test_upgrade_clears_legacy_delay_warning_without_recovery_alert(tmp_path):
    import hashlib,json
    from nullion.tracker_status import _scope
    db=tmp_path/'r.db';j=job(next_run=NOW.isoformat());sent=[]
    check(db,j,'no_match',NOW-timedelta(minutes=1))
    route=(str(j.workspace_id),j.delivery_channel,j.delivery_target)
    key=_scope(j)+':route:'+hashlib.sha256(json.dumps(route).encode()).hexdigest()
    with sqlite3.connect(db) as conn:
        conn.execute('INSERT INTO cron_tracker_state VALUES (?,?)',(key,json.dumps({'warning_sent_at':NOW.isoformat()})))
    assert dispatch(db,[j],NOW+timedelta(minutes=11),sent)==0
    with sqlite3.connect(db) as conn:
        state=json.loads(conn.execute('SELECT payload FROM cron_tracker_state WHERE scope=?',(key,)).fetchone()[0])
        assert state['warning_sent_at']  # Legacy notices are inert; no recovery is sent.
    assert dispatch(db,[j],NOW+timedelta(minutes=12),sent)==0
    assert not sent
