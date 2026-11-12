from datetime import datetime, timedelta, timezone
import json

import pytest

from tests.test_live_tracking import fixture, create, matcher
from nullion.live_tracking import tracking_deadline, LiveTrackerStore, Observation, utcnow
from nullion.live_tracking_tools import register_live_tracker_tools
from nullion.tools import ToolInvocation


@pytest.mark.parametrize('seconds',[1,17,10800,7*86400,90*86400,2*365*86400])
def test_any_positive_finite_window_survives_store_reload(fixture,seconds):
    now=utcnow();end=tracking_deadline(duration_seconds=seconds,now=now)
    job=create(fixture,now=now,expires_at=end)
    assert datetime.fromisoformat(job['expires_at'])-now==timedelta(seconds=seconds)
    assert LiveTrackerStore(fixture.store.path).get(job['id'])['expires_at']==job['expires_at']


@pytest.mark.parametrize('args',[{}, {'duration_seconds':0},{'duration_seconds':True},{'duration_seconds':1.5},
    {'duration_seconds':10**100},{'expires_at':'2030-01-01'}, {'expires_at':'2000-01-01T00:00:00Z'},
    {'expires_at':'2030-01-01T00:00:00Z','duration_seconds':10800}])
def test_window_requires_one_valid_future_deadline(args):
    with pytest.raises(ValueError):tracking_deadline(**args)


def test_absolute_deadline_preserves_the_users_clock_and_offset():
    end=tracking_deadline(expires_at='2030-11-03T18:30:00-05:00',now=datetime(2030,11,1,tzinfo=timezone.utc))
    assert end==datetime(2030,11,3,23,30,tzinfo=timezone.utc)


def test_absolute_update_and_restart_retry_after_deadline_remain_idempotent(fixture):
    now=utcnow();job=create(fixture,now=now,expires_at=now+timedelta(hours=1))
    end=now+timedelta(minutes=2)
    args=(job['id'],job['workspace_id'],job['conversation_id'])
    first=fixture.store.update_window(*args,request_key='absolute-change',expires_at=end,now=now)
    repeated=fixture.store.update_window(*args,request_key='absolute-change',expires_at=end,now=end+timedelta(seconds=1))
    assert repeated['expires_at']==first['expires_at']
    fixture.store.stop(job['id'],job['workspace_id'])
    end+=timedelta(hours=1)
    first=fixture.store.restart(*args,None,'absolute-restart',expires_at=end,now=now)
    repeated=fixture.store.restart(*args,None,'absolute-restart',expires_at=end,now=end+timedelta(seconds=1))
    assert repeated['expires_at']==first['expires_at']


def test_amending_window_reuses_identity_baseline_and_prevents_duplicate_extensions(fixture):
    now=utcnow();job=create(fixture,now=now,expires_at=now+timedelta(hours=1))
    claimed,token=fixture.store.claim(1,now=now)
    fixture.store.finish(job['id'],token,Observation('no_match',baseline=[23],
        observed_identity_keys=('already-seen',),observation_baseline_set=True),now=now)
    before=fixture.store.get(job['id'])
    start=now+timedelta(minutes=5)
    inflight,old_token=fixture.store.claim(1,now=start)
    assert inflight['id']==job['id']
    saved=fixture.store.update_window(job['id'],job['workspace_id'],job['conversation_id'],
        request_key='window-correction',duration_seconds=10800,interval_seconds=900,now=start)
    assert saved['expires_at']==(start+timedelta(hours=3)).isoformat()
    for key in ('id','probe','matcher','baseline','checks','seen_observation_keys'):
        assert saved.get(key)==before.get(key)
    assert saved['interval_seconds']==900 and len(fixture.store.list(job['workspace_id']))==1
    assert fixture.store.finish(job['id'],old_token,Observation('matched','Old result'),now=start) is False
    duplicate=fixture.store.update_window(job['id'],job['workspace_id'],job['conversation_id'],
        request_key='window-correction',duration_seconds=10800,now=start+timedelta(minutes=10))
    assert duplicate['expires_at']==saved['expires_at']
    with pytest.raises(ValueError):fixture.store.update_window(job['id'],job['workspace_id'],'web:other',
        request_key='foreign',duration_seconds=10800,now=start)


def test_paused_failure_is_not_silently_resumed_by_a_duration_change(fixture):
    now=utcnow();job=create(fixture,now=now,expires_at=now+timedelta(days=7))
    when=now
    for _ in range(3):
        claimed,token=fixture.store.claim(1,now=when)
        assert claimed
        fixture.store.finish(job['id'],token,Observation('source_error',reason='source_unavailable'),now=when)
        when=datetime.fromisoformat(fixture.store.get(job['id'])['next_check_at'])
    assert fixture.store.get(job['id'])['state']=='blocked'
    saved=fixture.store.update_window(job['id'],job['workspace_id'],job['conversation_id'],
        request_key='extend-paused',duration_seconds=30*86400,now=now+timedelta(minutes=1))
    assert saved['state']=='blocked' and saved['consecutive_errors']==3


def test_elapsed_window_uses_restart_admission_even_before_worker_marks_expired(fixture):
    now=utcnow();job=create(fixture,now=now,expires_at=now+timedelta(seconds=1))
    later=now+timedelta(seconds=2)
    assert fixture.store.get(job['id'])['state']=='active'
    with pytest.raises(ValueError,match='Resume'):
        fixture.store.update_window(job['id'],job['workspace_id'],job['conversation_id'],
            request_key='late-update',duration_seconds=10800,now=later)
    resumed=fixture.store.restart(job['id'],job['workspace_id'],job['conversation_id'],
        10800,'new-window',now=later)
    assert resumed['id']==job['id'] and resumed['expires_at']==(later+timedelta(hours=3)).isoformat()


def test_tool_preserves_full_relative_duration_after_source_setup(fixture,monkeypatch):
    from nullion import live_tracking_tools as tools
    now=utcnow()+timedelta(minutes=5)
    monkeypatch.setattr(tools,'utcnow',lambda:now)
    fixture.registry.live_tracker_store_path=fixture.store.path;register_live_tracker_tools(fixture.registry)
    result=fixture.registry.invoke(ToolInvocation('create','create_live_tracker','web:alpha',{
        'name':'観測','duration_seconds':10800,'interval_seconds':900,
        'probe':[{'tool_name':'sensor_read','arguments':{}}],'matcher':matcher()},capsule_id='request'))
    assert result.status=='completed',result
    config=result.output['deferred_execution']['configuration']
    assert config['duration_seconds']==10800 and config['accepted_at']==now.isoformat()
    assert datetime.fromisoformat(config['expires_at'])-now==timedelta(hours=3)
    assert config['deadline_kind']=='duration'


def test_deferred_configuration_uses_latest_receipt_for_each_owner_and_handle():
    from nullion.tools import ToolResult
    from nullion.workflow_reasoning import deferred_execution_receipts
    def receipt(ident,handle,seconds):
        return ToolResult(ident,'設定','completed',{'action_receipt':{'type':'action_receipt','object_id':handle},
            'deferred_execution':{'version':1,'phase':'accepted','owner':'観測','handle':handle,
                'configuration':{'duration_seconds':seconds}}})
    results=[receipt('first','one',10500),receipt('other','two',86400),receipt('amended','one',10800)]
    rows=deferred_execution_receipts(results)
    assert len(rows)==2
    assert next(r for r in rows if r['handle']=='one')['invocation_id']=='amended'
    assert next(r for r in rows if r['handle']=='one')['configuration']['duration_seconds']==10800
    assert next(r for r in rows if r['handle']=='two')['configuration']['duration_seconds']==86400
    assert results[0].output['deferred_execution']['configuration']['duration_seconds']==10500


def test_filter_update_preserves_snapshot_and_invalidates_stale_worker_and_notice(fixture):
    now = utcnow()
    old = {'kind':'semantic','history_mode':'snapshot','instruction':'Report all changed values.'}
    job = create(fixture, now=now, matcher=old, stop_after_match=False)
    current, token = fixture.store.claim(1, now=now)
    snapshot = {'outputs':[{'reading':23}], 'plan_binding':'opaque'}
    fixture.store.finish(job['id'], token, Observation('matched','Old broad alert',
        comparison_snapshot=snapshot), now=now)
    snapshot = fixture.store.get(job['id'])['comparison_snapshot']
    notice = fixture.store.claim_notice(now=now)
    inflight, token = fixture.store.claim(1, now=now+timedelta(seconds=6))
    assert inflight and notice
    updated = fixture.store.update_window(job['id'],job['workspace_id'],job['conversation_id'],
        request_key='filter-change', matcher={**old,'instruction':'変更は重要な競争だけ。'}, now=now+timedelta(seconds=7))
    for key in ('id','probe','expires_at','interval_seconds','comparison_snapshot','checks','state'):
        assert updated[key] == fixture.store.get(job['id'])[key]
    assert updated['expires_at']==job['expires_at'] and updated['comparison_snapshot']==snapshot
    assert updated['condition_revision']==1
    assert not fixture.store.notice_is_current(notice, now=now)
    assert fixture.store.finish(job['id'],token,Observation('matched','Stale broad alert'),now=now) is False
    duplicate=fixture.store.update_window(job['id'],job['workspace_id'],job['conversation_id'],
        request_key='filter-change',matcher={**old,'instruction':'変更は重要な競争だけ。'},now=now+timedelta(seconds=8))
    assert duplicate['condition_revision']==1


@pytest.mark.parametrize('condition',[{'kind':'semantic','instruction':'重要な変化だけ。','history_mode':'snapshot'},
    {'kind':'structured','conditions':[{'step':0,'pointer':'/reading','op':'greater_than','value':25}],'summary':'Límite superado'}])
def test_condition_tool_receipt_exposes_persisted_config_without_source_replay(fixture,condition):
    register_live_tracker_tools(fixture.registry)
    fixture.registry.live_tracker_store_path=fixture.store.path
    job=create(fixture)
    def invoke(name,args):
        return fixture.registry.invoke(ToolInvocation(invocation_id=name,tool_name=name,arguments=args,
            capsule_id='condition-turn',principal_id='web:alpha',flow_context={'conversation_id':'web:alpha'}))
    listed=invoke('list_live_trackers',{})
    assert listed.output['trackers'][0]['matcher']==job['matcher']
    result=invoke('update_live_tracker',{'id':job['id'],'matcher':condition})
    assert result.status=='completed',result
    saved=fixture.store.get(job['id'])
    assert saved['matcher']==condition and saved['expires_at']==job['expires_at']
    assert result.output['deferred_execution']['configuration']['matcher']==condition
    assert result.output['action_receipt']['object_id']==job['id']
    assert not fixture.readings, 'A condition-only amendment must reuse the saved source'
    before=saved.copy()
    failed=invoke('update_live_tracker',{'id':job['id'],'matcher':{'kind':'structured','conditions':[{'step':9,'pointer':'/reading','op':'equals','value':23}],'summary':'Bad'}})
    assert failed.status=='failed' and failed.output['executed'] is False
    assert fixture.store.get(job['id'])==before
