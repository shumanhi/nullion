"""Opt-in live tracker delivery probe using isolated, clearly marked fixtures.

Exercises the actual stage notification worker. Only fixture ledger timestamps
are accelerated; no external source check or ticket availability is asserted.
Leave the fixture chat available for visual inspection, then use --cleanup.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import urllib.request
import uuid

from nullion.crons import CronJob
from nullion.tracker_status import _scope, start_check, finish_check


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--cleanup',action='store_true')
    args=parser.parse_args()
    assert os.environ.get('NULLION_LIVE_STAGE_E2E')=='1'
    base='http://127.0.0.1:8753';db=Path.home()/'.nullion-stage/runtime.db'
    assert db.exists()
    def api(path,method='GET',data=None):
        request=urllib.request.Request(base+path,method=method,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=10) as response:return json.load(response)
    def save(value):
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(value,indent=2,ensure_ascii=False));args.output.chmod(0o600)
    def cleanup(evidence):
        for identifier in evidence.get('jobs',[]):api('/api/crons/'+identifier,method='DELETE')
        scopes=evidence.get('scopes',[])
        with sqlite3.connect(db) as conn:
            for scope in scopes:
                conn.execute('DELETE FROM cron_tracker_runs WHERE scope=?',(scope,))
                conn.execute('DELETE FROM cron_tracker_state WHERE scope=? OR scope LIKE ?',(scope,scope+':route:%'))
                conn.execute('DELETE FROM cron_tracker_notices WHERE notice_key LIKE ?',(scope+':route:%',))
            key=evidence.get('daily_key')
            if key:
                conn.execute('DELETE FROM cron_tracker_state WHERE scope=?',(key,))
                conn.execute('DELETE FROM cron_tracker_notices WHERE notice_key LIKE ?',(key+':%',))
        evidence['cleaned_up']=True;save(evidence)
    if args.cleanup:
        cleanup(json.loads(args.output.read_text()));return
    identifier=uuid.uuid4().hex[:10];target='web:tracker-status-stage-'+identifier
    workspace='workspace_tracker_probe_'+identifier
    evidence={'fixture':identifier,'conversation_id':target,'workspace_id':workspace,'jobs':[],'scopes':[]}
    jobs=[]
    now=datetime.now(timezone.utc)
    for index in range(2):
        created=api('/api/crons','POST',{'name':f'[Stage verification] Condition {index+1}',
            'task':'Synthetic tracker ledger fixture; no source availability claim.', 'schedule':'0 0 1 1 *',
            'enabled':False,'workspace_id':workspace,'delivery_channel':'web','delivery_target':target,
            'artifact_delivery_options':{'alert_delivery_mode':'verified_matches_only'}})
        job=CronJob.from_dict(created['job']);jobs.append(job);evidence['jobs'].append(job.id);evidence['scopes'].append(_scope(job));save(evidence)
        kinds=['no_match'] if index==0 else ['incomplete']*3
        for n,kind in enumerate(kinds):
            at=now-(timedelta(minutes=30) if index==0 else timedelta(seconds=40-n*5));rid=f'{identifier}-{index}-{n}'
            start_check(db,job,rid,now=at)
            outcome=({'cron_report_failed':True,'response_fulfilled':False} if kind=='incomplete' else
                {'execution_outcome':{'execution_succeeded':True},'response_fulfilled':True,
                 'cron_alert_delivery_decision':'silent','cron_alert_delivery_reason':'no_verified_match'})
            finish_check(db,job,rid,outcome,now=at+timedelta(seconds=1))
        api('/api/crons/'+job.id,'PATCH',{'enabled':True})
    # A long-running check and a legacy delay warning must stay quiet.
    slow_id=identifier+'-slow'
    start_check(db,jobs[0],slow_id,now=now-timedelta(minutes=12))
    route=(workspace,'web',target)
    legacy_scope=_scope(jobs[0])+':route:'+hashlib.sha256(json.dumps(route).encode()).hexdigest()
    with sqlite3.connect(db) as conn:
        conn.execute('INSERT OR REPLACE INTO cron_tracker_state VALUES (?,?)',
                     (legacy_scope,json.dumps({'warning_sent_at':(now-timedelta(minutes=20)).isoformat()})))
    def wait_for(text):
        deadline=time.monotonic()+55
        while time.monotonic()<deadline:
            history=api('/api/chat/history/'+target+'?sync_runtime=0')['messages']
            matches=[m for m in history if m.get('role')=='bot' and text in m.get('text','')]
            if matches:return matches[-1]['text']
            time.sleep(1)
        raise RuntimeError('Stage tracker delivery not observed: '+text)
    try:
        evidence['warning']=wait_for('last 3 checks could not verify')
        at=datetime.now(timezone.utc);rid=identifier+'-recovery'
        start_check(db,jobs[1],rid,now=at)
        finish_check(db,jobs[1],rid,{'execution_outcome':{'execution_succeeded':True},'response_fulfilled':True,
            'cron_alert_delivery_decision':'silent','cron_alert_delivery_reason':'no_verified_match'},now=at+timedelta(seconds=1))
        evidence['recovery']=wait_for('Checks are working again')
        rows=api('/api/crons')
        assert next(j for j in rows if j['id']==jobs[0].id)['tracker_status']['state']=='running'
        finish_check(db,jobs[0],slow_id,{'execution_outcome':{'execution_succeeded':True},'response_fulfilled':True,
            'cron_alert_delivery_decision':'silent','cron_alert_delivery_reason':'no_verified_match'})
        route=(workspace,'web',target);key='daily:'+hashlib.sha256(json.dumps(route).encode()).hexdigest();evidence['daily_key']=key
        with sqlite3.connect(db) as conn:
            row=conn.execute('SELECT payload FROM cron_tracker_state WHERE scope=?',(key,)).fetchone();assert row
            payload=json.loads(row[0]);payload['daily_since']=(datetime.now(timezone.utc)-timedelta(days=1,minutes=1)).isoformat()
            conn.execute('UPDATE cron_tracker_state SET payload=? WHERE scope=?',(json.dumps(payload),key))
        evidence['daily_summary']=wait_for('Daily tracker summary')
        assert all(j.name in evidence['daily_summary'].replace("\\[", "[").replace("\\]", "]") for j in jobs)
        rows=api('/api/crons');evidence['status']=[j for j in rows if j['id'] in evidence['jobs']]
        assert all(j['tracker_status']['state']=='no_match' for j in evidence['status'])
        assert all(j['tracker_status']['last_success_at'] for j in evidence['status'])
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            with sqlite3.connect(db) as conn:
                events=conn.execute("SELECT payload FROM runtime_events WHERE collection='events' AND json_extract(payload,'$.event_type')='cron.tracker_status.sent' AND json_extract(payload,'$.payload.conversation_id')=? ORDER BY json_extract(payload,'$.created_at')",(target,)).fetchall()
            if len(events)==3:break
            time.sleep(0.5)
        evidence['events']=[json.loads(row[0]) for row in events]
        assert [e['payload']['notification_kind'] for e in evidence['events']]==['warning','recovery','daily']
        assert all(e['payload']['cron_id']==jobs[1].id for e in evidence['events'][:2])
        with sqlite3.connect(db) as conn:
            legacy=json.loads(conn.execute('SELECT payload FROM cron_tracker_state WHERE scope=?',(legacy_scope,)).fetchone()[0])
        assert legacy['warning_sent_at'] is None
        evidence['slow_check_and_legacy_warning_stayed_quiet']=True
        evidence['passed']=True;save(evidence)
        print(json.dumps({'passed':True,'conversation_id':target,'jobs':evidence['jobs']}))
    except BaseException:
        cleanup(evidence);raise


if __name__=='__main__':main()
