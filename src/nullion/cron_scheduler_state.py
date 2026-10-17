"""Transactional scheduled-run claims, leases and runtime capacity settings."""
from __future__ import annotations
from contextlib import contextmanager
from datetime import datetime,timedelta,timezone
from pathlib import Path
import json
import os
import sqlite3
import threading
import uuid

_local=threading.local()
LEASE_SECONDS=120

def validate_limit(value):
    if type(value) is not int or not 1 <= value <= 32:
        raise ValueError('max_parallel_jobs must be an integer from 1 to 32')
    return value

@contextmanager
def connection(path):
    from nullion.runtime_persistence import runtime_store_file_lock
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    with runtime_store_file_lock(path):
        with sqlite3.connect(str(path),timeout=10) as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS cron_execution_leases (cron_id TEXT PRIMARY KEY, token TEXT NOT NULL, expires_at TEXT NOT NULL)')
            conn.execute('CREATE TABLE IF NOT EXISTS cron_scheduler_settings (key TEXT PRIMARY KEY, payload TEXT NOT NULL)')
            conn.commit();conn.execute('BEGIN IMMEDIATE')
            yield conn

def now():return datetime.now(timezone.utc)
def expiry():return (now()+timedelta(seconds=LEASE_SECONDS)).isoformat()

def read_limit(path,default=None):
    if default is None:
        try:default=validate_limit(int(os.environ.get('NULLION_CRON_MAX_PARALLEL_JOBS','4')))
        except ValueError:default=4
    try:
        with sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro",uri=True,timeout=10) as conn:
            row=conn.execute("SELECT payload FROM cron_scheduler_settings WHERE key='max_parallel_jobs'").fetchone()
        return validate_limit(json.loads(row[0])) if row else default
    except sqlite3.OperationalError:
        return default


def write_limit(path,value):
    value=validate_limit(value)
    with connection(path) as conn:
        conn.execute("INSERT OR REPLACE INTO cron_scheduler_settings VALUES ('max_parallel_jobs',?)",(json.dumps(value),))
    return value

def claim_scheduled(path,job,limit):
    from nullion.crons import CronJob,_CRON_COLLECTION,_compute_job_next_run,_payload_is_deleted
    with connection(path) as conn:
        stamp=now().isoformat()
        conn.execute('DELETE FROM cron_execution_leases WHERE expires_at<=?',(stamp,))
        if conn.execute('SELECT 1 FROM cron_execution_leases WHERE cron_id=?',(job.id,)).fetchone():return None
        setting=conn.execute("SELECT payload FROM cron_scheduler_settings WHERE key='max_parallel_jobs'").fetchone()
        if setting:limit=validate_limit(json.loads(setting[0]))
        running=conn.execute("SELECT COUNT(*) FROM cron_execution_leases WHERE token LIKE 'scheduler:%'").fetchone()[0]
        if running>=limit:return None
        row=conn.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',(_CRON_COLLECTION,job.id)).fetchone()
        if not row or _payload_is_deleted(row[0]):return None
        data=json.loads(row[0]);current=CronJob.from_dict(data)
        if not current.enabled or current.next_run!=job.next_run or not current.next_run_dt() or current.next_run_dt()>now():return None
        token='scheduler:'+uuid.uuid4().hex
        conn.execute('INSERT INTO cron_execution_leases VALUES (?,?,?)',(job.id,token,expiry()))
        current.last_run=stamp;current.next_run=_compute_job_next_run(current,after=now())
        data.update(last_run=current.last_run,next_run=current.next_run)
        conn.execute('UPDATE reminders_crons SET payload=?,updated_at=? WHERE collection=? AND item_key=?',(json.dumps(data),stamp,_CRON_COLLECTION,job.id))
        return current,token

def release(path,job_id,token):
    with connection(path) as conn:conn.execute('DELETE FROM cron_execution_leases WHERE cron_id=? AND token=?',(job_id,token))

def renew(path,job_id,token):
    with connection(path) as conn:
        return conn.execute('UPDATE cron_execution_leases SET expires_at=? WHERE cron_id=? AND token=?',(expiry(),job_id,token)).rowcount==1

def leases(path):
    try:
        with sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro",uri=True,timeout=10) as conn:
            return {row[0]:row[1] for row in conn.execute('SELECT cron_id,token FROM cron_execution_leases WHERE expires_at>?',(now().isoformat(),))}
    except sqlite3.OperationalError:
        return {}


def load_jobs(path):
    from nullion.crons import CronJob,_CRON_COLLECTION,_payload_is_deleted
    try:
        with sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro",uri=True,timeout=10) as conn:
            rows=conn.execute('SELECT payload FROM reminders_crons WHERE collection=?',(_CRON_COLLECTION,)).fetchall()
        return [CronJob.from_dict(json.loads(row[0])) for row in rows if not _payload_is_deleted(row[0])]
    except sqlite3.OperationalError:
        return []


def update_schedule_fields(path,job,**fields):
    from nullion.crons import _CRON_COLLECTION,_payload_is_deleted
    with connection(path) as conn:
        row=conn.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',(_CRON_COLLECTION,job.id)).fetchone()
        if not row or _payload_is_deleted(row[0]):return
        data=json.loads(row[0])
        if data.get('next_run')!=job.next_run or not data.get('enabled'):return
        data.update(fields)
        conn.execute('UPDATE reminders_crons SET payload=?,updated_at=? WHERE collection=? AND item_key=?',(json.dumps(data),now().isoformat(),_CRON_COLLECTION,job.id))


def save_result(path,job,result):
    from nullion.crons import _CRON_COLLECTION,_payload_is_deleted
    with connection(path) as conn:
        row=conn.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',(_CRON_COLLECTION,job.id)).fetchone()
        if not row or _payload_is_deleted(row[0]):return
        data=json.loads(row[0])
        if data.get('last_run')!=job.last_run or any(data.get(k)!=getattr(job,k) for k in ('task','workspace_id','delivery_channel','delivery_target')):return
        data['last_result']=result
        conn.execute('UPDATE reminders_crons SET payload=?,updated_at=? WHERE collection=? AND item_key=?',(json.dumps(data),now().isoformat(),_CRON_COLLECTION,job.id))

@contextmanager
def scheduled_token(token):
    previous=getattr(_local,'token',None);_local.token=token
    try:yield
    finally:_local.token=previous

def current_execution_token():
    return getattr(_local, "token", None)


@contextmanager
def execution_lease(path,job_id):
    """Share the scheduled claim; gate manual runs at the same workflow boundary."""
    if path is None:
        yield True;return
    token=getattr(_local,'token',None)
    own=not token
    if own:
        token='manual:'+uuid.uuid4().hex
        with connection(path) as conn:
            conn.execute('DELETE FROM cron_execution_leases WHERE expires_at<=?',(now().isoformat(),))
            acquired=conn.execute('INSERT OR IGNORE INTO cron_execution_leases VALUES (?,?,?)',(job_id,token,expiry())).rowcount==1
        if not acquired:
            yield False;return
    elif not renew(path,job_id,token):
        yield False;return
    done=threading.Event()
    def heartbeat():
        while not done.wait(30):
            if not renew(path,job_id,token):return
    thread=threading.Thread(target=heartbeat,daemon=True,name='cron-lease-heartbeat');thread.start()
    previous = getattr(_local, "token", None)
    _local.token = token
    try:yield True
    finally:
        _local.token = previous
        done.set();thread.join(timeout=2)
        if own:release(path,job_id,token)
