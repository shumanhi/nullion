"""Transactional scheduled-run claims, leases and runtime capacity settings."""
from __future__ import annotations

from nullion.sqlite_connections import sqlite_connection
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


def execution_result_label(result, *, failed_to_run=False):
    """One stored outcome policy for scheduled, manual and resumed executions."""
    if failed_to_run:
        return 'error: manual run failed'
    if not isinstance(result, dict):
        return 'ok'
    if result.get('suspended_for_approval'):
        return 'waiting_for_approval'
    if result.get('cron_run_cancelled'):
        return 'cancelled'
    outcome = result.get('execution_outcome') or {}
    if not isinstance(outcome, dict):
        return 'error: invalid execution outcome'
    failures = any(result.get(key) for key in ('cron_run_failed', 'cron_report_failed', 'cron_delivery_failed'))
    if outcome.get('status') == 'partially_succeeded' and not failures:
        return 'partial_success'
    if failures or outcome.get('execution_succeeded') is False:
        return 'error: ' + str(result.get('reason') or 'scheduled task incomplete')
    if result.get('reached_iteration_limit') and outcome.get('execution_succeeded') is not True:
        return 'error: cron_run_reached_iteration_limit'
    if result.get('cron_delivery_partial_success'):
        return 'partial_success'
    return 'ok'

def validate_limit(value):
    if type(value) is not int or not 1 <= value <= 32:
        raise ValueError('max_parallel_jobs must be an integer from 1 to 32')
    return value

@contextmanager
def connection(path):
    from nullion.runtime_persistence import runtime_store_file_lock
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    with runtime_store_file_lock(path):
        with sqlite_connection(str(path),timeout=10) as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS cron_execution_leases (cron_id TEXT PRIMARY KEY, token TEXT NOT NULL, expires_at TEXT NOT NULL)')
            conn.execute('CREATE TABLE IF NOT EXISTS cron_scheduler_settings (key TEXT PRIMARY KEY, payload TEXT NOT NULL)')
            conn.commit();conn.execute('BEGIN IMMEDIATE')
            yield conn

def now():return datetime.now(timezone.utc)
def expiry():return (now()+timedelta(seconds=LEASE_SECONDS)).isoformat()

def lease_owner_alive(token):
    """Legacy tokens have unknown ownership and keep their expiry grace."""
    parts = str(token).split(':')
    if len(parts) != 3 or parts[0] not in {'scheduler', 'manual'} or not parts[1].isdecimal():
        return None
    pid = int(parts[1])
    if pid <= 0:
        return None
    if os.name != 'posix':
        # signal 0 is not a portable liveness probe (notably on Windows).
        # Unknown ownership keeps the existing bounded lease-expiry grace.
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True

def _prune_execution_leases(conn, stamp):
    from nullion.crons import _CRON_COLLECTION, _payload_is_deleted
    reclaimed = set()
    for job_id, token, expires_at in conn.execute('SELECT cron_id,token,expires_at FROM cron_execution_leases').fetchall():
        if expires_at <= stamp or lease_owner_alive(token) is False:
            conn.execute('DELETE FROM cron_execution_leases WHERE cron_id=? AND token=?', (job_id, token))
            reclaimed.add(job_id)
    if reclaimed and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reminders_crons'").fetchone():
        for job_id in reclaimed:
            row = conn.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',
                               (_CRON_COLLECTION, job_id)).fetchone()
            if row and not _payload_is_deleted(row[0]):
                data = json.loads(row[0])
                if data.get('last_result') == 'running':
                    data['last_result'] = 'error: execution_interrupted'
                    conn.execute('UPDATE reminders_crons SET payload=?,updated_at=? WHERE collection=? AND item_key=?',
                                 (json.dumps(data), stamp, _CRON_COLLECTION, job_id))
    return reclaimed

def reclaim_stale_execution_leases(path):
    with connection(path) as conn:
        return _prune_execution_leases(conn, now().isoformat())

def read_limit(path,default=None):
    if default is None:
        try:default=validate_limit(int(os.environ.get('NULLION_CRON_MAX_PARALLEL_JOBS','4')))
        except ValueError:default=4
    try:
        with sqlite_connection(Path(path).resolve().as_uri()+"?mode=ro",uri=True,timeout=10) as conn:
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
        _prune_execution_leases(conn, stamp)
        if conn.execute('SELECT 1 FROM cron_execution_leases WHERE cron_id=?',(job.id,)).fetchone():return None
        setting=conn.execute("SELECT payload FROM cron_scheduler_settings WHERE key='max_parallel_jobs'").fetchone()
        if setting:limit=validate_limit(json.loads(setting[0]))
        running=conn.execute("SELECT COUNT(*) FROM cron_execution_leases WHERE token LIKE 'scheduler:%'").fetchone()[0]
        if running>=limit:return None
        row=conn.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',(_CRON_COLLECTION,job.id)).fetchone()
        if not row or _payload_is_deleted(row[0]):return None
        data=json.loads(row[0]);current=CronJob.from_dict(data)
        if not current.enabled or current.next_run!=job.next_run or not current.next_run_dt() or current.next_run_dt()>now():return None
        token='scheduler:'+str(os.getpid())+':'+uuid.uuid4().hex
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
        with sqlite_connection(Path(path).resolve().as_uri()+"?mode=ro",uri=True,timeout=10) as conn:
            return {row[0]:row[1] for row in conn.execute('SELECT cron_id,token FROM cron_execution_leases WHERE expires_at>?',(now().isoformat(),)) if lease_owner_alive(row[1]) is not False}
    except sqlite3.OperationalError:
        return {}


def load_jobs(path):
    from nullion.crons import CronJob,_CRON_COLLECTION,_payload_is_deleted
    try:
        with sqlite_connection(Path(path).resolve().as_uri()+"?mode=ro",uri=True,timeout=10) as conn:
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


def begin_manual_run(path, job):
    """Bind a manual run to one current job revision without replacing its settings."""
    from nullion.crons import _CRON_COLLECTION, _payload_is_deleted
    with connection(path) as conn:
        row = conn.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',
                           (_CRON_COLLECTION, job.id)).fetchone()
        if not row or _payload_is_deleted(row[0]):
            return None
        data = json.loads(row[0])
        reclaimed = _prune_execution_leases(conn, now().isoformat())
        if conn.execute('SELECT 1 FROM cron_execution_leases WHERE cron_id=? AND expires_at>?',
                        (job.id, now().isoformat())).fetchone():
            return None
        if job.id not in reclaimed and data.get('last_result') == 'running' and data.get('last_run'):
            try:
                if (now() - datetime.fromisoformat(data['last_run'])).total_seconds() < LEASE_SECONDS:
                    return None
            except (ValueError, TypeError):
                return None
        if any(data.get(key) != getattr(job, key) for key in
               ('last_run', 'task', 'workspace_id', 'delivery_channel', 'delivery_target')):
            return None
        stamp = now().isoformat()
        data.update(last_run=stamp, last_result='running')
        conn.execute('UPDATE reminders_crons SET payload=?,updated_at=? WHERE collection=? AND item_key=?',
                     (json.dumps(data), stamp, _CRON_COLLECTION, job.id))
    return stamp


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
        token='manual:'+str(os.getpid())+':'+uuid.uuid4().hex
        with connection(path) as conn:
            _prune_execution_leases(conn, now().isoformat())
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
