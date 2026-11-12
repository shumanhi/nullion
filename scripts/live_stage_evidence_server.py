#!/usr/bin/env python3
"""Read-only native SQLite evidence for Docker stage UI probes.

Docker Desktop shared-file SQLite reads can observe inconsistent pages while
native SQLite readers remain healthy. This test-only bridge queries native
SQLite; it never submits a chat, changes runtime data, or serves production.
"""
import argparse
from contextlib import closing
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import signal
import sqlite3
from datetime import datetime
from urllib.parse import parse_qs,urlsplit


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--connection-file',type=Path,required=True)
    parser.add_argument('--bind',default='127.0.0.1')
    args=parser.parse_args()
    database=Path.home()/'.nullion-stage/runtime.db'
    assert database.is_file()
    token=secrets.token_urlsafe(32)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            if not hmac.compare_digest(self.headers.get('Authorization',''),'Bearer '+token):
                self.send_error(403);return
            url=urlsplit(self.path);cid=parse_qs(url.query).get('conversation_id',[''])[0]
            if url.path == '/model-health':
                from live_stage_provider_guard import terminal_provider_event
                since=parse_qs(url.query).get('since',[''])[0]
                try:
                    assert len(since)<=40 and datetime.fromisoformat(since).tzinfo is not None
                except (ValueError, AssertionError):
                    self.send_error(400);return
                with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)) as db:
                    rows=db.execute("SELECT payload FROM runtime_events WHERE json_extract(payload,'$.event_type') IN ('doctor.signal_received','model.provider_health') AND json_extract(payload,'$.created_at')>=? ORDER BY updated_at DESC LIMIT 100",(since,)).fetchall()
                    rows+=db.execute("SELECT payload FROM conversation_events WHERE json_extract(payload,'$.event_type')='conversation.model_failure' AND json_extract(payload,'$.created_at')>=? ORDER BY updated_at DESC LIMIT 100",(since,)).fetchall()
                events=[]
                for row in rows:
                    record=json.loads(row[0]);details=terminal_provider_event(record)
                    if details:
                        events.append({'created_at':record.get('created_at'), 'details':details})
                body=json.dumps({'events':sorted(events,key=lambda e:e['created_at'])}).encode()
                self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
            prefixes=('web:',)
            if url.path == '/runtime-events':
                if not cid.startswith(prefixes) or len(cid)>160:
                    self.send_error(400);return
                try:
                    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)) as db:
                        rows=db.execute("SELECT payload FROM runtime_events WHERE "
                            "json_extract(payload,'$.actor')=? OR "
                            "json_extract(payload,'$.payload.principal_id')=? OR "
                            "json_extract(payload,'$.payload.conversation_id')=? "
                            "ORDER BY updated_at DESC LIMIT 1000",(cid,cid,cid)).fetchall()
                    body=json.dumps({'conversation_id':cid,'events':[json.loads(row[0]) for row in rows]}).encode()
                except sqlite3.DatabaseError:
                    self.send_error(503);return
                self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
            if url.path == '/conversation-events':
                if not cid.startswith(prefixes) or len(cid)>160:
                    self.send_error(400);return
                event_type=parse_qs(url.query).get('event_type',[''])[0]
                try:
                    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)) as db:
                        sql="SELECT payload FROM conversation_events WHERE collection='conversation_events' AND json_extract(payload,'$.conversation_id')=?"
                        params=[cid]
                        if event_type:
                            sql+=" AND json_extract(payload,'$.event_type')=?";params.append(event_type)
                        rows=db.execute(sql+" ORDER BY json_extract(payload,'$.created_at') DESC LIMIT 1000",params).fetchall()
                    body=json.dumps({'conversation_id':cid,'events':[json.loads(row[0]) for row in reversed(rows)]}).encode()
                except sqlite3.DatabaseError:
                    self.send_error(503);return
                self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
            if url.path == '/cron-runs':
                if not cid.startswith('web:live-ui-audit:') or len(cid)>160:
                    self.send_error(400);return
                try:
                    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)) as db:
                        rows=db.execute("SELECT payload FROM conversation_events WHERE "
                            "json_extract(payload,'$.conversation_id')=? AND "
                            "json_extract(payload,'$.cron_run_id') IS NOT NULL ORDER BY updated_at",(cid,)).fetchall()
                    body=json.dumps({'conversation_id':cid,'events':[json.loads(row[0]) for row in rows]}).encode()
                except sqlite3.DatabaseError:
                    self.send_error(503);return
                self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
            if url.path == '/browser-jobs':
                if not cid.startswith('web:live-ui-audit:') or len(cid)>160:
                    self.send_error(400);return
                jobs=[]
                for path in (database.parent/'browser-task-scratch').glob('run-*/job.json'):
                    try:
                        metadata=json.loads(path.read_text())
                        if (metadata.get('owner') or {}).get('conversation_id') != cid:
                            continue
                        log=path.parent/'worker.log'
                        jobs.append({'resource_id':path.parent.name,
                            'worker_log_bytes':log.stat().st_size if log.is_file() else 0})
                    except (OSError,ValueError):
                        continue
                body=json.dumps({'conversation_id':cid,'jobs':jobs}).encode()
                self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
            if url.path == '/tracking':
                name=parse_qs(url.query).get('name',[''])[0]
                if not name or len(name)>160:
                    self.send_error(400);return
                try:
                    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)) as db:
                        jobs=[json.loads(row[0]) for row in db.execute(
                            "SELECT payload FROM live_trackers WHERE json_extract(payload,'$.name')=?",(name,))]
                        notices=[];events=[]
                        for job in jobs:
                            notices.extend(db.execute('SELECT kind,state,text FROM live_tracker_notifications WHERE tracker_id=?',(job['id'],)).fetchall())
                            events.extend(json.loads(row[0]) for row in db.execute(
                                "SELECT payload FROM runtime_events WHERE collection='events' AND updated_at>=? AND json_extract(payload,'$.payload.capsule_id')=?",(job['created_at'],'live-tracker:'+job['id'])))
                    body=json.dumps({'jobs':jobs,'notices':notices,'events':events}).encode()
                except sqlite3.DatabaseError:
                    self.send_error(503);return
                self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
            if url.path!='/turns' or not any(cid.startswith(prefix) for prefix in prefixes) or len(cid)>160:
                self.send_error(400);return
            try:
                with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)) as db:
                    rows=db.execute("SELECT payload FROM conversation_events WHERE collection='conversation_events' "
                        "AND json_extract(payload,'$.conversation_id')=? "
                        "AND json_extract(payload,'$.event_type')='conversation.chat_turn' ORDER BY updated_at LIMIT 100",(cid,)).fetchall()
                body=json.dumps({'conversation_id':cid,'turns':[json.loads(row[0]) for row in rows]}).encode()
            except sqlite3.DatabaseError:
                self.send_error(503,'Native stage evidence read failed');return
            self.send_response(200);self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    server=ThreadingHTTPServer((args.bind,0),Handler)
    args.connection_file.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(args.connection_file,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'w') as stream:
        json.dump({'port':server.server_port,'token':token},stream)
    def stop(_signum, _frame):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, stop)
    try:server.serve_forever()
    finally:
        server.server_close();args.connection_file.unlink(missing_ok=True)

if __name__=='__main__':main()
