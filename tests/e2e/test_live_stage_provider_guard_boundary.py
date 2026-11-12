"""A real pytest session stops after a native SQLite/HTTP quota receipt."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('transient_first_read',[False,True,'after_first_outage'])
@pytest.mark.parametrize('event_type',['doctor.signal_received','model.provider_health'])
def test_paid_live_session_stops_after_first_terminal_provider_failure(tmp_path,event_type,transient_first_read):
    root=Path(__file__).parents[2]
    home=tmp_path/'home';stage=home/'.nullion-stage';stage.mkdir(parents=True)
    database=stage/'runtime.db'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE runtime_events(payload TEXT,updated_at TEXT)')
        db.execute('CREATE TABLE conversation_events(payload TEXT,updated_at TEXT)')
    connection=tmp_path/'connection.json'
    evidence=tmp_path/'evidence'
    server=subprocess.Popen([sys.executable,str(root/'scripts/live_stage_evidence_server.py'),
                             '--connection-file',str(connection)],env={**os.environ,'HOME':str(home)})
    try:
        deadline=time.monotonic()+5
        while not connection.exists() and time.monotonic()<deadline:time.sleep(.02)
        assert connection.exists()
        suite=tmp_path/'suite';suite.mkdir()
        (suite/'conftest.py').write_text('''
import os
from tests.e2e.conftest import live_stage_provider_guard, stop_paid_live_tests_after_terminal_provider_failure
from scripts import live_stage_provider_guard as guard
from urllib.request import Request, urlopen
_reads=0
_outage_started=None
def local_transport(request, **kwargs):
    global _reads, _outage_started
    import time
    _reads += 1
    if os.environ.get("LONG_OUTAGE") == "1" and os.path.exists(os.environ["PAID_MARKER"]):
        if _outage_started is None: _outage_started = time.monotonic()
        if time.monotonic() - _outage_started < 9:
            from urllib.error import URLError
            raise URLError(OSError(101, "Network is unreachable"))
    if _reads == 1 and os.environ.get('TRANSIENT_FIRST_READ') == '1':
        from urllib.error import URLError
        raise URLError(OSError(101, 'Network is unreachable'))
    return urlopen(Request(request.full_url.replace('host.docker.internal', '127.0.0.1'), headers=dict(request.header_items())), **kwargs)
guard.urlopen=local_transport
''')
        (suite/'test_live_stage_owned.py').write_text('''
import json,os,sqlite3
from pathlib import Path
from datetime import datetime,timezone
def test_first_paid_request():
    Path(os.environ['PAID_MARKER']).write_text('1')
    from scripts.live_stage_provider_guard import process_provider_guard
    process_provider_guard().poll()
    now=datetime.now(timezone.utc).isoformat()
    event={'created_at':now,'event_type':'doctor.signal_received','payload':{'reason':'detail=HTTP 429: {"error":{"type":"usage_limit_reached","message":"SECRET"}}'}}
    if os.environ['FIXTURE_EVENT_TYPE']=='model.provider_health':
        event={'created_at':now,'event_type':'model.provider_health','payload':{'provider_error':{'category':'quota','terminal':True,'http_status':429,'error_type':'usage_limit_reached'}}}
    with sqlite3.connect(os.environ['FIXTURE_DATABASE']) as db:
        db.execute('INSERT INTO runtime_events VALUES(?,?)',(json.dumps(event),now))
def test_second_must_not_run():
    Path(os.environ['PAID_MARKER']).write_text('2')
''')
        marker=tmp_path/'paid.txt'
        env={**os.environ,'NULLION_LIVE_STAGE_E2E':'1','NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION':str(connection),
             'NULLION_LIVE_EVIDENCE_DIR':str(evidence),'PAID_MARKER':str(marker),'FIXTURE_DATABASE':str(database),
             'FIXTURE_EVENT_TYPE':event_type,'TRANSIENT_FIRST_READ':'1' if transient_first_read is True else '0',
             'LONG_OUTAGE':'1' if transient_first_read == 'after_first_outage' else '0',
             'PYTHONPATH':str(root)+os.pathsep+os.environ.get('PYTHONPATH','')}
        result=subprocess.run([sys.executable,'-m','pytest','-q','-c','/dev/null',str(suite)],
                              env=env,capture_output=True,text=True,timeout=30)
        assert result.returncode==1,result.stdout+result.stderr
        assert marker.read_text()=='1',result.stdout+result.stderr
        receipt=json.loads((evidence/'provider-blocked.json').read_text())
        assert receipt['details']['category']=='quota' and receipt['details']['http_status']==429
        assert 'SECRET' not in json.dumps(receipt)
    finally:
        server.terminate();server.wait(timeout=5)
