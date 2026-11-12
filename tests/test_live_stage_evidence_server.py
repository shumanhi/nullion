"""The native reader is authenticated, fixture-scoped and read-only."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request,urlopen
import pytest


def test_native_evidence_bridge_reads_only_owned_fixture_conversations(tmp_path):
    home=tmp_path/'home';stage=home/'.nullion-stage';stage.mkdir(parents=True)
    database=stage/'runtime.db';cid='web:live-ui-audit:fixture'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE conversation_events(collection TEXT,item_key TEXT,payload TEXT,updated_at TEXT)')
        db.execute('CREATE TABLE runtime_events(payload TEXT,updated_at TEXT)')
        for who in (cid, 'telegram:private'):
            db.execute('INSERT INTO runtime_events VALUES(?,?)',(json.dumps({'actor':who,'event_type':'tool.completed'}),'now'))
        for key,conversation in [('owned',cid),('other','telegram:private')]:
            db.execute('INSERT INTO conversation_events VALUES(?,?,?,?)',('conversation_events',key,
                json.dumps({'conversation_id':conversation,'event_type':'conversation.chat_turn','assistant_reply':key}),'now'))
        for stamp in ('2026-10-01T00:00:00+00:00', '2026-11-03T12:00:00+00:00'):
            event={'created_at':stamp,'event_type':'doctor.signal_received','payload':{'reason':
                'detail=HTTP 429: {"error":{"type":"usage_limit_reached","message":"SECRET"}};message_text=PRIVATE'}}
            db.execute('INSERT INTO runtime_events VALUES(?,?)',(json.dumps(event),stamp))
    before=database.read_bytes();connection=tmp_path/'connection.json'
    script=Path(__file__).parents[1]/'scripts/live_stage_evidence_server.py'
    process=subprocess.Popen([sys.executable,str(script),'--connection-file',str(connection)],env={**os.environ,'HOME':str(home)})
    try:
        deadline=time.monotonic()+5
        while not connection.exists() and time.monotonic()<deadline:time.sleep(.02)
        settings=json.loads(connection.read_text());assert connection.stat().st_mode&0o077==0
        base='http://127.0.0.1:'+str(settings['port'])+'/turns?conversation_id='
        with pytest.raises(HTTPError) as error:urlopen(base+cid,timeout=3)
        assert error.value.code==403
        def read(conversation):
            request=Request(base+conversation,headers={'Authorization':'Bearer '+settings['token']})
            return json.load(urlopen(request,timeout=3))
        with pytest.raises(HTTPError) as error:read('telegram:private')
        assert error.value.code==400
        result=read(cid);assert result['conversation_id']==cid and len(result['turns'])==1
        assert result['turns'][0]['assistant_reply']=='owned' and database.read_bytes()==before
        events=base.replace('/turns?', '/conversation-events?')+cid+'&event_type=conversation.chat_turn'
        for _ in range(30):
            with urlopen(Request(events,headers={'Authorization':'Bearer '+settings['token']}),timeout=3) as response:
                payload=json.load(response)
            assert payload['conversation_id']==cid and len(payload['events'])==1
        runtime_events=events.replace('/conversation-events?', '/runtime-events?')
        with urlopen(Request(runtime_events,headers={'Authorization':'Bearer '+settings['token']}),timeout=3) as response:
            payload=json.load(response)
        assert len(payload['events'])==1 and payload['events'][0]['actor']==cid
        health_url='http://127.0.0.1:'+str(settings['port'])+'/model-health?since=2026-11-03T00:00:00%2B00:00'
        with urlopen(Request(health_url,headers={'Authorization':'Bearer '+settings['token']}),timeout=3) as response:
            health=json.load(response)
        assert len(health['events'])==1 and health['events'][0]['details']['category']=='quota'
        assert 'SECRET' not in json.dumps(health) and 'PRIVATE' not in json.dumps(health)
        with pytest.raises(HTTPError) as error:
            urlopen(Request(health_url.replace('2026-11-03T00:00:00%2B00:00','bad'),headers={'Authorization':'Bearer '+settings['token']}),timeout=3)
        assert error.value.code==400
        denied=events.replace(cid,'telegram:private')
        with pytest.raises(HTTPError) as error:
            urlopen(Request(denied,headers={'Authorization':'Bearer '+settings['token']}),timeout=3)
        assert error.value.code==400 and database.read_bytes()==before
    finally:
        process.terminate();process.wait(timeout=5)
        assert not connection.exists()
