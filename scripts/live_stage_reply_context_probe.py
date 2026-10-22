"""Opt-in stage API follow-up and plain-turn latency probe."""
import json
import os
from pathlib import Path
import sqlite3
import statistics
import time
import urllib.request
from uuid import uuid4

assert os.environ.get('NULLION_LIVE_STAGE_E2E')=='1'
base='http://127.0.0.1:8753';suffix=uuid4().hex[:10];cid='web:reply-context-'+suffix
report={'conversation_id':cid,'plain_seconds':[]}
deadline=time.monotonic()+60
while True:
    try:
        with urllib.request.urlopen(base+'/api/status',timeout=2) as response:
            assert response.status==200
        break
    except OSError:
        if time.monotonic()>=deadline:raise
        time.sleep(0.5)
def chat(prompt,conversation):
    started=time.monotonic()
    request=urllib.request.Request(base+'/api/chat',data=json.dumps({'text':prompt,'conversation_id':conversation}).encode(),headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=180) as response:value=json.load(response)
    assert value.get('type') not in {'error','approval_required'},value
    return value,time.monotonic()-started
for index,prompt in enumerate(['Hello','Hola','こんにちは']):
    value,elapsed=chat(prompt,cid+'-plain-'+str(index));report['plain_seconds'].append(elapsed)
    assert value.get('text'),value
first,_=chat('For this conversation, our two project labels are Copper Finch and Amber Pine. Reply with just those two labels.',cid)
second,elapsed=chat('Alphabetize those two labels and return them as a numbered list.',cid)
report.update(first=first,followup=second,followup_seconds=elapsed)
assert all(label in second.get('text','') for label in ['Copper Finch','Amber Pine']),second
with sqlite3.connect('file:'+str(Path.home()/'.nullion-stage/runtime.db')+'?mode=ro',uri=True) as conn:
    events=[json.loads(row[0]) for row in conn.execute("SELECT payload FROM conversation_events WHERE json_extract(payload,'$.conversation_id')=?",(cid,))]
accepted=[row for row in events if row.get('event_type')=='conversation.turn_recorded']
assert len(accepted)>=2,events
report['accepted_turn_ids']=[row['turn_id'] for row in accepted]
report['model_timings']=[row for row in events if row.get('event_type')=='conversation.model_timing']
with urllib.request.urlopen(base+'/api/chat/history/'+cid+'?sync_runtime=1',timeout=15) as response:history=json.load(response)
assert any('Copper Finch' in row.get('text','') and 'Amber Pine' in row.get('text','') for row in history['messages']),history
report['history_verified']=True
report['plain_average_seconds']=statistics.mean(report['plain_seconds'])
assert report['plain_average_seconds']<20 and max(report['plain_seconds'])<35,report
report['passed']=True
Path('/tmp/nullion-reply-context-stage.json').write_text(json.dumps(report,indent=2,ensure_ascii=False))
print(json.dumps({'passed':True,'conversation_id':cid,'plain_average_seconds':report['plain_average_seconds']}))
