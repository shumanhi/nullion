"""Opt-in stage scheduled HTML delivery probe; creates and removes one fixture job."""
import json
import os
import sqlite3
import time
import urllib.request
import uuid
from pathlib import Path

assert os.environ.get('NULLION_LIVE_STAGE_E2E') == '1'
base = 'http://127.0.0.1:8753'
marker = 'review-removal-' + uuid.uuid4().hex[:10]
conversation = 'web:' + marker

def api(path, method='GET', data=None):
    request = urllib.request.Request(base+path, method=method, data=None if data is None else json.dumps(data).encode(),
        headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request, timeout=15) as response:
        raw = response.read()
        return json.loads(raw)

job = api('/api/crons', 'POST', {'name':'[Stage verification] HTML delivery', 'schedule':'* * * * *',
    'task':f'Create a self-contained HTML report with a single heading {marker} and the sentence "Scheduled report delivery works." Use file_write and attach that HTML file. This is a synthetic fixture; no external sources or tools other than file writing are needed.',
    'delivery_channel':'web', 'delivery_target':conversation,
    'artifact_delivery_options':{'requires_attachment_delivery':True, 'required_artifact_extensions':['.html'], 'doctor_monitoring_enabled':False}})['job']
output = {'job_id':job['id'], 'conversation_id':conversation}
try:
    deadline = time.monotonic()+240
    while time.monotonic()<deadline:
        rows = api('/api/crons')
        stored = next(row for row in rows if row['id']==job['id'])
        if stored.get('last_result'):
            output['last_result'] = stored['last_result']
            break
        time.sleep(2)
    else:
        raise RuntimeError('No completed scheduled run within stage deadline')
    history = api('/api/chat/history/'+conversation+'?sync_runtime=1')
    output['history'] = history
    artifacts = [artifact for message in history['messages'] for artifact in message.get('artifacts', [])]
    assert artifacts, history
    with urllib.request.urlopen(base+artifacts[0]['url'], timeout=15) as response:
        delivered = response.read()
    assert marker.encode() in delivered
    assert b'Scheduled report delivery works.' in delivered
    assert delivered == Path(artifacts[0]['path']).read_bytes()
    output['download_bytes'] = len(delivered)
    db = Path.home()/'.nullion-stage/runtime.db'
    with sqlite3.connect('file:'+str(db)+'?mode=ro',uri=True) as conn:
        output['events'] = [json.loads(row[0]) for row in conn.execute(
            "SELECT payload FROM runtime_events WHERE json_extract(payload,'$.payload.cron_id')=?",(job['id'],))]
    serialized = json.dumps(output)
    assert 'artifact_file_accepted' in serialized, serialized[:1200]
    assert '"phase": "review"' not in serialized
    assert output['last_result'] == 'ok', output['last_result']
    assert marker in json.dumps(history), json.dumps(history)[:1200]
    assert any(event.get('event_type')=='cron.delivery.saved' for event in output['events'])
    output['passed'] = True
finally:
    api('/api/crons/'+job['id'], 'DELETE')
    Path('/tmp/nullion-no-review-stage.json').write_text(json.dumps(output,indent=2))
print(json.dumps({'passed':True,'conversation_id':conversation,'last_result':output['last_result']}))
