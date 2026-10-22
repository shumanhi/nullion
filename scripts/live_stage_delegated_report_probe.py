#!/usr/bin/env python3
"""Opt-in real stage planner run; no account mutations or cron fixtures."""
import json,os,re,sqlite3,time,urllib.request,urllib.parse
from pathlib import Path
from uuid import uuid4
from html.parser import HTMLParser

assert os.environ.get('NULLION_LIVE_STAGE_E2E')=='1'
base=os.environ['NULLION_LIVE_BASE_URL'];db=Path(os.environ['NULLION_LIVE_RUNTIME_DB'])
headers={'Content-Type':'application/json','Host':os.environ.get('NULLION_LIVE_HOST_HEADER','127.0.0.1:8753')}
out=Path('/evidence/live-stage-planner-current.json');cid='web:complete-planner-stage-'+uuid4().hex[:10]
report={'lane':'stage','conversation_id':cid}
def api(path,body=None):
 req=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
 with urllib.request.urlopen(req,timeout=180) as r:return json.load(r)
def save():out.write_text(json.dumps(report,indent=2,ensure_ascii=False));out.chmod(0o600)
def descriptors(value):
 if isinstance(value,dict):
  if str(value.get('url','')).startswith('/api/artifacts/'):yield value
  for child in value.values():yield from descriptors(child)
 elif isinstance(value,list):
  for child in value:yield from descriptors(child)
start=time.monotonic()
save()
try:
 report['initial']=api('/api/chat',{'conversation_id':cid,'text':
  '/planner Check current weather for New York and San Francisco, then create one complete HTML report named stage_complete_planner_weather.html. '
  'Include both cities, current temperature with units, conditions and observation times. Use compact readable cards. '
  'Finish both source checks and the file before replying. Attach only the final HTML report; do not attach source or state files.'})
 save();deadline=time.monotonic()+540
 while time.monotonic()<deadline:
  history=api('/api/chat/history/'+cid+'?sync_runtime=1');links=list({d['url']:d for d in descriptors(history)}.values())
  with sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True) as conn:
   runs=[json.loads(raw) for raw, in conn.execute("SELECT payload FROM mini_agent_runs WHERE json_extract(payload,'$.metadata.conversation_id')=? ORDER BY updated_at",(cid,))]
  report['runs']=[{k:r.get(k) for k in ('run_id','status','task_id','capsule_id','result_summary')} for r in runs]
  failed=[r for r in runs if str(r.get('status','')).lower() in {'failed','cancelled'}]
  assert not failed,report['runs']
  active=[r for r in runs if str(r.get('status','')).lower() not in {'completed','failed','cancelled'}]
  if links and not active:break
  save();time.sleep(2)
 assert links and not active,report
 report['execution_mode']='delegated' if runs else 'single_agent_fallback'
 assert len(links)==1,links
 with urllib.request.urlopen(urllib.request.Request(base+links[0]['url'],headers=headers),timeout=30) as response:document=response.read()
 artifact=out.with_suffix('.html');artifact.write_bytes(document);artifact.chmod(0o600)
 class Text(HTMLParser):
  values=[]
  def handle_data(self,value):self.values.append(value)
 parser=Text();parser.feed(document.decode());text=' '.join(parser.values)
 assert all(city in text for city in ('New York','San Francisco'))
 assert len(re.findall(r'[-+]?\d+(?:\.\d+)?\s*°\s*[FC]',text))>=2,text
 report.update(status='passed',seconds=round(time.monotonic()-start,3),artifact={'bytes':len(document),'url':links[0]['url']})
 save();print(json.dumps({'status':'passed','seconds':report['seconds'],'tasks':len(runs)}))
finally:save()
