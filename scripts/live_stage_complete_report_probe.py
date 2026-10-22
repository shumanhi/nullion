#!/usr/bin/env python3
"""Opt-in live stage reports through scheduler/API and Telegram delivery."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import statistics
import time
import urllib.request
import urllib.parse
import re
from html.parser import HTMLParser
from uuid import uuid4


def run(args):
    assert os.environ.get('NULLION_LIVE_STAGE_E2E') == '1', 'Stage probe is opt-in'
    base=os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753')
    db=Path(os.environ.get('NULLION_LIVE_RUNTIME_DB',str(Path.home()/'.nullion-stage/runtime.db')))
    headers={'Content-Type':'application/json'}
    if os.environ.get('NULLION_LIVE_HOST_HEADER'): headers['Host']=os.environ['NULLION_LIVE_HOST_HEADER']
    def api(path,method='GET',body=None):
        request=urllib.request.Request(base+path,data=json.dumps(body).encode() if body is not None else None,
            headers=headers,method=method)
        with urllib.request.urlopen(request,timeout=180) as response: return json.load(response)
    def records(table,field,value):
        with sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True) as conn:
            return [json.loads(row[0]) for row in conn.execute(
                f'SELECT payload FROM {table} WHERE json_extract(payload,?)=? ORDER BY updated_at',(field,value))]
    identifier=uuid4().hex[:10]; conversation='web:complete-report-stage-'+identifier
    report={'lane':'stage','identifier':identifier,'plain_turns':[],'runs':[]}; owned=[]
    def save():
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(report,indent=2,ensure_ascii=False));args.output.chmod(0o600)
    def chat(prompt,suffix):
        cid=conversation+':'+suffix;start=time.monotonic()
        payload=api('/api/chat','POST',{'text':prompt,'conversation_id':cid})
        assert payload.get('type') not in {'error','approval_required'},payload
        return {'conversation_id':cid,'seconds':round(time.monotonic()-start,3),'reply':payload.get('text','')}
    def wait_terminal(job_id):
        terminal_seen=None
        deadline=time.monotonic()+900
        while time.monotonic()<deadline:
            events=records('runtime_events','$.payload.cron_id',job_id)
            terminal=[e for e in events if e.get('event_type') in {'cron.delivery.sent','cron.delivery.saved',
                'cron.delivery.silent','cron.delivery.failed','cron.delivery.paused'}]
            if terminal:
                turns=records('conversation_events','$.cron_id',job_id)
                if turns or terminal[-1]['event_type'] not in {'cron.delivery.sent','cron.delivery.saved'}:
                    return events,terminal[-1],turns
                terminal_seen=terminal_seen or time.monotonic()
                assert time.monotonic()-terminal_seen<30, 'Successful delivery has no durable receipt after 30 seconds'
            time.sleep(2)
        raise AssertionError('Stage report did not reach terminal delivery within 900 seconds')
    def validate_delivery(entry,events,terminal,turns):
        name=entry['name']
        assert terminal['event_type'] in {'cron.delivery.sent','cron.delivery.saved'},terminal
        delivered=[t for t in turns if t.get('event_type')=='conversation.chat_turn']
        assert delivered,'Missing durable delivery receipt'
        assert all('Partial report' not in t.get('assistant_reply','') for t in delivered)
        failures=[e for e in events if e.get('payload',{}).get('stage') in {'completion_required','final_repair_failed'}]
        entry['recovery_events']=failures
        final_text=delivered[-1].get('assistant_reply','')
        assert 'could not publish' not in final_text and 'could not be completed' not in final_text,final_text
        completed=[e for e in events if e.get('payload',{}).get('stage')=='single_agent_done']
        assert completed and completed[-1]['payload'].get('tool_results',0)>0
        if name=='email-calendar':
            assert 'calendar' in final_text.lower(),final_text
        else:
            assert 'MEDIA:' in final_text,final_text
            turn=delivered[-1]
            query=urllib.parse.urlencode({'turn_ids':json.dumps([turn['turn_id']])})
            history=api('/api/chat/history/'+urllib.parse.quote(turn['conversation_id'],safe=':')+'?'+query)
            def descriptors(value):
                if isinstance(value,dict):
                    if str(value.get('url','')).startswith('/api/artifacts/'): yield value
                    for child in value.values(): yield from descriptors(child)
                elif isinstance(value,list):
                    for child in value:yield from descriptors(child)
            links=list({d['url']:d for d in descriptors(history)}.values())
            assert len(links)==1,history
            request=urllib.request.Request(base+links[0]['url'],headers=headers)
            with urllib.request.urlopen(request,timeout=30) as response:document=response.read()
            artifact=args.output.with_suffix('.html');artifact.write_bytes(document);artifact.chmod(0o600)
            class Text(HTMLParser):
                text=[]
                def handle_data(self,value):self.text.append(value)
            parser=Text();parser.feed(document.decode())
            body=' '.join(parser.text)
            assert 'New York' in body and 'San Francisco' in body
            assert len(re.findall(r'[-+]?\d+(?:\.\d+)?\s*°\s*[FC]',body))>=2,body
            assert 'Current temperature and apparent temperature were unavailable' not in body,body
            entry['artifact']={'bytes':len(document),'download_url':links[0]['url'],'path':str(artifact)}
        entry['status']='passed';save()
    if args.validate_recorded:
        report.update(json.loads(args.output.read_text()))
        for entry in report['runs']:
            events,terminal,turns=wait_terminal(entry['cron_id'])
            entry.update(events=events,terminal_event=terminal,turns=turns)
            validate_delivery(entry,events,terminal,turns)
        report['status']='passed';save();return report
    try:
        for index,prompt in enumerate(('Say hello in one short sentence.','こんにちは。一文で挨拶してください。')):
            report['plain_turns'].append(chat(prompt,'plain-'+str(index)));save()
        report['simple_average_seconds']=statistics.mean(t['seconds'] for t in report['plain_turns'])
        assert report['simple_average_seconds']<20 and max(t['seconds'] for t in report['plain_turns'])<30
        tasks=[('email-calendar','telegram',args.telegram_target,True,
            'Read-only email deadline triage: search for the two newest available emails received in the past 3 days and read each full message. '
            'Also list calendar events over the next 7 days. Complete both source checks before reporting. '
            'Give a concise report of any dated obligations, the source message dates, and existing calendar events. '
            'If nothing needs action, say so after checking both sources. Do not send emails or create/change calendar events. '
            'Do not include full message bodies, raw tool output, or internal paths.',{}),
            ('weather-file','web',conversation+':file',False,
             'Check current weather for New York and San Francisco, then create one readable HTML report with both cities, '
             'temperatures, conditions and check date. Finish both weather checks and write the complete report file before delivery. '
             'Use concise sections and disclose genuinely unavailable optional details. Do not deliver source files or sidecar state.',
             {'required_artifact_extensions':['.html'],'requires_attachment_delivery':True,'html_image_delivery_mode':'file'})]
        for name,channel,target,scheduled,task,options in tasks:
            if args.case and name != args.case: continue
            created=api('/api/crons','POST',{'name':'[Stage report verification] '+name+' '+identifier,
                'task':task,'schedule':'* * * * *' if scheduled else '0 0 1 1 *','timezone':'UTC',
                'enabled':scheduled,'workspace_id':'workspace_admin','delivery_channel':channel,
                'delivery_target':target,'artifact_delivery_options':options})
            job_id=created['job']['id'];owned.append(job_id)
            entry={'name':name,'cron_id':job_id,'surface':channel};report['runs'].append(entry);save()
            print(json.dumps({'started':name,'cron_id':job_id}),flush=True)
            if not scheduled:entry['manual_request']=chat('Run scheduled task '+job_id+' once now.','manual-file')
            events,terminal,turns=wait_terminal(job_id)
            entry.update(terminal_event=terminal,events=events,turns=turns);save()
            # This probe owns only its temporary job. Stop it immediately so
            # a second periodic check cannot start after its verified delivery.
            api('/api/crons/'+job_id,'DELETE');owned.remove(job_id);entry['removed']=True;save()
            validate_delivery(entry,events,terminal,turns)
            print(json.dumps({'completed':name,'status':'passed'}),flush=True)
        report['status']='passed';save()
        return report
    finally:
        for job_id in owned:
            try:api('/api/crons/'+job_id,'DELETE')
            except Exception:report.setdefault('cleanup_failed',[]).append(job_id)
        save()


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--telegram-target',required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--case',choices=('email-calendar','weather-file'))
    parser.add_argument('--validate-recorded',action='store_true')
    run(parser.parse_args())
