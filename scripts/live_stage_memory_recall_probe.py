#!/usr/bin/env python3
"""Opt-in stage contract: real chat replies, artifact bytes, and persisted tool evidence."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sqlite3
import statistics
import time
from uuid import uuid4

import httpx


def run(base_url: str, db_path: Path, document_prompt: str = 'For sf trip, do u have the pdf?') -> dict:
    if os.environ.get('NULLION_LIVE_STAGE_E2E') != '1':
        raise SystemExit('Set NULLION_LIVE_STAGE_E2E=1 for the live stage probe.')
    if base_url.rstrip('/') != 'http://127.0.0.1:8753':
        raise SystemExit('This probe is restricted to the existing local stage lane.')
    run_id=uuid4().hex[:10]
    report={'run_id':run_id,'lane':'stage','document_prompt':document_prompt,'simple_turns':[],'checks':[]}
    with httpx.Client(base_url=base_url,timeout=150) as client:
        def send(text, name):
            turn_id=f'memory-probe:{run_id}:{name}'
            start=time.perf_counter()
            result=client.post('/api/chat',json={'text':text,'conversation_id':f'web:memory-probe-{run_id}-{name}','turn_id':turn_id})
            result.raise_for_status()
            payload=result.json()
            assert payload.get('type') not in {'error','approval_required'},payload
            assert payload.get('text'),payload
            return turn_id,payload,time.perf_counter()-start
        for index,prompt in enumerate(['Return only the number: 2+2','Devuelve solo el número: 3+2','数字だけで答えてください：4+2']):
            turn_id,payload,elapsed=send(prompt,f'plain-{index}')
            report['simple_turns'].append({'turn_id':turn_id,'elapsed_seconds':elapsed,'text':payload['text']})
        turn_id,payload,elapsed=send(document_prompt,'document')
        if not payload.get('artifacts'):
            deadline=time.monotonic()+30
            candidates=[]
            while not candidates and time.monotonic()<deadline:
                with sqlite3.connect(db_path.resolve().as_uri()+'?mode=ro',uri=True) as conn:
                    saved=conn.execute("SELECT payload FROM conversation_events WHERE json_extract(payload,'$.event_type')='conversation.chat_turn' AND json_extract(payload,'$.turn_id')=?",(turn_id,)).fetchone()
                if saved:
                    event=json.loads(saved[0])
                    discovered=[path for result in event.get('tool_results',[])
                        if result.get('tool_name')=='file_search' and result.get('status')=='completed'
                        for path in result.get('output',{}).get('matches',[]) if isinstance(path,str) and Path(path).suffix.lower()=='.pdf']
                    discovered.extend(ref['path'] for result in event.get('tool_results',[])
                        if result.get('tool_name')=='chat_history_search' and result.get('status')=='completed'
                        for match in result.get('output',{}).get('matches',[]) if isinstance(match,dict)
                        for ref in match.get('artifact_refs',[]) if isinstance(ref,dict) and ref.get('availability')=='available'
                        and ref.get('extension')=='.pdf' and isinstance(ref.get('path'),str))
                    candidates=list(dict.fromkeys(path for path in discovered if Path(path).name in payload['text']))
                if not candidates:time.sleep(.2)
            assert len(candidates)==1 and Path(candidates[0]).name in payload['text'],payload
            report['checks'].append({'kind':'document_lookup','turn_id':turn_id,'elapsed_seconds':elapsed,'text':payload['text']})
            turn_id,payload,elapsed=send('Send the existing file '+Path(candidates[0]).name,'attachment')
        assert payload.get('artifacts'),payload
        assert 'MEDIA:' not in payload['text'] and len(payload['text'].splitlines())<12,payload
        files=[]
        for item in payload['artifacts']:
            url=item.get('url') or item.get('download_url')
            assert item.get('name','').lower().endswith('.pdf') and url,item
            fetched=client.get(url);fetched.raise_for_status()
            assert fetched.content.startswith(b'%PDF'),item
            files.append({'name':item['name'],'bytes':len(fetched.content)})
        report['checks'].append({'kind':'document_recall','turn_id':turn_id,'elapsed_seconds':elapsed,'text':payload['text'],'files':files})
        turn_id,payload,elapsed=send('Search saved workspace memory for my preference about brief replies and attaching requested files.','memory')
        assert any(value in payload['text'].casefold() for value in ['brief','concise']),payload
        assert any(value in payload['text'].casefold() for value in ['attach','file']),payload
        report['checks'].append({'kind':'memory_recall','turn_id':turn_id,'elapsed_seconds':elapsed,'text':payload['text']})
    expected={row['turn_id']:row for row in report['checks']+report['simple_turns']}
    deadline=time.monotonic()+30
    while expected and time.monotonic()<deadline:
        with sqlite3.connect(db_path.resolve().as_uri()+'?mode=ro',uri=True) as conn:
            placeholders=','.join('?' for _ in expected)
            rows=conn.execute(f"SELECT payload FROM conversation_events WHERE json_extract(payload,'$.event_type')='conversation.chat_turn' AND json_extract(payload,'$.turn_id') IN ({placeholders})",list(expected))
            for raw, in rows:
                event=json.loads(raw);row=expected.pop(event['turn_id'])
                row['tools']=[{'name':r['tool_name'],'status':r['status']} for r in event.get('tool_results',[])]
                if row.get('kind')=='document_recall':
                    assert row.get('files'),row
                    assert not {tool['name'] for tool in row['tools']}.intersection({'pdf_create','pdf_edit','file_write','file_patch','document_create'}),row
                elif row.get('kind')=='document_lookup':
                    assert any(r['tool_name'] in {'file_search','chat_history_search'} and r['status']=='completed' for r in event.get('tool_results',[])),row
                elif row.get('kind')=='memory_recall':
                    assert any(r['tool_name']=='memory_search' and r['status']=='completed' and r.get('output',{}).get('matches') for r in event.get('tool_results',[])),row
                else:
                    assert not row['tools'],row
        if expected:time.sleep(.2)
    assert not expected,f'Backing chat records were not checkpointed: {list(expected)}'
    elapsed=[row['elapsed_seconds'] for row in report['simple_turns']]
    report['simple_latency']={'average_seconds':statistics.mean(elapsed),'max_seconds':max(elapsed)}
    assert statistics.mean(elapsed)<12 and max(elapsed)<25,report['simple_latency']
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--base-url',default='http://127.0.0.1:8753')
    parser.add_argument('--db',type=Path,default=Path.home()/'.nullion-stage/runtime.db')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--document-prompt',default='For sf trip, do u have the pdf?')
    args=parser.parse_args()
    report=run(args.base_url,args.db,args.document_prompt)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps({'status':'passed','simple_latency':report['simple_latency'],'checks':[row['kind'] for row in report['checks']]}))
