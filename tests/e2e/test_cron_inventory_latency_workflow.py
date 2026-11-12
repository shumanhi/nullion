"""Cron inventory is a read, and cannot stall chat during checkpoint writes."""
from tests.workflow_provider_protocol import workflow_provider_reply
import asyncio
import json
import os
import sqlite3
import statistics
import threading
import time

import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


def test_cron_inventory_and_plain_chat_remain_responsive_under_checkpoint_contention(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import runtime_store_file_lock
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    class Provider:
        def create(self,**kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'4'}]}
    provider=Provider();runtime.model_client=provider
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',runtime.checkpoint_path)
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=ToolRegistry())
    with TestClient(app) as client:
        job=crons.add_cron('Owned inventory','0 12 * * *','Prepared status',enabled=False)
        # Simulate the exact legacy schedule metadata that used to cause a
        # synchronous write on the inventory GET.
        with sqlite3.connect(str(crons._runtime_db_path())) as db:
            row=db.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',('cron_jobs',job.id)).fetchone()
            payload=json.loads(row[0]);payload['schedule_timezone']=''
            db.execute('UPDATE reminders_crons SET payload=? WHERE collection=? AND item_key=?',
                (json.dumps(payload),'cron_jobs',job.id))
        locked=threading.Event();release=threading.Event()
        def checkpoint_writer():
            with runtime_store_file_lock(runtime.checkpoint_path):
                locked.set();release.wait(5)
        writer=threading.Thread(target=checkpoint_writer);writer.start();assert locked.wait(2)
        try:
            # Real routes and SQLite stay live; only the external model is fake.
            # The file lock models a slow checkpoint without mocking a route.
            from concurrent.futures import ThreadPoolExecutor
            def read_inventory():
                started=time.monotonic();response=client.get('/api/crons')
                assert response.status_code==200,response.text
                assert any(row['id']==job.id for row in response.json())
                return time.monotonic()-started
            def read_tool_inventory():
                from nullion.tools import ToolRegistry,ToolInvocation,register_cron_tools
                selected=ToolRegistry();register_cron_tools(selected)
                started=time.monotonic()
                response=selected.invoke(ToolInvocation('inventory-read','list_crons','web:latency',{}))
                assert response.status=='completed',response
                assert any(row['id']==job.id for row in response.output['crons'])
                return time.monotonic()-started
            def plain(index):
                started=time.monotonic();response=client.post('/api/chat',json={
                    'text':('Return only 4','Solo 4','数字は4だけ')[index], 'conversation_id':'web:latency:'+str(index)})
                assert response.status_code==200 and response.json()['text']=='4',response.text
                assert not response.json().get('artifacts')
                return time.monotonic()-started
            with ThreadPoolExecutor(max_workers=5) as pool:
                inventory=pool.submit(read_inventory)
                tool_inventory=pool.submit(read_tool_inventory)
                samples=[pool.submit(plain,index) for index in range(3)]
                inventory_elapsed=inventory.result(timeout=2)
                tool_inventory_elapsed=tool_inventory.result(timeout=2)
                elapsed=[sample.result(timeout=2) for sample in samples]
            assert inventory_elapsed<1 and tool_inventory_elapsed<1 and statistics.mean(elapsed)<1 and max(elapsed)<1.5,(inventory_elapsed,tool_inventory_elapsed,elapsed)
            with sqlite3.connect(str(runtime.checkpoint_path)) as db:
                payload=json.loads(db.execute('SELECT payload FROM reminders_crons WHERE collection=? AND item_key=?',
                    ('cron_jobs',job.id)).fetchone()[0])
            assert payload['schedule_timezone']=='',payload
        finally:
            release.set();writer.join(2)


def test_large_inventory_is_paged_without_truncated_json_or_mutations(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app, crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_MODEL_TOOL_RESULT_MAX_CHARS', '10000')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', runtime.checkpoint_path)
    pages = []; seen = {}
    class Provider:
        def create(self, **kw):
            if 'semantic completion reviewer' in kw.get('system',''):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                reviewed=[item for row in packet['tool_evidence'] if row['tool_name']=='list_crons' for item in row['output']['crons']]
                assert {row['id'] for row in reviewed}==set(seen), 'Outcome review lost observed inventory identities'
            auxiliary = workflow_provider_reply(kw, completion=True)
            if auxiliary is not None: return auxiliary
            names = {row['name'] for row in kw.get('tools', [])}
            if 'list_crons' not in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                    'input':{'capabilities':['scheduler_read'],'scheduler_action':'inspect','tool_names':['list_crons'], 'required_tool_names':['list_crons']}}]}
            latest = None
            for message in kw['messages']:
                for block in message.get('content', []) if isinstance(message.get('content'), list) else []:
                    if block.get('type') != 'tool_result': continue
                    content = block.get('content', [])
                    raw = content if isinstance(content, str) else ''.join(row.get('text', '') for row in content)
                    try: payload = json.loads(raw)
                    except ValueError: continue
                    output = payload.get('output')
                    if isinstance(output, dict) and 'crons' in output: latest = output
            args = {}
            if latest is not None:
                assert isinstance(latest, dict), 'Inventory became a truncated JSON string'
                if latest.get('inventory_index'):
                    assert latest.get('inventory_index_guidance'), 'Full identity guidance was lost during projection'
                pages.append(latest)
                seen.update({row['id']: row['name'] for row in latest['crons']})
                if not latest['has_more']:
                    return {'stop_reason':'end_turn','content':[{'type':'text','text':'Scheduled task inventory:\n' +
                        '\n'.join(str(i)+'. '+name for i,name in enumerate(seen.values(),1))}]}
                args = {'offset':latest['next_offset'], 'inventory_version':latest['inventory_version']}
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'page'+str(len(pages)), 'name':'list_crons','input':args}]}
    provider = Provider(); runtime.model_client = provider
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=provider), registry=ToolRegistry())
    with TestClient(app) as client:
        expected = set()
        for i in range(120):
            name = '記録-' + str(i) + ' — 完全な名前 / revisión ' + str(i)
            response = client.post('/api/crons', json={'name':name,'schedule':'0 * * * *','enabled':False,
                'task':'Private saved task instructions. '*200})
            assert response.status_code == 200, response.text
            expected.add(name)
        started = time.monotonic()
        response = client.post('/api/chat', json={'text':'予定の一覧を番号付きで表示。実行や変更は不要。','conversation_id':'web:inventory-pages'})
        assert response.status_code == 200, response.text
        assert expected == set(seen.values()), response.json()
        assert all(name in response.json()['text'] for name in expected)
        assert 2 <= len(pages) <= 8 and all(isinstance(page['crons'], list) for page in pages)
        assert time.monotonic() - started < 5
        assert all(not row['enabled'] and not row.get('last_run') for row in client.get('/api/crons').json())
        version = pages[0]['inventory_version']
        client.post('/api/crons', json={'name':'追加','schedule':'0 * * * *','enabled':False,'task':'Unchanged'})
        from nullion.tools import _build_list_crons_handler, ToolInvocation
        stale = _build_list_crons_handler()(ToolInvocation('stale','list_crons','web:inventory-pages',{'offset':40,'inventory_version':version}))
        assert stale.status == 'failed' and stale.output['reason'] == 'inventory_changed'


@pytest.mark.parametrize('timezone_name', ['UTC','Asia/Tokyo','America/New_York'])
def test_cron_api_preserves_explicit_timezone_across_create_and_update(tmp_path,monkeypatch,timezone_name):
    from fastapi.testclient import TestClient
    from nullion import crons,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',runtime.checkpoint_path)
    class Provider:
        def create(self,**kwargs):raise AssertionError('Scheduler configuration must not call a model')
    provider=Provider();runtime.model_client=provider
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=provider),registry=ToolRegistry())
    with TestClient(app) as client:
        created=client.post('/api/crons',json={'name':'予定','schedule':'0 12 * * *','task':'確認する',
            'enabled':False,'schedule_timezone':timezone_name})
        assert created.status_code==200,created.text
        job=created.json()['job'];assert job['schedule_timezone']==timezone_name and job['next_run'] is None
        updated=client.patch('/api/crons/'+job['id'],json={'enabled':True,'schedule_timezone':timezone_name})
        assert updated.status_code==200,updated.text
        saved=updated.json()['job'];assert saved['schedule_timezone']==timezone_name
        from datetime import datetime
        assert datetime.fromisoformat(saved['next_run']).astimezone(crons._resolve_timezone_name(timezone_name)).hour==12
        bad=client.patch('/api/crons/'+job['id'],json={'schedule_timezone':'Missing/Invalid'})
        assert bad.status_code==400
        retained=next(row for row in client.get('/api/crons').json() if row['id']==job['id'])
        assert retained['schedule_timezone']==timezone_name and retained['next_run']==saved['next_run']
        rejected=client.post('/api/crons',json={'name':'別の予定','schedule':'0 * * * *','task':'確認する',
            'enabled':False,'schedule_timezone':'Missing/Invalid'})
        assert rejected.status_code==400


@pytest.mark.parametrize('prompt', ['Corrige los nombres y las frecuencias de la lista anterior.', '前の一覧の名前と頻度を正確に直してください。'])
def test_inventory_revision_keeps_all_observed_names_without_detail_rereads(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app, crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', runtime.checkpoint_path)
    class Provider:
        revision=False; phase=0; rows=[]
        def create(self, **kw):
            if 'semantic completion reviewer' in kw.get('system', ''):
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                for record in packet['tool_evidence']:
                    if record['tool_name'] != 'list_crons':
                        continue
                    output = record['output']
                    assert output['inventory_scope']['scheduler_kind'] == 'cron'
                    if output.get('context_sample_only'):
                        assert output['cron_count'] == 69
                        assert output['inventory_index_complete'] is True
                        assert output['source_page_has_more'] is False
                        assert {row['name'] for row in output['inventory_index']} == set(self.rows)
            auxiliary=workflow_provider_reply(kw, completion=True)
            if auxiliary is not None:return auxiliary
            if self.revision:
                context=str(kw['messages'])
                assert '記録-68' in context, 'The final task was lost from repair context'
                return {'stop_reason':'end_turn','content':[{'type':'text','text':'\n'.join(
                    str(i+1)+'. '+name+' — Annually, January 1' for i,name in enumerate(self.rows))}]}
            self.phase+=1
            if self.phase==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                    'input':{'capabilities':['scheduler_read'],'scheduler_action':'inspect','tool_names':['list_crons'], 'required_tool_names':['list_crons']}}]}
            if self.phase==2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'inventory','name':'list_crons','input':{}}]}
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'Scheduled inventory:\n'+'\n'.join(self.rows)}]}
    model=Provider();runtime.model_client=model
    app=web_app.create_app(runtime, AgentOrchestrator(model_client=model), ToolRegistry())
    with TestClient(app) as client:
        for i in range(69):
            name='記録-'+str(i);model.rows.append(name)
            assert client.post('/api/crons',json={'name':name,'schedule':'0 0 1 1 *','enabled':False,
                'task':'Internal task details. '*100}).status_code==200
        first=client.post('/api/chat',json={'text':'番号付きの予定一覧を表示してください。','conversation_id':'web:inventory-revision'})
        assert first.status_code==200 and all(name in first.json()['text'] for name in model.rows)
        model.revision=True;elapsed=[]
        for _ in range(3):
            started=time.monotonic()
            revised=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:inventory-revision'})
            elapsed.append(time.monotonic()-started)
            assert revised.status_code==200 and '記録-68' in revised.json()['text'],revised.text
            assert all(name in revised.json()['text'] for name in model.rows)
        assert statistics.mean(elapsed)<1.5 and max(elapsed)<2,elapsed
        turns=[row for row in runtime.store.list_conversation_events('web:inventory-revision') if row.get('event_type')=='conversation.chat_turn']
        assert sum(t['tool_name']=='list_crons' for row in turns for t in row['tool_results'])==1
        stored=next(t['output'] for row in turns for t in row['tool_results'] if t['tool_name']=='list_crons')
        assert stored['inventory_index_complete'] is True and len(stored['inventory_index'])==69
        assert {r['name'] for r in stored['inventory_index']}==set(model.rows)
        assert not any(t['tool_name']=='inspect_retained_evidence' for row in turns for t in row['tool_results'])
        assert all(not row['enabled'] for row in client.get('/api/crons').json())
