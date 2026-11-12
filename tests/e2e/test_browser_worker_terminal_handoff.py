"""Real chat, native browser adapter, child process, source capture and SQLite.

Only the external model and SDK browser implementation are deterministic fakes.
The child deliberately refuses asyncio shutdown after publishing native evidence.
"""
import json
import os
import threading
import time
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker workflow E2E')


@pytest.mark.parametrize('prompt',['表示価格を教えて。','Dime el precio mostrado.'])
@pytest.mark.parametrize('fail',[False,True])
@pytest.mark.parametrize('exit_after_publish',[False,True])
def test_published_browser_report_survives_stuck_sdk_shutdown(tmp_path,monkeypatch,prompt,source_summary_model_reply,fail,exit_after_publish):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.end_headers();self.wfile.write(b'Total $37')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);threading.Thread(target=server.serve_forever,daemon=True).start()
    url=f'http://localhost:{server.server_port}/'
    sdk=tmp_path/'sdk-process';marker=tmp_path/'worker-pid'
    sdk.write_text('''#!/usr/bin/env python3
import asyncio,json,os,sys
from pathlib import Path
from types import SimpleNamespace
from urllib.request import urlopen
from nullion.plugins.browser_plugin import browser_use_adapter as adapter
from nullion.plugins.browser_plugin import browser_use_worker as worker
job=json.loads(Path(sys.argv[-2]).read_text())
Path(MARKER).write_text(str(os.getpid()))
async def sdk_read(task,client,principal_id,policy,timeout):
    async def stubborn():
        while True:
            try:await asyncio.sleep(60)
            except asyncio.CancelledError:pass
    asyncio.create_task(stubborn())
    if FAIL:raise ValueError('Synthetic SDK source failure')
    body=urlopen(task.url,timeout=2).read().decode()
    evidence=adapter.PageEvidence(principal_id,policy)
    await evidence.capture(SimpleNamespace(url=task.url,title='Owned source',screenshot=None,
        dom_state=SimpleNamespace(llm_representation=lambda:body)),step=0)
    evidence.retain(adapter.BrowserReport(goal_status='partial',next_action='report',
        observations=[{'label':'Displayed total','value':'$37','quote':'Total $37','source_id':0,'semantic_role':'attribute'}],
        unknowns=[],blocker=None))
    result=evidence.result(error='Synthetic SDK deadline; captured value retained',elapsed=.1,steps=1,done=False)
    result['execution_audit']={'version':1,'complete':False,'url_inventory_complete':False,
        'visited_urls':[task.url],'action_names':['record_observations'],'read_only_actions_only':False,
        'host_file_actions_enabled':False,'downloaded_file_count':0,'captures_are_internal':True}
    return result
adapter.run_browser_task=sdk_read
if EXIT:
    publish=worker._publish_worker_result
    def publish_then_exit(path,result):
        publish(path,result)
        os._exit(7)
    worker._publish_worker_result=publish_then_exit
asyncio.run(worker._worker(job,Path(sys.argv[-1])))
'''.replace('MARKER',repr(str(marker))).replace('FAIL',repr(fail)).replace('EXIT',repr(exit_after_publish)))
    sdk.chmod(0o700)
    for key,value in {'NULLION_HOME':str(tmp_path/'home'),'NULLION_BROWSER_USE_PYTHON':str(sdk),
        'NULLION_BROWSER_BLOCK_PRIVATE':'false','NULLION_MEMORY_ENABLED':'false','NULLION_WORKSPACE_STORAGE_ROOT':str(tmp_path/'workspaces')}.items():monkeypatch.setenv(key,value)
    monkeypatch.setattr(chat_store,'_store',chat_store.ChatStore(tmp_path/'chat.db'))
    class Model:
        model='external-provider-fixture';calls=0
        def create(self,**kwargs):
            if fail and str(kwargs.get('system','')).startswith('You are a semantic completion reviewer'):
                raise TimeoutError('Optional review unavailable in the error fixture')
            answer=source_summary_model_reply(kwargs)
            if answer is not None:return answer
            names={t['name'] for t in kwargs.get('tools',[])}
            if 'present_source_answer' in names:
                from tests.source_answer_fixture import source_presentation_response
                response=source_presentation_response(kwargs)
                draft=response['content'][0]['input'];draft['focus_roles']=['attribute']
                for point in draft['points']:point['semantic_role']='attribute'
                return response
            if not names:
                return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps({'web_action':'browser_interaction',
                    'requested_tool_names':['browser_run_task'],'required_tool_names':['browser_run_task'],
                    'requested_artifact_extensions':[],'confidence':1})}]}
            if 'select_attachment_format' in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':None,'confidence':1}}]}
            if self.calls==0:
                self.calls+=1;name='request_tool_scope';args={'capabilities':['web'],'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task']}
            elif self.calls==1:
                self.calls+=1;name='browser_run_task';args={'url':url,'task':prompt,'max_steps':1}
            else:
                if fail:return {'stop_reason':'end_turn','content':[{'type':'text','text':'The source could not be read; no price was verified.'}]}
                assert '$37' in json.dumps(kwargs['messages'])
                return {'stop_reason':'end_turn','content':[{'type':'text','text':'Displayed price: $37. '+url}]}
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    model=Model();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    runtime.active_tool_registry=registry
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)
    before=time.monotonic();cid='web:owned-shutdown'
    try:
        with TestClient(app) as client:
            response=client.post('/api/chat',json={'text':prompt+' '+url,'conversation_id':cid})
            assert response.status_code==200,response.text
            rows=runtime.store.list_conversation_events(cid)
            if not fail and '$37' not in response.json()['text']:
                print(json.dumps({'calls':model.calls,'tools':[r.get('tool_results') for r in rows
                    if r.get('event_type')=='conversation.chat_turn']},ensure_ascii=False,default=str))
            assert response.json()['text'].strip()
            if fail:assert '$37' not in response.json()['text'],response.json()
            else:assert '$37' in response.json()['text'],{'response':response.json(),'calls':model.calls,
                    'turns':[r for r in rows if r.get('event_type')=='conversation.chat_turn']}
            assert time.monotonic()-before<10,'SDK shutdown held the visible response'
            rows=runtime.store.list_conversation_events(cid)
            turn=next(r for r in reversed(rows) if r.get('event_type')=='conversation.chat_turn')
            report=next(t for t in turn['tool_results'] if t['tool_name']=='browser_run_task')
            if fail:
                assert report['status']=='failed' and 'Synthetic SDK source failure' in report['error'],report
            else:
                assert report['status']=='completed' and report['output']['report_status']=='partial',report
                assert report['output']['observations'][0]['value']=='$37'
                assert report['output']['execution_audit']['complete'] is False
            assert len([t for t in turn['tool_results'] if t['tool_name']=='browser_run_task'])==1
            history=client.get('/api/chat/history/'+cid+'?sync_runtime=1').json()['messages']
            assert any(r['role']=='bot' and r['text'].strip() for r in history)
            if not fail:assert any(r['role']=='bot' and '$37' in r['text'] for r in history)
        with pytest.raises(ProcessLookupError):os.kill(int(marker.read_text()),0)
        assert not list((tmp_path/'home'/'browser-task-scratch').glob('run-*'))
    finally:server.shutdown();server.server_close()
