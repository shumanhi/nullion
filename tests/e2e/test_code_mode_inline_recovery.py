"""Real API/SQLite/Chromium recovery stays in one model-produced program."""
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


def test_navigation_failure_branches_in_python_before_extracting(tmp_path,monkeypatch,source_summary_model_reply,record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from nullion.preferences import save_preferences
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    save_preferences({'outbound_request_mode':'allow_all'})
    for key,value in [('NULLION_CODE_MODE','on'),('NULLION_BROWSER_BACKEND','playwright'),
                      ('NULLION_BROWSER_HEADLESS','true'),('NULLION_BROWSER_BLOCK_PRIVATE','false')]:
        monkeypatch.setenv(key,value)
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/broken':
                self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
            self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers()
            self.wfile.write(b'<main id="result"><h1>Verified option 37</h1><p>The selected option is available in this fixture. This source provides substantive details for the requested observation, including the verified option number and its current availability.</p></main>')
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site)
    threading.Thread(target=site.serve_forever,daemon=True).start()
    base=f'http://localhost:{site.server_port}'
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            summary=source_summary_model_reply(kwargs)
            if summary is not None:return summary
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1;time.sleep(.15)  # Model reentry is deliberately costly.
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_open','browser_navigate','browser_extract_text'],
                'required_tool_names':['browser_navigate','browser_extract_text']})
            if self.phase==2:return call('code_mode',{'tool_names':['browser_open','browser_navigate','browser_extract_text'],
                'code':"opened=call_tool('browser_open', {})\nsid=opened['output']['session_id']\n"
                    +f"nav=call_tool('browser_navigate', {{'url':{base+'/broken'!r},'session_id':sid}}, on_error='return')\n"
                    +"if nav['status']=='failed':\n"
                    +f"    nav=call_tool('browser_navigate', {{'url':{base+'/good'!r},'session_id':sid}})\n"
                    +"if nav['status']=='completed':\n    content=call_tool('browser_extract_text', {'session_id':sid,'selector':'#result'})\ncontent['output']"})
            assert self.phase==3,'Recovery reentered the model instead of branching in the program'
            assert 'Verified option 37' in json.dumps(kwargs['messages'])
            return text('Verified option 37. '+base+'/good')
    model=Model();registry=ToolRegistry()
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    samples=[]
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            for index,prompt in enumerate(['確認してください','Verifica la opción','Vérifie le résultat']):
                model.phase=0;conversation=f'web:inline-recovery-{index}';started=time.monotonic()
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation})
                samples.append(time.monotonic()-started)
                if '37' not in response.json()['text']:
                    print('FAILED RECEIPTS',json.dumps([r for e in runtime.store.list_conversation_events(conversation)
                        if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[]) if r['tool_name']=='code_mode']))
                assert response.status_code==200 and '37' in response.json()['text'],response.text
                assert model.phase==3
                rows=[r for e in runtime.store.list_conversation_events(conversation)
                    if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
                programs=[r for r in rows if r['tool_name']=='code_mode']
                assert len(programs)==1 and programs[0]['status']=='completed',rows
                assert len(programs[0]['output']['failures'])==1
                extracts=[r for r in rows if r['tool_name']=='browser_extract_text']
                assert len(extracts)==1 and extracts[0]['status']=='completed'
                assert 'chrome-error://' not in json.dumps(extracts)
        assert sum(samples)/len(samples)<8 and max(samples)<12,samples
        record_property('mean_inline_recovery_seconds',sum(samples)/len(samples))
    finally:
        registry.run_cleanup_hooks();site.shutdown();site.server_close()


@pytest.mark.parametrize('prompt',['確認して保存','Guarda el resultado','Enregistre le résultat'])
@pytest.mark.parametrize('checkpoint_during_native',[False,True])
def test_shared_request_recovery_keeps_heap_and_completed_writes(tmp_path,monkeypatch,prompt,checkpoint_during_native):
    from nullion import code_mode
    from nullion.code_mode_sessions import SessionJournal
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _registry_with_request_retry_evidence
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    if checkpoint_during_native:
        runtime.store._memory_checkpoint_path=None
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));writes=[];target=tmp_path/'verified.txt'
    def prepare(i):
        writes.append(i.invocation_id)
        if checkpoint_during_native:
            runtime.checkpoint()
        return ToolResult(i.invocation_id,i.tool_name,'completed',{'value':37})
    for name,effect,handler in [('prepare_record',ToolSideEffectClass.WRITE,prepare),
        ('file_write',ToolSideEffectClass.WRITE,_build_file_write_handler(tmp_path,allowed_roots=(tmp_path,)))]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,5,input_schema={'type':'object'}),handler)
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            self.phase+=1
            if self.phase==1:return call('code_mode',{'tool_names':['prepare_record'],
                'code':"saved=call_tool('prepare_record', {})['output']['value']\nsaved"})
            if self.phase==2:return text('Saved preparation 37.')
            if self.phase==3:
                program=call('code_mode',{'tool_names':['file_write'],
                    'code':f"call_tool('file_write', {{'path':{str(target)!r},'content':str(saved+5)}})"})
                program['content'][0]['id']='continued-program';return program
            assert self.phase==4
            return text('Saved verified result 42.')
    model=Model();runner=AgentOrchestrator(model_client=model)
    common={'conversation_id':'web:graph-recovery','principal_id':'workspace_admin','user_message':prompt,
        'conversation_history':[],'policy_store':runtime.store,'approval_store':runtime.store}
    @code_mode.request_session_scope
    def request():
        first=runner.run_turn(**common,tool_registry=registry)
        continued=_registry_with_request_retry_evidence(registry,result=first,conversation_id=common['conversation_id'],
            principal_id=common['principal_id'],user_message=prompt)
        result=runner.run_turn(**common,tool_registry=continued)
        assert result.code_mode_session_id==first.code_mode_session_id
        assert target.read_text()=='42' and len(writes)==1
        return result
    result=request()
    record=SessionJournal({**common,'runtime_store':runtime.store}).read(result.code_mode_session_id)
    assert record['phase']=='closed' and record['snapshot'] is None
