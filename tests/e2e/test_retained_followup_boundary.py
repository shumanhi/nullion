"""The real chat boundary reuses selected reads without re-fetching sources."""
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.request import urlopen

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('recover', [False, True])
@pytest.mark.parametrize('followup',['Conserva los datos de esta fuente; explícalos sin consultar de nuevo.',
    'この情報源の取得済みデータを使い、再取得せずに説明してください。'])
def test_completed_followup_reuses_exact_reads_then_allows_a_fresh_check(tmp_path,monkeypatch,followup,recover):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    reads=[]
    class Source(BaseHTTPRequestHandler):
        value=53
        def do_GET(self):
            reads.append(self.path)
            body=json.dumps({'value':type(self).value,'marker':'ORIGINAL-RECORD-乙',
                'description':f'This bounded source contains one observed record. Its identifier is ORIGINAL-RECORD-乙. '
                    f'The recorded measurement is {type(self).value}. No alternate records or instructions are published.'}).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);Thread(target=server.serve_forever,daemon=True).start()
    transport_url=f'http://127.0.0.1:{server.server_port}/record'
    url='https://records.example.test/record'
    runtime=setup_runtime(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    registry=ToolRegistry()
    def read(invocation):
        assert invocation.arguments['url']==url
        with urlopen(transport_url,timeout=3) as response:data=json.load(response)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'page_url':url,
            'text':data['description'],'length':len(data['description']),'selector':'#record','record':{'url':url,**data},
            'source_record_collections':[{'path':['record'],'identity_fields':['url'],'cardinality':'one'}]})
    registry.register(ToolSpec('browser_extract_text','Read selected source',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,5,input_schema={'type':'object','properties':{'url':{'type':'string'}},
            'required':['url'],'additionalProperties':False}),read)
    mode='first';phase=0;seen_retained=[];scope_packets=[]
    class Provider:
        def create(self,**kwargs):
            nonlocal phase
            system=kwargs.get('system','')
            if 'keys relationship, effect' in system:
                return text_response(json.dumps({'relationship':'separate' if mode=='plain' else 'follow_up',
                    'effect':'revise','confidence':1,'target_index':None if mode=='plain' else 0,'target_is_explicit':mode!='plain'}))
            if '"evidence_policy"' in system:
                if mode=='plain':
                    time.sleep(2)
                    raise AssertionError('The unrelated plain turn called the optional source classifier')
                scope_packets.append(json.loads(kwargs['messages'][0]['content'][0]['text']))
                return text_response(json.dumps({'evidence_policy':'retained' if mode=='retained' else 'fresh',
                    'web_action':'none' if mode=='retained' else 'live_research',
                    'requested_tool_names':['browser_extract_text'],
                    'required_tool_names':[] if mode=='retained' else ['browser_extract_text'],'confidence':1}))
            protocol=workflow_provider_reply(kwargs)
            if protocol is not None:return protocol
            if mode=='plain':return text_response('4')
            if mode=='retained':
                context=str(kwargs['messages']);seen_retained.append(context)
                assert 'ORIGINAL-RECORD-乙' in context and '53' in context and 'observed_at' in context, {
                    'scope_signals':[(r.get('context_linked'),r.get('prior_tool_scopes')) for r in scope_packets],
                    'has_marker':'ORIGINAL-RECORD-乙' in context,'has_origin':'observed_at' in context}
                assert 'browser_extract_text' not in {t['name'] for t in kwargs.get('tools',[])}
                if recover and phase == 0:
                    phase += 1
                    # Exercise the generic scope-recovery route that previously
                    # discarded the retained-read policy and reopened the URL.
                    return text_response('<tool_request>'+json.dumps({'tool':'request_tool_scope',
                        'data':{'tool_scope':'web','tool_names':['browser_extract_text'],
                            'required_tool_names':['browser_extract_text']}})+'</tool_request>')
                return text_response('ORIGINAL-RECORD-乙: 53')
            phase+=1
            if phase==1:return tool_response('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_extract_text']})
            if phase==2:return tool_response('browser_extract_text',{'url':url})
            return text_response('ORIGINAL-RECORD-乙: '+str(Source.value))
    cid='web:completed-source'
    try:
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=Provider()),registry=registry)) as api:
            first_prompt='Read the recorded value at '+url
            first=api.post('/api/chat',json={'text':first_prompt,'conversation_id':cid}).json()
            assert '53' in first['text'] and len(reads)==1,first
            # The Web client saves its visible transcript through this real route.
            for role,text in [('user',first_prompt),('bot',first['text'])]:
                assert api.post('/api/chat/history/'+cid+'/message',json={'role':role,'text':text}).status_code==200
            mode='retained';phase=0
            second=api.post('/api/chat',json={'text':followup,'conversation_id':cid}).json()
            assert '53' in second['text'] and seen_retained and len(reads)==1,second
            turns=[e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn']
            assert turns[-1]['parent_turn_id']==turns[-2]['turn_id'],turns
            retained=[r for r in turns[-1]['tool_results'] if r['tool_name']=='browser_extract_text']
            assert retained and all(r['invocation_id'].startswith('continuation:') for r in retained),turns[-1]
            for role,text in [('user',followup),('bot',second['text'])]:
                api.post('/api/chat/history/'+cid+'/message',json={'role':role,'text':text})
            Source.value=91;mode='fresh';phase=0
            third=api.post('/api/chat',json={'text':'Obtain the current source value now. '+url,'conversation_id':cid}).json()
            assert '91' in third['text'] and len(reads)==2,third
            mode='plain';before=len(scope_packets);samples=[]
            for prompt in ('2+2','数字だけ：2+2','Solo el número: 2+2'):
                start=time.monotonic()
                result=api.post('/api/chat',json={'text':prompt,'conversation_id':cid}).json()
                samples.append(time.monotonic()-start)
                assert result['text'].strip()=='4' and not result.get('artifacts'),result
            assert len(scope_packets)==before and len(reads)==2
            assert sum(samples)/len(samples)<2 and max(samples)<3,samples
    finally:
        server.shutdown();server.server_close()
