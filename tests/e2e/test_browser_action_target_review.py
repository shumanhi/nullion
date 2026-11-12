"""Real Web, browser DOM and preflight; no complaint leaves the test server."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime,text_response,tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('label,prompt',[('Submit feedback','Open the feedback form and prepare a management notice. Ask before submitting.'),
    ('意見を送信','管理者向けの連絡フォームを開き、下書きを準備してください。送信前に確認してください。')])
@pytest.mark.parametrize('target_kind',['navigation','submission'])
def test_late_control_is_reviewed_exactly_and_final_submission_still_needs_confirmation(tmp_path,monkeypatch,label,prompt,target_kind):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry
    visits=[];submissions=[];packets=[];phase=0;target=None
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            visits.append(self.path)
            menu=''.join(f'<a href="/other/{i}">Menu {i}</a>' for i in range(85))
            control=(f'<a href="/feedback">{label}</a>' if target_kind=='navigation' else
                f'<form action="/submit" method="post"><button>{label}</button></form>')
            body=('<main><h1>Complaint draft</h1><p>Order 05B33: eleven sauces were missing. Please notify management only.</p></main>'
                  if self.path=='/feedback' else menu+control)
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers();self.wfile.write(body.encode())
        def do_POST(self):
            submissions.append(self.path);self.send_response(204);self.end_headers()
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);threading.Thread(target=server.serve_forever,daemon=True).start()
    base=f'http://127.0.0.1:{server.server_port}'
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright');monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false')
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0');runtime=setup_runtime(tmp_path,monkeypatch)
    registry=ToolRegistry();register_browser_tools(registry)
    class Provider:
        def create(self,**kw):
            nonlocal phase,target
            system=kw.get('system','')
            if 'Workflow action preflight.' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                if packet['proposed_action']['tool_name']=='browser_click_id':
                    packets.append(packet);context=packet['adapter_target_evidence']['context']
                    control=context['target'];assert context['status']=='observed' and control['text']==label
                    assert control['element_id']==target and context['source_invocation_id']
                    sampled=json.dumps(packet['execution_evidence'])
                    assert label not in sampled, 'The test must reproduce a target outside the sampled evidence prefix'
                    if target_kind=='submission':
                        assert control['native_type']=='submit' and control['form_action']==base+'/submit'
                        return text_response(json.dumps({'decision':'clarify','issues':['Final submission needs confirmation.'],
                            'issue_kind':'concrete_conflict','step_effect':'mutation','question':'Approve this prepared notice?',
                            'options':['Send the notice','Edit the notice']}))
                    assert control['tag']=='a' and control['href']==base+'/feedback' and not control['form_action']
                return text_response(json.dumps({'decision':'proceed','issues':[],'issue_kind':'none','step_effect':'observation'}))
            protocol=workflow_provider_reply(kw)
            if protocol is not None:return protocol
            phase+=1
            if phase==1:return tool_response('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_navigate','browser_snapshot','browser_click_id','browser_extract_text']})
            if phase==2:return tool_response('browser_navigate',{'url':base})
            if phase==3:return tool_response('browser_snapshot',{'max_elements':120})
            if phase==4:
                # Script only the external model's proposed action. Its target
                # is the fixture's 86th control; the real DOM and adapter must
                # independently bind its identity and effect during review.
                target='n-86'
                return tool_response('browser_click_id',{'element_id':target})
            if phase==5:return tool_response('browser_extract_text',{})
            return text_response('Draft for management: Order 05B33 was missing all eleven sauces. Please notify the restaurant team. Nothing has been submitted.\n\n1. Approve this draft\n2. Edit it')
    try:
        cid='web:target-review'
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=Provider()),registry=registry)) as api:
            response=api.post('/api/chat',json={'text':'/chat '+prompt,'conversation_id':cid});assert response.status_code==200,response.text
            visible=response.json()['text'];assert not submissions
            assert packets and ('1.' in visible and '2.' in visible), json.dumps({'reply':visible,'phase':phase,
                'events':[{k:e[k] for k in ('event_type','decision','issues','review_error') if k in e}
                    for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.action_review']},ensure_ascii=False)
            if target_kind=='navigation':
                assert '/feedback' in visits and 'eleven sauces' in visible and 'Draft for management' in visible,visible
            else:
                assert '/feedback' not in visits and 'Approve this prepared notice' in visible,visible
            turns=[e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn']
            assert turns[-1]['assistant_reply']==visible
            click=next(r for r in turns[-1]['tool_results'] if r['tool_name']=='browser_click_id')
            assert click['status']==('completed' if target_kind=='navigation' else 'failed')
    finally:
        registry.run_cleanup_hooks();server.shutdown();server.server_close()
