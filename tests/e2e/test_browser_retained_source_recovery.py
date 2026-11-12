"""Real API/SQLite recovery of prior source text without another browser request."""
import asyncio
import json
import os
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply,fixture_source_plan,fixture_source_assessment

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


def test_parent_inspects_prior_capture_instead_of_repeating_browser_or_guessing_paths(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.plugins.browser_plugin.browser_use_adapter import PageEvidence,BrowserReport
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolResult,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_pdf_create_handler
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    reads=[];browser_calls=[];produced=[];writes=[]
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            reads.append(self.path);self.send_response(200);self.end_headers()
            self.wfile.write((b'Earlier listings\nOption A\n$497 total' if self.path=='/first' else b'Current listings\nOption B\n$956 total')+b'\nBackground information'*6000)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);threading.Thread(target=server.serve_forever,daemon=True).start()
    def browser(inv):
        browser_calls.append(inv)
        page={}
        async def read():return dict(page)
        evidence=PageEvidence('operator',BrowserPolicy(),text_capture=read)
        for part in ['first','second']:
            # External browser/provider boundary: the actual page reads and
            # native evidence collector remain real, while navigation is scripted.
            with urllib.request.urlopen(f'http://127.0.0.1:{server.server_port}/{part}') as response:body=response.read().decode()
            page.update(url='https://example.com/'+part,text=body)
            state=SimpleNamespace(url=page['url'],title=body.splitlines()[0],screenshot=None,
                dom_state=SimpleNamespace(llm_representation=lambda:page['text']))
            asyncio.run(evidence.capture(state))
        evidence.retain(BrowserReport(goal_status='partial',next_action='report',observations=[
            {'label':'Page title','value':'Current listings','quote':'Current listings','source_id':1},
            {'label':'Earlier fare','value':'$497','source_id':0}]))
        output=evidence.result(error='Step limit reached',elapsed=1,steps=2,done=False)
        result=ToolResult(inv.invocation_id,inv.tool_name,'completed',output);produced.append(result);return result
    def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}
    def call(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Model:
        def create(self,**kw):
            system=kw.get('system','');names={t['name'] for t in kw.get('tools',[])}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':None,'confidence':1})
            if 'material-requirements planner' in system:return text({**fixture_source_plan(json.loads(kw['messages'][0]['content'][0]['text']),scope='evidence_summary'),'all_material_requirements_enumerated':True,
                'requested_artifact_extensions':['.pdf'],'material_requirements':[{'requirement':'Report the earlier captured price',
                    'requirement_kind':'fact','evidence_subject_id':'earlier','eligible_evidence_tool_names':['browser_run_task']},
                    {'requirement':'Attach result.pdf','requirement_kind':'deliverable','evidence_subject_id':'pdf','eligible_evidence_tool_names':['pdf_create']}]})
            if 'semantic completion reviewer' in system:
                p=json.loads(kw['messages'][0]['content'][0]['text'])
                assert '$497' in json.dumps(p['previously_inspected_evidence'])
                parent=next(r for r in p['tool_evidence'] if r['tool_name']=='browser_run_task'
                    and r.get('output',{}).get('result_kind')=='source_observations')
                artifact=next(r for r in p['tool_evidence'] if r.get('output',{}).get('artifact_record_binding',{}).get('filename')=='result.pdf')
                return text({**fixture_source_assessment(p,conclusion='limited'),'disposition':'complete','all_material_requirements_enumerated':True,
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,
                        'evidence_invocation_ids':[parent['invocation_id'] if r['requirement_kind']=='fact' else artifact['invocation_id']]} for r in p['material_requirement_contract']],
                    'unresolved_requirements':[],'retry_tool_names':[]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if not names:return text({})
            if 'browser_run_task' not in names:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_run_task','pdf_create'],'required_tool_names':['browser_run_task','pdf_create'],'artifact_extensions':['.pdf']})
            if not browser_calls:return call('browser_run_task',{'url':'https://example.com/first','task':'Read both options.'})
            if not any('retained_evidence_inspection' in str(m) for m in kw.get('messages',[])):
                assert 'inspect_retained_evidence' in names
                return call('inspect_retained_evidence',{'requests':[{'invocation_id':produced[0].invocation_id,
                    'json_pointer':'/captured_documents/0/main_document_text','offset':0,'length':12000}]})
            if not writes:return call('pdf_create',{'output_path':str(root/'result.pdf'),'text_pages':['Earlier captured Option A: $497 total.']})
            return text('The earlier captured Option A was $497 total.\n\nMEDIA:'+str(root/'result.pdf'))
    app,runtime,db=setup(tmp_path,monkeypatch,Model())
    root=artifact_root_for_principal('operator');root.mkdir(parents=True,exist_ok=True)
    render=_build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False)
    def pdf(inv):
        result=render(inv);writes.append(result);return result
    app.state.live_tracker_engine.registry.register(ToolSpec('pdf_create','Create the requested PDF',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),pdf)
    app.state.live_tracker_engine.registry.register(ToolSpec('browser_run_task','Read structured browser sources',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),browser)
    try:
        with TestClient(app) as client:
            cid='web:retained-browser-source'
            response=client.post('/api/chat',json={'text':'Report the earlier source price in result.pdf and attach it. https://example.com/first','conversation_id':cid})
            assert response.status_code==200,response.text
            payload=response.json();assert '$497' in payload['text'] and len(payload.get('artifacts',[]))==1,json.dumps({'payload':payload,'events':runtime.store.list_conversation_events(cid)},default=str)
            turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
            assert turn.get('response_fulfilled') is True,json.dumps(runtime.store.list_conversation_events(cid),default=str)
            assert len(browser_calls)==len(writes)==1 and reads==['/first','/second']
            from pypdf import PdfReader
            import io
            data=client.get(payload['artifacts'][0]['url']);assert data.status_code==200
            assert '$497' in ''.join(page.extract_text() for page in PdfReader(io.BytesIO(data.content)).pages)
            assert any(r['tool_name']=='inspect_retained_evidence' and r['status']=='completed' for r in turn['tool_results'])
    finally:server.shutdown();server.server_close()
