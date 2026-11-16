"""An exact existing-file request delivers the original bytes through the API."""
import hashlib
import os
from pathlib import Path
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

@pytest.mark.parametrize('mode',['off','on'])
@pytest.mark.parametrize('prompt',['Adjunta cedar.pdf como archivo descargable','cedar.pdf を添付してください','Joignez le fichier cedar.pdf'])
def test_exact_filename_lookup_delivers_without_research_or_regeneration(tmp_path,monkeypatch,mode,prompt):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_search_handler
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE',mode)
    root=workspace_storage_roots_for_principal('workspace_admin').artifacts;root.mkdir(parents=True,exist_ok=True)
    target=root/'cedar.pdf';Image.new('RGB',(40,40),'blue').save(target);original=target.read_bytes()
    registry=ToolRegistry(filesystem_allowed_roots=(root,))
    registry.register(ToolSpec('file_search','Find native file identities',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,code_mode_allowed=True),
        _build_file_search_handler(root,allowed_roots=(root,),include_principal_workspace=False))
    class Model:
        phase=0;errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            try:return self.respond(kw)
            except Exception as e:self.errors.append(repr(e));raise
        def respond(self,kw):
            assert not any(t['name']=='select_attachment_format' for t in kw.get('tools',[]))
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_search'],
                'required_tool_names':['file_search'],'artifact_extensions':['.pdf'],
                'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt})
            if self.phase==2:
                args={'pattern':'cedar.pdf','root':str(root)}
                return call('code_mode',{'tool_names':['file_search'],'code':f"call_tool('file_search', {args!r})"}) if mode=='on' else call('file_search',args)
            assert self.phase==3
            return text('Found cedar.pdf')
    model=Model();cid='web:exact-original'
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
        assert not model.errors,model.errors
        body=response.json();assert response.status_code==200 and len(body['artifacts'])==1,body
        artifact=body['artifacts'][0];assert artifact['name']=='cedar.pdf'
        download=client.get(artifact['url']);assert download.status_code==200
        assert hashlib.sha256(download.content).digest()==hashlib.sha256(original).digest()
    assert target.read_bytes()==original
    turns=[e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn']
    assert turns[-1]['response_fulfilled'] is True,turns[-1]
    assert {r['tool_name'] for r in turns[-1]['tool_results']} <= {'request_tool_scope','file_search','code_mode'}
    assert model.phase<=3


@pytest.mark.parametrize('prompt', ['Attach the same Cedar PDF and confirm its dates; do not regenerate it.',
    'Adjunta el mismo PDF Cedar y confirma las fechas sin regenerarlo.',
    '同じCedar PDFを添付し、日付を確認してください。再生成しないでください。'])
def test_history_delivery_receipt_reaches_code_mode_actor_and_preserves_original_bytes(tmp_path, monkeypatch, prompt):
    import json
    from datetime import UTC, datetime
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry, ToolInvocation, _build_pdf_create_handler
    from nullion.conversation_history_tools import with_conversation_history_tool
    from nullion.artifacts import artifact_root_for_principal
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch); monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    cid = 'web:history-reattach'; root = artifact_root_for_principal(cid); root.mkdir(parents=True, exist_ok=True)
    target = root/'cedar.pdf'
    produced = _build_pdf_create_handler(root, allowed_roots=(root,))(ToolInvocation('original-pdf', 'pdf_create', cid,
        {'output_path':str(target), 'text_pages':['Cedar trip: 2031-04-09 to 2031-04-10'], 'expected_page_count':1}))
    assert produced.status == 'completed', produced
    target = Path(produced.output['path']); original = target.read_bytes()
    runtime.store.add_conversation_event({'conversation_id':cid, 'workspace_id':'workspace_admin',
        'turn_id':'cedar-original', 'event_type':'conversation.chat_turn', 'created_at':datetime.now(UTC).isoformat(),
        'user_message':'Cedar trip PDF', 'assistant_reply':'Cedar dates: 2031-04-09 to 2031-04-10.\n' +
            'Verified trip details. '*150 + '\nMEDIA:'+str(target), 'response_fulfilled':True,
        'tool_results':[{'invocation_id':'original-pdf', 'tool_name':'pdf_create', 'status':'completed', 'output':produced.output}]})
    class Model:
        phase = 0
        observed = ''
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            names = {r['name'] for r in kw.get('tools', [])}
            if 'select_attachment_format' in names: return call('select_attachment_format', {'extension':None, 'confidence':1})
            if not names: return text('{}')
            self.phase += 1
            assert self.phase <= 3, 'A verified local file entered download/tool-scope recovery'
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['conversation_history'], 'tool_names':['chat_history_search'],
                    'artifact_extensions':['.pdf'], 'source_artifact_delivery_requested':True, 'source_artifact_request_evidence':prompt})
            if self.phase == 2:
                return call('code_mode', {'tool_names':['chat_history_search'],
                    'code':"call_tool('chat_history_search', {'query':'Cedar trip PDF','limit':1,'search_scope':'workspace','artifact_extensions':['.pdf']})"})
            observed = json.dumps(kw.get('messages', []), ensure_ascii=False)
            self.observed = observed
            assert 'native_artifact_receipts' in observed and str(target) in observed, observed[-6000:]
            return text('Cedar dates: 2031-04-09 to 2031-04-10. Attached the same PDF.\nMEDIA:'+str(target))
    model = Model(); registry = with_conversation_history_tool(ToolRegistry(filesystem_allowed_roots=(root,)),
        runtime=runtime, conversation_id=cid)
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        response = client.post('/api/chat', json={'conversation_id':cid, 'text':prompt})
        body = response.json(); assert response.status_code == 200 and len(body.get('artifacts', [])) == 1, (body, model.phase, model.observed[-5000:])
        assert '2031-04-09' in body['text'] and '2031-04-10' in body['text'], body
        assert client.get(body['artifacts'][0]['url']).content == original
    turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
    assert turn['response_fulfilled'] is True, turn
    assert {r['tool_name'] for r in turn['tool_results']} <= {'request_tool_scope', 'chat_history_search', 'code_mode'}
    assert model.phase == 3 and target.read_bytes() == original
