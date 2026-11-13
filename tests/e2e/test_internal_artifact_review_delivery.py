"""Internal requested files remain reviewable without becoming attachments."""
from tests.workflow_provider_protocol import workflow_provider_reply
import io,json,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container product boundary')

@pytest.mark.parametrize('language',['en','es','ja'])
@pytest.mark.parametrize('internal_kind',['deliverable','constraint','action'])
def test_internal_working_file_is_proven_but_only_final_is_delivered(tmp_path,monkeypatch,language,internal_kind):
    from fastapi.testclient import TestClient
    from docx import Document
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');cid='web:internal-evidence';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    working=root/'working.docx';final=root/'published.docx';writes=[]
    registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);writer=tools._build_document_create_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):result=writer(inv);writes.append(result);return result
    registry.register(tools.ToolSpec('document_create','Create document',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,30),write)
    class Provider:
        phase=0;errors=[];reviews=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def run(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Create internal working file with 10 units','requirement_kind':internal_kind,
                     'delivery_required':internal_kind!='constraint','evidence_subject_id':'working','eligible_evidence_tool_names':['document_create']},
                    {'requirement':'Deliver only approved final with 12 units','requirement_kind':'deliverable','evidence_subject_id':'final','eligible_evidence_tool_names':['document_create']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text'])
                assert [x['filename'] for x in data['prepared_delivery_receipts']]==['published.docx']
                assert [x['filename'] for x in data['internal_artifact_evidence']]==['working.docx']
                assert '10 units' in data['internal_artifact_evidence'][0]['content_preview']
                assert '12 units' in data['artifact_evidence'][0]['content_preview']
                coverage=[]
                for row in data['material_requirement_contract']:
                    name='working.docx' if row['evidence_subject_id']=='working' else 'published.docx'
                    records=[x for x in data['tool_evidence'] if x.get('output',{}).get('artifact_record_binding',{}).get('filename')==name]
                    assert len(records)==1
                    if name=='working.docx':assert records[0]['output']['artifact_record_binding']['delivery_role']=='internal_not_selected'
                    coverage.append({'requirement_id':row['requirement_id'],'satisfied':True,
                        'draft_covered':row['delivery_required'],'evidence_invocation_ids':[records[0]['invocation_id']]})
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'select_artifact_delivery' in names:
                packet=kw['messages'][0]['content'][0]['text'].split('Prepared file candidates: ',1)[1].split('\n',1)[1].split('\n')[0]
                candidates=json.loads(packet);ids=[x['candidate_id'] for x in candidates if x['filename']=='published.docx'];assert len(ids)==1
                return call('select_artifact_delivery',{'status':'selected','selected_ids':ids,'reason':'Requested final only; keep working copy internal.'})
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.docx','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['document_create'],'required_tool_names':['document_create'],'artifact_extensions':['.docx']})
            if self.phase==2:return {'stop_reason':'tool_use','content':[
                {'type':'tool_use','id':'working','name':'document_create','input':{'output_path':str(working),'title':'Working','paragraphs':['Inventory working copy: 10 units.']}},
                {'type':'tool_use','id':'final','name':'document_create','input':{'output_path':str(final),'title':'Approved','paragraphs':['Inventory approved copy: 12 units.']}}]}
            assert self.phase==3,'Review must not restart generation'
            return text('Created the internal working copy and prepared the approved document.' if internal_kind=='action' else 'The approved document is ready.')
    prompt={'en':'Create working.docx with 10 units, then published.docx with 12 units. Keep the working copy internal and attach only the approved document.','es':'Crea working.docx con 10 unidades y published.docx con 12 unidades. Conserva el borrador interno y adjunta solo el documento aprobado.','ja':'working.docx に10 units、published.docx に12 unitsを記載してください。作業版は内部に保持し、承認済み文書だけを添付してください。'}[language]
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});assert response.status_code==200,response.text
        data=response.json();assert not model.errors,model.errors
        assert len(data['artifacts'])==1 and data['artifacts'][0]['name']=='published.docx',data
        document=Document(io.BytesIO(client.get(data['artifacts'][0]['url']).content));content=' '.join(p.text for p in document.paragraphs)
        assert '12 units' in content and '10 units' not in content
        assert len(writes) == 2 and model.reviews == 0 and working.exists()
        turn=next(row for row in reversed(runtime.store.list_conversation_events(cid)) if row.get('event_type')=='conversation.chat_turn')
        assert turn.get('response_fulfilled') is True,turn
        history=client.get('/api/chat/history/'+cid,params={'turn_ids':json.dumps([turn['turn_id']])}).json()
        assert len(history['terminal_replies'])==1,history
        assert all(a['name']!='working.docx' for reply in history['terminal_replies'] for a in reply['artifacts'])
