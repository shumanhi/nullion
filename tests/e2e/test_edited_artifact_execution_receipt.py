"""A successful edit supersedes content without erasing the original action."""
from tests.workflow_provider_protocol import workflow_provider_reply
import io,json,os
from pathlib import Path
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real product boundary')

@pytest.mark.parametrize('language',['en','es','ja'])
def test_create_then_edit_retains_execution_without_reviving_old_contents(tmp_path,monkeypatch,language):
    from fastapi.testclient import TestClient
    from pypdf import PdfReader
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');cid='web:edited-output';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);path=root/'report.pdf';appendix=root/'appendix.pdf';created=[];edited=[]
    registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);creator=tools._build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path]);editor=tools._build_pdf_edit_handler(tmp_path,allowed_roots=[tmp_path])
    def create(inv):r=creator(inv);created.append(r);return r
    def edit(inv):r=editor(inv);edited.append(r);return r
    registry.register(tools.ToolSpec('file_read','Optional local notes',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.READ,False,10),tools._build_file_read_handler(tmp_path,allowed_roots=[tmp_path]))
    for name,fn in [('pdf_create',create),('pdf_edit',edit)]:registry.register(tools.ToolSpec(name,name,tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,30),fn)
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
            if system.startswith('You are a material-requirements planner'):return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Use pdf_create to create the report','requirement_kind':'action','evidence_subject_id':'creation','eligible_evidence_tool_names':['pdf_create']},{'requirement':'Deliver the final edited two-page PDF','requirement_kind':'deliverable','evidence_subject_id':'final','eligible_evidence_tool_names':['pdf_edit']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);coverage=[]
                for row in data['material_requirement_contract']:
                    if row['evidence_subject_id']=='creation':
                        # The reviewer receives request-local references, not
                        # the long runtime invocation IDs. Both completed create
                        # actions remain execution proof in original tool order;
                        # neither can revive its superseded file contents.
                        receipts=[item for item in data['tool_evidence'] if item['tool_name']=='pdf_create'
                            and item.get('output',{}).get('result_kind')=='superseded_producer_execution']
                        assert len(receipts)==2,data
                        assert len({item['invocation_id'] for item in receipts})==2
                        assert all(item['output']['execution_succeeded'] and not item['output'].get('artifact_path')
                                   for item in receipts)
                        receipts=receipts[:1]
                    else:
                        receipts=[item for item in data['tool_evidence'] if item.get('output',{}).get('artifact_record_binding',{}).get('filename')=='report.pdf']
                        assert len(receipts)==1 and receipts[0]['tool_name']=='pdf_edit',([(item['tool_name'],item['invocation_id'],item.get('output')) for item in data['tool_evidence']],edited[0].output,data['artifact_candidate_filenames'])
                    coverage.append({'requirement_id':row['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[receipts[0]['invocation_id']]})
                assert '53' in next(row for row in data['artifact_evidence'] if row['filename']=='report.pdf')['content_preview']
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'select_artifact_delivery' in names:
                packet=kw['messages'][0]['content'][0]['text'].split('Prepared file candidates: ',1)[1].split('\n',1)[1].split('\n')[0]
                candidates=json.loads(packet);ids=[item['candidate_id'] for item in candidates if item['filename']=='report.pdf'];assert len(ids)==1
                return call('select_artifact_delivery',{'status':'selected','selected_ids':ids,'reason':'Deliver only the edited final report; the appendix source is internal.'})
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.pdf','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['pdf_create','pdf_edit','file_read'],'required_tool_names':['pdf_create','pdf_edit'],'artifact_extensions':['.pdf']})
            if self.phase==2:return call('file_read',{'path':str(root/'optional-notes.txt')})
            if self.phase==3:return call('pdf_create',{'output_path':str(path),'text_pages':['Initial report 37'],'expected_page_count':1})
            if self.phase==4:return call('pdf_create',{'output_path':str(appendix),'text_pages':['Approved appendix 53'],'expected_page_count':1})
            if self.phase==5:return call('pdf_edit',{'input_path':str(path),'output_path':str(path),'append_pdf_paths':[str(appendix)]})
            assert self.phase==6,'Review incorrectly restarted an already completed create step'
            return text('The edited two-page report is ready.\nMEDIA:'+str(path))
    prompts={'en':'Use pdf_create to create a report, then edit it to append the approved appendix. Deliver only the final two-page PDF.','es':'Crea el informe con pdf_create y edítalo para añadir el apéndice aprobado. Entrega solo el PDF final de dos páginas.','ja':'pdf_createでレポートを作成してから編集し、承認済み付録を追加してください。最終の2ページPDFだけを渡してください。'}
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompts[language]+{'en':' Optional local notes are not required; use supplied content if absent.','es':' Las notas locales son opcionales; si faltan, usa el contenido proporcionado.','ja':'ローカルのメモは任意です。存在しなければ提示された内容を使ってください。'}[language],'conversation_id':cid});assert response.status_code==200,response.text
        data=response.json();assert not model.errors,model.errors
        assert len(data['artifacts'])==1,data
        document=PdfReader(io.BytesIO(client.get(data['artifacts'][0]['url']).content))
        assert len(document.pages)==2 and '37' in document.pages[0].extract_text() and '53' in document.pages[1].extract_text()
        assert len(created)==2 and all(r.status=='completed' for r in created) and len(edited)==1 and model.reviews==1
