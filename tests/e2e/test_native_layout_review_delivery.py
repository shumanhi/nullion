"""Missing optional rendering software cannot replace actual native file facts."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real API/store/native file downloads')

@pytest.mark.parametrize('language',['en','es','ja'])
@pytest.mark.parametrize('multipage',[False,True],ids=['text','multipage-table'])
def test_native_layout_evidence_survives_optional_renderer_failure(tmp_path,monkeypatch,language,multipage):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,tools
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);cid='web:native-layout';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    creators={'document_create':('.docx',tools._build_document_create_handler(tmp_path,allowed_roots=[tmp_path])), 'pdf_create':('.pdf',tools._build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path])), 'presentation_create':('.pptx',tools._build_presentation_create_handler(tmp_path,allowed_roots=[tmp_path]))}
    created=[];commands=[]
    for name,(_,handler) in creators.items():
        def create(inv,fn=handler):r=fn(inv);created.append(r);return r
        registry.register(tools.ToolSpec(name,name,tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,20),create)
    executor=tools._build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path])
    def execute(inv):r=executor(inv);commands.append(r);return r
    registry.register(tools.ToolSpec('terminal_exec','Run optional local inspection',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.DANGEROUS_EXEC,False,10),execute)
    class Provider:
        phase=0;reviews=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except Exception as e:self.errors.append(repr(e));raise
        def run(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Deliver the requested formatted '+ext,'requirement_kind':'deliverable','evidence_subject_id':ext,'eligible_evidence_tool_names':[name]} for name,(ext,_) in creators.items()]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);coverage=[]
                for name,(ext,_) in creators.items():
                    artifact=next(row for row in data['artifact_evidence'] if row['filename'].endswith(ext));structure=artifact['structure']
                    assert structure['layout_evidence_kind']=='native_format_structure' and structure['rendered_visual_inspection'] is False
                    assert structure['layout_inspection_complete'] is True
                    if ext=='.pptx':
                        assert structure['slide_layouts'][0]['shapes_outside_slide']==0
                        assert 28 in structure['slide_layouts'][0]['explicit_font_sizes_pt']
                    elif ext=='.pdf':
                        assert all(page['text_origins_outside_page']==0 for page in structure['page_layouts']),structure
                        assert structure['page_count']==(2 if multipage else 1)
                    else:assert structure['sections'][0]['page_width_twips']>0
                    ids=[r['invocation_id'] for r in data['tool_evidence'] if r.get('output',{}).get('artifact_record_binding',{}).get('extension')==ext]
                    row=next(r for r in data['material_requirement_contract'] if r['evidence_subject_id']==ext)
                    coverage.append({'requirement_id':row['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids})
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'select_artifact_delivery' in names:
                packet=kw['messages'][0]['content'][0]['text'].split('Prepared file candidates: ',1)[1].split('\n',1)[1].split('\n')[0]
                candidates=json.loads(packet)
                return call('select_artifact_delivery',{'status':'selected','selected_ids':[row['candidate_id'] for row in candidates],'reason':'Deliver the three requested current files.'})
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.docx','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files','local_shell'],'tool_names':[*creators,'terminal_exec'],'required_tool_names':list(creators),'artifact_extensions':['.docx','.pdf','.pptx']})
            if self.phase in [2,3,4]:
                name=list(creators)[self.phase-2];ext=creators[name][0];args={'output_path':str(root/('report'+ext)),'title':'Current value 37'}
                args.update({'paragraphs':['Current value 37.']} if ext=='.docx' else {'text_pages':['Current value 37.']} if ext=='.pdf' else {'slides':[{'title':'Current value 37','bullets':['Current value 37.']}]})
                if ext=='.pdf' and multipage:
                    table='<table><thead><tr><th>Record</th><th>Current value</th></tr></thead><tbody>'+''.join('<tr><td>Record '+str(i)+'</td><td>37</td></tr>' for i in range(15))+'</tbody></table>'
                    args.pop('text_pages');args['html_pages']=['<h1>Current value 37</h1>'+table,'<h1>Second page</h1>'+table]
                return call(name,args)
            if self.phase==5:return call('terminal_exec',{'command':'python -c '+__import__('shlex').quote('print("NO_LIBREOFFICE");raise SystemExit(2)'),'timeout_seconds':5,'network_mode':'deny'})
            assert self.phase==6,'Optional renderer failure restarted generation'
            return text('The three formatted files are ready. No rendered visual inspection was performed.')
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        prompt={'en':'Create a polished DOCX, PDF, and PPTX with readable headings and the value 37. Attach only those three files.','es':'Crea un DOCX, PDF y PPTX con encabezados legibles y el valor 37. Adjunta solo esos tres archivos.','ja':'読みやすい見出しと値37を含むDOCX、PDF、PPTXを作成して、その3ファイルだけを添付してください。'}[language]
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});data=response.json();assert not model.errors,model.errors
        assert response.status_code==200 and {row['name'] for row in data['artifacts']}=={'report.docx','report.pdf','report.pptx'},data
        for artifact in data['artifacts']:assert client.get(artifact['url']).content==(root/artifact['name']).read_bytes()
        assert len(created) == 3 and all((row.status == 'completed' for row in created)) and (len(commands) == 1) and (commands[0].status == 'failed') and (model.reviews == 0)
