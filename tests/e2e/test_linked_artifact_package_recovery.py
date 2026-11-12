"""A typed selection can return a linked page to packaging without starting over."""
from tests.workflow_provider_protocol import workflow_provider_reply
import io,json,os,zipfile
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary E2E')

@pytest.mark.parametrize('standalone_extension',['.html','.htm'])
@pytest.mark.parametrize('standalone_sibling',[False,True])
@pytest.mark.parametrize('recovery_bound',[False,True])
@pytest.mark.parametrize('mixed_formats',[False,True])
@pytest.mark.parametrize('prompt',['Deliver the web page with its image as one portable bundle.','Entrega la página y su imagen en un paquete portátil.','画像付きページを持ち運べる一つのファイルにまとめてください。'])
def test_linked_page_and_image_are_packaged_then_delivered_once(tmp_path,monkeypatch,prompt,mixed_formats,recovery_bound,standalone_sibling,standalone_extension):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler,_build_archive_create_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');root=artifact_root_for_principal('web:portable-page');package=root/'page-assets';package.mkdir(parents=True,exist_ok=True)
    image=package/'image.png';Image.new('RGB',(100,100),'blue').save(image)
    entry=package/'page.html';draft=root/('standalone'+standalone_extension if standalone_sibling else 'working.html');output=root/'page.zip';csv=root/'figures.csv';writes=[];archives=[]
    declared_format=standalone_extension if standalone_sibling else '.html'
    standalone='<html><body><h1>Standalone self-contained page</h1><svg width=100 height=100><rect width=100 height=100 fill=blue /></svg></body></html>'
    html='<!doctype html><html><body><h1>Portable page</h1><img src="image.png" alt="Blue reference image"><p>Complete page.</p></body></html>'
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);writer=_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]);archiver=_build_archive_create_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):r=writer(inv);writes.append(r);return r
    def archive(inv):r=archiver(inv);archives.append(r);return r
    registry.register(ToolSpec('file_write','Write page',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),write)
    registry.register(ToolSpec('archive_create','Package exact requested files',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),archive)
    class Provider:
        phase=0;selections=0;reviews=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:
                return self._create(**kw)
            except Exception as exc:
                self.errors.append(repr(exc))
                raise
        def _create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(v):return {'stop_reason':'end_turn','content':[{'type':'text','text':v}]}
            def tool(n,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                assert kw['timeout']>=30
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Deliver one portable archive with page and referenced image','requirement_kind':'deliverable','evidence_subject_id':'package','eligible_evidence_tool_names':['archive_create']},*([{'requirement':'Deliver the separate self-contained page unchanged','requirement_kind':'deliverable','evidence_subject_id':'standalone','eligible_evidence_tool_names':['file_write']}] if standalone_sibling else []),*([{'requirement':'Deliver the existing CSV unchanged','requirement_kind':'deliverable','evidence_subject_id':'csv','eligible_evidence_tool_names':['file_write']}] if mixed_formats else [])]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);rows=data['material_requirement_contract']
                assert data['delivery_packaging_context']['kind']=='runtime_selected_linked_html_packaging'
                bundle=next(row for row in data['artifact_evidence'] if row['filename']=='page.zip')
                assert bundle['structure']['inspection_complete']
                assert bundle['structure']['portable_html_documents'][0]['portable']
                assert set(bundle['structure']['archive_member_names'])=={'page.html','image.png'}
                assert set(data['artifact_candidate_filenames'])==({'page.zip',*(['figures.csv'] if mixed_formats else []),*([draft.name] if standalone_sibling else [])})
                coverage=[]
                for r in rows:
                    filename={'csv':'figures.csv','standalone':draft.name}.get(r['evidence_subject_id'],'page.zip')
                    ids=[item['invocation_id'] for item in data['tool_evidence'] if item.get('output',{}).get('artifact_record_binding',{}).get('filename')==filename]
                    assert len(ids)==1,data
                    coverage.append({**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids})
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'select_artifact_delivery' in names:
                self.selections+=1
                assert 'needs_packaging' in kw['tools'][0]['input_schema']['properties']['status']['enum']
                if self.selections==1:
                    # Deliberately omit the ready standalone sibling. The final
                    # packaging selection must recover it without a new write.
                    return tool('select_artifact_delivery',{'status':'needs_packaging','selected_ids':['a0',*(['a2'] if mixed_formats else [])],'reason':'The page needs its referenced local image in the final portable package.'})
                assert self.selections==2
                return tool('select_artifact_delivery',{'status':'selected','selected_ids':['a'+str(2+int(mixed_formats)),*(['a2'] if mixed_formats else []),*(['a1'] if standalone_sibling else [])],'reason':'The portable bundle and every separately requested completed sibling are final.'})
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.zip','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],'artifact_extensions':[declared_format,*(['.csv'] if mixed_formats else [])]})
            if self.phase==2:return tool('file_write',{'path':str(entry),'content':html,'inline_local_html_images':False})
            if self.phase==3:return tool('file_write',{'path':str(draft),'content':standalone if standalone_sibling else '<html><body><p>Working copy only.</p></body></html>'})
            offset=int(mixed_formats)
            if mixed_formats and self.phase==4:return tool('file_write',{'path':str(csv),'content':'Name,Value\nVerified,37\n'})
            if self.phase==4+offset:return text('Prepared the page candidates.')
            if self.phase==5+offset:
                blocks=[b for m in kw['messages'] for b in m.get('content',[]) if isinstance(b,dict)]
                assert any(b.get('type')=='tool_result' and b.get('tool_use_id')=='select_artifact_delivery'+str(4+offset) for b in blocks)
                return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['archive_create'],'required_tool_names':['archive_create'],'artifact_extensions':['.zip',*(['.csv'] if mixed_formats else [])],'excluded_artifact_extensions':['.html']})
            if self.phase==6+offset:return tool('archive_create',{'output_path':str(output),'source_paths':[str(package)]})
            assert self.phase==7+offset,'Completed page or image was unnecessarily regenerated'
            return text('The portable page is attached.\nMEDIA:'+str(output)+('\nMEDIA:'+str(csv) if mixed_formats else ''))
    if recovery_bound:
        from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision
        from nullion.recovery_contract import RequestRecoveryContract
        initial=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(requested_extensions=(declared_format,*(['.csv'] if mixed_formats else [])),current_user_message=prompt),
            tool_scope_decision=TurnToolScopeDecision(valid=True,requested_tool_names=('file_write',),requested_artifact_extensions=(declared_format,*(['.csv'] if mixed_formats else []))))
        registry=RequestRecoveryContract.from_registry(initial).bind(registry)
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:portable-page'});assert response.status_code==200,response.text;data=response.json()
        assert not model.errors,model.errors
        assert {item['path'] for item in data['artifacts']}=={str(output),*([str(csv)] if mixed_formats else []),*([str(draft)] if standalone_sibling else [])},data
        with zipfile.ZipFile(io.BytesIO(client.get(next(item['url'] for item in data['artifacts'] if item['path']==str(output))).content)) as z:
            assert set(z.namelist())=={'page.html','image.png'}
            assert z.read('page.html').decode()==html and z.read('image.png')==image.read_bytes()
        assert len(writes)==2+int(mixed_formats) and len(archives)==1 and model.selections==2 and model.reviews==1
        if standalone_sibling:
            assert client.get(next(row['url'] for row in data['artifacts'] if row['path']==str(draft))).content==standalone.encode()
        linked=writes[0].output
        assert linked['delivery_packaging']['dependencies_complete'] is True
        assert linked['linked_image_assets'][0]['path']==str(image)
        history=client.get('/api/chat/history/web:portable-page').json()
        assert 'working.html' not in json.dumps(history.get('terminal_replies'))
