"""A focused content repair can still package its selected final linked page."""
from tests.workflow_provider_protocol import workflow_provider_reply
import io,json,os,zipfile,shlex
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')

@pytest.mark.parametrize('edit_tool',[None,'file_patch','terminal_exec'])
@pytest.mark.parametrize('language',['en','es','ja'])
def test_review_repair_can_change_to_packaging_and_deliver_once(tmp_path,monkeypatch,language,edit_tool):
    patch_before_packaging=edit_tool is not None
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');cid='web:repair-packaging';root=artifact_root_for_principal(cid);package=root/'linked';package.mkdir(parents=True,exist_ok=True)
    image=package/'image.png';Image.new('RGB',(100,100),'blue').save(image)
    linked=package/'linked.html';standalone=root/'standalone.html';bundle=root/'page.zip';writes=[];archives=[];patches=[]
    content='<html><body><h1>Linked report</h1><img src="image.png"><p>Verified value 37.</p></body></html>'
    separate='<html><body><h1>Independent self-contained report 37</h1></body></html>'
    registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);writer=tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path]);archiver=tools._build_archive_create_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):r=writer(inv);writes.append(r);return r
    def archive(inv):r=archiver(inv);archives.append(r);return r
    patcher=tools._build_file_patch_handler(tmp_path,allowed_roots=[tmp_path])
    def patch(inv):r=patcher(inv);patches.append(r);return r
    executor=tools._build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path])
    def execute(inv):r=executor(inv);patches.append(r);return r
    for name,effect,fn in [('terminal_exec',tools.ToolSideEffectClass.DANGEROUS_EXEC,execute),('file_patch',tools.ToolSideEffectClass.WRITE,patch),('file_write',tools.ToolSideEffectClass.WRITE,write),('file_read',tools.ToolSideEffectClass.READ,tools._build_file_read_handler(tmp_path,allowed_roots=[tmp_path])),('archive_create',tools.ToolSideEffectClass.WRITE,archive)]:registry.register(tools.ToolSpec(name,name,tools.ToolRiskLevel.LOW,effect,False,20),fn)
    class Provider:
        phase=0;selections=0;reviews=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def run(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Deliver portable linked report including its image','requirement_kind':'deliverable','evidence_subject_id':'linked','eligible_evidence_tool_names':['file_write']},{'requirement':'Deliver the separate standalone HTML','requirement_kind':'deliverable','evidence_subject_id':'standalone','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);coverage=[]
                for row in data['material_requirement_contract']:
                    name='standalone.html' if row['evidence_subject_id']=='standalone' else ('linked.html' if self.reviews==1 else 'page.zip')
                    ids=[item['invocation_id'] for item in data['tool_evidence'] if item.get('output',{}).get('artifact_record_binding',{}).get('filename')==name and item.get('output',{}).get('artifact_record_binding',{}).get('delivery_role')!='internal_not_selected']
                    assert len(ids)==1,data
                    satisfied=self.reviews>1 or row['evidence_subject_id']=='standalone'
                    coverage.append({'requirement_id':row['requirement_id'],'satisfied':satisfied,'draft_covered':True,'evidence_invocation_ids':ids})
                if self.reviews==1:return text(json.dumps({'disposition':'retry','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':['Repair linked report to reference the existing image.'],'retry_tool_names':['file_write','file_read']}))
                package_evidence=next(row for row in data['artifact_evidence'] if row['filename']=='page.zip')
                assert package_evidence['structure']['portable_html_documents'][0]['portable'] is True
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'select_artifact_delivery' in names:
                self.selections+=1;packet=kw['messages'][0]['content'][0]['text'].split('Prepared file candidates: ',1)[1].split('\n',1)[1].split('\n')[0];candidates=json.loads(packet)
                wanted={'linked.html','standalone.html'} if self.selections<3 else {'page.zip','standalone.html'}
                ids=[r['candidate_id'] for r in candidates if r['filename'] in wanted]
                assert len(ids)==2,candidates
                return call('select_artifact_delivery',{'status':'needs_packaging' if self.selections==2 else 'selected','selected_ids':ids,'reason':'Package the repaired linked report; preserve the standalone sibling.'})
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write','file_read'],'required_tool_names':['file_write'],'artifact_extensions':['.html']})
            if self.phase==2:return call('file_write',{'path':str(linked),'content':'<html><body><h1>Linked report 37</h1></body></html>','inline_local_html_images':False})
            if self.phase==3:return call('file_write',{'path':str(standalone),'content':separate})
            if self.phase==4:return text('The two reports are ready.')
            if self.phase==5:
                assert 'file_write' in names,names
                return call('file_write',{'path':str(linked),'content':content,'inline_local_html_images':False})
            if self.phase==6:return text('The report image is repaired.')
            if self.phase==7:
                assert 'request_tool_scope' in names,'Content-review focus hid the packaging transition'
                return call('request_tool_scope',{'capabilities':['local_files','local_shell'] if edit_tool=='terminal_exec' else ['local_files'],'tool_names':[edit_tool,'archive_create'] if patch_before_packaging else ['archive_create'],'required_tool_names':[edit_tool,'archive_create'] if patch_before_packaging else ['archive_create'],'artifact_extensions':['.zip','.html']})
            if self.phase==8 and patch_before_packaging:
                assert edit_tool in names,'Packaging focus hid the explicitly required edit tool'
                if edit_tool=='terminal_exec':
                    script='from pathlib import Path;p=Path('+repr(str(linked))+');p.write_text(p.read_text().replace("Verified value 37.","Verified value 38."))'
                    return call(edit_tool,{'command':'python -c '+shlex.quote(script),'timeout_seconds':10,'network_mode':'deny'})
                return call(edit_tool,{'path':str(linked),'old_string':'Verified value 37.','new_string':'Verified value 38.'})
            if self.phase==(9 if patch_before_packaging else 8):return call('archive_create',{'output_path':str(bundle),'source_paths':[str(package)]})
            assert self.phase==(10 if patch_before_packaging else 9),'Repair or packaging restarted unrelated generation'
            return text('The final portable reports are ready.')
    prompt={'en':'Deliver two reports: one standalone HTML and one linked HTML with its image as a portable bundle.','es':'Entrega dos informes: un HTML autónomo y un HTML enlazado con su imagen en un paquete portátil.','ja':'独立したHTMLと、画像を含むリンク型HTMLの持ち運べるパッケージの2件を渡してください。'}[language]
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});assert response.status_code==200,response.text
        data=response.json();assert not model.errors,model.errors
        assert {r['name'] for r in data['artifacts']}=={'page.zip','standalone.html'},data
        assert client.get(next(r['url'] for r in data['artifacts'] if r['name']=='standalone.html')).content==separate.encode()
        body=client.get(next(r['url'] for r in data['artifacts'] if r['name']=='page.zip')).content
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            assert set(archive.namelist())=={'linked.html','image.png'}
            assert archive.read('linked.html').decode()==(content.replace('Verified value 37.','Verified value 38.') if patch_before_packaging else content) and archive.read('image.png')==image.read_bytes()
        assert len(writes)==3 and len(archives)==1 and model.selections==3 and model.reviews==2

        assert len(patches)==int(patch_before_packaging) and all(row.status=='completed' for row in patches)
