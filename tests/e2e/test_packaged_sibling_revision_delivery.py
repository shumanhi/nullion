"""A repair after packaging retains a requested sibling, never its stale proof."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os,zipfile
from pathlib import Path
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker native API/store/delivery boundary')

@pytest.mark.parametrize('language',['en','es','ja'])
@pytest.mark.parametrize('revision',[False],ids=['read-only-check'])
def test_packaged_member_exclusion_does_not_hide_revised_standalone_sibling(tmp_path,monkeypatch,language,revision):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');cid='web:packaged-revision';root=artifact_root_for_principal(cid);package=root/'linked';package.mkdir(parents=True,exist_ok=True)
    image=package/'image.png';Image.new('RGB',(32,32),'green').save(image);linked=package/'linked.html';standalone=root/'standalone.html';bundle=root/'page.zip'
    registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);writes=[];archives=[]
    writer=tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path]);archiver=tools._build_archive_create_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):r=writer(inv);writes.append(r);return r
    def archive(inv):r=archiver(inv);archives.append(r);return r
    for name,effect,handler in [('file_write',tools.ToolSideEffectClass.WRITE,write),('file_read',tools.ToolSideEffectClass.READ,tools._build_file_read_handler(tmp_path,allowed_roots=[tmp_path])),('archive_create',tools.ToolSideEffectClass.WRITE,archive)]:registry.register(tools.ToolSpec(name,name,tools.ToolRiskLevel.LOW,effect,False,10),handler)
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
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase)+str(self.selections),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):return text(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.html'],'material_requirements':[{'requirement':'Deliver portable linked HTML with its image','requirement_kind':'deliverable','evidence_subject_id':'linked','eligible_evidence_tool_names':['file_write']},{'requirement':'Deliver separate self-contained HTML with value 38','requirement_kind':'deliverable','evidence_subject_id':'standalone','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);coverage=[]
                for row in data['material_requirement_contract']:
                    name='page.zip' if row['evidence_subject_id']=='linked' else 'standalone.html'
                    ids=[r['invocation_id'] for r in data['tool_evidence'] if r.get('output',{}).get('artifact_record_binding',{}).get('filename')==name and r['output']['artifact_record_binding'].get('delivery_role')!='internal_not_selected']
                    assert len(ids)==1,(name,data)
                    satisfied=row['evidence_subject_id']=='linked' or self.reviews==2
                    coverage.append({'requirement_id':row['requirement_id'],'satisfied':satisfied,'draft_covered':True,'evidence_invocation_ids':ids})
                if self.reviews==2:
                    native=next(r for r in data['artifact_evidence'] if r['filename']=='standalone.html')
                    assert '38' in native['content_preview'] and '37' not in native['content_preview']
                return text(json.dumps({'disposition':'complete' if self.reviews==2 else 'retry','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[] if self.reviews==2 else ['Correct the separate self-contained report value to 38.' if revision else 'Read back the standalone report to confirm its current value.'],'retry_tool_names':[] if self.reviews==2 else ['file_write' if revision else 'file_read']}))
            if 'select_artifact_delivery' in names:
                self.selections+=1;packet=kw['messages'][0]['content'][0]['text'].split('Prepared file candidates: ',1)[1].split('\n',1)[1].split('\n')[0];rows=json.loads(packet)
                expected={'linked.html','standalone.html'} if self.selections==1 else {'page.zip','standalone.html'}
                ids=[r['candidate_id'] for r in rows if r['filename'] in expected];assert len(ids)==2,rows
                return call('select_artifact_delivery',{'status':'needs_packaging' if self.selections==1 else 'selected','selected_ids':ids,'reason':'Retain the separate requested self-contained edition with the portable linked bundle.'})
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],'required_tool_names':['file_write'],'artifact_extensions':['.html']})
            if self.phase==2:return call('file_write',{'path':str(linked),'content':'<html><body><h1>Linked</h1><img src="image.png"></body></html>','inline_local_html_images':False})
            if self.phase==3:return call('file_write',{'path':str(standalone),'content':'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Independent report</title></head><body><h1>Separate value '+('37' if revision else '38')+'</h1><p>This self-contained edition is a distinct requested deliverable. Its current value is recorded in the heading above.</p></body></html>'})
            if self.phase==4:return text('The two reports are prepared.')
            if self.phase==5:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['archive_create'],'required_tool_names':['archive_create'],'artifact_extensions':['.zip'],'excluded_artifact_extensions':['.html']})
            if self.phase==6:return call('archive_create',{'output_path':str(bundle),'source_paths':[str(package)]})
            if self.phase==7:return text('The two reports are prepared.')
            if self.phase==8:
                if not revision:return call('file_read',{'path':str(standalone)})
                return call('file_write',{'path':str(standalone),'content':'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Independent report</title></head><body><h1>Separate value 38</h1><p>This self-contained edition is a distinct requested deliverable. Its current value is recorded in the heading above.</p></body></html>'})
            assert self.phase==9,'The sibling repair restarted unrelated generation'
            return text('The final reports are ready.'+('' if revision else '\nMEDIA:'+str(bundle)))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        prompt={'en':'Deliver a portable bundle of linked HTML with its image, and a separate self-contained HTML with value 38.','es':'Entrega un paquete portátil de HTML enlazado con su imagen y un HTML autónomo separado con el valor 38.','ja':'画像付きリンク型HTMLの持ち運べるパッケージと、値38を含む独立した自己完結型HTMLを渡してください。'}[language]
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});data=response.json();assert not model.errors,model.errors
        assert response.status_code==200 and {a['name'] for a in data['artifacts']}=={'page.zip','standalone.html'},data
        for artifact in data['artifacts']:
            downloaded=client.get(artifact['url']);assert downloaded.status_code==200
            if artifact['name']=='standalone.html':assert b'38' in downloaded.content and b'37' not in downloaded.content
            else:
                import io
                with zipfile.ZipFile(io.BytesIO(downloaded.content)) as archive:assert archive.read('image.png')==image.read_bytes()
        assert model.reviews == 0 and model.selections == (3 if revision else 2) and (len(writes) == (3 if revision else 2)) and (len(archives) == 1)
