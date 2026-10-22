"""Real artifact tools, graph, FastAPI/SQLite/history, and attachment bytes."""
import asyncio
import io
import json
import os
import time
from datetime import datetime, UTC
from pathlib import Path
from types import SimpleNamespace
import pytest
from test_scheduled_report_completion import setup_runtime, text_response, tool_response
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('source_required',[True,False])
@pytest.mark.parametrize('family',['pdf','document','spreadsheet','presentation','archive','html'])
@pytest.mark.parametrize('prompt',['Create one final document.', '最終文書を一つ作成。', 'Crea un documento final.'])
@pytest.mark.parametrize('mode',['selected','two_outputs','review_unavailable','ambiguous','selection_repair','blocked'])
def test_only_selected_files_cross_delivery_and_history_boundaries(tmp_path,monkeypatch,prompt,mode,family,source_required):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass, _build_pdf_create_handler,register_web_fetch_tool, _build_document_create_handler, _build_spreadsheet_create_handler, _build_presentation_create_handler, _build_archive_create_handler, _build_file_write_handler, _build_file_read_handler
    from nullion.policy import BoundaryKind,GLOBAL_PERMISSION_PRINCIPAL,BoundaryPolicyRule,PolicyMode
    from nullion.telegram_app import _deliver_reply
    from nullion.messaging_adapters import DeliveryContract
    from pypdf import PdfReader
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED','1');monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    runtime=setup_runtime(tmp_path,monkeypatch);chat_store._store=None
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='network',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));writes=[]
    from nullion.artifacts import artifact_root_for_principal
    inputs=artifact_root_for_principal('web:revision-case')/'source-record.txt';inputs.parent.mkdir(parents=True,exist_ok=True);inputs.write_text('Verified source record: 42.')
    tool,extension,builder,arguments={
        'pdf':('pdf_create','.pdf',_build_pdf_create_handler,{'text_pages':['Source record: 42. Complete document.'],'expected_page_count':1}),
        'document':('document_create','.docx',_build_document_create_handler,{'title':'Record','paragraphs':['Verified source record: 42.']}),
        'spreadsheet':('spreadsheet_create','.xlsx',_build_spreadsheet_create_handler,{'columns':['Record'],'rows':[[42]]}),
        'presentation':('presentation_create','.pptx',_build_presentation_create_handler,{'title':'Record','slides':[{'title':'Verified record','bullets':['42']}]}),
        'archive':('archive_create','.zip',_build_archive_create_handler,{'source_paths':[str(inputs)]}),
        'html':('file_write','.html',_build_file_write_handler,{'content':'<!doctype html><html><body><h1>Verified record</h1><p>Source record: 42. This complete document preserves the checked source information for the requested output.</p></body></html>'}),
    }[family]
    handler=builder(allowed_roots=[tmp_path],include_principal_workspace=False)
    def produce(call):
        result=handler(call);writes.append(result);return result
    registry.register(ToolSpec(tool,'Create the requested artifact',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,60),produce)
    registry.register(ToolSpec('file_read','Inspect the generated artifact',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,20),_build_file_read_handler(allowed_roots=[tmp_path],include_principal_workspace=False))
    from nullion import tools as tool_module
    fetch_source=lambda url,timeout:{'url':url,'status_code':200,'content_type':'text/html','text':'Verified source record: 42.'}
    monkeypatch.setattr(tool_module,'_default_web_fetcher',fetch_source)
    register_web_fetch_tool(registry,web_fetcher=fetch_source)
    from nullion.artifacts import artifact_root_for_principal
    paths=[str(artifact_root_for_principal('web:revision-case')/n) for n in ['candidate-a'+extension,'candidate-b'+extension]]
    first_producer_step=3 if source_required else 2
    final_step=first_producer_step+3
    class Model:
        steps=0;reviews=0;trace=[];plain_mode=False;plain_calls=0
        def create(self,**kw):
            system=kw.get('system','');names={t['name'] for t in kw.get('tools',[])}
            self.trace.append((self.steps,system[:100],sorted(names),str(kw.get('messages',[])[-1:])[-2500:]))
            if self.plain_mode:
                self.plain_calls+=1
                assert not system.startswith(('You are a material-requirements planner','You are a semantic completion reviewer'))
                return text_response('OK')
            if system.startswith('You are a material-requirements planner'):
                return text_response(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver the requested source document','requirement_kind':'deliverable','evidence_subject_id':'document','eligible_evidence_tool_names':[tool]}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1
                if mode=='review_unavailable':return text_response('{invalid')
                if mode=='blocked':return text_response(json.dumps({'disposition':'blocked','unresolved_requirements':['Required source verification failed.']}))
                payload=json.loads(kw['messages'][0]['content'][0]['text']);rows=payload['material_requirement_contract']
                # The final reviewer must never inspect superseded candidates.
                assert payload['artifact_candidate_filenames']==(['candidate-a'+extension,'candidate-b'+extension] if mode=='two_outputs' else ['candidate-b'+extension])
                ids=[r.invocation_id for r in (writes if mode=="two_outputs" else writes[-1:])]
                return text_response(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,
                    'material_requirements':[{**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids} for r in rows]}))
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':None,'confidence':1})
            if not names and self.steps<final_step:return text_response('{}')
            self.steps+=1
            if self.steps==1:return tool_response('request_tool_scope',{'capabilities':['web','local_files'] if source_required else ['local_files'],'tool_names':['web_fetch',tool,'file_read'] if source_required else [tool,'file_read'],'required_tool_names':['web_fetch',tool,'file_read'] if source_required else [tool,'file_read'],'artifact_extensions':[extension]})
            if source_required and self.steps==2:return tool_response('web_fetch',{'url':'https://example.com/source'})
            if self.steps in (first_producer_step,first_producer_step+1):return tool_response(tool,{('path' if tool=='file_write' else 'output_path'):paths[self.steps-first_producer_step],**arguments})
            if self.steps==first_producer_step+2:return tool_response('file_read',{'path':paths[-1]})
            assert self.steps<=final_step+1,'Whole artifact production restarted'
            if mode=='ambiguous' or (mode=='selection_repair' and self.steps==final_step):return text_response('Created files.')
            if mode=='selection_repair' and self.steps==final_step+1:assert not names
            selected=paths if mode=='two_outputs' else paths[1:]
            return text_response('Created the requested document.\n'+'\n'.join('MEDIA:'+p for p in selected))
    model=Model();runtime.model_client=model
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)
    with TestClient(app) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:revision-case'})
        assert response.status_code==200,response.text
        payload=response.json();expected=[] if mode=='ambiguous' or (mode=='blocked' and model.reviews) else paths if mode=='two_outputs' else paths[1:]
        actual=[a['path'] for a in payload['artifacts']]
        if actual != expected or len(writes)!=2:
            print(json.dumps({'trace':model.trace,'writes':[r.output for r in writes],'payload':payload},indent=2,default=str))
        assert actual==expected,(payload,model.trace,[(r.status,r.error) for r in writes])
        assert len(writes)==2 and all(r.status=='completed' for r in writes),writes
        if mode=='review_unavailable' and model.reviews:assert 'final content review could not be completed' in payload['text'],payload
        assert 'completion remains unverified' not in payload['text']
        if mode=='ambiguous':assert "couldn't determine which versions" in payload['text'],payload
        if mode=='blocked' and model.reviews:assert 'Required source verification failed' in payload['text'],payload
        events=runtime.store.list_conversation_events('web:revision-case');turns=[e for e in events if e.get('event_type')=='conversation.chat_turn'];assert turns,(payload,model.trace,[(r.status,r.error) for r in writes]);turn=turns[-1]
        assert turn.get('artifact_paths',[])==expected
        history=client.get('/api/chat/history/web:revision-case',params={'turn_ids':json.dumps([turn['turn_id']])}).json()
        assert [a['path'] for a in history['terminal_replies'][0].get('artifacts',[])]==expected,history
        from nullion.artifacts import split_media_reply_attachments
        stored_caption,_=split_media_reply_attachments(turn['assistant_reply'],is_safe_attachment_path=lambda path:path.is_file())
        assert history['terminal_replies'][0]['text']==stored_caption
        if mode=='review_unavailable' and model.reviews:assert 'final content review could not be completed' in history['terminal_replies'][0]['text']
        from nullion.artifact_validation import validate_artifact_paths
        assert validate_artifact_paths(actual).ok
        if family=='pdf':
            for path in actual:assert len(PdfReader(path).pages)==1
        if family=='pdf' and mode=='selected':
            model.plain_mode=True;latencies=[]
            for index, plain in enumerate(['Reply OK.','OK と返答。','Responde OK.']):
                started=time.perf_counter()
                plain_response=client.post('/api/chat',json={'text':plain,'conversation_id':f'web:plain-{index}'})
                latencies.append(time.perf_counter()-started)
                assert plain_response.json()['text']=='OK' and not plain_response.json()['artifacts']
            assert sum(latencies)/len(latencies)<1.5 and max(latencies)<3,latencies
            assert model.plain_calls==3 and len(writes)==2
    # Real transport serialization/upload boundary; only the external Telegram API is replaced.
    uploads=[]
    class Message:
        chat=SimpleNamespace(id=31337);message_id=15
        async def reply_text(self,text,**kw):return SimpleNamespace(message_id=16)
        async def reply_document(self,document,**kw):
            uploads.append(document.read());return SimpleNamespace(message_id=17)
    asyncio.run(_deliver_reply(Message(),payload['text'],artifact_paths=actual,principal_id='telegram:31337',delivery_contract=DeliveryContract.attachment_required(source='artifact_manifest',required_attachment_extensions=(extension,)) if actual else DeliveryContract.message_only()))
    assert len(uploads)==len(expected)
    assert uploads==[Path(p).read_bytes() for p in expected]
    if family=='pdf':assert all(len(PdfReader(io.BytesIO(data)).pages)==1 for data in uploads)


@pytest.mark.parametrize('scratch',[True,False])
@pytest.mark.parametrize('producer',['image_generate','terminal_exec'])
@pytest.mark.parametrize('both',[False,True])
@pytest.mark.parametrize('prompt',['Create the requested images.','要求した画像を作成。'])
def test_image_and_terminal_artifacts_share_final_selection(tmp_path,monkeypatch,producer,both,prompt,scratch):
    import shlex
    from PIL import Image
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_image_generate_handler,_build_terminal_exec_handler
    from nullion.artifacts import artifact_root_for_principal
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.telegram_app import _deliver_reply
    from nullion.messaging_adapters import DeliveryContract
    runtime=setup_runtime(tmp_path,monkeypatch)
    principal='telegram:424242';root=artifact_root_for_principal(principal);root.mkdir(parents=True,exist_ok=True)
    output_root=tmp_path/'scratch' if scratch else root
    output_root.mkdir(parents=True,exist_ok=True)
    paths=[str(output_root/n) for n in ['image-a.png','image-b.png']]
    def image_provider(prompt,path,size):
        Image.new('RGB',(40,50),'green').save(path)
        return {'path':path,'artifact_path':path,'artifact_paths':[path]}
    handler=(_build_image_generate_handler(image_provider) if producer=='image_generate' else
             _build_terminal_exec_handler(workspace_root=root,allowed_roots=[tmp_path,root]))
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,root))
    registry.register(ToolSpec(producer,'Produce the image',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,20),handler)
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-consent',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)))
    from nullion.tools import _build_file_read_handler
    registry.register(ToolSpec('file_read','Inspect the artifact',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,20),
        _build_file_read_handler(allowed_roots=[tmp_path,root],include_principal_workspace=False))
    def forbidden_download(call):
        pytest.fail('A local generated file must never reach the network downloader')
    registry.register(ToolSpec('file_download','Download a remote source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,20),forbidden_download)
    class Model:
        steps=0
        def create(self,**kw):
            self.steps+=1
            assert self.steps<=5,'Artifact workflow restarted'
            if self.steps<=2:
                path=paths[self.steps-1]
                if producer=='image_generate':return tool_response(producer,{'prompt':'A green rectangle','output_path':path})
                script=f"from PIL import Image; Image.new('RGB',(40,50),'green').save({path!r}); print('MEDIA:'+{path!r})"
                return tool_response(producer,{'command':'python3 -c '+shlex.quote(script),'network_mode':'none'})
            if self.steps==3:return tool_response('file_download',{'url':Path(paths[-1]).as_uri()})
            if self.steps==4:return tool_response('file_read',{'path':paths[-1]})
            return text_response('Images created.\n'+'\n'.join('MEDIA:'+p for p in (paths if both else paths[1:])))
    model=Model();runtime.model_client=model
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id=principal,principal_id=principal,user_message=prompt,
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
        tool_flow_context={'artifact_extensions':['.png'],'requires_artifact_delivery':True})
    expected=paths if both else paths[1:]
    assert result.artifacts==expected,[(r.tool_name,r.status,r.error,r.output) for r in result.tool_results]
    assert len(result.tool_results)==4
    assert [r.status for r in result.tool_results]==['completed','completed','failed','completed']
    assert result.tool_results[2].output['reason']=='local_artifact_already_available'
    uploads=[]
    class Message:
        chat=SimpleNamespace(id=424242);message_id=19
        async def reply_document(self,document,**kw):uploads.append(document.read());return SimpleNamespace(message_id=20)
        async def reply_text(self,text,**kw):return SimpleNamespace(message_id=21)
    asyncio.run(_deliver_reply(Message(),result.final_text,artifact_paths=result.artifacts,principal_id=principal,
        delivery_contract=DeliveryContract.attachment_required(source='artifact_manifest',required_attachment_extensions=('.png',))))
    assert uploads==[Path(p).read_bytes() for p in expected]
    for data in uploads:
        with Image.open(io.BytesIO(data)) as im:assert im.size==(40,50)


@pytest.mark.parametrize('repair',[True,False])
@pytest.mark.parametrize('prompt',['Create a visual PDF.','図入りPDFを作成。'])
def test_selected_revision_must_itself_contain_required_media(tmp_path,monkeypatch,repair,prompt):
    from PIL import Image
    from pypdf import PdfReader
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_pdf_create_handler,_build_pdf_edit_handler
    from nullion.artifacts import artifact_root_for_principal
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime=setup_runtime(tmp_path,monkeypatch)
    principal='telegram:revision-media';root=artifact_root_for_principal(principal);root.mkdir(parents=True,exist_ok=True)
    image=root/'source.png';Image.new('RGB',(40,40),'green').save(image)
    original=root/'original.pdf';final=root/'selected.pdf'
    registry=ToolRegistry(filesystem_allowed_roots=(root,));writes=[]
    for name,builder in [('pdf_create',_build_pdf_create_handler),('pdf_edit',_build_pdf_edit_handler)]:
        handler=builder(allowed_roots=[root],include_principal_workspace=False)
        def run(call,handler=handler):
            result=handler(call);writes.append(result);return result
        registry.register(ToolSpec(name,'Create or edit the document',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,60),run)
    class Model:
        steps=0
        def create(self,**kw):
            self.steps+=1
            assert self.steps<=5,'Selected revision repair restarted the workflow'
            if self.steps==1:return tool_response('pdf_create',{'output_path':str(original),'html_pages':['<p>Text only</p>',f'<img src="{image}">'],'expected_page_count':2})
            if self.steps==2:return tool_response('pdf_edit',{'input_path':str(original),'output_path':str(final),'page_numbers':[1],'expected_page_count':1})
            if self.steps==4 and repair:return tool_response('pdf_create',{'output_path':str(final),'html_pages':[f'<p>Final</p><img src="{image}">'],'expected_page_count':1})
            return text_response('Created.\nMEDIA:'+str(final))
    model=Model();runtime.model_client=model
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id=principal,principal_id=principal,user_message=prompt,
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
        tool_flow_context={'artifact_extensions':['.pdf'],'required_embedded_media_extensions':['.pdf'],'requires_artifact_delivery':True})
    assert all(r.status=='completed' for r in writes),[(r.status,r.error,r.output) for r in writes]
    assert sum(len(p.images) for p in PdfReader(original).pages)>0
    assert result.artifacts==([str(final)] if repair else []),(result.final_text,[(r.tool_name,r.output) for r in result.tool_results])
    assert len(writes)==(3 if repair else 2)
    if repair:assert len(PdfReader(final).pages)==1 and sum(len(p.images) for p in PdfReader(final).pages)>0


@pytest.mark.parametrize('prompt',['Convert the draft to a final document.','下書きを最終文書に変換。'])
@pytest.mark.parametrize('both',[True,False])
def test_different_formats_still_require_an_explicit_final_selection(tmp_path,monkeypatch,prompt,both):
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_pdf_create_handler,_build_document_create_handler
    from nullion.artifacts import artifact_root_for_principal
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime=setup_runtime(tmp_path,monkeypatch);principal='telegram:conversion';root=artifact_root_for_principal(principal);root.mkdir(parents=True,exist_ok=True)
    pdf=str(root/'draft.pdf');doc=str(root/'converted.docx')
    registry=ToolRegistry(filesystem_allowed_roots=(root,))
    for name,builder in [('pdf_create',_build_pdf_create_handler),('document_create',_build_document_create_handler)]:
        registry.register(ToolSpec(name,'Produce document',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,60),builder(allowed_roots=[root],include_principal_workspace=False))
    class Model:
        steps=0
        def create(self,**kw):
            self.steps+=1
            assert self.steps<=4,'Selection regenerated the documents'
            if self.steps==1:return tool_response('pdf_create',{'output_path':pdf,'text_pages':['Source document.'],'expected_page_count':1})
            if self.steps==2:return tool_response('document_create',{'output_path':doc,'paragraphs':['Final converted document.']})
            if self.steps==3:return text_response('Created files.')
            assert not kw.get('tools')
            return text_response('Ready.\n'+'\n'.join('MEDIA:'+p for p in ([pdf,doc] if both else [doc])))
    model=Model();runtime.model_client=model
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id=principal,principal_id=principal,user_message=prompt,
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
        tool_flow_context={'requires_artifact_delivery':True})
    assert result.artifacts==([pdf,doc] if both else [doc]),result.final_text
    assert len(result.tool_results)==2


@pytest.mark.parametrize('prompt',['Rotate the existing PDF and retain its pictures.','既存PDFを回転し、画像を維持。'])
def test_existing_pdf_edit_needs_no_new_images_or_preliminary_read(tmp_path,monkeypatch,prompt):
    from PIL import Image
    from pypdf import PdfReader
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_pdf_edit_handler
    from nullion.artifacts import artifact_root_for_principal
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime=setup_runtime(tmp_path,monkeypatch);principal='telegram:existing-edit'
    root=artifact_root_for_principal(principal);root.mkdir(parents=True,exist_ok=True)
    source=root/'existing.pdf';final=root/'rotated.pdf'
    Image.new('RGB',(80,100),'green').save(source,'PDF')
    registry=ToolRegistry(filesystem_allowed_roots=(root,))
    registry.register(ToolSpec('pdf_edit','Edit the file',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),
        _build_pdf_edit_handler(allowed_roots=[root],include_principal_workspace=False))
    class Model:
        steps=0
        def create(self,**kw):
            self.steps+=1
            assert self.steps<=2,'The edit should not enter recovery or regenerate content'
            if self.steps==1:return tool_response('pdf_edit',{'input_path':str(source),'output_path':str(final),'rotate_degrees':180,'expected_page_count':1})
            return text_response('Rotated.\nMEDIA:'+str(final))
    model=Model();runtime.model_client=model
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id=principal,principal_id=principal,user_message=prompt,
        conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
        tool_flow_context={'artifact_extensions':['.pdf'],'required_embedded_media_extensions':['.pdf'],'requires_artifact_delivery':True})
    assert result.artifacts==[str(final)],result.final_text
    assert len(result.tool_results)==1 and result.tool_results[0].status=='completed'
    reader=PdfReader(final)
    assert len(reader.pages)==1 and reader.pages[0].rotation==180 and len(reader.pages[0].images)>0
