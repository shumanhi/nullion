"""Real chat/runtime/native files distinguish table data and optional artwork."""
import io
import json
import os
import zipfile
import pytest
from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_scheduled_report_completion import setup_runtime,text_response,tool_response
from tests.test_optional_artifact_media_contract import CASES,ALIASES

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker native artifact API boundary')


def _register(registry,name,handler):
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass
    registry.register(ToolSpec(name,'Native artifact operation',ToolRiskLevel.LOW,
                              ToolSideEffectClass.WRITE,False,30),handler)


@pytest.mark.parametrize('name,extension,builder,arguments',CASES+ALIASES)
@pytest.mark.parametrize('prompt',['Create and attach a concise data note.','データをまとめたファイルを作成して添付してください。'])
def test_optional_failed_artwork_does_not_block_plain_native_repair(tmp_path,monkeypatch,name,extension,builder,arguments,prompt):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry
    runtime=setup_runtime(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    with sync_playwright() as pw:monkeypatch.setenv('NULLION_CHROMIUM_EXECUTABLE',pw.chromium.executable_path)
    cid='web:optional-media';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    output=root/('result'+extension);native=builder(tmp_path,allowed_roots=[tmp_path]);executed=[]
    def create(inv):
        result=native(inv);executed.append(result);return result
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);_register(registry,name,create)
    class Provider:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            names={t['name'] for t in kw.get('tools',[])}
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':extension,'confidence':1})
            self.phase+=1
            if self.phase==1:return tool_response('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':[name],'required_tool_names':[name],'artifact_extensions':[extension],
                'embedded_media_artifact_extensions':[]})
            if self.phase==2:return tool_response(name,{**arguments,'output_path':str(output),
                'image_paths':[str(root/'missing.png')]})
            if self.phase==3:return tool_response(name,{**arguments,'output_path':str(output)})
            assert self.phase==4,'Optional image failure must not cause a regeneration loop'
            return text_response('Ready.\nMEDIA:'+str(output))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as api:
        response=api.post('/api/chat',json={'text':prompt,'conversation_id':cid})
        assert response.status_code==200,response.text
        data=response.json();assert len(data.get('artifacts',[]))==1,(data,executed)
        artifact=data['artifacts'][0];assert artifact['name']==output.name
        downloaded=api.get(artifact['url']);assert downloaded.status_code==200
        if extension in {'.html','.htm'}:
            assert b'53' in downloaded.content and b'<img' not in downloaded.content.lower()
        elif extension=='.pdf':
            from pypdf import PdfReader
            document=PdfReader(io.BytesIO(downloaded.content));assert '53' in document.pages[0].extract_text()
            assert not document.pages[0].images
        else:
            with zipfile.ZipFile(io.BytesIO(downloaded.content)) as package:
                assert not any('/media/' in n for n in package.namelist())
                xml=b''.join(package.read(n) for n in package.namelist() if n.endswith('.xml'))
                assert b'53' in xml
        turn=next(t for t in reversed(runtime.store.list_conversation_events(cid)) if t.get('event_type')=='conversation.chat_turn')
        assert turn.get('response_fulfilled') is True,turn
        results=[r for r in turn['tool_results'] if r['tool_name']!= 'request_tool_scope']
        assert [(r['tool_name'],r['status']) for r in results]==[(name,'failed'),(name,'completed')]
        assert results[0]['response_requirements']['_native_artifact_media']['required'] is False
        assert len(executed)==2


@pytest.mark.parametrize('prompt',['List all files from this archive in a new workbook.','このアーカイブのすべてのファイルを新しい表にしてください。'])
def test_archive_source_inventory_image_column_stays_literal_through_chat(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from PIL import Image
    from openpyxl import load_workbook
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,_build_archive_extract_handler,_build_spreadsheet_create_handler
    runtime=setup_runtime(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    cid='web:archive-literal';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    png=io.BytesIO();Image.new('RGB',(160,160),'blue').save(png,format='PNG')
    source=root/'source.zip'
    with zipfile.ZipFile(source,'w') as archive:
        archive.writestr('present.png',png.getvalue());archive.writestr('broken.png',b'not an image')
        archive.writestr('記録.txt','Verified value 53')
        archive.writestr('__MACOSX/._present.png',b'metadata')
    output=root/'inventory.xlsx';receipts=[];native=_build_archive_extract_handler(tmp_path,allowed_roots=[tmp_path])
    def extract(inv):
        result=native(inv);receipts.append(result);return result
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);_register(registry,'archive_extract',extract)
    _register(registry,'spreadsheet_create',_build_spreadsheet_create_handler(tmp_path,allowed_roots=[tmp_path]))
    class Provider:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            names={t['name'] for t in kw.get('tools',[])}
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format',{'extension':'.xlsx','confidence':1})
            if 'select_artifact_delivery' in names:
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                candidates=json.loads(raw)
                selected=[r['candidate_id'] for r in candidates if r.get('filename')==output.name]
                assert len(selected)==1,candidates
                return tool_response('select_artifact_delivery',{'status':'selected','selected_ids':selected,
                    'reason':'The new workbook is requested; the extracted manifest is a supporting source.'})
            self.phase+=1
            if self.phase==1:return tool_response('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['archive_extract','spreadsheet_create'],'required_tool_names':['archive_extract','spreadsheet_create'],
                'artifact_extensions':['.xlsx'],'embedded_media_artifact_extensions':[]})
            if self.phase==2:return tool_response('archive_extract',{'path':str(source),'output_dir':str(root/'extracted')})
            if self.phase==3:
                assert receipts[0].status=='completed',receipts[0]
                return tool_response('spreadsheet_create',{'output_path':str(output),
                    'rows_source':{'path':receipts[0].output['manifest_json_path'],
                                   'rows_key':'rows','columns_key':'columns'},'expected_rows':3})
            assert self.phase==4,'Imported Image data must not cause extra repair or image-generation calls'
            return text_response('Ready.\nMEDIA:'+str(output))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as api:
        data=api.post('/api/chat',json={'text':prompt,'conversation_id':cid}).json()
        assert [a['name'] for a in data.get('artifacts',[])]==['inventory.xlsx'],data
        downloaded=api.get(data['artifacts'][0]['url']);assert downloaded.status_code==200
        book=load_workbook(io.BytesIO(downloaded.content),read_only=True);values=list(book.active.values);book.close()
        header=list(values[0]);assert 'Image' in header
        assert {row[header.index('Filename')] for row in values[1:]}=={'present.png','broken.png','記録.txt'}
        with zipfile.ZipFile(io.BytesIO(downloaded.content)) as package:assert not any(n.startswith('xl/media/') for n in package.namelist())
        turn=next(t for t in reversed(runtime.store.list_conversation_events(cid)) if t.get('event_type')=='conversation.chat_turn')
        assert turn.get('response_fulfilled') is True,turn
        assert all(r['status']=='completed' for r in turn['tool_results']),turn
