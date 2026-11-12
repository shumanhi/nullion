"""Archive filenames stay cell data through real extraction, workbook, and Web delivery."""
from tests.workflow_provider_protocol import workflow_provider_reply
import io
import json
import os
from zipfile import ZipFile

import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker workflow only')


@pytest.mark.parametrize('prompt',['List the archive files in one workbook.','Enumera los archivos en un solo libro.','書庫のファイルを一つのブックに一覧表示。'])
@pytest.mark.parametrize('remote_download',[False,True])
def test_file_names_are_not_implicit_image_inputs(tmp_path,monkeypatch,prompt,remote_download):
    from fastapi.testclient import TestClient
    from openpyxl import load_workbook
    from PIL import Image
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_archive_extract_handler,_build_spreadsheet_create_handler,_build_file_download_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    conversation='web:archive-manifest';root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    source=root/'input.zip';final=root/'manifest.xlsx';names=['alpha.png','beta.jpg','画像.png','facts.txt']
    image=io.BytesIO();Image.new('RGB',(8,8),'red').save(image,'PNG')
    archive_bytes=io.BytesIO()
    with ZipFile(archive_bytes,'w') as archive:
        for name in names:archive.writestr(name,image.getvalue() if name.endswith(('.png','.jpg')) else b'Example record')
        archive.writestr('__MACOSX/._alpha.png',b'metadata')
    if not remote_download: source.write_bytes(archive_bytes.getvalue())
    if remote_download:
        from email.message import Message
        from types import SimpleNamespace
        from nullion import tools
        class Response(io.BytesIO):
            status=200
            def __init__(self):
                super().__init__(archive_bytes.getvalue());self.headers=Message()
                self.headers['Content-Type']='application/zip';self.headers['Content-Length']=str(len(archive_bytes.getvalue()))
            def geturl(self): return 'https://93.184.216.34/archive.zip'
        monkeypatch.setattr(tools,'_build_web_fetch_opener',lambda:SimpleNamespace(open=lambda request,timeout:Response()))
    native_extract=_build_archive_extract_handler(tmp_path,allowed_roots=[tmp_path]);extractions=[]
    def extract(invocation):
        result=native_extract(invocation);extractions.append(result);return result
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    for name,handler in [('archive_extract',extract),('spreadsheet_create',_build_spreadsheet_create_handler(tmp_path,allowed_roots=[tmp_path]))]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),handler)
    registry.register(ToolSpec('file_download','Acquire a URL source',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),
        _build_file_download_handler(tmp_path,allowed_roots=[tmp_path]))
    registry.register(ToolSpec('browser_navigate','Read a public page',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,30),
        lambda _:pytest.fail('A required file download must not add unrelated browser navigation'))
    class Provider:
        phase=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            system=str(kw.get('system') or '');visible={row['name'] for row in kw.get('tools',[])}
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver one workbook listing every real archive file','requirement_kind':'deliverable','evidence_subject_id':'manifest',
                     'eligible_evidence_tool_names':['spreadsheet_create']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                data=json.loads(kw['messages'][0]['content'][0]['text']);ids=[row['invocation_id'] for row in data['tool_evidence'] if row.get('tool_name')=='spreadsheet_create' and row.get('status')=='completed']
                return text(json.dumps({'disposition':'complete','unresolved_requirements':[],'retry_tool_names':[],
                    'all_material_requirements_enumerated':True,'material_requirements':[{**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids} for row in data['material_requirement_contract']]}))
            if 'select_attachment_format' in visible:return tool('select_attachment_format',{'extension':'.xlsx','confidence':1})
            if not visible:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files','web'] if remote_download else ['local_files'],
                'tool_names':['file_download','archive_extract','spreadsheet_create'] if remote_download else ['archive_extract','spreadsheet_create'],
                'required_tool_names':['file_download','archive_extract','spreadsheet_create'] if remote_download else ['archive_extract','spreadsheet_create'],
                'artifact_extensions':['.xlsx']})
            if self.phase==2 and remote_download:return tool('file_download',{'url':'https://93.184.216.34/archive.zip','output_path':str(source)})
            if self.phase==2+int(remote_download):return tool('archive_extract',{'path':str(source),'output_dir':str(root/'extracted')})
            if self.phase==3+int(remote_download):
                assert extractions[0].status=='completed',extractions[0]
                return tool('spreadsheet_create',{'output_path':str(final),'columns':['項目','値'],
                    'rows':[[name,name] for name in names]+[['URL','https://assets.example.test/plain-data.png']],
                    'expected_rows':len(names)+1})
            assert self.phase==4+int(remote_download),'A file manifest must not enter an image repair or generation loop'
            return text('Prepared the file list.\nMEDIA:'+str(final))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');model=Provider();runtime.model_client=model
    if remote_download:
        from datetime import UTC,datetime
        from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,OPERATOR_PERMISSION_PRINCIPAL
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-download-consent',OPERATOR_PERMISSION_PRINCIPAL,
            BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'93.184.216.34','fixture',datetime.now(UTC)))
        runtime.checkpoint()
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        request_prompt=prompt+(' URL: https://93.184.216.34/archive.zip' if remote_download else '')
        response=client.post('/api/chat',json={'text':request_prompt,'conversation_id':conversation});assert response.status_code==200,response.text
        data=response.json();assert len(data.get('artifacts',[]))==1,data;assert data['artifacts'][0]['name']=='manifest.xlsx'
        download=client.get(data['artifacts'][0]['url']);assert download.status_code==200
        workbook=load_workbook(io.BytesIO(download.content));rows=list(workbook.active.values)
        assert [r[0] for r in rows[1:-1]]==names and rows[-1][1]=='https://assets.example.test/plain-data.png',rows
        with ZipFile(io.BytesIO(download.content)) as archive:assert not any('/media/' in name for name in archive.namelist())
        turns=[e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type')=='conversation.chat_turn']
        tools=turns[-1]['tool_results'];assert sum(r['tool_name']=='spreadsheet_create' for r in tools)==1
        assert not any(r['tool_name']=='browser_navigate' for r in tools)
        assert sum(r['tool_name']=='file_download' for r in tools)==int(remote_download)
        assert all(r['status']=='completed' for r in tools)
        assert len(extractions)==1 and extractions[0].output['entry_count']==len(names)
