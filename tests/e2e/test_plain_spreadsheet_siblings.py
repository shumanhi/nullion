"""A collected image must not turn a native CSV/TSV export into a visual workbook."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json, os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product-boundary E2E')

@pytest.mark.parametrize('extension',['.csv','.tsv'])
@pytest.mark.parametrize('prompt',['Deliver the data export after collecting the image.','画像を取得してからデータを書き出してください。','Recoge la imagen y entrega la exportación de datos.'])
def test_collected_image_does_not_block_plain_spreadsheet_export(tmp_path,monkeypatch,extension,prompt):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal,artifact_output_descriptor
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult,_build_spreadsheet_create_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    root=artifact_root_for_principal('web:plain-export');root.mkdir(parents=True,exist_ok=True);image=root/'source.png';Image.new('RGB',(160,160),'blue').save(image);output=root/('data'+extension);workbook=root/'visual.xlsx';calls=[]
    registry.register(ToolSpec('browser_image_collect','External source image boundary',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{'image_paths':[str(image)],'artifact_paths':[str(image)],'artifact_descriptors':[artifact_output_descriptor(str(image),role='source',kind='image')]}))
    native=_build_spreadsheet_create_handler(tmp_path,allowed_roots=[tmp_path])
    def create(inv):calls.append(inv);return native(inv)
    registry.register(ToolSpec('spreadsheet_create','Create CSV or workbook',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),create)
    class Model:
        phase=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            system=str(kw.get('system') or '');names={t['name'] for t in kw.get('tools',[])}
            def text(v):return {'stop_reason':'end_turn','content':[{'type':'text','text':v}]}
            def tool(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Deliver '+name,'requirement_kind':'deliverable','evidence_subject_id':name,'eligible_evidence_tool_names':['spreadsheet_create']} for name in [workbook.name,output.name]]}))
            if system.startswith('You are a semantic completion reviewer'):
                d=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':[{**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[item['invocation_id'] for item in d['tool_evidence'] if item.get('output',{}).get('artifact_record_binding',{}).get('filename')==r['evidence_subject_id']]} for r in d['material_requirement_contract']],'unresolved_requirements':[]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':extension,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files','web'],'tool_names':['browser_image_collect','spreadsheet_create'],'artifact_extensions':['.xlsx',extension],'embedded_media_artifact_extensions':['.xlsx']})
            if self.phase==2:return tool('browser_image_collect',{'url':'https://example.test/source'})
            if self.phase==3:return tool('spreadsheet_create',{'output_path':str(workbook),'columns':['Name','Value'],'rows':[['Verified',37]],'image_paths':[str(image)]})
            if self.phase==4:return tool('spreadsheet_create',{'output_path':str(output),'columns':['Name','Value'],'rows':[['Verified',37]]})
            assert self.phase==5,'Plain export was blocked or unnecessarily repeated'
            return text('The workbook and data export are attached.\nMEDIA:'+str(workbook)+'\nMEDIA:'+str(output))
    model=Model();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:plain-export'});assert response.status_code==200,response.text;data=response.json()
        assert {item['path'] for item in data['artifacts']}=={str(workbook),str(output)},data
        body=client.get(next(item['url'] for item in data['artifacts'] if item['path']==str(output))).content.decode();delimiter=',' if extension=='.csv' else '\t'
        assert body.splitlines()==[delimiter.join(['Name','Value']),delimiter.join(['Verified','37'])]
        assert len(calls)==2
        import io,zipfile
        wb=client.get(next(item['url'] for item in data['artifacts'] if item['path']==str(workbook))).content
        with zipfile.ZipFile(io.BytesIO(wb)) as z:
            assert len([name for name in z.namelist() if name.startswith('xl/media/')])==1
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db');turn=next(r for r in restored.store.list_conversation_events('web:plain-export') if r.get('event_type')=='conversation.chat_turn')
    assert all(r['status']=='completed' for r in turn['tool_results'])
