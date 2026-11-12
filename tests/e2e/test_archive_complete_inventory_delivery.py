"""A compact archive preview keeps a complete inventory handle and all output rows."""
from tests.workflow_provider_protocol import workflow_provider_reply
import io,json,os,zipfile
from pathlib import Path
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')

@pytest.mark.parametrize('create_from_source',[False,True])
@pytest.mark.parametrize('mixed_media',[False,True])
@pytest.mark.parametrize('prompt',['List every real archive file in inventory.xlsx and attach only that workbook.','Enumera todos los archivos reales en inventory.xlsx y adjunta solo ese libro.','実際の全ファイルを inventory.xlsx に一覧にして、そのファイルだけ添付してください。'])
def test_full_archive_inventory_survives_compaction_and_final_name(tmp_path,monkeypatch,prompt,create_from_source,mixed_media):
    from fastapi.testclient import TestClient
    from openpyxl import load_workbook
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_archive_extract_handler,_build_spreadsheet_create_handler
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from datetime import datetime,UTC
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    archive=tmp_path/'input.zip';expected={f'entry-{i:03}.txt' for i in range(111)}
    if mixed_media:expected.remove('entry-110.txt');expected.add('poster.png')
    image=io.BytesIO()
    if mixed_media:
        from PIL import Image
        Image.new('RGB',(320,200),'green').save(image,'PNG')
    with zipfile.ZipFile(archive,'w') as z:
        for name in sorted(expected):z.writestr(name,image.getvalue() if name=='poster.png' else 'Actual file content 37')
        z.writestr('__MACOSX/._entry-050.txt','metadata')
    root=artifact_root_for_principal('web:inventory');root.mkdir(parents=True,exist_ok=True);manifest=root/'complete-source.xlsx'
    stale=root/'expanded';stale.mkdir();(stale/'unrelated.txt').write_text('Previous run');(stale/'entry-000.txt').write_text('Preserve this prior version')
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);native=_build_archive_extract_handler(tmp_path,allowed_roots=[tmp_path]);extractions=[]
    def extract(inv):r=native(inv);extractions.append(r);return r
    registry.register(ToolSpec('archive_extract','Extract and inspect archive',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,20),extract)
    creations=[];native_sheet=_build_spreadsheet_create_handler(tmp_path,allowed_roots=[tmp_path])
    def create(inv):r=native_sheet(inv);creations.append(r);return r
    registry.register(ToolSpec('spreadsheet_create','Create full table from structured JSON source',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,20),create)
    class Model:
        phase=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.xlsx','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['archive_extract','spreadsheet_create'] if create_from_source else ['archive_extract'],'required_tool_names':['archive_extract','spreadsheet_create'] if create_from_source else ['archive_extract'],'artifact_extensions':['.xlsx']})
            if self.phase==2:return call('archive_extract',{'path':str(archive),'output_dir':str(root/'expanded'),'manifest_output_path':str(manifest)})
            if self.phase==4 and create_from_source:
                assert creations[0].status=='completed',creations[0].error
                return text('The complete inventory workbook is ready.\nMEDIA:'+str(root/'inventory.xlsx'))
            assert self.phase==3,'Complete extraction must not be repeated for a filename change'
            context=json.dumps(kw['messages'],ensure_ascii=False)
            assert 'source_inventory' in context and 'record_count' in context and '111' in context
            assert extractions[0].output['manifest_json_path'] in context
            assert extractions[0].output['output_dir'] in context
            assert 'entry-050.txt' not in context,'The model packet should remain compact'
            if create_from_source:
                assert 'rows_source' in context
                return call('spreadsheet_create',{'output_path':str(root/'inventory.xlsx'),'rows_source':{'path':extractions[0].output['manifest_json_path'],'rows_key':'rows','columns_key':'columns'},'expected_rows':111,
                    **({'image_paths':extractions[0].output['source_image_paths']} if mixed_media else {})})
            return text('The complete inventory workbook is ready.\nMEDIA:'+str(manifest))
    model=Model();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',GLOBAL_PERMISSION_PRINCIPAL,BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture-user',datetime.now(UTC)))
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:inventory'});assert result.status_code==200,result.text
        data=result.json();assert not model.errors,model.errors
        assert len(data['artifacts'])==1 and data['artifacts'][0]['name']=='inventory.xlsx',data
        body=client.get(data['artifacts'][0]['url']).content
        workbook=load_workbook(io.BytesIO(body),read_only=True);rows=list(workbook.active.iter_rows(values_only=True));workbook.close()
        column=rows[0].index('Filename');assert {r[column] for r in rows[1:]}==expected
        assert len(extractions)==1 and extractions[0].output['entry_count']==111 and extractions[0].output['metadata_entries_ignored']==1
        assert not any(a['name'].endswith(('.json','.csv','.zip')) for a in data['artifacts'])

        assert len(creations)==int(create_from_source)
        if mixed_media:
            with zipfile.ZipFile(io.BytesIO(body)) as package:assert sum(name.startswith('xl/media/') for name in package.namelist())==1

        actual_root=Path(extractions[0].output['output_dir'])
        assert actual_root!=stale and {p.name for p in actual_root.iterdir() if p.is_file()}==expected
        assert (stale/'unrelated.txt').read_text()=='Previous run' and (stale/'entry-000.txt').read_text()=='Preserve this prior version'
