"""The real API retains extraction proof while delivering only the final workbook."""
import io
import json
import os
import zipfile

import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('prompt',['ZIP を展開して final.xlsx だけを添付してください。',
    'Extrae el ZIP y adjunta solamente final.xlsx.'])
def test_internal_extraction_proof_survives_final_artifact_selection(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from openpyxl import load_workbook
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_archive_extract_handler,_build_spreadsheet_create_handler
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED','0')
    archive=tmp_path/'input.zip'
    with zipfile.ZipFile(archive,'w') as z:z.writestr('cedar.txt','Cedar: 42')
    writes=[]
    def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}
    def call(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Model:
        phase=0;plans=0;reviews=0;trace=[]
        def create(self,**kw):
            system=kw.get('system','');names={r['name'] for r in kw.get('tools',[])}
            self.trace.append([self.phase,system[:65],sorted(names)])
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.xlsx','confidence':1})
            if names=={'select_artifact_delivery'}:
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                return call('select_artifact_delivery',{'status':'selected','selected_ids':[c['candidate_id'] for c in json.loads(raw) if c['filename']=='final.xlsx'],'reason':'Only the requested final workbook'})
            if 'material-requirements planner' in system:
                self.plans+=1
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.xlsx'],
                    'material_requirements':[{'requirement':'ZIP の展開','requirement_kind':'constraint','delivery_required':False,
                        'evidence_subject_id':'extract','eligible_evidence_tool_names':['archive_extract']},
                        {'requirement':'final.xlsx のみ添付','requirement_kind':'deliverable','evidence_subject_id':'final',
                         'eligible_evidence_tool_names':['spreadsheet_create']}]})
            if 'semantic completion reviewer' in system:
                self.reviews+=1;p=json.loads(kw['messages'][0]['content'][0]['text'])
                records=[r for r in p['tool_evidence'] if r.get('tool_name')=='archive_extract'
                    and r.get('output',{}).get('result_kind')=='structured_source_record']
                assert len(records)==1,p['tool_evidence']
                source=records[0]
                assert source['output']['record']['extraction_complete'] is True
                assert source['invocation_id'] in {r['invocation_id'] for r in p['citable_evidence']}
                final=next(r for r in p['tool_evidence'] if r.get('output',{}).get('artifact_record_binding')
                    and r['output']['artifact_record_binding'].get('filename')=='final.xlsx')
                return text({'disposition':'complete','all_material_requirements_enumerated':True,
                    'material_requirements':[{'requirement_id':row['requirement_id'],'satisfied':True,
                        'draft_covered':row.get('delivery_required',True),
                        'evidence_invocation_ids':[source['invocation_id'] if row['requirement_kind']=='constraint' else final['invocation_id']]}
                        for row in p['material_requirement_contract']], 'unresolved_requirements':[],'retry_tool_names':[]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text({})
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['archive_extract','spreadsheet_create'],'required_tool_names':['archive_extract','spreadsheet_create'],
                'artifact_extensions':['.xlsx']})
            if self.phase==2:return call('archive_extract',{'path':str(archive),'manifest_output_path':str(root/'internal.xlsx')})
            if self.phase==3:return call('spreadsheet_create',{'output_path':str(root/'final.xlsx'),'columns':['Name','Value'],'rows':[['Cedar',42]]})
            return text('Final workbook attached.\n\nMEDIA:'+str(root/'final.xlsx'))
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    from datetime import datetime,UTC
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    root=artifact_root_for_principal('operator');root.mkdir(parents=True,exist_ok=True)
    registry=app.state.live_tracker_engine.registry
    for name,builder in [('archive_extract',_build_archive_extract_handler),('spreadsheet_create',_build_spreadsheet_create_handler)]:
        handler=builder(tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False)
        def native(inv,handler=handler):
            result=handler(inv);writes.append(result);return result
        registry.register(ToolSpec(name,'Local artifact tool',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),native)
    with TestClient(app) as client:
        cid='web:archive-proof'
        response=client.post('/api/chat',json={'text':prompt+' '+str(archive),'conversation_id':cid})
        assert response.status_code==200,response.text
        payload=response.json();files=payload.get('artifacts',[])
        assert len(files)==1 and files[0]['name']=='final.xlsx',json.dumps({'payload':payload,'trace':model.trace,'events':runtime.store.list_conversation_events(cid)},default=str)
        content=client.get(files[0]['url']);assert content.status_code==200
        book=load_workbook(io.BytesIO(content.content));assert list(book.active.values)==[('Name','Value'),('Cedar',42)]
        turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
        assert turn.get('response_fulfilled') is True,json.dumps(runtime.store.list_conversation_events(cid),default=str)
        assert 'internal.xlsx' not in payload['text'] and 'review could not' not in payload['text']
        assert [r.tool_name for r in writes]==['archive_extract','spreadsheet_create']
        assert model.plans==model.reviews==1
