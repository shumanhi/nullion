"""One real generator can prove five distinct files without fabricating source facts."""
from tests.workflow_provider_protocol import workflow_provider_reply
import hashlib
import json
import os
import shlex
import sys

import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('planner_timeouts',[0])
@pytest.mark.parametrize('review_timeouts',[0])
@pytest.mark.parametrize('prompt',['Create the DOCX, XLSX, CSV, PDF and HTML package.',
    'Crea el paquete DOCX, XLSX, CSV, PDF y HTML.', 'DOCX、XLSX、CSV、PDF、HTMLのファイル一式を作成してください。'])
def test_repaired_multi_format_package_delivers_all_verified_files(tmp_path,monkeypatch,prompt,planner_timeouts,review_timeouts,review_invalid=0,review_min_timeout=0,hard_limit=False):
    from datetime import UTC,datetime
    from pathlib import Path
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_terminal_exec_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    extensions=('.docx','.xlsx','.csv','.pdf','.html');conversation='web:package'
    root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    script="""
from pathlib import Path
from docx import Document
from openpyxl import Workbook
from openpyxl.chart import BarChart,Reference
from openpyxl.formatting.rule import CellIsRule
from pypdf import PdfWriter
from pypdf.generic import NameObject,DictionaryObject,DecodedStreamObject
marker='CURRENT PACKAGE 31'
d=Document();d.add_paragraph(marker);d.save('bundle.docx')
w=Workbook();s=w.active;s.append(['Marker',marker]);s.append(['Observed',31]);s.append(['Derived','=B2*2'])
chart=BarChart();chart.add_data(Reference(s,min_col=2,min_row=2,max_row=3));s.add_chart(chart,'D2')
s.conditional_formatting.add('B2:B3',CellIsRule(operator='greaterThan',formula=['30']));w.save('bundle.xlsx')
Path('bundle.csv').write_text('Name,Value\\n'+marker+',31\\n')
Path('bundle.html').write_text('<!doctype html><html><body><h1>'+marker+'</h1></body></html>')
w=PdfWriter();p=w.add_blank_page(612,792)
f=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
p[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):DictionaryObject({NameObject('/F1'):w._add_object(f)})})
s=DecodedStreamObject();s.set_data(b'BT /F1 12 Tf 40 750 Td (CURRENT PACKAGE 31) Tj ET');p[NameObject('/Contents')]=w._add_object(s);w.write('bundle.pdf')
"""
    class Provider:
        phase=0;reviewed=False;contract_calls=0;review_calls=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={tool['name'] for tool in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':data}]}
            if system.startswith('You are a material-requirements planner'):
                from nullion.model_clients import ModelClientTimeoutError
                self.contract_calls+=1
                if self.contract_calls<=planner_timeouts:
                    raise ModelClientTimeoutError('External verifier fixture timed out')
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver bundle'+extension+' with CURRENT PACKAGE 31','requirement_kind':'deliverable',
                     'evidence_subject_id':extension,'eligible_evidence_tool_names':['terminal_exec']} for extension in extensions]+[
                     {'requirement':'Deliver exactly these five final files without sidecars','requirement_kind':'constraint',
                      'evidence_subject_id':'package','artifact_member_subject_ids':list(extensions),'eligible_evidence_tool_names':[]}]}))
            if system.startswith('You are a semantic completion reviewer'):
                from nullion.model_clients import ModelClientTimeoutError
                self.review_calls+=1
                if kw['timeout'] < review_min_timeout:
                    raise ModelClientTimeoutError('The multi-file review deadline is too short for its required output')
                if self.review_calls<=review_invalid:
                    response=text('{"disposition":')
                    response['completion']={'status':'incomplete','reason':'max_output_tokens'}
                    return response
                if self.review_calls<=review_timeouts:
                    raise ModelClientTimeoutError('External reviewer fixture timed out')
                self.reviewed=True;data=json.loads(kw['messages'][0]['content'][0]['text'])
                records=[item for item in data['tool_evidence'] if isinstance(item.get('output'),dict)
                         and item['output'].get('artifact_record_binding')]
                assert len(records)==5,data['tool_evidence']
                bindings={item['output']['artifact_record_binding']['extension']:item for item in records}
                assert len({item['output']['artifact_record_binding']['parent_invocation_id'] for item in records})==1
                assert all('CURRENT PACKAGE 31' in item.get('content_preview','') for item in data['artifact_evidence']),data['artifact_evidence']
                workbook = next(item['structure'] for item in data['artifact_evidence'] if item['extension']=='.xlsx')
                assert workbook['formula_cell_count']==1 and workbook['chart_count']==1
                assert workbook['conditional_formatting_rule_count']==1 and workbook['workbook_inspection_complete']
                rows=[]
                for row in data['material_requirement_contract']:
                    members=row.get('artifact_member_subject_ids') or [row['evidence_subject_id']]
                    rows.append({**row,'satisfied':True,'draft_covered':True,
                        'evidence_invocation_ids':[bindings[member]['invocation_id'] for member in members]})
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,
                    'material_requirements':rows,'unresolved_requirements':[]}))
            if 'select_artifact_delivery' in names:
                payload=kw['messages'][0]['content'][0]['text'].split('Prepared file candidates: ',1)[1].split('\n',1)[1].split('\n')[0]
                candidates=json.loads(payload)
                selected=[r['candidate_id'] for r in candidates if r.get('filename') in {'bundle'+e for e in extensions}]
                assert len(selected)==5,candidates
                return tool('select_artifact_delivery',{'status':'selected','selected_ids':selected,'reason':'The five finished requested files; no sidecars.'})
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.docx','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_shell'],'tool_names':['terminal_exec'],
                'required_tool_names':['terminal_exec'],'artifact_extensions':list(extensions)})
            if self.phase==2:return tool('terminal_exec',{'command':shlex.quote(sys.executable)+" -c 'raise ValueError(\"retry fixture\")'"})
            if self.phase==3:return tool('terminal_exec',{'command':shlex.quote(sys.executable)+' -c '+shlex.quote(script)})
            return text('Created the five files.\n'+'\n'.join('MEDIA:'+str(root/('bundle'+extension)) for extension in extensions))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');model=Provider();runtime.model_client=model
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)));runtime.checkpoint()
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec(name='terminal_exec',description='Generate files',risk_level=ToolRiskLevel.HIGH,
        side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,requires_approval=True,timeout_seconds=20),
        _build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))
    class BoundedOrchestrator(AgentOrchestrator):
        def run_turn(self,**kwargs):
            if hard_limit:kwargs['max_iterations']=3
            return super().run_turn(**kwargs)
    with TestClient(web_app.create_app(runtime,orchestrator=BoundedOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation})
        assert response.status_code==200,response.text
        data=response.json();assert len(data['artifacts'])==5,data
        assert model.contract_calls == model.review_calls == 0 and not model.reviewed
        assert model.phase==(3 if hard_limit else 4), 'Terminal work must not restart generation'
        turns=[row for row in runtime.store.list_conversation_events(conversation) if row.get('event_type')=='conversation.chat_turn']
        produced=[row for row in turns[-1]['tool_results'] if row['tool_name']=='terminal_exec']
        assert len(produced)==2 and [row['status'] for row in produced]==['failed','completed']
        assert {Path(item['name']).suffix for item in data['artifacts']}==set(extensions)
        for artifact in data['artifacts']:
            downloaded=client.get(artifact['url']);assert downloaded.status_code==200
            assert hashlib.sha256(downloaded.content).hexdigest()==hashlib.sha256(Path(artifact['path']).read_bytes()).hexdigest()






@pytest.mark.parametrize('prompt',['Finish the five files.','Completa los cinco archivos.','5つのファイルを完成させてください。'])
def test_finished_package_survives_producer_iteration_limit(tmp_path,monkeypatch,prompt):
    test_repaired_multi_format_package_delivers_all_verified_files(tmp_path,monkeypatch,prompt,0,0,hard_limit=True)
