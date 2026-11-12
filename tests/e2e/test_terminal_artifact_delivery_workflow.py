"""Real shell -> owned workspace -> actual Web download, across output formats."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
import shlex
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


@pytest.mark.parametrize('extension,prompt,script', [
    ('.csv', 'Create the CSV attachment.', "from pathlib import Path;Path('result.csv').write_text('name,value\\nNorth,12\\nSouth,19\\n')"),
    ('.html', 'Crea el informe HTML adjunto.', "from pathlib import Path;Path('result.html').write_text('<!doctype html><html><body><h1>North 12 South 19 Total 31</h1></body></html>')"),
    ('.xlsx', '添付の表計算ファイルを作成してください。', "from openpyxl import Workbook;w=Workbook();w.active.append(['North',12]);w.active.append(['South',19]);w.save('result.xlsx')"),
    ('.pdf', 'Create the PDF attachment.', "from pypdf import PdfWriter;from pypdf.generic import NameObject,DictionaryObject,DecodedStreamObject;w=PdfWriter();p=w.add_blank_page(612,792);f=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')});p[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):DictionaryObject({NameObject('/F1'):w._add_object(f)})});s=DecodedStreamObject();s.set_data(b'BT /F1 12 Tf 40 750 Td (North 12 South 19 Total 31) Tj ET');p[NameObject('/Contents')]=w._add_object(s);w.write('result.pdf')"),
    ('.png', '添付の PNG 画像を作成してください。', "from PIL import Image;Image.new('RGB',(48,48),'navy').save('result.png')"),
    ('.docx', 'Crea el documento DOCX adjunto.', "from docx import Document;d=Document();d.add_paragraph('North 12 South 19 Total 31');d.save('result.docx')"),
    ('.zip', 'Create the ZIP attachment.', "from zipfile import ZipFile;z=ZipFile('result.zip','w');z.writestr('findings.txt','North 12 South 19 Total 31');z.close()"),
    ('.wav', '添付の WAV 音声ファイルを作成してください。', "import wave;w=wave.open('result.wav','wb');w.setnchannels(1);w.setsampwidth(2);w.setframerate(8000);w.writeframes(bytes(1600));w.close()"),
])
@pytest.mark.parametrize('outside_delivery_root', [False, True])
@pytest.mark.parametrize('predelete', [False, True])
def test_terminal_creation_delivers_bytes_from_owned_directory(tmp_path, monkeypatch, extension, prompt, script, outside_delivery_root, predelete):
    from datetime import UTC, datetime
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_terminal_exec_handler
    monkeypatch.setenv('HOME', str(tmp_path));monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    if outside_delivery_root:
        script=script.replace("'result"+extension+"'", repr(str(tmp_path/('result'+extension))))
    script+=";from pathlib import Path;Path('verification.txt').write_text('Internal verification note')"
    if outside_delivery_root:script+=';print('+repr(str(tmp_path/('result'+extension)))+')'
    registry=ToolRegistry()
    registry.register(ToolSpec(name='terminal_exec',description='Execute the prepared command',risk_level=ToolRiskLevel.HIGH,
        side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,requires_approval=True,timeout_seconds=20),
        _build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))
    class Provider:
        phase=0
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={tool['name'] for tool in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':data}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Create the requested file','requirement_kind':'deliverable','evidence_subject_id':'output','eligible_evidence_tool_names':['terminal_exec']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                data=json.loads(kw['messages'][0]['content'][0]['text'])
                ids=[row['invocation_id'] for row in data['tool_evidence'] if row['tool_name']=='terminal_exec' and row.get('status')=='completed']
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,
                    'material_requirements':[{**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids[-1:]} for row in data['material_requirement_contract']],
                    'unresolved_requirements':[]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':extension,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_shell'],
                'tool_names':['terminal_exec'],'required_tool_names':['terminal_exec'],'artifact_extensions':[extension]})
            if self.phase==2 or (predelete and self.phase==3):
                command=shlex.quote(sys.executable)+' -c '+shlex.quote(script)
                if predelete and self.phase==2:command='rm -f never-created-for-this-test.csv; '+command
                return tool('terminal_exec',{'command':command})
            return text('Created the requested file.'+('\nMEDIA:'+str(tmp_path/('result'+extension)) if outside_delivery_root else ''))
    from nullion import platform_chat
    shared_results=[];original=platform_chat.run_platform_chat_request
    def observe(*args,**kwargs):
        value=original(*args,**kwargs);shared_results.append(value);return value
    monkeypatch.setattr(platform_chat,'run_platform_chat_request',observe)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)))
    runtime.checkpoint();model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:terminal-artifact'})
        assert result.status_code==200,result.text
        data=result.json()
        assert len(data['artifacts'])==1,(data,[(r.text,r.artifact_paths,r.artifacts) for r in shared_results])
        artifact=data['artifacts'][0];path=Path(artifact['path'])
        assert path.name=='result'+extension and 'artifacts' in path.parts
        if outside_delivery_root:
            assert path.read_bytes()==(tmp_path/('result'+extension)).read_bytes()
        else:
            assert not (tmp_path/('result'+extension)).exists()
        if predelete:
            turns=[row for row in runtime.store.list_conversation_events('web:terminal-artifact') if row.get('event_type')=='conversation.chat_turn']
            guards=[row for row in turns[-1]['tool_results'] if row.get('output',{}).get('reason')=='artifact_deletion_alternative_available']
            assert len(guards)==1 and guards[0]['output']['command_executed'] is False
        delivered=client.get(artifact['url']);assert delivered.status_code==200
        assert delivered.content==path.read_bytes() and len(delivered.content)>20
