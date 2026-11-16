"""A repaired archive creation publishes the real file and a consistent receipt."""
import io
import json
import os
import time
import zipfile
from pathlib import Path

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('mode',['off','on'])
@pytest.mark.parametrize('prompt',['Create and attach the requested ZIP.','Crea y adjunta el ZIP solicitado.','指定された ZIP を作成して添付してください。'])
def test_archive_repair_rehomes_new_runtime_output_and_delivers_consistent_receipt(tmp_path,monkeypatch,mode,prompt):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal,nullion_data_home
    from nullion.tools import create_core_tool_registry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    from datetime import UTC,datetime
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    monkeypatch.setenv('NULLION_CODE_MODE',mode)
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1')
    root=artifact_root_for_principal('web:archive-handoff');root.mkdir(parents=True,exist_ok=True)
    source=root/'north.csv';source.write_text('Region,Value\nNorth,12\n')
    wrong=nullion_data_home()/'uploads'/'source-37.zip'
    expected=root/'source-37.zip'
    class Provider:
        rounds=0
        errors=[]
        def create(self,**kw):
            try:
                response=self._create(**kw)
                for block in response.get('content',[]):
                    if block.get('type')=='tool_use':block['id']+='-'+str(self.rounds)
                return response
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            names={t['name'] for t in kw.get('tools',[])}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return text('{}')
            self.rounds+=1
            assert self.rounds<=4,'Completed archive restarted or failed to reach delivery'
            if self.rounds==1:return call('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['archive_create'],'required_tool_names':['archive_create'],'artifact_extensions':['.zip']})
            if self.rounds in {2,3}:
                args={'output_path':str(wrong),'source_paths':[str(root/'absent.csv' if self.rounds==2 else source)]}
                return (call('code_mode',{'tool_names':['archive_create'],
                    'code':"call_tool('archive_create', "+repr(args)+")"}) if mode=='on' else call('archive_create',args))
            return text('Archivo listo.\n\nMEDIA:'+str(expected))
    model=Provider();registry=create_core_tool_registry(workspace_root=tmp_path)
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        start=time.perf_counter()
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:archive-handoff'})
        assert response.status_code==200,response.text
        data=response.json();assert len(data.get('artifacts',[]))==1,{
            'reply':data,'errors':model.errors,'tools':[(r['tool_name'],r['status'],r.get('error'),
                {k:v for k,v in (r.get('output')or{}).items() if k in {'reason','path','artifact_path','execution_error'}})
                for e in runtime.store.list_conversation_events('web:archive-handoff')
                if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]}
        assert 'not available' not in data['text'] and 'not created' not in data['text'],data
        payload=client.get(data['artifacts'][0]['url']);assert payload.status_code==200
        with zipfile.ZipFile(io.BytesIO(payload.content)) as archive:
            assert archive.namelist()==['north.csv']
            assert archive.read('north.csv')==source.read_bytes()
        assert expected.is_file() and not wrong.exists()
        history=client.get('/api/chat/history/web:archive-handoff',
            params={'turn_ids':json.dumps([data['turn_id']])}).json()
        final=history['terminal_replies'][0]
        assert final['text'].strip() and 'not available' not in final['text'],final
        assert len(final['artifacts'])==1,final
        assert client.get(final['artifacts'][0]['url']).content==payload.content
        assert time.perf_counter()-start<5
    turn=runtime.store.list_recent_conversation_events('web:archive-handoff',event_type='conversation.chat_turn')[-1]
    from nullion.artifacts import parse_media_directive_line
    canonical='\n'.join(line for line in turn['assistant_reply'].splitlines()
        if parse_media_directive_line(line) is None).strip()
    assert final['text']==canonical
    receipts=[r for r in turn['tool_results'] if r['tool_name']=='archive_create']
    assert [r['status'] for r in receipts]==['failed','completed']
    assert receipts[-1]['output']['artifact_path']==str(expected)
    assert turn['response_fulfilled'] is True
