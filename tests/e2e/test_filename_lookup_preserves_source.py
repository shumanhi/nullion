"""Mentioning a recently delivered filename does not request another artifact."""
import hashlib
import os
import time

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('mode',['off','on'])
def test_recent_filename_lookup_stays_read_only_without_a_new_scope_round(tmp_path,monkeypatch,mode,record_property):
    from datetime import UTC,datetime
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import _remember_chat_turn
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_search_handler,_build_file_write_handler
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE',mode)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('files',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    root=artifact_root_for_principal('workspace_admin');root.mkdir(parents=True,exist_ok=True)
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);calls=[]
    for name,builder,effect in [('file_search',_build_file_search_handler,ToolSideEffectClass.READ),
        ('file_write',_build_file_write_handler,ToolSideEffectClass.WRITE)]:
        handler=builder(tmp_path,allowed_roots=[tmp_path])
        def invoke(inv,handler=handler):calls.append(inv.tool_name);return handler(inv)
        registry.register(ToolSpec(name,'Inspect or write selected local file',ToolRiskLevel.LOW,effect,False,5),invoke)
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if 'select_attachment_format' in {r['name'] for r in kw.get('tools',[])}:
                return call('select_attachment_format',{'extension':None,'confidence':1})
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:
                args={'pattern':self.filename,'root':str(root),'limit':10}
                return call('code_mode',{'tool_names':['file_search'],'code':"call_tool('file_search', "+repr(args)+")"}) if mode=='on' else call('file_search',args)
            assert self.phase==2,'A filename-only lookup forced another production/scope round'
            return text(self.filename)
    model=Model();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,prompt in enumerate(['Search for {name}; reply with its filename only. Do not attach or modify files.',
            'Busca {name}; responde solo con el nombre, sin adjuntar ni modificar archivos.',
            '{name} を検索し、ファイル名だけ答えてください。添付も変更も不要です。']):
            cid='web:filename-only-'+str(index);model.phase=0;model.filename=f'stage_identity_{index}.csv'
            path=root/model.filename;path.write_text('Name,Value\nCedar,37\n');before=path.stat().st_mtime_ns
            digest=hashlib.sha256(path.read_bytes()).hexdigest()
            _remember_chat_turn(runtime,chat_id=cid,conversation_id=cid,conversation_turn_id='prior-'+str(index),
                user_message='Create the selected CSV.',assistant_reply='Created.\nMEDIA:'+str(path))
            start=time.monotonic();response=client.post('/api/chat',json={'conversation_id':cid,'text':prompt.format(name=model.filename)})
            samples.append(time.monotonic()-start)
            assert response.status_code==200,response.text
            assert response.json()['text']==model.filename and not response.json().get('artifacts'),response.text
            assert path.stat().st_mtime_ns==before and hashlib.sha256(path.read_bytes()).hexdigest()==digest
            turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True
            assert [r['tool_name'] for r in turn['tool_results'] if r['tool_name']!='code_mode']==['file_search']
            assert all(r['status']=='completed' for r in turn['tool_results'])
    assert calls==['file_search']*3
    assert sum(samples)/len(samples)<3 and max(samples)<5,samples
    record_property('mean_filename_lookup_seconds',sum(samples)/len(samples))
