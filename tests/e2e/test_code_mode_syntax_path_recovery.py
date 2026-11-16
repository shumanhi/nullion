"""A rejected program defines no variables or files; recovery uses exact native roots."""
import json
import os
import time

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


def test_syntax_and_output_path_recovery_delivers_once_without_replaying(tmp_path,monkeypatch,record_property):
    from datetime import UTC,datetime
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler,_build_terminal_exec_handler
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE','on')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('local',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);native=[]
    for name,builder in [('file_write',_build_file_write_handler),('terminal_exec',_build_terminal_exec_handler)]:
        handler=builder(tmp_path,allowed_roots=[tmp_path])
        def run(invocation,handler=handler):
            result=handler(invocation);native.append(result);return result
        registry.register(ToolSpec(name,'Create local output',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),run)
    class Model:
        phase=0
        errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            try:return self.respond(**kw)
            except Exception as error:self.errors.append(repr(error));raise
        def respond(self,**kw):
            if not kw.get('tools'):return text('{}')
            self.phase+=1;time.sleep(.08)
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files','local_shell'],
                'tool_names':['file_write','terminal_exec'],'required_tool_names':['file_write'],'artifact_extensions':['.html']})
            if self.phase==2:
                program="call_tool('file_write', "+repr({'path':str(self.target),'content':'must not be written'})+")\nscript = 'unterminated"
                result=call('code_mode',{'tool_names':['file_write'],'code':program});result['content'][0]['id']='broken-program';return result
            if self.phase==3:
                assert not self.target.exists() and not native[self.native_start:]
                payload=next(json.JSONDecoder().raw_decode(b['text'])[0] for m in reversed(kw['messages'])
                    if isinstance(m.get('content'),list) for part in m['content']
                    if isinstance(part,dict) and part.get('type')=='tool_result' and part.get('tool_use_id')=='broken-program'
                    for b in part.get('content',[]) if b.get('type')=='text')
                assert payload['output']['execution_error']['program_executed'] is False
                bad={'command':'printf unexpected_execution','output_paths':[str(tmp_path.parent/'outside.html')]}
                program="attempt=call_tool('terminal_exec', "+repr(bad)+", on_error='return')\n"
                program+="assert attempt['output']['executed']==False, 'preflight must not execute'\n"
                program+="allowed=attempt['output']['recovery']['argument_constraints']['allowed_roots']\n"
                program+="assert len(allowed)>0, 'roots required'\n"
                program+="document="+repr('<!doctype html><html><body><h1>'+self.label+'</h1><p>Verified value 37.</p></body></html>')+"\n"
                program+="call_tool('file_write', {'path':"+repr(str(self.target))+",'content':document})"
                result=call('code_mode',{'tool_names':['file_write','terminal_exec'],'code':program});result['content'][0]['id']='corrected-program';return result
            assert self.phase==4,'Recovery added an unexpected model round'
            return text('Completed the document.\nMEDIA:'+str(self.target))
    model=Model();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,label in enumerate(['確認済み','Verificado','Vérifié']):
            cid='web:syntax-recovery-'+str(index);root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
            model.phase=0;model.label=label;model.target=root/f'verified-{index}.html';model.native_start=len(native)
            started=time.monotonic();response=client.post('/api/chat',json={'text':label+' '+model.target.name,'conversation_id':cid})
            samples.append(time.monotonic()-started)
            assert not model.errors,model.errors
            assert response.status_code==200,response.text
            artifacts=response.json().get('artifacts',[]);assert len(artifacts)==1,response.text
            assert label in client.get(artifacts[0]['url']).content.decode()
            assert [r.tool_name for r in native[model.native_start:]]==['terminal_exec','file_write']
            assert native[-2].output['command_executed'] is False and native[-1].status=='completed'
    assert sum(samples)/len(samples)<3 and max(samples)<5,samples
    record_property('mean_syntax_path_recovery_seconds',sum(samples)/len(samples))
