"""A single real Code Mode program selects data, quotes Python and delivers HTML."""
import os
import time
from html import escape

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


def test_iterator_selection_and_shell_helpers_complete_three_real_api_requests(tmp_path,monkeypatch,record_property):
    from datetime import UTC,datetime
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_terminal_exec_handler
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on');monkeypatch.setenv('NULLION_TERMINAL_ENABLED','true')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('terminal_exec','Run a local artifact producer',ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC,False,10),_build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))
    class Provider:
        rounds=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            names={r['name'] for r in kw.get('tools',[])}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return text('{}')
            self.rounds+=1;time.sleep(.05)
            assert self.rounds<=3,'A data selection or shell quoting error re-entered the model'
            if self.rounds==1:return call('request_tool_scope',{'capabilities':['local_shell'],
                'tool_names':['terminal_exec'],'required_tool_names':['terminal_exec'],'artifact_extensions':['.html']})
            if self.rounds==2:
                source=("import shlex\nfrom html import escape\nrows=[{'value':"+repr(self.payload)+"}]\n"
                    "selected=next(iter(row['value'] for row in rows), '')\n"
                    "markup='<html><body><pre>'+escape(selected)+'</pre></body></html>'\n"
                    "script='from pathlib import Path\\np=Path('+repr("+repr(str(self.path))+" )+')\\np.parent.mkdir(parents=True,exist_ok=True)\\np.write_text('+repr(markup)+',encoding=\"utf-8\")'\n"
                    "command=shlex.join(['python3','-c',script])\n"
                    "call_tool('terminal_exec',{'command':command,'output_paths':["+repr(str(self.path))+"]})")
                return call('code_mode',{'tool_names':['terminal_exec'],'code':source})
            return text('Created the requested HTML.\n\nMEDIA:'+str(self.path))
    model=Provider();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for i,(prompt,payload) in enumerate([('ページを .html に保存','東京 "37" & café\n2行目'),
                ('Guarda una página .html',"Café '37'; dos líneas\nsegunda línea"),
                ('احفظ الصفحة .html','مرحبا "37" & <37>')]):
            model.rounds=0;model.payload=payload
            cid='web:quoted-terminal-'+str(i);model.path=artifact_root_for_principal(cid)/('quoted-'+str(i)+'.html')
            start=time.perf_counter();response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
            samples.append(time.perf_counter()-start)
            assert response.status_code==200,response.text
            data=response.json();assert len(data.get('artifacts',[]))==1,data
            expected=('<html><body><pre>'+escape(payload)+'</pre></body></html>').encode()
            assert client.get(data['artifacts'][0]['url']).content==expected
            turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True
            assert all(row['status']=='completed' for row in turn['tool_results']),turn['tool_results']
            programs=[r for r in turn['tool_results'] if r['tool_name']=='code_mode']
            assert len(programs)==1 and programs[0]['output']['host_calls']=={'html.escape':1,'shlex.join':1}
            assert len([r for r in turn['tool_results'] if r['tool_name']=='terminal_exec'])==1
    assert sum(samples)/len(samples)<3 and max(samples)<5,samples
    record_property('mean_quoted_terminal_seconds',sum(samples)/len(samples))
