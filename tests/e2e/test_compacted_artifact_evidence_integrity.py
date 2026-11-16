"""A report must recover omitted middle facts through the real retained-evidence tool."""
import json
import os
from pathlib import Path

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker workflow boundary')


@pytest.mark.parametrize('prompt', ['Create the HTML report using the exact required value in the source.',
                                   '資料内の正確な必須値を使ってHTMLレポートを作成してください。'])
def test_focused_report_inspects_omitted_fact_and_delivers_exact_bytes(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_file_read_handler, _build_file_write_handler
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    from datetime import datetime, UTC
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED','0')
    monkeypatch.setenv('NULLION_MODEL_TOOL_RESULT_MAX_CHARS','12000')
    source=tmp_path/'source.txt'
    source.write_text('a'*20000+'REQUIRED_VALUE=73\n'+'z'*20000)
    principal='web:compressed-report'
    output=artifact_root_for_principal(principal)/'report.html'
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_read','Read source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5),
                      _build_file_read_handler(tmp_path,allowed_roots=[tmp_path]))
    registry.register(ToolSpec('file_write','Write report',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5,
                              produced_artifact_extensions=('.html',)),
                      _build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if 'select_attachment_format' in {t['name'] for t in kwargs.get('tools',[])}:
                return call('select_attachment_format',{'extension':'.html','confidence':1})
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_read','file_write'],
                'required_tool_names':['file_read','file_write'],'artifact_extensions':['.html']})
            if self.phase==2:return call('file_read',{'path':str(source)})
            if self.phase==3:
                assert 'inspect_retained_evidence' in {t['name'] for t in kwargs['tools']}
                packets=[json.loads(m['content'][0]['text'].split('\n',1)[1]) for m in kwargs['messages']
                    if isinstance(m.get('content'),list) and m['content'] and m['content'][0].get('type')=='text'
                    and m['content'][0]['text'].startswith('Runtime evidence index.')]
                record=next(r for r in packets[-1]['records'] if r['tool_name']=='file_read')
                return call('inspect_retained_evidence',{'requests':[{'invocation_id':record['invocation_id'],
                    'json_pointer':'/content','offset':20000,'length':18}]})
            if self.phase==4:
                assert 'REQUIRED_VALUE=73' in json.dumps(kwargs['messages']), [str(m.get('content'))[-3000:] for m in kwargs['messages'][-2:]]
                return call('file_write',{'path':str(output),'content':'<html><body><h1>Exact result</h1><p>REQUIRED_VALUE=73</p></body></html>'})
            assert self.phase==5,'Do not reread the external source or restart the report'
            return text('Report attached. Confirmed value: 73.\nMEDIA:'+str(output))
    model=Model();runtime.active_tool_registry=registry
    with TestClient(web_app.create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        response=client.post('/api/chat',json={'text':prompt+' Source: '+str(source),'conversation_id':principal})
        assert response.status_code==200,response.text
        data=response.json()
        assert len(data['artifacts'])==1,(data,model.phase)
        downloaded=client.get(data['artifacts'][0]['url'])
        assert downloaded.content==output.read_bytes() and b'REQUIRED_VALUE=73' in downloaded.content
    turns=[v for v in runtime.store.list_conversation_events(principal) if v.get('event_type')=='conversation.chat_turn']
    results=turns[-1]['tool_results']
    assert sum(v['tool_name']=='file_read' for v in results)==1
    inspection=next(v['output'] for v in results if v['tool_name']=='inspect_retained_evidence')
    assert inspection['windows'][0]['content'].startswith('REQUIRED_VALUE=73')
    assert source.read_text()=='a'*20000+'REQUIRED_VALUE=73\n'+'z'*20000
