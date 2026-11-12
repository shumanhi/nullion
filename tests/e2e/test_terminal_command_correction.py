"""A failing command does not retire core execution for the rest of the turn."""
from tests.workflow_provider_protocol import workflow_provider_reply
from datetime import UTC,datetime
import json,os,shlex,sys
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

@pytest.mark.parametrize('prompt',['Create the CSV, verify it, and attach the final file.','Crea el CSV, verifícalo y adjunta el archivo final.','CSVを作成して検証し、完成したファイルを添付してください。'])
@pytest.mark.parametrize('repeat',[False,True])
def test_core_exec_corrects_command_and_bounds_identical_retries(tmp_path,monkeypatch,prompt,repeat,preflight=False,scratch_preflight=False):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_terminal_exec_handler,_build_file_write_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');monkeypatch.setenv('NULLION_REPEATED_TOOL_FAILURE_LIMIT','2');chat_store._store=None
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('terminal_exec','Execute a permitted command',ToolRiskLevel.HIGH,ToolSideEffectClass.DANGEROUS_EXEC,True,20),_build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))
    registry.register(ToolSpec('file_write','Alternative file writer',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    class Provider:
        phase=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={r['name'] for r in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(v):return {'stop_reason':'end_turn','content':[{'type':'text','text':v}]}
            def tool(n,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':n,'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Deliver verified CSV','requirement_kind':'deliverable','evidence_subject_id':'report','eligible_evidence_tool_names':['terminal_exec']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                data=json.loads(kw['messages'][0]['content'][0]['text']);ids=[r['invocation_id'] for r in data['tool_evidence'] if r['tool_name']=='terminal_exec' and r.get('status')=='completed']
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':[{**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids[-1:]} for r in data['material_requirement_contract']],'unresolved_requirements':[]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.csv','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_shell'],'tool_names':['terminal_exec'],'required_tool_names':['terminal_exec'],'artifact_extensions':['.csv']})
            if self.phase in (2,3):
                assert 'terminal_exec' in names, 'A process exit incorrectly retired core execution'
                code="raise AssertionError('verification expected a different heading')" if self.phase==2 or repeat else "from pathlib import Path;p=Path('approved.csv');p.write_text('Name,Value\\nApproved,12\\n');assert 'Approved,12' in p.read_text()"
                command=shlex.quote(sys.executable)+' -c '+shlex.quote(code)
                if preflight and self.phase==2:command='rm -f never-created-for-this-fixture.csv; '+command
                if scratch_preflight and self.phase==2:
                    command='printf preview > /tmp/nullion-render-'+tmp_path.name+'.out'
                return tool('terminal_exec',{'command':command})
            assert self.phase==4 and not repeat,'The identical failure guard did not stop the request'
            return text('The verified CSV is attached.')
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,permission='tool:terminal_exec',granted_by='fixture',granted_at=datetime.now(UTC)));runtime.checkpoint();model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:command-repair'});assert response.status_code==200,response.text;data=response.json()
        turns=[r for r in runtime.store.list_conversation_events('web:command-repair') if r.get('event_type')=='conversation.chat_turn'];tools=turns[-1]['tool_results'];commands=[r for r in tools if r['tool_name']=='terminal_exec']
        assert len(commands)==2 and sum(r['tool_name']=='request_tool_scope' for r in tools)==1,tools
        if repeat:
            assert not data['artifacts'] and all(r['status']=='failed' for r in commands) and model.phase==3,data
        else:
            assert [r['status'] for r in commands]==['denied' if scratch_preflight else 'failed','completed'],commands
            assert len(data['artifacts'])==1 and model.phase==4,data
            assert client.get(data['artifacts'][0]['url']).content==b'Name,Value\nApproved,12\n'
        assert 'Tool scope updated' not in data['text'],data


@pytest.mark.parametrize('prompt',['Create and verify the CSV.','Crea y verifica el CSV.','CSVを作成して検証してください。'])
def test_preflight_correction_preserves_exec_with_alternative_producers(tmp_path,monkeypatch,prompt):
    test_core_exec_corrects_command_and_bounds_identical_retries(tmp_path,monkeypatch,prompt,False,preflight=True)


@pytest.mark.parametrize('prompt',['Create and check the final CSV.','Crea y comprueba el CSV final.','完成したCSVを作成して確認してください。'])
def test_internal_render_logs_recover_into_workspace_without_permission_prompt(tmp_path,monkeypatch,prompt):
    test_core_exec_corrects_command_and_bounds_identical_retries(tmp_path,monkeypatch,prompt,False,
        scratch_preflight=True)
    assert not (__import__('pathlib').Path('/tmp')/('nullion-render-'+tmp_path.name+'.out')).exists()
