"""Declared intermediate files need verified creation, without final delivery."""
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize('change',['new','modified','unchanged','missing'])
@pytest.mark.parametrize('extension',['.png','.xlsx'])
def test_declared_outputs_require_fresh_bytes_and_keep_their_role(tmp_path,monkeypatch,change,extension):
    from nullion import tools
    from nullion.tools import ToolInvocation
    path=tmp_path/('result'+extension)
    if change in ('modified','unchanged'):path.write_bytes(b'old')
    monkeypatch.setattr(tools,'_workspace_artifact_root_for_principal',lambda _:tmp_path)
    monkeypatch.setattr(tools,'_terminal_deliverable_artifact_paths_since',
        lambda *a,**kw:pytest.fail('Exact declared paths must not scan broad roots'))
    class Backend:
        def run(self,*args,**kw):
            if change in ('new','modified'):path.write_bytes(b'fresh verified output')
            return SimpleNamespace(stdout='',stderr='',exit_code=0,shell='sh')
    handler=tools._build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path],terminal_executor_backend=Backend())
    result=handler(ToolInvocation('owned','terminal_exec','workspace_admin',
        {'command':'render','output_paths':[str(path)]},flow_context={'requested_artifact_extensions':['.xlsx']}))
    if change in ('unchanged','missing'):
        assert result.status=='failed' and result.output['reason']=='declared_outputs_not_created'
        assert not result.output.get('artifact_paths') and not result.output.get('generated_outputs')
    elif extension=='.png':
        assert result.status=='completed' and not result.output.get('artifact_paths')
        assert result.output['generated_outputs'][0]['path']==str(path)
        assert result.output['generated_outputs'][0]['role']=='intermediate'
        view=__import__('nullion.agent_orchestrator',fromlist=['_compact_tool_output_for_model_context'])._compact_tool_output_for_model_context('terminal_exec',result.output)
        assert view['generated_outputs']==result.output['generated_outputs']
    else:
        assert result.status=='completed' and result.output['artifact_paths']==[str(path)]
        assert result.output['artifact_descriptors'][0]['role']=='deliverable'


@pytest.mark.parametrize('case',['not_list','empty','outside','unsupported'])
def test_invalid_declared_output_returns_permitted_paths_without_executing(tmp_path,case):
    from nullion.tools import ToolInvocation,_build_terminal_exec_handler
    from nullion.tool_repair_contract import tool_repair_kind
    class Backend:
        def run(self,*args,**kwargs):pytest.fail('Invalid output paths must not execute a command')
    values={'not_list':'result.png','empty':[''],'outside':[str(tmp_path.parent/'outside.png')],
        'unsupported':[str(tmp_path/'program.exe')]}
    result=_build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path],terminal_executor_backend=Backend())(
        ToolInvocation('invalid','terminal_exec','workspace_admin',{'command':'render','output_paths':values[case]}))
    assert result.status=='failed' and result.output['executed'] is False
    assert result.output['command_executed'] is False and tool_repair_kind(result)=='correct_arguments'
    constraints=result.output['recovery']['argument_constraints']
    assert constraints['allowed_roots']==[str(tmp_path)] and constraints['suggested_scratch_root']==str(tmp_path)
    assert '.png' in constraints['supported_extensions']
