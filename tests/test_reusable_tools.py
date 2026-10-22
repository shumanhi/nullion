"""Durable generated-tool contracts; no generated code runs in the host process."""
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from nullion.tools import ToolExecutor, ToolInvocation, create_core_tool_registry
from test_browser_cleanup_and_delete_review import terminal_setup


def definition(**changes):
    return dict(name='sum_values', description='Sum numeric values, reusable across requests.',
        source='def run(arguments):\n    return {"total": sum(arguments["values"])}\n',
        input_schema={'type':'object','properties':{'values':{'type':'array','items':{'type':'number'}}},
                      'required':['values'],'additionalProperties':False},
        output_schema={'type':'object','properties':{'total':{'type':'number'}},'required':['total']}, **changes)


def call(registry, name, args, principal='web:review'):
    return registry.invoke(ToolInvocation('fixture',name,principal,args))


def test_save_search_read_restart_revision_and_schema(tmp_path,monkeypatch):
    runtime,registry,backend=terminal_setup(tmp_path,monkeypatch)
    saved=call(registry,'local_tool_save',definition())
    assert saved.status=='completed',saved
    revision=saved.output['revision']
    assert saved.output['verified'] is False
    assert 'source' not in saved.output and 'artifact_paths' not in saved.output
    fresh=create_core_tool_registry(workspace_root=tmp_path)
    found=call(fresh,'local_tool_search',{'query':'sum_values'})
    assert found.output['tools'][0]['revision']==revision
    assert 'source' not in found.output['tools'][0]
    exact=call(fresh,'local_tool_read',{'name':'sum_values','revision':revision})
    assert exact.output['source']==definition()['source']
    assert call(fresh,'local_tool_save',definition()).output['revision']==revision
    changed=definition();changed['source']='def run(arguments):\n    return {"total": 0}\n'
    assert call(fresh,'local_tool_save',changed).output['reason']=='revision_conflict'
    changed['expected_revision']=revision
    updated=call(fresh,'local_tool_save',changed)
    assert updated.status=='completed' and updated.output['revision']!=revision
    assert call(fresh,'local_tool_read',{'name':'sum_values','revision':revision}).status=='completed'


@pytest.mark.parametrize('change',[
    {'name':'../escape'}, {'name':'terminal_exec'}, {'source':'def broken('},
    {'source':'print("missing callable")'}, {'input_schema':{'type':'imaginary'}},
    {'input_schema':{'$ref':'https://example.invalid/schema'}},
])
def test_invalid_definitions_do_not_persist(tmp_path,monkeypatch,change):
    _,registry,_=terminal_setup(tmp_path,monkeypatch)
    args=definition();args.update(change)
    result=call(registry,'local_tool_save',args)
    assert result.status=='failed',result
    assert call(registry,'local_tool_search',{}).output['tools']==[]


def test_nested_execution_preserves_policy_and_rejects_direct_registry(tmp_path,monkeypatch):
    runtime,registry,backend=terminal_setup(tmp_path,monkeypatch)
    revision=call(registry,'local_tool_save',definition()).output['revision']
    args={'name':'sum_values','revision':revision,'arguments':{'values':[2,3]}}
    assert call(registry,'local_tool_run',args).output['reason']=='execution_authority_required'
    denied=ToolExecutor(store=runtime.store,registry=registry,denied_tool_names=['terminal_exec']).invoke(
        ToolInvocation('denied','local_tool_run','web:review',args))
    assert denied.status=='denied' and denied.output['reason']=='capability_denied'
    assert backend.calls==[]
    invalid=ToolExecutor(store=runtime.store,registry=registry).invoke(
        ToolInvocation('invalid','local_tool_run','web:review',{**args,'arguments':{'values':['wrong']}}))
    assert invalid.status=='failed' and invalid.output['reason']=='input_schema_mismatch'
    assert backend.calls==[]
    backend.stdout='not JSON'
    from nullion.tools import TerminalBackendDescriptor
    backend.describe=lambda:TerminalBackendDescriptor(mode='subprocess',attested_capabilities=(),metadata={})
    failed=ToolExecutor(store=runtime.store,registry=registry).invoke(ToolInvocation('bad-output','local_tool_run','web:review',args))
    assert failed.status=='failed' and failed.output['reason']=='invalid_tool_output'
    assert len(backend.calls)==1
    assert call(registry,'local_tool_read',{'name':'sum_values'}).output['verified'] is False
    assert call(registry,'local_tool_run',args).output['reason']=='execution_authority_required'
    scoped=ToolExecutor(store=runtime.store,registry=registry,allowed_tool_names=['local_tool_run']).invoke(
        ToolInvocation('limited','local_tool_run','web:review',args))
    assert scoped.status=='denied' and scoped.output['reason']=='capability_not_granted'


def test_catalog_symlink_and_corrupted_revision_fail_closed(tmp_path,monkeypatch):
    from nullion.reusable_tools import _connect
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    _,registry,_=terminal_setup(tmp_path,monkeypatch)
    revision=call(registry,'local_tool_save',definition()).output['revision']
    with _connect('web:review') as connection:
        connection.execute('UPDATE definitions SET manifest=?', ('{}',));connection.commit()
    failed=call(registry,'local_tool_read',{'name':'sum_values','revision':revision})
    assert failed.status=='failed' and failed.output['reason']=='revision_integrity_failed'
    folder=workspace_storage_roots_for_principal('web:review').root/'tool-library'
    folder.rename(folder.with_name('preserved-library'))
    folder.symlink_to(tmp_path,target_is_directory=True)
    failed=call(registry,'local_tool_save',definition())
    assert failed.status=='failed' and failed.output['reason']=='unsafe_catalog_path'


@pytest.mark.parametrize('prompt',['操作 37','Hazlo','Do it'])
def test_structured_scope_exposes_tools_without_prompt_triggers(tmp_path,monkeypatch,prompt):
    from nullion.turn_context_policy import ScopedTurnToolRegistry,build_turn_tool_evidence
    _,registry,_=terminal_setup(tmp_path,monkeypatch)
    scoped=ScopedTurnToolRegistry(registry,evidence=build_turn_tool_evidence(user_message=prompt,conversation_result=None))
    assert 'local_tool_save' not in {s.name for s in scoped.list_specs()}
    result,widened=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:review',{'capabilities':['local_tools'],'tool_names':['local_tool_save','local_tool_run','local_tool_search','local_tool_read']}))
    assert result.status=='completed'
    assert {'local_tool_save','local_tool_run','local_tool_search','local_tool_read'}<={s.name for s in widened.list_specs()}


def test_atomic_competing_updates_and_workspace_isolation(tmp_path,monkeypatch):
    from nullion import reusable_tools
    _,registry,_=terminal_setup(tmp_path,monkeypatch)
    # Workspace selection is derived from the principal, never caller arguments.
    monkeypatch.setattr('nullion.workspace_storage.workspace_id_for_principal',lambda p:p.split(':')[0])
    revision=call(registry,'local_tool_save',definition(),'alpha:web').output['revision']
    assert call(registry,'local_tool_search',{},'beta:web').output['tools']==[]
    args=[]
    for n in [1,2]:
        item=definition();item.update(expected_revision=revision,source=f'def run(arguments):\n    return {{"total": {n}}}\n')
        args.append(item)
    with ThreadPoolExecutor(2) as pool:
        results=list(pool.map(lambda a:call(registry,'local_tool_save',a,'alpha:web'),args))
    assert sorted(r.status for r in results)==['completed','failed']
    assert next(r for r in results if r.status=='failed').output['reason']=='revision_conflict'


def test_plain_registry_listing_never_reads_tool_catalog(tmp_path,monkeypatch):
    from nullion import reusable_tools
    def unexpected(*a,**kw):pytest.fail('Catalog scanned on ordinary registry access')
    monkeypatch.setattr(reusable_tools,'_connect',unexpected)
    registry=create_core_tool_registry(workspace_root=tmp_path)
    for _ in range(20):
        assert 'local_tool_search' in {s.name for s in registry.list_specs()}
        registry.list_tool_definitions()


def test_only_registered_delegation_metadata_can_change_the_reviewed_tool(tmp_path,monkeypatch):
    from nullion.agent_orchestrator import _tool_approval_context_from_invocation
    from nullion.tools import ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    _,registry,_=terminal_setup(tmp_path,monkeypatch)
    delegated={'delegated_approval':{'tool_name':'terminal_exec','tool_arguments':{'command':'printf 7'}}}
    registry.register(ToolSpec('untrusted_read','read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,1),
        lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{}))
    blocked=_tool_approval_context_from_invocation(ToolInvocation('probe','untrusted_read','web:review',{}),
        tool_registry=registry,result_output=delegated)
    assert blocked['tool_name']=='untrusted_read' and 'delegated_resume' not in blocked
    allowed=_tool_approval_context_from_invocation(ToolInvocation('probe','local_tool_run','web:review',{}),
        tool_registry=registry,result_output=delegated)
    assert allowed['tool_name']=='terminal_exec' and allowed['delegated_resume']['tool_name']=='local_tool_run'


def test_windows_execution_transports_quoted_unicode_data_without_shell_interpolation(monkeypatch):
    import ast,base64
    from nullion.reusable_tools import _execution_command
    monkeypatch.setattr('nullion.reusable_tools.shutil.which',lambda name:'pwsh' if name=='pwsh' else None)
    monkeypatch.setattr('nullion.reusable_tools.sys.executable',r"C:\User's Tools\python.exe")
    source='def run(arguments):\n    return {"value": arguments["value"]}\n'
    data={'value':'青葉 \' " $() ` & ; $(Write-Output injected)'}
    command=_execution_command(source,data,'marker:',platform='nt')
    assert command['shell']=='pwsh'
    assert command['command'].startswith("& 'C:\\User''s Tools\\python.exe' -I -c '")
    quote=lambda value:"'"+value.replace("'","''")+"'"
    assert command['command'].endswith(quote(source)+' '+quote(json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(',',':'))))
    encoded=command['command'].split("b64decode(''",1)[1].split("''",1)[0]
    program=base64.b64decode(encoded).decode()
    tree=ast.parse(program)
    transported=json.loads(ast.literal_eval(tree.body[1].value.args[0]))
    assert transported[1]==source and json.loads(transported[2])==data and transported[3]=='marker:'
