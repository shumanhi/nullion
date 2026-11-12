import pytest
from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolInvocation,ToolResult
from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision

@pytest.mark.parametrize('name,description',[('q7','Optional expensive reader'),('z9','追加の読み取り'),('x3','Lectura opcional')])
def test_explicit_metadata_is_not_activated_by_broad_scope_or_read_continuation(name,description):
    registry=ToolRegistry();calls=[]
    def read(inv):calls.append(inv.tool_name);return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':37})
    registry.register(ToolSpec('r0','Ordinary reader',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,continuation_tools=(name,)),read)
    registry.register(ToolSpec(name,description,ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,capability_tags=('explicit_scope_only',)),read)
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(current_user_message=description),tool_scope_decision=TurnToolScopeDecision(valid=True,web_action='live_research',requested_tool_names=('r0',)))
    assert name not in {r['name'] for r in scoped.list_tool_definitions()}
    assert not scoped.can_invoke_tool(name)
    hidden=scoped.invoke(ToolInvocation('hidden',name,'user:fixture',{}));assert hidden.status!='completed' and not calls
    result,widened=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','user:fixture',{'capabilities':[],'tool_names':[name],'required_tool_names':[name]}))
    assert result.status=='completed' and widened.can_invoke_tool(name),result
    assert widened.invoke(ToolInvocation('run',name,'user:fixture',{})).output['value']==37
    assert calls==[name]


def test_browser_adapter_declares_full_worker_optional_but_explicit_request_keeps_it_available(monkeypatch):
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin import browser_use_adapter as adapter
    monkeypatch.setattr(adapter,'browser_use_available',lambda:True);monkeypatch.setattr(adapter,'browser_use_python',lambda:'/fixture/sdk/python')
    registry=ToolRegistry();register_browser_tools(registry,model_client_getter=lambda:None)
    assert 'explicit_scope_only' in registry.get_spec('browser_run_task').capability_tags
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(current_user_message='資料を確認して。'),tool_scope_decision=TurnToolScopeDecision(valid=True,web_action='live_research'))
    result,readers=scoped.apply_scope_request(ToolInvocation('read','request_tool_scope','user:fixture',{'capabilities':['web'],'tool_names':['browser_navigate','browser_extract_detail']}))
    assert result.status=='completed' and readers.can_invoke_tool('browser_navigate')
    assert not readers.can_invoke_tool('browser_run_task')
    assert 'browser_run_task' not in result.output['available_tools']
    result,worker=readers.apply_scope_request(ToolInvocation('worker','request_tool_scope','user:fixture',{'capabilities':['web'],'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task']}))
    assert result.status=='completed' and worker.can_invoke_tool('browser_run_task'),result


def test_artifact_companion_expansion_does_not_bypass_explicit_metadata():
    registry=ToolRegistry()
    registry.register(ToolSpec('file_write','Write file',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5),lambda i:None)
    registry.register(ToolSpec('file_read','Optional inspector',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,capability_tags=('explicit_scope_only',)),lambda i:None)
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(current_user_message='出力を作成。',requested_extensions=('.html',)),tool_scope_decision=TurnToolScopeDecision(valid=True))
    result,widened=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','user:fixture',{'capabilities':['local_files'],'tool_names':['file_write'],'required_tool_names':['file_write'],'artifact_extensions':['.html']}))
    assert result.status=='completed' and widened.can_invoke_tool('file_write')
    assert not widened.can_invoke_tool('file_read')


@pytest.mark.parametrize('label',['Compare the records.','Compara los registros.','記録を比較してください。'])
def test_optional_heavy_workflow_does_not_displace_required_readers(label):
    registry=ToolRegistry()
    def read(inv):return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'value':37})
    registry.register(ToolSpec('r0','Ordinary reader',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5),read)
    registry.register(ToolSpec('h9','Full workflow',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,capability_tags=('explicit_scope_only','required_scope_only')),read)
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(current_user_message=label),tool_scope_decision=TurnToolScopeDecision(valid=True))
    result,readers=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','user:fixture',{'capabilities':[],'tool_names':['r0','h9'],'required_tool_names':['r0']}))
    assert result.status=='completed' and readers.can_invoke_tool('r0')
    assert not readers.can_invoke_tool('h9') and 'h9' not in result.output['available_tools']
    result,worker=readers.apply_scope_request(ToolInvocation('worker','request_tool_scope','user:fixture',{'capabilities':[],'tool_names':['h9'],'required_tool_names':['h9']}))
    assert result.status=='completed' and worker.can_invoke_tool('h9')
    assert worker.invoke(ToolInvocation('run','h9','user:fixture',{})).output['value']==37
