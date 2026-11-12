"""Validate scope plans against actual registration before installing obligations."""
from nullion.tools import ToolInvocation,ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision


def test_unknown_requirement_does_not_change_scope_or_weaken_registered_effect():
    registry=ToolRegistry()
    for name,effect in [('file_write',ToolSideEffectClass.WRITE),('email_send',ToolSideEffectClass.WRITE)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,True,10),
            lambda invocation:ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{}))
    decision=TurnToolScopeDecision(valid=True,requested_tool_names=('file_write',),required_tool_names=('file_write',),
        requested_artifact_extensions=('.csv',))
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence(requested_extensions=('.csv',)),tool_scope_decision=decision)
    before=scoped.turn_tool_scope_decision
    failed,same=scoped.apply_scope_request(ToolInvocation('invalid','request_tool_scope','web:fixture',{
        'capabilities':['local_files'],'tool_names':['file_write'],'required_tool_names':['web_fetch']}))
    assert same is scoped and scoped.turn_tool_scope_decision is before
    assert failed.status=='failed' and failed.output['scope_changed'] is False
    assert failed.output['reason']=='invalid_tool_arguments'
    assert failed.output['unavailable_required_tool_names']==['web_fetch']
    assert failed.output['recovery']['kind']=='correct_arguments'
    assert not scoped.can_invoke_tool('email_send')
    corrected,widened=scoped.apply_scope_request(ToolInvocation('valid','request_tool_scope','web:fixture',{
        'capabilities':['local_files'],'tool_names':['file_write'],'required_tool_names':[]}))
    assert corrected.status=='completed' and corrected.output['required_tool_names']==['file_write']
    assert widened.get_spec('file_write').requires_approval is True
    assert not widened.can_invoke_tool('email_send')


def test_advertised_exact_scope_choices_follow_runtime_registration_without_changing_global_schema():
    registry=ToolRegistry()
    registry.register(ToolSpec('terminal_exec','Core execution',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),lambda invocation:None)
    scoped=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence())
    for spec in [scoped.get_spec('request_tool_scope'),next(s for s in scoped.list_specs() if s.name=='request_tool_scope')]:
        for key in ('tool_names','required_tool_names'):
            assert spec.input_schema['properties'][key]['items']['enum']==['terminal_exec']
    definition=next(row for row in scoped.list_tool_definitions() if row['name']=='request_tool_scope')
    assert definition['input_schema']['properties']['tool_names']['items']['enum']==['terminal_exec']
    registry.register(ToolSpec('web_fetch','Public fetch',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),lambda invocation:None)
    assert scoped.get_spec('request_tool_scope').input_schema['properties']['tool_names']['items']['enum']==['terminal_exec','web_fetch']
    empty=ScopedTurnToolRegistry(ToolRegistry(),evidence=TurnToolEvidence())
    assert empty.get_spec('request_tool_scope').input_schema['properties']['tool_names']['maxItems']==0
    assert definition['input_schema']['properties']['tool_names']['items']['enum']==['terminal_exec']
