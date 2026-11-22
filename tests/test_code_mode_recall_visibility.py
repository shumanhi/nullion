"""Native presentation never grants extra program authority."""
from nullion import code_mode
from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult


def test_dual_visibility_is_generic_metadata_and_rechecks_current_scope(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    native_calls=[]
    base=ToolRegistry()
    base.register(ToolSpec('照会','Read exact context',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3,
        code_mode_allowed=True,code_mode_keep_native=True,input_schema={'type':'object'}),
        lambda invocation:native_calls.append(invocation) or ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{}))
    class Scope:
        allowed=True
        def get_spec(self,name):return base.get_spec(name)
        def list_tool_definitions(self):return base.list_tool_definitions()
        def can_invoke_tool(self,name):return self.allowed
    scoped=Scope();state={'tool_registry':scoped,'messages':[],'principal_id':'owned-principal'}
    offered=code_mode.offer_code_mode(scoped.list_tool_definitions(),scoped,state=state)
    assert {'照会','code_mode'}=={row['name'] for row in offered}
    scoped.allowed=False
    def execute(*args):raise AssertionError('Scope denial must precede native execution')
    update=code_mode.execute_if_selected(state,[{'type':'tool_use','id':'program','name':'code_mode',
        'input':{'tool_names':['照会'],'code':"call_tool('照会', {})"}}],execute=execute)
    assert update['tool_results'][-1].status!='completed' and not native_calls


def test_scope_cache_fingerprint_tracks_program_and_native_visibility():
    from nullion.turn_context_policy import _tool_scope_registry_signature
    registry=ToolRegistry();spec=ToolSpec('lookup','Read context',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3)
    registry.register(spec,lambda invocation:None)
    old=_tool_scope_registry_signature(registry)
    spec.code_mode_allowed=True
    eligible=_tool_scope_registry_signature(registry)
    spec.code_mode_keep_native=True
    assert old!=eligible!=_tool_scope_registry_signature(registry)
