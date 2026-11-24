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


def test_ambient_native_context_defers_composition_until_scoped_or_resuming(monkeypatch):
    from nullion.conversation_history_tools import CHAT_HISTORY_SEARCH_TOOL_SPEC
    from nullion.turn_context_policy import TurnToolScopeDecision

    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    registry = ToolRegistry()
    registry.register(CHAT_HISTORY_SEARCH_TOOL_SPEC, lambda invocation: None)
    registry.turn_tool_scope_decision = TurnToolScopeDecision(
        requested_tool_names=('request_tool_scope',), valid=True,
    )
    definitions = registry.list_tool_definitions()
    state = {'completion_requirement_contract_cache': {'_code_mode_offered': ['stale']}}
    assert code_mode.offer_code_mode(definitions, registry, state=state) == definitions
    assert '_code_mode_offered' not in state['completion_requirement_contract_cache']

    resumed = code_mode.offer_code_mode(definitions, registry, state={**state, 'code_mode_session_id': 'existing'})
    assert {tool['name'] for tool in resumed} == {'chat_history_search', 'code_mode'}
    registry.turn_tool_scope_decision = TurnToolScopeDecision(
        requested_tool_names=('chat_history_search',), valid=True,
    )
    scoped = code_mode.offer_code_mode(definitions, registry)
    assert {tool['name'] for tool in scoped} == {'chat_history_search', 'code_mode'}



def test_unscoped_fast_path_uses_ready_reads_without_program_signatures(monkeypatch):
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence

    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    base = ToolRegistry()
    base.register(ToolSpec('browser_read_pages', 'Read pages', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 3, capability_tags=('web',)), lambda invocation: None)
    registry = ScopedTurnToolRegistry(base, evidence=TurnToolEvidence(),
        default_read_tool_names=('browser_read_pages',))
    definitions = registry.list_tool_definitions()
    assert {tool['name'] for tool in definitions} == {'browser_read_pages', 'request_tool_scope'}
    assert code_mode.offer_code_mode(definitions, registry) == definitions
