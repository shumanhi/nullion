"""Declared optional reads stay visible without making them mandatory actions."""
from dataclasses import replace
import pytest
from nullion.tools import ToolRegistry,ToolSpec,ToolSideEffectClass,ToolRiskLevel,ToolResult
from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence,TurnToolScopeDecision


def registry():
    result=ToolRegistry()
    for name,effect,continuations in [('browser_navigate',ToolSideEffectClass.READ,('web_search','memory_save')),
        ('web_search',ToolSideEffectClass.READ,('weather_forecast',)),
        ('weather_forecast',ToolSideEffectClass.READ,()),('memory_save',ToolSideEffectClass.WRITE,())]:
        result.register(ToolSpec(name=name,description=name,risk_level=ToolRiskLevel.LOW,side_effect_class=effect,
            requires_approval=False,timeout_seconds=5,continuation_tools=continuations),
            lambda invocation:ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{}))
    return result


@pytest.mark.parametrize('text',['Find the subjects in this source','Investiga los sujetos de esta fuente','この資料の対象を調べてください','ابحث عن الأشخاص في هذا المصدر'])
def test_selected_entry_exposes_only_one_hop_declared_read(text):
    evidence=TurnToolEvidence(current_user_message=text,has_url_target=True,context_linked=True)
    decision=TurnToolScopeDecision(web_action='open_url',requested_tool_names=('browser_navigate',),
        required_tool_names=('browser_navigate',),valid=True,confidence=1)
    scoped=ScopedTurnToolRegistry(registry(),evidence=evidence,tool_scope_decision=decision)
    names={s.name for s in scoped.list_specs()}
    assert {'browser_navigate','web_search'}<=names
    assert not names&{'memory_save','weather_forecast'}
    assert scoped.turn_tool_scope_decision.required_tool_names==('browser_navigate',)
    assert scoped.turn_tool_scope_decision.requested_tool_names==('browser_navigate',)


def test_plain_turn_does_not_expose_configured_source_or_load_inventory():
    scoped=ScopedTurnToolRegistry(registry(),evidence=TurnToolEvidence(current_user_message='2+2'))
    assert not {s.name for s in scoped.list_specs()}&{'browser_navigate','web_search','weather_forecast','memory_save'}


def test_continuation_visibility_cache_invalidates_when_typed_scope_changes():
    scoped=ScopedTurnToolRegistry(registry(),evidence=TurnToolEvidence(has_url_target=True,context_linked=True),
        tool_scope_decision=TurnToolScopeDecision(requested_tool_names=('browser_navigate',),valid=True))
    assert scoped.can_invoke_tool('web_search')
    scoped.turn_tool_scope_decision=replace(scoped.turn_tool_scope_decision,requested_tool_names=())
    assert not scoped.can_invoke_tool('web_search')


def test_research_scope_is_never_satisfied_by_a_navigation_receipt():
    from nullion.chat_operator import _direct_open_url_tool_name
    scoped=ScopedTurnToolRegistry(registry(),evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=TurnToolScopeDecision(web_action='live_research',requested_tool_names=('browser_navigate',),
            required_tool_names=('browser_navigate',),valid=True))
    assert _direct_open_url_tool_name(scoped,scoped.turn_tool_scope_decision) is None


def test_scope_cache_includes_bounded_original_request_and_latest_amendments():
    from nullion.turn_context_policy import _tool_scope_cache_key,_scope_linked_request_context
    evidence=TurnToolEvidence(context_linked=True,linked_user_messages=tuple(str(i)*2000 for i in range(8)))
    selected=_scope_linked_request_context(evidence)
    assert len(selected)==4 and sum(map(len,selected))==6000 and selected[-1]=='7'*1500
    first=_tool_scope_cache_key(user_message='https://source.example',evidence=evidence,registry=registry(),model_client=None)
    other=replace(evidence,linked_user_messages=('Different original mission',))
    second=_tool_scope_cache_key(user_message='https://source.example',evidence=other,registry=registry(),model_client=None)
    assert first['evidence']['linked_request_user_messages']!=second['evidence']['linked_request_user_messages']
    assert _scope_linked_request_context(replace(evidence,context_linked=False))==[]


@pytest.mark.parametrize('text',['Use this source','Usa esta fuente','この資料を使ってください','استخدم هذا المصدر'])
def test_public_discovery_metadata_survives_an_empty_exact_tool_selection(text):
    tools=registry()
    spec=tools.get_spec('web_search');tools.unregister('web_search')
    tools.register(replace(spec,capability_tags=('public_source_discovery',)),
        lambda invocation:ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{}))
    evidence=TurnToolEvidence(current_user_message=text,context_linked=True,has_url_target=True)
    scoped=ScopedTurnToolRegistry(tools,evidence=evidence,tool_scope_decision=TurnToolScopeDecision())
    assert scoped.can_invoke_tool('web_search')
    plain=ScopedTurnToolRegistry(tools,evidence=replace(evidence,has_url_target=False,context_linked=False))
    assert not plain.can_invoke_tool('web_search')
    generated=ScopedTurnToolRegistry(tools,evidence=evidence,
        tool_scope_decision=TurnToolScopeDecision(requested_outcome='generated_media',valid=True))
    assert not generated.can_invoke_tool('web_search')
