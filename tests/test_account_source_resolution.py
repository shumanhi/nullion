"""Source naming does not require the user to identify an adapter/vendor."""
import json
import pytest

from nullion import turn_context_policy
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass
from nullion.turn_context_policy import ScopedTurnToolRegistry, build_turn_tool_evidence


def registry():
    result = ToolRegistry()
    for name, tags, effect in (
        ("calendar_list", ("calendar", "connector", "account_read"), ToolSideEffectClass.READ),
        ("email_search", ("email", "connector", "account_read"), ToolSideEffectClass.READ),
        ("email_send", ("email", "connector", "account_write"), ToolSideEffectClass.ACCOUNT_WRITE),
    ):
        result.register(ToolSpec(name=name, description=name, capability_tags=tags,
            risk_level=ToolRiskLevel.LOW, side_effect_class=effect, requires_approval=False,
            timeout_seconds=1), lambda inv: ToolResult(inv.invocation_id, inv.tool_name, "completed", {}))
    return result


@pytest.fixture
def provider(monkeypatch):
    monkeypatch.setattr(turn_context_policy, "_active_connector_provider_context", lambda: [{
        "provider_id": "example", "display_name": "Example gateway",
        "active_app_ids": ["google-calendar", "google-mail"],
        "structured_tools": ["calendar_list", "email_search", "email_send"],
    }])


@pytest.mark.parametrize("prompt,quote", [
    ("Check my calendar", "my calendar"),
    ("Revisa mi calendario", "mi calendario"),
    ("私の予定表を確認してください", "私の予定表"),
    ("Check unread emails", "emails"),
])
def test_verified_source_declaration_resolves_metadata_without_granting_reads(provider, prompt, quote):
    scoped = ScopedTurnToolRegistry(registry(), evidence=build_turn_tool_evidence(
        user_message=prompt, conversation_result=None))
    result, widened = scoped.apply_scope_request(ToolInvocation("scope", "request_tool_scope", "web:1", {
        "capabilities": ["connector"], "source_user_requested": True, "source_evidence": quote}))
    assert result.output["source_resolution_required"] is True
    assert result.output["source_selection_required"] is False
    assert isinstance(result.output["message"], str)
    assert not result.output["available_tools"]
    assert not {"calendar_list", "email_search", "email_send"}.intersection(
        spec.name for spec in widened.list_specs())
    assert widened.turn_tool_scope_decision.connector_source_user_requested is False


@pytest.mark.parametrize("flag,evidence", [(False, "external records"), (True, ""), (True, "absent quote")])
def test_unselected_or_unverified_source_still_requires_numbered_choice(provider, flag, evidence):
    scoped = ScopedTurnToolRegistry(registry(), evidence=build_turn_tool_evidence(
        user_message="external records", conversation_result=None))
    result, widened = scoped.apply_scope_request(ToolInvocation("scope", "request_tool_scope", "web:1", {
        "capabilities": ["connector"], "source_user_requested": flag, "source_evidence": evidence}))
    assert result.output["source_selection_required"] is True
    assert result.output["source_resolution_required"] is False
    assert not result.output["available_tools"]


def test_metadata_resolution_only_exposes_the_subsequently_selected_read(provider):
    scoped = ScopedTurnToolRegistry(registry(), evidence=build_turn_tool_evidence(
        user_message="私の予定表を確認してください", conversation_result=None))
    arguments = {"capabilities": ["connector"], "source_user_requested": True, "source_evidence": "私の予定表"}
    _, discovered = scoped.apply_scope_request(ToolInvocation("discover", "request_tool_scope", "web:1", arguments))
    result, selected = discovered.apply_scope_request(ToolInvocation("select", "request_tool_scope", "web:1", {
        **arguments, "tool_names": ["calendar_list"], "required_tool_names": ["calendar_list"]}))
    assert "calendar_list" in result.output["available_tools"]
    assert not {"email_search", "email_send"}.intersection(result.output["available_tools"])
    assert not result.output["source_resolution_required"] and not result.output["source_selection_required"]


@pytest.mark.parametrize('quote',['my email','mis correos','私のメール'])
def test_compacted_resolution_keeps_exact_adapter_ids_and_recovery_contract(quote):
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context, _project_tool_output_for_repair
    output={'source_resolution_required':True,'source_selection_required':False,
        'source_evidence_verified':True,'connector_source_user_requested':False,
        'connector_source_evidence':quote,'available_tools':[],
        'available_sources':[{'display_name':'Neutral gateway','read_tools':['email_search','calendar_list'],
            'active_app_ids':['mail-adapter','agenda-adapter'],'permission_mode':'read',
            'app_inventory_kind':'supported_catalogue'}],
        'message':'Resolve the exact named read tool and adapter; this metadata result grants no account read.',
        'active_connector_providers':[{'catalogue_documentation':'x'*30000}]}
    for compact in (_compact_tool_output_for_model_context('request_tool_scope',output),
                    _project_tool_output_for_repair('request_tool_scope',output)):
        assert compact['result_kind']=='tool_scope_evidence'
        assert compact['available_sources']==output['available_sources']
        assert compact['message']==output['message']
        assert compact['connector_source_evidence']==quote
        assert compact['source_resolution_required'] and not compact['source_selection_required']
        assert not compact['connector_source_user_requested'] and compact['available_tools']==[]
        assert 'active_connector_providers' not in compact


def test_exact_authorized_read_remains_required_when_model_omits_redundant_field(provider):
    scoped = ScopedTurnToolRegistry(registry(), evidence=build_turn_tool_evidence(
        user_message="私の予定表を確認してください", conversation_result=None))
    result, _ = scoped.apply_scope_request(ToolInvocation("scope", "request_tool_scope", "web:1", {
        "capabilities":["connector"],"source_user_requested":True,"source_evidence":"私の予定表",
        "tool_names":["calendar_list"]}))
    assert result.output["required_tool_names"] == ["calendar_list"]


@pytest.mark.parametrize("state", [
    {"available_tools":["email_search"],"connector_source_unavailable":False},
    {"source_selection_required":True}, {"source_resolution_required":True}, {},
])
def test_builder_setup_requires_unavailable_route_evidence(state):
    from nullion.chat_operator import _connector_scope_builder_fallback
    result = ToolResult("scope", "request_tool_scope", "completed", {
        "capabilities":["connector"],"connector_app_ids":["google-mail"], **state})
    assert _connector_scope_builder_fallback([result]) is None


def test_compact_inventory_builds_read_routes_from_metadata_not_provider_names(tmp_path, monkeypatch):
    from nullion.chat_operator import _chat_capability_inventory_prompt
    from nullion.runtime import bootstrap_persistent_runtime
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / "absent.env"))
    tools = registry()
    tools.register(ToolSpec(name="vendor_neutral_records", description="Read authorized records",
        capability_tags=("connector", "account_read"), side_effect_class=ToolSideEffectClass.READ,
        risk_level=ToolRiskLevel.LOW, requires_approval=False, timeout_seconds=1),
        lambda inv: ToolResult(inv.invocation_id, inv.tool_name, "completed", {}))
    prompt = _chat_capability_inventory_prompt(bootstrap_persistent_runtime(tmp_path / "runtime.db"), tool_registry=tools)
    line = next(line for line in prompt.splitlines() if line.startswith("Structured direct scope routes: "))
    routes = json.loads(line.split(": ", 1)[1])
    names = {route["tool_name"] for route in routes}
    assert {"vendor_neutral_records", "calendar_list", "email_search"} <= names
    assert "email_send" not in names
    route = next(route for route in routes if route["tool_name"] == "vendor_neutral_records")
    assert route["requires_source_evidence"] and route["allows_linked_read_source_evidence"] and route["scope_request"]["source_user_requested"]
    assert "connector_app_ids" not in route["scope_request"]


def test_compact_and_full_tool_awareness_share_source_read_policy(tmp_path,monkeypatch):
    from nullion.chat_operator import _chat_capability_inventory_prompt, _compact_tool_scope_awareness_prompt
    from nullion.source_selection import NAMED_SOURCE_READ_GUIDANCE
    from nullion.runtime import bootstrap_persistent_runtime
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'absent.env'))
    tools=registry()
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    for prompt in (_compact_tool_scope_awareness_prompt(tools),
                   _chat_capability_inventory_prompt(runtime,tool_registry=tools)):
        assert prompt.count(NAMED_SOURCE_READ_GUIDANCE)==1
        assert 'email_search' in prompt and 'calendar_list' in prompt


@pytest.mark.parametrize('source', ['my calendar', 'mi calendario', '私の予定表'])
def test_selected_read_source_survives_linked_date_clarification(provider, source):
    from nullion.turn_context_policy import TurnToolEvidence
    evidence=TurnToolEvidence(context_linked=True,current_user_message='2026-10-24',
        linked_user_messages=(source,))
    scoped=ScopedTurnToolRegistry(registry(),evidence=evidence)
    result,widened=scoped.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:linked',{
        'capabilities':['connector'],'tool_names':['calendar_list'],'source_user_requested':True,
        'source_evidence':source}))
    assert result.output['source_evidence_verified'] and not result.output['source_selection_required']
    assert result.output['required_tool_names']==['calendar_list'] and widened.can_invoke_tool('calendar_list')


@pytest.mark.parametrize('linked,tool,quote', [(False,'calendar_list','my calendar'),
    (True,'email_send','my calendar'),(True,'calendar_list','not in selected request'),
    (True,'connector_request','my calendar')])
def test_background_or_write_cannot_borrow_read_source_from_history(provider,linked,tool,quote):
    from nullion.turn_context_policy import TurnToolEvidence
    evidence=TurnToolEvidence(context_linked=linked,current_user_message='2026-10-24',
        linked_user_messages=('my calendar',))
    result,_=ScopedTurnToolRegistry(registry(),evidence=evidence).apply_scope_request(
        ToolInvocation('scope','request_tool_scope','web:linked',{'capabilities':['connector'],
            'tool_names':[tool],'source_user_requested':True,'source_evidence':quote}))
    assert not result.output['source_evidence_verified']
    assert not result.output['connector_source_user_requested']


def test_linked_request_context_reads_only_same_conversation_selected_ancestors():
    from types import SimpleNamespace
    from nullion.chat_operator import _linked_request_user_messages
    turns={'p':SimpleNamespace(conversation_id='web:one',branch_id='p',status='completed',user_message='私の予定表',parent_turn_id='other'),
        'other':SimpleNamespace(conversation_id='web:two',branch_id='other',status='completed',user_message='unrelated',parent_turn_id=None)}
    runtime=SimpleNamespace(store=SimpleNamespace(get_conversation_turn=turns.get,get_conversation_branch=lambda _:None))
    current=SimpleNamespace(turn=SimpleNamespace(conversation_id='web:one',turn_id='current',branch_id='current',disposition='continue',parent_turn_id='p'))
    assert _linked_request_user_messages(runtime,current)==('私の予定表',)
    current.turn.parent_turn_id=None
    assert _linked_request_user_messages(runtime,current)==()


def test_context_only_awareness_is_cached_metadata_with_revision_invalidation(monkeypatch):
    from nullion import chat_operator
    tools=registry(); calls=[]; original=tools.list_specs
    def counted():
        calls.append(True)
        return original()
    monkeypatch.setattr(tools,"list_specs",counted)
    def forbidden(*args,**kwargs):
        raise AssertionError("Compact awareness must not load optional runtime context")
    monkeypatch.setattr(chat_operator,"_load_settings_if_available",forbidden)
    monkeypatch.setattr(chat_operator,"active_connector_provider_context_snapshot",forbidden)
    monkeypatch.setattr(chat_operator,"_chat_capability_inventory_prompt",forbidden)
    first=chat_operator._compact_tool_scope_awareness_prompt(tools)
    assert chat_operator._compact_tool_scope_awareness_prompt(tools)==first and len(calls)==1
    index=json.loads(first.splitlines()[1])
    assert {r["tool_name"] for r in index["account_read_routes"]}=={"calendar_list","email_search"}
    assert "email_send" in index["registered_tool_names"]
    tools.register(ToolSpec(name="notas_privadas",description="Leer notas privadas",
        capability_tags=("connector","account_read"),side_effect_class=ToolSideEffectClass.READ,
        risk_level=ToolRiskLevel.LOW,requires_approval=False,timeout_seconds=1),
        lambda inv:ToolResult(inv.invocation_id,inv.tool_name,"completed",{}))
    updated=chat_operator._compact_tool_scope_awareness_prompt(tools)
    assert "notas_privadas" in updated and len(calls)==2
    tools.unregister("email_search")
    assert "email_search" not in chat_operator._compact_tool_scope_awareness_prompt(tools)


@pytest.mark.parametrize('resolution',[True,False,None,'true',1])
def test_runtime_source_resolution_is_a_typed_unresolved_scope(resolution):
    from nullion.source_selection import pending_source_resolution
    from nullion.tools import ToolResult
    payload={'source_resolution_required':resolution,'source_evidence_verified':True,'source_selection_required':False}
    row=ToolResult('scope','request_tool_scope','completed',payload)
    assert (pending_source_resolution([row]) is not None) is (resolution is True)
    selected=ToolResult('selected','request_tool_scope','completed',{'source_resolution_required':False,'available_tools':['calendar_list']})
    assert pending_source_resolution([row,selected]) is None
    payload['source_selection_required']=True
    assert pending_source_resolution([row]) is None
