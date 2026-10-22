"""Pending input is distinct from failed execution; recall is a core read capability."""
from types import SimpleNamespace

import pytest

from nullion import agent_orchestrator as agent, chat_operator
from nullion.conversation_history_tools import with_conversation_history_tool
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import ToolRegistry, ToolResult
from nullion.turn_context_policy import TurnToolEvidence, turn_tool_registry_for_evidence


def source_result(*, single=False):
    return ToolResult('scope', 'request_tool_scope', 'completed', {
        'source_selection_required': True,
        'source_evidence_verified': False,
        'connector_source_evidence_required': True,
        'requested_required_tool_names': ['email_search'],
        'required_tool_names': [],
        'unavailable_required_tool_names': ['email_search'],
        'available_tools': [],
        'available_sources': [{'display_name': 'Example account',
            'read_tools': ['email_search'] if single else ['email_search', 'calendar_list']}],
    })


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'home'/'.env'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path/'storage'))
    return bootstrap_persistent_runtime(tmp_path/'runtime.db')


@pytest.mark.parametrize('prompt', ['Has my parcel arrived?', '¿Ya llegó mi paquete?', '荷物は届きましたか？'])
@pytest.mark.parametrize('single', [False, True])
def test_real_store_terminal_preserves_pending_source_input(runtime, prompt, single):
    results = [source_result(single=single)]
    state = {'runtime_store': runtime.store, 'tool_registry': ToolRegistry(),
        'conversation_id': 'web:selection', 'principal_id': 'web:selection',
        'user_message': prompt, 'tool_results': results, 'artifacts': []}
    question = agent._account_source_selection_reply_from_scope_results(results)
    completed = agent._complete_agent_turn(state, final_text=question)['result']
    assert '1.' in completed.final_text and '2.' in completed.final_text
    assert 'required step' not in completed.final_text
    assert completed.response_fulfilled is not True
    assert completed.completion_failure_reply is None
    assert completed.response_presentation['needs_input'] is True
    rendered = chat_operator._enforce_chat_response_fulfillment(runtime,
        conversation_id='web:selection', prompt=prompt, reply=completed.final_text,
        tool_results=results, required_tool_names=['email_search'])
    assert rendered == completed.final_text


def test_later_authorized_scope_resolves_pending_source(runtime):
    results = [source_result(), ToolResult('authorized', 'request_tool_scope', 'completed', {
        'source_selection_required': False, 'source_evidence_verified': True,
        'available_tools': ['email_search'], 'required_tool_names': ['email_search']}),
        ToolResult('read', 'email_search', 'completed', {'results': []})]
    assert agent._account_source_selection_reply_from_scope_results(results) is None
    assert chat_operator._account_source_selection_reply_from_scope_results(results) is None


def test_independent_turn_exposes_recall_without_search_or_classifier(runtime, monkeypatch):
    base = with_conversation_history_tool(ToolRegistry(), runtime=runtime, conversation_id='web:recall')
    class NoClassifier:
        def create(self, **kwargs):
            pytest.fail('ordinary independent turn must not classify tools')
    monkeypatch.setattr(type(runtime.store), 'list_workspace_conversation_events',
        lambda *a, **kw: pytest.fail('registry creation must not scan saved history'), raising=False)
    scoped = turn_tool_registry_for_evidence(base, evidence=TurnToolEvidence(),
        model_client=NoClassifier(), user_message='A self-contained question', skip_tool_scope_decision=True)
    visible = {d['name'] for d in scoped.list_tool_definitions()}
    assert {'chat_history_search', 'memory_search'} <= visible
    assert not {'email_search', 'calendar_list', 'memory_save'} & visible
    assert chat_operator._plain_no_tool_common_retries_may_bypass(
        SimpleNamespace(final_text='A complete answer.', tool_results=[], artifacts=[], suspended_for_approval=False), scoped)


def test_alternative_scope_clears_pending_question_without_granting_account_access(runtime):
    results=[source_result(), ToolResult('browser-scope','request_tool_scope','completed',{
        'capabilities':['web'],'available_tools':['browser_navigate'],'unavailable_required_tool_names':[]})]
    assert agent._account_source_selection_reply_from_scope_results(results) is None
    # A denied/empty alternative scope cannot erase the unresolved question.
    results[-1]=ToolResult('denied','request_tool_scope','completed',{
        'capabilities':['web'],'available_tools':[],'unavailable_required_tool_names':['browser_navigate']})
    assert agent._account_source_selection_reply_from_scope_results(results)


def test_nested_scope_registry_has_unique_tool_schemas(runtime):
    base=with_conversation_history_tool(ToolRegistry(),runtime=runtime,conversation_id='web:unique')
    scoped=turn_tool_registry_for_evidence(base,evidence=TurnToolEvidence(),model_client=None,user_message="Hello",skip_tool_scope_decision=True)
    nested=turn_tool_registry_for_evidence(scoped,evidence=TurnToolEvidence(),model_client=None,user_message="Hello",skip_tool_scope_decision=True)
    names=[row['name'] for row in nested.list_tool_definitions()]
    assert names.count('request_tool_scope')==1
    assert len(names)==len(set(names))


def test_pending_source_does_not_start_artifact_repair(runtime):
    results=[source_result()]
    assert not chat_operator._needs_required_attachment_repair(runtime,
        conversation_id='web:pending',prompt='Informe PDF',reply='Choose source',
        tool_results=results,required_attachment_extensions=['.pdf'])
    assert chat_operator._missing_required_attachment_extensions_for_repair(runtime,
        prompt='Informe PDF',principal_id='web:pending',required_attachment_extensions=['.pdf'],
        tool_results=results,artifact_paths=[])==()


def test_saved_target_fallback_preserves_pending_source_question(runtime):
    result=SimpleNamespace(final_text='1. Email records\n2. Another source',tool_results=[source_result()])
    context=('Structured saved reference candidates (ranked by relevance and recency):\n'
        'Target 1: {"type":"url","url":"https://carrier.example.test/track/317"}\n'
        'Target source assistant: Earlier status was shipped.\nCandidate prior turns:')
    assert not chat_operator._apply_saved_history_structured_target_fallback(result,context)
    assert result.final_text.startswith('1. Email records')


def test_history_scope_alone_does_not_discharge_source_selection():
    results=[source_result(),ToolResult('history-scope','request_tool_scope','completed',{
        'capabilities':['conversation_history'],'available_tools':['chat_history_search'],
        'unavailable_required_tool_names':[]})]
    assert agent._account_source_selection_reply_from_scope_results(results)


@pytest.mark.parametrize('selected,new_capability,retain',[(False,'web',False),(True,'web',True),(False,'conversation_history',True)])
def test_alternative_scope_replaces_only_unselected_account_obligation(selected,new_capability,retain):
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolScopeDecision
    from nullion.tools import ToolInvocation,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    base=ToolRegistry()
    for name,tags in [('email_search',('email','connector','account_read')),('browser_navigate',()),
        ('file_read',('local',)),('chat_history_search',('conversation_history',))]:
        base.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
            input_schema={'type':'object','properties':{}},capability_tags=tags),
            lambda invocation:ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{}))
    scoped=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(),tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=('email_search','file_read'),required_tool_names=('email_search','file_read'),
        connector_source_user_requested=selected,valid=True))
    result,widened=scoped.apply_scope_request(ToolInvocation('alternative','request_tool_scope','web:alternative',{
        'capabilities':[new_capability],'tool_names':['browser_navigate' if new_capability=='web' else 'chat_history_search']}))
    assert ('email_search' in result.output['requested_required_tool_names']) is retain,result.output
    assert 'file_read' in result.output['requested_required_tool_names']
    if not selected:assert not widened.can_invoke_tool('email_search')
