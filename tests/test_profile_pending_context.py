from types import SimpleNamespace

from nullion.agent_orchestrator import _completion_review_request_context, _completion_review_required
from nullion.chat_operator import _selected_pending_request
from nullion.conversation_runtime import ConversationTurnDisposition


def test_review_receives_profile_defaults_without_replacing_memory_or_current_request():
    state = {'tool_flow_context': {'user_profile_context': 'Address: Example City, Region A',
        'known_user_context': 'Ask before sending messages.'}}
    context = _completion_review_request_context(state)
    assert 'Region A' in context['user_profile_context']
    assert 'Ask before' in context['known_user_context']
    assert not _completion_review_required(state, tool_results=[])


def test_plain_turns_never_load_pending_parent_history():
    def unexpected(*args, **kwargs):
        raise AssertionError('Ordinary turns cannot scan pending history')
    runtime = SimpleNamespace(store=SimpleNamespace(list_recent_conversation_events_after_reset=unexpected))
    for parent in (None, 'unrelated'):
        result = SimpleNamespace(turn=SimpleNamespace(parent_turn_id=parent,
            disposition=ConversationTurnDisposition.INDEPENDENT))
        assert _selected_pending_request(runtime, result) is None


def test_only_a_selected_pending_parent_activates_no_tool_review():
    assert not _completion_review_required({'tool_flow_context': {'pending_request': {}}}, tool_results=[])
    assert _completion_review_required({'tool_flow_context': {'pending_request': {
        'parent_turn_id': 'selected', 'original_request': '調査して報告してください。'}}}, tool_results=[])


def test_pending_state_requires_same_workspace_and_typed_clarification(monkeypatch):
    from nullion import connections
    monkeypatch.setattr(connections, 'workspace_id_for_principal', lambda value: 'workspace-a')
    result = SimpleNamespace(turn=SimpleNamespace(parent_turn_id='parent', conversation_id='web:one',
        disposition=ConversationTurnDisposition.CONTINUE))
    event = {'turn_id': 'parent', 'workspace_id': 'workspace-a', 'user_message': '調査して報告。',
        'assistant_reply': '対象を選んでください。', 'tool_results': []}
    runtime = SimpleNamespace(store=SimpleNamespace(list_recent_conversation_events_after_reset=lambda *a, **kw: [event]))
    assert _selected_pending_request(runtime, result) is None
    event['tool_results'] = [{'tool_name': 'source_read', 'status': 'completed',
        'output': {'executed': False, 'reason': 'request_plan_conflict', 'review_decision': 'clarify'}}]
    assert _selected_pending_request(runtime, result) is None
    event['pending_request'] = {'original_request': '調査して報告。', 'tool_names': ['source_read']}
    assert _selected_pending_request(runtime, result)['original_request'] == '調査して報告。'
    event['workspace_id'] = 'workspace-b'
    assert _selected_pending_request(runtime, result) is None


def test_selected_partial_parent_restores_execution_without_parsing_its_words(monkeypatch):
    from nullion import connections
    monkeypatch.setattr(connections,'workspace_id_for_principal',lambda value:'workspace-a')
    turn=SimpleNamespace(parent_turn_id='partial',conversation_id='web:one',
        disposition=ConversationTurnDisposition.CONTINUE)
    event={'turn_id':'partial','workspace_id':'workspace-a','user_message':'もう一度',
        'assistant_reply':'Some evidence is retained.','response_fulfilled':False,
        'tool_results':[{'tool_name':'request_tool_scope','status':'completed','output':{}},
            {'tool_name':'opaque_read','status':'completed','output':{'candidate':'Cedar'}}]}
    runtime=SimpleNamespace(store=SimpleNamespace(list_recent_conversation_events_after_reset=lambda *a,**kw:[event]))
    selected={'parent_turn_id':'partial','original_request':'制約に合う候補を調べて。'}
    context=_selected_pending_request(runtime,SimpleNamespace(turn=turn),selected)
    assert context['original_request']==selected['original_request']
    assert context['tool_names']==['opaque_read']
    assert _completion_review_required({'tool_flow_context':{'pending_request':context}},tool_results=[])
    for replacement in (True,None):
        event['response_fulfilled']=replacement
        assert _selected_pending_request(runtime,SimpleNamespace(turn=turn),selected) is None
