import pytest
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import ToolInvocation
from nullion.conversation_history_tools import _chat_history_search_result


@pytest.mark.parametrize('query', ['bulletin', 'boletín', '運行情報'])
def test_automatic_history_search_is_bounded_local_and_reset_aware(tmp_path, monkeypatch, query):
    runtime = bootstrap_persistent_runtime(tmp_path / 'history.db')
    def add(conversation, text):
        runtime.store.add_conversation_event({'conversation_id': conversation,
            'event_type': 'conversation.chat_turn', 'user_message': text,
            'assistant_reply': text, 'turn_id': text})
    add('web:current', query + ' before reset')
    runtime.store.add_conversation_event({'conversation_id': 'web:current', 'event_type': 'conversation.session_reset'})
    add('web:other', query + ' unrelated conversation')
    for index in range(205):
        add('web:current', f'{query} item {index}')
    monkeypatch.setattr(type(runtime.store), 'list_conversation_events', lambda *a, **k: pytest.fail('Automatic lookup must not scan the workspace'))
    result = _chat_history_search_result(runtime=runtime, conversation_id='web:current',
        invocation=ToolInvocation('lookup', 'chat_history_search', 'member',
            {'query': query, 'search_scope': 'recent_conversation', 'limit': 24}))
    assert result.status == 'completed'
    assert result.output['searched_scope'] == 'recent_conversation_after_reset'
    text = str(result.output)
    assert 'before reset' not in text
    assert 'unrelated conversation' not in text
    assert f'{query} item 0\'' not in text
    assert f'{query} item 204' in text
    runtime.store.add_conversation_event({'conversation_id': 'web:current', 'event_type': 'conversation.session_reset'})
    empty = _chat_history_search_result(runtime=runtime, conversation_id='web:current',
        invocation=ToolInvocation('lookup-empty', 'chat_history_search', 'member',
            {'query': query, 'search_scope': 'recent_conversation', 'limit': 24}))
    assert 'item 204' not in str(empty.output)
