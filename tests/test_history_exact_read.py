"""Complete saved replies remain readable without broadening workspace access."""
from datetime import UTC,datetime
import json
from types import SimpleNamespace
import pytest
from nullion.conversation_history_tools import _chat_history_search_result,compact_history_search_output
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import ToolInvocation

@pytest.fixture
def history(tmp_path,monkeypatch):
    from nullion import chat_store
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    body='First subject: '+'詳細 '*900+'\nSecond subject: house music.\nThird subject: music and makeup.\nSources: https://example.org/second https://example.org/third'
    runtime.store.add_conversation_event({'conversation_id':'web:source','workspace_id':'workspace_admin','turn_id':'source-id',
        'event_type':'conversation.chat_turn','created_at':datetime.now(UTC).isoformat(),'user_message':'比較','assistant_reply':body})
    def invoke(args):
        return _chat_history_search_result(runtime=runtime,conversation_id='web:source',workspace_id='workspace_admin',
            invocation=ToolInvocation(invocation_id='read',principal_id='web:source',tool_name='chat_history_search',arguments=args))
    return runtime,body,invoke


def test_search_marks_excerpt_and_exact_read_retains_missing_subjects_and_links(history):
    _,body,invoke=history
    search=invoke({'query':'比較','limit':1});row=search.output['matches'][0]
    assert row['assistant_reply_truncated'] and 'https://example.org/third' not in row['assistant_reply']
    result=invoke({'read_turn_id':row['turn_id']})
    assert result.status=='completed' and result.output['reference_only']
    record=result.output['matches'][0]
    assert record['assistant_reply']==body and record['reply_window']['next_start_char'] is None
    compact=compact_history_search_output(result.output)
    assert compact['matches'][0]['assistant_reply']==body
    assert 'Second subject: house music.' in compact['matches'][0]['assistant_reply']


def test_read_pages_use_exact_unicode_character_offsets_and_compaction_cursor(history):
    _,body,invoke=history;offset=0;parts=[]
    while True:
        result=invoke({'read_turn_id':'source-id','start_char':offset,'max_chars':200})
        row=compact_history_search_output(result.output,max_chars=1600)['matches'][0]
        assert row['reply_window']['start_char']==offset and row['assistant_reply']==body[offset:row['reply_window']['end_char']]
        parts.append(row['assistant_reply']);next_offset=row['reply_window']['next_start_char']
        if next_offset is None:break
        assert next_offset>offset;offset=next_offset
    assert ''.join(parts)==body


@pytest.mark.parametrize('args',[{'read_turn_id':'missing'},{'read_turn_id':True},{'read_turn_id':'source-id','start_char':True},
    {'read_turn_id':'source-id','start_char':-1},{'read_turn_id':'source-id','max_chars':1000000},
    {'read_turn_id':'source-id','start_char':1000000}])
def test_invalid_or_stale_read_never_falls_back_to_another_reply(history,args):
    _,_,invoke=history;result=invoke(args)
    assert result.status=='failed' and not result.output.get('matches')


def test_read_cannot_cross_workspace_or_choose_ambiguous_identifier(history):
    runtime,body,invoke=history
    runtime.store.add_conversation_event({'conversation_id':'web:private','workspace_id':'workspace_private','turn_id':'private',
        'event_type':'conversation.chat_turn','created_at':datetime.now(UTC).isoformat(),'user_message':'比較','assistant_reply':'secret'})
    assert invoke({'read_turn_id':'private'}).status=='failed'
    runtime.store.add_conversation_event({'conversation_id':'web:other','workspace_id':'workspace_admin','turn_id':'source-id',
        'event_type':'conversation.chat_turn','created_at':datetime.now(UTC).isoformat(),'user_message':'Other','assistant_reply':'different'})
    assert invoke({'read_turn_id':'source-id'}).status=='failed'


@pytest.mark.parametrize('fulfilled',[None,False])
def test_exact_read_skips_discovery_restart_without_suppressing_material_failure(history,fulfilled):
    from nullion.chat_operator import _turn_result_should_retry_with_saved_history
    _,_,invoke=history;result=invoke({'read_turn_id':'source-id'})
    outcome=SimpleNamespace(tool_results=[result],response_fulfilled=fulfilled,reached_iteration_limit=False,
        artifacts=[],suspended_for_approval=False)
    assert _turn_result_should_retry_with_saved_history(outcome) is (fulfilled is False)


def test_tiny_context_budget_never_returns_a_nonadvancing_reply_page(history):
    _,_,invoke=history;result=invoke({'read_turn_id':'source-id'})
    compact=compact_history_search_output(result.output,max_chars=700)
    assert not compact['matches'] and compact['reason']=='history_context_budget_exhausted'
