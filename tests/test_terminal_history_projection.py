"""Native terminal revisions replace provisional history without downgrades."""
from datetime import UTC, datetime

import pytest


def test_terminal_projection_is_idempotent_and_old_events_cannot_replace_artifacts(tmp_path):
    from nullion.chat_store import ChatStore
    from nullion.chat_history_projection import project_terminal_chat_turn
    store = ChatStore(tmp_path/'chat.db', key_path=tmp_path/'chat.key')
    draft, first, current = (tmp_path/name for name in ['draft.txt','first.txt','current.txt'])
    for p in [draft,first,current]: p.write_text(p.name)
    provisional = dict(conversation_id='web:owner',turn_id='owned-turn',event_type='conversation.chat_turn',
        created_at='2026-11-22T01:00:00+00:00',user_message='資料',assistant_reply='確認待ち',artifact_paths=[str(draft)])
    terminal = {**provisional,'history_state':'terminal','created_at':'2026-11-22T02:00:00+00:00',
        'assistant_reply':'作成しました。','artifact_paths':[str(first)]}
    store.import_runtime_chat_turns([provisional])
    project_terminal_chat_turn(terminal,store=store)
    latest = {**terminal,'created_at':'2026-11-22T03:00:00+00:00','assistant_reply':'改訂しました。','artifact_paths':[str(current)]}
    project_terminal_chat_turn(latest,store=store)
    for event in [terminal,provisional,{**provisional,'created_at':'2026-11-22T04:00:00+00:00'},latest]:
        store.import_runtime_chat_turns([event])
    project_terminal_chat_turn(latest,store=store)
    messages = store.load_messages('web:owner')
    assert [(m['role'],m['text']) for m in messages] == [('user','資料'),('bot','改訂しました。')]
    assert messages[-1]['metadata']['artifacts'] == [{'path':str(current)}]
    assert '_runtime_history_projection_at' not in messages[-1]['metadata']
    assert messages[-1]['created_at'] == latest['created_at']
    assert store.get_conversation('web:owner')['last_message_at'] == latest['created_at']
    other = {**latest,'conversation_id':'web:other','assistant_reply':'Otro resultado'}
    project_terminal_chat_turn(other,store=store)
    assert store.load_messages('web:owner')[-1]['text'] == '改訂しました。'
    assert store.load_messages('web:other')[-1]['text'] == 'Otro resultado'


@pytest.mark.parametrize('retained',[False,True])
def test_selected_repair_respects_destructive_clear_and_archive(tmp_path,monkeypatch,retained):
    from nullion.chat_store import ChatStore
    from nullion.chat_history_projection import repair_selected_terminal_history
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path,monkeypatch)
    store = ChatStore(tmp_path/'chat.db',key_path=tmp_path/'chat.key')
    runtime.store.add_conversation_event(dict(conversation_id='web:owner',event_type='conversation.chat_turn',
        history_state='terminal',turn_id='one',created_at='2026-11-22T01:00:00+00:00',user_message='資料',assistant_reply='完了'))
    runtime.store.add_conversation_event(dict(conversation_id='web:owner',event_type='conversation.session_reset',
        created_at='2026-11-22T02:00:00+00:00',history_retained=retained))
    runtime.checkpoint(force=True)
    repair_selected_terminal_history(runtime,'web:owner',store)
    assert [m['text'] for m in store.load_messages('web:owner')] == (['資料','完了'] if retained else [])
    repair_selected_terminal_history(runtime,'web:foreign',store)
    assert store.load_messages('web:foreign') == []


def test_late_client_save_cannot_replace_native_terminal_text_or_artifacts(tmp_path):
    from nullion.chat_store import ChatStore
    from nullion.chat_history_projection import project_terminal_chat_turn
    store = ChatStore(tmp_path/'chat.db',key_path=tmp_path/'chat.key')
    current = tmp_path/'current.txt';current.write_text('selected bytes')
    event = dict(conversation_id='web:owner',turn_id='one',event_type='conversation.chat_turn',history_state='terminal',
        created_at='2026-11-22T02:00:00+00:00',user_message='資料',assistant_reply='完了',artifact_paths=[str(current)])
    project_terminal_chat_turn(event,store=store)
    store.save_message('web:owner','bot','確認待ち',metadata={'runtime_turn_id':'one','history_message_kind':'terminal_reply',
        '_runtime_history_projection_at':'2099-01-01T00:00:00+00:00','artifacts':[{'path':'/private/stale.txt'}]})
    project_terminal_chat_turn(event,store=store)
    assert store.load_messages('web:owner')[-1]['text'] == '完了'
    assert store.load_messages('web:owner')[-1]['metadata']['artifacts'] == [{'path':str(current)}]


def test_selected_recovery_cursor_eventually_repairs_more_than_one_batch_and_late_rows(tmp_path,monkeypatch):
    from nullion.chat_store import ChatStore
    from nullion.chat_history_projection import repair_selected_terminal_history
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path,monkeypatch)
    store = ChatStore(tmp_path/'chat.db',key_path=tmp_path/'chat.key')
    for index in range(19):
        runtime.store.add_conversation_event(dict(conversation_id='web:owner',event_type='conversation.chat_turn',
            history_state='terminal',turn_id=str(index),created_at=f'2026-11-22T02:00:{index:02d}+00:00',
            user_message='Request '+str(index),assistant_reply='結果 '+str(index)))
    runtime.checkpoint(force=True)
    repair_selected_terminal_history(runtime,'web:owner',store)
    assert len([m for m in store.load_messages('web:owner') if m['role']=='bot']) <= 9
    # Reopening the history store proves progress is durable, not a process cache.
    for _ in range(3):
        store = ChatStore(store._path,key_path=tmp_path/'chat.key')
        repair_selected_terminal_history(runtime,'web:owner',store)
    assert {m['text'] for m in store.load_messages('web:owner') if m['role']=='bot'} == {'結果 '+str(i) for i in range(19)}
    runtime.store.add_conversation_event(dict(conversation_id='web:owner',event_type='conversation.chat_turn',
        history_state='terminal',turn_id='late',created_at='2026-11-22T01:00:00+00:00',user_message='Late source',assistant_reply='遅れて保存'))
    runtime.checkpoint(force=True)
    for _ in range(4):repair_selected_terminal_history(runtime,'web:owner',store)
    assert [m['text'] for m in store.load_messages('web:owner')].count('遅れて保存') == 1
    assert len([m for m in store.load_messages('web:owner') if m['role']=='bot']) == 20


@pytest.mark.parametrize('pending,reply,typed_state',[
    (True,'確認をお願いします。',None),
    (True,'Se requiere autorización.',None),
    (True,'継続の確認が必要です。','suspended'),
    (False,'Tool approval requested is a literal quote in this completed report.','terminal'),
])
def test_resume_completion_uses_native_state_instead_of_reply_prefix(tmp_path,monkeypatch,pending,reply,typed_state):
    from nullion import chat_operator,chat_store
    from nullion.approvals import create_approval_request
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolResult
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path,monkeypatch)
    mirror = chat_store.ChatStore(tmp_path/'chat.db',key_path=tmp_path/'chat.key')
    monkeypatch.setattr(chat_store,'get_chat_store',lambda:mirror)
    conversation,turn_id = 'web:native-state','owned-turn'
    approval = create_approval_request(conversation,'use_tool','opaque_tool')
    runtime.store.add_approval_request(approval)
    runtime.approve_approval_request(approval.approval_id,principal_id=conversation,permissions=())
    runtime.store.add_suspended_turn(SuspendedTurn(approval_id=approval.approval_id,conversation_id=conversation,
        chat_id='native-state',message='/chat 資料',request_id=None,message_id=None,created_at=datetime.now(UTC),
        messages_snapshot=[],resume_token={'conversation_turn_id':turn_id}))
    next_approval=[]
    def native_runner(owner,**kwargs):
        assert kwargs['chat_id']==conversation and kwargs['turn_id']==turn_id
        results=[]
        if pending:
            following=create_approval_request(conversation,'use_tool','opaque_next_tool')
            owner.store.add_approval_request(following);next_approval.append(following)
            results=[ToolResult('next','opaque_next_tool','denied',{'reason':'approval_required',
                'requires_approval':True,'approval_id':following.approval_id})]
            if typed_state == 'suspended':
                results=[]  # Budget pauses need no denied native tool result.
        chat_operator._remember_chat_turn(owner,chat_id=conversation,conversation_id=conversation,
            user_message='資料',assistant_reply=reply,conversation_turn_id=turn_id,tool_results=results,
            history_state=typed_state)
        return reply
    monkeypatch.setattr(chat_operator,'_render_chat_turn',native_runner)
    assert chat_operator.resume_approved_telegram_request(runtime,approval_id=approval.approval_id,chat_id='operator') == reply
    events=runtime.store.list_conversation_events(conversation,event_types=('conversation.chat_turn',))
    if pending:
        assert all(e.get('history_state')!='terminal' for e in events)
        assert mirror.load_messages(conversation)==[]
        assert runtime.store.get_approval_request(next_approval[0].approval_id).status.value=='pending'
    else:
        assert events[-1]['history_state']=='terminal'
        assert mirror.load_messages(conversation)[-1]['text']==reply
