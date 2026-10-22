from datetime import UTC, datetime
from types import SimpleNamespace

from nullion import conversation_history_tools as history
from nullion import builder_memory
from nullion.memory import UserMemoryEntry, UserMemoryKind


def turn(identity, conversation='web:a', *, when='2026-10-01T10:00:00Z', text='Cedar reference', reply='Recorded Cedar reference'):
    return {'event_type':'conversation.chat_turn', 'conversation_id':conversation,
        'workspace_id':'workspace_admin', 'turn_id':identity, 'created_at':when,
        'user_message':text, 'assistant_reply':reply}


def test_history_equal_matches_prioritize_latest_observation():
    old=turn('old'); new=turn('new', when='2026-10-02T10:00:00Z')
    selected,_=history._ranked_history_events([old,new],query='Cedar reference',limit=2,fallback_to_recent_on_no_match=False)
    assert [row['turn_id'] for row in selected]==['new','old']


def test_context_does_not_cross_conversation_or_branch():
    anchor=turn('anchor', text='Cedar reference')
    foreign=turn('foreign','web:other',when='2026-10-01T09:59:00Z',text='Unrelated',reply='PRIVATE OTHER TOPIC')
    local=turn('local',when='2026-10-01T09:58:00Z',text='Earlier detail',reply='Identifier CEDAR-7')
    selected,_=history._ranked_history_events([local,foreign,anchor],query='Cedar reference',limit=4,fallback_to_recent_on_no_match=False)
    assert 'foreign' not in {r['turn_id'] for r in selected}
    assert 'local' in {r['turn_id'] for r in selected}


def test_reset_filters_encrypted_history_fallback_and_does_not_reopen_old_events(monkeypatch):
    old=turn('old',text='Forgotten Cedar detail')
    reset={'event_type':'conversation.session_reset','conversation_id':'web:a','workspace_id':'workspace_admin','created_at':'2026-10-02T00:00:00Z'}
    store=SimpleNamespace(list_conversation_events=lambda:[old,reset],
        list_recent_conversation_events_after_reset=lambda *a,**kw:[],
        list_recent_conversation_events=lambda *a,**kw:[old])
    monkeypatch.setattr(history,'_chat_store_history_turns',lambda *a,**kw:[{**old,'source':'chat_history'}])
    assert history._conversation_events_after_reset(SimpleNamespace(),store,'web:a',full_history=True,workspace_id='workspace_admin')==[]


def test_identical_history_dedup_keeps_latest_equivalent_record():
    result=history._dedupe_history_events([turn('same'),turn('same',when='2026-10-02T10:00:00Z')],scan_limit=None)
    assert len(result)==1 and result[0]['created_at']=='2026-10-02T10:00:00Z'


def test_different_turns_with_identical_text_remain_distinct():
    result=history._dedupe_history_events([turn('old'),turn('new',when='2026-10-02T10:00:00Z')],scan_limit=None)
    assert {r['turn_id'] for r in result}=={'old','new'}


def test_encrypted_import_is_idempotent_without_collapsing_repeated_turns(tmp_path):
    from nullion.chat_store import ChatStore
    store=ChatStore(db_path=tmp_path/'history.db',key_path=tmp_path/'key')
    records=[turn('old'),turn('new',when='2026-10-02T10:00:00Z')]
    assert store.import_runtime_chat_turns(records)==4
    assert store.import_runtime_chat_turns(records)==0
    assert len(store.load_messages('web:a',limit=10))==4


def test_import_links_an_existing_adapter_delivery_only_once(tmp_path):
    from nullion.chat_store import ChatStore
    store=ChatStore(db_path=tmp_path/'history.db',key_path=tmp_path/'key')
    old=turn('old'); new=turn('new',when='2026-10-02T10:00:00Z')
    store.save_message('web:a','user',old['user_message'])
    store.save_message('web:a','bot',old['assistant_reply'])
    assert store.import_runtime_chat_turns([old])==0
    assert store.import_runtime_chat_turns([old,new])==2
    assert store.import_runtime_chat_turns([old,new])==0
    assert len(store.load_messages('web:a',limit=10))==4


def test_uncased_script_and_compatibility_text_history_recall():
    rows=[turn('target',text='青葉の発送記録',reply='受付番号３１７')]
    rows.extend(turn(str(i),text=f'別の話題{i}',reply='記録済み') for i in range(10))
    selected,_=history._ranked_history_events(rows,query='青葉の現在地',limit=1,fallback_to_recent_on_no_match=False)
    assert selected and selected[0]['turn_id']=='target'
    selected,_=history._ranked_history_events(rows,query='317',limit=1,fallback_to_recent_on_no_match=False)
    assert selected and selected[0]['turn_id']=='target'


def test_uncased_script_memory_can_match_a_topic_inside_a_sentence():
    entries=[UserMemoryEntry('target','workspace:alpha',UserMemoryKind.FACT,'青葉の記録','受付番号３１７',created_at=datetime.now(UTC)),
             UserMemoryEntry('other','workspace:alpha',UserMemoryKind.FACT,'別件','記録なし',created_at=datetime.now(UTC))]
    scores=builder_memory._memory_query_scores(entries,'青葉の現在地')
    assert scores.get('target',0)>scores.get('other',0)


def test_history_does_not_invent_provider_urls_from_prose():
    assert history._structured_references_from_text('PR #17 in `example/project`')==[]
    url='https://github.com/example/project/pull/17'
    assert history._structured_references_from_text(url)==[{'type':'url','domain':'github.com','url':url}]


def test_recall_only_reply_does_not_become_new_source_evidence():
    event=turn('derived',text='A previous reference?',reply='過去の参照: https://records.example.test/unverified')
    event['tool_results']=[{'tool_name':'chat_history_search','status':'completed','output':{'matches':[]}}]
    assert history._structured_references_from_event(event)==[]


def test_import_migrates_legacy_storage_without_leaking_origin_metadata(tmp_path):
    import sqlite3
    from nullion.chat_store import ChatStore
    path=tmp_path/'history.db'
    # A previous install has messages but no runtime identity column.
    store=ChatStore(db_path=path,key_path=tmp_path/'key')
    store.save_message('web:a','user','Cedar reference')
    store.save_message('web:a','bot','Recorded Cedar reference')
    with sqlite3.connect(path) as conn:
        conn.execute('ALTER TABLE messages DROP COLUMN runtime_turn_id')
    migrated=ChatStore(db_path=path,key_path=tmp_path/'key')
    assert migrated.import_runtime_chat_turns([turn('original')])==0
    assert migrated.import_runtime_chat_turns([turn('original')])==0
    assert all(row['metadata']=={} and 'runtime_turn_id' not in row for row in migrated.load_messages('web:a'))


def test_repeated_scheduled_deliveries_keep_distinct_turn_identity(tmp_path):
    from nullion.chat_store import ChatStore
    store=ChatStore(db_path=tmp_path/'history.db',key_path=tmp_path/'key')
    records=[turn('first',text=''),turn('second',text='',when='2026-10-02T10:00:00Z')]
    assert store.import_runtime_chat_turns(records)==2
    assert len(store.load_messages('web:a'))==2


def test_one_runtime_mirror_does_not_hide_a_later_identical_history_turn():
    old=turn('old')
    mirror={**old,'source':'chat_history'}
    later=turn('later',when='2026-10-02T10:00:00Z');later['source']='chat_history'
    result=history._dedupe_history_events([old,mirror,later],scan_limit=None)
    assert [row['turn_id'] for row in result]==['old','later']


def test_retained_archive_is_searchable_but_later_clear_is_a_boundary():
    from nullion.session_stop import retained_history_turns
    old=turn('old')
    archive={'event_type':'conversation.session_reset','conversation_id':'web:a',
        'created_at':'2026-10-02T00:00:00Z','history_retained':True}
    assert retained_history_turns([archive,old])==[old]
    clear={**archive,'created_at':'2026-10-03T00:00:00Z','history_retained':False}
    assert retained_history_turns([archive,clear,old])==[]
    fresh=turn('fresh',when='2026-10-04T00:00:00Z')
    assert retained_history_turns([fresh,archive,clear,old])==[fresh]


def test_legacy_archived_encrypted_history_survives_untyped_session_reset(tmp_path):
    from nullion.chat_store import ChatStore
    from nullion.runtime import bootstrap_persistent_runtime
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    old=turn('legacy-archived')
    store=ChatStore(db_path=tmp_path/'chat_history.db',key_path=tmp_path/'chat_history.key')
    store.import_runtime_chat_turns([old]);store.archive_conversation('web:a')
    runtime.store.add_conversation_event(old)
    runtime.store.add_conversation_event({'event_type':'conversation.session_reset',
        'conversation_id':'web:a','workspace_id':'workspace_admin','created_at':'2026-10-02T00:00:00Z'})
    result=history._conversation_events_after_reset(runtime,runtime.store,'web:new',
        full_history=True,workspace_id='workspace_admin')
    assert len(result)==1 and result[0]['assistant_reply']==old['assistant_reply']


def test_legacy_messaging_new_preserves_explicit_search_not_current_context():
    from nullion.session_stop import retained_history_turns
    old=turn('old','telegram:42')
    legacy={'event_type':'conversation.session_reset','conversation_id':'telegram:42',
        'chat_id':'42','created_at':'2026-10-02T00:00:00Z'}
    assert retained_history_turns([old,legacy])==[old]
    assert retained_history_turns([old,{**legacy,'history_retained':False}])==[]
