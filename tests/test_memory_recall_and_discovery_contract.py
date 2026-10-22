from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from nullion import builder_memory, chat_operator
from nullion.conversation_history_tools import with_conversation_history_tool
from nullion.memory import UserMemoryEntry, UserMemoryKind, remember_text_fact
from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_store import RuntimeStore
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, register_workspace_plugin
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence


@pytest.fixture(autouse=True)
def isolated_home(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'missing.env'))
    monkeypatch.setenv('NULLION_KEY_STORAGE', 'file')


@pytest.mark.parametrize('prefix', ['The document is', '文書はこちら', 'El documento es'])
def test_selected_file_answer_survives_prior_broad_searches(prefix, tmp_path):
    target = tmp_path / 'cedar-itinerary.pdf'
    unrelated = [str(tmp_path / f'library-{i}.py') for i in range(30)]
    results = [
        ToolResult('broad', 'file_search', 'completed', {'matches': unrelated}),
        ToolResult('all', 'file_search', 'completed', {'matches': [str(target), str(tmp_path / 'other.pdf')]}),
        ToolResult('focused', 'file_search', 'completed', {'matches': [str(target)]}),
        ToolResult('empty', 'file_search', 'completed', {'matches': []}),
    ]
    reply = f'{prefix}: **{target.name}**.'
    assert chat_operator._sanitize_chat_reply(prompt='旅行の文書は？', reply=reply, tool_results=results) == reply


def test_discovery_fallback_is_bounded_and_reports_omitted_candidates():
    text = chat_operator._local_discovery_results_reply('', tuple(f'/data/record-{i}.pdf' for i in range(80)))
    assert len(text.splitlines()) <= 12
    assert '80' in text and '72' in text


def test_query_relevant_memory_outranks_unrelated_use_score():
    now = datetime.now(UTC)
    relevant = UserMemoryEntry('trip', 'workspace:alpha', UserMemoryKind.FACT, 'cedar_trip', 'Cedar itinerary departs 19:30', created_at=now-timedelta(days=5))
    unrelated = UserMemoryEntry('other', 'workspace:alpha', UserMemoryKind.FACT, 'garden', 'Garden equipment measurements', use_score=100, created_at=now)
    consent = UserMemoryEntry('consent', 'workspace:alpha', UserMemoryKind.PREFERENCE, 'consent', 'Confirm before sending messages', created_at=now)
    policy = builder_memory.MemoryPolicy(long_term_limit=1, mid_term_limit=0, short_term_limit=1)
    entries = builder_memory.select_memory_entries_for_prompt([unrelated, relevant, consent], policy=policy, query='Cedar itinerary')
    assert {e.entry_id for e in entries} == {'trip', 'consent'}


def test_legacy_naive_memory_timestamps_do_not_disable_selection():
    entries = [UserMemoryEntry(str(i), 'workspace:alpha', UserMemoryKind.FACT, 'key', 'value', created_at=when)
               for i, when in enumerate((datetime(2026, 1, 1), datetime(2026, 1, 2, tzinfo=UTC)))]
    assert builder_memory.select_memory_entries_for_prompt(entries)


def test_non_latin_structured_memory_keys_remain_distinct():
    store = RuntimeStore()
    first = remember_text_fact(store, owner='workspace:alpha', key='旅行', value='19:30', source='structured')
    second = remember_text_fact(store, owner='workspace:alpha', key='連絡', value='確認', source='structured')
    assert first.entry_id != second.entry_id
    assert len(store.list_user_memory_entries()) == 2


def test_history_returns_verified_artifact_reference_beyond_reply_snippet(tmp_path, monkeypatch):
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    artifact = workspace_storage_roots_for_workspace('workspace_admin').artifacts / 'cedar-itinerary.pdf'
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(b'%PDF-1.4\n%synthetic\n')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'telegram:example',
        'workspace_id':'workspace_admin','created_at':'2026-10-01T10:00:00+00:00','turn_id':'saved',
        'user_message':'Cedar itinerary for the weekend','assistant_reply':'Details. '*180 + '\nMEDIA:'+str(artifact)})
    registry = with_conversation_history_tool(ToolRegistry(), runtime=runtime, conversation_id='web:example')
    result = registry.invoke(ToolInvocation('history', 'chat_history_search', 'web:example', {'query':'Cedar itinerary'}))
    refs = result.output['matches'][0].get('artifact_refs', [])
    assert any(r['name']==artifact.name and r['path']==str(artifact) for r in refs)
    assert result.output.get('suppress_activity') is True


def test_local_file_scope_exposes_saved_history_without_another_scope_round_trip(tmp_path):
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    base = with_conversation_history_tool(register_workspace_plugin(ToolRegistry(), workspace_root=tmp_path), runtime=runtime, conversation_id='web:example')
    registry = ScopedTurnToolRegistry(base, evidence=TurnToolEvidence(saved_history_available=True))
    result = registry.invoke(ToolInvocation('scope', 'request_tool_scope', 'web:example', {'capabilities':['local_files'], 'tool_names':['file_search']}))
    assert result.status == 'completed'
    assert 'chat_history_search' in result.output['available_tools']
    assert 'chat_history_search' not in result.output['requested_required_tool_names']


def test_default_file_search_prunes_virtualenvs_by_metadata(tmp_path):
    env = tmp_path / 'arbitrary-runtime';env.mkdir()
    (env / 'pyvenv.cfg').write_text('home = /usr/bin\n')
    (env / 'report-transform.py').write_text('internal')
    target = tmp_path / 'report.pdf';target.write_bytes(b'%PDF-1.4\n')
    registry = ToolRegistry()
    register_workspace_plugin(registry, workspace_root=tmp_path)
    result = registry.invoke(ToolInvocation('search','file_search','web:example',{'pattern':'report'}))
    assert result.output['matches'] == [str(target)]


def test_running_surfaces_see_committed_memory_changes_and_deletions(tmp_path):
    from nullion.memory import memory_entries_for_owner
    path=tmp_path/'runtime.db'
    first=bootstrap_persistent_runtime(path)
    stale=bootstrap_persistent_runtime(path)
    owner='workspace:alpha'
    entry=remember_text_fact(first.store,owner=owner,key='consent',value='確認',source='structured')
    first.checkpoint(force=True)
    assert memory_entries_for_owner(stale.store,owner,checkpoint_path=path)[0].value=='確認'
    stale.checkpoint(force=True)
    assert memory_entries_for_owner(first.store,owner,checkpoint_path=path)[0].value=='確認'
    first.store.remove_user_memory_entry(entry.entry_id)
    first.checkpoint(force=True)
    stale.checkpoint(force=True)
    assert memory_entries_for_owner(stale.store,owner,checkpoint_path=path)==[]


def test_memory_snapshot_cache_avoids_repeated_decryption_and_full_runtime_load(tmp_path,monkeypatch):
    from nullion import runtime_persistence as persistence
    from nullion.memory import memory_entries_for_owner
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path)
    remember_text_fact(runtime.store,owner='workspace:alpha',key='旅行',value='19:30',source='structured')
    runtime.checkpoint(force=True)
    original=persistence._decode_sqlite_runtime_payload;seen=[]
    def decode(*args):
        seen.append(args[0]);return original(*args)
    monkeypatch.setattr(persistence,'_decode_sqlite_runtime_payload',decode)
    monkeypatch.setattr(persistence,'load_runtime_store',lambda *a,**k:pytest.fail('broad runtime reload'))
    for _ in range(4):assert memory_entries_for_owner(runtime.store,'workspace:alpha',checkpoint_path=path)
    assert seen==['user_facts']


def test_transient_checkpoint_lock_keeps_verified_memory_and_refreshes_after_unlock(tmp_path):
    import sqlite3
    from nullion.memory import memory_entries_for_owner
    path = tmp_path / 'runtime.db'
    runtime = bootstrap_persistent_runtime(path)
    entry = remember_text_fact(runtime.store, owner='workspace:alpha', key='consent', value='確認', source='structured')
    runtime.checkpoint(force=True)
    assert memory_entries_for_owner(runtime.store, entry.owner)[0].value == '確認'
    with sqlite3.connect(path) as writer:
        writer.execute('PRAGMA journal_mode=DELETE')
        writer.execute('BEGIN EXCLUSIVE')
        assert memory_entries_for_owner(runtime.store, entry.owner)[0].value == '確認'
        writer.rollback()
    other = bootstrap_persistent_runtime(path)
    other.store.remove_user_memory_entry(entry.entry_id)
    other.checkpoint(force=True)
    assert memory_entries_for_owner(runtime.store, entry.owner) == []


@pytest.mark.parametrize('text',['remember my preference','メールは要確認'])
def test_unstructured_words_do_not_dispatch_memory_writes(text):
    from nullion.memory import capture_explicit_user_memory
    store=RuntimeStore()
    assert capture_explicit_user_memory(store,owner='workspace:alpha',text=text,source='chat')==[]
    assert store.list_user_memory_entries()==[]


def memory_registry(runtime, *, workspace='alpha', text='メールを送る前に確認してください'):
    from nullion.memory_tools import MemoryToolRegistry
    return MemoryToolRegistry(ToolRegistry(), runtime=runtime, workspace_id=workspace,
                              conversation_id='web:memory', user_message=text)


def save_invocation(quote='メールを送る前に確認してください', value='メールを送る前に確認'):
    return ToolInvocation('save', 'memory_save', 'web:memory', {
        'key': '連絡の確認', 'value': value, 'kind': 'preference', 'source_quote': quote})


def test_structured_memory_save_has_durable_receipt_and_workspace_isolation(tmp_path):
    from nullion.memory import memory_entries_for_owner
    path = tmp_path/'runtime.db'
    runtime = bootstrap_persistent_runtime(path)
    stale = bootstrap_persistent_runtime(path)
    result = memory_registry(runtime).invoke(save_invocation())
    assert result.status == 'completed' and result.output['persisted'] is True
    assert memory_entries_for_owner(stale.store,'workspace:alpha')[0].value == 'メールを送る前に確認'
    stale.checkpoint(force=True)
    restarted = bootstrap_persistent_runtime(path)
    search = memory_registry(restarted).invoke(ToolInvocation('read','memory_search','web:memory',{'query':'連絡の確認'}))
    assert len(search.output['matches']) == 1
    other = memory_registry(restarted,workspace='beta').invoke(ToolInvocation('read','memory_search','web:memory',{}))
    assert other.output['matches'] == []
    # A new entry update changes the snapshot even within the same process.
    memory_registry(runtime).invoke(save_invocation(value='必ず確認する'))
    assert memory_entries_for_owner(stale.store,'workspace:alpha')[0].value == '必ず確認する'


@pytest.mark.parametrize('case', ['source','secret','disabled','write_failure'])
def test_memory_save_rejects_unverified_sources_and_never_claims_failed_write(tmp_path, monkeypatch, case):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=memory_registry(runtime)
    invocation=save_invocation()
    if case=='source':invocation.arguments['source_quote']='unrelated human quote'
    if case=='secret':invocation.arguments['value']='api_key=secret-credential'
    if case=='disabled':monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    if case=='write_failure':
        def fail(*args):raise OSError('unwritable store')
        monkeypatch.setattr('nullion.runtime_persistence.save_user_memory_entry',fail)
    result=registry.invoke(invocation)
    assert result.status=='failed' and result.output['persisted'] is False
    assert runtime.store.list_user_memory_entries()==[]
    assert not runtime.store._dirty_memory_entry_ids


def test_automatic_rewrite_and_cleanup_preserve_explicit_preference(tmp_path):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    saved=memory_registry(runtime).invoke(save_invocation())
    policy=builder_memory.MemoryPolicy(long_term_limit=1,mid_term_limit=1,short_term_limit=1)
    builder_memory.smart_cleanup_owner_memory(runtime.store,'workspace:alpha',policy=policy,reserve_ratio=0)
    assert runtime.store.get_user_memory_entry(saved.output['memory_id']) is not None
    import json
    state={'runtime':runtime,'owner':'workspace:alpha','existing':runtime.store.list_user_memory_entries(),
           'raw_json':json.dumps({'entries':[{'kind':'preference','key':'連絡の確認','value':'no confirmation needed'},
                                          {'kind':'fact','key':'garden','value':'Measurements recorded'}]})}
    builder_memory._memory_write_node(state)
    assert runtime.store.get_user_memory_entry(saved.output['memory_id']).value=='メールを送る前に確認'


def test_stale_reinforcement_cannot_overwrite_edit_or_resurrect_deletion(tmp_path):
    from nullion.memory import memory_entries_for_owner
    path=tmp_path/'runtime.db';first=bootstrap_persistent_runtime(path)
    entry=remember_text_fact(first.store,owner='workspace:alpha',key='consent',value='confirm',source='structured')
    first.checkpoint(force=True);stale=bootstrap_persistent_runtime(path)
    builder_memory.reinforce_memory_entries(stale.store, stale.store.list_user_memory_entries())
    remember_text_fact(first.store,owner='workspace:alpha',key='consent',value='always confirm',source='structured')
    first.checkpoint(force=True);stale.checkpoint(force=True)
    assert memory_entries_for_owner(stale.store,'workspace:alpha')[0].value=='always confirm'
    builder_memory.reinforce_memory_entries(stale.store, stale.store.list_user_memory_entries())
    first.store.remove_user_memory_entry(entry.entry_id);first.checkpoint(force=True);stale.checkpoint(force=True)
    assert not memory_entries_for_owner(stale.store,'workspace:alpha')


def test_file_reference_matching_rejects_filename_prefixes():
    from nullion.artifacts import text_references_file_path
    assert text_references_file_path('Here: **旅程.pdf**.', '/data/旅程.pdf')
    assert not text_references_file_path('旅程.pdf.backup', '/data/旅程.pdf')
    assert not text_references_file_path('other-旅程.pdf', '/data/旅程.pdf')


def test_history_candidate_dump_does_not_become_authoritative_memory():
    from nullion.conversation_history_tools import _event_record, _event_text
    event={'user_message':'Find document','assistant_reply':'I found 112 files:\nTransforms.py',
           'tool_results':[{'tool_name':'file_search','status':'completed','output':{'matches':['Transforms.py']}}]}
    assert 'Transforms' not in _event_text(event)
    record=_event_record(event,index=0)
    assert record['evidence_kind']=='search_candidates_only'
    assert 'Transforms' not in record['assistant_reply']


def test_memory_save_is_required_after_explicit_scope(tmp_path):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=ScopedTurnToolRegistry(memory_registry(runtime),evidence=TurnToolEvidence(saved_history_available=True))
    result=registry.invoke(ToolInvocation('scope','request_tool_scope','web:memory',{'tool_names':['memory_save']}))
    assert 'memory_save' in result.output['required_tool_names']


def test_history_references_do_not_escape_workspace_and_report_missing_files(tmp_path):
    from nullion.conversation_history_tools import _event_artifact_refs
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    roots=workspace_storage_roots_for_workspace('workspace_admin')
    foreign=workspace_storage_roots_for_workspace('another-owner').artifacts/'foreign.pdf'
    foreign.parent.mkdir(parents=True,exist_ok=True);foreign.write_bytes(b'%PDF-1.4\n')
    missing=roots.artifacts/'missing.pdf'
    refs=_event_artifact_refs({'workspace_id':'workspace_admin','assistant_reply':f'MEDIA:{foreign}\nMEDIA:{missing}'})
    assert len(refs)==1 and refs[0]['name']=='missing.pdf' and refs[0]['availability']=='missing'


def test_corrupt_memory_is_not_reported_as_empty_or_overwritten_by_checkpoint(tmp_path):
    import sqlite3
    from nullion.memory import memory_entries_for_owner
    from nullion.runtime_persistence import save_runtime_store
    path=tmp_path/'runtime.db';runtime=bootstrap_persistent_runtime(path)
    memory_registry(runtime).invoke(save_invocation())
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE memory SET payload='unreadable',updated_at='changed'")
    with pytest.raises(ValueError,match='could not be verified'):
        memory_entries_for_owner(runtime.store,'workspace:alpha')
    with pytest.raises(ValueError,match='could not be verified'):
        save_runtime_store(runtime.store,path,merge_existing=True)
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT payload FROM memory').fetchone()[0]=='unreadable'
    result=memory_registry(runtime).invoke(ToolInvocation('read','memory_search','web:memory',{}))
    assert result.status=='failed' and result.output['reason']=='memory_recall_unavailable'


def test_narrow_extraction_cannot_rewrite_explicit_preference(tmp_path):
    import json
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    saved=memory_registry(runtime).invoke(save_invocation())
    class Model:
        def create(self,**kwargs):
            return {'content':[{'type':'text','text':json.dumps({'entries':[{'kind':'preference','key':'連絡の確認','value':'Changed automatically'}]})}]}
    builder_memory.capture_turn_memory_claims(runtime,Model(),owner='workspace:alpha',user_message='Unrelated',assistant_reply='Okay')
    assert runtime.store.get_user_memory_entry(saved.output['memory_id']).value=='メールを送る前に確認'


def test_history_accepts_string_paths_from_real_artifact_descriptors(tmp_path):
    from nullion.conversation_history_tools import _event_record, _event_text
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    target=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'cedar.pdf'
    target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b'%PDF-1.4\n')
    event={'workspace_id':'workspace_admin','user_message':'Cedar','assistant_reply':'Attached document',
           'tool_results':[{'tool_name':'artifact_delivery','status':'completed','output':{
               'artifact_descriptors':[{'path':str(target),'role':'deliverable'}]}}]}
    assert 'cedar.pdf' in _event_text(event)
    assert _event_record(event,index=0)['artifact_refs'][0]['availability']=='available'


@pytest.mark.parametrize("tool_name", ["memory_save", "memory_search"])
@pytest.mark.parametrize("source_requested", [False, True])
def test_local_memory_scope_does_not_prompt_for_unused_account_sources(tmp_path,monkeypatch,tool_name,source_requested):
    from nullion import turn_context_policy
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass
    monkeypatch.setattr(turn_context_policy,'_active_connector_provider_context',lambda:[{
        'provider_id':'example','display_name':'Example connector','active_app_ids':['google-mail','google-calendar'],
        'structured_tools':['email_search','calendar_list']}])
    delegate=ToolRegistry()
    for name in ['email_search','calendar_list']:
        delegate.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,
                                  capability_tags=('connector','account_read')),lambda invocation:None)
    from nullion.memory_tools import MemoryToolRegistry
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=ScopedTurnToolRegistry(MemoryToolRegistry(delegate,runtime=runtime,workspace_id='alpha',conversation_id='web:memory',user_message='確認'),
                                   evidence=TurnToolEvidence())
    result,_=registry.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:memory',{'capabilities':['connector'],'tool_names':[tool_name],'source_user_requested':source_requested}))
    assert result.output['available_tools']==[tool_name]
    assert result.output['source_selection_required'] is False


def test_failed_memory_write_does_not_rollback_a_newer_concurrent_edit(tmp_path,monkeypatch):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    def concurrent_edit_then_failure(*args):
        remember_text_fact(runtime.store,owner='workspace:alpha',key='連絡の確認',value='Newer authorized edit',source='explicit_memory_tool',kind=UserMemoryKind.PREFERENCE)
        raise OSError('First write failed')
    monkeypatch.setattr('nullion.runtime_persistence.save_user_memory_entry',concurrent_edit_then_failure)
    result=memory_registry(runtime).invoke(save_invocation())
    assert result.status=='failed'
    assert runtime.store.list_user_memory_entries()[0].value=='Newer authorized edit'
    assert runtime.store._dirty_memory_entry_ids


@pytest.mark.parametrize('prompt',['Retrieve the saved document','保存した文書を送ってください','Entrega el documento guardado'])
def test_verified_source_delivery_scope_is_not_downgraded_to_a_file_listing(tmp_path,prompt):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    base=register_workspace_plugin(ToolRegistry(),workspace_root=tmp_path)
    registry=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(current_user_message=prompt))
    arguments={'capabilities':['local_files'],'tool_names':['file_search'],'artifact_extensions':['.pdf'],
               'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt}
    result,_=registry.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:memory',arguments))
    assert result.output['artifact_extensions']==['.pdf']
    arguments['source_artifact_request_evidence']='Unverified different request'
    result,_=registry.apply_scope_request(ToolInvocation('scope2','request_tool_scope','web:memory',arguments))
    assert result.output['artifact_extensions']==[]


@pytest.mark.parametrize('case',['valid','no_consent','other_file','writer_required','failed_read'])
def test_existing_document_receipt_requires_verified_selection_and_read(tmp_path,case):
    from nullion.agent_orchestrator import TurnResult
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    target=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'cedar.pdf'
    target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b'%PDF-1.4\n')
    scope=ToolResult('scope','request_tool_scope','completed',{'artifact_extensions':['.pdf'],
        'source_artifact_delivery_requested':case!='no_consent','source_artifact_request_evidence':'Send saved document',
        'requested_required_tool_names':['pdf_create'] if case=='writer_required' else ['file_read']})
    read=ToolResult('read','file_read','failed' if case=='failed_read' else 'completed',
                   {'path':str(tmp_path/'foreign'/target.name) if case=='other_file' else str(target),'content':'Cedar'})
    result=TurnResult('turn','Here is the document.\nMEDIA:'+str(target),tool_results=[scope,read])
    chat_operator._register_selected_existing_source_artifacts(runtime,result,prompt='Send saved document',principal_id='web:example')
    if case=='valid':
        assert result.artifacts==[str(target)]
        assert read.output['artifact_descriptors'][0]['role']=='deliverable'
        assert read.output['artifact_descriptors'][0]['origin']=='existing_workspace_artifact'
    else:
        assert not result.artifacts and not read.output.get('artifact_descriptors')


@pytest.mark.parametrize('prompt',['For the Cedar trip, do you have the PDF?','杉の旅行のPDFはありますか？','¿Tienes el PDF del viaje Cedar?'])
def test_history_selected_document_answer_survives_unrelated_file_search(prompt):
    from nullion.chat_operator import _sanitize_chat_reply
    from nullion.response_sanitizer import sanitize_user_visible_reply
    result=ToolResult('history','chat_history_search','completed',{'matches':[{'artifact_refs':[
        {'path':'/tmp/cedar-itinerary.pdf','name':'cedar-itinerary.pdf','extension':'.pdf','availability':'available'}]}]})
    searches=[ToolResult('search','file_search','completed',{'matches':['/tmp/msft.xml','/tmp/cedar-photo.png']}),
              ToolResult('empty','file_search','completed',{'matches':[]})]
    reply='Yes — cedar-itinerary.pdf is available from the earlier report.'
    assert sanitize_user_visible_reply(user_message=prompt,reply=reply,tool_results=[result,*searches])==reply
    assert _sanitize_chat_reply(prompt=prompt,reply=reply,tool_results=[result,*searches])==reply


@pytest.mark.parametrize('boundary',['model','web','telegram'])
def test_large_history_keeps_ranked_document_references_typed_at_each_boundary(monkeypatch,boundary):
    import json
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.chat_operator import _compact_tool_output_for_context
    from nullion.web_app import _compact_web_tool_output_for_context
    records=[{'index':index,'turn_id':f'previous-{index}','user_message':'Cedar itinerary '*60,'assistant_reply':'Trip details '*70,
              'artifact_refs':[{'type':'artifact','name':f'cedar-{index}.pdf','path':f'/owned/artifacts/cedar-{index}.pdf','extension':'.pdf','availability':'available'}],
              'tool_evidence':[{'tool_name':'pdf_create','status':'completed','output_preview':'internal source payload '*400}]} for index in range(20)]
    output={'conversation_id':'web:current','workspace_id':'alpha','match_count':20,'matches':records,'structured_matches':records[:12]}
    monkeypatch.setenv('NULLION_MODEL_TOOL_RESULT_MAX_CHARS','10000')
    if boundary=='model':
        encoded=_tool_result_message_payload(ToolResult('history','chat_history_search','completed',output))
        result=json.loads(encoded)['output'];assert len(encoded)<=10000
    else:
        result=(_compact_web_tool_output_for_context if boundary=='web' else _compact_tool_output_for_context)('chat_history_search',output)
        assert len(json.dumps(result,ensure_ascii=False))<=12000
    assert isinstance(result['matches'],list)
    assert result['matches'][0]['artifact_refs'][0]['path']=='/owned/artifacts/cedar-0.pdf'
    assert result['matches'][0]['artifact_refs'][0]['availability']=='available'
    assert 'internal source payload' not in json.dumps(result)
    assert 'preview' not in result


def test_retrieved_history_artifact_is_not_a_new_producer_receipt(tmp_path):
    from nullion.agent_orchestrator import _stale_artifact_extensions_after_new_evidence
    from nullion.artifacts import register_selected_existing_source_artifacts
    path=tmp_path/'cedar.pdf';path.write_bytes(b'%PDF-1.4\n')
    history=ToolResult('history','chat_history_search','completed',{'matches':[{'artifact_refs':[
        {'path':str(path),'name':path.name,'extension':'.pdf','availability':'available'}]}]})
    search=ToolResult('search','file_search','completed',{'matches':[str(tmp_path/'cedar-photo.png')]})
    selected=register_selected_existing_source_artifacts(reply='Attached.\nMEDIA:'+str(path),user_message='保存した文書は？',tool_results=[history,search],artifact_roots=[tmp_path])
    assert selected==[str(path)]
    assert history.output['artifact_descriptors'][0]['origin']=='existing_workspace_artifact'
    assert not _stale_artifact_extensions_after_new_evidence([history,search],required_extensions=['.pdf'])


@pytest.mark.parametrize('topic',['Cedar','杉の旅行','Viaje Cedro'])
def test_typed_history_format_filter_recovers_available_caption_filename(tmp_path,monkeypatch,topic):
    from nullion.conversation_history_tools import _chat_history_search_result
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'storage'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    root=workspace_storage_roots_for_workspace('alpha').artifacts
    root.mkdir(parents=True,exist_ok=True);(root/'cedar.pdf').write_bytes(b'%PDF-1.4\n')
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'telegram:alpha','workspace_id':'alpha',
        'created_at':'2026-10-01T10:00:00Z','turn_id':'prior-document','user_message':topic,'assistant_reply':'Attached **cedar.pdf**.'})
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'web:alpha','workspace_id':'alpha',
        'created_at':'2026-10-02T10:00:00Z','turn_id':'unrelated-inventory','user_message':topic+' weather tooling','assistant_reply':topic+' directory inventory'})
    result=_chat_history_search_result(runtime=runtime,conversation_id='web:alpha',workspace_id='alpha',
        invocation=ToolInvocation('history','chat_history_search','web:alpha',{'query':topic,'artifact_extensions':['.pdf']}))
    assert result.status=='completed'
    assert result.output['requested_artifact_extensions']==['.pdf']
    assert len(result.output['matches'])==1
    assert result.output['matches'][0]['turn_id']=='prior-document'
    assert result.output['matches'][0]['artifact_refs'][0]['path']==str(root/'cedar.pdf')
    assert result.output['matches'][0]['artifact_refs'][0]['availability']=='available'


@pytest.mark.parametrize('prompt',['For the Cedar trip, do you have the PDF?','杉の旅行のPDFはありますか？','¿Tienes el PDF del viaje Cedar?'])
def test_existing_history_delivery_scope_does_not_enable_document_rewriters(tmp_path,prompt):
    from nullion.memory_tools import MemoryToolRegistry
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass
    from nullion.tools import create_plugin_tool_registry
    delegate=register_workspace_plugin(create_plugin_tool_registry(workspace_root=tmp_path),workspace_root=tmp_path)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=ScopedTurnToolRegistry(MemoryToolRegistry(delegate,runtime=runtime,workspace_id='alpha',conversation_id='web:source',user_message=prompt),
                                   evidence=TurnToolEvidence(current_user_message=prompt))
    result,_=registry.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:source',{
        'capabilities':['conversation_history'],'tool_names':['memory_search'],'artifact_extensions':['.pdf'],
        'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt}))
    assert result.status=='completed'
    assert {'file_read','file_search'}.issubset(result.output['available_tools'])
    assert not {'pdf_create','pdf_edit','file_write','file_patch'}.intersection(result.output['available_tools'])
    assert not registry.can_invoke_tool('pdf_create') and not registry.can_invoke_tool('pdf_edit')


def test_verified_history_selection_reconciles_read_source_role(tmp_path):
    from nullion.artifacts import register_selected_existing_source_artifacts
    from nullion.chat_operator import _non_deliverable_artifact_descriptor_paths
    path=tmp_path/'cedar.pdf';path.write_bytes(b'%PDF-1.4\n')
    history=ToolResult('history','chat_history_search','completed',{'matches':[{'artifact_refs':[
        {'path':str(path),'name':path.name,'extension':'.pdf','availability':'available'}]}]})
    read=ToolResult('read','file_read','completed',{'path':str(path),'artifact_descriptors':[
        {'path':str(path),'role':'source'},{'path':str(tmp_path/'unrelated.png'),'role':'source'}]})
    assert register_selected_existing_source_artifacts(reply='MEDIA:'+str(path),user_message='保存した文書は？',tool_results=[history,read],artifact_roots=[tmp_path])==[str(path)]
    assert str(path) not in _non_deliverable_artifact_descriptor_paths([history,read])
    assert str(tmp_path/'unrelated.png') in _non_deliverable_artifact_descriptor_paths([history,read])
    assert read.output['artifact_descriptors'][0]['origin']=='existing_workspace_artifact'


@pytest.mark.parametrize('prompt',['Retrieve the saved Cedar document.','保存した杉の文書を送ってください。','Entrega el documento Cedar guardado.'])
def test_last_mile_retrieval_repair_keeps_read_scope_and_human_evidence(tmp_path,prompt):
    from nullion.chat_operator import _repair_tool_registry_for_missing_attachment_extensions,_required_attachment_repair_prompt
    from nullion.conversation_history_tools import with_conversation_history_tool
    from nullion.tools import create_plugin_tool_registry
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    base=with_conversation_history_tool(register_workspace_plugin(create_plugin_tool_registry(workspace_root=tmp_path),workspace_root=tmp_path),
        runtime=runtime,conversation_id='web:repair',workspace_id='alpha',user_message=prompt)
    scope=ToolResult('scope','request_tool_scope','completed',{'capabilities':['conversation_history','local_files'],
        'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt,
        'explicit_tool_names':['chat_history_search','file_read'],'required_tool_names':[]})
    active=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(current_user_message=prompt))
    repair=_repair_tool_registry_for_missing_attachment_extensions(base_registry=base,active_registry=active,missing_extensions=('.pdf',),
        source_tool_results=[scope],user_message=prompt)
    names={spec.name for spec in repair.list_specs()}
    assert {'file_read','file_search','chat_history_search'}.issubset(names)
    assert not {'pdf_create','pdf_edit','file_write','file_patch','memory_save','request_tool_scope'}.intersection(names)
    assert repair.turn_tool_scope_decision.source_artifact_request_evidence==prompt
    assert repair.turn_tool_scope_decision.source_artifact_delivery_requested is True
    text=_required_attachment_repair_prompt(('.pdf',),source_tool_results=[scope])
    assert 'existing' in text and 'Do not create, edit, overwrite or regenerate' in text


def test_existing_delivery_drops_hidden_creator_requirement_before_repair(tmp_path):
    from nullion.chat_operator import _existing_source_retrieval_scope
    from nullion.tools import create_plugin_tool_registry
    base=register_workspace_plugin(create_plugin_tool_registry(workspace_root=tmp_path),workspace_root=tmp_path)
    prompt='保存したcedar.pdfを送ってください。'
    registry=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(current_user_message=prompt,existing_named_artifact_extensions=('.pdf',)))
    result,_=registry.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:existing',{
        'capabilities':['conversation_history'],'tool_names':['file_read','file_search','pdf_create'],'required_tool_names':['file_read','pdf_create'],
        'artifact_extensions':['.pdf'],'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt}))
    assert result.output['read_only_source_delivery'] is True
    assert 'pdf_create' not in result.output['available_tools']
    assert 'pdf_create' not in result.output['required_tool_names']
    assert 'pdf_create' not in result.output['requested_required_tool_names']
    assert set(result.output['required_tool_names']).issubset(result.output['available_tools'])
    assert _existing_source_retrieval_scope([result]) is not None
