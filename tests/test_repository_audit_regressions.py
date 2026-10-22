"""Fault contracts for authorization, persistence, publication, and recovery."""
from __future__ import annotations
import asyncio
import ast
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
from urllib.request import Request

import pytest


def test_runtime_modules_do_not_silently_shadow_module_definitions():
    import nullion
    duplicates=[]
    for path in Path(nullion.__file__).parent.glob('*.py'):
        names=set()
        for node in ast.parse(path.read_text()).body:
            if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):
                if node.name in names:duplicates.append(f'{path.name}:{node.lineno}:{node.name}')
                names.add(node.name)
    assert not duplicates,duplicates


@pytest.mark.parametrize('selected',[[],['result.png']])
def test_history_metadata_preserves_selected_package_over_read_inputs(tmp_path,selected):
    from nullion.chat_store import _artifact_metadata_from_runtime_turn
    source=tmp_path/'source.html';source.write_text('<h1>Source</h1>')
    artifact=tmp_path/'result.png';artifact.write_bytes(b'fixture')
    paths=[str(tmp_path/name) for name in selected]
    metadata=_artifact_metadata_from_runtime_turn({'artifact_paths':paths,'tool_results':[
        {'tool_name':'file_read','status':'completed','output':{'path':str(source)}},
        {'tool_name':'terminal_exec','status':'completed','output':{'artifact_paths':[str(artifact)]}},
    ]})
    assert metadata==({'artifacts':[{'path':path} for path in paths]} if paths else None)


def test_explicit_format_exclusions_survive_raw_producer_paths_and_descriptors(tmp_path):
    from nullion.artifacts import filter_source_artifact_paths
    from nullion.tools import ToolResult
    source=str(tmp_path/'source.html');out=str(tmp_path/'out.png')
    results=[ToolResult('scope','request_tool_scope','completed',{'excluded_artifact_extensions':['.html']}),
             ToolResult('shell','terminal_exec','completed',{'artifact_paths':[source,out],
                        'artifact_descriptors':[{'path':source,'role':'deliverable'}]})]
    assert filter_source_artifact_paths([source,out],results)==[out]


def test_scheduler_tool_context_preserves_health_and_doctor_threshold():
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.tools import ToolResult
    status={'state':'paused','doctor_streak_limit':12,'health_notifications_enabled':True,
            'consecutive_failures':0,'consecutive_no_matches':4}
    result=ToolResult('cron','list_crons','completed',{'crons':[{'id':'fixture','name':'Cedar',
        'enabled':False,'tracker_status':status}]})
    payload=json.loads(_tool_result_message_payload(result))
    assert payload['output']['crons'][0]['tracker_status']==status


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    from nullion import crons, users, connections
    home = tmp_path / 'lane'
    home.mkdir()
    for name, value in {'NULLION_HOME':home, 'NULLION_ENV_FILE':home/'.env',
        'NULLION_CHECKPOINT_PATH':home/'runtime.db', 'NULLION_KEY_STORAGE':'local',
        'NULLION_MEMORY_ENABLED':'0', 'NULLION_CHAT_STARTUP_WARMUP':'0'}.items():
        monkeypatch.setenv(name, str(value))
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', home/'runtime.db')
    monkeypatch.setattr(users, '_USERS_PATH', home/'users.json')
    monkeypatch.setattr(connections, '_CONNECTIONS_PATH', home/'connections.json')
    return home


def test_cron_workspace_authorization_uses_registered_users(isolated):
    from nullion.users import UserRegistry, NullionUser, save_user_registry
    from nullion.crons import get_cron
    from nullion.tools import ToolRegistry, ToolInvocation, register_cron_tools
    save_user_registry(UserRegistry(True,[NullionUser('member','Member',workspace_id='workspace_member')]))
    registry=ToolRegistry();register_cron_tools(registry)
    def invoke(name,args,principal='user:member'):
        return registry.invoke(ToolInvocation(name,name,principal,args))
    args={'name':'Fixture','task':'Fixture instructions','schedule':'0 9 * * *'}
    admin=invoke('create_cron',{**args,'workspace_id':'workspace_admin'},'operator')
    assert admin.status=='completed'
    assert invoke('list_crons',{'include_all_workspaces':True}).status=='denied'
    assert invoke('list_crons',{'workspace_id':'workspace_admin'}).status=='denied'
    assert invoke('create_cron',{**args,'workspace_id':'workspace_admin'}).status=='denied'
    member=invoke('create_cron',args)
    assert member.status=='completed'
    member_id=member.output['id']
    assert invoke('update_cron',{'id':member_id,'workspace_id':'workspace_admin'}).status=='denied'
    assert get_cron(member_id).workspace_id=='workspace_member'
    assert invoke('delete_cron',{'id':admin.output['id']}).status=='failed'
    assert invoke('list_crons',{'include_all_workspaces':True},'operator').status=='completed'
    for name,args in [('create_cron',args),('list_crons',{})]:
        assert invoke(name,args,'user:unknown').status=='denied'


def test_cron_write_failure_and_conflict_cannot_create_success_receipt(isolated):
    from nullion.crons import add_cron, get_cron, list_crons, save_crons, update_cron
    from nullion.tools import ToolRegistry, ToolInvocation, register_cron_tools
    first=add_cron(name='A',task='A',schedule='0 9 * * *')
    stale=get_cron(first.id)
    update_cron(first.id,task='B')
    stale.task='C'
    with pytest.raises(RuntimeError,match='changed'):
        save_crons([stale])
    assert get_cron(first.id).task=='B'
    with sqlite3.connect(isolated/'runtime.db') as conn:
        conn.execute("CREATE TRIGGER fail_cron BEFORE INSERT ON reminders_crons WHEN NEW.collection='cron_jobs' BEGIN SELECT RAISE(ABORT,'fixture storage failure'); END")
    registry=ToolRegistry();register_cron_tools(registry)
    result=registry.invoke(ToolInvocation('create','create_cron','operator',{'name':'D','task':'D','schedule':'0 8 * * *'}))
    assert result.status=='failed' and not result.output.get('action_receipt')
    assert len(list_crons(refresh_next_runs=False))==1


def test_monitor_settings_preserve_due_occurrence(isolated):
    from nullion import crons
    job=crons.add_cron(name='Fixture',task='Fixture',schedule='0 9 * * *')
    job.next_run=(datetime.now(UTC)-timedelta(hours=1)).isoformat()
    crons.save_crons([job])
    updated=crons.update_cron(job.id,artifact_delivery_options={'doctor_streak_limit':20})
    assert updated.next_run==job.next_run


def test_cron_edits_preserve_inventory_order(isolated):
    from nullion import crons
    jobs=[crons.add_cron(name=name,task='Fixture',schedule='0 9 * * *') for name in ('One','Two','Three')]
    crons.update_cron(jobs[0].id,task='Updated fixture')
    crons.toggle_cron(jobs[1].id,False)
    assert [job.id for job in crons.list_crons(refresh_next_runs=False)]==[job.id for job in jobs]


def test_backup_during_writer_rollback_contains_only_committed_rows(tmp_path):
    from nullion.runtime_persistence import _snapshot_runtime_sqlite, _backup_path_for
    database=tmp_path/'runtime.db'
    with sqlite3.connect(database) as conn:
        conn.execute('CREATE TABLE fixture(value TEXT)')
        conn.executemany('INSERT INTO fixture VALUES (?)',[('old'*4000,)]*100)
    ready=threading.Event()
    def writer():
        with sqlite3.connect(database) as conn:
            conn.execute('PRAGMA cache_size=5');conn.execute('BEGIN IMMEDIATE')
            conn.execute('UPDATE fixture SET value=?',('new'*4000,));ready.set()
            time.sleep(.15);conn.rollback()
    thread=threading.Thread(target=writer);thread.start();assert ready.wait(2)
    _snapshot_runtime_sqlite(database)
    thread.join(2);assert not thread.is_alive()
    with sqlite3.connect(_backup_path_for(database,0)) as conn:
        assert conn.execute('SELECT DISTINCT substr(value,1,3) FROM fixture').fetchall()==[('old',)]
        assert conn.execute('PRAGMA quick_check').fetchone()==('ok',)


def test_backup_closes_handles_before_atomic_publish(tmp_path,monkeypatch):
    from nullion.runtime_persistence import _snapshot_runtime_sqlite
    database=tmp_path/'runtime.db'
    initial=sqlite3.connect(database);initial.execute('CREATE TABLE fixture(value TEXT)');initial.commit();initial.close()
    connect=sqlite3.connect;replace_path=Path.replace;open_handles=set()
    class TrackedConnection(sqlite3.Connection):
        def __init__(self,*a,**kw):super().__init__(*a,**kw);open_handles.add(id(self))
        def close(self):
            try:return super().close()
            finally:open_handles.discard(id(self))
    def tracked_connect(*a,**kw):return connect(*a,**kw,factory=TrackedConnection)
    def publish(path,target):
        assert not open_handles,'Publishing an open SQLite file fails on Windows'
        return replace_path(path,target)
    monkeypatch.setattr(sqlite3,'connect',tracked_connect);monkeypatch.setattr(Path,'replace',publish)
    _snapshot_runtime_sqlite(database)
    assert not open_handles


@pytest.mark.parametrize('failure',['missing','invalid','swap'])
def test_skill_replacement_preserves_working_pack_on_failure(isolated,tmp_path,monkeypatch,failure):
    from nullion.skill_pack_installer import install_skill_pack, skill_pack_path
    root=isolated/'packs';source=tmp_path/'source';source.mkdir();(source/'SKILL.md').write_text('Original')
    install_skill_pack(str(source),pack_id='fixture/pack',root=root)
    destination=skill_pack_path('fixture/pack',root=root)
    replacement=tmp_path/'replacement'
    if failure!='missing': replacement.mkdir()
    if failure=='swap':
        (replacement/'SKILL.md').write_text('Replacement')
        original=Path.replace
        def replace(path,target):
            if path.name=='pack' and path.parent.name.startswith('.install-'):
                raise OSError('fixture interrupted swap')
            return original(path,target)
        monkeypatch.setattr(Path,'replace',replace)
    with pytest.raises((ValueError,OSError)):
        install_skill_pack(str(replacement),pack_id='fixture/pack',root=root,force=True)
    assert (destination/'SKILL.md').read_text()=='Original'


def test_redirect_denies_private_dns_and_strips_cross_origin_authorization(monkeypatch):
    from nullion import tools
    def resolve(url):
        return object() if 'public.example.test' in url else None
    monkeypatch.setattr(tools,'_resolve_web_fetch_resolution',resolve)
    handler=tools._SafeWebFetchRedirectHandler()
    req=Request('https://origin.example.test/',headers={'Authorization':'Bearer fixture','Cookie':'fixture=1'})
    redirected=handler.redirect_request(req,None,302,'Found',{},'https://public.example.test/')
    assert redirected is not None and redirected.get_header('Authorization') is None and redirected.get_header('Cookie') is None
    assert handler.redirect_request(req,None,302,'Found',{},'http://localhost/') is None
    assert handler.redirect_request(req,None,302,'Found',{},'https://private.example.test/') is None
    req._nullion_allowed_base_urls=('https://origin.example.test/api/',)
    assert handler.redirect_request(req,None,302,'Found',{},'https://public.example.test/') is None


def test_publication_cannot_embed_outside_or_symlinked_assets(tmp_path,monkeypatch):
    from nullion.cron_delivery import _inline_html_local_image_assets
    reports=tmp_path/'reports';reports.mkdir();private=tmp_path/'private.png';private.write_bytes(b'private fixture')
    (reports/'alias.png').symlink_to(private)
    local=reports/'allowed.png';local.write_bytes(b'allowed fixture')
    report=reports/'report.html'
    report.write_text(f'<html><body><img src="{private}"><img src="alias.png"><img src="allowed.png"><img src="http://localhost/private.png"></body></html>')
    def network_forbidden(*args,**kwargs): raise AssertionError('Publication attempted network I/O')
    monkeypatch.setattr('nullion.cron_delivery.urlopen',network_forbidden)
    assets=_inline_html_local_image_assets(str(report),html_image_delivery_mode='self_contained')
    assert assets=={str(local.resolve())}
    assert 'cHJpdmF0ZSBmaXh0dXJl' not in report.read_text()
    assert 'data:image/png;base64,' in report.read_text()


def test_tracker_reconciles_lost_lease_without_reclassifying_active_job(isolated):
    from nullion.crons import CronJob
    from nullion.cron_scheduler_state import execution_lease
    from nullion.tracker_status import start_check,tracker_statuses
    job=CronJob('fixture','Fixture','* * * * *','Fixture')
    with execution_lease(isolated/'runtime.db',job.id):
        start_check(isolated/'runtime.db',job,'owned',now=datetime.now(UTC)-timedelta(days=3))
        assert tracker_statuses(isolated/'runtime.db',[job])[job.id]['state']=='running'
    facts=tracker_statuses(isolated/'runtime.db',[job])[job.id]
    assert facts['state']=='interrupted' and facts['consecutive_failures']==1
    assert facts['incomplete_checks_24h']==1
    with sqlite3.connect(isolated/'runtime.db') as conn:
        assert conn.execute("SELECT status FROM cron_tracker_runs WHERE run_id='owned'").fetchone()==('interrupted',)


@pytest.mark.parametrize('prompt',['Do not attach fixture.pdf','No adjuntes fixture.pdf','fixture.pdf を添付しないでください'])
def test_delivery_exclusions_use_typed_scope_in_every_language(prompt):
    from nullion.chat_operator import _refine_required_attachment_extensions_for_structured_outputs
    from nullion.tools import ToolResult
    scope=ToolResult('scope','request_tool_scope','completed',{'excluded_artifact_extensions':['.pdf']})
    assert _refine_required_attachment_extensions_for_structured_outputs(prompt=prompt,required_attachment_extensions=('.pdf',),tool_results=[scope])==()


@pytest.mark.parametrize('source',['decision','tool_result'])
def test_current_delivery_exclusions_override_inherited_flow_contract(source):
    from types import SimpleNamespace
    from nullion.turn_context_policy import TurnToolScopeDecision,TurnToolEvidence
    from nullion.tools import ToolResult
    from nullion.agent_orchestrator import (_required_attachment_extensions_from_turn_state,
        _required_embedded_media_extensions_from_turn_state,_turn_has_artifact_delivery_contract,_refresh_scoped_delivery_context)
    decision=TurnToolScopeDecision(valid=True,requested_artifact_extensions=('.pdf',),
        excluded_artifact_extensions=('.pdf',) if source=='decision' else ())
    registry=SimpleNamespace(turn_tool_scope_decision=decision,_evidence=TurnToolEvidence(requested_extensions=('.pdf',)))
    state={'tool_registry':registry,'tool_flow_context':{'required_artifact_extensions':['.pdf'],
        'requires_artifact_delivery':True,'required_embedded_media_extensions':['.pdf']},
        'tool_results':[ToolResult('scope','request_tool_scope','completed',{'excluded_artifact_extensions':['.pdf']})] if source=='tool_result' else []}
    assert _required_attachment_extensions_from_turn_state(state)==()
    assert _required_embedded_media_extensions_from_turn_state(state)==()
    assert not _turn_has_artifact_delivery_contract(state)
    _refresh_scoped_delivery_context(state,registry)
    assert state['tool_flow_context']['requires_artifact_delivery'] is False


def test_explicit_empty_delivery_contract_does_not_reinfer_filename_repair(isolated):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.chat_operator import _missing_required_attachment_extensions_for_repair,_append_chat_artifacts_to_reply
    runtime=bootstrap_persistent_runtime(isolated/'runtime.db')
    assert _missing_required_attachment_extensions_for_repair(runtime,prompt='Create fixture.html',principal_id='operator',
        required_attachment_extensions=(),tool_results=[],artifact_paths=[])==()
    assert _append_chat_artifacts_to_reply(runtime,reply='fixture.html',prompt='Create fixture.html',
        principal_id='operator',artifact_paths=[],tool_results=[],required_attachment_extensions=())=='fixture.html'


@pytest.mark.asyncio
async def test_failed_final_text_stays_retryable_across_aggregator_restart(tmp_path):
    from nullion.result_aggregator import ResultAggregator,GroupState
    from nullion.task_queue import TaskGroup,TaskRegistry,TaskRecord,TaskStatus,TaskPriority,TaskResult
    registry=TaskRegistry();calls=[]
    def fail(*args,**kwargs): calls.append((args,kwargs));return False
    task=TaskRecord('t','g','web:fixture','operator','Fixture','Fixture',TaskStatus.COMPLETE,TaskPriority.NORMAL,[],[],result=TaskResult('t','success',output='Final fixture'))
    group=TaskGroup('g','web:fixture','Fixture',[task]);await registry.add_group(group)
    first=ResultAggregator(deliver_fn=fail,task_registry=registry,outbox_path=tmp_path/'outbox.db')
    state=GroupState('g','web:fixture','Fixture')
    await first._on_group_complete(state,group)
    assert group.planner_metadata['delivery_status']=='failed'
    assert 'g' not in first._completed_groups
    second=ResultAggregator(deliver_fn=lambda *a,**kw: True,task_registry=registry,outbox_path=tmp_path/'outbox.db')
    await second._on_group_complete(state,group)
    assert group.planner_metadata['delivery_status']=='completed'
    assert second._outbox.get('g')['text_delivered'] is True


@pytest.mark.asyncio
async def test_updater_locks_same_source_across_callers(isolated,monkeypatch):
    from nullion import updater
    source=isolated/'src';source.mkdir();monkeypatch.setenv('NULLION_SRC_DIR',str(source))
    started=asyncio.Event();finish=asyncio.Event();entered=[]
    async def operation(**kwargs):
        entered.append(True);started.set();await finish.wait()
        return updater.UpdateResult(success=True)
    monkeypatch.setattr(updater,'_run_update_locked',operation)
    first=asyncio.create_task(updater.run_update());await asyncio.wait_for(started.wait(),2)
    second=await updater.run_update()
    assert not second.success and 'already running' in second.error
    assert len(entered)==1
    finish.set();assert (await first).success


def test_update_operation_idempotency_and_observation_survive_new_store(isolated):
    from nullion.update_operations import UpdateOperations
    store=UpdateOperations(isolated/'runtime.db')
    identifier,created=store.start('one')
    assert created
    assert store.start('one')==(identifier,False)
    assert store.start('two')==(identifier,False)
    store.append(identifier,{'step':'complete','success':True},complete=True)
    restarted=UpdateOperations(isolated/'runtime.db')
    assert restarted.start('one')==(identifier,False)
    assert restarted.read(identifier)['status']=='completed'
    assert restarted.start('two')[1] is True


@pytest.mark.parametrize('access',['read','same_request','new_request'])
def test_update_crash_is_terminal_even_for_observation_and_same_request(isolated,monkeypatch,access):
    from nullion.update_operations import UpdateOperations
    import os
    store=UpdateOperations(isolated/'runtime.db');identifier,_=store.start('fixture')
    def dead(pid, signal):raise ProcessLookupError()
    monkeypatch.setattr(os,'kill',dead)
    if access=='same_request':assert store.start('fixture')==(identifier,False)
    if access=='new_request':assert store.start('retry')[1] is True
    record=store.read(identifier)
    assert record['status']=='interrupted'
    assert len([event for event in record['events'] if event['step']=='complete'])==1
    assert store.read(identifier)==record


def test_outbox_recovery_cannot_consume_another_adapter_delivery(tmp_path):
    from nullion.delivery_outbox import DeliveryOutbox
    store=DeliveryOutbox(tmp_path/'runtime.db')
    for surface in ('web','telegram','slack','discord'):
        store.put(surface,{'conversation_id':surface+':fixture','status':'pending','attempts':0,'retry_at':0})
    assert store.due(surfaces=('web',))==['web']
    assert store.due(surfaces=())==[]
    assert set(store.due())=={'web','telegram','slack','discord'}


def test_duplicate_outbox_preparation_preserves_existing_receipts(tmp_path):
    from nullion.delivery_outbox import DeliveryOutbox
    first=DeliveryOutbox(tmp_path/'runtime.db');second=DeliveryOutbox(tmp_path/'runtime.db')
    first.enqueue('fixture',{'status':'pending','delivered_artifacts':[]})
    first.put('fixture',{'status':'delivered','delivered_artifacts':['fixture.pdf']})
    second.enqueue('fixture',{'status':'pending','delivered_artifacts':[]})
    assert second.get('fixture')=={'status':'delivered','delivered_artifacts':['fixture.pdf']}


@pytest.mark.parametrize('in_loop',[False,True])
def test_confirmed_telegram_delivery_preserves_transport_false(in_loop):
    from nullion.telegram_app import _run_confirmed_telegram_delivery
    async def failed():return False
    async def in_async():return _run_confirmed_telegram_delivery(failed)
    assert (asyncio.run(in_async()) if in_loop else _run_confirmed_telegram_delivery(failed)) is False


def test_collected_image_receipt_cannot_authorize_later_symlink(tmp_path):
    from nullion.cron_delivery import _inline_html_local_image_assets
    reports=tmp_path/'reports';reports.mkdir()
    private=tmp_path/'private.png';private.write_bytes(b'PRIVATE-FIXTURE')
    image=reports/'download.png';image.symlink_to(private)
    report=reports/'report.html';report.write_text('<img src="https://images.example.test/fixture.png">')
    assert not _inline_html_local_image_assets(str(report),html_image_delivery_mode='self_contained',
        collected_assets={'https://images.example.test/fixture.png':str(image)})
    assert 'UFJJVkFURS1GSVhUVVJF' not in report.read_text()


def test_collected_media_can_be_reused_only_inside_its_workspace(tmp_path,monkeypatch):
    from nullion.cron_delivery import _inline_html_local_image_assets
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    own=workspace_storage_roots_for_workspace('workspace_admin');other=workspace_storage_roots_for_workspace('workspace_other')
    image=own.media/'collected.png';image.write_bytes(b'ALLOWED-FIXTURE')
    private=other.media/'private.png';private.write_bytes(b'PRIVATE-FIXTURE')
    alias=own.media/'alias.png';alias.symlink_to(private)
    report=own.artifacts/'report.html'
    for candidate,expected in ((image,True),(private,False),(alias,False)):
        report.write_text('<img src="https://images.example.test/fixture.png">')
        assets=_inline_html_local_image_assets(str(report),html_image_delivery_mode='self_contained',principal_id='operator',
            collected_assets={'https://images.example.test/fixture.png':str(candidate)})
        assert bool(assets) is expected
        assert 'UFJJVkFURS1GSVhUVVJF' not in report.read_text()


@pytest.mark.parametrize('path',['/api/../private','/api/%2e%2e/private','/api/%2e%2e%5cprivate'])
def test_connector_endpoint_scope_cannot_be_escaped_by_dot_segments(path):
    from nullion.tools import _url_is_under_base
    assert not _url_is_under_base('https://endpoint.example.test'+path,'https://endpoint.example.test/api/')
    assert _url_is_under_base('https://endpoint.example.test/api/records','https://endpoint.example.test/api/')


@pytest.mark.asyncio
async def test_outbox_partial_upload_retries_only_missing_receipts(tmp_path):
    from nullion.result_aggregator import ResultAggregator
    from nullion.task_queue import TaskRegistry
    registry=TaskRegistry();calls=[]
    def transport(conv,text,**kwargs):
        calls.append(text)
        return text!='second' or calls.count('second')>1
    agg=ResultAggregator(deliver_fn=transport,task_registry=registry,outbox_path=tmp_path/'outbox.db')
    agg._outbox.put('g',{'conversation_id':'web:fixture','summary':'Final fixture', 'artifacts':['first','second'],
        'delivered_artifacts':[],'text_delivered':False,'work_complete':True,'status':'pending','attempts':0,'retry_at':0})
    await agg._deliver_outbox('g')
    assert agg._outbox.get('g')['status']=='failed'
    assert calls.count('first')==1 and 'Final fixture' not in calls
    resumed=ResultAggregator(deliver_fn=transport,task_registry=registry,outbox_path=tmp_path/'outbox.db')
    await resumed._deliver_outbox('g')
    assert resumed._outbox.get('g')['status']=='delivered'
    assert calls.count('first')==1 and calls.count('second')==2 and calls.count('Final fixture')==1


def test_legacy_orphan_is_terminalized_after_grace(isolated):
    from nullion.crons import CronJob
    from nullion.tracker_status import start_check,tracker_statuses
    job=CronJob('legacy','Fixture','* * * * *','Fixture')
    start_check(isolated/'runtime.db',job,'old',now=datetime.now(UTC)-timedelta(days=7))
    first=tracker_statuses(isolated/'runtime.db',[job])[job.id]
    second=tracker_statuses(isolated/'runtime.db',[job])[job.id]
    assert first['state']==second['state']=='interrupted'
    assert first['consecutive_failures']==second['consecutive_failures']==1


@pytest.mark.parametrize("answer", [
    "The jacket shipped on October 13. The carrier page did not show a verified current location or delivery estimate.",
    "La chaqueta salió el 13 de octubre. No pude confirmar la ubicación actual ni la fecha de entrega.",
    "ジャケットは10月13日に発送されています。現在地と配達予定日は確認できませんでした。",
])
def test_email_then_browser_answer_survives_repeated_final_formatting(answer):
    from nullion.response_sanitizer import sanitize_user_visible_reply
    from nullion.tools import ToolResult
    results = [ToolResult("mail", "email_read", "completed", {"message": {
        "subject": "Your order is on the way", "from": "Shop <shop@example.test>",
        "date": "Tue, 13 Oct 2026 16:19:00 -0600", "snippet": "Your order is headed out.",
        "body": "Jacket shipped October 13. Tracking number: FIXTURE123."}}),
        ToolResult("navigate", "browser_navigate", "completed", {"session_id": "fixture", "result": "Navigated (status 200)"}),
        ToolResult("read", "browser_extract_text", "completed", {"session_id": "fixture", "text": "Track a package. Sign in."})]
    reply = answer
    for _ in range(3):
        metadata = {}
        reply = sanitize_user_visible_reply(user_message="📦?", reply=reply, tool_results=results,
            presentation_metadata=metadata)
        assert answer in reply
        assert "Preview:" not in reply and "From:" not in reply
        assert metadata.get("fulfillment_satisfied") is not False


def test_delivery_scope_exclusions_override_stale_artifact_paths_and_required_formats(isolated):
    from nullion.chat_operator import _append_chat_artifacts_to_reply
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    artifact=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'scope-fixture.html'
    artifact.parent.mkdir(parents=True,exist_ok=True)
    artifact.write_text('<!doctype html><html><body>Fixture</body></html>')
    runtime=bootstrap_persistent_runtime(artifact.parent/'fixture-runtime.db')
    scope=ToolResult('scope','request_tool_scope','completed',{'artifact_extensions':[],
        'excluded_artifact_extensions':['.html']})
    reply=_append_chat_artifacts_to_reply(runtime,reply=artifact.name,artifact_paths=[str(artifact)],
        prompt='📂 .html',principal_id='operator',required_attachment_extensions=('.html',),tool_results=[scope])
    assert artifact.name in reply and 'MEDIA:' not in reply and 'Done — attached' not in reply


@pytest.mark.parametrize('flow_flag',['requires_new_artifact_content','requires_artifact_delivery'])
@pytest.mark.parametrize('user_request',['Make a compact image from the earlier report.','前の資料を読みやすい画像にまとめてください。','Convierte el informe anterior en una imagen.'])
def test_internal_temporary_helper_recovers_into_workspace_without_approval(isolated,monkeypatch,user_request,flow_flag):
    from test_core_tool_permission_matrix import FakeTerminalBackend
    from nullion.runtime_store import RuntimeStore
    from nullion.tools import create_core_tool_registry,ToolExecutor,ToolInvocation
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1');monkeypatch.setenv('NULLION_TERMINAL_ENABLED','1')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(isolated/'workspaces'))
    backend=FakeTerminalBackend();store=RuntimeStore()
    registry=create_core_tool_registry(workspace_root=isolated,terminal_executor_backend=backend)
    result=ToolExecutor(store=store,registry=registry).invoke(ToolInvocation('fixture','terminal_exec','operator',
        {'command':"cat > /tmp/nullion_fixture_helper.py <<'PY'\nprint('fixture')\nPY"},
        flow_context={flow_flag:True,'required_artifact_extensions':['.png'],'user_request':user_request}))
    assert result.status=='denied' and result.output['reason']=='workspace_scratch_path_required'
    assert result.output['retryable'] is True and result.output['requires_approval'] is False
    assert result.output['suggested_path'].endswith('/scratch/nullion_fixture_helper.py')
    assert not backend.calls and not store.list_approval_requests()


def test_temporary_helper_recovery_does_not_follow_symlink_to_private_file(isolated,monkeypatch):
    import tempfile
    from uuid import uuid4
    from test_core_tool_permission_matrix import FakeTerminalBackend
    from nullion.runtime_store import RuntimeStore
    from nullion.tools import create_core_tool_registry,ToolExecutor,ToolInvocation
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1');monkeypatch.setenv('NULLION_TERMINAL_ENABLED','1')
    backend=FakeTerminalBackend();store=RuntimeStore()
    registry=create_core_tool_registry(workspace_root=isolated,terminal_executor_backend=backend)
    private=Path('/Users')/'fixture-private.py' if os.name=='posix' else Path('C:/Users/fixture-private.py')
    alias=Path(tempfile.gettempdir())/('nullion-fixture-'+uuid4().hex+'.py')
    try:alias.symlink_to(private)
    except OSError:pytest.skip('Creating a symlink is unavailable on this host')
    try:
        result=ToolExecutor(store=store,registry=registry).invoke(ToolInvocation('fixture','terminal_exec','operator',
            {'command':f'printf fixture > {alias}'},flow_context={'requires_new_artifact_content':True,'required_artifact_extensions':['.png']}))
        assert result.status=='denied' and result.output['requires_approval'] is True
        assert not backend.calls
    finally:alias.unlink()


@pytest.mark.parametrize('tool_name',['terminal_exec','file_write','file_patch'])
def test_model_context_compaction_retains_structured_recovery_paths(tool_name):
    from nullion.agent_orchestrator import _tool_result_message_payload
    from nullion.tools import ToolResult
    output={'message':'Use the workspace scratch path.','reason':'workspace_scratch_path_required',
        'requires_approval':False,'retryable':True,'suggested_scratch_root':'/workspace/scratch',
        'suggested_path':'/workspace/scratch/helper.py','recovery':{'kind':'retry_in_workspace_scratch','tool_name':tool_name}}
    payload=json.loads(_tool_result_message_payload(ToolResult('fixture',tool_name,'denied',output)))
    for key,value in output.items():assert payload['output'][key]==value


@pytest.mark.parametrize('names',['input.html output.pdf','output.pdf input.html'])
def test_partial_existing_source_lookup_does_not_enable_direct_multi_file_delivery(isolated,monkeypatch,names):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.chat_operator import _existing_named_workspace_artifact_paths
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(isolated/'workspaces'))
    roots=workspace_storage_roots_for_principal('operator');source=roots.artifacts/'input.html'
    source.write_text('<!doctype html><html><head><title>Fixture</title></head><body><p>'+('Fixture data '*20)+'</p></body></html>')
    runtime=bootstrap_persistent_runtime(isolated/'runtime.db')
    assert _existing_named_workspace_artifact_paths(runtime,prompt=names,principal_id='operator')==()
    assert _existing_named_workspace_artifact_paths(runtime,prompt=names,principal_id='operator',require_all=False)==(str(source),)
