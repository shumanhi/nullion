"""Actual status/approval routes, SQLite and terminal execution; provider boundary only is faked."""
import errno
from datetime import UTC, datetime
import os
import time
from pathlib import Path

import pytest
pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='container E2E')


def test_status_refresh_never_approves_deletion_and_explicit_review_runs_once(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.approval_display import approval_display_from_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolExecutor, ToolInvocation, create_core_tool_registry
    from nullion.web_app import create_app
    from test_browser_cleanup_and_delete_review import terminal_setup
    runtime, _, _ = terminal_setup(tmp_path, monkeypatch)
    from nullion.approvals import create_boundary_permit
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL
    runtime.store.add_boundary_permit(create_boundary_permit(approval_id='network-fixture',
        principal_id=GLOBAL_PERMISSION_PRINCIPAL, boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector='*',granted_by='fixture',granted_at=datetime.now(UTC)))
    # The real terminal backend runs only inside this isolated container.
    registry = create_core_tool_registry(workspace_root=tmp_path)
    runtime.active_tool_registry = registry
    victim = tmp_path / '資料.txt'; victim.write_text('fixture')
    from nullion.policy import permission_scope_principal
    runtime.store.add_boundary_permit(create_boundary_permit(approval_id='filesystem-fixture',
        principal_id=permission_scope_principal('web:review'),boundary_kind=BoundaryKind.FILESYSTEM_ACCESS,
        selector=str(victim),granted_by='fixture',granted_at=datetime.now(UTC)))
    command = f"set -e\nrm -f '{victim}'\nprintf completed"
    executor = ToolExecutor(store=runtime.store, registry=registry)
    invocation = ToolInvocation('delete','terminal_exec','web:review',{'command':command})
    blocked = executor.invoke(invocation)
    aid = blocked.output['approval_id']; runtime.checkpoint(force=True)
    with TestClient(create_app(runtime,orchestrator=None,registry=registry)) as client:
        for _ in range(3):
            response=client.get('/api/status?full=1')
            assert response.status_code == 200
        approval = runtime.store.get_approval_request(aid)
        assert approval.status.value == 'pending'
        assert '資料.txt' in approval_display_from_request(approval).detail
        assert victim.exists()
        persisted = bootstrap_persistent_runtime(tmp_path/'runtime.db')
        assert persisted.store.get_approval_request(aid).status.value == 'pending'
        response = client.post(f'/api/approve/{aid}',json={'mode':'once'})
        assert response.status_code == 200, response.text
        assert not victim.exists(),response.text
        # Exact action permission was consumed; a second invocation pauses again.
        repeated=executor.invoke(invocation)
        assert repeated.status == 'denied'
        assert repeated.output['reason'] == 'terminal_destructive_action_confirmation_required'


@pytest.mark.parametrize('prompt',['確認して','Revisa esto'])
def test_real_graph_stops_on_disk_exhaustion_without_cleanup_or_extra_model_calls(tmp_path, monkeypatch, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolInvocation,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    from test_browser_cleanup_and_delete_review import terminal_setup
    runtime,registry,backend=terminal_setup(tmp_path,monkeypatch)
    def full(invocation):raise OSError(errno.ENOSPC,'external storage boundary')
    registry.register(ToolSpec(name='storage_probe',description='Read resource',risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=1,input_schema={'type':'object','properties':{}}),full)
    class Model:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            pytest.fail('Disk exhaustion must end the run before recovery planning')
    model=Model()
    runner=AgentOrchestrator(model_client=model)
    snapshot=[{'role':'user','content':[{'type':'text','text':prompt}]},
        {'role':'assistant','content':[{'type':'tool_use','id':'probe','name':'storage_probe','input':{}},
        {'type':'tool_use','id':'cleanup','name':'terminal_exec','input':{'command':f'rm -rf {tmp_path / "keep"}'}}]}]
    started=time.perf_counter()
    result=runner.resume_turn(conversation_id='telegram:202',principal_id='telegram:202',user_message=prompt,
        messages_snapshot=snapshot,tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    assert time.perf_counter()-started < 2
    assert model.calls == 0 and backend.calls == []
    assert not result.suspended_for_approval
    assert result.tool_results[-1].output['reason'] == 'storage_exhausted'
    assert 'storage is full' in result.final_text
    assert not runtime.store.list_approval_requests()
    runtime.checkpoint(force=True)
    assert any(e.event_type == 'tool.invocation.completed' and e.payload.get('reason') == 'storage_exhausted' for e in runtime.store.list_events())


@pytest.mark.parametrize('with_url',[False,True])
def test_delete_review_card_renders_exact_files_and_filtered_scope_and_denies(tmp_path,monkeypatch,with_url):
    from nullion.tools import ToolExecutor,ToolInvocation
    from nullion.web_app import create_app
    from test_browser_cleanup_and_delete_review import terminal_setup
    from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
    from playwright.sync_api import expect
    runtime,registry,backend=terminal_setup(tmp_path,monkeypatch)
    victim=tmp_path/'画像.png';victim.write_bytes(b'keep')
    command=f"set -e\nrm -f '{victim}'\nfind '{tmp_path}' -maxdepth 1 -name 'cache_*' -delete"
    if with_url:
        command+="\nprintf 'https://example.com/page'"
    blocked=ToolExecutor(store=runtime.store,registry=registry).invoke(ToolInvocation('card','terminal_exec','web:review',{'command':command}))
    aid=blocked.output['approval_id'];runtime.checkpoint(force=True)
    with _UvicornThread(create_app(runtime,orchestrator=None,registry=registry),_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator('#attention-chip').click()
            card=page.locator(f'#attention-overlay .approval-panel-card[data-approval-id="{aid}"]')
            expect(card).to_be_visible()
            expect(card).to_contain_text('Files to delete (1)')
            expect(card).to_contain_text('Delete matching entries within:')
            expect(card).to_contain_text('exact matches have not been enumerated')
            lines=card.locator('.approval-detail').inner_text().splitlines()
            assert 'Files to delete (1):' in lines
            assert 'Delete matching entries within:' in lines
            assert '- '+str(victim) in lines
            expect(card.get_by_role('button',name='Delete',exact=True)).to_be_visible()
            assert runtime.store.get_approval_request(aid).status.value=='pending'
            page.screenshot(path='/tmp/delete-review-card.png')
            card.get_by_role('button',name='Deny',exact=True).click()
            expect(card).to_be_hidden()
            assert runtime.store.get_approval_request(aid).status.value=='denied'
            assert victim.read_bytes()==b'keep' and not backend.calls
            assert not errors
        finally:
            browser.close();playwright.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize('entry',['button','numeric_reply'])
async def test_telegram_delete_review_preserves_full_command_and_deny_is_terminal(tmp_path,monkeypatch,entry):
    from types import SimpleNamespace
    from nullion import telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.tools import ToolExecutor,ToolInvocation
    from test_browser_cleanup_and_delete_review import terminal_setup
    runtime,registry,backend=terminal_setup(tmp_path,monkeypatch)
    victim=tmp_path/'資料.png';victim.write_bytes(b'keep')
    command=f"set -e\nrm -f '{victim}'\nfind '{tmp_path}' -maxdepth 1 -name 'cache_*' -delete\nprintf 'https://example.com/page'"
    blocked=ToolExecutor(store=runtime.store,registry=registry).invoke(ToolInvocation('telegram-card','terminal_exec','telegram:77',{'command':command}))
    aid=blocked.output['approval_id'];approval=runtime.store.get_approval_request(aid)
    text=telegram_app._approval_card_text(approval)
    assert 'Files to delete (1):' in text and str(victim) in text
    assert 'Delete matching entries within:' in text
    assert '1. Delete\n2. Deny' in text
    assert 'Requested URL:' not in text
    class Model:
        def create(self,**kwargs):pytest.fail('Deny must never execute the paused operation or ask the model')
    runner=AgentOrchestrator(model_client=Model())
    settings=load_settings(env={'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true','NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz'})
    service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token='fixture',operator_chat_id='77',agent_orchestrator=runner)
    class Message:
        chat_id=77;message_id=3;caption=None
        from_user=SimpleNamespace(id=77)
        reply_markup=telegram_app._build_approval_markup(approval=approval)
        def __init__(self):self.chat=SimpleNamespace(id=77,send_action=self.send_action);self.text=text;self.edits=[];self.replies=[]
        async def send_action(self,*args):pass
        async def reply_text(self,text,**kwargs):self.replies.append(text);return SimpleNamespace(message_id=12)
        async def edit_text(self,text,**kwargs):self.edits.append(text)
        async def reply_document(self,*args,**kwargs):pytest.fail('Approval reviews do not upload source files')
    message=Message()
    class Callback:
        from_user=SimpleNamespace(id=77)
        def __init__(self):self.message=message;self.data=telegram_app._build_callback_data(kind='approval',action='deny',record_id=aid)
        async def answer(self,*args,**kwargs):pass
    if entry=='button':
        await service.on_callback_query(SimpleNamespace(callback_query=Callback()),SimpleNamespace())
    else:
        message.from_user=SimpleNamespace(id=888,is_bot=True)
        numeric=SimpleNamespace(text='2',chat=message.chat,chat_id=77,message_id=4,from_user=SimpleNamespace(id=77),reply_to_message=message,reply_text=message.reply_text)
        await service.on_message(SimpleNamespace(message=numeric,update_id=51),SimpleNamespace(bot=SimpleNamespace(id=888)))
    assert runtime.store.get_approval_request(aid).status.value=='denied'
    assert len(message.edits)==1 and 'Denied' in message.edits[0]
    assert not backend.calls and victim.read_bytes()==b'keep'
    assert not [g for g in runtime.store.list_permission_grants() if g.permission.startswith('terminal_destructive:')]


def test_first_full_status_refresh_loads_review_from_an_independent_process(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolExecutor,ToolInvocation
    from nullion.web_app import create_app
    from test_browser_cleanup_and_delete_review import terminal_setup
    runtime,registry,backend=terminal_setup(tmp_path,monkeypatch)
    runtime.checkpoint(force=True)
    app=create_app(runtime,orchestrator=None,registry=registry)
    # Web holds its initial snapshot; a messaging process persists the review.
    writer=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    command=f"set -e\nrm -f '{tmp_path/'資料.txt'}'"
    blocked=ToolExecutor(store=writer.store,registry=registry).invoke(ToolInvocation('external-review','terminal_exec','telegram:77',{'command':command}))
    aid=blocked.output['approval_id'];writer.checkpoint(force=True)
    assert runtime.store.get_approval_request(aid) is None
    with TestClient(app) as client:
        response=client.get('/api/status?full=1')
        assert response.status_code==200
        row=next((a for a in response.json()['approvals'] if a['approval_id']==aid),None)
        assert row is not None,'First refresh missed the independently-created review'
        assert row['status']=='pending'
        assert runtime.store.get_approval_request(aid).status.value=='pending'
        assert not backend.calls
