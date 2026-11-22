"""Real provider-driven approvals keep native scope under checkpoint contention."""
from contextlib import closing
import inspect
import json
import os
from pathlib import Path
import shlex
import sqlite3
import threading

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker Web UI')


@pytest.mark.parametrize('surface,prompt', [
    ('http','Review deleting the selected temporary file.'),
    ('websocket','選択した一時ファイルの削除を確認してください。'),
])
def test_native_pending_approval_card_retains_scope_while_runtime_checkpoint_is_locked(tmp_path,monkeypatch,surface,prompt):
    from playwright.sync_api import expect
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.preferences import save_preferences
    from nullion.runtime_persistence import _runtime_store_file_lock
    from nullion.runtime_store import RuntimeStore
    from nullion.runtime_checkpoints import wait_runtime_checkpoints
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path,monkeypatch)
    save_preferences({'sentinel_mode':'risk_based','emoji_level':'none','markdown_style':'plain'})
    monkeypatch.setenv('NULLION_CODE_MODE','off')
    monkeypatch.setenv('NULLION_CHAT_STREAMING_ENABLED','false')
    selected = tmp_path/'選択した一時ファイル.txt'
    selected.write_text('Must stay intact until the human reviews this exact action')
    command = 'rm -f -- '+shlex.quote(str(selected))+' 2>/dev/null; printf https://public.example.test'
    tool_name = 'terminal_exec'
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec(tool_name,'Review this local operation using https://public.example.test as reference',
        ToolRiskLevel.HIGH,ToolSideEffectClass.DANGEROUS_EXEC,
        True,5,input_schema={'type':'object','properties':{'command':{'type':'string'}},'required':['command']}),
        lambda invocation:pytest.fail('Rendering the pending review must not execute the action'))
    runtime.active_tool_registry = registry
    class Model:
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs,completion=False)
            if protocol is not None:
                return protocol
            if not kwargs.get('tools'):
                return text('{}')
            names = {tool['name'] for tool in kwargs['tools']}
            if tool_name in names:
                return call(tool_name,{'command':command})
            return call('request_tool_scope',{'capabilities':['local_shell'],
                'tool_names':[tool_name],'required_tool_names':[tool_name]})
    arm, held, release = threading.Event(),threading.Event(),threading.Event()
    observed = {}
    def writer():
        if not arm.wait(20) or 'approval_id' not in observed:
            return
        with _runtime_store_file_lock(runtime.checkpoint_path):
            with closing(sqlite3.connect(Path(runtime.checkpoint_path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                raw = db.execute('SELECT payload FROM approvals WHERE item_key=?',(observed['approval_id'],)).fetchone()
                observed['durable_approval'] = json.loads(raw[0]) if raw else None
                raw = db.execute('SELECT payload FROM runtime_suspended_turns WHERE item_key=?',(observed['approval_id'],)).fetchone()
                observed['durable_suspended'] = json.loads(raw[0]) if raw else None
            held.set()
            release.wait(15)
    # Observe the real read without substituting its value. The OS lock is
    # acquired only when display begins, after mandatory native persistence.
    original_get = RuntimeStore.get_approval_request
    display_code = web_app._web_approval_required_payload.__code__
    def observe_display_lookup(store, identity):
        approval = original_get(store,identity)
        caller = inspect.currentframe().f_back
        if store is runtime.store and caller.f_code is display_code and not arm.is_set():
            observed['approval_id'] = identity
            arm.set()
            assert held.wait(3), 'The native checkpoint lock was not acquired'
        return approval
    monkeypatch.setattr(RuntimeStore,'get_approval_request',observe_display_lookup)
    thread = threading.Thread(target=writer)
    thread.start()
    def configure(page):
        if surface=='http':
            page.add_init_script("window.WebSocket = class extends WebSocket { constructor() { throw new Error('Fixture: HTTP transport'); } };")
    try:
        with _UvicornThread(web_app.create_app(runtime,AgentOrchestrator(model_client=Model()),registry),_free_port()) as server:
            pw,browser,page,errors = _open_page(server.base_url,configure_page=configure)
            try:
                if surface=='websocket':
                    page.wait_for_function('ws && ws.readyState === WebSocket.OPEN')
                page.locator('#user-input').fill(prompt)
                page.locator('#send-btn').click()
                card = page.locator('#messages .approval-bubble')
                expect(card).to_have_count(1,timeout=6000)
                assert held.is_set() and not release.is_set()
                aid = card.get_attribute('data-approval-id')
                assert aid==observed['approval_id']
                native = runtime.store.get_approval_request(aid)
                assert native.status.value=='pending'
                assert observed['durable_approval']['status']=='pending'
                assert observed['durable_approval']['request_kind']==native.request_kind
                assert observed['durable_suspended'] is not None
                assert native.request_kind=='terminal_destructive_action'
                expect(card).to_contain_text(selected.name)
                expect(card.get_by_role('button',name='Delete',exact=True)).to_have_count(1)
                expect(card.get_by_role('button',name='Always allow',exact=True)).to_have_count(0)
                assert native.context['destructive_target_count']==1
                expect(card.locator('.web-scope')).to_have_count(0)
                expect(card.get_by_role('button',name='Allow all web domains',exact=True)).to_have_count(0)
                expect(card.locator('.approval-url-card')).to_have_count(0)
                assert selected.read_text()=='Must stay intact until the human reviews this exact action'
                assert native.status.value=='pending' and not errors
                release.set()
            finally:
                release.set()
                browser.close();pw.stop()
    finally:
        release.set()
        arm.set()
        thread.join(4)
        assert wait_runtime_checkpoints(runtime,timeout=5)
