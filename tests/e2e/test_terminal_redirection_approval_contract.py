"""Real approval routes and shell effects, confined to disposable Docker files."""
from pathlib import Path
import os
import shlex

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker terminal E2E')


def terminal_runtime(tmp_path, monkeypatch, *, backend=None):
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_terminal_exec_handler
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path, monkeypatch)
    workspace = tmp_path/'workspace'
    workspace.mkdir()
    registry = ToolRegistry(filesystem_allowed_roots=[workspace])
    registry.register(ToolSpec('terminal_exec','Execute the reviewed shell command',ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC,True,10),
        _build_terminal_exec_handler(workspace,allowed_roots=[workspace],terminal_executor_backend=backend))
    runtime.active_tool_registry = registry
    return runtime, registry, workspace


def allow_selected_file(runtime, path):
    """Model an existing exact filesystem grant, separate from delete review."""
    from datetime import UTC, datetime
    from nullion.approvals import create_boundary_permit
    from nullion.policy import BoundaryKind, permission_scope_principal
    runtime.store.add_boundary_permit(create_boundary_permit(approval_id='selected-file-fixture',
        principal_id=permission_scope_principal('web:review'),boundary_kind=BoundaryKind.FILESYSTEM_ACCESS,
        selector=str(path),granted_by='fixture',granted_at=datetime.now(UTC)))


@pytest.mark.parametrize('redirection', ['2>/dev/null', '2>&1 >/dev/null'])
def test_discard_redirection_is_not_a_delete_target_and_exact_review_resumes(tmp_path, monkeypatch, redirection):
    from fastapi.testclient import TestClient
    from nullion.approval_display import approval_display_from_request
    from nullion.policy import BoundaryKind
    from nullion.tool_boundaries import extract_boundary_facts
    from nullion.tools import ToolExecutor, ToolInvocation
    from nullion.web_app import create_app
    runtime, registry, workspace = terminal_runtime(tmp_path, monkeypatch)
    selected = workspace/'選択したキャッシュ.txt'
    selected.write_text('disposable cache')
    allow_selected_file(runtime, selected)
    command = f'rm -f -- {shlex.quote(str(selected))} {redirection} || true'
    invocation = ToolInvocation('review-discard','terminal_exec','web:review',{'command':command})
    facts = extract_boundary_facts(invocation)
    deletes = [fact.target for fact in facts if fact.kind is BoundaryKind.FILESYSTEM_ACCESS and fact.operation=='delete']
    assert deletes == [str(selected)]
    blocked = ToolExecutor(store=runtime.store, registry=registry).invoke(invocation)
    assert blocked.output['reason']=='terminal_destructive_action_confirmation_required'
    aid = blocked.output['approval_id']
    review = runtime.store.get_approval_request(aid)
    assert [row['target'] for row in review.context['destructive_targets']] == [str(selected)]
    detail = approval_display_from_request(review).detail
    assert 'About to delete 1 path:' in detail and '- '+str(selected) in detail
    assert '- /dev/null' not in detail and '- '+str(workspace/'2') not in detail
    assert selected.is_file()
    runtime.checkpoint(force=True)
    with TestClient(create_app(runtime,orchestrator=None,registry=registry)) as client:
        response = client.post('/api/approve/'+aid,json={'mode':'once'})
    assert response.status_code==200, response.text
    visible = response.json().get('resumed_text') or ''
    assert 'Deleted 1 path.' in visible and selected.name in visible, response.json()
    assert not selected.exists()
    approvals = runtime.store.list_approval_requests()
    assert len(approvals)==1 and approvals[0].status.value=='approved'
    assert not (workspace/'2').exists() and not (workspace/'>').exists()
    assert Path('/dev/null').is_char_device()


def test_concurrent_delete_consumes_the_review_before_running_the_command(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.tools import SubprocessTerminalExecutorBackend, ToolExecutor, ToolInvocation

    started, release = Event(), Event()

    class HeldBackend(SubprocessTerminalExecutorBackend):
        def run(self, command, **kwargs):
            started.set()
            assert release.wait(8)
            return super().run(command, **kwargs)

    runtime, registry, workspace = terminal_runtime(tmp_path, monkeypatch, backend=HeldBackend())
    selected = workspace / '資料.txt'
    selected.write_text('disposable cache')
    allow_selected_file(runtime, selected)
    command = f'rm -f -- {shlex.quote(str(selected))}'
    invocation = ToolInvocation('review-once', 'terminal_exec', 'web:review', {'command': command})
    executor = ToolExecutor(store=runtime.store, registry=registry)
    review = executor.invoke(invocation)
    approve_request_with_mode(runtime, review.output['approval_id'], mode='once', source='web')

    with ThreadPoolExecutor() as pool:
        first = pool.submit(executor.invoke, invocation)
        try:
            assert started.wait(5)
            duplicate = pool.submit(executor.invoke, invocation).result(timeout=2)
            assert duplicate.output['reason'] == 'approval_already_used'
            assert selected.exists()
        finally:
            release.set()
        assert first.result(timeout=5).status == 'completed'
    assert not selected.exists()


@pytest.mark.parametrize("revoked_manually", [False, True])
def test_destructive_claim_survives_grant_revocation_during_preflight(tmp_path, monkeypatch, revoked_manually):
    from concurrent.futures import ThreadPoolExecutor
    from dataclasses import replace
    from threading import Event
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.tools import ToolExecutor, ToolInvocation

    paused, release = Event(), Event()
    runtime, registry, workspace = terminal_runtime(tmp_path, monkeypatch)
    selected = workspace / '資料.txt'
    selected.write_text('disposable cache')
    allow_selected_file(runtime, selected)
    invocation = ToolInvocation('first', 'terminal_exec', 'web:review',
        {'command': f'rm -f -- {shlex.quote(str(selected))}'})
    executor = ToolExecutor(store=runtime.store, registry=registry)
    review = executor.invoke(invocation)
    approve_request_with_mode(runtime, review.output['approval_id'], mode='once', source='web')
    original_preflight = executor._preflight_boundary_policy_result

    def delay_second(action, **kwargs):
        if action.invocation_id == 'second':
            paused.set()
            assert release.wait(8)
        return original_preflight(action, **kwargs)

    monkeypatch.setattr(executor, '_preflight_boundary_policy_result', delay_second)
    with ThreadPoolExecutor() as pool:
        second = pool.submit(executor.invoke, replace(invocation, invocation_id='second'))
        try:
            assert paused.wait(5)
            if revoked_manually:
                grant = next(grant for grant in runtime.store.list_permission_grants()
                    if grant.approval_id == review.output['approval_id'])
                runtime.revoke_permission_grant(grant.grant_id)
            else:
                assert executor.invoke(invocation).status == 'completed'
            selected.write_text('replacement must survive')
        finally:
            release.set()
        reason = 'approval_no_longer_active' if revoked_manually else 'approval_already_used'
        assert second.result(timeout=5).output['reason'] == reason
    assert selected.read_text() == 'replacement must survive'


def test_real_redirect_file_keeps_write_boundary_separate_from_reviewed_deletion(tmp_path, monkeypatch):
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.approval_display import approval_display_from_request
    from nullion.policy import BoundaryKind
    from nullion.tool_boundaries import extract_boundary_facts
    from nullion.tools import ToolExecutor, ToolInvocation
    from nullion.response_sanitizer import native_filesystem_receipt_reply
    runtime, registry, workspace = terminal_runtime(tmp_path,monkeypatch)
    selected = workspace/'cache.txt'
    selected.write_text('disposable cache')
    logfile = tmp_path/'outside-approved-workspace.log'
    command = f'rm -f -- {shlex.quote(str(selected))} 2>{shlex.quote(str(logfile))}'
    invocation = ToolInvocation('review-log','terminal_exec','web:review',{'command':command})
    facts = extract_boundary_facts(invocation)
    filesystem = {(fact.operation,fact.target) for fact in facts if fact.kind is BoundaryKind.FILESYSTEM_ACCESS}
    assert filesystem == {('delete',str(selected)),('write',str(logfile))}
    executor = ToolExecutor(store=runtime.store,registry=registry)
    deletion = executor.invoke(invocation)
    assert deletion.output['reason']=='terminal_destructive_action_confirmation_required'
    review = runtime.store.get_approval_request(deletion.output['approval_id'])
    assert [row['target'] for row in review.context['destructive_targets']] == [str(selected)]
    assert '- '+str(logfile) not in approval_display_from_request(review).detail
    approve_request_with_mode(runtime,review.approval_id,mode='once',source='web')
    write_pause = executor.invoke(invocation)
    assert write_pause.status=='denied'
    write_review = runtime.store.get_approval_request(write_pause.output['approval_id'])
    assert write_review.request_kind=='boundary_policy'
    assert write_review.context['operation']=='write' and write_review.resource==str(logfile)
    assert selected.read_text()=='disposable cache' and not logfile.exists()
    approve_request_with_mode(runtime,write_review.approval_id,mode='once',source='web')
    completed = executor.invoke(invocation)
    assert completed.status=='completed', (completed.output,completed.error)
    assert 'Deleted 1 path.' in native_filesystem_receipt_reply(completed)
    assert not selected.exists() and logfile.is_file() and logfile.read_bytes()==b''
    assert len(runtime.store.list_approval_requests())==2
    runtime.checkpoint(force=True)
    from nullion.runtime import bootstrap_persistent_runtime
    restored = bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert restored.store.get_approval_request(write_review.approval_id).context['operation']=='write'


@pytest.mark.parametrize('operand', ['device', 'quoted_operator', 'spaced_fd_operand'])
def test_explicit_operands_still_require_delete_review_without_execution(tmp_path, monkeypatch, operand):
    from datetime import UTC, datetime
    from nullion.approvals import create_boundary_permit
    from nullion.approval_display import approval_display_from_request
    from nullion.policy import BoundaryKind, permission_scope_principal
    from nullion.tool_boundaries import extract_boundary_facts
    from nullion.tools import ToolExecutor, ToolInvocation
    class ProhibitedBackend:
        def run(self,*args,**kwargs):
            pytest.fail('The explicit device deletion must never reach a backend')
    runtime, registry, workspace = terminal_runtime(tmp_path,monkeypatch,backend=ProhibitedBackend())
    if operand=='device':
        # A filesystem grant from the erroneous follow-up review cannot replace
        # consent for an explicit destructive terminal invocation.
        runtime.store.add_boundary_permit(create_boundary_permit(approval_id='previous-filesystem-review',
            principal_id=permission_scope_principal('web:review'), boundary_kind=BoundaryKind.FILESYSTEM_ACCESS,
            selector='/dev/null',granted_by='fixture',granted_at=datetime.now(UTC)))
    literal = '>' if operand=='quoted_operator' else '2'
    selected = Path('/dev/null') if operand=='device' else workspace/literal
    if operand!='device':
        selected.write_text('A literal operator-shaped filename')
    command = ('rm -- /dev/null' if operand=='device' else "rm -- '>'" if operand=='quoted_operator'
        else 'rm -- 2 >/dev/null')
    invocation = ToolInvocation('explicit-operand','terminal_exec','web:review',{'command':command})
    facts = extract_boundary_facts(invocation)
    assert any(fact.kind is BoundaryKind.FILESYSTEM_ACCESS and fact.operation=='delete'
        and fact.target==('/dev/null' if operand=='device' else literal) for fact in facts)
    result = ToolExecutor(store=runtime.store,registry=registry).invoke(invocation)
    assert result.status=='denied' and result.output['reason']=='terminal_destructive_action_confirmation_required'
    review = runtime.store.get_approval_request(result.output['approval_id'])
    assert review.status.value=='pending'
    assert [row['target'] for row in review.context['destructive_targets']]==[str(selected)]
    assert '- '+str(selected) in approval_display_from_request(review).detail
    if operand!='device':
        assert selected.read_text()=='A literal operator-shaped filename'
    if operand=='spaced_fd_operand':
        adjacent = ToolInvocation('adjacent-fd','terminal_exec','web:review',{'command':'rm -- 2>/dev/null'})
        assert not any(fact.operation=='delete' for fact in extract_boundary_facts(adjacent))
        next_pause = ToolExecutor(store=runtime.store,registry=registry).invoke(adjacent)
        assert next_pause.status=='denied'
        assert next_pause.output['reason']!='terminal_destructive_action_confirmation_required'
        assert selected.is_file()
    assert Path('/dev/null').is_char_device()


@pytest.mark.parametrize('prompt', ['Clean the selected disposable cache.', '選択した一時キャッシュを削除してください。'])
def test_unresolved_delete_requires_literal_repair_before_approval_or_execution(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion.tools import ToolExecutor, ToolInvocation, SubprocessTerminalExecutorBackend
    from nullion.web_app import create_app
    class ObservedBackend(SubprocessTerminalExecutorBackend):
        def __init__(self):
            self.calls = []
        def run(self, command, **kwargs):
            self.calls.append(command)
            return super().run(command, **kwargs)
    backend = ObservedBackend()
    runtime, registry, workspace = terminal_runtime(tmp_path,monkeypatch,backend=backend)
    cache = workspace/'temporary-cache'
    cache.mkdir()
    selected = cache/'選択.txt'
    selected.write_text('disposable selected bytes')
    expression = '$(printf %s '+shlex.quote(str(cache))+')'
    dynamic_command = 'rm -rf "'+expression+'"/* 2>/dev/null || true'
    context = {'conversation_id':'web:review','user_message':prompt}
    executor = ToolExecutor(store=runtime.store,registry=registry)
    refused = executor.invoke(ToolInvocation('dynamic','terminal_exec','web:review',
        {'command':dynamic_command},capsule_id='selected-cleanup',flow_context=context))
    assert refused.status in {'denied','failed'}
    assert refused.output['executed'] is False
    assert refused.output['command_executed'] is False and refused.output['requires_approval'] is False
    assert refused.output['retryable'] is True
    assert refused.output['recovery']['kind']=='correct_arguments'
    assert refused.output['recovery']['approval_bypass'] is False
    assert not refused.output.get('action_receipt') and not refused.output.get('approval_id')
    assert not runtime.store.list_approval_requests() and not backend.calls
    assert selected.read_text()=='disposable selected bytes'
    allow_selected_file(runtime, selected)
    resolved_command = f'rm -f -- {shlex.quote(str(selected))} 2>/dev/null || true'
    reviewed = executor.invoke(ToolInvocation('resolved','terminal_exec','web:review',
        {'command':resolved_command},capsule_id='selected-cleanup',flow_context=context))
    assert reviewed.status=='denied' and reviewed.output['reason']=='terminal_destructive_action_confirmation_required'
    aid = reviewed.output['approval_id']
    approval = runtime.store.get_approval_request(aid)
    assert [row['target'] for row in approval.context['destructive_targets']]==[str(selected)]
    assert not backend.calls and selected.exists()
    runtime.checkpoint(force=True)
    with TestClient(create_app(runtime,orchestrator=None,registry=registry)) as client:
        response = client.post('/api/approve/'+aid,json={'mode':'once'})
    assert response.status_code==200, response.text
    assert 'Deleted 1 path.' in (response.json().get('resumed_text') or ''), response.json()
    assert backend.calls==[resolved_command] and not selected.exists() and cache.is_dir()
