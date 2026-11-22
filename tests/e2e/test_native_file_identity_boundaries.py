"""Real approval/persistence boundaries for file previews and unlink receipts."""
from datetime import UTC, datetime
import os
from pathlib import Path
import shlex

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('source_kind', ['absolute', 'symlink'])
def test_unapproved_html_source_is_never_read_or_copied_into_approval_preview(tmp_path, monkeypatch, source_kind):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolExecutor, ToolInvocation, ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path, monkeypatch)
    root = artifact_root_for_principal('telegram:123')
    root.mkdir(parents=True, exist_ok=True)
    private = tmp_path / 'unapproved-private.txt'
    private.write_text('PRIVATE-BYTES-MUST-NOT-REACH-PREVIEW')
    selected = private
    if source_kind == 'symlink':
        selected = root / 'looks-owned.html'
        selected.symlink_to(private)
    reads = []
    original_read = Path.read_text
    def read_text(path, *args, **kwargs):
        if path.resolve() == private.resolve():
            reads.append(path)
            pytest.fail('Unapproved source bytes were read while preparing an approval')
        return original_read(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read_text)
    registry = ToolRegistry(filesystem_allowed_roots=[root])
    registry.register(ToolSpec('email_send', 'Send reviewed email', ToolRiskLevel.HIGH,
        ToolSideEffectClass.ACCOUNT_WRITE, True, 10),
        lambda invocation: pytest.fail('Unapproved email must not be sent'))
    result = ToolExecutor(store=runtime.store, registry=registry).invoke(ToolInvocation(
        'unapproved-preview', 'email_send', 'telegram:123',
        {'to':'reader@example.test', 'subject':'Review', 'body':'Fallback', 'html_path':str(selected)}))
    assert result.status == 'denied' and result.output['reason'] == 'approval_required'
    assert not reads and not result.output.get('html_preview_path')
    approval = runtime.store.get_approval_request(result.output['approval_id'])
    assert approval.request_kind == "boundary_policy"
    assert not approval.context.get('html_preview_path')
    assert not list(root.glob('email-preview-*.html'))


@pytest.mark.parametrize('source_kind', ['owned', 'explicitly_approved'])
def test_permitted_html_source_keeps_unicode_preview(tmp_path, monkeypatch, source_kind):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolExecutor, ToolInvocation, ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path, monkeypatch)
    root = artifact_root_for_principal('telegram:123')
    root.mkdir(parents=True, exist_ok=True)
    selected = (root if source_kind == 'owned' else tmp_path) / 'source.html'
    document = '<h1>東京 → Montréal</h1>'
    selected.write_text(document, encoding='utf-8')
    registry = ToolRegistry(filesystem_allowed_roots=[root])
    registry.register(ToolSpec('email_send', 'Send reviewed email', ToolRiskLevel.HIGH,
        ToolSideEffectClass.ACCOUNT_WRITE, True, 10),
        lambda invocation: pytest.fail('Send still needs approval'))
    invocation = ToolInvocation('permitted-preview', 'email_send', 'telegram:123',
        {'to':'reader@example.test', 'subject':'Review', 'body':'Fallback', 'html_path':str(selected)},
        trusted_filesystem_selectors=(str(selected),) if source_kind == 'explicitly_approved' else ())
    result = ToolExecutor(store=runtime.store, registry=registry).invoke(invocation)
    assert result.status == 'denied'
    preview = Path(result.output['html_preview_path'])
    assert preview != selected and preview.is_relative_to(root)
    assert document in preview.read_text(encoding='utf-8')
    assert b'<meta charset="utf-8">' in preview.read_bytes()[:1024]
    assert selected.read_text(encoding='utf-8') == document


def test_exact_email_approval_grants_only_the_reviewed_html_read(tmp_path, monkeypatch):
    from dataclasses import replace
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.tool_boundaries import extract_boundary_facts
    from nullion.policy import BoundaryKind
    from nullion.tools import (ToolExecutor, ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel,
        ToolSideEffectClass, ToolSpec, _email_message_for_invocation)
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path, monkeypatch)
    selected = tmp_path / 'explicitly-reviewed.html'
    selected.write_text('<h1>本文 → résumé</h1>', encoding='utf-8')
    unrelated = tmp_path / 'unrelated.html'
    unrelated.write_text('UNRELATED PRIVATE DATA')
    calls = []
    def handler(invocation):
        calls.append(invocation)
        mime, _ = _email_message_for_invocation(invocation)
        html_part = next(part for part in mime.walk() if part.get_content_type() == 'text/html')
        assert '本文 → résumé' in html_part.get_content()
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'message':'Sent reviewed email'})
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path / 'permitted'])
    registry.register(ToolSpec('email_send', 'Send reviewed email', ToolRiskLevel.HIGH,
        ToolSideEffectClass.ACCOUNT_WRITE, True, 10), handler)
    executor = ToolExecutor(store=runtime.store, registry=registry)
    invocation = ToolInvocation('reviewed', 'email_send', 'telegram:123',
        {'to':'reader@example.test', 'subject':'Review', 'body':'Fallback', 'html_path':str(selected)})
    assert any(fact.kind is BoundaryKind.FILESYSTEM_ACCESS and fact.operation == 'read'
        and fact.target == str(selected) for fact in extract_boundary_facts(invocation))
    blocked = executor.invoke(invocation)
    assert blocked.status == 'denied' and not calls and not blocked.output.get('html_preview_path')
    first = runtime.store.get_approval_request(blocked.output['approval_id'])
    assert first.request_kind == 'boundary_policy'
    approve_request_with_mode(runtime, first.approval_id, mode='always', source='web')
    review = executor.invoke(invocation)
    assert review.status == 'denied' and review.output.get('html_preview_path')
    approve_request_with_mode(runtime, review.output['approval_id'], mode='once', source='web')
    # A different source or principal cannot reuse the exact reviewed-file grant.
    for changed in [replace(invocation, arguments={**invocation.arguments, 'html_path':str(unrelated)}),
                    replace(invocation, principal_id='telegram:456')]:
        refused = executor.invoke(changed)
        assert refused.status == 'denied' and not calls
        assert refused.output['approval_id'] != review.output['approval_id']
        if changed.principal_id == invocation.principal_id:
            assert not refused.output.get('html_preview_path')
    completed = executor.invoke(invocation)
    assert completed.status == 'completed' and len(calls) == 1
    assert calls[0].trusted_filesystem_selectors == (str(selected),)
    assert executor.invoke(invocation).status == 'denied' and len(calls) == 1


@pytest.mark.parametrize('broken', [False, True])
def test_approved_symlink_unlink_reports_selected_entry_and_preserves_referent(tmp_path, monkeypatch, broken):
    from fastapi.testclient import TestClient
    from nullion.approvals import create_boundary_permit
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, permission_scope_principal
    from nullion.tools import ToolExecutor, ToolInvocation, create_core_tool_registry
    from nullion.web_app import create_app
    from test_browser_cleanup_and_delete_review import terminal_setup
    runtime, _, _ = terminal_setup(tmp_path, monkeypatch)
    target = tmp_path / 'referent.txt'
    if not broken:
        target.write_text('Keep these bytes')
    link = tmp_path / '選択したリンク.txt'
    link.symlink_to(target)
    for kind, principal, selector in [
        (BoundaryKind.OUTBOUND_NETWORK, GLOBAL_PERMISSION_PRINCIPAL, '*'),
        (BoundaryKind.FILESYSTEM_ACCESS, permission_scope_principal('web:review'), str(target)),
        (BoundaryKind.FILESYSTEM_ACCESS, permission_scope_principal('web:review'), str(link)),
    ]:
        runtime.store.add_boundary_permit(create_boundary_permit(approval_id='fixture-'+kind.value,
            principal_id=principal, boundary_kind=kind, selector=selector,
            granted_by='fixture', granted_at=datetime.now(UTC)))
    registry = create_core_tool_registry(workspace_root=tmp_path)
    runtime.active_tool_registry = registry
    invocation = ToolInvocation('unlink', 'terminal_exec', 'web:review',
        {'command':shlex.join(['rm', '--', str(link)])})
    blocked = ToolExecutor(store=runtime.store, registry=registry).invoke(invocation)
    assert blocked.output['reason'] == 'terminal_destructive_action_confirmation_required'
    # Approval identity continues using the canonical target; only observations
    # distinguish the unlinked directory entry from its referent.
    assert blocked.output['destructive_targets'][0]['target'] == str(target)
    assert link.is_symlink()
    runtime.checkpoint(force=True)
    with TestClient(create_app(runtime, orchestrator=None, registry=registry)) as client:
        response = client.post('/api/approve/'+blocked.output['approval_id'], json={'mode':'once'})
    assert response.status_code == 200, response.text
    visible = response.json().get('resumed_text') or ''
    assert 'Deleted 1 path.' in visible and str(link) in visible, (response.json(),
        [row.context for row in runtime.store.list_approval_requests()])
    assert str(target) not in visible and not link.is_symlink()
    assert target.exists() is not broken
    if not broken:
        assert target.read_text() == 'Keep these bytes'
