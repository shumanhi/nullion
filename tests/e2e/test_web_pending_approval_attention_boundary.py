"""New native consent remains actionable beyond historical dashboard limits."""
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker Web UI')


def test_attention_panel_keeps_new_native_delete_review_after_over_100_decisions(tmp_path, monkeypatch):
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolExecutor, ToolInvocation, ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    from playwright.sync_api import expect
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from tests.test_web_pending_approval_visibility import seed_approval_history
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.delenv('NULLION_WEB_STATUS_APPROVALS_LIMIT', raising=False)
    monkeypatch.setenv('NULLION_WEB_STATUS_CACHE_TTL_S', '0')
    _, pending = seed_approval_history(runtime, pending_count=3)
    selected = tmp_path / '選択した一時ファイル.txt'
    selected.write_text('Never delete this file after denial')
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('terminal_exec', 'Review local deletion', ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC, True, 5),
        lambda invocation: pytest.fail('A denied native approval must never execute'))
    result = ToolExecutor(store=runtime.store, registry=registry).invoke(ToolInvocation(
        'new-owned-delete', 'terminal_exec', 'web:pending', {'command': "rm -f -- '" + str(selected) + "' 2>/dev/null"}))
    aid = result.output['approval_id']
    runtime.checkpoint(force=True)
    with _UvicornThread(web_app.create_app(runtime, orchestrator=None, registry=registry), _free_port()) as server:
        pw, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator('#attention-chip').click()
            card = page.locator('#attention-body .approval-panel-card[data-approval-id="' + aid + '"]')
            expect(card).to_have_count(1, timeout=10000)
            expect(card).to_contain_text(selected.name)
            expect(card.get_by_role('button', name='Delete', exact=True)).to_have_count(1)
            expect(card.get_by_role('button', name='Allow all web domains', exact=True)).to_have_count(0)
            payload = page.request.get(server.base_url + '/api/status?full=1').json()
            assert {a['approval_id'] for a in payload['approvals'] if a['status'] == 'pending'} == {a.approval_id for a in pending} | {aid}
            assert len(payload['approvals']) == 104
            assert payload['status_meta']['truncated']['approvals'] == {'shown': 104, 'total': 109}
            assert runtime.store.get_approval_request(aid).status.value == 'pending'
            with page.expect_response(lambda response: response.url.endswith('/api/reject/' + aid) and response.request.method == 'POST') as rejected:
                card.get_by_role('button', name='Deny', exact=True).click()
            assert rejected.value.status == 200 and rejected.value.json()['ok'] is True
            expect(card).to_have_count(0, timeout=10000)
            assert not errors
            assert selected.read_text() == 'Never delete this file after denial'
            assert runtime.store.get_approval_request(aid).status.value == 'denied'
            assert all(runtime.store.get_approval_request(a.approval_id).status.value == 'pending' for a in pending)
        finally:
            browser.close()
            pw.stop()
    restored = bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert restored.store.get_approval_request(aid).status.value == 'denied'
