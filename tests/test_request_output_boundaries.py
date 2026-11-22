import pytest

from nullion.tools import ToolResult


def test_terminal_inspection_does_not_scan_for_nearby_files(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from nullion import tools
    monkeypatch.setattr(tools, '_terminal_deliverable_artifact_paths_since',
        lambda *args, **kwargs: pytest.fail('Read-only inspection must not scan for deliverables'))
    class Backend:
        def run(self, *args, **kwargs):
            return SimpleNamespace(stdout='identifier R-7301',stderr='',exit_code=0,shell='sh')
    result = tools._build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path],terminal_executor_backend=Backend())(
        tools.ToolInvocation('inspect','terminal_exec','workspace_admin',{'command':'inspect'}))
    assert result.status == 'completed' and not result.output.get('artifact_paths')


@pytest.mark.parametrize('native_role', [None, 'source', 'intermediate'])
def test_incidental_terminal_paths_do_not_declare_deliverables(tmp_path, native_role):
    from nullion.chat_operator import _terminal_stdout_artifact_paths_from_tool_results
    from nullion.response_fulfillment_contract import artifact_paths_from_tool_results
    path = tmp_path/'old-document.pdf'
    path.write_bytes(b'%PDF-1.4 fixture')
    output = {'stdout':'Stored diagnostic record: '+str(path), 'exit_code':0}
    if native_role:
        output['artifact_descriptors'] = [{'path':str(path), 'role':native_role}]
    rows = [ToolResult('read', 'terminal_exec', 'completed', output)]
    assert artifact_paths_from_tool_results(rows) == []
    assert _terminal_stdout_artifact_paths_from_tool_results(rows,
        requested_extensions=(), artifact_roots=(tmp_path,)) == ()
    if native_role:
        assert artifact_paths_from_tool_results(rows, required_extensions=('.pdf',)) == []
        assert _terminal_stdout_artifact_paths_from_tool_results(rows,
            requested_extensions=('.pdf',), artifact_roots=(tmp_path,)) == ()
    else:
        assert artifact_paths_from_tool_results(rows, required_extensions=('.pdf',)) == [str(path)]


@pytest.mark.parametrize('subject', ['Field report', '現地調査', 'Informe de campo'])
def test_email_confirmation_renders_one_review_block(subject):
    from nullion.approvals import create_approval_request
    from nullion.telegram_app import _approval_decision_messages
    approval = create_approval_request(requested_by='telegram:123', action='use_tool',
        resource='tool:email_send', request_kind='capability_grant', context={
            'tool_name':'email_send', 'tool_arguments':{'to':'reader@example.test',
                'subject':subject, 'body':'Plain version', 'html_body':'<p>HTML version</p>'},
            'html_preview_path':'preview.html'})
    _, reply = _approval_decision_messages(approval, 'allow_once')
    assert reply.count('Email draft:') == 1
    assert reply.count('reader@example.test') == 1
    assert reply.count(subject) == 1
    assert 'Continuing...' in reply
