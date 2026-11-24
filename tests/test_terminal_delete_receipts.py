import shlex
import subprocess
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize('mode', ['deleted', 'retained', 'already_absent', 'partial_failure', 'timeout'])
@pytest.mark.parametrize('filename', ['notes.txt', '資料.txt'])
def test_deletion_receipts_use_filesystem_changes_not_stdout(tmp_path, mode, filename):
    from nullion.tools import ToolInvocation, _build_terminal_exec_handler
    from nullion.chat_operator import _terminal_exec_outcome_reply, _approved_pending_tool_resume_reply
    target = tmp_path/filename
    if mode != 'already_absent': target.write_bytes(b'selected fixture')
    retained = tmp_path/'retained.txt'
    retained.write_bytes(b'preserve')
    targets = [target, retained] if mode=='partial_failure' else [target]
    command = shlex.join(['rm','-f','--',*(str(path) for path in targets)])
    class Backend:
        def run(self,*args,**kwargs):
            if mode in {'deleted','partial_failure','timeout'}: target.unlink()
            if mode=='timeout': raise subprocess.TimeoutExpired(command,.1,output='deleted=yes')
            # Intentionally misleading prose must not certify the deletion.
            return SimpleNamespace(stdout='deleted=yes\nretained=yes\n',stderr='',
                exit_code=1 if mode=='partial_failure' else 0,shell='sh')
    result = _build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path],terminal_executor_backend=Backend())(
        ToolInvocation('delete','terminal_exec','workspace_admin',{'command':command}))
    receipt = result.output['action_receipt']
    changes = receipt['changes']
    expected = 'unconfirmed' if mode=='retained' else 'deleted' if mode in {'partial_failure','timeout'} else mode
    assert changes[0]['status'] == expected
    assert changes[0]['path'] == str(target)
    assert retained.read_bytes() == b'preserve'
    if mode in {'deleted','partial_failure','timeout'}:
        assert changes[0]['size_bytes_before'] == len(b'selected fixture')
        assert receipt['summary'].startswith('Deleted 1 path.')
    if mode=='partial_failure':
        assert changes[1]['status']=='unconfirmed'
        assert 'was not confirmed' in receipt['summary']
    visible = _terminal_exec_outcome_reply([result])
    assert filename in visible and 'deleted=yes' not in visible and '```' not in visible
    assert _approved_pending_tool_resume_reply(result) == visible
    from dataclasses import replace
    from nullion.response_sanitizer import native_filesystem_receipt_reply
    assert native_filesystem_receipt_reply(replace(result, invocation_id='another-request')) is None
    assert native_filesystem_receipt_reply(replace(result, output={**result.output,
        'action_receipt':{**receipt,'summary':'Unverified replacement'}})) is None


def test_unresolved_pattern_is_not_reported_as_already_absent(tmp_path):
    from nullion.tools import ToolInvocation, _terminal_delete_observations, _terminal_delete_receipt
    invocation = ToolInvocation('pattern','terminal_exec','workspace_admin',
        {'command':'rm -f -- '+str(tmp_path/'selection-*.tmp')})
    observations = _terminal_delete_observations(invocation,execution_cwd=tmp_path)
    receipt = _terminal_delete_receipt(observations)
    assert receipt['changes'][0]['status']=='unconfirmed'
    assert receipt['summary']=='Deletion was not confirmed.'


def test_shell_discovery_preserves_verified_partial_deletion_receipt(tmp_path):
    from nullion.chat_operator import _append_chat_artifacts_to_reply, _sanitize_chat_reply
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolResult, _build_terminal_exec_handler

    deleted = tmp_path / "delete.txt"
    retained = tmp_path / "retain.txt"
    for path in (deleted, retained):
        path.write_text("fixture", encoding="utf-8")
    command = shlex.join(["rm", "-f", "--", str(deleted), str(retained)])

    class Backend:
        def run(self, *args, **kwargs):
            deleted.unlink()
            return SimpleNamespace(stdout="Both deleted", stderr="", exit_code=0, shell="sh")

    handler = _build_terminal_exec_handler(tmp_path, allowed_roots=[tmp_path], terminal_executor_backend=Backend())
    result = handler(ToolInvocation("delete", "terminal_exec", "workspace_admin", {"command": command}))
    results = [
        ToolResult("discover", "file_search", "completed", {"matches": [str(deleted), str(retained)]}),
        result,
    ]
    prompt = "Elimina los dos archivos seleccionados."
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    reply = _append_chat_artifacts_to_reply(
        runtime, reply="Ya eliminé los dos archivos.", artifact_paths=(), prompt=prompt,
        tool_results=results, required_attachment_extensions=(),
    )
    assert _sanitize_chat_reply(prompt=prompt, reply=reply, tool_results=results) == reply
    assert "Deleted 1 path." in reply and "was not confirmed" in reply
    assert "Both deleted" not in reply
    assert retained.is_file()
