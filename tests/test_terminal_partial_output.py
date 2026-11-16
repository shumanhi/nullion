"""Failed shell processes retain evidence without claiming complete execution."""
import shlex
import subprocess
import sys

import pytest

from nullion.tools import ToolInvocation, _build_terminal_exec_handler


@pytest.mark.parametrize('stdout,stderr', [(b'known row\xff\n', b'partial warning'), ('known row\n', ''), (None, None)])
def test_timeout_keeps_captured_streams_and_incomplete_status(tmp_path, stdout, stderr):
    class Backend:
        def run(self, *args, **kwargs):
            raise subprocess.TimeoutExpired('fixture', 1, output=stdout, stderr=stderr)
    result = _build_terminal_exec_handler(tmp_path, allowed_roots=[tmp_path], terminal_executor_backend=Backend())(
        ToolInvocation('timeout', 'terminal_exec', 'workspace_admin', {'command': 'fixture', 'timeout_seconds': 1}))
    assert result.status == 'failed' and result.output['execution_outcome'] == 'timeout'
    assert result.output['output_complete'] is False and result.output['command_executed'] is True
    assert isinstance(result.output['stdout'], str) and isinstance(result.output['stderr'], str)
    if stdout:
        assert 'known row' in result.output['stdout']
    assert not result.output.get('artifact_paths')
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    compact = _compact_tool_output_for_model_context('terminal_exec', result.output)
    assert compact['output_complete'] is False and compact['execution_outcome'] == 'timeout'


def test_real_nonzero_exit_keeps_rows_without_becoming_success(tmp_path):
    command = shlex.quote(sys.executable) + ' -c ' + shlex.quote("print('known row', flush=True); raise SystemExit(141)")
    result = _build_terminal_exec_handler(tmp_path, allowed_roots=[tmp_path])(
        ToolInvocation('partial', 'terminal_exec', 'workspace_admin', {'command': command, 'shell': 'bash'}))
    assert result.status == 'failed' and result.output['exit_code'] == 141
    assert 'known row' in result.output['stdout'] and result.output['output_complete'] is False


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX process group ownership')
@pytest.mark.parametrize('backend_kind', ['subprocess', 'launcher'])
def test_timeout_kills_only_owned_shell_pipeline_and_retains_output(tmp_path, backend_kind):
    import os
    import time
    from pathlib import Path
    from nullion.tools import SubprocessTerminalExecutorBackend, TerminalExecutionPolicy
    unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
    code = "import os,time; print('owned='+str(os.getpid()),flush=True); time.sleep(30)"
    command = shlex.quote(sys.executable)+' -c '+shlex.quote(code)+' | cat'
    backend = SubprocessTerminalExecutorBackend()
    if backend_kind == 'launcher':
        from nullion.tools import SandboxLauncherTerminalExecutorBackend
        launcher = tmp_path/'launcher.py'
        launcher.write_text('import subprocess,sys; subprocess.run(sys.argv[-1],shell=True)')
        backend = SandboxLauncherTerminalExecutorBackend(launcher_command=sys.executable, launcher_args=[str(launcher)])
    try:
        started = time.monotonic()
        with pytest.raises(subprocess.TimeoutExpired) as caught:
            backend.run(command, cwd=str(tmp_path), timeout=1,
                policy=TerminalExecutionPolicy(None, ()), shell='bash')
        assert time.monotonic()-started < 2.5
        output = caught.value.stdout
        if isinstance(output, bytes): output = output.decode()
        identity = next(row.split('=', 1)[1] for row in output.splitlines() if row.startswith('owned='))
        status = Path('/proc')/identity/'stat'
        if status.parent.parent.exists():
            try:
                assert status.read_text().split()[2] == 'Z'
            except (FileNotFoundError, ProcessLookupError):
                pass
        assert unrelated.poll() is None, 'Cleanup escaped its owned process group'
    finally:
        unrelated.kill(); unrelated.wait()
