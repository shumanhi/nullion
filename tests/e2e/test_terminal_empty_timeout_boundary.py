"""A real empty-output subprocess timeout survives the persistent runtime boundary."""
import os
import shlex
import sys
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker subprocess E2E')


def test_empty_subprocess_timeout_returns_native_failure_through_runtime(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    from nullion.preferences import save_preferences
    from nullion.runtime import invoke_tool
    from nullion.tools import ToolInvocation, ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_terminal_exec_handler
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate

    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'sentinel_mode': 'allow_all'})
    workspace = tmp_path / 'workspace'
    workspace.mkdir()
    registry = ToolRegistry(filesystem_allowed_roots=[workspace])
    registry.register(ToolSpec('terminal_exec', 'Run the selected command', ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC, False, 5), _build_terminal_exec_handler(
            workspace, allowed_roots=[workspace]))
    command = shlex.quote(sys.executable) + ' -c ' + shlex.quote('import time; time.sleep(20)')
    started = time.monotonic()
    result = invoke_tool(runtime.store, ToolInvocation('empty-timeout', 'terminal_exec', 'workspace_admin',
        {'command': command, 'shell': 'bash', 'timeout_seconds': 1}), registry=registry)
    assert time.monotonic() - started < 5
    assert result.status == 'failed', result
    assert result.output['execution_outcome'] == 'timeout'
    assert result.output['command_executed'] is True
    assert result.output['output_complete'] is False
    assert result.output['stdout'] == result.output['stderr'] == ''
    assert not result.output.get('artifact_paths')
    assert not (result.response_requirements or {}).get('_native_partial_capture')
    view = _compact_tool_output_for_model_context('terminal_exec', result.output)
    assert view['execution_outcome'] == 'timeout' and view['output_complete'] is False
    completed = [event for event in runtime.store.list_events() if event.event_type == 'tool.invocation.completed']
    assert len(completed) == 1
    assert completed[0].payload['invocation_id'] == 'empty-timeout'
    assert completed[0].payload['status'] == 'failed'
