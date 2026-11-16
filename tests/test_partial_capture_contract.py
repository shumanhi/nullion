"""Incomplete observations may inform a partial answer, never satisfy native effects."""
from dataclasses import replace
import subprocess

import pytest

from nullion.runtime_store import RuntimeStore
from nullion.response_fulfillment_contract import evaluate_response_fulfillment
from nullion.tools import ToolInvocation, _build_terminal_exec_handler


@pytest.mark.parametrize('prompt', ['Report the captured observation.', '取得できた観測を報告してください。'])
@pytest.mark.parametrize('gap', ['capture', 'missing_binding', 'wrong_identity', 'empty_capture', 'action', 'artifact'])
def test_bound_partial_capture_preserves_primary_text_without_promoting_success(tmp_path, prompt, gap):
    class Backend:
        def run(self, *args, **kwargs):
            raise subprocess.TimeoutExpired('fixture', 1, output=b'Cedar 120 bytes\n')
    result = _build_terminal_exec_handler(tmp_path, allowed_roots=[tmp_path], terminal_executor_backend=Backend())(
        ToolInvocation('owned-capture', 'terminal_exec', 'workspace_admin', {'command':'fixture', 'timeout_seconds':1}))
    if gap == 'missing_binding': result = replace(result, response_requirements=None)
    if gap == 'wrong_identity':
        result = replace(result, response_requirements={'_native_partial_capture':{
            **result.response_requirements['_native_partial_capture'], 'invocation_id':'someone-else'}})
    if gap == 'empty_capture': result = replace(result, output={**result.output, 'stdout':''})
    decision = evaluate_response_fulfillment(store=RuntimeStore(), conversation_id='web:partial', user_message=prompt,
        reply='Captured Cedar 120 bytes. This observation is incomplete.', tool_results=[result],
        preserve_authored_reply=True,
        required_tool_names=['terminal_exec', *(['email_send'] if gap == 'action' else [])],
        required_attachment_extensions=['.pdf'] if gap == 'artifact' else (), artifact_roots=[tmp_path])
    assert decision.satisfied is False and 'tool:terminal_exec' in decision.missing_requirements
    assert decision.partial_report_ready is (gap == 'capture')
    if gap == 'capture':
        assert 'Cedar 120 bytes' in decision.reply and 'timed out after 1s' in decision.reply
    if gap in {'missing_binding','wrong_identity','empty_capture'}:
        assert 'Cedar 120 bytes' not in decision.reply
    if gap == 'action': assert 'tool:email_send' in decision.missing_requirements
    if gap == 'artifact': assert any(r.startswith('attachment:') for r in decision.missing_requirements)
