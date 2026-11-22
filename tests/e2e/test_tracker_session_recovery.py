"""Real tracker writes and rejection recovery through the Code Mode runner."""
import json
import os
import pytest
from tests.test_live_tracking import fixture, create

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('prompt', ['監視設定を修復してください', 'Repara la configuración del seguimiento'])
@pytest.mark.parametrize('with_deadline', [False, True])
def test_known_tracker_rejection_does_not_poison_execution_session(fixture, monkeypatch, prompt, with_deadline):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.preferences import save_preferences
    from tests.workflow_provider_protocol import workflow_provider_reply
    from tests.e2e.test_quoted_reply_review_and_cleanup import text
    f = fixture
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    save_preferences({'sentinel_mode': 'risk_based'})
    register_live_tracker_tools(f.registry)
    f.registry.live_tracker_store_path = f.store.path
    job = create(f)
    args = {'id': job['id'], **({'expires_at': job['expires_at']} if with_deadline else {})}
    programs = [
        f"changed = call_tool('update_live_tracker', {{'id': {job['id']!r}, 'interval_seconds': 600}})\n"
        f"resumed = call_tool('resume_live_tracker', {args!r})\nresumed",
        f"rejected = call_tool('resume_live_tracker', {args!r}, on_error='return')\n"
        "observed = call_tool('list_live_trackers', {})\nobserved",
    ]
    class Model:
        calls = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            if self.calls < len(programs):
                index = self.calls
                self.calls += 1
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': f'program-{index}',
                    'name': 'code_mode', 'input': {'tool_names': ['update_live_tracker', 'resume_live_tracker', 'list_live_trackers'], 'code': programs[index]}}]}
            assert f.store.get(job['id'])['interval_seconds'] == 600
            return text('Tracker is active and checks every 600 seconds. No restart was needed.')
    result = AgentOrchestrator(model_client=Model()).run_turn(
        conversation_id='web:alpha', principal_id='web:alpha', user_message=prompt,
        conversation_history=[], tool_registry=f.registry, policy_store=f.runtime.store,
        approval_store=f.runtime.store, max_iterations=8)
    assert 'Tracker is active' in result.final_text, result.final_text
    summaries = [r for r in result.tool_results if r.tool_name == 'code_mode']
    assert summaries[-1].status == 'completed', [(r.status, r.output) for r in summaries]
    assert all(r.output.get('reason') not in {'code_mode_unknown_write_outcome', 'code_mode_session_blocked'} for r in summaries)
    assert len([r for r in result.tool_results if r.tool_name == 'update_live_tracker']) == 1
    rejections = [r for r in result.tool_results if r.tool_name == 'resume_live_tracker']
    assert len(rejections) == 2 and all(r.output['executed'] is False for r in rejections)
    saved = f.store.get(job['id'])
    assert saved['state'] == 'active' and saved['expires_at'] == job['expires_at']
    assert saved['interval_seconds'] == 600
    assert saved.get('resume_count', 0) == 0
