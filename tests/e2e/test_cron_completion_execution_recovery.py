"""Real scheduled runner, graph, SQLite and platform artifact delivery boundary."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, tool_response, text_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def encoded(value):
    return text_response(json.dumps(value, ensure_ascii=False))


@pytest.mark.parametrize('task', ['新しい対象が確認できた場合のみ通知。', 'Avisa solo si hay nuevos elementos confirmados.'])
@pytest.mark.parametrize('name', ['opaque_read', 'browser_extract_text'])
def test_verified_negative_check_closes_silently_without_restarting_sources(tmp_path, monkeypatch, task, name):
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(); reads = []
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'items': [], 'count': 0, 'text': 'Completed check: no qualifying records.'})
    registry.register(ToolSpec(name, 'Read selected source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        producer_calls = 0
        reviews = 0
        def create(self, **kw):
            system = kw.get('system') or ''
            if 'material-requirements planner' in system:
                return encoded({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Verify current selected records against delivery history.',
                        'completion_owner': 'producer', 'delivery_required': True, 'requirement_kind': 'fact',
                        'evidence_subject_id': 'selected', 'artifact_member_subject_ids': [],
                        'eligible_evidence_tool_names': [name]}]})
            if 'semantic completion reviewer' in system:
                self.reviews += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                reference = next(row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name'] == name)
                return encoded({'disposition': 'retry', 'all_material_requirements_enumerated': True,
                    'unresolved_requirements': ['Suppress the unnecessary draft; the completed check found no new match.'],
                    'retry_tool_names': [], 'user_facing_failure': 'Internal draft correction.',
                    'scheduled_alert_delivery': {'decision': 'silent', 'reason': 'no_verified_match'},
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': True,
                        'draft_covered': False, 'evidence_invocation_ids': [reference]}
                        for row in packet['material_requirement_contract']]})
            if system.startswith('Classify delivery'):
                return encoded({'decision':'silent','reason':'no_verified_match'})
            protocol = workflow_provider_reply(kw, completion=False)
            if protocol is not None: return protocol
            self.producer_calls += 1
            return tool_response(name, {}) if not reads else text_response('Completed check: no qualifying records.')
    provider = Provider(); deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='quiet-check', name='確認', task=task, schedule='* * * * *',
        delivery_channel='telegram', delivery_target='123',
        artifact_delivery_options={'alert_delivery_mode': 'verified_matches_only'})
    result = run_scheduled(job, provider, runtime, registry, send)
    assert result['response_fulfilled'] is True and result['execution_outcome']['execution_succeeded'] is True, result
    assert result['cron_alert_delivery_decision'] == 'silent' and not deliveries
    assert len(reads) == 1 and provider.producer_calls == 2 and (provider.reviews == 0)
    assert not result.get('cron_completion_continuations')
    events = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.completion_review')
    assert not events










def test_cancelled_native_report_never_enters_delivery_content_review(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    path = Path(artifact_root_for_principal('telegram:123'))/'selected.html'
    path.parent.mkdir(parents=True, exist_ok=True)
    cancelled = {'value': False}; writes = []
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    def write(inv):
        writes.append(inv)
        path.write_text('<!doctype html><html><body><p>Selected result</p></body></html>')
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'path': str(path), 'bytes_written': path.stat().st_size})
    registry.register(ToolSpec('file_write', 'Write selected HTML', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2,
        input_schema={'type': 'object', 'properties': {'path': {'type': 'string'}, 'content': {'type': 'string'}},
            'required': ['path', 'content']}), write)
    class Provider:
        after_cancel_calls = 0
        def create(self, **kw):
            if cancelled['value']: self.after_cancel_calls += 1
            protocol = workflow_provider_reply(kw)
            if protocol is not None: return protocol
            return tool_response('file_write', {'path': str(path), 'content':
                '<!doctype html><html><body><p>Selected result</p></body></html>'}) if not writes else text_response('Report attached.')
    @contextmanager
    def cancellation_boundary():
        yield
        # A real external cancellation arrives after the producer returns,
        # before the delegated content owner would start another model call.
        cancelled['value'] = True
    provider = Provider()
    job = CronJob(id='cancel-handoff', name='確認', task='選択された結果を .html に保存。', schedule='* * * * *',
        artifact_delivery_options={'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html']})
    result = run_single_agent_cron_turn(job, 'telegram:123', label='Scheduled task',
        orchestrator=AgentOrchestrator(model_client=provider), runtime=runtime, tool_registry=registry,
        settings=SimpleNamespace(enabled_skill_packs=()), cancellation_checker=lambda: cancelled['value'],
        turn_guard=cancellation_boundary)
    assert len(writes) == 1 and path.exists()
    assert result['cron_run_cancelled'] is True and result['response_fulfilled'] is False
    assert provider.after_cancel_calls == 0
