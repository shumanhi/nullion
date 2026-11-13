"""Real runner/SQLite/delivery graph; only external source, model and SDK faked."""
import json
import os
from datetime import UTC, datetime

import pytest

from tests.e2e.test_scheduled_report_completion import (
    setup_runtime, run_scheduled, telegram_boundary, tool_response, text_response,
)
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('task', ['確認できた予定と残りの確認結果を報告。', 'Resume las citas y las comprobaciones pendientes.'])
@pytest.mark.parametrize('tool_name', ['email_read', 'browser_extract_text'])
@pytest.mark.parametrize('followup', ['none'])
def test_primary_partial_report_keeps_findings_without_quality_retry(tmp_path, monkeypatch, task, tool_name, followup):
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-'+kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=kind, mode=PolicyMode.ALLOW,
            selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    registry = ToolRegistry()
    reads = []
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'text': 'The selected appointment starts at 7 PM; the second source has not been checked.'})
    registry.register(ToolSpec(tool_name, 'Read selected source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    followups = []
    def next_step(inv):
        followups.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'items': [{'id': 'second-source', 'text': 'The second appointment starts at 8 PM.'}]}
            if followup == 'read' else {'action_receipt': {'object_id': 'fixture-event'}})
    if followup != 'none':
        registry.register(ToolSpec('next_step', 'Selected followup', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ if followup == 'read' else ToolSideEffectClass.WRITE, False, 2), next_step)
    class Provider:
        writes = 0
        producer = 0
        def create(self, **kw):
            system = kw.get('system') or ''
            if 'You repair scheduled-task final answers' in system:
                self.writes += 1
                if self.writes > 1:
                    raise TimeoutError('External final writer failed')
                return text_response(json.dumps({'requirements_satisfied': False, 'source_state': 'available',
                    'evidence_refs': ['e0'], 'delivery_decision': 'send', 'delivery_reason': 'report',
                    'remaining_work': ['Check the second selected source.'],
                    'report': 'The selected appointment starts at 7 PM.'}))
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [
                        {'requirement': 'Check both selected sources.', 'requirement_kind': 'fact',
                         'evidence_subject_id': 'selected', 'eligible_evidence_tool_names': [tool_name]}]}))
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                return text_response(json.dumps({'disposition': 'retry',
                    'all_material_requirements_enumerated': True,
                    'unresolved_requirements': ['Check the second selected source.'],
                    # Ask for one legitimate content correction after retained
                    # work, so the failed second writer is actually exercised.
                    'retry_tool_names': ([] if self.writes and (followup == 'none' or followups)
                        else [tool_name, *(['next_step'] if followup != 'none' else [])]),
                    'user_facing_failure': 'The second source is unfinished.',
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': False,
                        'draft_covered': False, 'evidence_invocation_ids': []}
                        for row in packet['material_requirement_contract']]}))
            protocol = workflow_provider_reply(kw, completion=False)
            if protocol is not None:
                return protocol
            self.producer += 1
            if not reads:
                return tool_response(tool_name, {'id': 'fixture-a'} if tool_name == 'email_read' else {})
            if self.writes == 1 and followup != 'none' and not followups:
                return tool_response('next_step', {})
            raise TimeoutError('External producer interrupted after reading the first source')
    provider = Provider()
    deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='retained-text', name='確認', task=task, schedule='* * * * *',
        delivery_channel='telegram', delivery_target='123')
    result = run_scheduled(job, provider, runtime, registry, send)
    assert provider.writes == 1, result
    assert len(reads) == 1, 'A failed writer must not repeat completed source reads'
    if followup != 'none':
        assert len(followups) == 1
    if followup == 'write':
        assert len(deliveries) == 1 and '7 PM' not in deliveries[0]['text']
        assert not result.get('cron_report_partial') and result['response_fulfilled'] is False
        return
    assert len(deliveries) == 1 and '7 PM' in deliveries[0]['text'], (result, deliveries)
    assert deliveries[0]['text'].count('7 PM') == 1
    assert 'remaining checks' in deliveries[0]['text']
    assert result['response_fulfilled'] is False and result['cron_report_partial'] is True
    assert result['execution_outcome']['status'] == 'partially_succeeded'
    assert not result['execution_outcome'].get('completion_verified')
    events = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.tool_timing')
    assert any(row.get('tool_name') == tool_name and row.get('status') == 'completed' for row in events)
