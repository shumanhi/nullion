"""Completed batch work survives cross-role verification and report revision."""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary
from tests.e2e.test_universal_outcome_review_boundary import text

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('prompt', ['確認した記録と処理結果を報告してください。', 'Informe de los registros y las acciones comprobadas.'])
@pytest.mark.parametrize('invalid_proof', [None])
def test_batch_report_reuses_constraint_proof_without_repeating_source_work(tmp_path, monkeypatch, prompt, invalid_proof):
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry()
    reads = []
    def read(inv):
        reads.append(inv.tool_name)
        output = ({'records': [{'id': str(i), 'value': i + 1} for i in range(54)]}
                  if inv.tool_name == 'record_read' else {'existing': True, 'id': 'event-7'})
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', output)
    for name in ('record_read', 'action_check'):
        registry.register(ToolSpec(name, 'Read selected records', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2), read)
    kinds = ['fact', 'constraint', 'fact', 'action', 'constraint']
    if invalid_proof == 'independent_fact':
        kinds[1] = 'fact'
    report = 'Reviewed 54 records. Existing event verified; no duplicate created.'
    class Provider:
        repairs = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': f'確認-{i}', 'requirement_kind': kind,
                        'delivery_required': kind != 'constraint' or i == 4, 'evidence_subject_id': f'subject-{i}',
                        'eligible_evidence_tool_names': ['action_check'] if i == 2 else ['record_read', 'action_check']}
                        for i, kind in enumerate(kinds)]})
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                proof = {row['tool_name']: row['invocation_id'] for row in packet.get('citable_evidence', packet['tool_evidence'])}
                assert set(proof) == {'record_read', 'action_check'}
                correct = packet['draft_final_response'] == report
                return text({'disposition': 'complete' if correct else 'retry',
                    'all_material_requirements_enumerated': True,
                    'unresolved_requirements': [] if correct else ['Correct the report from retained records.'],
                    'retry_tool_names': [], 'user_facing_failure': None if correct else 'Report wording needs correction.',
                    'material_requirements': [{'requirement_id': row['requirement_id'],
                        'satisfied': correct or i < 4, 'draft_covered': correct,
                        'evidence_invocation_ids': (['inspection-receipt'] if invalid_proof == 'inspection_receipt'
                            else [proof['action_check']] if i == 2 else
                            [proof['record_read'], proof['action_check']] if i >= 3 else [proof['record_read']])}
                        for i, row in enumerate(packet['material_requirement_contract'])]})
            if system.startswith('You repair scheduled-task final answers'):
                self.repairs += 1
                assert not kw['tools'], 'Report correction cannot read or write account data'
                records = json.JSONDecoder().raw_decode(kw['messages'][0]['content'][0]['text'].split(
                    'Verified compact tool evidence from this run:\n', 1)[1])[0]
                return text({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [row['evidence_id'] for row in records if row.get('status') == 'completed'],
                    'report': report})
            if len(reads) < 2:
                name = ('record_read', 'action_check')[len(reads)]
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': name,
                    'name': name, 'input': {}}]}
            return text(report)
    provider = Provider()
    deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='batch-proof', name='確認', schedule='0 * * * *', task=prompt,
        delivery_channel='telegram', delivery_target='123')
    result = run_scheduled(job, provider, runtime, registry, send)
    assert reads == ['record_read', 'action_check']
    assert not result.get('cron_completion_continuations')
    assert len(deliveries) == 1
    assert report in deliveries[0]['text']
    assert not result.get('cron_report_failed')
    if invalid_proof is None:
        assert result['response_fulfilled'] is True
        assert 'Review warning:' not in deliveries[0]['text']
    else:
        # Invalid citation bindings invalidate the grader's decision, not the
        # completed source reads or usable primary report. Completion stays
        # unknown instead of being certified by an unavailable review.
        assert result['response_fulfilled'] is None
        assert result['response_presentation']['outcome_review_role'] == 'helper'
        assert result['response_presentation']['outcome_review_status'] == 'unavailable'
        assert result['execution_outcome']['completion_verified'] is False
        assert deliveries[0]['text'].count('Review warning:') == 0
    assert 'Incorrect report.' not in deliveries[0]['text']
    assert runtime.store.list_events(), 'Real persistent runner must record its execution'
