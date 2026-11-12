"""A rejected report is revised without restarting successful source work."""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary
from tests.e2e.test_universal_outcome_review_boundary import text

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('prompt', ['Informa del valor seleccionado.', '選択した値を報告してください。'])
@pytest.mark.parametrize('corrects_report', [True, False])
def test_report_gap_gets_one_tools_disabled_revision_before_terminal_delivery(tmp_path, monkeypatch, prompt, corrects_report):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    reads = []
    registry = ToolRegistry()
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7})
    registry.register(ToolSpec('opaque_adapter', 'Read selected resource value', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    report = 'Selected resource: 7.'
    class Provider:
        agent_calls = 0
        repairs = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Report the selected value accurately',
                        'requirement_kind': 'fact', 'evidence_subject_id': 'report',
                        'eligible_evidence_tool_names': ['opaque_adapter']}]})
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                correct = packet['draft_final_response'] == report
                evidence = next(row for row in packet['tool_evidence'] if row['tool_name'] == 'opaque_adapter')
                return text({'disposition': 'complete' if correct else 'retry',
                    'all_material_requirements_enumerated': True,
                    'unresolved_requirements': [] if correct else ['Correct the report label and value from the completed source.'],
                    'retry_tool_names': [], 'user_facing_failure': None if correct else 'The source was checked; the report still needs correction.',
                    'material_requirements': [{'requirement_id': row['requirement_id'],
                        'satisfied': correct or row['requirement_kind'] == 'fact', 'draft_covered': correct,
                        'draft_contradicted': not correct,
                        'evidence_invocation_ids': [evidence['invocation_id']]}
                        for row in packet['material_requirement_contract']]})
            if system.startswith('You repair scheduled-task final answers'):
                self.repairs += 1
                assert not kw['tools']
                prompt_text = kw['messages'][0]['content'][0]['text']
                records = json.JSONDecoder().raw_decode(prompt_text.split('Verified compact tool evidence from this run:\n', 1)[1])[0]
                return text({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [next(row['evidence_id'] for row in records if row.get('status') == 'completed')],
                    'report': report if self.repairs > 1 and corrects_report else 'Incorrect resource: 8.'})
            self.agent_calls += 1
            if not reads:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'selected',
                    'name': 'opaque_adapter', 'input': {}}]}
            return text('Incorrect resource: 8.')
    provider = Provider()
    deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='report-revision', name='確認', schedule='0 * * * *', task=prompt,
                  delivery_channel='telegram', delivery_target='123')
    result = run_scheduled(job, provider, runtime, registry, send)
    if not corrects_report and not result.get('cron_report_failed'):
        from pathlib import Path
        Path('/outputs/report-revision-diagnostic.json').write_text(json.dumps(result,default=str))
    assert len(reads) == 1
    assert not result.get('cron_completion_continuations'), 'A wording-only retry must not restart the producer'
    assert provider.repairs == 2, result
    assert bool(result.get('cron_report_failed')) is not corrects_report
    assert len(deliveries) == 1
    assert (report in deliveries[0]['text']) is corrects_report
    assert 'Incorrect resource: 8.' not in deliveries[0]['text']
