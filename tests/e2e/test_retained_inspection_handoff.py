"""Real scheduled runner, graph, SQLite and transport; fake external IO only."""
import json
import os

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary
from tests.e2e.test_universal_outcome_review_boundary import text

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('source_size',[600,1500])
@pytest.mark.parametrize('prompt', ['Informa del valor seleccionado.', '選択した値を報告してください。'])
@pytest.mark.parametrize('inspection_owner', ['reviewer', 'composer'])
@pytest.mark.parametrize('corrects_report', [True, False])
def test_inspected_source_survives_report_correction_without_research_restart(tmp_path, monkeypatch, prompt, inspection_owner, corrects_report, source_size):
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.runtime_persistence import load_runtime_store
    runtime = setup_runtime(tmp_path, monkeypatch)
    reads = []
    registry = ToolRegistry()
    prefix = 'Earlier context. ' * source_size
    fact = 'Selected resource: 7.'
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'body': prefix + fact + ' Later unrelated context.' * source_size})
    registry.register(ToolSpec('opaque_adapter', 'Read selected resource', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        repairs = 0
        inspections = 0
        reviews_after_correction = 0
        producer_calls = 0
        def request_inspection(self, identity):
            self.inspections += 1
            return text({'disposition': 'inspect_evidence', 'evidence_requests': [{
                'invocation_id': identity, 'json_pointer': '/body', 'offset': len(prefix), 'length': len(fact)}]})
        def create(self, **kw):
            system = str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Report the selected value accurately',
                        'requirement_kind': 'fact', 'evidence_subject_id': 'report',
                        'eligible_evidence_tool_names': ['opaque_adapter']}]})
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                assert fact not in json.dumps(packet['tool_evidence']), 'The decisive field must be absent from compact previews'
                evidence = next(row for row in packet['tool_evidence'] if row['tool_name'] == 'opaque_adapter')
                if inspection_owner == 'reviewer' and not self.inspections:
                    return self.request_inspection(evidence['invocation_id'])
                if self.repairs:
                    self.reviews_after_correction += 1
                    windows = packet['previously_inspected_evidence']
                    assert any(row['content'] == fact and row['invocation_id'] == evidence['invocation_id'] for row in windows)
                elif inspection_owner == 'reviewer':
                    assert any(row['content'] == fact for row in [*packet.get('additional_evidence',[]), *packet.get('previously_inspected_evidence',[])])
                correct = packet['draft_final_response'] == fact
                return text({'disposition': 'complete' if correct else 'retry',
                    'all_material_requirements_enumerated': True, 'retry_tool_names': [],
                    'unresolved_requirements': [] if correct else ['Correct the report from the inspected source.'],
                    'user_facing_failure': None if correct else 'The report does not match the inspected source.',
                    'supported_findings': ([{'text':fact,'evidence_invocation_ids':[evidence['invocation_id']]}]
                        if self.repairs>=2 and not corrects_report else []),
                    'material_requirements': [{'requirement_id': row['requirement_id'],
                        'satisfied': correct, 'draft_covered': correct,
                        'draft_contradicted': not correct,
                        'evidence_invocation_ids': [evidence['invocation_id']]}
                        for row in packet['material_requirement_contract']]})
            if system.startswith('You repair scheduled-task final answers'):
                assert not kw['tools']
                prompt_text = kw['messages'][0]['content'][0]['text']
                records = json.JSONDecoder().raw_decode(prompt_text.split('Verified compact tool evidence from this run:\n', 1)[1])[0]
                assert fact not in json.dumps(records), 'The composer must use inspected proof, not a lucky tail preview'
                source = next(row for row in records if row.get('status') == 'completed')
                if inspection_owner == 'composer' and not self.inspections:
                    return self.request_inspection(source['evidence_id'])
                if inspection_owner == 'reviewer' or self.repairs:
                    windows = json.JSONDecoder().raw_decode(prompt_text.split(
                        'Previously inspected current-run source windows (rebuilt from unchanged owned results, not approval):\n', 1)[1])[0]
                    fresh = (json.JSONDecoder().raw_decode(prompt_text.split(
                        'Additional retained evidence: ', 1)[1])[0]
                        if 'Additional retained evidence: ' in prompt_text else [])
                    assert any(row['content'] == fact for row in [*windows, *fresh])
                else:
                    assert fact in prompt_text.split('Additional retained evidence: ', 1)[1]
                self.repairs += 1
                return text({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [source['evidence_id']],
                    'report': fact if self.repairs > 1 and corrects_report else 'Incorrect resource: '+str(7+self.repairs)+'.'})
            self.producer_calls += 1
            if not reads:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'selected',
                    'name': 'opaque_adapter', 'input': {}}]}
            return text('Incorrect resource: 8.')
    provider = Provider()
    deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='inspection-handoff', name='確認', schedule='0 * * * *', task=prompt,
        delivery_channel='telegram', delivery_target='123')
    result = run_scheduled(job, provider, runtime, registry, send)
    assert len(reads) == 1 and provider.inspections == 1
    # The common completion owner performs the bounded content corrections.
    # It must not restart the producer just to rewrite an already-read result.
    assert provider.repairs == 2 and provider.producer_calls == 2, json.dumps({
        'result':{k:v for k,v in result.items() if k not in {'tool_results','messages_snapshot','completion_review_handoff'}},
        'reviews':[{k:e.get(k) for k in ('disposition','review_error','review_protocol_errors','unresolved_requirements')}
            for e in runtime.store.list_recent_conversation_events('telegram:123')
            if e.get('event_type')=='conversation.completion_review']},default=str)
    assert provider.reviews_after_correction >= 1
    assert not result.get('cron_completion_continuations')
    assert not result.get('cron_report_failed')
    assert result['execution_outcome'].get('completion_verified',corrects_report) is corrects_report
    assert len(deliveries) == 1 and fact in deliveries[0]['text'], repr({**result, 'review_events':[e for e in runtime.store.list_conversation_events('telegram:123') if e.get('event_type')=='conversation.completion_review']})
    assert 'Incorrect resource: 8.' not in deliveries[0]['text']
    events = load_runtime_store(runtime.checkpoint_path).list_recent_conversation_events('telegram:123', limit=100)
    terminal = [row for row in events if row.get('event_type') == 'conversation.cron_run']
    assert len(terminal) == 1 and terminal[0]['response_fulfilled'] is corrects_report
