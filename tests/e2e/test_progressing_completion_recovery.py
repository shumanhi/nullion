"""Real graph and SQLite boundary for long, progressing completion repairs."""
import json
import os

import pytest

from tests.e2e.test_universal_outcome_review_boundary import runtime_and_registry, text

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('prompt', ['Revisa los nueve registros.', '9件の記録を確認してください。'])
@pytest.mark.parametrize('mode', ['progress', 'duplicate_calls', 'hard_limit'])
def test_rejected_early_draft_does_not_truncate_progressing_source_work(tmp_path, monkeypatch, prompt, mode):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolResult

    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'opaque_adapter')
    # The only fake boundaries are the external model and selected data source.
    def read(inv):
        calls.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': inv.arguments['record']})
    spec = registry.get_spec('opaque_adapter')
    registry.unregister('opaque_adapter')
    registry.register(spec, read)

    class Provider:
        drafts = 0
        agent_calls = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Inspect all nine selected records and report their identifiers.',
                        'requirement_kind': 'fact', 'evidence_subject_id': 'selected-records',
                        'eligible_evidence_tool_names': ['opaque_adapter']}]})
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                evidence = [row for row in packet['tool_evidence'] if row['tool_name'] == 'opaque_adapter'
                            and row['status'] == 'completed']
                complete = {inv.arguments['record'] for inv in calls} == {'R' + str(i) for i in range(9)} and 'R8' in packet['draft_final_response']
                return text({'disposition': 'complete' if complete else 'retry',
                    'all_material_requirements_enumerated': True,
                    'unresolved_requirements': [] if complete else ['Inspect the remaining selected records.'],
                    'retry_tool_names': [] if complete else ['opaque_adapter'],
                    'user_facing_failure': None if complete else 'Some selected records still need review.',
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': complete,
                        'draft_covered': complete, 'evidence_invocation_ids': [e['invocation_id'] for e in evidence]}
                        for row in packet['material_requirement_contract']]})
            self.agent_calls += 1
            if len(calls) == 1 and self.drafts == 0:
                self.drafts += 1
                return text('R0 only.')
            if len(calls) < 9:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'read-' + str(len(calls)),
                    'name': 'opaque_adapter', 'input': {'record': 'R0' if mode == 'duplicate_calls' else 'R' + str(len(calls))}}]}
            return text('Reviewed: ' + ', '.join('R' + str(i) for i in range(9)))

    provider = Provider()
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:progress-repair',
        principal_id='web:progress-repair', user_message=prompt, conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, max_iterations=7 if mode == 'hard_limit' else 20)
    reviews = runtime.store.list_recent_conversation_events('web:progress-repair', event_type='conversation.completion_review')
    if mode != 'progress':
        assert result.response_fulfilled is False
        # The first rejection has the pre-existing one-time eight-call
        # allowance. A progressing window must not enlarge that final ceiling.
        assert provider.agent_calls <= (10 if mode == 'hard_limit' else 11), json.dumps({'agent_calls': provider.agent_calls,
            'tool_calls': [(i.invocation_id, i.arguments) for i in calls], 'review_count': len(reviews),
            'tool_results': [(r.invocation_id, r.tool_name, r.status) for r in result.tool_results]})
        if mode == 'hard_limit':
            assert result.reached_iteration_limit
        else:
            assert not result.reached_iteration_limit, 'Stall cutoff must preserve the larger hard budget'
        return
    assert result.response_fulfilled is True, json.dumps({'reply': result.final_text,
        'calls': [i.arguments for i in calls], 'reviews': reviews}, ensure_ascii=False)
    assert [inv.arguments['record'] for inv in calls] == ['R' + str(i) for i in range(9)]
    assert all('R' + str(i) in result.final_text for i in range(9))
    events = runtime.store.list_conversation_events('web:progress-repair')
    assert any(row.get('event_type') == 'conversation.completion_review' and row.get('disposition') == 'complete'
               for row in events)
    renewals = [row for row in events if row.get('event_type') == 'conversation.completion_recovery_budget']
    assert renewals and all(row['next_soft_limit'] <= row['hard_limit'] == 20 for row in renewals)
    assert not result.reached_iteration_limit
