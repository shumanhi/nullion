import json

import pytest

from nullion.workflow_reasoning import assess_action
from nullion.tools import ToolInvocation, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult


def run(responses, evidence=()):
    calls = []
    class Provider:
        def create(self, **kw):
            calls.append(kw)
            value = responses[min(len(calls)-1, len(responses)-1)]
            if isinstance(value, Exception):
                raise value
            return {'content': [{'type': 'text', 'text': value if isinstance(value, str) else json.dumps(value)}]}
    spec = ToolSpec('opaque_accept', 'Accept the selected configuration', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2)
    review = assess_action(model_client=Provider(), request='選択した条件で開始してください。', context={},
        invocation=ToolInvocation('a', spec.name, 'web:one', {'target': 'selected'}), spec=spec,
        evidence=[], retained_results=evidence, now='2028-04-02T12:00:00Z')
    return review, calls


@pytest.mark.parametrize('first', ['{', [], {'decision': 'proceed', 'issues': [], 'approval_granted': True}])
def test_one_protocol_correction_reuses_same_action_without_granting_consent(first):
    review, calls = run([first, {'decision': 'proceed', 'issues': []}])
    assert review.decision == 'proceed' and review.error is None and review.protocol_errors
    assert len(calls) == 2
    packets = [json.loads(c['messages'][0]['content'][0]['text']) for c in calls]
    assert packets[0]['proposed_action'] == packets[1]['proposed_action']
    assert packets[0]['original_request'] == packets[1]['original_request']
    assert 'protocol_correction' in packets[1] and calls[1]['timeout'] <= calls[0]['timeout']


@pytest.mark.parametrize('decision', ['proceed', 'revise'])
def test_unused_nullable_clarification_fields_are_valid(decision):
    review, calls = run([{'decision': decision, 'issues': [] if decision == 'proceed' else ['Selected target conflicts.'],
        'question': None, 'options': None}])
    assert review.decision == decision and review.error is None and len(calls) == 1


@pytest.mark.parametrize('invalid', ['{', {'decision': 'clarify', 'issues': ['Target ambiguous'], 'question': None, 'options': None}])
def test_repeated_protocol_errors_remain_closed(invalid):
    review, calls = run([invalid])
    assert review.decision == 'revise' and review.error and len(calls) == 2


def test_protocol_correction_and_retained_inspection_have_separate_bounded_allowances():
    source = ToolResult('source', 'opaque_read', 'completed', {'value': 7})
    review, calls = run(['{', {'disposition': 'inspect_evidence', 'evidence_requests': [
        {'invocation_id': 'e0000', 'json_pointer': '/value', 'offset': 0, 'length': 20}]},
        {'decision': 'proceed', 'issues': []}], [source])
    assert review.decision == 'proceed' and review.error is None and len(calls) == 3
    final = json.loads(calls[-1]['messages'][0]['content'][0]['text'])
    assert final['evidence_inspection_state'] == {'performed': True, 'may_inspect': False}
    assert final['additional_evidence']


def test_transport_failure_does_not_approve_or_retry_the_action():
    review, calls = run([TimeoutError('private-provider-detail')])
    assert review.error == 'TimeoutError' and review.decision == 'revise' and len(calls) == 1
    assert 'private-provider-detail' not in str(review)


@pytest.mark.parametrize('pointer', ['/absent', '/body/invalid'])
def test_invalid_retained_pointer_can_be_corrected_without_repeating_source(pointer):
    source = ToolResult('source', 'opaque_read', 'completed', {'body': 'Verified current record.'})
    def inspection(path):
        return {'disposition': 'inspect_evidence', 'evidence_requests': [
            {'invocation_id': 'e0000', 'json_pointer': path, 'offset': 0, 'length': 100}]}
    review, calls = run([inspection(pointer), inspection('/body'), {'decision': 'proceed', 'issues': []}], [source])
    assert review.decision == 'proceed' and review.error is None and len(calls) == 3
    assert review.protocol_errors == ('invalid_evidence_inspection',)
    final = json.loads(calls[-1]['messages'][0]['content'][0]['text'])
    assert final['additional_evidence'] and final['evidence_inspection_state']['performed']
    assert calls[-1]['timeout'] <= calls[0]['timeout']
