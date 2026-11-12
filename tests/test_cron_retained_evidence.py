"""Long research must not silently erase later checks during report recovery."""
import json
from types import SimpleNamespace

import pytest

from nullion.cron_delivery import _cron_final_repair_tool_evidence, _repair_cron_agent_final_text_if_needed
from nullion.tools import ToolResult


def long_evidence():
    messages = [ToolResult(f'message-{i}', 'email_read', 'completed', {'message': {
        'id': f'{i:016x}', 'subject': f'通知 {i}', 'body': f'Obligation {i}: ' + 'supporting content ' * 800}})
        for i in range(70)]
    messages.append(ToolResult('late-check', 'calendar_list', 'completed', {'events': [
        {'id': f'event-{i}', 'summary': f'予定 {i}', 'start': {'date': '2028-11-14'}} for i in range(90)]}))
    return messages


def test_every_check_identity_survives_account_budget_before_details_are_allocated():
    source = long_evidence()
    encoded = _cron_final_repair_tool_evidence(source)
    records = json.loads(encoded)
    assert {r['invocation_id'] for r in records} == {r.invocation_id for r in source}
    assert len(encoded) <= 32_000
    assert next(r for r in records if r['invocation_id'] == 'late-check')['status'] == 'completed'
    assert any(r.get('content_truncated_for_prompt') for r in records)


@pytest.mark.parametrize('malformed', [None, 'foreign', 'pointer', 'repeat', 'canonical'])
def test_final_repair_inspects_retained_check_with_bounded_protocol_correction(malformed):
    source = long_evidence()
    state = {'text': 'Preview', 'response_presentation': {'kind': 'source_preview'}, 'tool_results': source}
    events = []
    class Model:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            prompt = kwargs['messages'][0]['content'][0]['text']
            records, _ = json.JSONDecoder().raw_decode(
                prompt.split('Verified compact tool evidence from this run:\n')[1].lstrip())
            selected = next(row['evidence_id'] for row in records if row.get('invocation_id') == 'late-check')
            if self.calls == 1 and malformed in {'foreign', 'pointer'}:
                return {'content': [{'type': 'text', 'text': json.dumps({'disposition': 'inspect_evidence',
                    'evidence_requests': [{'invocation_id': 'foreign' if malformed == 'foreign' else selected,
                        'json_pointer': '/missing' if malformed == 'pointer' else '/events/45'}]})}]}
            if 'Additional retained evidence:' not in prompt or malformed == 'repeat':
                return {'content': [{'type': 'text', 'text': json.dumps({'disposition': 'inspect_evidence',
                    'evidence_requests': [{'invocation_id': 'late-check' if malformed == 'canonical' else selected, 'json_pointer': '/events/45'}]})}]}
            assert 'event-45' in prompt and '予定 45' in prompt
            return {'content': [{'type': 'text', 'text': json.dumps({'requirements_satisfied': True,
                'source_state': 'available', 'evidence_refs': [selected],
                'report': 'The existing November 14 event was checked; no duplicate was created.'})}]}
    model = Model()
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='確認', task='既存の予定を確認して報告。'),
        state, model_client=model, record_preflight=lambda stage, **kw: events.append((stage, kw)))
    if malformed == 'repeat':
        assert model.calls == 2 and state['cron_agent_final_repair_failure'] == 'invalid_evidence_inspection'
        assert state['response_fulfilled'] is False
    else:
        assert state['cron_agent_final_repaired'] and state['response_fulfilled'] is True
        assert model.calls == (3 if malformed in {'foreign', 'pointer'} else 2)
        assert sum(stage == 'final_repair_evidence_inspected' for stage, _ in events) == 1
    if malformed in {'foreign', 'pointer'}:
        rejected = [details for stage, details in events if stage == 'final_repair_inspection_rejected']
        assert len(rejected) == 1 and rejected[0]['error_type'] == 'ValueError'
        assert rejected[0]['unavailable_reference_count'] == (1 if malformed == 'foreign' else 0)
        assert 'event-45' not in json.dumps(rejected)
    # Inspector accesses only retained values; original outputs are unchanged.
    assert len(source[-1].output['events']) == 90


def test_successful_repair_clears_only_its_stale_failure_flags():
    state={'text':'Preview','response_presentation':{'kind':'source_preview'},
        'tool_results':[ToolResult('read','web_fetch','completed',{'text':'Verified value 37.'})],
        'cron_agent_final_repair_failed':True,'cron_agent_final_repair_failure':'old_failure',
        'cron_completion_required':True,'cron_remaining_work':['Old gap'],
        'cron_unfinished_findings':'Old draft','cron_source_verification_satisfied':False}
    class Model:
        def create(self,**kw):
            return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':True,
                'source_state':'available','evidence_refs':['e0'],'report':'The verified value is 37.'})}]}
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='Report',task='Report the current value.'),state,model_client=Model())
    assert state['cron_agent_final_repaired'] and state['response_fulfilled'] is True
    assert not state.get('cron_agent_final_repair_failed') and not state.get('cron_completion_required')
    assert not state.get('cron_remaining_work')
    # Report composition never overrides an independent source-verification gate.
    assert state['cron_source_verification_satisfied'] is False


def test_canonical_alias_never_exposes_an_internal_result_outside_visible_inventory():
    state={'text':'Preview','response_presentation':{'kind':'source_preview'},'tool_results':[
        ToolResult('visible-source','web_fetch','completed',{'text':'Verified value 37.'}),
        ToolResult('hidden-current','skill_pack_read','completed',{'text':'PRIVATE_INTERNAL_SENTINEL'})]}
    class Model:
        calls=0
        def create(self,**kw):
            self.calls+=1;prompt=kw['messages'][0]['content'][0]['text']
            assert 'PRIVATE_INTERNAL_SENTINEL' not in prompt and 'hidden-current' not in prompt
            return {'content':[{'type':'text','text':json.dumps({'disposition':'inspect_evidence',
                'evidence_requests':[{'invocation_id':'hidden-current','json_pointer':''}]})}]}
    model=Model();_repair_cron_agent_final_text_if_needed(SimpleNamespace(name='Report',task='確認。'),state,model_client=model)
    assert model.calls==2 and state['cron_agent_final_repair_failure']=='invalid_evidence_inspection'
    assert state['response_fulfilled'] is False
