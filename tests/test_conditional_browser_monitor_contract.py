import json
from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import _completion_review_required, _completion_review_original_request
from nullion.cron_delivery import (_apply_cron_alert_delivery_policy, CronRunDeliveryCallbacks,
    run_cron_delivery_workflow, cron_delivery_block_reason, manual_cron_silent_delivery_text)
from nullion.tools import ToolResult


def job(task='条件一致だけ通知。', mode='verified_matches_only'):
    return SimpleNamespace(id='monitor', name='Monitor', task=task, artifact_delivery_options={'alert_delivery_mode':mode})


@pytest.mark.parametrize('task', ['条件一致だけ通知。', 'Solo coincidencias verificadas.', 'Uniquement les résultats vérifiés.'])
@pytest.mark.parametrize('manual', [False, True])
def test_failed_matches_only_run_stays_failed_without_automatic_alert(task, manual):
    state={'text':'Unverified date and checkout.', 'cron_report_failed':True,
           'cron_failure_notice':True, 'response_fulfilled':False, 'tool_results':[], 'artifacts':[]}
    events=[];sent=[]
    callbacks=CronRunDeliveryCallbacks(effective_channel=lambda _: 'telegram', delivery_target=lambda *_:'83',
        run_agent_turn=lambda *_:state, record_event=lambda kind,*a,**kw:events.append(kind),
        block_reason=cron_delivery_block_reason, save_web_delivery=lambda *a:False,
        send_platform_delivery=lambda *a:sent.append(a[-1]) or True,
        silent_delivery_text=manual_cron_silent_delivery_text if manual else None)
    result=run_cron_delivery_workflow(job(task),label='Run',callbacks=callbacks)
    assert result['cron_run_failed'] and not result['execution_outcome']['execution_succeeded']
    assert result['cron_alert_delivery_reason']=='insufficient_evidence'
    assert result['cron_delivery_status']==('sent' if manual else 'silent')
    assert len(sent)==(1 if manual else 0)
    assert not sent or 'Unverified date and checkout.' in sent[0]
    assert events[-1]==('cron.delivery.sent' if manual else 'cron.delivery.silent')


@pytest.mark.parametrize('cause', ['cron_report_failed','cron_run_failed','cron_delivery_failed'])
def test_strict_alert_policy_cannot_be_bypassed_by_typed_failure(cause):
    result={cause:True,'response_fulfilled':False,'text':'Internal failure'}
    _apply_cron_alert_delivery_policy(job(),result,model_client=None)
    assert result['cron_alert_delivery_decision']=='silent'
    assert result['cron_alert_delivery_reason']=='insufficient_evidence'
    assert result['response_fulfilled'] is False


@pytest.mark.parametrize('reply', ['{}', '{"decision":"send","reason":"insufficient_evidence"}',
                                  '{"decision":"silent","reason":"insufficient_evidence"}'])
def test_unknown_alert_decision_never_becomes_a_verified_negative(reply):
    class Model:
        def create(self, **kwargs):return {'content':[{'type':'text','text':reply}]}
    result={'text':'No current match confirmed.', 'tool_results':[ToolResult('read','browser_extract_text','completed',{'text':'Date options only.'})]}
    _apply_cron_alert_delivery_policy(job(), result, model_client=Model())
    assert result['cron_alert_delivery_decision']=='silent'
    assert result['cron_alert_delivery_reason']=='insufficient_evidence'
    assert result['cron_report_failed'] and result['response_fulfilled'] is False


def test_browser_monitor_requires_semantic_review_even_when_all_tools_complete():
    results=[ToolResult('read','browser_extract_text','completed',{'text':'2026-10-16 exists as an option.'})]
    state={'tool_flow_context':{'scheduled_task_run':True,'scheduled_alert_delivery_mode':'verified_matches_only',
                               'scheduled_task_request':'指定日の条件を確認。'},'user_message':'Runtime wrapper'}
    assert _completion_review_required(state,tool_results=results)
    assert _completion_review_original_request(state)=='指定日の条件を確認。'
    assert not _completion_review_required({'user_message':'同じ質問。'},tool_results=results)
    assert not _completion_review_required(state,tool_results=[])
    state['tool_flow_context']['scheduled_alert_delivery_mode']='report_each_run'
    assert not _completion_review_required(state,tool_results=results)


def test_presentation_repair_cannot_override_unverified_source_scope():
    result={'text':'Readable partial findings','response_fulfilled':True,
            'cron_source_verification_required':True,'cron_source_verification_satisfied':False}
    _apply_cron_alert_delivery_policy(job(),result,model_client=None)
    assert result['cron_alert_delivery_reason']=='insufficient_evidence'
    assert result['response_fulfilled'] is False


def test_material_contract_reuses_typed_catalog_and_invalidates_on_new_source():
    from nullion.agent_orchestrator import _extract_completion_requirement_contract
    from nullion.tools import ToolRegistry
    class Model:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            # Simulate a reasoning provider needing more than the old 8-second budget.
            if kwargs['timeout'] < 12 or kwargs['max_tokens'] < 1500:
                raise TimeoutError('insufficient completion budget')
            payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
            names = [row['name'] for row in payload['completed_current_turn_tools']]
            return {'content': [{'type': 'text', 'text': json.dumps({
                'all_material_requirements_enumerated': True,
                'material_requirements': [{'requirement': '指定日の条件を確認。', 'requirement_kind': 'fact',
                    'evidence_subject_id': 'condition', 'eligible_evidence_tool_names': names}]})}]}
    model = Model()
    state = {'orchestrator': SimpleNamespace(model_client=model), 'tool_registry': ToolRegistry(),
             'user_message': '指定日の条件を確認。', 'tool_flow_context': {
                 'scheduled_task_run': True, 'scheduled_alert_delivery_mode': 'verified_matches_only'}}
    results = [ToolResult('read', 'browser_extract_text', 'completed', {'text': 'selected date'})]
    first = _extract_completion_requirement_contract(state, tool_results=results)
    assert first
    assert _extract_completion_requirement_contract(state, tool_results=iter(results)) == first
    assert model.calls == 1
    results.append(ToolResult('detail', 'browser_extract_detail', 'completed', {'text': 'seat state'}))
    assert _extract_completion_requirement_contract(state, tool_results=results)
    assert model.calls == 2
    state['user_message'] = '別の日付を確認。'
    assert _extract_completion_requirement_contract(state, tool_results=results)
    assert model.calls == 3


@pytest.mark.parametrize('task', ['指定日の条件を確認。', 'Verifica la condición de la fecha.'])
def test_scheduler_delivery_ownership_is_a_typed_phase_not_a_missing_source_receipt(task):
    from nullion.agent_orchestrator import _parse_completion_requirement_contract
    rows = [{'requirement': task, 'requirement_kind': 'fact', 'completion_owner': 'producer',
             'evidence_subject_id': 'scope', 'eligible_evidence_tool_names': ['browser_extract_text']},
            {'requirement': 'Conditional delivery', 'requirement_kind': 'action',
             'completion_owner': 'cron_delivery', 'evidence_subject_id': 'delivery',
             'eligible_evidence_tool_names': []}]
    reply = {'content': [{'type': 'text', 'text': json.dumps({
        'all_material_requirements_enumerated': True, 'material_requirements': rows})}]}
    assert not _parse_completion_requirement_contract(reply, registered_tool_names={'browser_extract_text'})
    parsed = _parse_completion_requirement_contract(reply, registered_tool_names={'browser_extract_text'}, scheduler_owns_delivery=True)
    assert len(parsed) == 1 and parsed[0].requirement == task
    rows[1]['requirement_kind'] = 'fact'
    reply['content'][0]['text'] = json.dumps({'all_material_requirements_enumerated': True, 'material_requirements': rows})
    assert not _parse_completion_requirement_contract(reply, scheduler_owns_delivery=True)


def test_material_contract_preserves_constraints_in_long_requirement():
    from nullion.agent_orchestrator import _parse_completion_requirement_contract
    requirement = '対象の制約。' * 80 + '2026-10-16; venue Z; IMAX; working purchase flow.'
    reply = {'content': [{'type': 'text', 'text': json.dumps({
        'all_material_requirements_enumerated': True, 'material_requirements': [{
            'requirement': requirement, 'requirement_kind': 'fact', 'evidence_subject_id': 'scope',
            'eligible_evidence_tool_names': ['browser_extract_text']}]})}]}
    parsed = _parse_completion_requirement_contract(reply, registered_tool_names={'browser_extract_text'})
    assert parsed[0].requirement == requirement


def test_manual_matches_only_failure_withholds_legacy_partial_findings():
    result = {'text': 'Long partial report and generic retry footer', 'cron_report_failed': True,
              'response_fulfilled': False, 'cron_partial_report_text': 'Source blocked; target-date seats unverified.'}
    sent = []
    callbacks = CronRunDeliveryCallbacks(effective_channel=lambda _: 'telegram', delivery_target=lambda *_: '83',
        run_agent_turn=lambda *_: result, record_event=lambda *a, **kw: None,
        block_reason=cron_delivery_block_reason, save_web_delivery=lambda *a: False,
        send_platform_delivery=lambda *a: sent.append(a[-1]) or True,
        silent_delivery_text=manual_cron_silent_delivery_text)
    outcome = run_cron_delivery_workflow(job(), label='Run', callbacks=callbacks)
    assert len(sent) == 1
    assert 'Source blocked; target-date seats unverified.' not in sent[0]
    assert 'did not produce a complete report' in sent[0]
    assert 'generic retry footer' not in sent[0]
    assert outcome['execution_outcome']['execution_succeeded'] is False
