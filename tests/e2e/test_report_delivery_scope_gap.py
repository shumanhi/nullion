"""Unsupported scheduler destinations require a real connector delivery receipt."""
import json
import os
from types import SimpleNamespace

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('task', [
    'Email the prepared report to reader@example.test.',
    '作成済みのレポートを reader@example.test にメールで送ってください。',
])
@pytest.mark.parametrize('delivery', ['accepted', 'approval', 'rejected'])
def test_saved_report_uses_connector_for_non_scheduler_destination(tmp_path, monkeypatch, task, delivery):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.crons import CronJob
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, _email_message_for_invocation
    from tests.e2e.test_cron_execution_contract_delivery import setup, response
    runtime, registry, _ = setup(tmp_path, monkeypatch)
    accepted, attempts, preflight = [], [], []
    report = 'Prepared report: the selected item is due November 7, 2026 (期限).'
    def send(invocation):
        message, paths = _email_message_for_invocation(invocation)
        assert not paths
        wire = message.as_bytes()
        attempts.append(wire)
        if delivery == 'rejected':
            return ToolResult(invocation.invocation_id,invocation.tool_name,'failed',
                {'reason':'transport_rejected','executed':False},'The mail transport rejected the report before acceptance.')
        accepted.append(wire)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{
            'message_id':'accepted-report','status':'sent',
            'action_receipt':{'type':'action_receipt','action':'send','object_type':'email',
                'summary':'Report delivered to reader@example.test.','id':'accepted-report'}})
    registry.register(ToolSpec('report_send','Email the prepared report using the connected account',
        ToolRiskLevel.HIGH,ToolSideEffectClass.ACCOUNT_WRITE,delivery=='approval',5,
        capability_tags=('connector','records')),send)
    class Model:
        scope_calls = 0
        steps = 0
        def create(self, **kwargs):
            auxiliary = workflow_provider_reply(kwargs,completion=True,action=True)
            if auxiliary is not None: return auxiliary
            if not kwargs.get('tools'):
                packet_text = kwargs['messages'][0]['content'][0]['text']
                if 'available_structured_write_tools' in packet_text:
                    packet = json.loads(packet_text)
                    self.scope_calls += 1
                    assert 'email' not in packet['scheduler_delivery_channels']
                    assert 'report_send' in {row['name'] for row in packet['available_structured_write_tools']}
                    return response({'requires_account_connector':True,
                        'provider_ids':['skill_pack_connector_records'],'confidence':1,
                        'write_tool_names':['report_send'],'delivery_mode':'report_each_run'})
                return response({'disposition':'complete','reason':'Native report transport result retained.',
                    'requirements_satisfied':delivery=='accepted','source_state':'available',
                    'evidence_refs':['e0','e1'],'delivery_decision':'send','delivery_reason':'report',
                    'remaining_work':[] if delivery=='accepted' else ['Report transport was rejected.'],
                    'report':report+' Report delivered to reader@example.test.'
                    if delivery=='accepted' else 'The mail transport rejected the report before acceptance.'})
            assert 'report_send' in {row['name'] for row in kwargs['tools']}
            self.steps += 1
            if self.steps == 1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read-report',
                    'name':'records_scan','input':{}}]}
            if self.steps == 2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'mail-report',
                    'name':'report_send','input':{'to':['reader@example.test'],'subject':'Prepared report',
                        'body':report,'html_body':'<html><body><p>'+report+'</p></body></html>'}}]}
            return response('Report delivery was rejected.' if delivery=='rejected' else report+' Report delivered to reader@example.test.')
    model = Model()
    result = run_single_agent_cron_turn(CronJob(id='mail-report',name='報告',task=task,schedule='0 * * * *'),
        'web:fixture',label='Scheduled task',orchestrator=AgentOrchestrator(model_client=model),
        runtime=runtime,settings=SimpleNamespace(),tool_registry=registry,model_client=model,
        record_event=lambda *args,**kwargs:preflight.append(kwargs))
    assert model.scope_calls == 1
    writes = [row for row in result['tool_results'] if row.tool_name=='report_send']
    assert len(writes)==1, result
    if delivery=='approval':
        assert result['suspended_for_approval'] is True and not attempts
        assert runtime.store.get_approval_request(result['approval_id']) is not None
    elif delivery=='rejected':
        assert len(attempts)==1 and not accepted
        assert writes[0].status=='failed'
        assert result.get('response_fulfilled') is False or result.get('cron_report_failed') is True
    else:
        from email.parser import BytesParser
        from email.policy import default
        assert len(accepted)==1 and writes[0].status=='completed'
        message = BytesParser(policy=default).parsebytes(accepted[0])
        assert str(message['To'])=='reader@example.test'
        assert report in message.get_body(preferencelist=('plain',)).get_content()
        assert report in message.get_body(preferencelist=('html',)).get_content()
        assert not result.get('cron_report_failed') and 'reader@example.test' in result['text'], {
            key:value for key,value in result.items() if key not in {'tool_results','artifacts'}}
