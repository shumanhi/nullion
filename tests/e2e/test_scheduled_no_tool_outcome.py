"""Saved work must be verified even when the producer initially does nothing."""
import json
import os
from datetime import UTC, datetime

import pytest

from test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('task', ['選択済みの記録を確認して報告。', 'Consulta los registros seleccionados y entrega el resultado.'])
@pytest.mark.parametrize('authorized', [True, False])
def test_saved_source_work_cannot_finish_with_an_unsupported_selection_question(tmp_path, monkeypatch, task, authorized):
    from nullion import crons, cron_execution_tools
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    # Only external connection discovery/provider IO is scripted. Runtime
    # scoping, review, SQLite, invocation and delivery remain real.
    monkeypatch.setattr(cron_execution_tools, '_connected_connector_provider_summaries',
        lambda principal: ({'provider_id':'selected-provider','permission_mode':'read','active_app_ids':['records']},))
    for kind in (BoundaryKind.ACCOUNT_ACCESS, BoundaryKind.OUTBOUND_NETWORK):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id=kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=kind, mode=PolicyMode.ALLOW,
            selector='*', created_by='fixture', created_at=datetime.now(UTC)))
    reads = []; registry = ToolRegistry()
    def read(invocation):
        reads.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'value':37})
    registry.register(ToolSpec('collection_read', 'Read selected connected records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('connector','records')), read)
    class Model:
        producer = 0
        plans = 0
        reviews = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            if 'requires_account_connector' in system:
                return text_response(json.dumps({'requires_account_connector':authorized,
                    'provider_ids':['selected-provider'] if authorized else [],'confidence':1,
                    'write_tool_names':[], 'delivery_mode':'report_each_run'}))
            if system.startswith('You are a material-requirements planner'):
                self.plans += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                names = [r['name'] for r in packet['current_turn_tool_catalog']]
                assert ('collection_read' in names) is authorized
                return text_response(json.dumps({'all_material_requirements_enumerated':True,
                    'material_requirements':[{'requirement':'Verify selected records','requirement_kind':'fact',
                        'evidence_subject_id':'records','eligible_evidence_tool_names':['collection_read'] if authorized else []}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                scope = packet['required_source_and_action_contract']
                assert scope['connector_source_user_requested'] is authorized
                assert scope['source_evidence_verified'] is authorized
                matches = [r for r in packet['tool_evidence'] if r['tool_name']=='collection_read' and r['status']=='completed']
                complete = bool(matches)
                value = {'disposition':'complete' if complete else 'retry' if authorized else 'needs_user_input',
                    'all_material_requirements_enumerated':True,'unresolved_requirements':[] if complete else ['Read the selected source' if authorized else 'Select a source'],
                    'retry_tool_names':['collection_read'] if authorized and not complete else [],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':complete,
                        'draft_covered':complete,'evidence_invocation_ids':[m['invocation_id'] for m in matches[-1:]]}
                        for r in packet['material_requirement_contract']]}
                if not authorized:value['clarification']={'question':'Which source?','options':['First source','Second source']}
                return text_response(json.dumps(value))
            protocol = workflow_provider_reply(kw, completion=False)
            if protocol is not None:return protocol
            self.producer += 1
            if self.producer == 1:return text_response('Choose the source before I can continue.')
            if self.producer == 2 and authorized:return tool_response('collection_read', {})
            return text_response('Verified record: 37.')
    job = crons.add_cron('記録','* * * * *',task,enabled=False,delivery_channel='telegram',delivery_target='2222')
    model = Model(); deliveries, send = telegram_boundary(monkeypatch)
    result = run_scheduled(job, model, runtime, registry, send)
    assert model.plans == 1 and model.reviews >= 1, result
    assert len(deliveries) == 1
    if authorized:
        assert len(reads)==1 and model.producer==3, (model.producer, result)
        assert result['response_fulfilled'] is True and result['execution_outcome']['execution_succeeded'] is True
        assert '37' in deliveries[0]['text']
    else:
        assert not reads and model.producer==1
        assert result['response_fulfilled'] is not True and result['execution_outcome']['execution_succeeded'] is False
        # The editor supplied no evidence for these invented sources. It may
        # not turn that opinion into a new source-selection gate.
        assert 'First source' not in deliveries[0]['text'] and 'Second source' not in deliveries[0]['text']
        assert 'Verified record: 37' not in deliveries[0]['text']


@pytest.mark.parametrize('task', ['短い挨拶を一つ。', 'Envía un saludo breve.'])
def test_scheduled_reasoned_answer_can_complete_without_invoking_tools(tmp_path, monkeypatch, task):
    from nullion import crons
    from nullion.tools import ToolRegistry
    runtime = setup_runtime(tmp_path, monkeypatch)
    class Model:
        producer = 0
        reviews = 0
        def create(self, **kw):
            if str(kw.get('system','')).startswith('You are a semantic completion reviewer'):self.reviews += 1
            protocol = workflow_provider_reply(kw, completion=True, reasoned=True)
            if protocol is not None:return protocol
            self.producer += 1
            return text_response('こんにちは。')
    job = crons.add_cron('挨拶','* * * * *',task,enabled=False,delivery_channel='telegram',delivery_target='2222')
    model=Model(); deliveries,send=telegram_boundary(monkeypatch)
    result=run_scheduled(job,model,runtime,ToolRegistry(),send)
    assert result['response_fulfilled'] is True and result['execution_outcome']['execution_succeeded'] is True
    assert not result['tool_results'] and model.producer==1 and model.reviews==1
    assert len(deliveries)==1 and 'こんにちは。' in deliveries[0]['text']
