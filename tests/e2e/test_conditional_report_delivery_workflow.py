from datetime import datetime
import json
import os
from types import SimpleNamespace

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_cron_repair_runtime_contract import runtime_and_registry, text_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


def delivery(job, runtime, registry, provider):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import (CronRunDeliveryCallbacks, run_cron_delivery_workflow,
        run_single_agent_cron_turn, cron_delivery_block_reason)
    sent = []
    result = run_cron_delivery_workflow(job, label='Scheduled task', callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda job: 'telegram', delivery_target=lambda job, channel: '123',
        run_agent_turn=lambda job, conversation: run_single_agent_cron_turn(job, conversation,
            label='Scheduled task', orchestrator=AgentOrchestrator(model_client=provider), runtime=runtime,
            tool_registry=registry, settings=SimpleNamespace(enabled_skill_packs=()), model_client=provider),
        record_event=lambda *a, **kw: None, block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda *a: pytest.fail('Unexpected Web delivery'),
        send_platform_delivery=lambda *a: sent.append(a) or True))
    return result, sent


def test_daily_report_route_survives_dst_and_early_manual_run_needs_no_provider(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app, crons, workflow_validity
    runtime, registry = runtime_and_registry(tmp_path, monkeypatch)
    class Provider:
        def create(self, **kwargs):
            pytest.fail('Out-of-window runs must not spend model tokens')
    policy = dict(mode='daily', timezone='America/New_York', start_time='16:00',
                  end_time='23:59:59', weekdays=[0, 1, 2, 3, 4])
    with TestClient(web_app.create_app(runtime, orchestrator=None, registry=registry)) as client:
        reply = client.post('/api/crons', json=dict(name='終値', task='締め後に報告。',
            schedule='5 16 * * 1-5', schedule_timezone='America/New_York', execution_window=policy,
            artifact_delivery_options={'required_artifact_extensions': ['.html'], 'requires_attachment_delivery': True}))
        assert reply.status_code == 200, reply.text
        ident = reply.json()['job']['id']
        job = crons.get_cron(ident)
        assert job.execution_window == policy
        from nullion.crons import _compute_next_run
        from zoneinfo import ZoneInfo
        for before, expected in [('2026-10-30T19:00:00+00:00', '2026-10-30T20:05:00+00:00'),
                                 ('2026-11-04T20:00:00+00:00', '2026-11-04T21:05:00+00:00')]:
            actual = _compute_next_run(job.schedule, after=datetime.fromisoformat(before), tz=ZoneInfo(job.schedule_timezone))
            assert datetime.fromisoformat(actual) == datetime.fromisoformat(expected)
        monkeypatch.setattr(workflow_validity, 'current_time', lambda: datetime.fromisoformat('2026-11-04T20:00:00+00:00'))
        result, sent = delivery(job, runtime, registry, Provider())
        assert result['cron_delivery_status'] == 'silent', result
        assert sent == []
        assert crons.get_cron(ident).enabled
        assert not list(tmp_path.rglob('*.html'))


@pytest.mark.parametrize('condition', ['閉館日は何も送らない。', 'No enviar si no hay cambios.'])
@pytest.mark.parametrize('outcome', ['withhold', 'partial'])
def test_recovery_honors_evidenced_no_delivery_without_creating_placeholder(tmp_path, monkeypatch, condition, outcome):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    monkeypatch.setattr('nullion.artifacts.artifact_root_for_principal', lambda _: str(tmp_path / 'artifacts'))
    job = crons.add_cron('確認', '0 12 * * 1-5', condition + ' HTML。', artifact_delivery_options={
        'required_artifact_extensions': ['.html'], 'requires_attachment_delivery': True})
    registry = ToolRegistry()
    registry.register(ToolSpec(name='source_read', description='Read current source state',
        risk_level=ToolRiskLevel.LOW, side_effect_class=ToolSideEffectClass.READ,
        requires_approval=False, timeout_seconds=2),
        lambda call: ToolResult(call.invocation_id, call.tool_name, 'completed',
            {'source_url': 'https://example.test/status', 'closed': outcome == 'withhold',
             'changes': [] if outcome == 'withhold' else [{'title': 'Confirmed update'}]}))
    class Provider:
        agent_calls = 0
        compose_calls = 0
        def create(self, **kwargs):
            auxiliary = workflow_provider_reply(kwargs, completion=True)
            if auxiliary is not None:
                return auxiliary
            if not kwargs.get('tools'):
                payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
                self.compose_calls += 1
                contract = payload['delivery_gate_contract']
                refs = contract['response_schema']['properties']['evidence_refs']['items']['enum']
                assert 'tool_evidence:0' in refs
                if outcome == 'partial':
                    from nullion.report_completion import MARKER
                    receipt = dict(contract_id=payload['report_outcome_contract']['contract_id'],
                        status='partial', all_material_requirements_enumerated=True,
                        remaining_work=['One source check remains unavailable.'])
                    return text_response('<!doctype html><html><body>Confirmed update<!--' + MARKER
                        + json.dumps(receipt) + '--></body></html>')
                return text_response(json.dumps(dict(contract_id=contract['contract_id'],
                    delivery_decision='withhold', instruction_condition=condition, evidence_refs=['tool_evidence:0'])))
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'read', 'name': 'source_read', 'input': {}}]}
            raise TimeoutError('External producer interrupted after source check')
    provider = Provider()
    result, sent = delivery(job, runtime, registry, provider)
    assert result['cron_delivery_status'] == ('silent' if outcome == 'withhold' else 'sent'), result
    assert provider.compose_calls == 1
    if outcome == 'withhold':
        assert sent == []
        assert not list(tmp_path.rglob('*.html'))
    else:
        assert len(sent) == 1
        files = list(tmp_path.rglob('*.html'))
        assert len(files) == 1 and b'Confirmed update' in files[0].read_bytes()
    assert crons.get_cron(job.id).enabled
    from nullion.runtime_persistence import load_runtime_store
    terminal = next(row for row in load_runtime_store(runtime.checkpoint_path).list_conversation_events()
                    if row.get('event_type') == 'conversation.cron_run')
    assert terminal['response_fulfilled'] is (outcome == 'withhold')
    assert terminal['report_failed'] is False, json.dumps({'terminal': terminal,
        'result': {key: value for key, value in result.items() if key not in {'tool_results', 'messages_snapshot'}}}, default=str)
