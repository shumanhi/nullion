"""Real scheduled runner and Telegram upload retain files after review inspection."""
import json
import os
from datetime import UTC, datetime

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('task', ['確認済みの数値をHTML資料にしてください。', 'Prepara un informe HTML con los valores comprobados.'])
@pytest.mark.parametrize('failure', ['timeout', 'invalid_binding'])
def test_prepared_report_is_delivered_after_inspection_and_review_failure(tmp_path, monkeypatch, task, failure):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, _build_file_write_handler
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.FILESYSTEM_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(kind.value, GLOBAL_PERMISSION_PRINCIPAL,
            kind, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    reads = []
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'text': 'Source context. '*600+'Cedar value 37'})
    registry.register(ToolSpec('collection_read', 'Read records', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5), read)
    registry.register(ToolSpec('browser_extract_text', 'Read an additional current source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5), lambda inv: ToolResult(inv.invocation_id, inv.tool_name,
            'completed', {'text': 'Cedar value 37; additional source coverage remains incomplete.'}))
    registry.register(ToolSpec('file_write', 'Write a report', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        _build_file_write_handler(allowed_roots=(tmp_path,)))
    job = crons.add_cron('Report', '* * * * *', task, enabled=False, delivery_channel='telegram', delivery_target='2222',
        artifact_delivery_options={'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html']})
    path = artifact_root_for_principal('telegram:2222')/'cedar.html'
    document = '<!doctype html><html><body><h1>Cedar</h1><p>Value 37</p></body></html>'
    class Provider:
        phase = 0
        reviews = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                response = workflow_provider_reply(kw, completion=True)
                decision = json.loads(response['content'][0]['text'])
                # A mistaken extra format in planning cannot erase the saved
                # task's accepted HTML file when overall completion fails.
                decision['requested_artifact_extensions'] = ['.html', '.csv']
                return text_response(json.dumps(decision))
            if 'semantic completion reviewer' in system:
                self.reviews += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                if self.reviews == 1:
                    response = workflow_provider_reply(kw, completion=True)
                    decision = json.loads(response['content'][0]['text'])
                    decision.update(disposition='retry', retry_tool_names=['browser_extract_text'],
                        unresolved_requirements=['The additional source check is unfinished.'])
                    for row in decision['material_requirements']:
                        row.update(satisfied=False, draft_covered=False)
                    return text_response(json.dumps(decision))
                if self.reviews == 2:
                    ref = next(r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name'] == 'collection_read')
                    return text_response(json.dumps({'disposition':'inspect_evidence', 'evidence_requests':[
                        {'invocation_id': ref, 'json_pointer':'/text', 'offset':len('Source context. ')*600, 'length':100}]}))
                assert packet['additional_evidence'][0]['content'] == 'Cedar value 37'
                if failure == 'timeout':
                    from nullion.model_clients import ModelClientTimeoutError
                    raise ModelClientTimeoutError('External reviewer unavailable after inspection')
                response = workflow_provider_reply(kw, completion=True)
                decision = json.loads(response['content'][0]['text'])
                artifact = next(r['invocation_id'] for r in packet['tool_evidence']
                    if (r.get('output') or {}).get('artifact_record_binding'))
                for row in decision['material_requirements']:
                    row['evidence_invocation_ids'] = [artifact]
                return text_response(json.dumps(decision))
            response = workflow_provider_reply(kw, completion=True)
            if response is not None: return response
            if not kw.get('tools'): return text_response('{}')
            self.phase += 1
            if self.phase == 1: return tool_response('collection_read', {})
            if self.phase == 2: return tool_response('file_write', {'path':str(path), 'content':document})
            if self.phase == 4: return tool_response('browser_extract_text', {})
            return text_response('Report attached.')
    deliveries, send = telegram_boundary(monkeypatch)
    provider = Provider()
    result = run_scheduled(job, provider, runtime, registry, send)
    assert len(reads) == 1
    assert result.get('artifact_delivery_satisfied') is True, result
    assert result.get('response_fulfilled') is True, result
    assert result.get('cron_delivery_status') == 'sent', result
    assert len(deliveries) == 1 and deliveries[0]['bytes'] == path.read_bytes(), deliveries
    assert b'<h1>Cedar</h1><p>Value 37</p>' in deliveries[0]['bytes']
    assert 'MEDIA:' not in (deliveries[0].get('caption') or '')
    assert 'required step didn' not in (deliveries[0].get('caption') or '')
    assert provider.reviews == 0
    assert 'cron_source_verification_satisfied' not in result
    assert len([r for r in result['tool_results'] if r.tool_name == 'browser_extract_text']) == 0
    reviews = [e for e in runtime.store.list_conversation_events('telegram:2222')
        if e.get('event_type') == 'conversation.completion_review']
    assert not reviews
