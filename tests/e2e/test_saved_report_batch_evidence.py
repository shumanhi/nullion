"""Saved reports must consume all parallel source/inspection results.

The model reacts to evidence actually visible in each request. Only external
model, account-source and Telegram network boundaries are deterministic fakes.
"""
import json
import os

import pytest

from test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, text_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('prompt', ['照合済みの期限と予定表の結果を報告。', 'Informa las obligaciones y el resultado del calendario.'])
def test_saved_report_delivers_all_findings_after_parallel_inspection_and_calendar_check(tmp_path, monkeypatch, prompt):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.retained_work_context import INSPECT_TOOL
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry()
    dates = {i: f'CONFIRMED_{i}=2028-04-{10+i}' for i in range(14)}
    reads, calendars = [], []
    def read(inv):
        i = inv.arguments['index']; reads.append(i)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'record_id': i, 'body': 'a'*60_000 + dates[i] + 'z'*60_000})
    def calendar(inv):
        calendars.append(inv.arguments)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'checked': True,
            'records': [{'id': f'existing-{i}', 'date': token.split('=')[1]} for i,token in dates.items()]})
    for name, handler in [('record_read', read), ('calendar_inspect', calendar)]:
        registry.register(ToolSpec(name, 'Read an authorized source', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2), handler)
    deliveries, send = telegram_boundary(monkeypatch)
    monkeypatch.setenv('NULLION_CRON_AGENT_MAX_ITERATIONS', '8')
    job = crons.add_cron('Obligation report', '* * * * *', prompt, enabled=False,
        delivery_channel='telegram', delivery_target='2222')
    class Provider:
        rounds = 0
        inspection_batches = 0
        def create(self, **kwargs):
            system = kwargs.get('system', '')
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [
                        {'requirement': f'Report obligation {i}', 'requirement_kind': 'fact',
                         'evidence_subject_id': f'record-{i}', 'eligible_evidence_tool_names': ['record_read']}
                        for i in dates] + [
                        {'requirement': 'Check existing calendar entries', 'requirement_kind': 'action',
                         'evidence_subject_id': 'calendar', 'eligible_evidence_tool_names': ['calendar_inspect']},
                        {'requirement': 'Report all findings and calendar outcomes', 'requirement_kind': 'constraint',
                         'evidence_subject_id': 'report', 'eligible_evidence_tool_names': ['record_read']}]}))
            if 'semantic completion reviewer' in system:
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                sources = {r['output']['record_id']: r['invocation_id'] for r in packet['tool_evidence']
                    if r['tool_name'] == 'record_read' and isinstance(r.get('output'), dict)
                    and 'record_id' in r['output']}
                calendar = next((r['invocation_id'] for r in packet['tool_evidence']
                    if r['tool_name'] == 'calendar_inspect' and r.get('output', {}).get('records')), None)
                complete = all(t in packet['draft_final_response'] for t in dates.values()) and calendar is not None
                calendar_windows = [w for w in [*packet.get('previously_inspected_evidence', []),
                    *packet.get('additional_evidence', [])] if w.get('invocation_id') == calendar
                    and w.get('json_pointer') == '/records']
                if complete and not calendar_windows:
                    return text_response(json.dumps({'disposition': 'inspect_evidence', 'evidence_requests': [{
                        'invocation_id': calendar, 'json_pointer': '/records', 'offset': 0, 'length': 12_000}]}))
                if complete:
                    entries = json.loads(calendar_windows[0]['content'])
                    assert {r['date'] for r in entries} == {token.split('=')[1] for token in dates.values()}
                    inspected = json.dumps(packet.get('previously_inspected_evidence', []))
                    assert all(token in inspected for token in dates.values())
                coverage = []
                for i, row in enumerate(packet['material_requirement_contract']):
                    reference = (sources.get(i) if i < 14 else calendar if i == 14 else sources.get(0))
                    coverage.append({'requirement_id': row['requirement_id'], 'satisfied': complete,
                        'draft_covered': complete, 'evidence_invocation_ids': [reference] if reference else []})
                return text_response(json.dumps({'disposition': 'complete' if complete else 'retry',
                    'unresolved_requirements': [] if complete else ['Finish the confirmed obligation report'],
                    'retry_tool_names': [] if complete else ['record_read', 'calendar_inspect'],
                    'all_material_requirements_enumerated': True, 'material_requirements': coverage}))
            protocol = workflow_provider_reply(kwargs, completion=False)
            if protocol is not None: return protocol
            self.rounds += 1
            if self.rounds == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': f'read-{i}',
                    'name': 'record_read', 'input': {'index': i}} for i in dates]}
            # Read only current model-visible evidence, without a private list
            # of facts remembered by the fake between requests.
            visible = []
            index = {}
            checked = False
            for message in kwargs['messages']:
                if not isinstance(message.get('content'), list): continue
                for block in message['content']:
                    if block.get('type') == 'tool_result':
                        try: output = json.JSONDecoder().raw_decode(block['content'][0]['text'])[0].get('output', {})
                        except (KeyError, ValueError, IndexError): continue
                        visible.append(output)
                        checked = checked or output.get('checked') is True
                    elif block.get('type') == 'text' and block.get('text', '').startswith('Runtime evidence index.'):
                        index = json.loads(block['text'].split('\n', 1)[1])
                        visible.append(index.get('previously_inspected_evidence', []))
            encoded = json.dumps(visible)
            missing = [i for i, token in dates.items() if token not in encoded]
            if missing:
                self.inspection_batches += 1
                sources = [r for r in index.get('records', []) if r['tool_name'] == 'record_read']
                assert len(sources) == 14, 'Original owned source references must remain discoverable'
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use',
                    'id': f'inspect-{self.rounds}-{i}', 'name': INSPECT_TOOL, 'input': {'requests': [{
                        'invocation_id': sources[i]['invocation_id'], 'json_pointer': '/body',
                        'offset': 59_400, 'length': 1200}]}} for i in missing]}
            if not checked:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'calendar',
                    'name': 'calendar_inspect', 'input': {}}]}
            return text_response('Upcoming obligations\n\n' + '\n'.join(
                f'• Provider {i}: {token}; matching calendar event exists.' for i, token in dates.items())
                + '\n\nCalendar: 0 new events; 14 existing events verified.')
    provider = Provider()
    result = run_scheduled(job, provider, runtime, registry, send)
    diagnostics = [{k:e.get(k) for k in ('event_type', 'status', 'validation_errors', 'review_error', 'disposition', 'unresolved_requirements', 'response_diagnostics')}
        for e in runtime.store.list_conversation_events('telegram:2222')
        if e.get('event_type') in {'conversation.completion_review', 'conversation.completion_requirement_contract'}]
    assert result['response_fulfilled'] is True and result['cron_delivery_status'] == 'sent', (result.get('reason'), diagnostics)
    assert provider.inspection_batches == 1 and provider.rounds == 4
    assert reads == list(range(14)) and len(calendars) == 1
    assert len(deliveries) == 1
    delivered = deliveries[0]['text']
    assert all(token in delivered for token in dates.values())
    assert '14 existing events verified' in delivered
    assert 'incomplete' not in delivered and 'work limit' not in delivered
