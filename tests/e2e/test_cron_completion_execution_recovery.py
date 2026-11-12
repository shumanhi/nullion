"""Real scheduled runner, graph, SQLite and platform artifact delivery boundary."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, tool_response, text_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def encoded(value):
    return text_response(json.dumps(value, ensure_ascii=False))


@pytest.mark.parametrize('task', ['新しい対象が確認できた場合のみ通知。', 'Avisa solo si hay nuevos elementos confirmados.'])
@pytest.mark.parametrize('name', ['opaque_read', 'browser_extract_text'])
def test_verified_negative_check_closes_silently_without_restarting_sources(tmp_path, monkeypatch, task, name):
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(); reads = []
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'items': [], 'count': 0, 'text': 'Completed check: no qualifying records.'})
    registry.register(ToolSpec(name, 'Read selected source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        producer_calls = 0
        reviews = 0
        def create(self, **kw):
            system = kw.get('system') or ''
            if 'material-requirements planner' in system:
                return encoded({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Verify current selected records against delivery history.',
                        'completion_owner': 'producer', 'delivery_required': True, 'requirement_kind': 'fact',
                        'evidence_subject_id': 'selected', 'artifact_member_subject_ids': [],
                        'eligible_evidence_tool_names': [name]}]})
            if 'semantic completion reviewer' in system:
                self.reviews += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                reference = next(row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name'] == name)
                return encoded({'disposition': 'retry', 'all_material_requirements_enumerated': True,
                    'unresolved_requirements': ['Suppress the unnecessary draft; the completed check found no new match.'],
                    'retry_tool_names': [], 'user_facing_failure': 'Internal draft correction.',
                    'scheduled_alert_delivery': {'decision': 'silent', 'reason': 'no_verified_match'},
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': True,
                        'draft_covered': False, 'evidence_invocation_ids': [reference]}
                        for row in packet['material_requirement_contract']]})
            protocol = workflow_provider_reply(kw, completion=False)
            if protocol is not None: return protocol
            self.producer_calls += 1
            return tool_response(name, {}) if not reads else text_response('No report is ready yet.')
    provider = Provider(); deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='quiet-check', name='確認', task=task, schedule='* * * * *',
        delivery_channel='telegram', delivery_target='123',
        artifact_delivery_options={'alert_delivery_mode': 'verified_matches_only'})
    result = run_scheduled(job, provider, runtime, registry, send)
    assert result['response_fulfilled'] is True and result['execution_outcome']['execution_succeeded'] is True, result
    assert result['cron_alert_delivery_decision'] == 'silent' and not deliveries
    assert len(reads) == 1 and provider.producer_calls == 2 and provider.reviews == 1
    assert not result.get('cron_completion_continuations')
    events = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.completion_review')
    assert any(row['disposition'] == 'complete' for row in events)


@pytest.mark.parametrize('task', ['対象を確認して .html の報告書を作成。', 'Revisa los registros y crea el informe .html.'])
@pytest.mark.parametrize('recover', [True, False])
@pytest.mark.parametrize('source_mode', ['fallback', 'native', 'native_failure', 'progress', 'long_progress'])
def test_partial_file_survives_and_known_missing_work_can_finish_in_same_run(tmp_path, monkeypatch, task, recover, source_mode):
    from nullion.agent_orchestrator import AgentOrchestrator
    execution_entries = []
    resume_entries = []
    actual_run, actual_resume = AgentOrchestrator.run_turn, AgentOrchestrator.resume_turn
    def run_once(self, **kwargs):
        execution_entries.append(kwargs['conversation_id'])
        return actual_run(self, **kwargs)
    def resume_once(self, **kwargs):
        resume_entries.append(kwargs['conversation_id'])
        return actual_resume(self, **kwargs)
    monkeypatch.setattr(AgentOrchestrator, 'run_turn', run_once)
    monkeypatch.setattr(AgentOrchestrator, 'resume_turn', resume_once)
    from nullion.crons import CronJob
    from nullion.report_completion import MARKER
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,)); reads = []; writes = []
    is_native = source_mode in {'native', 'native_failure', 'native_timeout'}
    expected_records = list('ABCDEFG') if source_mode == 'long_progress' else (
        ['A', 'B', 'C'] if source_mode == 'progress' else ['A', 'B'])
    if source_mode == 'progress': monkeypatch.setenv('NULLION_CRON_AGENT_MAX_ITERATIONS', '5')
    if source_mode == 'long_progress': monkeypatch.setenv('NULLION_CRON_AGENT_MAX_ITERATIONS', '8')
    document = '<!doctype html><html><body><h1>Selected report</h1><p>A: 7</p><p>B: 8</p></body></html>'
    if source_mode == 'progress': document = document.replace('</body>', '<p>C: 8</p></body>')
    if source_mode == 'long_progress':
        document = document.replace('</body>', ''.join(f'<p>{key}: 8</p>' for key in 'CDEFG') + '</body>')
    partial_document = '<!doctype html><html><body><h1>Selected report</h1><p>A: 7; B not checked.</p></body></html>'
    from nullion.artifacts import artifact_root_for_principal
    native_path = Path(artifact_root_for_principal('telegram:123'))/'selected-report.html'
    native_path.parent.mkdir(parents=True, exist_ok=True)
    def read(inv):
        reads.append(inv.arguments['record'])
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7 if inv.arguments['record'] == 'A' else 8})
    def write(inv):
        path = Path(inv.arguments['path']); path.write_text(inv.arguments['content']); writes.append(str(path))
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'path': str(path), 'bytes_written': path.stat().st_size})
    registry.register(ToolSpec('sensor_read', 'Read selected record', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2,
        input_schema={'type': 'object', 'properties': {'record': {'type': 'string'}}, 'required': ['record']}), read)
    registry.register(ToolSpec('file_write', 'Write selected report', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2,
        input_schema={'type': 'object', 'properties': {'path': {'type': 'string'}, 'content': {'type': 'string'}},
                      'required': ['path', 'content']}), write)
    failed_discovery = []
    def optional_read(inv):
        failed_discovery.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'failed', {'reason': 'source_unavailable'}, 'Optional source unavailable')
    registry.register(ToolSpec('optional_read', 'Read additional source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), optional_read)
    class Provider:
        producer_calls = 0
        generation_calls = 0
        interruptions = 0
        native_reviewed = False
        review_snapshots = []
        def create(self, **kw):
            system = kw.get('system') or ''
            if 'material-requirements planner' in system:
                return encoded({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': ['.html'],
                    'material_requirements': [
                        {'requirement': 'Check all selected records: ' + ', '.join(expected_records), 'requirement_kind': 'fact',
                         'evidence_subject_id': 'records', 'eligible_evidence_tool_names': ['sensor_read']},
                        {'requirement': 'Prepare the updated HTML report from both records', 'requirement_kind': 'deliverable',
                         'evidence_subject_id': '.html', 'eligible_evidence_tool_names': ['file_write']}]})
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                self.native_reviewed = True
                complete = set(expected_records).issubset(reads) and bool(writes) and Path(writes[-1]).read_text() == document
                self.review_snapshots.append({'reads':list(reads),'complete':complete})
                rows = []
                for row in packet['material_requirement_contract']:
                    candidates = [record for record in packet['tool_evidence'] if record['status'] == 'completed'
                        and record['tool_name'] in row['eligible_evidence_tool_names']]
                    if row['requirement_kind'] == 'deliverable':
                        bound = [record for record in candidates if (record.get('output') or {}).get('artifact_record_binding')]
                        if bound: candidates = bound
                    rows.append({'requirement_id': row['requirement_id'], 'satisfied': complete,
                        'draft_covered': complete, 'evidence_invocation_ids': [record['invocation_id'] for record in candidates[-1:]]})
                return encoded({'disposition': 'complete' if complete else 'retry',
                    'all_material_requirements_enumerated': True, 'material_requirements': rows,
                    'unresolved_requirements': [] if complete else ['Read remaining record B and update the selected report.'],
                    'retry_tool_names': [] if complete else ['sensor_read', 'file_write'],
                    'user_facing_failure': None if complete else 'Selected record B and the final report remain incomplete.'})
            if not kw.get('tools'):
                try: packet = json.loads(kw['messages'][0]['content'][0]['text'])
                except (ValueError, KeyError, TypeError): packet = {}
                if 'report_outcome_contract' in packet:
                    self.generation_calls += 1
                    metadata = {'contract_id': packet['report_outcome_contract']['contract_id'], 'status': 'partial',
                        'all_material_requirements_enumerated': True, 'remaining_work': ['Read selected record B and update this report.'],
                        'retry_tool_names': ['sensor_read', 'file_write']}
                    return text_response('<!doctype html><html><body><h1>Selected report</h1><p>A: 7; B not checked.</p><!--'
                        + MARKER + json.dumps(metadata) + '--></body></html>')
            protocol = workflow_provider_reply(kw)
            if protocol is not None: return protocol
            self.producer_calls += 1
            if source_mode == 'native_failure' and not failed_discovery: return tool_response('optional_read', {})
            if not reads: return tool_response('sensor_read', {'record': 'A'})
            if is_native and not self.native_reviewed:
                if source_mode == 'native_timeout' and len(reads) < len(expected_records):
                    return tool_response('sensor_read', {'record': expected_records[len(reads)]})
                if not writes: return tool_response('file_write', {'path': str(native_path),
                    'content': document if source_mode == 'native_timeout' else partial_document})
                if source_mode == 'native_timeout':
                    raise TimeoutError('External producer interrupted after completing the native file')
                return text_response('Report attached.')
            if self.generation_calls == 0 and not is_native: raise TimeoutError('synthetic initial provider failure')
            # Continuation receives the same known record and selected file.
            if not recover: raise TimeoutError('synthetic persistent provider failure')
            if source_mode == 'progress' and reads == ['A', 'B'] and self.interruptions < 2:
                self.interruptions += 1
                raise TimeoutError('synthetic interruption after useful progress')
            if len(reads) < len(expected_records): return tool_response('sensor_read', {'record': expected_records[len(reads)]})
            if not writes or Path(writes[-1]).read_text() != document:
                paths = list(tmp_path.rglob('*.html'))
                if not paths:
                    from nullion.artifacts import artifact_root_for_principal
                    paths = list(Path(artifact_root_for_principal('telegram:123')).glob('*.html'))
                assert len(paths) == 1
                return tool_response('file_write', {'path': str(paths[0]), 'content': document})
            return text_response('Report attached.')
    provider = Provider(); deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='remaining-records', name='Selected report', task=task, schedule='* * * * *',
        delivery_channel='telegram', delivery_target='123', artifact_delivery_options={
            'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html'], 'alert_delivery_mode': 'report_each_run'})
    result = run_scheduled(job, provider, runtime, registry, send)
    files = [row for row in deliveries if 'bytes' in row]
    assert len(files) == 1, (result, deliveries)
    assert provider.generation_calls == (0 if is_native else 1), 'Do not create duplicate fallback report files'
    assert result.get('cron_shared_completion_owner') is True, result
    assert execution_entries == ['telegram:123'] and resume_entries == []
    assert result['response_fulfilled'] is recover, result
    assert result['execution_outcome']['status'] == ('succeeded' if recover else 'partially_succeeded')
    assert result['execution_outcome']['completion_verified'] is recover
    if recover:
        assert 'Partial report attached' not in result['text']
        assert provider.review_snapshots and provider.review_snapshots[-1] == {'reads':expected_records,'complete':True}
        assert result['cron_source_verification_satisfied'] is True
        assert result['cron_report_content_receipt']['status'] == 'complete'
        assert reads == expected_records and len(writes) == (2 if is_native and source_mode != 'native_timeout' else 1)
        assert b'<p>B: 8</p>' in files[0]['bytes'] and b'B not checked' not in files[0]['bytes']
        assert files[0]['bytes'] == Path(files[0]['path']).read_bytes()
        if source_mode == 'progress': assert provider.producer_calls <= 10, 'Recovery enlarged the previous combined producer budget'
    else:
        assert reads == ['A'] and len(writes) == (1 if is_native else 0)
        assert b'B not checked' in files[0]['bytes']
        assert result['cron_report_content_receipt']['status'] == 'partial'


@pytest.mark.parametrize('task', ['対象を確認して .html の報告書を作成。', 'Revisa los registros y crea el informe .html.'])
def test_completed_native_report_can_be_verified_after_producer_timeout_without_rewriting(tmp_path, monkeypatch, task):
    test_partial_file_survives_and_known_missing_work_can_finish_in_same_run(
        tmp_path, monkeypatch, task, True, 'native_timeout')


def test_inline_contract_protocol_repair_keeps_completed_source_and_delivers_answer(tmp_path, monkeypatch):
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch); registry = ToolRegistry(); reads = []
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7})
    registry.register(ToolSpec('sensor_read', 'Read selected observation', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        plans = 0
        def create(self, **kw):
            if 'material-requirements planner' in (kw.get('system') or ''):
                self.plans += 1
                return encoded({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Report selected observation', 'completion_owner': 'producer',
                        'delivery_required': True, 'requirement_kind': 'deliverable' if self.plans == 1 else 'fact',
                        'evidence_subject_id': 'selected', 'artifact_member_subject_ids': [], 'eligible_evidence_tool_names': ['sensor_read']}]})
            protocol = workflow_provider_reply(kw)
            if protocol is not None: return protocol
            return tool_response('sensor_read', {}) if not reads else text_response('Selected observation: 7.')
    provider = Provider(); deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='inline', name='確認', task='選択した観測値を回答してください。', schedule='* * * * *',
        delivery_channel='telegram', delivery_target='123')
    result = run_scheduled(job, provider, runtime, registry, send)
    assert result['response_fulfilled'] is True and provider.plans == 2 and len(reads) == 1, result
    assert len(deliveries) == 1 and '7' in deliveries[0]['text'] and 'bytes' not in deliveries[0]


def test_action_inspection_pointer_repair_preserves_source_and_executes_once(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch); registry = ToolRegistry(); reads = []; writes = []
    selected = tmp_path/'selected.json'
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7})
    def update(inv):
        writes.append(inv)
        selected.write_text(json.dumps(inv.arguments))
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7,
            'action_receipt': {'type': 'action_receipt', 'object_id': 'selected'}})
    registry.register(ToolSpec('opaque_read', 'Read selected record', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), read)
    registry.register(ToolSpec('opaque_update', 'Update selected record', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2), update)
    class Provider:
        preflights = 0
        producer = 0
        def create(self, **kw):
            system = kw.get('system') or ''
            if 'Workflow action preflight.' in system:
                self.preflights += 1
                if self.preflights < 3:
                    return encoded({'disposition': 'inspect_evidence', 'evidence_requests': [{
                        'invocation_id': 'e0000', 'json_pointer': '/absent' if self.preflights == 1 else '/value',
                        'offset': 0, 'length': 100}]})
                return encoded({'decision': 'proceed', 'issues': []})
            protocol = workflow_provider_reply(kw, action=True)
            if protocol is not None: return protocol
            self.producer += 1
            if not reads: return tool_response('opaque_read', {})
            if not writes: return tool_response('opaque_update', {'value': 7})
            return text_response('Selected record updated: 7.')
    provider = Provider()
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:action-pointer',
        principal_id='web:action-pointer', user_message='確認した値で選択された記録を更新してください。',
        conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=6)
    assert result.response_fulfilled is True and '7' in result.final_text
    assert len(reads) == len(writes) == 1 and json.loads(selected.read_text()) == {'value': 7}
    assert provider.preflights == 3
    reviews = runtime.store.list_recent_conversation_events('web:action-pointer', event_type='conversation.action_review')
    assert any(row['decision'] == 'proceed' for row in reviews)


def test_cancelled_native_report_never_enters_delivery_content_review(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    path = Path(artifact_root_for_principal('telegram:123'))/'selected.html'
    path.parent.mkdir(parents=True, exist_ok=True)
    cancelled = {'value': False}; writes = []
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    def write(inv):
        writes.append(inv)
        path.write_text('<!doctype html><html><body><p>Selected result</p></body></html>')
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'path': str(path), 'bytes_written': path.stat().st_size})
    registry.register(ToolSpec('file_write', 'Write selected HTML', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2,
        input_schema={'type': 'object', 'properties': {'path': {'type': 'string'}, 'content': {'type': 'string'}},
            'required': ['path', 'content']}), write)
    class Provider:
        after_cancel_calls = 0
        def create(self, **kw):
            if cancelled['value']: self.after_cancel_calls += 1
            protocol = workflow_provider_reply(kw)
            if protocol is not None: return protocol
            return tool_response('file_write', {'path': str(path), 'content':
                '<!doctype html><html><body><p>Selected result</p></body></html>'}) if not writes else text_response('Report attached.')
    @contextmanager
    def cancellation_boundary():
        yield
        # A real external cancellation arrives after the producer returns,
        # before the delegated content owner would start another model call.
        cancelled['value'] = True
    provider = Provider()
    job = CronJob(id='cancel-handoff', name='確認', task='選択された結果を .html に保存。', schedule='* * * * *',
        artifact_delivery_options={'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html']})
    result = run_single_agent_cron_turn(job, 'telegram:123', label='Scheduled task',
        orchestrator=AgentOrchestrator(model_client=provider), runtime=runtime, tool_registry=registry,
        settings=SimpleNamespace(enabled_skill_packs=()), cancellation_checker=lambda: cancelled['value'],
        turn_guard=cancellation_boundary)
    assert len(writes) == 1 and path.exists()
    assert result['cron_run_cancelled'] is True and result['response_fulfilled'] is False
    assert provider.after_cancel_calls == 0
