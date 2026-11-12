"""Approved continuation keeps native file selection through producer timeout."""
import json
import os
import time

import pytest

from tests.e2e.test_scheduled_report_completion import (
    setup_runtime, run_scheduled, telegram_boundary, text_response, tool_response,
)
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker API/SQLite/upload boundary')


@pytest.mark.parametrize('extension,missing_media', [('.html', False), ('.html', True),
    ('.pdf', False), ('.pdf', True), ('.xlsx', False), ('.xlsx', True),
    ('.csv', False), ('.png', False), ('.docx', False), ('.zip', False), ('.wav', False)])
@pytest.mark.parametrize('task', ['選択した6件の値を .html 報告書で送信。',
    'Comprueba los 6 valores seleccionados y entrega el informe .html.'])
def test_approved_run_uploads_one_existing_selected_report_after_timeout(tmp_path, monkeypatch, task, extension, missing_media, record_property):
    from fastapi.testclient import TestClient
    from nullion import crons, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifact_lifecycle import record_delivery_manifest
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
        ToolResult, _build_file_write_handler)
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'none.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_KEY_STORAGE', 'file')
    monkeypatch.setenv('NULLION_CRON_AGENT_MAX_ITERATIONS', '2')
    monkeypatch.setenv('NULLION_TELEGRAM_BOT_TOKEN', 'synthetic-token')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: None)
    runtime = setup_runtime(tmp_path, monkeypatch)
    reads, writes = [], []
    root = artifact_root_for_principal('telegram:123')
    root.mkdir(parents=True, exist_ok=True)
    path = root / ('selected' + extension)
    task = task.replace('.html', extension)
    producer_name = {'.pdf': 'pdf_create', '.xlsx': 'spreadsheet_create',
        '.png': 'image_generate', '.docx': 'document_create'}.get(extension, 'file_write')
    document = '<!doctype html><html><head><title>Selected report</title></head><body><h1>Selected report</h1><p>6 records; total 42.</p></body></html>'
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    def read(invocation):
        reads.append(invocation.arguments['record'])
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'value': 7})
    registry.register(ToolSpec('browser_extract_text', 'Read selected value', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    write = _build_file_write_handler(tmp_path, allowed_roots=[tmp_path])
    def produce(invocation):
        if extension == '.html':
            result = write(invocation)
        else:
            if extension == '.pdf':
                from pypdf import PdfWriter
                from pypdf.generic import NameObject, DictionaryObject, DecodedStreamObject
                writer = PdfWriter(); page = writer.add_blank_page(612, 792)
                font = DictionaryObject({NameObject('/Type'): NameObject('/Font'),
                    NameObject('/Subtype'): NameObject('/Type1'), NameObject('/BaseFont'): NameObject('/Helvetica')})
                page[NameObject('/Resources')] = DictionaryObject({NameObject('/Font'):
                    DictionaryObject({NameObject('/F1'): writer._add_object(font)})})
                stream = DecodedStreamObject(); stream.set_data(b'BT /F1 12 Tf 40 750 Td (6 records; total 42.) Tj ET')
                page[NameObject('/Contents')] = writer._add_object(stream); writer.write(path)
            elif extension == '.xlsx':
                from openpyxl import Workbook
                workbook = Workbook(); workbook.active.append(['records', 'total'])
                workbook.active.append([6, 42]); workbook.save(path)
            elif extension == '.png':
                from PIL import Image, ImageDraw
                image = Image.new('RGB', (320, 100), 'white')
                ImageDraw.Draw(image).text((10, 30), '6 records; total 42.', fill='black'); image.save(path)
            elif extension == '.docx':
                from docx import Document
                doc = Document(); doc.add_paragraph('6 records; total 42.'); doc.save(path)
            elif extension == '.zip':
                from zipfile import ZipFile
                with ZipFile(path, 'w') as archive: archive.writestr('report.txt', '6 records; total 42.')
            elif extension == '.wav':
                import wave
                with wave.open(str(path), 'wb') as audio:
                    audio.setnchannels(1); audio.setsampwidth(2); audio.setframerate(8000)
                    audio.writeframes(bytes(1600))
            else:
                path.write_text('records,total\n6,42\n')
            result = ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {
                'path': str(path), 'artifact_paths': [str(path)], 'bytes_written': path.stat().st_size})
        assert result.status == 'completed'
        if missing_media:
            result.output['required_embedded_media_extensions'] = [extension]
        writes.append(result)
        # Native adapters can return the final selected deliverable receipt.
        # The real selector/checkpointer must keep that ownership on inspection.
        record_delivery_manifest([result], [str(path)], candidate_paths=[str(path)])
        return result
    registry.register(ToolSpec(producer_name, 'Produce the selected report', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 3), produce)
    class Provider:
        def create(self, **kwargs):
            system = kwargs.get('system') or ''
            if 'semantic completion reviewer' in system:
                if writes:
                    raise TimeoutError('External final reviewer unavailable')
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                evidence = [row['invocation_id'] for row in packet['tool_evidence']
                    if row['tool_name'] == 'browser_extract_text']
                return text_response(json.dumps({'disposition': 'retry',
                    'all_material_requirements_enumerated': True,
                    'unresolved_requirements': ['Read the remaining selected records and produce the report.'],
                    'retry_tool_names': ['browser_extract_text'],
                    'material_requirements': [{'requirement_id': row['requirement_id'],
                        'satisfied': False, 'draft_covered': False,
                        'evidence_invocation_ids': evidence} for row in packet['material_requirement_contract']]}))
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [extension], 'material_requirements': [
                        {'requirement': 'Inspect six selected records', 'requirement_kind': 'fact',
                         'evidence_subject_id': 'values', 'eligible_evidence_tool_names': ['browser_extract_text']},
                        {'requirement': 'Deliver the selected HTML report', 'requirement_kind': 'deliverable',
                         'completion_owner': 'cron_delivery', 'evidence_subject_id': 'report',
                         'eligible_evidence_tool_names': []}]}))
            protocol = workflow_provider_reply(kwargs, completion=False)
            if protocol is not None:
                return protocol
            if len(reads) < 6:
                return tool_response('browser_extract_text', {'record': len(reads)})
            if not writes:
                return tool_response(producer_name, {'path': str(path), 'output_path': str(path),
                    'prompt': '6 records; total 42.', 'content': document})
            raise TimeoutError('External producer timed out after completing its selected file')
    provider = Provider()
    deliveries, send = telegram_boundary(monkeypatch)
    import telegram as telegram_sdk
    class NativeMediaBot(telegram_sdk.Bot):
        async def send_photo(self, chat_id, photo, caption=None, **kwargs):
            return await self.send_document(chat_id, photo, caption=caption, **kwargs)
        async def send_audio(self, chat_id, audio, caption=None, **kwargs):
            return await self.send_document(chat_id, audio, caption=caption, **kwargs)
        async def send_voice(self, chat_id, voice, caption=None, **kwargs):
            return await self.send_document(chat_id, voice, caption=caption, **kwargs)
    monkeypatch.setattr(telegram_sdk, 'Bot', NativeMediaBot)
    job = crons.CronJob(id='native-budget-report', name='選択した報告書', task=task,
        schedule='0 9 * * *', delivery_channel='telegram', delivery_target='123',
        artifact_delivery_options={'requires_attachment_delivery': True, 'required_artifact_extensions': [extension]})
    crons.save_crons([job])
    first = run_scheduled(job, provider, runtime, registry, lambda *args: False)
    record_property('native_diagnostic',json.dumps(first,default=str))
    assert first.get('suspended_for_approval'), {k:first.get(k) for k in ('reason','text','response_fulfilled','work_usage','cron_remaining_work','cron_recovery_tool_names','artifact_delivery_required','artifact_delivery_satisfied','cron_source_verification_feedback')}
    assert reads == list(range(4)) and not writes
    runtime = bootstrap_persistent_runtime(runtime.checkpoint_path)
    app = web_app.create_app(runtime, AgentOrchestrator(model_client=provider), registry)
    with TestClient(app) as client:
        response = client.post('/api/approve/' + first['approval_id'], json={'mode': 'limit_2x'})
        assert response.status_code == 200, response.text
        deadline = time.monotonic() + 15
        while not deliveries and time.monotonic() < deadline:
            time.sleep(.02)
        files = [row for row in deliveries if 'bytes' in row]
        assert len(files) == 1, json.dumps({'deliveries': deliveries, 'read_count': len(reads), 'write_count': len(writes),
            'runs': [{key: row.get(key) for key in ('reason', 'report_failed', 'response_fulfilled', 'tool_results')}
                for row in runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.cron_run', limit=2)]}, default=str)
        # The real file adapter may publish a managed HTML copy. Verify the
        # exact selected upload bytes, not the adapter's pre-publication input.
        from pathlib import Path
        assert files[0]['bytes'] == Path(files[0]['path']).read_bytes()
        assert Path(files[0]['path']).suffix == extension
        if extension == '.html':
            assert b'6 records; total 42.' in files[0]['bytes']
        assert reads == list(range(6)) and len(writes) == 1
        assert 'final versions' not in str(deliveries)
        visible = ' '.join(str(row.get('text') or row.get('caption') or '') for row in deliveries).casefold()
        assert not any(phrase in visible for phrase in (
            "couldn't attach", 'could not attach', 'could not create the required report file',
            'the report has not been delivered', 'needs correction', 'draft needs'))
        persisted = bootstrap_persistent_runtime(runtime.checkpoint_path)
        rows = persisted.store.list_recent_conversation_events('telegram:123',
            event_type='conversation.cron_run', limit=5)
        # Every requested native embed remains a content gap across resume.
        # Uploading a usable file does not discharge the producer's requirement.
        resumed = [row for row in rows if any(tool.get('tool_name') == producer_name
            and tool.get('status') == 'completed' for tool in row.get('tool_results') or ())]
        assert len(resumed) == 1, rows
        assert resumed[0]['report_partial'] is missing_media
        assert resumed[0].get('source_verification_satisfied') is not True
        if not missing_media:
            assert resumed[0]['response_fulfilled'] is True
            assert not resumed[0].get('outcome_review_status')
