"""A real terminal can exit zero while its PDF needs repair before upload."""
import io
import json
import os
import shlex
import sys
import time
import zipfile

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('mode', ['off', 'on'])
def test_native_pdf_and_archive_are_repaired_before_real_download(tmp_path, monkeypatch, mode, record_property):
    from datetime import UTC, datetime
    from fastapi.testclient import TestClient
    from pypdf import PdfReader
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
        _build_terminal_exec_handler, _build_pdf_create_handler, _build_archive_create_handler)
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('files', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS, PolicyMode.ALLOW, str(tmp_path)+'/*', 'fixture', datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    receipts = []
    for name, builder in [('terminal_exec', _build_terminal_exec_handler),
                          ('pdf_create', _build_pdf_create_handler), ('archive_create', _build_archive_create_handler)]:
        handler = builder(tmp_path, allowed_roots=[tmp_path])
        def run(inv, handler=handler):
            result = handler(inv)
            receipts.append(result)
            return result
        registry.register(ToolSpec(name, 'Create selected output', ToolRiskLevel.LOW,
            ToolSideEffectClass.WRITE, False, 20), run)
    class Model:
        phase = 0
        errors = []
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None:
                return protocol
            try:
                return self.respond(**kw)
            except Exception as exc:
                self.errors.append(repr(exc))
                raise
        def respond(self, **kw):
            names = {row['name'] for row in kw.get('tools', [])}
            if 'select_attachment_format' in names:
                return call('select_attachment_format', {'extension': None, 'confidence': 1})
            if not kw.get('tools'):
                return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['local_files', 'local_shell'],
                    'tool_names': ['terminal_exec', 'pdf_create', 'archive_create'],
                    'required_tool_names': ['terminal_exec', 'pdf_create', 'archive_create'],
                    'artifact_extensions': ['.pdf', '.zip']})
            if self.phase == 2:
                return (call('code_mode', {'tool_names': ['terminal_exec'],
                    'code': "call_tool('terminal_exec', "+repr(self.command)+")"})
                    if mode == 'on' else call('terminal_exec', self.command))
            if self.phase == 3:
                bad = receipts[-1]
                assert bad.status == 'completed' and bad.output['exit_code'] == 0
                assert len(bad.output['artifact_validation']['issues']) == 2
                assert 'artifact_validation' in json.dumps(kw['messages'])
                if mode == 'on':
                    response = call('code_mode', {'tool_names': ['pdf_create', 'archive_create'],
                        'code': "call_tool('pdf_create', "+repr(self.pdf)+")\ncall_tool('archive_create', "+repr(self.archive)+")"})
                    response['content'][0]['id'] = 'repair-pdf-and-archive'
                    return response
                return call('pdf_create', self.pdf)
            if mode == 'off' and self.phase == 4:
                return call('archive_create', self.archive)
            return text('Completed.\nMEDIA:'+self.pdf['output_path']+'\nMEDIA:'+self.archive['output_path'])
    model = Model()
    samples = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['PDF と ZIP を作成してください。', 'Crea el PDF y el ZIP.', 'Créez le PDF et le ZIP.']):
            cid = 'web:generated-pdf-repair-'+str(index)
            root = artifact_root_for_principal(cid)
            root.mkdir(parents=True, exist_ok=True)
            pdf, package = root/f'output-{index}.pdf', root/f'output-{index}.zip'
            script = ("from pypdf import PdfWriter;from pypdf.generic import NameObject,DictionaryObject;import zipfile;"
                "w=PdfWriter();p=w.add_blank_page(200,200);p[NameObject('/Contents')]=w._add_object(DictionaryObject());"
                "w.write("+repr(str(pdf))+");z=zipfile.ZipFile("+repr(str(package))+",'w');z.write("+repr(str(pdf))+",'report.pdf');z.close()")
            model.phase = 0
            model.command = {'command': shlex.quote(sys.executable)+' -c '+shlex.quote(script), 'output_paths': [str(pdf), str(package)]}
            model.pdf = {'output_path': str(pdf), 'text_pages': ['Verified total 37'], 'title': 'Verified report'}
            model.archive = {'output_path': str(package), 'source_paths': [str(pdf)]}
            started = time.monotonic()
            response = client.post('/api/chat', json={'conversation_id': cid, 'text': prompt})
            samples.append(time.monotonic()-started)
            assert not model.errors, model.errors
            assert response.status_code == 200, response.text
            artifacts = response.json().get('artifacts', [])
            assert {row['name'] for row in artifacts} == {pdf.name, package.name}, response.text
            for row in artifacts:
                data = client.get(row['url']).content
                if row['name'].endswith('.zip'):
                    with zipfile.ZipFile(io.BytesIO(data)) as archive:
                        data = archive.read(pdf.name)
                assert 'Verified total 37' in PdfReader(io.BytesIO(data)).pages[0].extract_text()
            turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True
    assert len(receipts) == 9 and all(row.status == 'completed' for row in receipts)
    assert sum(samples)/len(samples) < 8 and max(samples) < 12, samples
    record_property('mean_pdf_repair_seconds', sum(samples)/len(samples))
