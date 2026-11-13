"""A different editing tool cannot hide a retained report from shared review."""
import json
import os
from datetime import UTC, datetime

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('task', ['確認した値37の資料を作成してください。', 'Prepara el informe con el valor comprobado 37.'])
@pytest.mark.parametrize('extension', ['.html', '.pdf'])
def test_native_report_finishes_without_an_unrequested_cross_tool_edit(tmp_path, monkeypatch, task, extension):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_file_write_handler, _build_pdf_create_handler
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime = setup_runtime(tmp_path, monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('filesystem', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    producer = 'file_write' if extension == '.html' else 'pdf_create'
    handler = (_build_file_write_handler if extension == '.html' else _build_pdf_create_handler)(allowed_roots=(tmp_path,))
    writes = []
    def generate(inv):
        writes.append(inv.invocation_id)
        return handler(inv)
    registry.register(ToolSpec(producer, 'Create requested report', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5), generate)
    registry.register(ToolSpec('file_patch', 'Edit a local file', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        lambda inv: pytest.fail('Rejected edit executed'))
    job = crons.add_cron('Report', '* * * * *', task, enabled=False, delivery_channel='telegram', delivery_target='2222',
        artifact_delivery_options={'requires_attachment_delivery':True, 'required_artifact_extensions':[extension]})
    path = artifact_root_for_principal('telegram:2222')/('cedar'+extension)
    class Provider:
        phase = 0
        rejected = 0
        selections = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            names = {r['name'] for r in kw.get('tools') or ()}
            if 'Workflow action preflight.' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                if packet['proposed_action']['tool_name'] == 'file_patch':
                    self.rejected += 1
                    return text_response(json.dumps({'decision':'revise','issues':['La afirmación propuesta no tiene pruebas.']}))
            response = workflow_provider_reply(kw, completion=True)
            if response is not None: return response
            if names == {'select_artifact_delivery'}:
                self.selections += 1
                raw = kw['messages'][0]['content'][0]['text']
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                candidates = json.loads(raw.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1]
                    .split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0])
                return tool_response('select_artifact_delivery', {'status':'selected',
                    'selected_ids':[r['id'] for r in candidates], 'reason':'Retain the existing verified file.'})
            if not names: return text_response('{}')
            self.phase += 1
            if self.phase == 1:
                args = {'path':str(path),'content':'<!doctype html><html><body><h1>Cedar</h1><p>Value 37</p></body></html>'} if extension == '.html' else {
                    'output_path':str(path),'text_pages':['Cedar value 37'],'expected_page_count':1}
                return tool_response(producer, args)
            return text_response('Report attached.\nMEDIA:'+str(path))
    provider = Provider()
    deliveries, send = telegram_boundary(monkeypatch)
    result = run_scheduled(job, provider, runtime, registry, send)
    assert len(writes) == 1 and provider.rejected == 0, (provider.phase, provider.rejected, [(r.tool_name, r.status, r.output, r.error) for r in result['tool_results'][-3:]])
    assert provider.phase == 2 and provider.selections == 0
    assert result.get('artifact_delivery_satisfied') is True, result
    assert len(deliveries) == 1 and deliveries[0]['bytes'] == path.read_bytes(), deliveries
    if extension == '.html': assert b'Value 37' in deliveries[0]['bytes'] and b'unsupported claim' not in deliveries[0]['bytes']
    else:
        import io
        from pypdf import PdfReader
        assert '37' in PdfReader(io.BytesIO(deliveries[0]['bytes'])).pages[0].extract_text()
