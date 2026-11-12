"""A real browser layout check of the generated HTML cannot restart generation."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


@pytest.mark.parametrize('prompt', ['Create and inspect the HTML report.', 'Crea y revisa el informe HTML.', 'HTMLレポートを作成して表示を確認してください。'])
@pytest.mark.parametrize('inspection_tool', ['browser_run_js', 'browser_extract_text', 'browser_extract_items'])
def test_generated_html_browser_inspection_is_not_new_source_research(tmp_path, monkeypatch, prompt, inspection_tool):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator, _stale_artifact_extensions_after_new_evidence
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass, _build_file_write_handler
    from nullion.plugins.browser_plugin import register_browser_tools
    monkeypatch.setenv('HOME', str(tmp_path)); monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'none.env')); monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off'); monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true'); chat_store._store = None
    conversation = 'web:generated-html-inspection'; root = artifact_root_for_principal(conversation); root.mkdir(parents=True, exist_ok=True)
    path = root/'report.html'; markup = '<!doctype html><html><head><title>Approved report</title></head><body><article><h1>Approved report</h1><p>Value 31</p><a href="#details">Read details</a></article></body></html>'
    class Provider:
        phase = 0; reviews = 0
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            system = str(kw.get('system') or ''); names = {row['name'] for row in kw.get('tools', [])}
            def text(value): return {'stop_reason':'end_turn', 'content':[{'type':'text', 'text':value}]}
            def tool(name, args): return {'stop_reason':'tool_use', 'content':[{'type':'tool_use', 'id':str(self.phase), 'name':name, 'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True, 'material_requirements':[
                    {'requirement':'Deliver the inspected HTML with Value 31', 'requirement_kind':'deliverable',
                     'evidence_subject_id':'report', 'eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews += 1; data = json.loads(kw['messages'][0]['content'][0]['text'])
                record = next(row for row in data['tool_evidence'] if row.get('output', {}).get('result_kind') == 'artifact_receipt')
                return text(json.dumps({'disposition':'complete', 'all_material_requirements_enumerated':True,
                    'material_requirements':[{**row, 'satisfied':True, 'draft_covered':True,
                        'evidence_invocation_ids':[record['invocation_id']]} for row in data['material_requirement_contract']]}))
            if 'select_attachment_format' in names: return tool('select_attachment_format', {'extension':'.html', 'confidence':1})
            if not names: return text('{}')
            self.phase += 1
            if self.phase == 1: return tool('request_tool_scope', {'capabilities':['local_files', 'web'],
                'tool_names':['file_write', 'browser_navigate', inspection_tool], 'required_tool_names':['file_write', inspection_tool],
                'artifact_extensions':['.html']})
            if self.phase == 2: return tool('file_write', {'path':str(path), 'content':markup})
            if self.phase == 3: return tool('browser_navigate', {'url':path.as_uri()})
            if self.phase == 4: return tool(inspection_tool, {
                'browser_run_js':{'script':'() => ({heading: document.querySelector("h1").textContent, overflow: document.documentElement.scrollWidth > innerWidth})'},
                'browser_extract_text':{}, 'browser_extract_items':{'selector':'body'}}[inspection_tool])
            assert self.phase == 5, 'Generated-file inspection restarted the producer loop'
            return text('The inspected report is attached.\nMEDIA:'+str(path))
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db'); model = Provider(); runtime.model_client = model
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_write', 'Write HTML', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 10),
        _build_file_write_handler(tmp_path, allowed_roots=[tmp_path]))
    register_browser_tools(registry)
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
        response = client.post('/api/chat', json={'text':prompt+' .html', 'conversation_id':conversation})
        assert response.status_code == 200, response.text
        data = response.json(); assert len(data['artifacts']) == 1 and model.phase == 5, data
        assert client.get(data['artifacts'][0]['url']).content == markup.encode()
        turn = next(row for row in reversed(runtime.store.list_conversation_events(conversation)) if row.get('event_type') == 'conversation.chat_turn')
        writes = [row for row in turn['tool_results'] if row['tool_name'] == 'file_write']; assert len(writes) == 1
        inspection = next(row for row in turn['tool_results'] if row['tool_name'] == inspection_tool)
        assert inspection['status'] == 'completed' and 'Approved report' in json.dumps(inspection['output']), inspection
        assert inspection['output']['source_document_path'] == str(path), inspection
        assert inspection['output']['page_url'] == path.as_uri(), inspection
        results = [ToolResult(str(i), row['tool_name'], row['status'], row.get('output', {})) for i, row in enumerate(turn['tool_results'])]
        assert not _stale_artifact_extensions_after_new_evidence(results, required_extensions=['.html'])
