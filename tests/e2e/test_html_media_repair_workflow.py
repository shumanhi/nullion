"""The real chat route repairs only the broken image output, then delivers both reports."""
from tests.workflow_provider_protocol import workflow_provider_reply
import base64
import io
import json
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


@pytest.mark.parametrize('prompt', ['Create the linked and embedded reports.', 'Crea los informes enlazado e integrado.', 'リンク版と画像埋め込み版を作成してください。'])
def test_html_media_repair_reuses_current_image_and_preserves_linked_sibling(tmp_path, monkeypatch, prompt):
    from PIL import Image
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_file_write_handler, _build_file_read_handler

    monkeypatch.setenv('HOME', str(tmp_path)); monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'none.env')); monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off'); chat_store._store = None
    conversation = 'web:html-media-repair'; root = artifact_root_for_principal(conversation); root.mkdir(parents=True, exist_ok=True)
    image = root/'visual.png'; linked = root/'linked.html'; embedded = root/'embedded.html'
    stream = io.BytesIO(); Image.effect_noise((160, 100), 30).convert('RGB').save(stream, format='PNG')
    encoded = base64.b64encode(stream.getvalue()).decode()
    linked_html = '<!doctype html><html><body><h1>Linked report</h1><a href="embedded.html">Embedded report</a></body></html>'
    repaired_html = f'<!doctype html><html><body><h1>Embedded report</h1><img src="{image}"></body></html>'
    class Provider:
        phase = 0; selections = 0; repairs = 0
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            system = str(kw.get('system') or ''); names = {row['name'] for row in kw.get('tools', [])}
            def text(value): return {'stop_reason':'end_turn', 'content':[{'type':'text', 'text':value}]}
            def tool(name, arguments): return {'stop_reason':'tool_use', 'content':[{'type':'tool_use', 'id':f'{name}{self.phase}', 'name':name, 'input':arguments}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True, 'material_requirements':[
                    {'requirement':f'Deliver {kind} HTML report', 'requirement_kind':'deliverable',
                     'evidence_subject_id':kind, 'eligible_evidence_tool_names':['file_write']} for kind in ('linked', 'embedded')]}))
            if system.startswith('You are a semantic completion reviewer'):
                data = json.loads(kw['messages'][0]['content'][0]['text'])
                assert len(data['artifact_evidence']) == 2
                records = [row['invocation_id'] for row in data['tool_evidence'] if row.get('output', {}).get('result_kind') == 'artifact_receipt']
                assert len(records) == 2
                return text(json.dumps({'disposition':'complete', 'all_material_requirements_enumerated':True,
                    'unresolved_requirements':[], 'material_requirements':[
                    {**row, 'satisfied':True, 'draft_covered':True, 'evidence_invocation_ids':[records[index]]}
                    for index, row in enumerate(data['material_requirement_contract'])]}))
            if 'select_artifact_delivery' in names:
                self.selections += 1
                raw = kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n', 1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END, 1)[0]
                candidates = json.loads(raw); assert len(candidates) == 2, candidates
                return tool('select_artifact_delivery', {'status':'selected', 'selected_ids':[row['candidate_id'] for row in candidates], 'reason':'Both separately requested final reports.'})
            if 'select_attachment_format' in names:
                return tool('select_attachment_format', {'extension':'.html', 'confidence':1})
            if not names: return text('{}')
            self.phase += 1
            if self.phase == 1:
                return tool('request_tool_scope', {'capabilities':['local_files'], 'tool_names':['file_write', 'file_read'],
                    'required_tool_names':['file_write'], 'artifact_extensions':['.html', '.html'],
                    'embedded_media_artifact_extensions':['.html']})
            if self.phase == 2:
                return {'stop_reason':'tool_use', 'content':[
                    {'type':'tool_use', 'id':str(index), 'name':'file_write', 'input':args} for index, args in enumerate([
                        {'path':str(image), 'content':encoded},
                        {'path':str(linked), 'content':linked_html, 'inline_local_html_images':False},
                        {'path':str(embedded), 'content':'<!doctype html><html><body><img src="data:image/svg+xml,%3Csvg/%3E"></body></html>', 'inline_local_html_images':False}])]}
            if self.phase == 4:
                context = '\n'.join(str(row) for row in kw['messages'][-2:])
                assert 'reusable_current_turn_artwork_paths' in context and str(image) in context, context
                assert 'verified_embedded_media_count' in context and 'raster' in context
                self.repairs += 1
                return tool('file_write', {'path':str(embedded), 'content':repaired_html, 'inline_local_html_images':True})
            assert self.phase in {3, 5}, self.phase
            return text('Both reports attached.\nMEDIA:'+str(linked)+'\nMEDIA:'+str(embedded))
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db'); model = Provider(); runtime.model_client = model
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_write', 'Write requested files', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 10),
        _build_file_write_handler(tmp_path, allowed_roots=[tmp_path]))
    registry.register(ToolSpec('file_read', 'Read generated files', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 10),
        _build_file_read_handler(tmp_path, allowed_roots=[tmp_path]))
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
        response = client.post('/api/chat', json={'text':prompt+' .html', 'conversation_id':conversation})
        assert response.status_code == 200, response.text
        data = response.json()
        turns = [row for row in runtime.store.list_conversation_events(conversation) if row.get('event_type') == 'conversation.chat_turn']
        assert len(data['artifacts']) == 2, json.dumps({'response':data, 'model':vars(model), 'tools':turns[-1]['tool_results']}, default=str)
        assert model.repairs == 1 and model.selections == 1 and model.phase == 5
        assert linked.read_text() == linked_html
        assert any(mime in embedded.read_text() for mime in ('data:image/png;base64,', 'data:image/jpeg;base64,'))
        assert str(image) not in embedded.read_text()
        for artifact in data['artifacts']:
            downloaded = client.get(artifact['url']); assert downloaded.status_code == 200
            assert downloaded.content == (linked if artifact['name'] == linked.name else embedded).read_bytes()
        turns = [row for row in runtime.store.list_conversation_events(conversation) if row.get('event_type') == 'conversation.chat_turn']
        writes = [row for row in turns[-1]['tool_results'] if row['tool_name'] == 'file_write']
        assert len(writes) == 4 and all(row['status'] == 'completed' for row in writes), writes
