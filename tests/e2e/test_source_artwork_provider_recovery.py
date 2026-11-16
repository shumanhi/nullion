"""A collected source image replaces failed optional illustration inside DOCX."""
import io
import json
import os
import time
import urllib.error
import zipfile

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('failure_kind',['provider','source'])
@pytest.mark.parametrize('stop_at_budget',[False,True])
@pytest.mark.parametrize('mode', ['off', 'on'])
def test_unavailable_internal_illustration_is_replaced_by_native_source_image(tmp_path, monkeypatch, mode, record_property, stop_at_budget, failure_kind, request):
    from datetime import UTC, datetime
    from PIL import Image
    from fastapi.testclient import TestClient
    from nullion import tools as native_tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
        register_media_plugin, register_browser_image_collect_tool, _build_document_create_handler)
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED', 'true')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('files', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS, PolicyMode.ALLOW, str(tmp_path)+'/*', 'fixture', datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('network', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    buffer = io.BytesIO()
    Image.new('RGB', (120, 90), 'navy').save(buffer, format='PNG')
    image_bytes = buffer.getvalue()
    fetched = []
    def fetch(url, timeout):
        fetched.append(url)
        return {'data': image_bytes, 'content_type': 'image/png'}
    monkeypatch.setattr(native_tools, '_fetch_browser_image_binary', fetch)
    generated = []
    def generate(*args, **kwargs):
        generated.append(1)
        raise RuntimeError('Payment refused') from urllib.error.HTTPError('https://provider.invalid', 402, 'Payment required', {}, None)
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    register_media_plugin(registry, image_generator=generate)
    register_browser_image_collect_tool(registry)
    if failure_kind=='source':
        import threading
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        from nullion.plugins.browser_plugin import register_browser_tools
        from nullion.preferences import save_preferences
        monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright');monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
        monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false');save_preferences({'outbound_request_mode':'allow_all'})
        class Page(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers();self.wfile.write(b'<main><article><h2><a href="/record-detail">Verified source</a></h2><p>Total $37.00</p><img width="120" height="80" alt="Verified source" src="https://source.example/cover.jpg"></article></main>')
            def log_message(self,*args):pass
        source=ThreadingHTTPServer(('127.0.0.1',0),Page);threading.Thread(target=source.serve_forever,daemon=True).start()
        request.addfinalizer(lambda:(registry.run_cleanup_hooks(),source.shutdown(),source.server_close()))
        register_browser_tools(registry)
        source_url=f'http://localhost:{source.server_port}/record'
    registry.register(ToolSpec('document_create', 'Create document', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE,
        False, 20, produced_artifact_extensions=('.docx',)), _build_document_create_handler(tmp_path, allowed_roots=[tmp_path]))
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
            if 'select_attachment_format' in {r['name'] for r in kw.get('tools', [])}:
                return call('select_attachment_format', {'extension': '.docx', 'confidence': 1})
            if not kw.get('tools'):
                return text('{}')
            self.phase += 1
            if self.phase in (1, 3):
                names = (['image_generate', 'document_create']+(['browser_read_pages'] if failure_kind=='source' else [])) if self.phase == 1 else ['browser_image_collect', 'document_create']
                result = call('request_tool_scope', {'capabilities': ['image_generation', 'local_files', 'web'],
                    'tool_names': names, 'required_tool_names': names, 'artifact_extensions': ['.docx'],
                    'embedded_media_artifact_extensions': ['.docx']})
                result['content'][0]['id'] = 'scope-'+str(self.phase)
                return result
            if self.phase == 2:
                args = {'prompt': 'Optional illustration', 'output_path': str(self.root/'optional.png')}
                if failure_kind=='source':
                    read={'pages':[{'url':source_url,'read_kind':'items'}]}
                    if mode=='on':return call('code_mode',{'tool_names':['browser_read_pages','image_generate'],
                        'code':"call_tool('browser_read_pages', "+repr(read)+")\ncall_tool('image_generate', "+repr(args)+")"})
                    result=call('browser_read_pages',read);result['content'].extend(call('image_generate',args)['content']);return result
                return (call('code_mode', {'tool_names': ['image_generate'], 'code': "call_tool('image_generate', "+repr(args)+")"})
                    if mode == 'on' else call('image_generate', args))
            if self.phase == 4:
                self.collect = {'image_urls': ['https://source.example.test/artwork.png'], 'quality_profile': 'any', 'max_images': 1}
                if mode == 'on':
                    program = "image=call_tool('browser_image_collect', "+repr(self.collect)+")['output']['image_paths'][0]\n"
                    program += "call_tool('document_create', {'output_path':"+repr(str(self.target)) + ", 'title':'Verified source', 'paragraphs':['Total 37'], 'image_paths':[image]})"
                    result = call('code_mode', {'tool_names': ['browser_image_collect', 'document_create'], 'code': program})
                    result['content'][0]['id'] = 'collected-artwork-program'
                    return result
                return call('browser_image_collect', self.collect)
            if mode == 'off' and self.phase == 5:
                packets = [json.JSONDecoder().raw_decode(b['text'])[0] for m in kw['messages'] if isinstance(m.get('content'), list)
                    for part in m['content'] if isinstance(part, dict) and part.get('type') == 'tool_result'
                    and part.get('tool_use_id') == 'browser_image_collect-id'
                    for b in part.get('content', []) if b.get('type') == 'text']
                return call('document_create', {'output_path': str(self.target), 'title': 'Verified source',
                    'paragraphs': ['Total 37'], 'image_paths': [packets[-1]['output']['image_paths'][0]]})
            return text('Completed using the collected source.\nMEDIA:'+str(self.target))
    model = Model()
    samples = []
    class BoundedOrchestrator(AgentOrchestrator):
        def run_turn(self,**kwargs):
            if stop_at_budget:kwargs['max_iterations']=4 if mode=='on' else 5
            return super().run_turn(**kwargs)
    with TestClient(create_app(runtime, BoundedOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['資料を作成。挿絵が使えなければ指定ソースの画像を使用。',
            'Crea el documento; si falla la ilustración, usa la imagen de la fuente.',
            "Créez le document; en cas d'échec, utilisez l'image de la source."]):
            cid = 'web:source-artwork-'+str(index)
            model.phase = 0
            model.root = artifact_root_for_principal(cid)
            model.root.mkdir(parents=True, exist_ok=True)
            model.target = model.root/f'source-{index}.docx'
            start = time.monotonic()
            response = client.post('/api/chat', json={'conversation_id': cid, 'text': prompt})
            samples.append(time.monotonic()-start)
            assert not model.errors, model.errors
            artifacts = response.json().get('artifacts', [])
            assert len(artifacts) == 1, response.text
            with zipfile.ZipFile(io.BytesIO(client.get(artifacts[0]['url']).content)) as docx:
                members = [n for n in docx.namelist() if n.startswith('word/media/')]
                assert len(members) == 1 and docx.read(members[0]) == image_bytes
            turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True
            scopes = [r['output'] for r in turn['tool_results'] if r['tool_name'] == 'request_tool_scope']
            assert scopes[-1]['retired_unavailable_artifact_producers'] == ['image_generate']
    assert len(generated)==(0 if failure_kind=='source' else 3)
    assert len(fetched)==3
    assert sum(samples)/len(samples) < 5 and max(samples) < 8, samples
    record_property('mean_source_artwork_recovery_seconds', sum(samples)/len(samples))


@pytest.mark.parametrize('mode', ['off', 'on'])
def test_native_embedded_artwork_replaces_unavailable_intermediate_without_another_scope(tmp_path, monkeypatch, mode, record_property):
    from datetime import UTC, datetime
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
        register_media_plugin, _build_file_write_handler, _build_pdf_create_handler)
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED', 'true')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('files', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS, PolicyMode.ALLOW, str(tmp_path)+'/*', 'fixture', datetime.now(UTC)))
    generated = []
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('network', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    def generate(*args, **kwargs):
        generated.append(1)
        raise RuntimeError('Payment refused') from urllib.error.HTTPError('https://provider.invalid', 402, 'Payment required', {}, None)
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    register_media_plugin(registry, image_generator=generate)
    for name, formats, handler in [
        ('file_write', ('.html',), _build_file_write_handler(tmp_path, allowed_roots=[tmp_path])),
        ('pdf_create', ('.pdf',), _build_pdf_create_handler(tmp_path, allowed_roots=[tmp_path])),
    ]:
        registry.register(ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.WRITE,
            False, 20, produced_artifact_extensions=formats), handler)
    markup = ('<!doctype html><html><body><h1>Verified 37</h1>'
        '<svg xmlns="http://www.w3.org/2000/svg" width="200" height="120">'
        '<rect width="200" height="120" fill="navy"/><circle cx="100" cy="60" r="35" fill="gold"/>'
        '</svg><p>Original artwork and verified value 37.</p></body></html>')
    class Provider:
        phase = 0
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            names = {r['name'] for r in kw.get('tools', [])}
            if 'select_attachment_format' in names:
                return call('select_attachment_format', {'extension':None, 'confidence':1})
            if not names: return text('{}')
            self.phase += 1
            assert self.phase <= (4 if mode == 'on' else 5), 'A completed fallback re-entered recovery'
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['local_files', 'image_generation'],
                    'tool_names':['image_generate', 'file_write', 'pdf_create'],
                    'required_tool_names':['image_generate', 'file_write', 'pdf_create'],
                    'artifact_extensions':['.html', '.pdf'], 'embedded_media_artifact_extensions':['.html', '.pdf']})
            if self.phase == 2:
                args = {'prompt':'Optional original artwork', 'output_path':str(self.root/'optional.png')}
                return (call('code_mode', {'tool_names':['image_generate'], 'code':"call_tool('image_generate', "+repr(args)+")"})
                    if mode == 'on' else call('image_generate', args))
            html_args = {'path':str(self.root/'report.html'), 'content':markup}
            pdf_args = {'output_path':str(self.root/'report.pdf'), 'html_pages':[markup], 'expected_page_count':1}
            if self.phase == 3:
                if mode == 'on':
                    result = call('code_mode', {'tool_names':['file_write', 'pdf_create'],
                        'code':"call_tool('file_write', "+repr(html_args)+")\ncall_tool('pdf_create', "+repr(pdf_args)+")"})
                    result['content'][0]['id'] = 'native-embedded-artwork-program'
                    return result
                return call('file_write', html_args)
            if mode == 'off' and self.phase == 4: return call('pdf_create', pdf_args)
            return text('Completed both reports with original artwork. Verified value: 37.')
    model = Provider(); samples = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['元の挿絵を含む資料を .html と .pdf で作成', 'Crea informes .html y .pdf con ilustraciones originales']):
            cid = 'web:embedded-artwork-'+str(index); model.phase = 0
            model.root = artifact_root_for_principal(cid); model.root.mkdir(parents=True, exist_ok=True)
            start = time.monotonic(); response = client.post('/api/chat', json={'conversation_id':cid, 'text':prompt})
            samples.append(time.monotonic()-start); data = response.json()
            assert response.status_code == 200, data
            assert 'report.html' in data['text'] and 'report.pdf' in data['text'], data
            assert 'No usable result' not in data['text'] and len(data.get('artifacts', [])) == 2, data
            from nullion.response_fulfillment_contract import artifact_file_embedded_media_count
            for artifact in data['artifacts']:
                content = client.get(artifact['url']).content
                target = model.root/artifact['name']
                assert content == target.read_bytes()
                assert artifact_file_embedded_media_count(target) > 0
            turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True, json.dumps(turn, ensure_ascii=False)
            scopes = [r['output'] for r in turn['tool_results'] if r['tool_name'] == 'request_tool_scope']
            assert len(scopes) == 2, scopes
            if mode == 'on':
                assert scopes[-1]['retired_unavailable_artifact_producers'] == ['image_generate'], scopes
            else:
                assert 'image_generate' in scopes[-1]['unavailable_required_tool_names'], scopes
    assert len(generated) == 2
    assert sum(samples)/len(samples) < 10 and max(samples) < 15, samples
    record_property('mean_embedded_artwork_recovery_seconds', sum(samples)/len(samples))
