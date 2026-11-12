"""Real settings, registry, chat, SQLite and image-download boundaries."""
import io
import json
import os
import shlex
import sys
from datetime import UTC, datetime

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('prompt', ['画像を .png ファイルで作成して。', 'Genera una imagen y entrega el archivo .png.'])
def test_media_setting_enable_rebind_disable_reaches_real_chat_and_download(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL, BoundaryPolicyRule, BoundaryKind, PolicyMode
    from nullion.tools import ToolRegistry, register_media_plugin
    runtime = setup_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'.env'))
    monkeypatch.setenv('NULLION_KEY_STORAGE', 'local')
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED', 'false')
    monkeypatch.setenv('NULLION_ENABLED_PLUGINS', 'media_plugin')
    monkeypatch.setenv('NULLION_PROVIDER_BINDINGS', 'media_plugin=local_media_provider')
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_media_plugin(registry)
    runtime.active_tool_registry = registry
    runtime.store.add_permission_grant(create_permission_grant(approval_id='image-permission',
        principal_id=OPERATOR_PERMISSION_PRINCIPAL, permission='tool:image_generate',
        granted_by='fixture', granted_at=datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('image-files', OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    # This command is the external generator boundary. Registration, policy,
    # execution, artifact validation and download use the actual app code.
    generator = tmp_path/'generator.py'
    generator.write_text('import sys\nfrom PIL import Image, ImageDraw\n'
        'im=Image.new("RGB",(320,320),sys.argv[2]);ImageDraw.Draw(im).rectangle((20,20,80,80),fill="white");im.save(sys.argv[1])\n')
    def command(color):
        return f'{shlex.quote(sys.executable)} {shlex.quote(str(generator))} {{output}} {color}'

    class Provider:
        output = tmp_path/'one.png'
        generated = False
        def create(self, **kw):
            if 'material-requirements planner' in (kw.get('system') or ''):
                return text_response(json.dumps({'all_material_requirements_enumerated':True,
                    'requested_artifact_extensions':['.png'],'material_requirements':[{
                        'requirement':'Deliver the requested generated PNG image',
                        'requirement_kind':'deliverable','evidence_subject_id':'generated-image',
                        'eligible_evidence_tool_names':['image_generate']}]}))
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None:
                return protocol
            names = {t['name'] for t in kw.get('tools', [])}
            try:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
            except (KeyError, IndexError, TypeError, ValueError):
                packet = None
            if isinstance(packet, dict) and packet.get('surface') == 'ordinary_chat':
                return text_response(json.dumps({'requested_tool_names': ['image_generate'],
                    'required_tool_names': ['image_generate'], 'requested_artifact_extensions': ['.png'], 'confidence': 1}))
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': '.png', 'confidence': 1})
            if 'image_generate' not in names and 'request_tool_scope' in names:
                return tool_response('request_tool_scope', {'capabilities': ['image_generation'],
                    'tool_names': ['image_generate'], 'artifact_extensions': ['.png']})
            if 'image_generate' in names and not self.generated:
                self.generated = True
                return tool_response('image_generate', {'prompt': '風景', 'output_path': str(self.output)})
            if not names:
                # An optional classifier request cannot execute a tool or
                # consume the producer's generation state.
                return text_response('{}')
            return text_response('画像を作成しました。 '+str(self.output))
    from nullion.artifacts import ensure_artifact_root
    artifact_root = ensure_artifact_root(runtime)
    provider = Provider()
    with TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=provider), registry)) as client:
        assert 'image_generate' not in {s.name for s in registry.list_specs()}
        for index, color in enumerate(['navy', 'green']):
            response = client.post('/api/config', json={'image_generate_enabled': True,
                'image_generate_command': command(color)})
            assert response.status_code == 200 and response.json()['ok'], response.text
            assert not response.json().get('media_reload_error'), response.text
            assert 'image_generate' in {s.name for s in registry.list_specs()}
            provider.output = artifact_root/f'image-{index}.png'; provider.generated = False
            cid = f'web:media-after-settings-{index}'
            result = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
            assert result.status_code == 200, result.text
            body = result.json(); assert len(body.get('artifacts', [])) == 1, body
            artifact = body['artifacts'][0]
            download = client.get(artifact['url'])
            assert download.status_code == 200 and download.content == provider.output.read_bytes()
            with Image.open(io.BytesIO(download.content)) as image:
                assert image.size == (320, 320) and image.getpixel((0, 0)) == Image.new('RGB', (1, 1), color).getpixel((0, 0))
            turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
            assert any(r['tool_name'] == 'image_generate' and r['status'] == 'completed' for r in turn['tool_results'])
        failed_generator = tmp_path/'failed-generator.py'
        failed_generator.write_text('import sys\nsys.stderr.write("Provider rejected generation")\nsys.exit(1)\n')
        failed = client.post('/api/config', json={'image_generate_command':
            f'{shlex.quote(sys.executable)} {shlex.quote(str(failed_generator))} {{output}}'})
        assert failed.status_code == 200 and failed.json()['ok']
        provider.output = artifact_root/'failed.png'; provider.generated = False
        cid = 'web:configured-generator-rejection'
        rejected = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
        assert rejected.status_code == 200 and not rejected.json().get('artifacts'), rejected.text
        turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
        assert turn.get('response_fulfilled') is not True
        assert any(r['tool_name'] == 'image_generate' and r['status'] == 'failed' for r in turn['tool_results'])
        assert not provider.output.exists() and not provider.output.with_suffix('.svg').exists()
        import urllib.request
        import urllib.error
        real_urlopen = urllib.request.urlopen
        def payment_required(request, *args, **kwargs):
            url = getattr(request, 'full_url', str(request))
            if url.startswith('https://generativelanguage.googleapis.com/'):
                raise urllib.error.HTTPError(url, 402, 'Payment required', {},
                    io.BytesIO(b'{"error":{"message":"PRIVATE payment payload token=secret"}}'))
            return real_urlopen(request, *args, **kwargs)
        monkeypatch.setattr(urllib.request, 'urlopen', payment_required)
        monkeypatch.setenv('NULLION_MEDIA_GEMINI_API_KEY', 'fixture-key')
        saved = client.post('/api/config', json={'image_generate_command':'',
            'image_generate_provider':'gemini', 'image_generate_model':'gemini-2.5-flash-image',
            'media_models':{'gemini':[{'model':'gemini-2.5-flash-image','capabilities':['image_output']}]}})
        assert saved.status_code == 200 and saved.json()['ok'], saved.text
        provider.output = artifact_root/'payment-rejected.png'; provider.generated = False
        rejected = client.post('/api/chat', json={'text':prompt, 'conversation_id':'web:image-payment'})
        assert rejected.status_code == 200 and not rejected.json().get('artifacts'), rejected.text
        payment_turn = runtime.store.list_recent_conversation_events('web:image-payment', event_type='conversation.chat_turn')[-1]
        assert '402' in rejected.json()['text'] and 'credits' in rejected.json()['text'].lower(), json.dumps({
            'reply':rejected.json(), 'turn':payment_turn},ensure_ascii=False)
        assert 'PRIVATE' not in rejected.json()['text'] and 'token=secret' not in rejected.json()['text']
        assert not provider.output.exists() and not provider.output.with_suffix('.svg').exists()
        restored = client.post('/api/config', json={'image_generate_command':command('green')})
        assert restored.status_code == 200 and restored.json()['ok']
        provider.output = artifact_root/'recovered.png'; provider.generated = False
        recovered = client.post('/api/chat', json={'text':prompt,'conversation_id':'web:image-payment'})
        recovered_turn = runtime.store.list_recent_conversation_events('web:image-payment', event_type='conversation.chat_turn')[-1]
        assert recovered.status_code == 200 and len(recovered.json().get('artifacts',[])) == 1, json.dumps({
            'reply':recovered.json(),'turn':recovered_turn,'file_exists':provider.output.exists()},ensure_ascii=False)
        assert '402' not in recovered.json()['text']
        downloaded = client.get(recovered.json()['artifacts'][0]['url'])
        assert downloaded.status_code == 200 and downloaded.content == provider.output.read_bytes()
        disabled = client.post('/api/config', json={'image_generate_enabled': False})
        assert disabled.status_code == 200 and disabled.json()['ok']
        assert 'image_generate' not in {s.name for s in registry.list_specs()}
        assert client.get('/api/config').json()['image_generate_enabled'] is False


@pytest.mark.parametrize('channel', ['telegram', 'slack', 'discord'])
def test_saved_media_change_refreshes_the_existing_messaging_registry(tmp_path, monkeypatch, channel):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import ChatOperatorService
    from nullion.tools import ToolInvocation, ToolRegistry, register_media_plugin
    runtime = setup_runtime(tmp_path, monkeypatch)
    env_path = tmp_path/'.env'; env_path.write_text('NULLION_IMAGE_GENERATE_ENABLED=false\n'); env_path.chmod(0o600)
    for key, value in {'NULLION_ENV_FILE':str(env_path), 'NULLION_IMAGE_GENERATE_ENABLED':'false',
        'NULLION_KEY_STORAGE':'local', 'NULLION_ENABLED_PLUGINS':'media_plugin',
        'NULLION_PROVIDER_BINDINGS':'media_plugin=local_media_provider',
        'NULLION_TELEGRAM_CHAT_ENABLED':'false','NULLION_SLACK_ENABLED':'false','NULLION_DISCORD_ENABLED':'false',
        'NULLION_CHAT_STARTUP_WARMUP':'0'}.items():
        monkeypatch.setenv(key,value)
    web_registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,)); register_media_plugin(web_registry)
    messaging_registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,)); register_media_plugin(messaging_registry)
    messaging_runtime = bootstrap_persistent_runtime(tmp_path/'messaging.db')
    messaging_runtime.active_tool_registry = messaging_registry
    service = ChatOperatorService(messaging_runtime, '', None, load_settings(env_path=env_path), chat_channels=(channel,))
    generator = tmp_path/'generator.py'
    generator.write_text('import sys\nfrom PIL import Image\nImage.new("RGB",(320,320),"navy").save(sys.argv[1])\n')
    command = f'{shlex.quote(sys.executable)} {shlex.quote(str(generator))} {{output}}'
    with TestClient(web_app.create_app(runtime, None, web_registry)) as client:
        saved = client.post('/api/config',json={'image_generate_enabled':True,'image_generate_command':command})
        assert saved.status_code == 200 and saved.json()['ok'], saved.text
        assert 'image_generate' not in {s.name for s in messaging_registry.list_specs()}
        assert service.refresh_live_configuration() is True
        assert 'image_generate' in {s.name for s in messaging_registry.list_specs()}
        assert service.refresh_live_configuration() is False
        output = tmp_path/(channel+'.png')
        result = messaging_registry.invoke(ToolInvocation(invocation_id='image-after-refresh', tool_name='image_generate',
            principal_id='web:operator', arguments={'prompt':'風景','output_path':str(output)}))
        assert result.status == 'completed',result
        with Image.open(output) as image:
            assert image.size == (320,320)
        saved = client.post('/api/config',json={'image_generate_enabled':False})
        assert saved.status_code == 200 and saved.json()['ok']
        assert service.refresh_live_configuration() is True
        assert 'image_generate' not in {s.name for s in messaging_registry.list_specs()}
