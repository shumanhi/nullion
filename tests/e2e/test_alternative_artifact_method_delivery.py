"""Real scope, producer, SQLite and download/upload survive a method change."""
import asyncio
import os
from pathlib import Path
import shlex

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('extension', ['.png', '.pdf'])
@pytest.mark.parametrize('prompt', ['Create a visual page showing value 37.', '値37を示すページを作成して渡してください。'])
def test_alternative_native_producer_delivers_once_without_obsolete_method_failure(tmp_path, monkeypatch, surface, extension, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
        _build_terminal_exec_handler, _build_file_write_handler, _build_file_read_handler, _build_pdf_create_handler)
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', '0')
    monkeypatch.setenv('NULLION_TERMINAL_ENABLED', 'true')
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    for spec, handler in [
        (ToolSpec('file_write', 'Write text files', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 10,
            produced_artifact_extensions=('.html', '.csv')),
            _build_file_write_handler(tmp_path, allowed_roots=[tmp_path])),
        (ToolSpec('terminal_exec', 'Generate a local artifact', ToolRiskLevel.HIGH, ToolSideEffectClass.DANGEROUS_EXEC, False, 20),
            _build_terminal_exec_handler(tmp_path, allowed_roots=[tmp_path])),
        (ToolSpec('file_read', 'Inspect output', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 10),
            _build_file_read_handler(tmp_path, allowed_roots=[tmp_path])),
        (ToolSpec('pdf_create', 'Create PDF', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 10,
            produced_artifact_extensions=('.pdf',)), _build_pdf_create_handler(tmp_path, allowed_roots=[tmp_path])),
    ]:
        registry.register(spec, handler)
    source = tmp_path/('rendered'+extension)
    producer = 'terminal_exec' if extension == '.png' else 'pdf_create'
    if extension == '.png':
        script = ("from PIL import Image,ImageDraw; im=Image.new('RGB',(480,640),'navy'); "
            "ImageDraw.Draw(im).text((30,40),'Confirmed value: 37',fill='white'); "
            f"im.save({str(source)!r})")
    else:
        from nullion.artifacts import artifact_root_for_principal
        source = artifact_root_for_principal('telegram:42' if surface == 'telegram' else 'web:method-change')/'rendered.pdf'
    class Model:
        phase = 0
        trace = []
        errors = []
        def create(self, **kwargs):
            try:
                return self._create(**kwargs)
            except Exception as exc:
                self.errors.append(repr(exc))
                raise
        def _create(self, **kwargs):
            self.trace.append({'tools':[row['name'] for row in kwargs.get('tools', [])],
                'last':str(kwargs.get('messages', [])[-1:])[-1400:]})
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            names = {row['name'] for row in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return call('select_attachment_format', {'extension':extension, 'confidence':1})
            if not names: return text('{}')
            self.phase += 1
            assert self.phase <= 5, 'An obsolete production method restarted execution'
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['local_files'], 'tool_names':['file_write'],
                    'required_tool_names':['file_write']})
            if self.phase == 2:
                return call('request_tool_scope', {'capabilities':['local_shell', 'local_files'],
                    'tool_names':[producer, 'file_read'], 'required_tool_names':[producer],
                    'artifact_extensions':[extension]})
            if self.phase == 3:
                if producer == 'pdf_create':
                    return call(producer, {'output_path':str(source), 'text_pages':['Confirmed value: 37'], 'expected_page_count':1})
                return call('terminal_exec', {'command':'python -c '+shlex.quote(script), 'output_paths':[str(source)]})
            paths = list(Path(os.environ['NULLION_WORKSPACE_STORAGE_ROOT']).rglob('rendered'+extension))
            assert len(paths) == 1
            if self.phase == 4: return call('file_read', {'path':str(paths[0])})
            return text('確認結果: 37\n\nMEDIA:'+str(paths[0]))
    model = Model()
    runtime.active_tool_registry = registry
    if surface == 'web':
        from fastapi.testclient import TestClient
        from nullion.web_app import create_app
        with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
            response = client.post('/api/chat', json={'text':prompt, 'conversation_id':'web:method-change'})
            assert response.status_code == 200, response.text
            data = response.json()
            answer = data['text']
            assert len(data['artifacts']) == 1, (data['text'], model.phase, model.errors, model.trace[-1:])
            assert client.get(data['artifacts'][0]['url']).content == source.read_bytes()
    else:
        from telegram import Bot, Message
        from nullion.config import NullionSettings, TelegramSettings
        from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
        from nullion.telegram_app import _deliver_reply
        from tests.e2e.test_generated_artifact_protocol_recovery import upload_boundary
        settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic', operator_chat_id='42', chat_enabled=True))
        result = run_platform_chat_request(runtime,
            PlatformChatRequest(platform='telegram', chat_id='42', text=prompt,
                conversation_id='telegram:42', turn_id='synthetic-method-change',
                settings=settings, model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
                append_activity_trace=False))
        answer = result.text
        from nullion.telegram_turn_graph import plan_telegram_post_run_delivery
        plan = plan_telegram_post_run_delivery(text_for_ack=prompt, reply=answer,
            artifact_paths=tuple(result.artifact_paths), runtime=runtime, conversation_id='telegram:42',
            decision_card=None, stream_final_reply=False, streaming_mode=None, final_only_streaming_mode=None)
        messages = []
        with upload_boundary(messages) as (base_url, uploads):
            async def deliver():
                async with Bot('123456:synthetic', base_url=base_url) as bot:
                    message = Message.de_json({'message_id':1, 'date':1,
                        'chat':{'id':42, 'type':'private'}, 'text':prompt}, bot)
                    return await _deliver_reply(message, answer, principal_id='telegram:42',
                        artifact_paths=result.artifact_paths, tool_results=result.tool_results,
                        delivery_contract=plan.delivery_contract)
            asyncio.run(deliver())
            assert uploads == [source.read_bytes()], (answer, result.artifact_paths, messages)
    assert answer.strip() and 'required step' not in answer and 'file_write' not in answer, answer
    if extension == '.pdf':
        from pypdf import PdfReader
        assert '37' in PdfReader(source).pages[0].extract_text()
    else:
        from PIL import Image
        assert Image.open(source).size == (480, 640)
    turns = [row for row in runtime.store.list_conversation_events() if row.get('event_type') == 'conversation.chat_turn']
    assert len(turns) == 1 and turns[0]['response_fulfilled'] is True, turns
    scopes = [row for row in turns[0]['tool_results'] if row['tool_name'] == 'request_tool_scope']
    assert 'file_write' in scopes[-1]['output']['unavailable_required_tool_names'], scopes
    assert all(row['tool_name'] != 'file_write' for row in turns[0]['tool_results'])
    assert model.phase in {4, 5}, 'Do not restart source work or an obsolete method'
