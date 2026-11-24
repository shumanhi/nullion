"""Real Telegram reply delivery and history with a controlled tool result."""

import asyncio
import os
from types import SimpleNamespace

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('repair,execution', [
    ('answer', 'off'), ('decline', 'off'), ('tool_attempt', 'off'), ('answer', 'read_only'),
    ('inline_html', 'off'),
])
def test_telegram_numeric_followup_cannot_publish_unrelated_files(tmp_path, monkeypatch, repair, execution):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_runtime
    from nullion.config import load_settings
    from nullion.telegram_app import ChatOperatorService
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import call, isolate, text

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', execution)
    root = artifact_root_for_runtime(runtime.store)
    root.mkdir(parents=True, exist_ok=True)
    old_pngs = [root / f'prior-file-{index:02}.png' for index in range(34)]
    for path in old_pngs:
        Image.new('RGB', (12, 12)).save(path)
    source_log = root / 'synthetic-tool-output.txt'
    source_log.write_text('\n'.join(str(path) for path in old_pngs))
    registry = ToolRegistry()
    calls = []

    def observe(invocation):
        calls.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {
            'configured_users': 4,
            'stdout': source_log.read_text(), 'artifact_paths': [str(source_log)],
            'artifact_descriptors': [{'path': str(source_log), 'role': 'source', 'kind': 'terminal_output'}],
        })

    native_tool = 'fixture_account_status'
    registry.register(ToolSpec(native_tool, 'Read test account state', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, capability_tags=('explicit_scope_only',)), observe)
    choice = '対象を選んでください。\n\n1. メッセージ\n2. メール\n\n1 または 2 を入力してください。'

    class Model:
        phase = 0
        repairs = 0

        def create(self, **kwargs):
            if 'outside the verified message-only scope' in str(kwargs['messages'][-1]):
                self.repairs += 1
                assert not kwargs.get('tools')
                if repair == 'answer':
                    return text('設定済みの利用者は4人です。')
                if repair == 'inline_html':
                    return text('設定済みの利用者は4人です。\n\n```html\n<!doctype html><html><head><title>'
                                'Fixture</title></head><body><p>' + 'fixture paragraph ' * 50 + '</p></body></html>\n```')
                if repair == 'tool_attempt':
                    return {'stop_reason': 'tool_use', 'content': [
                        {'type': 'tool_use', 'id': 'extra', 'name': native_tool, 'input': {}}]}
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            self.phase += 1
            if self.phase == 1:
                return text(choice)
            if self.phase == 2:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'scope',
                    'name': 'request_tool_scope', 'input': {'capabilities': ['local_shell'],
                        'tool_names': [native_tool], 'artifact_extensions': []}}]}
            if self.phase == 3:
                if execution == 'read_only':
                    return call('code_mode', {'tool_names': [native_tool],
                        'code': f"call_tool('{native_tool}', {{}})['output']['configured_users']"})
                return {'stop_reason': 'tool_use', 'content': [
                    {'type': 'tool_use', 'id': 'read', 'name': native_tool, 'input': {}}]}
            return text('Done — attached the requested images.\n\n' +
                        '\n'.join(f'MEDIA:{path}' for path in old_pngs))

    model = Model()
    orchestrator = AgentOrchestrator(model_client=model)
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN': '123456:abcdefghijklmnopqrstuvwxyz',
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID': '123', 'NULLION_TELEGRAM_CHAT_ENABLED': 'true'})
    service = ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
        operator_chat_id='123', model_client=model, agent_orchestrator=orchestrator)
    sent, uploads = [], []

    class Message:
        chat = SimpleNamespace(id=123, send_action=lambda **kwargs: asyncio.sleep(0))
        reply_to_message = None

        def __init__(self, value, identity):
            self.text, self.message_id = value, identity

        async def reply_text(self, value, **kwargs):
            sent.append(value)
            return SimpleNamespace(message_id=self.message_id + 100)

        async def reply_document(self, *args, **kwargs):
            uploads.append(args)

    async def run():
        for identity, message in [(51001, '対象アカウントの権限を確認してください。'), (51002, '1')]:
            await service.on_message(SimpleNamespace(update_id=identity, message=Message(message, identity)),
                                     SimpleNamespace(bot=None))

    with TestClient(create_app(runtime, orchestrator, registry)):
        asyncio.run(run())

    assert len(calls) == 1 and model.repairs == 1 and not uploads
    assert any('1.' in reply and '2.' in reply for reply in sent)
    if repair in {'answer', 'inline_html'}:
        assert any('設定済みの利用者は4人です。' in reply for reply in sent)
    else:
        assert any('verify' in reply and 'completed' in reply for reply in sent)
    assert not any('34 files' in reply or 'prior-file-' in reply for reply in sent)
    turn = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.chat_turn')[-1]
    assert turn['parent_turn_id'] == 'telegram-update:51001'
    assert turn.get('response_fulfilled') is (repair in {'answer', 'inline_html'})
    assert 'MEDIA:' not in turn['assistant_reply']
    assert not list(root.rglob('html-preview*.html'))
    if execution == 'read_only':
        assert any(result['tool_name'] == 'code_mode' and result['status'] == 'completed'
                   for result in turn['tool_results'])
