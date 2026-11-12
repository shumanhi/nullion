"""Real adapter runners, SQLite preferences and rendered platform delivery.

Only the external Slack/Discord SDK boundary is replaced; no live messages.
"""
import asyncio
import os
import sys
from types import ModuleType, SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker adapter boundary')


@pytest.mark.parametrize('surface', ['slack', 'discord'])
@pytest.mark.parametrize('enabled', [False, True])
def test_background_cards_use_saved_chat_visibility_and_keep_terminal_results(tmp_path, monkeypatch, surface, enabled):
    from nullion import discord_app, slack_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import set_activity_trace_enabled_for_chat
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'missing.env'))
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'false' if enabled else 'true')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    conversation = surface + ':2001'
    set_activity_trace_enabled_for_chat(runtime, chat_id=conversation, enabled=enabled)
    orchestrator = AgentOrchestrator(model_client=object())
    service = SimpleNamespace(runtime=runtime, agent_orchestrator=orchestrator)
    sent = []

    async def exercise():
        deliver = orchestrator._deliver_fn
        activity_sent = await deliver(conversation, '✓ file_read — marker-43', is_status=True,
                                      status_kind='tool_activity', group_id='owned-group', activity_id='read-43')
        assert activity_sent is enabled
        assert await deliver(conversation, 'PLANNER\nFor: 保存された設定\n1. Read the source',
                             is_status=True, status_kind='task_summary', group_id='owned-group')
        assert await deliver(conversation, 'Final result: 43.')

    class SlackEndpoint:
        def __init__(self, **kwargs): pass
        async def chat_postMessage(self, **kwargs):
            sent.append(kwargs['text'])
            return {'ok': True, 'ts': str(len(sent))}
        async def chat_update(self, **kwargs):
            sent.append(kwargs['text'])
            return {'ok': True, 'ts': kwargs['ts']}

    class SlackApp:
        def __init__(self, **kwargs): self.client = SlackEndpoint()
        def event(self, name): return lambda fn: fn
        def command(self, name): return lambda fn: fn

    class SlackSocket:
        def __init__(self, *args): pass
        async def start_async(self): await exercise()

    class DiscordChannel:
        async def send(self, content, **kwargs):
            sent.append(content)
            async def edit(**changes): sent.append(changes['content'])
            return SimpleNamespace(edit=edit)

    class DiscordClient:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        def get_channel(self, channel): return DiscordChannel()
        async def start(self, token): await exercise()

    if surface == 'slack':
        modules = {
            'slack_bolt': {}, 'slack_bolt.adapter': {}, 'slack_bolt.adapter.socket_mode': {},
            'slack_bolt.adapter.socket_mode.async_handler': {'AsyncSocketModeHandler': SlackSocket},
            'slack_bolt.async_app': {'AsyncApp': SlackApp},
            'slack_sdk': {}, 'slack_sdk.web': {},
            'slack_sdk.web.async_client': {'AsyncWebClient': SlackEndpoint},
        }
        settings = SimpleNamespace(slack=SimpleNamespace(enabled=True, bot_token='xoxb-test',
                                    app_token='xapp-test', signing_secret='test'))
        module, run = slack_app, slack_app.run_slack_app
    else:
        modules = {'discord': {'Client': DiscordClient,
            'Intents': SimpleNamespace(default=lambda: SimpleNamespace(message_content=False)),
            'LoginFailure': type('LoginFailure', (Exception,), {}),
            'PrivilegedIntentsRequired': type('PrivilegedIntentsRequired', (Exception,), {})}}
        settings = SimpleNamespace(discord=SimpleNamespace(enabled=True, bot_token='discord-test', operator_user_id='2001'))
        module, run = discord_app, discord_app.run_discord_app
    for name, attributes in modules.items():
        fake = ModuleType(name)
        fake.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, fake)
    monkeypatch.setattr(module, 'load_settings', lambda **kwargs: settings)
    asyncio.run(run(checkpoint_path=tmp_path / 'runtime.db', service_builder=lambda **kwargs: service))
    assert len(sent) == 2, sent
    assert 'PLANNER' in sent[0] and '保存された設定' in sent[0], sent
    assert ('file_read' in sent[0]) is enabled, sent
    assert ('Activity' in sent[0]) is enabled, sent
    assert 'marker-43' not in sent[0], sent
    assert sent[1] == 'Final result: 43.', sent
    assert runtime.store.get_conversation_session_setting(conversation, 'activity_trace_enabled') is enabled
