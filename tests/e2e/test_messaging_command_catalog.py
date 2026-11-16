"""Real platform ingress and Telegram menu transport, without provider calls."""
import asyncio
from datetime import UTC,datetime
import json
import os
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('surface',['telegram','slack','discord'])
def test_full_command_list_crosses_the_real_messaging_boundary(tmp_path,monkeypatch,surface):
    from nullion import telegram_app,slack_app,discord_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.operator_commands import operator_command_reference,telegram_bot_command_menu
    from nullion.tools import ToolRegistry
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN,TelegramWire,install_wire
    runtime=isolate(tmp_path,monkeypatch);runtime.active_tool_registry=ToolRegistry()
    class Model:
        def create(self,**kwargs):raise AssertionError('Command help must not invoke a model')
    settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77',
        'NULLION_TELEGRAM_CHAT_ENABLED':'true','NULLION_SLACK_OPERATOR_USER_ID':'U1',
        'NULLION_DISCORD_OPERATOR_USER_ID':'123','NULLION_SLACK_ENABLED':'true','NULLION_DISCORD_ENABLED':'true'})
    model=Model();service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token=TOKEN,
        operator_chat_id='77',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
    delivered=[];menus={'default':[{'command':'cancel','description':'obsolete'}],
        'all_private_chats':[{'command':'cancel','description':'obsolete'}],'chat':[{'command':'cancel','description':'obsolete'}]}
    wire=TelegramWire();original=wire.handle;parse_modes=[]
    async def handle(request):
        method=request.url.path.rsplit('/',1)[-1]
        if method in {'setMyCommands','getMyCommands'}:
            fields=parse_qs((await request.aread()).decode())
            scope=json.loads(fields.get('scope',['{}'])[0]).get('type','default')
            if method=='setMyCommands':menus[scope]=json.loads(fields['commands'][0]);result=True
            else:result=menus[scope]
            return httpx.Response(200,json={'ok':True,'result':result})
        if method=='sendMessage':
            fields=parse_qs((await request.aread()).decode())
            parse_modes.append(fields.get('parse_mode',[''])[0])
        return await original(request)
    wire.handle=handle;install_wire(monkeypatch,wire)
    async def replay():
        if surface=='telegram':
            from telegram import Bot,Chat,Message,Update,User,BotCommandScopeChat
            from nullion.telegram_transport import build_telegram_bot
            async with build_telegram_bot(Bot,TOKEN) as bot:
                assert (await bot.get_my_commands(scope=BotCommandScopeChat(chat_id=77)))[0].command=='cancel'
                assert await telegram_app.register_telegram_bot_commands(bot,chat_id='77')
                actual=await bot.get_my_commands(scope=BotCommandScopeChat(chat_id=77))
                assert [(c.command,c.description) for c in actual]==list(telegram_bot_command_menu(include_private_aliases=False))
                message=Message(message_id=1,date=datetime.now(UTC),chat=Chat(77,'private'),
                    from_user=User(77,'Fixture',False),text='/help commands')
                message.set_bot(bot);message.chat.set_bot(bot)
                await service.on_message(Update(update_id=1,message=message),SimpleNamespace(bot=bot))
            from nullion.telegram_formatting import telegram_html_to_plain
            delivered.extend(telegram_html_to_plain(text) if mode=='HTML' else text
                for mode,text in zip(parse_modes,wire.texts,strict=True))
        elif surface=='slack':
            async def respond(text):delivered.append(text)
            await slack_app.handle_slack_command(service,settings,command={'user_id':'U1','channel_id':'C1',
                'text':'help commands','trigger_id':'1'},respond=respond)
        else:
            class Channel:
                id='C1'
                async def send(self,text):delivered.append(text)
            await discord_app.handle_discord_message(service,settings,SimpleNamespace(author=SimpleNamespace(id=123,bot=False),
                content='/help commands',id='1',channel=Channel(),attachments=[]))
    asyncio.run(replay())
    text='\n'.join(delivered)
    expected=operator_command_reference(platform=surface,full=True)
    for line in expected.splitlines():
        assert line in text,(line,[row for row in text.splitlines() if line.split()[0] in row])
    assert '/cancel' not in text and '/planner_feed' not in text
    assert not runtime.store.list_conversation_events(event_types=('conversation.model_timing',))
