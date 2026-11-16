import asyncio

import pytest


@pytest.mark.parametrize('platform',['telegram','slack','discord','web'])
def test_every_documented_command_has_the_correct_platform_invocation(platform):
    from nullion.operator_commands import operator_command_catalog, operator_command_reference, normalize_operator_command_head
    specs=operator_command_catalog()
    aliases={s.head:s.telegram_command for s in specs if s.telegram_command}
    reference=operator_command_reference(platform=platform,full=True)
    for spec in specs:
        head,_,args=spec.command.partition(' ')
        if platform=='telegram':
            head='/'+aliases.get(head,head[1:])
            assert normalize_operator_command_head(head+'@NullionBot')==spec.head
        elif platform=='slack':head='/nullion '+head[1:]
        assert head+(' '+args if args else '')+' — '+spec.description in reference
    assert '/cancel' not in reference and '/planner_feed' not in reference
    assert '/verbose [on|off|status]' in reference or '/nullion verbose [on|off|status]' in reference


def test_telegram_registration_refreshes_specific_scopes_that_override_default():
    from nullion.telegram_app import register_telegram_bot_commands, TELEGRAM_BOT_COMMANDS
    class Bot:
        menus={'default':[('cancel','obsolete')],'all_private_chats':[('cancel','obsolete')],'chat':[('cancel','obsolete')]}
        async def set_my_commands(self,commands,**kwargs):
            scope=kwargs.get('scope')
            self.menus[scope.type if scope else 'default']=[(c.command,c.description) for c in commands]
    bot=Bot()
    assert asyncio.run(register_telegram_bot_commands(bot,chat_id='77'))
    assert all(rows==list(TELEGRAM_BOT_COMMANDS) for rows in bot.menus.values())
    assert all(1<=len(command)<=32 and set(command)<=set('abcdefghijklmnopqrstuvwxyz0123456789_')
        and 1<=len(description)<=256 for command,description in TELEGRAM_BOT_COMMANDS)
