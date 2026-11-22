"""Real shared runner, SQLite, local terminal and client rendering boundaries."""
import asyncio
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import shlex
from types import SimpleNamespace

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('surface', ['api', 'telegram'])
@pytest.mark.parametrize('prompt', ['What identifier is configured for Robin?', 'ロビンに設定された識別子は？'])
def test_configuration_answer_survives_incidental_old_file_paths(tmp_path, monkeypatch, surface, prompt):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_terminal_exec_handler
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_recall_continuity_workflow import deliver_telegram
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('local',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    conversation = 'web:configuration-check' if surface == 'api' else 'telegram:90210'
    root = artifact_root_for_principal(conversation)
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for index in range(15):
        path = root/f'old-document-{index}.pdf'
        path.write_bytes(b'%PDF-1.4 old source')
        paths.append(str(path))
    config = tmp_path/'synthetic-config.json'
    config.write_text(json.dumps({'person':'Robin', 'identifier':'R-7301', 'archived_documents':paths}))
    command = shlex.join(['python3','-c','from pathlib import Path; print(Path('+repr(str(config))+').read_text())'])
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('terminal_exec','Read local configuration',ToolRiskLevel.LOW,
        ToolSideEffectClass.DANGEROUS_EXEC,False,10), _build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))
    class Model:
        phase = 0
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            if not kw.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['local_shell'],
                    'tool_names':['terminal_exec'], 'required_tool_names':['terminal_exec'], 'artifact_extensions':[]})
            if self.phase == 2:
                return call('code_mode', {'tool_names':['terminal_exec'],
                    'code':"call_tool('terminal_exec', "+repr({'command':command})+")"})
            assert self.phase == 3, 'An informational answer must not enter artifact selection/recovery'
            assert 'R-7301' in json.dumps(kw['messages'])
            return text('Robin is configured with identifier R-7301.')
    model = Model()
    runtime.active_tool_registry = registry
    runtime.model_client = model
    orchestrator = AgentOrchestrator(model_client=model)
    if surface == 'api':
        with TestClient(create_app(runtime,orchestrator,registry)) as client:
            response = client.post('/api/chat',json={'text':prompt,'conversation_id':conversation})
            assert response.status_code == 200, response.text
            payload = response.json()
            visible = payload['text']
            assert not payload.get('artifacts'), payload
    else:
        visible = handle_chat_operator_message(runtime,'/chat '+prompt,chat_id='90210',
            settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuvwxyz',
                operator_chat_id='90210',chat_enabled=True)),agent_orchestrator=orchestrator,allow_mini_agents=False)
        assert 'MEDIA:' not in visible
        visible = deliver_telegram(visible)
    assert visible == 'Robin is configured with identifier R-7301.'
    event = next(row for row in reversed(runtime.store.list_conversation_events(conversation))
        if row.get('event_type') == 'conversation.chat_turn')
    assert event['response_fulfilled'] is True
    assert any(row['tool_name']=='terminal_exec' and row['status']=='completed' for row in event['tool_results'])
    assert all(path.is_file() for path in root.glob('old-document-*.pdf'))


@pytest.mark.parametrize('subject', ['Field report', '現地調査'])
def test_real_approval_callback_edits_one_email_review(tmp_path, monkeypatch, subject):
    from nullion import telegram_app
    from nullion.approvals import create_approval_request
    from nullion.config import NullionSettings, TelegramSettings
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path,monkeypatch)
    approval = create_approval_request(requested_by='telegram:123', action='use_tool',
        resource='tool:email_send',request_kind='capability_grant',context={
            'tool_name':'email_send','tool_permission_scope':'exact_invocation',
            'tool_arguments':{'to':'reader@example.test','subject':subject,
                'body':'Plain fallback','html_body':'<p>Report contents</p>'},
            'html_preview_path':'preview.html'})
    runtime.store.add_approval_request(approval)
    edits = []
    class Message:
        chat = SimpleNamespace(id=123)
        message_id = 1
        async def edit_text(self, text, **kw): edits.append(text)
        async def reply_text(self, text, **kw): edits.append(text)
    class Query:
        message = Message()
        from_user = SimpleNamespace(id=123)
        data = telegram_app._build_callback_data(kind='approval',action='allow_once',record_id=approval.approval_id)
        async def answer(self, *args, **kw): pass
    settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuvwxyz',operator_chat_id='123',chat_enabled=True))
    service = telegram_app.ChatOperatorService(runtime,'123456:abcdefghijklmnopqrstuvwxyz','123',settings)
    asyncio.run(service.on_callback_query(SimpleNamespace(callback_query=Query()),None))
    visible = '\n'.join(edits)
    assert visible.count('Email draft:') == 1, visible
    assert visible.count(subject) == 1 and visible.count('reader@example.test') == 1, visible
    assert str(runtime.store.get_approval_request(approval.approval_id).status).lower().endswith('approved')


@pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E') != '1', reason='Opt-in live stage')
def test_live_stage_configuration_answer_has_no_incidental_attachments():
    from uuid import uuid4
    from scripts import live_stage_chat_contract_probe as probe
    home = Path(os.environ.get('NULLION_LIVE_RUNTIME_HOME', str(Path.home()/'.nullion-stage')))
    configured = json.loads((home/'users.json').read_text()).get('users', [])
    selected = next(row for row in configured if row.get('active')
        and row.get('display_name') and row.get('telegram_chat_id'))
    prompt = f"What is {selected['display_name']}'s Telegram ID configured in this installation?"
    conversation = 'web:live-configuration-check:'+uuid4().hex[:10]
    base = os.environ.get('NULLION_LIVE_BASE_URL','http://127.0.0.1:8753').rstrip('/')
    reply, elapsed = probe._post_json(base+'/api/chat', {'text':prompt,'conversation_id':conversation}, timeout=180)
    event = probe._chat_turn_for_user_message_with_poll(home/'runtime.db',conversation,prompt)
    assert str(selected['telegram_chat_id']) in reply.get('text',''), reply
    assert not reply.get('artifacts') and 'MEDIA:' not in reply.get('text',''), reply
    assert event and event.get('response_fulfilled') is True
    assert elapsed < 90000
