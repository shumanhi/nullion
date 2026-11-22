"""Identical prepared report contracts rendered at real client boundaries."""
import asyncio
import json
import os
from types import SimpleNamespace

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


@pytest.mark.parametrize('surface', ['web','telegram','slack','discord'])
@pytest.mark.parametrize('summary', ['Prepared report delivered.', '準備済みの報告を配信しました。'])
def test_prepared_report_contract_renders_at_each_client(tmp_path, monkeypatch, surface, summary):
    from tests.e2e.test_scheduled_report_completion import setup_runtime, telegram_boundary
    from tests.report_presentation_fixture import report_fixture
    runtime = setup_runtime(tmp_path,monkeypatch)
    message, _, _ = report_fixture()
    secret = 'ghp_' + 'fixture' * 5
    message['summary'] = f'{summary} Token: {secret}'
    raw = json.dumps({'format':'nullion.report.v1','message':message})
    wire = []
    # Reproduce the historic account-icon prefix on the actual delivery path.
    legacy_raw = '✉️ ' + raw
    if surface=='telegram':
        deliveries, send = telegram_boundary(monkeypatch)
        assert send(SimpleNamespace(),'telegram','2222',legacy_raw)
        wire.extend(row['text'] for row in deliveries)
    elif surface=='slack':
        from slack_sdk.web.async_client import AsyncWebClient
        from nullion.slack_app import send_slack_platform_delivery
        async def post(self, **kwargs):
            wire.append(kwargs['text'])
            return {'ok':True,'ts':'1'}
        monkeypatch.setattr(AsyncWebClient,'chat_postMessage',post)
        assert asyncio.run(send_slack_platform_delivery(bot_token='fixture',channel='C-fixture',text=legacy_raw))
    elif surface=='discord':
        import httpx
        from nullion.discord_app import send_discord_platform_delivery
        class Remote:
            def __init__(self, **kwargs): pass
            async def __aenter__(self): return self
            async def __aexit__(self,*args): pass
            async def post(self,url,**kwargs):
                wire.append(kwargs['json']['content'])
                return SimpleNamespace(raise_for_status=lambda:None)
        monkeypatch.setattr(httpx,'AsyncClient',Remote)
        assert asyncio.run(send_discord_platform_delivery(bot_token='fixture',channel_id='2222',text=legacy_raw))
    else:
        from fastapi.testclient import TestClient
        from nullion.agent_orchestrator import AgentOrchestrator
        from nullion.tools import ToolRegistry
        from nullion.web_app import create_app
        class Model:
            def create(self, **kwargs):
                auxiliary = workflow_provider_reply(kwargs,completion=True)
                if auxiliary is not None: return auxiliary
                return {'stop_reason':'end_turn','content':[{'type':'text','text':legacy_raw}]}
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=Model()),ToolRegistry())) as client:
            result = client.post('/api/chat',json={'text':'表示してください。','conversation_id':'web:prepared-report'})
            assert result.status_code==200, result.text
            wire.append(result.json()['text'])
            event = next(row for row in reversed(runtime.store.list_conversation_events('web:prepared-report'))
                if row.get('event_type')=='conversation.chat_turn')
            expected = json.loads(raw.replace(secret, '[redacted]'))
            assert json.loads(event['assistant_reply']) == expected
            history = client.get('/api/chat/history/web:prepared-report',
                params={'turn_ids':json.dumps([event['turn_id']])})
            assert history.status_code==200
            restored = history.json()['terminal_replies'][0]['text']
            assert summary in restored and 'nullion.report.v1' not in restored
    visible = '\n'.join(wire)
    assert visible.count(summary)==1, visible
    assert secret not in visible and '[redacted]' in visible
    assert 'nullion.report.v1' not in visible and 'title_is_required' not in visible
    assert '$720' in visible and 'Calendar:' in visible
    assert 'Report-wide explanation' not in visible
