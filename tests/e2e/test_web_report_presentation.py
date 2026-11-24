"""Web approval and history expose rendered report text at the API boundary."""

from datetime import UTC, datetime
import json
import os

from fastapi.testclient import TestClient
import pytest

from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
from tests.e2e.test_scheduled_report_completion import telegram_boundary
from tests.report_presentation_fixture import report_fixture
from tests.workflow_provider_protocol import workflow_provider_reply


pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('origin', ['web', 'telegram'])
def test_approval_returns_report_prose_to_web(tmp_path, monkeypatch, origin):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_approval_request
    from nullion.chat_text import REPORT_MESSAGE_FORMAT
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    runtime = isolate(tmp_path, monkeypatch)
    registry = ToolRegistry()
    runtime.active_tool_registry = registry
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'missing.env'))
    monkeypatch.setenv('NULLION_TELEGRAM_BOT_TOKEN', 'fixture-only')
    telegram_deliveries, _ = telegram_boundary(monkeypatch)
    chat_id = '123' if origin == 'telegram' else None
    conversation_id = f'{origin}:{chat_id or "report-review"}'
    prompt = 'この検査の数値を報告してください。'
    report, _, _ = report_fixture()
    report['summary'] = 'Asunción → Cusco · 3件確認しました。'
    envelope = json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': report}, ensure_ascii=False)

    class ReportProvider:
        def create(self, **kwargs):
            return workflow_provider_reply(kwargs, completion=True) or text(envelope)

    approval = create_approval_request(conversation_id, 'allow_boundary', 'example.test',
        request_kind='boundary_policy')
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(SuspendedTurn(approval_id=approval.approval_id,
        conversation_id=conversation_id, chat_id=chat_id, message=prompt,
        request_id=None, message_id=None, created_at=datetime.now(UTC),
        messages_snapshot=[{'role': 'user', 'content': [{'type': 'text', 'text': prompt}]}]))
    runtime.checkpoint(force=True)

    orchestrator = AgentOrchestrator(model_client=ReportProvider())
    with TestClient(create_app(runtime, orchestrator, registry)) as client:
        response = client.post(f'/api/approve/{approval.approval_id}', json={'mode': 'once'})

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['ok'] is True
    visible = payload['resume']['text']
    assert visible == payload['resumed_text']
    assert report['summary'] in visible
    assert REPORT_MESSAGE_FORMAT not in visible
    assert runtime.store.get_approval_request(approval.approval_id).status.value == 'approved'
    assert runtime.store.get_suspended_turn(approval.approval_id) is None
    if origin == 'telegram':
        assert len(telegram_deliveries) == 1
        assert report['summary'] in telegram_deliveries[0]['text']
        assert REPORT_MESSAGE_FORMAT not in telegram_deliveries[0]['text']
    else:
        assert not telegram_deliveries


def test_web_history_keeps_ordinary_json_and_hides_unrenderable_report(tmp_path, monkeypatch):
    from nullion.chat_text import REPORT_MESSAGE_FORMAT
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    runtime = isolate(tmp_path, monkeypatch)
    endpoint = '/api/chat/history/web:report-history'
    unrenderable_report = json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': None})
    ordinary_json = '{"origin":"Asunción","destination":"Cusco"}'
    report, _, _ = report_fixture()
    old_report = '✉️ ' + json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': report})

    with TestClient(create_app(runtime, None, ToolRegistry())) as client:
        for stored in (unrenderable_report, ordinary_json, old_report):
            response = client.post(f'{endpoint}/message', json={'role': 'bot', 'text': stored})
            assert response.status_code == 200, response.text
        response = client.get(endpoint)

    assert response.status_code == 200, response.text
    visible = [message['text'] for message in response.json()['messages']]
    assert ordinary_json in visible
    assert any(report['summary'] in message for message in visible)
    assert all(REPORT_MESSAGE_FORMAT not in message for message in visible)
