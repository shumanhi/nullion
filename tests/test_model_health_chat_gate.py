from datetime import UTC, datetime, timedelta

import pytest


def seed_outage(runtime, *, recorded_at):
    action = {
        'action_id': 'invalid-probe-outage',
        'owner': 'doctor', 'action_type': 'inspect', 'summary': 'Model API is unreachable', 'severity': 'high',
        'status': 'pending',
        'recommendation_code': 'model_api_unreachable',
        'error': 'HTTP 400: completion budget below the minimum',
        'created_at': recorded_at,
        'updated_at': recorded_at,
    }
    runtime.store.add_doctor_action(action)


@pytest.mark.parametrize('recorded_at', [
    (datetime.now(UTC) - timedelta(hours=18)).isoformat(),
    (datetime.now(UTC) - timedelta(hours=18)).replace(tzinfo=None).isoformat(),
    'invalid-timestamp',
], ids=['expired-aware', 'expired-naive', 'invalid-timestamp'])
def test_expired_model_warning_does_not_gate_chat_or_call_recovery_probe(tmp_path, monkeypatch, recorded_at):
    from nullion import chat_operator, health_probes
    from nullion.runtime import bootstrap_persistent_runtime
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    seed_outage(runtime, recorded_at=recorded_at)
    def forbidden_probe(client):
        raise AssertionError('Expired warnings must not add a model call to a new turn')
    monkeypatch.setattr(health_probes, 'make_model_api_probe', forbidden_probe)
    assert chat_operator._chat_model_issue_reply(runtime, message='次の候補を比較してください', model_client=object()) is None
    assert runtime.store.get_doctor_action('invalid-probe-outage')['status'] == 'pending'


@pytest.mark.parametrize('reachable', [True, False])
def test_fresh_model_warning_uses_real_valid_probe_before_gating_chat(tmp_path, reachable):
    from nullion import chat_operator
    from nullion.runtime import bootstrap_persistent_runtime
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    seed_outage(runtime, recorded_at=datetime.now(UTC).isoformat())
    class Client:
        calls = []
        def create(self, *, messages, tools, max_tokens):
            self.calls.append(max_tokens)
            assert max_tokens == 16
            if not reachable:
                raise TimeoutError('Provider connection timed out')
            return {'content': []}
    client = Client()
    reply = chat_operator._chat_model_issue_reply(runtime, message='قارن الخيارات المتاحة', model_client=client)
    assert (reply is None) == reachable
    assert client.calls == [16]
    assert runtime.store.get_doctor_action('invalid-probe-outage')['status'] == ('cancelled' if reachable else 'pending')


@pytest.mark.parametrize('payload,expected', [
    ({'error': {'code': 'token_expired', 'message': '認証エラー'}}, True),
    ({'error': {'code': 'token_invalidated', 'message': 'رمز غير صالح'}}, True),
    ({'error': {'code': 'access_denied', 'message': 'token_expired token_invalidated'}}, False),
    ({'error': {'message': 'authentication token is expired'}}, False),
    ({'code': 'token_invalidated'}, False),
    ({'error': ['token_invalidated']}, False),
    ({'error': {'code': ['token_invalidated']}}, False),
    ({'error': {'code': {'code': 'token_invalidated'}}}, False),
    ({'error': {'code': None}}, False),
    (['token_invalidated'], False),
])
def test_auth_recovery_uses_provider_error_schema(payload, expected):
    import json
    from nullion.model_clients import _codex_token_refreshable_response
    assert _codex_token_refreshable_response(json.dumps(payload)) is expected
    assert not _codex_token_refreshable_response('token_invalidated')


@pytest.mark.parametrize('description', ['利用上限に達しました', 'Se agotó; capacidad agotada', 'تم بلوغ الحد'])
def test_legacy_usage_limit_signal_is_quota_in_chat_and_doctor(description):
    import json
    from nullion import chat_operator, telegram_app
    error = 'Codex Responses request failed with HTTP 429: ' + json.dumps({'error': {
        'type': 'usage_limit_reached', 'message': description, 'resets_in_seconds': 600}})
    record = {'recommendation_code': 'review_doctor_route', 'reason':
              'source=telegram_chat;detail=Agent orchestrator error: ' + error + ';message_text=こんにちは'}
    reply = chat_operator._model_issue_reply_for_doctor_record(record)
    assert 'quota' in reply and 'reset' in reply and 'connectivity' not in reply
    assert telegram_app._doctor_title_text(record) == 'Model quota exhausted'
    assert 'quota' in telegram_app._doctor_diagnosis_text(record)
    assert 'reset' in telegram_app._doctor_suggestion_text(record)


@pytest.mark.parametrize('failure', ['HTTP 503: upstream unavailable', 'Connection unavailable'])
def test_user_quoted_provider_error_cannot_create_quota_diagnosis(failure):
    from nullion.health_probes import recorded_model_error_details
    action = {'reason': 'source=telegram_chat;detail='+failure+';message_text=' +
              'HTTP 429: {"error":{"type":"usage_limit_reached"}}'}
    assert recorded_model_error_details(action).get('category') is None
