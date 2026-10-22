from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from nullion.cron_delivery import (
    _cron_final_repair_tool_evidence, _repair_cron_agent_final_text_if_needed,
    _resolve_missing_cron_artifact_contract, CronRunDeliveryCallbacks,
    run_cron_delivery_workflow, cron_delivery_block_reason,
)
from nullion.response_sanitizer import sanitize_user_visible_reply
from nullion.tools import ToolResult, ToolInvocation, ToolRegistry, register_cron_tools


@dataclass
class Job:
    id: str = 'completion-case'
    name: str = 'Report'
    task: str = '受信メールの期限を報告'
    workspace_id: str = 'workspace-example'
    delivery_channel: str = 'telegram'
    artifact_delivery_options: dict = field(default_factory=dict)


def response(text='', **metadata):
    return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': text}] if text else [], **metadata}


def report_response(report, **updates):
    return response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                                'evidence_refs': ['e0'], 'report': report, **updates}))


def source_result():
    return ToolResult('read-1', 'email_read', 'completed', {'message': {
        'subject': '更新', 'from': 'Example office', 'date': '2028-04-02',
        'body': 'The renewal is due on 2028-04-09. Amount: 45 USD.',
    }})


@pytest.mark.parametrize('edits', [
    [{'old': 'missing', 'new': 'fixed'}],
    [{'old': 'AAA', 'new': '<script>alert(1)</script>'}],
    [{'old': 'AAA', 'new': '<img src="https://example.org/pixel.png">'}],
    [{'old': 'AAA', 'new': None}],
    ['malformed'],
])
def test_html_targeted_edits_reject_invalid_or_executable_bytes(edits):
    from nullion.cron_delivery import _instruction_aware_cron_html
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'quotes': [
        {'symbol': 'AAA', 'price': 20, 'regular_market_time': '2028-04-02T12:00:00Z'}]})]}
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return response(json.dumps({'requirements_satisfied': False, 'html': '', 'edits': edits}))
    client = Client()
    assert _instruction_aware_cron_html(Job(task='AAA の時刻'), result, model_client=client,
        existing_html='<!doctype html><html><body>AAA</body></html>') is None
    assert client.calls == 1
    assert result['cron_artifact_review_attempts'][0]['status'] == 'invalid_edits'


@pytest.mark.parametrize('configured_timeout, expected_timeout', [('120', 120), ('180', 180), ('30', 90)])
def test_html_review_uses_configured_artifact_budget(monkeypatch, configured_timeout, expected_timeout):
    from nullion.cron_delivery import _instruction_aware_cron_html
    monkeypatch.setenv('NULLION_ARTIFACT_MODEL_TIMEOUT_SECONDS', configured_timeout)
    report = '<!doctype html><html><body>AAA 20</body></html>'
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'quotes': [
        {'symbol': 'AAA', 'price': 20}]})]}
    calls = []
    class Client:
        def create(self, **kwargs):
            calls.append(kwargs)
            return response(json.dumps({'requirements_satisfied': True, 'html': report}))
    assert _instruction_aware_cron_html(Job(task='AAA の価格'), result, model_client=Client()) == report
    assert [call['timeout'] for call in calls] == [expected_timeout]
    assert [call['max_tokens'] for call in calls] == [16000]
    assert [item['phase'] for item in result['cron_artifact_review_attempts']] == ['generate']


@pytest.mark.parametrize('phase', ['generate', 'review'])
@pytest.mark.parametrize('recovers', [True, False])
def test_html_timeout_retries_same_evidence_once_with_bounded_budget(monkeypatch, phase, recovers):
    from nullion.cron_delivery import _instruction_aware_cron_html
    from nullion.model_clients import ModelClientTimeoutError
    monkeypatch.setenv('NULLION_ARTIFACT_MODEL_TIMEOUT_SECONDS', '120')
    report = '<!doctype html><html><body>AAA 20</body></html>'
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'quotes': [
        {'symbol': 'AAA', 'price': 20}]})]}
    calls = []
    class Client:
        def create(self, **kwargs):
            payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
            current_phase = 'generate' if payload['existing_html'] is None else 'review'
            calls.append((current_phase, kwargs['timeout'], payload))
            attempts = sum(name == phase for name, _, _ in calls)
            if current_phase == phase and (attempts == 1 or not recovers):
                raise ModelClientTimeoutError('Transient artifact-phase timeout')
            return response(json.dumps({'requirements_satisfied': True, 'html': report}))
    outcome = _instruction_aware_cron_html(Job(task='AAA の価格'), result, model_client=Client(),
        existing_html=report if phase == 'review' else None)
    assert outcome == (report if recovers else None)
    retried = [call for call in calls if call[0] == phase]
    assert [call[1] for call in retried] == [180 if phase == 'review' else 120, 180]
    assert retried[0][2] == retried[1][2]
    traces = [item for item in result['cron_artifact_review_attempts'] if item['phase'] == phase]
    assert [item['attempt'] for item in traces] == [1, 2]
    assert [item['status'] for item in traces] == ['error', 'accepted' if recovers else 'error']


@pytest.mark.parametrize('raw_html', [
    '<!doctype html><html><body>AAA 20</body></html>',
    '<!doctype html><html><body><script>alert(1)</script>AAA 20</body></html>',
])
def test_raw_html_generation_requires_safe_bytes_without_second_review(raw_html):
    from nullion.cron_delivery import _instruction_aware_cron_html
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'symbol': 'AAA', 'price': 20})]}
    calls = []
    class Client:
        def create(self, **kwargs):
            calls.append(kwargs)
            payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
            if payload['existing_html'] is None:
                assert 'Do not output JSON' in kwargs['system']
                return response(raw_html)
            assert payload['existing_html'] == raw_html
            return response(json.dumps({'requirements_satisfied': False, 'issues': ['specific requirement unmet']}))
    expected = None if '<script>' in raw_html else raw_html
    assert _instruction_aware_cron_html(Job(), result, model_client=Client()) == expected
    assert len(calls) == 1


def test_html_invalid_model_response_is_not_retried():
    from nullion.cron_delivery import _instruction_aware_cron_html
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'symbol': 'AAA', 'price': 20})]}
    calls = []
    class Client:
        def create(self, **kwargs):
            calls.append(kwargs)
            return response('invalid JSON')
    assert _instruction_aware_cron_html(Job(), result, model_client=Client()) is None
    assert len(calls) == 1


def test_html_targeted_edits_are_bounded_and_require_independent_acceptance():
    from nullion.cron_delivery import _instruction_aware_cron_html
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'quotes': [
        {'symbol': 'AAA', 'price': 20, 'regular_market_time': '2028-04-02T12:00:00Z'}]})]}
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
            assert payload['allow_targeted_edits'] is (self.calls < 3)
            return response(json.dumps({'requirements_satisfied': False, 'html': '',
                'issues': ['source label missing'], 'edits': [{'old': 'AAA', 'new': 'AAA delayed'}]}))
    client = Client()
    assert _instruction_aware_cron_html(Job(task='وقت AAA'), result, model_client=client,
        existing_html='<!doctype html><html><body>AAA</body></html>') is None
    assert client.calls == 3
    assert [x['status'] for x in result['cron_artifact_review_attempts']] == ['edits_proposed', 'edits_proposed', 'rejected']


def test_html_review_compacts_images_without_changing_delivered_bytes():
    from nullion.cron_delivery import _instruction_aware_cron_html
    encoded = 'A' * 300_000
    report = '<!doctype html><html><body>AAA 20<img alt="図" src="data:image/png;base64,' + encoded + '"></body></html>'
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'symbol': 'AAA', 'price': 20})]}
    class Client:
        def create(self, **kwargs):
            request = kwargs['messages'][0]['content'][0]['text']
            assert len(request) < 2000
            payload = json.loads(request)
            assert 'AAA 20' in payload['existing_html'] and 'alt="図"' in payload['existing_html']
            assert 'chars=300000' in payload['existing_html']
            assert encoded not in request
            assert len(payload['other_documents']) == 2
            return response(json.dumps({'requirements_satisfied': True, 'html': ''}))
    assert _instruction_aware_cron_html(Job(task='図と AAA の価格'), result, model_client=Client(),
        existing_html=report, existing_documents=(report, report)) == report


def test_html_review_rejects_edits_that_make_no_progress():
    from nullion.cron_delivery import _instruction_aware_cron_html
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'symbol': 'AAA', 'price': 20})]}
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return response(json.dumps({'requirements_satisfied': False, 'edits': [{'old': 'AAA', 'new': 'AAA'}]}))
    client = Client()
    assert _instruction_aware_cron_html(Job(), result, model_client=client,
        existing_html='<!doctype html><html><body>AAA</body></html>') is None
    assert client.calls == 1


@pytest.mark.parametrize('complete_before_timeout', [False, True])
@pytest.mark.parametrize('signal', ['timeout', 'incomplete'])
def test_streamed_review_decision_survives_timeout_without_restarting(complete_before_timeout, signal):
    from nullion.cron_delivery import _instruction_aware_cron_html
    from nullion.model_clients import ModelClientTimeoutError
    report = '<!doctype html><html><body>AAA 20</body></html>'
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'symbol': 'AAA', 'price': 20})]}
    decision = json.dumps({'requirements_satisfied': True, 'html': '', 'issues': [], 'edits': []})
    prefix = decision if complete_before_timeout else decision[:-2]
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                if signal == 'incomplete':
                    return response(prefix, completion={'status': 'incomplete', 'incomplete_reason': 'max_output_tokens'})
                kwargs['text_delta_callback'](prefix)
                raise ModelClientTimeoutError('Response streamed before timeout')
            assert kwargs['messages'][0]['content'][-1]['text'] == prefix
            assert kwargs['timeout'] == 180
            return response(decision[len(prefix):])
    client = Client()
    assert _instruction_aware_cron_html(Job(task='AAA の価格'), result, model_client=client, existing_html=report) == report
    assert client.calls == (1 if complete_before_timeout else 2)


def test_streamed_review_continuation_stops_when_no_progress_is_made():
    from nullion.cron_delivery import _instruction_aware_cron_html
    from nullion.model_clients import ModelClientTimeoutError
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'symbol': 'AAA', 'price': 20})]}
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                kwargs['text_delta_callback']('{"requirements_satisfied":')
            raise ModelClientTimeoutError('No further response bytes')
    client = Client()
    assert _instruction_aware_cron_html(Job(), result, model_client=client,
        existing_html='<!doctype html><html><body>AAA</body></html>') is None
    assert client.calls == 2


@pytest.mark.parametrize('continuation', ['restarted', 'malformed', 'persistent_malformed'])
def test_review_restart_keeps_original_document_and_remains_bounded(continuation):
    from nullion.cron_delivery import _instruction_aware_cron_html
    from nullion.model_clients import ModelClientTimeoutError
    report = '<!doctype html><html><body>AAA 20</body></html>'
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'symbol': 'AAA', 'price': 20})]}
    decision = json.dumps({'requirements_satisfied': True, 'html': '', 'issues': [], 'edits': []})
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            assert json.loads(kwargs['messages'][0]['content'][0]['text'])['existing_html'] == report
            if self.calls in (1, 3) and (self.calls == 1 or continuation == 'persistent_malformed'):
                kwargs['text_delta_callback']('{"requirements_satisfied":')
                raise ModelClientTimeoutError('Decision interrupted')
            if self.calls == 2 or continuation == 'persistent_malformed':
                return response(decision if continuation == 'restarted' else 'broken-json')
            assert len(kwargs['messages'][0]['content']) == 1
            return response(decision)
    client = Client()
    actual = _instruction_aware_cron_html(Job(task='AAA の価格'), result, model_client=client, existing_html=report)
    assert actual == (None if continuation == 'persistent_malformed' else report)
    assert client.calls == {'restarted': 2, 'malformed': 3, 'persistent_malformed': 4}[continuation]


def test_review_continuation_preserves_whitespace_inside_exact_edit():
    from nullion.cron_delivery import _instruction_aware_cron_html
    from nullion.model_clients import ModelClientTimeoutError
    report = '<!doctype html><html><body>AAA 20</body></html>'
    result = {'tool_results': [ToolResult('q', 'market_quote', 'completed', {'symbol': 'AAA', 'price': 20})]}
    decision = json.dumps({'requirements_satisfied': False, 'html': '', 'issues': [],
                           'edits': [{'old': 'AAA 20', 'new': 'AAA 20 更新'}]}, ensure_ascii=False)
    split = decision.index('AAA 20') + 3
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                kwargs['text_delta_callback'](decision[:split])
                raise ModelClientTimeoutError('Interrupted inside an exact substring')
            if self.calls == 2:
                return response(decision[split:])
            return response(json.dumps({'requirements_satisfied': True, 'html': ''}))
    assert _instruction_aware_cron_html(Job(task='AAA の価格'), result, model_client=Client(), existing_html=report) == report.replace('AAA 20', 'AAA 20 更新')


@pytest.mark.parametrize('first', [response(), response('partial', completion={'status': 'incomplete', 'reason': 'max_output_tokens'})])
def test_report_completion_retries_only_model_and_keeps_source_evidence(first):
    replies = iter([first, report_response('更新期限: 2028-04-09、45 USD。予定の作成はなし。')])
    calls = []
    class Client:
        def create(self, **kwargs):
            calls.append(kwargs)
            return next(replies)
    result = {'text': 'source-only', 'response_presentation': {'kind': 'source_preview'},
              'tool_results': [source_result()]}
    _repair_cron_agent_final_text_if_needed(Job(), result, model_client=Client())
    assert result['cron_agent_final_repaired'] is True
    assert result['cron_agent_final_repair_attempts'] == 2
    assert '45 USD' in result['text']
    assert [call['max_tokens'] for call in calls] == [4000, 8000]
    assert all(call['tools'] == [] and call['timeout'] == 45 for call in calls)
    assert result['tool_results'] == [source_result()]
    assert all('2028-04-09' in call['messages'][0]['content'][0]['text'] for call in calls)


@pytest.mark.parametrize('reply', [response(), report_response('No data.', source_state='empty'),
                                 report_response('Done.', evidence_refs=['nonexistent']),
                                 report_response('Done.', requirements_satisfied=False)])
def test_empty_unfounded_or_incomplete_reports_never_fulfill(reply):
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return reply
    result = {'text': 'مقتطف', 'response_presentation': {'kind': 'source_summary'}, 'tool_results': [source_result()]}
    client = Client()
    _repair_cron_agent_final_text_if_needed(Job(task='اقرأ الرسائل واذكر المواعيد'), result, model_client=client)
    assert result['cron_agent_final_repair_failed'] is True
    assert result['response_fulfilled'] is False
    assert client.calls == 2


def test_early_quotes_survive_later_browser_noise_and_timeout():
    rows = [{'symbol': f'X{i:02}', 'price': 20+i, 'change_percent': i-8,
             'regular_market_time': '2028-04-02T12:00:00Z'} for i in range(16)]
    results = [ToolResult('quotes', 'market_quote', 'completed', {'quotes': rows})]
    results += [ToolResult(f'b{i}', 'browser_extract_text', 'completed', {'text': 'navigation text ' * 400}) for i in range(30)]
    results += [ToolResult('timeout', 'browser_navigate', 'failed', {}, 'timeout')]
    evidence = _cron_final_repair_tool_evidence(results)
    assert len(evidence) < 9500
    data = json.loads(evidence)
    retained = data[0]['source_records']
    assert [row['symbol'] for row in retained] == [row['symbol'] for row in rows]
    assert retained[0]['price'] == 20
    assert 'regular_market_time' in retained[-1]


def test_sanitizer_records_source_provenance_without_parsing_language():
    metadata = {}
    result = source_result()
    result.output['reply_metadata'] = {'reply_contains_internal_tool_state': True}
    text = sanitize_user_visible_reply(user_message='اقرأ الرسالة', reply='حالة داخلية', tool_results=[result],
                                      presentation_metadata=metadata)
    assert text
    assert metadata['kind'] in {'source_summary', 'source_preview'}
    assert metadata['fulfillment_satisfied'] is False


@pytest.mark.parametrize('channel', ['telegram', 'web'])
def test_failure_notice_delivered_but_execution_failed(channel):
    delivered = []
    events = []
    result = run_cron_delivery_workflow(Job(), label='Scheduled task', callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda _: channel, delivery_target=lambda *_: 'example',
        run_agent_turn=lambda *_: {'text': 'Report could not finish.', 'artifacts': [], 'tool_results': [],
                                  'cron_report_failed': True, 'cron_failure_notice': True,
                                  'response_fulfilled': False, 'reason': 'cron_report_incomplete'},
        block_reason=cron_delivery_block_reason, record_event=lambda event, *args, **kw: events.append(event),
        send_platform_delivery=lambda *args: delivered.append(args[-1]) or True,
        save_web_delivery=lambda *args: delivered.append(args[2]) or True,
    ))
    assert len(delivered) == 1 and 'Report could not finish.' in delivered[0]
    assert result['cron_run_failed'] is True
    assert result['execution_outcome']['execution_succeeded'] is False
    assert result['cron_delivery_status'] in {'sent', 'saved'}
    assert events[-1] in {'cron.delivery.sent', 'cron.delivery.saved'}


def test_legacy_file_contract_is_cached_and_invalidated_by_instructions(tmp_path):
    from nullion.runtime_cache import clear_hot_cache
    checkpoint = tmp_path / "runtime.db"
    clear_hot_cache()
    calls = []
    class Client:
        def create(self, **kwargs):
            calls.append(kwargs)
            return response(json.dumps({'delivery_kind': 'attachment', 'required_artifact_extensions': ['.html'],
                                        'required_artifact_content_tokens': []}))
    job = Job(task='.htmlを作成して送信')
    resolved = _resolve_missing_cron_artifact_contract(job, Client(), checkpoint_path=checkpoint)
    assert resolved.artifact_delivery_options['requires_attachment_delivery'] is True
    assert job.artifact_delivery_options == {}
    _resolve_missing_cron_artifact_contract(job, Client(), checkpoint_path=checkpoint)
    assert len(calls) == 1
    job.task = 'تقرير .html مع المصادر'
    _resolve_missing_cron_artifact_contract(job, Client(), checkpoint_path=checkpoint)
    assert len(calls) == 2
    _resolve_missing_cron_artifact_contract(Job(task='こんにちは'), Client(), checkpoint_path=checkpoint)
    assert len(calls) == 2



def test_legacy_contract_cache_never_defaults_to_running_instance(monkeypatch):
    from nullion import runtime_cache
    runtime_cache.clear_hot_cache()
    options = []
    original_get, original_set = runtime_cache.get_json, runtime_cache.set_json
    def get(*args, **kwargs):
        options.append(kwargs)
        return original_get(*args, **kwargs)
    def put(*args, **kwargs):
        options.append(kwargs)
        return original_set(*args, **kwargs)
    monkeypatch.setattr(runtime_cache, "get_json", get)
    monkeypatch.setattr(runtime_cache, "set_json", put)
    class Client:
        def create(self, **kwargs):
            return response(json.dumps({"delivery_kind": "attachment", "required_artifact_extensions": [".html"]}))
    _resolve_missing_cron_artifact_contract(Job(task=".htmlを添付"), Client())
    assert len(options) == 2
    assert all(item["persistent"] is False and item["db_path"] is None for item in options)


def test_contract_cache_is_separate_for_each_runtime(tmp_path):
    from nullion.runtime_cache import clear_hot_cache
    clear_hot_cache()
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return response(json.dumps({"delivery_kind": "attachment", "required_artifact_extensions": [".html"]}))
    client = Client()
    job = Job(task=".htmlを添付")
    for runtime in ("first.db", "second.db", "first.db"):
        _resolve_missing_cron_artifact_contract(job, client, checkpoint_path=tmp_path/runtime)
    assert client.calls == 2


@pytest.mark.parametrize("status", ["incomplete", "failed", "cancelled"])
def test_incomplete_contract_decision_is_never_cached(status):
    class Client:
        def create(self, **kwargs):
            return response(json.dumps({"delivery_kind": "attachment", "required_artifact_extensions": [".html"]}),
                            completion={"status": status})
    from nullion.runtime_cache import clear_hot_cache
    clear_hot_cache()
    with pytest.raises(ValueError, match="cron_delivery_contract_unresolved"):
        _resolve_missing_cron_artifact_contract(Job(task=".htmlを添付"), Client())


def test_source_provenance_preserves_generator_evidence():
    metadata = {}
    result = sanitize_user_visible_reply(user_message="メール", reply="", tool_results=iter([source_result()]),
                                         presentation_metadata=metadata)
    assert result and "更新" in result
    assert metadata["kind"] in {"source_preview", "source_summary"}


def test_html_recovery_cannot_fulfill_an_additional_required_format():
    from nullion.cron_delivery import _recover_typed_html_artifact_after_terminal_limit
    result = {"artifact_delivery_satisfied": False, "tool_results": [source_result()]}
    job = Job(artifact_delivery_options={"requires_attachment_delivery": True,
              "required_artifact_extensions": [".html", ".pdf"]})
    assert _recover_typed_html_artifact_after_terminal_limit(job, "example", result) is False
    assert result["artifact_delivery_satisfied"] is False

def test_cron_management_tools_persist_and_update_attachment_contract(tmp_path, monkeypatch):
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    checkpoint = tmp_path/'runtime.db'
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', checkpoint)
    bootstrap_persistent_runtime(checkpoint)
    registry = ToolRegistry()
    register_cron_tools(registry)
    options = {'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html']}
    created = registry.invoke(ToolInvocation('create', 'create_cron', 'telegram:example',
        {'name': '例', 'schedule': '0 8 * * 1-5', 'task': '添付', 'artifact_delivery_options': options}))
    assert created.status == 'completed', created
    job = crons.get_cron(created.output['id'])
    assert job.artifact_delivery_options == options
    updated = registry.invoke(ToolInvocation('update', 'update_cron', 'telegram:example',
        {'id': job.id, 'artifact_delivery_options': {**options, 'required_artifact_extensions': ['.pdf']}}))
    assert updated.status == 'completed', updated
    assert crons.get_cron(job.id).artifact_delivery_options['required_artifact_extensions'] == ['.pdf']


@pytest.mark.parametrize('status', ['incomplete', 'completed'])
def test_provider_stream_preserves_completion_status_and_reasoning_usage(monkeypatch, status):
    from nullion import model_clients
    terminal = {'status': status, 'incomplete_details': {'reason': 'max_output_tokens'},
                'usage': {'output_tokens': 1400, 'output_tokens_details': {'reasoning_tokens': 1400}}}
    class Response:
        status_code = 200
        reason_phrase = 'OK'
        def raise_for_status(self): pass
        def iter_lines(self):
            yield 'data: '+json.dumps({'type': 'response.'+status, 'response': terminal})
            raise AssertionError('terminal event must stop stream iteration')
    class Stream:
        def __enter__(self): return Response()
        def __exit__(self, *args): return False
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def stream(self, *args, **kwargs): return Stream()
    monkeypatch.setattr(model_clients, '_httpx', SimpleNamespace(Client=Client))
    model = model_clients.CodexResponsesModelClient(api_key='synthetic', account_id='synthetic', model='test')
    if status == 'completed':
        with pytest.raises(model_clients.ModelClientResponseError, match='empty_response'):
            model.create(messages=[{'role': 'user', 'content': '例'}], tools=[])
        return
    reply = model.create(messages=[{'role': 'user', 'content': '例'}], tools=[])
    assert reply['completion']['status'] == status
    assert reply['completion']['reason'] == 'max_output_tokens'
    assert reply['completion']['usage']['reasoning_tokens'] == 1400
    assert reply['content'] == []


@pytest.mark.parametrize('failure', [
    {'cron_report_failed': True}, {'cron_run_failed': True},
    {'execution_outcome': {'execution_succeeded': False}},
])
def test_manual_cron_report_failure_is_not_successful_even_when_notice_was_sent(tmp_path, monkeypatch, failure):
    from nullion import crons
    from nullion.tools import _build_run_cron_handler
    from nullion.runtime import bootstrap_persistent_runtime
    checkpoint = tmp_path/'runtime.db'
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', checkpoint)
    bootstrap_persistent_runtime(checkpoint)
    job = crons.add_cron('Reporte', '* * * * *', '期限を報告', workspace_id='workspace-example')
    result = _build_run_cron_handler(lambda _: {'text': 'Scheduled task could not finish.',
        'cron_delivery_status': 'sent', 'reason': 'cron_report_incomplete', **failure})(
        ToolInvocation('manual-report', 'run_cron', 'workspace:workspace-example', {'id': job.id}))
    assert result.status == 'failed'
    assert result.output['reason'] == 'cron_report_incomplete'
    assert result.output['delivery_status'] == 'sent'
    assert 'approval' not in result.error.lower()
    assert crons.get_cron(job.id).last_result == 'manual run failed: cron_report_incomplete'


def test_all_hydrated_email_obligations_survive_many_earlier_searches():
    searches = [ToolResult(f's{i}', 'email_search', 'completed', {'results': [
        {'subject': f'Preview {j}', 'snippet': 'Search preview ' * 20} for j in range(20)]}) for i in range(13)]
    reads = [ToolResult(f'r{i}', 'email_read', 'completed', {'subject': f'Read {i}',
        'body': f'Unique obligation {i:02}: action due 2028-04-{i+1:02}. '+('Supporting detail ' * 28)}) for i in range(28)]
    results = searches + reads + [ToolResult(f'c{i}', 'calendar_list', 'completed', {'events': []}) for i in range(2)]
    evidence = _cron_final_repair_tool_evidence(results)
    assert all(f'Unique obligation {i:02}:' in evidence for i in range(28))
    assert len(evidence) <= 32_000
    records = json.loads(evidence)
    assert [record['tool_name'] for record in records[:28]] == ['email_read'] * 28
    assert sum(record.get('tool_name') == 'calendar_list' for record in records) == 2
