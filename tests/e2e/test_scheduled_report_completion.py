from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


def text_response(text):
    return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': text}]}


def tool_response(name, arguments):
    return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': name+'-call', 'name': name, 'input': arguments}]}


def setup_runtime(tmp_path, monkeypatch):
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    checkpoint = tmp_path/'runtime.db'
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', checkpoint)
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path/'data'))
    return bootstrap_persistent_runtime(checkpoint)


def telegram_boundary(monkeypatch):
    from nullion import telegram_app
    deliveries = []
    class Bot:
        def __init__(self, token): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def send_message(self, chat_id, text, **kwargs):
            deliveries.append({'text': text, 'chat_id': chat_id})
            return SimpleNamespace(message_id=len(deliveries))
        async def send_document(self, chat_id, document, caption=None, **kwargs):
            deliveries.append({'bytes': document.read(), 'path': document.name, 'caption': caption, 'chat_id': chat_id})
            return SimpleNamespace(message_id=len(deliveries))
    monkeypatch.setitem(sys.modules, 'telegram', SimpleNamespace(Bot=Bot))
    def send(job, channel, target, text):
        assert channel == 'telegram'
        return asyncio.run(telegram_app._send_operator_telegram_message('synthetic-token', target, text,
                           principal_id='telegram:'+target))
    return deliveries, send


def run_scheduled(job, client, runtime, registry, send):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn, run_cron_delivery_workflow, CronRunDeliveryCallbacks, cron_delivery_block_reason
    from nullion.events import make_event
    def record(event, job, channel, target, conversation_id, **extra):
        runtime.store.add_event(make_event(event, 'cron_scheduler', {'cron_id': job.id, 'channel': channel, **extra}))
    return run_cron_delivery_workflow(job, label='Scheduled task', callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda job: job.delivery_channel, delivery_target=lambda job, channel: job.delivery_target,
        run_agent_turn=lambda job, conversation: run_single_agent_cron_turn(job, conversation, label='Scheduled task',
            orchestrator=AgentOrchestrator(model_client=client), runtime=runtime, tool_registry=registry,
            settings=SimpleNamespace(enabled_skill_packs=()), record_event=record),
        record_event=record, block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda *args: None, send_platform_delivery=send,
    ))


@pytest.mark.parametrize('persist_ok', [True, False])
def test_web_scheduler_records_delivery_only_after_actual_chat_storage(tmp_path, monkeypatch, persist_ok):
    from fastapi.testclient import TestClient
    from nullion import crons, config, web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, register_market_quote_tool
    runtime=setup_runtime(tmp_path,monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-network',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_market_quote_tool(registry,quote_fetcher=lambda symbol,timeout:{'symbol':symbol,'price':20})
    class Model:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            if self.calls==1: return tool_response('market_quote',{'symbols':['AAA']})
            return text_response('**確認済み**\nAAA: 20 USD.')
    settings=NullionSettings(doctor_enabled=False)
    monkeypatch.setattr(config,'load_settings',lambda *a,**k:settings)
    monkeypatch.setattr(web_app,'load_settings',lambda *a,**k:settings)
    schedulers=[]
    monkeypatch.setattr(crons.CronScheduler,'start',lambda self:schedulers.append(self))
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=Model()),registry=registry)
    with TestClient(app) as client:
        created=client.post('/api/crons',json={'name':'確認','schedule':'* * * * *','task':'AAAの価格を報告。',
            'enabled':True,'delivery_channel':'web','delivery_target':'web:operator'}).json()
        ident=created['job']['id']; job=crons.get_cron(ident)
        job.next_run=(datetime.now(UTC)-timedelta(minutes=1)).isoformat();crons.save_crons([job])
        if not persist_ok:
            def unavailable_storage(*args,**kwargs): raise OSError('Simulated unavailable storage boundary')
            monkeypatch.setattr(chat_store.ChatStore,'save_message',unavailable_storage)
        schedulers[-1]._tick()
        from cron_scheduler_helpers import finish_scheduler
        finish_scheduler(schedulers[-1])
        events=[event for event in runtime.store.list_events() if event.payload.get('cron_id')==ident]
        assert any(event.event_type=='cron.delivery.saved' for event in events) is persist_ok
        assert any(event.event_type=='cron.delivery.failed' for event in events) is not persist_ok
        receipts=[row for row in runtime.store.list_recent_conversation_events('web:operator') if row.get('cron_id')==ident]
        assert bool(receipts) is persist_ok
        if persist_ok:
            assert '20 USD' in receipts[-1]['assistant_reply']
        else:
            assert str(crons.get_cron(ident).last_result).startswith('error:')


def test_web_scheduler_telegram_delivery_is_saved_in_actual_chat_history(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import crons, config, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, register_market_quote_tool
    runtime = setup_runtime(tmp_path, monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        rule_id='history-network', principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector='*',
        created_by='fixture-user', created_at=datetime.now(UTC)))
    deliveries, _ = telegram_boundary(monkeypatch)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_market_quote_tool(registry, quote_fetcher=lambda symbol, timeout: {'symbol': symbol, 'price': 20})
    class Client:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return tool_response('market_quote', {'symbols': ['AAA']})
            return text_response('AAA السعر 20 USD.')
    settings = NullionSettings(doctor_enabled=False, telegram=TelegramSettings(bot_token='synthetic-token'))
    monkeypatch.setattr(config, 'load_settings', lambda *args, **kwargs: settings)
    monkeypatch.setattr(web_app, 'load_settings', lambda *args, **kwargs: settings)
    schedulers = []
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: schedulers.append(self))
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=Client()), registry=registry)
    with TestClient(app) as client:
        created = client.post('/api/crons', json={'name': 'ملخص', 'schedule': '* * * * *',
            'task': 'أخبرني بسعر AAA', 'enabled': True, 'delivery_channel': 'telegram', 'delivery_target': '2222'}).json()
        ident = created['job']['id']
        job = crons.get_cron(ident)
        job.next_run = (datetime.now(UTC)-timedelta(minutes=1)).isoformat()
        crons.save_crons([job])
        schedulers[-1]._tick()
        from cron_scheduler_helpers import finish_scheduler
        finish_scheduler(schedulers[-1])
        with sqlite3.connect(tmp_path/'runtime.db') as connection:
            persisted = connection.execute(
                "SELECT payload FROM conversation_events WHERE json_extract(payload, '$.cron_id') = ?",
                (ident,),
            ).fetchall()
        assert len(persisted) == 1 and '20 USD' in json.loads(persisted[0][0])['assistant_reply']
        assert crons.get_cron(ident).last_result == 'ok'
        assert deliveries and '20 USD' in deliveries[-1]['text']
        turns = runtime.store.list_recent_conversation_events('telegram:2222', event_type='conversation.chat_turn', limit=10)
        assert any(turn.get('source') == 'cron' and turn.get('cron_id') == ident and '20 USD' in turn['assistant_reply'] for turn in turns)
        history = client.get('/api/chat/history/telegram%3A2222?sync_runtime=1').json()
        assert any(message['role'] == 'bot' and '20 USD' in message['text'] for message in history['messages'])


@pytest.mark.parametrize('instructions', ['AAA の価格とニュースを .html に保存', 'احفظ سعر AAA وأخباره في .html'])
def test_scheduled_file_report_keeps_research_tools_until_sources_are_complete(tmp_path, monkeypatch, instructions, sourced_report_model_reply):
    from nullion import crons
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
                              register_market_quote_tool, register_web_fetch_tool, _build_file_write_handler)
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.FILESYSTEM_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id='source-fixture-'+kind.value, principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind, mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    deliveries, send = telegram_boundary(monkeypatch)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_market_quote_tool(registry, quote_fetcher=lambda symbol, timeout: {'symbol': symbol, 'price': 20})
    reads = []
    def fetch(url, timeout):
        reads.append(url)
        return {'url': url, 'title': '新製品', 'text': 'AAA announced a new product on 2028-04-02.', 'status_code': 200}
    register_web_fetch_tool(registry, fetch)
    registry.register(ToolSpec(name='file_write', description='Write report bytes', risk_level=ToolRiskLevel.MEDIUM,
        side_effect_class=ToolSideEffectClass.WRITE, requires_approval=False, timeout_seconds=20),
        _build_file_write_handler(allowed_roots=(tmp_path,)))
    # This fixture drives each run explicitly. Keep automatic timers from
    # executing the same job at a minute boundary while the suite is running.
    job = crons.add_cron('例', '* * * * *', instructions, enabled=False, delivery_channel='telegram', delivery_target='2222',
        artifact_delivery_options={'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html'],
                                   'required_artifact_content_tokens': ['AAA']})
    path = artifact_root_for_principal('telegram:2222')/'complete-report.html'
    html = '<!doctype html><html><body><article>AAA 20: new product announced 2028-04-02.</article></body></html>'
    class Client:
        calls = 0
        def create(self, **kwargs):
            review = sourced_report_model_reply(kwargs, source_tools=['market_quote', 'web_fetch'],
                producer_tool='file_write', expected_tokens=['AAA 20', 'new product announced 2028-04-02'])
            if review is not None:
                return review
            names = {tool['name'] for tool in kwargs.get('tools', [])}
            if not names:
                payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
                assert payload['existing_html'] == html
                return text_response(json.dumps({'requirements_satisfied': True, 'html': ''}))
            self.calls += 1
            if self.calls == 1:
                return tool_response('market_quote', {'symbols': ['AAA']})
            if self.calls == 2:
                if 'web_fetch' not in names:
                    time.sleep(0.25)
                    raise TimeoutError('Forced file production before the news source could be read')
                return tool_response('web_fetch', {'url': 'https://example.org/news/aaa'})
            if self.calls == 3:
                if kwargs.get('timeout', 0) < 120:
                    raise TimeoutError('The report needs its file-generation budget while research tools remain visible')
                assert reads == ['https://example.org/news/aaa']
                assert 'new product' in json.dumps(kwargs['messages'])
                return tool_response('file_write', {'path': str(path), 'content': html})
            return text_response('Report attached.\nMEDIA:'+str(path))
    elapsed = []
    for _ in range(3):
        reads.clear()
        client = Client()
        delivery_start = len(deliveries)
        started = time.monotonic()
        result = run_scheduled(job, client, runtime, registry, send)
        elapsed.append(time.monotonic()-started)
        assert reads == ['https://example.org/news/aaa']
        assert result['artifact_delivery_satisfied'] is True, result
        assert result['execution_outcome']['execution_succeeded'] is True, result
        assert client.calls <= 4
        sent = deliveries[delivery_start:]
        assert len(sent) == 1, [{key: value for key, value in item.items() if key != 'bytes'} for item in sent]
    assert len(deliveries) == 3 and all(item['bytes'] == path.read_bytes() for item in deliveries)
    assert all(b'AAA 20: new product announced 2028-04-02.' in item['bytes'] for item in deliveries)
    assert sum(elapsed)/len(elapsed) < 2.5 and max(elapsed) < 3.5, elapsed
    assert 'AAA 20: new product announced 2028-04-02.' in path.read_text()
    assert any(event.event_type == 'tool.invocation.completed' and event.payload.get('tool_name') == 'web_fetch'
               for event in runtime.store.list_events())


@pytest.mark.parametrize('instructions', ['価格と時刻を表示', 'اعرض السعر والوقت', 'Show price and time'])
@pytest.mark.parametrize('review_failure', ['reject', 'timeout', 'malformed'])
def test_completed_report_delivers_without_extra_model_review(tmp_path, monkeypatch, instructions, review_failure):
    from nullion import crons
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, register_market_quote_tool, _build_file_write_handler
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.FILESYSTEM_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id=kind.value, principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind, mode=PolicyMode.ALLOW, selector='*', created_by='fixture', created_at=datetime.now(UTC)))
    deliveries, send = telegram_boundary(monkeypatch)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_market_quote_tool(registry, quote_fetcher=lambda symbol, timeout: {'symbol':symbol, 'price':20})
    registry.register(ToolSpec(name='file_write', description='Write a file', risk_level=ToolRiskLevel.MEDIUM,
        side_effect_class=ToolSideEffectClass.WRITE, requires_approval=False, timeout_seconds=20),
        _build_file_write_handler(allowed_roots=(tmp_path,)))
    job = crons.add_cron('例', '* * * * *', instructions, delivery_channel='telegram', delivery_target='2222',
        artifact_delivery_options={'requires_attachment_delivery':True, 'required_artifact_extensions':['.html']})
    path = artifact_root_for_principal('telegram:2222')/'report.html'
    document = '<!doctype html><html><body>AAA 20</body></html>'
    class Client:
        calls = 0
        reviews = 0
        def create(self, **kwargs):
            if not kwargs.get('tools'):
                self.reviews += 1
                if review_failure == 'timeout': raise TimeoutError('Former review dependency')
                return text_response('broken-json' if review_failure == 'malformed' else '{"requirements_satisfied":false}')
            self.calls += 1
            if self.calls == 1: return tool_response('market_quote', {'symbols':['AAA']})
            if self.calls == 2: return tool_response('file_write', {'path':str(path), 'content':document})
            return text_response('Report attached.\nMEDIA:'+str(path))
    client = Client()
    elapsed = []
    for _ in range(3):
        client.calls = 0
        started = time.monotonic()
        result = run_scheduled(job, client, runtime, registry, send)
        elapsed.append(time.monotonic()-started)
        assert result['execution_outcome']['execution_succeeded'] is True, result
        assert result['artifact_delivery_satisfied'] is True
    assert client.reviews == 0
    assert len(deliveries) == 3
    assert all(item['bytes'] == Path(item['path']).read_bytes() for item in deliveries)
    assert all(b'<body>AAA 20</body>' in item['bytes'] for item in deliveries)
    assert sum(elapsed)/len(elapsed) < 2.5 and max(elapsed) < 3.5, elapsed
    assert any(event.event_type == 'tool.invocation.completed' and event.payload.get('tool_name') == 'file_write'
               for event in runtime.store.list_events())


@pytest.mark.parametrize('recover', [True, False])
def test_scheduled_email_report_empty_repair_delivers_correct_result_and_persists_status(tmp_path, monkeypatch, recover):
    from nullion import crons
    from nullion.tools import ToolRegistry, register_email_plugin
    runtime = setup_runtime(tmp_path, monkeypatch)
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id='fixture-'+kind.value, principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind, mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    deliveries, send = telegram_boundary(monkeypatch)
    reads = []
    registry = ToolRegistry()
    def reader(identifier):
        reads.append(identifier)
        return {'subject': 'Renovación', 'from': 'Example service', 'date': '2028-04-02',
                'snippet': 'The renewal deadline is 2028-04-09; amount 45 USD.',
                'body': 'The renewal deadline is 2028-04-09; amount 45 USD.'}
    register_email_plugin(registry, email_searcher=lambda query, limit: [{'id': '9bf5c72109a44e92', 'subject': 'Renovación'}],
                          email_reader=reader)
    job = crons.add_cron('Reporte', '* * * * *', 'Lee el correo y resume fechas y obligaciones.',
                         delivery_channel='telegram', delivery_target='2222')
    job.next_run = (datetime.now(UTC)-timedelta(minutes=1)).isoformat()
    crons.save_crons([job])
    class Client:
        agent_calls = 0
        repair_calls = 0
        def create(self, **kwargs):
            if not kwargs.get('tools'):
                # Only report synthesis may run here; no writes or source rereads.
                self.repair_calls += 1
                if self.repair_calls == 1 or not recover:
                    return {'stop_reason': 'end_turn', 'content': [{'type': 'thinking', 'text': 'reasoning only'}],
                            'completion': {'status': 'incomplete', 'reason': 'max_output_tokens'}}
                return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': ['e0'], 'report': 'Renovación: vence el 2028-04-09, por 45 USD. No se creó ningún evento.'}))
            self.agent_calls += 1
            if self.agent_calls == 1: return tool_response('email_search', {'query': 'obligations', 'limit': 10})
            if self.agent_calls == 2: return tool_response('email_read', {'id': '9bf5c72109a44e92'})
            return text_response('I checked your email.\n\n**Renovación**\nFrom: Example service\nDate: 2028-04-02\n\nPreview: The renewal deadline is 2028-04-09; amount 45 USD.')
    client = Client()
    outcomes = []
    def fire(job):
        result = run_scheduled(job, client, runtime, registry, send)
        outcomes.append(result)
        return result
    scheduler=crons.CronScheduler(fire)
    scheduler._tick()
    from cron_scheduler_helpers import finish_scheduler
    finish_scheduler(scheduler)
    assert len(deliveries) == 1, ([(item.tool_name, item.status, item.output.get('reason'), item.error) for item in outcomes[0]['tool_results']], reads, client.agent_calls)
    assert reads == ['9bf5c72109a44e92']
    stored = crons.get_cron(job.id)
    result = outcomes[0]
    assert client.repair_calls == 2, result
    assert result['cron_delivery_status'] == 'sent'
    if recover:
        assert '2028-04-09' in deliveries[0]['text'] and '45 USD' in deliveries[0]['text']
        assert 'Preview:' not in deliveries[0]['text']
        assert stored.last_result == 'ok'
        assert result['execution_outcome']['execution_succeeded'] is True
    else:
        assert 'Some source checks returned information' in deliveries[0]['text']
        assert '45 USD' not in deliveries[0]['text']
        assert stored.last_result == 'error: cron_report_incomplete'
        assert result['execution_outcome']['execution_succeeded'] is False
    assert any(event.event_type == 'cron.delivery.sent' for event in runtime.store.list_events())
    assert stored.next_run_dt() > datetime.now(UTC)


@pytest.mark.parametrize('recover', [True, False])
@pytest.mark.parametrize('transient_phase', [None, 'generate', 'review', 'generate_stream',
                                          'generate_complete_stream', 'generate_incomplete',
                                          'generate_stream_two', 'review_budget'])
def test_legacy_html_job_recovers_after_source_success_or_reports_failure_without_false_ok(tmp_path, monkeypatch, recover, transient_phase):
    from nullion import crons
    from nullion.tools import ToolRegistry, register_market_quote_tool
    from playwright.sync_api import sync_playwright
    runtime = setup_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_ARTIFACT_MODEL_TIMEOUT_SECONDS', '120')
    deliveries, send = telegram_boundary(monkeypatch)
    registry = ToolRegistry()
    quote_calls = []
    def quotes(symbol, timeout):
        quote_calls.append(symbol)
        return {'symbol': symbol, 'price': 20 if symbol == 'AAA' else 10, 'change_percent': 2 if symbol == 'AAA' else -2,
                'regular_market_time': '2028-04-02T12:00:00Z'}
    register_market_quote_tool(registry, quote_fetcher=quotes)
    instructions = 'AAAとBBBの .html を添付。上昇は緑、下落は赤、時刻と価格を表示。'
    job = crons.add_cron('例', '* * * * *', instructions, delivery_channel='telegram', delivery_target='2222')
    job.next_run = (datetime.now(UTC)-timedelta(minutes=1)).isoformat()
    crons.save_crons([job])
    html = '<!doctype html><html><body><main><article style="background:green">AAA 20 +2% 2028-04-02 12:00 UTC</article><article style="background:red">BBB 10 -2% 2028-04-02 12:00 UTC</article></main></body></html>'
    # Split at a space: dropping prefix/suffix whitespace would corrupt the
    # visible source-backed value, even if the resulting HTML still parses.
    prefix = html[:html.index(' 20') + 1]
    class Client:
        agent_calls = 0
        contract_calls = 0
        recovery_calls = 0
        phase_calls = {}
        def create(self, **kwargs):
            if not kwargs.get('tools'):
                payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
                if 'extensions' in payload:
                    self.contract_calls += 1
                    return text_response(json.dumps({'delivery_kind': 'attachment', 'required_artifact_extensions': ['.html'],
                                                    'required_artifact_content_tokens': ['AAA', 'BBB']}))
                self.recovery_calls += 1
                assert payload['instructions'] == instructions
                assert payload['verification_scope'] == ['document_content', 'layout', 'source_grounding', 'required_source_checks', 'required_actions']
                assert payload['delivery_state'] == 'pending_runtime_delivery'
                rows = payload['verified_source_records']
                assert any(row.get('symbol') == 'AAA' and row.get('price') == 20 for row in rows)
                assert any(row.get('symbol') == 'BBB' and row.get('change_percent') == -2 for row in rows)
                phase = 'generate' if payload['existing_html'] is None else 'review'
                self.phase_calls[phase] = self.phase_calls.get(phase, 0) + 1
                if phase == 'review' and transient_phase == 'review_budget':
                    # External provider consumes reasoning tokens before its
                    # decision. The former 4k cap returned no output at all.
                    if kwargs['max_tokens'] < 4600:
                        response = text_response('')
                        response['completion'] = {
                            'status': 'incomplete', 'reason': 'max_output_tokens',
                            'usage': {'output_tokens': kwargs['max_tokens'],
                                      'reasoning_tokens': kwargs['max_tokens']},
                        }
                        return response
                if phase == 'generate' and transient_phase in {
                    'generate_stream', 'generate_complete_stream', 'generate_incomplete',
                    'generate_stream_two'
                }:
                    if self.phase_calls[phase] == 1:
                        fragment = html if transient_phase == 'generate_complete_stream' else prefix
                        kwargs['text_delta_callback'](fragment)
                        time.sleep(0.03)
                        if transient_phase == 'generate_incomplete':
                            response = text_response(fragment)
                            response['completion'] = {'status': 'incomplete', 'incomplete_reason': 'max_output_tokens'}
                            return response
                        from nullion.model_clients import ModelClientTimeoutError
                        raise ModelClientTimeoutError('Stream interrupted after document progress')
                    assert 'document_prefix' not in payload
                    received_prefix = kwargs['messages'][0]['content'][1]['text']
                    expected_prefix = prefix if self.phase_calls[phase] == 2 else html[:len(prefix) + 12]
                    assert received_prefix == expected_prefix
                    assert kwargs['timeout'] == (180 if self.phase_calls[phase] == 2 else 90)
                    assert 'raw HTML, never a JSON-escaped string' in kwargs['system']
                    if transient_phase == 'generate_stream_two' and self.phase_calls[phase] == 2:
                        kwargs['text_delta_callback'](html[len(prefix):len(prefix) + 12])
                        time.sleep(0.03)
                        from nullion.model_clients import ModelClientTimeoutError
                        raise ModelClientTimeoutError('Second stream made progress before interruption')
                    return text_response(html[len(received_prefix):])
                if phase == transient_phase and self.phase_calls[phase] == 1:
                    from nullion.model_clients import ModelClientTimeoutError
                    time.sleep(0.03)
                    raise ModelClientTimeoutError('Previously discarded artifact after a transient timeout')
                if phase == transient_phase:
                    assert kwargs['timeout'] == 180
                if phase == 'generate' and recover:
                    assert 'Do not output JSON' in kwargs['system']
                    return text_response(html)
                return text_response(json.dumps({'requirements_satisfied': recover, 'html': html if recover else ''}))
            self.agent_calls += 1
            if self.agent_calls == 1: return tool_response('market_quote', {'symbols': ['AAA', 'BBB']})
            raise TimeoutError('external provider timed out after the source results')
    client = Client()
    results = []
    def fire(job):
        result = run_scheduled(job, client, runtime, registry, send)
        results.append(result)
        return result
    runs = 3 if recover and transient_phase else 1
    elapsed = []
    for _ in range(runs):
        client.agent_calls = 0
        client.phase_calls = {}
        due = crons.get_cron(job.id)
        due.next_run = (datetime.now(UTC)-timedelta(minutes=1)).isoformat()
        crons.save_crons([due])
        started = time.monotonic()
        scheduler=crons.CronScheduler(fire)
        scheduler._tick()
        from cron_scheduler_helpers import finish_scheduler
        finish_scheduler(scheduler)
        elapsed.append(time.monotonic() - started)
    result = results[0]
    assert client.contract_calls == 1
    assert sorted(quote_calls) == sorted(['AAA', 'BBB'] * runs)
    assert len(deliveries) == runs, result
    if transient_phase in {'generate_stream', 'generate_complete_stream', 'generate_incomplete',
                           'generate_stream_two'}:
        trace = result['cron_artifact_review_attempts']
        assert trace[0]['partial_document_chars'] == len(html if transient_phase == 'generate_complete_stream' else prefix)
        if transient_phase != 'generate_complete_stream':
            assert trace[1]['prefix_chars'] == len(prefix)
        assert all(attempt['phase'] == 'generate' for attempt in trace)
    if runs > 1:
        assert sum(elapsed)/len(elapsed) < 2.5 and max(elapsed) < 3.5, elapsed
    stored = crons.get_cron(job.id)
    if recover or transient_phase in {'generate_stream', 'generate_complete_stream', 'generate_incomplete', 'generate_stream_two'}:
        assert stored.last_result == 'ok'
        assert 'bytes' in deliveries[0]
        delivered_html = deliveries[0]['bytes'].decode()
        assert 'AAA 20' in delivered_html and 'BBB 10' in delivered_html
        assert 'MEDIA:' not in (deliveries[0]['caption'] or '')
        with sync_playwright() as api:
            browser = api.chromium.launch()
            page = browser.new_page(viewport={'width': 390, 'height': 844})
            page.set_content(delivered_html)
            assert page.locator('article').count() == 2
            colors = page.locator('article').evaluate_all('nodes => nodes.map(n => getComputedStyle(n).backgroundColor)')
            assert colors == ['rgb(0, 128, 0)', 'rgb(255, 0, 0)']
            assert page.evaluate('document.body.scrollWidth <= innerWidth')
            browser.close()
        receipts = [json.loads(line) for line in (tmp_path/'data'/'delivery_receipts.jsonl').read_text().splitlines()]
        assert receipts[-1]['status'] == 'succeeded' and receipts[-1]['attachment_count'] == 1
    else:
        assert stored.last_result == 'error: cron_required_artifact_missing'
        assert 'bytes' not in deliveries[0]
        assert 'could not create the required report file' in deliveries[0]['text']
        assert result['execution_outcome']['execution_succeeded'] is False
    assert result['cron_delivery_status'] == 'sent'


@pytest.mark.parametrize('instructions', ['全メールの期限と対応を要約', 'لخّص مواعيد وإجراءات كل الرسائل'])
def test_email_report_after_43_source_calls_preserves_last_read_and_delivers_summary(tmp_path, monkeypatch, instructions):
    from nullion import crons
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, register_email_plugin, register_calendar_plugin
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id='many-read-'+kind.value, principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind, mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    deliveries, send = telegram_boundary(monkeypatch)
    ids = [f'{i+1:016x}' for i in range(28)]
    reads, searches, calendars = [], [], []
    registry = ToolRegistry()
    def search(query, limit):
        searches.append(query)
        return [{'id': identifier, 'subject': f'更新 {i}', 'snippet': 'Search preview without deadline.'}
                for i, identifier in enumerate(ids[:limit])]
    def reader(identifier):
        reads.append(identifier)
        i = ids.index(identifier)
        return {'subject': f'更新 {i}', 'from': 'Example service', 'date': '2028-04-02',
                'body': f'Unique obligation {i:02}: renewal due 2028-05-{i+1:02}; amount {i+1} USD. '+('Supporting detail ' * 28)}
    def calendar(time_min, time_max, limit):
        calendars.append((time_min, time_max))
        return []
    register_email_plugin(registry, email_searcher=search, email_reader=reader)
    register_calendar_plugin(registry, calendar_lister=calendar)
    job = crons.add_cron('Reporte', '* * * * *', instructions, delivery_channel='telegram', delivery_target='2222')
    job.next_run = (datetime.now(UTC)-timedelta(minutes=1)).isoformat()
    crons.save_crons([job])
    class Client:
        agent_calls = 0
        repair_calls = 0
        def create(self, **kwargs):
            if not kwargs.get('tools'):
                self.repair_calls += 1
                evidence = kwargs['messages'][0]['content'][0]['text']
                assert all(f'Unique obligation {i:02}:' in evidence for i in range(28))
                assert evidence.count('\"tool_name\": \"calendar_list\"') == 2
                if self.repair_calls == 1:
                    return {'stop_reason': 'end_turn', 'content': [],
                            'completion': {'status': 'incomplete', 'reason': 'max_output_tokens'}}
                return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [f'e{i}' for i in range(28)], 'report': '28 renovaciones: próximas fechas del 1 al 28 de mayo de 2028. La última vence el 2028-05-28 por 28 USD. No hay eventos existentes ni se creó ninguno.'}))
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {'stop_reason': 'tool_use', 'content': [
                    {'type': 'tool_use', 'id': f's{i}', 'name': 'email_search', 'input': {'query': f'fixture-{i}', 'limit': 28}}
                    for i in range(13)]}
            if self.agent_calls == 2:
                calls = [{'type': 'tool_use', 'id': f'r{i}', 'name': 'email_read', 'input': {'id': identifier}}
                         for i, identifier in enumerate(ids)]
                calls += [{'type': 'tool_use', 'id': f'c{i}', 'name': 'calendar_list',
                           'input': {'start': f'2028-05-0{i+1}T00:00:00Z', 'end': '2028-06-01T00:00:00Z', 'max': 20}}
                          for i in range(2)]
                return {'stop_reason': 'tool_use', 'content': calls}
            return text_response('I checked your email.\n\n**更新 27**\nFrom: Example service\nDate: 2028-04-02\n\nPreview: Unique obligation 27: renewal due 2028-05-28; amount 28 USD.')
    client = Client()
    results = []
    def fire(job):
        result = run_scheduled(job, client, runtime, registry, send)
        results.append(result)
        return result
    scheduler=crons.CronScheduler(fire)
    scheduler._tick()
    from cron_scheduler_helpers import finish_scheduler
    finish_scheduler(scheduler)
    assert len(reads) == 28 and set(reads) == set(ids), results
    assert len(searches) == 13 and len(calendars) == 2, results
    assert client.repair_calls == 2
    assert len(deliveries) == 1 and '2028-05-28' in deliveries[0]['text'] and '28 USD' in deliveries[0]['text']
    assert 'Preview:' not in deliveries[0]['text']
    assert crons.get_cron(job.id).last_result == 'ok'
    assert results[0]['execution_outcome']['execution_succeeded'] is True
    completed = [event for event in runtime.store.list_events() if event.event_type == 'tool.invocation.completed']
    assert sum(event.payload.get('tool_name') == 'email_read' for event in completed) == 28


@pytest.mark.parametrize('instructions', [
    'Report new matching events with date, ticket link and price; otherwise stay silent.',
    '新しい該当イベントの日付、チケットリンク、価格を報告。該当なしなら通知不要。',
    'Informa eventos nuevos coincidentes con fecha, enlace y precio; si no hay, no envíes nada.',
])
@pytest.mark.parametrize('has_match', [True, False])
@pytest.mark.parametrize('legacy', [True, False])
@pytest.mark.parametrize('discovery', ['files', 'previews'])
def test_scheduled_monitor_recovers_sources_without_internal_docs_and_respects_silence(tmp_path, monkeypatch, instructions, has_match, legacy, discovery):
    from nullion import crons
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
                              _build_skill_pack_read_handler, _build_file_read_handler,
                              _build_web_search_handler, register_web_fetch_tool)
    from nullion.skill_pack_installer import read_skill_pack_reference
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.FILESYSTEM_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id='monitor-'+kind.value, principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind, mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    registry.register(ToolSpec('file_read', 'Read saved checkpoint', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2),
                      _build_file_read_handler(tmp_path, allowed_roots=(tmp_path,)))
    registry.register(ToolSpec('skill_pack_read', 'Read installed reference', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2,
                              capability_tags=('skill_pack',)), _build_skill_pack_read_handler())
    checkpoints = []
    for i in range(22 if discovery == 'files' else 2):
        path = tmp_path / f'checkpoint-{i}.json'
        path.write_text('Previous unrelated state ' * 200 if discovery == 'files' else
                        json.dumps({'reported_events': [{'id': 'already-reported-'+str(i)}]}))
        checkpoints.append(path)
    searches = []
    def search(query, limit):
        searches.append(query)
        return [{'title': f'Preview {i}', 'snippet': 'Repeated discovery context ' * 60,
                 'url': f'https://example.org/preview/{i}'} for i in range(6)]
    registry.register(ToolSpec('web_search', 'Search public listings', ToolRiskLevel.LOW,
                               ToolSideEffectClass.READ, False, 2), _build_web_search_handler(search))
    docs = read_skill_pack_reference('nullion/web-research', 'README.md')
    source = 'SOURCE_EVENT: Club Q, 2028-04-09, Venue Q, USD 25, https://example.org/tickets/q.' if has_match else 'SOURCE_EVENT: Other current listings are present. None matches the monitored event.'
    reads = []
    def fetch(url, timeout):
        reads.append(url)
        return {'url': url, 'title': 'Current event listings', 'text': source, 'status_code': 200}
    register_web_fetch_tool(registry, fetch)
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Monitor', '* * * * *', instructions, enabled=False, delivery_channel='telegram', delivery_target='2222',
                         artifact_delivery_options={} if legacy else {'alert_delivery_mode': 'verified_matches_only'})
    elapsed = []
    for _ in range(3):
        class Client:
            agent_calls = 0
            repair_calls = 0
            policy_calls = 0
            def create(self, **kwargs):
                if not kwargs.get('tools'):
                    system = str(kwargs.get('system', ''))
                    if system.startswith('You are a material-requirements planner'):
                        return text_response(json.dumps({'all_material_requirements_enumerated': True,
                            'material_requirements': [{'requirement': 'Verify current monitored source records',
                                'requirement_kind': 'fact', 'evidence_subject_id': 'monitor',
                                'eligible_evidence_tool_names': ['web_fetch']}]}))
                    if system.startswith('You are a semantic completion reviewer'):
                        payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
                        row = payload['material_requirement_contract'][0]
                        ids = [r['invocation_id'] for r in payload['tool_evidence']
                               if r['tool_name'] == 'web_fetch' and r['status'] == 'completed']
                        assert ids and 'SOURCE_EVENT' in json.dumps(payload['tool_evidence'])
                        complete = payload['draft_final_response'] != docs
                        return text_response(json.dumps({'disposition': 'complete' if complete else 'retry',
                            'all_material_requirements_enumerated': True, 'unresolved_requirements': [],
                            'material_requirements': [{'requirement_id': row['requirement_id'],
                                'requirement': row['requirement'], 'requirement_kind': row['requirement_kind'],
                                'satisfied': complete, 'draft_covered': complete,
                                'evidence_invocation_ids': ids if complete else []}]}))
                    prompt = kwargs['messages'][0]['content'][0]['text']
                    if str(kwargs.get('system', '')).startswith('Classify delivery'):
                        self.policy_calls += 1
                        assert 'SOURCE_EVENT' in prompt
                        return text_response(json.dumps({'decision': 'send' if has_match else 'silent',
                            'reason': 'verified_match' if has_match else 'no_verified_match'}))
                    self.repair_calls += 1
                    evidence_text = prompt.split('Verified compact tool evidence from this run:\n', 1)[1].split('\n\n', 1)[0]
                    rows = json.loads(evidence_text)
                    assert 'SOURCE_EVENT' in evidence_text and 'skill_pack_read' not in evidence_text
                    assert docs not in evidence_text
                    if discovery == 'previews':
                        assert 'already-reported-0' in evidence_text and 'already-reported-1' in evidence_text
                    if self.repair_calls == 1:
                        return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'empty',
                            'evidence_refs': ['e0'], 'report': 'No new matching events.'}))
                    assert 'report_not_grounded_or_complete' in prompt
                    refs = [row['evidence_id'] for row in rows if row.get('tool_name') == 'web_fetch']
                    assert refs
                    report = 'Club Q — 2028-04-09, Venue Q, USD 25. Tickets: https://example.org/tickets/q.' if has_match else 'No new matching event was found in the checked listings.'
                    return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                        'evidence_refs': refs, 'report': report if has_match or not legacy else '',
                        **({'delivery_decision': 'send' if has_match else 'silent',
                            'delivery_reason': 'report' if has_match else 'no_verified_match'} if legacy else {})}))
                self.agent_calls += 1
                if self.agent_calls == 1:
                    calls = [{'type': 'tool_use', 'id': 'checkpoint-'+str(i), 'name': 'file_read', 'input': {'path': str(path)}}
                             for i, path in enumerate(checkpoints)]
                    if discovery == 'previews':
                        calls = [{'type': 'tool_use', 'id': 'preview-'+str(i), 'name': 'web_search',
                                  'input': {'query': f'listing {i}', 'limit': 6}} for i in range(20)] + calls
                    calls.append({'type': 'tool_use', 'id': 'reference', 'name': 'skill_pack_read',
                                  'input': {'pack_id': 'nullion/web-research', 'path': 'README.md'}})
                    return {'stop_reason': 'tool_use', 'content': calls}
                if self.agent_calls == 2:
                    return tool_response('web_fetch', {'url': 'https://example.org/events'})
                return text_response(docs)
        client = Client()
        before = len(deliveries)
        started = time.monotonic()
        outcome = run_scheduled(job, client, runtime, registry, send)
        elapsed.append(time.monotonic()-started)
        assert outcome['response_fulfilled'] is True, outcome
        assert outcome['execution_outcome']['execution_succeeded'] is True, outcome
        assert client.repair_calls == 2 and client.policy_calls == (0 if legacy else 1)
        assert any(r.tool_name == 'web_fetch' and r.status == 'completed' for r in outcome['tool_results'])
        if has_match:
            assert outcome['cron_delivery_status'] == 'sent'
            assert len(deliveries) == before + 1
            text = deliveries[-1]['text']
            assert '2028-04-09' in text and 'USD 25' in text and 'https://example.org/tickets/q' in text
            assert 'Skill pack:' not in text and 'still open' not in text and 'could not finish' not in text
        else:
            assert outcome['cron_delivery_status'] == 'silent'
            assert len(deliveries) == before
        runtime.checkpoint(force=True)
    assert reads == ['https://example.org/events'] * 3
    assert len(searches) == (60 if discovery == 'previews' else 0)
    assert sum(elapsed)/len(elapsed) < 2.5 and max(elapsed) < 3.5, elapsed
    assert any(event.event_type == 'cron.delivery.agent_preflight' and event.payload.get('stage') == 'final_repaired'
               for event in runtime.store.list_events())
    from nullion.runtime_persistence import load_runtime_store
    restored = load_runtime_store(tmp_path/'runtime.db')
    assert any(event.event_type == ('cron.delivery.sent' if has_match else 'cron.delivery.silent')
               for event in restored.list_events())


@pytest.mark.parametrize('prompt', ['Run this saved report.', '保存済みのレポートを実行して。', 'Ejecuta este informe guardado.'])
def test_manual_report_start_receipt_survives_extra_foreground_source_requirements(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion import crons, config, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, register_market_quote_tool
    runtime = setup_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'home'/'.env'))
    from nullion import preferences
    monkeypatch.setattr(preferences, '_PREFS_PATH', tmp_path/'home'/'preferences.json')
    from nullion import turn_context_policy
    monkeypatch.setattr(turn_context_policy, '_active_connector_provider_context', lambda: ({
        'provider_id': 'fixture-account', 'active_app_ids': ['google-mail'],
        'structured_tools': ['email_read', 'email_search'],
    },))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        rule_id='manual-history-network', principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector='*',
        created_by='fixture-user', created_at=datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_market_quote_tool(registry, quote_fetcher=lambda symbol, timeout: {'symbol': symbol, 'price': 20})
    def unused_account_adapter(invocation):
        raise AssertionError('The foreground dispatch must not execute background source reads')
    for name in ('email_read', 'email_search'):
        registry.register(ToolSpec(name=name, description='Connected account read', risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=10), unused_account_adapter)
    task = 'أخبرني بسعر AAA'
    class Client:
        ident = ''
        scoped = False
        dispatched = False
        def create(self, **kwargs):
            names = {row['name'] for row in kwargs.get('tools', [])}
            system = str(kwargs.get('system') or '')
            messages = json.dumps(kwargs.get('messages', []), ensure_ascii=False)
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            if not names and 'web_action' in system and 'scheduler_action' in system:
                return text_response(json.dumps({'web_action': 'none', 'scheduler_action': 'run',
                    'scheduler_selection_policy': 'user_selected', 'skill_pack_action': 'connector',
                    'connector_app_ids': ['google-mail'], 'connector_source_user_requested': True,
                    'connector_source_evidence': f'{prompt} id={self.ident}',
                    'requested_tool_names': ['email_read', 'email_search', 'run_cron'],
                    'required_tool_names': ['email_read', 'email_search', 'run_cron'], 'confidence': 1}))
            if not names:
                return text_response('{}')
            if task in messages and 'market_quote' in names:
                if '"tool_result"' not in messages:
                    return tool_response('market_quote', {'symbols': ['AAA']})
                return text_response('AAA السعر 20 USD.')
            if 'request_tool_scope' in names and not self.scoped:
                self.scoped = True
                return tool_response('request_tool_scope', {'capabilities': ['scheduler_run'],
                    'tool_names': ['email_read', 'email_search', 'run_cron'], 'required_tool_names': ['email_read', 'email_search', 'run_cron'],
                    'scheduler_action': 'run', 'scheduler_selection_policy': 'user_selected'})
            if 'run_cron' in names and not self.dispatched:
                self.dispatched = True
                return tool_response('run_cron', {'id': self.ident})
            return text_response('Started the saved report. The result will arrive here.')
    model = Client()
    class ObservedOrchestrator(AgentOrchestrator):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.results = []
        def run_turn(self, *args, **kwargs):
            result = super().run_turn(*args, **kwargs)
            self.results.append(result)
            return result
    orchestrator = ObservedOrchestrator(model_client=model)
    settings = NullionSettings(doctor_enabled=False)
    monkeypatch.setattr(config, 'load_settings', lambda *args, **kwargs: settings)
    monkeypatch.setattr(web_app, 'load_settings', lambda *args, **kwargs: settings)
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: None)
    app = web_app.create_app(runtime, orchestrator=orchestrator, registry=registry)
    with TestClient(app) as client:
        created = client.post('/api/crons', json={'name': 'ملخص', 'schedule': '* * * * *',
            'task': task, 'enabled': False, 'delivery_channel': 'web', 'delivery_target': 'web:receipt-history'}).json()
        model.ident = created['job']['id']
        result = client.post('/api/chat', json={'text': f'{prompt} id={model.ident}', 'conversation_id': 'web:receipt-history', 'turn_id': 'manual-start'})
        assert result.status_code == 200, result.text
        body = result.json()
        turns = runtime.store.list_recent_conversation_events('web:receipt-history', event_type='conversation.chat_turn', limit=20)
        foreground = next(row for row in turns if row.get('turn_id') == 'manual-start')
        receipt = next(row['output'] for row in foreground['tool_results'] if row['tool_name'] == 'run_cron')
        assert receipt['delivery_status'] == 'deferred'
        assert receipt['message'] in json.dumps(body, ensure_ascii=False)
        assert foreground['assistant_reply'] == receipt['message']
        scope = next(row['output'] for row in foreground['tool_results'] if row['tool_name'] == 'request_tool_scope')
        assert set(scope['requested_required_tool_names']) == {'email_read', 'email_search', 'run_cron'}
        dispatched = next(result for result in orchestrator.results
                          if any(tool.tool_name == 'run_cron' for tool in result.tool_results))
        assert dispatched.response_fulfilled is False
        deadline = time.monotonic()+5
        while time.monotonic() < deadline:
            turns = runtime.store.list_recent_conversation_events('web:receipt-history', event_type='conversation.chat_turn', limit=20)
            if any(row.get('cron_id') == model.ident for row in turns): break
            time.sleep(0.02)
        report = next(row for row in turns if row.get('cron_id') == model.ident)
        assert '20 USD' in report['assistant_reply']
        history = client.get('/api/chat/history/web%3Areceipt-history?sync_runtime=1').json()
        assert any(row['role'] == 'bot' and row['text'] == receipt['message'] for row in history['messages']), history
        assert crons.get_cron(model.ident).enabled is False


@pytest.mark.parametrize('instructions', ['Report the complete quote list and source timestamps.', 'すべての銘柄価格と取得時刻を報告。', 'Informa todos los precios y sus horas de origen.'])
def test_model_timeout_report_retains_all_completed_quote_rows(tmp_path, monkeypatch, instructions):
    from nullion import crons
    from nullion.model_clients import ModelClientTimeoutError
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, register_market_quote_tool
    runtime = setup_runtime(tmp_path, monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        rule_id='complete-quotes-network', principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector='*',
        created_by='fixture-user', created_at=datetime.now(UTC)))
    deliveries, send = telegram_boundary(monkeypatch)
    symbols = [f'SYM{i:02}' for i in range(16)]
    reads = []
    def fetch(symbol, timeout):
        reads.append(symbol)
        return {'symbol': symbol, 'price': 20, 'previous_close': 19, 'change': 1, 'change_percent': 4.5,
            'day_high': 21, 'day_low': 19, 'volume': 200000, 'currency': 'USD', 'exchange': 'NASDAQ',
            'regular_market_time': '2026-10-05T19:23:00Z',
            'source_url': f'https://example.test/finance/chart/{symbol}?interval=1d&range=5d'}
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_market_quote_tool(registry, quote_fetcher=fetch)
    job = crons.add_cron('価格', '* * * * *', instructions, enabled=False,
        delivery_channel='telegram', delivery_target='2222')
    class Client:
        calls = 0
        def create(self, **kwargs):
            if not kwargs.get('tools'):
                evidence = kwargs['messages'][0]['content'][0]['text'].split('Verified compact tool evidence from this run:\n',1)[1].split('\n\n',1)[0]
                rows = json.loads(evidence)
                quotes = rows[0]['source_records']
                assert {row['symbol'] for row in quotes if row.get('symbol')} == set(symbols)
                assert all(row['regular_market_time'] == '2026-10-05T19:23:00Z' for row in quotes if row.get('symbol'))
                return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': ['e0'], 'delivery_decision': 'send', 'delivery_reason': 'report',
                    'report': '\n'.join(f'{symbol}: 20 USD as of 2026-10-05T19:23:00Z' for symbol in symbols)}))
            self.calls += 1
            if self.calls == 1:
                return tool_response('market_quote', {'symbols': symbols})
            raise ModelClientTimeoutError('Simulated provider timeout after all quote reads completed')
    elapsed = []
    for _ in range(3):
        reads.clear()
        start = time.monotonic()
        result = run_scheduled(job, Client(), runtime, registry, send)
        elapsed.append(time.monotonic()-start)
        assert sorted(reads) == symbols
        assert result['execution_outcome']['execution_succeeded'] is True, result
        assert all(symbol+': 20 USD' in deliveries[-1]['text'] for symbol in symbols)
    assert len(deliveries) == 3
    assert sum(elapsed)/len(elapsed) < 2.5 and max(elapsed) < 3.5, elapsed


@pytest.mark.parametrize('instructions', ['Attach an .html report with source ranges and timestamps.', '出典の年間範囲と時刻を含む .html を添付。', 'Adjunta un informe .html con rangos y horas de origen.'])
def test_html_producer_preserves_annual_ranges_without_review_or_regeneration(tmp_path, monkeypatch, instructions):
    from nullion import crons
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, register_market_quote_tool, _build_file_write_handler
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.FILESYSTEM_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id='range-report-'+kind.value, principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind, mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    register_market_quote_tool(registry, quote_fetcher=lambda symbol, timeout: {
        'symbol': symbol, 'price': 20, 'day_low': 19, 'day_high': 21,
        'fifty_two_week_low': 10, 'fifty_two_week_high': 50,
        'regular_market_time': '2026-10-05T20:00:00Z', 'source_url': 'https://example.test/quote/AAA'})
    registry.register(ToolSpec('file_write', 'Write report', ToolRiskLevel.MEDIUM,
        ToolSideEffectClass.WRITE, False, 20), _build_file_write_handler(allowed_roots=(tmp_path,)))
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Ranges', '* * * * *', instructions, enabled=False,
        delivery_channel='telegram', delivery_target='2222', artifact_delivery_options={
            'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html'],
            'required_artifact_content_tokens': ['AAA'], 'deliver_required_extensions_only': True})
    path = artifact_root_for_principal('telegram:2222')/'range-report.html'
    incomplete = '<!doctype html><html><body>AAA 20; range 19–21</body></html>'
    complete = '<!doctype html><html><body>AAA 20; annual range 10–50; 2026-10-05T20:00:00Z; news unavailable</body></html>'
    class Client:
        calls = 0
        reviews = 0
        def create(self, **kwargs):
            if not kwargs.get('tools'):
                payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
                quotes = [row for row in payload['verified_source_records'] if row.get('symbol') == 'AAA']
                assert len(quotes) == 1
                assert (quotes[0]['fifty_two_week_low'], quotes[0]['fifty_two_week_high']) == (10, 50)
                assert quotes[0]['regular_market_time'] == '2026-10-05T20:00:00Z'
                if payload['existing_html'] is None:
                    assert 'Use annual range and quote timestamp' in payload['prior_review_issues']
                    return text_response(complete)
                self.reviews += 1
                accepted = payload['existing_html'] == complete
                return text_response(json.dumps({'requirements_satisfied': accepted, 'html': '',
                    'issues': [] if accepted else ['Use annual range and quote timestamp'], 'edits': []}))
            self.calls += 1
            if self.calls == 1: return tool_response('market_quote', {'symbols': ['AAA']})
            if self.calls == 2: return tool_response('file_write', {'path': str(path), 'content': complete})
            return text_response('Report attached.\nMEDIA:'+str(path))
    client = Client()
    result = run_scheduled(job, client, runtime, registry, send)
    assert result['execution_outcome']['execution_succeeded'] is True, result
    assert result['artifact_delivery_satisfied'] is True, result
    assert client.reviews == 0 and client.calls == 3
    assert len(deliveries) == 1
    uploaded = deliveries[0]['bytes']
    assert uploaded == Path(deliveries[0]['path']).read_bytes()
    assert 'AAA 20; annual range 10–50; 2026-10-05T20:00:00Z; news unavailable'.encode() in uploaded
    assert 'range 19–21'.encode() not in uploaded
    assert any(event.event_type == 'tool.invocation.completed' and event.payload.get('tool_name') == 'market_quote'
               for event in runtime.store.list_events())
    assert any(event.event_type == 'tool.invocation.completed' and event.payload.get('tool_name') == 'file_write'
               for event in runtime.store.list_events())


@pytest.mark.parametrize('instructions', ['Report only new upcoming events; stay silent otherwise.', '新しい今後のイベントだけを報告し、なければ通知不要。', 'Informa solo eventos futuros nuevos; si no hay, no envíes nada.'])
def test_past_and_already_reported_events_do_not_become_upcoming_in_recovery(tmp_path, monkeypatch, instructions):
    from nullion import crons, cron_delivery
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, register_web_fetch_tool, _build_file_read_handler, _build_skill_pack_read_handler
    from nullion.skill_pack_installer import read_skill_pack_reference
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 5, 20, 0, tzinfo=UTC).astimezone(tz)
    monkeypatch.setattr(cron_delivery, 'datetime', Clock)
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.FILESYSTEM_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id='dated-monitor-'+kind.value, principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind, mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    registry.register(ToolSpec('skill_pack_read', 'Read installed reference', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('skill_pack',)), _build_skill_pack_read_handler())
    docs = read_skill_pack_reference('nullion/web-research', 'README.md')
    checkpoint = tmp_path/'reported.json'
    checkpoint.write_text(json.dumps({'reported_events': [{'id': 'prior-report', 'date': '2026-10-31'}]}))
    registry.register(ToolSpec('file_read', 'Read saved checkpoint', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), _build_file_read_handler(tmp_path, allowed_roots=(tmp_path,)))
    sources = [{'id': 'old-listing', 'date': '2026-05-16'}, {'id': 'prior-report', 'date': '2026-10-31'}]
    page = json.dumps({'events': sources, 'context': 'Official current listings. ' * 8})
    register_web_fetch_tool(registry, lambda url, timeout: {'url': url, 'text': page, 'status_code': 200})
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Events', '* * * * *', instructions, enabled=False,
        delivery_channel='telegram', delivery_target='2222')
    class Client:
        calls = 0
        repairs = 0
        def create(self, **kwargs):
            if not kwargs.get('tools'):
                self.repairs += 1
                prompt = kwargs['messages'][0]['content'][0]['text']
                assert 'old-listing' in prompt and 'prior-report' in prompt
                marker = 'Current run context:\n'
                clock = json.loads(prompt.split(marker,1)[1].split('\n\n',1)[0]) if marker in prompt else None
                as_of = datetime.fromisoformat(clock['current_datetime']).date() if clock else datetime(2026, 5, 1).date()
                fresh = [item for item in sources if datetime.fromisoformat(item['date']).date() >= as_of and item['id'] != 'prior-report']
                return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': ['e0'], 'delivery_decision': 'send' if fresh else 'silent',
                    'delivery_reason': 'report' if fresh else 'no_verified_match',
                    'report': 'New upcoming event: May 16, 2026. Tickets: https://example.org/past.' if fresh else ''}))
            self.calls += 1
            if self.calls == 1:
                return {'stop_reason': 'tool_use', 'content': [
                    {'type': 'tool_use', 'id': 'read-state', 'name': 'file_read', 'input': {'path': str(checkpoint)}},
                    {'type': 'tool_use', 'id': 'read-page', 'name': 'web_fetch', 'input': {'url': 'https://example.org/events'}},
                    {'type': 'tool_use', 'id': 'read-reference', 'name': 'skill_pack_read', 'input': {'pack_id': 'nullion/web-research', 'path': 'README.md'}}]}
            return text_response(docs)
    client = Client()
    result = run_scheduled(job, client, runtime, registry, send)
    assert client.repairs == 1, result
    assert result['execution_outcome']['execution_succeeded'] is True, result
    assert result['cron_delivery_status'] == 'silent' and not deliveries
    runtime.checkpoint(force=True)
    from nullion.runtime_persistence import load_runtime_store
    restored = load_runtime_store(tmp_path/'runtime.db')
    assert any(event.event_type == 'cron.delivery.silent' for event in restored.list_events())


@pytest.mark.parametrize('manual', [False, True])
def test_verified_no_match_has_a_real_web_receipt_only_for_manual_run(tmp_path, monkeypatch, manual):
    from fastapi.testclient import TestClient
    from nullion import crons, config, web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings
    from nullion.cron_delivery import (CronRunDeliveryCallbacks, run_cron_delivery_workflow,
        run_single_agent_cron_turn, cron_delivery_block_reason, manual_cron_silent_delivery_text)
    from nullion.events import make_event
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    runtime=setup_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setattr(chat_store, '_store', None)
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    calls=[]
    def lookup(invocation):
        calls.append(invocation.tool_name)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'items': []})
    registry.register(ToolSpec(name='lookup_entries', description='Read current records',
        risk_level=ToolRiskLevel.LOW, side_effect_class=ToolSideEffectClass.READ,
        requires_approval=False, timeout_seconds=2), lookup)
    class Model:
        reads=0
        def create(self, **kwargs):
            if kwargs.get('tools'):
                self.reads+=1
                if self.reads==1: return tool_response('lookup_entries', {})
                return text_response('新しい一致はありません。')
            if 'requirements_satisfied' in str(kwargs.get('system', '')):
                return text_response(json.dumps({'requirements_satisfied':True,'source_state':'empty',
                    'evidence_refs':['e0'], 'delivery_decision':'silent',
                    'delivery_reason':'no_verified_match','report':''}))
            return text_response(json.dumps({'decision':'silent','reason':'no_verified_match'}))
    model=Model(); settings=NullionSettings(doctor_enabled=False)
    monkeypatch.setattr(config, 'load_settings', lambda *a,**k: settings)
    monkeypatch.setattr(web_app, 'load_settings', lambda *a,**k: settings)
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: None)
    app=web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    job=crons.CronJob(id='quiet-receipt', name='確認', schedule='* * * * *', task='新規一致だけ通知。',
        delivery_channel='web', delivery_target='web:quiet-receipt',
        artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'})
    with TestClient(app) as client:
        saved=[]
        def save(job, conversation, text, artifacts, result):
            response=client.post('/api/chat/history/web%3Aquiet-receipt/message',
                json={'role':'bot','text':text})
            saved.append(text)
            return response.status_code==200
        def record(kind, job, channel, target, conversation, **extra):
            runtime.store.add_event(make_event(kind, 'cron_scheduler', {'cron_id':job.id, **extra}))
        result=run_cron_delivery_workflow(job, label='Manual scheduled task run' if manual else 'Scheduled task',
            callbacks=CronRunDeliveryCallbacks(effective_channel=lambda job:'web',
                delivery_target=lambda job, channel:'web:quiet-receipt',
                run_agent_turn=lambda job, conversation:run_single_agent_cron_turn(job, conversation,
                    label='Manual scheduled task run' if manual else 'Scheduled task',
                    orchestrator=AgentOrchestrator(model_client=model),runtime=runtime,tool_registry=registry,
                    settings=settings, model_client=model), record_event=record,
                block_reason=cron_delivery_block_reason,save_web_delivery=save,
                send_platform_delivery=lambda *args:False,
                silent_delivery_text=manual_cron_silent_delivery_text if manual else None))
        assert calls==['lookup_entries']
        assert result['cron_alert_delivery_decision']=='silent'
        assert result['cron_delivery_status']==('saved' if manual else 'silent'), result
        assert bool(saved) is manual
        history=client.get('/api/chat/history/web%3Aquiet-receipt').json()
        assert any('There was nothing new to report.' in row['text'] for row in history['messages']) is manual
        events=[event.event_type for event in runtime.store.list_events() if event.payload.get('cron_id')==job.id]
        assert ('cron.delivery.saved' in events) is manual
        assert ('cron.delivery.silent' in events) is not manual
