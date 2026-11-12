from __future__ import annotations

from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
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


def report_layout_review(kwargs, required_fragment):
    """Script the external reviewer's content correction, leaving the owner real."""
    if 'semantic completion reviewer' not in (kwargs.get('system') or ''):
        return None
    packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
    response = workflow_provider_reply(kwargs, completion=True)
    decision = json.loads(response['content'][0]['text'])
    if required_fragment not in packet['draft_final_response']:
        decision.update(disposition='retry', retry_tool_names=[],
            unresolved_requirements=['Compose the requested report from all retained source checks.'])
        for row in decision['material_requirements']:
            row.update(satisfied=True, draft_covered=False)
    return text_response(json.dumps(decision))


def setup_runtime(tmp_path, monkeypatch):
    from nullion import crons, chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    checkpoint = tmp_path/'runtime.db'
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', checkpoint)
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path/'data'))
    # Each app owns the history under its isolated home. A previous app's
    # singleton otherwise makes a silent run appear to have delivered a report.
    monkeypatch.setattr(chat_store, '_store', None)
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
    import telegram as telegram_sdk
    monkeypatch.setattr(telegram_sdk, 'Bot', Bot)
    def send(job, channel, target, text):
        assert channel == 'telegram'
        return asyncio.run(telegram_app._send_operator_telegram_message('synthetic-token', target, text,
                           principal_id='telegram:'+target))
    return deliveries, send


def run_scheduled(job, client, runtime, registry, send, *, agent_conversation_id=None):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn, run_cron_delivery_workflow, CronRunDeliveryCallbacks, cron_delivery_block_reason
    from nullion.events import make_event
    def record(event, job, channel, target, conversation_id, **extra):
        runtime.store.add_event(make_event(event, 'cron_scheduler', {'cron_id': job.id, 'channel': channel, **extra}))
    return run_cron_delivery_workflow(job, label='Scheduled task', callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda job: job.delivery_channel, delivery_target=lambda job, channel: job.delivery_target,
        run_agent_turn=lambda job, conversation, delivery_conversation_id=None: run_single_agent_cron_turn(job, conversation, label='Scheduled task',
            orchestrator=AgentOrchestrator(model_client=client), runtime=runtime, tool_registry=registry,
            settings=SimpleNamespace(enabled_skill_packs=()), record_event=record,
            delivery_conversation_id=delivery_conversation_id),
        record_event=record, block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda *args: None, send_platform_delivery=send,
    ), agent_conversation_id=agent_conversation_id)


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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
        receipts=[row for row in runtime.store.list_recent_conversation_events('web:operator', event_type='conversation.chat_turn') if row.get('cron_id')==ident]
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
                "SELECT payload FROM conversation_events WHERE json_extract(payload, '$.cron_id') = ? "
                "AND json_extract(payload, '$.event_type') = 'conversation.chat_turn'",
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
    if not recover:
        assert not result.get('cron_completion_continuations'), result
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
                from nullion.report_completion import MARKER
                contract = payload['report_outcome_contract']
                metadata = {'contract_id': contract['contract_id'], 'status': 'complete',
                    'all_material_requirements_enumerated': True, 'remaining_work': []}
                document = html.replace('</html>', '<!--'+MARKER+json.dumps(metadata)+'--></html>')
                self.generated_document = document
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
                        fragment = document if transient_phase == 'generate_complete_stream' else prefix
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
                    return text_response(document[len(received_prefix):])
                if phase == transient_phase and self.phase_calls[phase] == 1:
                    from nullion.model_clients import ModelClientTimeoutError
                    time.sleep(0.03)
                    raise ModelClientTimeoutError('Previously discarded artifact after a transient timeout')
                if phase == transient_phase:
                    assert kwargs['timeout'] == 180
                if phase == 'generate' and recover:
                    assert 'Do not output JSON' in kwargs['system']
                    return text_response(document)
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
        assert trace[0]['partial_document_chars'] == len(client.generated_document if transient_phase == 'generate_complete_stream' else prefix)
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
            reviewed = report_layout_review(kwargs, '28 renovaciones')
            if reviewed is not None: return reviewed
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
                protocol_reply = workflow_provider_reply(kwargs, completion=False)
                if protocol_reply is not None: return protocol_reply
                if not kwargs.get('tools'):
                    system = str(kwargs.get('system', ''))
                    if system.startswith('You are a material-requirements planner'):
                        packet=json.loads(kwargs['messages'][0]['content'][0]['text'])
                        return text_response(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'), 'all_material_requirements_enumerated': True,
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
                        return text_response(json.dumps({**fixture_source_assessment(payload,conclusion='limited'), 'disposition': 'complete' if complete else 'retry',
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
        assert dispatched.response_fulfilled is True
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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
    from nullion.tools import ToolRegistry, register_calendar_plugin
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime=setup_runtime(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setattr(chat_store, '_store', None)
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='empty-calendar-'+kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL,kind=kind,mode=PolicyMode.ALLOW,selector='*',
            created_by='fixture',created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    calls=[]
    def lookup(start,end,limit):
        calls.append((start,end,limit))
        return []
    register_calendar_plugin(registry,calendar_lister=lookup)
    class Model:
        reads=0
        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            if kwargs.get('tools'):
                self.reads+=1
                if self.reads==1: return tool_response('calendar_list', {
                    'start':'2028-11-10T00:00:00Z','end':'2028-11-11T00:00:00Z','max':20})
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
        assert calls==[('2028-11-10T00:00:00Z','2028-11-11T00:00:00Z',20)]
        assert result['response_fulfilled'] is True and not result.get('cron_report_failed'), result
        assert result['cron_alert_delivery_decision']=='silent'
        assert result['cron_delivery_status']==('saved' if manual else 'silent'), result
        assert bool(saved) is manual
        history=client.get('/api/chat/history/web%3Aquiet-receipt').json()
        assert any('There was nothing new to report.' in row['text'] for row in history['messages']) is manual
        events=[event.event_type for event in runtime.store.list_events() if event.payload.get('cron_id')==job.id]
        assert ('cron.delivery.saved' in events) is manual
        assert ('cron.delivery.silent' in events) is not manual


@pytest.mark.parametrize('instructions', ['Check the existing appointment and report whether it needs creating.', '既存の予定を確認し、作成が必要か報告。'])
def test_long_scheduled_report_inspects_retained_calendar_before_repeating_work(tmp_path, monkeypatch, instructions):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from tests.test_cron_retained_evidence import long_evidence
    runtime = setup_runtime(tmp_path, monkeypatch)
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id='retained-source-'+kind.value, principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind, mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    deliveries, send = telegram_boundary(monkeypatch)
    source = long_evidence(); calls = []
    registry = ToolRegistry()
    def read(inv):
        calls.append(inv.tool_name)
        output = source[-1].output if inv.tool_name == 'calendar_list' else source[int(inv.arguments['id'], 16)-1].output
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', output)
    for name in ('email_read', 'calendar_list'):
        registry.register(ToolSpec(name=name, description='Read authorized fixture records',
            risk_level=ToolRiskLevel.LOW, side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False, timeout_seconds=2), read)
    class Model:
        agent_calls = 0
        inspections = 0
        def create(self, **kw):
            reviewed = report_layout_review(kw, 'No duplicate was created')
            if reviewed is not None: return reviewed
            auxiliary = workflow_provider_reply(kw, completion=True)
            if auxiliary is not None: return auxiliary
            if kw.get('tools'):
                self.agent_calls += 1
                if self.agent_calls == 1:
                    return {'stop_reason':'tool_use','content':[
                        {'type':'tool_use','id':f'm{i}','name':'email_read','input':{'id':f'{i+1:016x}'}} for i in range(70)] +
                        [{'type':'tool_use','id':'calendar','name':'calendar_list','input':{'start':'2028-11-13T00:00:00Z','end':'2028-11-16T00:00:00Z'}}]}
                return text_response('I checked your email.\n\n**通知 69**\nFrom: Service\nDate: 2028-01-01\n\nPreview: Obligation 69: supporting content')
            prompt = kw['messages'][0]['content'][0]['text']
            records, _ = json.JSONDecoder().raw_decode(
                prompt.split('Verified compact tool evidence from this run:\n')[1].lstrip())
            calendar = next(row for row in records if row.get('tool_name') == 'calendar_list')
            if not self.inspections:
                assert 'event-45' not in json.dumps(calendar)
                self.inspections += 1
                return text_response(json.dumps({'disposition':'inspect_evidence', 'evidence_requests':[
                    {'invocation_id':calendar['evidence_id'],'json_pointer':'/events/45'}]}))
            assert 'Additional retained evidence:' in prompt and 'event-45' in prompt
            return text_response(json.dumps({'requirements_satisfied':True,'source_state':'available',
                'evidence_refs':[calendar['evidence_id']],
                'report':'The November 14 appointment already exists. No duplicate was created.'}))
    job = crons.add_cron('Retained checks', '* * * * *', instructions,
                         delivery_channel='telegram', delivery_target='2222')
    model = Model()
    outcome = run_scheduled(job, model, runtime, registry, send)
    assert model.inspections == 1 and calls.count('calendar_list') == 1 and calls.count('email_read') == 70, outcome
    assert outcome['execution_outcome']['execution_succeeded'] is True, outcome
    assert len(deliveries) == 1 and 'No duplicate was created' in deliveries[0]['text']
    assert 'Preview:' not in deliveries[0]['text'] and 'event-45' not in deliveries[0]['text']
    from nullion.runtime_persistence import load_runtime_store
    rows = load_runtime_store(runtime.checkpoint_path).list_conversation_events()
    terminal = next(row for row in rows if row.get('event_type') == 'conversation.cron_run')
    assert terminal['response_fulfilled'] is True and terminal['remaining_work'] == []
    assert sum(row.get('event_type') == 'conversation.tool_timing' and row.get('tool_name') == 'calendar_list' for row in rows) == 1


@pytest.mark.parametrize('instructions', ['Report all three current readings.', '現在の3件の観測値をすべて報告。'])
@pytest.mark.parametrize('recovery_case', ['report_only', 'missing_source', 'report_then_missing_source', 'repeated_report_gap',
    'needs_user_input', 'blocked', 'review_unavailable', 'planning_unavailable',
    'composer_invents_gap', 'composer_reports_real_gap', 'composer_gap_review_unavailable'])
@pytest.mark.parametrize('source_family', ['public_alert', 'native_report'])
def test_recovered_report_uses_specific_review_feedback_without_restarting_research(tmp_path, monkeypatch, instructions, recovery_case, source_family):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from tests.e2e.test_cron_repair_runtime_contract import _monitor_completion_response
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime=setup_runtime(tmp_path,monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='feedback-read',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True);registry=ToolRegistry();reads=[]
    source_tool = 'web_fetch' if source_family=='public_alert' else 'collection_read'
    additional_read_needed = recovery_case in {'missing_source', 'report_then_missing_source', 'composer_reports_real_gap'}
    def read(inv):
        reads.append(inv)
        if recovery_case=='blocked' and inv.arguments['url'].endswith('/blocked'):
            return ToolResult(inv.invocation_id,inv.tool_name,'failed',
                {'reason':'source_connection_required'},'Reconnect the source account.')
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',
            {'url':inv.arguments['url'],'text':('Beta 41.' if len(reads)>1 else
                'Current collection: Alpha 37, Gamma 53.' if additional_read_needed else
                'Current collection: Alpha 37, Beta 41, Gamma 53.')})
    registry.register(ToolSpec(source_tool,'Read current collection',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    class Model:
        producer=0;repairs=0;checked_drafts=[];planner_calls=0
        def create(self,**kw):
            system=kw.get('system','')
            if system.startswith('You are a material-requirements planner'):
                self.planner_calls+=1
                if recovery_case=='planning_unavailable':
                    raise TimeoutError('Requirement planning connection unavailable')
                return text_response(json.dumps({'all_material_requirements_enumerated':True,
                    'material_requirements':[{'requirement':'Report all three current readings.',
                        'requirement_kind':'fact','evidence_subject_id':'collection',
                        'eligible_evidence_tool_names':[source_tool]}]}))
            if system.startswith('You are a semantic completion reviewer'):
                if recovery_case in {'review_unavailable', 'composer_gap_review_unavailable'}:
                    raise TimeoutError('Verification connection unavailable')
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                self.checked_drafts.append(packet['draft_final_response'])
                response=_monitor_completion_response(kw);decision=json.loads(response['content'][0]['text'])
                if recovery_case in {'needs_user_input', 'blocked'}:
                    decision.update(disposition=recovery_case,retry_tool_names=[],
                        unresolved_requirements=['Select the necessary collection.' if recovery_case=='needs_user_input' else 'Reconnect the source account.'])
                    for row in decision['material_requirements']:row.update(satisfied=False,draft_covered=False)
                    if recovery_case=='needs_user_input':
                        decision['clarification']={'question':'Which collection should be used?',
                            'options':['First collection','Second collection']}
                    else:
                        decision['user_facing_failure']='The source requires an account connection. Reconnect it before retrying.'
                        # A mixed successful/blocked source batch needs explicit
                        # causal evidence; the internal diagnostic is not proof.
                        failures=[r['invocation_id'] for r in packet['tool_evidence']
                            if r['tool_name']==source_tool and r['status']=='failed']
                        assert failures
                        contracts={r['requirement_id']:r for r in packet['material_requirement_contract']}
                        for row in decision['material_requirements']:
                            if source_tool in contracts[row['requirement_id']]['eligible_evidence_tool_names']:
                                row['blocking_failure_invocation_ids']=failures
                    return text_response(json.dumps(decision))
                complete=all(word in packet['draft_final_response'] for word in ('Alpha 37','Beta 41','Gamma 53'))
                if recovery_case == 'repeated_report_gap':
                    complete = False
                needs_read = bool(additional_read_needed and
                    (recovery_case != 'report_then_missing_source' or self.repairs > 1) and len(reads) < 2)
                if not complete or needs_read:
                    decision.update(disposition='retry',retry_tool_names=[source_tool] if needs_read else [],
                        unresolved_requirements=['Read https://example.org/beta to obtain Beta, then include Alpha 37 and Beta 41 alongside Gamma 53.'
                            if needs_read else 'Use the existing verified collection to include Alpha 37 and Beta 41 alongside Gamma 53.'])
                    for row in decision['material_requirements']:row.update(satisfied=not needs_read,draft_covered=False)
                if recovery_case == 'repeated_report_gap':
                    decision['user_facing_failure'] = ('Verified readings: Alpha 37; Beta 41; Gamma 53. '
                        "I couldn't finish the requested report format. The task is paused; retry it manually.")
                    # Public findings need their own current-source certificate;
                    # the legacy diagnostic field is internal recovery prose.
                    ref=next(row['invocation_id'] for row in packet['tool_evidence']
                        if row['tool_name']==source_tool and row['status']=='completed')
                    decision['supported_findings']=[{'text':'Verified readings: Alpha 37; Beta 41; Gamma 53.',
                        'evidence_invocation_ids':[ref]}]
                return text_response(json.dumps(decision))
            auxiliary=_monitor_completion_response(kw) or workflow_provider_reply(kw)
            if auxiliary is not None:return auxiliary
            if system.startswith('You repair scheduled-task final answers'):
                self.repairs+=1
                assert self.planner_calls <= 2 if recovery_case=='planning_unavailable' else self.planner_calls==1
                prompt=kw['messages'][0]['content'][0]['text']
                assert 'Verified compact tool evidence' in prompt
                if recovery_case!='planning_unavailable':
                    assert 'Verified material outcome contract' in prompt and 'Report all three current readings.' in prompt
                if self.repairs>1:
                    prompt=kw['messages'][0]['content'][0]['text']
                    assert 'include Alpha 37 and Beta 41' in prompt
                    assert 'Gamma 53' in prompt.split('Rejected draft final response:\n')[1].split('\n\n')[0]
                composer_claims_gap = recovery_case in {'composer_invents_gap', 'composer_reports_real_gap', 'composer_gap_review_unavailable'}
                return text_response(json.dumps({'requirements_satisfied':not composer_claims_gap,'source_state':'available',
                    'remaining_work':['Read https://example.org/beta to obtain Beta.' if additional_read_needed else
                        'Recheck an optional blocked discovery source when it becomes accessible.'] if composer_claims_gap else [],
                    'evidence_refs':['e0'],'delivery_decision':'send','delivery_reason':'report',
                    'report': 'Alpha 37; Beta 41; Gamma 53.' if recovery_case in {'composer_invents_gap', 'composer_gap_review_unavailable','planning_unavailable'} else
                        'Gamma 53.' if self.repairs==1 or recovery_case=='repeated_report_gap' else 'Alpha 37; Beta 41; Gamma 53.'}))
            if 'Classify delivery' in system:return text_response(json.dumps({'decision':'send','reason':'verified_match'}))
            self.producer+=1
            if self.producer==1:
                response=tool_response(source_tool,{'url':'https://example.org/readings'})
                if recovery_case=='blocked':
                    response['content'].append({'type':'tool_use','id':'blocked-read','name':source_tool,
                        'input':{'url':'https://example.org/blocked'}})
                return response
            if self.producer<=3:raise TimeoutError('Provider ended after current-source collection')
            if additional_read_needed:
                context=json.dumps(kw['messages'],ensure_ascii=False)
                assert 'outcome_review' in context and 'https://example.org/beta' in context
                assert 'current_draft' in context and 'Gamma 53.' in context
                if self.producer==4:return tool_response(source_tool,{'url':'https://example.org/beta'})
                return text_response('Alpha 37; Beta 41; Gamma 53.')
            pytest.fail('Report-only feedback restarted producer execution')
    job=crons.add_cron('Collection report','* * * * *',instructions,enabled=False,
        delivery_channel='telegram',delivery_target='2222',artifact_delivery_options=(
            {'alert_delivery_mode':'verified_matches_only'} if source_family=='public_alert' else {}))
    deliveries,send=telegram_boundary(monkeypatch);model=Model();outcome=run_scheduled(job,model,runtime,registry,send)
    assert 1 <= model.planner_calls <= (2 if recovery_case=='planning_unavailable' else 1)
    if recovery_case in {'review_unavailable', 'composer_gap_review_unavailable'}:
        assert len(reads) == 1 and model.producer == 3 and model.repairs == 1
        assert outcome['response_presentation']['outcome_review_status'] == 'unavailable'
        assert not outcome.get('cron_completion_continuations')
        if source_family == 'public_alert':
            assert not deliveries and outcome['response_fulfilled'] is False
        else:
            # The editor cannot overwrite the producer's typed completion
            # state, including its explicit unfinished-work declaration.
            assert outcome['response_fulfilled'] is (recovery_case == 'review_unavailable')
            assert not outcome.get('cron_report_failed')
            assert outcome['response_presentation']['outcome_review_role'] == 'helper'
            assert outcome['execution_outcome']['completion_verified'] is False
            assert len(deliveries) == 1 and 'Gamma 53.' in deliveries[0]['text'], json.dumps({
                key: value for key, value in outcome.items() if key not in {'tool_results', 'messages_snapshot'}
            }, default=str)
            assert 'Review warning:' not in deliveries[0]['text']
        return
    if source_family=='native_report' and recovery_case in {'needs_user_input','blocked','planning_unavailable','repeated_report_gap'}:
        assert len(deliveries)==1 and 'Gamma 53.' in deliveries[0]['text']
        assert 'Review warning:' not in deliveries[0]['text']
        assert 'Replace the' not in deliveries[0]['text']
        assert not outcome.get('cron_report_failed')
        assert outcome['execution_outcome'].get('completion_verified') is False
        assert not crons.get_cron(job.id).enabled
        if recovery_case=='blocked':
            assert any(row.status=='failed' for row in outcome['tool_results'])
        if recovery_case=='needs_user_input':
            assert '1. First collection' in outcome['text'] and '2. Second collection' in outcome['text']
        return
    if recovery_case in {'needs_user_input', 'blocked', 'review_unavailable', 'planning_unavailable', 'composer_gap_review_unavailable'}:
        assert outcome['response_fulfilled'] is False and outcome['cron_report_failed'],outcome
        assert len(reads)==(2 if recovery_case=='blocked' else 1) and model.producer==3
        # A failed request planner cannot certify an alert, but the shared
        # content producer may still make its single bounded salvage attempt.
        assert model.repairs==1
        assert not crons.get_cron(job.id).enabled
        if source_family=='public_alert':
            assert not deliveries
        else:
            assert len(deliveries)==1 and 'Gamma 53.' not in deliveries[0]['text']
        if recovery_case=='needs_user_input':
            assert '1. First collection' in outcome['text'] and '2. Second collection' in outcome['text']
            assert outcome['response_presentation']['needs_input'] is True
        elif recovery_case=='blocked':
            assert 'Reconnect it before retrying' in outcome['text']
        else:
            assert outcome['cron_verification_failed'] and outcome['reason']=='cron_verification_unavailable'
        return
    if recovery_case == 'repeated_report_gap':
        assert outcome['response_fulfilled'] is False and outcome['cron_report_failed'], outcome
        assert len(reads)==1 and model.producer==3 and model.repairs==2
        assert not crons.get_cron(job.id).enabled
        if source_family=='public_alert':assert not deliveries
        else:
            assert len(deliveries)==1
            assert all(value in deliveries[0]['text'] for value in ('Alpha 37','Beta 41','Gamma 53'))
            from html import unescape
            visible = unescape(deliveries[0]['text'])
            # Every requested reading is satisfied; a draft-only rejection
            # does not justify publishing a fabricated report-format gap.
            assert "couldn't finish the requested report format" not in visible
        return
    assert outcome['response_fulfilled'] is True and not outcome.get('cron_report_failed'), json.dumps({
        'counts': {'reads': len(reads), 'producer': model.producer, 'repairs': model.repairs},
        'drafts': model.checked_drafts,
        'result': {key: outcome.get(key) for key in ('text', 'response_fulfilled',
            'response_presentation', 'cron_source_verification_feedback', 'cron_report_failed',
            'cron_agent_final_repair_failed', 'cron_agent_final_repair_failure_reason')},
    }, default=str)
    assert len(reads)==(2 if additional_read_needed else 1)
    assert model.producer==(5 if additional_read_needed else 3)
    assert model.repairs==(1 if recovery_case in {'missing_source', 'composer_invents_gap', 'composer_reports_real_gap'} else 2)
    assert len(deliveries)==1 and all(value in deliveries[0]['text'] for value in ('Alpha 37','Beta 41','Gamma 53'))
    assert any('Alpha 37' in draft for draft in model.checked_drafts)
    assert not crons.get_cron(job.id).enabled


@pytest.mark.parametrize('requires_approval',[False,True])
@pytest.mark.parametrize('instructions',['Persist this run’s local checkpoint.','この実行の作業記録を保存。'])
def test_saved_disabled_task_checkpoint_is_execution_and_keeps_normal_approval(tmp_path,monkeypatch,requires_approval,instructions):
    from nullion import crons
    from nullion.memory import UserMemoryEntry,UserMemoryKind
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    from nullion.policy import BoundaryKind,GLOBAL_PERMISSION_PRINCIPAL,BoundaryPolicyRule,PolicyMode
    runtime=setup_runtime(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_MEMORY_ENABLED','1')
    runtime.store.add_user_memory_entry(UserMemoryEntry('consent','workspace:workspace_admin',UserMemoryKind.PREFERENCE,
        'creation_requires_confirmation','Ask before creating any new persistent tracker or recurring task.'))
    for kind in (BoundaryKind.FILESYSTEM_ACCESS,):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='checkpoint-'+kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL,kind=kind,mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    runtime.checkpoint(force=True)
    roots=workspace_storage_roots_for_principal('telegram:2222');path=roots.scratch/'observed.json';path.write_text('{"value":6}')
    registry=ToolRegistry(filesystem_allowed_roots=(roots.root,));writes=[];handler=_build_file_write_handler(roots.root,allowed_roots=[roots.root])
    def write(inv):writes.append(inv);return handler(inv)
    registry.register(ToolSpec('file_write','Write a local workspace support file',
        ToolRiskLevel.HIGH if requires_approval else ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,requires_approval,2),write)
    job=crons.add_cron('Existing check','* * * * *',instructions,enabled=False,delivery_channel='telegram',delivery_target='2222')
    class Model:
        step=0;checks=0
        def create(self,**kw):
            if 'Workflow action preflight.' in str(kw.get('system','')):
                packet=json.loads(kw['messages'][0]['content'][0]['text']);context=packet['request_context']
                assert 'Ask before creating' in context['known_user_context']
                execution=context['scheduled_execution_context']
                assert execution['phase']=='execute_saved_task' and execution['saved_task_id']==job.id
                assert execution['enabled'] is False and execution['schedule_changes_permitted'] is False
                self.checks+=1
                return text_response(json.dumps({'decision':'proceed','issues':[]}))
            reply=workflow_provider_reply(kw,completion=True)
            if reply is not None:return reply
            self.step+=1
            if self.step==1:return tool_response('file_write',{'path':str(path),'content':'{"value":7}'})
            return text_response('The current checkpoint contains value 7; no new recurring task was created.')
    deliveries,send=telegram_boundary(monkeypatch);model=Model();outcome=run_scheduled(job,model,runtime,registry,send)
    assert model.checks==1 and not crons.get_cron(job.id).enabled
    if requires_approval:
        assert outcome['suspended_for_approval'] and not writes and path.read_text()=='{"value":6}',outcome
    else:
        assert outcome['response_fulfilled'] is True and len(writes)==1 and path.read_text()=='{"value":7}',outcome
        assert len(deliveries)==1 and 'value 7' in deliveries[0]['text']
    assert runtime.store.get_user_memory_entry('consent').use_score==0


@pytest.mark.parametrize('task', ['条件に一致する新しい記録のみ通知。', 'Notifica solo registros nuevos que coincidan.'])
@pytest.mark.parametrize('failure', ['truncated', 'invalid', 'incomplete', 'permanent', 'exception'])
def test_alert_decision_recovers_provider_format_without_repeating_research(tmp_path, monkeypatch, task, failure):
    from dataclasses import dataclass, field
    from nullion import crons
    from nullion.tools import ToolRegistry, register_web_fetch_tool
    runtime = setup_runtime(tmp_path, monkeypatch)
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-source',
        principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=BoundaryKind.OUTBOUND_NETWORK,
        mode=PolicyMode.ALLOW, selector='*', created_by='fixture', created_at=datetime.now(UTC)))
    registry = ToolRegistry()
    reads = []
    def fetch(url, timeout):
        reads.append(url)
        return {'url': url, 'text': 'Current new record Q: 2028-04-09, Venue Q, USD 25.', 'status_code': 200}
    register_web_fetch_tool(registry, fetch)
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Monitor', '* * * * *', task, enabled=False, delivery_channel='telegram',
        delivery_target='2222', artifact_delivery_options={'alert_delivery_mode': 'verified_matches_only'})
    @dataclass
    class Client:
        reasoning_effort: str = 'high'
        calls: list = field(default_factory=list)
        research: list = field(default_factory=list)
        def create(self, **kwargs):
            if str(kwargs.get('system', '')).startswith('Classify delivery'):
                self.calls.append(kwargs)
                # The configured research profile must not leak into a tiny JSON decision.
                assert self.reasoning_effort == 'low'
                assert kwargs['max_tokens'] >= 1500
                if failure == 'exception': raise TimeoutError('Provider unavailable')
                if failure == 'incomplete' and len(self.calls) == 1:
                    return {**text_response('{"decision":"send","reason":"verified_match"}'),
                            'completion': {'status': 'incomplete', 'reason': 'max_output_tokens'}}
                if failure == 'permanent' or len(self.calls) == 1:
                    return {'stop_reason': 'max_tokens' if failure == 'truncated' else 'end_turn',
                            'content': [] if failure == 'truncated' else [{'type': 'text', 'text': '{}'}]}
                assert len(kwargs['messages']) == 2
                assert 'decision' in kwargs['messages'][-1]['content'][0]['text']
                return text_response('{"decision":"send","reason":"verified_match"}')
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            self.research.append(kwargs)
            if len(self.research) == 1: return tool_response('web_fetch', {'url': 'https://example.org/q'})
            return text_response('Record Q — 2028-04-09, Venue Q, USD 25. https://example.org/q')
    client = Client()
    outcome = run_scheduled(job, client, runtime, registry, send)
    assert len(reads) == 1 and len(client.research) == 2
    assert client.reasoning_effort == 'high'
    assert len(client.calls) == (1 if failure == 'exception' else 2)
    if failure in {'permanent', 'exception'}:
        assert outcome['cron_delivery_status'] == 'silent'
        assert outcome['cron_report_failed'] and outcome['response_fulfilled'] is False
        assert not deliveries
    else:
        assert outcome['cron_delivery_status'] == 'sent', outcome
        assert outcome['response_fulfilled'] is True
        assert len(deliveries) == 1 and 'Record Q' in deliveries[0]['text']
    from nullion.runtime_persistence import load_runtime_store
    persisted = load_runtime_store(runtime.checkpoint_path)
    terminal = [row for row in persisted.list_recent_conversation_events('telegram:2222', limit=200)
        if row.get('event_type') == 'conversation.cron_run' and row.get('cron_id') == job.id]
    assert len(terminal) == 1
    assert terminal[0]['alert_delivery_policy_failure'] == (
        'model_call_failed' if failure == 'exception' else 'invalid_structured_decision' if failure == 'permanent' else None)
    diagnostics = [e.payload for e in runtime.store.list_events()
        if e.event_type == 'cron.delivery.agent_preflight' and e.payload.get('stage') == 'alert_delivery_policy_response']
    assert len(diagnostics) == (0 if failure == 'exception' else 2)
    assert all('response_chars' in row and 'stop_reason' in row and 'schema_valid' in row for row in diagnostics)


@pytest.mark.parametrize('task', ['現在の記録を確認し、結果をこの会話に届ける。', 'Comprueba los registros actuales y entrega el informe aquí.'])
def test_ordinary_saved_report_verifies_content_before_scheduler_transport(tmp_path, monkeypatch, task):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry()
    reads = []
    def read(invocation):
        reads.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'results': [{'id': 'record-Q', 'amount': 37}], 'result_count': 1})
    registry.register(ToolSpec('collection_read', 'Read current records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Report', '* * * * *', task, enabled=False,
        delivery_channel='telegram', delivery_target='2222')
    class Client:
        calls = 0
        planning = 0
        def create(self, **kwargs):
            if str(kwargs.get('system', '')).startswith('You are a material-requirements planner'):
                self.planning += 1
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                assert packet['scheduled_delivery_owner'] == 'cron_delivery'
                assert packet['scheduled_execution_context']['saved_task_id'] == job.id
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'material_requirements': [
                        {'requirement': 'Current record amount', 'requirement_kind': 'fact',
                         'eligible_evidence_tool_names': ['collection_read'], 'evidence_subject_id': 'record'},
                        {'requirement': 'Deliver completed report', 'requirement_kind': 'action',
                         'completion_owner': 'cron_delivery', 'eligible_evidence_tool_names': [],
                         'evidence_subject_id': 'transport'}]}))
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            self.calls += 1
            if self.calls == 1: return tool_response('collection_read', {})
            return text_response('Record Q: 37.')
    client = Client()
    result = run_scheduled(job, client, runtime, registry, send)
    assert result['response_fulfilled'] is True and not result.get('cron_report_failed'), result
    assert result['cron_delivery_status'] == 'sent'
    assert len(reads) == 1 and client.planning == 1
    assert len(deliveries) == 1 and 'Record Q: 37' in deliveries[0]['text']
    from nullion.runtime_persistence import load_runtime_store
    rows = load_runtime_store(runtime.checkpoint_path).list_recent_conversation_events('telegram:2222', limit=100)
    terminal = [row for row in rows if row.get('event_type') == 'conversation.cron_run']
    assert len(terminal) == 1 and terminal[0]['response_fulfilled'] is True
    assert terminal[0]['tool_results'][0]['tool_name'] == 'collection_read'


@pytest.mark.parametrize('task', ['現在の記録Qを確認して報告。', 'Comprueba el registro Q actual y resume el resultado.'])
def test_saved_report_review_repairs_truncation_after_inspection_without_research_restart(tmp_path, monkeypatch, task):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry()
    reads = []
    def read(invocation):
        reads.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'text': 'Earlier source context. ' * 400 + 'Current record Q: value 37.'})
    registry.register(ToolSpec('collection_read', 'Read current records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Report', '* * * * *', task, enabled=False,
        delivery_channel='telegram', delivery_target='2222')
    class Client:
        producer_calls = 0
        review_calls = []
        def create(self, **kwargs):
            if str(kwargs.get('system', '')).startswith('You are a semantic completion reviewer'):
                self.review_calls.append(kwargs)
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                if len(self.review_calls) == 1:
                    ref = next(row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name'] == 'collection_read')
                    inspection = {'disposition': 'inspect_evidence', 'evidence_requests': [
                        {'invocation_id': ref, 'json_pointer': '/text', 'offset': len('Earlier source context. ') * 400, 'length': 100}]}
                    from jsonschema import Draft202012Validator
                    Draft202012Validator(packet['review_response_schema']).validate(inspection)
                    return text_response(json.dumps(inspection))
                assert packet['additional_evidence'][0]['content'] == 'Current record Q: value 37.'
                from jsonschema import Draft202012Validator
                assert not Draft202012Validator(packet['review_response_schema']).is_valid({
                    'disposition': 'inspect_evidence', 'evidence_requests': [{'invocation_id': 'e0000',
                        'json_pointer': '', 'offset': 0, 'length': 100}]})
                if len(self.review_calls) == 2:
                    return {'content': [{'type': 'text', 'text': '{"disposition":"complete",'}],
                        'completion': {'status': 'incomplete', 'reason': 'max_output_tokens'}}
                assert kwargs['max_tokens'] > self.review_calls[1]['max_tokens']
                assert packet['review_protocol_validation_errors'] == ['invalid_json']
                assert packet['review_protocol_correction']
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            self.producer_calls += 1
            if self.producer_calls == 1: return tool_response('collection_read', {})
            return text_response('Record Q: 37.')
    client = Client()
    result = run_scheduled(job, client, runtime, registry, send)
    assert result['response_fulfilled'] is True and not result.get('cron_report_failed'), result
    assert result['cron_delivery_status'] == 'sent'
    assert len(reads) == 1 and len(client.review_calls) == 3 and client.producer_calls == 2
    assert len(deliveries) == 1 and 'Record Q: 37' in deliveries[0]['text']


@pytest.mark.parametrize('task', ['確認した日付で記録を一度だけ作成。', 'Crea una sola vez el registro con la fecha verificada.'])
def test_saved_action_review_inspects_older_full_source_without_repeating_reads(tmp_path, monkeypatch, task):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    reads, writes = [], []
    target = tmp_path/'created-record.json'
    def read(invocation):
        index = invocation.arguments['index']; reads.append(index)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'id': str(index), 'body': ('Source context. ' * 600 + 'EXACT_DATE=2028-04-09') if index == 0 else 'Unrelated current record.'})
    def write(invocation):
        writes.append(invocation.arguments)
        target.write_text(json.dumps(invocation.arguments))
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'action_receipt': {'type': 'action_receipt', 'action': 'create', 'object_type': 'record',
                               'summary': 'Created record with verified date 2028-04-09'}})
    registry.register(ToolSpec('collection_read', 'Read a source record', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    registry.register(ToolSpec('record_create', 'Create the requested record', ToolRiskLevel.MEDIUM,
        ToolSideEffectClass.WRITE, False, 2), write)
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Report', '* * * * *', task, enabled=False,
        delivery_channel='telegram', delivery_target='2222')
    class Client:
        producer_calls = 0
        action_calls = 0
        def create(self, **kwargs):
            if 'Workflow action preflight.' in str(kwargs.get('system', '')):
                self.action_calls += 1
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                if self.action_calls == 1:
                    # The original proof is older than the sixteen-result preview.
                    assert not any(row['output'].get('id') == '0' for row in packet['execution_evidence'])
                    first = packet['retained_evidence_index']['records'][0]
                    assert first['tool_name'] == 'collection_read'
                    inspection = {'disposition': 'inspect_evidence', 'evidence_requests': [
                        {'invocation_id': first['reference'], 'json_pointer': '/body',
                         'offset': len('Source context. ') * 600, 'length': 100}]}
                    from jsonschema import Draft202012Validator
                    Draft202012Validator(packet['response_schema']).validate(inspection)
                    return text_response(json.dumps(inspection))
                assert packet['additional_evidence'][0]['content'] == 'EXACT_DATE=2028-04-09'
                from jsonschema import Draft202012Validator
                decision = {'decision':'proceed','issues':[]}
                Draft202012Validator(packet['response_schema']).validate(decision)
                return text_response('{"decision":"proceed","issues":[]}')
            protocol = workflow_provider_reply(kwargs, completion=True, action=True)
            if protocol is not None: return protocol
            self.producer_calls += 1
            if self.producer_calls == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'read-'+str(i),
                    'name': 'collection_read', 'input': {'index': i}} for i in range(21)]}
            if self.producer_calls == 2: return tool_response('record_create', {'date': '2028-04-09'})
            return text_response('Created the record for 2028-04-09.')
    client = Client()
    outcome = run_scheduled(job, client, runtime, registry, send)
    assert outcome['response_fulfilled'] is True and outcome['cron_delivery_status'] == 'sent', outcome
    assert reads == list(range(21)) and writes == [{'date': '2028-04-09'}]
    assert json.loads(target.read_text()) == {'date': '2028-04-09'}
    assert client.action_calls == 2 and len(deliveries) == 1


@pytest.mark.parametrize('task', ['条件に一致する場合だけ通知。', 'Notifica solo si se cumple la condición.'])
@pytest.mark.parametrize('decision', ['send', 'silent'])
@pytest.mark.parametrize('empty', [False, True])
def test_saved_alert_reuses_bound_full_evidence_review_for_delivery(tmp_path, monkeypatch, task, decision, empty):
    from nullion import crons
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(); reads = []
    def read(invocation):
        reads.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'results': [] if empty else [{'id': 'record-Q', 'value': 37}], 'result_count': 0 if empty else 1})
    registry.register(ToolSpec('collection_read', 'Read current records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    deliveries, send = telegram_boundary(monkeypatch)
    job = crons.add_cron('Report', '* * * * *', task, enabled=False, delivery_channel='telegram',
        delivery_target='2222', artifact_delivery_options={'alert_delivery_mode': 'verified_matches_only'})
    class Client:
        producer = 0
        legacy_calls = 0
        def create(self, **kwargs):
            system = str(kwargs.get('system', ''))
            if system.startswith('Classify delivery'):
                self.legacy_calls += 1
                time.sleep(4)  # The previously slow dependency must stay off this path.
                return text_response(json.dumps({'decision': decision,
                    'reason': 'verified_match' if decision == 'send' else 'no_verified_match'}))
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                if system.startswith('You are a semantic completion reviewer'):
                    packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                    assert packet['scheduled_alert_delivery_mode'] == 'verified_matches_only'
                    value = json.loads(protocol['content'][0]['text'])
                    value['scheduled_alert_delivery'] = {'decision': decision,
                        'reason': 'verified_match' if decision == 'send' else 'no_verified_match'}
                    return text_response(json.dumps(value))
                return protocol
            self.producer += 1
            if self.producer == 1: return tool_response('collection_read', {})
            return text_response(('Verified condition: no current records.' if empty else 'Verified record Q: 37.')
                if decision == 'send' else 'Checked current records. None meets the condition.')
    elapsed = []
    for run in range(3):
        if run:
            job = crons.add_cron('Report '+str(run), '* * * * *', task, enabled=False,
                delivery_channel='telegram', delivery_target='2222',
                artifact_delivery_options={'alert_delivery_mode': 'verified_matches_only'})
        client = Client(); started = time.monotonic()
        result = run_scheduled(job, client, runtime, registry, send)
        elapsed.append(time.monotonic()-started)
        assert result['response_fulfilled'] is True and not result.get('cron_report_failed'), result
        assert result['cron_delivery_status'] == ('sent' if decision == 'send' else 'silent')
        assert client.legacy_calls == 0
    assert sum(elapsed)/len(elapsed) < 3 and max(elapsed) < 4, elapsed
    assert len(reads) == 3 and len(deliveries) == (3 if decision == 'send' else 0)
    if deliveries: assert ('no current records' if empty else 'Verified record Q: 37') in deliveries[0]['text']
    from nullion.runtime_persistence import load_runtime_store
    terminal = [row for row in load_runtime_store(runtime.checkpoint_path).list_recent_conversation_events('telegram:2222', limit=200)
        if row.get('event_type') == 'conversation.cron_run']
    assert len(terminal) == 3 and all(row['alert_delivery_decision'] == decision for row in terminal)
    events = [row for row in runtime.store.list_events()
        if row.payload.get('stage') == 'alert_delivery_policy_decided']
    assert len(events) == 3 and all(row.payload['verified_by'] == 'completion_review' for row in events)


@pytest.mark.parametrize('decision', ['send', 'silent'])
def test_web_saved_alert_persists_only_the_bound_public_delivery(tmp_path, monkeypatch, decision):
    from fastapi.testclient import TestClient
    from nullion import crons, config, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(); reads = []
    def read(invocation):
        reads.append(invocation.invocation_id)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'value': 37})
    registry.register(ToolSpec('collection_read', 'Read selected records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Model:
        producer = 0
        def create(self, **kwargs):
            assert not str(kwargs.get('system', '')).startswith('Classify delivery')
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                if str(kwargs.get('system', '')).startswith('You are a semantic completion reviewer'):
                    value = json.loads(protocol['content'][0]['text'])
                    value['scheduled_alert_delivery'] = {'decision': decision,
                        'reason': 'verified_match' if decision == 'send' else 'no_verified_match'}
                    return text_response(json.dumps(value))
                return protocol
            self.producer += 1
            if self.producer == 1: return tool_response('collection_read', {})
            return text_response('Verified current condition: 37.' if decision == 'send' else 'The current condition does not match.')
    settings = NullionSettings(doctor_enabled=False)
    monkeypatch.setattr(config, 'load_settings', lambda *a, **k: settings)
    monkeypatch.setattr(web_app, 'load_settings', lambda *a, **k: settings)
    schedulers = []
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: schedulers.append(self))
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=Model()), registry=registry)
    with TestClient(app) as client:
        assert not client.get('/api/chat/history/web%3Averified-alert').json()['messages']
        created = client.post('/api/crons', json={'name': '条件', 'schedule': '* * * * *',
            'task': '選択した条件に一致するときだけ通知。', 'enabled': True,
            'delivery_channel': 'web', 'delivery_target': 'web:verified-alert',
            'artifact_delivery_options': {'alert_delivery_mode': 'verified_matches_only'}})
        assert created.status_code == 200, created.text
        ident = created.json()['job']['id']; job = crons.get_cron(ident)
        job.next_run = (datetime.now(UTC)-timedelta(minutes=1)).isoformat(); crons.save_crons([job])
        schedulers[-1]._tick()
        from cron_scheduler_helpers import finish_scheduler
        finish_scheduler(schedulers[-1])
        history = client.get('/api/chat/history/web%3Averified-alert').json()
        messages = [row for row in history['messages'] if row.get('role') == 'bot']
        assert len(messages) == (1 if decision == 'send' else 0), history
        if messages:
            assert 'Verified current condition: 37' in messages[0]['text']
            assert messages[0]['text'].count('SCHEDULED TASK:') == 1
        assert '_verified_cron_alert_decision' not in json.dumps(history)
        assert crons.get_cron(ident).last_result == 'ok'
        assert len(reads) == 1
