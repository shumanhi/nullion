"""Real API/SQLite/native dispatch, with only source/provider boundaries faked."""
import json
import asyncio
import os
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def call(name, arguments):
    from tests.e2e.test_quoted_reply_review_and_cleanup import call as provider_call
    reply = provider_call(name, arguments)
    reply['content'][0]['id'] = 'call-' + uuid4().hex
    return reply


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('mode,effort,expected,projection_repair', [
    ('off','standard',3,False), ('on','standard',3,False),
    ('off','exhaustive',8,False), ('on','exhaustive',8,False), ('on','exhaustive',8,True)])
def test_research_allowance_returns_findings_and_preserves_exhaustive_coverage(tmp_path, monkeypatch, mode, surface, effort, expected, projection_repair, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from datetime import datetime, UTC
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('source-fixture', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    runtime.checkpoint()
    save_preferences({'outbound_request_mode':'allow_all'})
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED', 'true')
    monkeypatch.setenv('NULLION_RESEARCH_TOOL_CALLS', '3')
    monkeypatch.setenv('NULLION_RESEARCH_MODEL_CALLS', '50')
    visits = []
    def source(inv):
        if inv.arguments['url'].endswith('/optional'):
            time.sleep(2)
        visits.append(inv.arguments['url'])
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'url':inv.arguments['url'], 'text':'Verified measure 37; dated 2031-04-08.\n'+'Context. '*2000})
    registry = ToolRegistry()
    registry.register(ToolSpec('web_fetch', 'Read public source', ToolRiskLevel.LOW, ToolSideEffectClass.READ,
        False, 5, capability_tags=('web',), same_request_read_cache=True,
        input_schema={'type':'object', 'properties':{'url':{'type':'string'}}, 'required':['url']}), source)
    # App startup may install the core web adapter. Replace only its external
    # transport; keep scope, policy, adapter receipts and persistence real.
    from nullion import tools as native_tools
    monkeypatch.setattr(native_tools, '_default_web_fetcher', lambda url, timeout:
        source(SimpleNamespace(invocation_id='network', tool_name='web_fetch', arguments={'url':url})).output)
    urls = [f'https://source.test/{i}' for i in range(8)]
    class Model:
        phase = 0
        next_read = 0
        def create(self, **kwargs):
            reply = workflow_provider_reply(kwargs, completion=True)
            if reply is not None: return reply
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['web'], 'tool_names':['web_fetch'],
                    'required_tool_names':['web_fetch'], 'research_effort':effort,
                    'research_questions':['必要な数値と日付', 'Fuentes de las observaciones']})
            writing = '"phase": "writing"' in str(kwargs.get('system'))
            if not writing and mode == 'on' and self.phase == 2:
                code = 'facts=[]\nsources=[]\n'
                for url in urls:
                    code += "saved=call_tool('web_fetch', {'url':"+repr(url)+"})\n"
                    code += "again=call_tool('web_fetch', {'url':"+repr(url)+"})\n"
                    code += "facts.append(saved['output']['text'].split('\\n')[0])\nsources.append(saved['output']['url'])\n"
                code += "{'facts':facts,'sources':sources,'unused':'x'*20000}"
                return call('code_mode', {'tool_names':['web_fetch'], 'code':code,
                    'output_fields':['/missing'] if projection_repair else ['/facts','/sources']})
            if mode == 'on' and projection_repair and self.phase == 3:
                assert 'projection_error' in json.dumps(kwargs['messages'])
                return call('code_mode', {'tool_names':[], 'code':"{'facts':facts,'sources':sources}",
                    'output_fields':['/facts','/sources']})
            if not writing and mode == 'off' and self.next_read < 16:
                url = urls[self.next_read//2]; self.next_read += 1
                return call('web_fetch', {'url':url})
            wire = json.dumps(kwargs['messages'])
            assert '37' in wire and '2031-04-08' in wire
            if effort == 'standard':
                assert writing and self.phase <= 10
                return text('Verified measure 37, dated 2031-04-08. '+urls[0]+
                    '\nResearch reached its allowance. The remaining selected sources were not checked; full coverage is unverified.')
            assert len(visits) % 8 == 0
            return text('Verified measure 37, dated 2031-04-08. All eight selected sources were checked.\n'+'\n'.join(urls))
    model = Model(); elapsed = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        if surface == 'telegram':
            from nullion.config import load_settings
            from nullion.telegram_app import ChatOperatorService
            settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz',
                'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123', 'NULLION_TELEGRAM_CHAT_ENABLED':'true'})
            service = ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
                operator_chat_id='123', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
        for index, prompt in enumerate(['Revisa las fuentes seleccionadas.', '選択された情報源を確認。', 'تحقق من المصادر المحددة.']):
            model.phase = model.next_read = 0
            before = len(visits); cid = f'web:bounded-{index}'
            started = time.monotonic()
            if surface == 'telegram':
                sent = []
                class Message:
                    text = prompt; message_id = 2000+index; reply_to_message = None
                    chat = SimpleNamespace(id=123, send_action=lambda **kw: asyncio.sleep(0))
                    async def reply_text(self, value, **kwargs):
                        sent.append(value); return SimpleNamespace(message_id=3000+len(sent))
                asyncio.run(service.on_message(SimpleNamespace(update_id=2000+index, message=Message()), SimpleNamespace(bot=None)))
                assert sent
                data = {'text':sent[-1]}
                response = SimpleNamespace(status_code=200, text=json.dumps(data), json=lambda:data)
                cid = 'telegram:123'
            else:
                response = client.post('/api/chat', json={'text':prompt, 'conversation_id':cid})
            elapsed.append(time.monotonic()-started)
            assert response.status_code == 200 and '37' in response.json()['text'], response.text
            assert len(visits)-before == expected, visits[before:]
            assert len(set(visits[before:])) == expected
            if effort == 'standard': assert 'unverified' in response.json()['text']
            else: assert 'eight' in response.json()['text']
            events = runtime.store.list_recent_conversation_events(cid)
            budget = next(e for e in reversed(events) if e['event_type']=='conversation.research_budget')
            assert budget['usage']['research_native_calls'] == expected
            if effort == 'standard': assert budget['phase'] == 'writing'
            turn = next(e for e in reversed(events) if e['event_type']=='conversation.chat_turn')
            reads = [r for r in turn['tool_results'] if r['tool_name']=='web_fetch' and r['status']=='completed']
            assert len(reads) >= expected
            if effort == 'exhaustive' and mode == 'on':
                program = next(r for r in reversed(turn['tool_results']) if r['tool_name']=='code_mode')
                assert program['output']['value']['/facts'][0].startswith('Verified measure 37')
                assert '/unused' not in program['output']['value']
    assert sum(elapsed)/len(elapsed) < 2 and max(elapsed) < 4, elapsed
    record_property('bounded_research_mean_seconds', sum(elapsed)/len(elapsed))


@pytest.mark.parametrize('mode', ['off', 'on'])
@pytest.mark.parametrize('prompt', ['観測した出典を保存。', 'Guarda la fuente verificada.'])
def test_observed_page_reference_writes_artifact_without_source_guard_retry(tmp_path, monkeypatch, mode, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import (ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass,
        _build_file_write_handler)
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    registry.register(ToolSpec('browser_navigate', 'Native public source read', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5), lambda inv:ToolResult(inv.invocation_id, inv.tool_name,
            'completed', {'page_url':'https://example.com', 'result_kind':'browser_navigation', 'result':'Example Domain'}))
    registry.register(ToolSpec('file_write', 'Save verified text', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE,
        False, 5), _build_file_write_handler(tmp_path, allowed_roots=(tmp_path,)))
    path = tmp_path/'観測.html'
    content = '<html><body><h1>Example Domain</h1><a href="https://example.com/">出典</a></body></html>'
    expected = content.replace("<body>", '<head><meta charset="utf-8"><title>観測</title></head><body>', 1)
    class Model:
        phase = 0
        def create(self, **kwargs):
            reply = workflow_provider_reply(kwargs, completion=True)
            if reply is not None: return reply
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['web','local_files'],
                    'tool_names':['browser_navigate','file_write'], 'required_tool_names':['browser_navigate','file_write'],
                    'artifact_extensions':['.html'], 'research_effort':'standard'})
            if mode == 'on' and self.phase == 2:
                return call('code_mode', {'tool_names':['browser_navigate','file_write'], 'code':
                    "call_tool('browser_navigate', {'url':'https://example.com'})\n"
                    +"call_tool('file_write', "+repr({'path':str(path), 'content':content})+")"})
            if mode == 'off':
                if self.phase == 2:return call('browser_navigate', {'url':'https://example.com'})
                if self.phase == 3:return call('file_write', {'path':str(path), 'content':content})
            assert path.read_text() == expected
            return text('保存しました。 '+str(path))
    result = AgentOrchestrator(model_client=Model()).run_turn(conversation_id='web:verified-reference',
        principal_id='workspace_admin', user_message=prompt, conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store)
    writes = [row for row in result.tool_results if row.tool_name == 'file_write']
    assert len(writes) == 1 and writes[0].status == 'completed', writes
    assert path.read_bytes() == expected.encode('utf-8')
    assert str(path) in result.artifacts


@pytest.mark.parametrize('mode', ['off', 'on'])
def test_explicit_refresh_invalidates_both_execution_and_request_snapshots(tmp_path, monkeypatch, mode):
    from datetime import datetime, UTC
    from fastapi.testclient import TestClient
    from nullion import tools as native_tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    runtime.checkpoint()
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    monkeypatch.setenv('NULLION_DIRECT_WEB_FETCH_ENABLED', 'true')
    visits = []
    def read(url, timeout):
        visits.append(url)
        return {'url':url, 'text':f'Verified measure {36+len(visits)}.'}
    monkeypatch.setattr(native_tools, '_default_web_fetcher', read)
    url = 'https://source.test/current'
    class Model:
        phase = 0
        def create(self, **kwargs):
            reply = workflow_provider_reply(kwargs, completion=True)
            if reply is not None: return reply
            self.phase += 1
            scope = {'capabilities':['web'], 'tool_names':['web_fetch'], 'required_tool_names':['web_fetch']}
            if self.phase == 1: return call('request_tool_scope', scope)
            if mode == 'on':
                if self.phase == 2:
                    return call('code_mode', {'tool_names':['web_fetch'], 'code':
                        "first=call_tool('web_fetch', {'url':"+repr(url)+"})\ncall_tool('web_fetch', {'url':"+repr(url)+"})"})
                if self.phase == 3:
                    assert len(visits) == 1
                    return call('request_tool_scope', {**scope, 'research_refresh':True})
                if self.phase == 4:
                    return call('code_mode', {'tool_names':['web_fetch'], 'code':"call_tool('web_fetch', {'url':"+repr(url)+"})"})
            else:
                if self.phase in {2,3,5}: return call('web_fetch', {'url':url})
                if self.phase == 4:
                    assert len(visits) == 1
                    return call('request_tool_scope', {**scope, 'research_refresh':True})
            assert len(visits) == 2
            return text('The first observation was 37. The explicitly refreshed observation was 38. '+url)
    model = Model(); registry = ToolRegistry()
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        response = client.post('/api/chat', json={'text':'確認してから新しい観測を取得。', 'conversation_id':'web:refresh'})
        assert response.status_code == 200 and '38' in response.json()['text'], response.text
        assert visits == [url, url]
        events = runtime.store.list_recent_conversation_events('web:refresh')
        budget = next(e for e in reversed(events) if e['event_type']=='conversation.research_budget')
        assert budget['usage']['research_native_calls'] == 2


@pytest.mark.parametrize('prompt', ['照合して結果を教えてください。', 'Contrasta ambas fuentes.'])
def test_required_source_keeps_a_native_attempt_after_a_slow_read(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'off')
    monkeypatch.setenv('NULLION_RESEARCH_SECONDS', '2')
    first, second = 'fixture_primary_read', 'fixture_comparison_read'
    names = [first, second]
    registry = ToolRegistry()

    def read(invocation):
        if invocation.arguments.get('attempt') == 2:
            time.sleep(2.1)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
                          {'value': 37 if invocation.tool_name == first else 42})

    for name in names:
        registry.register(ToolSpec(name, 'Read a required observation', ToolRiskLevel.LOW,
                                   ToolSideEffectClass.READ, False, 3,
                                   capability_tags=('web', 'explicit_scope_only', 'required_scope_only')), read)

    class Provider:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['web'], 'tool_names': names,
                                               'required_tool_names': names})
            if self.phase in (2, 3):
                return call(first, {'attempt': self.phase - 1})
            if self.phase == 4:
                time.sleep(.25)
                return call(second, {})
            return text('37 / 42')

    model = Provider()
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        conversation = 'web:required-source-reservation'
        started = time.monotonic()
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
        assert time.monotonic() - started < 3
        assert response.status_code == 200 and '37 / 42' in response.json()['text'], response.json()
        turn = runtime.store.list_recent_conversation_events(conversation, event_type='conversation.chat_turn')[-1]
        assert turn['response_fulfilled'] is True
        assert {row['tool_name'] for row in turn['tool_results'] if row['status'] == 'completed'} >= set(names), [
            (row['tool_name'], row['status'], row.get('output', {}).get('reason'), row.get('output', {}).get('required_tool_names'))
            for row in turn['tool_results']]
