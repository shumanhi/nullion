from __future__ import annotations

from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E only")


def text_response(text):
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}


def runtime_and_registry(tmp_path, monkeypatch):
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import build_default_tool_registry, register_cron_tools
    checkpoint = tmp_path / "runtime.db"
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", checkpoint)
    runtime = bootstrap_persistent_runtime(checkpoint)
    registry = build_default_tool_registry(workspace_root=tmp_path, allowed_roots=[tmp_path])
    register_cron_tools(registry)
    return runtime, registry


@pytest.mark.parametrize('saved_request', ['Read entries, check duplicate events, and report the result.', '記録を読み、予定の重複を確認して結果を報告。'])
def test_scheduled_source_observations_do_not_end_before_remaining_checks(tmp_path, monkeypatch, saved_request):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.crons import CronJob
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from types import SimpleNamespace
    runtime, _=runtime_and_registry(tmp_path, monkeypatch)
    registry=ToolRegistry(); called=[]
    for name, output in [('lookup_entries', {'result_kind':'source_observations','report_status':'completed',
        'observations':[{'label':'期限','value':'19:30','quote':'19:30'}],'unknowns':[]}),
        ('lookup_schedule', {'events':[{'id':'existing','time':'19:30'}]})]:
        def handler(invocation, output=output):
            called.append(invocation.tool_name)
            if invocation.tool_name == 'lookup_schedule':
                from nullion.runtime_persistence import load_runtime_store
                # A concurrent refresh must not discard this execution's audit.
                runtime.store = load_runtime_store(runtime.checkpoint_path)
            return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', output)
        registry.register(ToolSpec(name=name, description=name, risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=2), handler)
    class Model:
        calls=0
        def create(self, **kwargs):
            if 'material-requirements planner' in str(kwargs.get('system') or ''):
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                assert packet['scheduled_execution_context']['schedule'] == '0 * * * *'
                assert packet['scheduled_execution_context']['schedule_changes_permitted'] is False
                assert packet['scheduled_execution_context']['current_datetime']
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            assert kwargs.get('tools'), 'A completed ordinary report needs no recovery model call'
            self.calls+=1
            if self.calls<=2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.calls),
                    'name':'lookup_entries' if self.calls==1 else 'lookup_schedule','input':{}}]}
            return text_response('19:30 — an existing matching event was checked; no duplicate was created.')
    model=Model()
    result=run_single_agent_cron_turn(CronJob(id='checks',name='確認',schedule='0 * * * *',task=saved_request),
        'web:checks',label='Manual scheduled task run',orchestrator=AgentOrchestrator(model_client=model),
        runtime=runtime,tool_registry=registry,settings=SimpleNamespace(),model_client=model)
    assert called==['lookup_entries','lookup_schedule'], result
    assert model.calls==3
    assert 'existing matching event was checked' in result['text']
    assert not result.get('cron_report_failed')
    assert len(result['tool_results'])==2
    from nullion.runtime_persistence import load_runtime_store
    persisted = load_runtime_store(runtime.checkpoint_path).list_conversation_events('web:checks')
    diagnostics = [row for row in persisted if row.get('cron_run_id') == result['cron_run_id']]
    assert {row['tool_name'] for row in diagnostics if row['event_type']=='conversation.tool_timing'} == set(called)
    assert any(row['event_type']=='conversation.model_timing' for row in diagnostics)
    assert not any(row['event_type']=='conversation.completion_review' for row in diagnostics)
    terminal = next(row for row in diagnostics if row['event_type']=='conversation.cron_run')
    assert terminal['response_fulfilled'] is True
    assert all(row['cron_id']=='checks' for row in diagnostics)


def test_stopped_cron_preserves_diagnostics_without_repairing_or_reporting_failure(tmp_path, monkeypatch):
    from threading import Event
    from types import SimpleNamespace
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.crons import CronJob
    from nullion import cron_delivery
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    registry = ToolRegistry()
    cancelled = Event()
    def handler(invocation):
        cancelled.set()
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'value': 37})
    registry.register(ToolSpec(name='source_read', description='Read source', risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=2), handler)
    class Model:
        def create(self, **kwargs):
            auxiliary = workflow_provider_reply(kwargs)
            if auxiliary is not None:
                return auxiliary
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'read',
                'name': 'source_read', 'input': {}}]}
    monkeypatch.setattr(cron_delivery, '_repair_cron_agent_final_text_if_needed',
        lambda *a, **k: pytest.fail('A cancelled execution entered report repair'))
    model = Model()
    result = cron_delivery.run_single_agent_cron_turn(
        CronJob(id='owned-stop', name='Stop acceptance', schedule='0 * * * *', task='確認して報告。'),
        'web:stop-acceptance', label='Manual scheduled task run', orchestrator=AgentOrchestrator(model_client=model),
        runtime=runtime, tool_registry=registry, settings=SimpleNamespace(), model_client=model,
        cancellation_checker=cancelled.is_set)
    assert result['cron_run_cancelled'] is True and result['reason'] == 'stopped_by_user'
    assert not result.get('cron_report_failed')
    assert len(result['tool_results']) == 1 and result['tool_results'][0].status == 'completed'
    from nullion.runtime_persistence import load_runtime_store
    terminal = next(row for row in load_runtime_store(runtime.checkpoint_path).list_conversation_events()
        if row.get('event_type') == 'conversation.cron_run')
    assert terminal['cancelled'] is True and terminal['report_failed'] is False


class DeleteCandidateClient:
    def __init__(self, target_id, request, authorized):
        self.target_id, self.request, self.authorized = target_id, request, authorized
        self.calls = []

    def create(self, **kwargs):
        protocol_reply = workflow_provider_reply(kwargs, completion=True)
        if protocol_reply is not None: return protocol_reply
        self.calls.append(kwargs)
        if not kwargs.get("tools"):
            payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
            assert payload["candidate_action"] == "delete_cron"
            assert payload["user_request"] == self.request
            assert payload["target"]["id"] == self.target_id
            return text_response(json.dumps({"authorized": self.authorized, "target_id": self.target_id,
                                             "user_evidence": self.request}))
        if len(self.calls) == 1:
            return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "delete-candidate",
                    "name": "delete_cron", "input": {"id": self.target_id}}]}
        return text_response("削除せずに修正を続けます。")


@pytest.mark.parametrize("user_prompt", ["The cron still produced the same html", "同じHTMLが出ました"])
def test_complaint_blocks_real_delete_and_creates_no_approval(tmp_path, monkeypatch, user_prompt):
    from nullion import crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime_persistence import load_runtime_store
    runtime, registry = runtime_and_registry(tmp_path, monkeypatch)
    job = crons.add_cron("Market report", "0 12 * * 1-5", "価格を緑と赤で表示")
    client = DeleteCandidateClient(job.id, user_prompt, False)
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123", principal_id="telegram_chat", user_message=user_prompt,
        conversation_history=[], tool_registry=registry, policy_store=runtime.store,
        approval_store=runtime.store, max_iterations=3,
    )
    assert crons.get_cron(job.id).task == job.task
    assert result.suspended_for_approval is False
    assert any(r.output.get("reason") == "scheduler_delete_not_user_authorized" for r in result.tool_results), result.tool_results
    assert runtime.store.list_approval_requests() == []
    assert load_runtime_store(tmp_path / "runtime.db").list_approval_requests() == []
    assert result.final_text


def test_intentional_delete_has_named_approval_and_keeps_job_pending(tmp_path, monkeypatch):
    from nullion import crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.telegram_app import _approval_card_text
    runtime, registry = runtime_and_registry(tmp_path, monkeypatch)
    job = crons.add_cron("Daily Pre-Market Stock Research", "0 12 * * 1-5", "価格を緑と赤で表示")
    request = "Daily Pre-Market Stock Researchを削除"
    result = AgentOrchestrator(model_client=DeleteCandidateClient(job.id, request, True)).run_turn(
        conversation_id="telegram:123", principal_id="telegram_chat", user_message=request,
        conversation_history=[], tool_registry=registry, policy_store=runtime.store,
        approval_store=runtime.store, max_iterations=3,
    )
    assert result.suspended_for_approval is True, result.tool_results
    approval = runtime.store.get_approval_request(result.approval_id)
    card = _approval_card_text(approval)
    assert job.name in card and job.id in card
    assert "stops all future scheduled runs" in card
    assert crons.get_cron(job.id) is not None


def test_plain_turn_latency_with_scheduler_installed_avoids_delete_classifier(tmp_path, monkeypatch):
    from nullion import crons
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime, registry = runtime_and_registry(tmp_path, monkeypatch)
    crons.add_cron("Installed report", "0 12 * * 1-5", "価格を緑と赤で表示")

    class Client:
        def __init__(self):
            self.calls = 0
            self.classifier_calls = 0

        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            self.calls += 1
            if not kwargs.get("tools"):
                self.classifier_calls += 1
                time.sleep(0.4)  # Previously slow optional dependency must stay off this path.
            return text_response("こんにちは。")

    elapsed = []
    for request in ["こんにちは", "Hola", "مرحبا", "Bonjour", "नमस्ते"]:
        client = Client()
        started = time.perf_counter()
        result = AgentOrchestrator(model_client=client).run_turn(
            conversation_id="telegram:123", principal_id="telegram_chat", user_message=request,
            conversation_history=[], tool_registry=registry, policy_store=runtime.store,
            approval_store=runtime.store, max_iterations=2,
        )
        elapsed.append(time.perf_counter() - started)
        assert result.final_text == "こんにちは。"
        assert client.calls == 1 and client.classifier_calls == 0
    assert sum(elapsed) / len(elapsed) < 0.25, elapsed
    assert sorted(elapsed)[-1] < 0.4, elapsed


@pytest.mark.parametrize("can_satisfy", [True, False])
def test_real_cron_runner_recovers_saved_design_and_delivers_html_bytes(tmp_path, monkeypatch, can_satisfy):
    from nullion import crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn, cron_delivery_text_from_result, cron_delivery_artifact_paths_from_result
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from types import SimpleNamespace
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda _: str(tmp_path / "artifacts"))
    instructions = 'AAAとBBBの価格。上昇は緑、下落は赤。data-template-version="watch-v2"。'
    job = crons.add_cron("Market report", "0 12 * * 1-5", instructions, artifact_delivery_options={
        "required_artifact_extensions": [".html"], "required_artifact_content_tokens": ["AAA", "BBB"],
        "requires_attachment_delivery": True, "deliver_required_extensions_only": True,
    })
    quotes = {"quotes": [{"symbol": "AAA", "price": 20, "change_percent": 2},
                         {"symbol": "BBB", "price": 10, "change_percent": -2}]}
    # Deterministic external quote service boundary; routing, runner, persistence and delivery stay real.
    registry = ToolRegistry()
    registry.register(ToolSpec(name="market_quote", description="Live quote service", risk_level=ToolRiskLevel.LOW,
                              side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=2),
                      lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", quotes))
    html = '<!doctype html><html data-template-version="watch-v2"><body><article style="background:green">AAA 20 +2%</article><article style="background:red">BBB 10 -2%</article></body></html>'

    class Client:
        def __init__(self):
            self.agent_calls = 0
            self.recovery_payload = None

        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            if not kwargs.get("tools"):
                payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
                assert payload["instructions"] == instructions
                self.recovery_payload = payload
                return text_response(json.dumps({"requirements_satisfied": can_satisfy, "html": html if can_satisfy else ""}))
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "quotes", "name": "market_quote", "input": {"symbols": ["AAA", "BBB"]}}]}
            raise TimeoutError("external provider timed out")

    client = Client()
    def record_event(event_type, job, channel, target, conversation_id, **extra):
        from nullion.events import make_event
        runtime.store.add_event(make_event(event_type, "cron_scheduler", {"cron_id": job.id, **extra}))

    result = run_single_agent_cron_turn(job, "telegram:123", label="Scheduled task",
        orchestrator=AgentOrchestrator(model_client=client), runtime=runtime, tool_registry=registry,
        settings=SimpleNamespace(enabled_skill_packs=()), record_event=record_event)
    assert client.recovery_payload is not None, result
    if not can_satisfy:
        assert result["artifact_delivery_satisfied"] is False
        assert result["cron_instruction_aware_artifact_recovery_failed"] is True
        assert not cron_delivery_artifact_paths_from_result(result)
        assert "MEDIA:" not in cron_delivery_text_from_result(result)
        return
    assert result["artifact_delivery_satisfied"] is True
    paths = cron_delivery_artifact_paths_from_result(result)
    assert len(paths) == 1 and Path(paths[0]).read_text() == html
    rendered_delivery = cron_delivery_text_from_result(result)
    assert "MEDIA:" in rendered_delivery and paths[0] in rendered_delivery
    assert result['response_fulfilled'] is True
    assert 'Partial report attached' not in rendered_delivery
    from playwright.sync_api import sync_playwright
    with sync_playwright() as browser_api:
        browser = browser_api.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.set_content(Path(paths[0]).read_text())
        assert page.locator("html").get_attribute("data-template-version") == "watch-v2"
        colors = page.locator("article").evaluate_all("cards => cards.map(c => getComputedStyle(c).backgroundColor)")
        assert colors == ["rgb(0, 128, 0)", "rgb(255, 0, 0)"]
        browser.close()
    assert any(event.event_type == "cron.delivery.agent_preflight" and event.payload.get("stage") == "structured_artifact_recovered"
               for event in runtime.store.list_events())


def _monitor_completion_response(kwargs):
    system = kwargs.get('system', '')
    if system.startswith('You are a material-requirements planner'):
        payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
        names = [row['name'] for row in payload['completed_current_turn_tools']
                 if row['name'] in {'web_fetch', 'browser_extract_text'}]
        return text_response(json.dumps({'all_material_requirements_enumerated': True,
            'material_requirements': [{'requirement': 'Verify matching current source records',
                'requirement_kind': 'fact', 'evidence_subject_id': 'monitor',
                'eligible_evidence_tool_names': names}]}))
    if system.startswith('You are a semantic completion reviewer'):
        payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
        row = payload['material_requirement_contract'][0]
        ids = [r['invocation_id'] for r in payload['tool_evidence']
               if r['tool_name'] in row['eligible_evidence_tool_names'] and r['status'] == 'completed']
        assert ids
        return text_response(json.dumps({'disposition': 'complete',
            'all_material_requirements_enumerated': True, 'unresolved_requirements': [],
            'material_requirements': [{'requirement_id': row['requirement_id'],
                'requirement': row['requirement'], 'requirement_kind': row['requirement_kind'],
                'satisfied': True, 'draft_covered': True, 'evidence_invocation_ids': ids[-1:]}]}))
    return None


@pytest.mark.parametrize('source_tool', ['web_fetch', 'browser_extract_text'])
@pytest.mark.parametrize('transport_ok,repair_ok', [(True, True), (False, True), (True, False)])
def test_alert_checkpoint_changes_only_after_verified_report_delivery(tmp_path, monkeypatch, transport_ok, repair_ok, source_tool):
    from types import SimpleNamespace
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import CronRunDeliveryCallbacks, run_single_agent_cron_turn, run_cron_delivery_workflow
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, _build_file_write_handler
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    roots = workspace_storage_roots_for_principal('telegram:123')
    checkpoint=roots.scratch/'checkpoint.json'; checkpoint.write_text('{"reported": []}')
    source='Source heading\n'+'older unrelated listings '*180+'\n公演 at BASEMENT\nNovember 7, 22:30\nhttps://example.com/tickets\n'
    report='**公演 at BASEMENT**\nNovember 7, 22:30\nVerified upcoming event. Price unavailable.\n[Tickets](https://example.com/tickets)'
    from datetime import UTC, datetime
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
        rule_id='source-fixture',principal_id=GLOBAL_PERMISSION_PRINCIPAL,kind=BoundaryKind.OUTBOUND_NETWORK,
        mode=PolicyMode.ALLOW,selector='*',created_by='fixture-user',created_at=datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    registry.register(ToolSpec(name=source_tool,description='Public source read',risk_level=ToolRiskLevel.LOW,
                              side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=2),
                      lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{'text':source}))
    registry.register(ToolSpec(name='file_write',description='Write checkpoint',risk_level=ToolRiskLevel.LOW,
                              side_effect_class=ToolSideEffectClass.WRITE,requires_approval=False,timeout_seconds=2),
                      _build_file_write_handler(workspace_root=tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False))
    class Client:
        calls=0
        repairs=0
        def create(self,**kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            if not kwargs.get('tools'):
                completion = _monitor_completion_response(kwargs)
                if completion is not None:
                    return completion
                system=kwargs.get('system','')
                if 'Classify delivery' in system:
                    return text_response(json.dumps({'decision':'send','reason':'verified_match'}))
                prompt=kwargs['messages'][0]['content'][0]['text']
                assert 'November 7' in prompt and 'https://example.com/tickets' in prompt
                self.repairs += 1
                if not repair_ok:
                    # A composer's pessimistic completion claim is not the
                    # delivery authority when independent evidence is valid.
                    # Exercise an actual failed composition protocol instead.
                    return text_response('{"requirements_satisfied":')
                return text_response(json.dumps({'requirements_satisfied':repair_ok,'source_state':'available',
                    'evidence_refs':['e0'],'report':report}))
            self.calls+=1
            if self.calls==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'source','name':source_tool,'input':{'url':'https://example.com/'}}]}
            if self.calls==2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'write','name':'file_write','input':{'path':str(checkpoint),'content':'{"reported": ["event"]}'}}]}
            raise TimeoutError('Simulated provider failure after source and checkpoint')
    client=Client(); job=SimpleNamespace(id='alert',name='Monitor',task='新しい公演を通知。価格は取得できれば記載。',artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'})
    events=[];sent=[];wire=[]
    import asyncio
    from nullion.telegram_app import _deliver_reply
    from nullion.cron_delivery import cron_structured_result_block_reason, record_cron_delivery_chat_turn
    class Message:
        chat=SimpleNamespace(id=123)
        message_id=701
        async def reply_text(self,text,**kwargs):
            wire.append((text,kwargs))
        async def reply_document(self,*args,**kwargs):
            raise AssertionError('An internal checkpoint must never be uploaded')
    def run(job,conv,**kwargs):
        result=run_single_agent_cron_turn(job,conv,label='Scheduled task',orchestrator=AgentOrchestrator(model_client=client),
            runtime=runtime,tool_registry=registry,settings=SimpleNamespace(enabled_skill_packs=()),model_client=client)
        assert json.loads(checkpoint.read_text())=={'reported':[]}
        return result
    def send(job,channel,target,text):
        assert json.loads(checkpoint.read_text())=={'reported':[]}
        sent.append(text)
        if transport_ok:
            asyncio.run(_deliver_reply(Message(),text,principal_id='telegram:123'))
        return transport_ok
    callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:'telegram',delivery_target=lambda j,c:'123',
        run_agent_turn=run,record_event=lambda kind,*a,**kw:events.append(kind),block_reason=lambda result,text,artifacts: cron_structured_result_block_reason(result,text=text,artifacts=artifacts),
        save_web_delivery=lambda *a:False,send_platform_delivery=send,
        record_chat_turn=lambda job,conv,channel,target,text:record_cron_delivery_chat_turn(runtime.store,job,
            conversation_id=conv,delivery_channel=channel,delivery_target=target,delivered_text=text))
    result=run_cron_delivery_workflow(job,label='Scheduled task',callbacks=callbacks)
    assert json.loads(checkpoint.read_text())=={'reported':['event'] if transport_ok and repair_ok else []}, result
    if repair_ok:
        assert any('November 7' in text for text in sent), result
        if transport_ok:
            assert any('<b>公演 at BASEMENT</b>' in text and '<a href="https://example.com/tickets">Tickets</a>' in text
                       and kwargs.get('parse_mode')=='HTML' for text,kwargs in wire)
            assert all('could not be completed' not in text for text,_ in wire)
            history=runtime.store.list_recent_conversation_events('telegram:123')
            assert any(row.get('cron_id')=='alert' and 'November 7' in row.get('assistant_reply','') for row in history)
            receipts=Path(os.environ['NULLION_DATA_DIR'])/'delivery_receipts.jsonl'
            assert json.loads(receipts.read_text().splitlines()[-1])['status']=='succeeded'
        assert all('MEDIA:' not in text and 'checkpoint.json' not in text for text in sent)
        write = next(row for row in result['tool_results'] if row.tool_name == 'file_write')
        assert write.output['path'] == str(checkpoint)
        assert write.output['artifact_descriptors'][0]['role'] == 'source'
    else:
        assert result.get('cron_report_failed'), result
    assert bool('cron.delivery.sent' in events)==(transport_ok and repair_ok)
    if not repair_ok:
        assert result['cron_delivery_status'] == 'silent'
        assert not sent and not wire
        assert client.repairs == 2


@pytest.mark.parametrize('instructions', ['Check the saved sources and report matching upcoming events.', '保存された情報源を確認して今後の該当公演を報告。', 'Comprueba las fuentes guardadas e informa eventos próximos coincidentes.'])
def test_scheduled_run_continues_after_failed_search_and_browser_discovery(tmp_path, monkeypatch, instructions):
    from types import SimpleNamespace
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn, cron_structured_result_block_reason
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    registry=ToolRegistry()
    calls=[]
    responses={
        'web_search': ('failed', {'reason':'source_evidence_unavailable'}),
        'browser_extract_text': ('completed', {'text':'Upcoming listings\nCopyright 2026. This page has no detailed event records.'}),
        'web_fetch': ('completed', {'text':'公演 at BASEMENT\nNovember 7, 22:30\nhttps://example.com/tickets'}),
    }
    for name,(status,output) in responses.items():
        def handler(invocation, status=status, output=output):
            calls.append(invocation.tool_name)
            return ToolResult(invocation.invocation_id,invocation.tool_name,status,output)
        registry.register(ToolSpec(name=name,description='External public source',risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=2),handler)
    report='**公演 at BASEMENT**\nNovember 7, 22:30\nVerified upcoming event. [Tickets](https://example.com/tickets)'
    class Client:
        step=0
        def create(self,**kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            if not kwargs.get('tools'):
                completion = _monitor_completion_response(kwargs)
                if completion is not None:
                    return completion
                return text_response(json.dumps({'decision':'send','reason':'verified_match'}))
            self.step+=1
            if self.step <= 3:
                name=['web_search','browser_extract_text','web_fetch'][self.step-1]
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.step),'name':name,'input':{'query':'公演','url':'https://example.com/tickets'}}]}
            return text_response(report)
    from datetime import UTC, datetime
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='source-fixture',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='*',created_by='fixture-user',created_at=datetime.now(UTC)))
    client=Client()
    job=SimpleNamespace(id='discovery',name='Monitor',task=instructions,artifact_delivery_options={'alert_delivery_mode':'verified_matches_only'})
    result=run_single_agent_cron_turn(job,'telegram:123',label='Scheduled task',orchestrator=AgentOrchestrator(model_client=client),
        runtime=runtime,tool_registry=registry,settings=SimpleNamespace(enabled_skill_packs=()),model_client=client)
    assert calls==['web_search','browser_extract_text','web_fetch'],result
    assert result['text']==report and not result.get('cron_report_failed'),result
    assert cron_structured_result_block_reason(result,text=report,artifacts=None) is None
    assert client.step==4


@pytest.mark.parametrize('task', ['Read relevant messages, check calendar duplicates, and report obligations with uncertainty.', '関連メールを読み、カレンダーの重複を確認し、義務と不確実性を報告。'])
@pytest.mark.parametrize('complete_report', [True, False])
def test_email_triage_recovers_mixed_source_refs_and_delivers_report(tmp_path, monkeypatch, task, complete_report):
    from types import SimpleNamespace
    import asyncio
    from datetime import UTC, datetime
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import CronRunDeliveryCallbacks, run_cron_delivery_workflow, run_single_agent_cron_turn, cron_structured_result_block_reason, record_cron_delivery_chat_turn
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.telegram_app import _deliver_reply
    runtime,_=runtime_and_registry(tmp_path,monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-account',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='*',created_by='fixture-user',created_at=datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-account-access',principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.ACCOUNT_ACCESS,mode=PolicyMode.ALLOW,selector='*',created_by='fixture-user',created_at=datetime.now(UTC)))
    registry=ToolRegistry(); invoked=[]; wire=[]; repairs=[]
    responses={
        'email_search':('failed',{'reason':'handler_timeout','timeout_seconds':45}),
        'email_read':('completed',{'id':'message-1','message':{'id':'message-1','subject':'Renewal notice',
            'from':'billing@example.com','date':'2026-10-10','body':'The membership renews on November 7, 2026 for $35. Other account instructions are unchanged. Please review your membership preferences on the authenticated account page before the renewal date.'}}),
        'calendar_list':('completed',{'results':[{'id':'event-1','summary':'Membership renewal','start':{'date':'2026-11-07'}}],'result_count':1}),
    }
    for name,(status,output) in responses.items():
        def handler(invocation,status=status,output=output):
            invoked.append(invocation.tool_name)
            return ToolResult(invocation.invocation_id,invocation.tool_name,status,output)
        registry.register(ToolSpec(name=name,description='External account provider',risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=2),handler)
    report='**Email deadline triage**\n\n• Membership renewal: November 7, 2026, $35. Already on calendar; no duplicate created.\n\n**Coverage:** One email search timed out. Findings above were checked, but the scan is incomplete.'
    class Client:
        step=0
        def create(self,**kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            if not kwargs.get('tools'):
                prompt=kwargs['messages'][0]['content'][0]['text']
                evidence=json.loads(prompt.split('Verified compact tool evidence from this run:\n')[1].split('\n\n')[0])
                assert {'email_search','email_read','calendar_list'} <= {r.get('tool_name') for r in evidence},evidence
                refs=[r['evidence_id'] for r in evidence]
                repairs.append(refs)
                if not complete_report:
                    return text_response('{"requirements_satisfied":')
                return text_response(json.dumps({'requirements_satisfied':complete_report,'source_state':'available','evidence_refs':refs,'report':report}))
            self.step+=1
            if self.step <= 3:
                name=['email_search','email_read','calendar_list'][self.step-1]
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.step),'name':name,
                    'input':{'query':'recent dated obligations','id':'message-1','start':'2026-11-06','end':'2026-11-08'}}]}
            raise TimeoutError('External model interruption after completed account reads')
    client=Client(); job=SimpleNamespace(id='email-triage',name='Email deadline triage',task=task,artifact_delivery_options={})
    class Message:
        chat=SimpleNamespace(id=123);message_id=702
        async def reply_text(self,text,**kwargs):wire.append((text,kwargs))
        async def reply_document(self,*a,**k):raise AssertionError('No raw account artifact should be uploaded')
    def run(job,conv,**kwargs):
        return run_single_agent_cron_turn(job,conv,label='Scheduled task',orchestrator=AgentOrchestrator(model_client=client),
            runtime=runtime,tool_registry=registry,settings=SimpleNamespace(enabled_skill_packs=()),model_client=client)
    def send(job,channel,target,text):
        asyncio.run(_deliver_reply(Message(),text,principal_id='telegram:123'));return True
    callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:'telegram',delivery_target=lambda j,c:'123',run_agent_turn=run,
        record_event=lambda *a,**k:None,block_reason=lambda r,t,a:cron_structured_result_block_reason(r,text=t,artifacts=a),
        save_web_delivery=lambda *a:False,send_platform_delivery=send,
        record_chat_turn=lambda j,c,ch,target,t:record_cron_delivery_chat_turn(runtime.store,j,conversation_id=c,
            delivery_channel=ch,delivery_target=target,delivered_text=t))
    outcome=run_cron_delivery_workflow(job,label='Scheduled task',callbacks=callbacks)
    assert invoked==['email_search','email_read','calendar_list'],[(r.tool_name,r.output) for r in outcome['tool_results']]
    assert outcome['cron_delivery_status']=='sent',outcome
    assert outcome['response_fulfilled'] is complete_report,outcome
    assert bool(outcome.get('cron_report_partial')) is not complete_report,outcome
    assert len(repairs)==(1 if complete_report else 2)
    if not complete_report:
        assert outcome['response_fulfilled'] is False
        assert all('Partial report' not in text and 'November 7' not in text for text,_ in wire)
        assert not outcome.get('cron_completion_continuations')
    assert any('November 7' in text and 'scan is incomplete' in text and options.get('parse_mode')=='HTML' for text,options in wire) is complete_report
    assert all('Please review your membership preferences' not in text for text,_ in wire)
    assert runtime.store.list_recent_conversation_events('telegram:123')[-1]['cron_id']=='email-triage'


@pytest.mark.parametrize('instructions', ['Include current metrics and their daily history.', '現在の数値と日別履歴を含めてください。'])
def test_cron_html_recovery_keeps_late_read_data_after_many_page_results(tmp_path,monkeypatch,instructions):
    from types import SimpleNamespace
    from nullion import crons
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn,cron_delivery_artifact_paths_from_result
    runtime,registry=runtime_and_registry(tmp_path,monkeypatch)
    current={'visits':29,'audience':7,'daily':[{'date':f'2026-01-{i:02d}','count':i} for i in range(1,15)]}
    source=tmp_path/'current.json';source.write_text(json.dumps(current))
    registry.register(ToolSpec('browser_extract_text','Read a public page',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),
        lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',{'text':'Public source '+str(inv.arguments['index'])+': '+('finding '*220)}))
    job=crons.add_cron('Dataset report','0 12 * * *',instructions,artifact_delivery_options={
        'required_artifact_extensions':['.html'],'requires_attachment_delivery':True,'deliver_required_extensions_only':True})
    class Model:
        calls=0
        packet=None
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            if not kw.get('tools'):
                self.packet=json.loads(kw['messages'][0]['content'][0]['text'])
                evidence=self.packet['verified_tool_evidence']
                data=[r['source_data'] for r in evidence if 'source_data' in r]
                assert current in data, evidence
                return text_response('<!doctype html><html><body><h1>Dataset</h1><p>Visits 29; audience 7.</p><table>'+''.join(f'<tr><td>{r["date"]}</td><td>{r["count"]}</td></tr>' for r in current['daily'])+'</table></body></html>')
            self.calls+=1
            if self.calls==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(i),'name':'browser_extract_text','input':{'index':i}} for i in range(24)]}
            if self.calls==2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'data','name':'file_read','input':{'path':str(source)}}]}
            raise TimeoutError('External answer generation interrupted')
    model=Model()
    outcome=run_single_agent_cron_turn(job,'web:dataset-report',label='Scheduled task',orchestrator=AgentOrchestrator(model_client=model),
        runtime=runtime,tool_registry=registry,settings=SimpleNamespace(enabled_skill_packs=()),model_client=model)
    assert model.packet and outcome['artifact_delivery_satisfied'],outcome
    paths=cron_delivery_artifact_paths_from_result(outcome)
    assert len(paths)==1
    document=Path(paths[0]).read_text()
    assert 'Visits 29; audience 7' in document and '<td>14</td>' in document
    assert 'unavailable' not in document and 'finding finding' not in document


def test_file_write_invalid_bracketed_source_is_validation_not_handler_exception(tmp_path,monkeypatch):
    from nullion.tools import ToolInvocation
    runtime,registry=runtime_and_registry(tmp_path,monkeypatch)
    malformed=registry.invoke(ToolInvocation('invalid-reference','file_write','web:author',{
        'path':str(tmp_path/'report.html'),'content':'<!doctype html><html><body><a href="https://[missing-host/">Source</a></body></html>'}))
    assert malformed.status=='failed' and malformed.output['reason']=='placeholder_source_urls'
    assert 'Invalid IPv6 URL' not in str(malformed.error)
    valid=registry.invoke(ToolInvocation('valid-reference','file_write','web:author',{
        'path':str(tmp_path/'report.html'),'content':'<!doctype html><html><body><h1>観測</h1><p>29 visits</p><a href="https://source.test/record">Source</a></body></html>'}))
    assert valid.status=='completed',valid
    assert '29 visits' in Path(valid.output['path']).read_text()


def test_generated_source_code_does_not_apply_rendered_reference_validation(tmp_path,monkeypatch):
    from nullion.tools import ToolInvocation
    runtime,registry=runtime_and_registry(tmp_path,monkeypatch)
    code='endpoint_template = "https://[host]/record"\nprint(endpoint_template)\n'
    created=registry.invoke(ToolInvocation('source-template','file_write','web:author',{
        'path':str(tmp_path/'build_document.py'),'content':code}))
    assert created.status=='completed',created
    assert Path(created.output['path']).read_text()==code




def test_long_account_report_recovery_keeps_calendar_checks_and_delivers_once(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.crons import CronJob
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    from datetime import UTC, datetime
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-' + kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=kind, mode=PolicyMode.ALLOW, selector='*',
            created_by='fixture-user', created_at=datetime.now(UTC)))
    registry = ToolRegistry()
    reads = []
    def read(inv):
        reads.append(inv.tool_name)
        if inv.tool_name == 'calendar_list':
            output = {'start': '2026-11-06', 'end': '2026-11-08', 'result_count': 1,
                'results': [{'id': 'existing', 'summary': 'Renewal', 'start': {'date': '2026-11-07'}}]}
        else:
            output = {'message': {'id': inv.arguments['id'], 'subject': 'Renewal ' + inv.arguments['id'],
                'date': '2026-10-26', 'body': 'Renewal is November 7. ' + ('Additional context. ' * 300)}}
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', output)
    for name in ('email_read', 'calendar_list'):
        registry.register(ToolSpec(name, 'Read selected account records', ToolRiskLevel.LOW,
            ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        step = 0
        repairs = 0
        def create(self, **kw):
            reply = workflow_provider_reply(kw, completion=True)
            if reply is not None:
                return reply
            if not kw.get('tools'):
                prompt = kw['messages'][0]['content'][0]['text']
                evidence = json.loads(prompt.split('Verified compact tool evidence from this run:\n')[1].split('\n\n')[0])
                calendar = next(row for row in evidence if row.get('tool_name') == 'calendar_list')
                assert calendar['source_scope']['start'] == '2026-11-06'
                assert calendar['items'][0]['source_record_id'] == 'existing'
                assert calendar['items'][0]['start']['date'] == '2026-11-07'
                assert len([row for row in evidence if row.get('tool_name') == 'email_read']) == 24
                self.repairs += 1
                return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [row['evidence_id'] for row in evidence],
                    'report': 'Checked 24 renewal messages dated October 26. Renewal is November 7; an existing calendar event was verified. No duplicate created.'}))
            self.step += 1
            if self.step == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': str(i),
                    'name': 'email_read', 'input': {'id': str(i)}} for i in range(24)]}
            if self.step == 2:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'calendar',
                    'name': 'calendar_list', 'input': {'start': '2026-11-06T00:00:00-05:00', 'end': '2026-11-08T00:00:00-05:00'}}]}
            return text_response('I checked your email.\nFrom: billing@example.test\nPreview: Renewal notice.\nDate: October 26.')
    provider = Provider()
    result = run_single_agent_cron_turn(CronJob(id='long-report', name='更新', schedule='0 * * * *',
        task='Read selected renewal records, verify existing calendar events, and report without duplicates.'),
        'web:long-report', label='Scheduled task', orchestrator=AgentOrchestrator(model_client=provider),
        runtime=runtime, tool_registry=registry, settings=SimpleNamespace(), model_client=provider)
    assert result['response_fulfilled'] is True and not result.get('cron_report_failed'), result
    assert '24 renewal messages' in result['text'] and 'existing calendar event' in result['text']
    assert provider.repairs == 1 and reads == ['email_read'] * 24 + ['calendar_list']


def test_real_chat_email_pagination_reads_next_page_before_answer(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from fastapi.testclient import TestClient
    from nullion import web_app, tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from datetime import UTC, datetime
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-' + kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=kind, mode=PolicyMode.ALLOW, selector='*',
            created_by='fixture-user', created_at=datetime.now(UTC)))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setattr(tools, '_connector_connection_for_invocation', lambda *_:
        SimpleNamespace(provider_profile='https://example.test', permission_mode='read'))
    monkeypatch.setattr(tools, '_email_messages_endpoint_for_provider', lambda *_: 'https://example.test/messages')
    pages = []
    def external(inv, *, provider_id, url, params=None, **kw):
        if url.endswith('/messages'):
            pages.append(params)
            return {'messages': [{'id': 'second' if params.get('pageToken') else 'first'}],
                **({} if params.get('pageToken') else {'nextPageToken': 'next-page'}), 'resultSizeEstimate': 2}
        identity = url.rsplit('/', 1)[-1]
        return {'id': identity, 'payload': {'headers': [{'name': 'Subject',
            'value': '最初の通知' if identity == 'first' else '次の通知'}]}}
    monkeypatch.setattr(tools, '_connector_json_request', external)
    registry = tools.ToolRegistry()
    registry.register(tools.ToolSpec('email_search', 'Search the selected email source', tools.ToolRiskLevel.LOW,
        tools.ToolSideEffectClass.READ, False, 2, input_schema=tools._email_search_input_schema(supports_pagination=True)),
        tools._build_connector_email_search_handler())
    class Provider:
        step = 0
        def create(self, **kw):
            if 'semantic completion reviewer' in str(kw.get('system', '')):
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                coverage = packet['source_pagination']
                assert len(coverage) == 1 and coverage[0]['complete'] is True
                assert coverage[0]['linked_pages'] == coverage[0]['linked_result_count'] == 2
            auxiliary = workflow_provider_reply(kw, completion=True)
            if auxiliary is not None:
                return auxiliary
            if 'email_search' not in {row['name'] for row in kw.get('tools', [])}:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'scope',
                    'name': 'request_tool_scope', 'input': {'capabilities': ['connector'],
                        'tool_names': ['email_search'], 'required_tool_names': ['email_search'],
                        'source_user_requested': True, 'source_evidence': '期限に関するメールの件名をすべて列挙して。'}}]}
            self.step += 1
            if self.step <= 2:
                arguments={'query':'期限','provider_id':'selected','limit':1}
                if self.step==2:
                    continuation=None
                    for message in kw['messages']:
                        for block in message.get('content',[]) if isinstance(message.get('content'),list) else []:
                            if block.get('type')!='tool_result':continue
                            raw=block.get('content',[])
                            raw=raw if isinstance(raw,str) else ''.join(item.get('text','') for item in raw)
                            try:payload=json.loads(raw)
                            except (ValueError,TypeError):continue
                            output=payload.get('output',{})
                            if isinstance(output,dict) and output.get('next_page_request'):
                                continuation=output['next_page_request']
                    assert continuation and continuation['tool_name']=='email_search'
                    arguments=continuation['arguments']
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': str(self.step),
                    'name': 'email_search', 'input': arguments}]}
            assert len(pages) == 2
            return text_response('Matching subjects: 最初の通知; 次の通知.')
    provider = Provider(); runtime.model_client = provider
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=provider), registry=registry)
    with TestClient(app) as client:
        response = client.post('/api/chat', json={'conversation_id': 'web:page-coverage', 'text': '期限に関するメールの件名をすべて列挙して。', 'auto_approve': True})
        assert response.status_code == 200, response.text
        assert all(subject in response.json()['text'] for subject in ['最初の通知', '次の通知']), response.text
    assert pages == [{'q': '期限', 'maxResults': 1}, {'q': '期限', 'maxResults': 1, 'pageToken': 'next-page'}]
    turns = runtime.store.list_recent_conversation_events('web:page-coverage')
    turn = next(row for row in reversed(turns) if row['event_type'] == 'conversation.chat_turn')
    searches = [row for row in turn['tool_results'] if row['tool_name'] == 'email_search']
    assert len(searches) == 2 and searches[0]['output']['has_more'] is True
    assert searches[1]['output']['has_more'] is False and searches[1]['output']['results'][0]['id'] == 'second'




def test_recovery_keeps_planned_conditional_action_and_normal_approval(tmp_path, monkeypatch):
    import sqlite3
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.approvals import approve, create_permission_grant
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    registry = ToolRegistry(); writes = []
    registry.register(ToolSpec('observe', 'Read selected state', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2),
        lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'needs_action': True, 'value': 7}))
    def save(inv):
        writes.append(inv.arguments['value'])
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7,
            'action_receipt': {'type': 'action_receipt', 'action': 'saved', 'object_type': 'record',
                'object_id': 'selected', 'summary': 'Saved selected record.'}})
    registry.register(ToolSpec('conditional_save', 'Save after condition and approval', ToolRiskLevel.HIGH,
        ToolSideEffectClass.WRITE, True, 2), save)
    class Provider:
        step = 0
        reviews = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            if 'material-requirements planner' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                assert 'conditional_save' in {r['name'] for r in packet['current_turn_tool_catalog']}
            if 'semantic completion reviewer' in system:
                self.reviews += 1
                if self.reviews == 1:
                    return text_response(json.dumps({'disposition': 'retry', 'unresolved_requirements': ['The selected conditional action is not committed.'],
                        'retry_tool_names': ['observe']}))
            auxiliary = workflow_provider_reply(kw, completion=True, action=True)
            if auxiliary is not None: return auxiliary
            self.step += 1
            if self.step == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'read', 'name': 'observe', 'input': {}}]}
            if self.step == 2:
                assert 'conditional_save' in {r['name'] for r in kw['tools']}
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'save', 'name': 'conditional_save', 'input': {'value': 7}}]}
            return text_response('Saved the selected value: 7.')
    model = Provider(); runner = AgentOrchestrator(model_client=model)
    kwargs = dict(conversation_id='web:conditional', principal_id='web:conditional', user_message='状態を確認し、必要なら選択した項目を保存。',
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=6,
        tool_flow_context={'planned_action_tool_names': ['conditional_save']})
    first = runner.run_turn(**kwargs, conversation_history=[])
    assert first.suspended_for_approval and not writes, first
    request = runtime.store.get_approval_request(first.approval_id)
    runtime.store.add_approval_request(approve(request, decided_by='fixture'))
    runtime.store.add_permission_grant(create_permission_grant(approval_id=first.approval_id, principal_id='web:conditional',
        permission='tool:conditional_save', granted_by='fixture'))
    final = runner.resume_turn(**kwargs, messages_snapshot=runtime.store.get_suspended_turn(first.approval_id).messages_snapshot, previous_result=first)
    assert writes == [7] and final.response_fulfilled is True, final


@pytest.mark.parametrize('prompt', ['Summarize the selected records and their existing matching event.', '選択された記録と既存の一致する予定をまとめて。'])
def test_batch_report_review_receives_hydrated_records_and_late_check(tmp_path, monkeypatch, prompt):
    from types import SimpleNamespace
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.crons import CronJob
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, _ = runtime_and_registry(tmp_path, monkeypatch)
    registry = ToolRegistry(); reads = []; reviews = []
    def read(inv):
        index = inv.arguments['index']; reads.append(index)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'id':str(index), 'summary':'Read complete',
            'message':{'id':str(index), 'subject':'記録'+str(index),
                'body':'Due 2026-11-14, amount '+str(40+index)+'. '+('Source context. '*30)},
            'evidence_scope':{'message_sent_at':'2026-10-26T10:00:00Z'}})
    registry.register(ToolSpec('record_read', 'Read one selected record', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, input_schema={'type':'object','properties':{'index':{'type':'integer'}},'required':['index']}),read)
    registry.register(ToolSpec('existing_check', 'Check existing matching event', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2),lambda inv:ToolResult(inv.invocation_id, inv.tool_name,'completed',{
            'start':'2026-11-14T00:00:00Z','end':'2026-11-15T00:00:00Z','result_count':1,
            'results':[{'id':'existing','summary':'Matching obligation','start':{'dateTime':'2026-11-14T09:00:00Z'}}]}))
    class Provider:
        calls = 0
        def create(self, **kw):
            if 'semantic completion reviewer' in kw.get('system',''):
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                records = [row for row in packet['tool_evidence'] if row['tool_name']=='record_read']
                from nullion.agent_orchestrator import _completion_review_request_timeout
                budget = _completion_review_request_timeout(kw['max_tokens'],
                    request_chars=len(json.dumps(packet,ensure_ascii=False)))
                # The common deadline includes local request preparation;
                # a provider receives the remaining window, not a fresh one.
                assert budget - 1 <= kw['timeout'] <= budget
                assert len(records)==24
                assert {row['output']['message']['id'] for row in records} == {str(i) for i in range(24)}
                assert all('Due 2026-11-14' in row['output']['message']['body'] for row in records)
                check = next(row for row in packet['tool_evidence'] if row['tool_name']=='existing_check')
                assert check['output']['results'][0]['start']['dateTime']=='2026-11-14T09:00:00Z'
                reviews.append(packet)
            auxiliary = workflow_provider_reply(kw)
            if auxiliary is not None:return auxiliary
            self.calls += 1
            if self.calls <= 24:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.calls),
                    'name':'record_read','input':{'index':self.calls-1}}]}
            if self.calls == 25:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'check','name':'existing_check','input':{}}]}
            return text_response('Matching obligation: an existing event on 2026-11-14 at 09:00 UTC. 24 records checked, amounts 40 through 63. No duplicate was created.')
    monkeypatch.setenv('NULLION_CRON_AGENT_MAX_ITERATIONS','32')
    provider=Provider()
    result=run_single_agent_cron_turn(CronJob(id='batch',name='記録',schedule='0 * * * *',task=prompt),
        'web:batch',label='Scheduled run',orchestrator=AgentOrchestrator(model_client=provider),
        runtime=runtime,tool_registry=registry,settings=SimpleNamespace(),model_client=provider)
    assert not result.get('cron_report_failed') and result['response_fulfilled'] is True, json.dumps({
        key:value for key,value in result.items() if key not in {'tool_results', 'messages_snapshot'}}, default=str)
    assert len(reads)==24 and len(reviews)==0 and provider.calls==26
    from nullion.runtime_persistence import load_runtime_store
    persisted=load_runtime_store(runtime.checkpoint_path).list_conversation_events('web:batch')
    assert any(row['event_type']=='conversation.cron_run' and row['response_fulfilled'] for row in persisted)
