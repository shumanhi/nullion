from __future__ import annotations

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
            return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', output)
        registry.register(ToolSpec(name=name, description=name, risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=2), handler)
    class Model:
        calls=0
        def create(self, **kwargs):
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


class DeleteCandidateClient:
    def __init__(self, target_id, request, authorized):
        self.target_id, self.request, self.authorized = target_id, request, authorized
        self.calls = []

    def create(self, **kwargs):
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
    assert "Report attached" in rendered_delivery
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
        def create(self,**kwargs):
            if not kwargs.get('tools'):
                completion = _monitor_completion_response(kwargs)
                if completion is not None:
                    return completion
                system=kwargs.get('system','')
                if 'Classify delivery' in system:
                    return text_response(json.dumps({'decision':'send','reason':'verified_match'}))
                prompt=kwargs['messages'][0]['content'][0]['text']
                assert 'November 7' in prompt and 'https://example.com/tickets' in prompt
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
            if not kwargs.get('tools'):
                prompt=kwargs['messages'][0]['content'][0]['text']
                evidence=json.loads(prompt.split('Verified compact tool evidence from this run:\n')[1].split('\n\n')[0])
                assert {'email_search','email_read','calendar_list'} <= {r.get('tool_name') for r in evidence},evidence
                refs=[r['evidence_id'] for r in evidence]
                repairs.append(refs)
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
    assert bool(outcome.get('cron_report_failed')) is not complete_report,outcome
    assert not outcome.get('cron_report_partial'),outcome
    assert len(repairs)==(1 if complete_report else 4)
    if not complete_report:
        assert outcome['response_fulfilled'] is False
        assert all('Partial report' not in text and 'November 7' not in text for text,_ in wire)
        assert outcome['cron_completion_continuations']==1
    assert any('November 7' in text and 'scan is incomplete' in text and options.get('parse_mode')=='HTML' for text,options in wire) is complete_report
    assert all('Please review your membership preferences' not in text for text,_ in wire)
    assert runtime.store.list_recent_conversation_events('telegram:123')[-1]['cron_id']=='email-triage'
