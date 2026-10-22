"""Real runner, SQLite, tool policy and platform formatter; only provider IO is fake."""
from types import SimpleNamespace
import asyncio
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


def response(value):
    return {'stop_reason':'end_turn', 'content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}


def setup(tmp_path, monkeypatch, approval=False, existing=False):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion import crons
    checkpoint = tmp_path/'runtime.db'
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', checkpoint)
    runtime = bootstrap_persistent_runtime(checkpoint)
    registry = ToolRegistry()
    called = []
    for name, effect, output in [
        ('records_scan',ToolSideEffectClass.READ, {'results':[{'id':'record-a','subject':'期限','due_date':'2026-11-07'}]}),
        ('schedule_scan',ToolSideEffectClass.READ, {'events':[{'id':'existing-event','due_date':'2026-11-07'}] if existing else []}),
        ('schedule_add',ToolSideEffectClass.ACCOUNT_WRITE, {'action_receipt':{'status':'created','id':'event-a'}}),
    ]:
        def handler(invocation, output=output):
            called.append(invocation.tool_name)
            return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',output)
        registry.register(ToolSpec(name=name,description=name,capability_tags=('connector','records'),
            risk_level=ToolRiskLevel.HIGH if name=='schedule_add' else ToolRiskLevel.LOW,side_effect_class=effect,requires_approval=approval and name=='schedule_add',
            timeout_seconds=2),handler)
    monkeypatch.setattr('nullion.cron_execution_tools._connected_connector_provider_summaries',lambda _:(
        {'provider_id':'skill_pack_connector_records','permission_mode':'write'},))
    from nullion.tools import ToolExecutor
    from nullion.policy import SentinelPolicy
    class Guarded:
        def __getattr__(self, name): return getattr(registry, name)
        def invoke(self, invocation):
            return ToolExecutor(store=runtime.store, registry=registry, sentinel_policy=SentinelPolicy()).invoke(invocation)
    return runtime, Guarded(), called


@pytest.mark.parametrize('saved_request', ['Read records and create missing events.', '記録を確認して、未登録の予定を作成。'])
@pytest.mark.parametrize('approval,existing', [(False,False),(True,False),(False,True)])
def test_requested_conditional_account_action_finishes_or_pauses_at_real_approval(tmp_path,monkeypatch,saved_request,approval,existing):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.crons import CronJob
    runtime, registry, called = setup(tmp_path,monkeypatch,approval,existing)
    class Model:
        step = 0
        def create(self, **kw):
            if not kw.get('tools'):
                if 'available_structured_write_tools' in kw['messages'][0]['content'][0]['text']:
                    return response({'requires_account_connector':True,'provider_ids':['skill_pack_connector_records'],
                        'confidence':1,'write_tool_names':['schedule_add'],'delivery_mode':'report_each_run'})
                return response({'disposition':'complete','reason':'Completed requested checks and action.',
                                 'requirements_satisfied':True,'source_state':'available','evidence_refs':['e0'],
                                 'report':'11月7日の予定は登録済みです。重複は作成していません。' if existing else '11月7日の予定を作成しました。'})
            assert 'schedule_add' in {tool['name'] for tool in kw['tools']}
            self.step += 1
            if self.step <= (2 if existing else 3):
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.step),
                    'name':['records_scan','schedule_scan','schedule_add'][self.step-1],'input':{}}]}
            return response('11月7日の予定は登録済みのため、重複を作成しませんでした。' if existing else '期限の記録を確認し、重複がないため11月7日の予定を作成しました。')
    model=Model()
    from contextlib import contextmanager
    from nullion.runtime_persistence import load_runtime_store
    @contextmanager
    def dashboard_refresh_after_turn():
        yield
        # Exercise the real SQLite reload that can race the end of a cron turn.
        runtime.store = load_runtime_store(runtime.checkpoint_path)
    result=run_single_agent_cron_turn(CronJob(id='conditional-action',name='確認',task=saved_request,schedule='0 * * * *'),
        'web:fixture',label='Scheduled task',orchestrator=AgentOrchestrator(model_client=model),runtime=runtime,
        settings=SimpleNamespace(),tool_registry=registry,model_client=model,
        turn_guard=dashboard_refresh_after_turn if approval else None)
    assert result['suspended_for_approval'] is approval, result
    assert called == (['records_scan','schedule_scan'] if approval or existing else ['records_scan','schedule_scan','schedule_add'])
    if approval:
        assert runtime.store.get_approval_request(result['approval_id']) is not None
        assert load_runtime_store(runtime.checkpoint_path).get_approval_request(result['approval_id']) is not None
    else:
        assert '11月7日' in result['text'] and not result.get('cron_report_failed'), result


@pytest.mark.parametrize('decision,reason', [('silent','no_verified_match'),('silent','insufficient_evidence'),('send','verified_match')])
def test_legacy_conditional_monitor_uses_typed_policy_at_delivery_boundary(tmp_path,monkeypatch,decision,reason):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import (run_single_agent_cron_turn,run_cron_delivery_workflow,
        CronRunDeliveryCallbacks,cron_structured_result_block_reason)
    from nullion.crons import CronJob
    from nullion.telegram_app import _deliver_reply
    runtime, registry, called=setup(tmp_path,monkeypatch)
    wire=[]; preflight=[]
    class Model:
        step=0
        def create(self,**kw):
            if not kw.get('tools'):
                if 'available_structured_write_tools' in kw['messages'][0]['content'][0]['text']:
                    return response({'requires_account_connector':True,'provider_ids':['skill_pack_connector_records'],
                        'confidence':1,'write_tool_names':[],'delivery_mode':'verified_matches_only'})
                return response({'decision':decision,'reason':reason})
            self.step+=1
            if self.step==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':'records_scan','input':{}}]}
            return response('確認結果: 期限11月7日。')
    model=Model(); job=CronJob(id='legacy-monitor',name='Avisos',task='新しい未確認の期限だけ通知。',schedule='0 * * * *')
    class Message:
        chat=SimpleNamespace(id=123);message_id=11
        async def reply_text(self,text,**kw):wire.append((text,kw))
        async def reply_document(self,*a,**kw):raise AssertionError('No attachment expected')
    def run(job,conversation_id,**kw):
        return run_single_agent_cron_turn(job,conversation_id,label='Scheduled task',orchestrator=AgentOrchestrator(model_client=model),
            runtime=runtime,settings=SimpleNamespace(),tool_registry=registry,model_client=model,
            record_event=lambda *a,**kw:preflight.append(kw))
    def send(job,channel,target,text):
        asyncio.run(_deliver_reply(Message(),text,principal_id='telegram:123'));return True
    result=run_cron_delivery_workflow(job,label='Scheduled task',callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda j:'telegram',delivery_target=lambda j,c:'123',run_agent_turn=run,
        record_event=lambda *a,**k:None,block_reason=lambda r,t,a:cron_structured_result_block_reason(r,text=t,artifacts=a),
        save_web_delivery=lambda *a:False,send_platform_delivery=send,record_chat_turn=lambda *a:None,
        health_store_path=runtime.checkpoint_path))
    from nullion.tracker_status import tracker_statuses
    facts=tracker_statuses(runtime.checkpoint_path,[job])[job.id]
    assert facts['is_tracker'] is True,facts
    assert facts['last_outcome']==({'verified_match':'match','no_verified_match':'no_match','insufficient_evidence':'incomplete'}[reason]),facts
    assert facts['consecutive_no_matches']==(1 if reason=='no_verified_match' else 0),facts
    assert called==['records_scan']
    assert result['cron_delivery_status']==('sent' if decision=='send' else 'silent'),result
    assert bool(wire)==(decision=='send'),result
    assert bool(result.get('cron_report_failed'))==(reason=='insufficient_evidence')
    assert any(item.get('alert_delivery_mode')=='verified_matches_only' for item in preflight)
    if wire:
        assert '11月7日' in wire[0][0] and wire[0][1]['parse_mode']=='HTML'


def test_repeated_scheduled_runs_cache_scope_and_keep_plain_chat_fast(tmp_path,monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.crons import CronJob
    runtime,registry,_=setup(tmp_path,monkeypatch)
    planning=[]
    class Model:
        step=0
        def create(self,**kw):
            if not kw.get('tools'):
                planning.append(1)
                time.sleep(1.5)  # Simulate the previously slow planner/provider boundary.
                return response({'requires_account_connector':True,'provider_ids':['skill_pack_connector_records'],
                    'confidence':1,'write_tool_names':[],'delivery_mode':'report_each_run'})
            self.step+=1
            if self.step==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':'records_scan','input':{}}]}
            return response('確認した記録の期限は11月7日です。')
    job=CronJob(id='cached',name='記録',task='記録を確認して毎回報告。',schedule='0 * * * *')
    def run():
        model=Model()
        return run_single_agent_cron_turn(job,'web:cache',label='Scheduled task',
            orchestrator=AgentOrchestrator(model_client=model),runtime=runtime,settings=SimpleNamespace(),
            tool_registry=registry,model_client=model)
    assert not run().get('cron_report_failed')
    elapsed=[]
    for _ in range(4):
        start=time.perf_counter(); result=run(); elapsed.append(time.perf_counter()-start)
        assert '11月7日' in result['text'],result
    assert len(planning)==1
    assert sum(elapsed)/len(elapsed)<0.8 and max(elapsed)<1.2,elapsed
    class PlainModel:
        def create(self,**kw):
            assert kw.get('tools')
            return response('こんにちは。')
    plain=[]
    for request in ['こんにちは','Hola','مرحبا']:
        start=time.perf_counter()
        answer=AgentOrchestrator(model_client=PlainModel()).run_turn(conversation_id='web:plain',principal_id='web:plain',
            user_message=request,conversation_history=[],tool_registry=registry,policy_store=runtime.store,
            approval_store=runtime.store,max_iterations=2)
        plain.append(time.perf_counter()-start)
        assert answer.final_text=='こんにちは。'
    assert sum(plain)/len(plain)<0.8 and max(plain)<1.2,plain
    assert len(planning)==1


@pytest.mark.parametrize('label', ['Deferred digest','期限の確認','Informe aplazado'])
def test_named_paused_task_lookup_runs_verified_id_through_real_runner(tmp_path,monkeypatch,label):
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass,register_cron_tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import run_single_agent_cron_turn
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence
    monkeypatch.setattr(crons,'_RUNTIME_DB_PATH',tmp_path/'runtime.db')
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    job=crons.add_cron(label,'0 * * * *','現在の件数を確認して報告。')
    crons.toggle_cron(job.id,False)
    invoked=[]
    class WorkerModel:
        step=0
        def create(self,**kw):
            self.step+=1
            if self.step==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'count','name':'records_count','input':{}}]}
            return response('Verified count: 7. The current records were checked.')
    source=ToolRegistry()
    def read(invocation):
        invoked.append(invocation.tool_name)
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'count':7})
    source.register(ToolSpec(name='records_count',description='Read current record count',risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,requires_approval=False,timeout_seconds=2),read)
    started=[]
    def runner(selected,**kw):
        started.append(selected.id)
        model=WorkerModel()
        return run_single_agent_cron_turn(selected,'web:worker',label='Scheduled task',
            orchestrator=AgentOrchestrator(model_client=model),runtime=runtime,settings=SimpleNamespace(),
            tool_registry=source,model_client=model)
    registry=ToolRegistry();register_cron_tools(registry,cron_runner=runner)
    class ChatModel:
        calls=0
        def create(self,**kw):
            self.calls+=1
            if self.calls==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                    'input':{'capabilities':['scheduler_run'],'tool_names':['list_crons','run_cron'],
                        'required_tool_names':['run_cron'],'scheduler_action':'run','scheduler_selection_policy':'user_selected'}}]}
            assert self.calls==2,'Verified lookup should not need another selection/model round'
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'list','name':'list_crons','input':{'query':label}}]}
    client=ChatModel()
    result=AgentOrchestrator(model_client=client).run_turn(conversation_id='web:named',principal_id='web:named',
        user_message='ابدأ المهمة التي اخترتها؛ اترك الجدول متوقفاً.',conversation_history=[],
        tool_registry=ScopedTurnToolRegistry(registry,evidence=TurnToolEvidence()),
        policy_store=runtime.store,approval_store=runtime.store,max_iterations=6)
    assert started==[job.id] and invoked==['records_count'],result
    assert client.calls==2 and '7' in result.final_text,result
    assert crons.get_cron(job.id).enabled is False
    assert [item.tool_name for item in result.tool_results]==['request_tool_scope','list_crons','run_cron']
    assert result.tool_results[-1].output['selected_from_tool']=='list_crons'
