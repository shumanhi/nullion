"""Real agent/scheduler/store/delivery boundary; only provider calls are faked."""
from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from test_scheduled_report_completion import setup_runtime, telegram_boundary, run_scheduled, text_response, tool_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('task', [
    'Read obligations and create every missing event before reporting.',
    '記録を確認し、不足する予定をすべて作成してから報告。',
    'Comprueba las obligaciones y crea todos los eventos pendientes antes del informe.',
])
@pytest.mark.parametrize('finish', [True, False])
def test_scheduled_run_resumes_missing_actions_before_publishing_once(tmp_path, monkeypatch, task, finish):
    from nullion.crons import CronJob
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id=kind.value,
            principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=kind, mode=PolicyMode.ALLOW,
            selector='*', created_by='fixture', created_at=datetime.now(UTC)))
    registry = ToolRegistry(); calls = []
    def read(invocation):
        calls.append(('read', None))
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'text': 'Two obligations: A due 2030-11-04 at 19:30 and B due 2030-11-05 at 18:00.'})
    def write(invocation):
        ident = invocation.arguments['id']; calls.append(('create', ident))
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed',
            {'id': ident, 'action_receipt': {'status':'completed', 'summary':'Event created', 'object_id':ident}})
    registry.register(ToolSpec(name='web_fetch', description='Read fixture records', risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=2), read)
    registry.register(ToolSpec(name='create_record', description='Create a missing fixture calendar entry', risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE, requires_approval=False, timeout_seconds=2), write)
    class Model:
        steps = 0
        repairs = 0
        resumed_steps = 0
        def create(self, **kw):
            if not kw.get('tools'):
                self.repairs += 1
                complete = ('create', 'B') in calls
                return text_response(json.dumps({'requirements_satisfied':complete,
                    'source_state':'available', 'evidence_refs':['e0'],
                    'remaining_work':[] if complete else ['Check duplicate B and create it if missing'],
                    'report':'Both obligations were checked. Events A and B were created.' if complete else
                             'A was created. B remains unscheduled.'}))
            self.steps += 1
            if self.steps == 1: return tool_response('web_fetch', {'url':'https://example.com/obligations'})
            if self.steps == 2: return tool_response('create_record', {'id':'A'})
            resuming = 'remaining_work' in json.dumps(kw['messages'], ensure_ascii=False)
            if not resuming: raise TimeoutError('Provider stopped before the second obligation')
            self.resumed_steps += 1
            if self.resumed_steps == 1:
                # A model repeats a successful action after resuming. The real
                # executor must reuse its receipt without making a second write.
                return tool_response('create_record', {'id':'A'})
            if self.resumed_steps == 2 and finish: return tool_response('create_record', {'id':'B'})
            if finish: return text_response('Both obligations were checked. Events A and B were created.')
            raise TimeoutError('Required provider remains unavailable')
    client = Model(); deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='complete-report', name='義務', schedule='0 * * * *', task=task,
        delivery_channel='telegram', delivery_target='2222')
    outcome = run_scheduled(job, client, runtime, registry, send)
    assert calls == [('read',None), ('create','A')] + ([('create','B')] if finish else [])
    assert outcome['cron_completion_continuations'] == 1
    assert len(deliveries) == 1
    wire = deliveries[0]['text']
    assert 'Partial report' not in wire and 'B remains unscheduled' not in wire
    assert not outcome.get('cron_report_partial')
    assert outcome['execution_outcome']['execution_succeeded'] is finish
    if finish:
        assert 'Events A and B were created' in wire
        assert not outcome.get('cron_report_failed')
    else:
        assert 'Events A and B' not in wire
        assert outcome['cron_report_failed'] and outcome['response_fulfilled'] is False
    actual = [e for e in runtime.store.list_events() if e.event_type == 'tool.invocation.completed']
    assert len([e for e in actual if e.payload.get('tool_name') == 'create_record']) == (2 if finish else 1)


@pytest.mark.parametrize('channel', ['web', 'telegram', 'slack', 'discord'])
@pytest.mark.parametrize('manual', [True, False])
def test_publication_gate_rejects_legacy_unfinished_report_on_every_surface(tmp_path, monkeypatch, channel, manual):
    from nullion.cron_delivery import CronRunDeliveryCallbacks, cron_delivery_block_reason, run_cron_delivery_workflow, manual_cron_terminal_status_text
    runtime = setup_runtime(tmp_path, monkeypatch); sent=[]
    job = SimpleNamespace(id='gate', name='確認', task='すべて確認。', delivery_channel=channel,
        delivery_target='22', artifact_delivery_options={})
    draft={'text':'**Partial report**\nPRIVATE-UNFINISHED-FINDING', 'cron_report_partial':True,
        'response_fulfilled':False, 'artifacts':[], 'tool_results':[]}
    def deliver(*args): sent.append(args[-1]); return True
    result=run_cron_delivery_workflow(job, label='Manual scheduled task run' if manual else 'Scheduled task',
        callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:channel, delivery_target=lambda j,c:'22',
            run_agent_turn=lambda *a:dict(draft), record_event=lambda *a,**k:None,
            block_reason=cron_delivery_block_reason, save_web_delivery=deliver, send_platform_delivery=deliver,
            silent_delivery_text=(lambda *a:'Nothing new') if manual else None))
    assert len(sent)==1 and 'PRIVATE-UNFINISHED-FINDING' not in str(sent)
    assert 'Partial report' not in str(sent)
    assert result['execution_outcome']['execution_succeeded'] is False
    assert 'Failed:' in manual_cron_terminal_status_text(job, result=result)


def test_plain_chat_keeps_one_model_call_and_never_loads_completion_contract(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    from nullion import agent_orchestrator
    runtime=setup_runtime(tmp_path, monkeypatch)
    def slow_optional(*a, **k):
        time.sleep(0.5)
        raise AssertionError('Completion review entered a plain turn')
    monkeypatch.setattr(agent_orchestrator, '_extract_completion_requirement_contract', slow_optional)
    class Model:
        calls=0
        def create(self, **kw): self.calls+=1; return text_response('Hello.')
    client=Model(); agent=AgentOrchestrator(model_client=client); durations=[]
    for prompt in ('Hello', 'こんにちは', 'Hola', 'مرحبا', 'Bonjour'):
        start=time.perf_counter()
        result=agent.run_turn(conversation_id='web:plain', principal_id='web:plain', user_message=prompt,
            conversation_history=[], tool_registry=ToolRegistry(), policy_store=runtime.store, approval_store=runtime.store)
        durations.append(time.perf_counter()-start)
        assert result.final_text=='Hello.' and not result.messages_snapshot
    assert client.calls==5
    assert sum(durations)/len(durations)<0.3 and sorted(durations)[-1]<0.5, durations


@pytest.mark.parametrize('finish', [True, False])
def test_real_chat_continues_a_partial_source_before_terminal_answer(tmp_path, monkeypatch, finish):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime=setup_runtime(tmp_path,monkeypatch); registry=ToolRegistry(); source_calls=[]
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0'); monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    chat_store._store=None
    def read(call):
        source_calls.append(call.arguments['part'])
        rows=[{'value':'A at 19:30','quote':'A at 19:30'}]
        if len(source_calls)>1 and finish: rows.append({'value':'B at 18:00','quote':'B at 18:00'})
        return ToolResult(call.invocation_id,call.tool_name,'completed',{'result_kind':'source_observations',
            'report_status':'completed' if len(rows)==2 else 'partial','observations':rows,
            'unknowns':[] if len(rows)==2 else ['Required B not checked']})
    registry.register(ToolSpec('lookup_source','Read fixture parts',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,capability_tags=('web',)),read)
    class Model:
        steps=0; reviews=0
        def create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}; system=str(kw.get('system') or '')
            if system.startswith('You are a material-requirements planner'):
                return text_response(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Verify A and B together','requirement_kind':'fact','evidence_subject_id':'both',
                     'eligible_evidence_tool_names':['lookup_source']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1
                return text_response(json.dumps({'disposition':'retry' if len(source_calls)==1 else 'blocked',
                    'unresolved_requirements':['B must also be checked'], 'retry_tool_names':['lookup_source'] if len(source_calls)==1 else []}))
            if names=={'present_source_answer'}:
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                message=kw['messages'][0]['content'][0]['text']
                context=json.loads(message.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n')[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END)[0])
                return tool_response('present_source_answer',{'intro':'Checked:', 'points':[
                    {'evidence_ids':[r['id']],'detail_ids':[],'relevance':'Requested entry'} for r in context['evidence']],
                    'limitation_ids':[r['id'] for r in context['limitations']]})
            if not names:return text_response('{}')
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':None,'confidence':1})
            self.steps+=1
            if self.steps==1: return tool_response('request_tool_scope',{'capabilities':['web'], 'tool_names':['lookup_source'], 'required_tool_names':['lookup_source']})
            assert self.steps<=4, (source_calls, names)
            if self.steps==4: return text_response('A at 19:30. B at 18:00.' if finish else 'B must also be checked.')
            return tool_response('lookup_source',{'part':'A' if self.steps==2 else 'B'})
    model=Model()
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':'AとBの時刻を両方確認。','conversation_id':'web:complete-two'})
        assert response.status_code==200,response.text
        payload=response.json()
    assert source_calls==['A','B']
    assert model.reviews>=1
    assert not payload.get('artifacts') and 'Partial report' not in payload['text']
    if finish: assert 'A at 19:30' in payload['text'] and 'B at 18:00' in payload['text']
    else:
        # Interactive requests retain checked facts while clearly disclosing
        # the unfinished requirement. Scheduled publication still stays gated.
        assert 'A at 19:30' in payload['text'] and 'Required B not checked' in payload['text']
        assert 'B at 18:00' not in payload['text']
    turns=[e for e in runtime.store.list_conversation_events('web:complete-two') if e.get('event_type')=='conversation.chat_turn']
    assert len([t for t in turns[-1]['tool_results'] if t['tool_name']=='lookup_source'])==2


def test_required_report_upload_failure_is_failed_delivery_without_report_preview(tmp_path,monkeypatch):
    from nullion.cron_delivery import CronRunDeliveryCallbacks, run_cron_delivery_workflow, manual_cron_terminal_status_text
    setup_runtime(tmp_path,monkeypatch)
    from nullion.artifacts import artifact_path_for_generated_workspace_file
    artifact=artifact_path_for_generated_workspace_file(principal_id='telegram:22',suffix='.html',stem='report'); artifact.write_text('<html><body><h1>Complete findings</h1><p>Verified</p></body></html>')
    job=SimpleNamespace(id='file',name='File report',task='Report file',artifact_delivery_options={'required_artifact_extensions':['.html'], 'html_image_delivery_mode':'file'})
    sent=[]; events=[]
    def send(*args):sent.append(args[-1]);return 'MEDIA:' not in args[-1]
    result=run_cron_delivery_workflow(job,label='Scheduled task',callbacks=CronRunDeliveryCallbacks(
        effective_channel=lambda j:'telegram',delivery_target=lambda j,c:'22',
        run_agent_turn=lambda *a:{'text':'PRIVATE-FINDING\nMEDIA:'+str(artifact),'response_fulfilled':True,'artifacts':[str(artifact)],
            'artifact_delivery_required':True,'artifact_delivery_satisfied':True,'required_artifact_extensions':['.html']},
        record_event=lambda event,*a,**kw:events.append(event),block_reason=lambda *a:None,
        save_web_delivery=lambda *a:False,send_platform_delivery=send))
    assert len(sent)==2 and 'MEDIA:' in sent[0]
    assert 'PRIVATE-FINDING' not in sent[1] and 'not been delivered' in sent[1]
    assert result['cron_delivery_status']=='failed' and not result['cron_delivery_partial_success']
    assert result['execution_outcome']['execution_succeeded'] is False
    assert events[-1]=='cron.delivery.failed'
    assert 'Failed:' in manual_cron_terminal_status_text(job,result=result)


@pytest.mark.parametrize('mode', ['complete','blocked','approval'])
def test_html_report_executes_account_action_and_preserves_approval_before_file_delivery(tmp_path,monkeypatch,mode):
    from nullion.crons import CronJob
    from nullion.tools import create_plugin_tool_registry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.policy import BoundaryKind,GLOBAL_PERMISSION_PRINCIPAL,BoundaryPolicyRule,PolicyMode
    from nullion.artifacts import artifact_path_for_generated_workspace_file
    runtime=setup_runtime(tmp_path,monkeypatch); registry=create_plugin_tool_registry(workspace_root=tmp_path,allowed_roots=[tmp_path])
    path=artifact_path_for_generated_workspace_file(principal_id='telegram:2222',suffix='.html',stem='complete')
    for kind in (BoundaryKind.OUTBOUND_NETWORK,BoundaryKind.ACCOUNT_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id=kind.value,principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=kind,mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    writes=[]
    def read(call):return ToolResult(call.invocation_id,call.tool_name,'completed',{
        'title':'Recorded reading','sensor':{'reading':37.5,'unit':'C','observed_at':'2030-11-04T19:30:00Z'}})
    def write(call):
        if mode=='blocked':return ToolResult(call.invocation_id,call.tool_name,'failed',{},error='External account unavailable')
        writes.append(call.arguments['id'])
        return ToolResult(call.invocation_id,call.tool_name,'completed',{'id':'B','action_receipt':{'type':'action_receipt','object_id':'B'}})
    registry.register(ToolSpec('lookup_reading','Read source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    registry.register(ToolSpec('create_record','Create required record',ToolRiskLevel.HIGH if mode=='approval' else ToolRiskLevel.LOW,
        ToolSideEffectClass.ACCOUNT_WRITE,mode=='approval',2),write)
    initial='<!doctype html><html><body><h1>Reading</h1><p>37.5 C at 2030-11-04T19:30:00Z</p><p>Record B pending</p></body></html>'
    complete=initial.replace('Record B pending','Record B created')
    class Model:
        step=0;reviews=0
        def create(self,**kw):
            if not kw.get('tools'):
                payload=json.loads(kw['messages'][0]['content'][0]['text'])
                assert payload.get('existing_html') is None, 'Completed reports must not require a second model review'
                return text_response(json.dumps({'requirements_satisfied':False,'html':''}))
            self.step+=1
            if self.step==1:return tool_response('lookup_reading',{})
            if self.step==2:return tool_response('create_record',{'id':'B'})
            if mode=='blocked':return text_response('The account service is unavailable. The report could not be completed.')
            if self.step==3:return tool_response('file_write',{'path':str(path),'content':complete})
            return text_response('Report attached.')
    deliveries,send=telegram_boundary(monkeypatch);model=Model()
    job=CronJob(id='html-actions',name='完全',task='記録を読み、Bを作成して、数値・単位・日時と作成結果をHTMLで報告。',
        schedule='0 * * * *',delivery_channel='telegram',delivery_target='2222',artifact_delivery_options={
            'required_artifact_extensions':['.html'],'html_image_delivery_mode':'file'})
    result=run_scheduled(job,model,runtime,registry,send)
    assert model.reviews==0
    assert all(attempt.get('phase')!='review' for attempt in result.get('cron_artifact_review_attempts',[]))
    if mode=='complete':
        assert writes==['B'] and result['execution_outcome']['execution_succeeded']
        assert len(deliveries)==1 and 'bytes' in deliveries[0]
        assert b'37.5 C' in deliveries[0]['bytes'] and b'Record B created' in deliveries[0]['bytes']
    elif mode=='approval':
        assert writes==[] and deliveries==[] and result['cron_delivery_status']=='paused_for_approval'
        assert runtime.store.get_approval_request(result['approval_id']).status.value=='pending'
    else:
        assert writes==[] and len(deliveries)==1 and 'bytes' not in deliveries[0]
        assert '37.5' not in deliveries[0]['text'] and result['execution_outcome']['execution_succeeded'] is False
