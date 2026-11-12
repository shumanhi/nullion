"""Reviewer criticism cannot take over real chat routing, history or delivery."""
import asyncio
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_universal_outcome_review_boundary import ReasoningProvider,runtime_and_registry,text

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('surface',['runner','web','telegram'])
@pytest.mark.parametrize('mode',['timeout','invalid','critique'])
@pytest.mark.parametrize('prompt',['選択した値を報告してください。','Informa del valor seleccionado.'])
def test_helper_keeps_answer_and_completed_read_without_spurious_questions_or_research(tmp_path,monkeypatch,surface,mode,prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion import chat_store,web_app
    runtime,registry,reads=runtime_and_registry(tmp_path,monkeypatch,'weather_forecast')
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    registry=ToolRegistry()
    def read(call):
        reads.append(call)
        return ToolResult(call.invocation_id,call.tool_name,'completed',{'value':7,'target':'resource'})
    registry.register(ToolSpec('weather_forecast','Read the selected fixture value',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2,capability_tags=('weather',)),read)
    runtime.active_tool_registry=registry
    from datetime import UTC,datetime
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-network',
        principal_id=GLOBAL_PERMISSION_PRINCIPAL,kind=BoundaryKind.OUTBOUND_NETWORK,
        mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setenv('NULLION_MULTI_AGENT_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED','0')
    monkeypatch.setattr(chat_store,'_store',None)
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system') or '';names={row['name'] for row in kw.get('tools',[])}
            if 'semantic completion reviewer' in system:
                self.review_calls+=1
                if mode=='timeout':
                    from nullion.model_clients import ModelClientTimeoutError
                    raise ModelClientTimeoutError('Synthetic editor outage')
                if mode=='invalid':return text('{')
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                ref=next(row['invocation_id'] for row in packet['citable_evidence'] if row['tool_name']==self.name)
                return text({'disposition':'blocked','all_material_requirements_enumerated':True,
                    'unresolved_requirements':['Editorial preference: rewrite the already supported answer.'],
                    'retry_tool_names':[],'material_requirements':[{'requirement_id':row['requirement_id'],
                        'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[ref]}
                        for row in packet['material_requirement_contract']]})
            if 'material-requirements planner' in system:return super().create(**kw)
            if 'select_attachment_format' in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format',
                    'input':{'extension':None,'confidence':1,'embedded_media_extensions':[]}}]}
            if 'relationship' in system and 'target_index' in system:
                return text({'relationship':'independent','effect':'none','confidence':1,'target_index':None})
            if 'Return JSON only for a single already-selected read-only tool' in system:
                return text({'use_direct':False,'arguments':{}})
            if reads and self.name not in names:
                self.agent_calls+=1
                return text('Observed value: 7.')
            if 'request_tool_scope' in names and self.name not in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope',
                    'input':{'capabilities':['weather'],'tool_names':[self.name]}}]}
            if not names and 'WORKFLOW REASONING' not in system and 'material-requirements planner' not in system:
                from nullion.workflow_reasoning import WORKFLOW_REASONING_INSTRUCTION
                if WORKFLOW_REASONING_INSTRUCTION not in system:return text({})
            return super().create(**kw)
    provider=Provider('weather_forecast');runner=AgentOrchestrator(model_client=provider)
    cid='telegram:42' if surface=='telegram' else 'web:editor-'+surface+'-'+mode
    if surface=='runner':
        result=runner.run_turn(conversation_id=cid,principal_id=cid,user_message=prompt,conversation_history=[],
            tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,max_iterations=6)
        answer=result.final_text
        assert result.response_fulfilled is not False,repr(result)
    elif surface=='web':
        from fastapi.testclient import TestClient
        with TestClient(web_app.create_app(runtime,orchestrator=runner,registry=registry)) as client:
            response=client.post('/api/chat',json={'text':'/chat '+prompt,'conversation_id':cid})
            assert response.status_code==200,response.text
            answer=response.json()['text']
            if answer!='Observed value: 7.':
                raise AssertionError(json.dumps({'response':response.json(),'reads':len(reads),
                    'agent_calls':provider.agent_calls,'review_calls':provider.review_calls,
                    'events':[{k:row.get(k) for k in ('event_type','assistant_reply','response_fulfilled','response_presentation','reason','disposition','review_error')}
                        for row in runtime.store.list_conversation_events(cid)]},default=str))
            history=client.get('/api/chat/history/'+cid+'?sync_runtime=1').json()['messages']
            assert any(row['text']==answer and row['role']=='bot' for row in history)
    else:
        from nullion.messaging_turn_graph import PlatformChatRequest,run_platform_chat_request
        from nullion.config import NullionSettings,TelegramSettings
        settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',operator_chat_id='42',chat_enabled=True))
        result=run_platform_chat_request(runtime,PlatformChatRequest(platform='telegram',text=prompt,
            conversation_id=cid,chat_id='42',settings=settings,model_client=provider,agent_orchestrator=runner,
            append_activity_trace=False))
        answer=result.text
        from dataclasses import asdict
        assert answer=='Observed value: 7.',(answer,[asdict(row) for row in runtime.store.list_approval_requests()])
        from telegram import Bot,Message
        from nullion.telegram_app import _deliver_reply
        from tests.e2e.test_health_action_acknowledgements import telegram_boundary
        with telegram_boundary() as (url,calls):
            async def deliver():
                async with Bot('123456:synthetic',base_url=url) as bot:
                    message=Message.de_json({'message_id':1,'date':1,'chat':{'id':42,'type':'private'},'text':prompt},bot)
                    await _deliver_reply(message,answer)
            asyncio.run(deliver())
        wire=[row.get('text') for method,row in calls if method=='sendMessage']
        assert len(wire)==1 and 'Observed value: 7.' in wire[0]
    assert answer=='Observed value: 7.'
    assert len(reads)==1 and provider.agent_calls<=3
    assert provider.review_calls<=3
    if surface!='runner':
        turns=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')
        assert turns and turns[-1]['assistant_reply']==answer


@pytest.mark.asyncio
async def test_delegated_completed_result_survives_outer_helper_deadline(tmp_path,monkeypatch):
    import time
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig
    from nullion.context_bus import ContextBus
    from nullion.task_queue import TaskRecord,TaskStatus,TaskPriority
    runtime,registry,reads=runtime_and_registry(tmp_path,monkeypatch,'resource_read')
    class SlowEditor(ReasoningProvider):
        def create(self,**kw):
            if 'semantic completion reviewer' in (kw.get('system') or ''):
                time.sleep(8)
            return super().create(**kw)
    provider=SlowEditor('resource_read')
    task=TaskRecord('child','group','web:delegated-helper','web:delegated-helper','Selected resource',
        '選択した値を報告してください。',TaskStatus.RUNNING,TaskPriority.NORMAL,['resource_read'],[],context_key_out='observed')
    context=ContextBus()
    result=await DeepAgentMiniAgentRunner().run(MiniAgentConfig('reader',task,timeout_s=6),
        anthropic_client=provider,tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,
        context_bus=context,progress_queue=asyncio.Queue())
    assert result.status=='success' and result.output=='Observed value: 7.'
    assert len(reads)==1 and context.get('observed',group_id='group')=='Observed value: 7.'
    assert result.report_presentation['outcome_review_role']=='helper'
    assert result.report_presentation['outcome_review_status']=='unavailable'


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['timeout','critique'])
async def test_group_helper_cannot_revoke_an_acknowledged_result_or_valid_file(tmp_path,mode):
    from nullion.result_aggregator import ResultAggregator,GroupState
    from nullion.task_queue import TaskGroup,TaskRecord,TaskResult,TaskRegistry,TaskStatus,TaskPriority
    path=tmp_path/'report.html'
    content='<!doctype html><html><body><h1>Selected metrics</h1><p>49 views; 14 unique visitors.</p></body></html>'
    path.write_text(content)
    task=TaskRecord('child','group','web:group-helper','web:group-helper','Metrics','Report the selected metrics',
        TaskStatus.COMPLETE,TaskPriority.NORMAL,[],[],result=TaskResult('child','success',
            '49 views; 14 unique visitors.',artifacts=[str(path)],
            report_presentation={'outcome_review_role':'helper','outcome_review_status':'unavailable'}))
    group=TaskGroup('group','web:group-helper','Return the selected metrics and .html report',tasks=[task])
    class Provider:
        def create(self,**kw):
            packet=json.loads(kw['messages'][0]['content'][0]['text'])
            assert packet['task_results'][0]['outcome_review_status']=='unavailable'
            if mode=='timeout':raise TimeoutError('Synthetic editor outage')
            return text({'status':'partial','answer':'49 views; 14 unique visitors.',
                'evaluated_task_ids':['child'],'missing_outcomes':['An editorial preference remains.']})
    wire=[]
    async def deliver(_conversation,value,**kwargs):
        wire.append((value,kwargs))
        if kwargs.get('is_artifact'):assert Path(value).read_bytes()==content.encode()
        return True
    registry=TaskRegistry();await registry.add_group(group)
    async def forbidden_rework(*args):pytest.fail('A grading verdict alone cannot dispatch more execution')
    aggregator=ResultAggregator(deliver_fn=deliver,task_registry=registry,model_client=Provider(),
        outbox_path=tmp_path/'outbox.db',recover_outcome=forbidden_rework)
    await aggregator._on_group_complete(GroupState('group','web:group-helper',group.original_message),group)
    assert len([row for row in wire if row[1].get('is_artifact')])==1
    assert any('49 views' in row[0] for row in wire if not row[1].get('is_artifact'))
    assert all('Review warning:' not in row[0] for row in wire)
    assert aggregator._outbox.get('group')['status']=='delivered'
    assert group.planner_metadata['delivery_status']=='completed'


@pytest.mark.asyncio
async def test_stop_after_children_finish_prevents_helper_from_restarting_execution(tmp_path):
    from nullion.result_aggregator import ResultAggregator,GroupState
    from nullion.task_queue import TaskGroup,TaskRecord,TaskResult,TaskRegistry,TaskStatus,TaskPriority
    from nullion.agent_orchestrator import AgentOrchestrator
    registry=TaskRegistry()
    task=TaskRecord('child','group','web:stop','web:stop','Read','Report value',
        TaskStatus.COMPLETE,TaskPriority.NORMAL,['resource_read'],[],
        result=TaskResult('child','success','Observed value: 7.'))
    group=TaskGroup('group','web:stop','Report value and unit',tasks=[task])
    await registry.add_group(group)
    runner=AgentOrchestrator(model_client=None);runner._task_registry=registry
    # The child is already terminal, but its helper is still preparing output.
    assert await runner.cancel_conversation('web:stop')==0
    assert group.cancel_requested
    class Provider:
        def create(self,**kw):
            return text({'status':'partial','answer':'Observed value: 7.',
                'evaluated_task_ids':['child'],'missing_outcomes':['Unit missing.'],
                'assistance_plan':{'kind':'execution_improvement',
                    'task_ids':['child'],'tool_names':['resource_read']}})
    async def forbidden_rework(*args):pytest.fail('Stopped group restarted execution')
    wire=[]
    async def deliver(_conversation,value,**kw):wire.append(value);return True
    aggregator=ResultAggregator(deliver_fn=deliver,task_registry=registry,model_client=Provider(),
        outbox_path=tmp_path/'outbox.db',recover_outcome=forbidden_rework)
    await aggregator._on_group_complete(GroupState('group','web:stop',group.original_message),group)
    assert not group.planner_metadata.get('outcome_recovery_attempted')
    assert any('Observed value: 7.' in value for value in wire)
    assert await runner._recover_group_outcome(group,'Observed value: 7.',[]) is None


@pytest.mark.parametrize('prompt',['選択した値を報告してください。','Informa del valor seleccionado.'])
def test_optional_helper_rewrite_timeout_retains_the_completed_producer_answer(tmp_path,monkeypatch,prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.model_clients import ModelClientTimeoutError
    runtime,registry,reads=runtime_and_registry(tmp_path,monkeypatch,'resource_read')
    class Provider(ReasoningProvider):
        def create(self,**kw):
            if 'semantic completion reviewer' in (kw.get('system') or ''):
                self.review_calls+=1
                return text({'disposition':'retry','retry_tool_names':[],
                    'unresolved_requirements':['Improve the presentation of the observed result.']})
            if 'material-requirements planner' not in (kw.get('system') or '') and self.agent_calls>=2:
                raise ModelClientTimeoutError('Optional editor-requested rewrite timed out')
            return super().create(**kw)
    provider=Provider('resource_read')
    result=AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:rewrite-timeout',
        principal_id='web:rewrite-timeout',user_message=prompt,conversation_history=[],
        tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,max_iterations=5)
    assert result.final_text=='Observed value: 7.'
    assert result.response_fulfilled is not False
    assert result.response_presentation['outcome_review_role']=='helper'
    assert len(reads)==1 and provider.review_calls==1


def test_web_partial_helper_cannot_replace_thirteen_existing_findings_with_three(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion import web_app,chat_store
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    runtime,_,reads=runtime_and_registry(tmp_path,monkeypatch,'weather_forecast')
    registry=ToolRegistry();runtime.active_tool_registry=registry;draft='\n'.join('Record '+str(i) for i in range(13))
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'text':draft})
    registry.register(ToolSpec('weather_forecast','Read selected records',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,capability_tags=('weather',)),read)
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from datetime import UTC,datetime
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-records',principal_id=OPERATOR_PERMISSION_PRINCIPAL,permission='tool:weather_forecast',granted_by='fixture',granted_at=datetime.now(UTC)))
    monkeypatch.setattr(chat_store,'_store',None)
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED','0')
    monkeypatch.setenv('NULLION_MULTI_AGENT_ENABLED','0')
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'absent.env'))
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system') or ''
            if 'semantic completion reviewer' in system:
                self.review_calls+=1;packet=json.loads(kw['messages'][0]['content'][0]['text'])
                ref=next(row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name']=='weather_forecast')
                return text({'disposition':'blocked','all_material_requirements_enumerated':True,
                    'retry_tool_names':[],'unresolved_requirements':['One optional detail was not captured.'],
                    'material_requirements':[{'requirement_id':row['requirement_id'],
                        'satisfied':row['evidence_subject_id']=='records',
                        'draft_covered':row['evidence_subject_id']=='records',
                        'evidence_invocation_ids':[ref] if row['evidence_subject_id']=='records' else []}
                        for row in packet['material_requirement_contract']],
                    'supported_findings':[{'text':'\n'.join('Record '+str(i) for i in range(3)),
                        'evidence_invocation_ids':[ref]}]})
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'Report all selected records',
                        'requirement_kind':'fact','evidence_subject_id':'records',
                        'eligible_evidence_tool_names':['weather_forecast']},
                        {'requirement':'Identify missing metadata','requirement_kind':'fact',
                         'evidence_subject_id':'metadata','eligible_evidence_tool_names':['weather_forecast']}]})
            from tests.workflow_provider_protocol import workflow_provider_reply
            protocol=workflow_provider_reply(kw)
            if protocol is not None:return protocol
            self.agent_calls+=1
            names={row['name'] for row in kw.get('tools',[])}
            if 'request_tool_scope' in names and 'weather_forecast' not in names:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'records-scope',
                    'name':'request_tool_scope','input':{'capabilities':['weather'],'tool_names':['weather_forecast']}}]}
            if not reads:return {'content':[{'type':'tool_use','id':'records','name':'weather_forecast','input':{}}],'stop_reason':'tool_use'}
            return text(draft)
    provider=Provider('weather_forecast')
    with TestClient(web_app.create_app(runtime,AgentOrchestrator(model_client=provider),registry)) as client:
        response=client.post('/api/chat',json={'text':'/chat 13件の記録と不足する情報を報告してください。','conversation_id':'web:thirteen'})
        assert response.status_code==200,response.text
        reply=response.json()['text']
    assert all('Record '+str(i) in reply for i in range(13)),repr({'reply':reply,'events':runtime.store.list_conversation_events('web:thirteen')})
    assert len(reads)==1 and provider.review_calls==1
    review=next(event for event in reversed(runtime.store.list_conversation_events('web:thirteen'))
        if event.get('event_type')=='conversation.completion_review')
    assert review['disposition'] in {'blocked','retry'} and not review.get('review_error'),json.dumps(review,default=str)
    assert set(review.get('retry_tool_names') or []) <= {'weather_forecast'}
