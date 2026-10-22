"""Actual Deep Agents graph, registry and Telegram publication boundary."""
import asyncio
import json
import os
from datetime import UTC,datetime
from pathlib import Path

import pytest
from test_scheduled_report_completion import setup_runtime,telegram_boundary,text_response,tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container E2E only')


@pytest.mark.asyncio
@pytest.mark.parametrize('description',[
    'Check both required sources before reporting.',
    '必要な情報源を両方確認してから報告。',
])
@pytest.mark.parametrize('review_response',['rejected','outage','malformed'])
async def test_real_deep_agent_rejection_cannot_publish_findings_or_preexisting_report(tmp_path,monkeypatch,description,review_response):
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig,ProgressUpdate
    from nullion.context_bus import ContextBus
    from nullion.task_queue import TaskRecord,TaskGroup,TaskRegistry,TaskStatus,TaskPriority,TaskResult
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from nullion.result_aggregator import ResultAggregator
    from nullion.telegram_app import _send_operator_telegram_message
    runtime=setup_runtime(tmp_path,monkeypatch);registry=ToolRegistry();reads=[]
    monkeypatch.delenv('NULLION_DEEP_AGENTS_MODEL',raising=False)
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id,call.tool_name,'completed',{'text':'PRIVATE-UNFINISHED-FINDING: one source only.'})
    registry.register(ToolSpec('lookup_source','Read one required source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    class Model:
        calls=0;reviews=0
        def create(self,**kw):
            if str(kw.get('system','')).startswith('You validate one completed Nullion scheduled-task step'):
                self.reviews+=1
                if review_response=='outage':raise TimeoutError('Completion provider unavailable')
                if review_response=='malformed':return text_response('invalid decision')
                assert 'unfinished required work cannot become success' in kw['system']
                return text_response(json.dumps({'status':'failure','answer':'Required second source is unavailable.'}))
            self.calls+=1
            if self.calls==1:return tool_response('lookup_source',{})
            return text_response('PRIVATE-UNFINISHED-FINDING: one source only.')
    task=TaskRecord('source','group','telegram:2222','telegram:2222','Source checks',description,
        TaskStatus.QUEUED,TaskPriority.NORMAL,['lookup_source'],[],metadata={'scheduled_task_run':True})
    model=Model()
    result=await DeepAgentMiniAgentRunner().run(MiniAgentConfig(agent_id='agent',task=task,can_request_user_input=False),
        anthropic_client=model,tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,context_bus=ContextBus(),progress_queue=asyncio.Queue())
    assert reads and model.reviews==1
    assert result.status=='failure' and result.context_out is None and not result.artifacts
    artifact=tmp_path/'report.txt';artifact.write_text('PRIVATE-UNFINISHED-FINDING')
    task.status=TaskStatus.FAILED;task.result=result
    producer=TaskRecord('file','group','telegram:2222','telegram:2222','Report file','Create report',
        TaskStatus.COMPLETE,TaskPriority.NORMAL,['file_write'],[],result=TaskResult('file','success',artifacts=[str(artifact)]))
    group=TaskGroup('group','telegram:2222',description+' report.txt',tasks=[task,producer])
    tasks=TaskRegistry();await tasks.add_group(group);deliveries,_=telegram_boundary(monkeypatch)
    async def deliver(conversation,text,**kw):
        if kw.get('is_status'):return True
        assert not kw.get('is_artifact'),'Incomplete report must not be uploaded'
        receipt=await _send_operator_telegram_message('fixture-token','2222',text,principal_id=conversation)
        runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':conversation,
            'created_at':datetime.now(UTC).isoformat(),'assistant_reply':text})
        return receipt
    aggregator=ResultAggregator(deliver_fn=deliver,task_registry=tasks,min_progress_interval_s=0)
    await aggregator._handle(ProgressUpdate(agent_id='agent',task_id='source',group_id='group',kind='task_failed'))
    assert len(deliveries)==1 and 'bytes' not in deliveries[0]
    assert 'No report was published' in deliveries[0]['text'] and 'PRIVATE-UNFINISHED-FINDING' not in deliveries[0]['text']
    saved=runtime.store.list_conversation_events('telegram:2222')
    assert saved[-1]['assistant_reply']==deliveries[0]['text']


@pytest.mark.asyncio
async def test_delegated_upload_failure_never_announces_complete_delivery(tmp_path,monkeypatch):
    from nullion.task_queue import TaskRecord,TaskGroup,TaskRegistry,TaskStatus,TaskPriority,TaskResult
    from nullion.result_aggregator import ResultAggregator
    from nullion.mini_agent_runner import ProgressUpdate
    from nullion.telegram_app import _send_operator_telegram_message
    from nullion.artifacts import artifact_path_for_generated_workspace_file
    runtime=setup_runtime(tmp_path,monkeypatch);deliveries,_=telegram_boundary(monkeypatch)
    paths=[];tasks=[]
    for index in range(2):
        path=artifact_path_for_generated_workspace_file(principal_id='telegram:2222',suffix='.txt',stem='report-'+str(index))
        path.write_text('Complete verified report '+str(index));paths.append(str(path))
        tasks.append(TaskRecord('file-'+str(index),'uploads','telegram:2222','telegram:2222','Report '+str(index),
            'Create verified report',TaskStatus.COMPLETE,TaskPriority.NORMAL,['file_write'],[],
            result=TaskResult('file-'+str(index),'success',artifacts=[str(path)]),metadata={'artifact_role':'deliverable'}))
    group=TaskGroup('uploads','telegram:2222','Deliver both complete .txt report files.',tasks=tasks)
    registry=TaskRegistry();await registry.add_group(group);order=[]
    async def deliver(conversation,text,**kw):
        if kw.get('is_status'):return True
        order.append(('file' if kw.get('is_artifact') else 'text',text))
        if kw.get('is_artifact') and text==paths[1]:return False
        payload='MEDIA:'+text if kw.get('is_artifact') else text
        return await _send_operator_telegram_message('fixture-token','2222',payload,principal_id=conversation)
    aggregator=ResultAggregator(deliver_fn=deliver,task_registry=registry,min_progress_interval_s=0)
    await aggregator._handle(ProgressUpdate(agent_id='agent',task_id='file-1',group_id='uploads',kind='task_complete'))
    assert [kind for kind,_ in order]==['file','file','text']
    assert group.planner_metadata['delivery_status']=='failed'
    assert group.planner_metadata['undelivered_artifacts']==paths[1:]
    assert len([d for d in deliveries if 'bytes' in d])==1
    assert 'couldn’t deliver' in deliveries[-1]['text'] or "couldn't deliver" in deliveries[-1]['text']
    assert 'complete result' in deliveries[-1]['text'] and 'retry' in deliveries[-1]['text']
    assert 'Complete verified report' not in deliveries[-1]['text']
