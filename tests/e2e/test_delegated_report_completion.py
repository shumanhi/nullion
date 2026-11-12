"""Actual Deep Agents graph, registry and Telegram publication boundary."""
from tests.workflow_provider_protocol import workflow_provider_reply
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
async def test_delegated_report_preserves_findings_after_unsubstantiated_critique_or_review_outage(tmp_path,monkeypatch,description,review_response):
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig,ProgressUpdate
    from nullion.context_bus import ContextBus
    from nullion.task_queue import TaskRecord,TaskGroup,TaskRegistry,TaskStatus,TaskPriority,TaskResult
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from nullion.result_aggregator import ResultAggregator
    from nullion.telegram_app import _send_operator_telegram_message
    from nullion.artifacts import artifact_path_for_generated_workspace_file
    runtime=setup_runtime(tmp_path,monkeypatch);registry=ToolRegistry();reads=[]
    monkeypatch.delenv('NULLION_DEEP_AGENTS_MODEL',raising=False)
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id,call.tool_name,'completed',{'text':'PRIVATE-UNFINISHED-FINDING: one source only.'})
    registry.register(ToolSpec('lookup_source','Read one required source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    class Model:
        calls=0;reviews=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            if 'material-requirements planner' in kw.get('system', ''):
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [
                        {'requirement': 'Check both assigned sources', 'requirement_kind': 'fact',
                         'evidence_subject_id': 'sources', 'eligible_evidence_tool_names': ['lookup_source']}]}))
            if 'semantic completion reviewer' in kw.get('system', ''):
                self.reviews+=1
                if review_response=='outage':raise TimeoutError('Completion provider unavailable')
                if review_response=='malformed':return text_response('invalid decision')
                payload = json.loads(kw['messages'][0]['content'][0]['text'])
                assert payload['original_request'] == description and payload['tool_evidence']
                return text_response(json.dumps({'disposition':'blocked','unresolved_requirements':['Required second source is unavailable.'],
                    'user_facing_failure':'Required second source is unavailable.','retry_tool_names':[]}))
            self.calls+=1
            if self.calls==1:return tool_response('lookup_source',{})
            return text_response('PRIVATE-UNFINISHED-FINDING: one source only.')
    task=TaskRecord('source','group','telegram:2222','telegram:2222','Source checks',description,
        TaskStatus.QUEUED,TaskPriority.NORMAL,['lookup_source'],[],metadata={'scheduled_task_run':True})
    model=Model()
    result=await DeepAgentMiniAgentRunner().run(MiniAgentConfig(agent_id='agent',task=task,can_request_user_input=False),
        anthropic_client=model,tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,context_bus=ContextBus(),progress_queue=asyncio.Queue())
    assert reads and model.reviews == 0
    assert result.status == 'success'
    assert not result.artifacts
    assert result.verified_turn_result.response_fulfilled is True
    assert result.verified_turn_result.outcome_review_receipt is None
    assert 'outcome_review_status' not in result.verified_turn_result.response_presentation
    assert 'PRIVATE-UNFINISHED-FINDING' in str(result.output)
    artifact=artifact_path_for_generated_workspace_file(principal_id='telegram:2222', suffix='.txt', stem='report')
    artifact.write_text('PRIVATE-UNFINISHED-FINDING')
    task.status=TaskStatus.COMPLETE;task.result=result
    producer=TaskRecord('file','group','telegram:2222','telegram:2222','Report file','Create report',
        TaskStatus.COMPLETE,TaskPriority.NORMAL,['file_write'],['source'],result=TaskResult('file','success',artifacts=[str(artifact)]))
    group=TaskGroup('group','telegram:2222',description+' report.txt',tasks=[task,producer])
    tasks=TaskRegistry();await tasks.add_group(group);deliveries,_=telegram_boundary(monkeypatch)
    async def deliver(conversation,text,**kw):
        if kw.get('is_status'):return True
        receipt=await _send_operator_telegram_message('fixture-token','2222',
            'MEDIA:'+text if kw.get('is_artifact') else text,principal_id=conversation)
        runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':conversation,
            'created_at':datetime.now(UTC).isoformat(),'assistant_reply':text})
        return receipt
    aggregator=ResultAggregator(deliver_fn=deliver,task_registry=tasks,min_progress_interval_s=0)
    await aggregator._handle(ProgressUpdate(agent_id='agent',task_id='source',group_id='group',kind='task_failed'))
    assert len([delivery for delivery in deliveries if 'bytes' in delivery]) == 1
    assert 'Review warning:' not in deliveries[-1]['text']
    assert 'PRIVATE-UNFINISHED-FINDING' in deliveries[-1]['text']
    assert group.planner_metadata['outcome_verified'] is True
    saved=runtime.store.list_conversation_events('telegram:2222')
    assert saved[-1]['assistant_reply']==deliveries[-1]['text']


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


@pytest.mark.asyncio
@pytest.mark.parametrize('description',['Create one workbook containing the verified value 53.', '値53を含む表を１つ作成してください。'])
async def test_scheduled_deep_agent_optional_image_failure_repairs_and_delivers_one_workbook(tmp_path,monkeypatch,description):
    import io
    from openpyxl import load_workbook
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig,ProgressUpdate
    from nullion.context_bus import ContextBus
    from nullion.task_queue import TaskRecord,TaskGroup,TaskRegistry,TaskStatus,TaskPriority
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_spreadsheet_create_handler
    from nullion.artifacts import artifact_path_for_generated_workspace_file
    from nullion.result_aggregator import ResultAggregator
    from nullion.telegram_app import _send_operator_telegram_message
    runtime=setup_runtime(tmp_path,monkeypatch);monkeypatch.delenv('NULLION_DEEP_AGENTS_MODEL',raising=False)
    output=artifact_path_for_generated_workspace_file(principal_id='telegram:2222',suffix='.xlsx',stem='verified')
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);native=_build_spreadsheet_create_handler(tmp_path,allowed_roots=[tmp_path]);executed=[]
    def create(inv):
        result=native(inv);executed.append(result);return result
    registry.register(ToolSpec('spreadsheet_create','Create native workbook',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),create)
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            self.phase+=1
            args={'output_path':str(output),'columns':['Name'],'rows':[['Verified value 53']]}
            if self.phase==1:return tool_response('spreadsheet_create',{**args,'image_paths':['https://example.invalid/not-fetched.png']})
            if self.phase==2:return tool_response('spreadsheet_create',args)
            assert self.phase==3,'The optional image must not force a repair loop'
            return text_response('Ready.\nMEDIA:'+str(output))
    task=TaskRecord('file','group','telegram:2222','telegram:2222','Workbook',description,
        TaskStatus.QUEUED,TaskPriority.NORMAL,['spreadsheet_create'],[],metadata={
            'scheduled_task_run':True,'required_embedded_media_extensions':[]})
    model=Model();progress=asyncio.Queue()
    result=await DeepAgentMiniAgentRunner().run(MiniAgentConfig(agent_id='agent',task=task),
        progress_queue=progress,context_bus=ContextBus(),tool_registry=registry,anthropic_client=model,
        policy_store=None,approval_store=None)
    assert result.status=='success',result
    assert result.verified_turn_result.response_fulfilled is True,result.verified_turn_result
    failures=[r for r in result.verified_turn_result.tool_results if r.tool_name=='spreadsheet_create' and r.status=='failed']
    assert len(failures)==1 and failures[0].response_requirements['_native_artifact_media']['required'] is False
    assert [r.status for r in executed]==['failed','completed']
    assert executed[0].response_requirements['_native_artifact_media']['required'] is False
    assert str(output) in result.artifacts
    task.status=TaskStatus.COMPLETE;task.result=result
    group=TaskGroup('group','telegram:2222',description+' '+output.name,tasks=[task])
    tasks=TaskRegistry();await tasks.add_group(group);deliveries,_=telegram_boundary(monkeypatch)
    async def deliver(conversation,text,**kw):
        if kw.get('is_status'):return True
        return await _send_operator_telegram_message('fixture-token','2222',
            'MEDIA:'+text if kw.get('is_artifact') else text,principal_id=conversation)
    class SummaryModel:
        def create(self,**kw):
            payload=json.loads(kw['messages'][0]['content'][0]['text'])
            assert payload['prepared_delivery_artifacts']==[output.name]
            assert '53' in payload['artifact_evidence'][0]['content_preview']
            return text_response(json.dumps({'status':'complete','answer':'Workbook attached.',
                'evaluated_task_ids':['file'],'missing_outcomes':[]}))
    aggregator=ResultAggregator(deliver_fn=deliver,task_registry=tasks,min_progress_interval_s=0,
        model_client=SummaryModel())
    updates=[]
    while not progress.empty():updates.append(progress.get_nowait())
    completed=[update for update in updates if update.kind=='task_complete']
    assert len(completed)==1,updates
    await aggregator._handle(completed[0])
    files=[row for row in deliveries if 'bytes' in row]
    assert len(files)==1,deliveries
    assert group.planner_metadata['outcome_verified'] is True,group.planner_metadata
    workbook=load_workbook(io.BytesIO(files[0]['bytes']),read_only=True)
    assert workbook.active['A2'].value=='Verified value 53';workbook.close()
    assert not any('must embed' in row.get('text','') for row in deliveries)
