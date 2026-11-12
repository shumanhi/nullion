"""The real producer, persistence and transports finish without quality graders."""
import asyncio
import json
import os
from datetime import UTC, datetime

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, telegram_boundary, text_response, tool_response
from tests.workflow_provider_protocol import is_tool_scope_request

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('surface', ['web', 'telegram', 'scheduled'])
@pytest.mark.parametrize('mode', ['text', 'file', 'partial_source', 'missing_file'])
@pytest.mark.parametrize('prompt', ['選択した記録の結果を教えてください。', 'Entrega el resultado de los registros seleccionados.'])
def test_primary_result_crosses_real_boundaries_without_an_ai_review(tmp_path, monkeypatch, surface, mode, prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app, crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, _build_file_write_handler
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    runtime = setup_runtime(tmp_path, monkeypatch)
    for kind in (BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.FILESYSTEM_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(kind.value, GLOBAL_PERMISSION_PRINCIPAL,
            kind, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    calls = []
    def read(inv):
        calls.append(inv.tool_name)
        output = {'text':'Cedar value 37', 'value':37}
        if mode == 'partial_source':
            output.update(result_kind='source_observations', goal_status='partial', report_status='partial',
                completion_scope='tool_step', next_action='report',
                sources=[{'url':'https://records.example/cedar','text':'Cedar value 37'}],
                observations=[{'semantic_role':'entity','label':'Cedar','value':'37','quote':'Cedar value 37','source_index':0}],
                unknowns=['The remaining catalog was unavailable.'])
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', output)
    registry.register(ToolSpec('file_read', 'Read selected records', ToolRiskLevel.LOW, ToolSideEffectClass.READ,
        False, 5, capability_tags=('local_files',)), read)
    writer = _build_file_write_handler(allowed_roots=(tmp_path,))
    def write(inv): calls.append(inv.tool_name); return writer(inv)
    registry.register(ToolSpec('file_write', 'Write requested report', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE,
        False, 5, produced_artifact_extensions=('.html',)), write)
    cid = 'web:producer-only' if surface == 'web' else 'telegram:42' if surface == 'telegram' else 'telegram:2222'
    path = artifact_root_for_principal(cid)/'cedar.html'
    source = tmp_path/'cedar.txt'
    source.write_text('Cedar value 37')
    file_required = mode in {'file','missing_file'}
    class Provider:
        phase = 0
        quality_calls = []
        def create(self, **kw):
            system = str(kw.get('system') or '')
            names = {row['name'] for row in kw.get('tools') or ()}
            if any(tag in system for tag in ('semantic completion reviewer','material-requirements planner',
                    'Workflow action preflight.','You validate one completed Nullion scheduled-task step.')):
                self.quality_calls.append(system[:100])
                raise AssertionError('Retired quality grader was called')
            if 'present_source_answer' in names:
                self.quality_calls.append('forced source presentation')
                raise AssertionError('The producer answer was replaced by a forced presentation')
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension':'.html' if file_required else None,'confidence':1})
            if 'relationship' in system and 'target_index' in system:
                return text_response(json.dumps({'relationship':'independent','effect':'none','confidence':1,'target_index':None}))
            if 'Return JSON only for a single already-selected read-only tool' in system:
                return text_response(json.dumps({'use_direct':False,'arguments':{}}))
            if is_tool_scope_request(kw):
                return text_response(json.dumps({'web_action':'none','scheduler_action':'none',
                    'requested_tool_names':['file_read',*(['file_write'] if file_required else [])],
                    'required_tool_names':['file_read',*(['file_write'] if file_required else [])],
                    'artifact_extensions':['.html'] if file_required else [], 'confidence':1}))
            if names == {'select_artifact_delivery'}:
                from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
                raw=kw['messages'][0]['content'][0]['text']
                rows=json.loads(raw.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0])
                return tool_response('select_artifact_delivery',{'status':'selected','selected_ids':[r['id'] for r in rows], 'reason':'Final requested report.'})
            if not names: return text_response('{}')
            if 'file_read' not in names and self.phase == 0:
                return tool_response('request_tool_scope', {'capabilities':['local_files'],
                    'tool_names':['file_read',*(['file_write'] if file_required else [])],
                    'required_tool_names':['file_read',*(['file_write'] if file_required else [])],
                    'artifact_extensions':['.html'] if file_required else []})
            self.phase += 1
            if self.phase == 1: return tool_response('file_read', {'path':str(source)})
            if mode == 'file' and self.phase == 2:
                return tool_response('file_write', {'path':str(path),
                    'content':'<!doctype html><html><body><h1>Cedar</h1><p>Value 37</p></body></html>'})
            return text_response('Cedar value 37.'+ (' The remaining catalog was unavailable.' if mode=='partial_source' else ''))
    model=Provider(); runtime.model_client=model; runtime.active_tool_registry=registry
    runner=AgentOrchestrator(model_client=model)
    if surface == 'web':
        with TestClient(web_app.create_app(runtime, runner, registry)) as client:
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
            assert response.status_code==200,response.text
            data=response.json(); reply=data['text']; artifacts=data.get('artifacts') or []
            if mode=='file':
                assert len(artifacts)==1,data
                content=client.get(artifacts[0]['url']).content
                assert content==path.read_bytes() and b'Value 37' in content
            if mode=='missing_file': assert not artifacts,data
    elif surface == 'telegram':
        from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
        from nullion.config import NullionSettings, TelegramSettings
        from nullion.messaging_adapters import delivery_contract_for_turn
        from nullion.telegram_app import _deliver_reply
        from tests.e2e.test_generated_artifact_protocol_recovery import upload_boundary
        from telegram import Bot, Message
        settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',operator_chat_id='42',chat_enabled=True))
        response=run_platform_chat_request(runtime,PlatformChatRequest(platform='telegram',text=prompt,
            conversation_id=cid,chat_id='42',settings=settings,model_client=model,agent_orchestrator=runner,append_activity_trace=False))
        reply=response.text
        contract=delivery_contract_for_turn(None,reply=reply,artifact_paths=response.artifact_paths,requires_attachment_delivery=file_required)
        with upload_boundary() as (bot_url,uploads):
            async def deliver():
                async with Bot('123456:synthetic',base_url=bot_url) as bot:
                    message=Message.de_json({'message_id':1,'date':1,'chat':{'id':42,'type':'private'},'text':prompt},bot)
                    await _deliver_reply(message,reply,principal_id=cid,delivery_contract=contract,
                        artifact_paths=response.artifact_paths,tool_results=response.tool_results)
            asyncio.run(deliver())
        if mode=='file': assert uploads==[path.read_bytes()] and b'Value 37' in uploads[0]
        else: assert not uploads
    else:
        deliveries,send=telegram_boundary(monkeypatch)
        job=crons.add_cron('Report','* * * * *',prompt,enabled=False,delivery_channel='telegram',delivery_target='2222',
            artifact_delivery_options={'requires_attachment_delivery':file_required,'required_artifact_extensions':['.html'] if file_required else []})
        result=run_scheduled(job,model,runtime,registry,send)
        assert len(deliveries)==1,result
        reply=deliveries[0].get('text') or deliveries[0].get('caption') or ''
        if mode=='file': assert deliveries[0]['bytes']==path.read_bytes() and b'Value 37' in deliveries[0]['bytes']
        if mode=='missing_file': assert not result.get('artifacts') and result.get('response_fulfilled') is False,result
        else: assert not result.get('cron_report_failed'),result
    assert not model.quality_calls,model.quality_calls
    assert calls.count('file_read')==1,calls
    assert calls.count('file_write')==int(mode=='file'),calls
    if mode=='missing_file': assert not path.exists()
    elif mode != 'file': assert '37' in reply,reply
    events=runtime.store.list_conversation_events(cid)
    assert not any(e.get('event_type') in {'conversation.completion_review','conversation.action_review',
        'conversation.completion_requirement_contract'} for e in events)


@pytest.mark.parametrize('prompt', ['対象を更新してください。', 'Actualiza el destino seleccionado.'])
def test_native_approval_survives_without_quality_review(tmp_path, monkeypatch, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import approve, create_permission_grant
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, ToolRegistry
    runtime = setup_runtime(tmp_path, monkeypatch)
    writes = []
    registry = ToolRegistry()
    def execute(inv):
        writes.append(inv.arguments)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value':37})
    registry.register(ToolSpec('resource_write', 'Update selected resource', ToolRiskLevel.HIGH,
        ToolSideEffectClass.WRITE, True, 5), execute)
    class Provider:
        def create(self, **kw):
            assert not any(tag in str(kw.get('system')) for tag in
                ('semantic completion reviewer', 'material-requirements planner', 'Workflow action preflight.'))
            if not writes: return tool_response('resource_write', {'target':'selected'})
            return text_response('Updated value: 37.')
    runner = AgentOrchestrator(model_client=Provider())
    kwargs = dict(conversation_id='web:native-approval', principal_id='web:native-approval',
        user_message=prompt, tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
    first = runner.run_turn(**kwargs, conversation_history=[])
    assert first.suspended_for_approval and first.approval_id and writes == []
    stored = runtime.store.get_suspended_turn(first.approval_id)
    runtime.store.add_approval_request(approve(runtime.store.get_approval_request(first.approval_id), decided_by='fixture'))
    runtime.store.add_permission_grant(create_permission_grant(approval_id=first.approval_id,
        principal_id=kwargs['principal_id'], permission='tool:resource_write', granted_by='fixture'))
    # Old paused results must not bring a quality verdict back into the workflow.
    first.response_presentation = {'outcome_review_role':'helper', 'review_failed':True}
    result = runner.resume_turn(**kwargs, previous_result=first, messages_snapshot=stored.messages_snapshot)
    assert writes == [{'target':'selected'}]
    assert result.response_fulfilled and result.final_text == 'Updated value: 37.'
    assert result.outcome_review_receipt is None and result.completion_review_handoff is None
    assert not result.response_presentation.get('review_failed')


@pytest.mark.asyncio
async def test_deep_agent_finishes_from_native_tool_receipts_without_a_grader(tmp_path, monkeypatch):
    from nullion.context_bus import ContextBus
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig
    from nullion.task_queue import TaskRecord, TaskStatus, TaskPriority
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, ToolRegistry
    runtime = setup_runtime(tmp_path, monkeypatch)
    writes=[]
    registry=ToolRegistry()
    def execute(inv):
        writes.append(inv.arguments)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value':37})
    registry.register(ToolSpec('resource_write', 'Update selected resource', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 5, input_schema={'type':'object',
            'properties':{'target':{'type':'string'}}, 'required':['target']}), execute)
    class Provider:
        def create(self, **kw):
            assert not any(tag in str(kw.get('system')) for tag in ('semantic completion reviewer',
                'material-requirements planner', 'Workflow action preflight.', 'You validate one completed Nullion scheduled-task step.'))
            if not writes: return tool_response('resource_write', {'target':'selected'})
            return text_response('Updated value: 37.')
    task=TaskRecord(task_id='producer', group_id='group', conversation_id='web:delegated', principal_id='web:delegated',
        title='Selected resource', description='Actualiza el destino seleccionado.', status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL, allowed_tools=['resource_write'], dependencies=[], context_key_out='observed')
    result=await DeepAgentMiniAgentRunner().run(MiniAgentConfig('writer', task, timeout_s=30), anthropic_client=Provider(),
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store,
        context_bus=ContextBus(), progress_queue=asyncio.Queue())
    assert result.status=='success' and result.output=='Updated value: 37.', result
    assert writes==[{'target':'selected'}]
