"""Real request lineage, scope, SQLite, file generation and platform delivery."""
import asyncio
import json
import os
from pathlib import Path

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('review_timeout', [False])
@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('producer', ['native', 'executor', 'failed', 'missing'])
@pytest.mark.parametrize('extension', ['.png', '.pdf', '.html'])
@pytest.mark.parametrize('followups', [0, 2, 4])
def test_partial_source_stage_reaches_real_file_delivery_after_selected_followups(tmp_path, monkeypatch, extension, followups, surface, producer, review_timeout):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal, artifact_output_descriptor
    from nullion.chat_operator import _remember_chat_turn
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from datetime import datetime, UTC
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', '0')
    cid = 'web:output-stage' if surface == 'web' else 'telegram:42'
    producer_name = 'terminal_exec' if producer == 'executor' else 'p9'
    root = artifact_root_for_principal(cid)
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / ('result' + extension)
    source_path = root / 'source-capture.png'
    from PIL import Image
    Image.new('RGB', (32, 32), 'red').save(source_path)
    original = '選択した記録から読みやすい資料を作成してください。'
    for index in range(followups):
        identity = 'parent-' + str(index)
        _remember_chat_turn(runtime, chat_id=cid, conversation_id=cid, conversation_turn_id=identity,
            user_message=original if index == 0 else '2',
            assistant_reply='次の設定は？\n1. 設定A\n2. 設定B',
            response_presentation={'pending_request': {'original_request': original, 'tool_names': ['browser_run_task']}})
        from dataclasses import replace
        from nullion.conversation_runtime import ConversationTurnDisposition
        stored = runtime.store.get_conversation_turn(identity)
        runtime.store.add_conversation_turn(replace(stored, disposition=(
            ConversationTurnDisposition.INDEPENDENT if index == 0 else ConversationTurnDisposition.CONTINUE)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('network', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    calls = []
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    def observe(inv):
        calls.append(inv.tool_name)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'result_kind':'source_observations', 'goal_status':'answered', 'report_status':'partial',
            'blocker':'Source worker time limit', 'completion_scope':'tool_step',
            'text':'Cedar value 37', 'url':'https://example.test/records',
            'artifact_descriptors':[artifact_output_descriptor(source_path, role='source', kind='screenshot')],
            'sources':[{'url':'https://example.test/records', 'text':'Cedar value 37'}],
            'observations':[{'label':'Cedar', 'value':'37', 'quote':'Cedar value 37', 'source_index':0}]})
    def generate(inv):
        calls.append(inv.tool_name)
        assert (inv.arguments.get('source_value') == 37 if inv.tool_name == 'p9'
                else inv.arguments.get('command') == 'fixture-render 37')
        if producer == 'failed' and inv.tool_name == 'p9':
            return ToolResult(inv.invocation_id, inv.tool_name, 'failed', {'reason':'renderer_unavailable'})
        # Fake the external producer, not routing, file selection, persistence or delivery.
        if extension == '.png':
            from PIL import ImageDraw
            canvas = Image.new('RGB', (180, 240), 'white')
            ImageDraw.Draw(canvas).text((12, 20), 'Cedar value 37', fill='black')
            canvas.save(output_path)
        elif extension == '.pdf':
            from pypdf import PdfWriter
            from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject
            writer = PdfWriter(); page = writer.add_blank_page(width=180, height=240)
            font = DictionaryObject({NameObject('/Type'):NameObject('/Font'),
                NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
            page[NameObject('/Resources')] = DictionaryObject({NameObject('/Font'):
                DictionaryObject({NameObject('/F1'):font})})
            content = DecodedStreamObject(); content.set_data(b'BT /F1 14 Tf 12 210 Td (Cedar value 37) Tj ET')
            page[NameObject('/Contents')] = content
            with output_path.open('wb') as stream: writer.write(stream)
        else:
            output_path.write_text('<!doctype html><html><body><h1>Cedar</h1><p>37</p></body></html>')
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'artifact_path':str(output_path), 'artifact_descriptors':[
                artifact_output_descriptor(output_path, role='deliverable', kind='report')]})
    registry.register(ToolSpec('browser_run_task', 'Read selected records', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 5, capability_tags=('web',)), observe)
    if producer != 'missing':
        registry.register(ToolSpec(producer_name, 'Registered producer', ToolRiskLevel.LOW,
            ToolSideEffectClass.DANGEROUS_EXEC if producer == 'executor' else ToolSideEffectClass.WRITE, False, 5,
            produced_artifact_extensions=() if producer == 'executor' else (extension,)), generate)
    if producer == 'failed':
        registry.register(ToolSpec('terminal_exec', 'Fallback execution', ToolRiskLevel.LOW,
            ToolSideEffectClass.DANGEROUS_EXEC, False, 5), generate)
    packets = []
    trace = []
    class Model:
        phases = 0
        reviews = 0
        def create(self, **kw):
            system = str(kw.get('system') or '')
            names = {row['name'] for row in kw.get('tools', [])}
            trace.append({'system':system[:90], 'names':sorted(names),'phase':self.phases})
            if 'relationship' in system and 'target_index' in system:
                return text(json.dumps({'relationship':'follow_up','effect':'continue','confidence':1,
                    'target_index':0,'target_is_explicit':True}))
            if system.startswith('Return only a JSON object matching this schema:'):
                return text(json.dumps({'web_action':'live_research','scheduler_action':'none',
                    'requested_tool_names':['browser_run_task'],'required_tool_names':['browser_run_task'],
                    'confidence':1}))
            if 'select_attachment_format' in names:
                return call('select_attachment_format', {'extension':None,'confidence':1})
            if 'material-requirements planner' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text']); packets.append(packet)
                if followups:
                    assert packet['selected_request_context']['original_request'] == original
                    assert len(packet['selected_request_context']['turns']) == followups
                return text(json.dumps({'all_material_requirements_enumerated':True,
                    'requested_artifact_extensions':[extension], 'material_requirements':[
                        {'requirement':'Deliver the selected report', 'requirement_kind':'deliverable',
                         'evidence_subject_id':extension,'eligible_evidence_tool_names':[]}]}))
            if review_timeout and 'semantic completion reviewer' in system:
                self.reviews += 1
                from nullion.model_clients import ModelClientTimeoutError
                raise ModelClientTimeoutError('Synthetic review timeout')
            protocol = workflow_provider_reply(kw)
            if protocol is not None: return protocol
            if not names: return text('{}')
            self.phases += 1
            assert self.phases <= 8, 'The same source workflow must not restart'
            if self.phases == 1:
                return call('request_tool_scope', {'capabilities':['web','image_generation'], 'tool_names':['browser_run_task'],
                    'required_tool_names':['browser_run_task'], 'artifact_extensions':[extension]})
            if self.phases == 2:
                return call('browser_run_task', {'url':'https://example.test/records'})
            if producer_name in names and self.phases > 3:
                assert 'Cedar value 37' in json.dumps(kw['messages']), 'The producer lost the source facts'
            if producer == 'failed' and 'p9' in calls and 'terminal_exec' not in calls and self.phases > 4:
                return call('terminal_exec', {'command':'fixture-render 37'})
            if producer_name in names and producer_name not in calls and self.phases > 3:
                return call(producer_name, {'command':'fixture-render 37'} if producer == 'executor' else {'source_value':37})
            return text('資料を添付しました。\nMEDIA:'+str(output_path))
    model = Model()
    runtime.active_tool_registry = registry
    orchestrator = AgentOrchestrator(model_client=model)
    prompt = '1' if followups else original
    if surface == 'web':
        with TestClient(web_app.create_app(runtime, orchestrator=orchestrator, registry=registry)) as client:
            response = client.post('/api/chat', json={'text':prompt, 'conversation_id':cid})
            assert response.status_code == 200, response.text
            data = response.json()
            if producer == 'missing':
                assert not data.get('artifacts')
                assert 'not been created' in data['text'], json.dumps({'data':data,'events':runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')},default=str)
                assert calls == ['browser_run_task']
                return
            assert len(data.get('artifacts', [])) == 1, json.dumps({'data':data,'trace':trace,'calls':calls},default=str)
            delivered = client.get(data['artifacts'][0]['url']).content
            assert 'attach the requested file' not in data['text']
    else:
        from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
        from nullion.config import NullionSettings, TelegramSettings
        from nullion.messaging_adapters import delivery_contract_for_turn
        from nullion.telegram_app import _deliver_reply
        from tests.e2e.test_generated_artifact_protocol_recovery import upload_boundary
        from telegram import Bot, Message
        settings = NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',operator_chat_id='42',chat_enabled=True))
        response = run_platform_chat_request(runtime, PlatformChatRequest(platform='telegram',text=prompt,
            conversation_id=cid,chat_id='42',settings=settings,model_client=model,agent_orchestrator=orchestrator,
            append_activity_trace=False))
        if producer == 'missing':
            assert not response.artifact_paths
            assert 'not been created' in response.text, response.text
            assert calls == ['browser_run_task']
            wire = []
            with upload_boundary(wire) as (bot_url, uploads):
                async def deliver_failure():
                    async with Bot('123456:synthetic', base_url=bot_url) as bot:
                        message = Message.de_json({'message_id':1,'date':1,'chat':{'id':42,'type':'private'},'text':prompt},bot)
                        await _deliver_reply(message,response.text,principal_id=cid,tool_results=response.tool_results)
                asyncio.run(deliver_failure())
            assert not uploads
            assert any(method=='sendMessage' and 'not been created' in payload.get('text','')
                for method,payload in wire)
            return
        assert len(response.artifact_paths) == 1, {'reply':response.text,'trace':trace}
        contract = delivery_contract_for_turn(None, reply=response.text, artifact_paths=response.artifact_paths,
            requires_attachment_delivery=True)
        with upload_boundary() as (bot_url, uploads):
            async def deliver():
                async with Bot('123456:synthetic', base_url=bot_url) as bot:
                    message = Message.de_json({'message_id':1,'date':1,'chat':{'id':42,'type':'private'},'text':prompt},bot)
                    await _deliver_reply(message,response.text,principal_id=cid,delivery_contract=contract,
                        artifact_paths=response.artifact_paths,tool_results=response.tool_results)
            asyncio.run(deliver())
        assert len(uploads) == 1
        delivered = uploads[0]
    assert delivered == output_path.read_bytes()
    assert delivered != source_path.read_bytes()
    import io
    if extension == '.pdf':
        from pypdf import PdfReader
        assert 'Cedar value 37' in PdfReader(io.BytesIO(delivered)).pages[0].extract_text()
    elif extension == '.png':
        with Image.open(io.BytesIO(delivered)) as rendered:
            assert rendered.size == (180, 240) and len(rendered.getcolors()) > 1
    else:
        assert b'Cedar' in delivered and b'37' in delivered

    final = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
    receipt_name = 'terminal_exec' if producer == 'failed' else producer_name
    assert any(row['tool_name']==receipt_name and row['status']=='completed' for row in final['tool_results']), final
    assert str(output_path) in final['artifact_paths'], final
    assert calls == (['browser_run_task', 'p9', 'terminal_exec'] if producer == 'failed'
                     else ['browser_run_task', producer_name])
    assert not packets and model.reviews == 0
