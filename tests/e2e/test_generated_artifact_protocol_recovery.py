"""Real SSE adapter -> runner -> renderer -> Web download / Telegram upload."""
import asyncio
from contextlib import contextmanager
from datetime import datetime, UTC
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import shlex
import sys
import threading

import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@contextmanager
def model_boundary(decide):
    bodies=[]
    class Server(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def do_POST(self):
            body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            bodies.append(body)
            events=decide(body)
            payload=''.join('data: '+json.dumps(e,ensure_ascii=False)+'\n\n' for e in events).encode()
            self.send_response(200);self.send_header('Content-Type','text/event-stream')
            self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Server)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield 'http://127.0.0.1:'+str(server.server_port),bodies
    finally:server.shutdown();server.server_close();thread.join(2)


@contextmanager
def upload_boundary(messages=None):
    from email.parser import BytesParser
    from email.policy import default
    from urllib.parse import parse_qs
    uploads=[]
    class Server(BaseHTTPRequestHandler):
        def do_POST(self):
            raw=self.rfile.read(int(self.headers.get('Content-Length',0)))
            content_type=self.headers.get('Content-Type','')
            payload={}
            if content_type.startswith('multipart/'):
                message=BytesParser(policy=default).parsebytes(
                    b'Content-Type: '+content_type.encode()+b'\r\nMIME-Version: 1.0\r\n\r\n'+raw)
                for part in message.iter_parts():
                    name=part.get_param('name',header='content-disposition')
                    data=part.get_payload(decode=True)
                    if part.get_filename(): uploads.append(data)
                    else: payload[name]=data.decode()
            elif content_type.startswith('application/json'):payload=json.loads(raw)
            else:payload={k:v[0] for k,v in parse_qs(raw.decode()).items()}
            method=self.path.rsplit('/',1)[-1]
            if messages is not None: messages.append((method,dict(payload)))
            result={'id':700,'is_bot':True,'first_name':'Fixture','username':'fixture_bot'} if method=='getMe' else {
                'message_id':900,'date':1,'chat':{'id':42,'type':'private'},'text':payload.get('text','')}
            if method=='sendDocument':result['document']={'file_id':'file','file_unique_id':'unique','file_name':'guide'}
            if method=='sendPhoto':result['photo']=[{'file_id':'file','file_unique_id':'unique','width':600,'height':800}]
            body=json.dumps({'ok':True,'result':result}).encode()
            self.send_response(200);self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Server)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield 'http://127.0.0.1:'+str(server.server_port)+'/bot',uploads
    finally:server.shutdown();server.server_close();thread.join(2)


def completed(content):
    return [{'type':'response.completed','response':{'status':'completed','output':content}}]


def text(value):
    return completed([{'type':'message','content':[{'type':'output_text','text':value if isinstance(value,str) else json.dumps(value)}]}])


def tool(name,arguments,identity):
    return completed([{'type':'function_call','call_id':identity,'name':name,'arguments':json.dumps(arguments)}])


@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('prompt,extension',[
    ('Find nearby activities using my calendar and create a one-page infographic .png.','.png'),
    ('カレンダーから近くの場所を調べ、一枚の案内画像 .png を作ってください。','.png'),
    ('تحقق من التقويم وأنشئ دليلًا من صفحة واحدة .pdf','.pdf'),
])
@pytest.mark.parametrize('failure',['truncated_json','unsealed_stream','schema_missing','profile_budget'])
def test_malformed_generation_recovers_and_delivers_real_rendered_file(tmp_path,monkeypatch,surface,prompt,extension,failure):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_permission_grant
    from nullion.artifacts import artifact_root_for_principal
    from nullion.config import NullionSettings,TelegramSettings
    from nullion.model_clients import CodexResponsesModelClient
    from nullion.messaging_turn_graph import PlatformChatRequest,run_platform_chat_request
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL,BoundaryPolicyRule,BoundaryKind,PolicyMode
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,register_calendar_plugin,_build_file_write_handler,_build_terminal_exec_handler
    from nullion import connections
    from nullion.connections import ConnectionRegistry,ProviderConnection,save_connection_registry
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    for name in ['NULLION_MEMORY_ENABLED','NULLION_DOCTOR_ENABLED','NULLION_TASK_DECOMPOSITION_ENABLED']:
        monkeypatch.setenv(name,'0')
    chat_store._store=None
    monkeypatch.setattr(connections,'_CONNECTIONS_PATH',tmp_path/'home'/'connections.json')
    save_connection_registry(ConnectionRegistry(connections=[ProviderConnection(connection_id='test-calendar',
        workspace_id='workspace_admin',provider_id='fixture_connector_provider',display_name='Calendar',
        provider_profile='https://calendar.example.test',permission_mode='read')]))
    cid='web:generation' if surface=='web' else 'telegram:42'
    root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    output=root/('nearby-guide'+extension);script=root/'render-guide.py'
    drawing=("from PIL import Image,ImageDraw\n"
        "im=Image.new('RGB',(600,800),'#f8f3e8');d=ImageDraw.Draw(im)\n"
        "d.rectangle((20,100,580,330),fill='#144d40');d.rectangle((20,360,580,590),fill='#b14b36')\n"
        "d.text((30,40),'Nearby weekend guide',fill='black')\n"
        "d.text((40,140),'Cedar Walk - 6 minutes',fill='white')\n"
        "d.text((40,400),'River Loop - 9 minutes',fill='white')\n"
        f"im.save({str(output)!r})\nprint({str(output)!r})\n")
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    source_reads=[]
    def source(start,end,limit):
        source_reads.append((start,end,limit))
        return [{'id':'base','summary':'Weekend base: Cedar Walk 6 minutes; River Loop 9 minutes',
            'start':{'dateTime':'2026-11-01T09:00:00-05:00'},'end':{'dateTime':'2026-11-01T12:00:00-05:00'}}]
    register_calendar_plugin(registry,calendar_lister=source)
    registry.register(ToolSpec('file_write','Create a local working file',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,3),
        _build_file_write_handler(tmp_path,allowed_roots=[tmp_path],include_principal_workspace=False))
    registry.register(ToolSpec('terminal_exec','Render the selected guide locally',ToolRiskLevel.HIGH,ToolSideEffectClass.DANGEROUS_EXEC,False,10),
        _build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.active_tool_registry=registry
    for name in ['calendar_list','file_write','terminal_exec']:
        runtime.store.add_permission_grant(create_permission_grant(approval_id='test-consent',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission='tool:'+name,granted_by='test',granted_at=datetime.now(UTC)))
    for kind in [BoundaryKind.FILESYSTEM_ACCESS,BoundaryKind.ACCOUNT_ACCESS]:
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('test-'+kind.value,OPERATOR_PERMISSION_PRINCIPAL,kind,PolicyMode.ALLOW,'*','test',datetime.now(UTC)))
    phase=0;reviews=[]
    if failure=='profile_budget':
        drawing += '# retained render implementation\n' * 160
        monkeypatch.setenv('NULLION_WEB_INTERACTIVE_FAST_MAX_TOKENS','768')
        monkeypatch.setenv('NULLION_MESSAGING_INTERACTIVE_FAST_MAX_TOKENS','768')
    def decide(body):
        nonlocal phase
        instructions=body['instructions']
        inputs=body['input']
        if 'Workflow action preflight.' in instructions:
            packet=json.loads(inputs[0]['content'])
            reviews.append(packet['proposed_action'])
            assert packet['proposed_action']['arguments'].get('command') or packet['proposed_action']['arguments'].get('content')
            return text({'decision':'proceed','issues':[]})
        if 'material-requirements planner' in instructions:
            return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[extension],
                'material_requirements':[{'requirement':'Read the calendar source','requirement_kind':'fact','evidence_subject_id':'base','eligible_evidence_tool_names':['calendar_list']},
                    {'requirement':'Deliver the rendered guide','requirement_kind':'deliverable','evidence_subject_id':'guide','eligible_evidence_tool_names':['terminal_exec']}]})
        if 'semantic completion reviewer' in instructions:
            packet=json.loads(inputs[0]['content']); assert output.is_file()
            return text({'disposition':'complete','unresolved_requirements':[],'retry_tool_names':[],
                'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,
                     'evidence_invocation_ids':[e['invocation_id'] for e in packet['tool_evidence']
                        if e['status']=='completed' and e['tool_name'] in r['eligible_evidence_tool_names']]}
                    for r in packet['material_requirement_contract']]})
        names={r['name'] for r in body.get('tools',[])}
        if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':extension,'confidence':1},'format')
        if not names:return text({})
        phase+=1
        if phase==1:return tool('request_tool_scope',{'capabilities':['connector','local_files','local_shell','image_generation'],
            'source_user_requested':True,'source_evidence':prompt,
            'tool_names':['calendar_list','file_write','terminal_exec'],'required_tool_names':['calendar_list','terminal_exec'],
            'artifact_extensions':[extension]},'scope')
        if phase==2:return tool('calendar_list',{'start':'2026-10-31T00:00:00-04:00','end':'2026-11-02T00:00:00-05:00','max':20},'read')
        if failure=='profile_budget' and phase==3:
            args=json.dumps({'path':str(script),'content':drawing})
            if body.get('max_output_tokens')==768:
                return [{'type':'response.incomplete','response':{'status':'incomplete',
                    'incomplete_details':{'reason':'max_output_tokens'},'output':[{'type':'function_call',
                    'call_id':'budget-write','name':'file_write','arguments':args[:768]}]}}]
            return tool('file_write',{'path':str(script),'content':drawing},'write')
        if failure=='profile_budget' and phase==4:
            return tool('terminal_exec',{'command':shlex.quote(sys.executable)+' '+shlex.quote(str(script))},'render')
        if failure=='profile_budget' and phase==5:
            return text('Cedar Walk and River Loop are in the guide.\nMEDIA:'+str(output))
        if phase==3:
            raw='{"command":"from PIL import Image; draw.text(' if failure=='truncated_json' else '{}'
            if failure=='unsealed_stream':return [{'type':'response.output_item.added','output_index':0,'item':{
                'type':'function_call','call_id':'bad','name':'terminal_exec','arguments':raw}}]
            return completed([{'type':'function_call','call_id':'bad','name':'terminal_exec','arguments':raw}])
        if phase==4:
            assert 'invalid_tool_arguments' in json.dumps(inputs), 'Malformed call was not routed through protocol repair'
            assert not output.exists() and not reviews, 'Malformed call reached review or execution'
            return tool('file_write',{'path':str(script),'content':drawing},'write')
        if phase==5:return tool('terminal_exec',{'command':shlex.quote(sys.executable)+' '+shlex.quote(str(script))},'render')
        assert phase==6, 'Recovery repeated research or generation'
        return text('Cedar Walk and River Loop are in the guide.\nMEDIA:'+str(output))
    with model_boundary(decide) as (endpoint,bodies):
        model=CodexResponsesModelClient(api_key='synthetic',account_id='synthetic',model='fixture',_ENDPOINT=endpoint)
        orchestrator=AgentOrchestrator(model_client=model)
        if failure=='profile_budget':
            from nullion.chat_operator import _orchestrator_with_interactive_fast_profile
            orchestrator=_orchestrator_with_interactive_fast_profile(orchestrator,enabled=True,tool_registry=registry)
            assert orchestrator.model_client.max_tokens==768
            assert orchestrator.workflow_model_client is model
        if surface=='web':
            with TestClient(web_app.create_app(runtime,orchestrator,registry)) as client:
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
                assert response.status_code==200,response.text
                payload=response.json(); assert len(payload['artifacts'])==1,payload
                download=client.get(payload['artifacts'][0]['url']); assert download.status_code==200
                data=download.content
        else:
            settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:synthetic',operator_chat_id='42',chat_enabled=True))
            reply=run_platform_chat_request(runtime,PlatformChatRequest(platform='telegram',text=prompt,conversation_id=cid,
                chat_id='42',settings=settings,model_client=model,agent_orchestrator=orchestrator,append_activity_trace=False))
            assert len(reply.artifact_paths)==1,reply.text
            from telegram import Bot,Message
            from nullion.telegram_app import _deliver_reply
            from nullion.messaging_adapters import delivery_contract_for_turn
            from nullion.messaging_adapters import prepare_reply_for_platform_delivery
            contract=delivery_contract_for_turn(None,reply=reply.text,artifact_paths=reply.artifact_paths,
                requires_attachment_delivery=bool(reply.artifact_paths))
            prepared=prepare_reply_for_platform_delivery(reply.text,principal_id=cid,delivery_contract=contract,
                artifact_paths=reply.artifact_paths,tool_results=reply.tool_results)
            assert prepared.attachments,prepared
            with upload_boundary() as (bot_url,uploads):
                async def deliver():
                    async with Bot('123456:synthetic',base_url=bot_url) as bot:
                        message=Message.de_json({'message_id':1,'date':1,'chat':{'id':42,'type':'private'},'text':prompt},bot)
                        await _deliver_reply(message,reply.text,principal_id=cid,
                            delivery_contract=contract,
                            artifact_paths=reply.artifact_paths,tool_results=reply.tool_results)
                asyncio.run(deliver())
            # Bot API boundary receives multipart upload bytes, not just a receipt.
            assert len(uploads)==1
            data=uploads[0]
            assert data==output.read_bytes()
        assert len(source_reads)==1 and phase==(5 if failure=='profile_budget' else 6)
        assert len(reviews)==2
        assert output.is_file()
        if extension=='.png':
            image=Image.open(io.BytesIO(data));assert image.size==(600,800)
            assert image.getpixel((25,105))==(20,77,64)
            assert image.getpixel((25,365))==(177,75,54)
        else:
            from pypdf import PdfReader
            document=PdfReader(io.BytesIO(data));assert len(document.pages)==1
            assert list(document.pages[0].images)
        events=runtime.store.list_conversation_events(cid)
        malformed=[e for e in events if e.get('event_type')=='conversation.tool_timing' and e.get('failure_reason')=='invalid_tool_arguments']
        assert len(malformed)==(0 if failure=='profile_budget' else 1)
        if malformed: assert malformed[0].get('recovery_kind')=='correct_arguments'
        if failure=='profile_budget':
            timings=[e for e in events if e.get('event_type')=='conversation.model_timing']
            assert any(e.get('model_profile')=='workflow' for e in timings)
            assert all(e.get('max_output_tokens')!=768 for e in timings if e.get('model_profile')=='workflow')
        turns=[e for e in events if e.get('event_type')=='conversation.chat_turn']
        assert turns and 'Image generation is not configured' not in turns[-1]['assistant_reply']
        from nullion.messaging_turn_graph import _coerce_stored_tool_results
        from nullion.tool_repair_contract import tool_repair_kind
        failures=[r for r in _coerce_stored_tool_results(turns[-1]['tool_results'])
            if r.output.get('reason')=='invalid_tool_arguments']
        assert len(failures)==(0 if failure=='profile_budget' else 1)
        if failures: assert tool_repair_kind(failures[0])=='correct_arguments'
        model.close()
