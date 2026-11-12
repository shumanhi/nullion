"""The real chat route exports an edited existing file once, under its name."""
import json
import os
import time
from pathlib import Path
from urllib.parse import unquote

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('suffix', ['.csv', '.html', '.svg'])
@pytest.mark.parametrize('command', [False, True])
@pytest.mark.parametrize('transport', ['http', 'websocket'])
def test_existing_file_export_keeps_requested_name_source_edit_and_download_bytes(tmp_path, monkeypatch, suffix, command, transport):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_file_write_handler
    runtime = setup_runtime(tmp_path, monkeypatch)
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-file-consent',
        principal_id=OPERATOR_PERMISSION_PRINCIPAL, permission='tool:file_write', granted_by='fixture-user'))
    home = tmp_path/'home';home.mkdir(exist_ok=True)
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from datetime import datetime, UTC
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-file-access',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture-user',datetime.now(UTC)))
    monkeypatch.chdir(home)
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP', '0')
    source = home/('比較 final' + suffix);source.write_bytes(b'old version')
    content = {'.csv': 'Region,Value\nNorth,12\nSouth,19\n',
        '.html': '<!doctype html><html><body><h1>値 31</h1><p>Verified report contents.</p></body></html>',
        '.svg': '<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><text x="1" y="20">31</text></svg>'}.get(suffix)
    expected=content.encode()
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    writer=_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]);writes=[]
    def write(invocation):
        result=writer(invocation);writes.append(result);return result
    registry.register(ToolSpec('file_write','Write the selected workspace file',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,20),write)
    phase=0
    class Provider:
        def create(self, **kw):
            nonlocal phase
            protocol=workflow_provider_reply(kw)
            if protocol is not None:return protocol
            phase+=1
            if phase==1:return tool_response('request_tool_scope', {'capabilities':['local_files'],
                'tool_names':['file_write'],'artifact_extensions':[suffix]})
            if phase==2:return tool_response('file_write', {'path':str(source),'content':content})
            assert phase==3,'A native naming copy must not require another write'
            return text_response('Archivo actualizado y adjunto.\nMEDIA:'+writes[-1].output['artifact_path'])
    cid='web:named-export'
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=Provider()),registry=registry)) as api:
        payload={'text':('/chat ' if command else '')+'Actualiza '+str(source)+' y adjunta únicamente ese archivo.',
            'conversation_id':cid,'stream':False}
        if transport=='http':
            response=api.post('/api/chat',json=payload)
            assert response.status_code==200,response.text
            body=response.json()
        else:
            with api.websocket_connect('/ws/chat') as wire:
                wire.send_json(payload)
                while True:
                    body=wire.receive_json()
                    assert body.get('type')!='error',body
                    if body.get('type')=='done':break
        items=body.get('artifacts',[])
        saved=next(e for e in reversed(runtime.store.list_conversation_events(cid))
            if e.get('event_type')=='conversation.chat_turn')
        if not items:
            print('EXPORT_DEBUG '+json.dumps({'reply':body,'saved':saved,'phase':phase},ensure_ascii=False,default=str))
        assert len(items)==1 and items[0]['name']==source.name,{'reply':body,'tools':saved.get('tool_results')}
        downloaded=api.get(items[0]['url'])
        assert downloaded.status_code==200 and downloaded.content==expected
        assert source.name in unquote(downloaded.headers['content-disposition'])
        assert source.read_bytes()==expected
        writes=[r for r in saved['tool_results'] if r['tool_name']=='file_write']
        assert len(writes)==1 and writes[0]['status']=='completed'
        descriptors=writes[0]['output']['artifact_descriptors']
        assert len([d for d in descriptors if d['role']=='deliverable'])==1
        assert len([d for d in descriptors if d['role']=='source' and Path(d['path'])==source])==1


@pytest.mark.parametrize('transport',['http','websocket'])
def test_shared_command_path_keeps_static_commands_off_the_model_path(tmp_path,monkeypatch,transport):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    runtime=setup_runtime(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    calls=[]
    class SlowOptionalModel:
        def create(self,**kwargs):
            calls.append(kwargs);time.sleep(3)
            raise AssertionError('A static command invoked the optional model')
    samples=[]
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=SlowOptionalModel()),registry=ToolRegistry())) as api:
        for index,prompt in enumerate(['/status','/verbose status','/status']):
            payload={'text':prompt,'conversation_id':'web:command-latency-'+str(index),'stream':False}
            start=time.monotonic()
            if transport=='http':
                reply=api.post('/api/chat',json=payload)
                assert reply.status_code==200
                body=reply.json()
            else:
                with api.websocket_connect('/ws/chat') as wire:
                    wire.send_json(payload)
                    while True:
                        body=wire.receive_json()
                        assert body.get('type')!='error',body
                        if body.get('type')=='done':break
            samples.append(time.monotonic()-start)
            assert body.get('text') and not body.get('artifacts'),body
    assert not calls
    assert sum(samples)/len(samples)<1 and max(samples)<2,samples
