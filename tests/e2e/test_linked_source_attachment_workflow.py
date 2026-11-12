"""Real chat route preserves image bytes across a structured follow-up."""
from tests.workflow_provider_protocol import workflow_provider_reply
import base64
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real API/store')

@pytest.mark.parametrize('followup_transport',['http','websocket'])
@pytest.mark.parametrize('followup',['Use this source link.','Usa este enlace.','このリンクで確認してください。'])
def test_linked_followup_keeps_uploaded_image_and_persists_source_descriptor(tmp_path,monkeypatch,followup,followup_transport,record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    image=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jL1sAAAAASUVORK5CYII=')
    entered=threading.Event();release=threading.Event()
    class Provider:
        images=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            names={row['name'] for row in kw.get('tools',[])};system=str(kw.get('system') or '')
            if 'select_attachment_format' in names:return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'format','name':'select_attachment_format','input':{'extension':None,'confidence':1}}]}
            if 'relationship' in system:
                release.set()
                return text(json.dumps({'relationship':'follow_up','effect':'revise','confidence':1,'target_index':0,'target_is_explicit':True}))
            if not names:return text('{}')
            images=[block for message in kw['messages'] for block in (message.get('content') if isinstance(message.get('content'),list) else []) if block.get('type')=='image']
            self.images.append(images)
            if len(self.images)==1:
                entered.set();assert release.wait(10),'No concurrent follow-up was registered'
            assert images and any(base64.b64decode(b['source']['data'])==image for b in images),kw['messages'][-1]
            return text('Image source retained.')
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=ToolRegistry())) as client:
        upload=client.post('/api/upload',files={'file':('source.png',image,'image/png')});assert upload.status_code==200,upload.text
        data=upload.json();attachment={'path':data['path'],'name':'source.png','media_type':'image/png'}
        with ThreadPoolExecutor(max_workers=2) as pool:
            future=pool.submit(client.post,'/api/chat',json={'text':'Identify the attached source.','conversation_id':'web:source','turn_id':'first','attachments':[attachment]})
            assert entered.wait(10),'Original image request did not reach the provider'
            started=time.perf_counter()
            try:
                payload={'text':followup+' https://venue.example/event','conversation_id':'web:source','turn_id':'second'}
                if followup_transport=='http':
                    second=client.post('/api/chat',json=payload)
                    assert second.status_code==200,second.text
                    delivered=second.json()['text']
                else:
                    # The first request can use HTTP while the WebSocket is
                    # connecting. Its amendment must share the same admission
                    # state after the socket becomes available.
                    with client.websocket_connect('/ws/chat') as socket:
                        socket.send_json(payload);chunks=[]
                        while True:
                            frame=socket.receive_json()
                            if frame.get('turn_id')!='second':continue
                            assert frame.get('type')!='error',frame
                            if frame.get('type')=='chunk':chunks.append(frame.get('text',''))
                            if frame.get('type')=='done':
                                assert not frame.get('superseded'),frame
                                delivered=frame.get('text') or ''.join(chunks)
                                break
            finally:release.set()
            first=future.result(timeout=10)
        elapsed=time.perf_counter()-started
        record_property('amendment_seconds',elapsed)
        assert elapsed<2.0,elapsed
        assert first.status_code==200,first.text
        assert 'Image source retained' in delivered,delivered
        assert len(model.images)==2
        assert first.json().get('superseded') is True,first.text
    events=runtime.store.list_recent_conversation_events_after_reset('web:source',event_type='conversation.source_attachments',limit=10)
    assert len(events)==1 and events[0]['turn_id']=='first'
    runtime.checkpoint()
    restarted=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert restarted.store.list_recent_conversation_events_after_reset('web:source',event_type='conversation.source_attachments',limit=10)==events
