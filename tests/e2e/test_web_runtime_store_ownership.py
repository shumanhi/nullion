"""Status polling cannot replace an in-flight Web turn's durable state store."""
from tests.workflow_provider_protocol import workflow_provider_reply
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import os
import threading

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('transport',['http','websocket'])
def test_web_chat_pins_runtime_store_while_status_sync_observes_external_write(tmp_path,monkeypatch,transport):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_permission_grant
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');conversation='web:store-owner'
    root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True);path=root/'current.csv'
    entered=threading.Event();release=threading.Event()
    class Provider:
        phase=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names={row['name'] for row in kw.get('tools',[])}
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':args}]}
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.csv','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],
                'required_tool_names':['file_write'],'artifact_extensions':['.csv']})
            if self.phase==2:return tool('file_write',{'path':str(path),'content':'Name,Value\nCurrent,31\n'})
            assert self.phase==3
            entered.set();assert release.wait(20),'The test did not release the provider'
            return text('The current file is attached.\nMEDIA:'+str(path))
    model=Provider();runtime.model_client=model;registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_write','Write CSV',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),
        _build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    app=web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)
    payload={'text':'Create one CSV with Current 31.','conversation_id':conversation,'turn_id':'current-turn'}
    with TestClient(app) as client:
        assert client.get('/api/status?full=1').status_code==200
        def submit():
            if transport=='http':
                response=client.post('/api/chat',json=payload);assert response.status_code==200,response.text
                return response.json()
            with client.websocket_connect('/ws/chat') as socket:
                socket.send_json(payload)
                while True:
                    frame=socket.receive_json()
                    assert frame.get('type')!='error',frame
                    if frame.get('type')=='done':return frame
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(submit)
            try:
                assert entered.wait(10),'The in-flight request did not reach the provider'
                owner=runtime.store;active=app.state.nullion_runtime_turns_active()
                writer=bootstrap_persistent_runtime(tmp_path/'runtime.db')
                grant=create_permission_grant(approval_id='external-fixture',principal_id=OPERATOR_PERMISSION_PRINCIPAL,
                    permission='tool:fixture_only',granted_by='fixture',granted_at=datetime.now(UTC))
                writer.store.add_permission_grant(grant);writer.checkpoint(force=True)
                assert client.get('/api/status?full=1').status_code==200
                stable=runtime.store is owner
            finally:release.set()
            response=future.result(timeout=15)
        assert active and stable,('Status synchronization replaced the active chat store',active,stable)
        assert len(response.get('artifacts',[]))==1,response
        assert client.get(response['artifacts'][0]['url']).content==path.read_bytes()
        assert not app.state.nullion_runtime_turns_active()
        assert client.get('/api/status?full=1').status_code==200
        assert runtime.store.get_permission_grant(grant.grant_id) is not None
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    events=restored.store.list_conversation_events(conversation)
    model_events=[row for row in events if row.get('event_type')=='conversation.model_timing']
    assert [row['iteration'] for row in model_events]==[1,2,3],model_events
    assert any(row.get('event_type')=='conversation.chat_turn' and row.get('artifact_paths') for row in events)


@pytest.mark.parametrize('slow_phase',['reload','maintenance'])
def test_dashboard_checkpoint_read_keeps_health_requests_responsive(tmp_path,monkeypatch,slow_phase):
    """A delayed real SQLite reload must not occupy the ASGI event loop."""
    import time
    import statistics
    from concurrent.futures import TimeoutError as FutureTimeout
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,runtime_persistence
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));chat_store._store=None
    monkeypatch.setenv('NULLION_WEB_STATUS_CACHE_TTL_S','0')
    monkeypatch.setenv('NULLION_WEB_STATUS_MAINTENANCE_INTERVAL_S','1')
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    app=web_app.create_app(runtime,orchestrator=None,registry=ToolRegistry())
    original=runtime_persistence._load_runtime_store_sqlite_collections
    entered=threading.Event();release=threading.Event()
    def delayed_read(*args,**kwargs):
        entered.set();assert release.wait(4),'The status read was not released'
        return original(*args,**kwargs)
    if slow_phase=='reload':
        monkeypatch.setattr(runtime_persistence,'_load_runtime_store_sqlite_collections',delayed_read)
    else:
        runtime_type=type(runtime);original_maintenance=runtime_type.reconcile_effectively_approved_pending_approvals
        def delayed_maintenance(owner,*args,**kwargs):
            if owner is runtime:
                entered.set();assert release.wait(4),'Status maintenance was not released'
            return original_maintenance(owner,*args,**kwargs)
        monkeypatch.setattr(runtime_type,'reconcile_effectively_approved_pending_approvals',delayed_maintenance)
    samples=[]
    with TestClient(app) as client,ThreadPoolExecutor(max_workers=2) as pool:
        for index in range(3):
            if slow_phase=='maintenance' and index:time.sleep(1.05)
            entered.clear();release.clear()
            writer=bootstrap_persistent_runtime(tmp_path/'runtime.db')
            grant=create_permission_grant(approval_id='external-'+str(index),principal_id=OPERATOR_PERMISSION_PRINCIPAL,
                permission='tool:fixture_'+str(index),granted_by='fixture',granted_at=datetime.now(UTC))
            writer.store.add_permission_grant(grant);writer.checkpoint(force=True)
            refresh=pool.submit(client.get,'/api/status?full=1')
            try:
                assert entered.wait(2)
                started=time.monotonic();health=pool.submit(client.get,'/api/health')
                try:response=health.result(timeout=.75)
                except FutureTimeout:pytest.fail('The slow dashboard read blocked the ASGI health request')
                samples.append(time.monotonic()-started);assert response.status_code==200
            finally:release.set()
            assert refresh.result(timeout=3).status_code==200
            assert runtime.store.get_permission_grant(grant.grant_id)
    assert statistics.mean(samples)<.15 and max(samples)<.35,samples


def test_cold_settings_inventory_does_not_block_chat_health(tmp_path,monkeypatch):
    """Settings perform installed dependency probes; other Web routes stay live."""
    import time
    import statistics
    from concurrent.futures import TimeoutError as FutureTimeout
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,capability_dependencies
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    entered=threading.Event();release=threading.Event()
    original=capability_dependencies.dependency_status_payloads
    def slow_inventory(*args,**kwargs):
        entered.set();assert release.wait(4),'Settings inventory was not released'
        return original(*args,**kwargs)
    monkeypatch.setattr(capability_dependencies,'dependency_status_payloads',slow_inventory)
    app=web_app.create_app(runtime,orchestrator=None,registry=ToolRegistry())
    samples=[]
    with TestClient(app) as client,ThreadPoolExecutor(max_workers=2) as pool:
        for _ in range(3):
            entered.clear();release.clear();settings=pool.submit(client.get,'/api/config')
            try:
                assert entered.wait(2)
                started=time.monotonic();health=pool.submit(client.get,'/api/health')
                try:response=health.result(timeout=.75)
                except FutureTimeout:pytest.fail('Cold settings inventory blocked other Web requests')
                samples.append(time.monotonic()-started);assert response.status_code==200
            finally:release.set()
            assert settings.result(timeout=5).status_code==200
    assert statistics.mean(samples)<.15 and max(samples)<.35,samples
