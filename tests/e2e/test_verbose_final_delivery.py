"""Progress collection must respect the setting at real Web delivery."""
import json
import os

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker browser E2E only')


@pytest.mark.parametrize('enabled',[False,True])
def test_web_tool_reply_obeys_session_verbose_setting(tmp_path,monkeypatch,enabled):
    from playwright.sync_api import sync_playwright,expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_core_tool_registry
    from nullion.web_app import create_app
    from tests.workflow_provider_protocol import workflow_provider_reply
    from tests.e2e.test_web_surface_contracts import _UvicornThread,_free_port
    from nullion import chat_store
    monkeypatch.setenv('HOME',str(tmp_path))
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'absent.env'))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    # Verify that a session setting wins over the opposite global default.
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED','false' if enabled else 'true')
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','true')
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    chat_store._store=None
    source=tmp_path/'source.txt';source.write_text('42\n')
    registry=create_core_tool_registry(allowed_roots=[tmp_path])
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    from datetime import UTC,datetime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL,BoundaryPolicyRule,BoundaryKind,PolicyMode
    runtime.store.add_permission_grant(create_permission_grant(approval_id='fixture-read',
        principal_id=OPERATOR_PERMISSION_PRINCIPAL,permission='tool:file_read',
        granted_by='fixture',granted_at=datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    runtime.checkpoint(force=True)
    class Provider:
        read_requested=False
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            names={row['name'] for row in kw.get('tools',[])}
            if 'file_read' not in names:
                call={'name':'request_tool_scope','input':{'capabilities':['local_files'],
                    'tool_names':['file_read'],'required_tool_names':['file_read'],
                    'artifact_extensions':[],'source_artifact_delivery_requested':False}}
            elif not self.read_requested:
                self.read_requested=True
                call={'name':'file_read','input':{'path':str(source)}}
            else:
                return {'stop_reason':'end_turn','content':[{'type':'text','text':'Lectura completada: 42.'}]}
            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read-step',**call}]}
    model=Provider();runtime.model_client=model
    app=create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)
    cid='web:verbose-delivery'
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        try:
            context=browser.new_context()
            context.add_init_script("localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+");")
            setting=context.request.post(server.base_url+'/api/chat',data={
                'text':'/verbose '+('on' if enabled else 'off'),'conversation_id':cid})
            assert setting.ok,setting.text()
            page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
            page.goto(server.base_url,wait_until='domcontentloaded')
            box=page.locator('#user-input');expect(box).to_be_enabled(timeout=15000)
            box.fill(f'Lee {source} y responde con el valor. No cambies el archivo ni envíes adjuntos.')
            # A streamed text chunk is not a persisted terminal reply.
            with page.expect_response(lambda response: response.request.method=='POST'
                    and response.url.endswith('/message')
                    and (response.request.post_data_json or {}).get('role')=='bot'
                    and ((response.request.post_data_json or {}).get('metadata') or {}).get('history_message_kind')=='terminal_reply',timeout=20000):
                box.press('Enter')
            bubble=page.locator('#messages .msg.bot .bubble').filter(has_text='Lectura completada: 42.')
            expect(bubble).to_have_count(1,timeout=20000)
            raw=bubble.get_attribute('data-raw-text')
            assert ('Activity' in raw) is enabled,raw
            assert ('file_read' in raw) is enabled,raw
            assert str(source) not in raw and source.name not in raw,raw
            history=context.request.get(server.base_url+'/api/chat/history/'+cid).json()
            saved=[row for row in history['messages'] if row.get('role')=='bot'
                   and 'Lectura completada: 42.' in row.get('text','')]
            assert len(saved)==1 and saved[0]['text'].strip()==raw.strip()
            turns=[row for row in runtime.store.list_conversation_events(cid)
                   if row.get('event_type')=='conversation.chat_turn']
            assert any(result['tool_name']=='file_read' and result['status']=='completed'
                       for turn in turns for result in turn.get('tool_results',[])),turns
            assert source.read_text()=='42\n' and not saved[0].get('artifacts')
            page.reload();expect(bubble).to_have_count(1)
            assert ('Activity' in bubble.get_attribute('data-raw-text')) is enabled
            assert source.name not in bubble.get_attribute('data-raw-text')
            assert not errors,errors
        finally:browser.close()
