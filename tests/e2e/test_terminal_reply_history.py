"""Real Web composer and encrypted history reconcile one terminal publication."""
import json
import os
from pathlib import Path

import pytest

from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port
from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker Web boundary')


@pytest.mark.parametrize('hold_restore',[False,True])
@pytest.mark.parametrize('prompt',['資料を作成してください。','Crea el archivo solicitado.'])
def test_file_only_reply_has_one_history_receipt_and_keeps_first_user_message(tmp_path,monkeypatch,prompt,hold_restore):
    from playwright.sync_api import sync_playwright,expect
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    runtime=setup_runtime(tmp_path,monkeypatch)
    cid='web:terminal-owned';path=artifact_root_for_principal(cid)/'native.csv'
    registry=ToolRegistry(filesystem_allowed_roots=(tmp_path,));writes=[]
    writer=_build_file_write_handler(allowed_roots=(tmp_path,))
    def write(inv):writes.append(inv);return writer(inv)
    registry.register(ToolSpec('file_write','Create selected CSV',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,
        False,5,produced_artifact_extensions=('.csv',)),write)
    class Provider:
        phase=0
        def create(self,**kw):
            auxiliary=workflow_provider_reply(kw,completion=True)
            if auxiliary is not None:return auxiliary
            names={row['name'] for row in kw.get('tools',[])}
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format',{'extension':'.csv','confidence':1})
            if not names:return text_response('{}')
            self.phase+=1
            if self.phase==1:return tool_response('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_write'],'required_tool_names':['file_write'],'artifact_extensions':['.csv']})
            if self.phase==2:return tool_response('file_write',{'path':str(path),'content':'Name,Value\nCedar,37\n'})
            assert self.phase==3
            return text_response('MEDIA:'+str(path))
    provider=Provider();runtime.model_client=provider
    app=web_app.create_app(runtime,AgentOrchestrator(model_client=provider),registry)
    with _UvicornThread(app,_free_port()) as server,sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True)
        context=browser.new_context(accept_downloads=True)
        context.add_init_script("localStorage.setItem('nullion_conv_id',"+json.dumps(cid)+
            ");localStorage.setItem('nullion_chat_restore_suppressed','true');")
        page=context.new_page();pending=[]
        restore_route='**/api/chat/history/web*terminal-owned'
        if hold_restore:
            page.route(restore_route,lambda route:pending.append(route) if not pending else route.continue_())
        page.goto(server.base_url,wait_until='domcontentloaded')
        page.locator('#user-input').fill(prompt)
        with page.expect_response(lambda response: response.request.method=='POST'
                and response.url.endswith('/message')
                and (response.request.post_data_json or {}).get('role')=='bot'
                and ((response.request.post_data_json or {}).get('metadata') or {}).get('history_message_kind')=='terminal_reply',timeout=20000):
            page.locator('#user-input').press('Enter')
        expect(page.locator('#messages a[download]')).to_have_count(1,timeout=20000)
        def history():
            response=context.request.get(server.base_url+'/api/chat/history/'+cid)
            assert response.ok
            return response.json()['messages']
        page.wait_for_function("() => document.querySelector('#messages .msg.bot .bubble[data-turn-id]') !== null")
        if hold_restore:
            assert pending,'Startup history dependency was not exercised'
            for route in pending:route.continue_()
            page.unroute(restore_route)
        rows=history()
        users=[row for row in rows if row['role']=='user'];bots=[row for row in rows if row['role']=='bot']
        assert len(users)==1 and users[0]['text']==prompt,rows
        assert len(bots)==1 and len(bots[0]['artifacts'])==1,rows
        turn_id=bots[0]['metadata']['runtime_turn_id']
        assert bots[0]['metadata']['history_message_kind']=='terminal_reply'
        # Retries with another rendered caption use the same logical terminal,
        # while ordinary part/follow-up messages retain their own rows.
        url=server.base_url+'/api/chat/history/'+cid+'/message'
        for text in ('資料を添付しました。','Archivo listo.'):
            assert context.request.post(url,data={'role':'bot','text':text,'metadata':{
                **bots[0]['metadata'],'runtime_turn_id':turn_id}}).ok
        assert len([r for r in history() if r['role']=='bot'])==1
        synced=context.request.get(server.base_url+'/api/chat/history/'+cid+'?sync_runtime=1').json()['messages']
        assert len([r for r in synced if r['role']=='bot'])==1,synced
        page.reload(wait_until='domcontentloaded')
        expect(page.locator('#messages .msg.bot .bubble[data-turn-id]')).to_have_count(1,timeout=10000)
        expect(page.locator('#messages a[download]')).to_have_count(1)
        artifact=bots[0]['artifacts'][0]
        assert context.request.get(server.base_url+artifact['url']).body()==b'Name,Value\nCedar,37\n'
        assert len(writes)==1
        browser.close()
