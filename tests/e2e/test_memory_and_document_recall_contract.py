"""Real chat, SQLite and download boundaries; only provider responses are scripted."""
from datetime import UTC, datetime
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


def response(text):
    return {'stop_reason':'end_turn','content':[{'type':'text','text':text}]}


def tool(name, args):
    return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}


class ScriptedProvider:
    def auxiliary(self, kwargs):
        names={row['name'] for row in kwargs.get('tools',[])}
        system=str(kwargs.get('system') or '')
        if 'select_attachment_format' in names:
            return tool('select_attachment_format',{'extension':None,'confidence':1,'embedded_media_extensions':[]})
        if 'relationship' in system and 'target_index' in system:
            return response(json.dumps({'relationship':'independent','effect':'none','confidence':1,'target_index':None}))
        if not names and system:
            return response('{}')
        return None


def setup(tmp_path,monkeypatch):
    from nullion import chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,register_workspace_plugin,create_plugin_tool_registry
    monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'storage'))
    monkeypatch.setenv('NULLION_KEY_STORAGE','file')
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED','0')
    chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    registry=register_workspace_plugin(create_plugin_tool_registry(workspace_root=tmp_path),workspace_root=tmp_path)
    runtime.active_tool_registry=registry
    return runtime,registry


@pytest.mark.parametrize('prompt',['For the Cedar trip, do you have the PDF?','杉の旅行のPDFはありますか？','¿Tienes el PDF del viaje Cedar?'])
def test_recall_older_telegram_document_through_chat_api_and_download(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from pypdf import PdfWriter
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    runtime,registry=setup(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0') # File history survives fact-memory settings.
    artifact=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'cedar-itinerary.pdf'
    artifact.parent.mkdir(parents=True,exist_ok=True)
    pdf=PdfWriter();pdf.add_blank_page(width=200,height=200);pdf.write(artifact)
    for i in range(40):(tmp_path/f'internal-transform-{i}.py').write_text('unrelated software')
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'telegram:2222',
        'workspace_id':'workspace_admin','created_at':'2026-10-01T10:00:00Z','turn_id':'original-document',
        'user_message':'Cedar weekend itinerary','assistant_reply':'Trip details. '*160+'\nMEDIA:'+str(artifact),
        'tool_results':[{'tool_name':'artifact_delivery','status':'completed','output':{'artifact_descriptors':[{'path':str(artifact),'role':'deliverable'}]}}]})
    for i in range(30):
        runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'web:other',
            'workspace_id':'workspace_admin','created_at':f'2026-10-10T10:{i:02}:00Z','turn_id':f'distractor-{i}',
            'user_message':'Unrelated measurements','assistant_reply':'Recorded dimensions.'})
    runtime.checkpoint(force=True)
    class Recall(ScriptedProvider):
        steps=0
        deliver=True
        history_requested=False
        def create(self,**kwargs):
            auxiliary=self.auxiliary(kwargs)
            if auxiliary:return auxiliary
            if self.steps>=5:
                if not getattr(self,'early_end',False):
                    self.early_end=True
                    return response('先の会話を確認しています。')
                return response('Yes — **cedar-itinerary.pdf** is available from the earlier trip report.' + ('\nMEDIA:'+str(artifact) if self.deliver else ''))
            names={row['name'] for row in kwargs.get('tools',[])}
            if 'file_search' not in names and 'chat_history_search' in names and not self.history_requested:
                self.history_requested=True
                return tool('chat_history_search',{'query':'Cedar itinerary','limit':5,'artifact_extensions':['.pdf']})
            if 'file_search' not in names:
                return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_search'],'artifact_extensions':['.pdf'],'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt})
            assert 'chat_history_search' in names # Read companion adds no scope/classifier round trip.
            self.steps+=1
            if self.steps==1:return tool('file_search',{'pattern':'transform'})
            if self.steps==2:return tool('chat_history_search',{'query':'Cedar itinerary','limit':5,'artifact_extensions':['.pdf']})
            history=json.dumps(kwargs['messages'])
            assert 'original-document' in history and 'artifact_refs' in history and 'cedar-itinerary.pdf' in history
            if self.steps==3:return tool('file_search',{'pattern':'internal-transform','root':str(tmp_path)})
            if self.steps==4:return tool('file_search',{'pattern':'nonexistent-candidate'})
            if self.steps==5:return tool('file_read',{'path':str(artifact.parent)})
            return response('Yes — **cedar-itinerary.pdf** is available from the earlier trip report.' + ('\nMEDIA:'+str(artifact) if self.deliver else ''))
    model=Recall()
    app=create_app(runtime,AgentOrchestrator(model_client=model),registry)
    with TestClient(app) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:recall','turn_id':'recall-document'})
        assert result.status_code==200,result.text
        payload=result.json()
        assert 'cedar-itinerary.pdf' in json.dumps(payload) and 'transform' not in payload['text'],payload
        assert 'I found 40 files' not in payload['text'],payload
        assert payload.get('artifacts'),payload
        ref=payload['artifacts'][0]
        assert ref['name']==artifact.name,ref
        url=ref.get('url') or ref.get('download_url')
        assert url,ref
        download=client.get(url)
        assert download.status_code==200 and download.content==artifact.read_bytes()
        recovered=client.get('/api/chat/history/web:recall',params={'turn_ids':json.dumps(['recall-document'])})
        assert recovered.status_code==200,recovered.text
        terminal=recovered.json()['terminal_replies'][0]
        assert 'MEDIA:' not in terminal['text'] and str(artifact.parent) not in terminal['text'],terminal
        assert terminal['artifacts'][0]['name']==artifact.name
    turns=[event for event in runtime.store.list_conversation_events('web:recall') if event.get('event_type')=='conversation.chat_turn']
    assert turns and any(row['tool_name']=='chat_history_search' for row in turns[-1]['tool_results'])
    assert len([row for row in turns[-1]['tool_results'] if row['tool_name']=='request_tool_scope'])<=1


@pytest.mark.parametrize('prompt',['Remember: confirm before sending emails.','メールを送る前に確認することを覚えてください','Recuerda: confirma antes de enviar correos.'])
def test_telegram_memory_save_then_web_recall_after_restart(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import chat_operator,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings,TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    runtime,registry=setup(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','1')
    from nullion import turn_context_policy
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass
    monkeypatch.setattr(turn_context_policy,'_active_connector_provider_context',lambda:[{
        'provider_id':'example','display_name':'Example connector','active_app_ids':['google-mail','google-calendar'],
        'structured_tools':['email_search','calendar_list']}])
    for name in ['email_search','calendar_list']:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2,
                                  capability_tags=('connector','account_read')),lambda invocation:None)
    class Memory(ScriptedProvider):
        def __init__(self,save):self.save=save;self.steps=0
        def create(self,**kwargs):
            auxiliary=self.auxiliary(kwargs)
            if auxiliary:return auxiliary
            names={row['name'] for row in kwargs.get('tools',[])}
            wanted='memory_save' if self.save else 'memory_search'
            if wanted not in names:
                return tool('request_tool_scope',{'capabilities':['conversation_history'] if self.save else ['connector'],'tool_names':[wanted],'source_user_requested':True})
            self.steps+=1
            if self.steps==1:
                return tool(wanted,{'key':'mail_consent','value':'Confirm before sending emails','kind':'preference','source_quote':prompt}
                            if self.save else {'query':'mail_consent'})
            history=json.dumps(kwargs['messages'])
            if self.save:assert 'persisted' in history and 'true' in history
            else:assert 'Confirm before sending emails' in history
            return response('Saved your email confirmation preference.' if self.save else 'I will confirm before sending emails.')
    model=Memory(True)
    settings=NullionSettings(telegram=TelegramSettings(bot_token='123456:abcdefghijklmnopqrstuv',operator_chat_id='2222',chat_enabled=True))
    reply=chat_operator.handle_chat_operator_message(runtime,prompt,chat_id='2222',settings=settings,request_id='save-memory',
        model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model),append_activity_trace=False,allow_mini_agents=False)
    assert 'Saved your email confirmation preference.' in reply,reply
    runtime.checkpoint(force=True)
    restarted=bootstrap_persistent_runtime(runtime.checkpoint_path)
    model=Memory(False)
    with TestClient(web_app.create_app(restarted,AgentOrchestrator(model_client=model),registry)) as client:
        result=client.post('/api/chat',json={'text':'連絡の設定は？','conversation_id':'web:memory-recall'})
        assert result.status_code==200 and 'confirm before sending emails' in result.json()['text'],result.text
    assert any(entry.source=='explicit_memory_tool' and entry.value=='Confirm before sending emails'
               for entry in restarted.store.list_user_memory_entries())


def test_memory_tools_leave_plain_requests_on_one_call_fast_path(tmp_path,monkeypatch,record_property):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.conversation_history_tools import with_conversation_history_tool
    from nullion.turn_context_policy import turn_tool_registry_for_evidence,TurnToolEvidence
    runtime,registry=setup(tmp_path,monkeypatch)
    def expensive(*args,**kwargs):
        time.sleep(2)
        raise AssertionError('Plain chat loaded broad runtime/history state')
    monkeypatch.setattr('nullion.runtime_persistence.load_runtime_store',expensive)
    registry=turn_tool_registry_for_evidence(with_conversation_history_tool(registry,runtime=runtime,conversation_id='web:plain'),
        evidence=TurnToolEvidence(saved_history_available=True),model_client=None,user_message='',skip_tool_scope_decision=True)
    timings=[]
    for prompt in ['Hello','Hola','こんにちは','Thanks','Gracias','ありがとう']:
        class Plain:
            calls=0
            def create(self,**kwargs):
                self.calls+=1
                assert self.calls==1 and {r['name'] for r in kwargs['tools']}=={'request_tool_scope','terminal_exec','chat_history_search','memory_search'}
                return response('Hello.')
        model=Plain();start=time.perf_counter()
        result=AgentOrchestrator(model_client=model).run_turn(conversation_id='web:plain',principal_id='operator',user_message=prompt,
            conversation_history=[],tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
        timings.append(time.perf_counter()-start)
        assert result.final_text=='Hello.' and model.calls==1 and not result.tool_results
    assert sum(timings)/len(timings)<.5 and max(timings)<1.5
    record_property('elapsed_seconds',json.dumps(timings))


def test_chat_cannot_claim_saved_memory_without_a_receipt(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    runtime,registry=setup(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','1')
    class NoSave(ScriptedProvider):
        def create(self,**kwargs):
            auxiliary=self.auxiliary(kwargs)
            if auxiliary:return auxiliary
            names={row['name'] for row in kwargs.get('tools',[])}
            if 'memory_save' not in names:
                return tool('request_tool_scope',{'tool_names':['memory_save'],'required_tool_names':['memory_save']})
            return response('Saved your preference.')
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=NoSave()),registry)) as client:
        result=client.post('/api/chat',json={'text':'返信は短くすることを覚えてください','conversation_id':'web:unsaved'})
        assert result.status_code==200
        assert 'Saved your preference.' not in result.json()['text'],result.json()
    assert not runtime.store.list_user_memory_entries()


def test_web_memory_delete_observes_other_process_write_and_stays_deleted(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.memory_tools import MemoryToolRegistry
    from nullion.memory import memory_entries_for_owner
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation,ToolRegistry
    from nullion.web_app import create_app
    first,registry=setup(tmp_path,monkeypatch)
    stale=bootstrap_persistent_runtime(first.checkpoint_path)
    writer=MemoryToolRegistry(ToolRegistry(),runtime=first,workspace_id='workspace_admin',conversation_id='telegram:2222',user_message='確認する')
    saved=writer.invoke(ToolInvocation('save','memory_save','telegram:2222',{'key':'確認','value':'Confirm first','kind':'preference','source_quote':'確認する'}))
    assert saved.status=='completed'
    with TestClient(create_app(stale,None,registry)) as client:
        status=client.get('/api/status?full=1')
        assert status.status_code==200 and saved.output['memory_id'] in status.text,status.text[:500]
        deleted=client.delete('/api/memory/'+saved.output['memory_id'])
        assert deleted.status_code==200 and deleted.json()['deleted']==1,deleted.text
    first.checkpoint(force=True)
    assert not memory_entries_for_owner(first.store,'workspace:workspace_admin')


def test_web_polling_recovers_attachment_without_exposing_internal_directives(tmp_path,monkeypatch):
    from pypdf import PdfWriter
    from playwright.sync_api import expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    from test_web_surface_contracts import _UvicornThread,_free_port,_open_page
    runtime,registry=setup(tmp_path,monkeypatch)
    from nullion.tools import create_plugin_tool_registry,register_workspace_plugin
    monkeypatch.setenv('NULLION_FILE_ACCESS_ENABLED','1')
    registry=register_workspace_plugin(create_plugin_tool_registry(workspace_root=tmp_path),workspace_root=tmp_path)
    runtime.active_tool_registry=registry
    artifact=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'cedar-itinerary.pdf'
    artifact.parent.mkdir(parents=True,exist_ok=True)
    from pypdf.generic import DictionaryObject,NameObject,DecodedStreamObject
    pdf=PdfWriter();pdf_page=pdf.add_blank_page(width=200,height=200)
    font=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
    pdf_page[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):DictionaryObject({NameObject('/F1'):pdf._add_object(font)})})
    content=DecodedStreamObject();content.set_data(b'BT /F1 12 Tf 20 100 Td (Cedar itinerary) Tj ET')
    pdf_page[NameObject('/Contents')]=pdf._add_object(content)
    pdf.write(artifact)
    class Delivery(ScriptedProvider):
        steps=0
        def create(self,**kwargs):
            if 'select_attachment_format' in {row['name'] for row in kwargs.get('tools',[])}:
                return tool('select_attachment_format',{'extension':'.pdf','confidence':1,'embedded_media_extensions':[]})
            auxiliary=self.auxiliary(kwargs)
            if auxiliary:return auxiliary
            names={row['name'] for row in kwargs.get('tools',[])}
            if self.steps>=2:
                return response('Here is the saved document.\nMEDIA:'+str(artifact))
            if 'file_search' not in names:
                return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_search','file_read'],'required_tool_names':['file_read'],'artifact_extensions':['.pdf'],'source_artifact_delivery_requested':True,'source_artifact_request_evidence':'Retrieve the saved Cedar trip document.'})
            self.steps+=1
            if self.steps==1:return tool('file_search',{'pattern':artifact.name,'root':str(artifact.parent)})
            if self.steps==2:return tool('file_read',{'path':str(artifact)})
            return response('Here is the saved document.\nMEDIA:'+str(artifact))
    model=Delivery();runtime.model_client=model
    app=create_app(runtime,AgentOrchestrator(model_client=model),registry)
    with _UvicornThread(app,_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            # Drop only final server delivery at the external transport boundary.
            # The real runtime still records its result; the UI must reconcile it.
            def intercept(ws):
                peer=ws.connect_to_server()
                def receive(message):
                    if isinstance(message,str):
                        payload=json.loads(message)
                        if payload.get('type') in {'done','chunk'}:
                            return
                    ws.send(message)
                peer.on_message(receive)
            recovery_requests=[]
            page.on('request',lambda request: recovery_requests.append(request.url) if '/api/chat/history/' in request.url and 'turn_ids=' in request.url else None)
            page.route_web_socket('**/ws/chat',intercept)
            page.reload()
            expect(page.get_by_text('Connected',exact=True)).to_be_visible(timeout=10000)
            page.locator('#user-input').fill('Retrieve the saved Cedar trip document.')
            page.locator('#send-btn').click()
            bubble=page.locator('.msg.bot .bubble').last
            expect(bubble.get_by_role('link',name='Download cedar-itinerary.pdf')).to_be_visible(timeout=30000)
            assert 'MEDIA:' not in bubble.inner_text() and str(artifact.parent) not in bubble.inner_text()
            url=bubble.get_by_role('link',name='Download cedar-itinerary.pdf').get_attribute('href')
            downloaded=page.request.get(server.base_url+url)
            assert downloaded.ok and downloaded.body()==artifact.read_bytes()
            assert recovery_requests,'The visible reply did not use the real recovery route'
            page.screenshot(path='/tmp/memory-browser-polling-proof.png')
            assert not errors,errors
        finally:
            browser.close();playwright.stop()


def test_unresolved_document_lookup_finishes_without_inventory_or_budget_prompt(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from pypdf import PdfWriter
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    runtime,registry=setup(tmp_path,monkeypatch)
    artifact=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'cedar.pdf'
    artifact.parent.mkdir(parents=True,exist_ok=True)
    writer=PdfWriter();writer.add_blank_page(width=200,height=200);writer.write(artifact)
    runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':'telegram:2222',
        'workspace_id':'workspace_admin','created_at':'2026-10-01T10:00:00Z','turn_id':'prior-cedar',
        'user_message':'Cedar itinerary','assistant_reply':'Attached cedar.pdf.'})
    class Unresolved(ScriptedProvider):
        calls=0
        def create(self,**kwargs):
            auxiliary=self.auxiliary(kwargs)
            if auxiliary:return auxiliary
            self.calls+=1
            if self.calls==1:return tool('request_tool_scope',{'capabilities':['conversation_history'],'tool_names':['chat_history_search']})
            if self.calls==2:return tool('chat_history_search',{'query':'Cedar','artifact_extensions':['.pdf']})
            return response('処理中です。')
    model=Unresolved()
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        result=client.post('/api/chat',json={'text':'杉の旅行のPDFはありますか？','conversation_id':'web:unresolved'})
        assert result.status_code==200,result.text
        payload=result.json()
        assert payload['type']=='message',payload
        assert 'could not confirm the matching document' in payload['text'],payload
        assert not payload.get('artifacts') and 'MEDIA:' not in payload['text'],payload
    assert 5 <= model.calls <= 6
    event=next(event for event in reversed(runtime.store.list_conversation_events('web:unresolved')) if event.get('event_type')=='conversation.chat_turn')
    assert any(row['tool_name']=='chat_history_search' and row['output'].get('lookup_selection_unresolved') is True for row in event['tool_results'])


def test_retrieval_repair_delivers_original_bytes_without_exposing_writers(tmp_path,monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _repair_tool_registry_for_missing_attachment_extensions,_required_attachment_repair_prompt
    from nullion.conversation_history_tools import with_conversation_history_tool
    from nullion.tools import ToolResult
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolEvidence
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    from pypdf import PdfWriter
    from pypdf.generic import DictionaryObject,NameObject,DecodedStreamObject
    runtime,base=setup(tmp_path,monkeypatch)
    prompt='保存した杉の文書を送ってください。'
    base=with_conversation_history_tool(base,runtime=runtime,conversation_id='web:repair',workspace_id='workspace_admin',user_message=prompt)
    target=workspace_storage_roots_for_workspace('workspace_admin').artifacts/'cedar-retrieval.pdf'
    target.parent.mkdir(parents=True,exist_ok=True)
    writer=PdfWriter();page=writer.add_blank_page(width=200,height=200)
    font=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
    page[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):DictionaryObject({NameObject('/F1'):writer._add_object(font)})})
    content=DecodedStreamObject();content.set_data(b'BT /F1 12 Tf 20 100 Td (Cedar itinerary) Tj ET')
    page[NameObject('/Contents')]=writer._add_object(content);writer.write(target);original=target.read_bytes()
    scope=ToolResult('scope','request_tool_scope','completed',{'capabilities':['conversation_history','local_files'],
        'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt,
        'explicit_tool_names':['chat_history_search','file_read'],'required_tool_names':[]})
    registry=_repair_tool_registry_for_missing_attachment_extensions(base_registry=base,
        active_registry=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(current_user_message=prompt)),missing_extensions=('.pdf',),
        source_tool_results=[scope],user_message=prompt)
    class Recover:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            names={spec['name'] for spec in kwargs.get('tools',[])}
            assert not {'pdf_create','pdf_edit','file_write','file_patch','memory_save'}.intersection(names)
            assert kwargs['messages'][-1]['role']=='user'
            if self.calls==1:
                assert kwargs['messages'][-1]['content'][0]['text']==prompt
                return tool('file_read',{'path':str(target)})
            return response('Attached the existing document.\nMEDIA:'+str(target))
    model=Recover()
    result=AgentOrchestrator(model_client=model).run_turn(conversation_id='web:repair',principal_id='operator',user_message=prompt,
        conversation_history=[{'role':'user','content':[{'type':'text','text':'[Runtime recovery instructions]\n'+_required_attachment_repair_prompt(('.pdf',),source_tool_results=[scope])}]}],
        tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store)
    assert result.response_fulfilled is not False and result.artifact_delivery_satisfied and result.artifacts==[str(target)],result
    assert target.read_bytes()==original
    assert [row.tool_name for row in result.tool_results]==['file_read']
    assert result.tool_results[0].output['artifact_descriptors'][0]['origin']=='existing_workspace_artifact'
