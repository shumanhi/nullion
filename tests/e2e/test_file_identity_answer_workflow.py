from tests.workflow_provider_protocol import workflow_provider_reply
import os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product API boundary')

@pytest.mark.parametrize('delivery_marker',[False,True])
@pytest.mark.parametrize('code_mode',[False,True])
@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('prompt,answer',[('Where is my itinerary?','The document is'),('旅程の文書はどれ？','文書はこちら'),('¿Dónde está mi itinerario?','El documento es'),('Find the pdf file cedar-itinerary.pdf. Reply with its filename. Do not attach it.','The document is'),('cedar-itinerary.pdf を探して名前だけ教えてください。添付しないでください。','文書はこちら'),('Busca el archivo pdf cedar-itinerary.pdf y dime su nombre sin adjuntarlo.','El documento es')])
def test_narrowed_native_file_discovery_preserves_composed_answer(tmp_path,monkeypatch,prompt,answer,surface,delivery_marker,code_mode):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_search_handler
    monkeypatch.setenv('NULLION_CODE_MODE','on' if code_mode else 'off')
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    files=tmp_path/'documents';files.mkdir();target=files/'cedar-itinerary.pdf';target.write_bytes(b'%PDF-1.4\n')
    if not delivery_marker:
        (files/'other.pdf').write_bytes(b'%PDF-1.4\n')
        for i in range(30):(files/f'library-{i}.py').write_text('value=37')
    registry=ToolRegistry(filesystem_allowed_roots=[files]);registry.register(ToolSpec('file_search','Find matching file identities',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,code_mode_allowed=True),_build_file_search_handler(files,allowed_roots=[files],include_principal_workspace=False))
    expected=f'I found 1 file:\n- {target.name}' if delivery_marker else f'{answer}: **{target.name}**.'
    class Model:
        phase=0;errors=[];format_calls=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as e:self.errors.append(repr(e));raise
        def _create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            def text(t):return {'stop_reason':'end_turn','content':[{'type':'text','text':t}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if 'select_attachment_format' in names:
                self.format_calls+=1
                return call('select_attachment_format',{'extension':'.pdf' if 'cedar-itinerary.pdf' in prompt else None,'output_kind':'attachment','embedded_media_extensions':[],'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_search'],'required_tool_names':['file_search'],'artifact_extensions':[],'source_artifact_delivery_requested':False})
            if self.phase in (2,3):
                arguments={'pattern':'.' if self.phase==2 else 'cedar-itinerary.pdf','root':str(files)}
                return call('code_mode',{'tool_names':['file_search'],'code':f"call_tool('file_search', {arguments!r})"}) if code_mode else call('file_search',arguments)
            assert self.phase==4
            return text("Done — attached the requested files.\n\nMEDIA:"+str(target) if delivery_marker else expected)
    model=Model();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    cid='web:file-identity' if surface=='web' else 'telegram:123'
    if surface=='web':
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
            r=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});assert r.status_code==200,r.text
            assert r.json()['text']==expected and not r.json()['artifacts'],r.json()
    else:
        import asyncio
        from types import SimpleNamespace
        from nullion import telegram_app
        from nullion.config import load_settings
        from nullion.telegram_formatting import format_telegram_text
        runtime.active_tool_registry=registry
        settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':'123456:abcdefghijklmnopqrstuvwxyz','NULLION_TELEGRAM_OPERATOR_CHAT_ID':'123','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
        service=telegram_app.ChatOperatorService(runtime=runtime,settings=settings,bot_token=settings.telegram.bot_token,operator_chat_id='123',model_client=model,agent_orchestrator=AgentOrchestrator(model_client=model))
        sent=[]
        class Message:
            text=prompt;message_id=500;reply_to_message=None
            chat=SimpleNamespace(id=123)
            async def reply_text(self,value,**kwargs):
                slot=len(sent);sent.append((value,kwargs))
                class SentMessage:
                    message_id=501+slot
                    chat=SimpleNamespace(id=123)
                    async def edit_text(self,value,**options):
                        sent[slot]=(value,options);return self
                return SentMessage()
        Message.chat.send_action=lambda **kwargs:asyncio.sleep(0)
        asyncio.run(service.on_message(SimpleNamespace(update_id=500,message=Message()),SimpleNamespace(bot=None)))
        assert len([text for text,options in sent if text==format_telegram_text(expected)[0] and options.get('parse_mode') in ({None,'HTML'} if delivery_marker else {'HTML'})])==1,sent
        assert not any('library-' in text or 'other.pdf' in text for text,options in sent),sent
    assert not model.errors,model.errors
    if 'cedar-itinerary.pdf' in prompt:
        assert model.format_calls==0, 'Filename references must use the existing primary structured scope decision'
    reopened=bootstrap_persistent_runtime(tmp_path/'runtime.db');turns=[r for r in reopened.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn']
    assert turns[-1]['assistant_reply']==expected
    results=[r for r in turns[-1]['tool_results'] if r['tool_name']=='file_search'];assert len(results)==2
    assert len(results[0]['output']['matches'])==(1 if delivery_marker else 32) and results[1]['output']['matches']==[str(target)]
    programs=[r for r in turns[-1]['tool_results'] if r['tool_name']=='code_mode']
    assert len(programs)==(2 if code_mode else 0)


@pytest.mark.parametrize('mode',['off','on'])
@pytest.mark.parametrize('prompt',[
    'Busca el archivo existente absent-37.pdf. Si no existe, dilo. No crees ni adjuntes archivos.',
    'absent-37.pdf を探してください。存在しなければ、その旨だけ回答し、作成や添付はしないでください。',
    'ابحث عن الملف absent-37.pdf وأخبرني إن لم يكن موجودًا. لا تنشئ ملفات ولا ترفقها.',
])
def test_missing_filename_is_not_a_new_artifact_requirement(tmp_path,monkeypatch,mode,prompt):
    import json,time
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_search_handler,_build_pdf_create_handler
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE',mode)
    files=tmp_path/'files';files.mkdir()
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_search','Search existing files',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),
        _build_file_search_handler(files,allowed_roots=[files]))
    registry.register(ToolSpec('pdf_create','Create a PDF',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10,
        produced_artifact_extensions=('.pdf',)),_build_pdf_create_handler(tmp_path,allowed_roots=[tmp_path]))
    expected='Archivo no encontrado: absent-37.pdf'
    class Provider:
        calls=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            from tests.workflow_provider_protocol import is_tool_scope_request
            if is_tool_scope_request(kw):
                return text(json.dumps({'requested_tool_names':['file_search'],'required_tool_names':['file_search'],
                    'artifact_extensions':[],'confidence':1}))
            self.calls+=1
            assert self.calls<=2,'A filename incorrectly forced artifact generation or a new classifier'
            if self.calls==1:
                args={'pattern':'absent-37.pdf','root':str(files)}
                return (call('code_mode',{'tool_names':['file_search'],
                    'code':"call_tool('file_search', "+repr(args)+")"}) if mode=='on' else call('file_search',args))
            return text(expected)
    model=Provider()
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        started=time.monotonic()
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:missing-file'})
        assert time.monotonic()-started<5
        assert response.status_code==200 and response.json()['text']==expected,response.text
        assert not response.json().get('artifacts')
        turns=[row for row in runtime.store.list_conversation_events('web:missing-file') if row.get('event_type')=='conversation.chat_turn']
        searches=[row for row in turns[-1]['tool_results'] if row['tool_name']=='file_search']
        assert len(searches)==1 and searches[0]['status']=='completed' and searches[0]['output']['matches']==[]
        assert turns[-1]['response_fulfilled'] is True
        assert not any(row['tool_name']=='pdf_create' for row in turns[-1]['tool_results'])
        assert not list(tmp_path.rglob('*.pdf')) and model.calls==2
