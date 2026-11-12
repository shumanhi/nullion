from tests.workflow_provider_protocol import workflow_provider_reply
import os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product API boundary')

@pytest.mark.parametrize('delivery_marker',[False,True])
@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('prompt,answer',[('Where is my itinerary?','The document is'),('旅程の文書はどれ？','文書はこちら'),('¿Dónde está mi itinerario?','El documento es')])
def test_narrowed_native_file_discovery_preserves_composed_answer(tmp_path,monkeypatch,prompt,answer,surface,delivery_marker):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_search_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    files=tmp_path/'documents';files.mkdir();target=files/'cedar-itinerary.pdf';target.write_bytes(b'%PDF-1.4\n')
    if not delivery_marker:
        (files/'other.pdf').write_bytes(b'%PDF-1.4\n')
        for i in range(30):(files/f'library-{i}.py').write_text('value=37')
    registry=ToolRegistry(filesystem_allowed_roots=[files]);registry.register(ToolSpec('file_search','Find matching file identities',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5),_build_file_search_handler(files,allowed_roots=[files],include_principal_workspace=False))
    expected=f'I found 1 file:\n- {target.name}' if delivery_marker else f'{answer}: **{target.name}**.'
    class Model:
        phase=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as e:self.errors.append(repr(e));raise
        def _create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            def text(t):return {'stop_reason':'end_turn','content':[{'type':'text','text':t}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_search'],'required_tool_names':['file_search'],'artifact_extensions':[],'source_artifact_delivery_requested':False})
            if self.phase==2:return call('file_search',{'pattern':'.','root':str(files)})
            if self.phase==3:return call('file_search',{'pattern':'cedar-itinerary.pdf','root':str(files)})
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
    reopened=bootstrap_persistent_runtime(tmp_path/'runtime.db');turns=[r for r in reopened.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn']
    assert turns[-1]['assistant_reply']==expected
    results=[r for r in turns[-1]['tool_results'] if r['tool_name']=='file_search'];assert len(results)==2
    assert len(results[0]['output']['matches'])==(1 if delivery_marker else 32) and results[1]['output']['matches']==[str(target)]
