"""An uploaded/source record's exact URL survives authoring and API download."""
import html
import json
import os
import time
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

@pytest.mark.parametrize('mode',['off','on'])
def test_observed_url_is_written_once_without_source_research(tmp_path,monkeypatch,mode,record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_read_handler,_build_file_write_handler
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE',mode)
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    for name,effect,handler in [('file_read',ToolSideEffectClass.READ,_build_file_read_handler),('file_write',ToolSideEffectClass.WRITE,_build_file_write_handler)]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,5,code_mode_allowed=True,
            produced_artifact_extensions=('.html',) if name=='file_write' else ()),handler(tmp_path,allowed_roots=[tmp_path]))
    url='https://example.com/aurora?record=73&scope=original'
    source_root=artifact_root_for_principal('web:source-reference');source_root.mkdir(parents=True,exist_ok=True)
    source=source_root/'records.json';source.write_text(json.dumps({'name':'Aurora','quantity':73,'url':url}))
    class Model:
        phase=0
        errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if any(t['name']=='select_attachment_format' for t in kw.get('tools',[])):
                return call('select_attachment_format',{'extension':'.html','confidence':1})
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_read','file_write'],'required_tool_names':['file_read','file_write'],
                'artifact_extensions':['.html'],'source_artifact_delivery_requested':False})
            if self.phase==2:
                args={'path':str(source)}
                if mode=='on':return call('code_mode',{'tool_names':['file_read','file_write'],'code':
                    "record=call_tool('file_read',"+repr(args)+")[\"output\"][\"content\"]\n"
                    "call_tool('file_write',"+repr({'path':str(self.target),'content':markup})+")"})
                return call('file_read',args)
            if mode=='off' and self.phase==3:return call('file_write',{'path':str(self.target),'content':markup})
            if self.phase!=(3 if mode=='on' else 4):self.errors.append(self.phase)
            return text('完成しました。\nMEDIA:'+str(self.target))
    model=Model();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for i,prompt in enumerate(['このJSONから正確なリンクを含む .html を作成してください。','Crea un .html con el enlace exacto del JSON.','Crée un .html avec le lien exact du JSON.']):
            cid='web:source-reference-'+str(i);root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
            upload=client.post('/api/upload',files={'file':('records.json',source.read_bytes(),'application/json')})
            assert upload.status_code==200,upload.text
            source=__import__('pathlib').Path(upload.json()['path'])
            model.target=root/'report.html';model.phase=0
            markup='<html><body><h1>Aurora 73</h1><a href="'+html.escape(url,quote=True)+'">Source</a></body></html>'
            started=time.monotonic();response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid,'attachments':[{'path':str(source),'name':'records.json','media_type':'application/json'}]});samples.append(time.monotonic()-started)
            assert response.status_code==200 and not model.errors,response.text
            artifacts=response.json()['artifacts'];assert len(artifacts)==1,response.text
            downloaded=client.get(artifacts[0]['url']);assert downloaded.content==markup.encode()
            turn=next(e for e in reversed(runtime.store.list_conversation_events(cid)) if e.get('event_type')=='conversation.chat_turn')
            assert turn['response_fulfilled'] is True,turn
            writes=[r for r in turn['tool_results'] if r['tool_name']=='file_write']
            assert len(writes)==1 and writes[0]['status']=='completed',writes
            assert not any(r['tool_name'] in {'terminal_exec','browser_navigate','web_search'} for r in turn['tool_results'])
    assert sum(samples)/len(samples)<3 and max(samples)<5,samples
    record_property('mean_observed_source_delivery_seconds',sum(samples)/len(samples))
