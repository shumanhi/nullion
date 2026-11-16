"""Capture actual generated content without treating about:blank as empty."""
import io
import json
import os

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


def test_generated_document_delivers_a_real_png_without_external_navigation(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    for key,value in [('NULLION_CODE_MODE','on'),('NULLION_BROWSER_BACKEND','playwright'),
        ('NULLION_BROWSER_HEADLESS','true')]:monkeypatch.setenv(key,value)
    prompt='Render a local page and capture it as a .png image.'
    names=['browser_open','browser_extract_text','browser_run_js','browser_screenshot']
    markup='<html><body style="margin:0;background:rgb(17,34,51);color:white"><h1>Local render verified 37</h1></body></html>'
    script='document.open();document.write('+json.dumps(markup)+');document.close();document.title="Local render verified";true'
    class Model:
        phase=0
        def create(self,**kwargs):
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            if any(t['name']=='select_attachment_format' for t in kwargs.get('tools',[])):
                return call('select_attachment_format',{'extension':'.png','confidence':1})
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],'tool_names':names,
                'required_tool_names':names,'artifact_extensions':['.png'],
                'source_artifact_delivery_requested':True,'source_artifact_request_evidence':prompt})
            if self.phase==2:return call('code_mode',{'tool_names':names,'code':
                "opened=call_tool('browser_open',{'mode':'headless'})\nsid=opened['output']['session_id']\n"
                "call_tool('browser_extract_text',{'session_id':sid})\n"
                f"call_tool('browser_run_js',{{'session_id':sid,'script':{script!r}}})\n"
                "call_tool('browser_screenshot',{'session_id':sid,'mode':'viewport'})['output']"})
            assert self.phase==3
            return text('Captured the generated page image.')
    model=Model();registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:local-render'})
            assert response.status_code==200 and response.json()['artifacts'],response.text
            downloaded=client.get(response.json()['artifacts'][0]['url'])
            with Image.open(io.BytesIO(downloaded.content)) as picture:
                assert picture.convert('RGB').getpixel((10,150))==(17,34,51)
            events=runtime.store.list_conversation_events('web:local-render')
            results=[r for e in events if e.get('event_type')=='conversation.chat_turn' for r in e.get('tool_results',[])]
            screenshot=next(r for r in results if r['tool_name']=='browser_screenshot')
            assert screenshot['status']=='completed' and screenshot['output']['has_document_content'] is True
            assert screenshot['output']['page_url']=='about:blank'
            assert not any(r['tool_name']=='browser_navigate' for r in results)
    finally:registry.run_cleanup_hooks()


@pytest.mark.parametrize('initial_failure',[False,True])
def test_authored_html_render_is_final_image_without_second_producer(tmp_path,monkeypatch,record_property,initial_failure):
    import time
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    names=['file_write','browser_open','browser_navigate','browser_screenshot']
    class Model:
        phase=0
        errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if any(t['name']=='select_attachment_format' for t in kw.get('tools',[])):
                return call('select_attachment_format',{'extension':'.png','confidence':1})
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if initial_failure and self.phase==1:
                return call('request_tool_scope',{'capabilities':['image_generation'],'tool_names':['image_generate'],
                    'required_tool_names':['image_generate'],'artifact_extensions':['.png']})
            if initial_failure and self.phase==2:
                result=call('code_mode',{'tool_names':['image_generate'],'code':"call_tool('image_generate', "+repr({'prompt':'Data graphic 37','output_path':str(self.source.with_suffix('.png'))})+")"})
                result['content'][0]['id']='image-program'
                return result
            phase=self.phase-2 if initial_failure else self.phase
            if phase==1:return call('request_tool_scope',{'capabilities':['web','local_files'],
                'tool_names':names,'required_tool_names':names,'artifact_extensions':['.png'],
                'source_artifact_delivery_requested':False})
            if phase==2:return call('code_mode',{'tool_names':names,'code':
                "opened=call_tool('browser_open',{'mode':'headless'})\nsid=opened['output']['session_id']\n"
                "call_tool('browser_navigate',{'session_id':sid,'url':"+repr(self.incidental.as_uri())+"})\n"
                "call_tool('browser_screenshot',{'session_id':sid,'mode':'viewport'})\n"
                "call_tool('file_write',"+repr({'path':str(self.source),'content':markup})+")\n"
                "call_tool('browser_navigate',{'session_id':sid,'url':"+repr(self.source.as_uri())+"})\n"
                "call_tool('browser_screenshot',{'session_id':sid,'mode':'viewport'})['output']"})
            if phase!=3:self.errors.append('Unexpected additional producer/scope round')
            return text('完成しました。')
    model=Model();registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_write','Write local HTML',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10,
        produced_artifact_extensions=('.html',)),_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    if initial_failure:
        import urllib.error
        from nullion.tools import register_media_plugin
        monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED','true')
        def reject(*args,**kwargs):
            raise RuntimeError('Provider payment required') from urllib.error.HTTPError('https://provider.invalid',402,'Payment required',{},io.BytesIO(b''))
        register_media_plugin(registry,image_generator=reject)
    register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    samples=[]
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            for index,prompt in enumerate(['予定を図にして .png でください。','Haz una infografía del itinerario en .png.','Crée une infographie du trajet au format .png.']):
                cid='web:authored-render-'+str(index);root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
                model.source=root/f'authored-{index}.html';model.phase=0
                model.incidental=root/f'source-{index}.html'
                model.incidental.write_text('<html><body style="background:red">Internal source only</body></html>')
                markup='<html><body style="margin:0;background:rgb(17,34,51);color:white"><h1>Itinerary 37</h1><p>10:53 train · 13:30 lunch</p></body></html>'
                started=time.monotonic();response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});samples.append(time.monotonic()-started)
                assert response.status_code==200 and not model.errors,response.text
                artifacts=response.json().get('artifacts',[])
                assert len(artifacts)==1 and artifacts[0]['name'].endswith('.png'),response.text
                data=client.get(artifacts[0]['url']).content
                with Image.open(io.BytesIO(data)) as image:assert image.convert('RGB').getpixel((10,180))==(17,34,51)
                turn=next(e for e in reversed(runtime.store.list_conversation_events(cid)) if e.get('event_type')=='conversation.chat_turn')
                assert turn['response_fulfilled'] is True,turn
                captures=[r for r in turn['tool_results'] if r['tool_name']=='browser_screenshot' and r['status']=='completed']
                assert len(captures)==2 and captures[0]['output']['artifact_descriptors'][0]['role']=='source',captures
                assert captures[-1]['output']['artifact_descriptors'][0]['role']=='deliverable',captures
                assert sum(r['tool_name']=='image_generate' for r in turn['tool_results'])==int(initial_failure)
                assert len([r for r in turn['tool_results'] if r['tool_name']=='request_tool_scope'])==1+int(initial_failure)
        assert sum(samples)/len(samples)<12 and max(samples)<20,samples
        record_property('mean_authored_render_seconds',sum(samples)/len(samples))
    finally:registry.run_cleanup_hooks()
