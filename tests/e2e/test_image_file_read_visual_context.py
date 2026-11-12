"""Image reads provide actual authorized pixels, without delivering the input."""
from tests.workflow_provider_protocol import workflow_provider_reply
import base64,io,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary E2E')

@pytest.mark.parametrize('prompt',['Describe the provided image.','Describe la imagen proporcionada.','指定された画像を説明してください。'])
def test_real_api_file_read_forwards_pixels_without_exposing_source_as_attachment(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_read_handler
    from nullion.model_clients import CodexResponsesModelClient,OpenAIChatCompletionsModelClient
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');root=artifact_root_for_principal('web:visual-read');root.mkdir(parents=True,exist_ok=True)
    path=root/'reference.png';Image.new('RGB',(400,300),(0,0,255)).save(path)
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);registry.register(ToolSpec('file_read','Read image or text',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,10),_build_file_read_handler(tmp_path,allowed_roots=[tmp_path]))
    class Provider:
        phase=0;seen=False
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names={r['name'] for r in kw.get('tools',[])}
            def tool(n,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':n,'input':args}]}
            if not names:return {'content':[{'type':'text','text':'{}'}]}
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_read'],'required_tool_names':['file_read']})
            if self.phase==2:return tool('file_read',{'path':str(path)})
            assert self.phase==3
            parts=[b for m in kw['messages'] for b in m.get('content',[]) if isinstance(b,dict)]
            images=[b for b in parts if b.get('type')=='image'];assert len(images)==1
            with Image.open(io.BytesIO(base64.b64decode(images[0]['source']['data']))) as im:
                r,g,b=im.getpixel((100,100));assert b>240 and r<15 and g<15
            response_input=CodexResponsesModelClient._to_input(kw['messages'])
            assert any(isinstance(m.get('content'),list) and any(b.get('type')=='input_image' for b in m['content']) for m in response_input)
            chat_input=OpenAIChatCompletionsModelClient._serialize_messages(kw['messages'])
            assert any(isinstance(m.get('content'),list) and any(b.get('type')=='image_url' for b in m['content']) for m in chat_input)
            self.seen=True;return {'stop_reason':'end_turn','content':[{'type':'text','text':'The image is blue.'}]}
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        r=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:visual-read'});assert r.status_code==200,r.text;data=r.json()
        assert model.seen and data['text']=='The image is blue.' and not data['artifacts'],data
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    turn=next(t for t in restored.store.list_conversation_events('web:visual-read') if t.get('event_type')=='conversation.chat_turn')
    read=next(t for t in turn['tool_results'] if t['tool_name']=='file_read')
    assert read['output']['visual_preview_available'] is True and len(read['output']['image_sha256'])==64
    assert 'data:image' not in str(read['output']) and 'base64' not in str(read['output'])
