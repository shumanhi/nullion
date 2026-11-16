"""Authorized image-read pixels reach the same primary model through both layers."""
import base64
import io
import os
import time

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('mode',['off','on'])
def test_image_read_reaches_primary_as_pixels_without_attachment_or_repeat(tmp_path,monkeypatch,record_property,mode):
    from datetime import UTC,datetime
    from PIL import Image
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_read_handler
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,call,text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE',mode)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    path=tmp_path/'source-37.png'
    image=Image.new('RGB',(200,80),'red');image.paste('navy',(100,0,200,80));image.save(path)
    calls=[];registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    reader=_build_file_read_handler(tmp_path,allowed_roots=[tmp_path])
    def read(inv):calls.append(inv.invocation_id);return reader(inv)
    registry.register(ToolSpec('file_read','Read the selected image',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,5,input_schema={'type':'object','properties':{'path':{'type':'string'}},'required':['path']}),read)
    class Provider:
        rounds=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            names={r['name'] for r in kw.get('tools',[])}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return text('{}')
            self.rounds+=1;time.sleep(.05)
            assert self.rounds<=3,'Visual inspection restarted the native read'
            if self.rounds==1:return call('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_read'],'required_tool_names':['file_read'],'artifact_extensions':[],
                'source_artifact_delivery_requested':False})
            if self.rounds==2:
                args={'path':str(path)}
                return (call('code_mode',{'tool_names':['file_read'],
                    'code':"call_tool('file_read', "+repr(args)+")"}) if mode=='on' else call('file_read',args))
            pictures=[b for m in kw['messages'] for b in m.get('content',[]) if isinstance(b,dict) and b.get('type')=='image']
            assert len(pictures)==1,'The primary needs the authorized pixels, not image metadata'
            with Image.open(io.BytesIO(base64.b64decode(pictures[0]['source']['data']))) as picture:
                left=picture.getpixel((25,40));right=picture.getpixel((175,40))
                assert left[0]>200 and left[1]<50 and left[2]<50,left
                assert right[2]>80 and right[0]<50 and right[1]<50,right
            return text('Left: red. Right: navy.')
    model=Provider();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for i,prompt in enumerate(['Read the local reference image and describe its two colors. Do not attach files.',
            'Lee la imagen de referencia local y describe sus dos colores. No adjuntes archivos.',
            'ローカル参照画像の二つの色を説明してください。ファイルは添付しないでください。']):
            model.rounds=0;before=len(calls);cid='web:visual-reference-'+str(i)
            start=time.perf_counter();response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
            samples.append(time.perf_counter()-start)
            assert response.status_code==200,response.text
            payload=response.json();assert payload['text']=='Left: red. Right: navy.',payload
            assert not payload.get('artifacts') and len(calls)==before+1
            turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True
            reads=[r for r in turn['tool_results'] if r['tool_name']=='file_read']
            assert len(reads)==1 and reads[0]['output']['visual_preview_delivered'] is True
            assert all(r['status']=='completed' for r in turn['tool_results'])
    assert sum(samples)/len(samples)<3 and max(samples)<5,samples
    record_property('mean_visual_reference_seconds',sum(samples)/len(samples))


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['off','on'])
async def test_langchain_tool_images_reach_the_bound_model_with_real_sqlite(tmp_path,monkeypatch,mode):
    from datetime import UTC,datetime
    from PIL import Image
    from langchain_core.messages import HumanMessage,AIMessage,ToolMessage
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools,nullion_client_as_langchain_chat_model
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_read_handler
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime=isolate(tmp_path,monkeypatch);monkeypatch.setenv('NULLION_CODE_MODE',mode)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-files',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    path=tmp_path/'framework-reference.png';Image.new('RGB',(80,60),'navy').save(path)
    native=[];registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_read','Read selected image',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
        input_schema={'type':'object','properties':{'path':{'type':'string'}},'required':['path']}),
        _build_file_read_handler(tmp_path,allowed_roots=[tmp_path]))
    tools=nullion_tools_as_langchain_tools(registry,allowed_tools=['file_read'],principal_id='operator',
        cleanup_scope='image-reference',policy_store=runtime.store,tool_result_callback=native.append)
    tool=next(t for t in tools if t.name==('code_mode' if mode=='on' else 'file_read'))
    args=({'code':"call_tool('file_read', {'path':"+repr(str(path))+"})",'tool_names':['file_read']}
        if mode=='on' else {'path':str(path)})
    payload=await tool.ainvoke(args)
    received=[]
    class Model:
        def create(self,**kw):
            received.append(kw)
            images=[b for m in kw['messages'] for b in m.get('content',[]) if isinstance(b,dict) and b.get('type')=='image']
            assert len(images)==1,'Framework conversion discarded the authorized image'
            with Image.open(io.BytesIO(base64.b64decode(images[0]['source']['data']))) as picture:
                r,g,b=picture.getpixel((20,20));assert b>80 and r<50 and g<50
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'Navy reference verified.'}]}
    model=nullion_client_as_langchain_chat_model(Model()).bind_tools(tools)
    answer=await model.ainvoke([HumanMessage(content='画像を説明してください。'),
        AIMessage(content='',tool_calls=[{'id':'image-read','name':tool.name,'args':args}]),
        ToolMessage(content=payload,tool_call_id='image-read')])
    assert answer.content=='Navy reference verified.' and len(received)==1
    assert len([r for r in native if r.tool_name=='file_read'])==1
    assert all(r.status=='completed' for r in native)
