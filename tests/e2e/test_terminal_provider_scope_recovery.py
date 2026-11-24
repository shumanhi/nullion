"""Real API/native failure receipts cannot be resurrected by scope discovery."""
import io
import json
import os
import shlex
import sys
import time
import urllib.error

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')


@pytest.mark.parametrize('mode',['off','on'])
@pytest.mark.parametrize('fallback',[False,True])
@pytest.mark.parametrize('transport',['urllib','sdk'])
def test_terminal_generation_failure_stays_unavailable_without_recovery_loop(tmp_path,monkeypatch,mode,fallback,record_property,transport,rediscover=False):
    from datetime import UTC,datetime
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,register_media_plugin,_build_terminal_exec_handler
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('local',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture',datetime.now(UTC)))
    monkeypatch.setenv('NULLION_CODE_MODE',mode)
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED','true')
    native_calls=[]
    def generate(prompt,*args,**kwargs):
        native_calls.append(prompt)
        if transport == 'sdk':
            import httpx
            from openai import APIStatusError
            response = httpx.Response(402, request=httpx.Request('POST', 'https://provider.invalid/generate'))
            raise APIStatusError('Provider requires payment', response=response, body=None)
        cause=urllib.error.HTTPError('https://provider.invalid/generate',402,'Payment required',{},io.BytesIO(b''))
        raise RuntimeError('Provider requires payment') from cause
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    register_media_plugin(registry,image_generator=generate)
    registry.register(ToolSpec('terminal_exec','Render local data graphics',ToolRiskLevel.LOW,
        ToolSideEffectClass.DANGEROUS_EXEC,False,10),_build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))
    class Model:
        phase=0
        errors=[]
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            try:return self.respond(**kw)
            except Exception as error:self.errors.append(repr(error));raise
        def respond(self,**kw):
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            time.sleep(.08)  # External provider latency exposes extra recovery calls.
            if self.phase in (1,3):
                name='terminal_exec' if fallback and self.phase==3 else 'image_generate'
                result=call('request_tool_scope',{'capabilities':['local_shell' if name=='terminal_exec' else 'image_generation'],
                    'tool_names':[name],'required_tool_names':[name],'artifact_extensions':['.png']})
                result['content'][0]['id']='scope-'+str(self.phase)
                return result
            if self.phase==2:
                args={'prompt':self.prompt,'output_path':str(tmp_path/'requested.png')}
                if mode=='on':
                    result=call('code_mode',{'tool_names':['image_generate'],
                        'code':"call_tool('image_generate', "+repr(args)+")"})
                    result['content'][0]['id']='image-program'
                    return result
                return call('image_generate',args)
            if fallback and self.phase==5:return text('Completed the local chart.\nMEDIA:'+str(self.target))
            assert self.phase==4,'Terminal failure must not re-enter discovery or artifact repair'
            if rediscover:
                return call('request_tool_scope',{'capabilities':['image_generation'],
                    'tool_names':['image_generate'],'required_tool_names':['image_generate'],'artifact_extensions':['.png']})
            results=[json.JSONDecoder().raw_decode(b['text'])[0] for m in kw['messages'] if isinstance(m.get('content'),list)
                for part in m['content'] if isinstance(part,dict) and part.get('type')=='tool_result' and part.get('tool_use_id')=='scope-3'
                for b in part.get('content',[]) if b.get('type')=='text']
            assert results,kw['messages'][-2:]
            scope=results[-1]['output']
            assert 'image_generate' not in scope['available_tools']
            assert 'image_generate' in scope['unavailable_tools']
            if fallback:
                assert 'image_generate' not in scope['requested_required_tool_names']
                assert scope['required_tool_names']==['terminal_exec']
                script="from PIL import Image,ImageDraw;i=Image.new('RGB',(160,100),'white');d=ImageDraw.Draw(i);d.rectangle((10,40,50,90),fill='navy');d.rectangle((70,10,110,90),fill='teal');i.save("+repr(str(self.target))+")"
                args={'command':shlex.quote(sys.executable)+' -c '+shlex.quote(script),'output_paths':[str(self.target)]}
                if mode=='on':
                    result=call('code_mode',{'tool_names':['terminal_exec'],'code':"call_tool('terminal_exec', "+repr(args)+")"})
                    result['content'][0]['id']='renderer-program'
                    return result
                return call('terminal_exec',args)
            assert 'image_generate' in scope['unavailable_required_tool_names']
            return text('The image provider requires payment (HTTP 402). No image was created.')
    model=Model();samples=[]
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        for index,prompt in enumerate(['湖の家へ向かう猿の画像を作成してください。',
                'Crea una imagen de un mono camino a una casa junto al lago.',
                'Crée une image de singe allant vers une maison au bord du lac.']):
            model.phase=0;model.prompt=prompt;cid='web:terminal-image-'+str(index)
            root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);model.target=root/f'chart-{index}.png'
            started=time.monotonic();response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
            samples.append(time.monotonic()-started)
            assert not model.errors,model.errors
            assert response.status_code==200,response.text
            artifacts=response.json().get('artifacts',[])
            if fallback:
                assert len(artifacts)==1 and artifacts[0]['name']==model.target.name,response.text
                assert client.get(artifacts[0]['url']).content==model.target.read_bytes()
            else:
                assert '402' in response.json()['text'] and not artifacts,response.text
            turns=[e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn']
            assert turns[-1]['response_fulfilled'] is fallback
            assert sum(r['tool_name']=='image_generate' for r in turns[-1]['tool_results'])==1
    assert len(native_calls)==3 and not (tmp_path/'requested.png').exists()
    assert sum(samples)/len(samples)<3 and max(samples)<5,samples
    record_property('mean_terminal_failure_seconds',sum(samples)/len(samples))


@pytest.mark.parametrize('mode',['off','on'])
def test_repeated_terminal_provider_scope_stops_before_budget_prompt(tmp_path,monkeypatch,mode,record_property):
    test_terminal_generation_failure_stays_unavailable_without_recovery_loop(
        tmp_path,monkeypatch,mode,False,record_property,'urllib',rediscover=True)
