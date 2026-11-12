"""Completed different-format files survive a later plan stall/review outage."""
import json,os
import pytest
from tests.workflow_provider_protocol import workflow_provider_reply
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real Web/file/store boundary')


@pytest.mark.parametrize('prompt',['Entrega cuatro archivos, uno en cada formato solicitado.','指定された各形式のファイルを一つずつ、四つ渡してください。'])
@pytest.mark.parametrize('repeat_existing',[False,True])
def test_native_pack_survives_unexecuted_duplicate_and_review_timeout(tmp_path,monkeypatch,prompt,repeat_existing):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    for k,v in {'HOME':str(tmp_path),'NULLION_HOME':str(tmp_path/'home'),'NULLION_ENV_FILE':str(tmp_path/'none.env'),
        'NULLION_MEMORY_ENABLED':'0','NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_CHAT_STARTUP_WARMUP':'0'}.items():monkeypatch.setenv(k,v)
    monkeypatch.setattr(chat_store,'_store',None)
    cid='web:terminal-pack';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    bodies={'.csv':'Subject,Value\nCurrent,37\n','.html':'<!doctype html><html><head><meta charset="utf-8"><title>Current measurement</title></head><body><main><h1>Current 37</h1><p>The requested current measurement is 37 units. This generated HTML edition presents the same recorded value as the companion data table, illustration, and text edition.</p><p>These are synthetic test measurements used to verify complete package delivery and exact download bytes.</p></main></body></html>',
        '.svg':'<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100"><text x="10" y="30">Current 37</text></svg>',
        '.txt':'Current 37\n'}
    files={str(root/('result'+ext)):text for ext,text in bodies.items()}
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);writes=[]
    writer=_build_file_write_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):result=writer(inv);writes.append(result);return result
    registry.register(ToolSpec('file_write','Write requested file',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),write)
    class Model:
        phase=0;denied=0;reviews=0
        def create(self,**kw):
            system=str(kw.get('system',''));names={r['name'] for r in kw.get('tools',[])}
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'phase-'+str(self.phase)+'-'+name,'name':name,'input':args}]}
            if 'Workflow action preflight.' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                # The real action reviewer sees native completed files. The
                # external model rejects an unrequested fifth copy, not them.
                if self.phase>=3:
                    self.denied+=1
                    return text(json.dumps({'decision':'revise','issues':['Retain the completed four outputs. This extra copy is not requested.']}))
                return text(json.dumps({'decision':'proceed','issues':[]}))
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':list(bodies),
                    'material_requirements':[{'requirement':'Deliver the requested '+ext+' file','requirement_kind':'deliverable',
                        'evidence_subject_id':ext,'eligible_evidence_tool_names':['file_write']} for ext in bodies]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;raise TimeoutError('External review service unavailable')
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if 'select_artifact_delivery' in names:return tool('select_artifact_delivery',{'status':'selected','selected_ids':['a'+str(i) for i in range(4)],'reason':'Four requested formats.'})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],
                'required_tool_names':['file_write'],'artifact_extensions':list(bodies)})
            if self.phase==2:return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'file-'+str(i),'name':'file_write','input':{'path':p,'content':body}} for i,(p,body) in enumerate(files.items())]}
            assert self.phase<=6,'Terminal repair restarted artifact generation'
            if repeat_existing:return {'stop_reason':'tool_use','content':[{'type':'tool_use',
                'id':f'repeat-{self.phase}-{i}','name':'file_write','input':{'path':p,'content':'Unrequested replacement'}}
                for i,p in enumerate(files)]}
            return tool('file_write',{'path':next(iter(files)) if repeat_existing else str(root/'unrequested-copy.txt'),
                'content':'Unrequested replacement'})
    model=Model();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        result=client.post('/api/chat',json={'text':prompt+' .csv .html .svg .txt','conversation_id':cid})
        assert result.status_code==200,result.text
        data=result.json();assert {r['path'] for r in data['artifacts']}==set(files),json.dumps({'response':data,'writes':[{'status':r.status,'output':r.output,'error':r.error} for r in writes]},ensure_ascii=False)
        for row in data['artifacts']:assert client.get(row['url']).content==files[row['path']].encode()
        assert (model.denied==1 if repeat_existing else 2<=model.denied<=3) and model.reviews and len(writes)==4
        assert not (root/'unrequested-copy.txt').exists()
        turns=[r for r in runtime.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn']
        assert turns[-1].get('response_fulfilled') is not True
        from nullion.artifact_lifecycle import delivery_manifest
        assert set(delivery_manifest(turns[-1]['tool_results']))==set(files)
