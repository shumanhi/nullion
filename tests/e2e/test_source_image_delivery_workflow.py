"""A verified replacement image can finish the real Web artifact workflow."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
from datetime import UTC, datetime

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('failed_attempts',[0,1,2,3])
@pytest.mark.parametrize('prompt',['Deliver one self-contained HTML report with source raster images.',
    'Entrega un informe HTML autocontenido con imágenes ráster de las fuentes.',
    '出典のラスター画像を埋め込んだ単一のHTMLレポートを添付してください。'])
def test_replacement_source_download_is_eligible_for_html_embedding(tmp_path,monkeypatch,prompt,failed_attempts):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import chat_store,web_app,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal,artifact_output_descriptor
    from nullion.artifact_evidence import artifact_structure_evidence
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    conversation='web:replacement-image';root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    first=root/'earlier-source.png';replacement=root/'verified-replacement.png';report=root/'report.html'
    for path in (first,replacement):Image.effect_noise((160,160),50).convert('RGB').save(path)
    def receipt(invocation,path,kind):
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{
            'artifact_paths':[str(path)],'image_paths':[str(path)],'saved_count':1,
            'artifact_descriptors':[artifact_output_descriptor(path,role='source',kind=kind)]})
    def fetch_page(url, timeout):
        raise RuntimeError('Fixture source returned HTTP 403')
    monkeypatch.setattr(tools,'_default_web_fetcher',fetch_page)
    monkeypatch.setattr(tools,'_fetch_browser_image_binary',lambda url,timeout:{'data':first.read_bytes(),'content_type':'image/png','source_url':url,'final_url':url})
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('browser_image_collect','Collect public source images',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,10),tools._build_browser_image_collect_handler() if failed_attempts else lambda invocation:receipt(invocation,first,'browser_image'))
    registry.register(ToolSpec('file_download','Download replacement public source',ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE,False,10),lambda invocation:receipt(invocation,replacement,'download'))
    registry.register(ToolSpec('file_write','Write report',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,20),
        _build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    class Provider:
        phase=0
        def create(self,**kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={row['name'] for row in kwargs.get('tools',[])};system=str(kwargs.get('system') or '')
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.phase),'name':name,'input':data}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver self-contained report with a verified source raster image','requirement_kind':'deliverable',
                     'evidence_subject_id':'report','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                data=json.loads(kwargs['messages'][0]['content'][0]['text'])
                records=[row for row in data['tool_evidence'] if isinstance(row.get('output'),dict) and row['output'].get('artifact_record_binding')]
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,
                    'unresolved_requirements':[],'material_requirements':[{**row,'satisfied':True,'draft_covered':True,
                    'evidence_invocation_ids':[records[0]['invocation_id']]} for row in data['material_requirement_contract']]}))
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['web','local_files'],
                'tool_names':['browser_image_collect','file_download','file_write'],'artifact_extensions':['.html'],
                'embedded_media_artifact_extensions':['.html'],'required_tool_names':['file_write']})
            if self.phase==2:
                return {'stop_reason':'tool_use','content':[
                    *[{'type':'tool_use','id':f'blocked-{index}','name':'browser_image_collect',
                       'input':{'page_url':'https://example.org/blocked','output_stem':f'variant-{index}'}}
                      for index in range(failed_attempts)],
                    {'type':'tool_use','id':'good-image','name':'browser_image_collect',
                     'input':{'image_urls':['https://example.org/earlier.png'],'quality_profile':'any'}}]}
            if failed_attempts==3 and self.phase>=3:return text('The source remains inaccessible.')
            if self.phase==3:return tool('file_download',{'url':'https://example.org/replacement.png'})
            if self.phase==4:return tool('file_write',{'path':str(report),'content':
                '<!doctype html><html><body><h1>Source report 31</h1><img src="'+str(replacement)+'"></body></html>'})
            return text('Report attached.\nMEDIA:'+str(report))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)));runtime.checkpoint()
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':conversation})
        assert response.status_code==200,response.text
        data=response.json()
        if failed_attempts==3:
            assert data['artifacts']==[],data
            turns=[row for row in runtime.store.list_conversation_events(conversation) if row.get('event_type')=='conversation.chat_turn']
            collected=[row for row in turns[-1]['tool_results'] if row['tool_name']=='browser_image_collect']
            assert len(collected)==3 and all(row['status']=='failed' for row in collected)
            assert not report.exists(), 'Exhausted source retries must not fabricate a report'
            return
        assert len(data['artifacts'])==1,data
        downloaded=client.get(data['artifacts'][0]['url']);assert downloaded.status_code==200
        assert b'Source report 31' in downloaded.content and b'data:image/' in downloaded.content
        assert artifact_structure_evidence(report)['decoded_embedded_raster_image_count']==1
        turns=[row for row in runtime.store.list_conversation_events(conversation) if row.get('event_type')=='conversation.chat_turn']
        collected=[row for row in turns[-1]['tool_results'] if row['tool_name']=='browser_image_collect']
        assert len([row for row in collected if row['status']=='failed'])==failed_attempts
        assert collected[-1]['status']=='completed'
        if failed_attempts:
            assert collected[0]['output']['failure_scope']=='source'
            assert collected[0]['output']['source_urls']==['https://example.org/blocked']
        written=next(row['output'] for row in turns[-1]['tool_results'] if row['tool_name']=='file_write')
        assert [item['path'] for item in written['embedded_html_images']]==[str(replacement)]
        assert all(artifact['name']=='report.html' for artifact in data['artifacts'])
