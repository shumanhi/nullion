"""One real downloaded image supports two distinct native file deliverables."""
import io
import json
import os
import zipfile
from datetime import UTC, datetime

import pytest

from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker native artifact/API boundary')


@pytest.mark.parametrize('prompt',['同じ公開画像を使って文書とスライドを作成してください。','Crea un documento y una presentación con la misma imagen pública.'])
def test_shared_download_is_support_not_a_duplicate_deliverable(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import tools,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass

    runtime=setup_runtime(tmp_path,monkeypatch)
    from nullion.policy import BoundaryKind,GLOBAL_PERMISSION_PRINCIPAL,BoundaryPolicyRule,PolicyMode
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-image-network',
        principal_id=GLOBAL_PERMISSION_PRINCIPAL,kind=BoundaryKind.OUTBOUND_NETWORK,
        mode=PolicyMode.ALLOW,selector='*',created_by='fixture',created_at=datetime.now(UTC)))
    for key,value in {'NULLION_ENV_FILE':str(tmp_path/'none.env'),'NULLION_MEMORY_ENABLED':'0',
        'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_CHAT_STARTUP_WARMUP':'0'}.items():monkeypatch.setenv(key,value)
    cid='web:shared-source';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True)
    docx,pptx=root/'book.docx',root/'slides.pptx'
    image=Image.new('RGB',(320,240),(29,140,79));encoded=io.BytesIO();image.save(encoded,format='PNG')
    downloaded=[];collected=[]
    final_url='https://cdn.example.test/shared.png'
    def network(url,timeout_seconds):
        downloaded.append(url)
        return {'source_url':url,'final_url':final_url,'status_code':200,'content_type':'image/png','data':encoded.getvalue()}
    monkeypatch.setattr(tools,'_fetch_browser_image_binary',network)
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    image_handler=tools._build_browser_image_collect_handler()
    def collect(invocation):
        result=image_handler(invocation);collected.append(result);return result
    registry.register(ToolSpec('browser_image_collect','Collect requested image',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,
        False,30,source_artifact_extensions=('.png',),source_artifact_only=True),collect)
    for name,builder in [('document_create',tools._build_document_create_handler),('presentation_create',tools._build_presentation_create_handler)]:
        registry.register(ToolSpec(name,'Create requested edition',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,30),
            builder(allowed_roots=[tmp_path],include_principal_workspace=False))
    class Provider:
        phase=0;reviews=0
        def create(self,**kwargs):
            system=str(kwargs.get('system',''));names={row['name'] for row in kwargs.get('tools',[])}
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.docx','.pptx'],
                    'search_coverage':{'scope':'find_any','constraints':[],'restricted_source_ids':[]},
                    'material_requirements':[{'requirement':name,'requirement_kind':'deliverable','evidence_subject_id':name,
                        'eligible_evidence_tool_names':[tool,'browser_image_collect']}
                        for name,tool in [('文書','document_create'),('Diapositivas','presentation_create')]]}))
            if 'semantic completion reviewer' in system:
                self.reviews+=1;packet=json.loads(kwargs['messages'][0]['content'][0]['text'])
                document = next(row for row in packet['artifact_evidence'] if row['filename'] == 'book.docx')
                assert document['structure']['footer_page_number_field_count'] == 1
                shared=next(row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name']=='browser_image_collect'
                    and (row.get('output') or {}).get('source_record_binding'))
                coverage=[]
                for requirement in packet['material_requirement_contract']:
                    proof=next(row['invocation_id'] for row in packet['tool_evidence']
                        if (row.get('output') or {}).get('artifact_record_binding')
                        and row['tool_name'] in requirement['eligible_evidence_tool_names'] and row['tool_name']!='browser_image_collect')
                    coverage.append({'requirement_id':requirement['requirement_id'],'satisfied':True,
                        'draft_covered':True,'evidence_invocation_ids':[proof,shared]})
                context=packet['search_coverage_context']
                return text_response(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,
                    'material_requirements':coverage,'unresolved_requirements':[],'retry_tool_names':[],
                    'search_coverage':{'conclusion':'positive','coverage_sufficient':True,'scope_matches_request':True,
                        'match_evidence_invocation_ids':[shared],'discovery_evidence_invocation_ids':context['discovery_invocation_ids'],
                        'constraints':[],'candidates':[{'candidate_id':row['candidate_id'],'disposition':'checked',
                            'reason':'Actual downloaded source','evidence_invocation_ids':row['observation_invocation_ids']}
                            for row in context['candidates']]}}))
            reply=workflow_provider_reply(kwargs,completion=False)
            if reply is not None:return reply
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':None,'confidence':1})
            if not names:return text_response('{}')
            self.phase+=1
            if self.phase==1:return tool_response('request_tool_scope',{'capabilities':['web','local_files'],
                'tool_names':['browser_image_collect','document_create','presentation_create'],
                'required_tool_names':['browser_image_collect','document_create','presentation_create'],
                'artifact_extensions':['.docx','.pptx'],'embedded_media_artifact_extensions':['.docx','.pptx']})
            if self.phase==2:return tool_response('browser_image_collect',{'image_urls':['https://assets.example.test/shared.png'],
                'quality_profile':'any','max_images':1})
            asset=collected[0].output['image_paths'][0]
            if self.phase==3:return tool_response('document_create',{'output_path':str(docx),'title':'Shared source',
                'paragraphs':['An illustrated document using the collected source.'],'image_paths':[asset], 'page_numbers':True})
            if self.phase==4:return tool_response('presentation_create',{'output_path':str(pptx),'title':'Shared source',
                'slides':[{'title':'Illustration','bullets':['The same collected source.'],'image_paths':[asset]}]})
            assert self.phase==5,'Verified media caused unnecessary new research or regeneration'
            return text_response('Listo.\nMEDIA:'+str(docx)+'\nMEDIA:'+str(pptx))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt+' .docx .pptx','conversation_id':cid})
        assert response.status_code==200,response.text
        body=response.json();assert {row['path'] for row in body.get('artifacts',[])}=={str(docx),str(pptx)},body
        for row in body['artifacts']:
            payload=client.get(row['url']).content
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                media = [archive.read(name) for name in archive.namelist() if '/media/' in name and not name.endswith('/')]
                assert media, row
                with Image.open(io.BytesIO(media[0])) as embedded:
                    assert embedded.size == (320, 240) and embedded.convert('RGB').getpixel((20, 20)) == (29, 140, 79)
                if row['path'].endswith('.docx'):
                    from xml.etree import ElementTree as ET
                    fields = [node.get('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}instr', '')
                        for name in archive.namelist() if name.startswith('word/footer') and name.endswith('.xml')
                        for node in ET.fromstring(archive.read(name)).iter() if node.tag.endswith('}fldSimple')]
                    assert any('PAGE' in value.split() for value in fields)
        turns=[row for row in runtime.store.list_conversation_events(cid) if row.get('event_type')=='conversation.chat_turn']
        assert turns[-1]['response_fulfilled'] is True
        assert model.reviews == 0 and len(downloaded) == 1 and (len(collected) == 1)
        from nullion.search_coverage import discovery_ledger
        ledger=discovery_ledger(turns[-1]['tool_results'])
        assert any(row['url']==final_url for row in ledger['observed_sources'])
