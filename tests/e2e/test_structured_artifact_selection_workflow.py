"""Ambiguous generated files are selected through typed state, then reviewed."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('draft_lists_all',[False,True])
@pytest.mark.parametrize('extension',['.csv','.html','.svg'])
@pytest.mark.parametrize('prompt',['Make a 10-unit working copy, then a 12-unit revision; deliver only the 12-unit file.',
    'Crea una copia de 10 unidades y otra de 12; entrega solo el archivo de 12 unidades.',
    '10個の作業用コピーと12個の改訂版を作成し、12個のファイルだけを渡してください。'])
def test_typed_final_selection_delivers_one_real_file_without_regenerating(tmp_path,monkeypatch,extension,prompt,draft_lists_all):
    _run(tmp_path,monkeypatch,extension,prompt,'selected',draft_lists_all=draft_lists_all)


@pytest.mark.parametrize('mode',['unknown_id','none_ready','changed_file'])
def test_invalid_or_stale_selection_cannot_deliver_or_restart_generation(tmp_path,monkeypatch,mode):
    _run(tmp_path,monkeypatch,'.csv','Create two revisions and deliver only the approved 12-unit file.',mode)


def test_typed_selection_preserves_requested_multi_format_package(tmp_path,monkeypatch):
    _run(tmp_path,monkeypatch,'.csv','承認された12個のCSVとHTMLだけを渡してください。','selected',package=True)


def test_typed_selection_preserves_two_requested_same_format_outputs(tmp_path,monkeypatch):
    _run(tmp_path,monkeypatch,'.csv','Deliver the two separate approved CSV reports with value 12; keep the working copy internal.',
         'selected',package=True,draft_lists_all=True,same_format_package=True)


@pytest.mark.parametrize('mode',['repair_then_select','selected_bad_csv','repair_stays_invalid','repair_unique'])
@pytest.mark.parametrize('prompt',['Deliver the approved CSV and HTML package.', '承認済みCSVとHTMLを渡してください。'])
def test_native_table_validation_repairs_only_invalid_component_once(tmp_path,monkeypatch,mode,prompt):
    _run(tmp_path,monkeypatch,'.csv',prompt,mode,package=True)


@pytest.mark.parametrize('mode',['scope_stall_selected','scope_stall_invalid'])
def test_stalled_discovery_stops_generation_but_selects_completed_package(tmp_path,monkeypatch,mode):
    _run(tmp_path,monkeypatch,'.csv','承認済みCSVとHTMLを渡してください。',mode,package=True)


def _run(tmp_path,monkeypatch,extension,prompt,mode,package=False,draft_lists_all=False,same_format_package=False):
    from fastapi.testclient import TestClient
    from nullion import chat_store,web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.artifact_lifecycle import delivery_manifest
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    conversation='web:typed-selection';root=artifact_root_for_principal(conversation);root.mkdir(parents=True,exist_ok=True)
    earlier=root/(uuid4().hex+extension);approved=root/(uuid4().hex+extension)
    support=root/(uuid4().hex+'.txt'); extra=root/(uuid4().hex+(extension if same_format_package else '.html'))
    repair_mode=mode in {"repair_then_select","selected_bad_csv","repair_stays_invalid","repair_unique"}
    expected=[approved,extra] if package else [approved]
    formats=[extension,extra.suffix] if package else [extension]
    def body(value,fmt=extension):
        return {'.csv':f'Name,Value\nRecord,{value}\n',
            '.html':f'<!doctype html><html><body><h1>Record</h1><p>Value={value}</p></body></html>',
            '.svg':f'<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100"><text x="4" y="40">Value={value}</text></svg>'}[fmt]
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('file_write','Write requested files',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),
        _build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    class Provider:
        phase=0;selections=0;reviews=0;sibling_stat=None
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={row['name'] for row in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,data):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name+str(self.phase),'name':name,'input':data}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver the approved 12-unit revision '+fmt+' output '+str(index),'requirement_kind':'deliverable',
                     'evidence_subject_id':'approved'+str(index),'eligible_evidence_tool_names':['file_write']}
                    for index,fmt in enumerate(formats)]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;payload=json.loads(kw['messages'][0]['content'][0]['text'])
                assert len(payload['artifact_evidence'])==len(expected) and all('12' in e['content_preview'] for e in payload['artifact_evidence'])
                records=[r['invocation_id'] for r in payload['tool_evidence'] if r.get('output',{}).get('result_kind')=='artifact_receipt']
                assert len(records)==len(expected),payload
                return text(json.dumps({'disposition':'complete','unresolved_requirements':[],'retry_tool_names':[],
                    'all_material_requirements_enumerated':True,'material_requirements':[
                        {**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[records[index]]}
                        for index,row in enumerate(payload['material_requirement_contract'])]}))
            if 'select_artifact_delivery' in names:
                self.selections+=1;assert names=={'select_artifact_delivery'}
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                candidates=json.loads(raw)
                assert len(candidates)==len(expected)+(mode!='repair_unique') and all(c['extension'] in formats for c in candidates),candidates
                selected=[c['candidate_id'] for c in candidates if '12' in c.get('content_preview','')]
                assert len(selected)==len(expected),candidates
                if repair_mode:
                    if self.selections==1:self.sibling_stat=extra.stat().st_mtime_ns
                    else:assert extra.stat().st_mtime_ns==self.sibling_stat
                    if mode!='selected_bad_csv' and (self.selections==1 or mode=='repair_stays_invalid'):
                        return tool('select_artifact_delivery',{'status':'none_ready','selected_ids':[],
                            'repair_ids':[c['candidate_id'] for c in candidates if c['extension']=='.csv' and 'METADATA' in c.get('content_preview','')],
                            'reason':'The requested final CSV has inconsistent columns.'})
                if mode=='unknown_id':selected=['unregistered-file']
                if mode=='changed_file':approved.write_text(body(99))
                return tool('select_artifact_delivery',{'status':'none_ready' if mode=='none_ready' else 'selected',
                    'selected_ids':[] if mode=='none_ready' else selected,'reason':'The selected content has the requested approved value.'})
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':extension,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],
                'required_tool_names':['file_write'],'artifact_extensions':formats})
            if self.phase==2:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(i),'name':'file_write',
                    'input':{'path':str(path),'content':value}} for i,(path,value) in enumerate([
                        *([(earlier,body(10))] if mode!='repair_unique' else []),(approved,("METADATA\n" if repair_mode or mode=='scope_stall_invalid' else "")+body(12)),*([(extra,body(12,extra.suffix).replace('Record','Other') if same_format_package else body(12,extra.suffix))] if package else []),(support,'Internal preparation notes')])]}
            if repair_mode and self.phase==4:
                return tool('file_write',{'path':str(approved),'content':('METADATA\n' if mode=='repair_stays_invalid' else '')+body(12)})
            if mode in {'scope_stall_selected','scope_stall_invalid'} and self.phase in {3,4,5}:
                return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],
                    'required_tool_names':['file_write'],'artifact_extensions':formats})
            assert self.phase==(5 if repair_mode and self.selections else 3),'Selection or terminal failure restarted generation'
            draft='Prepared the requested file.'
            if draft_lists_all:
                draft+='\n'+'\n'.join('MEDIA:'+str(path) for path in [earlier,*expected])
            return text(draft)
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt+' '+' '.join(formats),'conversation_id':conversation})
        assert response.status_code==200,response.text
        data=response.json();assert not data.get('suspended_for_approval'),data
        assert model.selections==(2 if repair_mode and mode!='repair_unique' else 1) and model.phase==(5 if repair_mode or mode in {'scope_stall_selected','scope_stall_invalid'} else 3)
        turns=[e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type')=='conversation.chat_turn']
        assert len([r for r in turns[-1]['tool_results'] if r['tool_name']=='file_write'])==len(expected)+2+(1 if repair_mode else 0)-(mode=='repair_unique')
        selected=delivery_manifest(turns[-1]['tool_results'])
        if mode in {'selected','repair_then_select','selected_bad_csv','repair_unique','scope_stall_selected'}:
            assert len(data['artifacts'])==len(expected) and selected==tuple(str(p) for p in expected),data
            if mode=='scope_stall_selected':assert turns[-1]['response_fulfilled'] is True,turns[-1]
            for artifact in data['artifacts']:
                download=client.get(artifact['url']);assert download.status_code==200
                expected_body=body(12,Path(artifact['name']).suffix)
                if same_format_package and Path(artifact['path'])==extra:expected_body=expected_body.replace('Record','Other')
                assert download.content==expected_body.encode()
            assert model.reviews == 0
            history=client.get('/api/chat/history/'+conversation,params={'turn_ids':json.dumps([turns[-1]['turn_id']])}).json()
            assert len(history['terminal_replies'])==1 and len(history['terminal_replies'][0]['artifacts'])==len(expected)
        else:
            assert not data['artifacts'] and (not selected) and (model.reviews == 0), data
            diagnostic = next(row['output']['artifact_delivery_manifest']['selection_diagnostic']
                for row in reversed(turns[-1]['tool_results']) if row.get('output', {}).get('artifact_delivery_manifest'))
            assert diagnostic['candidate_count']==len(expected)+1
            assert diagnostic['valid_response'] is (mode!='unknown_id')
            assert diagnostic['status']==('none_ready' if mode in {'none_ready','repair_stays_invalid'} else 'selected')
            if repair_mode:assert 'format validation after one repair attempt' in data.get('text','')
            if mode=='changed_file': assert diagnostic['files_unchanged'] is False
