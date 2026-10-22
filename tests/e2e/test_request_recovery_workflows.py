"""Recovery through the real chat route, executor and persisted file boundary."""
import json
import os

import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


@pytest.mark.parametrize('writer',['file_patch','file_write'])
@pytest.mark.parametrize('authorized', [False,True])
@pytest.mark.parametrize('read_prompt,edit_prompt', [
    ('既存のメモを確認して。','メモの記録値を23に変更して。'),
    ('Revisa la nota existente.','Cambia el valor de la nota a 23.'),
    ('Read /workspace/files/note.txt and report its value without modifying it.',
     'Change the value in /workspace/files/note.txt to 23 and confirm it inline.')])
def test_discovery_recovery_preserves_original_action_plan(tmp_path,monkeypatch,authorized,read_prompt,edit_prompt,writer):
    prompt=edit_prompt if authorized else read_prompt
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from test_memory_and_document_recall_contract import setup,ScriptedProvider,response,tool
    runtime,registry=setup(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0')
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'home'/'workspaces'))
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    target=workspace_storage_roots_for_workspace('workspace_admin').files/'note.txt'
    target.parent.mkdir(parents=True,exist_ok=True);target.write_text('original value 17')
    # The provider supplies the typed plan for the read or edit request. The
    # runtime must preserve it during recovery. Only the provider is scripted.
    class Model(ScriptedProvider):
        phase=0
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_read']+([writer] if authorized else []),
                'required_tool_names':[writer] if authorized else ['file_read']})
            if self.phase==2:
                if not authorized:assert writer not in {row['name'] for row in kw['tools']}
                return tool('file_read',{'path':str(target)})
            if self.phase==3:return response('<tool_request>'+json.dumps({'tool':'request_tool_scope',
                'data':{'tool_scope':'local_files','tool_names':[writer],
                        'required_tool_names':[writer]}})+'</tool_request>')
            if self.phase==4:return tool(writer,{'path':str(target),**(
                {'old_string':'original value 17','new_string':'updated value 23'} if writer=='file_patch'
                else {'content':'updated value 23'})})
            return response('The note was updated to 23.' if authorized else 'The existing note says 17.')
    model=Model();runtime.model_client=model
    app=create_app(runtime,AgentOrchestrator(model_client=model),registry)
    with TestClient(app) as client:
        result=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:contract'})
        assert result.status_code==200,result.text
        payload=result.json()
    assert target.read_text()==('updated value 23' if authorized else 'original value 17'),payload
    turns=[e for e in runtime.store.list_conversation_events('web:contract') if e.get('event_type')=='conversation.chat_turn']
    assert turns and turns[-1].get('assistant_reply')
    writes=[r for r in turns[-1].get('tool_results',[]) if r.get('tool_name')==writer and r.get('status')=='completed']
    assert bool(writes) is authorized,payload
    assert payload['text'].strip()
    assert '<tool_request' not in payload['text'] and str(target) not in payload['text']
    if authorized:assert '23' in payload['text'],{'response':payload,'turn':turns[-1]}


@pytest.mark.parametrize('writer',['file_patch','file_write'])
def test_new_followup_can_discover_an_edit_without_inheriting_old_read_authority(tmp_path,monkeypatch,writer):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    from test_memory_and_document_recall_contract import setup,ScriptedProvider,response,tool
    runtime,registry=setup(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'home'/'workspaces'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0')
    target=workspace_storage_roots_for_workspace('workspace_admin').files/'source.txt'
    target.write_text('Recorded value: 17\n')
    class Model(ScriptedProvider):
        mode='read';phase=0
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:
                if self.mode=='edit' and 'relationship' in str(kw.get('system','')) and 'target_index' in str(kw.get('system','')):
                    return response(json.dumps({'relationship':'follow_up','effect':'continue','confidence':1,'target_index':0}))
                return aux
            self.phase+=1
            names={row['name'] for row in kw.get('tools',[])}
            if self.phase==1:
                assert 'request_tool_scope' in names and 'terminal_exec' in names,names
                if self.mode=='edit':assert writer not in names
                return tool('request_tool_scope',{'capabilities':['local_files'],
                    'tool_names':['file_read']+([writer] if self.mode=='edit' else []),
                    'required_tool_names':[writer] if self.mode=='edit' else ['file_read']})
            if self.phase==2:return tool('file_read',{'path':str(target)})
            if self.mode=='read':return response('The recorded value is 17. The note is unchanged.')
            if self.phase==3:return tool(writer,{'path':str(target),**(
                {'old_string':'17','new_string':'23'} if writer=='file_patch' else {'content':'Recorded value: 23\n'})})
            return response('The recorded value is now 23. No attachment was sent.')
    model=Model();runtime.model_client=model
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        first=client.post('/api/chat',json={'text':'Read /workspace/files/source.txt and report the value.',
            'conversation_id':'web:followup'}).json()
        assert '17' in first['text'] and first['artifacts']==[],first
        assert target.read_text()=='Recorded value: 17\n'
        model.mode='edit';model.phase=0
        second=client.post('/api/chat',json={'text':'/workspace/files/source.txt の記録値を23に変更して。返答だけで、添付は不要。',
            'conversation_id':'web:followup'}).json()
        assert '23' in second['text'] and second['artifacts']==[],second
    assert target.read_text()=='Recorded value: 23\n'
    turns=[r for r in runtime.store.list_conversation_events('web:followup') if r.get('event_type')=='conversation.chat_turn']
    assert len(turns)==2
    first_scope=next(r['output'] for r in turns[0]['tool_results'] if r['tool_name']=='request_tool_scope')
    new_scope=next(r['output'] for r in turns[1]['tool_results'] if r['tool_name']=='request_tool_scope')
    assert writer not in first_scope['required_tool_names']
    assert writer in new_scope['required_tool_names']
    assert any(r['tool_name']==writer and r['status']=='completed' for r in turns[-1]['tool_results'])
    from nullion.runtime import bootstrap_persistent_runtime
    restored=bootstrap_persistent_runtime(runtime.checkpoint_path)
    durable=[r for r in restored.store.list_conversation_events('web:followup') if r.get('event_type')=='conversation.chat_turn']
    assert len(durable)==2 and '23' in durable[-1]['assistant_reply']


def test_explicit_delivery_updates_original_file_and_downloads_updated_bytes(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.workspace_storage import workspace_storage_roots_for_workspace
    from test_memory_and_document_recall_contract import setup,ScriptedProvider,response,tool
    runtime,registry=setup(tmp_path,monkeypatch)
    monkeypatch.setenv('NULLION_DATA_DIR',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'home'/'workspaces'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP','0')
    target=workspace_storage_roots_for_workspace('workspace_admin').files/'source.txt'
    target.write_text('Recorded value: 17\n')
    class Model(ScriptedProvider):
        phase=0
        def create(self,**kw):
            aux=self.auxiliary(kw)
            if aux is not None:return aux
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],
                'tool_names':['file_read','file_write'],'required_tool_names':['file_write'],
                'artifact_extensions':['.txt'],'requested_outcome':'artifact'})
            if self.phase==2:return tool('file_write',{'path':str(target),'content':'Recorded value: 23\n'})
            return response('The recorded value is now 23. The updated note is attached.')
    model=Model();runtime.model_client=model
    with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
        result=client.post('/api/chat',json={'text':'/workspace/files/source.txt の記録値を23に変更して、更新後のファイルを添付して。',
            'conversation_id':'web:edit-delivery'})
        assert result.status_code==200,result.text
        payload=result.json()
        assert payload['text'].strip() and len(payload['artifacts'])==1, {'reply':payload,'tools':[e.get('tool_results') for e in runtime.store.list_conversation_events('web:edit-delivery') if e.get('event_type')=='conversation.chat_turn']}
        downloaded=client.get(payload['artifacts'][0]['url'])
        assert downloaded.status_code==200 and downloaded.content==target.read_bytes()==b'Recorded value: 23\n'
    turns=[r for r in runtime.store.list_conversation_events('web:edit-delivery') if r.get('event_type')=='conversation.chat_turn']
    write=next(r for r in turns[-1]['tool_results'] if r['tool_name']=='file_write')
    assert write['status']=='completed' and write['output']['path']==str(target)
    assert write['output']['artifact_path']!=str(target)
