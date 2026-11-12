"""A corrective file-path hint cannot grant access to an unrelated file."""
import json
import os

import pytest

from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def test_intentional_different_file_still_requires_normal_boundary_approval(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_file_write_handler, _build_file_read_handler
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    known = artifact_root_for_principal('web:artifact-reference-boundary')/'known.csv'
    known.parent.mkdir(parents=True,exist_ok=True)
    unknown = '/unapproved/another.csv'
    reads = []

    class Model:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None:
                return protocol
            names = {tool['name'] for tool in kwargs.get('tools', [])}
            if 'select_attachment_format' in names:
                return {'stop_reason':'tool_use', 'content':[{'type':'tool_use', 'id':'format',
                    'name':'select_attachment_format', 'input':{'extension':'.csv','confidence':1}}]}
            if not names:
                return {'stop_reason':'end_turn','content':[{'type':'text','text':'{}'}]}
            self.phase += 1
            if self.phase == 1:
                name, arguments = 'request_tool_scope', {'capabilities':['local_files'],
                    'tool_names':['file_write','file_read'], 'required_tool_names':['file_write','file_read'],
                    'artifact_extensions':['.csv']}
            elif self.phase == 2:
                name, arguments = 'file_write', {'path':str(known), 'content':'Value\n37\n'}
            else:
                assert self.phase in {3, 4}
                if self.phase == 4:
                    assert 'artifact_read_reference_selection_required' in json.dumps(kwargs['messages'])
                name, arguments = 'file_read', {'path':unknown}
            return {'stop_reason':'tool_use', 'content':[{'type':'tool_use',
                'id':str(self.phase), 'name':name, 'input':arguments}]}

    model = Model()
    app, runtime, _ = setup(tmp_path, monkeypatch, model)
    registry = app.state.live_tracker_engine.registry
    reader = _build_file_read_handler(tmp_path, allowed_roots=[tmp_path])
    def read(invocation):
        reads.append(invocation)
        return reader(invocation)
    for name, effect, handler in [
        ('file_write', ToolSideEffectClass.WRITE, _build_file_write_handler(tmp_path, allowed_roots=[tmp_path])),
        ('file_read', ToolSideEffectClass.READ, read),
    ]:
        registry.register(ToolSpec(name, name, ToolRiskLevel.LOW, effect, False, 10), handler)
    with TestClient(app) as client:
        response = client.post('/api/chat', json={'text':'値37のCSVを作り、別の指定ファイルも確認してください。',
            'conversation_id':'web:artifact-reference-boundary'})
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload['type'] == 'approval_required' and payload['request_kind'] == 'boundary_policy', payload
        pending = runtime.store.list_approval_requests()
        assert len(pending) == 1 and pending[0].resource == unknown, [(p.action,p.resource) for p in pending]
        assert not reads and model.phase == 4
        assert known.read_bytes() == b'Value\n37\n'


@pytest.mark.asyncio
async def test_delegated_adapter_preserves_repair_choices_and_reads_the_exact_file(tmp_path, monkeypatch):
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_file_write_handler, _build_file_read_handler
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    known=artifact_root_for_principal('operator')/'values.csv'
    known.parent.mkdir(parents=True,exist_ok=True)
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])
    reads=[];results=[]
    reader=_build_file_read_handler(tmp_path,allowed_roots=[tmp_path])
    def read(invocation):
        reads.append(invocation.arguments['path'])
        return reader(invocation)
    for name,effect,handler in [
        ('file_write',ToolSideEffectClass.WRITE,_build_file_write_handler(tmp_path,allowed_roots=[tmp_path])),
        ('file_read',ToolSideEffectClass.READ,read),
    ]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,10),handler)
    tools={tool.name:tool for tool in nullion_tools_as_langchain_tools(registry,
        allowed_tools=['file_write','file_read'],principal_id='operator',cleanup_scope='reference',
        tool_result_callback=results.append)}
    await tools['file_write'].ainvoke({'path':str(known),'content':'Value\n37\n'})
    wrong=json.loads(await tools['file_read'].ainvoke({'path':'/outside/h839f.csv'}))
    assert wrong['output']['recovery']['candidate_paths']==[str(known)]
    assert not reads
    await tools['file_read'].ainvoke({'path':wrong['output']['recovery']['candidate_paths'][0]})
    assert reads==[str(known)] and results[-1].status=='completed'
    assert '37' in json.dumps(results[-1].output) and known.read_bytes()==b'Value\n37\n'
