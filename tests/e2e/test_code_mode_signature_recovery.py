"""Bad host-call syntax remains correctable after a committed native write."""
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


def test_signature_correction_delivers_the_once_written_file(tmp_path, monkeypatch, record_property):
    from datetime import UTC, datetime
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_file_write_handler
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('files', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.FILESYSTEM_ACCESS, PolicyMode.ALLOW, str(tmp_path)+'/*', 'fixture', datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    writer = _build_file_write_handler(tmp_path, allowed_roots=[tmp_path])
    writes = []
    def write(inv):
        writes.append(inv.invocation_id)
        return writer(inv)
    registry.register(ToolSpec('file_write', 'Write text', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5), write)
    class Model:
        phase = 0
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None:
                return protocol
            if 'select_attachment_format' in {r['name'] for r in kw.get('tools', [])}:
                return call('select_attachment_format', {'extension': None, 'confidence': 1})
            if not kw.get('tools'):
                return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['local_files'], 'tool_names': ['file_write'],
                    'required_tool_names': ['file_write'], 'artifact_extensions': ['.html']})
            if self.phase == 2:
                args = {'path': str(self.target), 'content': '<!doctype html><html><body><h1>Verified 37</h1><p>The completed native write is retained through correction and delivered exactly once.</p></body></html>'}
                return call('code_mode', {'tool_names': ['file_write'],
                    'code': "saved=call_tool('file_write', "+repr(args)+")\ncall_tool('file_write', {}, unsupported=True)"})
            if self.phase == 3:
                packets = [json.JSONDecoder().raw_decode(b['text'])[0] for m in kw['messages'] if isinstance(m.get('content'), list)
                    for p in m['content'] if isinstance(p, dict) and p.get('type') == 'tool_result'
                    and p.get('tool_use_id') == 'code_mode-id' for b in p.get('content', []) if b.get('type') == 'text']
                assert packets[-1]['output']['execution_error']['session_usable'] is True
                result = call('code_mode', {'tool_names': [], 'code': "saved['output']['artifact_path']"})
                result['content'][0]['id'] = 'reuse-the-receipt'
                return result
            assert self.phase == 4
            return text('Completed.\nMEDIA:'+str(self.target))
    model = Model()
    samples = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['HTMLを作成', 'Crea el HTML', 'Créez le HTML']):
            cid = 'web:signature-correction-'+str(index)
            root = artifact_root_for_principal(cid)
            root.mkdir(parents=True, exist_ok=True)
            model.phase = 0
            model.target = root/f'verified-{index}.html'
            started = time.monotonic()
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
            samples.append(time.monotonic()-started)
            artifacts = response.json().get('artifacts', [])
            assert len(artifacts) == 1, response.text
            assert b'Verified 37' in client.get(artifacts[0]['url']).content
            assert len(writes) == index+1
            turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True
    assert sum(samples)/len(samples) < 3 and max(samples) < 5, samples
    record_property('mean_signature_recovery_seconds', sum(samples)/len(samples))
