"""Real API/runtime/compaction: overlapping reads retain their unique facts."""
import json
import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def test_overlapping_reads_keep_middle_fact_visible_without_another_source_read(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, GLOBAL_PERMISSION_PRINCIPAL, PolicyMode
    from nullion.preferences import save_preferences
    from nullion.tools import ToolRegistry, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    from nullion.web_app import create_app
    from datetime import UTC, datetime
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'outbound_request_mode':'allow_all', 'emoji_level':'none', 'markdown_style':'plain'})
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('read',GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    body = 'x'*7000 + 'MIDDLE-TARGET:17' + 'y'*9000
    reads = []
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'result_kind':'browser_page_collection', 'url':'https://example.test/source', 'observations':body,
            'documents':[{'url':'https://example.test/source','text':body}],
            'source_record_collections':[{'path':['documents'],'cardinality':'many',
                'identity_fields':['url'],'context_fields':['text']}],
            'large_unrelated_field':'z'*100000,
        })
    registry = ToolRegistry()
    registry.register(ToolSpec('browser_read_pages','Read source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,3,
        capability_tags=('web',),input_schema={'type':'object','properties':{'pages':{'type':'array'}},'required':['pages']}), read)
    class Model:
        step = 0
        def create(self, **kwargs):
            try:
                return self.respond(**kwargs)
            except Exception:
                import traceback
                print(traceback.format_exc())
                raise
        def respond(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            if not reads and 'browser_read_pages' not in {t['name'] for t in kwargs.get('tools', [])}:
                return call('request_tool_scope', {'capabilities':['web'], 'tool_names':['browser_read_pages']})
            self.step += 1
            def invoke(name, arguments):
                response = call(name, arguments)
                response['content'][0]['id'] += f'-{self.step}'
                return response
            if self.step == 1:
                return invoke('browser_read_pages', {'pages':[{'url':'https://example.test/source'}]})
            if self.step in (2, 3, 4):
                start = (self.step-2)*200
                return invoke('inspect_retained_evidence', {'requests': [
                    {'invocation_id':reads[0], 'json_pointer':'/observations', 'offset':offset, 'length':12000}
                    for offset in (start, start+100)
                ]})
            if self.step == 5:
                return invoke('inspect_retained_evidence', {'requests': [{
                    'invocation_id':reads[0], 'json_pointer':'/observations', 'offset':9000, 'length':12000,
                }]})
            context = json.dumps(kwargs['messages'])
            assert self.step == 6 and 'MIDDLE-TARGET:17' in context, context[-3000:]
            return text('The captured middle value is 17.')
    model = Model()
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        response = client.post('/api/chat', json={
            'text':'https://example.test/source の資料を読み、中央の値を確認してください。', 'conversation_id':'web:overlap',
        }).json()
    assert response['text'] == 'The captured middle value is 17.', response
    assert len(reads) == 1 and model.step == 6
    turn = runtime.store.list_recent_conversation_events('web:overlap', event_type='conversation.chat_turn', limit=1)[0]
    assert sum(r['tool_name']=='inspect_retained_evidence' for r in turn['tool_results']) == 4
