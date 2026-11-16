"""Real shared Web/Telegram execution keeps completed evidence through repair."""
import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('repair', ['projection', 'inspection'])
def test_output_recovery_reuses_completed_work_without_extra_model_repair(
        tmp_path, monkeypatch, surface, repair, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    registry = ToolRegistry()
    reads = []
    source = {'hotel': {'name': 'Casa 37', 'check_in': '2031-04-08', 'source_url': 'https://source.test/stay'},
        'url': 'https://source.test/stay', 'body': 'retained context '*1800,
        'source_record_collections': [{'path': ['hotel'], 'cardinality': 'one',
            'identity_fields': ['name'], 'context_fields': ['check_in', 'source_url']}]}
    def external(invocation):
        reads.append(invocation.invocation_id)
        # Re-entering this expensive source dependency must fail the count and
        # measured latency contract instead of being hidden by a fast fake.
        if len(reads) > expected_reads[0]:
            time.sleep(5)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', source)
    expected_reads = [1]
    registry.register(ToolSpec('read_fixture', 'Read selected source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 5, capability_tags=('web',)), external)

    class Model:
        phase = 0
        errors = []
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            try:
                return self.respond(kwargs)
            except Exception as error:
                self.errors.append(repr(error))
                raise
        def respond(self, kwargs):
            if not kwargs.get('tools'):
                return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['read_fixture'], 'required_tool_names': ['read_fixture']})
            if self.phase == 2:
                args = {'tool_names': ['read_fixture'],
                    'code': "saved=call_tool('read_fixture', {})\nsaved['output']"}
                if repair == 'projection':
                    args['output_fields'] = ['/output/result']
                return call('code_mode', args)
            if repair == 'inspection' and self.phase == 3:
                code = (
                    "good=call_tool('inspect_retained_evidence', {'requests':[{'invocation_id':saved['invocation_id'],'json_pointer':'/url','offset':0,'length':1000}]})\n"
                    "bad=call_tool('inspect_retained_evidence', {'requests':[{'invocation_id':'unowned-reference','json_pointer':'/hotel','offset':0,'length':1000}]}, on_error='return')\n"
                    "refs=[r for r in bad['output']['owned_references'] if r['tool_name']=='read_fixture']\n"
                    "assert len(refs)==1\n"
                    "request=refs[0]['inspection_request']\nrequest['json_pointer']='/hotel'\n"
                    "fixed=call_tool('inspect_retained_evidence', {'requests':[request]})\n"
                    "{'source':good['output']['windows'][0]['content'],'hotel':fixed['output']['windows'][0]['content']}"
                )
                reply = call('code_mode', {'tool_names': ['inspect_retained_evidence'], 'code': code})
                reply['content'][0]['id'] = 'only-local-repair'
                return reply
            expected = 3 if repair == 'projection' else 4
            assert self.phase == expected, 'Do not start another model repair or source workflow'
            if repair == 'projection':
                packets = [json.loads(b['content'][0]['text']) for m in kwargs['messages']
                    if isinstance(m.get('content'), list) for b in m['content']
                    if isinstance(b, dict) and b.get('type') == 'tool_result'
                    and b.get('tool_use_id') == 'code_mode-id']
                output = packets[-1]['output']
                assert output['projection_error']['repair_scope'] == 'output_projection'
                assert 'Casa 37' in json.dumps(output['native_observations'])
                assert output['native_observations'][0]['inspection_request']['invocation_id'] == reads[-1]
            else:
                assert 'Casa 37' in json.dumps(kwargs['messages'])
            return text('Casa 37, check-in 2031-04-08. https://source.test/stay')

    model = Model()
    if surface == 'telegram':
        from nullion.config import load_settings
        from nullion.telegram_app import ChatOperatorService
        settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN': '123456:abcdefghijklmnopqrstuvwxyz',
            'NULLION_TELEGRAM_OPERATOR_CHAT_ID': '123', 'NULLION_TELEGRAM_CHAT_ENABLED': 'true'})
        service = ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
            operator_chat_id='123', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    samples = []
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        for index, prompt in enumerate(['宿泊情報を確認してください。', 'Comprueba mi alojamiento.', 'تحقق من مكان الإقامة.']):
            model.phase = 0
            expected_reads[0] = len(reads) + 1
            started = time.monotonic()
            cid = 'web:output-recovery-'+str(index)
            if surface == 'web':
                response = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
                assert response.status_code == 200, response.text
                visible = response.json()['text']
            else:
                sent = []
                class Message:
                    text = prompt
                    message_id = 8000+index
                    reply_to_message = None
                    chat = SimpleNamespace(id=123, send_action=lambda **kwargs: asyncio.sleep(0))
                    async def reply_text(self, value, **kwargs):
                        sent.append(value)
                        return SimpleNamespace(message_id=9000+len(sent))
                asyncio.run(service.on_message(SimpleNamespace(update_id=8000+index, message=Message()),
                    SimpleNamespace(bot=None)))
                assert sent
                visible = sent[-1]
                cid = 'telegram:123'
            samples.append(time.monotonic()-started)
            assert not model.errors, model.errors
            assert 'Casa 37' in visible and '2031-04-08' in visible
            assert len(reads) == expected_reads[0]
            assert model.phase == (3 if repair == 'projection' else 4)
            turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
            results = turn['tool_results']
            assert turn['response_fulfilled'] is True
            assert len([r for r in results if r['tool_name'] == 'read_fixture']) == 1
            programs = [r for r in results if r['tool_name'] == 'code_mode']
            assert all(r['status'] == 'completed' for r in programs)
            if repair == 'inspection':
                inspections = [r for r in results if r['tool_name'] == 'inspect_retained_evidence']
                assert [r['status'] for r in inspections] == ['completed', 'failed', 'completed']
                # The good source window was not repeated during correction.
                assert programs[-1]['output']['value']['source'] == source['url']
    assert sum(samples)/len(samples) < 4 and max(samples) < 6, samples
    record_property('mean_output_recovery_seconds', sum(samples)/len(samples))
