"""Real chat, browser, SQLite and Code Mode; deterministic external site/model."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('mode', ['off', 'on'])
@pytest.mark.parametrize('count', [3, 9])
def test_research_stops_at_verified_requested_coverage_without_a_page_cap(tmp_path, monkeypatch, mode, count, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.preferences import save_preferences
    from nullion.reasoning_guidance import RESEARCH_STOPPING_GUIDANCE
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'outbound_request_mode': 'allow_all'})
    for key, value in {'NULLION_CODE_MODE': mode, 'NULLION_BROWSER_BACKEND': 'playwright',
            'NULLION_BROWSER_HEADLESS': 'true', 'NULLION_BROWSER_BLOCK_PRIVATE': 'false'}.items():
        monkeypatch.setenv(key, value)
    requests = []
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if self.path.startswith('/optional'):
                time.sleep(1)
            number = self.path.rsplit('/', 1)[-1]
            body = f'<main><p>Verified option {number}; added 2026-11-15; available.</p>' + '<p>Source context.</p>' * 150
            body += '<a href="/optional/background">Additional background</a></main>'
            self.send_response(200); self.end_headers()
            self.wfile.write(body.encode())
        def log_message(self, *args): pass
    site = ThreadingHTTPServer(('127.0.0.1', 0), Site)
    threading.Thread(target=site.serve_forever, daemon=True).start()
    base = f'http://localhost:{site.server_port}'
    urls = [base + f'/read/{i}' for i in range(count)]
    batches = [urls[i:i+6] for i in range(0, count, 6)]
    class Model:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['browser'], 'tool_names': ['browser_read_pages'],
                    'required_tool_names': ['browser_read_pages']})
            if self.phase == 2 and mode == 'on':
                program = "summaries=[]\n"
                for batch in batches:
                    arguments = {'independent_pages': True, 'pages': [{'url': url, 'read_kind': 'text'} for url in batch]}
                    program += "saved=call_tool('browser_read_pages', " + repr(arguments) + ")\n"
                    program += "for document in saved['output']['documents']:\n    summaries.append(document['text'].splitlines()[0])\n"
                return call('code_mode', {'tool_names': ['browser_read_pages'], 'code': program + 'summaries'})
            if mode == 'off' and self.phase < 2 + len(batches):
                return call('browser_read_pages', {'independent_pages': True,
                    'pages': [{'url': url, 'read_kind': 'text'} for url in batches[self.phase-2]]})
            assert self.phase == (3 if mode == 'on' else 2 + len(batches)), 'Completed research restarted'
            blocks = [b.get('text', '') for m in kwargs['messages'] for b in m.get('content', [])
                if isinstance(b, dict) and b.get('type') == 'text']
            checkpoint = next(b for b in reversed(blocks) if b.startswith('Retained discovered sources:'))
            assert RESEARCH_STOPPING_GUIDANCE in checkpoint
            ledger = json.loads(checkpoint.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START + '\n', 1)[1]
                .split('\n' + UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END, 1)[0])
            observed = {r['url'] for r in ledger['observed_sources']}
            assert set(urls) <= observed, 'Batch evidence must prevent reopening already read pages'
            assert all(f'Verified option {i}' in json.dumps(kwargs['messages']) for i in range(count))
            return text('\n'.join(f'Verified option {i} — added 2026-11-15. {url}' for i, url in enumerate(urls)))
    model = Model(); registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    samples = []
    try:
        with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
            for index, prompt in enumerate(['選択した候補を確認してください。', 'Compara las opciones seleccionadas.', 'تحقق من جميع المصادر المحددة.']):
                model.phase = 0; before = len(requests); cid = f'web:research-{mode}-{count}-{index}'
                start = time.monotonic()
                response = client.post('/api/chat', json={'text': prompt, 'conversation_id': cid})
                samples.append(time.monotonic() - start)
                assert response.status_code == 200 and f'Verified option {count-1}' in response.json()['text'], response.text
                assert len([p for p in requests[before:] if p.startswith('/read/')]) == count
                assert not any(p.startswith('/optional') for p in requests[before:])
                turn = runtime.store.list_recent_conversation_events(cid, event_type='conversation.chat_turn')[-1]
                reads = [r for r in turn['tool_results'] if r['tool_name'] == 'browser_read_pages']
                # Older audit rows can be bounded DTO previews. The exact HTTP
                # visit count and observed source URLs above prove all pages.
                assert len(reads) == len(batches)
                assert all(r['status'] == 'completed' for r in turn['tool_results'])
        assert sum(samples) / len(samples) < 8 and max(samples) < 12, samples
        record_property('research_mean_seconds', sum(samples)/len(samples))
    finally:
        site.shutdown(); site.server_close()
