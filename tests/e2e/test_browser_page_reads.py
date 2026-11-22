"""Real chat/SQLite and shared Chromium page reads with external provider fixtures."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('partial,read_kind', [(False,'items'),(True,'items'),(False,'text'),(False,'detail'),(False,'detail_fallback'),
    pytest.param(False, 'bound_detail', id='bound-detail'), pytest.param(False, 'bound_text', id='bound-text')])
@pytest.mark.parametrize('split_plan', [False, True, 'dependent'])
def test_batched_reads_preserve_login_provenance_and_finish_without_sdk_loop(tmp_path, monkeypatch, partial, read_kind, source_summary_model_reply, split_plan):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.workflow_provider_protocol import workflow_provider_reply
    delegated = read_kind == 'delegated_batch'
    if delegated:
        import sys
        monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON',sys.executable)
        read_kind='text'
    fallback_detail = read_kind == 'detail_fallback'
    bound_detail = read_kind == 'bound_detail'
    bound_text = read_kind == 'bound_text'
    serial_batch = bound_text and split_plan == 'dependent'
    if fallback_detail or bound_detail:read_kind='detail'
    if bound_text:read_kind='text'
    runtime = isolate(tmp_path,monkeypatch)
    active, peak, cookies = 0, 0, []
    lock = threading.Lock()
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal active, peak
            if self.path == '/favicon.ico':
                self.send_response(204); self.end_headers(); return
            if self.path == '/keep':
                self.send_response(200);self.end_headers();self.wfile.write(b'<h1>USER_TAB_KEEP</h1>');return
            with lock:
                active += 1; peak = max(peak,active)
                cookies.append(self.headers.get('Cookie',''))
            try:
                time.sleep(.12)
                self.send_response(200); self.end_headers()
                if self.path.startswith('/missing'):
                    self.wfile.write(b'<h1>No selected region on this page</h1>'); return
                name, price = ('Alpha option', '$17') if self.path.startswith('/a') else ('Beta option', '$21')
                if bound_detail or bound_text:
                    name = '充電池 Alpha option de larga duración' if self.path.startswith('/a') else 'Batería Beta option de larga duración'
                body = (f'<html><body><main id="results"><article><h1><a href="{self.path}/item">{name}</a></h1><p>{price}</p><p>Verified current listing with an exact price and direct source link.</p><a style="display:none" href="/hidden">Hidden</a><a href="javascript:void(0)">Action</a><a href="https://">Invalid</a></article></main></body></html>').encode()
                if fallback_detail:body=body.replace(b'<h1>',b'<div>').replace(b'</h1>',b'</div>')
                if bound_detail or bound_text:
                    body = body.replace(f'<p>{price}</p>'.encode(), (
                        f'<p itemprop="price">{price}</p><h1>Especificaciones</h1><p>Paquete de 8</p>'
                    ).encode())
                    body = body.replace(b'<html>', b'<html><meta charset="utf-8">').replace(b'<article>', (
                        b'<header><h1>Ayuda</h1></header>'
                        b'<article><h1><a href="/unrelated">Otra oferta</a></h1><p class="price">$99</p></article>'
                        b'<article>'
                    ), 1)
                    if bound_text:
                        title = f'<title>{name} | Tienda de ejemplo</title>'.encode()
                        body = body.replace(b'<body>', title + b'<body>', 1)
                    if bound_detail and self.path.startswith('/b'):
                        body = body.replace(b'<h1><a href="/b/item">', (
                            b'<h1 hidden id="hydrating"><a href="/b/item">'
                        )).replace(b'</body>', b'<script>setTimeout(() => hydrating.hidden = false, 400)</script></body>')
                self.wfile.write(body)
            finally:
                with lock: active -= 1
        def log_message(self,*args): pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site)
    threading.Thread(target=site.serve_forever,daemon=True).start()
    base=f'http://localhost:{site.server_port}'
    port=_free_port()
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','cdp')
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL',f'http://127.0.0.1:{port}')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false')
    # An accidental inner browser model loop becomes both slow and a failure.
    class Model:
        phase=0
        errors=[]
        def create(self,**kwargs):
            try: return self._create(**kwargs)
            except Exception:
                import traceback
                self.errors.append(traceback.format_exc())
                raise
        def _create(self,**kwargs):
            if any(tool['name']=='browser_decision' for tool in kwargs.get('tools',[])):
                time.sleep(2)
                raise AssertionError('Readable pages must not start an inner model loop')
            protocol=workflow_provider_reply(kwargs,completion=True)
            if protocol is not None:return protocol
            summary=source_summary_model_reply(kwargs)
            if summary is not None:return summary
            if not kwargs.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_run_task'] if delegated else ['browser_read_pages'],'required_tool_names':['browser_run_task'] if delegated else ['browser_read_pages']})
            if self.phase==2:
                if delegated:return call('browser_run_task',{'url':base+'/a','task':'比較してください。',
                    'read_urls':[base+'/a',base+'/b'],'independent_pages':True})
                pages=[{'url':base+'/a','read_kind':read_kind,'ready_selector':'#results'},
                    {'url':base+('/missing' if partial else '/b'),'read_kind':read_kind,
                     **({'selector':'#not-present'} if partial else {'ready_selector':'#results'})}]
                if read_kind=='text':
                    for page in pages:page['selector']='#results'
                if read_kind=='detail':
                    for page in pages:page['selector']='#results article'
                if bound_detail:
                    for page, heading in zip(pages, ('Alpha option', 'Batería Beta option')):
                        del page['selector'], page['ready_selector']
                        page['expected_heading'] = heading
                if serial_batch:
                    return call('browser_read_pages',{'pages':pages})
                result=call('browser_read_pages',{'independent_pages':split_plan!='dependent','pages':pages[:1] if split_plan else pages})
                if split_plan:
                    second=call('browser_read_pages',{'independent_pages':split_plan!='dependent','pages':pages[1:]})['content'][0]
                    second['id']='independent-second';result['content'].append(second)
                return result
            if self.phase != 3:
                import traceback
                raise AssertionError('Unexpected extra model call: '+''.join(traceback.format_stack(limit=12)))
            evidence=json.dumps(kwargs['messages'], ensure_ascii=False)
            assert 'Alpha option' in evidence and '$17' in evidence, evidence[-5500:]
            if read_kind == 'detail':
                assert base+'/a/item' in evidence and base+'/b/item' in evidence
                assert 'link_coverage' in evidence
                if bound_detail:
                    assert '$99' not in evidence
                    assert 'Paquete de 8' in evidence
            if bound_text:
                assert 'observed_title' in evidence and 'observed_text' in evidence and 'Paquete de 8' in evidence
            if partial:
                assert 'source_scope_missing' in evidence
                return text(f'Alpha option: $17. [Open listing]({base}/a/item). The other page lacked the selected results region.')
            assert 'Beta option' in evidence and '$21' in evidence
            return text(f'Alpha option: $17. [Open listing]({base}/a/item)\n\nBeta option: $21. [Open listing]({base}/b/item)')
    samples=[]
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch_persistent_context(str(tmp_path/'profile'),headless=True,args=[f'--remote-debugging-port={port}'])
            try:
                browser.add_cookies([{'name':'login','value':'retained','url':base}])
                user_tab=browser.new_page();user_tab.goto(base+'/keep')
                model=Model();registry=ToolRegistry()
                register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
                with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
                    for i,prompt in enumerate(['比較して','Compara las opciones','قارن الخيارات']):
                        model.phase=0; started=time.monotonic()
                        response=client.post('/api/chat',json={'text':prompt,'conversation_id':f'web:batch-{i}'})
                        samples.append(time.monotonic()-started)
                        assert response.status_code==200 and '$17' in response.json()['text'], json.dumps({'phase':model.phase,'errors':model.errors,'reply':response.text},ensure_ascii=False)
                        assert base+'/a/item' in response.json()['text'] and not response.json()['artifacts']
                        if not partial:assert '$21' in response.json()['text'] and base+'/b/item' in response.json()['text']
                        turns=[e for e in runtime.store.list_conversation_events(f'web:batch-{i}') if e.get('event_type')=='conversation.chat_turn']
                        results=turns[-1]['tool_results']
                        reads=[r['output'] for r in results if r['tool_name']==('browser_run_task' if delegated else 'browser_read_pages')]
                        assert len(reads)==(2 if split_plan=='dependent' and not serial_batch else 1)
                        if serial_batch:
                            assert reads[0]['parallel_limit']==1
                        plans=[r for r in runtime.store.list_recent_conversation_events(f'web:batch-{i}')
                               if r.get('event_type')=='conversation.tool_batch_plan']
                        assert bool(plans) is (split_plan is True)
                        assert sum(read['completed_pages'] for read in reads)==(1 if partial else 2)
                        if read_kind == 'text' or fallback_detail:
                            assert all('document_title' in document and 'headings' in document
                                for read in reads for document in read['documents'])
                            if fallback_detail:assert all(document['headings']==[] for read in reads for document in read['documents'])
                            assert all(document['selector']==(None if delegated else '#results article' if fallback_detail else '#results') for read in reads for document in read['documents'])
                            if bound_text:
                                for document in (document for read in reads for document in read['documents']):
                                    assert document['observed_title'].endswith('de larga duración')
                                    assert 'Paquete de 8' in document['observed_text'] and '$99' not in document['observed_text']
                        if fallback_detail:
                            assert not any(read['details'] for read in reads)
                            assert all(doc['detail_extraction']['ok'] is False for read in reads for doc in read['documents'])
                        if read_kind == 'detail' and not fallback_detail:
                            records=[row['record'] for read in reads for row in read['details']]
                            assert {link['url'] for row in records for link in row['links']} == {base+'/a/item',base+'/b/item'}
                            assert all(row['link_coverage']['truncated'] is False for row in records)
                            if bound_detail:
                                assert {row['title'] for row in records} == {
                                    '充電池 Alpha option de larga duración', 'Batería Beta option de larga duración',
                                }
                                assert all(page['readiness']['condition_verified'] for read in reads for page in read['pages'])
                                assert all('Paquete de 8' in row['observed_text'] for read in reads for row in read['details'])
                        assert all(r['tool_name'] != 'browser_navigate' and (delegated or r['tool_name']!='browser_run_task') for r in results)
                        assert not user_tab.is_closed() and user_tab.url==base+'/keep'
                        assert len(browser.pages)==2, 'Batch tabs must close without closing the user tab'
                assert cookies and all('login=retained' in cookie for cookie in cookies)
                assert peak==(1 if split_plan=='dependent' else 2), 'Only independent reads may overlap'
                assert sum(samples)/len(samples)<3 and max(samples)<4, samples
            finally:browser.close()
    finally:site.shutdown();site.server_close()


def test_one_delegated_read_preserves_both_independent_source_records(tmp_path,monkeypatch,source_summary_model_reply):
    test_batched_reads_preserve_login_provenance_and_finish_without_sdk_loop(
        tmp_path,monkeypatch,False,'delegated_batch',source_summary_model_reply,False)


@pytest.mark.parametrize("case,label", [
    ("complete", "開く"), ("missing", "Ver detalhes"),
    ("ambiguous", "مشاهدة العناصر"), ("unavailable", "Показать товары"),
    ("redirected", "Ver elementos"),
])
def test_observed_link_reads_destination_and_retains_verified_overview(
    tmp_path, monkeypatch, source_summary_model_reply, case, label,
):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, call, text
    from tests.e2e.test_web_surface_contracts import _free_port
    from tests.workflow_provider_protocol import workflow_provider_reply

    runtime = isolate(tmp_path, monkeypatch)
    visited = []
    private_control = "FixtureBrowserSecret791"
    private_row = (f'<article><h2>Credential field</h2>'
                   f'<input type="password" class="summary" value="{private_control}"></article>')

    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            signed_in = "login=retained" in self.headers.get("Cookie", "")
            if self.path == "/keep":
                body = "<h1>Existing browser tab</h1>"
            elif self.path == "/overview":
                sibling = f'<a href="/wrong">{label}</a>' if case == "ambiguous" else ""
                destination = f'<a href="/items" onclick="event.preventDefault()">{label}</a>'
                if case == "complete":
                    destination = (f'<div id="hydrating" style="opacity:0">{destination}</div>'
                                   '<script>setTimeout(() => hydrating.style.opacity = 1, 650)</script>')
                if case in {"unavailable", "redirected"}:
                    destination = f'<div aria-disabled="true">{destination}</div>'
                if case == "redirected":
                    destination += '<script>setTimeout(() => location.replace("/redirected"), 700)</script>'
                body = (
                    '<h1 id="account">Lista privada</h1><main id="summary"><article>'
                    '<h2>Compartimento principal</h2><p class="summary">5 productos · 63.67 EUR</p>'
                    f'</article>{private_row}</main>'
                    f'{destination}<a hidden href="/wrong">{label}</a>{sibling}'
                    f'<div style="opacity:0"><a href="/wrong">{label}</a></div>'
                    f'<a aria-disabled="true" href="/wrong">{label}</a>'
                    f'<div inert><a href="/wrong">{label}</a></div>'
                )
            elif self.path == "/redirected":
                body = f'<a href="/wrong">{label}</a>'
            elif self.path == "/items" and signed_in and case == "complete":
                body = ('<h1 id="account">Lista privada</h1><main id="items">'
                    f'<article><h2>Óleo de oliva</h2><p>17.00 EUR</p></article>{private_row}</main>')
            else:
                body = '<h1 id="account">Vista anterior</h1><main id="items">Lista indisponível</main>'
            visited.append((self.path, signed_in))
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, *args):
            pass

    site = ThreadingHTTPServer(("127.0.0.1", 0), Site)
    threading.Thread(target=site.serve_forever, daemon=True).start()
    base = f"http://localhost:{site.server_port}"
    debug_port = _free_port()
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "cdp")
    monkeypatch.setenv("NULLION_BROWSER_CDP_URL", f"http://127.0.0.1:{debug_port}")
    monkeypatch.setenv("NULLION_BROWSER_BLOCK_PRIVATE", "false")
    monkeypatch.setenv("NULLION_CODE_MODE", "off")

    class Model:
        step = 0

        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True) or source_summary_model_reply(kwargs)
            if protocol is not None:
                return protocol
            self.step += 1
            if self.step == 1:
                return call("request_tool_scope", {"capabilities": ["web"],
                    "tool_names": ["browser_read_pages"], "required_tool_names": ["browser_read_pages"]})
            if self.step == 2:
                return call("browser_read_pages", {"pages": [{
                    "url": f"{base}/overview", "follow_link_text": label, "read_kind": "items",
                    "selector": "#items", "ready_selector": "#items",
                    "verify_records": {"context": [{"selector": "#account", "text": "Lista privada"}],
                        "row_selector": "article", "fields": {"title": "h2", "price_text": "p, input"}},
                    "retain_link_source": {"context": [{"selector": "#account", "text": "Lista privada"}],
                        "selector": "#summary", "row_selector": "article",
                        "fields": {"title": "h2", "compact_text": ".summary"}},
                }]})
            if case != "complete":
                raise TimeoutError("Illustrative writer outage after the page read")
            assert "17.00 EUR" in json.dumps(kwargs["messages"])
            return text(f"Óleo de oliva · 17.00 EUR. [Lista]({base}/items)")

    try:
        with sync_playwright() as pw:
            context = pw.chromium.launch_persistent_context(str(tmp_path/"profile"), headless=True,
                args=[f"--remote-debugging-port={debug_port}"])
            try:
                context.add_cookies([{"name": "login", "value": "retained", "url": base}])
                current = context.new_page()
                current.goto(base + "/keep")
                model = Model()
                registry = ToolRegistry()
                register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
                with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
                    response = client.post("/api/chat", json={"text": "Quais são os produtos da minha lista?",
                        "conversation_id": "web:private-list"})
                    assert response.status_code == 200, response.text
                    reply = response.json()["text"]
                    turn = next(row for row in runtime.store.list_conversation_events("web:private-list")
                        if row["event_type"] == "conversation.chat_turn")
                    reads = [row for row in turn["tool_results"] if row["tool_name"] == "browser_read_pages"]
                    assert len(reads) == 1, reads
                    saved = reads[0]["output"]
                    assert private_control not in json.dumps(saved)
                    assert private_control not in reply
                    if case == "redirected":
                        assert not saved.get("verified_pages") and ("/redirected", True) in visited
                    else:
                        assert saved["verified_pages"][0]["records"] == [
                            {"title": "Compartimento principal", "compact_text": "5 productos · 63.67 EUR"}]
                    if case == "complete":
                        assert "17.00 EUR" in reply and "Óleo de oliva" in reply, reply
                        assert saved["verified_pages"][1]["records"] == [
                            {"title": "Óleo de oliva", "price_text": "17.00 EUR"}]
                        assert saved["pages"][0]["page_url"] == base + "/items"
                        assert saved["pages"][0]["followed_link"]["source_url"] == base + "/overview"
                    else:
                        if case != "redirected":
                            assert "63.67 EUR" in reply and "5 productos" in reply, reply
                        assert reads[0]["status"] == "failed"
                        reason = {"missing": "source_context_unverified", "ambiguous": "source_link_ambiguous",
                                  "unavailable": "source_link_unavailable", "redirected": "source_context_unverified"}[case]
                        assert saved["pages"][0]["failure_reason"] == reason
                        assert turn["response_fulfilled"] is False
                    assert (("/items", True) in visited) == (case in {"complete", "missing"})
                    assert not any(path == "/wrong" for path, _ in visited)
                    assert not current.is_closed() and current.url == base + "/keep"
            finally:
                context.close()
    finally:
        site.shutdown()
        site.server_close()
