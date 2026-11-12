"""Real DOM extraction, live worker, SQLite and Web delivery for section watches."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
from types import SimpleNamespace
import time

import pytest
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_action_acknowledgement
from tests.e2e.test_live_tracker_workflow import live_fixture, text, use

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('heading', ['Developing', '進展', 'En desarrollo'])
def test_dom_collection_keeps_section_context_and_exposes_omitted_items(heading):
    from playwright.sync_api import sync_playwright
    from nullion.plugins.browser_plugin.browser_tools import _browser_extract_items_script
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=['--no-sandbox'])
        page = browser.new_page()
        page.set_content(f'''<main><section><h2>Other</h2><ul>
          <li><a href="https://example.test/other/item">Other 1</a></li></ul></section>
          <section id="watch"><div><h2>{heading}</h2></div><div><div><ul>
          <li><a href="https://example.test/news/a">1 hr ago First item</a></li>
          <li><a href="https://example.test/news/b">1 hr ago Second item</a></li>
          </ul></div></div></section></main>''')
        # Real news homepages may wrap all content in a semantic section while
        # the watched region itself is a closer generic container.
        page.locator('main').evaluate('(main)=>{const outer=document.createElement("section");while(main.firstChild)outer.appendChild(main.firstChild);main.appendChild(outer);const watched=main.querySelector("#watch");const div=document.createElement("div");div.id="watch";while(watched.firstChild)div.appendChild(watched.firstChild);watched.replaceWith(div)}')
        full = page.evaluate(_browser_extract_items_script(max_items=25))
        watched = [r for r in full['items'] if '/news/' in r['url']]
        assert len(watched) == 2 and all(r['section_heading'] == heading for r in watched)
        assert all(heading not in r['title'] for r in watched), 'The source only labels the enclosing section'
        named = page.evaluate(_browser_extract_items_script(max_items=25, section_heading=heading))
        assert len(named['items']) == 2 and named['coverage']['scope_found'] is True
        assert named['coverage']['section_heading'] == heading
        limited = page.evaluate(_browser_extract_items_script(max_items=1, section_heading=heading))
        scope = limited['coverage']['sections'][0]
        assert scope['heading'] == heading and scope['complete'] is False
        assert scope['total_items'] == 2 and scope['returned_items'] == 1
        assert limited['coverage']['truncated'] is True
        missing = page.evaluate(_browser_extract_items_script(selector='#gone'))
        assert missing['coverage']['scope_found'] is False and missing['items'] == []
        browser.close()


def test_section_story_appearing_after_start_delivers_exactly_one_real_web_alert(live_fixture, monkeypatch, record_property):
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import _open_page, _UvicornThread, _free_port
    from playwright.sync_api import expect

    f = live_fixture
    state = SimpleNamespace(fresh=False, evaluations=0)
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            fresh = '<li><a href="/news/new">1 min ago New bridge opens</a></li>' if state.fresh else ''
            body = ('<main><section id="watch"><h2>進展</h2><div><ul>'
                    '<li><a href="/news/known">1 hr ago Known baseline story</a></li>' + fresh + '</ul></div></section></main>')
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8'); self.end_headers()
            self.wfile.write(body.encode())
        def log_message(self, *_): pass
    source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{source.server_port}/'
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    register_browser_tools(f.registry)
    phase = 0
    probe = [{'tool_name': 'browser_navigate', 'arguments': {'url': url}},
             {'tool_name': 'browser_extract_items', 'arguments': {'section_heading': '進展', 'max_items': 25}}]
    def model(**kw):
        nonlocal phase
        protocol_reply = workflow_provider_reply(kw)
        if protocol_reply is not None: return protocol_reply
        if str(kw.get('system', '')).startswith('Evaluate only the supplied current observations'):
            state.evaluations += 1
            outputs = json.loads(kw['messages'][0]['content'][0]['text'])['observations']
            observed = outputs[1]
            assert observed['result_kind'] == 'browser_item_collection'
            assert all(row['section_heading'] == '進展' for row in observed['items'])
            hit = next(((i, r) for i, r in enumerate(observed['items']) if r['url'] == url+'news/new'), None)
            if hit:
                index, row = hit
                return text(json.dumps({'status': 'matched', 'summary': 'New bridge opens\n'+row['url'],
                    'evidence': [{'step': 1, 'pointer': f'/items/{index}/url', 'value': row['url']},
                                 {'step': 1, 'pointer': f'/items/{index}/section_heading', 'value': '進展'}]}))
            return text(json.dumps({'status': 'no_match', 'checked_scopes': [
                {'step': 1, 'section_id': observed['items'][0]['section_id']}]}))
        if not kw.get('tools'): return text('{}')
        phase += 1
        if phase == 1: return use('request_tool_scope', {
            'tracking_mode': 'live', 'capabilities': ['live_tracking', 'browser'],
            'tool_names': ['browser_navigate', 'browser_extract_items', 'create_live_tracker'],
            'required_tool_names': ['browser_navigate', 'browser_extract_items', 'create_live_tracker']})
        if phase in (2,3): return use(probe[phase-2]['tool_name'], probe[phase-2]['arguments'])
        if phase == 4: return use('create_live_tracker', {
            'name': '進展の観測', 'stop_after_match':True, 'duration_seconds': 90, 'interval_seconds': 5, 'probe': probe,
            'matcher': {'kind': 'semantic', 'instruction': '進展の新しい記事。既知の記事を除外。見出しとURLを通知。'}})
        return text(fixture_action_acknowledgement(kw) or 'Watching.')
    f.model.create = model
    app = create_app(f.runtime, f.orchestrator, f.registry)
    try:
        with _UvicornThread(app, _free_port()) as server:
            pw, browser, page, errors = _open_page(server.base_url)
            try:
                page.locator('#user-input').fill('このセクションに新しい記事が出たら90秒以内に一度だけ通知して。')
                page.locator('#user-input').press('Enter')
                try:
                    expect(page.locator('#messages')).to_contain_text('Live tracker', timeout=25000)
                finally:
                    record_property('setup_evidence',json.dumps({'jobs':app.state.live_tracker_store.list('workspace_admin'),
                        'events':f.runtime.store.list_conversation_events()},default=str))
                deadline=time.monotonic()+20
                while time.monotonic()<deadline:
                    jobs=app.state.live_tracker_store.list('workspace_admin')
                    if jobs and jobs[0]['checks'] >= 2: break
                    time.sleep(.1)
                assert jobs[0]['checks'] >= 2 and jobs[0]['last_outcome'] == 'no_match', jobs
                state.fresh=True
                expect(page.locator('#messages')).to_contain_text('New bridge opens', timeout=18000)
                expect(page.locator('#messages')).to_contain_text(url+'news/new', timeout=8000)
                expect(page.locator('#live-trackers-dash')).to_contain_text('Match found', timeout=8000)
                from nullion.chat_store import get_chat_store
                job=app.state.live_tracker_store.get(jobs[0]['id'])
                messages=get_chat_store().load_messages(job['conversation_id'])
                alerts=[m for m in messages if (m.get('metadata') or {}).get('source') == 'live_tracker']
                assert len(alerts)==1 and job['notification_status']=='sent' and job['consecutive_errors']==0
                assert job['last_evaluation']['source_coverage'][0]['coverage']['sections'][0]['heading']=='進展'
                assert phase == 5 and state.evaluations>=3, 'Polling must reuse the plan, not restart chat'
                assert not errors, errors
            finally:
                browser.close(); pw.stop()
    finally:
        f.registry.run_cleanup_hooks(); source.shutdown()


def test_repeated_urls_keep_all_sections_and_shared_images_do_not_hide_distinct_items():
    from playwright.sync_api import sync_playwright
    from nullion.plugins.browser_plugin.browser_tools import _browser_extract_items_script
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True, args=['--no-sandbox']); page=browser.new_page()
        image='https://example.test/common.png'
        page.route('**/common.png', lambda route: route.abort())
        page.set_content('''<main><section><h2>Headline</h2><ul><li>
          <a href="https://example.test/news/a">News 1</a></li></ul></section>
          <section><h2>進展</h2><ul>
          <li><a href="https://example.test/news/a">1 min ago News 1</a></li>
          <li><a href="https://example.test/news/b">1 min ago News 2</a></li>
          </ul></section></main>''')
        # A common image is not an item identity. Both distinct URLs must survive.
        page.locator('li').evaluate_all('(els,src)=>els.forEach(e=>{const i=document.createElement("img");i.src=src;i.width=200;i.height=100;e.appendChild(i)})', image)
        out=page.evaluate(_browser_extract_items_script(max_items=25))
        assert len(out['items'])==2
        assert all(item['image_url']==image for item in out['items'])
        row=next(r for r in out['items'] if r['url'].endswith('/a'))
        headings={row['section_heading'],*[r['heading'] for r in row.get('additional_sections',[])]}
        assert headings=={'Headline','進展'}
        section=next(s for s in out['coverage']['sections'] if s['heading']=='進展')
        assert section['complete'] and section['returned_items']==2 and section['total_items']==2
        browser.close()


def test_incomplete_collection_pauses_with_specific_repair_notice(live_fixture):
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import _open_page, _UvicornThread, _free_port
    from playwright.sync_api import expect
    f=live_fixture
    f.model.semantic=True
    app=create_app(f.runtime,f.orchestrator,f.registry)
    with _UvicornThread(app,_free_port()) as server:
        pw,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator('#user-input').fill('Vigila esta fuente durante 90 segundos.')
            page.locator('#user-input').press('Enter')
            expect(page.locator('#messages')).to_contain_text('Live tracker',timeout=18000)
            f.source.extra={'result_kind':'browser_item_collection','coverage':{
                'scope_found':True,'truncated':True,'sections':[{
                    'id':'section-a','heading':'進展','complete':False,'total_items':2,'returned_items':1}]}}
            expect(page.locator('#messages')).to_contain_text('Live tracking paused:',timeout=35000)
            expect(page.locator('#messages')).to_contain_text('complete section needed to rule out a match')
            expect(page.locator('#messages')).to_contain_text('narrow the saved read')
            job=app.state.live_tracker_store.list('workspace_admin')[0]
            assert job['state']=='blocked' and job['consecutive_errors']==3
            assert job['last_problem']['code']=='coverage_incomplete'
            assert job['last_outcome']=='unknown'
            from nullion.chat_store import get_chat_store
            alerts=[m for m in get_chat_store().load_messages(job['conversation_id'])
                    if (m.get('metadata') or {}).get('source')=='live_tracker']
            assert len(alerts)==1 and 'No new matching result was confirmed by these failed checks' in alerts[0]['text']
            assert not errors, errors
        finally:
            browser.close();pw.stop()


def test_empty_named_section_is_complete_without_borrowing_another_sections_items():
    from playwright.sync_api import sync_playwright
    from nullion.plugins.browser_plugin.browser_tools import _browser_extract_items_script
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True,args=['--no-sandbox']);page=browser.new_page()
        for tag, content in [('section','<ul></ul>'),('div','<ul></ul>'),('div','')]:
            page.set_content(f'<main><{tag}><h2>進展</h2>{content}</{tag}>'
                             '<section><h2>Other</h2><ul><li><a href="https://example.test/news/a">Other 1</a></li></ul></section></main>')
            out=page.evaluate(_browser_extract_items_script(section_heading='進展'))
            assert out['items']==[] and out['coverage']['scope_found'] is True
            assert out['coverage']['sections']==[{'id':'section-0','heading':'進展',
                'total_items':0,'returned_items':0,'complete':True}]
            assert out['coverage']['truncated'] is False
        browser.close()
