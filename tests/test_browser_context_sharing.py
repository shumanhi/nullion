import asyncio

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize('close_parent_first', [False, True])
async def test_explicit_child_tabs_share_only_their_task_and_close_independently(monkeypatch, close_parent_first):
    from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
    class Page:
        def __init__(self, context): self.context = context; self.closed = False
        async def close(self): self.closed = True
    class Context:
        def __init__(self): self.closed = False; self.pages = []
        async def new_page(self):
            page = Page(self); self.pages.append(page); return page
        async def close(self):
            self.closed = True
            for page in self.pages: page.closed = True
    class Browser:
        version = '149.0.0.0'
        contexts = []
        async def new_context(self, **kwargs):
            context = Context(); self.contexts.append(context); return context
    browser = Browser(); backend = PlaywrightBackend()
    async def ensure(): return browser
    monkeypatch.setattr(backend, '_ensure_browser', ensure)
    backend.share_session_context('task-a', 'child-a')
    child = await backend._get_page('child-a')
    parent = await backend._get_page('task-a')
    unrelated = await backend._get_page('task-b')
    assert child.context is parent.context and unrelated.context is not parent.context
    with pytest.raises(ValueError): backend.share_session_context('task-b', 'child-a')
    first, last = ('task-a', 'child-a') if close_parent_first else ('child-a', 'task-a')
    await backend.close_session(first)
    assert not parent.context.closed and not unrelated.closed
    assert not (child.closed if close_parent_first else parent.closed)
    await backend.close_session(last)
    assert parent.context.closed and not unrelated.closed
    await backend.close_session('task-b')
    assert unrelated.context.closed and not backend._pages and not backend._contexts
    assert not backend._context_roots


@pytest.mark.asyncio
@pytest.mark.parametrize('headless_recovery', [False, True])
async def test_context_inheritance_crosses_auto_and_recovery_wrappers_before_use(monkeypatch, headless_recovery):
    from nullion.plugins.browser_plugin.backends.auto_backend import AutoBackend
    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    calls = []
    class Configured:
        BACKEND_NAME = 'fixture'
        async def share_session_context(self, parent, child):
            await asyncio.sleep(0); calls.append((parent, child))
    configured = Configured(); auto = AutoBackend()
    async def ensure(): return configured
    monkeypatch.setattr(auto, '_ensure_backend', ensure)
    wrapper = SessionRecoveryBackend(auto)
    if headless_recovery: wrapper._headless['parent'] = configured
    await wrapper.share_session_context('parent', 'child')
    assert calls == [('parent', 'child')]
    assert (wrapper.mode_for_session('child') == 'headless') is headless_recovery


@pytest.mark.asyncio
async def test_cdp_never_reassigns_a_tab_while_its_close_is_in_flight(monkeypatch):
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    started=asyncio.Event();release=asyncio.Event()
    class Page:
        closed=False
        def is_closed(self):return self.closed
        async def close(self):
            started.set();await release.wait();self.closed=True
    closing=Page()
    class Context:
        pages=[closing]
        async def new_page(self):
            page=Page();self.pages.append(page);return page
    class Browser:
        contexts=[Context()]
    backend=CDPBackend();backend._pages['old-read']=closing;backend._owned_pages().add(id(closing))
    async def ensure():return Browser()
    monkeypatch.setattr(backend,'_ensure_browser',ensure)
    task=asyncio.create_task(backend.close_session('old-read'))
    try:
        await started.wait()
        opened=await backend._get_page('next-read')
        assert opened is not closing and not opened.is_closed()
        assert id(closing) in backend._closing_pages()
    finally:
        release.set();await task
    assert closing.is_closed() and id(closing) not in backend._owned_pages()
    assert not backend._closing_pages() and backend._pages['next-read'] is opened
