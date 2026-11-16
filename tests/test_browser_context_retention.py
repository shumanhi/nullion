"""Explicit native context leases outlive requests, without weakening default cleanup."""
import pytest

from nullion.tools import ToolInvocation
from nullion.plugins.browser_plugin.browser_tools import BrowserTools
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool


class Browser:
    BACKEND_NAME = 'playwright'
    def __init__(self): self.closed = []
    async def navigate(self, session, url): return url
    async def open(self, session): return 'Opened'
    async def close_session(self, session): self.closed.append(session)


@pytest.mark.parametrize('tool', ['browser_open', 'browser_navigate'])
@pytest.mark.parametrize('keep,has_identity', [(False, True), (True, True), (True, False)])
def test_only_explicit_keep_with_runtime_identity_retains_context(tool, keep, has_identity):
    backend = Browser()
    browser = BrowserTools(backend, BrowserSessionPool(), BrowserPolicy())
    context = {'browser_session_scope': 'turn-37', 'browser_session_owner': 'request:37','current_user_request':'開いたままにしてください。'}
    if has_identity: context['conversation_id'] = 'telegram:37'
    result = getattr(browser, tool)(ToolInvocation('open-37', tool, 'operator',
        {'url':'https://example.com', 'keep_open':keep, 'retention_request_evidence':'開いたままにしてください。', 'conversation_id':'forged-by-model'},
        capsule_id='turn-37', flow_context=context))
    assert result.status == 'completed'
    session = result.output['session_id']
    browser.close_tracked_sessions('turn-37')
    browser.close_tracked_sessions('request:37')
    if keep and has_identity:
        assert result.output['browser_context_retained'] is True and not backend.closed
        browser.close_tracked_sessions('conversation:forged-by-model')
        assert not backend.closed
        browser.close_tracked_sessions('conversation:telegram:37')
    else:
        assert result.output.get('browser_context_retained') is not True
    assert backend.closed == [session]


def test_failed_navigation_does_not_retain_a_conversation_lease():
    class Failed(Browser):
        async def navigate(self, session, url): raise RuntimeError('Source unavailable')
    backend = Failed()
    browser = BrowserTools(backend, BrowserSessionPool(), BrowserPolicy())
    result = browser.browser_navigate(ToolInvocation('failed', 'browser_navigate', 'operator',
        {'url':'https://example.com','keep_open':True}, capsule_id='turn-failed',
        flow_context={'conversation_id':'web:failed','browser_session_owner':'request:failed'}))
    assert result.status == 'failed'
    browser.close_tracked_sessions('turn-failed')
    browser.close_tracked_sessions('request:failed')
    assert len(backend.closed) == 1
    browser.close_tracked_sessions('conversation:web:failed')
    assert len(backend.closed) == 1


@pytest.mark.parametrize('quote',[None,'Unrelated old request'])
def test_unquoted_or_foreign_retention_flag_does_not_hold_a_research_tab(quote):
    backend=Browser();browser=BrowserTools(backend,BrowserSessionPool(),BrowserPolicy())
    result=browser.browser_open(ToolInvocation('read','browser_open','operator',
        {'keep_open':True,'retention_request_evidence':quote},capsule_id='owned',
        flow_context={'conversation_id':'web:37','current_user_request':'Lee la página y resume los datos.'}))
    assert result.output['browser_context_retained'] is False
    browser.close_tracked_sessions('owned')
    assert backend.closed==[result.output['session_id']]
