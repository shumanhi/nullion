"""Consolidated assertions inspect existing evidence without rerunning work."""
from types import SimpleNamespace

import pytest

from tests.e2e import live_primary_assertions as primary


@pytest.fixture
def evidence(tmp_path,monkeypatch):
    monkeypatch.setenv('NULLION_LIVE_EVIDENCE_DIR',str(tmp_path))
    state={'turns':[{'turn_id':'owned-37'}],'events':[],
           'messages':[{'role':'user'},{'role':'bot'}],'requests':[],'errors':[]}
    monkeypatch.setattr(primary,'_turns',lambda *args:state['turns'])
    monkeypatch.setattr(primary.catalogue,'_conversation_event_payloads',lambda *args:state['events'])
    def get(url):
        state['requests'].append(url)
        return SimpleNamespace(json=lambda:{'messages':state['messages']})
    def screenshot(path):
        state['screenshot']=path
    page=SimpleNamespace(request=SimpleNamespace(get=get),
                         locator=lambda selector:SimpleNamespace(screenshot=screenshot))
    state['live_web']=(page,[],state['errors'],'unused-read-only-db','web:owned-37','http://stage.test')
    return state


def test_existing_receipts_and_history_are_checked_without_another_workflow(evidence):
    primary.record_primary_boundary(evidence['live_web'],'canonical')
    assert evidence['requests']==['http://stage.test/api/chat/history/web:owned-37']
    assert evidence['screenshot'].endswith('canonical.png')


@pytest.mark.parametrize('violation',['extra_turn','quality_review','missing_reply','render_error'])
def test_consolidation_keeps_each_additional_failure_check(evidence,violation):
    if violation=='extra_turn':evidence['turns'].append({'turn_id':'unexpected'})
    if violation=='quality_review':evidence['events'].append({'event_type':'conversation.completion_review'})
    if violation=='missing_reply':evidence['messages'].pop()
    if violation=='render_error':evidence['errors'].append('render failure')
    with pytest.raises(AssertionError):primary.record_primary_boundary(evidence['live_web'],'canonical')


def test_deferred_native_turns_still_require_all_three_visible_replies(evidence):
    evidence['turns']=[]
    evidence['messages']=[{'role':role} for _ in range(3) for role in ('user','bot')]
    primary.record_primary_boundary(evidence['live_web'],'plain',expected_turns=3,allow_deferred=True)
    evidence['messages'].pop()
    with pytest.raises(AssertionError):
        primary.record_primary_boundary(evidence['live_web'],'plain',expected_turns=3,allow_deferred=True)
