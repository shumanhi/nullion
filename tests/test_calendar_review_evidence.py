from types import SimpleNamespace

import pytest

from nullion import tools
from nullion.tools import ToolInvocation, ToolResult


def connector(monkeypatch, pages):
    calls=[]
    monkeypatch.setattr(tools,'_connector_connection_for_invocation',lambda *a:SimpleNamespace())
    monkeypatch.setattr(tools,'_calendar_events_endpoint_for_provider',lambda *a,**k:'https://fixture.invalid/events')
    def request(invocation,**kw):
        calls.append(kw)
        return pages[len(calls)-1]
    monkeypatch.setattr(tools,'_connector_json_request',request)
    return tools._build_connector_calendar_list_handler(),calls


def test_calendar_continuation_preserves_exact_scope_and_does_not_certify_first_page(monkeypatch):
    from nullion.workflow_reasoning import source_pagination_summary
    handler,calls=connector(monkeypatch,[{'items':[{'id':'a','summary':'通知'}],'nextPageToken':'opaque-next'},
        {'items':[{'id':'b','summary':'Aviso'}]}])
    args={'provider_id':'fixture','start':'2028-11-15T00:00:00Z','end':'2028-11-19T00:00:00Z','max':1,'query':'通知'}
    first=handler(ToolInvocation('first','calendar_list','web:fixture',args))
    assert first.status=='completed' and first.output['has_more'] is True
    assert not source_pagination_summary([first])[0]['complete']
    next_request=first.output['next_page_request']
    assert next_request=={**args,'page_token':'opaque-next'}
    second=handler(ToolInvocation('second','calendar_list','web:fixture',next_request))
    assert calls[1]['params']['pageToken']=='opaque-next'
    assert second.output['has_more'] is False
    assert source_pagination_summary([first,second])[0]['complete'] is True
    assert [row['id'] for row in first.output['results']+second.output['results']]==['a','b']
    assert 'page_token' in tools._calendar_list_input_schema()['properties']
    assert 'page_token' not in tools._calendar_list_input_schema(include_provider_id=False)['properties']


@pytest.mark.parametrize('page',[{'items':None},{'items':['bad']},{'items':[],'nextPageToken':False}])
def test_malformed_calendar_page_is_not_evidence_of_no_existing_events(monkeypatch,page):
    handler,_=connector(monkeypatch,[page])
    result=handler(ToolInvocation('bad','calendar_list','web:fixture',{
        'provider_id':'fixture','start':'2028-11-15T00:00:00Z','end':'2028-11-19T00:00:00Z'}))
    assert result.status=='failed' and 'result_count' not in result.output


def test_action_context_includes_existing_event_beyond_first_three_and_nearby_date():
    registry=tools.ToolRegistry();tools.register_connector_plugin(registry)
    hook=registry.get_spec('calendar_create').action_review_context
    assert hook is tools._calendar_create_action_review_context
    events=[{'id':str(i),'summary':'別の予定','start':{'date':'2028-12-01'}} for i in range(3)]
    events.append({'id':'existing','summary':'Aviso para la misma obligación','start':{'date':'2028-11-16'}})
    retained=[ToolResult('list','calendar_list','completed',{'provider_id':'fixture','results':events,
        'result_count':4,'has_more':True,'next_page_token':'more'})]
    call=ToolInvocation('create','calendar_create','web:fixture',{'provider_id':'fixture',
        'summary':'通知','start':'2028-11-17','end':'2028-11-18'})
    context=hook(call,retained)
    assert context['coverage']=='observed_records_only'
    assert context['observed_event_count']==4 and context['observed_events'][0]['id']=='existing'
    assert context['listing_scopes'][0]['has_more'] is True
    assert context['proposed_start']=='2028-11-17'
    assert hook(call,[ToolResult('foreign','calendar_list','completed',{
        'provider_id':'other','results':events})])['observed_event_count']==0
    other_calendar=ToolInvocation('other-create','calendar_create','web:fixture',{
        **call.arguments,'calendar_id':'another-calendar'})
    assert hook(other_calendar,retained)['observed_event_count']==0


def test_selected_match_preview_does_not_hide_that_more_events_matched(monkeypatch):
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    handler,_=connector(monkeypatch,[{'items':[{'id':str(i),'summary':'通知'} for i in range(4)]}])
    result=handler(ToolInvocation('read','calendar_list','web:fixture',{
        'provider_id':'fixture','start':'2028-11-15T00:00:00Z','end':'2028-11-19T00:00:00Z','query':'通知'}))
    assert result.output['selected_result_count']==4 and result.output['selected_results_truncated'] is True
    assert len(result.output['selected_results'])==3 and len(result.output['results'])==4
    assert _completion_review_evidence_result_is_positive(result)
