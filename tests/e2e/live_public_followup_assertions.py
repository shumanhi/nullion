"""Public-source scope evidence; optional review availability is diagnostic."""
from urllib.parse import urlsplit


def assert_public_followup_outcome(*, response, turns, reviews):
    assert turns and reviews, 'Missing native request/review evidence'
    results=[r for turn in turns for r in turn.get('tool_results',[])]
    assert not any(r['tool_name'] in {'email_search','email_read','calendar_list'} for r in results),results
    final=turns[-1]
    assert final.get('response_fulfilled') is not False,final
    review=reviews[-1]
    if review.get('disposition')!='complete':
        assert review.get('review_error') and not review.get('runtime_failure_code'),review
    text=response['text'];observed=[]
    for row in final.get('tool_results',[]):
        if row.get('status')!='completed':continue
        out=row.get('output') or {};record=out.get('record') or {}
        url=out.get('page_url') or out.get('source_url')
        body=out.get('text') or record.get('text') or record.get('details')
        if (isinstance(url,str) and urlsplit(url).scheme in {'http','https'} and url in text
                and isinstance(body,str) and body.strip()
                and out.get('source_record_collections')):
            observed.append({'url':url,'invocation_id':row['invocation_id'],'observed_chars':len(body)})
    assert observed,'Final public follow-up lacks a captured source record and its visible direct link'
    return {'observed_sources':observed,'review_error':review.get('review_error'),
        'review_complete':review.get('disposition')=='complete','request_completion_certified':final.get('response_fulfilled') is True}
