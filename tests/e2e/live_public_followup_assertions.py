"""Public-source scope evidence; optional review availability is diagnostic."""
from urllib.parse import urlsplit


def assert_public_followup_outcome(*, response, turns, reviews):
    assert turns, 'Missing native request evidence'
    results=[r for turn in turns for r in turn.get('tool_results',[])]
    assert not any(r['tool_name'] in {'email_search','email_read','calendar_list'} for r in results),results
    final=turns[-1]
    assert final.get('response_fulfilled') is not False,final
    review=reviews[-1] if reviews else {}
    if not reviews:
        assert final.get('response_fulfilled') is True,final
    elif review.get('disposition')!='complete':
        assert review.get('review_error') and not review.get('runtime_failure_code'),review
    text=response['text'];observed=[]
    for row in final.get('tool_results',[]):
        if row.get('status')!='completed':continue
        out=row.get('output') or {}
        if out.get('result_kind')=='public_search_results' or not out.get('source_record_collections'):
            continue  # Discovery snippets are not a read of their linked page.
        candidates=[out]
        for declaration in out['source_record_collections']:
            path=declaration.get('path') if isinstance(declaration,dict) else None
            if not isinstance(path,list) or not 1<=len(path)<=4 or not all(isinstance(k,str) for k in path):
                continue
            records=out
            for key in path:records=records.get(key) if isinstance(records,dict) else None
            candidates.extend([records] if isinstance(records,dict) else
                records if isinstance(records,list) else [])
        for captured in candidates:
            if not isinstance(captured,dict):continue
            record=captured.get('record') or {}
            url=captured.get('page_url') or captured.get('source_url') or captured.get('source_page_url') or captured.get('url')
            body=captured.get('text') or record.get('text') or record.get('details')
            if (isinstance(url,str) and urlsplit(url).scheme in {'http','https'} and url in text
                    and isinstance(body,str) and body.strip()):
                observed.append({'url':url,'invocation_id':row['invocation_id'],'observed_chars':len(body)})
            # A listing can verify an item without navigating to its detail URL.
            # Require its exact native DOM link and visible label in that read's
            # actual text. Identity inventories and discovery snippets cannot qualify.
            if (row.get('tool_name')=='browser_read_pages'
                    and out.get('result_kind')=='browser_page_collection'
                    and isinstance(body,str) and body.strip()):
                normalized=' '.join(body.split()).casefold()
                for link in captured.get('links',[]):
                    if not isinstance(link,dict) or link.get('text_truncated'):continue
                    href,label=link.get('url'),link.get('text')
                    if (isinstance(href,str) and urlsplit(href).scheme in {'http','https'} and href in text
                            and isinstance(label,str) and label.strip()
                            and ' '.join(label.split()).casefold() in normalized):
                        observed.append({'url':href,'source_page_url':url,'invocation_id':row['invocation_id'],
                            'observed_chars':len(body),'evidence_kind':'native_listing_link'})
    assert observed,'Final public follow-up lacks a captured source record and its visible direct link'
    return {'observed_sources':observed,'review_error':review.get('review_error'),
        'review_complete':review.get('disposition')=='complete','request_completion_certified':final.get('response_fulfilled') is True}
