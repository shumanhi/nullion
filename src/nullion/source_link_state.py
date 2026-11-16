"""Check parameterized links against native calendar context before publication.

No provider parameter names or request-language heuristics are used. A model
decision is needed only when actual URL date values conflict with observed
calendar dates: an unrelated historical reference can still be appropriate.
"""
from __future__ import annotations

import json
import re
import time
from hashlib import sha256
from datetime import date
from urllib.parse import parse_qsl, urlsplit

_LINK = re.compile(r'\[([^\]\n]*)\]\((https?://[^\s<>]+)\)')
_URL = re.compile(r'https?://[^\s<>()]+')


def _day(value):
    if isinstance(value, dict):
        value = value.get('dateTime') or value.get('date')
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value.split('T', 1)[0]).isoformat()
    except ValueError:
        return None


def calendar_context(results):
    records = []
    for result in results or ():
        if result.tool_name != 'calendar_list' or result.status != 'completed' or not isinstance(result.output, dict):
            continue
        for row in result.output.get('results') or ():
            if not isinstance(row, dict):
                continue
            start, end = _day(row.get('start')), _day(row.get('end'))
            if start or end:
                records.append({'invocation_id':result.invocation_id, 'id':row.get('id'),
                    'summary':str(row.get('summary') or '')[:300], 'start':start, 'end':end})
    return records


def conflicting_links(text, records):
    days = {row[key] for row in records for key in ('start', 'end') if row.get(key)}
    if not days:
        return []
    found = []
    markdown = list(_LINK.finditer(text or ''))
    candidates = [(match.start(),match.end(),match[1],match[2]) for match in markdown]
    for match in _URL.finditer(text or ''):
        if any(link.start()<=match.start()<link.end() for link in markdown):
            continue
        url=match[0].rstrip('.,;:!?')
        candidates.append((match.start(),match.start()+len(url),'',url))
    for start,end,label,url in sorted(candidates):
        try:
            values = {value for _,value in parse_qsl(urlsplit(url).query)
                if _day(value) == value}
        except ValueError:
            continue
        if values and not values.issubset(days):
            found.append({'id':str(len(found)), 'label':label, 'url':url, 'date_values':sorted(values),
                'start':start,'end':end})
    return found


def review_links(text, *, request, results, client, cache):
    # The common fast path does not enumerate connectors, load history or call
    # a model. Even calendar turns skip review when their URL state agrees.
    if not text or '://' not in text:
        return text, None
    records = calendar_context(results)
    conflicts = conflicting_links(text, records)
    if not conflicts:
        return text, None
    key = sha256(json.dumps([request,text,records],sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    if key in cache:
        return cache[key]
    keep = set()
    started = time.monotonic()
    by_url = {row['url']:row['id'] for row in conflicts}
    review_answer = _LINK.sub(lambda match: match[1]+(
        ' [dated link '+by_url[match[2]]+']' if match[2] in by_url else ''), text)
    try:
        response = client.create(system=(
            'Review dated URLs in one already prepared answer. Native calendar dates and actual URL query '
            'date values are supplied. Do not research, rewrite URLs, infer availability, or alter facts. '
            'A source/action URL parameterized for other dates must be omitted when presented as relevant '
            'to the selected stay, appointment or trip. Keep a conflicting URL only if the existing answer '
            'clearly uses it as an unrelated background/historical reference. Its presence in search results '
            'does not make its selected dates match the request. Return one typed decision per link.'),
            messages=[{'role':'user','content':[{'type':'text','text':json.dumps({
                'request':request,'answer':review_answer,'native_calendar_records':records[:50],
                'omitted_calendar_record_count':max(0,len(records)-50),'conflicting_links':[
                    {k:row[k] for k in ('id','label','date_values')} for row in conflicts]},ensure_ascii=False)}]}],
            tools=[{'name':'review_source_link_state','description':'Decide whether each conflicting dated URL is an unrelated background reference or must be omitted.',
                'input_schema':{'type':'object','additionalProperties':False,'required':['decisions'],'properties':{
                    'decisions':{'type':'array','minItems':len(conflicts),'maxItems':len(conflicts),'items':{
                        'type':'object','additionalProperties':False,'required':['id','disposition'], 'properties':{
                            'id':{'type':'string','enum':[row['id'] for row in conflicts]},
                            'disposition':{'type':'string','enum':['omit','background_reference']}}}}}}}],
            max_tokens=1000,timeout=15)
        blocks=[b for b in response.get('content',[]) if b.get('type')=='tool_use' and b.get('name')=='review_source_link_state']
        decisions=blocks[0]['input']['decisions'] if len(blocks)==1 else []
        ids=[row.get('id') for row in decisions]
        if len(set(ids))==len(conflicts) and set(ids)=={row['id'] for row in conflicts}:
            keep={row['id'] for row in decisions if row.get('disposition')=='background_reference'}
    except Exception:
        # A failed optional link check must preserve the answer, without
        # publishing an action URL whose selected dates remain mismatched.
        pass
    omitted=[row for row in conflicts if row['id'] not in keep]
    cleaned=text
    for row in reversed(omitted):
        cleaned=cleaned[:row['start']]+cleaned[row['end']:]
    cleaned='\n'.join(line.rstrip().rstrip('·|').rstrip() for line in cleaned.splitlines()).strip()
    receipt={'reviewed_link_count':len(conflicts),'omitted_link_count':len(omitted),
        'duration_ms':round((time.monotonic()-started)*1000,1)}
    cache[key]=(cleaned,receipt)
    return cleaned,receipt
