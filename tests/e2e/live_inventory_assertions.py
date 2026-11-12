"""Check native numbered identities without requiring a verbatim prose copy."""
import re
import unicodedata


def _text(value):
    return ' '.join(unicodedata.normalize('NFKC',str(value)).replace('**','').split())


def assert_numbered_inventory_identities(reply,inventory):
    expected={int(row['selection_index']):_text(row['name']) for row in inventory['inventory_index']}
    rendered={}
    for line in reply.splitlines():
        match=re.fullmatch(r'\s*(\d+)[.)]\s+(.+?)\s*',line)
        if match:
            index=int(match.group(1));assert index not in rendered,('Duplicate inventory index',index)
            rendered[index]=_text(match.group(2))
    assert set(rendered)==set(expected),('Missing or extra numbered identities',set(expected)-set(rendered),set(rendered)-set(expected))
    for index,name in expected.items():
        body=rendered[index]
        if name in body:
            continue
        # A concise label is valid only if it is a unique whole-field prefix
        # of this exact numbered identity, followed by a presentation divider.
        # Overlapping labels and a different row's label fail.
        prefixes=[body[:match.start()].strip() for match in re.finditer(r'\s+[—–|]\s+',body)]
        valid=False
        for label in prefixes:
            if not label or not name.startswith(label) or len(label)>=len(name):continue
            if not name[len(label)].isspace():continue
            candidates=[key for key,value in expected.items() if value.startswith(label)]
            if candidates==[index]:valid=True;break
        assert valid,('Inventory identity was lost or ambiguous',index,name,body)
    return {'native_count':len(expected),'rendered_count':len(rendered),'all_identities_present':True}
