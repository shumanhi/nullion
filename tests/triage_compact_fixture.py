"""Fictional native-record report with the production verbosity failure shape."""


def triage_fixture():
    rows = [
        ('Card — Nov 15', '$420 payment scheduled; posting unconfirmed.', 'Reminder still lists $460.', 'NorthBank · payment notice · received Nov 4'),
        ('Utility — Nov 18', '$120 automatic payment scheduled.', '', 'CityPower · bill ready · received Nov 1'),
        ('Rent', '$2,000 payment initiated Nov 2; allow 2–3 business days to post.', '', 'ResidentPortal · payment receipt · received Nov 2'),
        ('Reserve transfer — Nov 5', '$150 planned; completion unconfirmed.', '', 'InvestCo · reserve deposit · received Nov 4'),
        ('Savings transfer — Nov 1', '$1,000 planned.', '', 'InvestCo · savings deposit · received Oct 31'),
        ('Performance — Dec 12, 11:00 PM', 'One ticket, $70.', '2:00 AM calendar end is a placeholder, not a confirmed end time.', 'TicketDesk · purchase confirmation · received Oct 23'),
    ]
    items = []; sources = []; records = []; verbose = []
    total = 'Calendar: 0 new events; 6 existing matches.'
    for i, (title, summary, exception, label) in enumerate(rows):
        subject = 'Full optional notification wording '+str(i)+': '+('Detailed account and transaction confirmation subject. '*4)
        item = {'title': title, 'summary': summary, 'required_details': [exception] if exception else [],
            'extra_details': ['Calendar duplicate skipped; the matching event was retained.'],
            'source_ids': [str(i)], 'shared_detail_ids': ['calendar']}
        source = {'id': str(i), 'label': label, 'expanded_label': subject, 'url': 'https://records.example/'+str(i)}
        items.append(item); sources.append(source)
        records.append({'title': title, 'summary': summary, 'exception': exception, 'source_label': label,
            'expanded_source_label': subject, 'source_url': source['url'], 'calendar_status': 'Existing match; duplicate skipped.'})
        verbose.append(title+' — '+summary+' '+exception+' Sender/subject/received: '+label+' '+subject
            +' Calendar duplicate skipped; the matching event was retained. ('+label+')')
    message = {'summary': 'Two outcomes need confirmation; the matching calendar entries already exist.',
        'detail_level': 'compact', 'sources': sources, 'shared_details': [{'id': 'calendar', 'text': total}],
        'sections': [{'heading': 'Needs attention', 'priority': 'primary', 'layout': 'list', 'items': [items[0], items[3]]},
            {'heading': 'Already covered', 'priority': 'secondary', 'layout': 'list', 'items': [items[i] for i in (1, 2, 4, 5)]}]}
    return message, '\n\n'.join(verbose)+'\nSearch chains were paginated through terminal pages. '+total, {
        'value': 7, 'records': records, 'calendar_outcome': total}
