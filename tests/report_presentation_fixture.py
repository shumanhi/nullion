"""Synthetic obligations with the shape of the overlong triage regression."""
from copy import deepcopy


def report_fixture():
    rows = [
        ('Transfer', 'Nov 5, 4:00 PM — $200 automatic transfer.', 'Review before the skip deadline.', 'primary'),
        ('Statement', 'Nov 14 — payment due; amount unavailable.', 'Read the statement before paying.', 'primary'),
        ('Renewal', 'Dec 2 — $60 renewal.', 'Cancel beforehand if unwanted.', 'primary'),
        ('Card', 'Nov 7 — $35 minimum; $720 scheduled.', 'Final posting is unconfirmed; the reminder retains an older $760 amount.', 'secondary'),
        ('Membership', 'Nov 13 — $5 automatic renewal.', '', 'secondary'),
        ('Property', '$3,800 receipt received.', 'Posting may take 2–3 business days.', 'secondary'),
        ('Completed transfer', '$1,000 transfer confirmed complete.', '', 'secondary'),
        ('Offer', 'Nov 6 — optional discount.', '', 'secondary'),
        ('Reward', 'Nov 12 — optional $10 reward expires.', '', 'secondary'),
        ('Benefits notice', 'Not a bill; no due date.', '', 'secondary'),
    ]
    common = 'Calendar: 0 new events; existing reminders cover Transfer, Statement, Renewal, Card and Membership.'
    groups = {'primary': [], 'secondary': []}; sources = []; expanded = []
    for index, (title, summary, required, priority) in enumerate(rows):
        ident = 's' + str(index)
        source = {'id': ident, 'label': f'Example {title}, "Notice", received Nov 1',
                  'expanded_label': f'Full subject: Important detailed account notification for {title}'}
        sources.append(source)
        optional = [f'Historical detail {index}: the matching record was reviewed and the same calendar explanation need not be repeated.']
        item = {'title': title, 'summary': summary, 'required_details': [required] if required else [],
                'extra_details': optional, 'source_ids': [ident],
                'shared_detail_ids': ['calendar'] if index < 5 else []}
        groups[priority].append(item)
        expanded.append(' '.join([title, summary, required, *optional, source['label'], source['expanded_label'], common]))
    message = {'title': '', 'summary': 'Three upcoming items need attention.', 'detail_level': 'compact',
        'sections': [{'heading': 'Act now', 'priority': 'primary', 'layout': 'list', 'items': groups['primary']},
                     {'heading': 'Other findings', 'priority': 'secondary', 'layout': 'list', 'items': groups['secondary']}],
        'sources': sources, 'shared_details': [{'id': 'calendar', 'text': common}], 'footer': '',
        'extra_details': ['Report-wide explanation: calendar status was checked against current entries. '
            'Email notices establish what was announced at the time, rather than subsequent account settlement.']}
    source_records = [deepcopy(item) for group in groups.values() for item in group]
    return message, '\n\n'.join(expanded), {'records': source_records, 'sources': sources,
                                           'calendar_outcome': common}


def triage_outcome_fixture():
    """Seven synthetic outcomes matching the production verbosity failure."""
    rows = [
        ('Property', '$1,200 balance due.', 'Payment unconfirmed.', 'primary'),
        ('Card', 'Nov 7 — $105.80 payment scheduled.', 'Posting unconfirmed; old reminder says $102.', 'primary'),
        ('Investment', 'Nov 1 — $400 transfer scheduled.', 'Completion unconfirmed.', 'secondary'),
        ('Savings', 'Nov 5 — $90 transfer scheduled.', 'Completion unconfirmed.', 'secondary'),
        ('Utility', 'Nov 16 — $41 automatic payment.', '', 'secondary'),
        ('Subscription', 'Dec 2 — $12 renewal.', 'Cancel beforehand if unwanted.', 'secondary'),
        ('Rail trip', 'Nov 25 trip canceled; removed from calendar.', '', 'secondary'),
    ]
    items = {'primary': [], 'secondary': []}
    for index, (title, summary, exception, priority) in enumerate(rows):
        items[priority].append({'title': title, 'summary': summary,
            'required_details': [exception] if exception else [],
            'extra_details': ['The notice records what was scheduled, not proof of later settlement.'],
            'source_ids': ['s' + str(index)], 'shared_detail_ids': ['calendar'] if index < 6 else []})
    return {'summary': 'Check the property balance and card payment.', 'detail_level': 'compact',
        'sections': [{'heading': 'Needs attention', 'priority': 'primary', 'layout': 'list', 'items': items['primary']},
            {'heading': 'Other items', 'priority': 'secondary', 'layout': 'list', 'items': items['secondary']}],
        'sources': [{'id': 's'+str(i), 'label': title+' notice · Nov '+str(i+1),
            'expanded_label': 'Full subject: detailed original notice for '+title} for i,(title,*_) in enumerate(rows)],
        'shared_details': [{'id': 'calendar', 'text': 'Calendar: 6 existing reminders; 0 new events.'}],
        'extra_details': ['Calendar status was verified against the current calendar. '
            'A matching entry means another duplicate is unnecessary. Emails confirm what was announced '
            'or scheduled at the time, rather than whether the property balance was subsequently paid '
            'or the card and investment transactions ultimately completed.'], 'footer': ''}
