"""Same-capture fields with a title captured as an attribute, not a new entity."""
from nullion.source_observation_contract import source_answer_context
from nullion.tools import ToolResult


def grouped_source_fixture(name='Trail SUV', *, shared_quote=True):
    descriptions = [
        ('entity', 'Oak Compact\n$118 Total\nFree cancellation', 100),
        ('attribute', 'Selected offer information\n$137 Total\nUnlimited miles\nFull-to-full\nFree cancellation before pickup', 120),
        ('attribute', name + '\nor similar Intermediate SUV\nPickup: 743 Sample Road', 120),
        ('interface_control', 'Choose an option', 120),
        ('entity', name + '\nEarlier listing', 110),
    ]
    report = {'result_kind': 'source_observations', 'report_status': 'partial', 'goal_status': 'partial',
        'next_action': 'report', 'observations': [
            {'label': str(i), 'value': quote, 'quote': quote, 'semantic_role': role,
             'source_url': 'https://catalog.example/offers', 'captured_at': stamp}
            for i, (role, quote, stamp) in enumerate(descriptions)],
        'unknowns': ['Fuel type is not shown for either offer.', 'Whether mandatory fees are included is not shown.']}
    if shared_quote:
        quote = descriptions[1][1] + '\n' + descriptions[2][1]
        for row in report['observations'][1:3]: row['quote'] = quote
    results = [ToolResult('captured', 'opaque_capture', 'completed', report)]
    rows = source_answer_context(results)['evidence']
    point = {'evidence_ids': [rows[1]['id'], rows[2]['id']], 'semantic_role': 'entity',
        'title_ids': [next(e['id'] for e in rows[2]['excerpts'] if e['text'] == name)],
        'detail_ids': [e['id'] for e in rows[1]['excerpts'] if e['text'] in descriptions[1][1].splitlines()[1:]]
            + [next(e['id'] for e in rows[2]['excerpts'] if e['text'] == 'Pickup: 743 Sample Road')],
        'relevance': 'The requested offer and its captured terms'}
    point['detail_groups'] = [[identity] for identity in point['detail_ids']]
    plan = {'intro': '', 'focus_roles': ['entity', 'attribute', 'context'], 'points': [
        {'evidence_ids': [rows[0]['id']], 'semantic_role': 'entity',
         'title_ids': [rows[0]['excerpts'][0]['id']], 'detail_ids': [rows[0]['excerpts'][1]['id']],
         'relevance': 'The other requested offer'}, point], 'limitation_ids': ['u0', 'u1']}
    return results, plan
