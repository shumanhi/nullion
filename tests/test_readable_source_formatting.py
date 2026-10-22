import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from nullion.source_observation_contract import partial_source_report_reply
from nullion.telegram_formatting import format_telegram_text

REPORT = json.loads((Path(__file__).parent / 'fixtures/nearby_source_report.json').read_text())


def test_recorded_nearby_report_is_readable_and_deduplicated():
    from nullion.source_observation_contract import source_answer_context
    results=[SimpleNamespace(output=REPORT,status='completed')]
    rows=source_answer_context(results)['evidence']
    assert len(rows)<len(REPORT['observations'])
    draft={'intro':'Nearby options:', 'points':[{'evidence_ids':[row['id']],
        'detail':'\n'.join(row['value'].splitlines()[1:]), 'relevance':'Nearby place'} for row in rows[:7]],
        'limitation_ids':[f'u{i}' for i in range(len(REPORT['unknowns']))][:3]}
    text=partial_source_report_reply(results,draft=draft)
    assert text.count('**Riverfront Park**')==1
    assert 'nearby listing:' not in text and '\ue934' not in text and '· ·' not in text
    rendered,options=format_telegram_text(text)
    assert '<b>Riverfront Park</b>' in rendered
    assert options=={'parse_mode':'HTML'}


@pytest.mark.parametrize('title,label', [('公園', '近くの施設'), ('Parque', 'Lugar cercano'), ('حديقة', 'مكان')])
def test_multiline_excerpts_do_not_depend_on_english_labels(title, label):
    row = {'label':label, 'value':title+'\n19:30', 'quote':title+'\n19:30'}
    text = partial_source_report_reply([SimpleNamespace(status='completed', output={'result_kind':'source_observations','report_status':'completed','observations':[row, {**row, 'label':'different'}]})],draft={'intro':'','points':[{'evidence_ids':['e0.0'],'detail':'19:30','relevance':'Requested time'}],'limitation_ids':[]})
    assert text == f'- **{title}** — 19:30'


def test_telegram_headings_tables_links_and_code_are_readable():
    text = '# Choices\n\n| Name | Time |\n| --- | --- |\n| 公園 | 19:30 |\n\n[Source](https://example.com/?a=1&b=2)\n```text\n# literal\n[link](https://example.com/)\n```'
    rendered, options = format_telegram_text(text)
    assert '<b>Choices</b>' in rendered and '| ---' not in rendered
    assert '<a href="https://example.com/?a=1&amp;b=2">Source</a>' in rendered
    assert '# literal\n[link](https://example.com/)' in rendered
    assert options['parse_mode'] == 'HTML'


def test_long_replies_split_between_entries_before_splitting_details():
    from nullion.messaging_adapters import split_reply_for_platform
    first = '1. **One**\n' + 'a' * 65
    second = '2. **Two**\n' + 'b' * 40 + '\n19:30'
    assert split_reply_for_platform(first + '\n\n' + second, limit=120) == [first, second]


def test_telegram_preserves_literal_bullets_and_tables_in_code():
    code = '- literal\n| A | B |\n| --- | --- |\n| 1 | 2 |'
    rendered, _ = format_telegram_text('```text\n' + code + '\n```\n- visible')
    assert code in rendered
    assert '• visible' in rendered


def test_shared_preparation_preserves_compacted_code_and_html_literals():
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    code = '<b>literal</b>\n| A | B | | --- | --- | | 1 | 2 |'
    text = 'Outside <b>emphasis</b>\n\n```html\n' + code + '\n```\n`<i>inline</i>`'
    prepared = prepare_reply_for_platform_delivery(text).text
    assert code in prepared and '`<i>inline</i>`' in prepared
    assert 'Outside emphasis' in prepared
    rendered, _ = format_telegram_text(prepared)
    assert '&lt;b&gt;literal&lt;/b&gt;' in rendered
    assert '| A | B | | --- | --- | | 1 | 2 |' in rendered
    assert '<code>&lt;i&gt;inline&lt;/i&gt;</code>' in rendered


def test_python_literal_quote_fences_are_not_rewritten_inside_markdown_code():
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery, split_reply_for_platform
    text="```python\n'''\ndocstring\n'''\n```"
    assert prepare_reply_for_platform_delivery(text).text==text
    chunks=split_reply_for_platform('```'+'x'*1000+'\n'+('a'*400)+'\n```',limit=100)
    assert chunks and all(len(chunk)<=100 for chunk in chunks)


def test_final_sanitizer_preserves_literal_indentation_numbering_and_unicode():
    from nullion.response_sanitizer import sanitize_user_visible_reply
    code = '    8. literal 🌳\n    9. literal\n\n\n\n    公園'
    text = '7. Visible\n8. Visible\n\n```text\n' + code + '\n```\n\n`8. inline 🌳`'
    cleaned = sanitize_user_visible_reply(user_message='Format this', reply=text)
    assert code in cleaned and '`8. inline 🌳`' in cleaned
    assert cleaned.startswith('1. Visible\n2. Visible')
    # Protecting literal code must not bypass invisible-control cleanup.
    assert '\u200b' not in sanitize_user_visible_reply(user_message='Format this', reply='```text\na\u200bb\n```')


def test_table_empty_edge_cells_and_extra_columns_do_not_lose_values():
    from nullion.chat_text import make_markdown_tables_chat_readable
    text = make_markdown_tables_chat_readable('| Name | Time |\n| --- | --- |\n| | 19:30 |\n| 公園 | |\n| Plaza | 20:00 | Extra fact |')
    assert 'Time: 19:30' in text and '公園' in text
    assert 'Extra fact' in text


def test_source_renderer_keeps_only_completed_quoted_rows_and_discloses_omissions():
    report = {'result_kind':'source_observations','report_status':'partial','omitted_observation_count':3,
              'observations':[{'label':'事実','value':'19:30','quote':'19:30'},
                              {'label':'UNSUPPORTED','value':'invented','quote':'different'}]}
    text = partial_source_report_reply([SimpleNamespace(status='failed',output={**report,'observations':[{'value':'FAILED_ROW','quote':'FAILED_ROW'}]}),
                                       SimpleNamespace(status='completed',output=report)],draft={'intro':'','points':[{'evidence_ids':['e1.0'],'detail':'','relevance':'Requested time'}],'limitation_ids':[]})
    assert '19:30' in text and 'additional findings' not in text
    assert 'FAILED_ROW' not in text and 'UNSUPPORTED' not in text and 'invented' not in text
