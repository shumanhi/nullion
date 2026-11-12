"""Structural formatting contracts, independent of message language or task intent."""
from html.parser import HTMLParser
from types import SimpleNamespace

import pytest

from nullion.cron_delivery import scheduled_task_delivery_text
from nullion.telegram_formatting import format_telegram_text, format_telegram_chunks, telegram_html_to_plain


class TelegramHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.visible = []

    def handle_starttag(self, tag, attrs):
        assert tag in {'b', 'i', 's', 'code', 'pre', 'a', 'blockquote'}
        assert not (tag == 'a' and 'a' in self.stack), 'Nested links are invalid'
        self.stack.append(tag)

    def handle_endtag(self, tag):
        assert self.stack.pop() == tag

    def handle_data(self, value):
        self.visible.append(value)


def visible(text, options):
    if not options:
        return text
    parser = TelegramHTML()
    parser.feed(text)
    parser.close()
    assert not parser.stack
    return ''.join(parser.visible)


@pytest.mark.parametrize('label', ['No events created:', '予定は作成されていません:', 'Sin eventos nuevos:', 'لم يتم إنشاء أحداث:'])
def test_emphasis_across_a_soft_wrap_is_rendered(label):
    words = label.split(' ')
    split = max(1, len(words) // 2)
    wrapped = ' '.join(words[:split]) + '\n' + ' '.join(words[split:]) if len(words) > 1 else label[:4] + '\n' + label[4:]
    rendered, options = format_telegram_text('**' + wrapped + '** Result.')
    assert options == {'parse_mode': 'HTML'}
    assert '**' not in rendered
    assert rendered == '<b>' + wrapped + '</b> Result.'


@pytest.mark.parametrize('source, expected', [
    ('**outer *inner***', '<b>outer <i>inner</i></b>'),
    ('__強調__ and ~~old~~', '<b>強調</b> and <s>old</s>'),
    (r'\*\*literal\*\*', '**literal**'),
    ('**https://example.com/a**', '<b><a href="https://example.com/a">https://example.com/a</a></b>'),
    ('[**Source**](https://example.com/a)', '<a href="https://example.com/a"><b>Source</b></a>'),
    ('> **引用**\n> next', '<blockquote><b>引用</b>\nnext</blockquote>'),
    ('`**literal** <b>`', '<code>**literal** &lt;b&gt;</code>'),
])
def test_markdown_is_parsed_as_syntax(source, expected):
    rendered, options = format_telegram_text(source)
    assert rendered == expected
    visible(rendered, options)


@pytest.mark.parametrize('source', [
    '**' + ('évidence 山 🌳 & < > ' * 900).strip() + '**',
    '[**' + ('Linked detail ' * 900).strip() + '**](https://example.com/a?x=1&y=2)',
    '```python\n' + ('    print("**literal** & < >")\n' * 500) + '```',
    '> **' + ('quoted ' * 900).strip() + '**',
    '🌳' * 2000,
])
def test_chunks_preserve_all_visible_content_and_balanced_formatting(source):
    whole, options = format_telegram_text(source)
    chunks = format_telegram_chunks(source, limit=400)
    assert len(chunks) > 1
    from nullion.telegram_formatting import telegram_text_units
    assert all(telegram_text_units(text) <= 400 for text, _ in chunks)
    assert ''.join(visible(text, opts) for text, opts in chunks) == visible(whole, options)


def test_emphasis_never_crosses_a_paragraph_boundary():
    rendered, _ = format_telegram_text('**unclosed\n\nnext**')
    assert '<b>' not in rendered


def test_cron_paragraphs_are_not_hard_wrapped_or_split_into_gaps():
    paragraph = 'Refunds were issued to the account balance; no action is needed. ' * 5
    body = '**Findings**\n\n• **Record:** ' + paragraph.strip() + '\n\n**Other findings**\n\n' + paragraph.strip()
    job = SimpleNamespace(name='Synthetic report')
    first = scheduled_task_delivery_text(job, body)
    assert paragraph.strip() in first
    assert first.count('\n') < 15
    assert scheduled_task_delivery_text(job, first) == first
    rendered, options = format_telegram_text(first)
    assert '**' not in visible(rendered, options)


@pytest.mark.parametrize('name', ['条件', 'تنبيه', '📊', '报告: 每日', 'Δοκιμή'])
def test_generated_cron_header_is_idempotent_for_any_job_name(name, monkeypatch):
    from nullion import preferences
    monkeypatch.setattr(preferences,'load_preferences',lambda:preferences.Preferences(emoji_level='expressive'))
    job = SimpleNamespace(name=name)
    first = scheduled_task_delivery_text(job, '37')
    assert scheduled_task_delivery_text(job, first) == first
    rendered, options = format_telegram_text(first)
    assert visible(rendered, options).count(name) == 1


def test_existing_soft_wrapped_prose_does_not_gain_blank_lines_on_delivery():
    body = '**Findings**\n\n• One item\n• Another item\n\nRefunds were issued to the\naccount balance; no action is needed. The next\nnotice requires a review.'
    result = scheduled_task_delivery_text(SimpleNamespace(name='Synthetic report'), body)
    assert 'to the\naccount balance' in result
    assert 'next\nnotice' in result


def test_scheduled_code_is_literal():
    code = '**not emphasis**\n- literal\n' + '    x = 1\n' * 20
    body = '**Findings**\n\n- Item\n- Item two\n\n```text\n' + code + '```'
    result = scheduled_task_delivery_text(SimpleNamespace(name='Example'), body)
    assert '```text\n' + code + '```' in result
    rendered, _ = format_telegram_text(result)
    assert code in telegram_html_to_plain(rendered)


def test_telegram_keeps_code_outside_emphasis_entities():
    rendered, options = format_telegram_text('**Use `x < y` now**')
    assert rendered == '<b>Use </b><code>x &lt; y</code><b> now</b>'
    assert visible(rendered, options) == 'Use x < y now'


def test_telegram_quotes_and_link_labels_do_not_nest_code_entities():
    rendered, options = format_telegram_text('> **Use `x < y`**\n\n[`value`](https://example.com/)')
    assert '<code>' not in rendered
    assert 'x &lt; y' in rendered and '>value</a>' in rendered
    visible(rendered, options)


@pytest.mark.parametrize('label', ['&', '🌳'])
def test_large_link_prefix_cannot_split_an_entity_or_stall(label):
    # At 80 characters this anchor leaves just one serialized unit for text.
    url = 'https://example.com/' + 'a' * 45
    source = '[' + label + '](<' + url + '>)'
    rendered, options = format_telegram_text(source)
    chunks = format_telegram_chunks(source, limit=80)
    assert ''.join(visible(text, opts) for text, opts in chunks) == visible(rendered, options)
    from nullion.telegram_formatting import telegram_text_units
    assert all(telegram_text_units(text) <= 80 for text, _ in chunks)


def test_plain_fallback_retains_source_urls_without_markdown_wrappers():
    from nullion.telegram_formatting import telegram_plain_text
    assert telegram_plain_text('**Result** [Source](https://example.com/?a=1&b=2)') == 'Result Source (https://example.com/?a=1&b=2)'
