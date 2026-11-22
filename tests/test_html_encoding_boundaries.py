import pytest

from nullion.artifacts import normalize_html_document


@pytest.mark.parametrize('document', [
    '<!doctype html><html><body><h1>{text}</h1></body></html>',
    '<html><head><style>'+'/* style */'*200+'</style><meta charset="windows-1252"></head><body>{text}</body></html>',
    '<html><head><meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"></head><body>{text}</body></html>',
    '<body>{text}</body>',
    '<section>{text}</section>',
])
@pytest.mark.parametrize('text', ['Montréal → Québec · été — €', '東京 → 大阪 · 旅程', 'Bogotá → México · información'])
def test_documents_and_fragments_declare_utf8_before_non_ascii(document, text):
    normalized = normalize_html_document(document.format(text=text))
    assert '<meta charset="utf-8">' in normalized
    assert normalized.encode('utf-8').index(b'<meta charset="utf-8">') < 1000
    assert text in normalized
    assert 'windows-1252' not in normalized and 'iso-8859-1' not in normalized
    assert normalize_html_document(normalized) == normalized


def test_encoding_normalization_preserves_literal_script_style_and_comment_text():
    literal = '<meta charset="windows-1252">'
    document = '<html><head><script>const sample = '+repr(literal)+';</script></head><body><!--'+literal+'--></body></html>'
    normalized = normalize_html_document(document)
    assert '<script>const sample = '+repr(literal)+';</script>' in normalized
    assert '<!--'+literal+'-->' in normalized
    assert normalized.startswith('<html><head><meta charset="utf-8">')


def test_html_parser_offsets_follow_newlines_and_preserve_unicode_separators():
    source = '<html>\u2028\n<head><meta charset="latin1"></head><body>北 → 南</body></html>'
    normalized = normalize_html_document(source)
    assert normalized == '<html>\u2028\n<head><meta charset="utf-8"></head><body>北 → 南</body></html>'


def test_long_opening_tag_uses_encoding_signature_without_reordering_content():
    prefix = '<!-- sample: <meta charset="utf-8"> -->'
    source = prefix + '<html data-note="' + 'x'*1100 + '"><body>北 → 南</body></html>'
    normalized = normalize_html_document(source)
    assert normalized.startswith('\ufeff'+prefix)
    assert '北 → 南' in normalized
    assert normalize_html_document(normalized) == normalized
