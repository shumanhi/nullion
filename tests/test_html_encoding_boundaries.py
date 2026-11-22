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


def test_legacy_html_declaration_is_browser_compatible():
    from nullion.artifacts import decode_html_document

    source = '<html><head><meta charset="iso-8859-1"></head><body>Asunción · €</body></html>'
    assert decode_html_document(source.encode("cp1252")) == source


def test_html_mime_utf16le_is_honored_without_bom():
    from nullion.artifacts import decode_html_document

    source = '<html><head><meta charset="windows-1252"></head><body>北 → 南</body></html>'
    assert decode_html_document(source.encode("utf-16-le"), content_type="text/html; charset=utf-16le") == source


def test_html_bom_takes_precedence_over_transport_and_document_labels():
    from nullion.artifacts import decode_html_document

    source = '<html><head><meta charset="windows-1252"></head><body>北 → 南</body></html>'
    assert decode_html_document(source.encode("utf-16"), content_type="text/html; charset=ascii") == source


def test_non_web_codec_label_does_not_transform_html_text():
    from nullion.artifacts import decode_html_document

    source = r'<html><head><meta charset="unicode_escape"></head><body>\u00a3</body></html>'
    assert decode_html_document(source.encode("utf-8")) == source


def test_normalization_preserves_literal_meta_markup_in_textarea():
    sample = '<meta charset="windows-1252">'
    source = '<html><body><textarea>' + sample + '</textarea></body></html>'
    assert '<textarea>' + sample + '</textarea>' in normalize_html_document(source)


@pytest.mark.parametrize("source", [
    '<html><body>ASCII</body></html>\n',
    '<html><head><meta charset="UTF-8"></head><body>北 → 南</body></html>\n',
    '\ufeff<html><body>北 → 南</body></html>\n',
])
def test_utf8_write_preserves_existing_safe_html_bytes(source):
    from nullion.artifacts import ensure_utf8_html_encoding

    assert ensure_utf8_html_encoding(source, title="fixture") == source


def test_utf8_write_corrects_charset_without_stripping_document():
    from nullion.artifacts import ensure_utf8_html_encoding

    source = '  <html><head><meta charset="windows-1252"></head><body>北 → 南</body></html>\n'
    expected = source.replace('windows-1252', 'utf-8')
    assert ensure_utf8_html_encoding(source, title="fixture") == expected


@pytest.mark.parametrize("tag", ["iframe", "noembed", "noframes", "xmp"])
def test_utf8_write_preserves_literal_meta_in_raw_text_elements(tag, monkeypatch):
    from html.parser import HTMLParser
    from nullion.artifacts import ensure_utf8_html_encoding

    # Earlier supported CPython releases treated only script/style as raw text.
    monkeypatch.setattr(HTMLParser, "CDATA_CONTENT_ELEMENTS", ("script", "style"))

    example = '<meta charset="windows-1252"> 北 → 南'
    raw_text = f"<{tag}>{example}</{tag}>"
    source = f"<html><body>{raw_text}</body></html>"
    repaired = ensure_utf8_html_encoding(source, title="fixture")
    assert raw_text in repaired
    assert '<meta charset="utf-8">' in repaired
