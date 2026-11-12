import pytest
from nullion.redaction import redact_text


@pytest.mark.parametrize('label',['情報源','Fuente'])
def test_secret_redaction_preserves_markdown_and_other_query_values(label):
    raw=f'[{label}](https://example.test/check?token=synthetic-secret-value&language=es)'
    redacted=redact_text(raw)
    assert 'synthetic-secret-value' not in redacted
    assert redacted==f'[{label}](https://example.test/check?token=[redacted]&language=es)'
    assert redact_text(redacted)==redacted


@pytest.mark.parametrize('quote',["'",'"'])
def test_quoted_secrets_include_punctuation_without_eating_structure(quote):
    raw='password='+quote+'secret-value(with)[brackets]'+quote+'; next=visible'
    assert redact_text(raw)=='password='+quote+'[redacted]'+quote+'; next=visible'


@pytest.mark.parametrize('secret',['long-secret(with)suffix','short(very-long-secret)','long-secret[private]','long-secret&private'])
def test_unquoted_non_url_secrets_do_not_leak_punctuation_suffixes(secret):
    assert redact_text('password='+secret)=='password=[redacted]'


@pytest.mark.parametrize('secret',['long-secret(with)suffix','short(very-long-secret)','long-secret[private]'])
def test_url_secret_punctuation_is_masked_with_markdown_and_public_query_preserved(secret):
    raw=f'[Fuente](https://example.test/check?token={secret}&language=es&password=second-secret)'
    masked='[Fuente](https://example.test/check?token=[redacted]&language=es&password=[redacted])'
    assert redact_text(raw)==masked
    assert redact_text(masked)==masked


@pytest.mark.parametrize('emoji',['none','light'])
def test_shared_presentation_redacts_secret_urls_before_every_adapter(emoji):
    from types import SimpleNamespace
    from nullion.chat_text import apply_presentation_preferences
    prefs=SimpleNamespace(markdown_style='light',emoji_level=emoji)
    raw='[Fuente](https://example.test/check?token=synthetic-secret-value)'
    assert apply_presentation_preferences(raw,preferences=prefs)=='[Fuente](https://example.test/check?token=[redacted])'
