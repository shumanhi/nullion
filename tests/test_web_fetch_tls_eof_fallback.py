from __future__ import annotations

import ssl
from types import SimpleNamespace
import urllib.error

from nullion.tools import _default_web_fetcher


def test_web_fetch_retries_same_url_over_http_after_https_transport_eof(monkeypatch) -> None:
    calls: list[str] = []

    class FakeResponse:
        status = 200

        def __init__(self) -> None:
            self.headers = SimpleNamespace(get_content_type=lambda: "text/xml")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self, size):
            return b"<?xml version='1.0'?><rss><channel><title>Noticias</title></channel></rss>"

    class FakeOpener:
        def open(self, request, timeout):
            calls.append(request.full_url)
            if request.full_url.startswith("https://"):
                raise urllib.error.URLError(
                    ssl.SSLEOFError(8, "[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol")
                )
            return FakeResponse()

    monkeypatch.setattr(
        "nullion.tools._resolve_web_fetch_resolution",
        lambda url: SimpleNamespace(host="feeds.example.test", address_infos=()),
    )
    monkeypatch.setattr("nullion.tools._build_web_fetch_opener", lambda: FakeOpener())

    result = _default_web_fetcher("https://feeds.example.test/noticias.xml", 5)

    assert calls == [
        "https://feeds.example.test/noticias.xml",
        "http://feeds.example.test/noticias.xml",
    ]
    assert result["url"] == "http://feeds.example.test/noticias.xml"
    assert result["requested_url"] == "https://feeds.example.test/noticias.xml"
    assert result["transport_fallback"] == {
        "from_scheme": "https",
        "to_scheme": "http",
        "reason": "https_transport_eof",
    }
    assert "Noticias" in result["text"]
