"""Small text normalization helpers for matching runtime-owned labels."""
from __future__ import annotations

import unicodedata

_LATIN_ASCII_FALLBACKS = str.maketrans(
    {
        "ø": "o",
        "Ø": "O",
        "æ": "ae",
        "Æ": "AE",
        "œ": "oe",
        "Œ": "OE",
        "ł": "l",
        "Ł": "L",
        "đ": "d",
        "Đ": "D",
        "ð": "d",
        "Ð": "D",
        "þ": "th",
        "Þ": "Th",
        "ı": "i",
    }
)


def ascii_match_text(value: object) -> str:
    text = str(value or "").translate(_LATIN_ASCII_FALLBACKS)
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")

