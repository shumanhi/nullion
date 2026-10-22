"""Unicode data tokenization for recall, independent of task intent or language."""
import re
import unicodedata


def normalize_search_text(value: object) -> str:
    return unicodedata.normalize('NFKC', str(value or '')).casefold()


def has_uncased_letters(token: str) -> bool:
    return any(unicodedata.category(char) == 'Lo' for char in token)


def search_tokens(value: object) -> tuple[str, ...]:
    tokens = []
    for word in re.findall(r'[^\W_]+', normalize_search_text(value)):
        tokens.append(word)
        # Uncased scripts often have no word delimiters. Corpus-weighted
        # character pairs let a topic match inside a longer sentence.
        if has_uncased_letters(word):
            tokens.extend(word[index:index+2] for index in range(len(word)-1))
    return tuple(dict.fromkeys(tokens))
