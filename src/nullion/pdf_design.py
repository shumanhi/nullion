"""A compact, typed content contract for designed PDF cards."""
from html import escape
from functools import lru_cache
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StrictBool, TypeAdapter


class PdfCard(BaseModel):
    model_config = ConfigDict(extra="forbid")
    heading: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=1, max_length=5000)
    image_path: str | None = Field(default=None, max_length=4000)
    wide: StrictBool = False


class PdfCardPage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = Field(min_length=1, max_length=200)
    subtitle: str = Field(default="", max_length=500)
    cards: list[PdfCard] = Field(min_length=1, max_length=12)


_pages = TypeAdapter(Annotated[list[PdfCardPage], Field(min_length=1, max_length=6)])


@lru_cache(maxsize=1)
def card_pages_schema():
    # Inline definitions keep the tool contract usable by all supported clients.
    schema = _pages.json_schema()
    definitions = schema.pop("$defs", {})
    def inline(value):
        if isinstance(value, list): return [inline(item) for item in value]
        if not isinstance(value, dict): return value
        if "$ref" in value: return inline(definitions[value["$ref"].rsplit("/", 1)[-1]])
        return {key: inline(item) for key, item in value.items()}
    return inline(schema)


def render_card_pages(value, *, image_paths=(), output_kind="pdf"):
    pages = _pages.validate_python(value)
    style = """<style>
    .n-card-report{display:flow-root;padding:20px;background:#f4f2ed;color:#193b35;font-family:Arial,sans-serif;}
    .n-card-report header{border-bottom:3px solid #b68551;margin-bottom:16px;padding-bottom:12px;}
    .n-card-report h1{font-size:27px;line-height:1.15;margin:0 0 8px;}
    .n-card-report header p{font-size:10.5pt;color:#52645e;margin:0;}
    .n-card-cover{display:flex;justify-content:center;gap:10px;flex-wrap:wrap;margin:12px 0 0;}
    .n-card-cover img{width:auto;height:95px;max-width:100%;object-fit:contain;}
    .n-card-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:start;}
    .n-card{background:white;border:1px solid #dfdfd5;border-left:4px solid #b68551;border-radius:10px;padding:14px;}
    .n-card-wide{grid-column:1/-1;}
    .n-card h2{font-size:16px;line-height:1.2;margin:0 0 9px;}
    .n-card p{font-size:11pt;line-height:1.45;margin:0;white-space:pre-wrap;overflow-wrap:anywhere;}
    .n-card img{display:block;width:100%;height:auto;max-height:130px;object-fit:contain;margin:12px auto 0;}
    </style>"""
    if output_kind == "image":
        style += """<style>
        .n-card-report{padding:36px;box-sizing:border-box;}
        .n-card-report header{padding-bottom:24px;margin-bottom:24px;}
        .n-card-report h1{font-size:44px;overflow-wrap:anywhere;}
        .n-card-report header p{font-size:21px;line-height:1.4;}
        .n-card-grid{grid-template-columns:1fr;gap:22px;}
        .n-card{padding:22px;border-radius:16px;min-width:0;}
        .n-card h2{font-size:29px;line-height:1.25;margin-bottom:14px;overflow-wrap:anywhere;}
        .n-card p{font-size:26px;line-height:1.4;}
        .n-card img{max-height:180px;}
        @media(min-width:960px){.n-card-grid{grid-template-columns:1fr 1fr;}}
        </style>"""
    rendered = []
    placed = {card.image_path for page in pages for card in page.cards if card.image_path}
    cover = ''.join('<img src="' + escape(path, quote=True) + '">' for path in dict.fromkeys(image_paths) if path not in placed)
    for index, page in enumerate(pages):
        cards = []
        for card in page.cards:
            art = '<img src="' + escape(card.image_path, quote=True) + '">' if card.image_path else ""
            cards.append('<article class="n-card' + (' n-card-wide' if card.wide else '') + '"><h2>'
                         + escape(card.heading) + '</h2><p>' + escape(card.text) + '</p>' + art + '</article>')
        rendered.append(style + '<main class="n-card-report"><header><h1>' + escape(page.title)
                        + '</h1><p>' + escape(page.subtitle) + '</p>'
                        + ('<div class="n-card-cover">' + cover + '</div>' if index == 0 and cover else '')
                        + '</header><div class="n-card-grid">'
                        + ''.join(cards) + '</div></main>')
    return rendered
