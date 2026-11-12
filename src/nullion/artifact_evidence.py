"""Runtime-bound artifact records for per-deliverable completion coverage."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from nullion.response_fulfillment_contract import artifact_paths_from_tool_results
from nullion.tools import ToolResult, ToolSideEffectClass


def source_inventory_reference(output: object) -> dict[str, object] | None:
    """Keep a bounded typed source handle without copying its private rows."""
    source = output.get("source_inventory") if isinstance(output, dict) else None
    if not isinstance(source, dict):
        return None
    if source.get("format") != "json" or source.get("role") != "source":
        return None
    count = source.get("record_count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        return None
    reference = {"format": "json", "role": "source", "record_count": count}
    for key in ("path", "rows_key", "columns_key", "extracted_root"):
        value = source.get(key)
        if value is None and key == "extracted_root":
            continue
        if not isinstance(value, str) or not value.strip() or len(value) > 4096:
            return None
        reference[key] = value
    counts = source.get("extension_counts")
    if (isinstance(counts, dict) and len(counts) <= 128
            and all(isinstance(key, str) and 0 < len(key) <= 64
                    and isinstance(value, int) and not isinstance(value, bool) and value >= 0
                    for key, value in counts.items())
            and sum(counts.values()) == count):
        reference["extension_counts"] = dict(counts)
    return reference


def _svg_has_drawing(element) -> bool:
    drawing_tags = {"path", "rect", "circle", "ellipse", "line", "polygon", "polyline", "image", "text", "use"}
    definitions = {"defs", "symbol", "clipPath", "mask", "pattern", "marker", "script", "style", "metadata"}
    pending = [element]
    while pending:
        current = pending.pop()
        name = str(current.tag).rsplit("}", 1)[-1]
        if name in definitions:
            continue
        if name in drawing_tags:
            return True
        pending.extend(current)
    return False


def inline_svg_xml_document(fragment: str) -> str:
    """Preserve unambiguous HTML character references in an XML SVG document."""
    import re
    from html import unescape
    from html.entities import html5
    from xml.sax.saxutils import escape

    def reference(match):
        token = match.group(0)
        decoded = unescape(token)
        # Semicolon-less named references are context-sensitive in attributes.
        # Do not guess their meaning when the original is not already XML.
        if (decoded != token and not token.startswith("&#")
                and (not token.endswith(";") or token[1:] not in html5)):
            return token
        return escape(decoded, {'"': "&quot;", "'": "&apos;"})

    sections = re.split(r"(<!\[CDATA\[.*?\]\]>)", fragment, flags=re.DOTALL)
    return "".join(section if section.startswith("<![CDATA[") else re.sub(
        r"&(?:#[xX][0-9A-Fa-f]+;?|#[0-9]+;?|[A-Za-z][A-Za-z0-9]*;?)?", reference, section)
        for section in sections)


def inline_svg_fragments(content: str) -> tuple[tuple[int, int, str], ...]:
    """Locate complete SVG illustrations, preserving their exact source spans."""
    from html.parser import HTMLParser
    from xml.etree import ElementTree as ET

    if not isinstance(content, str):
        return ()
    try:
        if len(content.encode("utf-8")) > 5 * 1024 * 1024:
            return ()
    except UnicodeError:
        return ()
    offsets = []
    position = 0
    for line in content.splitlines(keepends=True):
        offsets.append(position)
        position += len(line)

    class Collector(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=False)
            self.depth = 0
            self.start = 0
            self.spans = []

        def source_offset(self):
            line, column = self.getpos()
            return offsets[line - 1] + column

        def handle_starttag(self, tag, attrs):
            if tag == "svg":
                if not self.depth:
                    self.start = self.source_offset()
                self.depth += 1

        def handle_endtag(self, tag):
            if tag == "svg" and self.depth:
                self.depth -= 1
                if not self.depth:
                    end = content.find(">", self.source_offset()) + 1
                    self.spans.append((self.start, end, content[self.start:end]))

        def handle_startendtag(self, tag, attrs):
            # A self-closing SVG has no drawing. Child shapes do not change
            # the containing SVG's depth.
            pass

    parser = Collector()
    try:
        parser.feed(content)
        parser.close()
    except (ValueError, IndexError):
        return ()
    fragments = []
    for start, end, fragment in parser.spans:
        try:
            root = ET.fromstring(inline_svg_xml_document(fragment))
        except (ET.ParseError, UnicodeError):
            continue
        if str(root.tag).rsplit("}", 1)[-1] == "svg" and _svg_has_drawing(root):
            fragments.append((start, end, fragment))
    return tuple(fragments)


def self_contained_svg_fragment(fragment: str | bytes) -> bool:
    """Validate SVG resource references without fetching or interpreting intent."""
    import re
    from xml.etree import ElementTree as ET
    try:
        root = ET.fromstring(fragment)
    except (ET.ParseError, ValueError, UnicodeError):
        return False
    if str(root.tag).rsplit("}", 1)[-1].lower() != "svg":
        return False
    for element in root.iter():
        for key, value in element.attrib.items():
            if key.rsplit("}", 1)[-1].lower() in {"href", "src"} and not value.startswith(("#", "data:")):
                return False
        css = " ".join([*element.attrib.values(), element.text or ""])
        if re.search(r"@import\s", css, flags=re.IGNORECASE):
            return False
        for match in re.finditer(r"url\(\s*['\"]?([^)'\"]+)", css, flags=re.IGNORECASE):
            if not match.group(1).strip().startswith(("#", "data:")):
                return False
    return True


def svg_data_image_document(source: str, *, require_drawing: bool = True) -> str | None:
    """Decode a bounded, drawable, self-contained SVG image data URI."""
    import base64
    from urllib.parse import unquote_to_bytes
    from xml.etree import ElementTree as ET
    if not isinstance(source, str) or len(source) > 10 * 1024 * 1024 or "#" in source:
        return None
    header, separator, payload = source.partition(",")
    if not separator or header.split(";", 1)[0].casefold() != "data:image/svg+xml":
        return None
    try:
        data = (base64.b64decode("".join(payload.split()), validate=True)
                if ";base64" in header.casefold() else unquote_to_bytes(payload))
        if len(data) > 5 * 1024 * 1024:
            return None
        document = data.decode("utf-8")
        root = ET.fromstring(document)
        if root.tag != "{http://www.w3.org/2000/svg}svg":
            return None
    except (ValueError, UnicodeError, ET.ParseError):
        return None
    if not self_contained_svg_fragment(document):
        return None
    if require_drawing and not _svg_has_drawing(root):
        return None
    return document


def html_svg_media_summary(content: str) -> dict[str, int]:
    """Return content-free media diagnostics and the shared SVG evidence count."""
    from html.parser import HTMLParser

    class Sources(HTMLParser):
        def __init__(self):
            super().__init__()
            self.sources = []

        def handle_starttag(self, tag, attrs):
            if tag == "img":
                self.sources.append(dict(attrs).get("src") or "")

    parser = Sources()
    try:
        parser.feed(content)
    except (ValueError, IndexError):
        parser.sources = []
    sources = parser.sources
    data_svg = [source for source in sources if source.casefold().startswith("data:image/svg+xml")]
    fragments = inline_svg_fragments(content)
    return {
        "html_image_source_count": len(sources),
        "svg_data_image_count": len(data_svg),
        "valid_svg_data_image_count": sum(svg_data_image_document(source) is not None for source in data_svg),
        "inline_svg_drawing_count": len(fragments),
        "valid_inline_svg_drawing_count": sum(self_contained_svg_fragment(inline_svg_xml_document(fragment))
            for _start, _end, fragment in fragments),
        "linked_image_source_count": sum(bool(source) and not source.casefold().startswith("data:")
            for source in sources),
    }


def self_contained_html_svg_count(content: str) -> int:
    summary = html_svg_media_summary(content)
    return summary["valid_svg_data_image_count"] + summary["valid_inline_svg_drawing_count"]


def _workbook_structure_evidence(package) -> dict[str, object]:
    """Inspect bounded OOXML and follow actual worksheet drawing relations."""
    import posixpath
    from xml.etree import ElementTree as ET
    main = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
    relation = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id'
    chart_tag = '{http://schemas.openxmlformats.org/drawingml/2006/chart}chart'
    remaining = 4 * 1024 * 1024
    complete = True
    cache = {}
    def document(name):
        nonlocal remaining, complete
        if name in cache:
            return cache[name]
        try:
            item = package.getinfo(name)
            if item.file_size > min(1024 * 1024, remaining):
                complete = False
                return None
            remaining -= item.file_size
            root = ET.fromstring(package.read(item))
            cache[name] = root
            return root
        except (KeyError, OSError, ValueError, ET.ParseError):
            complete = False
            return None
    def relationships(source):
        name = posixpath.join(posixpath.dirname(source), '_rels', posixpath.basename(source) + '.rels')
        if name not in package.namelist():
            return {}
        root = document(name)
        if root is None:
            return {}
        return {row.get('Id'): (posixpath.normpath(posixpath.join(posixpath.dirname(source), row.get('Target', ''))).lstrip('/')
                if not row.get('Target', '').startswith('/') else row.get('Target', '').lstrip('/'))
                for row in root if row.get('TargetMode') != 'External' and row.get('Id') and row.get('Target')}
    workbook = document('xl/workbook.xml')
    if workbook is None:
        return {'workbook_inspection_complete': False}
    if workbook.tag != main + 'workbook':
        return {'workbook_inspection_complete': False, 'workbook_inspection_reason': 'unsupported_namespace'}
    sheets = workbook.findall(main + 'sheets/' + main + 'sheet')
    if not sheets:
        return {'workbook_inspection_complete': False, 'workbook_inspection_reason': 'missing_worksheets'}
    targets = relationships('xl/workbook.xml')
    summaries = []
    for sheet in sheets[:12]:
        name = targets.get(sheet.get(relation))
        root = document(name) if name else None
        if root is None:
            complete = False
            continue
        formulas = root.findall('.//' + main + 'c/' + main + 'f')
        formats = root.findall(main + 'conditionalFormatting')
        chart_parts = set()
        drawing_targets = relationships(name)
        for drawing in root.findall(main + 'drawing'):
            drawing_name = drawing_targets.get(drawing.get(relation))
            drawing_root = document(drawing_name) if drawing_name else None
            if drawing_root is None:
                complete = False
                continue
            chart_targets = relationships(drawing_name)
            for chart in drawing_root.iter(chart_tag):
                chart_name = chart_targets.get(chart.get(relation))
                chart_root = document(chart_name) if chart_name else None
                if (chart_root is not None
                        and chart_root.tag == '{http://schemas.openxmlformats.org/drawingml/2006/chart}chartSpace'):
                    chart_parts.add(chart_name)
        dimension = root.find(main + 'dimension')
        summaries.append({'name':str(sheet.get('name') or '')[:120],
            'dimension':str(dimension.get('ref') or '')[:120] if dimension is not None else None,
            'formula_cell_count':len(formulas), 'chart_count':len(chart_parts),
            'conditional_formatting_range_count':len(formats),
            'conditional_formatting_rule_count':sum(len(row.findall(main + 'cfRule')) for row in formats)})
    return {'workbook_inspection_complete':complete and len(sheets) <= 12,
        'worksheet_count':len(sheets), 'worksheets':summaries,
        'formula_cell_count':sum(row['formula_cell_count'] for row in summaries),
        'chart_count':sum(row['chart_count'] for row in summaries),
        'conditional_formatting_rule_count':sum(row['conditional_formatting_rule_count'] for row in summaries)}


def _native_layout_evidence(path: Path) -> dict[str, object]:
    """Read native layout facts; these are not a rendered visual inspection."""
    evidence = {"layout_evidence_kind": "native_format_structure", "rendered_visual_inspection": False}
    try:
        from itertools import islice
        if path.suffix.lower() in {".docx", ".pptx"}:
            from zipfile import ZipFile
            with ZipFile(path) as package:
                xml_members = [row for row in package.infolist() if row.filename.endswith(".xml")]
                if len(xml_members) > 1000 or sum(row.file_size for row in xml_members) > 4 * 1024 * 1024:
                    return {**evidence, "layout_inspection_complete": False, "layout_inspection_reason": "xml_limit"}
        if path.suffix.lower() == ".pptx":
            from pptx import Presentation
            deck = Presentation(path)
            width, height = deck.slide_width, deck.slide_height
            slides = []
            for slide in islice(deck.slides, 30):
                sizes, colors, overflow = set(), set(), 0
                for shape in islice(slide.shapes, 150):
                    if all(value is not None for value in (shape.left, shape.top, shape.width, shape.height)):
                        if shape.left < 0 or shape.top < 0 or shape.left + shape.width > width or shape.top + shape.height > height:
                            overflow += 1
                    if not shape.has_text_frame:
                        continue
                    for paragraph in shape.text_frame.paragraphs:
                        for font in [paragraph.font, *(run.font for run in paragraph.runs)]:
                            if font.size is not None:
                                sizes.add(round(font.size.pt, 2))
                            try:
                                if font.color.rgb is not None:
                                    colors.add(str(font.color.rgb))
                            except (AttributeError, TypeError):
                                pass
                slides.append({"shape_count": len(slide.shapes), "shapes_outside_slide": overflow,
                    "explicit_font_sizes_pt": sorted(sizes), "explicit_font_colors_rgb": sorted(colors)[:20]})
            return {**evidence, "slide_count": len(deck.slides), "slide_dimensions_emu": [width, height],
                    "slide_layouts": slides, "layout_inspection_complete": len(deck.slides) <= 30 and all(len(slide.shapes) <= 150 for slide in islice(deck.slides, 30))}
        if path.suffix.lower() == ".docx":
            from docx import Document
            from docx.opc.constants import RELATIONSHIP_TYPE
            from docx.oxml.ns import qn
            document = Document(path)
            footer_fields = 0
            for relation in document.part.rels.values():
                if relation.reltype != RELATIONSHIP_TYPE.FOOTER:
                    continue
                for node in relation.target_part.element.iter():
                    instruction = (node.text or '') if node.tag == qn('w:instrText') else (
                        node.get(qn('w:instr'), '') if node.tag == qn('w:fldSimple') else '')
                    footer_fields += int('PAGE' in instruction.split())
            sizes, colors, paragraph_styles = set(), set(), set()
            for paragraph in document.paragraphs[:150]:
                style = paragraph.style
                if style is not None:
                    paragraph_styles.add(style.style_id)
                for font in [*(run.font for run in paragraph.runs), *([style.font] if style is not None else [])]:
                    if font.size is not None:
                        sizes.add(round(font.size.pt, 2))
                    try:
                        if font.color.rgb is not None:
                            colors.add(str(font.color.rgb))
                    except (AttributeError, TypeError):
                        pass
            sections = [{"page_width_twips": section.page_width.twips if section.page_width is not None else None,
                         "page_height_twips": section.page_height.twips if section.page_height is not None else None,
                         "margins_twips": [value.twips if value is not None else None for value in
                             [section.top_margin, section.right_margin, section.bottom_margin, section.left_margin]]}
                        for section in islice(document.sections, 12)]
            return {**evidence, "paragraph_count": len(document.paragraphs), "table_count": len(document.tables),
                    "footer_page_number_field_count": footer_fields,
                    "paragraph_style_ids": sorted(paragraph_styles)[:40], "explicit_font_sizes_pt": sorted(sizes),
                    "explicit_font_colors_rgb": sorted(colors)[:20], "sections": sections,
                    "layout_inspection_complete": len(document.paragraphs) <= 150 and len(document.sections) <= 12}
        if path.suffix.lower() == ".pdf":
            from pypdf import PdfReader
            document = PdfReader(path)
            pages = []
            for page in (document.pages[index] for index in range(min(len(document.pages), 20))):
                sizes, colors, names, outside, count = set(), set(), set(), 0, 0
                left, bottom, right, top = map(float, page.mediabox)
                def text_visitor(text, cm, tm, font, size):
                    if not text.strip():
                        return
                    sizes.add(round(float(size), 2))
                    if font and font.get("/BaseFont"):
                        names.add(str(font["/BaseFont"]))
                def operand_visitor(operator, operands, cm, tm):
                    nonlocal outside, count
                    # Text visitors may flush buffered words after the graphics
                    # transform changes (notably on later Chromium PDF pages).
                    # Measure at the actual text-drawing operator instead.
                    if operator in {b"Tj", b"TJ", b"'", b'"'}:
                        def has_text(value):
                            return bool(value.strip()) if isinstance(value, (str, bytes)) else (
                                any(has_text(item) for item in value) if isinstance(value, (list, tuple)) else False)
                        if has_text(operands):
                            count += 1
                            x = float(tm[4]) * float(cm[0]) + float(tm[5]) * float(cm[2]) + float(cm[4])
                            y = float(tm[4]) * float(cm[1]) + float(tm[5]) * float(cm[3]) + float(cm[5])
                            if x < left - 2 or x > right + 2 or y < bottom - 2 or y > top + 2:
                                outside += 1
                    if operator in {b"rg", b"g", b"k"} and len(colors) < 20:
                        colors.add(operator.decode() + ":" + ",".join(str(round(float(value), 3)) for value in operands))
                page.extract_text(visitor_text=text_visitor, visitor_operand_before=operand_visitor)
                pages.append({"dimensions_pt": [right-left, top-bottom], "rotation": page.rotation,
                    "text_run_count": count, "text_origins_outside_page": outside,
                    "font_sizes_pt": sorted(sizes)[:40], "font_names": sorted(names)[:40],
                    "explicit_fill_colors": sorted(colors)})
            return {**evidence, "page_count": len(document.pages), "page_layouts": pages,
                    "layout_inspection_complete": len(document.pages) <= 20}
    except Exception:
        # Native format parsers use library-specific exceptions for corrupt or
        # encrypted input. An unreadable artifact is incomplete evidence, not
        # a reason to crash the whole request.
        return {**evidence, "layout_inspection_complete": False}
    return {}


def artifact_structure_evidence(path: Path) -> dict[str, object]:
    """Bounded file facts for content review, never claims about source accuracy."""
    if path.suffix.lower() in {'.csv', '.tsv'}:
        import csv
        import io
        from itertools import islice
        try:
            if path.stat().st_size > 4 * 1024 * 1024:
                return {'inspection_complete': False, 'reason': 'delimited_size_limit'}
            text = path.read_text(encoding='utf-8-sig')
            scanned = list(islice(csv.reader(io.StringIO(text),
                delimiter='\t' if path.suffix.lower() == '.tsv' else ',', strict=True), 10001))
            rows = [row for row in scanned if any(cell.strip() for cell in row)]
            width = len(rows[0]) if rows else 0
            return {'inspection_complete': len(scanned) <= 10000, 'format': 'delimited_text',
                'row_count': len(rows), 'data_row_count': max(0, len(rows)-1), 'column_count': width,
                'consistent_column_count': all(len(row) == width for row in rows),
                'literal_line_break_escape_count': text.count('\\n') + text.count('\\r')}
        except (OSError, UnicodeError, csv.Error):
            return {'inspection_complete': False, 'reason': 'invalid_delimited_text'}
    if path.suffix.lower() in {".docx", ".pptx", ".xlsx", ".xlsm"}:
        from zipfile import BadZipFile, ZipFile
        try:
            with ZipFile(path) as package:
                members = package.infolist()
                if len(members) > 10000:
                    return {"inspection_complete": False, "reason": "member_limit"}
                media = [member for member in members if not member.is_dir() and member.filename.startswith(
                    ("word/media/", "ppt/media/", "xl/media/"))]
                decoded = invalid = skipped = inspected_bytes = 0
                for member in media:
                    if (Path(member.filename).suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp", ".gif"}
                            or decoded + invalid >= 20 or inspected_bytes + member.file_size > 5 * 1024 * 1024):
                        skipped += 1
                        continue
                    inspected_bytes += member.file_size
                    try:
                        import io
                        from PIL import Image
                        with package.open(member) as handle:
                            data = handle.read(5 * 1024 * 1024 + 1)
                        with Image.open(io.BytesIO(data)) as image:
                            image.verify()
                        decoded += 1
                    except Exception:
                        invalid += 1
                return {"inspection_complete": skipped == 0, "embedded_media_count": len(media),
                        "decoded_embedded_raster_image_count": decoded, "invalid_embedded_image_count": invalid,
                        "uninspected_media_count": skipped,
                        **(_workbook_structure_evidence(package) if path.suffix.lower() in {'.xlsx', '.xlsm'} else _native_layout_evidence(path))}
        except (OSError, BadZipFile, ValueError):
            return {"inspection_complete": False, "reason": "invalid_package"}
    if path.suffix.lower() == ".pdf":
        return _native_layout_evidence(path)
    if path.suffix.lower() == ".zip":
        # Inspect portable HTML in place; never extract archive paths.
        import posixpath
        from html.parser import HTMLParser
        from urllib.parse import urlparse, unquote
        from zipfile import ZipFile, BadZipFile
        class References(HTMLParser):
            def __init__(self):
                super().__init__()
                self.images = []
            def handle_starttag(self, tag, attrs):
                if tag == "img":
                    self.images.append(dict(attrs).get("src") or "")
        try:
            with ZipFile(path) as archive:
                members = [row for row in archive.infolist() if not row.is_dir()]
                if len(members) > 1000:
                    return {"inspection_complete": False, "reason": "archive_member_limit"}
                names = {row.filename for row in members}
                documents, remaining = [], 4 * 1024 * 1024
                for member in members:
                    if Path(member.filename).suffix.lower() not in {".html", ".htm"}:
                        continue
                    if (member.flag_bits & 1 or member.file_size > min(1024 * 1024, remaining)
                            or len(documents) >= 12):
                        return {"inspection_complete": False, "reason": "archive_html_limit"}
                    remaining -= member.file_size
                    parser = References()
                    parser.feed(archive.read(member).decode("utf-8", errors="replace"))
                    assets, unsafe = [], []
                    for reference in parser.images:
                        parsed = urlparse(reference)
                        if parsed.scheme in {"data", "http", "https"}:
                            continue
                        name = posixpath.normpath(posixpath.join(posixpath.dirname(member.filename), unquote(parsed.path)))
                        if parsed.scheme or parsed.netloc or unquote(parsed.path).startswith("/") or name.startswith("../"):
                            unsafe.append(reference)
                        else:
                            assets.append(name)
                    missing = sorted(set(assets) - names)
                    documents.append({"entry_member": member.filename, "linked_asset_members": sorted(set(assets)),
                        "missing_asset_members": missing, "unsafe_asset_references": unsafe,
                        "portable": not missing and not unsafe and len(names) == len(members)})
                return {"inspection_complete": True, "archive_member_count": len(members),
                    "archive_member_names": sorted(names)[:100], "portable_html_documents": documents}
        except (OSError, ValueError, BadZipFile, RuntimeError):
            return {"inspection_complete": False, "reason": "unreadable_archive"}
    if path.suffix.lower() not in {".html", ".htm"}:
        return {}
    from html.parser import HTMLParser
    import base64
    import io
    from urllib.parse import unquote_to_bytes
    from PIL import Image
    from nullion.artifact_validation import MAX_TEXT_ARTIFACT_BYTES

    class Assets(HTMLParser):
        def __init__(self):
            super().__init__()
            self.sources = []
            self.svg_count = 0

        def handle_starttag(self, tag, attrs):
            if tag == "svg":
                self.svg_count += 1
            if tag == "img":
                self.sources.append(dict(attrs).get("src") or "")

    try:
        if path.stat().st_size > MAX_TEXT_ARTIFACT_BYTES:
            return {"inspection_complete": False, "reason": "size_limit"}
        parser = Assets()
        parser.feed(path.read_text(encoding="utf-8", errors="replace"))
        decoded = invalid = linked = svg_images = 0
        for source in parser.sources:
            if not source.lower().startswith("data:image/"):
                linked += 1
                continue
            if source.lower().startswith("data:image/svg+xml"):
                svg_images += 1
                continue
            try:
                header, payload = source.split(",", 1)
                data = (base64.b64decode("".join(payload.split()), validate=True)
                        if ";base64" in header.lower() else unquote_to_bytes(payload))
                with Image.open(io.BytesIO(data)) as image:
                    image.verify()
                decoded += 1
            except Exception:
                invalid += 1
        local_assets = []
        from urllib.parse import urlparse, unquote
        for source in parser.sources:
            parsed = urlparse(source)
            if parsed.scheme not in {"", "file"} or parsed.netloc not in {"", "localhost"} or not parsed.path:
                continue
            relative = Path(unquote(parsed.path))
            candidate = (path.parent / relative).resolve()
            contained = not parsed.scheme and not relative.is_absolute() and candidate.is_relative_to(path.parent.resolve())
            local_assets.append({"reference": source, "available_in_artifact_directory":
                contained and candidate.is_file(), "portable_reference": contained})
        return {"inspection_complete": True, "image_count": len(parser.sources),
                "linked_local_image_assets": local_assets[:20],
                "linked_local_image_assets_complete": len(local_assets) <= 20,
                "decoded_embedded_raster_image_count": decoded, "invalid_embedded_image_count": invalid,
                "linked_image_count": linked, "inline_svg_count": parser.svg_count,
                "svg_image_count": svg_images}
    except (OSError, ValueError):
        return {"inspection_complete": False, "reason": "unreadable_html"}


def artifact_review_records(paths, results, *, registry, allowed_roots, include_unselected=False):
    """Bind each real output file to its producer; never promote read/search data."""
    roots = tuple(Path(root).expanduser().resolve() for root in allowed_roots)
    results = tuple(results)
    records, fingerprints = [], {}
    seen = set()
    for value in paths:
        path = Path(value).expanduser().resolve()
        if path in seen or not any(path == root or path.is_relative_to(root) for root in roots):
            continue
        seen.add(path)
        parent = None
        for result in reversed(results):
            if result.status != 'completed':
                continue
            metadata_registry = registry
            effect = None
            for _ in range(8):
                try:
                    effect = metadata_registry.get_spec(result.tool_name).side_effect_class
                    break
                except (AttributeError, KeyError):
                    delegate = (getattr(metadata_registry, "_delegate", None)
                                or getattr(metadata_registry, "_registry", None))
                    if delegate is None or delegate is metadata_registry:
                        break
                    metadata_registry = delegate
            if effect not in {ToolSideEffectClass.WRITE, ToolSideEffectClass.DANGEROUS_EXEC}:
                continue
            if include_unselected:
                from nullion.artifact_lifecycle import artifact_candidate_versions
                produced = artifact_candidate_versions([result])
            else:
                produced = artifact_paths_from_tool_results([result])
            if str(path) in {str(Path(p).expanduser().resolve()) for p in produced}:
                parent = result
                break
        if parent is None:
            continue
        try:
            before = path.stat()
            if not path.is_file() or before.st_size <= 0:
                continue
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            after = path.stat()
            if (before.st_size,before.st_mtime_ns) != (after.st_size,after.st_mtime_ns):
                continue
        except OSError:
            continue
        identity = hashlib.sha256(str(path).encode()).hexdigest()[:16]
        record_id = f'{parent.invocation_id}:artifact:{identity}'
        binding = {'record_kind':'artifact', 'parent_invocation_id':parent.invocation_id,
            'filename':path.name,'extension':path.suffix.lower(), 'size_bytes':after.st_size,
            'sha256':digest.hexdigest()}
        records.append(ToolResult(record_id,parent.tool_name,'completed',{
            'result_kind':'artifact_receipt','artifact_path':str(path), 'artifact_record_binding':binding}))
        fingerprints[record_id] = json.dumps({'path':str(path),'sha256':digest.hexdigest()},sort_keys=True)
    return records,fingerprints
