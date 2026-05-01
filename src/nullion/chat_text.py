"""Shared chat text rendering helpers.

These helpers operate on assistant output after the model has produced a reply.
They avoid making routing decisions from message text; the goal is only to keep
known Markdown constructs readable across narrow chat surfaces.
"""

from __future__ import annotations


def _split_table_row(line: str) -> list[str] | None:
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
        return None
    cells = [cell.strip() for cell in stripped.strip("|").split("|")]
    if len(cells) < 2:
        return None
    return cells


def _is_separator_row(cells: list[str]) -> bool:
    for cell in cells:
        normalized = cell.replace(":", "").replace("-", "").strip()
        if normalized:
            return False
    return any("-" in cell for cell in cells)


def _render_table_as_list(header: list[str], rows: list[list[str]]) -> list[str]:
    rendered: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        if rendered:
            rendered.append("")
        rendered.append(f"Row {row_index}:")
        for cell_index, heading in enumerate(header):
            label = heading or f"Column {cell_index + 1}"
            value = row[cell_index] if cell_index < len(row) else ""
            rendered.append(f"- {label}: {value}")
    return rendered


def make_markdown_tables_chat_readable(text: str) -> str:
    """Convert Markdown table blocks into wrapped list text for chat delivery."""
    lines = str(text).splitlines()
    output: list[str] = []
    index = 0
    in_fence = False
    while index < len(lines):
        line = lines[index]
        if line.strip().startswith("```"):
            in_fence = not in_fence
            output.append(line)
            index += 1
            continue
        if in_fence:
            output.append(line)
            index += 1
            continue

        header = _split_table_row(line)
        separator = _split_table_row(lines[index + 1]) if header is not None and index + 1 < len(lines) else None
        if header is None or separator is None or len(header) != len(separator) or not _is_separator_row(separator):
            output.append(line)
            index += 1
            continue

        rows: list[list[str]] = []
        index += 2
        while index < len(lines):
            row = _split_table_row(lines[index])
            if row is None:
                break
            rows.append(row)
            index += 1
        if rows:
            output.extend(_render_table_as_list(header, rows))
        else:
            output.append(line)
            output.append(lines[index - 1])
    return "\n".join(output)


__all__ = ["make_markdown_tables_chat_readable"]
