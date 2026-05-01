"""LangGraph planning for requested attachment formats."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import re
from typing import TypedDict

from langgraph.graph import END, START, StateGraph


ATTACHMENT_TOKEN_EXTENSIONS: dict[str, str] = {
    "csv": ".csv",
    "doc": ".docx",
    "docs": ".docx",
    "docx": ".docx",
    "document": ".docx",
    "gif": ".gif",
    "htm": ".html",
    "html": ".html",
    "jpeg": ".jpg",
    "jpg": ".jpg",
    "json": ".json",
    "markdown": ".md",
    "md": ".md",
    "pdf": ".pdf",
    "png": ".png",
    "powerpoint": ".pptx",
    "ppt": ".pptx",
    "pptx": ".pptx",
    "presentation": ".pptx",
    "spreadsheet": ".xlsx",
    "svg": ".svg",
    "txt": ".txt",
    "webp": ".webp",
    "xls": ".xlsx",
    "xlsx": ".xlsx",
    "yaml": ".yaml",
    "yml": ".yaml",
}


@dataclass(frozen=True, slots=True)
class AttachmentFormatPlan:
    extension: str | None = None
    evidence: str = "none"


class AttachmentFormatState(TypedDict, total=False):
    text: str
    tokens: list[str]
    plan: AttachmentFormatPlan


def _word_tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(text or "").lower())


def _normalize_node(state: AttachmentFormatState) -> dict[str, object]:
    return {"tokens": _word_tokens(state.get("text") or "")}


def _extension_token_node(state: AttachmentFormatState) -> dict[str, object]:
    for token in state.get("tokens") or []:
        extension = ATTACHMENT_TOKEN_EXTENSIONS.get(token)
        if extension is not None:
            return {"plan": AttachmentFormatPlan(extension=extension, evidence="extension_token")}
    return {}


def _format_phrase_node(state: AttachmentFormatState) -> dict[str, object]:
    if state.get("plan") is not None:
        return {}
    tokens = state.get("tokens") or []
    for index in range(len(tokens) - 1):
        if tokens[index] == "text" and tokens[index + 1] == "file":
            return {"plan": AttachmentFormatPlan(extension=".txt", evidence="text_file_phrase")}
        if tokens[index] == "word" and tokens[index + 1] in {"doc", "document", "file"}:
            return {"plan": AttachmentFormatPlan(extension=".docx", evidence="word_file_phrase")}
        if tokens[index] in {"excel", "spreadsheet"} and tokens[index + 1] == "file":
            return {"plan": AttachmentFormatPlan(extension=".xlsx", evidence="spreadsheet_file_phrase")}
    return {}


def _default_node(state: AttachmentFormatState) -> dict[str, object]:
    if state.get("plan") is not None:
        return {}
    return {"plan": AttachmentFormatPlan()}


@lru_cache(maxsize=1)
def _compiled_attachment_format_graph():
    graph = StateGraph(AttachmentFormatState)
    graph.add_node("normalize", _normalize_node)
    graph.add_node("extension_token", _extension_token_node)
    graph.add_node("format_phrase", _format_phrase_node)
    graph.add_node("default", _default_node)
    graph.add_edge(START, "normalize")
    graph.add_edge("normalize", "extension_token")
    graph.add_edge("extension_token", "format_phrase")
    graph.add_edge("format_phrase", "default")
    graph.add_edge("default", END)
    return graph.compile()


def plan_attachment_format(text: str) -> AttachmentFormatPlan:
    final_state = _compiled_attachment_format_graph().invoke(
        {"text": text},
        config={"configurable": {"thread_id": "attachment-format-plan"}},
    )
    plan = final_state.get("plan")
    return plan if isinstance(plan, AttachmentFormatPlan) else AttachmentFormatPlan()


__all__ = [
    "ATTACHMENT_TOKEN_EXTENSIONS",
    "AttachmentFormatPlan",
    "AttachmentFormatState",
    "plan_attachment_format",
]
