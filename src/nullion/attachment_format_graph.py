"""LangGraph planning for requested attachment formats."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import re
from typing import TypedDict

from langgraph.graph import END, START, StateGraph


ATTACHMENT_TOKEN_EXTENSIONS: dict[str, str] = {
    "csv": ".csv",
    "doc": ".docx",
    "docx": ".docx",
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
    "ppt": ".pptx",
    "pptx": ".pptx",
    "svg": ".svg",
    "txt": ".txt",
    "webp": ".webp",
    "xls": ".xlsx",
    "xlsx": ".xlsx",
    "yaml": ".yaml",
    "yml": ".yaml",
}
VALID_ATTACHMENT_EXTENSIONS: tuple[str, ...] = tuple(sorted(set(ATTACHMENT_TOKEN_EXTENSIONS.values())))


@dataclass(frozen=True, slots=True)
class AttachmentFormatPlan:
    extension: str | None = None
    evidence: str = "none"


class AttachmentFormatState(TypedDict, total=False):
    text: str
    extensions: list[str]
    plan: AttachmentFormatPlan


def _text_from_model_response(response: object) -> str:
    if not isinstance(response, dict):
        return ""
    parts: list[str] = []
    for block in response.get("content") or ():
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "\n".join(parts).strip()


def _structured_payload_from_model_response(response: object) -> dict[str, object] | None:
    if not isinstance(response, dict):
        return None
    for block in response.get("content") or ():
        if not isinstance(block, dict):
            continue
        if block.get("type") != "tool_use" or block.get("name") != "select_attachment_format":
            continue
        payload = block.get("input")
        if isinstance(payload, dict):
            return payload
    return None


def _parse_json_object(text: str) -> dict[str, object] | None:
    stripped = str(text or "").strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            parsed = json.loads(stripped[start : end + 1])
        except json.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, dict) else None


def _model_attachment_format_plan(text: str, model_client: object | None) -> AttachmentFormatPlan | None:
    create = getattr(model_client, "create", None)
    if create is None:
        return None
    try:
        response = create(
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": str(text or ""),
                        }
                    ],
                }
            ],
            tools=[
                {
                    "name": "select_attachment_format",
                    "description": "Return the required attachment file format for this user request.",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "extension": {
                                "anyOf": [
                                    {"type": "string", "enum": list(VALID_ATTACHMENT_EXTENSIONS)},
                                    {"type": "null"},
                                ]
                            },
                            "confidence": {"type": "number"},
                        },
                        "required": ["extension"],
                        "additionalProperties": False,
                    },
                }
            ],
            max_tokens=120,
            system=(
                "Identify whether this user request specifies a required attachment file format. "
                "Call select_attachment_format with the result. "
                f"extension must be one of {list(VALID_ATTACHMENT_EXTENSIONS)} or null. "
                "Use null when no attachment file format is specified."
            ),
        )
    except Exception:
        return None
    payload = _structured_payload_from_model_response(response) or _parse_json_object(_text_from_model_response(response))
    if payload is None:
        return None
    extension = payload.get("extension")
    if extension is None:
        return AttachmentFormatPlan(evidence="model_structured_output")
    normalized = str(extension or "").strip().lower()
    if normalized and not normalized.startswith("."):
        normalized = f".{normalized}"
    if normalized not in VALID_ATTACHMENT_EXTENSIONS:
        return None
    return AttachmentFormatPlan(extension=normalized, evidence="model_structured_output")


def _explicit_extensions(text: str) -> list[str]:
    seen: list[str] = []
    for match in re.finditer(r"\.([A-Za-z0-9]{1,12})(?![\w/-])", str(text or "")):
        extension = ATTACHMENT_TOKEN_EXTENSIONS.get(match.group(1).lower())
        if extension is not None and extension not in seen:
            seen.append(extension)
    return seen


def _normalize_node(state: AttachmentFormatState) -> dict[str, object]:
    return {"extensions": _explicit_extensions(state.get("text") or "")}


def _extension_token_node(state: AttachmentFormatState) -> dict[str, object]:
    extensions = state.get("extensions") or []
    if extensions:
        return {"plan": AttachmentFormatPlan(extension=extensions[0], evidence="literal_extension")}
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
    graph.add_node("default", _default_node)
    graph.add_edge(START, "normalize")
    graph.add_edge("normalize", "extension_token")
    graph.add_edge("extension_token", "default")
    graph.add_edge("default", END)
    return graph.compile()


def plan_attachment_format(text: str, *, model_client: object | None = None) -> AttachmentFormatPlan:
    final_state = _compiled_attachment_format_graph().invoke(
        {"text": text},
        config={"configurable": {"thread_id": "attachment-format-plan"}},
    )
    plan = final_state.get("plan")
    if isinstance(plan, AttachmentFormatPlan) and plan.extension is not None:
        return plan
    model_plan = _model_attachment_format_plan(text, model_client)
    if model_plan is not None:
        return model_plan
    return plan if isinstance(plan, AttachmentFormatPlan) else AttachmentFormatPlan()


__all__ = [
    "ATTACHMENT_TOKEN_EXTENSIONS",
    "AttachmentFormatPlan",
    "AttachmentFormatState",
    "VALID_ATTACHMENT_EXTENSIONS",
    "plan_attachment_format",
]
