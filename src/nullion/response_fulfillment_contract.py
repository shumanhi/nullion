"""Platform-neutral final response fulfillment checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable

from nullion.artifacts import artifact_descriptors_for_paths, media_candidate_paths_from_text
from nullion.attachment_format_graph import ATTACHMENT_TOKEN_EXTENSIONS
from nullion.messaging_adapters import delivery_contract_for_turn
from nullion.task_frames import TaskFrameStatus
from nullion.tools import ToolResult, normalize_tool_status


USER_VISIBLE_TEXT_KEYS = ("result_text", "delivery_text", "final_text", "text", "message", "summary", "content", "stdout")
EMPTY_USER_VISIBLE_TEXTS = frozenset({"", "(no reply)", "no reply"})


@dataclass(frozen=True, slots=True)
class ResponseFulfillmentDecision:
    satisfied: bool
    reply: str
    missing_requirements: tuple[str, ...] = ()


def _text_from_value(value: object) -> str:
    if isinstance(value, str):
        text = value.strip()
        return "" if text.lower() in EMPTY_USER_VISIBLE_TEXTS else text
    if isinstance(value, dict):
        for key in USER_VISIBLE_TEXT_KEYS:
            text = _text_from_value(value.get(key))
            if text:
                return text
        nested = value.get("result")
        if isinstance(nested, dict):
            return _text_from_value(nested)
        return ""
    for key in USER_VISIBLE_TEXT_KEYS:
        if hasattr(value, key):
            text = _text_from_value(getattr(value, key))
            if text:
                return text
    return ""


def user_visible_text_from_output(output: object) -> str:
    """Extract deliverable text from provider/tool output without assuming a concrete result type."""
    return _text_from_value(output)


def guaranteed_user_visible_text(*, subject: object, output: object, kind: str = "task") -> str:
    """Return user-visible text or a deterministic diagnostic fallback.

    The delivery boundary should not trust a provider's final prose alone. This
    helper makes any runner/tool/platform result explicit before downstream
    channel formatting decides whether it is web text, chat text, media, or an
    attachment.
    """
    text = user_visible_text_from_output(output)
    if text:
        return text
    name = str(getattr(subject, "name", "") or subject or kind).strip() or kind
    return (
        f"{kind.capitalize()} '{name}' completed, but the runner returned no user-visible text. "
        "Check the task prompt, tool results, or generated artifacts for why no report was produced."
    )


def artifact_paths_from_tool_results(tool_results: Iterable[ToolResult] | None) -> list[str]:
    paths: list[str] = []
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if result.tool_name == "file_write":
            path = output.get("path")
            if isinstance(path, str) and path.strip():
                paths.append(path)
        for key in ("artifact_path", "path"):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                paths.append(value)
        for key in ("artifact_paths", "artifacts"):
            values = output.get(key)
            if isinstance(values, (list, tuple)):
                paths.extend(value for value in values if isinstance(value, str) and value.strip())
    return list(dict.fromkeys(paths))


def _valid_artifact_paths(paths: Iterable[str], *, artifact_roots: Iterable[Path]) -> list[str]:
    valid: list[str] = []
    seen: set[str] = set()
    for root in artifact_roots:
        for descriptor in artifact_descriptors_for_paths(list(paths), artifact_root=root):
            if descriptor.path in seen:
                continue
            seen.add(descriptor.path)
            valid.append(descriptor.path)
    return valid


def _existing_deliverable_paths(paths: Iterable[str], *, artifact_roots: Iterable[Path]) -> list[str]:
    valid = _valid_artifact_paths(paths, artifact_roots=artifact_roots)
    seen = set(valid)
    roots = [root.expanduser().resolve() for root in artifact_roots]
    for raw_path in paths:
        try:
            path = Path(raw_path).expanduser().resolve()
        except (OSError, RuntimeError, ValueError):
            continue
        if str(path) in seen:
            continue
        if not path.is_file() or path.stat().st_size <= 0:
            continue
        if not any(_is_relative_to(path, root) for root in roots):
            continue
        seen.add(str(path))
        valid.append(str(path))
    return valid


def _required_extension_for_artifact_kind(artifact_kind: str | None) -> str | None:
    normalized = str(artifact_kind or "").strip().lower().removeprefix(".")
    if not normalized or normalized == "file":
        return None
    extension = ATTACHMENT_TOKEN_EXTENSIONS.get(normalized)
    if extension is not None:
        return extension
    direct_extension = f".{normalized}"
    if direct_extension in set(ATTACHMENT_TOKEN_EXTENSIONS.values()):
        return direct_extension
    return None


def _paths_matching_required_extension(paths: Iterable[str], required_extension: str | None) -> list[str]:
    if required_extension is None:
        return list(paths)
    return [path for path in paths if Path(path).suffix.lower() == required_extension]


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _has_completion_claim(text: str) -> bool:
    tokens = re.findall(r"[a-z']+", text.casefold().replace("’", "'"))
    claim_words = {"done", "completed", "finished", "attached", "saved", "created", "generated", "added", "updated", "uploaded", "delivered"}
    negators = {"not", "never", "haven't", "hasn't", "hadn't", "didn't", "doesn't", "can't", "cannot", "couldn't", "won't"}
    for index, token in enumerate(tokens):
        if token not in claim_words:
            continue
        if any(previous in negators for previous in tokens[max(0, index - 4) : index]):
            continue
        return True
    return False


def _reply_valid_media_paths(reply: str, *, artifact_roots: Iterable[Path]) -> list[str]:
    candidates = [str(path) for path in media_candidate_paths_from_text(reply)]
    return _existing_deliverable_paths(candidates, artifact_roots=artifact_roots)


def _active_frame_for_contract(store, conversation_id: str):
    active_frame_id = store.get_active_task_frame_id(conversation_id)
    if not isinstance(active_frame_id, str) or not active_frame_id:
        return None
    frame = store.get_task_frame(active_frame_id)
    if frame is None or frame.status not in {
        TaskFrameStatus.ACTIVE,
        TaskFrameStatus.RUNNING,
        TaskFrameStatus.WAITING_APPROVAL,
        TaskFrameStatus.WAITING_INPUT,
        TaskFrameStatus.VERIFYING,
    }:
        return None
    return frame


def evaluate_response_fulfillment(
    *,
    store,
    conversation_id: str,
    user_message: str,
    reply: str,
    tool_results: Iterable[ToolResult] | None = None,
    artifact_paths: Iterable[str] | None = None,
    artifact_roots: Iterable[Path] = (),
    platform_artifact_count: int = 0,
) -> ResponseFulfillmentDecision:
    """Validate that a final response is backed by required runtime evidence."""
    frame = _active_frame_for_contract(store, conversation_id)
    normalized_results = [
        (result, normalize_tool_status(getattr(result, "status", None)))
        for result in (tool_results or ())
    ]
    completed_tool_names = {result.tool_name for result, status in normalized_results if status == "completed"}
    attempted_tool_names = {result.tool_name for result, _status in normalized_results}

    required_tools: set[str] = set()
    requires_artifact = False
    artifact_kind = "file"
    if frame is not None:
        required_tools.update(frame.finish.required_tool_completion)
        requires_artifact = bool(frame.finish.requires_artifact_delivery)
        artifact_kind = frame.finish.required_artifact_kind or frame.output.artifact_kind or artifact_kind
    required_extension = _required_extension_for_artifact_kind(artifact_kind)

    combined_artifact_paths = [
        *(artifact_paths or ()),
        *artifact_paths_from_tool_results(result for result, _status in normalized_results),
    ]
    delivery_contract = delivery_contract_for_turn(
        None,
        reply=reply,
        artifact_paths=combined_artifact_paths,
        requires_attachment_delivery=requires_artifact,
        required_attachment_extensions=(required_extension,) if required_extension else (),
    )
    if not requires_artifact and delivery_contract.requires_attachment_delivery:
        requires_artifact = True

    missing: list[str] = []
    missing_tools = sorted(tool for tool in required_tools if tool not in completed_tool_names)
    if missing_tools:
        missing.append(f"required tool completion: {', '.join(missing_tools)}")

    valid_artifacts = _paths_matching_required_extension(
        _existing_deliverable_paths(combined_artifact_paths, artifact_roots=artifact_roots),
        required_extension,
    )
    valid_reply_media = _paths_matching_required_extension(
        _reply_valid_media_paths(reply, artifact_roots=artifact_roots),
        required_extension,
    )
    has_deliverable = platform_artifact_count > 0 or bool(valid_artifacts) or bool(valid_reply_media)
    if requires_artifact and not has_deliverable:
        missing.append(f"{artifact_kind} attachment")

    if missing:
        has_completion_claim = _has_completion_claim(reply or "")
        if not has_completion_claim:
            return ResponseFulfillmentDecision(False, reply, tuple(missing))
        if attempted_tool_names:
            missing_text = ", ".join(f"a {item}" for item in missing)
            final_sentence = (
                "I’ll keep it open instead of marking it done."
                if frame is not None
                else "I won’t mark it done."
            )
            response = (
                f"I’m not done yet — this task still needs {missing_text}, "
                "but I didn’t produce one in that run. "
                f"{final_sentence}"
            )
        else:
            response = (
                f"I can’t mark this complete yet — this task still needs {', '.join(missing)}."
            )
        return ResponseFulfillmentDecision(False, response, tuple(missing))

    return ResponseFulfillmentDecision(True, reply)


__all__ = [
    "ResponseFulfillmentDecision",
    "artifact_paths_from_tool_results",
    "evaluate_response_fulfillment",
    "guaranteed_user_visible_text",
    "user_visible_text_from_output",
]
