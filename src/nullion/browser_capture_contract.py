"""Completion evidence for an explicitly scoped browser capture request."""

from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlparse


CAPTURE_TOOLS = frozenset({"browser_navigate", "browser_open", "browser_wait_for", "browser_screenshot"})



def _capture_results(tool_results: object) -> tuple:
    from nullion.tools import ToolResult

    return tuple(
        ToolResult(str(result.get("invocation_id") or ""), str(result.get("tool_name") or ""),
                   str(result.get("status") or ""), result.get("output"))
        if isinstance(result, Mapping) else result
        for result in (tool_results or ())
    )

def conversation_navigation(store: object, conversation_id: str | None):
    """Return owned navigation evidence only while later browser activity agrees."""
    read_events = getattr(store, "list_conversation_events", None)
    if not conversation_id or not callable(read_events):
        return None
    session_id = None
    for event in reversed(read_events(conversation_id)):
        if event.get("event_type") != "conversation.chat_turn":
            continue
        for result in reversed(event.get("tool_results") or ()):
            if not str(result.get("tool_name") or "").startswith("browser_"):
                continue
            output = result.get("output") or {}
            current_session = output.get("session_id") if isinstance(output, Mapping) else None
            if result.get("status") != "completed" or not current_session:
                return None
            if session_id is not None and current_session != session_id:
                return None
            session_id = current_session
            if result.get("tool_name") == "browser_navigate":
                from nullion.tools import ToolResult
                return ToolResult("prior-navigation", "browser_navigate", "completed", dict(output))
    return None


def capture_only_scope(decision: object, tool_results: object = ()) -> bool:
    if not getattr(decision, "valid", False):
        return False
    names = set(getattr(decision, "requested_tool_names", ()) or ()) | set(
        getattr(decision, "required_tool_names", ()) or ())
    # Scope expansion exposes supporting tools as requested_tool_names. The
    # scope receipt retains the model's explicit tools separately.
    for result in reversed(_capture_results(tool_results)):
        if getattr(result, "tool_name", None) != "request_tool_scope":
            continue
        output = getattr(result, "output", None)
        if getattr(result, "status", None) == "completed" and isinstance(output, Mapping):
            exact = set(output.get("explicit_tool_names") or ())
            required = set(getattr(decision, "required_tool_names", ()) or ())
            if exact:
                names = exact | required
        break
    extensions = set(getattr(decision, "requested_artifact_extensions", ()) or ())
    return bool(
        "browser_screenshot" in names
        and not names - CAPTURE_TOOLS
        and not extensions - {".png"}
        and not getattr(decision, "required_embedded_media_extensions", ())
        and ".png" not in (getattr(decision, "excluded_artifact_extensions", ()) or ())
        and getattr(decision, "scheduler_action", "none") == "none"
        and getattr(decision, "skill_pack_action", "none") == "none"
        and not getattr(decision, "connector_app_ids", ())
    )


def completed_capture_paths(decision: object, tool_results: object) -> tuple[str, ...]:
    results = _capture_results(tool_results)
    if not capture_only_scope(decision, results):
        return ()
    if any(r.status != "completed" or r.tool_name not in CAPTURE_TOOLS | {"request_tool_scope"} for r in results):
        return ()
    required = set(getattr(decision, "required_tool_names", ()) or ())
    if not required <= {r.tool_name for r in results}:
        return ()
    return verified_capture_paths(results)


def delivered_capture_paths(decision: object, tool_results: object, final_text: str) -> tuple[str, ...]:
    """Accept a model-declared attachment only when its capture is verified."""
    results = _capture_results(tool_results)
    if getattr(decision, "valid", False) and not capture_only_scope(decision, results):
        return ()
    if any(r.status != "completed" or r.tool_name not in CAPTURE_TOOLS | {"request_tool_scope"} for r in results):
        return ()
    if set(getattr(decision, "required_tool_names", ()) or ()) - {r.tool_name for r in results}:
        return ()
    verified = verified_capture_paths(results)
    if not verified:
        return ()
    from nullion.artifacts import media_candidate_paths_from_text
    declared = {str(path) for path in media_candidate_paths_from_text(final_text)}
    return tuple(path for path in verified if path in declared)


def verified_capture_paths(tool_results: object) -> tuple[str, ...]:
    """Require a real PNG from the last browser operation in a loaded session."""
    browser_results = [r for r in _capture_results(tool_results) if str(getattr(r, "tool_name", "")).startswith("browser_")]
    if not browser_results:
        return ()
    capture = browser_results[-1]
    if capture.tool_name != "browser_screenshot" or capture.status != "completed":
        return ()
    output = capture.output
    if not isinstance(output, Mapping):
        return ()
    session_id = output.get("session_id")
    parsed_url = urlparse(str(output.get("page_url") or ""))
    if not session_id or parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
        return ()
    # Navigation receipts are runtime-owned and must refer to this physical
    # session. A blank capture or one from another request cannot fulfill it.
    preceding = browser_results[:-1]
    navigations = [r for r in preceding if r.tool_name in {"browser_navigate", "browser_open"}]
    if navigations:
        if navigations[-1].status != "completed":
            return ()
        navigation_output = navigations[-1].output
    else:
        # Added by the runtime only after binding a contextual capture to a
        # navigation receipt from this conversation; never a model argument.
        navigation_output = output.get("navigation_receipt")
    if not isinstance(navigation_output, Mapping) or navigation_output.get("session_id") != session_id:
        return ()
    if any(r.status != "completed" or r.tool_name not in CAPTURE_TOOLS for r in preceding):
        return ()
    path = Path(str(output.get("artifact_path") or output.get("path") or ""))
    try:
        from PIL import Image
        with Image.open(path) as image:
            if image.format != "PNG" or image.width <= 0 or image.height <= 0:
                return ()
            image.verify()
    except (OSError, ValueError):
        return ()
    return (str(path),)
