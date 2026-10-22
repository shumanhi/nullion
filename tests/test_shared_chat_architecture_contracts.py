from __future__ import annotations

import os
from pathlib import Path


def _app_repo_root() -> Path:
    configured = os.environ.get("NULLION_APP_REPO")
    if configured:
        return Path(configured)
    import nullion

    return Path(nullion.__file__).resolve().parents[2]


def test_raw_turn_tool_scoper_is_not_called_outside_shared_policy() -> None:
    app_repo = _app_repo_root()
    src_root = app_repo / "src" / "nullion"
    assert src_root.exists()

    offenders: list[str] = []
    for path in src_root.rglob("*.py"):
        if path.name == "turn_context_policy.py":
            continue
        text = path.read_text(encoding="utf-8")
        if "scoped_turn_tool_registry(" in text:
            offenders.append(str(path.relative_to(app_repo)))

    assert offenders == []


def test_web_telegram_and_other_chat_surfaces_use_shared_platform_ingress() -> None:
    app_repo = _app_repo_root()
    src_root = app_repo / "src" / "nullion"
    platform_chat = src_root / "platform_chat.py"
    messaging_turn_graph = src_root / "messaging_turn_graph.py"
    web_app = src_root / "web_app.py"

    platform_source = platform_chat.read_text(encoding="utf-8")
    graph_source = messaging_turn_graph.read_text(encoding="utf-8")
    web_source = web_app.read_text(encoding="utf-8")

    assert "def run_platform_chat_request" not in platform_source
    assert "def run_platform_chat_request" in graph_source
    assert "from nullion.platform_chat import PlatformChatRequest, run_platform_chat_request" in web_source

    import nullion.messaging_turn_graph as messaging_turn_graph_module
    import nullion.platform_chat as platform_chat_module

    assert platform_chat_module.PlatformChatRequest is messaging_turn_graph_module.PlatformChatRequest
    assert platform_chat_module.PlatformChatResponse is messaging_turn_graph_module.PlatformChatResponse
    assert platform_chat_module.run_platform_chat_request is messaging_turn_graph_module.run_platform_chat_request


def test_legacy_web_only_turn_runner_is_not_present_in_app_or_tests() -> None:
    app_repo = _app_repo_root()
    src_root = app_repo / "src" / "nullion"
    test_repo = Path(__file__).resolve().parents[1]
    legacy_names = ("_run_turn_sync", "_run_guarded_turn_sync")
    offenders: list[str] = []

    for path in src_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for name in legacy_names:
            if name in text:
                offenders.append(str(path.relative_to(app_repo)))
                break

    for path in test_repo.rglob("*.py"):
        if path.resolve() == Path(__file__).resolve():
            continue
        text = path.read_text(encoding="utf-8")
        for name in legacy_names:
            if name in text:
                offenders.append(str(path.relative_to(test_repo)))
                break

    assert offenders == []
