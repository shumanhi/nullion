from __future__ import annotations

from dataclasses import dataclass

import pytest

from nullion.runtime_store import RuntimeStore
from nullion.tools import (
    ToolInvocation,
    ToolRegistry,
    register_browser_plugin,
    register_calendar_plugin,
    register_cron_tools,
    register_email_plugin,
    register_media_plugin,
    register_reminder_tools,
    register_search_plugin,
    register_workspace_plugin,
)


def _invoke(tool_name: str, arguments: dict[str, object], *, principal_id: str = "web:operator") -> ToolInvocation:
    return ToolInvocation(
        invocation_id=f"inv-{tool_name}",
        tool_name=tool_name,
        principal_id=principal_id,
        arguments=arguments,
    )


@dataclass
class RuntimeDouble:
    store: RuntimeStore


def test_plugin_registration_exposes_expected_tool_families(tmp_path, monkeypatch) -> None:
    from nullion import crons

    monkeypatch.setattr(crons, "_CRONS_PATH", tmp_path / "crons.json")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    runtime = RuntimeDouble(store=RuntimeStore())
    sample_file = tmp_path / "report.txt"
    sample_file.write_text("hello", encoding="utf-8")

    register_search_plugin(registry, web_searcher=lambda query, limit: [{"title": query, "url": "https://example.com"}])
    register_email_plugin(
        registry,
        email_searcher=lambda query, limit: [{"id": "msg-1", "subject": query}],
        email_reader=lambda message_id: {"id": message_id, "body": "hello"},
    )
    register_calendar_plugin(
        registry,
        calendar_lister=lambda start, end, limit: [{"id": "evt-1", "summary": f"{start}/{end}"}],
    )
    register_media_plugin(
        registry,
        audio_transcriber=lambda path, language=None: {"text": f"transcribed:{path}", "language": language or "auto"},
        image_text_extractor=lambda path: {"text": f"ocr:{path}"},
        image_generator=lambda prompt, output_path, source_path=None: {"prompt": prompt, "source_path": source_path},
    )
    register_browser_plugin(registry, browser_navigator=lambda url: {"url": url, "title": "Example"})
    register_workspace_plugin(registry, workspace_root=tmp_path)
    register_reminder_tools(registry, runtime, default_chat_id="web:operator")
    register_cron_tools(registry, default_delivery_channel="web", default_delivery_target="web:operator")

    expected_tools = {
        "web_search",
        "email_search",
        "email_read",
        "calendar_list",
        "audio_transcribe",
        "image_extract_text",
        "image_generate",
        "browser_navigate",
        "file_search",
        "set_reminder",
        "list_reminders",
        "create_cron",
        "list_crons",
        "delete_cron",
        "toggle_cron",
        "run_cron",
    }
    assert expected_tools.issubset({spec.name for spec in registry.list_specs()})
    assert registry.list_installed_plugins() == [
        "browser_plugin",
        "calendar_plugin",
        "email_plugin",
        "media_plugin",
        "search_plugin",
        "workspace_plugin",
    ]

    assert registry.invoke(_invoke("web_search", {"query": "stocks", "limit": 1})).status == "completed"
    assert registry.invoke(_invoke("email_search", {"query": "urgent", "limit": 1})).output["results"][0]["id"] == "msg-1"
    assert registry.invoke(_invoke("email_read", {"id": "msg-1"})).output["message"]["body"] == "hello"
    assert registry.invoke(_invoke("calendar_list", {"start": "2026-04-29", "end": "2026-04-30", "max": 1})).status == "completed"
    assert registry.invoke(_invoke("audio_transcribe", {"path": str(sample_file)})).output["text"].startswith("transcribed:")
    assert registry.invoke(_invoke("image_extract_text", {"path": str(sample_file)})).output["text"].startswith("ocr:")
    assert registry.invoke(_invoke("image_generate", {"prompt": "draw", "output_path": str(tmp_path / "out.png")})).status == "completed"
    assert registry.invoke(_invoke("browser_navigate", {"url": "https://example.com"})).output["title"] == "Example"
    assert registry.invoke(_invoke("file_search", {"pattern": "report"})).output["matches"] == [str(sample_file)]


def test_required_plugin_dependencies_fail_loudly(tmp_path) -> None:
    registry = ToolRegistry()

    with pytest.raises(ValueError, match="email_searcher"):
        register_email_plugin(registry)
    with pytest.raises(ValueError, match="calendar_lister"):
        register_calendar_plugin(registry)
    with pytest.raises(ValueError, match="workspace_root or allowed_roots"):
        register_workspace_plugin(registry)
