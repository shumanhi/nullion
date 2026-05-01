from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from nullion import web_app
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
)
from nullion.tools import ToolResult


class FakeStore:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.suspended: list[object] = []
        self.active_frame_id: str | None = None
        self.frames: dict[str, object] = {}
        self.task_frames = self.frames
        self.approvals: dict[str, object] = {}
        self.doctor_actions: dict[str, dict] = {}

    def add_conversation_event(self, event: dict) -> None:
        self.events.append(event)

    def list_conversation_events(self, conversation_id: str) -> list[dict]:
        return [event for event in self.events if event.get("conversation_id") == conversation_id]

    def set_active_task_frame_id(self, conversation_id: str, frame_id: str | None) -> None:
        self.active_frame_id = frame_id

    def get_active_task_frame_id(self, conversation_id: str) -> str | None:
        return self.active_frame_id

    def get_task_frame(self, frame_id: str | None):
        return self.frames.get(frame_id or "")

    def add_task_frame(self, frame) -> None:
        self.frames[frame.frame_id] = frame

    def add_suspended_turn(self, turn) -> None:
        self.suspended.append(turn)

    def get_approval_request(self, approval_id: str):
        return self.approvals.get(approval_id)

    def list_approval_requests(self) -> list[object]:
        return list(self.approvals.values())

    def list_suspended_turns(self) -> list[object]:
        return list(self.suspended)

    def add_doctor_action(self, action: dict) -> None:
        action = dict(action)
        action.setdefault("created_at", datetime.now(UTC).isoformat())
        action.setdefault("updated_at", action["created_at"])
        self.doctor_actions[action["action_id"]] = action

    def get_doctor_action(self, action_id: str):
        action = self.doctor_actions.get(action_id)
        return dict(action) if action is not None else None


class FakeRuntime:
    def __init__(self, store: FakeStore | None = None) -> None:
        self.store = store
        self.checkpoints = 0
        self.health_issues: list[dict] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def report_health_issue(self, **kwargs) -> None:
        self.health_issues.append(kwargs)


def test_basic_web_app_helpers_cover_edge_shapes(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    assert web_app._default_workspace_root() == str(tmp_path)
    assert web_app._truthy_env("yes") is True
    assert web_app._truthy_env(" false ") is False
    assert web_app._split_model_entries([" a ", "a", "", "b"]) == ["a", "b"]
    assert web_app._split_model_entries("a,b\na") == ["a", "b"]
    assert web_app._primary_model_entry(" first,second") == "first"

    monkeypatch.setattr(web_app.shutil, "which", lambda name: name if name == "found" else None)
    assert web_app._which_local_tool("found") == "found"
    assert web_app._which_local_tool("missing") == ""

    monkeypatch.setenv("CODEX_REFRESH_TOKEN", "env-refresh")
    provider, token = web_app._model_provider_and_codex_token_for_config(
        "openai",
        oai_key="oauth-token",
        creds={"provider": "codex", "api_key": "stored-access"},
        stored_keys={"codex": "stored-key"},
    )
    assert provider == "codex"
    assert token == "env-refresh"


@pytest.mark.parametrize(
    ("capability", "provider", "model", "expected"),
    [
        ("audio_transcribe", "openai", "whisper-1", True),
        ("audio_transcribe", "anthropic", "claude", False),
        ("image_ocr", "anthropic", "claude-sonnet", True),
        ("image_generate", "openai", "gpt-image-1", True),
        ("image_generate", "custom", "anything", True),
        ("video_input", "openai", "gpt-4o-video", True),
        ("video_input", "custom", "vision-vl", True),
        ("unknown", "openai", "gpt-5", False),
        ("image_ocr", "", "gpt-4o", False),
    ],
)
def test_media_support_matrix(capability: str, provider: str, model: str, expected: bool) -> None:
    assert web_app._media_model_supports(capability, provider, model) is expected


def test_media_model_normalization_and_options_cover_fallbacks() -> None:
    media_models = {
        "openai": [
            "gpt-4o",
            {"name": "gpt-image-1", "capabilities": ["image_output", "image_output"]},
            {"model": ""},
            object(),
        ],
        "bad": "not-list",
    }
    normalized = web_app._normalize_media_models(media_models)
    assert normalized == {
        "openai": [
            {"model": "gpt-4o", "capabilities": []},
            {"model": "gpt-image-1", "capabilities": ["image_output"]},
        ]
    }
    assert web_app._media_capability_keys("video_input") == {"video", "video_input", "video_analysis"}
    assert web_app._media_selection_supported(
        "image_ocr",
        provider="custom",
        model="declared-only",
        media_models={"custom": [{"model": "declared-only", "capabilities": ["vision"]}]},
    )
    assert web_app._media_model_options(
        "image_ocr",
        provider_models={"openai": "gpt-4o,gpt-4o-mini"},
        media_models={},
        providers_enabled={"openai": True},
        media_providers_enabled={"openai": True},
        providers_configured={"openai": True},
        active_provider="openai",
        active_model="gpt-4o",
    )[0]["connected"] == "true"
    assert web_app._media_capability_supported("image_input", "openai", "gpt-4o")
    assert web_app._media_capability_supported("audio_input", "openai", "whisper-1")
    assert web_app._media_capability_supported("image_output", "openai", "gpt-image-1")
    assert web_app._media_capability_supported("video_input", "openai", "gpt-4o")
    assert web_app._media_capability_supported("other", "openai", "gpt-4o") is False
    assert web_app._media_model_declares_capability(
        {"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        provider="openai",
        model="missing",
        capability="image_ocr",
    ) is False
    assert web_app._normalize_media_models("bad") == {}
    assert web_app._media_capability_keys("image_generate") == {"image_output", "image_generation", "image_generate"}
    assert web_app._media_model_options(
        "image_ocr",
        provider_models={"openai": "gpt-4o"},
        media_models={"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        providers_enabled={"openai": True},
        media_providers_enabled={"openai": True},
        providers_configured={"openai": False},
    ) == []


def test_log_buffer_and_runtime_history_sync(tmp_path) -> None:
    web_app._LOG_BUFFER.clear()
    handler = web_app._WebUILogBufferHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    record = logging.LogRecord("test.logger", logging.INFO, __file__, 1, "secret sk-test", (), None)
    handler.emit(record)
    assert web_app._logs_payload("memory")["entry_count"] == 1

    checkpoint = tmp_path / "runtime-store.json"
    checkpoint.write_text(
        '{"conversation_events":[{"event_type":"conversation.chat_turn","user_message":"hi"}]}',
        encoding="utf-8",
    )
    imported: list[list[dict]] = []
    runtime = SimpleNamespace(checkpoint_path=checkpoint)
    store = SimpleNamespace(import_runtime_chat_turns=lambda turns: imported.append(turns))
    web_app._sync_runtime_chat_history_to_store(runtime, store)
    web_app._sync_runtime_chat_history_to_store(runtime, store)
    assert imported == [[{"event_type": "conversation.chat_turn", "user_message": "hi"}]]

    missing_runtime = SimpleNamespace(checkpoint_path=tmp_path / "missing.json")
    web_app._sync_runtime_chat_history_to_store(missing_runtime, store)
    no_events = tmp_path / "no-events.json"
    no_events.write_text("{}", encoding="utf-8")
    web_app._sync_runtime_chat_history_to_store(SimpleNamespace(checkpoint_path=no_events), store)


def test_permission_and_session_duration_helpers(monkeypatch) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert web_app._permission_memory_expires_at("forever", now=now) is None
    assert web_app._permission_memory_expires_at("15m", now=now) == now + timedelta(minutes=15)
    assert web_app._permission_memory_expires_at("30m", now=now) == now + timedelta(minutes=30)
    assert web_app._permission_memory_expires_at("1h", now=now) == now + timedelta(hours=1)
    assert web_app._permission_memory_expires_at("2h", now=now) == now + timedelta(hours=2)
    assert web_app._permission_memory_expires_at("4h", now=now) == now + timedelta(hours=4)
    assert web_app._permission_memory_expires_at("today", now=now) == now + timedelta(days=1)
    assert web_app._permission_memory_expires_at("30d", now=now) == now + timedelta(days=30)
    assert web_app._permission_memory_expires_at("week", now=now) == now + timedelta(days=7)
    assert web_app._permission_memory_expires_at("mystery", now=now) == now + timedelta(days=7)
    monkeypatch.setenv("NULLION_WEB_SESSION_ALLOW_DURATION", "15m")
    assert web_app._web_session_allow_duration_value() == "15m"
    assert "15" in web_app._web_session_allow_duration_label()
    assert web_app._web_session_allow_expires_at(now=now) == now + timedelta(minutes=15)


def test_chat_media_hydration_and_artifact_notice(monkeypatch, tmp_path) -> None:
    existing = tmp_path / "image.png"
    existing.write_text("x", encoding="utf-8")
    missing = tmp_path / "missing.png"

    def fake_descriptors(runtime, paths, *, principal_id=None):
        return [{"path": str(existing), "url": "/api/artifacts/id"}] if str(existing) in paths else []

    monkeypatch.setattr(web_app, "_web_artifact_descriptors", fake_descriptors)
    metadata = {
        "artifacts": [{"path": str(existing), "name": "image"}],
        "attachments": [{"path": str(missing), "name": "missing", "url": "stale"}],
    }
    hydrated = web_app._hydrate_chat_history_media(
        object(),
        [{"role": "bot", "text": "ok", "metadata": metadata}],
        principal_id="web:operator",
    )
    assert hydrated[0]["artifacts"][0]["url"] == "/api/artifacts/id"
    assert hydrated[0]["attachments"][0]["missing"] is True
    assert "did not offer it as a download" in web_app._web_artifact_delivery_notice("Done", ["x"], [])
    assert web_app._web_screenshot_reply("https://example.com") == "Done — captured a screenshot of https://example.com."
    failure = SimpleNamespace(url="https://example.com", error="")
    assert "did not complete" in web_app._web_screenshot_failure_reply(failure)


def test_web_fetch_attachment_helpers(monkeypatch, tmp_path) -> None:
    completed = ToolResult(
        invocation_id="1",
        tool_name="web_fetch",
        status="completed",
        output={"url": "https://example.com/a", "body": "<html><head></head><body><script>x()</script>Hello</body></html>"},
    )
    failed = ToolResult(invocation_id="2", tool_name="web_fetch", status="failed", output={"body": "no"})
    assert web_app._requested_web_attachment_extension("save https://example.com as html") == ".html"
    assert web_app._requested_web_attachment_extension("download example.com text file") == ".txt"
    assert web_app._requested_web_attachment_extension("example.com as a text file") == ".txt"
    assert web_app._requested_web_attachment_extension("fetch https://example.com") is None
    assert web_app._requested_web_attachment_extension("send example.com") is None
    assert web_app._requested_web_attachment_extension("hello") is None
    assert web_app._latest_completed_web_tool_result([failed, completed], tool_name="web_fetch") is completed
    assert web_app._latest_completed_web_tool_result([failed], tool_name="web_fetch") is None
    assert web_app._web_fetch_body_for_attachment(completed, extension=".txt").startswith("<html")
    assert web_app._web_fetch_body_for_attachment(
        ToolResult(invocation_id="3", tool_name="web_fetch", status="completed", output={"html": "<p>x</p>"}),
        extension=".html",
    ) == "<p>x</p>"
    assert web_app._web_fetch_body_for_attachment(
        ToolResult(invocation_id="4", tool_name="web_fetch", status="completed", output={}),
        extension=".txt",
    ) is None
    terminal = ToolResult(
        invocation_id="5",
        tool_name="terminal_exec",
        status="completed",
        output={"stdout": "curl output\n", "stderr": "", "exit_code": 0},
    )
    assert web_app._terminal_exec_output_for_attachment(terminal, extension=".txt") == "curl output\n"
    assert web_app._terminal_exec_output_for_attachment(
        ToolResult(
            invocation_id="6",
            tool_name="terminal_exec",
            status="completed",
            output={"stdout": "", "stderr": "warning\n", "exit_code": 0},
        ),
        extension=".txt",
    ) == "warning\n"
    safe_html = web_app._viewable_static_html("<html><body><script>x()</script>Hi</body></html>", source_url="https://example.com")
    assert "<script" not in safe_html
    assert '<base href="https://example.com">' in safe_html
    assert "<pre>" in web_app._viewable_static_html("plain < text")

    monkeypatch.setattr(web_app, "artifact_path_for_generated_file", lambda runtime, suffix: tmp_path / f"artifact{suffix}")
    paths = web_app._materialize_fetch_artifact_for_web(
        object(),
        prompt="fetch https://example.com as html",
        tool_results=[completed],
    )
    assert paths == [str(tmp_path / "artifact.html")]
    assert "scripts disabled" in (tmp_path / "artifact.html").read_text(encoding="utf-8")
    paths = web_app._materialize_fetch_artifact_for_web(
        object(),
        prompt="curl google.com and send output in a text file",
        tool_results=[terminal],
    )
    assert paths == [str(tmp_path / "artifact.txt")]
    assert (tmp_path / "artifact.txt").read_text(encoding="utf-8") == "curl output\n"
    assert web_app._materialize_fetch_artifact_for_web(object(), prompt="hello", tool_results=[completed]) == []
    assert web_app._materialize_fetch_artifact_for_web(object(), prompt="save https://example.com as html", tool_results=[failed]) == []
    assert web_app._materialize_fetch_artifact_for_web(
        object(),
        prompt="save https://example.com as html",
        tool_results=[ToolResult(invocation_id="5", tool_name="web_fetch", status="completed", output={})],
    ) == []


def test_web_artifact_descriptors_cover_deduplication(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, media_root):
        root.mkdir()
    artifact = runtime_root / "artifact.txt"
    artifact.write_text("hello", encoding="utf-8")
    duplicate = principal_root / "artifact.txt"
    duplicate.write_text("hello", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(media=media_root),
    )
    payloads = web_app._web_artifact_descriptors(object(), [str(artifact), str(duplicate)], principal_id="web")
    assert len(payloads) == 2
    assert payloads[0]["url"].startswith("/api/artifacts/")


def test_web_delivery_artifact_paths_include_plain_pdf_reply_path(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, media_root):
        root.mkdir()
    txt_artifact = principal_root / "fresh_news_briefing_2026-05-01.txt"
    pdf_artifact = principal_root / "fresh_news_briefing_2026-05-01.pdf"
    txt_artifact.write_text("staging text", encoding="utf-8")
    pdf_artifact.write_bytes(b"%PDF-1.4\n%test\n")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="and then send me in a pdf",
        reply=f"Download / attachment path:\n{pdf_artifact}",
        tool_results=[
            ToolResult(
                invocation_id="write-txt",
                tool_name="file_write",
                status="completed",
                output={"path": str(txt_artifact)},
            )
        ],
        principal_id="web:1",
    )

    assert paths == [str(pdf_artifact)]


def test_artifact_lookup_download_path_and_logs(monkeypatch, tmp_path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    file_path = root / "one.txt"
    file_path.write_text("hello\nsecret", encoding="utf-8")
    descriptor = web_app.artifact_descriptor_for_path(file_path, artifact_root=root)
    assert descriptor is not None
    monkeypatch.setattr(web_app, "_web_artifact_roots", lambda runtime: [root])
    web_app._WEB_ARTIFACTS[descriptor.artifact_id] = file_path
    assert web_app._web_artifact_path_for_id(object(), descriptor.artifact_id) == file_path
    assert web_app._web_artifact_descriptor_for_path(object(), file_path).artifact_id == descriptor.artifact_id
    web_app._WEB_ARTIFACTS.pop(descriptor.artifact_id, None)
    assert web_app._web_artifact_path_for_id(object(), descriptor.artifact_id) == file_path
    assert web_app._web_artifact_path_for_id(object(), "missing") is None
    assert web_app._web_artifact_descriptor_for_path(object(), tmp_path / "outside.txt") is None

    downloads = tmp_path / "downloads"
    first = web_app._unique_download_path("../report.txt", downloads_dir=downloads)
    first.write_text("exists", encoding="utf-8")
    assert web_app._unique_download_path("../report.txt", downloads_dir=downloads).name == "report (1).txt"
    for idx in range(1, 1000):
        (downloads / f"report ({idx}).txt").write_text("exists", encoding="utf-8")
    with pytest.raises(RuntimeError):
        web_app._unique_download_path("../report.txt", downloads_dir=downloads)

    log_dir = tmp_path / ".nullion" / "logs"
    log_dir.mkdir(parents=True)
    log_file = log_dir / "nullion.log"
    log_file.write_text("line1\nline2", encoding="utf-8")
    monkeypatch.setattr(web_app.Path, "home", lambda: tmp_path)
    assert web_app._known_log_files() == [log_file]
    assert web_app._tail_text(log_file, max_bytes=5)["truncated"] is True
    assert web_app._logs_payload("nullion.log")["ok"] is True
    assert web_app._logs_payload("missing.log")["ok"] is False


def test_reporting_doctor_and_chat_service_helpers(monkeypatch) -> None:
    runtime = FakeRuntime()
    web_app._report_web_client_issue(runtime, issue_type="nonsense", message="", details={"x": "y"})
    assert runtime.health_issues[0]["source"] == "web_app"

    assert web_app._doctor_action_can_try_fix({"summary": "Telegram typing indicator stuck"})
    assert not web_app._doctor_action_can_try_fix({"summary": "unrelated"})
    with pytest.raises(ValueError):
        web_app._try_doctor_fix({"summary": "unrelated"})

    monkeypatch.delenv("NULLION_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("NULLION_SLACK_ENABLED", "true")
    monkeypatch.setenv("NULLION_SLACK_BOT_TOKEN", "x")
    monkeypatch.setenv("NULLION_SLACK_APP_TOKEN", "y")
    monkeypatch.setenv("NULLION_DISCORD_ENABLED", "true")
    monkeypatch.delenv("NULLION_DISCORD_BOT_TOKEN", raising=False)
    assert web_app._chat_service_enabled_from_env("slack") is True
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "chat")
    assert web_app._chat_service_configured_from_env("telegram") == (True, "Configured")
    monkeypatch.setenv("NULLION_DISCORD_BOT_TOKEN", "discord")
    assert web_app._chat_service_configured_from_env("discord") == (True, "Configured")
    monkeypatch.delenv("NULLION_DISCORD_BOT_TOKEN", raising=False)
    assert web_app._chat_service_configured_from_env("discord") == (False, "Needs token")
    monkeypatch.setattr(web_app, "_launchd_status_for_labels", lambda labels: (None, None))
    monkeypatch.setattr(web_app, "_process_running_for_command", lambda command: command == "nullion-slack")
    services = web_app._chat_services_status_payload()
    assert {service["name"]: service["state"] for service in services}["slack"] == "live"


def test_message_history_error_and_activity_helpers(monkeypatch) -> None:
    messages = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": [{"type": "text", "text": "old"}]},
        {"role": "assistant", "content": "Approval required for tool"},
        {"role": "user", "content": "new"},
    ]
    assert web_app._text_from_message_content(messages[1]["content"]) == "old"
    assert web_app._text_from_message_content(123) == ""
    assert web_app._text_from_message_content([{"type": "image", "text": "ignored"}]) == ""
    assert web_app._last_user_text_from_snapshot(messages) == "new"
    assert web_app._last_user_text_from_snapshot([]) is None
    assert web_app._last_user_text_from_snapshot([{"role": "assistant", "content": "x"}]) is None
    assert web_app._resume_history_from_snapshot(messages) == messages[:1]
    assert web_app._resume_history_from_snapshot([]) == []
    assert web_app._is_stale_approval_notice({"role": "user", "content": "approval required"}) is False
    assert web_app._is_stale_approval_notice({"role": "assistant", "content": "Tool approval requested: x"})
    label, detail, is_web = web_app._approval_display_from_request(None)
    assert label and isinstance(detail, str) and is_web is False
    assert web_app._approval_trigger_flow_label_from_request(None) is None

    assert web_app._short_error_text(ValueError("")) == "ValueError"
    long_error = ValueError("x" * 300)
    assert web_app._short_error_text(long_error).endswith("...")
    routed = ValueError(
        "Error code: 404 - {'error': {'message': 'No allowed providers are available', "
        "'metadata': {'available_providers': ['a'], 'requested_providers': ['b']}}}"
    )
    assert "restricted to providers" in web_app._short_error_text(routed)
    credit_hint = ValueError("No endpoints found metadata': {'available_providers': ['a']}")
    assert "Check credits" in web_app._short_error_text(credit_hint)
    assert web_app._short_error_text(ValueError("plain")) == "plain"
    assert web_app._is_new_command("/new@bot now")
    assert not web_app._is_new_command("/new@")
    assert web_app._web_task_frame_summary("  hello   world ") == "hello world"
    assert web_app._web_task_frame_summary("", limit=10) == "Web chat turn"
    assert web_app._web_task_frame_summary("abcdefghijk", limit=8) == "abcde..."

    store = FakeStore()
    runtime = FakeRuntime(store)
    web_app._record_web_conversation_reset(runtime, "conv")
    web_app._remember_web_chat_turn(
        runtime,
        conversation_id="conv",
        user_message="u",
        assistant_reply="a",
        tool_results=[ToolResult(invocation_id="1", tool_name="tool", status="completed", output={"api_key": "secret"})],
    )
    assert web_app._web_chat_events_after_latest_reset(runtime, "conv")[0]["assistant_reply"] == "a"
    assert web_app._web_chat_events_after_latest_reset(SimpleNamespace(store=None), "conv") == []
    assert web_app._web_chat_history_from_store(runtime, "conv") == [
        {"role": "user", "content": [{"type": "text", "text": "u"}]},
        {"role": "assistant", "content": [{"type": "text", "text": "a"}]},
    ]
    store.add_conversation_event({"conversation_id": "conv", "event_type": "conversation.chat_turn", "user_message": "only user"})
    assert web_app._web_chat_history_from_store(SimpleNamespace(store=None), "conv") == []
    assert "Recent tool outcomes" in web_app._recent_web_tool_context_prompt(runtime, "conv")
    assert web_app._recent_web_tool_context_prompt(SimpleNamespace(store=None), "conv") is None
    assert web_app._recent_web_tool_context_prompt(SimpleNamespace(store=FakeStore()), "conv") is None
    assert web_app._compact_web_tool_results_for_context(None) == []
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "off")
    assert web_app._web_memory_context(runtime) is None

    tool_results = [
        ToolResult(invocation_id="1", tool_name="web_fetch", status="completed", output={"url": "https://example.com/a"}),
        ToolResult(invocation_id="2", tool_name="tool", status="failed", output="plain", error="boom"),
        ToolResult(invocation_id="3", tool_name="tool", status="pending", output={}),
        ToolResult(invocation_id="4", tool_name="tool", status="denied", output={"reason": "policy"}),
    ]
    events = web_app._web_activity_events_for_tool_results(tool_results)
    assert [event["status"] for event in events if event["id"].startswith("tool-")] == ["done", "failed", "done", "blocked"]
    assert web_app._web_activity_events_for_tool_results([]) == []
    monkeypatch.setattr(web_app, "is_untrusted_tool_name", lambda name: False)
    assert web_app._web_tool_result_detail(ToolResult("5", "web_search", "completed", {"summary": "hidden"})) is None
    assert web_app._web_tool_result_detail(ToolResult("6", "tool", "completed", {"url": "https://example.com/a"})) == "example.com"
    assert web_app._web_tool_result_detail(ToolResult("7", "tool", "completed", {"summary": "sum"})) == "sum"
    assert web_app._web_tool_result_detail(ToolResult("8", "tool", "completed", "plain text")) == "plain text"
    assert web_app._web_tool_result_detail(ToolResult("9", "tool", "completed", {})) is None

    emitted: list[dict[str, str]] = []
    web_app._emit_activity(None, "none", "None")
    web_app._emit_activity(emitted.append, "x", "Label", "done", "detail")
    web_app._emit_skill_usage_activity(emitted.append, ["Skill A"])
    web_app._emit_skill_usage_activity(emitted.append, [])
    web_app._emit_activity(lambda event: (_ for _ in ()).throw(RuntimeError("boom")), "x", "Label")
    assert emitted[0]["detail"] == "detail"
    assert emitted[-1]["id"] == "skill"
    assert web_app._mini_agent_activity_detail(SimpleNamespace(task_status_detail="ready"), 2) == "ready"
    assert isinstance(web_app._mini_agent_activity_detail(SimpleNamespace(task_titles=["A", "B"]), 2), str)


def test_dead_task_frame_cleanup_cancels_and_records_doctor_item() -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-dead",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="stuck approval task",
        created_at=datetime.now(UTC) - timedelta(minutes=5),
        updated_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)

    cleaned = web_app._cleanup_dead_task_frames(runtime)

    assert cleaned == [
        {
            "frame_id": "frame-dead",
            "status": "cancelled",
            "summary": "stuck approval task",
            "message": "Killed task frame frame-dead.",
        }
    ]
    assert store.get_task_frame("frame-dead").status is TaskFrameStatus.CANCELLED
    assert store.get_active_task_frame_id("web:operator") is None
    action = store.get_doctor_action("act-dead-task-frame-dead")
    assert action["status"] == "completed"
    assert action["recommendation_code"] == "cleanup_dead_task_frame"
    assert "dead_task_frame" in action["source_reason"]
    assert runtime.checkpoints == 1


def test_dead_task_frame_cleanup_leaves_fresh_waiting_approval_alone() -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-fresh",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="fresh approval wait",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)

    assert web_app._cleanup_dead_task_frames(runtime) == []
    assert store.get_task_frame("frame-fresh").status is TaskFrameStatus.WAITING_APPROVAL
    assert store.get_active_task_frame_id("web:operator") == "frame-fresh"
    assert store.doctor_actions == {}


def test_dead_task_frame_cleanup_leaves_live_pending_approval_alone() -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-live",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="live approval wait",
        created_at=datetime.now(UTC) - timedelta(minutes=5),
        updated_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)
    store.approvals["ap-live"] = SimpleNamespace(
        approval_id="ap-live",
        requested_by="web:operator",
        status=SimpleNamespace(value="pending"),
        context={},
    )

    assert web_app._cleanup_dead_task_frames(runtime) == []
    assert store.get_task_frame("frame-live").status is TaskFrameStatus.WAITING_APPROVAL
    assert store.get_active_task_frame_id("web:operator") == "frame-live"
    assert store.doctor_actions == {}


def test_suspended_turn_config_feature_and_credentials_helpers(monkeypatch, tmp_path) -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    web_app._store_web_screenshot_suspended_turn(
        runtime,
        approval_id="approval-1",
        conversation_id="conv",
        user_text="screenshot https://example.com",
    )
    assert store.suspended[0].approval_id == "approval-1"
    assert runtime.checkpoints == 1

    monkeypatch.delenv("NULLION_MEMORY_ENABLED", raising=False)
    assert web_app._feature_enabled("NULLION_MEMORY_ENABLED") is True
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "off")
    assert web_app._feature_enabled("NULLION_MEMORY_ENABLED") is False
    web_app._remember_web_explicit_memory(runtime, user_message="remember this")

    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "false")
    assert web_app._resolve_browser_backend() is None
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "true")
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "cdp")
    assert web_app._resolve_browser_backend() == "cdp"
    monkeypatch.delenv("NULLION_BROWSER_BACKEND", raising=False)
    monkeypatch.setenv("NULLION_PLUGINS", "search,browser")
    assert web_app._resolve_browser_backend() == "auto"

    monkeypatch.delenv("NULLION_MODEL_PROVIDER", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert web_app._env_model_provider_configured() is False
    monkeypatch.setenv("NULLION_MODEL_PROVIDER", "openai")
    assert web_app._env_model_provider_configured() is True
    monkeypatch.setattr(web_app, "_build_openai_client", lambda: "openai-client")
    monkeypatch.setattr(web_app, "_build_anthropic_client", lambda: "anthropic-client")
    assert web_app._build_model_client_from_env() == "openai-client"
    monkeypatch.setenv("NULLION_MODEL_PROVIDER", "anthropic")
    assert web_app._build_model_client_from_env() == "anthropic-client"
    monkeypatch.delenv("NULLION_MODEL_PROVIDER", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        web_app._build_model_client_from_env()
    monkeypatch.setattr(sys, "argv", [])
    assert web_app._current_process_restart_command() == [sys.executable, "-m", "nullion.web_app"]
    monkeypatch.setattr(sys, "argv", ["-m", "nullion.web_app"])
    assert web_app._current_process_restart_command() == [sys.executable, "-m", "nullion.web_app"]
    monkeypatch.setattr(sys, "argv", ["nullion-web", "--port", "1"])
    assert web_app._current_process_restart_command() == [sys.executable, "nullion-web", "--port", "1"]
    assert web_app._codex_reauth_command() == [sys.executable, "-u", "-m", "nullion.auth", "--reauth", "codex"]

    creds_path = tmp_path / "credentials.json"
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", creds_path)
    web_app._write_credentials_json({"provider": "codex"})
    assert web_app._read_credentials_json() == {"provider": "codex"}
    creds_path.write_text("{bad", encoding="utf-8")
    assert web_app._read_credentials_json() == {}

    env_path = tmp_path / ".env"
    env_path.write_text("# comment\nA=\"old\"\nB=keep\n", encoding="utf-8")
    web_app._write_env_updates(env_path, {"A": "new", "C": "added"})
    assert env_path.read_text(encoding="utf-8") == '# comment\nA="new"\nB=keep\nC="added"\n'
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / "nested" / ".env"))
    assert web_app._find_env_path() == tmp_path / "nested" / ".env"
    monkeypatch.delenv("NULLION_ENV_FILE", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app.os.path, "expanduser", lambda path: str(tmp_path / path.removeprefix("~/")))
    assert web_app._find_env_path() == web_app.Path(".env")
    env_path.unlink()
    default_env = web_app._find_env_path()
    assert default_env == tmp_path / ".nullion" / ".env"
