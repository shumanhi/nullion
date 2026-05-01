from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion import discord_app, recovery, slack_app, tray_app
from nullion.messaging_adapters import MessagingAdapterConfigurationError


class AsyncContext:
    def __init__(self, *, enter_error: Exception | None = None, exit_error: Exception | None = None) -> None:
        self.enter_error = enter_error
        self.exit_error = exit_error
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        if self.enter_error:
            raise self.enter_error
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True
        if self.exit_error:
            raise self.exit_error
        return False


class AsyncResponse:
    def __init__(self, content: bytes = b"data", content_type: str = "text/plain") -> None:
        self.content = content
        self.headers = {"content-type": content_type}

    def raise_for_status(self) -> None:
        return None


def test_recovery_helpers_config_commands_and_cli(tmp_path, monkeypatch, capsys) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n# ignored\nexport NULLION_TELEGRAM_BOT_TOKEN='bot'\nKEEP=existing\nNEW_VALUE=\"fresh\"\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("KEEP", "present")
    recovery._load_env_file(env_file)
    assert recovery.os.environ["KEEP"] == "present"
    assert recovery.os.environ["NEW_VALUE"] == "fresh"

    assert recovery._parse_launchctl_print("last exit code = 9\nruns = 3") == (9, 3)
    assert recovery._parse_launchctl_print("last exit code = nope\nruns = bad") == (None, None)
    monkeypatch.setattr(recovery.shutil, "which", lambda name: "/usr/bin/pgrep" if name == "pgrep" else None)
    monkeypatch.setattr(recovery, "_run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="123 nullion-web"))
    assert recovery._process_running("nullion-web") is True
    monkeypatch.setattr(recovery, "_http_json", lambda url, timeout=2.0: {"status": "ok"})
    assert recovery.web_health()["ok"] is True

    monkeypatch.setattr(recovery, "SERVICE_LABELS", {"web": ("ai.nullion.web",), "telegram": ("ai.nullion.telegram",)})
    monkeypatch.setattr(recovery, "SERVICE_COMMAND_HINTS", {"web": "nullion-web", "telegram": "nullion-telegram"})
    monkeypatch.setattr(recovery, "_launchctl_available", lambda: False)
    assert recovery.service_status("web").to_dict()["running"] is True
    with pytest.raises(ValueError):
        recovery.service_status("missing")

    data_dir = tmp_path / "data"
    backup_dir = tmp_path / "backups"
    data_dir.mkdir()
    (data_dir / ".env").write_text("token", encoding="utf-8")
    monkeypatch.setattr(recovery.time, "strftime", lambda fmt: "20260101-010203")
    destination = recovery.snapshot_config(data_dir=data_dir, backup_dir=backup_dir)
    assert destination.name == "20260101-010203"
    assert recovery.list_config_backups(backup_dir=backup_dir)[0]["files"] == [".env"]
    (data_dir / ".env").unlink()
    assert "Restored config backup" in recovery.restore_config_backup("latest", data_dir=data_dir, backup_dir=backup_dir)
    with pytest.raises(FileNotFoundError):
        recovery.snapshot_config(data_dir=tmp_path / "empty", backup_dir=tmp_path / "empty-backups")
    with pytest.raises(FileNotFoundError):
        recovery.restore_config_backup("missing", data_dir=data_dir, backup_dir=backup_dir)

    monkeypatch.setattr(recovery, "all_service_statuses", lambda: [{"name": "web", "state": "missing", "detail": "down", "running": False, "restartable": True}])
    monkeypatch.setattr(recovery, "runtime_backups", lambda checkpoint=recovery.DEFAULT_CHECKPOINT: [{"path": str(env_file)}])
    monkeypatch.setattr(recovery, "list_config_backups", lambda **kwargs: [{"name": "latest", "path": str(destination), "files": [".env"]}])
    monkeypatch.setattr(recovery, "restart_services", lambda names: [{"service": "web", "ok": "true", "message": "restarted"}])
    monkeypatch.setattr(recovery, "restore_runtime_backup", lambda **kwargs: "runtime restored")
    monkeypatch.setattr(recovery, "restore_config_backup", lambda name="latest", **kwargs: f"config restored {name}")
    monkeypatch.setattr(
        recovery,
        "recovery_status",
        lambda **kwargs: {
            "web": {"ok": False, "detail": "down"},
            "services": [{"name": "web", "running": False, "restartable": True}],
            "runtime": {"backups": [1, 2]},
            "config": {"backups": [1], "telegram_token_present": False},
        },
    )
    assert "Web: down" in recovery.handle_recovery_command("/status")
    assert recovery.handle_recovery_command("/services") == "web: missing - down"
    assert recovery.handle_recovery_command("/restart web") == "restarted"
    assert recovery.handle_recovery_command("/backups").startswith("Runtime backups: 1")
    assert recovery.handle_recovery_command("/restore runtime 1") == "runtime restored"
    assert recovery.handle_recovery_command("/restore config latest") == "config restored latest"
    assert recovery.handle_recovery_command("/nope").startswith("Unknown recovery command")
    monkeypatch.setattr(recovery, "restart_services", lambda names: (_ for _ in ()).throw(RuntimeError("boom")))
    assert recovery.handle_recovery_command("/restart web").startswith("Recovery command failed:")
    monkeypatch.setattr(recovery, "restart_services", lambda names: [{"service": "web", "ok": "true", "message": "restarted"}])

    monkeypatch.setattr(recovery, "_load_env_file", lambda path: None)
    monkeypatch.setattr(recovery, "_print_json", lambda data: print(f"JSON:{type(data).__name__}"))
    assert recovery._cli_impl(["--checkpoint", str(tmp_path / "runtime.json"), "status"]) == 0
    assert "JSON:dict" in capsys.readouterr().out
    assert recovery._cli_impl(["services"]) == 0
    assert recovery._cli_impl(["restart", "web"]) == 0
    assert recovery._cli_impl(["backups"]) == 0
    assert recovery._cli_impl(["restore", "runtime", "2"]) == 0
    assert recovery._cli_impl(["restore", "config", "latest"]) == 0


def test_tray_helpers_and_actions(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("export A='1'\nB=two\nA=ignored\n", encoding="utf-8")
    monkeypatch.delenv("A", raising=False)
    tray_app._load_env(str(env_file))
    assert tray_app.os.environ["A"] == "1"
    assert tray_app.os.environ["B"] == "two"

    assert tray_app._web_base_url("0.0.0.0", 8742) == "http://127.0.0.1:8742"
    assert tray_app._web_base_url("::1", 8742) == "http://[::1]:8742"
    assert tray_app._short_status(None) == "Offline"
    assert tray_app._short_status({"status": "ok", "packages": []}) == "Running"
    assert tray_app._short_status({"summary": {"pending_approvals": 2}}) == "Running, 2 approvals waiting"
    assert tray_app._short_status({"summary": {"running_missions": 1}}) == "Running, 1 active task"
    assert tray_app._build_icon_image(True, size=16).size == (16, 16)

    opened: list[str] = []
    monkeypatch.setattr(tray_app.webbrowser, "open", lambda url: opened.append(url))
    monkeypatch.setattr(tray_app, "_open_path", lambda path: opened.append(str(path)))
    monkeypatch.setattr(tray_app, "_LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(tray_app, "_ENV_FILE", tmp_path / "home" / ".env")
    tray = tray_app.NullionTray(host="127.0.0.1", port=8742, poll_interval=0.01)
    tray.open_browser()
    tray.open_logs()
    tray.open_config()
    assert "http://127.0.0.1:8742" in opened
    assert any(item.endswith(".env") for item in opened)

    popens: list[list[str]] = []
    monkeypatch.setattr(tray_app.subprocess, "Popen", lambda cmd, **kwargs: popens.append(cmd) or SimpleNamespace(poll=lambda: 0, pid=42))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    tray._open_webview("#approvals")
    assert "--path" in popens[-1]
    assert "--env-file" in popens[-1]

    messages: list[str] = []
    tray.notify = lambda message: messages.append(message)  # type: ignore[method-assign]
    tray.refresh_status = lambda: messages.append("refresh")  # type: ignore[method-assign]
    monkeypatch.setattr(tray_app, "_request_json", lambda *args, **kwargs: {"message": "restarted chat"})
    monkeypatch.setattr(tray_app.threading, "Thread", lambda target, daemon=True: SimpleNamespace(start=lambda: target()))
    monkeypatch.setattr(tray_app.time, "sleep", lambda seconds: None)
    tray.restart_nullion()
    tray.restart_chat_services()
    assert "Restart requested." in messages
    assert "restarted chat" in messages

    monkeypatch.setattr(tray_app, "_request_json", lambda *args, **kwargs: (_ for _ in ()).throw(tray_app.urllib.error.URLError("down")))
    tray._run_nullion_shortcut = lambda flag: messages.append(flag)  # type: ignore[method-assign]
    tray.restart_nullion()
    assert "--restart" in messages
    tray.quit()
    assert tray._stop.is_set()


@pytest.mark.asyncio
async def test_slack_message_command_delivery_and_download(tmp_path, monkeypatch) -> None:
    assert slack_app._optional_event_text("  hi ") == "hi"
    assert slack_app._optional_event_text(" ") is None
    assert slack_app._normalize_slack_text("<@U1|Ada> visit <#C1|general> <!here> <https://example.com|site> *bold*") == "Ada visit #general  site bold"
    assert slack_app._format_slack_reply("See [docs](https://example.com) and **bold**") == "See <https://example.com|docs> and *bold*"
    assert slack_app._slack_response_field({"ts": "1"}, "ts") == "1"
    assert slack_app._slack_response_field(SimpleNamespace(ts="2"), "ts") == "2"
    assert slack_app._nullion_slack_command_text({"text": "status"}) == "/status"
    assert slack_app._nullion_slack_command_text({}) == "/help"

    for settings in [
        SimpleNamespace(slack=SimpleNamespace(enabled=False, bot_token="", app_token="")),
        SimpleNamespace(slack=SimpleNamespace(enabled=True, bot_token="", app_token="")),
        SimpleNamespace(slack=SimpleNamespace(enabled=True, bot_token="bot", app_token="")),
    ]:
        with pytest.raises(MessagingAdapterConfigurationError):
            slack_app._require_slack_settings(settings)
    assert slack_app._require_slack_settings(SimpleNamespace(slack=SimpleNamespace(enabled=True, bot_token="bot", app_token="app"))) == ("bot", "app")

    updates: list[tuple[str, str, str]] = []

    class SlackClient:
        def __init__(self) -> None:
            self.uploads: list[str] = []

        async def chat_update(self, *, channel, ts, text):
            updates.append((channel, ts, text))

        async def files_upload_v2(self, **kwargs):
            self.uploads.append(kwargs["filename"])

    client = SlackClient()
    assert await slack_app._update_slack_message(client, channel="C1", ts="1", text="done") is True
    assert await slack_app._update_slack_message(None, channel="C1", ts="1", text="done") is False
    file_path = tmp_path / "reply.txt"
    file_path.write_text("reply", encoding="utf-8")
    assert await slack_app._upload_slack_reply_files(client, channel="C1", paths=(file_path,), initial_comment="here") is True
    assert client.uploads == ["reply.txt"]

    class FormattingFailureSlackClient:
        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def chat_postMessage(self, **kwargs):
            self.messages.append(kwargs)
            if len(self.messages) == 1:
                raise RuntimeError("formatted send failed")

    formatting_failure_client = FormattingFailureSlackClient()
    await slack_app._post_slack_message_with_plain_fallback(
        formatting_failure_client,
        channel="C1",
        formatted_text="Hello *Ada*",
        plain_text="Hello **Ada**",
    )
    assert formatting_failure_client.messages[0]["text"] == "Hello *Ada*"
    assert formatting_failure_client.messages[1]["mrkdwn"] is False
    assert "```text\nHello **Ada**\n```" in formatting_failure_client.messages[1]["text"]

    settings = SimpleNamespace(slack=SimpleNamespace(bot_token="bot"))
    async def no_slack_attachments(*args, **kwargs):
        return ()

    monkeypatch.setattr(slack_app, "_download_slack_attachments", no_slack_attachments)
    monkeypatch.setattr(slack_app, "require_authorized_ingress", lambda ingress, settings: True)
    expected_contract = SimpleNamespace(requires_attachment_delivery=False)
    monkeypatch.setattr(
        slack_app,
        "handle_messaging_ingress_result",
        lambda service, ingress: SimpleNamespace(reply="Hello **Ada**", delivery_contract=expected_contract),
    )
    monkeypatch.setattr(slack_app, "principal_id_for_messaging_identity", lambda *args, **kwargs: "user:u1")
    monkeypatch.setattr(slack_app, "record_platform_delivery_receipt", lambda *args, **kwargs: True)
    prepare_calls: list[dict] = []
    monkeypatch.setattr(slack_app, "split_reply_for_platform", lambda text, limit: [text, "tail"])
    monkeypatch.setattr(
        slack_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, **kwargs: prepare_calls.append({"reply": reply, **kwargs}) or SimpleNamespace(text=reply, attachments=()),
    )
    said: list[str] = []

    async def say(text):
        said.append(text)
        return {"ts": "work", "channel": "C1"}

    await slack_app.handle_slack_message(object(), settings, event={"text": "<@U1|Ada> **hi**", "user": "U1", "channel": "C1", "event_ts": "e1"}, say=say, client=client)
    assert said[0] == "Working..."
    assert updates[-1] == ("C1", "work", "Hello *Ada*")
    assert "tail" in said
    assert prepare_calls[-1]["delivery_contract"] is expected_contract

    responses: list[str] = []
    async def respond(text):
        responses.append(text)

    await slack_app.handle_slack_command(object(), settings, command={"user_id": "U1", "text": "status", "channel_id": "C1"}, respond=respond)
    assert responses == ["Hello *Ada*", "tail"]

    monkeypatch.setattr(slack_app, "require_authorized_ingress", lambda ingress, settings: False)
    denied: list[str] = []
    async def deny_say(text):
        denied.append(text)

    await slack_app.handle_slack_message(object(), settings, event={"text": "hi", "user": "U1"}, say=deny_say, client=None)
    assert denied == ["Unauthorized messaging identity."]


@pytest.mark.asyncio
async def test_discord_message_delivery_typing_and_download(tmp_path, monkeypatch) -> None:
    assert discord_app._optional_message_text("  hi ") == "hi"
    assert discord_app._optional_message_text("") is None
    with pytest.raises(MessagingAdapterConfigurationError):
        discord_app._require_discord_settings(SimpleNamespace(discord=SimpleNamespace(enabled=False, bot_token="")))
    with pytest.raises(MessagingAdapterConfigurationError):
        discord_app._require_discord_settings(SimpleNamespace(discord=SimpleNamespace(enabled=True, bot_token="")))
    assert discord_app._require_discord_settings(SimpleNamespace(discord=SimpleNamespace(enabled=True, bot_token="bot"))) == "bot"

    sent: list[dict] = []

    class Channel:
        id = "C1"

        def typing(self):
            return AsyncContext()

        async def send(self, *args, **kwargs):
            if args:
                kwargs["content"] = args[0]
            sent.append(kwargs)

    async with discord_app._discord_typing(Channel()):
        pass

    class TriggerChannel:
        id = "C2"

        async def trigger_typing(self):
            sent.append({"typing": True})

    async with discord_app._discord_typing(TriggerChannel()):
        pass
    assert {"typing": True} in sent

    reply_path = tmp_path / "reply.txt"
    reply_path.write_text("reply", encoding="utf-8")
    assert await discord_app._send_discord_reply_files(Channel(), text="here", paths=(reply_path,)) is True
    assert sent[-1]["content"] == "here"

    class FormattingFailureChannel:
        id = "C3"

        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def send(self, *args, **kwargs):
            if args:
                kwargs["content"] = args[0]
            self.messages.append(kwargs)
            if len(self.messages) == 1:
                raise RuntimeError("formatted send failed")

    formatting_failure_channel = FormattingFailureChannel()
    await discord_app._send_discord_chunks_with_plain_fallback(formatting_failure_channel, "Hello **Ada**")
    assert formatting_failure_channel.messages[0]["content"] == "Hello **Ada**"
    assert "```text\nHello **Ada**\n```" in formatting_failure_channel.messages[1]["content"]

    settings = SimpleNamespace(discord=SimpleNamespace(bot_token="bot"))
    original_download_discord_attachments = discord_app._download_discord_attachments

    async def no_discord_attachments(*args, **kwargs):
        return ()

    monkeypatch.setattr(discord_app, "_download_discord_attachments", no_discord_attachments)
    monkeypatch.setattr(discord_app, "require_authorized_ingress", lambda ingress, settings: True)
    expected_contract = SimpleNamespace(requires_attachment_delivery=False)
    monkeypatch.setattr(
        discord_app,
        "handle_messaging_ingress_result",
        lambda service, ingress: SimpleNamespace(reply="hello", delivery_contract=expected_contract),
    )
    monkeypatch.setattr(discord_app, "principal_id_for_messaging_identity", lambda *args, **kwargs: "user:u1")
    monkeypatch.setattr(discord_app, "record_platform_delivery_receipt", lambda *args, **kwargs: True)
    prepare_calls: list[dict] = []
    monkeypatch.setattr(
        discord_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, **kwargs: prepare_calls.append({"reply": reply, **kwargs}) or SimpleNamespace(text=reply, attachments=()),
    )
    monkeypatch.setattr(discord_app, "split_reply_for_platform", lambda text, limit: [text, "tail"])
    message = SimpleNamespace(author=SimpleNamespace(bot=False, id="U1"), content="hi", id="M1", channel=Channel(), attachments=[])
    await discord_app.handle_discord_message(object(), settings, message)
    assert {"content": "hello"} in sent
    assert {"content": "tail"} in sent
    assert prepare_calls[-1]["delivery_contract"] is expected_contract

    monkeypatch.setattr(discord_app, "require_authorized_ingress", lambda ingress, settings: False)
    await discord_app.handle_discord_message(object(), settings, message)
    assert {"content": "Unauthorized messaging identity."} in sent

    monkeypatch.setattr(discord_app, "is_supported_chat_file", lambda **kwargs: True)
    monkeypatch.setattr(discord_app, "resolve_messaging_user", lambda *args, **kwargs: SimpleNamespace(role="member", user_id="u1"))
    monkeypatch.setattr(discord_app, "save_messaging_attachment", lambda **kwargs: {"path": "/tmp/file"})

    class HttpClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            return AsyncResponse(b"payload")

    monkeypatch.setitem(sys.modules, "httpx", SimpleNamespace(AsyncClient=lambda **kwargs: HttpClient()))
    attachment = SimpleNamespace(content_type="text/plain", filename="note.txt", url="https://example.com/note.txt")
    downloaded = await original_download_discord_attachments(SimpleNamespace(author=SimpleNamespace(id="U1"), attachments=[attachment]), settings=settings)
    assert downloaded == ({"path": "/tmp/file"},)
