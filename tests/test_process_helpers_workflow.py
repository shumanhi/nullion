from __future__ import annotations

import asyncio
import builtins
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from nullion.config import DiscordSettings, NullionSettings, SlackSettings, TelegramSettings
from nullion.reminders import ReminderRecord
from nullion.scheduler import ScheduleKind, ScheduledTask
from nullion.skills import SkillRecord
from nullion.tools import ToolInvocation
from nullion.users import MessagingDeliveryTarget


def test_skill_planner_builds_formats_and_advances_plans() -> None:
    from nullion import skill_planner
    from nullion.mini_agent_runs import MiniAgentRunStatus
    from nullion.progress import ProgressState

    skill = SkillRecord("skill-1", "Deploy", "Ship", "deploy", ["plan", "test"])
    plan = skill_planner.build_skill_execution_plan(skill)
    assert plan.step_states == ("in_progress", "pending")
    snapshot = skill_planner.build_skill_execution_plan_snapshot(plan)
    assert snapshot["active_step"] == "plan"
    assert skill_planner.build_skill_execution_intent_snapshot(plan)["side_effects_allowed"] is False
    assert "Progress: 0/2 complete" in skill_planner.render_skill_execution_plan_for_telegram(plan)

    advanced = skill_planner.transition_skill_execution_plan_for_mini_agent_status(plan, MiniAgentRunStatus.COMPLETED)
    assert advanced.step_states == ("completed", "in_progress")
    completed = skill_planner.transition_skill_execution_plan_for_step_completion(advanced)
    assert completed.active_step_index is None
    assert completed.step_states == ("completed", "completed")
    assert skill_planner.transition_skill_execution_plan_for_progress(plan, ProgressState.COMPLETED).active_step_index is None
    assert skill_planner.transition_skill_execution_plan_for_progress(plan, ProgressState.WORKING) is plan

    empty = skill_planner.build_skill_execution_plan(SkillRecord("empty", "Empty", "", "", []))
    assert "Progress: 0/0 complete" in skill_planner.format_skill_execution_plan_for_telegram(
        skill_planner.build_skill_execution_plan_snapshot(
            skill_planner.transition_skill_execution_plan_for_progress(empty, ProgressState.COMPLETED)
        )
    )


def test_reminder_delivery_marks_sent_only_after_success(monkeypatch) -> None:
    from nullion import reminder_delivery

    monkeypatch.setattr(
        reminder_delivery,
        "messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings: (),
    )

    now = datetime(2026, 1, 1, tzinfo=UTC)
    task_1 = ScheduledTask("rem-1", "cap", ScheduleKind.ONCE, 0, True, None, 0)
    task_2 = ScheduledTask("rem-2", "cap", ScheduleKind.ONCE, 0, True, None, 0)

    class Store:
        def __init__(self):
            self.reminders = {
                "rem-1": ReminderRecord("rem-1", "chat-1", "one", now),
                "rem-2": ReminderRecord("rem-2", "chat-2", "two", now),
            }
            self.saved_reminders = []
            self.saved_tasks = []

        def get_reminder(self, task_id):
            return self.reminders.get(task_id)

        def add_reminder(self, reminder):
            self.reminders[reminder.task_id] = reminder
            self.saved_reminders.append(reminder)

        def get_scheduled_task(self, task_id):
            for task in (task_1, task_2):
                if task.task_id == task_id:
                    return task
            return None

        def add_scheduled_task(self, task):
            self.saved_tasks.append(task)

    class Runtime:
        def __init__(self):
            self.store = Store()
            self.checkpoints = 0

        def run_due_scheduled_tasks(self, *, now):
            return [task_1, task_2]

        def checkpoint(self):
            self.checkpoints += 1

    runtime = Runtime()

    async def send(chat_id, text):
        return chat_id == "chat-1"

    delivered = asyncio.run(reminder_delivery.deliver_due_reminders_once(runtime, send=send, now=now))
    assert delivered == 1
    assert runtime.store.saved_reminders[0].delivered_at == now
    assert runtime.store.saved_tasks[0].enabled is False
    assert runtime.store.reminders["rem-1"].delivered_at == now
    assert runtime.store.reminders["rem-2"].delivered_at is None
    assert runtime.checkpoints == 3

    async def exploding_send(chat_id, text):
        raise RuntimeError("down")

    assert asyncio.run(reminder_delivery.deliver_due_reminders_once(runtime, send=exploding_send, now=now)) == 0
    assert runtime.store.reminders["rem-2"].delivered_at is None


def test_reminder_delivery_claims_before_send_to_block_concurrent_duplicates(monkeypatch) -> None:
    from nullion import reminder_delivery

    monkeypatch.setattr(
        reminder_delivery,
        "messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings: (),
    )

    now = datetime(2026, 1, 1, tzinfo=UTC)
    task = ScheduledTask("rem-1", "cap", ScheduleKind.ONCE, 0, True, None, 0)

    class Store:
        def __init__(self):
            self.reminders = {"rem-1": ReminderRecord("rem-1", "chat-1", "one", now)}
            self.scheduled_tasks = {"rem-1": task}

        def get_reminder(self, task_id):
            return self.reminders.get(task_id)

        def add_reminder(self, reminder):
            self.reminders[reminder.task_id] = reminder

        def get_scheduled_task(self, task_id):
            return self.scheduled_tasks.get(task_id)

        def add_scheduled_task(self, scheduled_task):
            self.scheduled_tasks[scheduled_task.task_id] = scheduled_task

    class Runtime:
        def __init__(self):
            self.store = Store()
            self.checkpoints = 0

        def run_due_scheduled_tasks(self, *, now):
            return [task]

        def checkpoint(self):
            self.checkpoints += 1

    runtime = Runtime()
    sent: list[tuple[str, str]] = []

    async def send(chat_id, text):
        sent.append((chat_id, text))
        await asyncio.sleep(0.05)
        return True

    async def run_two_delivery_loops():
        return await asyncio.gather(
            reminder_delivery.deliver_due_reminders_once(runtime, send=send, now=now),
            reminder_delivery.deliver_due_reminders_once(runtime, send=send, now=now),
        )

    delivered_counts = asyncio.run(run_two_delivery_loops())

    assert sorted(delivered_counts) == [0, 1]
    assert len(sent) == 1
    assert sent[0] == ("chat-1", "⏰ Reminder: one")
    assert runtime.store.reminders["rem-1"].delivered_at == now
    assert runtime.store.scheduled_tasks["rem-1"].enabled is False
    assert runtime.checkpoints == 1


def test_reminder_delivery_claims_across_runtime_process_snapshots(tmp_path, monkeypatch) -> None:
    from nullion import reminder_delivery
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setattr(
        reminder_delivery,
        "messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings: (),
    )

    now = datetime(2026, 1, 1, tzinfo=UTC)
    db_path = tmp_path / "runtime.db"
    owner_runtime = bootstrap_persistent_runtime(db_path)
    owner_runtime.schedule_reminder(chat_id="chat-1", text="one", due_at=now)
    owner_runtime.checkpoint(force=True)
    runtime_a = bootstrap_persistent_runtime(db_path)
    runtime_b = bootstrap_persistent_runtime(db_path)
    sent: list[tuple[str, str]] = []

    async def send(chat_id, text):
        sent.append((chat_id, text))
        await asyncio.sleep(0.05)
        return True

    async def run_two_delivery_processes():
        return await asyncio.gather(
            reminder_delivery.deliver_due_reminders_once(runtime_a, send=send, now=now),
            reminder_delivery.deliver_due_reminders_once(runtime_b, send=send, now=now),
        )

    delivered_counts = asyncio.run(run_two_delivery_processes())

    assert sorted(delivered_counts) == [0, 1]
    assert sent == [("chat-1", "⏰ Reminder: one")]
    reloaded = bootstrap_persistent_runtime(db_path)
    reminders = reloaded.store.list_reminders()
    assert len(reminders) == 1
    assert reminders[0].delivered_at == now


def test_reminder_delivery_fans_out_to_connected_workspace_apps(monkeypatch) -> None:
    from nullion import reminder_delivery

    now = datetime(2026, 1, 1, tzinfo=UTC)
    task = ScheduledTask("rem-1", "cap", ScheduleKind.ONCE, 0, True, None, 0)

    class Store:
        def __init__(self):
            self.reminders = {
                "rem-1": ReminderRecord("rem-1", "web:operator", "check menu", now),
            }
            self.saved_reminders = []
            self.saved_tasks = []

        def get_reminder(self, task_id):
            return self.reminders.get(task_id)

        def add_reminder(self, reminder):
            self.saved_reminders.append(reminder)

        def add_scheduled_task(self, scheduled_task):
            self.saved_tasks.append(scheduled_task)

    class Runtime:
        def __init__(self):
            self.store = Store()
            self.checkpoints = 0

        def run_due_scheduled_tasks(self, *, now):
            return [task]

        def checkpoint(self):
            self.checkpoints += 1

    targets = (
        MessagingDeliveryTarget("telegram", "T1", "telegram_chat", "workspace_admin", "admin", "Admin"),
        MessagingDeliveryTarget("slack", "S1", "telegram_chat", "workspace_admin", "admin", "Admin"),
        MessagingDeliveryTarget("discord", "D1", "telegram_chat", "workspace_admin", "admin", "Admin"),
    )
    sent: list[tuple[str, str]] = []

    monkeypatch.setattr(
        reminder_delivery,
        "messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings: targets,
    )

    async def fake_workspace_send(target, text, *, settings):  # noqa: ANN001
        sent.append((target.channel, target.target_id))
        return True

    monkeypatch.setattr(reminder_delivery, "_send_workspace_reminder_target", fake_workspace_send)

    async def send_origin(chat_id, text):
        sent.append(("origin", chat_id))
        return True

    settings = NullionSettings(
        telegram=TelegramSettings(bot_token="telegram-token"),
        slack=SlackSettings(bot_token="slack-token"),
        discord=DiscordSettings(bot_token="discord-token"),
    )
    runtime = Runtime()

    delivered = asyncio.run(
        reminder_delivery.deliver_due_reminders_once(runtime, send=send_origin, now=now, settings=settings)
    )

    assert delivered == 1
    assert ("telegram", "T1") in sent
    assert ("slack", "S1") in sent
    assert ("discord", "D1") in sent
    assert ("origin", "web:operator") in sent
    assert runtime.store.saved_reminders[0].delivered_at == now
    assert runtime.store.saved_tasks[0].enabled is False
    assert runtime.checkpoints == 1


def test_reminder_delivery_resolves_bare_telegram_chat_to_member_workspace(monkeypatch) -> None:
    from nullion import reminder_delivery

    now = datetime(2026, 1, 1, tzinfo=UTC)
    task = ScheduledTask("rem-1", "cap", ScheduleKind.ONCE, 0, True, None, 0)

    class Store:
        def __init__(self):
            self.reminders = {"rem-1": ReminderRecord("rem-1", "42", "member reminder", now)}
            self.saved_reminders = []
            self.saved_tasks = []

        def get_reminder(self, task_id):
            return self.reminders.get(task_id)

        def add_reminder(self, reminder):
            self.saved_reminders.append(reminder)

        def add_scheduled_task(self, scheduled_task):
            self.saved_tasks.append(scheduled_task)

    class Runtime:
        def __init__(self):
            self.store = Store()

        def run_due_scheduled_tasks(self, *, now):
            return [task]

        def checkpoint(self):
            pass

    monkeypatch.setattr(
        reminder_delivery,
        "resolve_telegram_user",
        lambda chat_id, settings: SimpleNamespace(workspace_id="workspace_member"),
    )

    def targets_for_workspace(workspace_id, *, settings):  # noqa: ANN001
        assert workspace_id == "workspace_member"
        return ()

    monkeypatch.setattr(reminder_delivery, "messaging_delivery_targets_for_workspace", targets_for_workspace)

    async def send(chat_id, text):
        return chat_id == "42"

    delivered = asyncio.run(
        reminder_delivery.deliver_due_reminders_once(
            Runtime(),
            send=send,
            now=now,
            settings=NullionSettings(telegram=TelegramSettings(bot_token="telegram-token")),
        )
    )

    assert delivered == 1


def test_service_control_restart_paths(monkeypatch, tmp_path) -> None:
    from nullion import service_control as services

    assert services.service_names(groups={"chat"}) == ["telegram", "slack", "discord"]
    assert services.service_for_name("WEB").display_name == "Web"
    with pytest.raises(ValueError):
        services.service_for_name("missing")

    monkeypatch.setattr(services, "systemd_unit_path", lambda unit: tmp_path / unit)
    monkeypatch.setattr(services, "systemd_available", lambda: True)
    monkeypatch.setattr(services, "launchd_available", lambda: False)
    monkeypatch.setattr(services, "windows_tasks_available", lambda: False)
    calls = []
    active_checks: dict[str, int] = {}

    def fake_run(args, *, timeout=15.0):
        calls.append(args)
        if args[:4] == ["systemctl", "--user", "is-active", "--quiet"]:
            unit = args[4]
            active_checks[unit] = active_checks.get(unit, 0) + 1
            return subprocess.CompletedProcess(args, 3 if active_checks[unit] % 2 == 1 else 0, stdout="", stderr="")
        return subprocess.CompletedProcess(args, 0, stdout="ok", stderr="")

    monkeypatch.setattr(services, "_run", fake_run)
    assert services.restart_managed_service("web", manager="systemd") == "Restarted the Web service (nullion.service)."

    monkeypatch.setattr(services, "_run", lambda args, *, timeout=15.0: subprocess.CompletedProcess(args, 1, stdout="", stderr="not found"))
    results = services.restart_managed_services(["web"], manager="systemd")
    assert results[0].ok is False
    assert "No managed service" in results[0].message

    monkeypatch.setattr(services, "_run", fake_run)
    results = services.restart_managed_services(["web", "telegram"], manager="systemd")
    assert services.successful_restart_message(results).startswith("Restarted the Web")


def test_cli_restart_falls_back_when_service_control_import_is_missing(monkeypatch) -> None:
    from nullion import cli

    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "nullion.service_control":
            raise ModuleNotFoundError(
                "No module named 'nullion.service_control'",
                name="nullion.service_control",
            )
        return original_import(name, globals, locals, fromlist, level)

    calls = []
    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.setattr(cli, "_restart_managed_services_legacy", lambda **kwargs: calls.append(kwargs))

    cli._restart_all_managed_services(manager="launchd")
    cli._restart_launchd_chat_services()
    cli._restart_systemd_chat_services()

    assert calls == [
        {"manager": "launchd"},
        {"manager": "launchd", "names": ("telegram", "slack", "discord")},
        {"manager": "systemd", "names": ("telegram", "slack", "discord")},
    ]


def test_cli_legacy_restart_uses_launchd_plist_labels(monkeypatch, tmp_path, capsys) -> None:
    from nullion import cli

    launch_agents = tmp_path / "Library" / "LaunchAgents"
    launch_agents.mkdir(parents=True)
    (launch_agents / "com.nullion.web.plist").write_text("", encoding="utf-8")

    calls = []

    def fake_run(args, **kwargs):  # noqa: ANN001, ARG001
        calls.append(args)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(cli.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(cli.os, "getuid", lambda: 501)
    monkeypatch.setattr(subprocess, "run", fake_run)

    cli._restart_managed_services_legacy(manager="launchd", names=("web",))

    assert calls == [
        ["launchctl", "bootout", "gui/501/com.nullion.web"],
        ["launchctl", "kickstart", "-k", "gui/501/com.nullion.web"],
    ]
    assert "Service helper unavailable; using legacy restart path." in capsys.readouterr().out


def test_detect_service_manager_prefers_windows_on_nt(monkeypatch) -> None:
    from nullion import cli

    monkeypatch.setattr(cli.os, "name", "nt")
    monkeypatch.setattr(cli.sys, "platform", "win32")

    assert cli._detect_service_manager() == "windows"


def test_service_cmd_windows_stop_and_status_use_schtasks(monkeypatch, capsys) -> None:
    from nullion import cli

    calls: list[list[str]] = []
    stopped: list[str] = []

    def fake_run(args, **kwargs):  # noqa: ANN001, ARG001
        calls.append(list(args))
        if len(args) >= 4 and args[0] == "schtasks" and args[1] == "/Query" and args[2] == "/TN":
            task_name = args[3]
            if task_name in {"Nullion Web Dashboard", "Nullion Telegram"}:
                return subprocess.CompletedProcess(args, 0, stdout="ok", stderr="")
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="missing")
        return subprocess.CompletedProcess(args, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(cli, "_stop_tray_service", lambda: stopped.append("tray"))

    cli._service_cmd("windows", "stop")
    cli._service_cmd("windows", "status")

    out = capsys.readouterr().out
    assert "Could not detect service manager" not in out
    assert "Stopped." in out
    assert stopped == ["tray"]
    assert any(cmd[:4] == ["schtasks", "/End", "/TN", "Nullion Web Dashboard"] for cmd in calls)
    assert any(cmd[:4] == ["schtasks", "/End", "/TN", "Nullion Telegram"] for cmd in calls)
    assert any(cmd[:6] == ["schtasks", "/Query", "/TN", "Nullion Web Dashboard", "/FO", "LIST"] for cmd in calls)


def test_install_tray_windows_task_access_denied_reports_autostart_warning(monkeypatch, tmp_path, capsys) -> None:
    from nullion import cli

    calls: list[list[str]] = []

    def fake_run(args, **kwargs):  # noqa: ANN001, ARG001
        calls.append(list(args))
        if "/IT" in args:
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="ERROR: Access is denied.")
        return subprocess.CompletedProcess(args, 1, stdout="", stderr="ERROR: Access is denied.")

    monkeypatch.setattr(subprocess, "run", fake_run)

    installed = cli._install_tray_windows_task(
        exe=tmp_path / "nullion-tray.exe",
        port=8742,
        env_file=tmp_path / ".env",
        home=tmp_path,
        log_dir=tmp_path / "logs",
    )

    out = capsys.readouterr().out
    assert installed is False
    assert "Task Scheduler interactive mode was refused" in out
    assert "Tray autostart task could not be installed" in out
    assert "Continuing without Windows autostart" in out
    assert any("/IT" in cmd for cmd in calls)


def test_approval_context_labels_and_tool_boundaries(tmp_path) -> None:
    from nullion import approval_context, tool_boundaries

    context = approval_context.build_trigger_flow_context(
        principal_id="slack:U1",
        invocation_id="invocation-abcdef",
        capsule_id="capsule-123456789",
    )
    flow = approval_context.approval_trigger_flow({approval_context.FLOW_TRIGGER_CONTEXT_KEY: context})
    assert flow["label"] == "Slack chat"
    assert approval_context.approval_trigger_flow_label({approval_context.FLOW_TRIGGER_CONTEXT_KEY: context}) == (
        "Slack chat (slack:U1 · capsule capsule- · call invocation-a)"
    )
    assert approval_context.approval_trigger_flow({}) is None

    facts = tool_boundaries.extract_boundary_facts(
        ToolInvocation("inv", "terminal_exec", "operator", {"command": "curl 'https://example.com/path'"})
    )
    assert facts[0].attributes["command_family"] == "curl"
    assert facts[0].attributes["address_class"] == "public"
    powershell_facts = tool_boundaries.extract_boundary_facts(
        ToolInvocation(
            "inv",
            "terminal_exec",
            "operator",
            {"command": "pwsh -NoProfile -Command Invoke-WebRequest https://example.com/path"},
        )
    )
    assert powershell_facts[0].attributes["command_family"] == "invoke-webrequest"
    assert powershell_facts[0].target == "https://example.com/path"
    assert tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "terminal_exec", "operator", {"command": "echo hi"})) == []
    assert tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "web_search", "operator", {}))[0].operation == "search"
    assert tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "file_write", "operator", {"path": "/tmp/a"}))[0].operation == "write"
    source_image = tmp_path / "in.png"
    source_image.write_bytes(b"source")
    assert len(tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "image_generate", "operator", {"source_path": str(source_image), "output_path": "/tmp/out.png"}))) == 2
    assert tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "email_send", "operator", {}))[0].kind.value == "account_access"
    assert tool_boundaries.extract_boundary_facts(
        ToolInvocation(
            "inv",
            "calendar_update",
            "operator",
            {"provider_id": "custom_connector_provider", "calendar_id": "primary", "event_id": "evt-1"},
        )
    )[0].target == "custom_connector_provider:primary:write:evt-1"
    assert tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "plugin:acme", "operator", {}))[0].attributes["account_type"] == "acme"
    assert tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "connector_request", "operator", {"url": "https://api.example", "provider_id": "acme"}))[1].target == "acme"
    assert tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "connector_request", "operator", {"url": "https://api.example"})) == []
    assert tool_boundaries.extract_boundary_facts(ToolInvocation("inv", "connector_request", "operator", {"provider_id": "acme"})) == []
