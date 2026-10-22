from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from nullion.approvals import (
    TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND,
    TERMINAL_DESTRUCTIVE_PERMISSION_PREFIX,
    create_boundary_permit,
    create_permission_grant,
)
from nullion.policy import BoundaryKind, OPERATOR_PERMISSION_PRINCIPAL
from nullion.runtime_store import RuntimeStore
from nullion.tools import (
    TerminalBackendDescriptor,
    TerminalExecutionPolicy,
    TerminalExecutionResult,
    ToolExecutor,
    ToolInvocation,
    ToolRegistry,
    ToolRiskLevel,
    ToolSideEffectClass,
    ToolSpec,
    _WebFetchResolution,
    create_core_tool_registry,
)
from nullion.workspace_storage import format_workspace_storage_for_prompt, workspace_storage_roots_for_principal


class FakeTerminalBackend:
    def __init__(self, *, stdout: str = "terminal ok\n", stderr: str = "") -> None:
        self.calls: list[tuple[str, str | None, int, TerminalExecutionPolicy]] = []
        self.stdout = stdout
        self.stderr = stderr

    def describe(self) -> TerminalBackendDescriptor:
        return TerminalBackendDescriptor(mode="test", attested_capabilities=(), metadata={})

    def run(
        self,
        command: str,
        *,
        cwd: str | None,
        timeout: int,
        policy: TerminalExecutionPolicy,
    ) -> TerminalExecutionResult:
        self.calls.append((command, cwd, timeout, policy))
        return TerminalExecutionResult(exit_code=0, stdout=self.stdout, stderr=self.stderr)


class FakeHeaders:
    def get_content_type(self) -> str:
        return "text/html"


class FakeResponse:
    status = 200

    def __init__(self) -> None:
        self.headers = FakeHeaders()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self, size: int) -> bytes:
        return b"<!doctype html><title>Allowed core fetch</title><p>fetch ok</p>"


class FakeOpener:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def open(self, request, timeout: int):  # noqa: ANN001
        self.urls.append(request.full_url)
        return FakeResponse()


def _invoke(
    executor: ToolExecutor,
    invocation_id: str,
    tool_name: str,
    args: dict[str, object],
    *,
    principal_id: str = "web:e2e",
):
    return executor.invoke(ToolInvocation(invocation_id, tool_name, principal_id, args))


def test_email_send_approval_required_result_includes_exact_draft_arguments() -> None:
    store = RuntimeStore()
    registry = ToolRegistry()
    calls: list[ToolInvocation] = []

    registry.register(
        ToolSpec(
            name="email_send",
            description="Send email",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
            timeout_seconds=10,
        ),
        lambda invocation: calls.append(invocation) or None,  # pragma: no cover - approval gate should stop first.
    )
    executor = ToolExecutor(store=store, registry=registry)

    arguments = {
        "to": ["recipient@example.com"],
        "subject": "Stage QA",
        "body": "Test body",
        "attachment_paths": ["/tmp/stage.csv", "/tmp/stage.xlsx"],
    }
    result = executor.invoke(ToolInvocation("email-approval", "email_send", "web:stage", arguments))

    assert result.status == "denied"
    assert result.output["reason"] == "approval_required"
    assert result.output["requires_approval"] is True
    assert result.output["tool_arguments"] == arguments
    assert result.output["tool_name"] == "email_send"
    assert calls == []

    approvals = store.list_approval_requests()
    assert len(approvals) == 1
    approval = approvals[0]
    assert approval.approval_id == result.output["approval_id"]
    assert approval.resource == "email_send"
    assert approval.context["tool_arguments"] == arguments
    assert approval.context["tool_side_effect_class"] == "account_write"


def test_workspace_storage_prompt_exposes_internal_scratch_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspace-storage"))

    roots = workspace_storage_roots_for_principal("telegram:100001")
    prompt = format_workspace_storage_for_prompt(principal_id="telegram:100001")

    assert str(roots.scratch) in prompt
    assert "internal scratch/helper scripts" in prompt
    assert str(roots.artifacts) in prompt


def test_scheduled_task_terminal_tmp_write_is_denied_without_approval(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    terminal_backend = FakeTerminalBackend()
    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspace-storage"))

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)

    result = executor.invoke(
        ToolInvocation(
            "cron-tmp-write",
            "terminal_exec",
            "telegram:100001",
            {"command": "printf report > /tmp/build_nullion_report.py"},
            flow_context={
                "scheduled_task_run": True,
                "cron_id": "5b4505db",
                "cron_name": "Nullion social + GitHub traffic monitor no empty graphs",
            },
        )
    )

    assert result.status == "denied"
    assert result.output["reason"] == "scheduled_task_workspace_path_required"
    assert result.output["requires_approval"] is False
    assert result.output["retryable"] is True
    assert result.output["target"] == "/tmp/build_nullion_report.py"
    assert str(workspace) in result.output["allowed_roots"]
    assert result.output["suggested_scratch_root"].endswith("/scratch")
    assert "artifact" not in result.output["message"].lower()
    assert store.list_approval_requests() == []
    assert terminal_backend.calls == []


def test_scheduled_task_terminal_helper_script_under_artifacts_is_denied_without_approval(
    monkeypatch,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspace-storage"))
    terminal_backend = FakeTerminalBackend()
    roots = workspace_storage_roots_for_principal("telegram:100001")

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)

    helper_path = roots.artifacts / "build_nullion_report.py"
    result = executor.invoke(
        ToolInvocation(
            "cron-artifact-helper-write",
            "terminal_exec",
            "telegram:100001",
            {"command": f"printf report > {helper_path}"},
            flow_context={
                "scheduled_task_run": True,
                "cron_id": "5b4505db",
            },
        )
    )

    assert result.status == "denied"
    assert result.output["reason"] == "scheduled_task_helper_script_scratch_path_required"
    assert result.output["requires_approval"] is False
    assert result.output["retryable"] is True
    assert result.output["target"] == str(helper_path)
    assert result.output["suggested_scratch_root"] == str(roots.scratch)
    assert result.output["blocked_delivery_root"] == str(roots.artifacts)
    assert store.list_approval_requests() == []
    assert terminal_backend.calls == []


def test_interactive_terminal_tmp_write_still_requests_approval(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    terminal_backend = FakeTerminalBackend()
    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)

    result = executor.invoke(
        ToolInvocation(
            "interactive-tmp-write",
            "terminal_exec",
            "telegram:100001",
            {"command": "printf report > /tmp/build_nullion_report.py"},
        )
    )

    assert result.status == "denied"
    assert result.output["reason"] == "approval_required"
    assert result.output["requires_approval"] is True
    assert result.output["approval_id"]
    assert len(store.list_approval_requests()) == 1
    assert terminal_backend.calls == []


def test_core_tools_run_directly_or_after_permission_grant(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    terminal_backend = FakeTerminalBackend()
    fake_opener = FakeOpener()

    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_CONNECTOR_ACCESS_ENABLED", "0")
    monkeypatch.setenv("NULLION_SKILL_PACK_ACCESS_ENABLED", "0")
    monkeypatch.setattr(
        "nullion.tools._resolve_web_fetch_resolution",
        lambda url: _WebFetchResolution(
            host="core-tools.test",
            address_infos=((0, 0, 0, "", ("93.184.216.34", 80)),),
        )
        if url == "https://core-tools.test/report"
        else None,
    )
    monkeypatch.setattr("nullion.tools._build_web_fetch_opener", lambda: fake_opener)

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
        direct_web_fetch_enabled=True,
    )
    executor = ToolExecutor(store=store, registry=registry)

    expected_core_tools = {
        "document_create",
        "file_read",
        "file_write",
        "pdf_create",
        "pdf_edit",
        "presentation_create",
        "terminal_exec",
        "web_fetch",
    }
    assert expected_core_tools <= {spec.name for spec in registry.list_specs()}

    note_path = workspace / "note.txt"
    written = _invoke(executor, "write-note", "file_write", {"path": str(note_path), "content": "core tool ok"})
    assert written.status == "completed"
    assert note_path.read_text(encoding="utf-8") == "core tool ok"

    read = _invoke(executor, "read-note", "file_read", {"path": str(note_path)})
    assert read.status == "completed"
    assert read.output["content"] == "core tool ok"

    created_pdf = _invoke(
        executor,
        "create-pdf",
        "pdf_create",
        {
            "title": "Core Tool Matrix",
            "text_pages": ["core pdf page"],
            "output_path": str(workspace / "core.pdf"),
        },
    )
    assert created_pdf.status == "completed"
    assert Path(str(created_pdf.output["path"])).is_file()

    edited_pdf = _invoke(
        executor,
        "edit-pdf",
        "pdf_edit",
        {
            "input_path": str(created_pdf.output["path"]),
            "append_text_pages": ["approved edit"],
            "output_path": str(workspace / "core-edited.pdf"),
        },
    )
    assert edited_pdf.status == "completed"
    assert Path(str(edited_pdf.output["path"])).is_file()
    assert edited_pdf.output["page_count"] == 2

    created_docx = _invoke(
        executor,
        "create-docx",
        "document_create",
        {
            "title": "Core Document",
            "paragraphs": ["core document paragraph"],
            "output_path": str(workspace / "core.docx"),
        },
    )
    if created_docx.status == "failed":
        assert created_docx.output["reason"] == "missing_dependency"
        assert created_docx.output["dependency_id"] == "python-docx"
    else:
        assert created_docx.status == "completed"
        assert Path(str(created_docx.output["path"])).is_file()

    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    outside_read = _invoke(executor, "outside-read", "file_read", {"path": str(outside)})
    assert outside_read.status == "denied"
    assert outside_read.output["reason"] == "approval_required"

    nonexistent_read = _invoke(executor, "nonexistent-read", "file_read", {"path": str(tmp_path / "missing.txt")})
    assert nonexistent_read.status == "denied"
    assert nonexistent_read.output["reason"] == "approval_required"

    terminal_before = _invoke(executor, "terminal-before", "terminal_exec", {"command": "printf core"})
    assert terminal_before.status == "denied"
    assert terminal_before.output["reason"] == "approval_required"
    terminal_approval_id = str(terminal_before.output["approval_id"])
    store.add_permission_grant(
        create_permission_grant(
            approval_id=terminal_approval_id,
            principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission="tool:terminal_exec",
            granted_by="test",
            granted_at=datetime.now(UTC),
        )
    )
    terminal_after = _invoke(executor, "terminal-after", "terminal_exec", {"command": "printf core"})
    assert terminal_after.status == "completed"
    assert terminal_after.output["stdout"] == "terminal ok\n"
    assert terminal_backend.calls == [("printf core", str(workspace.resolve()), 20, TerminalExecutionPolicy(None, ()))]

    web_before = _invoke(executor, "web-before", "web_fetch", {"url": "https://core-tools.test/report"})
    assert web_before.status == "denied"
    assert web_before.output["reason"] == "approval_required"
    web_approval_id = str(web_before.output["approval_id"])
    store.add_boundary_permit(
        create_boundary_permit(
            approval_id=web_approval_id,
            principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
            selector="core-tools.test",
            granted_by="test",
            uses_remaining=1,
        )
    )
    web_after = _invoke(executor, "web-after", "web_fetch", {"url": "https://core-tools.test/report"})
    assert web_after.status == "completed"
    assert web_after.output["status_code"] == 200
    assert web_after.output["title"] == "Allowed core fetch"
    assert fake_opener.urls == ["https://core-tools.test/report"]


def test_global_operator_terminal_grant_authorizes_member_by_default(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    terminal_backend = FakeTerminalBackend()
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)
    store.add_permission_grant(
        create_permission_grant(
            approval_id="operator-terminal",
            principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission="tool:terminal_exec",
            granted_by="test",
            granted_at=datetime.now(UTC),
        )
    )

    member_terminal = _invoke(
        executor,
        "member-terminal",
        "terminal_exec",
        {"command": "printf member"},
        principal_id="user:member",
    )

    assert member_terminal.status == "completed"
    assert member_terminal.output["stdout"] == "terminal ok\n"
    assert terminal_backend.calls == [("printf member", str(workspace.resolve()), 20, TerminalExecutionPolicy(None, ()))]


def test_member_tool_block_overrides_global_operator_terminal_grant(monkeypatch, tmp_path: Path) -> None:
    from nullion import users
    from nullion.users import NullionUser, UserRegistry, save_user_registry

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    users_path = tmp_path / "users.json"
    monkeypatch.setattr(users, "_USERS_PATH", users_path)
    save_user_registry(
        UserRegistry(
            multi_user_enabled=True,
            users=[
                NullionUser("admin", "Admin", role="admin"),
                NullionUser("member", "Member", role="member", blocked_tools=("terminal_exec",)),
            ],
        ),
        path=users_path,
    )
    terminal_backend = FakeTerminalBackend()
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)
    store.add_permission_grant(
        create_permission_grant(
            approval_id="operator-terminal",
            principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission="tool:terminal_exec",
            granted_by="test",
            granted_at=datetime.now(UTC),
        )
    )

    member_terminal = _invoke(
        executor,
        "member-terminal-blocked",
        "terminal_exec",
        {"command": "printf member"},
        principal_id="user:member",
    )

    assert member_terminal.status == "denied"
    assert member_terminal.output["reason"] == "user_tool_blocked"
    assert terminal_backend.calls == []


def test_destructive_terminal_command_requires_exact_delete_confirmation(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    victim = workspace / "runtime.db.corrupt-20260610"
    victim.write_text("bad", encoding="utf-8")
    terminal_backend = FakeTerminalBackend()
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)
    store.add_permission_grant(
        create_permission_grant(
            approval_id="operator-terminal",
            principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission="tool:terminal_exec",
            granted_by="test",
            granted_at=datetime.now(UTC),
        )
    )

    command = f"rm -f {victim}"
    blocked = _invoke(executor, "delete-blocked", "terminal_exec", {"command": command})

    assert blocked.status == "denied"
    assert blocked.output["reason"] == "terminal_destructive_action_confirmation_required"
    assert blocked.output["requires_approval"] is True
    assert blocked.output["destructive_target_count"] == 1
    assert blocked.output["destructive_targets"][0]["target"] == str(victim.resolve())
    approval = store.get_approval_request(str(blocked.output["approval_id"]))
    assert approval is not None
    assert approval.request_kind == TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND
    assert terminal_backend.calls == []


def test_destructive_terminal_find_delete_requires_confirmation(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    terminal_backend = FakeTerminalBackend()
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)
    store.add_permission_grant(
        create_permission_grant(
            approval_id="operator-terminal",
            principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission="tool:terminal_exec",
            granted_by="test",
            granted_at=datetime.now(UTC),
        )
    )

    command = f"find {workspace} -name '*.corrupt' -delete"
    blocked = _invoke(executor, "find-delete-blocked", "terminal_exec", {"command": command})

    assert blocked.status == "denied"
    assert blocked.output["reason"] == "terminal_destructive_action_confirmation_required"
    assert blocked.output["destructive_targets"][0]["target"] == str(workspace.resolve())
    assert blocked.output["destructive_targets"][0]["command_family"] == "find"
    assert terminal_backend.calls == []


def test_destructive_terminal_python_unlink_requires_confirmation(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    victim = workspace / "runtime.db.corrupt-20260610"
    victim.write_text("bad", encoding="utf-8")
    terminal_backend = FakeTerminalBackend()
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)
    store.add_permission_grant(
        create_permission_grant(
            approval_id="operator-terminal",
            principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission="tool:terminal_exec",
            granted_by="test",
            granted_at=datetime.now(UTC),
        )
    )

    command = f"python3 -c \"from pathlib import Path; Path('{victim}').unlink()\""
    blocked = _invoke(executor, "python-unlink-blocked", "terminal_exec", {"command": command})

    assert blocked.status == "denied"
    assert blocked.output["reason"] == "terminal_destructive_action_confirmation_required"
    assert blocked.output["destructive_targets"][0]["target"] == str(victim.resolve())
    assert blocked.output["destructive_targets"][0]["command_family"] == "script"
    assert terminal_backend.calls == []


def test_destructive_terminal_exact_confirmation_is_consumed_once(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    victim = workspace / "runtime.db.corrupt-20260610"
    victim.write_text("bad", encoding="utf-8")
    terminal_backend = FakeTerminalBackend()
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)

    command = f"rm -f {victim}"
    blocked = _invoke(executor, "delete-prompt", "terminal_exec", {"command": command})
    approval = store.get_approval_request(str(blocked.output["approval_id"]))
    assert approval is not None
    context = approval.context if isinstance(approval.context, dict) else {}
    token = str(context.get("destructive_permission_token") or "")
    assert token
    grant = create_permission_grant(
        approval_id=approval.approval_id,
        principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission=f"{TERMINAL_DESTRUCTIVE_PERMISSION_PREFIX}{token}",
        granted_by="test",
        granted_at=datetime.now(UTC),
    )
    store.add_permission_grant(grant)

    allowed = _invoke(executor, "delete-allowed", "terminal_exec", {"command": command})
    repeated = _invoke(executor, "delete-repeated", "terminal_exec", {"command": command})

    assert allowed.status == "completed"
    assert terminal_backend.calls == [(command, str(workspace.resolve()), 20, TerminalExecutionPolicy(None, ()))]
    consumed = store.get_permission_grant(grant.grant_id)
    assert consumed is not None
    assert consumed.revoked_at is not None
    assert repeated.status == "denied"
    assert repeated.output["reason"] == "terminal_destructive_action_confirmation_required"
    assert len(terminal_backend.calls) == 1


def test_long_terminal_output_is_saved_as_text_artifact(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    long_stdout = "terminal line\n" * 400
    terminal_backend = FakeTerminalBackend(stdout=long_stdout)
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspace-storage"))

    store = RuntimeStore()
    registry = create_core_tool_registry(
        workspace_root=workspace,
        terminal_executor_backend=terminal_backend,
    )
    executor = ToolExecutor(store=store, registry=registry)
    store.add_permission_grant(
        create_permission_grant(
            approval_id="operator-terminal",
            principal_id=OPERATOR_PERMISSION_PRINCIPAL,
            permission="tool:terminal_exec",
            granted_by="test",
            granted_at=datetime.now(UTC),
        )
    )

    result = _invoke(executor, "terminal-long-output", "terminal_exec", {"command": "printf lots"})

    assert result.status == "completed"
    output_path = Path(str(result.output["terminal_output_path"]))
    assert output_path.is_file()
    assert output_path.read_text(encoding="utf-8") == f"STDOUT\n{long_stdout.rstrip()}\n\nEXIT CODE\n0\n"
    assert str(output_path).startswith(str(workspace_storage_roots_for_principal("web:e2e").artifacts))
    assert str(output_path) in result.output["artifact_paths"]
    assert result.output["artifact_descriptors"][0]["kind"] == "terminal_output"
