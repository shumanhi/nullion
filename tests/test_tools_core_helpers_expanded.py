from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion.approvals import BoundaryPermit
from nullion.policy import BoundaryFact, BoundaryKind
from nullion.runtime import invoke_tool
from nullion.runtime_store import RuntimeStore
from nullion.tools import (
    TerminalAttestationEvidence,
    TerminalBackendDescriptor,
    ToolInvocation,
    ToolRegistry,
    ToolResult,
    ToolRiskLevel,
    ToolSideEffectClass,
    ToolSpec,
    _boundary_approval_context_from_fact,
    _boundary_approval_match_key,
    _boundary_policy_principal_for_fact,
    _boundary_risk_score,
    _build_file_search_handler,
    _build_file_patch_handler,
    _build_pdf_create_handler,
    _build_pdf_edit_handler,
    _build_file_read_handler,
    _build_file_write_handler,
    _build_connector_request_handler,
    _build_workspace_summary_handler,
    _build_terminal_exec_handler,
    _connector_access_enabled,
    _connector_allowed_base_urls,
    _connector_credential_value,
    _connector_provider_id_looks_external,
    _connector_request_url,
    create_core_tool_registry,
    _default_input_schema_for_tool,
    _effective_filesystem_roots,
    _env_flag,
    _is_global_literal_ip,
    _launcher_command_requires_probe_attestation,
    _missing_backend_attested_capabilities,
    _network_attempt_allowed,
    _network_is_approved,
    _normalize_network_mode,
    _outbound_network_approval_context_from_result,
    _path_within_any_root,
    _principal_workspace_file_roots,
    _record_wildcard_boundary_permit_accesses,
    _required_attested_capabilities_for_network_mode,
    _requires_probe_derived_launcher_attestation,
    _selector_candidates_for_boundary_target,
    _selector_matches_target,
    _selector_matches_www_family,
    normalize_tool_result,
    normalize_tool_status,
    register_email_plugin,
    TerminalBackendDescriptor,
    TerminalExecutionResult,
)


def fact(kind: BoundaryKind, target: str, **attrs) -> BoundaryFact:
    return BoundaryFact(kind=kind, tool_name="tool", operation="read", target=target, attributes={k: str(v) for k, v in attrs.items()})


def test_tool_status_registry_schemas_and_env_flags(monkeypatch, tmp_path) -> None:
    assert normalize_tool_status(None) == "unknown"
    assert normalize_tool_status("success") == "completed"
    assert normalize_tool_status("error") == "failed"
    assert normalize_tool_status("approval_required") == "denied"
    assert normalize_tool_status("pending") == "nonterminal"
    assert normalize_tool_status("???") == "unknown"
    assert normalize_tool_result(ToolResult("i", "t", "success", {"a": 1})).status == "completed"

    assert _default_input_schema_for_tool("file_read")["required"] == ["path"]
    assert _default_input_schema_for_tool("file_write")["required"] == ["path", "content"]
    assert "image_paths" in _default_input_schema_for_tool("pdf_create")["properties"]
    assert _default_input_schema_for_tool("pdf_edit")["required"] == ["input_path"]
    assert _default_input_schema_for_tool("unknown")["type"] == "object"

    registry = ToolRegistry(plugin_registration_allowed=False, filesystem_allowed_roots=[tmp_path])
    spec = ToolSpec("alpha", "Alpha tool", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5)
    registry.register(spec, lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True}))
    assert registry.get_spec("alpha") is spec
    assert registry.list_tool_definitions()[0]["name"] == "alpha"
    assert registry.invoke(ToolInvocation("inv", "alpha", "p", {})).output == {"ok": True}
    assert registry.filesystem_allowed_roots() == (tmp_path.resolve(),)
    registry.mark_plugin_installed(" search_plugin ")
    assert registry.is_plugin_installed("search_plugin") is True
    with pytest.raises(ValueError):
        registry.register(spec, lambda invocation: ToolResult("x", "alpha", "completed", {}))
    with pytest.raises(KeyError):
        registry.invoke(ToolInvocation("inv", "missing", "p", {}))
    with pytest.raises(ValueError):
        registry.require_plugin_registration_allowed()

    monkeypatch.delenv("FLAG", raising=False)
    assert _env_flag("FLAG", default=False) is False
    monkeypatch.setenv("FLAG", "off")
    assert _env_flag("FLAG") is False
    monkeypatch.setenv("NULLION_CONNECTOR_ACCESS_ENABLED", "true")
    assert _connector_access_enabled() is True
    monkeypatch.delenv("NULLION_CONNECTOR_ACCESS_ENABLED", raising=False)
    monkeypatch.delenv("NULLION_ENABLED_SKILL_PACKS", raising=False)
    monkeypatch.setattr(
        "nullion.connections.load_connection_registry",
        lambda: SimpleNamespace(connections=[SimpleNamespace(provider_id="skill_pack_connector_acme", active=True)]),
    )
    assert _connector_access_enabled() is True


def test_connector_access_is_enabled_by_generic_connector_skill_packs(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("NULLION_CONNECTOR_ACCESS_ENABLED", raising=False)
    monkeypatch.setenv("NULLION_ENABLED_SKILL_PACKS", "nullion/connector-skills")

    assert _connector_access_enabled() is True
    assert _connector_provider_id_looks_external("skill_pack_connector_acme") is True
    assert _connector_provider_id_looks_external("acme_connector_provider") is True
    assert _connector_provider_id_looks_external("email_plugin") is False

    registry = create_core_tool_registry(workspace_root=tmp_path)
    assert registry.is_plugin_installed("connector_plugin") is True
    assert "connector_request" in {spec.name for spec in registry.list_specs()}


def test_failed_account_tool_advertises_connected_connector_fallback(monkeypatch) -> None:
    connection = SimpleNamespace(
        provider_id="zephyr_connector_provider",
        display_name="Zephyr mail connector",
        credential_scope="workspace",
        active=True,
    )
    monkeypatch.setattr(
        "nullion.connections.load_connection_registry",
        lambda: SimpleNamespace(connections=[connection]),
    )
    monkeypatch.setattr(
        "nullion.connections.connection_for_principal",
        lambda principal_id, provider_id: connection if provider_id == "zephyr_connector_provider" else None,
    )

    def fail_search(query: str, limit: int) -> list[dict[str, object]]:
        raise RuntimeError("Invalid credentials")

    registry = ToolRegistry()
    register_email_plugin(registry, email_searcher=fail_search)

    result = registry.invoke(ToolInvocation("inv", "email_search", "workspace:workspace_admin", {"query": "inbox", "limit": 1}))

    assert result.status == "failed"
    assert result.output["query"] == "inbox"
    assert result.output["available_connector_providers"] == [
        {
            "provider_id": "zephyr_connector_provider",
            "display_name": "Zephyr mail connector",
            "credential_scope": "workspace",
        }
    ]
    assert "try connector_request" in result.output["next_step"]


def test_failed_account_tool_includes_installed_connector_skill_pack(monkeypatch) -> None:
    pack = SimpleNamespace(pack_id="zephyr/api-gateway-skill")
    connection = SimpleNamespace(
        provider_id="skill_pack_connector_zephyr_api_gateway_skill",
        display_name="Zephyr mail",
        credential_ref="ZEPHYR_TOKEN",
        credential_scope="workspace",
        active=True,
    )
    monkeypatch.setattr("nullion.skill_pack_installer.list_installed_skill_packs", lambda: [pack])
    monkeypatch.setattr(
        "nullion.connections.load_connection_registry",
        lambda: SimpleNamespace(connections=[connection]),
    )
    monkeypatch.setattr(
        "nullion.connections.connection_for_principal",
        lambda principal_id, provider_id: connection if provider_id == "skill_pack_connector_zephyr_api_gateway_skill" else None,
    )

    def fail_search(query: str, limit: int) -> list[dict[str, object]]:
        raise RuntimeError("Invalid credentials")

    registry = ToolRegistry()
    register_email_plugin(registry, email_searcher=fail_search)

    result = registry.invoke(ToolInvocation("inv", "email_search", "workspace:workspace_admin", {"query": "newer:7d", "limit": 3}))

    assert result.status == "failed"
    assert result.output["available_connector_providers"][0]["provider_id"] == "skill_pack_connector_zephyr_api_gateway_skill"
    assert result.output["available_connector_providers"][0]["skill_pack_id"] == "zephyr/api-gateway-skill"
    assert "consult that skill's instructions" in result.output["next_step"]
    assert "suggested_connector_requests" not in result.output


def test_connector_credentials_accept_generic_gateway_prefixes(monkeypatch) -> None:
    monkeypatch.setenv("ACME_API_KEY", "token-1")

    assert _connector_credential_value(None, "acme_connector_provider") == "token-1"
    assert _connector_credential_value(None, "skill_pack_connector_acme") == "token-1"

    monkeypatch.delenv("ACME_API_KEY", raising=False)
    monkeypatch.setenv("ACME_SECRET_KEY", "secret-1")
    assert _connector_credential_value(SimpleNamespace(credential_ref="env:ACME_SECRET_KEY"), "custom_connector_provider") == "secret-1"


def test_connector_base_urls_are_loaded_from_connection_config(monkeypatch) -> None:
    provider_id = "custom_connector_provider"
    connection = SimpleNamespace(
        provider_id=provider_id,
        provider_profile="https://www.example.com/profile-root",
        credential_ref="NULLION_CUSTOM_CONNECTOR_TOKEN",
    )
    monkeypatch.setenv("NULLION_CUSTOM_CONNECTOR_BASE_URL", "https://example.com/api")

    assert _connector_allowed_base_urls(connection, provider_id) == (
        "https://www.example.com/profile-root/",
        "https://example.com/api/",
    )
    assert _connector_request_url(
        "https://example.com/api/mail/v1/messages",
        {"q": "inbox"},
        connection,
        provider_id,
    ).endswith("?q=inbox")
    with pytest.raises(ValueError, match="not under configured connector base URL"):
        _connector_request_url(
            "https://www.google.com/gmail",
            {},
            connection,
            provider_id,
        )


def test_connector_base_urls_are_loaded_from_installed_skill_pack(monkeypatch, tmp_path) -> None:
    provider_id = "skill_pack_connector_zephyr_api_gateway_skill"
    pack_root = tmp_path / "pack"
    pack_root.mkdir()
    (pack_root / "SKILL.md").write_text(
        "Base URL: https://example.com/{app}/{native-api-path}\n"
        "Manage connections at https://www.example.com/connections.\n",
        encoding="utf-8",
    )
    pack = SimpleNamespace(pack_id="zephyr/api-gateway-skill", path=str(pack_root))
    connection = SimpleNamespace(provider_id=provider_id, credential_ref="ZEPHYR_TOKEN")
    monkeypatch.setattr("nullion.skill_pack_installer.list_installed_skill_packs", lambda: [pack])
    monkeypatch.setattr("nullion.skill_pack_installer.get_installed_skill_pack", lambda pack_id: pack)

    assert "https://example.com/" in _connector_allowed_base_urls(connection, provider_id)
    assert "https://www.example.com/connections/" in _connector_allowed_base_urls(connection, provider_id)
    assert _connector_request_url(
        "https://example.com/mail/v1/messages",
        {"q": "inbox"},
        connection,
        provider_id,
    ).endswith("?q=inbox")
    with pytest.raises(ValueError, match="not under configured connector base URL"):
        _connector_request_url(
            "https://www.google.com/gmail",
            {},
            connection,
            provider_id,
        )


def test_connector_write_requires_connection_permission(monkeypatch) -> None:
    provider_id = "skill_pack_connector_acme_mail"
    connection = SimpleNamespace(
        provider_id=provider_id,
        provider_profile="https://api.example.com/",
        credential_ref="ACME_TOKEN",
        permission_mode="read",
        active=True,
    )
    monkeypatch.setenv("ACME_TOKEN", "secret")
    monkeypatch.setattr("nullion.connections.require_workspace_connection_for_principal", lambda principal_id, candidate: connection)

    handler = _build_connector_request_handler()
    result = handler(
        ToolInvocation(
            "inv",
            "connector_request",
            "operator",
            {
                "provider_id": provider_id,
                "url": "https://api.example.com/gmail/send",
                "method": "POST",
                "json": {"to": "you@example.com"},
            },
        )
    )

    assert result.status == "failed"
    assert "read-only" in str(result.error)
    assert result.output["method"] == "POST"


def test_connector_write_posts_payload_when_permission_enabled(monkeypatch) -> None:
    provider_id = "skill_pack_connector_acme_mail"
    connection = SimpleNamespace(
        provider_id=provider_id,
        provider_profile="https://api.example.com/",
        credential_ref="ACME_TOKEN",
        permission_mode="write",
        active=True,
    )
    captured = {}

    class FakeHeaders:
        def get_content_type(self):
            return "application/json"

    class FakeResponse:
        headers = FakeHeaders()
        status = 202

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self, size):
            return b'{"ok": true}'

    class FakeOpener:
        def open(self, request, timeout):
            captured["method"] = request.get_method()
            captured["data"] = request.data
            captured["headers"] = dict(request.header_items())
            captured["url"] = request.full_url
            return FakeResponse()

    monkeypatch.setenv("ACME_TOKEN", "secret")
    monkeypatch.setattr("nullion.connections.require_workspace_connection_for_principal", lambda principal_id, candidate: connection)
    monkeypatch.setattr("nullion.tools._resolve_web_fetch_resolution", lambda url: SimpleNamespace(host="api.example.com", address_infos=()))
    monkeypatch.setattr("nullion.tools._build_web_fetch_opener", lambda: FakeOpener())

    handler = _build_connector_request_handler()
    result = handler(
        ToolInvocation(
            "inv",
            "connector_request",
            "operator",
            {
                "provider_id": provider_id,
                "url": "https://api.example.com/gmail/send",
                "method": "POST",
                "json": {"to": "you@example.com"},
            },
        )
    )

    assert result.status == "completed"
    assert result.output["method"] == "POST"
    assert captured["method"] == "POST"
    assert captured["data"] == b'{"to": "you@example.com"}'
    assert captured["headers"]["Content-type"] == "application/json"


def test_filesystem_and_network_helper_edges(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("nullion.workspace_storage.workspace_file_roots_for_principal", lambda principal: [tmp_path / "workspace"])
    assert _principal_workspace_file_roots("user:1") == ((tmp_path / "workspace").resolve(),)
    assert _principal_workspace_file_roots("anonymous") == ()

    invocation = ToolInvocation("inv", "file_read", "user:1", {})
    roots = _effective_filesystem_roots(invocation=invocation, resolved_root=tmp_path, resolved_allowed_roots=None, include_principal_workspace=True)
    assert tmp_path.resolve() in roots
    inside = tmp_path / "child.txt"
    outside = tmp_path.parent / "outside.txt"
    assert _path_within_any_root(inside.resolve(), (tmp_path.resolve(),)) is True
    assert _path_within_any_root(outside.resolve(), (tmp_path.resolve(),)) is False

    assert _is_global_literal_ip("8.8.8.8") is True
    assert _is_global_literal_ip("127.0.0.1") is False
    assert _normalize_network_mode("full") == "full"
    assert _normalize_network_mode("bad") is None
    assert _required_attested_capabilities_for_network_mode("none") == ("network_policy_enforced", "network_policy_enforced.none")
    assert _required_attested_capabilities_for_network_mode("full") == ()
    assert _network_attempt_allowed(attempt={"address_class": "localhost"}, network_mode="localhost_only", approved_targets=[]) is True
    assert _network_attempt_allowed(attempt={"target": "https://example.com/path"}, network_mode="approved_only", approved_targets=["https://example.com/*"]) is True
    assert _network_attempt_allowed(attempt={"target": "https://evil.com"}, network_mode="approved_only", approved_targets=["https://example.com/*"]) is False
    assert _network_attempt_allowed(attempt={}, network_mode="none", approved_targets=[]) is False
    assert _network_is_approved(target="https://www.example.com/a", approved_targets=["https://example.com/*"]) is True


def test_virtual_workspace_path_resolves_to_principal_workspace_without_approval(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    store = RuntimeStore()
    registry = create_core_tool_registry()

    result = invoke_tool(
        store,
        invocation=ToolInvocation(
            "inv",
            "file_write",
            "telegram_chat",
            {"path": "/workspace/bird1.png", "content": "png-ish"},
        ),
        registry=registry,
    )

    expected_path = tmp_path / "workspaces" / "workspace_admin" / "bird1.png"
    assert result.status == "completed"
    assert result.output["path"] == str(expected_path.resolve())
    assert expected_path.read_text(encoding="utf-8") == "png-ish"
    assert store.list_approval_requests() == []


def test_boundary_approval_contexts_and_match_keys() -> None:
    assert _selector_candidates_for_boundary_target("https://example.com/path")["allow_once"] == "example.com"
    assert _selector_candidates_for_boundary_target("/tmp/file")["allow_once"] == "/tmp/file"

    result = ToolResult(
        "inv",
        "terminal_exec",
        "denied",
        {
            "reason": "network_denied",
            "network_mode": "approved_only",
            "egress_attempts": [{"kind": "outbound_network", "target": "https://example.com", "address_class": "public"}],
        },
    )
    assert _outbound_network_approval_context_from_result(result)["target"] == "https://example.com"
    assert _outbound_network_approval_context_from_result(ToolResult("i", "web_fetch", "denied", {})) is None

    network = fact(BoundaryKind.OUTBOUND_NETWORK, "https://example.com", address_class="public")
    private_network = fact(BoundaryKind.OUTBOUND_NETWORK, "http://127.0.0.1", address_class="loopback")
    filesystem = fact(BoundaryKind.FILESYSTEM_ACCESS, "/tmp/a.txt", path="/tmp/a.txt")
    account = fact(BoundaryKind.ACCOUNT_ACCESS, "gmail:me", account_type="gmail")
    assert _boundary_approval_context_from_fact(network)["boundary_kind"] == "outbound_network"
    assert _boundary_approval_context_from_fact(private_network) is None
    assert _boundary_approval_context_from_fact(filesystem)["path"] == "/tmp/a.txt"
    assert _boundary_approval_context_from_fact(account)["selector_candidates"]["always_allow"] == "gmail:*"
    assert _boundary_risk_score(account) == 7
    assert _boundary_risk_score(filesystem) == 6
    assert _boundary_risk_score(private_network) == 9
    assert _boundary_risk_score(network) == 4
    assert _boundary_policy_principal_for_fact("user:1", filesystem)
    assert _boundary_policy_principal_for_fact("user:1", network)

    context = _boundary_approval_context_from_fact(network)
    assert _boundary_approval_match_key(context=context, fallback_target="fallback") == ("outbound_network", "example.com")
    assert _boundary_approval_match_key(context={}, fallback_target="fallback") is None


def test_selector_matching_and_terminal_attestation_helpers() -> None:
    assert _selector_matches_target(selector="https://example.com/*", target="https://www.example.com/a") is True
    assert _selector_matches_target(selector="/tmp/*", target="/tmp/file") is True
    assert _selector_matches_target(selector="/tmp/*", target="/tmp") is True
    assert _selector_matches_target(selector="*.txt", target="report.txt") is True
    assert _selector_matches_www_family(selector="https://example.com/path", target="https://www.example.com/path/") is True
    assert _selector_matches_www_family(selector="https://evil.com/*", target="https://example.com") is False

    descriptor = TerminalBackendDescriptor(
        mode="subprocess",
        attested_capabilities=("network_policy_enforced",),
        metadata={},
        evidence=None,
    )
    assert _missing_backend_attested_capabilities(descriptor, required_capabilities=["network_policy_enforced", "x"]) == ("x",)
    assert _requires_probe_derived_launcher_attestation(["network_policy_enforced.none"]) is True
    assert _requires_probe_derived_launcher_attestation(["other"]) is False
    assert _launcher_command_requires_probe_attestation("/usr/bin/nullion-launcher") is True
    assert _launcher_command_requires_probe_attestation("nullion-launcher") is False
    assert _launcher_command_requires_probe_attestation(None) is False


def test_wildcard_boundary_permit_access_records_events(monkeypatch) -> None:
    store = RuntimeStore()
    permit = BoundaryPermit(
        permit_id="permit",
        approval_id="approval",
        principal_id="global",
        boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector="*",
        granted_by="admin",
        granted_at=datetime.now(UTC),
    )
    network_fact = fact(BoundaryKind.OUTBOUND_NETWORK, "https://example.com", address_class="public")
    monkeypatch.setattr("nullion.tools.extract_boundary_facts", lambda invocation: [network_fact])

    invocation = ToolInvocation("inv", "terminal_exec", "user:1", {}, capsule_id="cap")
    _record_wildcard_boundary_permit_accesses(store, invocation, [permit, permit])

    events = store.list_events()
    assert len(events) == 1
    assert events[0].event_type == "boundary_permit.wildcard_access"
    assert store.list_audit_records()[0].action == "boundary_permit.wildcard_access"


def test_file_read_write_patch_and_workspace_summary_handlers(tmp_path) -> None:
    read_handler = _build_file_read_handler(workspace_root=tmp_path, include_principal_workspace=False)
    write_handler = _build_file_write_handler(workspace_root=tmp_path, include_principal_workspace=False)
    patch_handler = _build_file_patch_handler(workspace_root=tmp_path, include_principal_workspace=False)
    summary_handler = _build_workspace_summary_handler(workspace_root=tmp_path, include_principal_workspace=False)

    outside = tmp_path.parent / "outside-tools-test.txt"
    assert read_handler(ToolInvocation("inv", "file_read", "operator", {})).error == "Missing required argument: path"
    assert "outside workspace" in read_handler(ToolInvocation("inv", "file_read", "operator", {"path": str(outside)})).error
    assert "File not found" in read_handler(ToolInvocation("inv", "file_read", "operator", {"path": str(tmp_path / "missing.txt")})).error

    target = tmp_path / "folder" / "note.txt"
    assert write_handler(ToolInvocation("inv", "file_write", "operator", {"path": str(target)})).error == "Missing required argument: content"
    written = write_handler(ToolInvocation("inv", "file_write", "operator", {"path": str(target), "content": "alpha beta alpha"}))
    assert written.status == "completed"
    assert target.read_text(encoding="utf-8") == "alpha beta alpha"
    assert read_handler(ToolInvocation("inv", "file_read", "operator", {"path": str(target)})).output["content"] == "alpha beta alpha"

    assert patch_handler(ToolInvocation("inv", "file_patch", "operator", {"path": str(target), "old_string": "", "new_string": "x"})).error == "old_string must be non-empty"
    assert "replace_all" in patch_handler(ToolInvocation("inv", "file_patch", "operator", {"path": str(target), "old_string": "alpha", "new_string": "x", "replace_all": "yes"})).error
    assert "exactly once" in patch_handler(ToolInvocation("inv", "file_patch", "operator", {"path": str(target), "old_string": "alpha", "new_string": "x"})).error
    patched = patch_handler(ToolInvocation("inv", "file_patch", "operator", {"path": str(target), "old_string": "alpha", "new_string": "x", "replace_all": True}))
    assert patched.output["replacements"] == 2
    assert target.read_text(encoding="utf-8") == "x beta x"
    assert "Old string not found" in patch_handler(ToolInvocation("inv", "file_patch", "operator", {"path": str(target), "old_string": "missing", "new_string": "x"})).error

    summary = summary_handler(ToolInvocation("inv", "workspace_summary", "operator", {}))
    assert summary.status == "completed"
    assert summary.output["file_count"] == 1
    assert summary.output["extensions"] == [{"extension": ".txt", "count": 1}]


def test_file_search_and_summary_prune_browser_profiles_and_symlinks(tmp_path) -> None:
    safe_match = tmp_path / "cron" / "daily-cron.json"
    safe_match.parent.mkdir()
    safe_match.write_text("{}", encoding="utf-8")
    browser_match = tmp_path / "brave-debug" / "Default" / "secret-cron.json"
    browser_match.parent.mkdir(parents=True)
    browser_match.write_text("{}", encoding="utf-8")
    link_match = tmp_path / "linked-cron.json"
    link_match.symlink_to(safe_match)

    search_handler = _build_file_search_handler(workspace_root=tmp_path, include_principal_workspace=False)
    found = search_handler(ToolInvocation("inv", "file_search", "operator", {"pattern": "cron"}))

    assert found.status == "completed"
    assert found.output["matches"] == [str(safe_match.resolve())]

    summary_handler = _build_workspace_summary_handler(workspace_root=tmp_path, include_principal_workspace=False)
    summary = summary_handler(ToolInvocation("inv", "workspace_summary", "operator", {}))

    assert summary.output["file_count"] == 1
    assert summary.output["sample_files"] == ["cron/daily-cron.json"]


def test_pdf_create_and_edit_handlers_are_local_artifact_tools(tmp_path) -> None:
    from PIL import Image
    from pypdf import PdfReader

    first_image = tmp_path / "first.png"
    second_image = tmp_path / "second.png"
    Image.new("RGB", (80, 60), "blue").save(first_image)
    Image.new("RGB", (60, 80), "green").save(second_image)

    create_handler = _build_pdf_create_handler(workspace_root=tmp_path, include_principal_workspace=False)
    created_path = tmp_path / "combined.pdf"
    created = create_handler(
        ToolInvocation(
            "inv",
            "pdf_create",
            "operator",
            {
                "output_path": str(created_path),
                "image_paths": [str(first_image), str(second_image)],
                "title": "Birds",
            },
        )
    )

    assert created.status == "completed"
    assert created.output["artifact_path"] == str(created_path)
    assert created_path.read_bytes().startswith(b"%PDF")
    assert len(PdfReader(str(created_path)).pages) == 2

    text_created_path = tmp_path / "single-text.pdf"
    text_created = create_handler(
        ToolInvocation(
            "inv",
            "pdf_create",
            "operator",
            {
                "output_path": str(text_created_path),
                "text_pages": "A single text page should be accepted.",
                "title": "Text",
            },
        )
    )
    assert text_created.status == "completed"
    assert text_created.output["page_count"] == 1

    edit_handler = _build_pdf_edit_handler(workspace_root=tmp_path, include_principal_workspace=False)
    edited_path = tmp_path / "edited.pdf"
    edited = edit_handler(
        ToolInvocation(
            "inv",
            "pdf_edit",
            "operator",
            {
                "input_path": str(created_path),
                "output_path": str(edited_path),
                "page_numbers": [2, 1],
                "append_text_pages": ["A local text appendix."],
            },
        )
    )

    assert edited.status == "completed"
    assert edited.output["artifact_path"] == str(edited_path)
    assert len(PdfReader(str(edited_path)).pages) == 3


def test_file_handlers_allow_trusted_filesystem_selector_without_workspace_root(tmp_path) -> None:
    target = tmp_path / "trusted.txt"
    target.write_text("trusted", encoding="utf-8")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    handler = _build_file_read_handler(workspace_root=workspace, include_principal_workspace=False)

    denied = handler(ToolInvocation("inv", "file_read", "operator", {"path": str(target)}))
    assert "outside workspace" in denied.error

    allowed = handler(
        ToolInvocation(
            "inv",
            "file_read",
            "operator",
            {"path": str(target)},
            trusted_filesystem_selectors=(str(tmp_path / "*"),),
        )
    )
    assert allowed.output["content"] == "trusted"


def test_terminal_exec_handler_validates_network_policy_and_backend_results() -> None:
    class Backend:
        def __init__(self, *, descriptor=None, result=None, exc=None):
            self.descriptor = descriptor or TerminalBackendDescriptor("subprocess", (), {})
            self.result = result or TerminalExecutionResult(0, "ok", "")
            self.exc = exc
            self.calls = []

        def describe(self):
            return self.descriptor

        def run(self, command, *, cwd, timeout, policy):
            self.calls.append((command, cwd, timeout, policy))
            if self.exc:
                raise self.exc
            return self.result

    handler = _build_terminal_exec_handler(workspace_root=None, terminal_executor_backend=Backend())
    assert handler(ToolInvocation("inv", "terminal_exec", "operator", {})).error == "Missing required argument: command"
    assert handler(ToolInvocation("inv", "terminal_exec", "operator", {"command": "echo hi", "network_mode": "bad"})).output["reason"] == "invalid_network_mode"
    assert handler(ToolInvocation("inv", "terminal_exec", "operator", {"command": "curl https://example.com", "network_mode": "none"})).output["reason"] == "network_denied"

    failed_backend = Backend(result=TerminalExecutionResult(2, "", "bad"))
    failed = _build_terminal_exec_handler(terminal_executor_backend=failed_backend)(
        ToolInvocation("inv", "terminal_exec", "operator", {"command": "false"})
    )
    assert failed.status == "failed"
    assert failed.error == "Command failed with exit code 2"

    ok_backend = Backend(result=TerminalExecutionResult(0, "hello", ""))
    ok = _build_terminal_exec_handler(workspace_root=Path("/tmp"), terminal_executor_backend=ok_backend)(
        ToolInvocation("inv", "terminal_exec", "operator", {"command": "echo hello", "network_mode": "full"})
    )
    assert ok.status == "completed"
    assert ok.output["stdout"] == "hello"
