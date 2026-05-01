from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest


def test_settings_credentials_env_precedence_and_snapshot(tmp_path, monkeypatch) -> None:
    from nullion import settings as settings_module

    creds = tmp_path / "credentials.json"
    creds.write_text(
        json.dumps(
            {
                "provider": "anthropic",
                "keys": {"anthropic": "ant-key"},
                "models": {"anthropic": "claude-1,claude-2"},
                "refresh_token": "refresh",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings_module, "_CREDENTIALS_PATH", creds)
    monkeypatch.setenv("NULLION_MODEL_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-env")
    monkeypatch.setenv("NULLION_MODEL", "gpt-env,gpt-backup")
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("NULLION_OPERATOR_NAME", "Ada")

    settings = settings_module.Settings()

    assert settings.model.provider == "openai"
    assert settings.model.openai_api_key == "sk-env"
    assert settings.model.openai_model == "gpt-env"
    assert settings.model.codex_refresh_token == "refresh"
    assert settings.checkpoint_path == tmp_path / "data" / "runtime.db"
    assert settings.operator_name == "Ada"
    assert settings.has_llm() is True
    assert settings.to_dict()["has_api_key"] is True


def test_runtime_config_snapshot_prompt_and_persistence(tmp_path, monkeypatch) -> None:
    from nullion import runtime_config

    creds = tmp_path / "credentials.json"
    creds.write_text(json.dumps({"provider": "openrouter", "model": "stored-model"}), encoding="utf-8")
    monkeypatch.setattr(runtime_config, "_CREDENTIALS_PATH", creds)
    monkeypatch.setenv("NULLION_MODEL_PROVIDER", "codex")
    monkeypatch.setenv("NULLION_MODEL", "gpt-5.5")
    monkeypatch.setenv("NULLION_ADMIN_FORCED_MODEL", "admin-model")
    monkeypatch.setenv("NULLION_ADMIN_FORCED_PROVIDER", "openai")
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN", "bot")
    monkeypatch.setenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "chat")
    monkeypatch.setenv("NULLION_BROWSER_ENABLED", "false")

    snapshot = runtime_config.current_runtime_config(model_client=SimpleNamespace(model="client-model"))

    assert snapshot.provider == "codex"
    assert snapshot.model == "gpt-5.5"
    assert snapshot.admin_forced_model == "admin-model"
    assert snapshot.telegram_configured is True
    assert snapshot.browser_enabled is False
    prompt = runtime_config.format_runtime_config_for_prompt()
    assert "Admin-forced model" in prompt
    assert "Disabled capabilities: browser" in prompt

    runtime_config.persist_model_name("new-model")
    from nullion.credential_store import load_encrypted_credentials

    assert load_encrypted_credentials(db_path=creds.with_name("runtime.db"))["model"] == "new-model"
    assert runtime_config.os.environ["NULLION_MODEL"] == "new-model"
    runtime_config.persist_admin_forced_model("forced")
    assert load_encrypted_credentials(db_path=creds.with_name("runtime.db"))["admin_forced_model"] == "forced"
    runtime_config.clear_admin_forced_model()
    assert "admin_forced_model" not in load_encrypted_credentials(db_path=creds.with_name("runtime.db"))
    with pytest.raises(ValueError):
        runtime_config.persist_model_name(" ")
    with pytest.raises(ValueError):
        runtime_config.persist_admin_forced_model(" ")


def test_memory_capture_owners_and_formatting(monkeypatch) -> None:
    from nullion import memory

    class Store:
        def __init__(self):
            self.entries = {}

        def get_user_memory_entry(self, entry_id):
            return self.entries.get(entry_id)

        def add_user_memory_entry(self, entry):
            self.entries[entry.entry_id] = entry

        def list_user_memory_entries(self):
            return list(self.entries.values())

    store = Store()
    owner = memory.memory_owner_for_workspace("team")
    assert owner == "workspace:team"
    assert memory.memory_owner_for_web_admin() == "workspace:workspace_admin"

    written = memory.capture_explicit_user_memory(store, owner=owner, text="remember that my favorite color is blue.", source="test")
    assert written[0].kind is memory.UserMemoryKind.PREFERENCE
    assert written[0].key == "favorite_color"

    zip_written = memory.capture_explicit_user_memory(store, owner=owner, text="Weather for 12345 please", source="test")
    assert zip_written[0].key == "home_zip"
    updated = memory.remember_text_fact(store, owner=owner, key="Project Name", value="Nullion", source="test")
    assert updated.entry_id == f"{owner}:project_name"
    assert memory.memory_entries_for_owner(store, owner)
    formatted = memory.format_memory_context(memory.memory_entries_for_owner(store, owner))
    assert "Favorite Color: blue" in formatted
    assert "Home ZIP: 12345" in formatted
    assert memory.format_memory_context([]) is None

    monkeypatch.setattr("nullion.users.resolve_messaging_user", lambda channel, identity, settings: SimpleNamespace(workspace_id="member"))
    assert memory.memory_owner_for_messaging("slack", "U1", None) == "workspace:member"


def test_connections_registry_env_inference_principal_resolution_and_prompt(tmp_path, monkeypatch) -> None:
    from nullion import connections
    from nullion.users import NullionUser, UserRegistry

    monkeypatch.delenv("NULLION_CONNECTOR_GATEWAY", raising=False)
    path = tmp_path / "connections.json"
    connections.save_connection_registry(
        {
            "connections": [
                {
                    "workspace_id": "workspace_member",
                    "provider_id": "email",
                    "display_name": "Member email",
                    "provider_profile": "gmail",
                    "credential_ref": "EMAIL_KEY",
                },
                {"workspace_id": "", "provider_id": "bad"},
            ]
        },
        path=path,
    )
    registry = connections.load_connection_registry(path=path)
    assert len(registry.connections) == 1
    assert connections.connection_for_workspace("workspace_member", "email", path=path).display_name == "Member email"

    shared_path = tmp_path / "shared-connections.json"
    connections.save_connection_registry(
        {
            "connections": [
                {
                    "workspace_id": "workspace_admin",
                    "provider_id": "custom_api_provider",
                    "display_name": "Shared bridge",
                    "credential_ref": "NULLION_SHARED_TOKEN",
                    "credential_scope": "shared",
                    "permission_mode": "read_write",
                }
            ]
        },
        path=shared_path,
    )
    shared = connections.connection_for_workspace("workspace_member", "custom_api_provider", path=shared_path)
    assert shared is not None
    assert shared.display_name == "Shared bridge"
    assert shared.credential_scope == "shared"
    assert shared.permission_mode == "write"

    connector_path = tmp_path / "connectors.json"
    connections.save_connection_registry(
        {
            "connections": [
                {
                    "workspace_id": "workspace_admin",
                    "provider_id": "skill_pack_connector_acme_shipments",
                    "display_name": "ACME shipments",
                    "credential_ref": "ACME_SHIPMENTS_TOKEN",
                }
            ]
        },
        path=connector_path,
    )
    configured = connections.load_connection_registry(path=connector_path)
    assert configured.connections[0].provider_id == "skill_pack_connector_acme_shipments"

    imap_path = tmp_path / "imap-connections.json"
    connections.save_connection_registry(
        {
            "connections": [
                {
                    "workspace_id": "workspace_admin",
                    "provider_id": "imap_smtp_provider",
                    "display_name": "Agent email",
                    "provider_profile": "AGENT",
                    "credential_ref": "AGENT",
                }
            ]
        },
        path=imap_path,
    )
    assert connections.infer_email_plugin_provider(path=imap_path) == "imap_smtp_provider"

    monkeypatch.setattr(
        connections,
        "load_user_registry",
        lambda: UserRegistry(multi_user_enabled=True, users=[NullionUser("u1", "User", role="member", workspace_id="workspace_member")]),
    )
    monkeypatch.setattr(connections, "resolve_messaging_user", lambda channel, identity, settings: SimpleNamespace(workspace_id="workspace_member"))
    assert connections.workspace_id_for_principal("user:u1") == "workspace_member"
    assert connections.workspace_id_for_principal("slack:U1") == "workspace_member"
    assert connections.multi_user_connections_active() is True
    prompt = connections.format_workspace_connections_for_prompt(principal_id="workspace:workspace_member")
    assert "Configured workspace connections" in prompt or "No workspace provider connections" in prompt

    monkeypatch.setattr(connections, "connection_for_workspace", lambda workspace_id, provider_id: None)
    with pytest.raises(RuntimeError, match="not connected"):
        connections.require_workspace_connection_for_principal("user:u1", "email")


def test_workspace_connection_prompt_lists_generic_connector_providers(tmp_path, monkeypatch) -> None:
    from nullion import connections

    path = tmp_path / "connections.json"
    connections.save_connection_registry(
        {
            "connections": [
                {
                    "workspace_id": "workspace_admin",
                    "provider_id": "skill_pack_connector_acme_mail",
                    "display_name": "ACME mail connector",
                    "credential_ref": "ACME_MAIL_TOKEN",
                }
            ]
        },
        path=path,
    )
    monkeypatch.setattr(connections, "_CONNECTIONS_PATH", path)

    prompt = connections.format_workspace_connections_for_prompt(principal_id="workspace:workspace_admin")

    assert "provider=skill_pack_connector_acme_mail" in prompt
    assert "credential_ref=ACME_MAIL_TOKEN" in prompt


def test_workspace_connection_prompt_marks_shared_admin_credentials(tmp_path, monkeypatch) -> None:
    from nullion import connections

    path = tmp_path / "connections.json"
    connections.save_connection_registry(
        {
            "connections": [
                {
                    "workspace_id": "workspace_admin",
                    "provider_id": "custom_connector_provider",
                    "display_name": "Shared connector",
                    "credential_ref": "NULLION_SHARED_CONNECTOR_TOKEN",
                    "credential_scope": "shared",
                    "permission_mode": "read_write",
                }
            ]
        },
        path=path,
    )
    monkeypatch.setattr(connections, "_CONNECTIONS_PATH", path)

    prompt = connections.format_workspace_connections_for_prompt(principal_id="workspace:workspace_member")

    assert "provider=custom_connector_provider" in prompt
    assert "credential_scope=shared_by_admin" in prompt
    assert "permission_mode=read_write" in prompt


def test_skill_pack_auth_provider_catalog_is_skill_agnostic(tmp_path, monkeypatch) -> None:
    from nullion import skill_pack_catalog as catalog

    auth_pack = tmp_path / "auth_pack"
    no_auth_pack = tmp_path / "no_auth_pack"
    (auth_pack / "shipments").mkdir(parents=True)
    (no_auth_pack / "notes").mkdir(parents=True)
    (auth_pack / "shipments" / "SKILL.md").write_text(
        "# Shipments\n\nUse the configured API key token reference to read shipment data.\n",
        encoding="utf-8",
    )
    (no_auth_pack / "notes" / "SKILL.md").write_text(
        "# Notes\n\nSummarize public release notes.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        catalog,
        "list_installed_skill_packs",
        lambda: [
            SimpleNamespace(pack_id="custom/shipments", source="local", skills_count=1, path=auth_pack),
            SimpleNamespace(pack_id="custom/notes", source="local", skills_count=1, path=no_auth_pack),
        ],
    )

    providers = catalog.list_skill_pack_auth_providers()

    assert any(
        provider["skill_pack_id"] == "custom/shipments"
        and provider["provider_id"] == "skill_pack_connector_custom_shipments"
        for provider in providers
    )
    assert not any(provider["skill_pack_id"] == "custom/notes" for provider in providers)
    assert not any(provider["provider_id"] == "maton_connector_provider" for provider in providers)
    assert any(
        provider["provider_id"] == "imap_smtp_provider" and provider["shared_allowed"] is True
        for provider in providers
    )


def test_skill_pack_auth_provider_catalog_includes_active_external_connectors(monkeypatch) -> None:
    from nullion import skill_pack_catalog as catalog

    connection = SimpleNamespace(
        provider_id="zephyr_connector_provider",
        display_name="Zephyr connector",
        credential_scope="workspace",
        active=True,
    )
    monkeypatch.setattr(catalog, "list_installed_skill_packs", lambda: [])
    monkeypatch.setattr(
        "nullion.connections.load_connection_registry",
        lambda: SimpleNamespace(connections=[connection]),
    )
    monkeypatch.setattr(
        "nullion.connections.connection_for_principal",
        lambda principal_id, provider_id: connection if provider_id == "zephyr_connector_provider" else None,
    )

    providers = catalog.list_skill_pack_auth_providers()
    prompt = catalog.skill_pack_access_prompt(["nullion/connector-skills"], principal_id="workspace:workspace_admin")

    assert any(
        provider["skill_pack_id"] == "nullion/connector-skills"
        and provider["provider_id"] == "zephyr_connector_provider"
        and provider["required_tools"] == ["connector_request"]
        for provider in providers
    )
    assert "Zephyr connector (zephyr_connector_provider: connected)" in prompt
    assert "check enabled connector skills and active connector providers" in prompt


def test_env_connector_connection_inference_is_gateway_agnostic(tmp_path, monkeypatch) -> None:
    from nullion import connections

    monkeypatch.setattr(connections, "_CONNECTIONS_PATH", tmp_path / "connections.json")
    monkeypatch.setenv("NULLION_CONNECTOR_GATEWAY", "acme")
    monkeypatch.setenv("ACME_API_KEY", "secret")

    registry = connections.load_connection_registry()

    assert any(
        connection.provider_id == "acme_connector_provider"
        and connection.credential_ref == "ACME_API_KEY"
        for connection in registry.connections
    )


def test_image_generation_delivery_paths(tmp_path, monkeypatch) -> None:
    from nullion import image_generation_delivery as images
    from nullion.tools import ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec, ToolResult

    assert images.parse_image_generation_request("draw a skyline")
    assert images.parse_image_generation_request("make a logo")
    assert not images.parse_image_generation_request("")
    assert images.parse_image_edit_request("remove the background")
    assert images.parse_image_edit_request("add a hat to this photo")
    assert not images.parse_image_edit_request(
        "Can u add this to bug report excel sheet too. The /approvals command when listing items should show timestamps as well"
    )

    registry = ToolRegistry()
    registry.register(
        ToolSpec("image_generate", "Generate", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"path": invocation.arguments["output_path"]}),
    )
    output = tmp_path / "generated.png"
    monkeypatch.setattr(images, "artifact_path_for_generated_workspace_file", lambda **kwargs: output)
    monkeypatch.setattr(
        "nullion.runtime.invoke_tool_with_boundary_policy",
        lambda store, invocation, registry: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"path": str(output)}),
    )
    output.write_bytes(b"png")
    result = images.generate_image_artifact(SimpleNamespace(store=object()), prompt="make an image", registry=registry, principal_id="operator")
    assert result.completed
    assert result.artifact_paths == [str(output)]

    monkeypatch.setattr(
        "nullion.runtime.invoke_tool_with_boundary_policy",
        lambda store, invocation, registry: ToolResult(invocation.invocation_id, invocation.tool_name, "denied", {"approval_id": "ap-1"}),
    )
    denied = images.generate_image_artifact(SimpleNamespace(store=object()), prompt="make an image", registry=registry, principal_id="operator")
    assert denied.suspended_for_approval
    assert denied.approval_id == "ap-1"

    source = tmp_path / "source.jpg"
    source.write_bytes(b"jpg")
    seen_prompts: list[str] = []
    monkeypatch.setattr(
        "nullion.runtime.invoke_tool_with_boundary_policy",
        lambda store, invocation, registry: (
            seen_prompts.append(invocation.arguments["prompt"]),
            (_ for _ in ()).throw(RuntimeError("provider down")),
        )[1],
    )
    fallback = images.generate_image_artifact(
        SimpleNamespace(store=object()),
        prompt="remove background",
        registry=registry,
        principal_id="operator",
        source_path=str(source),
    )
    assert fallback.completed
    assert fallback.fallback_used
    assert fallback.error == "provider down"
    assert seen_prompts
    assert "Edit the attached source image" in seen_prompts[-1]
    assert "not an unchanged copy" in seen_prompts[-1]

    unconfigured = images.generate_image_artifact(SimpleNamespace(store=object()), prompt="make an image", registry=ToolRegistry(), principal_id="operator")
    assert "not configured" in unconfigured.error
    assert images.generate_image_artifact(SimpleNamespace(store=object()), prompt="hello", registry=registry, principal_id="operator").matched is False


def test_web_control_plane_authorization_and_routes(monkeypatch) -> None:
    from nullion import web_control_plane as control
    from nullion.runtime_store import RuntimeStore
    from nullion.skills import SkillRecord

    monkeypatch.setattr(control, "build_runtime_status_snapshot", lambda store: {"approval_requests": [{"id": "ap"}], "permission_grants": [{"id": "grant"}]})
    monkeypatch.setattr(control, "list_skills", lambda store: [SkillRecord("skill", "Title", "Summary", "trigger", ["step"], [], datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC))])
    monkeypatch.setattr(control, "build_skill_snapshot", lambda skill: {"skill_id": skill.skill_id})

    with pytest.raises(ValueError):
        control.create_web_control_plane_app(RuntimeStore(), bearer_token=" ")

    app = control.create_web_control_plane_app(RuntimeStore(), bearer_token="token")
    responses = []

    def start_response(status, headers):
        responses.append((status, headers))

    body = app({"PATH_INFO": "/api/status", "HTTP_AUTHORIZATION": "Bearer token"}, start_response)[0]
    assert responses[-1][0] == "200 OK"
    assert json.loads(body) == {"approval_requests": [{"id": "ap"}], "permission_grants": [{"id": "grant"}]}

    app({"PATH_INFO": "/api/status", "REQUEST_METHOD": "POST", "HTTP_AUTHORIZATION": "Bearer token"}, start_response)
    assert responses[-1][0] == "405 Method Not Allowed"
    app({"PATH_INFO": "/api/approvals", "HTTP_AUTHORIZATION": "Bearer token"}, start_response)
    assert responses[-1][0] == "200 OK"
    app({"PATH_INFO": "/api/grants", "HTTP_AUTHORIZATION": "Bearer token"}, start_response)
    assert responses[-1][0] == "200 OK"
    app({"PATH_INFO": "/api/skills", "HTTP_AUTHORIZATION": "Bearer token"}, start_response)
    assert responses[-1][0] == "200 OK"
    app({"PATH_INFO": "/missing", "HTTP_AUTHORIZATION": "Bearer token"}, start_response)
    assert responses[-1][0] == "404 Not Found"
    app({"PATH_INFO": "/api/status", "HTTP_AUTHORIZATION": "Bearer wrong"}, start_response)
    assert responses[-1][0] == "401 Unauthorized"
