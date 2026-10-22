from __future__ import annotations

from nullion.tools import (
    ToolInvocation,
    ToolRegistry,
    ToolResult,
    ToolRiskLevel,
    ToolSideEffectClass,
    ToolSpec,
)
from nullion.cron_execution_tools import (
    CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS,
    CRON_EXECUTION_BLOCKED_TOOLS,
    CRON_EXECUTION_STRUCTURED_READ_SIDE_EFFECTS,
    CRON_EXECUTION_STRUCTURED_WRITE_SIDE_EFFECTS,
    CronExecutionToolRegistry,
    _connected_connector_provider_summaries,
    build_cron_connector_scope_decision,
    structured_cron_passthrough_tool_names,
)


def _spec(
    name: str,
    capability_tags: tuple[str, ...] = (),
    *,
    requires_approval: bool = False,
    side_effect_class: ToolSideEffectClass = ToolSideEffectClass.READ,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=name,
        risk_level=ToolRiskLevel.LOW,
        side_effect_class=side_effect_class,
        requires_approval=requires_approval,
        timeout_seconds=1,
        capability_tags=capability_tags,
    )


def test_cron_execution_registry_hides_scheduler_capabilities() -> None:
    assert {"create_cron", "run_cron", "set_reminder", "delete_reminder", "update_reminder"}.issubset(
        CRON_EXECUTION_BLOCKED_TOOLS
    )

    registry = ToolRegistry()
    registry.register(
        _spec("file_read"),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("create_cron", ("scheduler", "cron")),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("run_cron"),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("set_reminder", ("scheduler", "reminder")),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("delete_reminder", ("scheduler", "reminder")),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("update_reminder", ("scheduler", "reminder")),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("update_cron", ("scheduler", "cron")),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    cron_registry = CronExecutionToolRegistry(registry)

    assert [spec.name for spec in cron_registry.list_specs()] == ["file_read"]
    assert [definition["name"] for definition in cron_registry.list_tool_definitions()] == [
        "file_read"
    ]
    denied = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-1",
            tool_name="create_cron",
            principal_id="telegram:42",
            arguments={},
        )
    )
    assert denied.status == "denied"
    assert denied.output["reason"] == "cron_execution_capability_denied"
    assert set(denied.output["denied_tools"]) == CRON_EXECUTION_BLOCKED_TOOLS
    assert set(denied.output["denied_capability_tags"]) == CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS
    assert set(denied.output["tool_capability_tags"]) == {"scheduler", "cron"}

    legacy_denied = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-legacy",
            tool_name="run_cron",
            principal_id="telegram:42",
            arguments={},
        )
    )
    assert legacy_denied.status == "denied"
    assert legacy_denied.output["tool_capability_tags"] == []

    reminder_denied = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-2",
            tool_name="set_reminder",
            principal_id="telegram:42",
            arguments={"text": "status", "due_in_seconds": 60},
        )
    )
    assert reminder_denied.status == "denied"
    assert set(reminder_denied.output["tool_capability_tags"]) == {"scheduler", "reminder"}

    update_denied = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-update",
            tool_name="update_cron",
            principal_id="telegram:42",
            arguments={"id": "cron-1", "schedule": "*/10 * * * *"},
        )
    )
    assert update_denied.status == "denied"
    assert set(update_denied.output["tool_capability_tags"]) == {"scheduler", "cron"}


def test_cron_execution_registry_hides_connectors_without_scope() -> None:
    registry = ToolRegistry()

    def _complete(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"provider_id": invocation.arguments.get("provider_id")},
        )

    registry.register(_spec("file_read"), _complete)
    registry.register(_spec("connector_request", ("connector",)), _complete)

    default_registry = CronExecutionToolRegistry(registry)

    assert [spec.name for spec in default_registry.list_specs()] == ["file_read"]
    assert [definition["name"] for definition in default_registry.list_tool_definitions()] == ["file_read"]
    denied = default_registry.invoke(
        ToolInvocation(
            invocation_id="tool-connector",
            tool_name="connector_request",
            principal_id="telegram:42",
            arguments={
                "provider_id": "skill_pack_connector_vendor_records",
                "url": "https://api.example.com/vendor-records",
            },
        )
    )
    assert denied.status == "denied"
    assert denied.output["reason"] == "cron_execution_connector_scope_denied"
    assert denied.output["suppress_activity"] is True

    planned_but_unscoped = CronExecutionToolRegistry(
        registry,
        allowed_tool_names=("connector_request",),
    )
    assert planned_but_unscoped.list_tool_definitions() == []
    planned_denied = planned_but_unscoped.invoke(
        ToolInvocation(
            invocation_id="tool-connector-planned",
            tool_name="connector_request",
            principal_id="telegram:42",
            arguments={
                "provider_id": "skill_pack_connector_vendor_records",
                "url": "https://api.example.com/vendor-records",
            },
        )
    )
    assert planned_denied.status == "denied"
    assert planned_denied.output["reason"] == "cron_execution_connector_scope_denied"

    allowed_registry = CronExecutionToolRegistry(
        registry,
        allowed_tool_names=("connector_request",),
        allow_connector_tools=True,
        connector_provider_ids=("skill_pack_connector_vendor_records",),
    )
    assert [definition["name"] for definition in allowed_registry.list_tool_definitions()] == [
        "connector_request"
    ]
    wrong_provider = allowed_registry.invoke(
        ToolInvocation(
            invocation_id="tool-connector-wrong-provider",
            tool_name="connector_request",
            principal_id="telegram:42",
            arguments={"provider_id": "other_connector_provider", "url": "https://api.example.com/"},
        )
    )
    assert wrong_provider.status == "denied"
    assert wrong_provider.output["reason"] == "cron_execution_connector_provider_denied"
    assert wrong_provider.output["suppress_activity"] is True

    completed = allowed_registry.invoke(
        ToolInvocation(
            invocation_id="tool-connector-allowed",
            tool_name="connector_request",
            principal_id="telegram:42",
            arguments={
                "provider_id": "skill_pack_connector_vendor_records",
                "url": "https://api.example.com/vendor-records",
            },
        )
    )
    assert completed.status == "completed"


def test_cron_execution_registry_enforces_connector_provider_ids_from_invocation_arguments() -> None:
    registry = ToolRegistry()

    def _complete(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"provider_id": invocation.arguments.get("provider_id")},
        )

    registry.register(_spec("vendor_record_search", ("records", "connector")), _complete)

    cron_registry = CronExecutionToolRegistry(
        registry,
        allow_connector_tools=True,
        connector_provider_ids=("provider-a",),
    )

    wrong_provider = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-wrong-provider",
            tool_name="vendor_record_search",
            principal_id="telegram:42",
            arguments={"provider_id": "provider-b"},
        )
    )
    assert wrong_provider.status == "denied"
    assert wrong_provider.output["reason"] == "cron_execution_connector_provider_denied"

    completed = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-record-search",
            tool_name="vendor_record_search",
            principal_id="telegram:42",
            arguments={"provider_id": "provider-a"},
        )
    )
    assert completed.status == "completed"
    assert completed.output["provider_id"] == "provider-a"


def test_cron_execution_registry_exposes_structured_connector_tools_from_metadata_without_gateway_scope() -> None:
    assert CRON_EXECUTION_STRUCTURED_READ_SIDE_EFFECTS == {"read"}
    assert CRON_EXECUTION_STRUCTURED_WRITE_SIDE_EFFECTS == {"account_write"}
    registry = ToolRegistry()

    def _complete(invocation: ToolInvocation) -> ToolResult:
        if invocation.tool_name == "vendor_record_send":
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "denied",
                {"reason": "approval_required", "requires_approval": True, "approval_id": "approval-send-1"},
                error="Approval required for tool: vendor_record_send",
            )
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"tool": invocation.tool_name},
        )

    registry.register(_spec("file_read"), _complete)
    registry.register(
        _spec("connector_request", ("connector",), side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE),
        _complete,
    )
    registry.register(_spec("vendor_record_search", ("records", "connector")), _complete)
    registry.register(_spec("generic_connector_probe", ("connector",)), _complete)
    registry.register(
        _spec(
            "vendor_record_send",
            ("records", "connector"),
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
        ),
        _complete,
    )
    registry.register(
        _spec(
            "vendor_record_mutate",
            ("records", "connector"),
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
        ),
        _complete,
    )

    structured_tool_names = structured_cron_passthrough_tool_names(registry)
    cron_registry = CronExecutionToolRegistry(
        registry,
        allowed_structured_tool_names=structured_tool_names,
    )

    assert set(structured_tool_names) == {"vendor_record_search"}
    assert {spec.name for spec in cron_registry.list_specs()} == {
        "file_read",
        "vendor_record_search",
    }
    assert {definition["name"] for definition in cron_registry.list_tool_definitions()} == {
        "file_read",
        "vendor_record_search",
    }

    completed = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-record-search",
            tool_name="vendor_record_search",
            principal_id="telegram:42",
            arguments={"limit": 1},
        )
    )
    assert completed.status == "completed"
    assert completed.output["tool"] == "vendor_record_search"

    denied_gateway = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-gateway",
            tool_name="connector_request",
            principal_id="telegram:42",
            arguments={"provider_id": "skill_pack_connector_example", "url": "https://example.com"},
        )
    )
    assert denied_gateway.status == "denied"
    assert denied_gateway.output["reason"] == "cron_execution_connector_scope_denied"

    denied_structured_write = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-send",
            tool_name="vendor_record_send",
            principal_id="telegram:42",
            arguments={},
        )
    )
    assert denied_structured_write.status == "denied"
    assert denied_structured_write.output["reason"] == "cron_execution_account_write_denied"

    denied_write = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-write-probe",
            tool_name="vendor_record_mutate",
            principal_id="telegram:42",
            arguments={},
        )
    )
    assert denied_write.status == "denied"
    assert denied_write.output["reason"] == "cron_execution_account_write_denied"

    denied_unstructured_read = cron_registry.invoke(
        ToolInvocation(
            invocation_id="tool-generic-read",
            tool_name="generic_connector_probe",
            principal_id="telegram:42",
            arguments={},
        )
    )
    assert denied_unstructured_read.status == "denied"
    assert denied_unstructured_read.output["reason"] == "cron_execution_connector_scope_denied"

    scoped_registry = CronExecutionToolRegistry(
        registry,
        allowed_structured_tool_names=structured_tool_names,
        allow_connector_tools=True,
    )
    assert "vendor_record_send" not in {
        definition["name"] for definition in scoped_registry.list_tool_definitions()
    }
    scoped_denied_write = scoped_registry.invoke(
        ToolInvocation(
            invocation_id="tool-send-scoped",
            tool_name="vendor_record_send",
            principal_id="telegram:42",
            arguments={},
        )
    )
    assert scoped_denied_write.status == "denied"
    assert scoped_denied_write.output["reason"] == "cron_execution_account_write_denied"


def test_cron_execution_registry_lists_approval_gated_tools_for_structured_planning() -> None:
    registry = ToolRegistry()
    registry.register(
        _spec("terminal_exec", requires_approval=True),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("file_read"),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    cron_registry = CronExecutionToolRegistry(registry)
    listed_names = [spec.name for spec in cron_registry.list_specs()]
    listed_definitions = [definition["name"] for definition in cron_registry.list_tool_definitions()]

    assert set(listed_names) == {"terminal_exec", "file_read"}
    assert set(listed_definitions) == {"terminal_exec", "file_read"}


def test_cron_connector_scope_decision_requires_model_and_connected_provider(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        _spec("connector_request", ("connector",)),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    monkeypatch.setattr(
        "nullion.cron_execution_tools._connected_connector_provider_summaries",
        lambda principal_id: (
            {
                "provider_id": "skill_pack_connector_vendor_records",
                "display_name": "Vendor records connector",
                "permission_mode": "read",
            },
        ),
    )

    class FakeModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            _ = kwargs
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            '{"requires_account_connector": true, '
                            '"provider_ids": ["skill_pack_connector_vendor_records"], '
                            '"confidence": 0.91}'
                        ),
                    }
                ]
            }

    decision = build_cron_connector_scope_decision(
        model_client=FakeModelClient(),
        user_message="Ejecuta la tarea programada usando el proveedor conectado descrito.",
        principal_id="telegram:42",
        registry=registry,
        planned_tool_names=("connector_request",),
    )

    assert decision.valid is True
    assert decision.allow_connector_tools is True
    assert decision.provider_ids == ("skill_pack_connector_vendor_records",)

    no_plan_decision = build_cron_connector_scope_decision(
        model_client=FakeModelClient(),
        user_message="Structured cron payload without planner output yet.",
        principal_id="telegram:42",
        registry=registry,
        planned_tool_names=None,
    )

    assert no_plan_decision.valid is True
    assert no_plan_decision.allow_connector_tools is True
    assert no_plan_decision.provider_ids == ("skill_pack_connector_vendor_records",)

    skipped_calls: list[dict] = []

    class SkippedModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            skipped_calls.append(kwargs)
            return {"content": []}

    skipped = build_cron_connector_scope_decision(
        model_client=SkippedModelClient(),
        user_message='{"connector_provider_id":"skill_pack_connector_vendor_records"}',
        principal_id="telegram:42",
        registry=registry,
        planned_tool_names=("web_fetch",),
    )

    assert skipped_calls == []
    assert skipped.valid is True
    assert skipped.allow_connector_tools is True
    assert skipped.provider_ids == ("skill_pack_connector_vendor_records",)

    plain_text_calls: list[dict] = []

    class PlainTextModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            plain_text_calls.append(kwargs)
            return {"content": []}

    plain_text = build_cron_connector_scope_decision(
        model_client=PlainTextModelClient(),
        user_message="skill_pack_connector_vendor_records",
        principal_id="telegram:42",
        registry=registry,
        planned_tool_names=("web_fetch",),
    )

    assert plain_text_calls
    assert plain_text.valid is False
    assert plain_text.allow_connector_tools is False

    class InventingModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            _ = kwargs
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            '{"requires_account_connector": true, '
                            '"provider_ids": ["missing_connector_provider"], '
                            '"confidence": 0.99}'
                        ),
                    }
                ]
            }

    invented = build_cron_connector_scope_decision(
        model_client=InventingModelClient(),
        user_message="No English trigger words here.",
        principal_id="telegram:42",
        registry=registry,
        planned_tool_names=("connector_request",),
    )

    assert invented.valid is True
    assert invented.allow_connector_tools is False
    assert invented.provider_ids == ()


def test_cron_connector_scope_decision_uses_connector_capability_tags(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        _spec("vendor_record_search", ("records", "connector")),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("web_fetch", ("public_web",)),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    monkeypatch.setattr(
        "nullion.cron_execution_tools._connected_connector_provider_summaries",
        lambda principal_id: (
            {
                "provider_id": "skill_pack_connector_vendor_records",
                "display_name": "Vendor records connector",
                "permission_mode": "read",
            },
        ),
    )

    calls: list[dict] = []

    class FakeModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            calls.append(kwargs)
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            '{"requires_account_connector": true, '
                            '"provider_ids": ["skill_pack_connector_vendor_records"], '
                            '"confidence": 0.88}'
                        ),
                    }
                ]
            }

    decision = build_cron_connector_scope_decision(
        model_client=FakeModelClient(),
        user_message="Structured scheduled task payload.",
        principal_id="telegram:42",
        registry=registry,
        planned_tool_names=("vendor_record_search",),
    )

    assert calls
    assert decision.valid is True
    assert decision.allow_connector_tools is True
    assert decision.provider_ids == ("skill_pack_connector_vendor_records",)

    calls.clear()
    scoped_from_model = build_cron_connector_scope_decision(
        model_client=FakeModelClient(),
        user_message="Structured scheduled task payload.",
        principal_id="telegram:42",
        registry=registry,
        planned_tool_names=("web_fetch",),
    )

    assert calls
    assert scoped_from_model.valid is True
    assert scoped_from_model.allow_connector_tools is True
    assert scoped_from_model.provider_ids == ("skill_pack_connector_vendor_records",)


def test_cron_connector_provider_summaries_include_active_app_ids(monkeypatch) -> None:
    class FakeConnection:
        provider_id = "skill_pack_connector_vendor_records"
        display_name = "Vendor records connector"
        permission_mode = "write"
        active = True
        provider_profile = None

    class FakeRegistry:
        connections = [FakeConnection()]

    monkeypatch.setattr("nullion.connections.load_connection_registry", lambda: FakeRegistry())
    monkeypatch.setattr(
        "nullion.connections.connection_for_principal",
        lambda principal_id, provider_id: FakeConnection(),
    )
    monkeypatch.setattr(
        "nullion.turn_context_policy._active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_vendor_records",
                "skill_pack_id": "vendor/records-skill",
                "active_app_ids": ["vendor-records", "vendor-calendar"],
                "structured_tools": ["connector_request", "vendor_record_search", "vendor_record_read"],
                "reference_paths": ["references/vendor-records/README.md"],
            }
        ],
    )

    summaries = _connected_connector_provider_summaries("telegram:42")

    assert summaries == (
        {
            "provider_id": "skill_pack_connector_vendor_records",
            "display_name": "Vendor records connector",
            "permission_mode": "write",
            "active_app_ids": ["vendor-records", "vendor-calendar"],
            "structured_tools": ["connector_request", "vendor_record_search", "vendor_record_read"],
            "skill_pack_id": "vendor/records-skill",
        },
    )


def test_cron_connector_scope_allows_exact_active_app_id_without_model(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        _spec("vendor_record_search", ("records", "connector")),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    monkeypatch.setattr(
        "nullion.cron_execution_tools._connected_connector_provider_summaries",
        lambda principal_id: (
            {
                "provider_id": "skill_pack_connector_vendor_records",
                "display_name": "Vendor records connector",
                "permission_mode": "write",
                "active_app_ids": ["vendor-records"],
                "structured_tools": ["vendor_record_search", "vendor_record_read"],
            },
        ),
    )
    calls: list[dict] = []

    class FakeModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            calls.append(kwargs)
            return {"content": []}

    decision = build_cron_connector_scope_decision(
        model_client=FakeModelClient(),
        user_message='{"selected_app_id":"vendor-records"}',
        principal_id="telegram:42",
        registry=registry,
        planned_tool_names=None,
    )

    assert calls == []
    assert decision.valid is True
    assert decision.allow_connector_tools is True
    assert decision.provider_ids == ("skill_pack_connector_vendor_records",)
