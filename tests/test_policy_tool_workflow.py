from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from nullion.approvals import create_boundary_permit
from nullion.policy import (
    BoundaryFact,
    BoundaryKind,
    GLOBAL_PERMISSION_PRINCIPAL,
    BoundaryPolicyRequest,
    BoundaryPolicyRule,
    PolicyDecision,
    PolicyMode,
    evaluate_boundary_request,
    normalize_outbound_network_selector,
)
from nullion.tool_boundaries import extract_boundary_facts
from nullion.runtime_store import RuntimeStore
from nullion.tools import ToolExecutor, ToolInvocation, ToolRegistry, ToolResult, ToolSideEffectClass, ToolSpec, ToolRiskLevel


def _invocation(tool_name: str, arguments: dict[str, object]) -> ToolInvocation:
    return ToolInvocation(
        invocation_id=f"inv-{tool_name}",
        tool_name=tool_name,
        principal_id="telegram:123",
        arguments=arguments,
    )


def test_boundary_extraction_covers_web_search_connector_terminal_and_files() -> None:
    web_search = extract_boundary_facts(_invocation("web_search", {"query": "nflx"}))
    connector = extract_boundary_facts(
        _invocation("connector_request", {"url": "https://api.example.com/events", "provider_id": "acme"})
    )
    terminal = extract_boundary_facts(_invocation("terminal_exec", {"command": "curl https://example.com/path"}))
    file_read = extract_boundary_facts(_invocation("file_read", {"path": "/tmp/report.txt"}))

    assert web_search[0].kind is BoundaryKind.OUTBOUND_NETWORK
    assert web_search[0].target == "https://www.bing.com/*"
    assert [fact.kind for fact in connector] == [BoundaryKind.OUTBOUND_NETWORK, BoundaryKind.ACCOUNT_ACCESS]
    assert connector[1].target == "acme"
    assert terminal[0].attributes["command_family"] == "curl"
    assert terminal[0].attributes["host"] == "example.com"
    assert file_read[0].kind is BoundaryKind.FILESYSTEM_ACCESS


def test_boundary_policy_allows_public_domain_but_hard_denies_private_network_even_with_wildcard_allow() -> None:
    rule = BoundaryPolicyRule(
        rule_id="allow-all",
        principal_id="global:operator",
        kind=BoundaryKind.OUTBOUND_NETWORK,
        mode=PolicyMode.ALLOW,
        selector="*",
        created_by="operator",
        created_at=datetime.now(UTC),
    )
    public_request = BoundaryPolicyRequest(
        principal_id="global:operator",
        boundary=BoundaryFact(
            kind=BoundaryKind.OUTBOUND_NETWORK,
            tool_name="web_fetch",
            operation="http_get",
            target="https://www.stockanalysis.com/stocks/nflx/",
            attributes={"address_class": "public", "host": "www.stockanalysis.com", "scheme": "https"},
        ),
    )
    private_request = BoundaryPolicyRequest(
        principal_id="global:operator",
        boundary=BoundaryFact(
            kind=BoundaryKind.OUTBOUND_NETWORK,
            tool_name="web_fetch",
            operation="http_get",
            target="http://127.0.0.1:8000/admin",
            attributes={"address_class": "loopback", "host": "127.0.0.1", "scheme": "http"},
        ),
    )

    assert evaluate_boundary_request(public_request, [rule]) is PolicyDecision.ALLOW
    assert evaluate_boundary_request(private_request, [rule]) is PolicyDecision.DENY


def test_boundary_policy_domain_normalization_groups_www_and_subdomains() -> None:
    assert normalize_outbound_network_selector("https://www.bing.com/search?q=x") == "bing.com"
    assert normalize_outbound_network_selector("https://finance.stockanalysis.com/stocks/nflx/") == "stockanalysis.com"
    assert normalize_outbound_network_selector("https://mail.google.co.uk/inbox") == "google.co.uk"


def test_boundary_policy_specific_deny_wins_over_wildcard_allow() -> None:
    now = datetime.now(UTC)
    rules = [
        BoundaryPolicyRule(
            rule_id="allow-all",
            principal_id="global:operator",
            kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.ALLOW,
            selector="*",
            created_by="operator",
            created_at=now,
        ),
        BoundaryPolicyRule(
            rule_id="deny-example",
            principal_id="global:operator",
            kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.DENY,
            selector="example.com",
            created_by="operator",
            created_at=now,
        ),
    ]
    request = BoundaryPolicyRequest(
        principal_id="global:operator",
        boundary=BoundaryFact(
            kind=BoundaryKind.OUTBOUND_NETWORK,
            tool_name="web_fetch",
            operation="http_get",
            target="https://www.example.com/path",
            attributes={"address_class": "public", "host": "www.example.com", "scheme": "https"},
        ),
    )

    assert evaluate_boundary_request(request, rules) is PolicyDecision.DENY


def test_boundary_policy_ignores_rules_for_other_principals_and_expired_rules() -> None:
    from datetime import timedelta

    now = datetime.now(UTC)
    request = BoundaryPolicyRequest(
        principal_id="global:operator",
        boundary=BoundaryFact(
            kind=BoundaryKind.OUTBOUND_NETWORK,
            tool_name="web_fetch",
            operation="http_get",
            target="https://example.com/path",
            attributes={"address_class": "public", "host": "example.com", "scheme": "https"},
        ),
    )
    rules = [
        BoundaryPolicyRule(
            rule_id="other-principal",
            principal_id="telegram:123",
            kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.ALLOW,
            selector="example.com",
            created_by="operator",
            created_at=now,
        ),
        BoundaryPolicyRule(
            rule_id="expired",
            principal_id="global:operator",
            kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.ALLOW,
            selector="example.com",
            created_by="operator",
            created_at=now,
            expires_at=now - timedelta(seconds=1),
        ),
    ]

    assert evaluate_boundary_request(request, rules) is PolicyDecision.REQUIRE_APPROVAL


def test_tool_registry_registers_invokes_and_lists_tool_definitions() -> None:
    registry = ToolRegistry()

    def handler(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="ok",
            output={"echo": invocation.arguments["value"]},
        )

    registry.register(
        ToolSpec(
            name="echo",
            description="Echo a value",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {"value": {"type": "string"}}},
        ),
        handler,
    )
    registry.mark_plugin_installed("test_plugin")

    result = registry.invoke(_invocation("echo", {"value": "hello"}))

    assert result.output == {"echo": "hello"}
    assert registry.list_installed_plugins() == ["test_plugin"]
    assert registry.list_tool_definitions() == [
        {
            "name": "echo",
            "description": "Echo a value",
            "input_schema": {"type": "object", "properties": {"value": {"type": "string"}}},
        }
    ]


def test_tool_executor_records_each_domain_used_by_allow_all_web_domains() -> None:
    store = RuntimeStore()
    permit = create_boundary_permit(
        approval_id="approval-all-web",
        principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector="*",
        granted_by="operator",
        uses_remaining=100,
    )
    store.add_boundary_permit(permit)
    registry = ToolRegistry()

    def handler(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"url": invocation.arguments["url"], "status_code": 200},
        )

    registry.register(
        ToolSpec(
            name="web_fetch",
            description="Fetch a URL",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        handler,
    )
    executor = ToolExecutor(store=store, registry=registry)

    result = executor.invoke(_invocation("web_fetch", {"url": "https://www.yahoo.com/"}))

    assert result.status == "completed"
    events = [event for event in store.list_events() if event.event_type == "boundary_permit.wildcard_access"]
    assert len(events) == 1
    assert events[0].payload["domain"] == "yahoo.com"
    assert events[0].payload["target"] == "https://www.yahoo.com/"
    assert events[0].payload["tool_name"] == "web_fetch"


def test_configured_connector_request_is_preapproved_for_configured_base_url(monkeypatch) -> None:
    provider_id = "skill_pack_connector_zephyr_api_gateway_skill"
    connection = SimpleNamespace(
        provider_id=provider_id,
        display_name="Zephyr gateway",
        provider_profile="https://example.com/",
        credential_ref="ZEPHYR_TOKEN",
        active=True,
    )
    monkeypatch.setattr(
        "nullion.connections.connection_for_principal",
        lambda principal_id, candidate: connection if candidate == provider_id else None,
    )
    store = RuntimeStore()
    registry = ToolRegistry()

    def handler(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True})

    registry.register(
        ToolSpec(
            name="connector_request",
            description="Connector request",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        handler,
    )
    executor = ToolExecutor(store=store, registry=registry)

    result = executor.invoke(
        _invocation(
            "connector_request",
            {
                "provider_id": provider_id,
                "url": "https://example.com/mail/v1/messages",
            },
        )
    )

    assert result.status == "completed"
    assert not store.list_approval_requests()


def test_configured_connector_request_is_preapproved_for_installed_skill_host(monkeypatch, tmp_path) -> None:
    provider_id = "skill_pack_connector_zephyr_api_gateway_skill"
    pack_root = tmp_path / "pack"
    pack_root.mkdir()
    (pack_root / "SKILL.md").write_text(
        "Base URL: https://example.com/{app}/{native-api-path}\n",
        encoding="utf-8",
    )
    pack = SimpleNamespace(pack_id="zephyr/api-gateway-skill", path=str(pack_root))
    connection = SimpleNamespace(
        provider_id=provider_id,
        display_name="Zephyr gateway",
        credential_ref="ZEPHYR_TOKEN",
        active=True,
    )
    monkeypatch.setattr("nullion.skill_pack_installer.list_installed_skill_packs", lambda: [pack])
    monkeypatch.setattr("nullion.skill_pack_installer.get_installed_skill_pack", lambda pack_id: pack)
    monkeypatch.setattr(
        "nullion.connections.connection_for_principal",
        lambda principal_id, candidate: connection if candidate == provider_id else None,
    )
    store = RuntimeStore()
    registry = ToolRegistry()

    def handler(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True})

    registry.register(
        ToolSpec(
            name="connector_request",
            description="Connector request",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        handler,
    )
    executor = ToolExecutor(store=store, registry=registry)

    result = executor.invoke(
        _invocation(
            "connector_request",
            {
                "provider_id": provider_id,
                "url": "https://example.com/mail/v1/messages",
            },
        )
    )

    assert result.status == "completed"
    assert not store.list_approval_requests()


def test_connector_write_request_still_requires_account_approval(monkeypatch) -> None:
    provider_id = "skill_pack_connector_zephyr_api_gateway_skill"
    connection = SimpleNamespace(
        provider_id=provider_id,
        display_name="Zephyr gateway",
        provider_profile="https://example.com/",
        credential_ref="ZEPHYR_TOKEN",
        permission_mode="write",
        active=True,
    )
    monkeypatch.setattr(
        "nullion.connections.connection_for_principal",
        lambda principal_id, candidate: connection if candidate == provider_id else None,
    )
    store = RuntimeStore()
    registry = ToolRegistry()

    def handler(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True})

    registry.register(
        ToolSpec(
            name="connector_request",
            description="Connector request",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=False,
            timeout_seconds=5,
        ),
        handler,
    )
    executor = ToolExecutor(store=store, registry=registry)

    result = executor.invoke(
        _invocation(
            "connector_request",
            {
                "provider_id": provider_id,
                "url": "https://example.com/mail/v1/send",
                "method": "POST",
                "json": {"to": "you@example.com"},
            },
        )
    )

    assert result.status == "denied"
    assert result.output["reason"] == "approval_required"
    approval = store.list_approval_requests()[0]
    assert approval.request_kind == "boundary_policy"
    assert approval.context["operation"] == "write"
