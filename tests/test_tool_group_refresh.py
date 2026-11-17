import pytest

from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


def _register(registry, name, value):
    registry.register(
        ToolSpec(name=name, description=name, risk_level=ToolRiskLevel.LOW,
                 side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=5),
        lambda invocation: ToolResult(invocation.invocation_id, name, "completed", {"value": value}),
    )


def test_replace_tool_group_discovers_all_members_and_only_retires_owned_resources():
    live = ToolRegistry()
    retired, unrelated, active = [], [], []
    for name in ("browser_navigate", "browser_read_pages", "browser_future_capability"):
        _register(live, name, "old")
    _register(live, "other_tool", "preserved")
    live.mark_plugin_installed("browser_plugin")
    live.mark_plugin_installed("other_plugin")
    live.register_cleanup_hook(retired.append, group="browser_plugin")
    live.register_cleanup_hook(unrelated.append, group="other_plugin")
    replacement = ToolRegistry()
    _register(replacement, "browser_navigate", "new")
    _register(replacement, "browser_read_pages", "new")
    replacement.mark_plugin_installed("browser_plugin")
    replacement.register_cleanup_hook(active.append, group="browser_plugin")
    revision = live.registry_revision
    live.replace_tool_group(replacement, tool_prefix="browser_", cleanup_group="browser_plugin", plugin_names=("browser_plugin",))
    assert live.registry_revision > revision
    assert {s.name for s in live.list_specs()} == {"browser_navigate", "browser_read_pages", "other_tool"}
    assert live.list_installed_plugins() == ["browser_plugin", "other_plugin"]
    result = live.invoke(ToolInvocation("probe", "browser_navigate", "operator", {"url": "https://example.com"}))
    assert result.output["value"] == "new"
    assert retired == [None]
    assert unrelated == active == []
    live.run_cleanup_hooks(scope_id="request")
    assert retired == [None]
    assert unrelated == active == ["request"]
    live.replace_tool_group(ToolRegistry(), tool_prefix="browser_", cleanup_group="browser_plugin", plugin_names=("browser_plugin",))
    assert {s.name for s in live.list_specs()} == {"other_tool"}
    assert live.list_installed_plugins() == ["other_plugin"]
    assert active == ["request", None]
    assert unrelated == ["request"]


def test_invalid_tool_group_replacement_preserves_live_registry():
    live, replacement = ToolRegistry(), ToolRegistry()
    _register(live, "browser_navigate", "working")
    _register(replacement, "unrelated_tool", "wrong family")
    revision = live.registry_revision
    with pytest.raises(ValueError, match="outside"):
        live.replace_tool_group(replacement, tool_prefix="browser_", cleanup_group="browser_plugin")
    assert live.registry_revision == revision
    assert [s.name for s in live.list_specs()] == ["browser_navigate"]
