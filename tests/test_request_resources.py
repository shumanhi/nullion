from types import SimpleNamespace

import pytest

from nullion.request_resources import current_request_resource_scope, request_resource_scope
from nullion.tools import ToolRegistry


@pytest.mark.parametrize('failure', [False, True])
def test_request_resource_scope_keeps_recovery_passes_and_cleans_every_exit(failure):
    scopes, cleaned = [], []
    registry = ToolRegistry()
    registry.register_cleanup_hook(lambda scope: cleaned.append(scope))
    runtime = SimpleNamespace(active_tool_registry=registry)

    @request_resource_scope
    def recovery(runtime):
        scopes.append(current_request_resource_scope())

    @request_resource_scope
    def request(runtime):
        scopes.append(current_request_resource_scope())
        recovery(runtime)
        assert not cleaned
        if failure: raise ValueError('provider failure')
        return '結果'

    if failure:
        with pytest.raises(ValueError): request(runtime)
    else:
        assert request(runtime) == '結果'
    assert scopes[0] == scopes[1] and scopes[0]
    assert cleaned == [scopes[0]]
    assert current_request_resource_scope() is None
    recovery(runtime)
    assert scopes[-1] != scopes[0]
    assert cleaned[-1] == scopes[-1]
