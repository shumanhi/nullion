"""Supported integrations must not become verified account inventory."""
import pytest

@pytest.mark.parametrize('ids', [('courrier-a','courrier-b'),('予定-a','予定-b')])
def test_catalogue_provenance_survives_cache_and_source_discovery(monkeypatch,ids):
    from nullion import connector_prompt_context as context,turn_context_policy as scope
    from tests.test_account_source_resolution import registry
    from nullion.tools import ToolInvocation
    provider={'provider_id':'example','display_name':'Example gateway','active_app_ids':list(ids),
        'app_inventory_kind':'supported_catalogue','structured_tools':['email_search','calendar_list']}
    monkeypatch.setattr(scope,'_active_connector_provider_context',lambda:[provider])
    monkeypatch.setattr(scope,'_runtime_has_active_connector_provider',lambda:True)
    for name in ['_ACTIVE_CONNECTOR_APP_ID_MEMORY_CACHE','_ACTIVE_CONNECTOR_PROVIDER_CONTEXT_MEMORY_CACHE']:
        monkeypatch.setattr(context,name,None)
    snapshot=context.active_connector_provider_context_snapshot()
    assert snapshot[0]['app_inventory_kind']=='supported_catalogue'
    cached=context.cached_active_connector_app_id_providers(allow_persistent=False)
    assert cached[0]['app_inventory_kind']=='supported_catalogue'
    # An ordinary cached awareness lookup must not reload provider inventory.
    monkeypatch.setattr(scope,'_active_connector_provider_context',lambda:pytest.fail('Unnecessary inventory reload'))
    assert context.active_connector_provider_context_snapshot(allow_runtime_load=False)==snapshot
    monkeypatch.setattr(scope,'_active_connector_provider_context',lambda:[provider])
    evidence=scope.build_turn_tool_evidence(user_message='私の予定表',conversation_result=None)
    scoped=scope.ScopedTurnToolRegistry(registry(),evidence=evidence)
    result,_=scoped.apply_scope_request(ToolInvocation('discover','request_tool_scope','web:catalogue',{
        'capabilities':['connector'],'source_user_requested':True,'source_evidence':'私の予定表'}))
    assert result.output['source_resolution_required'] is True
    assert result.output['available_sources'][0]['app_inventory_kind']=='supported_catalogue'
    assert not result.output['available_tools']
    assert set(result.output['available_sources'][0]['read_tools'])=={'email_search','calendar_list'}
