"""Large provider catalogues must not erase typed source, action or output state."""
from copy import deepcopy
import json
import pytest

@pytest.mark.parametrize('surface',['chat','web','startup'])
@pytest.mark.parametrize('source',['mi cuenta elegida','選択したアカウント','my selected account'])
def test_oversized_scope_keeps_control_state_without_catalogue_prose(surface,source):
    from nullion.chat_operator import _compact_tool_output_for_context
    from nullion.web_app import _compact_web_tool_output_for_context
    from nullion.startup_maintenance import _compact_tool_output
    from nullion.source_selection import pending_source_selection
    from nullion.tools import ToolResult
    controls={'source_selection_required':True,'source_resolution_required':False,
        'source_evidence_verified':True,'connector_source_user_requested':False,
        'connector_source_evidence':source,'available_tools':[],'required_tool_names':[],
        'capabilities':['connector'],'connector_app_ids':[],
        'artifact_extensions':['.html','.zip'],'excluded_artifact_extensions':['.png'],
        'scheduler_action':'none','tracking_mode':'unspecified','suppress_activity':True}
    output={**deepcopy(controls),'active_connector_providers':[{'reference_paths':['x'*200000],
        'active_app_ids':['catalogue-only-id'],'app_inventory_kind':'supported_catalogue'}],
        'available_sources':[{'provider_id':'example','display_name':'Example','read_tools':['email_search','calendar_list'],
            'reference_paths':['catalogue details '*20000]}]}
    compact={'chat':_compact_tool_output_for_context,'web':_compact_web_tool_output_for_context,'startup':_compact_tool_output}[surface]('request_tool_scope',output)
    assert all(compact[key]==value for key,value in controls.items())
    assert compact['result_kind']=='tool_scope_evidence' and compact['truncated'] is True
    assert len(json.dumps(compact))<5000 and 'preview' not in compact
    assert 'active_connector_providers' not in compact and 'reference_paths' not in compact['available_sources'][0]
    choice=pending_source_selection([ToolResult('scope','request_tool_scope','completed',compact)])
    assert choice is not None and len(choice.options)==2
    assert choice.reply.count('\n1. ')==1 and choice.reply.count('\n2. ')==1


def test_partial_source_quote_cannot_retain_verified_claim():
    from nullion.source_selection import compact_scope_evidence
    compact=compact_scope_evidence({'connector_source_evidence':'x'*3000,'source_evidence_verified':True},original_chars=3000)
    assert compact['source_evidence_verified'] is False and 'connector_source_evidence' not in compact
