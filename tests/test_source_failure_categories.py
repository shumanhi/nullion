"""Source failure classification uses exception types, never error wording."""
import errno
import socket
import urllib.error

import pytest
from nullion.tools import ToolInvocation,_build_web_fetch_handler


@pytest.mark.parametrize('failure,reason,status',[
    (urllib.error.HTTPError('https://example.org',401,'private account body',{},None),'http_error',401),
    (urllib.error.HTTPError('https://example.org',429,'private account body',{},None),'http_error',429),
    (urllib.error.HTTPError('https://example.org',503,'private account body',{},None),'http_error',503),
    (urllib.error.URLError(TimeoutError('タイムアウト')),'handler_timeout',None),
    (TimeoutError('Délai dépassé'),'handler_timeout',None),
    (PermissionError('秘密のパス'),'source_request_failed',None),
    (OSError(errno.ENOSPC,'private file path'),'source_request_failed',None),
    (OSError(errno.EADDRNOTAVAIL,'Adresse indisponible'),'network_error',None),
    (socket.gaierror(-2,'private hostname'),'network_error',None),
    (ConnectionError('Connexion refusée'),'network_error',None),
])
def test_http_rejections_are_not_network_outages(failure,reason,status):
    def fetch(url,timeout):raise failure
    result=_build_web_fetch_handler(fetch)(ToolInvocation('source','web_fetch','web:test',{'url':'https://example.org'}))
    assert result.status=='failed' and result.output['reason']==reason
    assert result.output.get('status_code')==status
    assert result.output['error_type']==type(failure).__name__
    assert not any('private' in str(value) or '秘密' in str(value) for value in result.output.values())


@pytest.mark.parametrize('tool,extra',[
    ('web_fetch',{}),('weather_forecast',{}),('browser_extract_text',{}),
    ('connector_request',{}),('archive_extract',{}),
    ('opaque_source',{'result_kind':'source_observations','observations':[]}),
    ('opaque_source',{'result_kind':'structured_source_record','record':{}}),
])
def test_typed_failure_facts_survive_specialized_context_projections(tool,extra):
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context,_compact_tool_output_for_repair
    facts={'reason':'http_error','status_code':429,'error_type':'HTTPError',
        'requires_approval':True,'retryable':False,'boundary_kind':'outbound_network'}
    for project in [_compact_tool_output_for_model_context,_compact_tool_output_for_repair]:
        output=project(tool,{**facts,**extra})
        assert all(output.get(key)==value for key,value in facts.items()),(project.__name__,output)


def test_recovery_overlay_keeps_binary_data_out_of_model_context():
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context,_compact_tool_output_for_repair
    for project in [_compact_tool_output_for_model_context,_compact_tool_output_for_repair]:
        projected=project('weather_forecast',{'reason':b'private binary content'})
        assert projected['reason']['body_omitted'] is True
        assert 'private binary content' not in str(projected)


@pytest.mark.parametrize('tool',['browser_extract_items','weather_forecast','opaque_source'])
def test_nested_recovery_contracts_keep_scalar_types_and_plan_shape(tool):
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context,_compact_tool_output_for_repair
    recovery={'kind':'repair_precondition','precondition':{'available_scopes':[
        {'selector':'a:has(span.entry-name)','visible_link_count':55,'visible':True,
         'sample_text':['見出し 0','見出し 1']} ]},
        'argument_constraints':{'probe':[{'tool_name':'browser_extract_items','arguments':
            {'max_items':100,'selector':'a:has(span.entry-name)','fields':['title','canonical_url']}}]}}
    for project in [_compact_tool_output_for_model_context,_compact_tool_output_for_repair]:
        output=project(tool,{'reason':'tool_precondition_unmet','recovery':recovery})
        assert output['recovery']==recovery
        scope=output['recovery']['precondition']['available_scopes'][0]
        assert type(scope['visible_link_count']) is int and scope['visible'] is True


def test_failed_http_source_closes_its_error_response_body():
    from io import BytesIO
    body=BytesIO(b'private response body')
    failure=urllib.error.HTTPError('https://example.org',403,'Denied',{},body)
    def fetch(url,timeout):raise failure
    result=_build_web_fetch_handler(fetch)(ToolInvocation('source','web_fetch','web:test',{'url':'https://example.org'}))
    try:
        assert result.output['status_code']==403
        assert body.closed, 'The HTTP error response is owned by the caught request'
        assert 'private response body' not in str(result.output) and 'private response body' not in str(result.error)
    finally:failure.close()


def test_response_cleanup_error_does_not_replace_the_original_http_failure():
    from io import BytesIO
    class Body(BytesIO):
        def close(self):
            super().close()
            raise OSError('private cleanup detail')
    body=Body(b'private body')
    failure=urllib.error.HTTPError('https://example.org',503,'Unavailable',{},body)
    def fetch(url,timeout):raise failure
    result=_build_web_fetch_handler(fetch)(ToolInvocation('source','web_fetch','web:test',{'url':'https://example.org'}))
    assert result.status=='failed' and result.output['status_code']==503
    assert body.closed and 'private cleanup detail' not in str(result.error)


@pytest.mark.parametrize('boundary',['connector','media_json','media_image','connection_test','loading_screen'])
@pytest.mark.parametrize('read_fails',[False,True])
def test_caught_http_responses_close_across_request_boundaries(monkeypatch,tmp_path,boundary,read_fails):
    from io import BytesIO
    from nullion import tools,providers,web_app,webview_app
    class Body(BytesIO):
        def read(self,*args):
            if read_fails:raise OSError('synthetic body read failure')
            return super().read(*args)
    body=Body(b'{"error":{"message":"service unavailable"}}')
    failure=urllib.error.HTTPError('https://example.org',503,'Unavailable',{},body)
    def request(*args,**kwargs):raise failure
    monkeypatch.setattr(providers.urllib.request,'urlopen',request)
    monkeypatch.setattr(webview_app,'urlopen',request)
    try:
        if boundary=='connector':
            if read_fails:
                with pytest.raises(OSError):tools._connector_http_error_output(failure,provider_id='fixture')
            else:
                output,error=tools._connector_http_error_output(failure,provider_id='fixture')
                assert output['status_code']==503
        elif boundary=='media_json':
            with pytest.raises(RuntimeError,match='503'):
                providers._request_gemini_json('https://example.org',api_key='fixture',payload={})
        elif boundary=='media_image':
            with pytest.raises(RuntimeError,match='503'):
                providers._openrouter_image_generate('fixture','fixture','fixture',str(tmp_path/'image.png'),None)
            assert not (tmp_path/'image.png').exists()
        elif boundary=='connection_test':
            with pytest.raises(OSError if read_fails else RuntimeError):
                web_app._custom_api_connection_test_details({'api_base_url':'https://example.org','token_value':'fixture'})
        else:
            result=webview_app._check_loading_screen_health('http://127.0.0.1:8742')
            assert result['status']==503 and result['ok'] is False
        assert body.closed,boundary
    finally:failure.close()
