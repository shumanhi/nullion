import asyncio
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest
from pydantic import BaseModel, ConfigDict


@pytest.mark.parametrize('response_kind', ['native', 'json_text', 'invalid_native', 'incomplete'])
def test_browser_model_uses_native_schema_without_an_extra_request(monkeypatch, response_kind):
    from nullion.plugins.browser_plugin.browser_use_adapter import NullionBrowserModel
    for name in ['browser_use', 'browser_use.llm', 'browser_use.llm.views']:
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    sys.modules['browser_use.llm.views'].ChatInvokeCompletion = lambda **kwargs: SimpleNamespace(**kwargs)
    class Decision(BaseModel):
        model_config = ConfigDict(extra='forbid')
        value: str
    calls = []
    class Client:
        def create(self, **kwargs):
            calls.append(kwargs)
            assert kwargs['tools'][0]['name'] == 'browser_decision'
            assert kwargs['tools'][0]['input_schema'] == Decision.model_json_schema()
            assert not any('Return valid JSON matching this schema' in str(m.get('content')) for m in kwargs['messages'])
            if response_kind == 'incomplete': return {'completion':{'status':'incomplete','reason':'max_output_tokens'}}
            if response_kind == 'json_text': return {'content':[{'type':'text','text':json.dumps({'value':'確認'})}]}
            return {'completion':{'usage':{'input_tokens':120,'output_tokens':9}},'content':[{'type':'tool_use','name':'browser_decision','input':{'value':'確認', **({'unexpected':1} if response_kind == 'invalid_native' else {})}}]}
    model = NullionBrowserModel(Client())
    if response_kind in {'invalid_native','incomplete'}:
        with pytest.raises(ValueError): asyncio.run(model.ainvoke([], output_format=Decision))
    else:
        assert asyncio.run(model.ainvoke([], output_format=Decision)).completion.value == '確認'
    assert len(calls) == 1
    if response_kind == 'native':
        assert model._timings[-1]['input_tokens'] == 120 and model._timings[-1]['output_tokens'] == 9
