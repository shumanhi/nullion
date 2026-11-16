"""Primary answer ownership across weather follow-ups and adjacent quote fields."""
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('followup', ['And tomorrow?', '明日は？', '¿Y mañana?'])
def test_weather_followup_answers_requested_day_instead_of_current_template(tmp_path, monkeypatch, followup):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, register_weather_forecast_tool
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    from nullion.preferences import save_preferences
    save_preferences({'timezone':'America/Chicago'})
    today = datetime.now(ZoneInfo('America/Chicago')).date()
    dates = [(today+timedelta(days=offset)).isoformat() for offset in range(3)]
    registry = ToolRegistry(); fetched = []
    def network(url, timeout):
        fetched.append(url)
        if '/search?' in url:
            return {'results':[{'name':'Harbor City','latitude':41,'longitude':-90,'country':'Example', 'timezone':'America/Chicago'}]}
        return {'timezone':'America/Chicago',
            'current':{'time':dates[0]+'T19:00','temperature_2m':40,'apparent_temperature':35,'weather_code':3},
            'daily':{'time':dates,
                'temperature_2m_max':[47,69,71], 'temperature_2m_min':[36,50,52],
                'precipitation_probability_max':[72,10,5], 'weather_code':[73,3,0]}}
    register_weather_forecast_tool(registry, json_get=network)
    class Model:
        phase = 0; answers = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            names = {t['name'] for t in kwargs.get('tools',[])}
            if not names: return text('{}')
            self.phase += 1
            stage = (self.phase-1)%3
            if stage == 0:
                return call('request_tool_scope', {'capabilities':['weather'], 'tool_names':['weather_forecast'],
                    'required_tool_names':['weather_forecast'], 'direct_read_complete_request':True})
            if stage == 1:
                return call('weather_forecast', {'location_text':'Harbor City', 'forecast_days':3})
            context = json.dumps(kwargs['messages'], ensure_ascii=False)
            assert dates[1] in context and '69' in context
            self.answers += 1
            if self.answers == 1: return text('Harbor City is currently 40°F, feeling like 35°F.')
            assert followup in context and 'Harbor City' in context
            return text('Tomorrow in Harbor City: high 69°F, low 50°F; 10% chance of precipitation.')
    model = Model()
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        first = client.post('/api/chat', json={'text':'What is the weather in Harbor City?', 'conversation_id':'web:period'})
        second = client.post('/api/chat', json={'text':followup,'conversation_id':'web:period'})
    assert first.status_code == second.status_code == 200
    assert '40°F' in first.json()['text']
    assert '69°F' in second.json()['text'] and '50°F' in second.json()['text']
    assert '40°F' not in second.json()['text'] and 'Today:' not in second.json()['text']
    assert model.answers == 2 and model.phase == 6
    turns = [e for e in runtime.store.list_conversation_events('web:period') if e.get('event_type') == 'conversation.chat_turn']
    assert len(turns) == 2 and all(any(r['tool_name']=='weather_forecast' for r in e['tool_results']) for e in turns)


def test_quote_question_keeps_requested_volume_field_in_primary_answer(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, register_market_quote_tool
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch); registry = ToolRegistry()
    register_market_quote_tool(registry, quote_fetcher=lambda symbol, timeout: {'symbol':symbol,'price':101,'volume':999})
    class Model:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1: return call('request_tool_scope', {'tool_names':['market_quote'],
                'required_tool_names':['market_quote'], 'direct_read_complete_request':True})
            if self.phase == 2: return call('market_quote', {'symbols':['ZZZ']})
            assert self.phase == 3 and '999' in json.dumps(kwargs['messages'])
            return text('The reported trading volume for ZZZ is 999 shares.')
    model = Model()
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
        result = client.post('/api/chat',json={'text':'ZZZの取引出来高は？','conversation_id':'web:volume'})
    assert result.status_code == 200 and '999' in result.json()['text']
    assert model.phase == 3
