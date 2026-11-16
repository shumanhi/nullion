"""Native weather adapter keeps observation labels separate from daily forecasts."""
import pytest
from nullion.tools import ToolRegistry, ToolInvocation, register_weather_forecast_tool


@pytest.mark.parametrize('current,expected', [
    ({'time': '2026-11-03T12:30', 'temperature_2m': 68.2, 'weather_code': 0}, 'clear sky'),
    ({'time': '2026-11-03T12:30', 'weather_code': 63}, 'moderate rain'),
    ({'time': '2026-11-03T12:30', 'weather_code': 999}, 'weather code 999'),
    ({}, None),
])
def test_current_conditions_are_labeled_independently(current, expected):
    payload = {'current': current, 'timezone': 'America/New_York',
        'daily': {'time': ['2026-11-03'], 'weather_code': [3]}}
    registry = ToolRegistry()
    register_weather_forecast_tool(registry, json_get=lambda url, timeout: payload)
    result = registry.invoke(ToolInvocation('native-weather', 'weather_forecast', 'operator',
        {'latitude': 40.7, 'longitude': -74.0}))
    assert result.status == 'completed'
    assert result.output['current'].get('summary') == expected
    assert result.output['current'].get('time') == current.get('time')
    assert result.output['daily'][0]['summary'] == 'overcast'
    assert 'summary' not in current, 'Adapter must not mutate the provider payload'


@pytest.mark.parametrize('address,label', [
    ({'house_number':'12','road':'Long Street','city':'Harbor City','state':'North Region','country':'Example'}, 'Harbor City, North Region'),
    ({'village':'港村','state':'北地方'}, '港村, 北地方'),
])
def test_weather_uses_structured_locality_label_without_street_address(address, label):
    from nullion.tools import _resolve_nominatim_weather_location
    result = _resolve_nominatim_weather_location('precise saved location', json_get=lambda *args:
        [{'lat':'41','lon':'-90','display_name':'12 Long Street, internal administrative hierarchy', 'address':address}])
    assert result['name'] == label
    assert result['latitude'] == 41 and result['longitude'] == -90


def test_no_model_weather_fallback_preserves_every_dated_forecast():
    from nullion.tools import ToolResult
    from nullion.agent_orchestrator import _weather_forecast_completion_text
    result = ToolResult('x','weather_forecast','completed',{'current':{'temperature_2m':40},'daily':[
        {'date':'2027-04-15','temperature_max_f':47}, {'date':'2027-04-16','temperature_max_f':69}]})
    text = _weather_forecast_completion_text(result)
    assert 'Apr 15' in text and 'Apr 16' in text and '69°F' in text
    assert 'Today:' not in text
