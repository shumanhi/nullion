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
