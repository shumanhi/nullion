"""Real Web source verification and response-only obligations in one request."""
import os
import json
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
import pytest
from tests.e2e.test_live_stage_web_matrix import live_web,_submit,catalogue

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in stage UI')

@pytest.mark.parametrize('prompt',["ニューヨーク市の明日の天気を確認して短く答え、時間別の内訳が必要か最後に質問してください。",
    'Consulta el pronóstico de mañana para Nueva York. Responde brevemente y termina preguntando si quiero el desglose por hora.'])
def test_live_web_forecast_and_composed_question_use_different_proof(live_web,record_property,prompt):
    response,turn,elapsed=_submit(live_web,prompt,timeout=150)
    assert any(t['tool_name']=='weather_forecast' and t['status']=='completed' for t in turn.get('tool_results',[])),turn
    assert '?' in response['text'] or '？' in response['text'],response
    assert turn.get('response_fulfilled') is True,turn
    tomorrow=(datetime.now(ZoneInfo('America/New_York'))+timedelta(days=1)).date().isoformat()
    forecasts=[t['output'] for t in turn.get('tool_results',[]) if t['tool_name']=='weather_forecast' and t['status']=='completed']
    assert any(tomorrow in json.dumps(result) for result in forecasts),forecasts
    assert not response.get('artifacts')
    assert elapsed<120000,elapsed
    record_property('forecast_question_latency_ms',elapsed)
